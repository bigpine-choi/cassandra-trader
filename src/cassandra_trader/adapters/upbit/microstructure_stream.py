"""Lossless public trade and orderbook WebSocket adapter."""

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from cassandra_trader.adapters.upbit.public_stream import (
    ConnectPublicStream,
    LifecycleCallback,
    LifecycleStatus,
    PublicStreamConnectionError,
    PublicStreamProtocolError,
    StreamLifecycleEvent,
    _notify_lifecycle,
    _sdk_public_connection,
    normalize_markets,
)
from cassandra_trader.domain.microstructure import (
    MicrostructureEvent,
    OrderbookEvent,
    OrderbookUnit,
    TradeEvent,
    TradeSide,
)
from cassandra_trader.domain.public_stream import StreamType

_RECONNECT_DELAYS = (1.0, 2.0, 4.0, 8.0, 16.0, 30.0)
_VALID_ORDERBOOK_UNITS = frozenset((1, 5, 15, 30))


@dataclass(frozen=True, slots=True)
class MicrostructureFrame:
    """One losslessly retained raw frame and its normalized event."""

    raw_payload: bytes
    payload_sha256: str
    event: MicrostructureEvent


class UpbitMicrostructureStreamAdapter:
    """Subscribe without deduplication and reconnect with a fixed market set."""

    def __init__(
        self,
        *,
        connect: ConnectPublicStream | None = None,
        sleep=asyncio.sleep,
        clock=None,
        ticket_factory=None,
    ) -> None:
        self._connect = connect or _sdk_public_connection
        self._sleep = sleep
        self._clock = clock or (lambda: datetime.now(UTC))
        self._ticket_factory = ticket_factory

    def stream(
        self,
        markets: list[str] | tuple[str, ...],
        *,
        orderbook_units: int = 15,
        lifecycle_callback: LifecycleCallback | None = None,
    ) -> AsyncIterator[MicrostructureFrame]:
        normalized = normalize_markets(markets)
        if any(not market.isascii() for market in normalized):
            raise ValueError("markets must be ASCII Upbit market codes")
        validate_orderbook_units(orderbook_units)
        return self._stream(normalized, orderbook_units, lifecycle_callback)

    async def _stream(
        self,
        markets: tuple[str, ...],
        orderbook_units: int,
        lifecycle_callback: LifecycleCallback | None,
    ) -> AsyncIterator[MicrostructureFrame]:
        from uuid import uuid4

        backoff_index = 0
        reconnect_attempt = 0
        while True:
            ticket = self._ticket_factory() if self._ticket_factory is not None else str(uuid4())
            payload = build_microstructure_subscription_payload(markets, ticket, orderbook_units)
            connected = False
            manager = self._connect()
            socket = None
            body_error: BaseException | None = None
            cleanup_failed = False
            try:
                socket = await manager.__aenter__()
                await socket.send(json.dumps(payload, separators=(",", ":")))
                connected = True
                await _notify_lifecycle(
                    lifecycle_callback,
                    StreamLifecycleEvent(LifecycleStatus.CONNECTED, reconnect_attempt),
                )
                while True:
                    raw = await socket.recv()
                    frame = parse_microstructure_message(raw, observed_at=self._clock())
                    if isinstance(frame.event, OrderbookEvent) and (
                        frame.event.grouping_level != 0 or len(frame.event.units) > orderbook_units
                    ):
                        raise PublicStreamProtocolError(
                            "orderbook frame violates requested level or unit count"
                        )
                    backoff_index = 0
                    reconnect_attempt = 0
                    yield frame
            except BaseException as error:
                body_error = error
            finally:
                if connected:
                    try:
                        await _notify_lifecycle(
                            lifecycle_callback,
                            StreamLifecycleEvent(LifecycleStatus.DISCONNECTED, reconnect_attempt),
                        )
                    except BaseException as close_error:
                        cleanup_failed = True
                        if body_error is None:
                            body_error = close_error
                        else:
                            body_error.add_note(
                                f"failed to record disconnect lifecycle: {close_error}"
                            )
                if socket is not None:
                    try:
                        await manager.__aexit__(
                            None if body_error is None else type(body_error),
                            body_error,
                            None if body_error is None else body_error.__traceback__,
                        )
                    except BaseException as close_error:
                        cleanup_failed = True
                        if body_error is None:
                            body_error = close_error
                        else:
                            body_error.add_note(f"failed to close public stream: {close_error}")

            if body_error is None:
                raise PublicStreamConnectionError("Upbit public stream ended unexpectedly")
            if cleanup_failed:
                raise body_error
            if isinstance(body_error, (asyncio.CancelledError, GeneratorExit)):
                raise body_error
            if isinstance(body_error, PublicStreamProtocolError):
                raise body_error
            if not isinstance(body_error, PublicStreamConnectionError):
                if connected:
                    raise body_error
                raise PublicStreamConnectionError(
                    "failed to connect to Upbit public stream"
                ) from body_error

            delay = _RECONNECT_DELAYS[backoff_index]
            reconnect_attempt += 1
            await _notify_lifecycle(
                lifecycle_callback,
                StreamLifecycleEvent(
                    LifecycleStatus.RECONNECTING,
                    reconnect_attempt,
                    delay_seconds=delay,
                    detail=str(body_error),
                ),
            )
            await self._sleep(delay)
            backoff_index = min(backoff_index + 1, len(_RECONNECT_DELAYS) - 1)


def validate_orderbook_units(value: int) -> None:
    """Validate one supported Upbit orderbook depth unit."""

    if not isinstance(value, int) or isinstance(value, bool) or value not in _VALID_ORDERBOOK_UNITS:
        raise ValueError("orderbook_units must be one of 1, 5, 15, or 30")


def build_microstructure_subscription_payload(
    markets: tuple[str, ...],
    ticket: str,
    orderbook_units: int,
) -> list[dict[str, object]]:
    """Build one DEFAULT-format trade and orderbook subscription."""

    if not ticket:
        raise ValueError("ticket must not be empty")
    normalized = normalize_markets(markets)
    if any(not market.isascii() for market in normalized):
        raise ValueError("markets must be ASCII Upbit market codes")
    validate_orderbook_units(orderbook_units)
    return [
        {"ticket": ticket},
        {"type": "trade", "codes": list(normalized), "is_only_realtime": True},
        {
            "type": "orderbook",
            "codes": [f"{market}.{orderbook_units}" for market in normalized],
            "level": 0,
            "is_only_snapshot": False,
            "is_only_realtime": False,
        },
        {"format": "DEFAULT"},
    ]


def parse_microstructure_message(
    message: str | bytes,
    *,
    observed_at: datetime,
) -> MicrostructureFrame:
    """Parse DEFAULT JSON with Decimal while retaining exact decompressed bytes."""

    _require_utc(observed_at, "observed_at")
    raw_payload = message.encode("utf-8") if isinstance(message, str) else bytes(message)
    try:
        payload = json.loads(
            raw_payload,
            parse_float=Decimal,
            parse_constant=lambda value: _raise_constant(value),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise PublicStreamProtocolError("microstructure frame is not valid JSON") from error
    if not isinstance(payload, dict):
        raise PublicStreamProtocolError("microstructure frame must be a JSON object")
    if "error" in payload:
        raise PublicStreamProtocolError(f"Upbit server error: {payload['error']}")
    event_type = _required(payload, "type")
    try:
        if event_type == "trade":
            event = _trade_event(payload, observed_at)
        elif event_type == "orderbook":
            event = _orderbook_event(payload, observed_at)
        else:
            raise PublicStreamProtocolError(f"unsupported microstructure type: {event_type}")
    except PublicStreamProtocolError:
        raise
    except (InvalidOperation, OverflowError, TypeError, ValueError) as error:
        raise PublicStreamProtocolError(f"invalid {event_type} frame: {error}") from error
    return MicrostructureFrame(
        raw_payload=raw_payload,
        payload_sha256=hashlib.sha256(raw_payload).hexdigest(),
        event=event,
    )


def _trade_event(payload: dict[str, object], observed_at: datetime) -> TradeEvent:
    return TradeEvent(
        market=_string(_required(payload, "code"), "code"),
        trade_price=_decimal(_required(payload, "trade_price"), "trade_price"),
        trade_volume=_decimal(_required(payload, "trade_volume"), "trade_volume"),
        side=TradeSide(_string(_required(payload, "ask_bid"), "ask_bid")),
        trade_timestamp_utc=_milliseconds(_required(payload, "trade_timestamp"), "trade_timestamp"),
        message_timestamp_utc=_milliseconds(_required(payload, "timestamp"), "timestamp"),
        sequential_id=_integer(_required(payload, "sequential_id"), "sequential_id"),
        best_ask_price=_decimal(_required(payload, "best_ask_price"), "best_ask_price"),
        best_ask_size=_decimal(_required(payload, "best_ask_size"), "best_ask_size"),
        best_bid_price=_decimal(_required(payload, "best_bid_price"), "best_bid_price"),
        best_bid_size=_decimal(_required(payload, "best_bid_size"), "best_bid_size"),
        stream_type=StreamType(_string(_required(payload, "stream_type"), "stream_type")),
        observed_at=observed_at,
    )


def _orderbook_event(payload: dict[str, object], observed_at: datetime) -> OrderbookEvent:
    raw_units = _required(payload, "orderbook_units")
    if not isinstance(raw_units, list):
        raise ValueError("orderbook_units must be a list")
    units = tuple(_orderbook_unit(item) for item in raw_units)
    return OrderbookEvent(
        market=_string(_required(payload, "code"), "code"),
        message_timestamp_utc=_milliseconds(_required(payload, "timestamp"), "timestamp"),
        total_ask_size=_decimal(_required(payload, "total_ask_size"), "total_ask_size"),
        total_bid_size=_decimal(_required(payload, "total_bid_size"), "total_bid_size"),
        grouping_level=_decimal(_required(payload, "level"), "level"),
        stream_type=StreamType(_string(_required(payload, "stream_type"), "stream_type")),
        units=units,
        observed_at=observed_at,
    )


def _orderbook_unit(value: object) -> OrderbookUnit:
    if not isinstance(value, dict):
        raise ValueError("orderbook unit must be an object")
    return OrderbookUnit(
        ask_price=_decimal(_required(value, "ask_price"), "ask_price"),
        bid_price=_decimal(_required(value, "bid_price"), "bid_price"),
        ask_size=_decimal(_required(value, "ask_size"), "ask_size"),
        bid_size=_decimal(_required(value, "bid_size"), "bid_size"),
    )


def _required(payload: dict[str, object], name: str) -> object:
    value = payload.get(name)
    if value is None:
        raise PublicStreamProtocolError(f"missing required field: {name}")
    return value


def _decimal(value: object, name: str) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be numeric")
    number = value if isinstance(value, Decimal) else Decimal(str(value))
    if not number.is_finite():
        raise ValueError(f"{name} must be finite")
    return number


def _integer(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    return value


def _milliseconds(value: object, name: str) -> datetime:
    milliseconds = _integer(value, name)
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=milliseconds)


def _string(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    return value


def _require_utc(value: datetime, name: str) -> None:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be timezone-aware UTC")


def _raise_constant(value: str) -> Any:
    raise ValueError(f"invalid JSON numeric constant: {value}")
