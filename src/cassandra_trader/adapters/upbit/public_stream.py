"""Resilient adapter for Upbit's unauthenticated public WebSocket stream."""

import asyncio
import inspect
import json
from collections import OrderedDict
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any, Protocol, cast
from uuid import uuid4

from upbit import AsyncUpbit

from cassandra_trader.domain.public_stream import (
    MinuteCandleUpdate,
    PublicMarketEvent,
    StreamType,
    TickerEvent,
)

_BACKOFF_SECONDS = (1.0, 2.0, 4.0, 8.0, 16.0, 30.0)
_DEFAULT_DUPLICATE_CACHE_SIZE = 4096


class PublicStreamProtocolError(RuntimeError):
    """An Upbit frame violates the expected public-stream protocol."""


class PublicStreamConnectionError(ConnectionError):
    """The public WebSocket connection ended unexpectedly."""


class LifecycleStatus(StrEnum):
    """Observable connection lifecycle states."""

    CONNECTED = "CONNECTED"
    DISCONNECTED = "DISCONNECTED"
    RECONNECTING = "RECONNECTING"


@dataclass(frozen=True, slots=True)
class StreamLifecycleEvent:
    """One observable public-stream connection state transition."""

    status: LifecycleStatus
    attempt: int
    delay_seconds: float | None = None
    detail: str | None = None


class PublicStreamSocket(Protocol):
    """Minimal transport used by the resilient stream loop."""

    async def send(self, data: str) -> None: ...

    async def recv(self) -> str | bytes: ...


type ConnectPublicStream = Callable[[], AbstractAsyncContextManager[PublicStreamSocket]]
type Sleep = Callable[[float], Awaitable[None]]
type Clock = Callable[[], datetime]
type TicketFactory = Callable[[], str]
type LifecycleCallback = Callable[[StreamLifecycleEvent], object]
type EventFingerprint = tuple[object, ...]


class UpbitPublicStreamAdapter:
    """Subscribe, parse, deduplicate, and reconnect the Upbit public stream."""

    def __init__(
        self,
        *,
        connect: ConnectPublicStream | None = None,
        sleep: Sleep = asyncio.sleep,
        clock: Clock | None = None,
        ticket_factory: TicketFactory | None = None,
        duplicate_cache_size: int = _DEFAULT_DUPLICATE_CACHE_SIZE,
    ) -> None:
        if duplicate_cache_size < 1:
            raise ValueError("duplicate_cache_size must be at least 1")

        self._connect = connect
        self._sleep = sleep
        self._clock = clock or (lambda: datetime.now(UTC))
        self._ticket_factory = ticket_factory or (lambda: str(uuid4()))
        self._duplicate_cache_size = duplicate_cache_size

    def stream(
        self,
        markets: list[str] | tuple[str, ...],
        *,
        lifecycle_callback: LifecycleCallback | None = None,
    ) -> AsyncIterator[PublicMarketEvent]:
        """Yield domain events in received order until cancelled or closed."""

        normalized_markets = normalize_markets(markets)
        connect = self._connect or _sdk_public_connection
        return self._stream_with_connector(
            normalized_markets,
            connect,
            lifecycle_callback,
        )

    async def _stream_with_connector(
        self,
        markets: tuple[str, ...],
        connect: ConnectPublicStream,
        lifecycle_callback: LifecycleCallback | None,
    ) -> AsyncIterator[PublicMarketEvent]:
        duplicate_cache: OrderedDict[EventFingerprint, None] = OrderedDict()
        backoff_index = 0
        reconnect_attempt = 0

        while True:
            ticket = self._ticket_factory()
            payload = build_subscription_payload(markets, ticket)
            connected = False

            try:
                async with connect() as socket:
                    await socket.send(json.dumps(payload, separators=(",", ":")))
                    connected = True
                    await _notify_lifecycle(
                        lifecycle_callback,
                        StreamLifecycleEvent(
                            status=LifecycleStatus.CONNECTED,
                            attempt=reconnect_attempt,
                        ),
                    )

                    try:
                        while True:
                            raw_message = await socket.recv()
                            event = parse_public_stream_message(
                                raw_message,
                                received_at=self._clock(),
                            )
                            backoff_index = 0
                            reconnect_attempt = 0
                            fingerprint = _event_fingerprint(event)

                            if fingerprint in duplicate_cache:
                                duplicate_cache.move_to_end(fingerprint)
                                continue

                            duplicate_cache[fingerprint] = None

                            if len(duplicate_cache) > self._duplicate_cache_size:
                                duplicate_cache.popitem(last=False)

                            yield event
                    finally:
                        await _notify_lifecycle(
                            lifecycle_callback,
                            StreamLifecycleEvent(
                                status=LifecycleStatus.DISCONNECTED,
                                attempt=reconnect_attempt,
                            ),
                        )
            except asyncio.CancelledError:
                raise
            except PublicStreamProtocolError:
                raise
            except PublicStreamConnectionError as error:
                delay = _BACKOFF_SECONDS[backoff_index]
                reconnect_attempt += 1
                await _notify_lifecycle(
                    lifecycle_callback,
                    StreamLifecycleEvent(
                        status=LifecycleStatus.RECONNECTING,
                        attempt=reconnect_attempt,
                        delay_seconds=delay,
                        detail=str(error),
                    ),
                )
                await self._sleep(delay)
                backoff_index = min(backoff_index + 1, len(_BACKOFF_SECONDS) - 1)
            except Exception as error:
                if connected:
                    raise
                raise PublicStreamConnectionError(
                    "failed to connect to Upbit public stream"
                ) from error


def normalize_markets(markets: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    """Normalize market codes while preserving first-seen order."""

    normalized: list[str] = []
    seen: set[str] = set()

    for market in markets:
        code = market.strip().upper()
        quote, separator, base = code.partition("-")

        if not separator or not quote.isalnum() or not base.isalnum():
            raise ValueError(f"Invalid market code: {market}")

        if code not in seen:
            seen.add(code)
            normalized.append(code)

    if not normalized:
        raise ValueError("at least one market is required")

    return tuple(normalized)


def build_subscription_payload(markets: tuple[str, ...], ticket: str) -> list[dict[str, object]]:
    """Build one combined realtime ticker and one-minute candle subscription."""

    if not ticket:
        raise ValueError("ticket must not be empty")

    codes = list(markets)
    return [
        {"ticket": ticket},
        {"type": "ticker", "codes": codes, "is_only_realtime": True},
        {"type": "candle.1m", "codes": codes, "is_only_realtime": True},
        {"format": "DEFAULT"},
    ]


def parse_public_stream_message(
    message: str | bytes,
    *,
    received_at: datetime,
) -> PublicMarketEvent:
    """Translate one DEFAULT-format Upbit frame into a domain event."""

    _require_utc(received_at, "received_at")

    try:
        payload = json.loads(message, parse_float=Decimal)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PublicStreamProtocolError("public stream frame is not valid JSON") from error

    if not isinstance(payload, dict):
        raise PublicStreamProtocolError("public stream frame must be a JSON object")

    if "error" in payload:
        error = payload["error"]

        if isinstance(error, dict):
            name = error.get("name", "UNKNOWN")
            detail = error.get("message", "no detail")
            raise PublicStreamProtocolError(f"Upbit server error {name}: {detail}")

        raise PublicStreamProtocolError(f"Upbit server error: {error}")

    event_type = _required(payload, "type")

    try:
        if event_type == "ticker":
            return _ticker_event(payload, received_at)

        if event_type == "candle.1m":
            return _minute_candle_update(payload, received_at)
    except (InvalidOperation, OverflowError, TypeError, ValueError) as error:
        raise PublicStreamProtocolError(f"invalid {event_type} frame: {error}") from error

    raise PublicStreamProtocolError(f"unsupported public stream type: {event_type}")


def _ticker_event(payload: dict[str, object], received_at: datetime) -> TickerEvent:
    return TickerEvent(
        market=_market(_required(payload, "code")),
        trade_price=_decimal(_required(payload, "trade_price"), "trade_price"),
        signed_change_rate=_decimal(
            _required(payload, "signed_change_rate"),
            "signed_change_rate",
        ),
        acc_trade_price_24h=_decimal(
            _required(payload, "acc_trade_price_24h"),
            "acc_trade_price_24h",
        ),
        trade_timestamp_utc=_milliseconds_utc(
            _required(payload, "trade_timestamp"),
            "trade_timestamp",
        ),
        received_at=received_at,
        stream_type=_stream_type(_required(payload, "stream_type")),
    )


def _minute_candle_update(
    payload: dict[str, object],
    received_at: datetime,
) -> MinuteCandleUpdate:
    start = _upbit_utc(_required(payload, "candle_date_time_utc"))
    return MinuteCandleUpdate(
        market=_market(_required(payload, "code")),
        start_time_utc=start,
        end_time_utc=start + timedelta(minutes=1),
        open=_decimal(_required(payload, "opening_price"), "opening_price"),
        high=_decimal(_required(payload, "high_price"), "high_price"),
        low=_decimal(_required(payload, "low_price"), "low_price"),
        close=_decimal(_required(payload, "trade_price"), "trade_price"),
        base_volume=_decimal(
            _required(payload, "candle_acc_trade_volume"),
            "candle_acc_trade_volume",
        ),
        quote_volume=_decimal(
            _required(payload, "candle_acc_trade_price"),
            "candle_acc_trade_price",
        ),
        event_time_utc=_milliseconds_utc(_required(payload, "timestamp"), "timestamp"),
        received_at=received_at,
        stream_type=_stream_type(_required(payload, "stream_type")),
    )


def _event_fingerprint(event: PublicMarketEvent) -> EventFingerprint:
    if isinstance(event, TickerEvent):
        return (
            "ticker",
            event.market,
            event.trade_price,
            event.signed_change_rate,
            event.acc_trade_price_24h,
            event.trade_timestamp_utc,
            event.stream_type,
        )

    return (
        "candle.1m",
        event.market,
        event.start_time_utc,
        event.open,
        event.high,
        event.low,
        event.close,
        event.base_volume,
        event.quote_volume,
        event.event_time_utc,
        event.stream_type,
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


def _market(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("code must be a string")

    return normalize_markets([value])[0]


def _stream_type(value: object) -> StreamType:
    if not isinstance(value, str):
        raise ValueError("stream_type must be a string")

    return StreamType(value)


def _milliseconds_utc(value: object, name: str) -> datetime:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer millisecond timestamp")

    milliseconds = int(cast(Any, value))

    if str(milliseconds) != str(value):
        raise ValueError(f"{name} must be an integer millisecond timestamp")

    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=milliseconds)


def _upbit_utc(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("candle_date_time_utc must be a string")

    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)

    _require_utc(parsed, "candle_date_time_utc")
    return parsed.astimezone(UTC)


def _require_utc(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be timezone-aware UTC")


async def _notify_lifecycle(
    callback: LifecycleCallback | None,
    event: StreamLifecycleEvent,
) -> None:
    if callback is None:
        return

    result = callback(event)

    if inspect.isawaitable(result):
        await cast(Awaitable[object], result)


class _SdkPublicStreamSocket:
    def __init__(self, connection: Any) -> None:
        self._connection = connection

    async def send(self, data: str) -> None:
        try:
            await self._connection.send_raw(data)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            raise PublicStreamConnectionError("Upbit public stream connection closed") from error

    async def recv(self) -> str | bytes:
        try:
            return await self._connection.recv_bytes()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            raise PublicStreamConnectionError("Upbit public stream connection closed") from error


@asynccontextmanager
async def _sdk_public_connection() -> AsyncIterator[PublicStreamSocket]:
    async with AsyncUpbit(environment="kr") as client:
        manager = client.ws_public.connect_public(
            websocket_connection_options={
                "ping_interval": 30.0,
                "ping_timeout": 10.0,
            }
        )

        try:
            connection = await manager.__aenter__()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            raise PublicStreamConnectionError("failed to connect to Upbit public stream") from error

        try:
            yield _SdkPublicStreamSocket(connection)
        finally:
            await manager.__aexit__(None, None, None)
