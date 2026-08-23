import asyncio
import hashlib
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from cassandra_trader.adapters.upbit.microstructure_stream import (
    MicrostructureFrame,
    UpbitMicrostructureStreamAdapter,
    build_microstructure_subscription_payload,
    parse_microstructure_message,
    validate_orderbook_units,
)
from cassandra_trader.adapters.upbit.public_stream import (
    LifecycleStatus,
    PublicStreamConnectionError,
    PublicStreamProtocolError,
)
from cassandra_trader.domain.microstructure import OrderbookEvent, TradeEvent, TradeSide
from cassandra_trader.domain.public_stream import StreamType

_OBSERVED = datetime(2026, 8, 24, 12, 10, 1, 123000, tzinfo=UTC)


def _trade_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "type": "trade",
        "code": "KRW-BTC",
        "trade_price": 100473000.123456789123456789,
        "trade_volume": 0.000123456789123456789,
        "ask_bid": "BID",
        "trade_timestamp": 1787573401001,
        "timestamp": 1787573401002,
        "sequential_id": 123456789,
        "best_ask_price": 100474000.00000000,
        "best_ask_size": 1.200000000000000001,
        "best_bid_price": 100473000.00000000,
        "best_bid_size": 2.300000000000000002,
        "stream_type": "REALTIME",
    }
    payload.update(overrides)
    return payload


def _trade_bytes(**overrides: object) -> bytes:
    payload = _trade_payload(**overrides)
    return json.dumps(payload, separators=(",", ":")).encode()


def _precise_trade_bytes() -> bytes:
    return (
        b'{"type":"trade","code":"KRW-BTC",'
        b'"trade_price":100473000.123456789123456789,'
        b'"trade_volume":0.000123456789123456789,"ask_bid":"BID",'
        b'"trade_timestamp":1787573401001,"timestamp":1787573401002,'
        b'"sequential_id":123456789,"best_ask_price":100474000.00000000,'
        b'"best_ask_size":1.200000000000000001,'
        b'"best_bid_price":100473000.00000000,'
        b'"best_bid_size":2.300000000000000002,"stream_type":"REALTIME"}'
    )


def _orderbook_bytes(**overrides: object) -> bytes:
    payload: dict[str, object] = {
        "type": "orderbook",
        "code": "KRW-BTC",
        "timestamp": 1787573401003,
        "total_ask_size": "4.791584130000000001",
        "total_bid_size": "2.656096250000000002",
        "orderbook_units": [
            {
                "ask_price": "137002000.000000001",
                "bid_price": "137001000.000000001",
                "ask_size": "0.106238690000000001",
                "bid_size": "0.036568120000000001",
            },
            {
                "ask_price": "137023000.000000002",
                "bid_price": "137000000.000000002",
                "ask_size": "0.061440790000000002",
                "bid_size": "0.335432840000000002",
            },
        ],
        "stream_type": "SNAPSHOT",
        "level": 0,
    }
    payload.update(overrides)
    return json.dumps(payload, separators=(",", ":")).encode()


@pytest.mark.parametrize("units", [1, 5, 15, 30])
def test_combined_trade_orderbook_subscription_payload(units: int) -> None:
    payload = build_microstructure_subscription_payload(
        ("KRW-BTC", "KRW-ETH"),
        "ticket-1",
        units,
    )

    assert payload == [
        {"ticket": "ticket-1"},
        {
            "type": "trade",
            "codes": ["KRW-BTC", "KRW-ETH"],
            "is_only_realtime": True,
        },
        {
            "type": "orderbook",
            "codes": [f"KRW-BTC.{units}", f"KRW-ETH.{units}"],
            "level": 0,
            "is_only_snapshot": False,
            "is_only_realtime": False,
        },
        {"format": "DEFAULT"},
    ]


@pytest.mark.parametrize("units", [0, 2, 10, 31, True, 15.0])
def test_invalid_orderbook_unit_is_rejected(units: object) -> None:
    with pytest.raises(ValueError):
        validate_orderbook_units(units)  # type: ignore[arg-type]


def test_trade_parser_preserves_raw_bytes_hash_and_decimal_precision() -> None:
    raw = _precise_trade_bytes()

    frame = parse_microstructure_message(raw.decode(), observed_at=_OBSERVED)

    assert frame.raw_payload == raw
    assert frame.payload_sha256 == hashlib.sha256(raw).hexdigest()
    assert isinstance(frame.event, TradeEvent)
    assert frame.event.trade_price == Decimal("100473000.123456789123456789")
    assert frame.event.trade_volume == Decimal("0.000123456789123456789")
    assert frame.event.side is TradeSide.BID
    assert frame.event.sequential_id == 123456789


def test_orderbook_parser_preserves_every_level_in_source_order() -> None:
    raw = _orderbook_bytes()

    frame = parse_microstructure_message(raw, observed_at=_OBSERVED)

    assert isinstance(frame.event, OrderbookEvent)
    assert frame.event.stream_type is StreamType.SNAPSHOT
    assert frame.event.total_ask_size == Decimal("4.791584130000000001")
    assert [unit.ask_price for unit in frame.event.units] == [
        Decimal("137002000.000000001"),
        Decimal("137023000.000000002"),
    ]


@pytest.mark.parametrize(
    "raw",
    [
        b"{}",
        _trade_bytes(trade_price=True),
        _trade_bytes(trade_volume="NaN"),
        _trade_bytes(code="KRW-\ube44\ud2b8"),
        _orderbook_bytes(orderbook_units=[]),
        _orderbook_bytes(total_bid_size=True),
        b'{"type":"trade","trade_price":NaN}',
    ],
)
def test_protocol_validation_rejects_missing_bool_nonfinite_and_invalid_market(raw: bytes) -> None:
    with pytest.raises(PublicStreamProtocolError):
        parse_microstructure_message(raw, observed_at=_OBSERVED)


class _Socket:
    def __init__(self, messages: list[str | bytes | BaseException]) -> None:
        self.messages = list(messages)
        self.sent: list[str] = []

    async def send(self, data: str) -> None:
        self.sent.append(data)

    async def recv(self) -> str | bytes:
        item = self.messages.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item


def test_reconnect_uses_new_ticket_and_preserves_orderbook_snapshot() -> None:
    first = _Socket([PublicStreamConnectionError("disconnected")])
    second = _Socket([_orderbook_bytes()])
    sockets = [first, second]
    lifecycle = []
    tickets = iter(("ticket-1", "ticket-2"))

    @asynccontextmanager
    async def connect() -> AsyncIterator[_Socket]:
        yield sockets.pop(0)

    async def collect() -> tuple[MicrostructureFrame, ...]:
        adapter = UpbitMicrostructureStreamAdapter(
            connect=connect,
            sleep=lambda delay: _no_sleep(),
            clock=lambda: _OBSERVED,
            ticket_factory=lambda: next(tickets),
        )
        stream = adapter.stream(
            ("KRW-BTC",),
            orderbook_units=15,
            lifecycle_callback=lifecycle.append,
        )
        frame = await anext(stream)
        await stream.aclose()
        return (frame,)

    frames = asyncio.run(collect())

    assert isinstance(frames[0].event, OrderbookEvent)
    assert frames[0].event.stream_type is StreamType.SNAPSHOT
    assert json.loads(first.sent[0])[0] == {"ticket": "ticket-1"}
    assert json.loads(second.sent[0])[0] == {"ticket": "ticket-2"}
    assert [event.status for event in lifecycle] == [
        LifecycleStatus.CONNECTED,
        LifecycleStatus.DISCONNECTED,
        LifecycleStatus.RECONNECTING,
        LifecycleStatus.CONNECTED,
        LifecycleStatus.DISCONNECTED,
    ]


def test_disconnect_lifecycle_failure_stops_instead_of_reconnecting() -> None:
    socket = _Socket([PublicStreamConnectionError("disconnected")])
    connect_count = 0
    sleeps = []

    @asynccontextmanager
    async def connect() -> AsyncIterator[_Socket]:
        nonlocal connect_count
        connect_count += 1
        yield socket

    async def lifecycle(event) -> None:
        if event.status is LifecycleStatus.DISCONNECTED:
            raise OSError("lifecycle storage failed")

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    async def receive() -> None:
        adapter = UpbitMicrostructureStreamAdapter(
            connect=connect,
            sleep=sleep,
            clock=lambda: _OBSERVED,
            ticket_factory=lambda: "ticket-1",
        )
        await anext(
            adapter.stream(
                ("KRW-BTC",),
                orderbook_units=15,
                lifecycle_callback=lifecycle,
            )
        )

    with pytest.raises(PublicStreamConnectionError, match="disconnected") as caught:
        asyncio.run(receive())

    assert connect_count == 1
    assert sleeps == []
    assert any("failed to record disconnect lifecycle" in note for note in caught.value.__notes__)


async def _no_sleep() -> None:
    return None
