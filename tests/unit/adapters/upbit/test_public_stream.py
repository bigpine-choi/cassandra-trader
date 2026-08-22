import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from cassandra_trader.adapters.upbit.public_stream import (
    LifecycleStatus,
    PublicStreamConnectionError,
    PublicStreamProtocolError,
    PublicStreamSocket,
    StreamLifecycleEvent,
    UpbitPublicStreamAdapter,
    normalize_markets,
    parse_public_stream_message,
)
from cassandra_trader.domain.public_stream import MinuteCandleUpdate, StreamType, TickerEvent

_RECEIVED_AT = datetime(2026, 8, 22, 17, 0, tzinfo=UTC)
_TICKER_TIMESTAMP = 1787417999123
_CANDLE_TIMESTAMP = 1787417999456


def _ticker_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "type": "ticker",
        "code": "KRW-BTC",
        "trade_price": "105307000.123456789123456789",
        "signed_change_rate": "-0.00123456789123456789",
        "acc_trade_price_24h": "9876543210.123456789",
        "trade_timestamp": _TICKER_TIMESTAMP,
        "stream_type": "REALTIME",
        "ignored_future_field": {"value": 1},
    }
    payload.update(overrides)
    return payload


def _candle_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "type": "candle.1m",
        "code": "KRW-BTC",
        "candle_date_time_utc": "2026-08-22T16:59:00",
        "opening_price": "105300000.0000000001",
        "high_price": "105400000.0000000002",
        "low_price": "105200000.0000000003",
        "trade_price": "105307000.0000000004",
        "candle_acc_trade_volume": "0.123456789123456789",
        "candle_acc_trade_price": "12900000.987654321987654321",
        "timestamp": _CANDLE_TIMESTAMP,
        "stream_type": "REALTIME",
        "ignored_future_field": "safe",
    }
    payload.update(overrides)
    return payload


def _message(payload: dict[str, object]) -> str:
    return json.dumps(payload, separators=(",", ":"))


class FakeSocket:
    def __init__(
        self,
        messages: list[str | bytes | BaseException],
        *,
        wait_when_empty: bool = False,
    ) -> None:
        self.messages = list(messages)
        self.wait_when_empty = wait_when_empty
        self.sent: list[str] = []
        self.closed = False
        self.waiting = asyncio.Event()

    async def send(self, data: str) -> None:
        self.sent.append(data)

    async def recv(self) -> str | bytes:
        if self.messages:
            item = self.messages.pop(0)

            if isinstance(item, BaseException):
                raise item

            return item

        if self.wait_when_empty:
            self.waiting.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

        raise PublicStreamConnectionError("fake connection ended")


class FakeConnector:
    def __init__(self, sockets: list[FakeSocket]) -> None:
        self.sockets = list(sockets)
        self.connect_count = 0

    def __call__(self) -> AbstractAsyncContextManager[PublicStreamSocket]:
        socket = self.sockets[self.connect_count]
        self.connect_count += 1

        @asynccontextmanager
        async def connection() -> AsyncIterator[PublicStreamSocket]:
            try:
                yield socket
            finally:
                socket.closed = True

        return connection()


async def _take_events(
    adapter: UpbitPublicStreamAdapter,
    count: int,
    markets: list[str] | tuple[str, ...] = ("KRW-BTC",),
    *,
    lifecycle_callback=None,
):
    stream = adapter.stream(markets, lifecycle_callback=lifecycle_callback)
    events = []

    try:
        for _ in range(count):
            events.append(await anext(stream))
    finally:
        await stream.aclose()

    return events


def test_subscription_combines_ticker_and_candle_for_normalized_markets() -> None:
    socket = FakeSocket([_message(_ticker_payload())], wait_when_empty=True)
    adapter = UpbitPublicStreamAdapter(
        connect=FakeConnector([socket]),
        ticket_factory=lambda: "ticket-1",
    )

    events = asyncio.run(_take_events(adapter, 1, [" krw-btc ", "KRW-ETH", "krw-btc"]))

    assert len(events) == 1
    assert len(socket.sent) == 1
    assert json.loads(socket.sent[0]) == [
        {"ticket": "ticket-1"},
        {
            "type": "ticker",
            "codes": ["KRW-BTC", "KRW-ETH"],
            "is_only_realtime": True,
        },
        {
            "type": "candle.1m",
            "codes": ["KRW-BTC", "KRW-ETH"],
            "is_only_realtime": True,
        },
        {"format": "DEFAULT"},
    ]
    assert socket.closed is True


@pytest.mark.parametrize("markets", [[], [""], ["BTC"], ["KRW-BTC!"]])
def test_invalid_markets_are_rejected_before_connect(markets: list[str]) -> None:
    connector = FakeConnector([])
    adapter = UpbitPublicStreamAdapter(connect=connector)

    with pytest.raises(ValueError):
        asyncio.run(anext(adapter.stream(markets)))

    assert connector.connect_count == 0


def test_normalize_markets_preserves_first_seen_order() -> None:
    assert normalize_markets(["krw-eth", "KRW-BTC", " KRW-ETH "]) == (
        "KRW-ETH",
        "KRW-BTC",
    )


@pytest.mark.parametrize("as_bytes", [False, True])
def test_ticker_frame_maps_to_domain_event_with_decimal_precision(as_bytes: bool) -> None:
    message: str | bytes = _message(_ticker_payload())

    if as_bytes:
        message = message.encode()

    event = parse_public_stream_message(message, received_at=_RECEIVED_AT)

    assert isinstance(event, TickerEvent)
    assert event.market == "KRW-BTC"
    assert event.trade_price == Decimal("105307000.123456789123456789")
    assert event.signed_change_rate == Decimal("-0.00123456789123456789")
    assert event.acc_trade_price_24h == Decimal("9876543210.123456789")
    assert event.trade_timestamp_utc == datetime(2026, 8, 22, 16, 59, 59, 123000, tzinfo=UTC)
    assert event.received_at == _RECEIVED_AT
    assert event.stream_type is StreamType.REALTIME


def test_json_number_uses_decimal_without_float_round_trip() -> None:
    message = (
        '{"type":"ticker","code":"KRW-BTC",'
        '"trade_price":105307000.123456789123456789,'
        '"signed_change_rate":-0.00123456789123456789,'
        '"acc_trade_price_24h":9876543210.123456789,'
        f'"trade_timestamp":{_TICKER_TIMESTAMP},"stream_type":"REALTIME"}}'
    )

    event = parse_public_stream_message(message, received_at=_RECEIVED_AT)

    assert isinstance(event, TickerEvent)
    assert event.trade_price == Decimal("105307000.123456789123456789")
    assert event.signed_change_rate == Decimal("-0.00123456789123456789")


@pytest.mark.parametrize("as_bytes", [False, True])
def test_candle_frame_maps_to_provisional_domain_update(as_bytes: bool) -> None:
    message: str | bytes = _message(_candle_payload())

    if as_bytes:
        message = message.encode()

    event = parse_public_stream_message(message, received_at=_RECEIVED_AT)

    assert isinstance(event, MinuteCandleUpdate)
    assert event.start_time_utc == datetime(2026, 8, 22, 16, 59, tzinfo=UTC)
    assert event.end_time_utc == datetime(2026, 8, 22, 17, 0, tzinfo=UTC)
    assert event.open == Decimal("105300000.0000000001")
    assert event.high == Decimal("105400000.0000000002")
    assert event.low == Decimal("105200000.0000000003")
    assert event.close == Decimal("105307000.0000000004")
    assert event.base_volume == Decimal("0.123456789123456789")
    assert event.quote_volume == Decimal("12900000.987654321987654321")
    assert event.event_time_utc == datetime(2026, 8, 22, 16, 59, 59, 456000, tzinfo=UTC)
    assert not hasattr(event, "is_final")


def test_identical_retransmission_is_suppressed_but_changed_candle_is_kept() -> None:
    original = _message(_candle_payload())
    changed = _message(
        _candle_payload(
            trade_price="105308000.0000000004",
            timestamp=_CANDLE_TIMESTAMP + 1,
        )
    )
    socket = FakeSocket([original, original, changed], wait_when_empty=True)
    adapter = UpbitPublicStreamAdapter(connect=FakeConnector([socket]))

    events = asyncio.run(_take_events(adapter, 2))

    assert [event.close for event in events if isinstance(event, MinuteCandleUpdate)] == [
        Decimal("105307000.0000000004"),
        Decimal("105308000.0000000004"),
    ]
    assert events[0].start_time_utc == events[1].start_time_utc


def test_ticker_value_or_timestamp_change_is_not_suppressed() -> None:
    first = _message(_ticker_payload())
    second = _message(
        _ticker_payload(
            trade_price="105307001.123456789123456789",
            trade_timestamp=_TICKER_TIMESTAMP + 1,
        )
    )
    socket = FakeSocket([first, first, second], wait_when_empty=True)
    adapter = UpbitPublicStreamAdapter(connect=FakeConnector([socket]))

    events = asyncio.run(_take_events(adapter, 2))

    assert [event.trade_price for event in events if isinstance(event, TickerEvent)] == [
        Decimal("105307000.123456789123456789"),
        Decimal("105307001.123456789123456789"),
    ]


def test_bounded_duplicate_cache_evicts_oldest_event() -> None:
    first = _message(_ticker_payload(trade_price="1"))
    second = _message(_ticker_payload(trade_price="2"))
    third = _message(_ticker_payload(trade_price="3"))
    socket = FakeSocket([first, second, third, first], wait_when_empty=True)
    adapter = UpbitPublicStreamAdapter(
        connect=FakeConnector([socket]),
        duplicate_cache_size=2,
    )

    events = asyncio.run(_take_events(adapter, 4))

    assert [event.trade_price for event in events if isinstance(event, TickerEvent)] == [
        Decimal("1"),
        Decimal("2"),
        Decimal("3"),
        Decimal("1"),
    ]


def test_server_error_frame_raises_protocol_exception() -> None:
    message = json.dumps({"error": {"name": "WRONG_FORMAT", "message": "Format is invalid"}})

    with pytest.raises(PublicStreamProtocolError, match="WRONG_FORMAT"):
        parse_public_stream_message(message, received_at=_RECEIVED_AT)


def test_missing_required_field_is_not_silently_ignored() -> None:
    payload = _ticker_payload()
    del payload["trade_price"]

    with pytest.raises(PublicStreamProtocolError, match="trade_price"):
        parse_public_stream_message(_message(payload), received_at=_RECEIVED_AT)


def test_connection_close_backs_off_and_resubscribes_with_new_ticket() -> None:
    first = FakeSocket([PublicStreamConnectionError("lost")])
    second = FakeSocket([_message(_ticker_payload())], wait_when_empty=True)
    connector = FakeConnector([first, second])
    delays: list[float] = []
    lifecycle: list[StreamLifecycleEvent] = []
    tickets = iter(["ticket-1", "ticket-2"])

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)

    adapter = UpbitPublicStreamAdapter(
        connect=connector,
        sleep=fake_sleep,
        ticket_factory=lambda: next(tickets),
    )

    events = asyncio.run(_take_events(adapter, 1, lifecycle_callback=lifecycle.append))

    assert len(events) == 1
    assert delays == [1.0]
    assert json.loads(first.sent[0])[0] == {"ticket": "ticket-1"}
    assert json.loads(second.sent[0])[0] == {"ticket": "ticket-2"}
    assert [event.status for event in lifecycle] == [
        LifecycleStatus.CONNECTED,
        LifecycleStatus.DISCONNECTED,
        LifecycleStatus.RECONNECTING,
        LifecycleStatus.CONNECTED,
        LifecycleStatus.DISCONNECTED,
    ]


def test_first_normal_message_resets_exponential_backoff() -> None:
    sockets = [
        FakeSocket([PublicStreamConnectionError("one")]),
        FakeSocket([PublicStreamConnectionError("two")]),
        FakeSocket([_message(_ticker_payload()), PublicStreamConnectionError("three")]),
        FakeSocket([_message(_candle_payload())], wait_when_empty=True),
    ]
    delays: list[float] = []

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)

    adapter = UpbitPublicStreamAdapter(
        connect=FakeConnector(sockets),
        sleep=fake_sleep,
    )

    events = asyncio.run(_take_events(adapter, 2))

    assert isinstance(events[0], TickerEvent)
    assert isinstance(events[1], MinuteCandleUpdate)
    assert delays == [1.0, 2.0, 1.0]


def test_exponential_backoff_is_capped_at_thirty_seconds() -> None:
    sockets = [FakeSocket([PublicStreamConnectionError(f"failure-{index}")]) for index in range(7)]
    sockets.append(FakeSocket([_message(_ticker_payload())], wait_when_empty=True))
    delays: list[float] = []

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)

    adapter = UpbitPublicStreamAdapter(
        connect=FakeConnector(sockets),
        sleep=fake_sleep,
    )

    asyncio.run(_take_events(adapter, 1))

    assert delays == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0]


def test_cancellation_closes_socket_without_reconnect() -> None:
    async def scenario() -> tuple[bool, list[float]]:
        socket = FakeSocket([], wait_when_empty=True)
        delays: list[float] = []

        async def fake_sleep(delay: float) -> None:
            delays.append(delay)

        adapter = UpbitPublicStreamAdapter(
            connect=FakeConnector([socket]),
            sleep=fake_sleep,
        )
        stream = adapter.stream(["KRW-BTC"])
        pending = asyncio.create_task(anext(stream))
        await socket.waiting.wait()
        pending.cancel()

        with pytest.raises(asyncio.CancelledError):
            await pending

        await stream.aclose()
        return socket.closed, delays

    closed, delays = asyncio.run(scenario())

    assert closed is True
    assert delays == []
