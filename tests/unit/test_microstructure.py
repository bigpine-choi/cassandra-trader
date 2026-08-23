from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from cassandra_trader.domain.microstructure import (
    OrderbookEvent,
    OrderbookUnit,
    TradeEvent,
    TradeSide,
)
from cassandra_trader.domain.public_stream import StreamType

_OBSERVED = datetime(2026, 8, 24, 12, 10, 1, 123000, tzinfo=UTC)
_MESSAGE = _OBSERVED - timedelta(milliseconds=3)


def _trade() -> TradeEvent:
    return TradeEvent(
        market="KRW-BTC",
        trade_price=Decimal("100473000.123456789"),
        trade_volume=Decimal("0.000123456789"),
        side=TradeSide.BID,
        trade_timestamp_utc=_MESSAGE - timedelta(milliseconds=1),
        message_timestamp_utc=_MESSAGE,
        sequential_id=123456789,
        best_ask_price=Decimal("100474000"),
        best_ask_size=Decimal("1.2"),
        best_bid_price=Decimal("100473000"),
        best_bid_size=Decimal("2.3"),
        stream_type=StreamType.REALTIME,
        observed_at=_OBSERVED,
    )


def _orderbook() -> OrderbookEvent:
    return OrderbookEvent(
        market="KRW-BTC",
        message_timestamp_utc=_MESSAGE,
        total_ask_size=Decimal("4.79158413"),
        total_bid_size=Decimal("2.65609625"),
        grouping_level=Decimal("0"),
        stream_type=StreamType.SNAPSHOT,
        units=(
            OrderbookUnit(
                ask_price=Decimal("137002000"),
                bid_price=Decimal("137001000"),
                ask_size=Decimal("0.10623869"),
                bid_size=Decimal("0.03656812"),
            ),
            OrderbookUnit(
                ask_price=Decimal("137023000"),
                bid_price=Decimal("137000000"),
                ask_size=Decimal("0.06144079"),
                bid_size=Decimal("0.33543284"),
            ),
        ),
        observed_at=_OBSERVED,
    )


def test_trade_and_orderbook_preserve_exact_decimals_and_ordered_levels() -> None:
    trade = _trade()
    orderbook = _orderbook()

    assert trade.trade_price == Decimal("100473000.123456789")
    assert trade.trade_volume == Decimal("0.000123456789")
    assert [unit.ask_price for unit in orderbook.units] == [
        Decimal("137002000"),
        Decimal("137023000"),
    ]


@pytest.mark.parametrize(
    "changes",
    [
        {"market": "KRW-비트"},
        {"market": "krw-btc"},
        {"trade_price": True},
        {"trade_volume": Decimal("NaN")},
        {"best_ask_size": Decimal("Infinity")},
        {"sequential_id": True},
        {"sequential_id": -1},
        {"observed_at": datetime(2026, 8, 24, 12, 0)},
        {"message_timestamp_utc": datetime(2026, 8, 24, 12, 0)},
    ],
)
def test_trade_rejects_invalid_market_numeric_timestamp_and_identifier(changes: dict) -> None:
    with pytest.raises(ValueError):
        replace(_trade(), **changes)


@pytest.mark.parametrize(
    "value",
    [True, Decimal("NaN"), Decimal("Infinity"), Decimal("-0.1")],
)
def test_orderbook_unit_rejects_bool_nonfinite_and_negative_numeric(value: object) -> None:
    with pytest.raises(ValueError):
        replace(_orderbook().units[0], ask_size=value)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "changes",
    [
        {"market": "KRW-이더"},
        {"units": ()},
        {"total_bid_size": Decimal("-1")},
        {"grouping_level": Decimal("NaN")},
        {"stream_type": "SNAPSHOT"},
        {"observed_at": datetime(2026, 8, 24, 12, 0)},
    ],
)
def test_orderbook_rejects_invalid_state(changes: dict) -> None:
    with pytest.raises(ValueError):
        replace(_orderbook(), **changes)
