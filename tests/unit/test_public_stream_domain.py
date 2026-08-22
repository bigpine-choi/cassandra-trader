from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from cassandra_trader.domain.public_stream import MinuteCandleUpdate, StreamType, TickerEvent


def test_ticker_event_requires_utc_and_non_negative_values() -> None:
    with pytest.raises(ValueError, match="trade_timestamp_utc"):
        TickerEvent(
            market="KRW-BTC",
            trade_price=Decimal("1"),
            signed_change_rate=Decimal("-0.01"),
            acc_trade_price_24h=Decimal("10"),
            trade_timestamp_utc=datetime(2026, 8, 22, 12, 0),
            received_at=datetime(2026, 8, 22, 12, 0, tzinfo=UTC),
            stream_type=StreamType.REALTIME,
        )

    with pytest.raises(ValueError, match="trade_price"):
        TickerEvent(
            market="KRW-BTC",
            trade_price=Decimal("-1"),
            signed_change_rate=Decimal("-0.01"),
            acc_trade_price_24h=Decimal("10"),
            trade_timestamp_utc=datetime(2026, 8, 22, 12, 0, tzinfo=UTC),
            received_at=datetime(2026, 8, 22, 12, 0, tzinfo=UTC),
            stream_type=StreamType.REALTIME,
        )


def test_minute_candle_update_validates_utc_boundary_and_ohlcv() -> None:
    start = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)
    values = {
        "market": "KRW-BTC",
        "start_time_utc": start,
        "end_time_utc": start + timedelta(minutes=1),
        "open": Decimal("10"),
        "high": Decimal("11"),
        "low": Decimal("9"),
        "close": Decimal("10.5"),
        "base_volume": Decimal("1"),
        "quote_volume": Decimal("10"),
        "event_time_utc": start + timedelta(seconds=30),
        "received_at": start + timedelta(seconds=31),
        "stream_type": StreamType.REALTIME,
    }

    event = MinuteCandleUpdate(**values)

    assert event.start_time_utc == start

    with pytest.raises(ValueError, match="OHLC"):
        MinuteCandleUpdate(**(values | {"high": Decimal("9.5")}))

    with pytest.raises(ValueError, match="base_volume"):
        MinuteCandleUpdate(**(values | {"base_volume": Decimal("-0.1")}))

    with pytest.raises(ValueError, match="one-minute boundary"):
        shifted = start + timedelta(seconds=1)
        MinuteCandleUpdate(
            **(
                values
                | {
                    "start_time_utc": shifted,
                    "end_time_utc": shifted + timedelta(minutes=1),
                }
            )
        )
