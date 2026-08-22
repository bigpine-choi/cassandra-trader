from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from cassandra_trader.domain.candle import MinuteCandle
from cassandra_trader.domain.four_hour_bar import (
    FourHourBarReason,
    FourHourBarStatus,
    aggregate_four_hour_bar,
)

_START = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)
_END = _START + timedelta(hours=4)


def _minute(offset: int, *, market: str = "KRW-BTC") -> MinuteCandle:
    start = _START + timedelta(minutes=offset)
    open_price = Decimal(100 + offset)

    return MinuteCandle(
        market=market,
        start_time_utc=start,
        end_time_utc=start + timedelta(minutes=1),
        open=open_price,
        high=open_price + Decimal(2),
        low=open_price - Decimal(1),
        close=open_price + Decimal(1),
        base_volume=Decimal("0.1"),
        quote_volume=Decimal("0.00000000000000000001"),
    )


def _complete_minutes() -> tuple[MinuteCandle, ...]:
    return tuple(_minute(offset) for offset in range(240))


def test_aggregates_240_minutes_with_exact_ohlcv_and_boundaries() -> None:
    result = aggregate_four_hour_bar(
        market="KRW-BTC",
        start_time_utc=_START,
        as_of=_END,
        minute_candles=_complete_minutes(),
    )

    assert result.status is FourHourBarStatus.COMPLETE
    assert result.actual_count == 240
    assert result.missing_minute_starts == ()
    assert result.candle is not None
    assert result.candle.start_time_utc == _START
    assert result.candle.end_time_utc == _END
    assert result.candle.open == Decimal("100")
    assert result.candle.high == Decimal("341")
    assert result.candle.low == Decimal("99")
    assert result.candle.close == Decimal("340")
    assert result.candle.base_volume == Decimal("24.0")
    assert result.candle.quote_volume == Decimal("0.00000000000000000240")


@pytest.mark.parametrize(
    "start_time",
    [
        datetime(2026, 8, 22, 13, 0, tzinfo=UTC),
        datetime(2026, 8, 22, 12, 1, tzinfo=UTC),
        datetime(2026, 8, 22, 12, 0),
        datetime(2026, 8, 22, 12, 0, tzinfo=timezone(timedelta(hours=9))),
    ],
)
def test_rejects_invalid_four_hour_utc_boundary(start_time: datetime) -> None:
    with pytest.raises(ValueError):
        aggregate_four_hour_bar(
            market="KRW-BTC",
            start_time_utc=start_time,
            as_of=_END,
            minute_candles=(),
        )


def test_one_missing_minute_returns_structured_incomplete() -> None:
    minutes = tuple(candle for index, candle in enumerate(_complete_minutes()) if index != 42)

    result = aggregate_four_hour_bar(
        market="KRW-BTC",
        start_time_utc=_START,
        as_of=_END,
        minute_candles=minutes,
    )

    assert result.status is FourHourBarStatus.INCOMPLETE
    assert result.reason is FourHourBarReason.MISSING_MINUTES
    assert result.expected_count == 240
    assert result.actual_count == 239
    assert result.missing_minute_starts == (_START + timedelta(minutes=42),)
    assert result.candle is None


def test_rejects_duplicate_minute_start() -> None:
    minutes = (*_complete_minutes(), _minute(0))

    with pytest.raises(ValueError, match="duplicate"):
        aggregate_four_hour_bar(
            market="KRW-BTC",
            start_time_utc=_START,
            as_of=_END,
            minute_candles=minutes,
        )


def test_rejects_market_mismatch() -> None:
    minutes = list(_complete_minutes())
    minutes[10] = _minute(10, market="KRW-ETH")

    with pytest.raises(ValueError, match="market"):
        aggregate_four_hour_bar(
            market="KRW-BTC",
            start_time_utc=_START,
            as_of=_END,
            minute_candles=minutes,
        )


def test_rejects_minute_outside_interval() -> None:
    minutes = (
        *_complete_minutes(),
        replace(_minute(0), start_time_utc=_END, end_time_utc=_END + timedelta(minutes=1)),
    )

    with pytest.raises(ValueError, match="outside"):
        aggregate_four_hour_bar(
            market="KRW-BTC",
            start_time_utc=_START,
            as_of=_END,
            minute_candles=minutes,
        )


def test_in_progress_interval_never_produces_candle() -> None:
    result = aggregate_four_hour_bar(
        market="KRW-BTC",
        start_time_utc=_START,
        as_of=_END - timedelta(microseconds=1),
        minute_candles=_complete_minutes(),
    )

    assert result.status is FourHourBarStatus.INCOMPLETE
    assert result.reason is FourHourBarReason.INTERVAL_NOT_COMPLETE
    assert result.actual_count == 240
    assert result.missing_minute_starts == ()
    assert result.candle is None
