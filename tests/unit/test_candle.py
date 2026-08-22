from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from cassandra_trader.domain.candle import MinuteCandle


def _valid_candle() -> MinuteCandle:
    start = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)

    return MinuteCandle(
        market="KRW-BTC",
        start_time_utc=start,
        end_time_utc=start + timedelta(minutes=1),
        open=Decimal("100"),
        high=Decimal("110"),
        low=Decimal("90"),
        close=Decimal("105"),
        base_volume=Decimal("1.5"),
        quote_volume=Decimal("150"),
    )


def test_minute_candle_accepts_canonical_values() -> None:
    candle = _valid_candle()

    assert candle.market == "KRW-BTC"
    assert candle.end_time_utc - candle.start_time_utc == timedelta(minutes=1)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"market": "BTC"}, "market"),
        (
            {
                "start_time_utc": datetime(2025, 1, 1, 12, 0),
                "end_time_utc": datetime(2025, 1, 1, 12, 1),
            },
            "timezone-aware UTC",
        ),
        (
            {
                "start_time_utc": datetime(
                    2025,
                    1,
                    1,
                    12,
                    0,
                    tzinfo=timezone(timedelta(hours=9)),
                ),
                "end_time_utc": datetime(
                    2025,
                    1,
                    1,
                    12,
                    1,
                    tzinfo=timezone(timedelta(hours=9)),
                ),
            },
            "timezone-aware UTC",
        ),
        (
            {
                "start_time_utc": datetime(2025, 1, 1, 12, 0, 1, tzinfo=UTC),
                "end_time_utc": datetime(2025, 1, 1, 12, 1, 1, tzinfo=UTC),
            },
            "one-minute boundary",
        ),
        (
            {"end_time_utc": datetime(2025, 1, 1, 12, 2, tzinfo=UTC)},
            "plus one minute",
        ),
        ({"open": Decimal("-1")}, "prices"),
        ({"base_volume": Decimal("-1")}, "volumes"),
        ({"high": Decimal("99")}, "OHLC"),
        ({"low": Decimal("106")}, "OHLC"),
    ],
)
def test_minute_candle_rejects_invalid_values(
    changes: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        replace(_valid_candle(), **changes)
