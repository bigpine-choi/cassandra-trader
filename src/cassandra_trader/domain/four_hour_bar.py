"""Deterministic four-hour candle aggregation."""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from cassandra_trader.domain.candle import MinuteCandle

_EXPECTED_MINUTE_COUNT = 240


class FourHourBarStatus(StrEnum):
    """Outcome status for a four-hour aggregation attempt."""

    COMPLETE = "COMPLETE"
    INCOMPLETE = "INCOMPLETE"


class FourHourBarReason(StrEnum):
    """Reason an aggregation did not produce a tradable candle."""

    MISSING_MINUTES = "MISSING_MINUTES"
    INTERVAL_NOT_COMPLETE = "INTERVAL_NOT_COMPLETE"


@dataclass(frozen=True, slots=True)
class FourHourCandle:
    """Canonical completed four-hour candle aligned to UTC boundaries."""

    market: str
    start_time_utc: datetime
    end_time_utc: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    base_volume: Decimal
    quote_volume: Decimal

    def __post_init__(self) -> None:
        _validate_market(self.market)
        _validate_utc(self.start_time_utc, "start_time_utc")
        _validate_utc(self.end_time_utc, "end_time_utc")

        if (
            self.start_time_utc.hour % 4 != 0
            or self.start_time_utc.minute != 0
            or self.start_time_utc.second != 0
            or self.start_time_utc.microsecond != 0
        ):
            raise ValueError("start_time_utc must align to a four-hour UTC boundary")

        if self.end_time_utc != self.start_time_utc + timedelta(hours=4):
            raise ValueError("end_time_utc must equal start_time_utc plus four hours")

        prices = (self.open, self.high, self.low, self.close)
        volumes = (self.base_volume, self.quote_volume)

        if any(not value.is_finite() or value < 0 for value in prices):
            raise ValueError("prices must be finite and non-negative")

        if any(not value.is_finite() or value < 0 for value in volumes):
            raise ValueError("volumes must be finite and non-negative")

        if self.low > min(self.open, self.close) or self.high < max(self.open, self.close):
            raise ValueError("OHLC values must satisfy low <= open/close <= high")


@dataclass(frozen=True, slots=True)
class FourHourBarResult:
    """Structured COMPLETE or INCOMPLETE aggregation result."""

    status: FourHourBarStatus
    market: str
    start_time_utc: datetime
    end_time_utc: datetime
    expected_count: int
    actual_count: int
    missing_minute_starts: tuple[datetime, ...]
    candle: FourHourCandle | None
    reason: FourHourBarReason | None


def aggregate_four_hour_bar(
    *,
    market: str,
    start_time_utc: datetime,
    as_of: datetime,
    minute_candles: Sequence[MinuteCandle],
) -> FourHourBarResult:
    """Aggregate exactly 240 unique contiguous minute candles."""

    _validate_market(market)
    _validate_utc(start_time_utc, "start_time_utc")
    _validate_utc(as_of, "as_of")

    if (
        start_time_utc.hour % 4 != 0
        or start_time_utc.minute != 0
        or start_time_utc.second != 0
        or start_time_utc.microsecond != 0
    ):
        raise ValueError("start_time_utc must align to a four-hour UTC boundary")

    end_time_utc = start_time_utc + timedelta(hours=4)
    by_start: dict[datetime, MinuteCandle] = {}

    for candle in minute_candles:
        if candle.market != market:
            raise ValueError("minute candle market does not match aggregation market")

        if not start_time_utc <= candle.start_time_utc < end_time_utc:
            raise ValueError("minute candle falls outside the four-hour interval")

        if candle.start_time_utc in by_start:
            raise ValueError("duplicate minute candle start_time_utc")

        by_start[candle.start_time_utc] = candle

    expected_starts = tuple(
        start_time_utc + timedelta(minutes=offset) for offset in range(_EXPECTED_MINUTE_COUNT)
    )
    missing_starts = tuple(start for start in expected_starts if start not in by_start)
    actual_count = len(by_start)

    if end_time_utc > as_of:
        return FourHourBarResult(
            status=FourHourBarStatus.INCOMPLETE,
            market=market,
            start_time_utc=start_time_utc,
            end_time_utc=end_time_utc,
            expected_count=_EXPECTED_MINUTE_COUNT,
            actual_count=actual_count,
            missing_minute_starts=missing_starts,
            candle=None,
            reason=FourHourBarReason.INTERVAL_NOT_COMPLETE,
        )

    if missing_starts:
        return FourHourBarResult(
            status=FourHourBarStatus.INCOMPLETE,
            market=market,
            start_time_utc=start_time_utc,
            end_time_utc=end_time_utc,
            expected_count=_EXPECTED_MINUTE_COUNT,
            actual_count=actual_count,
            missing_minute_starts=missing_starts,
            candle=None,
            reason=FourHourBarReason.MISSING_MINUTES,
        )

    ordered = tuple(by_start[start] for start in expected_starts)
    candle = FourHourCandle(
        market=market,
        start_time_utc=start_time_utc,
        end_time_utc=end_time_utc,
        open=ordered[0].open,
        high=max(minute_candle.high for minute_candle in ordered),
        low=min(minute_candle.low for minute_candle in ordered),
        close=ordered[-1].close,
        base_volume=sum(
            (minute_candle.base_volume for minute_candle in ordered),
            start=Decimal(0),
        ),
        quote_volume=sum(
            (minute_candle.quote_volume for minute_candle in ordered),
            start=Decimal(0),
        ),
    )

    return FourHourBarResult(
        status=FourHourBarStatus.COMPLETE,
        market=market,
        start_time_utc=start_time_utc,
        end_time_utc=end_time_utc,
        expected_count=_EXPECTED_MINUTE_COUNT,
        actual_count=actual_count,
        missing_minute_starts=(),
        candle=candle,
        reason=None,
    )


def _validate_market(market: str) -> None:
    quote, separator, base = market.partition("-")

    if not separator or not quote.isalnum() or not base.isalnum() or market != market.upper():
        raise ValueError(f"Invalid market code: {market}")


def _validate_utc(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be timezone-aware UTC")
