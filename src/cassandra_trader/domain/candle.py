"""Canonical candle-domain value objects."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class MinuteCandle:
    """Exchange-independent representation of one completed minute candle."""

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
        quote, separator, base = self.market.partition("-")

        if (
            not separator
            or not quote.isalnum()
            or not base.isalnum()
            or self.market != self.market.upper()
        ):
            raise ValueError(f"Invalid market code: {self.market}")

        for name, value in (
            ("start_time_utc", self.start_time_utc),
            ("end_time_utc", self.end_time_utc),
        ):
            if value.tzinfo is None or value.utcoffset() != timedelta(0):
                raise ValueError(f"{name} must be timezone-aware UTC")

        if self.start_time_utc.second != 0 or self.start_time_utc.microsecond != 0:
            raise ValueError("start_time_utc must align to a one-minute boundary")

        if self.end_time_utc != self.start_time_utc + timedelta(minutes=1):
            raise ValueError("end_time_utc must equal start_time_utc plus one minute")

        prices = (self.open, self.high, self.low, self.close)
        volumes = (self.base_volume, self.quote_volume)

        if any(not value.is_finite() or value < 0 for value in prices):
            raise ValueError("prices must be finite and non-negative")

        if any(not value.is_finite() or value < 0 for value in volumes):
            raise ValueError("volumes must be finite and non-negative")

        if self.low > min(self.open, self.close) or self.high < max(self.open, self.close):
            raise ValueError("OHLC values must satisfy low <= open/close <= high")
