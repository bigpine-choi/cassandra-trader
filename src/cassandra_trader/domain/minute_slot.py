"""Point-in-time semantic status for canonical one-minute slots."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum

from cassandra_trader.domain.candle import MinuteCandle


class MinuteSlotStatus(StrEnum):
    """Evidence-backed semantic state of one UTC minute slot."""

    TRADED = "TRADED"
    NO_TRADE = "NO_TRADE"
    UNRESOLVED = "UNRESOLVED"


@dataclass(frozen=True, slots=True)
class ResolvedMinuteSlot:
    """One resolved minute with canonical data only when it was traded."""

    market: str
    start_time_utc: datetime
    end_time_utc: datetime
    status: MinuteSlotStatus
    candle: MinuteCandle | None
    canonical_revision: int | None
    canonical_available_at: datetime | None

    def __post_init__(self) -> None:
        for name, value in (
            ("start_time_utc", self.start_time_utc),
            ("end_time_utc", self.end_time_utc),
        ):
            if value.tzinfo is None or value.utcoffset() != timedelta(0):
                raise ValueError(f"{name} must be timezone-aware UTC")

        if self.start_time_utc.second != 0 or self.start_time_utc.microsecond != 0:
            raise ValueError("start_time_utc must align to a one-minute UTC boundary")

        if self.end_time_utc != self.start_time_utc + timedelta(minutes=1):
            raise ValueError("minute slot must span exactly one minute")

        if self.status is MinuteSlotStatus.TRADED:
            if (
                self.candle is None
                or self.canonical_revision is None
                or self.canonical_available_at is None
            ):
                raise ValueError("TRADED slot requires canonical candle revision information")

            if self.canonical_revision < 1:
                raise ValueError("TRADED canonical_revision must be at least 1")

            if (
                self.canonical_available_at.tzinfo is None
                or self.canonical_available_at.utcoffset() != timedelta(0)
            ):
                raise ValueError("canonical_available_at must be timezone-aware UTC")

            if (
                self.candle.market != self.market
                or self.candle.start_time_utc != self.start_time_utc
                or self.candle.end_time_utc != self.end_time_utc
            ):
                raise ValueError("canonical candle must match its minute slot")
        elif (
            self.candle is not None
            or self.canonical_revision is not None
            or self.canonical_available_at is not None
        ):
            raise ValueError("non-TRADED slot must not contain a synthetic candle")
