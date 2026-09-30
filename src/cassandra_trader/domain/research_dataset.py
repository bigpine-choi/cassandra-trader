"""An explicit, immutable request for exploratory four-hour research data."""

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from cassandra_trader.domain.market_eligibility import validate_maximum_snapshot_age

RESEARCH_DATASET_SCHEMA_VERSION = "1"
RESEARCH_DATASET_ALGORITHM_VERSION = "research-dataset-v1"


@dataclass(frozen=True, slots=True)
class ResearchDatasetRequest:
    """Select explicit KRW markets and one common information cutoff.

    The selection is a research input, not a reconstructed historical universe.
    Both bounds are four-hour UTC boundaries; the end is exclusive.
    """

    markets: tuple[str, ...]
    start_time_utc: datetime
    end_time_utc: datetime
    as_of: datetime
    maximum_snapshot_age: timedelta

    def __post_init__(self) -> None:
        if not isinstance(self.markets, tuple) or not self.markets:
            raise ValueError("markets must be a non-empty tuple of explicit KRW markets")
        normalized: set[str] = set()
        for market in self.markets:
            if not isinstance(market, str) or not market.isascii():
                raise ValueError("markets must be ASCII KRW market codes")
            code = market.strip().upper()
            if re.fullmatch(r"KRW-[A-Z0-9]+", code) is None:
                raise ValueError(f"Invalid KRW market code: {market}")
            normalized.add(code)
        object.__setattr__(self, "markets", tuple(sorted(normalized)))

        for name in ("start_time_utc", "end_time_utc", "as_of"):
            value = getattr(self, name)
            if (
                not isinstance(value, datetime)
                or value.tzinfo is None
                or value.utcoffset() != timedelta(0)
            ):
                raise ValueError(f"{name} must be timezone-aware UTC")
            object.__setattr__(self, name, value.astimezone(UTC))

        for name in ("start_time_utc", "end_time_utc"):
            value = getattr(self, name)
            if value.hour % 4 or value.minute or value.second or value.microsecond:
                raise ValueError(f"{name} must align to a four-hour UTC boundary")
        if self.start_time_utc >= self.end_time_utc:
            raise ValueError("start_time_utc must be before end_time_utc")
        if self.as_of < self.end_time_utc:
            raise ValueError("as_of must be at or after end_time_utc")
        validate_maximum_snapshot_age(self.maximum_snapshot_age)
