"""Market-domain value objects."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum


def normalize_quote_currency(value: str) -> str:
    """Normalize and validate an uppercase ASCII quote currency."""

    normalized = value.strip().upper()

    if not normalized or not normalized.isascii() or not normalized.isalnum():
        raise ValueError("quote_currency must be non-empty ASCII alphanumeric text")

    return normalized


@dataclass(frozen=True, slots=True)
class TradingPair:
    """Exchange-independent representation of a trading pair."""

    code: str
    korean_name: str
    english_name: str
    is_warning: bool

    def __post_init__(self) -> None:
        quote, separator, base = self.code.partition("-")

        if (
            not separator
            or "-" in base
            or not quote.isascii()
            or not quote.isalnum()
            or not base.isascii()
            or not base.isalnum()
            or self.code != self.code.upper()
        ):
            raise ValueError(f"Invalid trading-pair code: {self.code}")

        if not isinstance(self.is_warning, bool):
            raise ValueError("is_warning must be a boolean")

    @property
    def quote_currency(self) -> str:
        """Return the quote currency, such as KRW."""

        return self.code.split("-", maxsplit=1)[0]

    @property
    def base_currency(self) -> str:
        """Return the traded asset, such as BTC."""

        return self.code.split("-", maxsplit=1)[1]


class MarketUniverseStatus(StrEnum):
    """Point-in-time availability of one market universe."""

    AVAILABLE = "AVAILABLE"
    UNRESOLVED = "UNRESOLVED"


@dataclass(frozen=True, slots=True)
class MarketUniverseResult:
    """Market pairs known from one complete snapshot at an information cutoff."""

    status: MarketUniverseStatus
    quote_currency: str
    as_of: datetime
    snapshot_id: str | None
    observed_at: datetime | None
    available_at: datetime | None
    content_hash: str | None
    pairs: tuple[TradingPair, ...]

    def __post_init__(self) -> None:
        if self.quote_currency != normalize_quote_currency(self.quote_currency):
            raise ValueError("quote_currency must be normalized")

        _validate_utc(self.as_of, "as_of")

        if self.pairs != tuple(sorted(self.pairs, key=lambda pair: pair.code)):
            raise ValueError("universe pairs must be ordered by market code")

        if len({pair.code for pair in self.pairs}) != len(self.pairs):
            raise ValueError("universe pairs must not contain duplicate markets")

        if any(pair.quote_currency != self.quote_currency for pair in self.pairs):
            raise ValueError("universe pair quote currency must match the result")

        if self.status is MarketUniverseStatus.AVAILABLE:
            if (
                self.snapshot_id is None
                or not self.snapshot_id.strip()
                or self.observed_at is None
                or self.available_at is None
                or self.content_hash is None
                or len(self.content_hash) != 64
                or not self.pairs
            ):
                raise ValueError("AVAILABLE universe requires complete non-empty snapshot metadata")

            _validate_utc(self.observed_at, "observed_at")
            _validate_utc(self.available_at, "available_at")

            if self.observed_at > self.available_at:
                raise ValueError("observed_at must not be after available_at")

            if self.available_at > self.as_of:
                raise ValueError("available_at must not be after as_of")
        elif self.status is MarketUniverseStatus.UNRESOLVED:
            if (
                self.snapshot_id is not None
                or self.observed_at is not None
                or self.available_at is not None
                or self.content_hash is not None
                or self.pairs
            ):
                raise ValueError("UNRESOLVED universe must not contain synthetic snapshot data")
        else:
            raise ValueError(f"unsupported market universe status: {self.status}")

    @property
    def snapshot_age(self) -> timedelta | None:
        """Return age since the selected snapshot observation, without freshness policy."""

        if self.observed_at is None:
            return None

        return self.as_of - self.observed_at


def _validate_utc(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be timezone-aware UTC")
