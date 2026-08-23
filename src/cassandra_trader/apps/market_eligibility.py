"""Read-only point-in-time market eligibility application service."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

from cassandra_trader.apps.market_universe import resolve_market_universe_as_of
from cassandra_trader.domain.market import normalize_quote_currency
from cassandra_trader.domain.market_eligibility import (
    MarketEligibilityResult,
    evaluate_market_eligibility,
    validate_maximum_snapshot_age,
)


def evaluate_market_eligibility_as_of(
    quote_currency: str,
    as_of: datetime,
    database: str | Path,
    maximum_snapshot_age: timedelta,
) -> MarketEligibilityResult:
    """Resolve stored universe evidence and apply catalog eligibility without network I/O."""

    validate_maximum_snapshot_age(maximum_snapshot_age)
    normalized_quote = normalize_quote_currency(quote_currency)

    if as_of.tzinfo is None or as_of.utcoffset() != timedelta(0):
        raise ValueError("as_of must be timezone-aware UTC")

    cutoff = as_of.astimezone(UTC)
    universe = resolve_market_universe_as_of(normalized_quote, cutoff, database)
    return evaluate_market_eligibility(universe, maximum_snapshot_age)
