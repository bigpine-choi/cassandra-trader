"""Read-only point-in-time trailing market liquidity evaluation."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

from cassandra_trader.adapters.storage.sqlite import SqliteMinuteCandleRepository
from cassandra_trader.apps.market_eligibility import evaluate_market_eligibility_as_of
from cassandra_trader.apps.minute_slots import summarize_minute_slots_from_repository
from cassandra_trader.domain.market import normalize_quote_currency
from cassandra_trader.domain.market_eligibility import (
    MarketEligibilityEvaluationStatus,
    validate_maximum_snapshot_age,
)
from cassandra_trader.domain.market_liquidity import (
    MARKET_LIQUIDITY_LOOKBACK,
    MarketLiquidityResult,
    evaluate_market_liquidity,
)


def evaluate_market_liquidity_as_of(
    quote_currency: str,
    window_end: datetime,
    as_of: datetime,
    database: str | Path,
    maximum_snapshot_age: timedelta,
) -> MarketLiquidityResult:
    """Evaluate 30-day quote volume after fail-closed catalog eligibility."""

    validate_maximum_snapshot_age(maximum_snapshot_age)
    normalized_quote = normalize_quote_currency(quote_currency)

    if normalized_quote != "KRW":
        raise ValueError("market liquidity requires KRW quote currency")

    normalized_window_end = _require_utc(window_end, "window_end")
    cutoff = _require_utc(as_of, "as_of")

    if normalized_window_end.second != 0 or normalized_window_end.microsecond != 0:
        raise ValueError("window_end must align to a one-minute UTC boundary")

    if cutoff < normalized_window_end:
        raise ValueError("as_of must not be before window_end")

    try:
        window_start = normalized_window_end - MARKET_LIQUIDITY_LOOKBACK
    except OverflowError as error:
        raise ValueError("window_end does not support a 30-day lookback") from error

    eligibility = evaluate_market_eligibility_as_of(
        normalized_quote,
        cutoff,
        database,
        maximum_snapshot_age,
    )

    if eligibility.status is MarketEligibilityEvaluationStatus.UNRESOLVED:
        return evaluate_market_liquidity(eligibility, normalized_window_end, ())

    if not eligibility.eligible_pairs:
        return evaluate_market_liquidity(eligibility, normalized_window_end, ())

    with SqliteMinuteCandleRepository(database) as repository:
        summaries = tuple(
            summarize_minute_slots_from_repository(
                pair.code,
                window_start,
                normalized_window_end,
                cutoff,
                repository,
            )
            for pair in eligibility.eligible_pairs
        )

    return evaluate_market_liquidity(eligibility, normalized_window_end, summaries)


def _require_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be timezone-aware UTC")

    return value.astimezone(UTC)
