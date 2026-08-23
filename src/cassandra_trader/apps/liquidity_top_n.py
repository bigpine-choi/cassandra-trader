"""Read-only point-in-time liquidity top-N shortlist evaluation."""

from datetime import datetime, timedelta
from pathlib import Path

from cassandra_trader.apps.market_liquidity import evaluate_market_liquidity_as_of
from cassandra_trader.domain.liquidity_top_n import (
    LiquidityTopNResult,
    evaluate_liquidity_top_n,
    validate_top_n,
)


def evaluate_liquidity_top_n_as_of(
    quote_currency: str,
    window_end: datetime,
    as_of: datetime,
    database: str | Path,
    maximum_snapshot_age: timedelta,
    top_n: int,
) -> LiquidityTopNResult:
    """Evaluate a shortlist over the existing point-in-time liquidity service."""

    validate_top_n(top_n)
    liquidity = evaluate_market_liquidity_as_of(
        quote_currency,
        window_end,
        as_of,
        database,
        maximum_snapshot_age,
    )
    return evaluate_liquidity_top_n(liquidity, top_n)
