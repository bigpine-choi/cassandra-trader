"""Command-line interface for Cassandra Trader."""

import argparse
import asyncio
import json
import sys
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

from cassandra_trader.adapters.upbit.public_stream import StreamLifecycleEvent
from cassandra_trader.apps.four_hour_bars import derive_four_hour_bar
from cassandra_trader.apps.liquidity_dataset_bootstrap import (
    LiquidityDatasetBootstrapStatus,
    bootstrap_liquidity_dataset,
    bootstrap_market_payload,
    bootstrap_summary_payload,
)
from cassandra_trader.apps.liquidity_dataset_refresh import (
    LiquidityDatasetRefreshStatus,
    refresh_liquidity_dataset,
    refresh_market_payload,
    refresh_summary_payload,
)
from cassandra_trader.apps.liquidity_top_n import evaluate_liquidity_top_n_as_of
from cassandra_trader.apps.market_catalog import fetch_krw_trading_pairs
from cassandra_trader.apps.market_data_runtime import (
    RuntimeRecoveryEvent,
    run_market_data,
)
from cassandra_trader.apps.market_eligibility import evaluate_market_eligibility_as_of
from cassandra_trader.apps.market_liquidity import evaluate_market_liquidity_as_of
from cassandra_trader.apps.market_universe import (
    capture_krw_market_catalog,
    resolve_market_universe_as_of,
)
from cassandra_trader.apps.minute_candle_backfill import backfill_minute_candles
from cassandra_trader.apps.minute_candles import (
    fetch_recent_minute_candles,
    sync_recent_minute_candles,
)
from cassandra_trader.apps.minute_reconciliation import MinuteReconciliationResult
from cassandra_trader.apps.multi_market_dataset import (
    backfill_multi_market_dataset,
    market_result_payload,
    summary_payload,
)
from cassandra_trader.apps.public_stream import stream_public_events
from cassandra_trader.domain.four_hour_bar import FourHourBarStatus
from cassandra_trader.domain.liquidity_top_n import LiquidityTopNEvaluationStatus
from cassandra_trader.domain.market import MarketUniverseStatus, normalize_quote_currency
from cassandra_trader.domain.market_eligibility import MarketEligibilityEvaluationStatus
from cassandra_trader.domain.market_liquidity import MarketLiquidityEvaluationStatus
from cassandra_trader.domain.public_stream import MinuteCandleUpdate, PublicMarketEvent, TickerEvent


def _positive_integer(value: str) -> int:
    number = int(value)

    if number < 1:
        raise argparse.ArgumentTypeError("value must be at least 1")

    return number


def _positive_timedelta_seconds(value: str) -> int:
    number = _positive_integer(value)

    try:
        timedelta(seconds=number)
    except OverflowError as error:
        raise argparse.ArgumentTypeError("value exceeds the supported duration range") from error

    return number


def _non_negative_integer(value: str) -> int:
    number = int(value)

    if number < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")

    return number


def _candle_count(value: str) -> int:
    number = _positive_integer(value)

    if number > 200:
        raise argparse.ArgumentTypeError("value must not exceed 200")

    return number


def _market_code(value: str) -> str:
    normalized = value.strip().upper()
    quote, separator, base = normalized.partition("-")

    if (
        not separator
        or not quote.isascii()
        or not quote.isalnum()
        or not base.isascii()
        or not base.isalnum()
    ):
        raise argparse.ArgumentTypeError("market must look like KRW-BTC")

    return normalized


def _krw_market_code(value: str) -> str:
    normalized = _market_code(value)

    if not normalized.startswith("KRW-"):
        raise argparse.ArgumentTypeError("market must be an Upbit KRW market")

    return normalized


def _quote_currency(value: str) -> str:
    try:
        return normalize_quote_currency(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error


def _krw_quote_currency(value: str) -> str:
    normalized = _quote_currency(value)

    if normalized != "KRW":
        raise argparse.ArgumentTypeError("quote currency must be KRW")

    return normalized


def _utc_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise argparse.ArgumentTypeError("timestamp must be valid ISO 8601") from error

    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("timestamp must include a UTC timezone")

    if parsed.utcoffset() != timedelta(0):
        raise argparse.ArgumentTypeError("timestamp must use UTC (Z or +00:00)")

    return parsed.astimezone(UTC)


def _four_hour_start(value: str) -> datetime:
    parsed = _utc_datetime(value)

    if parsed.hour % 4 != 0 or parsed.minute != 0 or parsed.second != 0 or parsed.microsecond != 0:
        raise argparse.ArgumentTypeError("start must align to a four-hour UTC boundary")

    return parsed


def _minute_boundary(value: str) -> datetime:
    parsed = _utc_datetime(value)

    if parsed.second != 0 or parsed.microsecond != 0:
        raise argparse.ArgumentTypeError("timestamp must align to a one-minute UTC boundary")

    return parsed


def _liquidity_window_end(value: str) -> datetime:
    parsed = _minute_boundary(value)

    try:
        parsed - timedelta(days=30)
    except OverflowError as error:
        raise argparse.ArgumentTypeError("window-end must support a 30-day lookback") from error

    return parsed


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cassandra-trader")
    commands = parser.add_subparsers(dest="command")

    markets = commands.add_parser(
        "markets",
        help="list current Upbit KRW trading pairs",
    )
    markets.add_argument(
        "--limit",
        type=_positive_integer,
        default=10,
        help="maximum number of pairs to display",
    )

    capture_universe = commands.add_parser(
        "capture-universe",
        help="capture the current Upbit KRW market catalog in SQLite",
    )
    capture_universe.add_argument(
        "--database",
        type=Path,
        required=True,
        help="SQLite database path",
    )

    universe_as_of = commands.add_parser(
        "universe-as-of",
        help="resolve a stored point-in-time market universe without network access",
    )
    universe_as_of.add_argument(
        "--quote-currency",
        type=_quote_currency,
        default="KRW",
        help="ASCII quote currency (default: KRW)",
    )
    universe_as_of.add_argument(
        "--as-of",
        type=_utc_datetime,
        required=True,
        help="UTC information cutoff in ISO 8601 format",
    )
    universe_as_of.add_argument(
        "--database",
        type=Path,
        required=True,
        help="SQLite database path",
    )

    eligible_markets = commands.add_parser(
        "eligible-markets",
        help="evaluate stored point-in-time catalog eligibility without network access",
    )
    eligible_markets.add_argument(
        "--quote-currency",
        type=_quote_currency,
        default="KRW",
        help="ASCII quote currency (default: KRW)",
    )
    eligible_markets.add_argument(
        "--as-of",
        type=_utc_datetime,
        required=True,
        help="UTC information cutoff in ISO 8601 format",
    )
    eligible_markets.add_argument(
        "--maximum-snapshot-age-seconds",
        type=_positive_timedelta_seconds,
        required=True,
        help="positive maximum catalog snapshot age in seconds",
    )
    eligible_markets.add_argument(
        "--database",
        type=Path,
        required=True,
        help="SQLite database path",
    )

    market_liquidity = commands.add_parser(
        "market-liquidity",
        help="rank complete trailing 30-day KRW quote volume without network access",
    )
    market_liquidity.add_argument(
        "--quote-currency",
        type=_krw_quote_currency,
        default="KRW",
        help="quote currency (must be KRW)",
    )
    market_liquidity.add_argument(
        "--window-end",
        type=_liquidity_window_end,
        required=True,
        help="exclusive UTC minute boundary for the 30-day window",
    )
    market_liquidity.add_argument(
        "--as-of",
        type=_utc_datetime,
        required=True,
        help="UTC information cutoff in ISO 8601 format",
    )
    market_liquidity.add_argument(
        "--maximum-snapshot-age-seconds",
        type=_positive_timedelta_seconds,
        required=True,
        help="positive maximum catalog snapshot age in seconds",
    )
    market_liquidity.add_argument(
        "--database",
        type=Path,
        required=True,
        help="SQLite database path",
    )

    liquidity_top_n = commands.add_parser(
        "liquidity-top-n",
        help="select a deterministic top-N from complete point-in-time liquidity ranks",
    )
    liquidity_top_n.add_argument(
        "--quote-currency",
        type=_krw_quote_currency,
        required=True,
        help="quote currency (must be KRW)",
    )
    liquidity_top_n.add_argument(
        "--window-end",
        type=_liquidity_window_end,
        required=True,
        help="exclusive UTC minute boundary for the 30-day window",
    )
    liquidity_top_n.add_argument(
        "--as-of",
        type=_utc_datetime,
        required=True,
        help="UTC information cutoff in ISO 8601 format",
    )
    liquidity_top_n.add_argument(
        "--maximum-snapshot-age-seconds",
        type=_positive_timedelta_seconds,
        required=True,
        help="positive maximum catalog snapshot age in seconds",
    )
    liquidity_top_n.add_argument(
        "--top-n",
        type=_positive_integer,
        required=True,
        help="positive number of liquidity-ranked markets to shortlist",
    )
    liquidity_top_n.add_argument(
        "--database",
        type=Path,
        required=True,
        help="SQLite database path",
    )

    bootstrap_dataset = commands.add_parser(
        "bootstrap-liquidity-dataset",
        help="bootstrap the full fresh non-warning KRW analysis universe with resume support",
    )
    bootstrap_dataset.add_argument(
        "--quote-currency",
        type=_krw_quote_currency,
        required=True,
        help="quote currency (must be KRW)",
    )
    bootstrap_dataset.add_argument(
        "--universe-as-of",
        type=_utc_datetime,
        required=True,
        help="UTC point-in-time catalog cutoff",
    )
    bootstrap_dataset.add_argument(
        "--window-end",
        type=_liquidity_window_end,
        required=True,
        help="exclusive UTC minute boundary for the 30-day window",
    )
    bootstrap_dataset.add_argument(
        "--maximum-snapshot-age-seconds",
        type=_positive_timedelta_seconds,
        required=True,
        help="positive maximum catalog snapshot age in seconds",
    )
    bootstrap_dataset.add_argument(
        "--database",
        type=Path,
        required=True,
        help="SQLite database path",
    )

    refresh_dataset = commands.add_parser(
        "refresh-liquidity-dataset",
        help="capture the KRW catalog and incrementally refresh the rolling dataset",
    )
    refresh_dataset.add_argument(
        "--quote-currency",
        type=_krw_quote_currency,
        required=True,
        help="quote currency (must be KRW)",
    )
    refresh_dataset.add_argument(
        "--maximum-snapshot-age-seconds",
        type=_positive_timedelta_seconds,
        required=True,
        help="positive maximum catalog snapshot age in seconds",
    )
    refresh_dataset.add_argument(
        "--database",
        type=Path,
        required=True,
        help="SQLite database path",
    )

    candles = commands.add_parser(
        "candles",
        help="show recent completed Upbit one-minute candles",
    )
    candles.add_argument(
        "market",
        type=_market_code,
        help="Upbit market code, such as KRW-BTC",
    )
    candles.add_argument(
        "--count",
        type=_candle_count,
        default=5,
        help="number of completed candles to display (1-200)",
    )

    sync_candles = commands.add_parser(
        "sync-candles",
        help="store one raw and canonical Upbit minute-candle batch",
    )
    sync_candles.add_argument(
        "market",
        type=_market_code,
        help="Upbit market code, such as KRW-BTC",
    )
    sync_candles.add_argument(
        "--count",
        type=_candle_count,
        default=5,
        help="number of completed candles to request (1-200)",
    )
    sync_candles.add_argument(
        "--database",
        type=Path,
        required=True,
        help="SQLite database path",
    )

    backfill_candles = commands.add_parser(
        "backfill-candles",
        help="backfill a UTC range of Upbit one-minute candles",
    )
    backfill_candles.add_argument(
        "market",
        type=_market_code,
        help="Upbit market code, such as KRW-BTC",
    )
    backfill_candles.add_argument(
        "--start",
        type=_minute_boundary,
        required=True,
        help="inclusive UTC minute boundary in ISO 8601 format",
    )
    backfill_candles.add_argument(
        "--end",
        type=_minute_boundary,
        required=True,
        help="exclusive UTC minute boundary in ISO 8601 format",
    )
    backfill_candles.add_argument(
        "--database",
        type=Path,
        required=True,
        help="SQLite database path",
    )

    backfill_dataset = commands.add_parser(
        "backfill-dataset",
        help="sequentially backfill one UTC range for Upbit KRW markets",
    )
    backfill_dataset.add_argument(
        "markets",
        type=_krw_market_code,
        nargs="+",
        help="one or more explicit Upbit KRW market codes",
    )
    backfill_dataset.add_argument(
        "--start",
        type=_minute_boundary,
        required=True,
        help="inclusive UTC minute boundary in ISO 8601 format",
    )
    backfill_dataset.add_argument(
        "--end",
        type=_minute_boundary,
        required=True,
        help="exclusive UTC minute boundary in ISO 8601 format",
    )
    backfill_dataset.add_argument(
        "--database",
        type=Path,
        required=True,
        help="SQLite database path",
    )

    four_hour_bar = commands.add_parser(
        "four-hour-bar",
        help="derive one completed UTC four-hour bar from stored minute candles",
    )
    four_hour_bar.add_argument(
        "market",
        type=_market_code,
        help="market code, such as KRW-BTC",
    )
    four_hour_bar.add_argument(
        "--start",
        type=_four_hour_start,
        required=True,
        help="four-hour UTC boundary in ISO 8601 format",
    )
    four_hour_bar.add_argument(
        "--as-of",
        type=_utc_datetime,
        required=True,
        help="UTC information cutoff in ISO 8601 format",
    )
    four_hour_bar.add_argument(
        "--database",
        type=Path,
        required=True,
        help="SQLite database path",
    )

    stream_public = commands.add_parser(
        "stream-public",
        help="stream public Upbit ticker and one-minute candle updates",
    )
    stream_public.add_argument(
        "markets",
        type=_market_code,
        nargs="+",
        help="one or more Upbit market codes",
    )
    stream_public.add_argument(
        "--max-events",
        type=_positive_integer,
        help="stop normally after this many deduplicated data events",
    )

    run_market = commands.add_parser(
        "run-market-data",
        help="run one market's stream and completed-minute REST reconciliation",
    )
    run_market.add_argument(
        "market",
        type=_market_code,
        help="one Upbit market code, such as KRW-BTC",
    )
    run_market.add_argument(
        "--database",
        type=Path,
        required=True,
        help="SQLite database path",
    )
    run_market.add_argument(
        "--startup-lookback-minutes",
        type=_positive_integer,
        default=10,
        help="rolling REST recovery window at startup",
    )
    run_market.add_argument(
        "--finalization-delay-seconds",
        type=_non_negative_integer,
        default=3,
        help="delay after each minute end before REST reconciliation",
    )
    run_market.add_argument(
        "--max-reconciliations",
        type=_positive_integer,
        help="stop normally after this many completed-minute reconciliations",
    )

    return parser


async def _print_markets(limit: int) -> None:
    pairs = await fetch_krw_trading_pairs()

    print(f"KRW trading pairs: {len(pairs)}")

    for pair in pairs[:limit]:
        warning = " [WARNING]" if pair.is_warning else ""
        print(f"{pair.code:<12} {pair.korean_name}{warning}")


async def _capture_universe(database: Path) -> int:
    try:
        result = await capture_krw_market_catalog(database)
    except Exception as error:
        payload = {
            "type": "snapshot",
            "status": "FAILED",
            "error": {
                "type": type(error).__name__,
                "message": str(error),
            },
        }
        print(json.dumps(payload, separators=(",", ":")), file=sys.stderr)
        return 1

    payload = {
        "type": "snapshot",
        "status": "SUCCEEDED",
        "snapshot_id": result.snapshot_id,
        "quote_currency": result.quote_currency,
        "observed_at": _optional_utc_text(result.observed_at),
        "available_at": _optional_utc_text(result.available_at),
        "market_count": result.market_count,
        "content_hash": result.content_hash,
    }
    print(json.dumps(payload, separators=(",", ":")))
    return 0


def _print_universe_as_of(
    quote_currency: str,
    as_of: datetime,
    database: Path,
) -> int:
    result = resolve_market_universe_as_of(quote_currency, as_of, database)

    for pair in result.pairs:
        payload = {
            "type": "market",
            "market": pair.code,
            "korean_name": pair.korean_name,
            "english_name": pair.english_name,
            "is_warning": pair.is_warning,
        }
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))

    summary = {
        "type": "summary",
        "status": result.status.value,
        "quote_currency": result.quote_currency,
        "as_of": _optional_utc_text(result.as_of),
        "snapshot_id": result.snapshot_id,
        "observed_at": (
            None if result.observed_at is None else _optional_utc_text(result.observed_at)
        ),
        "available_at": (
            None if result.available_at is None else _optional_utc_text(result.available_at)
        ),
        "content_hash": result.content_hash,
        "market_count": len(result.pairs),
    }
    print(json.dumps(summary, ensure_ascii=False, separators=(",", ":")))
    return 2 if result.status is MarketUniverseStatus.UNRESOLVED else 0


def _print_eligible_markets(
    quote_currency: str,
    as_of: datetime,
    maximum_snapshot_age_seconds: int,
    database: Path,
) -> int:
    result = evaluate_market_eligibility_as_of(
        quote_currency,
        as_of,
        database,
        timedelta(seconds=maximum_snapshot_age_seconds),
    )

    for decision in result.decisions:
        payload = {
            "type": "market",
            "market": decision.market,
            "status": decision.status.value,
            "reasons": [reason.value for reason in decision.reasons],
            "is_warning": decision.is_warning,
        }
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))

    summary = {
        "type": "summary",
        "status": result.status.value,
        "reasons": [reason.value for reason in result.reasons],
        "policy_name": result.policy_name,
        "policy_version": result.policy_version,
        "quote_currency": result.quote_currency,
        "as_of": _optional_utc_text(result.as_of),
        "maximum_snapshot_age_seconds": _duration_seconds(result.maximum_snapshot_age),
        "snapshot_id": result.snapshot_id,
        "observed_at": (
            None if result.observed_at is None else _optional_utc_text(result.observed_at)
        ),
        "available_at": (
            None if result.available_at is None else _optional_utc_text(result.available_at)
        ),
        "content_hash": result.content_hash,
        "snapshot_age_seconds": (
            None if result.snapshot_age is None else _duration_seconds(result.snapshot_age)
        ),
        "candidate_count": result.candidate_count,
        "eligible_count": result.eligible_count,
        "excluded_count": result.excluded_count,
        "unresolved_count": result.unresolved_count,
    }
    print(json.dumps(summary, ensure_ascii=False, separators=(",", ":")))
    return 2 if result.status is MarketEligibilityEvaluationStatus.UNRESOLVED else 0


def _print_market_liquidity(
    quote_currency: str,
    window_end: datetime,
    as_of: datetime,
    maximum_snapshot_age_seconds: int,
    database: Path,
) -> int:
    result = evaluate_market_liquidity_as_of(
        quote_currency,
        window_end,
        as_of,
        database,
        timedelta(seconds=maximum_snapshot_age_seconds),
    )

    for metric in result.metrics:
        payload = {
            "type": "market",
            "market": metric.market,
            "status": metric.status.value,
            "reasons": [reason.value for reason in metric.reasons],
            "expected_count": metric.expected_count,
            "traded_count": metric.traded_count,
            "no_trade_count": metric.no_trade_count,
            "unresolved_count": metric.unresolved_count,
            "quote_volume": None if metric.quote_volume is None else str(metric.quote_volume),
            "rank": metric.rank,
        }
        print(json.dumps(payload, separators=(",", ":")))

    summary = {
        "type": "summary",
        "status": result.status.value,
        "reasons": [reason.value for reason in result.reasons],
        "policy_name": result.policy_name,
        "policy_version": result.policy_version,
        "catalog_policy_name": result.catalog_policy_name,
        "catalog_policy_version": result.catalog_policy_version,
        "quote_currency": result.quote_currency,
        "window_start": _optional_utc_text(result.window_start),
        "window_end": _optional_utc_text(result.window_end),
        "as_of": _optional_utc_text(result.as_of),
        "lookback_days": result.lookback.days,
        "maximum_snapshot_age_seconds": _duration_seconds(result.maximum_snapshot_age),
        "snapshot_id": result.snapshot_id,
        "observed_at": (
            None if result.observed_at is None else _optional_utc_text(result.observed_at)
        ),
        "available_at": (
            None if result.available_at is None else _optional_utc_text(result.available_at)
        ),
        "content_hash": result.content_hash,
        "catalog_candidate_count": result.catalog_candidate_count,
        "catalog_excluded_count": result.catalog_excluded_count,
        "liquidity_candidate_count": result.candidate_count,
        "liquidity_resolved_count": result.resolved_market_count,
        "liquidity_unresolved_count": result.unresolved_market_count,
    }
    print(json.dumps(summary, separators=(",", ":")))
    return 2 if result.status is MarketLiquidityEvaluationStatus.UNRESOLVED else 0


def _print_liquidity_top_n(
    quote_currency: str,
    window_end: datetime,
    as_of: datetime,
    maximum_snapshot_age_seconds: int,
    top_n: int,
    database: Path,
) -> int:
    result = evaluate_liquidity_top_n_as_of(
        quote_currency,
        window_end,
        as_of,
        database,
        timedelta(seconds=maximum_snapshot_age_seconds),
        top_n,
    )

    for decision in result.decisions:
        payload = {
            "type": "market",
            "market": decision.market,
            "status": decision.status.value,
            "reasons": [reason.value for reason in decision.reasons],
            "quote_volume": (None if decision.quote_volume is None else str(decision.quote_volume)),
            "liquidity_rank": decision.liquidity_rank,
        }
        print(json.dumps(payload, separators=(",", ":")))

    summary = {
        "type": "summary",
        "status": result.status.value,
        "reasons": [reason.value for reason in result.reasons],
        "policy_name": result.policy_name,
        "policy_version": result.policy_version,
        "liquidity_policy_name": result.liquidity_policy_name,
        "liquidity_policy_version": result.liquidity_policy_version,
        "catalog_policy_name": result.catalog_policy_name,
        "catalog_policy_version": result.catalog_policy_version,
        "top_n": result.top_n,
        "quote_currency": result.quote_currency,
        "window_start": _optional_utc_text(result.window_start),
        "window_end": _optional_utc_text(result.window_end),
        "as_of": _optional_utc_text(result.as_of),
        "lookback_days": result.lookback.days,
        "maximum_snapshot_age_seconds": _duration_seconds(result.maximum_snapshot_age),
        "snapshot_id": result.snapshot_id,
        "observed_at": (
            None if result.observed_at is None else _optional_utc_text(result.observed_at)
        ),
        "available_at": (
            None if result.available_at is None else _optional_utc_text(result.available_at)
        ),
        "content_hash": result.content_hash,
        "catalog_candidate_count": result.catalog_candidate_count,
        "catalog_excluded_count": result.catalog_excluded_count,
        "liquidity_candidate_count": result.liquidity_candidate_count,
        "liquidity_resolved_count": result.liquidity_resolved_count,
        "liquidity_unresolved_count": result.liquidity_unresolved_count,
        "selected_count": result.selected_count,
        "excluded_count": result.excluded_count,
        "unresolved_count": result.unresolved_count,
    }
    print(json.dumps(summary, separators=(",", ":")))
    return 2 if result.status is LiquidityTopNEvaluationStatus.UNRESOLVED else 0


async def _print_candles(market: str, count: int) -> None:
    candles = await fetch_recent_minute_candles(market, count)

    print(f"{market} completed 1m candles: {len(candles)}")

    for candle in candles:
        start_time = candle.start_time_utc.isoformat().replace("+00:00", "Z")
        print(
            f"{start_time} "
            f"O={candle.open} H={candle.high} L={candle.low} C={candle.close} "
            f"base_volume={candle.base_volume} quote_volume={candle.quote_volume}"
        )


async def _sync_candles(market: str, count: int, database: Path) -> None:
    result = await sync_recent_minute_candles(market, count, database)

    print(f"raw_received={result.raw_count}")
    print(f"canonical_new={result.new_canonical_count}")
    print(f"canonical_revised={result.revised_canonical_count}")
    print(f"canonical_unchanged={result.unchanged_canonical_count}")
    print(f"database={database.resolve()}")


async def _backfill_candles(
    market: str,
    start: datetime,
    end: datetime,
    database: Path,
) -> None:
    result = await backfill_minute_candles(market, start, end, database)

    print(f"pages_fetched={result.pages_fetched}")
    print(f"raw_received={result.raw_received}")
    print(f"canonical_new={result.canonical_new}")
    print(f"canonical_revised={result.canonical_revised}")
    print(f"canonical_unchanged={result.canonical_unchanged}")
    print(f"earliest_candle_time={_optional_utc_text(result.earliest_candle_time)}")
    print(f"latest_candle_time={_optional_utc_text(result.latest_candle_time)}")
    print(f"requested_start={_optional_utc_text(result.requested_start)}")
    print(f"requested_end={_optional_utc_text(result.requested_end)}")


async def _backfill_dataset(
    markets: list[str],
    start: datetime,
    end: datetime,
    database: Path,
) -> int:
    result = await backfill_multi_market_dataset(markets, start, end, database)

    for market_result in result.markets:
        print(json.dumps(market_result_payload(market_result), separators=(",", ":")))

    print(json.dumps(summary_payload(result.summary), separators=(",", ":")))
    return 1 if result.summary.failed else 0


async def _bootstrap_liquidity_dataset(
    quote_currency: str,
    universe_as_of: datetime,
    window_end: datetime,
    maximum_snapshot_age_seconds: int,
    database: Path,
) -> int:
    result = await bootstrap_liquidity_dataset(
        quote_currency,
        universe_as_of,
        window_end,
        database,
        timedelta(seconds=maximum_snapshot_age_seconds),
    )

    for market_result in result.markets:
        print(json.dumps(bootstrap_market_payload(market_result), separators=(",", ":")))

    print(json.dumps(bootstrap_summary_payload(result.summary), separators=(",", ":")))
    if result.summary.status is LiquidityDatasetBootstrapStatus.UNRESOLVED:
        return 2
    if result.summary.status is LiquidityDatasetBootstrapStatus.PARTIAL_FAILURE:
        return 1
    return 0


async def _refresh_liquidity_dataset(
    quote_currency: str,
    maximum_snapshot_age_seconds: int,
    database: Path,
) -> int:
    result = await refresh_liquidity_dataset(
        quote_currency,
        database,
        timedelta(seconds=maximum_snapshot_age_seconds),
    )
    for market_result in result.markets:
        print(json.dumps(refresh_market_payload(market_result), separators=(",", ":")))
    print(json.dumps(refresh_summary_payload(result.summary), separators=(",", ":")))
    if result.summary.status is LiquidityDatasetRefreshStatus.UNRESOLVED:
        return 2
    if result.summary.status in {
        LiquidityDatasetRefreshStatus.PARTIAL_FAILURE,
        LiquidityDatasetRefreshStatus.CATALOG_FAILED,
    }:
        return 1
    return 0


def _optional_utc_text(value: datetime | None) -> str:
    if value is None:
        return "None"

    return value.isoformat().replace("+00:00", "Z")


def _duration_seconds(value: timedelta) -> int | float:
    microseconds = (value.days * 86_400 + value.seconds) * 1_000_000 + value.microseconds

    if microseconds % 1_000_000 == 0:
        return microseconds // 1_000_000

    return microseconds / 1_000_000


async def _stream_public(markets: list[str], max_events: int | None) -> int:
    events = stream_public_events(
        markets,
        lifecycle_callback=_print_lifecycle,
    )
    emitted = 0

    try:
        async for event in events:
            print(json.dumps(_public_event_payload(event), separators=(",", ":")))
            emitted += 1

            if max_events is not None and emitted >= max_events:
                break
    finally:
        await events.aclose()

    return 0


def _public_event_payload(event: PublicMarketEvent) -> dict[str, str]:
    if isinstance(event, TickerEvent):
        return {
            "event_type": "ticker",
            "market": event.market,
            "trade_price": str(event.trade_price),
            "signed_change_rate": str(event.signed_change_rate),
            "acc_trade_price_24h": str(event.acc_trade_price_24h),
            "trade_timestamp_utc": _optional_utc_text(event.trade_timestamp_utc),
            "received_at": _optional_utc_text(event.received_at),
            "stream_type": event.stream_type,
        }

    if not isinstance(event, MinuteCandleUpdate):
        raise TypeError(f"unsupported public event: {type(event).__name__}")

    return {
        "event_type": "minute_candle_update",
        "market": event.market,
        "start_time_utc": _optional_utc_text(event.start_time_utc),
        "end_time_utc": _optional_utc_text(event.end_time_utc),
        "open": str(event.open),
        "high": str(event.high),
        "low": str(event.low),
        "close": str(event.close),
        "base_volume": str(event.base_volume),
        "quote_volume": str(event.quote_volume),
        "event_time_utc": _optional_utc_text(event.event_time_utc),
        "received_at": _optional_utc_text(event.received_at),
        "stream_type": event.stream_type,
    }


def _print_lifecycle(event: StreamLifecycleEvent) -> None:
    payload: dict[str, object] = {
        "lifecycle": event.status,
        "attempt": event.attempt,
    }

    if event.delay_seconds is not None:
        payload["delay_seconds"] = event.delay_seconds

    if event.detail is not None:
        payload["detail"] = event.detail

    print(json.dumps(payload, separators=(",", ":")), file=sys.stderr)


async def _run_market_data(
    market: str,
    database: Path,
    startup_lookback_minutes: int,
    finalization_delay_seconds: int,
    max_reconciliations: int | None,
) -> int:
    await run_market_data(
        market,
        database,
        startup_lookback_minutes=startup_lookback_minutes,
        finalization_delay_seconds=finalization_delay_seconds,
        max_reconciliations=max_reconciliations,
        reconciliation_callback=_print_reconciliation,
        lifecycle_callback=_print_lifecycle,
        runtime_event_callback=_print_runtime_recovery,
    )
    return 0


def _print_reconciliation(result: MinuteReconciliationResult) -> None:
    payload = {
        "market": result.market,
        "start_time_utc": _optional_utc_text(result.start_time_utc),
        "end_time_utc": _optional_utc_text(result.end_time_utc),
        "status": result.status,
        "raw_received": result.raw_received,
        "canonical_new": result.canonical_new,
        "canonical_revised": result.canonical_revised,
        "canonical_unchanged": result.canonical_unchanged,
        "available_at": _optional_utc_text(result.available_at),
    }
    print(json.dumps(payload, separators=(",", ":")))


def _print_runtime_recovery(event: RuntimeRecoveryEvent) -> None:
    payload: dict[str, object] = {
        "runtime": event.kind,
        "market": event.market,
        "start_time_utc": _optional_utc_text(event.start_time_utc),
        "end_time_utc": _optional_utc_text(event.end_time_utc),
    }

    if event.result is not None:
        payload.update(
            {
                "pages_fetched": event.result.pages_fetched,
                "raw_received": event.result.raw_received,
                "canonical_new": event.result.canonical_new,
                "canonical_revised": event.result.canonical_revised,
                "canonical_unchanged": event.result.canonical_unchanged,
            }
        )

    print(json.dumps(payload, separators=(",", ":")), file=sys.stderr)


def _print_four_hour_bar(
    market: str,
    start_time_utc: datetime,
    as_of: datetime,
    database: Path,
) -> int:
    result = derive_four_hour_bar(market, start_time_utc, as_of, database)
    start_text = result.start_time_utc.isoformat().replace("+00:00", "Z")
    end_text = result.end_time_utc.isoformat().replace("+00:00", "Z")

    print(f"status={result.status}")
    print(f"market={result.market}")
    print(f"start_time_utc={start_text}")
    print(f"end_time_utc={end_text}")

    if result.status is FourHourBarStatus.INCOMPLETE:
        print(f"reason={result.reason}")
        print(f"actual_count={result.actual_count}")
        print(f"expected_count={result.expected_count}")
        print(f"missing_count={len(result.missing_minute_starts)}")
        print(f"resolved_count={result.resolved_count}")
        print(f"traded_count={result.traded_count}")
        print(f"no_trade_count={result.no_trade_count}")
        print(f"unresolved_count={result.unresolved_count}")
        print(
            "unresolved_minute_starts="
            + ",".join(_optional_utc_text(start) for start in result.unresolved_minute_starts)
        )
        return 2

    if result.status is FourHourBarStatus.NO_TRADE:
        print(f"expected_count={result.expected_count}")
        print(f"resolved_count={result.resolved_count}")
        print(f"traded_count={result.traded_count}")
        print(f"no_trade_count={result.no_trade_count}")
        print(f"unresolved_count={result.unresolved_count}")
        return 0

    if result.candle is None:
        raise RuntimeError("COMPLETE result must contain a four-hour candle")

    print(f"open={result.candle.open}")
    print(f"high={result.candle.high}")
    print(f"low={result.candle.low}")
    print(f"close={result.candle.close}")
    print(f"base_volume={result.candle.base_volume}")
    print(f"quote_volume={result.candle.quote_volume}")
    print(f"expected_count={result.expected_count}")
    print(f"resolved_count={result.resolved_count}")
    print(f"traded_count={result.traded_count}")
    print(f"no_trade_count={result.no_trade_count}")
    print(f"unresolved_count={result.unresolved_count}")
    return 0


def main(argv: Sequence[str] | None = None) -> int | None:
    """Run the Cassandra Trader command-line interface."""

    parser = _build_parser()
    arguments = parser.parse_args(argv)

    if arguments.command is None:
        print("Cassandra Trader")
        return

    if arguments.command == "markets":
        asyncio.run(_print_markets(arguments.limit))

    if arguments.command == "capture-universe":
        return asyncio.run(_capture_universe(arguments.database))

    if arguments.command == "universe-as-of":
        return _print_universe_as_of(
            arguments.quote_currency,
            arguments.as_of,
            arguments.database,
        )

    if arguments.command == "eligible-markets":
        return _print_eligible_markets(
            arguments.quote_currency,
            arguments.as_of,
            arguments.maximum_snapshot_age_seconds,
            arguments.database,
        )

    if arguments.command == "market-liquidity":
        if arguments.as_of < arguments.window_end:
            parser.error("market-liquidity: --as-of must not be before --window-end")

        return _print_market_liquidity(
            arguments.quote_currency,
            arguments.window_end,
            arguments.as_of,
            arguments.maximum_snapshot_age_seconds,
            arguments.database,
        )

    if arguments.command == "liquidity-top-n":
        if arguments.as_of < arguments.window_end:
            parser.error("liquidity-top-n: --as-of must not be before --window-end")

        return _print_liquidity_top_n(
            arguments.quote_currency,
            arguments.window_end,
            arguments.as_of,
            arguments.maximum_snapshot_age_seconds,
            arguments.top_n,
            arguments.database,
        )

    if arguments.command == "bootstrap-liquidity-dataset":
        now = datetime.now(UTC)
        if arguments.window_end > arguments.universe_as_of:
            parser.error(
                "bootstrap-liquidity-dataset: --window-end must not be after --universe-as-of"
            )
        if arguments.window_end > now:
            parser.error(
                "bootstrap-liquidity-dataset: --window-end must not be after execution time"
            )
        if arguments.universe_as_of > now:
            parser.error(
                "bootstrap-liquidity-dataset: --universe-as-of must not be after execution time"
            )
        return asyncio.run(
            _bootstrap_liquidity_dataset(
                arguments.quote_currency,
                arguments.universe_as_of,
                arguments.window_end,
                arguments.maximum_snapshot_age_seconds,
                arguments.database,
            )
        )

    if arguments.command == "refresh-liquidity-dataset":
        return asyncio.run(
            _refresh_liquidity_dataset(
                arguments.quote_currency,
                arguments.maximum_snapshot_age_seconds,
                arguments.database,
            )
        )

    if arguments.command == "candles":
        asyncio.run(_print_candles(arguments.market, arguments.count))

    if arguments.command == "sync-candles":
        asyncio.run(_sync_candles(arguments.market, arguments.count, arguments.database))

    if arguments.command == "backfill-candles":
        if arguments.start >= arguments.end:
            parser.error("backfill-candles: --start must be before --end")

        asyncio.run(
            _backfill_candles(
                arguments.market,
                arguments.start,
                arguments.end,
                arguments.database,
            )
        )
        return 0

    if arguments.command == "backfill-dataset":
        if arguments.start >= arguments.end:
            parser.error("backfill-dataset: --start must be before --end")

        return asyncio.run(
            _backfill_dataset(
                arguments.markets,
                arguments.start,
                arguments.end,
                arguments.database,
            )
        )

    if arguments.command == "four-hour-bar":
        return _print_four_hour_bar(
            arguments.market,
            arguments.start,
            arguments.as_of,
            arguments.database,
        )

    if arguments.command == "stream-public":
        try:
            return asyncio.run(_stream_public(arguments.markets, arguments.max_events))
        except KeyboardInterrupt:
            return 0

    if arguments.command == "run-market-data":
        try:
            return asyncio.run(
                _run_market_data(
                    arguments.market,
                    arguments.database,
                    arguments.startup_lookback_minutes,
                    arguments.finalization_delay_seconds,
                    arguments.max_reconciliations,
                )
            )
        except KeyboardInterrupt:
            return 0

    return None
