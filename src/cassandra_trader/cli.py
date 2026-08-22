"""Command-line interface for Cassandra Trader."""

import argparse
import asyncio
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

from cassandra_trader.apps.four_hour_bars import derive_four_hour_bar
from cassandra_trader.apps.market_catalog import fetch_krw_trading_pairs
from cassandra_trader.apps.minute_candles import (
    fetch_recent_minute_candles,
    sync_recent_minute_candles,
)
from cassandra_trader.domain.four_hour_bar import FourHourBarStatus


def _positive_integer(value: str) -> int:
    number = int(value)

    if number < 1:
        raise argparse.ArgumentTypeError("value must be at least 1")

    return number


def _candle_count(value: str) -> int:
    number = _positive_integer(value)

    if number > 200:
        raise argparse.ArgumentTypeError("value must not exceed 200")

    return number


def _market_code(value: str) -> str:
    normalized = value.strip().upper()
    quote, separator, base = normalized.partition("-")

    if not separator or not quote.isalnum() or not base.isalnum():
        raise argparse.ArgumentTypeError("market must look like KRW-BTC")

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

    return parser


async def _print_markets(limit: int) -> None:
    pairs = await fetch_krw_trading_pairs()

    print(f"KRW trading pairs: {len(pairs)}")

    for pair in pairs[:limit]:
        warning = " [WARNING]" if pair.is_warning else ""
        print(f"{pair.code:<12} {pair.korean_name}{warning}")


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
        return 2

    if result.candle is None:
        raise RuntimeError("COMPLETE result must contain a four-hour candle")

    print(f"open={result.candle.open}")
    print(f"high={result.candle.high}")
    print(f"low={result.candle.low}")
    print(f"close={result.candle.close}")
    print(f"base_volume={result.candle.base_volume}")
    print(f"quote_volume={result.candle.quote_volume}")
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

    if arguments.command == "candles":
        asyncio.run(_print_candles(arguments.market, arguments.count))

    if arguments.command == "sync-candles":
        asyncio.run(_sync_candles(arguments.market, arguments.count, arguments.database))

    if arguments.command == "four-hour-bar":
        return _print_four_hour_bar(
            arguments.market,
            arguments.start,
            arguments.as_of,
            arguments.database,
        )

    return None
