"""Command-line interface for Cassandra Trader."""

import argparse
import asyncio
from collections.abc import Sequence

from cassandra_trader.apps.market_catalog import fetch_krw_trading_pairs


def _positive_integer(value: str) -> int:
    number = int(value)

    if number < 1:
        raise argparse.ArgumentTypeError("value must be at least 1")

    return number


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

    return parser


async def _print_markets(limit: int) -> None:
    pairs = await fetch_krw_trading_pairs()

    print(f"KRW trading pairs: {len(pairs)}")

    for pair in pairs[:limit]:
        warning = " [WARNING]" if pair.is_warning else ""
        print(f"{pair.code:<12} {pair.korean_name}{warning}")


def main(argv: Sequence[str] | None = None) -> None:
    """Run the Cassandra Trader command-line interface."""

    parser = _build_parser()
    arguments = parser.parse_args(argv)

    if arguments.command is None:
        print("Cassandra Trader")
        return

    if arguments.command == "markets":
        asyncio.run(_print_markets(arguments.limit))
