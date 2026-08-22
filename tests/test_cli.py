from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from cassandra_trader.adapters.storage.sqlite import IngestionStats
from cassandra_trader.apps.minute_candle_backfill import MinuteCandleBackfillResult
from cassandra_trader.cli import main
from cassandra_trader.domain.candle import MinuteCandle
from cassandra_trader.domain.four_hour_bar import (
    FourHourBarReason,
    FourHourBarResult,
    FourHourBarStatus,
    FourHourCandle,
)
from cassandra_trader.domain.market import TradingPair


def test_main(capsys: pytest.CaptureFixture[str]) -> None:
    main([])

    captured = capsys.readouterr()

    assert captured.out == "Cassandra Trader\n"


def test_markets_command(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def fake_fetch_krw_trading_pairs() -> tuple[TradingPair, ...]:
        return (
            TradingPair(
                code="KRW-BTC",
                korean_name="비트코인",
                english_name="Bitcoin",
                is_warning=False,
            ),
            TradingPair(
                code="KRW-ETH",
                korean_name="이더리움",
                english_name="Ethereum",
                is_warning=False,
            ),
        )

    monkeypatch.setattr(
        "cassandra_trader.cli.fetch_krw_trading_pairs",
        fake_fetch_krw_trading_pairs,
    )

    main(["markets", "--limit", "1"])

    captured = capsys.readouterr()

    assert captured.out == "KRW trading pairs: 2\nKRW-BTC      비트코인\n"


def test_candles_command(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def fake_fetch_recent_minute_candles(
        market: str,
        count: int,
    ) -> tuple[MinuteCandle, ...]:
        assert market == "KRW-BTC"
        assert count == 1
        start = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)

        return (
            MinuteCandle(
                market=market,
                start_time_utc=start,
                end_time_utc=start + timedelta(minutes=1),
                open=Decimal("100"),
                high=Decimal("110"),
                low=Decimal("90"),
                close=Decimal("105"),
                base_volume=Decimal("1.5"),
                quote_volume=Decimal("150"),
            ),
        )

    monkeypatch.setattr(
        "cassandra_trader.cli.fetch_recent_minute_candles",
        fake_fetch_recent_minute_candles,
    )

    main(["candles", "krw-btc", "--count", "1"])

    captured = capsys.readouterr()

    assert captured.out == (
        "KRW-BTC completed 1m candles: 1\n"
        "2025-01-01T12:00:00Z O=100 H=110 L=90 C=105 "
        "base_volume=1.5 quote_volume=150\n"
    )


@pytest.mark.parametrize(
    "arguments",
    [
        ["candles", "BTC", "--count", "1"],
        ["candles", "KRW-BTC", "--count", "0"],
        ["candles", "KRW-BTC", "--count", "201"],
    ],
)
def test_candles_command_rejects_invalid_arguments(arguments: list[str]) -> None:
    with pytest.raises(SystemExit):
        main(arguments)


def test_sync_candles_command(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    database = tmp_path / "nested" / "cassandra.db"

    async def fake_sync_recent_minute_candles(
        market: str,
        count: int,
        database_path: str | Path,
    ) -> IngestionStats:
        assert market == "KRW-BTC"
        assert count == 5
        assert database_path == database
        return IngestionStats(
            raw_count=5,
            new_canonical_count=3,
            revised_canonical_count=1,
            unchanged_canonical_count=1,
        )

    monkeypatch.setattr(
        "cassandra_trader.cli.sync_recent_minute_candles",
        fake_sync_recent_minute_candles,
    )

    main(
        [
            "sync-candles",
            "KRW-BTC",
            "--count",
            "5",
            "--database",
            str(database),
        ]
    )

    captured = capsys.readouterr()

    assert captured.out == (
        "raw_received=5\n"
        "canonical_new=3\n"
        "canonical_revised=1\n"
        "canonical_unchanged=1\n"
        f"database={database.resolve()}\n"
    )


def test_backfill_candles_command(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    start = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)
    end = start + timedelta(hours=4)
    database = tmp_path / "cassandra.db"

    async def fake_backfill_minute_candles(
        market: str,
        requested_start: datetime,
        requested_end: datetime,
        database_path: str | Path,
    ) -> MinuteCandleBackfillResult:
        assert market == "KRW-BTC"
        assert requested_start == start
        assert requested_end == end
        assert database_path == database
        return MinuteCandleBackfillResult(
            pages_fetched=2,
            raw_received=240,
            canonical_new=239,
            canonical_revised=0,
            canonical_unchanged=1,
            earliest_candle_time=start,
            latest_candle_time=end - timedelta(minutes=1),
            requested_start=start,
            requested_end=end,
        )

    monkeypatch.setattr(
        "cassandra_trader.cli.backfill_minute_candles",
        fake_backfill_minute_candles,
    )

    exit_code = main(
        [
            "backfill-candles",
            "krw-btc",
            "--start",
            "2026-08-22T12:00:00Z",
            "--end",
            "2026-08-22T16:00:00Z",
            "--database",
            str(database),
        ]
    )

    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.out == (
        "pages_fetched=2\n"
        "raw_received=240\n"
        "canonical_new=239\n"
        "canonical_revised=0\n"
        "canonical_unchanged=1\n"
        "earliest_candle_time=2026-08-22T12:00:00Z\n"
        "latest_candle_time=2026-08-22T15:59:00Z\n"
        "requested_start=2026-08-22T12:00:00Z\n"
        "requested_end=2026-08-22T16:00:00Z\n"
    )


@pytest.mark.parametrize(
    ("start", "end"),
    [
        ("2026-08-22T12:00:00", "2026-08-22T16:00:00Z"),
        ("2026-08-22T12:00:01Z", "2026-08-22T16:00:00Z"),
        ("2026-08-22T12:00:00Z", "2026-08-22T12:00:00Z"),
        ("2026-08-22T16:00:00Z", "2026-08-22T12:00:00Z"),
    ],
)
def test_backfill_candles_rejects_invalid_range(
    start: str,
    end: str,
    tmp_path: Path,
) -> None:
    with pytest.raises(SystemExit):
        main(
            [
                "backfill-candles",
                "KRW-BTC",
                "--start",
                start,
                "--end",
                end,
                "--database",
                str(tmp_path / "cassandra.db"),
            ]
        )


def test_four_hour_bar_complete_output(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    start = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)
    end = start + timedelta(hours=4)
    candle = FourHourCandle(
        market="KRW-BTC",
        start_time_utc=start,
        end_time_utc=end,
        open=Decimal("100"),
        high=Decimal("120"),
        low=Decimal("90"),
        close=Decimal("110"),
        base_volume=Decimal("24.5"),
        quote_volume=Decimal("2500.75"),
    )

    def fake_derive_four_hour_bar(
        market: str,
        start_time_utc: datetime,
        as_of: datetime,
        database: str | Path,
    ) -> FourHourBarResult:
        assert market == "KRW-BTC"
        assert start_time_utc == start
        assert as_of == end + timedelta(minutes=10)
        return FourHourBarResult(
            status=FourHourBarStatus.COMPLETE,
            market=market,
            start_time_utc=start,
            end_time_utc=end,
            expected_count=240,
            actual_count=240,
            missing_minute_starts=(),
            candle=candle,
            reason=None,
        )

    monkeypatch.setattr(
        "cassandra_trader.cli.derive_four_hour_bar",
        fake_derive_four_hour_bar,
    )

    exit_code = main(
        [
            "four-hour-bar",
            "KRW-BTC",
            "--start",
            "2026-08-22T12:00:00Z",
            "--as-of",
            "2026-08-22T16:10:00Z",
            "--database",
            str(tmp_path / "cassandra.db"),
        ]
    )

    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.out == (
        "status=COMPLETE\n"
        "market=KRW-BTC\n"
        "start_time_utc=2026-08-22T12:00:00Z\n"
        "end_time_utc=2026-08-22T16:00:00Z\n"
        "open=100\n"
        "high=120\n"
        "low=90\n"
        "close=110\n"
        "base_volume=24.5\n"
        "quote_volume=2500.75\n"
    )


def test_four_hour_bar_incomplete_output(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    start = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)
    missing = start + timedelta(minutes=42)

    def fake_derive_four_hour_bar(
        market: str,
        start_time_utc: datetime,
        as_of: datetime,
        database: str | Path,
    ) -> FourHourBarResult:
        return FourHourBarResult(
            status=FourHourBarStatus.INCOMPLETE,
            market=market,
            start_time_utc=start,
            end_time_utc=start + timedelta(hours=4),
            expected_count=240,
            actual_count=239,
            missing_minute_starts=(missing,),
            candle=None,
            reason=FourHourBarReason.MISSING_MINUTES,
        )

    monkeypatch.setattr(
        "cassandra_trader.cli.derive_four_hour_bar",
        fake_derive_four_hour_bar,
    )

    exit_code = main(
        [
            "four-hour-bar",
            "KRW-BTC",
            "--start",
            "2026-08-22T12:00:00Z",
            "--as-of",
            "2026-08-22T16:10:00Z",
            "--database",
            str(tmp_path / "cassandra.db"),
        ]
    )

    captured = capsys.readouterr()

    assert exit_code == 2
    assert captured.out == (
        "status=INCOMPLETE\n"
        "market=KRW-BTC\n"
        "start_time_utc=2026-08-22T12:00:00Z\n"
        "end_time_utc=2026-08-22T16:00:00Z\n"
        "reason=MISSING_MINUTES\n"
        "actual_count=239\n"
        "expected_count=240\n"
        "missing_count=1\n"
    )


@pytest.mark.parametrize(
    "arguments",
    [
        ["--start", "2026-08-22T13:00:00Z", "--as-of", "2026-08-22T16:10:00Z"],
        ["--start", "2026-08-22T12:00:00", "--as-of", "2026-08-22T16:10:00Z"],
        ["--start", "2026-08-22T12:00:00Z", "--as-of", "2026-08-22T16:10:00"],
    ],
)
def test_four_hour_bar_rejects_invalid_utc_arguments(
    arguments: list[str],
    tmp_path: Path,
) -> None:
    with pytest.raises(SystemExit):
        main(
            [
                "four-hour-bar",
                "KRW-BTC",
                *arguments,
                "--database",
                str(tmp_path / "cassandra.db"),
            ]
        )
