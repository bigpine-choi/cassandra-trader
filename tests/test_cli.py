import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from cassandra_trader.adapters.storage.sqlite import IngestionStats
from cassandra_trader.adapters.upbit.public_stream import (
    LifecycleStatus,
    StreamLifecycleEvent,
)
from cassandra_trader.apps.market_data_runtime import (
    RuntimeRecoveryEvent,
    RuntimeRecoveryKind,
)
from cassandra_trader.apps.minute_candle_backfill import MinuteCandleBackfillResult
from cassandra_trader.apps.minute_reconciliation import (
    MinuteReconciliationResult,
    MinuteReconciliationStatus,
)
from cassandra_trader.apps.multi_market_dataset import (
    DatasetBackfillResult,
    DatasetBackfillSummary,
    DatasetMarketResult,
    DatasetMarketStatus,
)
from cassandra_trader.cli import main
from cassandra_trader.domain.candle import MinuteCandle
from cassandra_trader.domain.four_hour_bar import (
    FourHourBarReason,
    FourHourBarResult,
    FourHourBarStatus,
    FourHourCandle,
)
from cassandra_trader.domain.market import TradingPair
from cassandra_trader.domain.public_stream import StreamType, TickerEvent


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


@pytest.mark.parametrize("market", ["KRW-비트", "ＫＲＷ-BTC"])
def test_candles_command_rejects_unicode_market_before_service(
    monkeypatch: pytest.MonkeyPatch,
    market: str,
) -> None:
    called = False

    async def unexpected_call(*args, **kwargs):
        nonlocal called
        called = True
        return ()

    monkeypatch.setattr(
        "cassandra_trader.cli.fetch_recent_minute_candles",
        unexpected_call,
    )

    with pytest.raises(SystemExit):
        main(["candles", market, "--count", "1"])

    assert called is False


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


def test_backfill_dataset_outputs_sorted_json_lines_and_failure_exit_code(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    from cassandra_trader.adapters.storage.sqlite import CanonicalMinuteCandleCoverage

    start = datetime(2026, 8, 22, 8, 0, tzinfo=UTC)
    end = start + timedelta(hours=8)
    observed_at = datetime(2026, 8, 23, tzinfo=UTC)
    coverage = CanonicalMinuteCandleCoverage(
        market="KRW-XRP",
        requested_start=start,
        requested_end=end,
        as_of=observed_at,
        expected_slots=480,
        observed_slots=479,
        absent_slots=1,
        earliest_candle_time=start,
        latest_candle_time=end - timedelta(minutes=1),
    )
    backfill = MinuteCandleBackfillResult(
        pages_fetched=3,
        raw_received=479,
        canonical_new=479,
        canonical_revised=0,
        canonical_unchanged=0,
        earliest_candle_time=start,
        latest_candle_time=end - timedelta(minutes=1),
        requested_start=start,
        requested_end=end,
    )

    async def fake_backfill_multi_market_dataset(markets, requested_start, requested_end, db):
        assert markets == ["KRW-XRP", "KRW-BTC", "KRW-XRP"]
        assert requested_start == start
        assert requested_end == end
        assert db == tmp_path / "candles.db"
        return DatasetBackfillResult(
            markets=(
                DatasetMarketResult(
                    market="KRW-BTC",
                    status=DatasetMarketStatus.FAILED,
                    backfill=None,
                    coverage=None,
                    error_type="TimeoutError",
                    error_message="request timed out",
                ),
                DatasetMarketResult(
                    market="KRW-XRP",
                    status=DatasetMarketStatus.SUCCEEDED,
                    backfill=backfill,
                    coverage=coverage,
                    error_type=None,
                    error_message=None,
                ),
            ),
            summary=DatasetBackfillSummary(
                requested_start=start,
                requested_end=end,
                observed_at=observed_at,
                coverage_as_of=observed_at,
                total=2,
                succeeded=1,
                failed=1,
            ),
        )

    monkeypatch.setattr(
        "cassandra_trader.cli.backfill_multi_market_dataset",
        fake_backfill_multi_market_dataset,
    )

    exit_code = main(
        [
            "backfill-dataset",
            "KRW-XRP",
            "krw-btc",
            "KRW-XRP",
            "--start",
            "2026-08-22T08:00:00Z",
            "--end",
            "2026-08-22T16:00:00Z",
            "--database",
            str(tmp_path / "candles.db"),
        ]
    )

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert exit_code == 1
    assert [line["type"] for line in lines] == ["market", "market", "summary"]
    assert [line.get("market") for line in lines] == ["KRW-BTC", "KRW-XRP", None]
    assert lines[0]["status"] == "FAILED"
    assert lines[0]["error"] == {"type": "TimeoutError", "message": "request timed out"}
    assert lines[1]["backfill"]["canonical_new"] == 479
    assert lines[1]["coverage"]["absent_slots"] == 1
    assert lines[2]["failed"] == 1
    assert lines[2]["requested_start"] == "2026-08-22T08:00:00Z"
    assert lines[2]["requested_end"] == "2026-08-22T16:00:00Z"


@pytest.mark.parametrize(
    "arguments",
    [
        ["BTC-USDT", "--start", "2026-08-22T08:00:00Z", "--end", "2026-08-22T09:00:00Z"],
        ["KRW-BTC", "--start", "2026-08-22T08:00:00", "--end", "2026-08-22T09:00:00Z"],
        ["KRW-BTC", "--start", "2026-08-22T09:00:00Z", "--end", "2026-08-22T08:00:00Z"],
    ],
)
def test_backfill_dataset_rejects_invalid_arguments_before_service(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    arguments: list[str],
) -> None:
    called = False

    async def unexpected_call(*args, **kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(
        "cassandra_trader.cli.backfill_multi_market_dataset",
        unexpected_call,
        raising=False,
    )

    with pytest.raises(SystemExit):
        main(
            [
                "backfill-dataset",
                *arguments,
                "--database",
                str(tmp_path / "candles.db"),
            ]
        )

    assert called is False


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


def test_stream_public_outputs_json_lines_and_lifecycle_to_stderr(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    closed = False
    event = TickerEvent(
        market="KRW-BTC",
        trade_price=Decimal("105307000.123456789"),
        signed_change_rate=Decimal("-0.0012"),
        acc_trade_price_24h=Decimal("123456789.987654321"),
        trade_timestamp_utc=datetime(2026, 8, 22, 16, 59, 59, 123000, tzinfo=UTC),
        received_at=datetime(2026, 8, 22, 17, 0, tzinfo=UTC),
        stream_type=StreamType.REALTIME,
    )

    async def fake_stream_public_events(markets, *, lifecycle_callback):
        nonlocal closed
        assert markets == ["KRW-BTC", "KRW-ETH"]
        lifecycle_callback(
            StreamLifecycleEvent(
                status=LifecycleStatus.CONNECTED,
                attempt=0,
            )
        )

        try:
            yield event
            yield event
        finally:
            closed = True
            lifecycle_callback(
                StreamLifecycleEvent(
                    status=LifecycleStatus.DISCONNECTED,
                    attempt=0,
                )
            )

    monkeypatch.setattr(
        "cassandra_trader.cli.stream_public_events",
        fake_stream_public_events,
    )

    exit_code = main(
        [
            "stream-public",
            "krw-btc",
            "KRW-ETH",
            "--max-events",
            "1",
        ]
    )

    captured = capsys.readouterr()
    output = json.loads(captured.out)
    lifecycle = [json.loads(line) for line in captured.err.splitlines()]

    assert exit_code == 0
    assert output == {
        "event_type": "ticker",
        "market": "KRW-BTC",
        "trade_price": "105307000.123456789",
        "signed_change_rate": "-0.0012",
        "acc_trade_price_24h": "123456789.987654321",
        "trade_timestamp_utc": "2026-08-22T16:59:59.123000Z",
        "received_at": "2026-08-22T17:00:00Z",
        "stream_type": "REALTIME",
    }
    assert lifecycle == [
        {"lifecycle": "CONNECTED", "attempt": 0},
        {"lifecycle": "DISCONNECTED", "attempt": 0},
    ]
    assert closed is True


@pytest.mark.parametrize(
    "arguments",
    [
        ["stream-public"],
        ["stream-public", "BTC"],
        ["stream-public", "KRW-BTC", "--max-events", "0"],
    ],
)
def test_stream_public_rejects_invalid_arguments(arguments: list[str]) -> None:
    with pytest.raises(SystemExit):
        main(arguments)


def test_stream_public_handles_keyboard_interrupt_without_traceback(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def interrupt(coroutine):
        coroutine.close()
        raise KeyboardInterrupt

    monkeypatch.setattr("cassandra_trader.cli.asyncio.run", interrupt)

    exit_code = main(["stream-public", "KRW-BTC"])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Traceback" not in captured.err


def test_run_market_data_outputs_only_reconciliation_to_stdout(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    database = tmp_path / "cassandra.db"
    start = datetime(2026, 8, 22, 17, 32, tzinfo=UTC)
    end = start + timedelta(minutes=1)
    backfill = MinuteCandleBackfillResult(
        pages_fetched=1,
        raw_received=10,
        canonical_new=9,
        canonical_revised=0,
        canonical_unchanged=1,
        earliest_candle_time=start - timedelta(minutes=9),
        latest_candle_time=start,
        requested_start=start - timedelta(minutes=9),
        requested_end=end,
    )
    reconciliation = MinuteReconciliationResult(
        market="KRW-BTC",
        start_time_utc=start,
        end_time_utc=end,
        status=MinuteReconciliationStatus.CONFIRMED,
        raw_received=1,
        canonical_new=0,
        canonical_revised=0,
        canonical_unchanged=1,
        available_at=end + timedelta(seconds=3),
    )

    async def fake_run_market_data(
        market,
        database_path,
        *,
        startup_lookback_minutes,
        finalization_delay_seconds,
        max_reconciliations,
        reconciliation_callback,
        lifecycle_callback,
        runtime_event_callback,
    ):
        assert market == "KRW-BTC"
        assert database_path == database
        assert startup_lookback_minutes == 5
        assert finalization_delay_seconds == 0
        assert max_reconciliations == 1
        runtime_event_callback(
            RuntimeRecoveryEvent(
                kind=RuntimeRecoveryKind.STARTUP_STARTED,
                market=market,
                start_time_utc=backfill.requested_start,
                end_time_utc=end,
            )
        )
        lifecycle_callback(StreamLifecycleEvent(status=LifecycleStatus.CONNECTED, attempt=0))
        runtime_event_callback(
            RuntimeRecoveryEvent(
                kind=RuntimeRecoveryKind.STARTUP_COMPLETED,
                market=market,
                start_time_utc=backfill.requested_start,
                end_time_utc=end,
                result=backfill,
            )
        )
        reconciliation_callback(reconciliation)
        lifecycle_callback(StreamLifecycleEvent(status=LifecycleStatus.DISCONNECTED, attempt=0))

    monkeypatch.setattr(
        "cassandra_trader.cli.run_market_data",
        fake_run_market_data,
    )

    exit_code = main(
        [
            "run-market-data",
            "krw-btc",
            "--database",
            str(database),
            "--startup-lookback-minutes",
            "5",
            "--finalization-delay-seconds",
            "0",
            "--max-reconciliations",
            "1",
        ]
    )

    captured = capsys.readouterr()
    stdout_lines = [json.loads(line) for line in captured.out.splitlines()]
    stderr_lines = [json.loads(line) for line in captured.err.splitlines()]

    assert exit_code == 0
    assert stdout_lines == [
        {
            "market": "KRW-BTC",
            "start_time_utc": "2026-08-22T17:32:00Z",
            "end_time_utc": "2026-08-22T17:33:00Z",
            "status": "CONFIRMED",
            "raw_received": 1,
            "canonical_new": 0,
            "canonical_revised": 0,
            "canonical_unchanged": 1,
            "available_at": "2026-08-22T17:33:03Z",
        }
    ]
    assert [line.get("lifecycle") or line.get("runtime") for line in stderr_lines] == [
        "STARTUP_RECOVERY_STARTED",
        "CONNECTED",
        "STARTUP_RECOVERY_COMPLETED",
        "DISCONNECTED",
    ]


@pytest.mark.parametrize(
    "arguments",
    [
        ["run-market-data", "BTC", "--database", "test.db"],
        [
            "run-market-data",
            "KRW-BTC",
            "--database",
            "test.db",
            "--startup-lookback-minutes",
            "0",
        ],
        [
            "run-market-data",
            "KRW-BTC",
            "--database",
            "test.db",
            "--finalization-delay-seconds",
            "-1",
        ],
        [
            "run-market-data",
            "KRW-BTC",
            "--database",
            "test.db",
            "--max-reconciliations",
            "0",
        ],
    ],
)
def test_run_market_data_rejects_invalid_arguments(arguments: list[str]) -> None:
    with pytest.raises(SystemExit):
        main(arguments)
