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
from cassandra_trader.apps.liquidity_dataset_bootstrap import (
    LiquidityDatasetBootstrapResult,
    LiquidityDatasetBootstrapStatus,
    LiquidityDatasetBootstrapSummary,
    LiquidityDatasetMarketResult,
    LiquidityDatasetMarketStatus,
)
from cassandra_trader.apps.market_data_runtime import (
    RuntimeRecoveryEvent,
    RuntimeRecoveryKind,
)
from cassandra_trader.apps.market_universe import MarketCatalogCaptureResult
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
from cassandra_trader.domain.liquidity_top_n import (
    LIQUIDITY_TOP_N_POLICY_NAME,
    LIQUIDITY_TOP_N_POLICY_VERSION,
    LiquidityTopNResult,
    evaluate_liquidity_top_n,
)
from cassandra_trader.domain.market import (
    MarketUniverseResult,
    MarketUniverseStatus,
    TradingPair,
)
from cassandra_trader.domain.market_eligibility import (
    MARKET_ELIGIBILITY_POLICY_NAME,
    MARKET_ELIGIBILITY_POLICY_VERSION,
    MarketEligibilityReason,
    MarketEligibilityResult,
    evaluate_market_eligibility,
)
from cassandra_trader.domain.market_liquidity import (
    MARKET_LIQUIDITY_POLICY_NAME,
    MARKET_LIQUIDITY_POLICY_VERSION,
    MarketLiquidityResult,
    evaluate_market_liquidity,
)
from cassandra_trader.domain.minute_slot import MinuteSlotSummary
from cassandra_trader.domain.public_stream import StreamType, TickerEvent

_BOOTSTRAP_WINDOW_END = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
_BOOTSTRAP_WINDOW_START = _BOOTSTRAP_WINDOW_END - timedelta(days=30)
_BOOTSTRAP_UNIVERSE_AS_OF = _BOOTSTRAP_WINDOW_END + timedelta(minutes=1)
_BOOTSTRAP_RESUME_AS_OF = _BOOTSTRAP_WINDOW_END + timedelta(minutes=2)
_BOOTSTRAP_FINAL_AS_OF = _BOOTSTRAP_WINDOW_END + timedelta(minutes=3)


def test_main(capsys: pytest.CaptureFixture[str]) -> None:
    main([])

    captured = capsys.readouterr()

    assert captured.out == "Cassandra Trader\n"


def _bootstrap_slot_summary(
    market: str,
    as_of: datetime,
    *,
    complete: bool,
) -> MinuteSlotSummary:
    return MinuteSlotSummary(
        market=market,
        start_time_utc=_BOOTSTRAP_WINDOW_START,
        end_time_utc=_BOOTSTRAP_WINDOW_END,
        as_of=as_of,
        expected_count=43_200,
        traded_count=0,
        no_trade_count=43_200 if complete else 0,
        unresolved_count=0 if complete else 43_200,
        quote_volume=Decimal(0),
    )


def _bootstrap_cli_result(
    statuses: tuple[tuple[str, LiquidityDatasetMarketStatus], ...],
) -> LiquidityDatasetBootstrapResult:
    markets = tuple(
        LiquidityDatasetMarketResult(
            market=market,
            status=status,
            initial_summary=_bootstrap_slot_summary(
                market,
                _BOOTSTRAP_RESUME_AS_OF,
                complete=status is LiquidityDatasetMarketStatus.SKIPPED_COMPLETE,
            ),
            final_summary=_bootstrap_slot_summary(
                market,
                _BOOTSTRAP_FINAL_AS_OF,
                complete=status is not LiquidityDatasetMarketStatus.FAILED,
            ),
            backfill=(
                MinuteCandleBackfillResult(
                    pages_fetched=1,
                    raw_received=0,
                    canonical_new=0,
                    canonical_revised=0,
                    canonical_unchanged=0,
                    earliest_candle_time=None,
                    latest_candle_time=None,
                    requested_start=_BOOTSTRAP_WINDOW_START,
                    requested_end=_BOOTSTRAP_WINDOW_END,
                )
                if status is LiquidityDatasetMarketStatus.SUCCEEDED
                else None
            ),
            error_type=("TimeoutError" if status is LiquidityDatasetMarketStatus.FAILED else None),
            error_message=("timed out" if status is LiquidityDatasetMarketStatus.FAILED else None),
        )
        for market, status in statuses
    )
    failed_count = sum(status is LiquidityDatasetMarketStatus.FAILED for _, status in statuses)
    return LiquidityDatasetBootstrapResult(
        markets=markets,
        summary=LiquidityDatasetBootstrapSummary(
            status=(
                LiquidityDatasetBootstrapStatus.PARTIAL_FAILURE
                if failed_count
                else LiquidityDatasetBootstrapStatus.SUCCEEDED
            ),
            reasons=(),
            quote_currency="KRW",
            window_start=_BOOTSTRAP_WINDOW_START,
            window_end=_BOOTSTRAP_WINDOW_END,
            universe_as_of=_BOOTSTRAP_UNIVERSE_AS_OF,
            resume_as_of=_BOOTSTRAP_RESUME_AS_OF,
            final_coverage_as_of=_BOOTSTRAP_FINAL_AS_OF,
            maximum_snapshot_age=timedelta(hours=1),
            catalog_policy_name=MARKET_ELIGIBILITY_POLICY_NAME,
            catalog_policy_version=MARKET_ELIGIBILITY_POLICY_VERSION,
            snapshot_id="snapshot-cli",
            observed_at=_BOOTSTRAP_WINDOW_END,
            available_at=_BOOTSTRAP_WINDOW_END + timedelta(microseconds=1),
            content_hash="a" * 64,
            catalog_candidate_count=len(statuses),
            catalog_excluded_count=0,
            candidate_count=len(statuses),
            succeeded_count=sum(
                status is LiquidityDatasetMarketStatus.SUCCEEDED for _, status in statuses
            ),
            skipped_count=sum(
                status is LiquidityDatasetMarketStatus.SKIPPED_COMPLETE for _, status in statuses
            ),
            failed_count=failed_count,
        ),
    )


def _bootstrap_cli_args(database: Path) -> list[str]:
    return [
        "bootstrap-liquidity-dataset",
        "--quote-currency",
        "KRW",
        "--universe-as-of",
        "2026-08-23T12:01:00Z",
        "--window-end",
        "2026-08-23T12:00:00Z",
        "--maximum-snapshot-age-seconds",
        "3600",
        "--database",
        str(database),
    ]


@pytest.mark.parametrize(
    ("result", "expected_exit"),
    [
        (
            _bootstrap_cli_result(
                (
                    ("KRW-BTC", LiquidityDatasetMarketStatus.SUCCEEDED),
                    ("KRW-ETH", LiquidityDatasetMarketStatus.SKIPPED_COMPLETE),
                )
            ),
            0,
        ),
        (
            _bootstrap_cli_result(
                (
                    ("KRW-BTC", LiquidityDatasetMarketStatus.FAILED),
                    ("KRW-ETH", LiquidityDatasetMarketStatus.SUCCEEDED),
                )
            ),
            1,
        ),
    ],
)
def test_bootstrap_liquidity_dataset_outputs_sorted_jsonl_and_exit_code(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    result: LiquidityDatasetBootstrapResult,
    expected_exit: int,
) -> None:
    async def fake_bootstrap(*args, **kwargs):
        return result

    monkeypatch.setattr(
        "cassandra_trader.cli.bootstrap_liquidity_dataset",
        fake_bootstrap,
    )

    exit_code = main(_bootstrap_cli_args(tmp_path / "candles.db"))
    records = [json.loads(line) for line in capsys.readouterr().out.splitlines()]

    assert exit_code == expected_exit
    assert [record["market"] for record in records[:-1]] == ["KRW-BTC", "KRW-ETH"]
    assert all(record["type"] == "market" for record in records[:-1])
    assert records[-1]["type"] == "summary"
    assert records[-1]["candidate_count"] == 2
    assert records[-1]["failed_count"] == expected_exit
    assert records[-1]["coverage_as_of"] == "2026-08-23T12:03:00Z"
    assert "final_coverage_as_of" not in records[-1]
    assert records[0]["initial_slot_summary"]["quote_volume"] == "0"


def test_bootstrap_liquidity_dataset_unresolved_outputs_summary_only_and_exit_two(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    result = LiquidityDatasetBootstrapResult(
        markets=(),
        summary=LiquidityDatasetBootstrapSummary(
            status=LiquidityDatasetBootstrapStatus.UNRESOLVED,
            reasons=(MarketEligibilityReason.UNIVERSE_UNRESOLVED,),
            quote_currency="KRW",
            window_start=_BOOTSTRAP_WINDOW_START,
            window_end=_BOOTSTRAP_WINDOW_END,
            universe_as_of=_BOOTSTRAP_UNIVERSE_AS_OF,
            resume_as_of=_BOOTSTRAP_RESUME_AS_OF,
            final_coverage_as_of=_BOOTSTRAP_RESUME_AS_OF,
            maximum_snapshot_age=timedelta(hours=1),
            catalog_policy_name=MARKET_ELIGIBILITY_POLICY_NAME,
            catalog_policy_version=MARKET_ELIGIBILITY_POLICY_VERSION,
            snapshot_id=None,
            observed_at=None,
            available_at=None,
            content_hash=None,
            catalog_candidate_count=0,
            catalog_excluded_count=0,
            candidate_count=0,
            succeeded_count=0,
            skipped_count=0,
            failed_count=0,
        ),
    )

    async def fake_bootstrap(*args, **kwargs):
        return result

    monkeypatch.setattr(
        "cassandra_trader.cli.bootstrap_liquidity_dataset",
        fake_bootstrap,
    )

    exit_code = main(_bootstrap_cli_args(tmp_path / "candles.db"))
    records = [json.loads(line) for line in capsys.readouterr().out.splitlines()]

    assert exit_code == 2
    assert records == [
        {
            **records[0],
            "type": "summary",
            "status": "UNRESOLVED",
            "candidate_count": 0,
        }
    ]


@pytest.mark.parametrize(
    "arguments",
    [
        ["--quote-currency", "BTC"],
        ["--universe-as-of", "2026-08-23T11:59:00Z"],
        ["--window-end", "2026-08-23T12:00:00.000001Z"],
        ["--maximum-snapshot-age-seconds", "0"],
    ],
)
def test_bootstrap_liquidity_dataset_rejects_invalid_cli_input_before_service(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    arguments: list[str],
) -> None:
    called = False

    async def unexpected_bootstrap(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("invalid CLI input must not reach the service")

    monkeypatch.setattr(
        "cassandra_trader.cli.bootstrap_liquidity_dataset",
        unexpected_bootstrap,
    )
    command = _bootstrap_cli_args(tmp_path / "candles.db")
    option = arguments[0]
    index = command.index(option)
    command[index : index + 2] = arguments

    with pytest.raises(SystemExit) as error:
        main(command)

    assert error.value.code == 2
    assert called is False


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


def test_capture_universe_outputs_deterministic_snapshot_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    observed_at = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
    available_at = observed_at + timedelta(microseconds=500_000)
    database = tmp_path / "catalog.db"

    async def fake_capture(database_path: str | Path) -> MarketCatalogCaptureResult:
        assert database_path == database
        return MarketCatalogCaptureResult(
            snapshot_id="snapshot-1",
            quote_currency="KRW",
            observed_at=observed_at,
            available_at=available_at,
            market_count=2,
            content_hash="a" * 64,
            pairs=(
                TradingPair("KRW-ADA", "에이다", "Cardano", True),
                TradingPair("KRW-BTC", "비트코인", "Bitcoin", False),
            ),
        )

    monkeypatch.setattr(
        "cassandra_trader.cli.capture_krw_market_catalog",
        fake_capture,
    )

    exit_code = main(["capture-universe", "--database", str(database)])

    assert exit_code == 0
    assert capsys.readouterr().out == (
        '{"type":"snapshot","status":"SUCCEEDED","snapshot_id":"snapshot-1",'
        '"quote_currency":"KRW","observed_at":"2026-08-23T12:00:00Z",'
        '"available_at":"2026-08-23T12:00:00.500000Z","market_count":2,'
        '"content_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}\n'
    )


def test_capture_universe_failure_returns_nonzero(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    async def fake_capture(database_path: str | Path) -> MarketCatalogCaptureResult:
        raise TimeoutError("timed out")

    monkeypatch.setattr(
        "cassandra_trader.cli.capture_krw_market_catalog",
        fake_capture,
    )

    exit_code = main(["capture-universe", "--database", str(tmp_path / "catalog.db")])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert captured.out == ""
    assert json.loads(captured.err) == {
        "type": "snapshot",
        "status": "FAILED",
        "error": {"type": "TimeoutError", "message": "timed out"},
    }


def test_universe_as_of_outputs_sorted_json_lines_without_network(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    as_of = datetime(2026, 8, 23, 13, 0, tzinfo=UTC)
    observed_at = as_of - timedelta(hours=1)
    available_at = observed_at + timedelta(seconds=1)
    database = tmp_path / "catalog.db"

    def fake_resolve(
        quote_currency: str,
        cutoff: datetime,
        database_path: str | Path,
    ) -> MarketUniverseResult:
        assert quote_currency == "KRW"
        assert cutoff == as_of
        assert database_path == database
        return MarketUniverseResult(
            status=MarketUniverseStatus.AVAILABLE,
            quote_currency="KRW",
            as_of=as_of,
            snapshot_id="snapshot-1",
            observed_at=observed_at,
            available_at=available_at,
            content_hash="b" * 64,
            pairs=(
                TradingPair("KRW-ADA", "에이다", "Cardano", True),
                TradingPair("KRW-BTC", "비트코인", "Bitcoin", False),
            ),
        )

    def unexpected_network(*args, **kwargs):
        raise AssertionError("universe-as-of must not call the network")

    monkeypatch.setattr("cassandra_trader.cli.resolve_market_universe_as_of", fake_resolve)
    monkeypatch.setattr("cassandra_trader.cli.fetch_krw_trading_pairs", unexpected_network)
    monkeypatch.setattr("cassandra_trader.cli.capture_krw_market_catalog", unexpected_network)

    exit_code = main(
        [
            "universe-as-of",
            "--as-of",
            "2026-08-23T13:00:00Z",
            "--database",
            str(database),
        ]
    )

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert exit_code == 0
    assert lines[:2] == [
        {
            "type": "market",
            "market": "KRW-ADA",
            "korean_name": "에이다",
            "english_name": "Cardano",
            "is_warning": True,
        },
        {
            "type": "market",
            "market": "KRW-BTC",
            "korean_name": "비트코인",
            "english_name": "Bitcoin",
            "is_warning": False,
        },
    ]
    assert lines[2] == {
        "type": "summary",
        "status": "AVAILABLE",
        "quote_currency": "KRW",
        "as_of": "2026-08-23T13:00:00Z",
        "snapshot_id": "snapshot-1",
        "observed_at": "2026-08-23T12:00:00Z",
        "available_at": "2026-08-23T12:00:01Z",
        "content_hash": "b" * 64,
        "market_count": 2,
    }


def test_universe_as_of_unresolved_outputs_summary_only_and_exit_two(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    as_of = datetime(2026, 8, 23, 11, 59, 59, tzinfo=UTC)

    def fake_resolve(*args, **kwargs) -> MarketUniverseResult:
        return MarketUniverseResult(
            status=MarketUniverseStatus.UNRESOLVED,
            quote_currency="KRW",
            as_of=as_of,
            snapshot_id=None,
            observed_at=None,
            available_at=None,
            content_hash=None,
            pairs=(),
        )

    monkeypatch.setattr("cassandra_trader.cli.resolve_market_universe_as_of", fake_resolve)

    exit_code = main(
        [
            "universe-as-of",
            "--as-of",
            "2026-08-23T11:59:59Z",
            "--database",
            str(tmp_path / "catalog.db"),
        ]
    )

    assert exit_code == 2
    assert [json.loads(line) for line in capsys.readouterr().out.splitlines()] == [
        {
            "type": "summary",
            "status": "UNRESOLVED",
            "quote_currency": "KRW",
            "as_of": "2026-08-23T11:59:59Z",
            "snapshot_id": None,
            "observed_at": None,
            "available_at": None,
            "content_hash": None,
            "market_count": 0,
        }
    ]


@pytest.mark.parametrize(
    "arguments",
    [
        ["--as-of", "2026-08-23T12:00:00"],
        ["--as-of", "2026-08-23T21:00:00+09:00"],
        ["--quote-currency", "ＫＲＷ", "--as-of", "2026-08-23T12:00:00Z"],
    ],
)
def test_universe_as_of_rejects_invalid_arguments_before_query(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    arguments: list[str],
) -> None:
    called = False

    def unexpected_query(*args, **kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(
        "cassandra_trader.cli.resolve_market_universe_as_of",
        unexpected_query,
    )

    with pytest.raises(SystemExit):
        main(
            [
                "universe-as-of",
                *arguments,
                "--database",
                str(tmp_path / "catalog.db"),
            ]
        )

    assert called is False


def test_eligible_markets_outputs_sorted_resolved_json_lines_without_network(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    as_of = datetime(2026, 8, 23, 13, 0, tzinfo=UTC)
    observed_at = as_of - timedelta(minutes=30)
    available_at = observed_at + timedelta(seconds=1)
    database = tmp_path / "catalog.db"

    def fake_evaluate(
        quote_currency: str,
        cutoff: datetime,
        database_path: str | Path,
        maximum_snapshot_age: timedelta,
    ) -> MarketEligibilityResult:
        assert quote_currency == "KRW"
        assert cutoff == as_of
        assert database_path == database
        assert maximum_snapshot_age == timedelta(seconds=3600)
        universe = MarketUniverseResult(
            status=MarketUniverseStatus.AVAILABLE,
            quote_currency="KRW",
            as_of=as_of,
            snapshot_id="snapshot-1",
            observed_at=observed_at,
            available_at=available_at,
            content_hash="c" * 64,
            pairs=(
                TradingPair("KRW-ADA", "에이다", "Cardano", True),
                TradingPair("KRW-BTC", "비트코인", "Bitcoin", False),
            ),
        )
        return evaluate_market_eligibility(universe, maximum_snapshot_age)

    def unexpected_network(*args, **kwargs):
        raise AssertionError("eligible-markets must not call the network")

    monkeypatch.setattr(
        "cassandra_trader.cli.evaluate_market_eligibility_as_of",
        fake_evaluate,
    )
    monkeypatch.setattr("cassandra_trader.cli.fetch_krw_trading_pairs", unexpected_network)
    monkeypatch.setattr("cassandra_trader.cli.capture_krw_market_catalog", unexpected_network)

    exit_code = main(
        [
            "eligible-markets",
            "--as-of",
            "2026-08-23T13:00:00Z",
            "--maximum-snapshot-age-seconds",
            "3600",
            "--database",
            str(database),
        ]
    )

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert exit_code == 0
    assert lines == [
        {
            "type": "market",
            "market": "KRW-ADA",
            "status": "EXCLUDED",
            "reasons": ["WARNING"],
            "is_warning": True,
        },
        {
            "type": "market",
            "market": "KRW-BTC",
            "status": "ELIGIBLE",
            "reasons": [],
            "is_warning": False,
        },
        {
            "type": "summary",
            "status": "RESOLVED",
            "reasons": [],
            "policy_name": MARKET_ELIGIBILITY_POLICY_NAME,
            "policy_version": MARKET_ELIGIBILITY_POLICY_VERSION,
            "quote_currency": "KRW",
            "as_of": "2026-08-23T13:00:00Z",
            "maximum_snapshot_age_seconds": 3600,
            "snapshot_id": "snapshot-1",
            "observed_at": "2026-08-23T12:30:00Z",
            "available_at": "2026-08-23T12:30:01Z",
            "content_hash": "c" * 64,
            "snapshot_age_seconds": 1800,
            "candidate_count": 2,
            "eligible_count": 1,
            "excluded_count": 1,
            "unresolved_count": 0,
        },
    ]


def test_eligible_markets_unresolved_outputs_summary_only_and_exit_two(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    as_of = datetime(2026, 8, 23, 11, 59, 59, tzinfo=UTC)

    def fake_evaluate(*args, **kwargs) -> MarketEligibilityResult:
        universe = MarketUniverseResult(
            status=MarketUniverseStatus.UNRESOLVED,
            quote_currency="KRW",
            as_of=as_of,
            snapshot_id=None,
            observed_at=None,
            available_at=None,
            content_hash=None,
            pairs=(),
        )
        return evaluate_market_eligibility(universe, timedelta(seconds=60))

    monkeypatch.setattr(
        "cassandra_trader.cli.evaluate_market_eligibility_as_of",
        fake_evaluate,
    )

    exit_code = main(
        [
            "eligible-markets",
            "--as-of",
            "2026-08-23T11:59:59Z",
            "--maximum-snapshot-age-seconds",
            "60",
            "--database",
            str(tmp_path / "catalog.db"),
        ]
    )

    assert exit_code == 2
    assert [json.loads(line) for line in capsys.readouterr().out.splitlines()] == [
        {
            "type": "summary",
            "status": "UNRESOLVED",
            "reasons": ["UNIVERSE_UNRESOLVED"],
            "policy_name": MARKET_ELIGIBILITY_POLICY_NAME,
            "policy_version": MARKET_ELIGIBILITY_POLICY_VERSION,
            "quote_currency": "KRW",
            "as_of": "2026-08-23T11:59:59Z",
            "maximum_snapshot_age_seconds": 60,
            "snapshot_id": None,
            "observed_at": None,
            "available_at": None,
            "content_hash": None,
            "snapshot_age_seconds": None,
            "candidate_count": 0,
            "eligible_count": 0,
            "excluded_count": 0,
            "unresolved_count": 0,
        }
    ]


def test_eligible_markets_stale_universe_outputs_unresolved_decisions(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    as_of = datetime(2026, 8, 23, 13, 0, tzinfo=UTC)

    def fake_evaluate(*args, **kwargs) -> MarketEligibilityResult:
        universe = MarketUniverseResult(
            status=MarketUniverseStatus.AVAILABLE,
            quote_currency="KRW",
            as_of=as_of,
            snapshot_id="snapshot-stale",
            observed_at=as_of - timedelta(hours=2),
            available_at=as_of - timedelta(hours=2) + timedelta(seconds=1),
            content_hash="d" * 64,
            pairs=(TradingPair("KRW-ADA", "에이다", "Cardano", True),),
        )
        return evaluate_market_eligibility(universe, timedelta(hours=1))

    monkeypatch.setattr(
        "cassandra_trader.cli.evaluate_market_eligibility_as_of",
        fake_evaluate,
    )

    exit_code = main(
        [
            "eligible-markets",
            "--as-of",
            "2026-08-23T13:00:00Z",
            "--maximum-snapshot-age-seconds",
            "3600",
            "--database",
            str(tmp_path / "catalog.db"),
        ]
    )

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert exit_code == 2
    assert lines[0] == {
        "type": "market",
        "market": "KRW-ADA",
        "status": "UNRESOLVED",
        "reasons": ["STALE_UNIVERSE"],
        "is_warning": True,
    }
    assert lines[1]["status"] == "UNRESOLVED"
    assert lines[1]["reasons"] == ["STALE_UNIVERSE"]
    assert lines[1]["candidate_count"] == 1
    assert lines[1]["eligible_count"] == 0
    assert lines[1]["excluded_count"] == 0
    assert lines[1]["unresolved_count"] == 1


@pytest.mark.parametrize(
    "arguments",
    [
        ["--as-of", "2026-08-23T12:00:00", "--maximum-snapshot-age-seconds", "60"],
        ["--as-of", "2026-08-23T21:00:00+09:00", "--maximum-snapshot-age-seconds", "60"],
        ["--as-of", "2026-08-23T12:00:00Z", "--maximum-snapshot-age-seconds", "0"],
        ["--as-of", "2026-08-23T12:00:00Z", "--maximum-snapshot-age-seconds", "-1"],
        [
            "--as-of",
            "2026-08-23T12:00:00Z",
            "--maximum-snapshot-age-seconds",
            "86400000000000",
        ],
    ],
)
def test_eligible_markets_rejects_invalid_arguments_before_query(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    arguments: list[str],
) -> None:
    called = False

    def unexpected_query(*args, **kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(
        "cassandra_trader.cli.evaluate_market_eligibility_as_of",
        unexpected_query,
    )

    with pytest.raises(SystemExit):
        main(
            [
                "eligible-markets",
                *arguments,
                "--database",
                str(tmp_path / "catalog.db"),
            ]
        )

    assert called is False
    assert not (tmp_path / "catalog.db").exists()


def test_market_liquidity_outputs_market_order_metrics_then_resolved_summary(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    window_end = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
    window_start = window_end - timedelta(days=30)
    as_of = window_end + timedelta(minutes=10)
    maximum_age = timedelta(hours=1)
    database = tmp_path / "liquidity.db"
    universe = MarketUniverseResult(
        status=MarketUniverseStatus.AVAILABLE,
        quote_currency="KRW",
        as_of=as_of,
        snapshot_id="snapshot-1",
        observed_at=as_of - timedelta(minutes=5),
        available_at=as_of - timedelta(minutes=4),
        content_hash="e" * 64,
        pairs=(
            TradingPair("KRW-ADA", "에이다", "Cardano", True),
            TradingPair("KRW-BTC", "비트코인", "Bitcoin", False),
            TradingPair("KRW-ETH", "이더리움", "Ethereum", False),
        ),
    )
    eligibility = evaluate_market_eligibility(universe, maximum_age)
    result = evaluate_market_liquidity(
        eligibility,
        window_end,
        (
            MinuteSlotSummary(
                market="KRW-BTC",
                start_time_utc=window_start,
                end_time_utc=window_end,
                as_of=as_of,
                expected_count=43_200,
                traded_count=1,
                no_trade_count=43_199,
                unresolved_count=0,
                quote_volume=Decimal("20.000000001"),
            ),
            MinuteSlotSummary(
                market="KRW-ETH",
                start_time_utc=window_start,
                end_time_utc=window_end,
                as_of=as_of,
                expected_count=43_200,
                traded_count=0,
                no_trade_count=43_200,
                unresolved_count=0,
                quote_volume=Decimal(0),
            ),
        ),
    )

    def fake_evaluate(
        quote_currency: str,
        requested_window_end: datetime,
        cutoff: datetime,
        database_path: str | Path,
        maximum_snapshot_age: timedelta,
    ) -> MarketLiquidityResult:
        assert quote_currency == "KRW"
        assert requested_window_end == window_end
        assert cutoff == as_of
        assert database_path == database
        assert maximum_snapshot_age == maximum_age
        return result

    monkeypatch.setattr(
        "cassandra_trader.cli.evaluate_market_liquidity_as_of",
        fake_evaluate,
    )

    exit_code = main(
        [
            "market-liquidity",
            "--quote-currency",
            "KRW",
            "--window-end",
            "2026-08-23T12:00:00Z",
            "--as-of",
            "2026-08-23T12:10:00Z",
            "--maximum-snapshot-age-seconds",
            "3600",
            "--database",
            str(database),
        ]
    )

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert exit_code == 0
    assert lines == [
        {
            "type": "market",
            "market": "KRW-BTC",
            "status": "RESOLVED",
            "reasons": [],
            "expected_count": 43_200,
            "traded_count": 1,
            "no_trade_count": 43_199,
            "unresolved_count": 0,
            "quote_volume": "20.000000001",
            "rank": 1,
        },
        {
            "type": "market",
            "market": "KRW-ETH",
            "status": "RESOLVED",
            "reasons": [],
            "expected_count": 43_200,
            "traded_count": 0,
            "no_trade_count": 43_200,
            "unresolved_count": 0,
            "quote_volume": "0",
            "rank": 2,
        },
        {
            "type": "summary",
            "status": "RESOLVED",
            "reasons": [],
            "policy_name": MARKET_LIQUIDITY_POLICY_NAME,
            "policy_version": MARKET_LIQUIDITY_POLICY_VERSION,
            "catalog_policy_name": MARKET_ELIGIBILITY_POLICY_NAME,
            "catalog_policy_version": MARKET_ELIGIBILITY_POLICY_VERSION,
            "quote_currency": "KRW",
            "window_start": "2026-07-24T12:00:00Z",
            "window_end": "2026-08-23T12:00:00Z",
            "as_of": "2026-08-23T12:10:00Z",
            "lookback_days": 30,
            "maximum_snapshot_age_seconds": 3600,
            "snapshot_id": "snapshot-1",
            "observed_at": "2026-08-23T12:05:00Z",
            "available_at": "2026-08-23T12:06:00Z",
            "content_hash": "e" * 64,
            "catalog_candidate_count": 3,
            "catalog_excluded_count": 1,
            "liquidity_candidate_count": 2,
            "liquidity_resolved_count": 2,
            "liquidity_unresolved_count": 0,
        },
    ]


def test_market_liquidity_unresolved_catalog_outputs_summary_only_and_exit_two(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    window_end = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
    as_of = window_end + timedelta(minutes=10)
    maximum_age = timedelta(hours=1)
    universe = MarketUniverseResult(
        status=MarketUniverseStatus.UNRESOLVED,
        quote_currency="KRW",
        as_of=as_of,
        snapshot_id=None,
        observed_at=None,
        available_at=None,
        content_hash=None,
        pairs=(),
    )
    result = evaluate_market_liquidity(
        evaluate_market_eligibility(universe, maximum_age),
        window_end,
        (),
    )
    monkeypatch.setattr(
        "cassandra_trader.cli.evaluate_market_liquidity_as_of",
        lambda *args, **kwargs: result,
    )

    exit_code = main(
        [
            "market-liquidity",
            "--window-end",
            "2026-08-23T12:00:00Z",
            "--as-of",
            "2026-08-23T12:10:00Z",
            "--maximum-snapshot-age-seconds",
            "3600",
            "--database",
            str(tmp_path / "liquidity.db"),
        ]
    )

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert exit_code == 2
    assert len(lines) == 1
    assert lines[0]["type"] == "summary"
    assert lines[0]["status"] == "UNRESOLVED"
    assert lines[0]["reasons"] == ["CATALOG_ELIGIBILITY_UNRESOLVED"]
    assert lines[0]["snapshot_id"] is None
    assert lines[0]["liquidity_candidate_count"] == 0


def test_market_liquidity_unresolved_data_suppresses_every_rank(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    window_end = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
    window_start = window_end - timedelta(days=30)
    as_of = window_end + timedelta(minutes=10)
    maximum_age = timedelta(hours=1)
    universe = MarketUniverseResult(
        status=MarketUniverseStatus.AVAILABLE,
        quote_currency="KRW",
        as_of=as_of,
        snapshot_id="snapshot-partial",
        observed_at=as_of - timedelta(minutes=5),
        available_at=as_of - timedelta(minutes=4),
        content_hash="d" * 64,
        pairs=(
            TradingPair("KRW-BTC", "비트코인", "Bitcoin", False),
            TradingPair("KRW-ETH", "이더리움", "Ethereum", False),
        ),
    )
    result = evaluate_market_liquidity(
        evaluate_market_eligibility(universe, maximum_age),
        window_end,
        (
            MinuteSlotSummary(
                market="KRW-BTC",
                start_time_utc=window_start,
                end_time_utc=window_end,
                as_of=as_of,
                expected_count=43_200,
                traded_count=1,
                no_trade_count=43_199,
                unresolved_count=0,
                quote_volume=Decimal("1.000000001"),
            ),
            MinuteSlotSummary(
                market="KRW-ETH",
                start_time_utc=window_start,
                end_time_utc=window_end,
                as_of=as_of,
                expected_count=43_200,
                traded_count=0,
                no_trade_count=43_199,
                unresolved_count=1,
                quote_volume=Decimal(0),
            ),
        ),
    )
    monkeypatch.setattr(
        "cassandra_trader.cli.evaluate_market_liquidity_as_of",
        lambda *args, **kwargs: result,
    )

    exit_code = main(
        [
            "market-liquidity",
            "--window-end",
            "2026-08-23T12:00:00Z",
            "--as-of",
            "2026-08-23T12:10:00Z",
            "--maximum-snapshot-age-seconds",
            "3600",
            "--database",
            str(tmp_path / "liquidity.db"),
        ]
    )

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert exit_code == 2
    assert [line["type"] for line in lines] == ["market", "market", "summary"]
    assert [line["market"] for line in lines[:-1]] == ["KRW-BTC", "KRW-ETH"]
    assert [line["rank"] for line in lines[:-1]] == [None, None]
    assert [line["quote_volume"] for line in lines[:-1]] == ["1.000000001", None]
    assert lines[-1]["status"] == "UNRESOLVED"
    assert lines[-1]["reasons"] == ["LIQUIDITY_DATA_UNRESOLVED"]


@pytest.mark.parametrize(
    "arguments",
    [
        [
            "--quote-currency",
            "USDT",
            "--window-end",
            "2026-08-23T12:00:00Z",
            "--as-of",
            "2026-08-23T12:10:00Z",
            "--maximum-snapshot-age-seconds",
            "3600",
        ],
        [
            "--window-end",
            "2026-08-23T12:00:00.000001Z",
            "--as-of",
            "2026-08-23T12:10:00Z",
            "--maximum-snapshot-age-seconds",
            "3600",
        ],
        [
            "--window-end",
            "2026-08-23T21:00:00+09:00",
            "--as-of",
            "2026-08-23T12:10:00Z",
            "--maximum-snapshot-age-seconds",
            "3600",
        ],
        [
            "--window-end",
            "0001-01-01T00:00:00Z",
            "--as-of",
            "0001-01-01T00:00:00Z",
            "--maximum-snapshot-age-seconds",
            "3600",
        ],
        [
            "--window-end",
            "2026-08-23T12:00:00Z",
            "--as-of",
            "2026-08-23T11:59:59.999999Z",
            "--maximum-snapshot-age-seconds",
            "3600",
        ],
        [
            "--window-end",
            "2026-08-23T12:00:00Z",
            "--as-of",
            "2026-08-23T12:10:00Z",
            "--maximum-snapshot-age-seconds",
            "0",
        ],
        [
            "--window-end",
            "2026-08-23T12:00:00Z",
            "--as-of",
            "2026-08-23T12:10:00Z",
            "--maximum-snapshot-age-seconds",
            "-1",
        ],
        [
            "--window-end",
            "2026-08-23T12:00:00Z",
            "--as-of",
            "2026-08-23T12:10:00Z",
            "--maximum-snapshot-age-seconds",
            "86400000000000",
        ],
    ],
)
def test_market_liquidity_rejects_invalid_arguments_before_query(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    arguments: list[str],
) -> None:
    called = False

    def unexpected_query(*args, **kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(
        "cassandra_trader.cli.evaluate_market_liquidity_as_of",
        unexpected_query,
    )

    with pytest.raises(SystemExit):
        main(
            [
                "market-liquidity",
                *arguments,
                "--database",
                str(tmp_path / "liquidity.db"),
            ]
        )

    assert called is False
    assert not (tmp_path / "liquidity.db").exists()


def test_liquidity_top_n_outputs_market_order_then_summary_with_exact_decimal(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    window_end = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
    window_start = window_end - timedelta(days=30)
    as_of = window_end + timedelta(minutes=10)
    maximum_age = timedelta(hours=1)
    database = tmp_path / "liquidity.db"
    universe = MarketUniverseResult(
        status=MarketUniverseStatus.AVAILABLE,
        quote_currency="KRW",
        as_of=as_of,
        snapshot_id="snapshot-top-n",
        observed_at=as_of - timedelta(minutes=5),
        available_at=as_of - timedelta(minutes=4),
        content_hash="c" * 64,
        pairs=(
            TradingPair("KRW-ADA", "에이다", "Cardano", False),
            TradingPair("KRW-BTC", "비트코인", "Bitcoin", False),
            TradingPair("KRW-ETH", "이더리움", "Ethereum", False),
        ),
    )
    eligibility = evaluate_market_eligibility(universe, maximum_age)
    liquidity = evaluate_market_liquidity(
        eligibility,
        window_end,
        tuple(
            MinuteSlotSummary(
                market=market,
                start_time_utc=window_start,
                end_time_utc=window_end,
                as_of=as_of,
                expected_count=43_200,
                traded_count=1,
                no_trade_count=43_199,
                unresolved_count=0,
                quote_volume=Decimal(volume),
            )
            for market, volume in (
                ("KRW-ADA", "20.000000001"),
                ("KRW-BTC", "10.5"),
                ("KRW-ETH", "1"),
            )
        ),
    )
    result = evaluate_liquidity_top_n(liquidity, 2)

    def fake_evaluate(
        quote_currency: str,
        requested_window_end: datetime,
        cutoff: datetime,
        database_path: str | Path,
        maximum_snapshot_age: timedelta,
        top_n: int,
    ) -> LiquidityTopNResult:
        assert quote_currency == "KRW"
        assert requested_window_end == window_end
        assert cutoff == as_of
        assert database_path == database
        assert maximum_snapshot_age == maximum_age
        assert top_n == 2
        return result

    monkeypatch.setattr(
        "cassandra_trader.cli.evaluate_liquidity_top_n_as_of",
        fake_evaluate,
    )

    exit_code = main(
        [
            "liquidity-top-n",
            "--quote-currency",
            "KRW",
            "--window-end",
            "2026-08-23T12:00:00Z",
            "--as-of",
            "2026-08-23T12:10:00Z",
            "--maximum-snapshot-age-seconds",
            "3600",
            "--top-n",
            "2",
            "--database",
            str(database),
        ]
    )

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert exit_code == 0
    assert lines == [
        {
            "type": "market",
            "market": "KRW-ADA",
            "status": "SELECTED",
            "reasons": [],
            "quote_volume": "20.000000001",
            "liquidity_rank": 1,
        },
        {
            "type": "market",
            "market": "KRW-BTC",
            "status": "SELECTED",
            "reasons": [],
            "quote_volume": "10.5",
            "liquidity_rank": 2,
        },
        {
            "type": "market",
            "market": "KRW-ETH",
            "status": "EXCLUDED",
            "reasons": ["OUTSIDE_TOP_N"],
            "quote_volume": "1",
            "liquidity_rank": 3,
        },
        {
            "type": "summary",
            "status": "RESOLVED",
            "reasons": [],
            "policy_name": LIQUIDITY_TOP_N_POLICY_NAME,
            "policy_version": LIQUIDITY_TOP_N_POLICY_VERSION,
            "liquidity_policy_name": MARKET_LIQUIDITY_POLICY_NAME,
            "liquidity_policy_version": MARKET_LIQUIDITY_POLICY_VERSION,
            "catalog_policy_name": MARKET_ELIGIBILITY_POLICY_NAME,
            "catalog_policy_version": MARKET_ELIGIBILITY_POLICY_VERSION,
            "top_n": 2,
            "quote_currency": "KRW",
            "window_start": "2026-07-24T12:00:00Z",
            "window_end": "2026-08-23T12:00:00Z",
            "as_of": "2026-08-23T12:10:00Z",
            "lookback_days": 30,
            "maximum_snapshot_age_seconds": 3600,
            "snapshot_id": "snapshot-top-n",
            "observed_at": "2026-08-23T12:05:00Z",
            "available_at": "2026-08-23T12:06:00Z",
            "content_hash": "c" * 64,
            "catalog_candidate_count": 3,
            "catalog_excluded_count": 0,
            "liquidity_candidate_count": 3,
            "liquidity_resolved_count": 3,
            "liquidity_unresolved_count": 0,
            "selected_count": 2,
            "excluded_count": 1,
            "unresolved_count": 0,
        },
    ]


def test_liquidity_top_n_unresolved_suppresses_all_selection_and_exits_two(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    window_end = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
    window_start = window_end - timedelta(days=30)
    as_of = window_end + timedelta(minutes=10)
    maximum_age = timedelta(hours=1)
    universe = MarketUniverseResult(
        status=MarketUniverseStatus.AVAILABLE,
        quote_currency="KRW",
        as_of=as_of,
        snapshot_id="snapshot-partial",
        observed_at=as_of - timedelta(minutes=5),
        available_at=as_of - timedelta(minutes=4),
        content_hash="d" * 64,
        pairs=(
            TradingPair("KRW-BTC", "비트코인", "Bitcoin", False),
            TradingPair("KRW-ETH", "이더리움", "Ethereum", False),
        ),
    )
    liquidity = evaluate_market_liquidity(
        evaluate_market_eligibility(universe, maximum_age),
        window_end,
        (
            MinuteSlotSummary(
                market="KRW-BTC",
                start_time_utc=window_start,
                end_time_utc=window_end,
                as_of=as_of,
                expected_count=43_200,
                traded_count=1,
                no_trade_count=43_199,
                unresolved_count=0,
                quote_volume=Decimal("1.000000001"),
            ),
            MinuteSlotSummary(
                market="KRW-ETH",
                start_time_utc=window_start,
                end_time_utc=window_end,
                as_of=as_of,
                expected_count=43_200,
                traded_count=0,
                no_trade_count=43_199,
                unresolved_count=1,
                quote_volume=Decimal(0),
            ),
        ),
    )
    result = evaluate_liquidity_top_n(liquidity, 1)
    monkeypatch.setattr(
        "cassandra_trader.cli.evaluate_liquidity_top_n_as_of",
        lambda *args, **kwargs: result,
    )

    exit_code = main(
        [
            "liquidity-top-n",
            "--quote-currency",
            "KRW",
            "--window-end",
            "2026-08-23T12:00:00Z",
            "--as-of",
            "2026-08-23T12:10:00Z",
            "--maximum-snapshot-age-seconds",
            "3600",
            "--top-n",
            "1",
            "--database",
            str(tmp_path / "liquidity.db"),
        ]
    )

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert exit_code == 2
    assert [line["type"] for line in lines] == ["market", "market", "summary"]
    assert [line["status"] for line in lines[:-1]] == ["UNRESOLVED", "UNRESOLVED"]
    assert [line["reasons"] for line in lines[:-1]] == [
        ["LIQUIDITY_UNRESOLVED"],
        ["LIQUIDITY_UNRESOLVED"],
    ]
    assert [line["quote_volume"] for line in lines[:-1]] == ["1.000000001", None]
    assert [line["liquidity_rank"] for line in lines[:-1]] == [None, None]
    assert lines[-1]["status"] == "UNRESOLVED"
    assert lines[-1]["selected_count"] == 0
    assert lines[-1]["excluded_count"] == 0
    assert lines[-1]["unresolved_count"] == 2
    assert lines[-1]["liquidity_candidate_count"] == 2
    assert lines[-1]["liquidity_resolved_count"] == 1
    assert lines[-1]["liquidity_unresolved_count"] == 1
    assert lines[-1]["selected_count"] == sum(line["status"] == "SELECTED" for line in lines[:-1])
    assert lines[-1]["excluded_count"] == sum(line["status"] == "EXCLUDED" for line in lines[:-1])
    assert lines[-1]["unresolved_count"] == sum(
        line["status"] == "UNRESOLVED" for line in lines[:-1]
    )


@pytest.mark.parametrize(
    "arguments",
    [
        ["--top-n", "1", "--quote-currency", "USDT"],
        ["--top-n", "0"],
        ["--top-n", "-1"],
        ["--top-n", "not-an-integer"],
        ["--top-n", "1", "--window-end", "2026-08-23T12:00:00.000001Z"],
        ["--top-n", "1", "--as-of", "2026-08-23T21:10:00+09:00"],
        ["--top-n", "1", "--as-of", "2026-08-23T11:59:59.999999Z"],
        ["--top-n", "1", "--maximum-snapshot-age-seconds", "0"],
        ["--top-n", "1", "--maximum-snapshot-age-seconds", "86400000000000"],
    ],
)
def test_liquidity_top_n_rejects_invalid_input_before_service_or_database(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    arguments: list[str],
) -> None:
    called = False

    def unexpected_query(*args, **kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(
        "cassandra_trader.cli.evaluate_liquidity_top_n_as_of",
        unexpected_query,
    )
    values = {
        "--quote-currency": "KRW",
        "--top-n": "1",
        "--window-end": "2026-08-23T12:00:00Z",
        "--as-of": "2026-08-23T12:10:00Z",
        "--maximum-snapshot-age-seconds": "3600",
    }

    for flag, value in zip(arguments[::2], arguments[1::2], strict=True):
        values[flag] = value

    argv = ["liquidity-top-n"]
    for flag, value in values.items():
        argv.extend((flag, value))
    argv.extend(("--database", str(tmp_path / "liquidity.db")))

    with pytest.raises(SystemExit) as error:
        main(argv)

    assert error.value.code == 2
    assert called is False
    assert not (tmp_path / "liquidity.db").exists()


def test_liquidity_top_n_requires_top_n_before_service_or_database(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    called = False

    def unexpected_query(*args, **kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(
        "cassandra_trader.cli.evaluate_liquidity_top_n_as_of",
        unexpected_query,
    )
    database = tmp_path / "liquidity.db"

    with pytest.raises(SystemExit) as error:
        main(
            [
                "liquidity-top-n",
                "--quote-currency",
                "KRW",
                "--window-end",
                "2026-08-23T12:00:00Z",
                "--as-of",
                "2026-08-23T12:10:00Z",
                "--maximum-snapshot-age-seconds",
                "3600",
                "--database",
                str(database),
            ]
        )

    assert error.value.code == 2
    assert called is False
    assert not database.exists()


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
            resolved_count=240,
            traded_count=239,
            no_trade_count=1,
            unresolved_count=0,
            unresolved_minute_starts=(),
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
        "expected_count=240\n"
        "resolved_count=240\n"
        "traded_count=239\n"
        "no_trade_count=1\n"
        "unresolved_count=0\n"
    )


def test_four_hour_bar_no_trade_output(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    start = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)

    def fake_derive_four_hour_bar(*args, **kwargs) -> FourHourBarResult:
        return FourHourBarResult(
            status=FourHourBarStatus.NO_TRADE,
            market="KRW-BTC",
            start_time_utc=start,
            end_time_utc=start + timedelta(hours=4),
            expected_count=240,
            resolved_count=240,
            traded_count=0,
            no_trade_count=240,
            unresolved_count=0,
            unresolved_minute_starts=(),
            candle=None,
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

    assert exit_code == 0
    assert capsys.readouterr().out == (
        "status=NO_TRADE\n"
        "market=KRW-BTC\n"
        "start_time_utc=2026-08-22T12:00:00Z\n"
        "end_time_utc=2026-08-22T16:00:00Z\n"
        "expected_count=240\n"
        "resolved_count=240\n"
        "traded_count=0\n"
        "no_trade_count=240\n"
        "unresolved_count=0\n"
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
            resolved_count=239,
            traded_count=238,
            no_trade_count=1,
            unresolved_count=1,
            unresolved_minute_starts=(missing,),
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
        "actual_count=238\n"
        "expected_count=240\n"
        "missing_count=1\n"
        "resolved_count=239\n"
        "traded_count=238\n"
        "no_trade_count=1\n"
        "unresolved_count=1\n"
        "unresolved_minute_starts=2026-08-22T12:42:00Z\n"
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
