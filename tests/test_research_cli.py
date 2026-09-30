"""Exercise the public research commands against real temporary SQLite files."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from cassandra_trader.adapters.storage.sqlite import (
    MarketCatalogSnapshotMemberInput,
    MarketCatalogSnapshotStatus,
    MinuteCandleBackfillRunStatus,
    SqliteMinuteCandleRepository,
)
from cassandra_trader.cli import main
from cassandra_trader.domain.market import TradingPair

START = datetime(2026, 8, 24, 0, tzinfo=UTC)
END = START + timedelta(hours=4)
CUTOFF = END + timedelta(minutes=1)


def _arguments(database: Path, *, output: Path | None = None) -> list[str]:
    args = [
        "research-readiness" if output is None else "export-research-dataset",
        " krw-btc ",
        "KRW-BTC",
        "--start",
        START.isoformat(),
        "--end",
        END.isoformat(),
        "--as-of",
        CUTOFF.isoformat(),
        "--maximum-snapshot-age-seconds",
        "3600",
        "--database",
        str(database),
    ]
    if output is not None:
        args.extend(("--output", str(output)))
    return args


def _database(database: Path, *, resolved: bool, catalog: bool = True) -> None:
    with SqliteMinuteCandleRepository(database) as repository:
        if catalog:
            repository.record_market_catalog_snapshot(
                snapshot_id="catalog",
                source="upbit",
                endpoint="/v1/market/all",
                request_parameters={"is_details": True},
                quote_currency="KRW",
                observed_at=END,
                available_at=CUTOFF,
                status=MarketCatalogSnapshotStatus.SUCCEEDED,
                members=(
                    MarketCatalogSnapshotMemberInput(
                        response_order=0,
                        pair=TradingPair("KRW-BTC", "비트코인", "Bitcoin", False),
                        raw_payload={"market": "KRW-BTC"},
                    ),
                ),
            )
        if resolved:
            repository.record_minute_candle_backfill_run(
                run_id="complete-no-trade",
                market="KRW-BTC",
                requested_start=START,
                requested_end=END,
                observed_at=END,
                available_at=CUTOFF,
                status=MinuteCandleBackfillRunStatus.SUCCEEDED,
            )


def test_ready_cli_reports_requested_scope_without_claiming_model_readiness(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    database = tmp_path / "source.db"
    _database(database, resolved=True)
    before = database.read_bytes()
    assert main(_arguments(database)) == 0
    captured = capsys.readouterr()
    report = json.loads(captured.out)
    assert captured.err == ""
    assert report["parameters"]["markets"] == ["KRW-BTC"]
    assert report["selection_scope"] == "EXPLICIT_MARKETS"
    assert report["data_complete"] is True
    assert report["catalog_ready"] is True
    assert report["confirmatory_ready"] is False
    assert report["evidence_grade"] == "NON_PIT_EXPLORATORY"
    assert database.read_bytes() == before


def test_incomplete_cli_export_is_retained_for_diagnosis_and_returns_two(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    database = tmp_path / "source.db"
    output = tmp_path / "dataset"
    _database(database, resolved=False)
    assert main(_arguments(database, output=output)) == 2
    captured = capsys.readouterr()
    manifest = json.loads(captured.out)
    assert captured.err == ""
    assert manifest["data_complete"] is False
    assert set(path.name for path in output.iterdir()) == {
        "manifest.json",
        "readiness.json",
        "bars.jsonl",
        "lineage.jsonl",
    }
    bars = [json.loads(line) for line in (output / "bars.jsonl").read_text().splitlines()]
    assert len(bars) == 1
    assert bars[0]["status"] == "INCOMPLETE"
    assert bars[0]["candle"] is None


def test_complete_cli_export_does_not_replace_existing_output(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    database = tmp_path / "source.db"
    output = tmp_path / "dataset"
    _database(database, resolved=True)
    assert main(_arguments(database, output=output)) == 0
    capsys.readouterr()
    before = {p.name: p.read_bytes() for p in output.iterdir()}
    assert main(_arguments(database, output=output)) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert json.loads(captured.err)["error"]["type"] == "FileExistsError"
    assert {p.name: p.read_bytes() for p in output.iterdir()} == before


def test_missing_catalog_is_distinct_from_complete_candle_coverage(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    database = tmp_path / "source.db"
    _database(database, resolved=True, catalog=False)
    assert main(_arguments(database)) == 2
    report = json.loads(capsys.readouterr().out)
    assert report["data_complete"] is True
    assert report["catalog_ready"] is False


def test_missing_database_fails_without_creating_database_or_dataset(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    database = tmp_path / "absent" / "source.db"
    output = tmp_path / "dataset"
    assert main(_arguments(database, output=output)) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert json.loads(captured.err)["type"] == "error"
    assert not database.parent.exists()
    assert not output.exists()


def test_non_ascii_market_cannot_become_an_ascii_code_during_cli_normalization(
    tmp_path: Path,
) -> None:
    database = tmp_path / "absent" / "source.db"
    args = _arguments(database)
    args[1] = "KRW-ß"
    with pytest.raises(SystemExit) as error:
        main(args)
    assert error.value.code == 2
    assert not database.parent.exists()


@pytest.mark.parametrize(
    "flag,value",
    [
        ("--end", START.isoformat()),
        ("--as-of", START.isoformat()),
        ("--start", "2026-08-24T00:01:00Z"),
        ("--start", "2026-08-24T00:00:00+09:00"),
        ("--maximum-snapshot-age-seconds", "0"),
    ],
)
def test_invalid_research_parameters_are_rejected_before_io(
    tmp_path: Path, flag: str, value: str
) -> None:
    database = tmp_path / "absent" / "source.db"
    args = _arguments(database)
    args[args.index(flag) + 1] = value
    with pytest.raises(SystemExit) as error:
        main(args)
    assert error.value.code == 2
    assert not database.parent.exists()
