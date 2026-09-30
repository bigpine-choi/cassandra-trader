"""Independent evidence, reproducibility and non-destructive export checks."""

import json
import sqlite3
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal, localcontext
from pathlib import Path

import pytest

from cassandra_trader.adapters.storage.sqlite import (
    MarketCatalogSnapshotMemberInput,
    MarketCatalogSnapshotStatus,
    MinuteCandleBackfillRunStatus,
    RawMinuteCandleInput,
    SqliteMinuteCandleRepository,
)
from cassandra_trader.apps.research_dataset import (
    export_research_dataset,
    inspect_research_dataset,
)
from cassandra_trader.domain.candle import MinuteCandle
from cassandra_trader.domain.market import TradingPair
from cassandra_trader.domain.research_dataset import ResearchDatasetRequest

_START = datetime(2026, 8, 22, 8, tzinfo=UTC)
_END = _START + timedelta(hours=4)
_CUTOFF = _START + timedelta(hours=16)
_FILES = ("bars.jsonl", "lineage.jsonl", "readiness.json", "manifest.json")


def _time(value: datetime) -> str:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _request(
    *,
    markets: tuple[str, ...] = ("KRW-BTC",),
    end: datetime = _END,
    as_of: datetime = _CUTOFF,
) -> ResearchDatasetRequest:
    return ResearchDatasetRequest(
        markets=markets,
        start_time_utc=_START,
        end_time_utc=end,
        as_of=as_of,
        maximum_snapshot_age=timedelta(hours=1),
    )


def _ingest(
    repository: SqliteMinuteCandleRepository,
    *,
    batch_id: str,
    start: datetime = _START,
    market: str = "KRW-BTC",
    available_at: datetime = _CUTOFF,
    close: str = "10.5",
    base_volume: str = "1",
    quote_volume: str = "10.5",
) -> None:
    candle = MinuteCandle(
        market=market,
        start_time_utc=start,
        end_time_utc=start + timedelta(minutes=1),
        open=Decimal("10"),
        high=Decimal("12"),
        low=Decimal("9"),
        close=Decimal(close),
        base_volume=Decimal(base_volume),
        quote_volume=Decimal(quote_volume),
    )
    repository.ingest_batch(
        batch_id=batch_id,
        source="upbit",
        endpoint="/v1/candles/minutes/1",
        request_parameters={"market": market, "count": 1},
        received_at=available_at,
        ingested_at=available_at,
        records=(
            RawMinuteCandleInput(
                response_order=0,
                market=market,
                source_candle_time=_time(start),
                raw_payload={
                    "market": market,
                    "candle_date_time_utc": _time(start),
                    "trade_price": close,
                },
                candle=candle,
            ),
        ),
    )


def _evidence(
    repository: SqliteMinuteCandleRepository,
    *,
    run_id: str,
    market: str = "KRW-BTC",
    start: datetime = _START,
    end: datetime = _END,
    available_at: datetime = _CUTOFF,
    status: MinuteCandleBackfillRunStatus = MinuteCandleBackfillRunStatus.SUCCEEDED,
) -> None:
    failed = status is MinuteCandleBackfillRunStatus.FAILED
    repository.record_minute_candle_backfill_run(
        run_id=run_id,
        market=market,
        requested_start=start,
        requested_end=end,
        observed_at=end,
        available_at=available_at,
        status=status,
        error_type="TimeoutError" if failed else None,
        error_message="bounded fetch failed" if failed else None,
    )


def _rows(directory: Path, name: str) -> list[dict]:
    return [json.loads(line) for line in (directory / name).read_text().splitlines()]


def _payloads(directory: Path) -> dict[str, bytes]:
    return {name: (directory / name).read_bytes() for name in _FILES}


def test_later_revision_cannot_rewrite_frozen_dataset_or_lineage(tmp_path: Path) -> None:
    database = tmp_path / "source.db"
    before = tmp_path / "before"
    replay = tmp_path / "replay"
    later = tmp_path / "later"

    with SqliteMinuteCandleRepository(database) as repository:
        _evidence(repository, run_id="complete-at-cutoff")
        _ingest(repository, batch_id="revision-1")
        original = repository.list_minute_candle_revisions("KRW-BTC", _START)[0]

    export_research_dataset(_request(), database, before)
    with SqliteMinuteCandleRepository(database) as repository:
        _ingest(
            repository,
            batch_id="revision-2",
            close="11",
            available_at=_CUTOFF + timedelta(microseconds=1),
        )

    export_research_dataset(_request(), database, replay)
    export_research_dataset(_request(as_of=_CUTOFF + timedelta(microseconds=1)), database, later)

    assert _payloads(before) == _payloads(replay)
    assert _rows(before, "bars.jsonl")[0]["candle"]["close"] == "10.5"
    assert _rows(later, "bars.jsonl")[0]["candle"]["close"] == "11"
    lineage = _rows(replay, "lineage.jsonl")[0]["canonical_minutes"]
    assert len(lineage) == 1
    assert lineage[0]["revision"] == 1
    assert lineage[0]["available_at"] == _time(_CUTOFF)
    assert lineage[0]["source_raw_record_id"] == original.source_raw_record_id
    assert lineage[0]["content_sha256"] == original.content_sha256


def test_failed_or_not_yet_available_evidence_never_implies_no_trade(tmp_path: Path) -> None:
    database = tmp_path / "source.db"
    early = tmp_path / "early"
    late = tmp_path / "late"
    after_cutoff = _CUTOFF + timedelta(microseconds=1)

    with SqliteMinuteCandleRepository(database) as repository:
        _ingest(repository, batch_id="observed-minute")
        _evidence(
            repository,
            run_id="failed-at-cutoff",
            status=MinuteCandleBackfillRunStatus.FAILED,
        )
        _evidence(repository, run_id="later-success", available_at=after_cutoff)

    export_research_dataset(_request(), database, early)
    export_research_dataset(_request(as_of=after_cutoff), database, late)

    early_bar = _rows(early, "bars.jsonl")[0]
    late_bar = _rows(late, "bars.jsonl")[0]
    assert early_bar["status"] == "INCOMPLETE"
    assert early_bar["candle"] is None
    assert early_bar["no_trade_count"] == 0
    assert early_bar["unresolved_count"] == 239
    assert _rows(early, "lineage.jsonl")[0]["no_trade_evidence"] == []
    assert late_bar["status"] == "COMPLETE"
    assert late_bar["no_trade_count"] == 239
    evidence = _rows(late, "lineage.jsonl")[0]["no_trade_evidence"]
    assert [item["run_id"] for item in evidence] == ["later-success"]
    assert evidence[0]["available_at"] == _time(after_cutoff)


def test_panel_retains_complete_no_trade_and_incomplete_bars(tmp_path: Path) -> None:
    database = tmp_path / "source.db"
    output = tmp_path / "panel"
    second_end = _END + timedelta(hours=4)
    third_end = second_end + timedelta(hours=4)

    with SqliteMinuteCandleRepository(database) as repository:
        _ingest(repository, batch_id="first-bar-trade")
        _evidence(repository, run_id="first-bar-coverage")
        _evidence(repository, run_id="second-bar-coverage", start=_END, end=second_end)
        _ingest(repository, batch_id="third-bar-trade", start=second_end)

    export_research_dataset(_request(end=third_end), database, output)
    bars = _rows(output, "bars.jsonl")
    readiness = json.loads((output / "readiness.json").read_text())

    assert [bar["status"] for bar in bars] == ["COMPLETE", "NO_TRADE", "INCOMPLETE"]
    assert [bar["expected_count"] for bar in bars] == [240, 240, 240]
    assert bars[1]["candle"] is None
    assert bars[2]["candle"] is None
    assert bars[1]["no_trade_count"] == 240
    assert len(_rows(output, "lineage.jsonl")) == 3
    market = readiness["markets"][0]
    assert market["complete_bars"] == 1
    assert market["no_trade_bars"] == 1
    assert market["incomplete_bars"] == 1
    assert market["expected_minutes"] == 720
    assert market["unresolved_minutes"] == 239
    assert readiness["data_complete"] is False
    assert readiness["evidence_grade"] == "NON_PIT_EXPLORATORY"
    assert readiness["confirmatory_ready"] is False


def test_export_is_independent_of_path_clock_decimal_context_and_does_not_write_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "source.db"
    first = tmp_path / "first"
    second = tmp_path / "second"

    with SqliteMinuteCandleRepository(database) as repository:
        _evidence(repository, run_id="complete")
        for index in range(2):
            _ingest(
                repository,
                batch_id=f"precise-{index}",
                start=_START + timedelta(minutes=index),
                base_volume="1.23456789",
                quote_volume="11.123456789",
            )

    source_bytes = database.read_bytes()
    with monkeypatch.context() as patched, localcontext() as context:
        patched.setattr(time, "time", lambda: 1.0)
        context.prec = 6
        export_research_dataset(_request(), database, first)
    with monkeypatch.context() as patched, localcontext() as context:
        patched.setattr(time, "time", lambda: 9000000000.0)
        context.prec = 80
        export_research_dataset(_request(), database, second)

    assert database.read_bytes() == source_bytes
    assert _payloads(first) == _payloads(second)
    candle = _rows(first, "bars.jsonl")[0]["candle"]
    assert Decimal(candle["base_volume"]) == Decimal("2.46913578")
    assert Decimal(candle["quote_volume"]) == Decimal("22.246913578")


def test_missing_source_is_not_created_by_inspection_or_export(tmp_path: Path) -> None:
    database = tmp_path / "absent-parent" / "missing.db"
    output = tmp_path / "should-not-exist"

    with pytest.raises((FileNotFoundError, sqlite3.OperationalError)):
        inspect_research_dataset(_request(), database)
    with pytest.raises((FileNotFoundError, sqlite3.OperationalError)):
        export_research_dataset(_request(), database, output)

    assert not database.exists()
    assert not database.parent.exists()
    assert not output.exists()


def test_existing_export_directory_is_never_replaced(tmp_path: Path) -> None:
    database = tmp_path / "source.db"
    output = tmp_path / "existing"
    output.mkdir()
    sentinel = output / "manifest.json"
    sentinel.write_bytes(b"keep the previous experiment\n")
    with SqliteMinuteCandleRepository(database):
        pass
    original_source = database.read_bytes()

    with pytest.raises(FileExistsError):
        export_research_dataset(_request(), database, output)

    assert list(output.iterdir()) == [sentinel]
    assert sentinel.read_bytes() == b"keep the previous experiment\n"
    assert database.read_bytes() == original_source


def test_explicit_warning_and_catalog_absent_markets_remain_visible_without_rank_claims(
    tmp_path: Path,
) -> None:
    database = tmp_path / "source.db"
    output = tmp_path / "selected-panel"
    markets = ("KRW-ADA", "KRW-BTC", "KRW-ETH")

    with SqliteMinuteCandleRepository(database) as repository:
        members = tuple(
            MarketCatalogSnapshotMemberInput(
                response_order=index,
                pair=TradingPair(market, market, market, market == "KRW-ETH"),
                raw_payload={
                    "market": market,
                    "market_event": {"warning": market == "KRW-ETH"},
                },
            )
            for index, market in enumerate(("KRW-BTC", "KRW-ETH"))
        )
        repository.record_market_catalog_snapshot(
            snapshot_id="current-only",
            source="upbit",
            endpoint="/v1/market/all",
            request_parameters={"is_details": True},
            quote_currency="KRW",
            observed_at=_CUTOFF - timedelta(seconds=1),
            available_at=_CUTOFF,
            status=MarketCatalogSnapshotStatus.SUCCEEDED,
            members=members,
        )
        for market in markets:
            _evidence(repository, run_id=f"coverage-{market}", market=market)

    export_research_dataset(_request(markets=markets), database, output)
    report = json.loads((output / "readiness.json").read_text())
    by_market = {row["market"]: row for row in report["markets"]}

    assert [bar["market"] for bar in _rows(output, "bars.jsonl")] == list(markets)
    assert report["selection_scope"] == "EXPLICIT_MARKETS"
    assert report["data_complete"] is True
    assert report["catalog_ready"] is False
    assert report["confirmatory_ready"] is False
    assert report["evidence_grade"] == "NON_PIT_EXPLORATORY"
    assert by_market["KRW-BTC"]["catalog_status"] == "ELIGIBLE"
    assert by_market["KRW-ETH"]["catalog_status"] != "ELIGIBLE"
    assert by_market["KRW-ADA"]["catalog_status"] != "ELIGIBLE"
    assert by_market["KRW-ETH"]["blockers"]
    assert by_market["KRW-ADA"]["blockers"]
    assert all("rank" not in row for row in report["markets"])
    assert report["catalog"]["snapshot_id"] == "current-only"


def test_missing_intervals_merge_across_bar_boundary_but_not_across_observed_minutes(
    tmp_path: Path,
) -> None:
    database = tmp_path / "source.db"
    end = _END + timedelta(hours=4)
    middle = _END + timedelta(minutes=1)
    last = end - timedelta(minutes=1)
    with SqliteMinuteCandleRepository(database) as repository:
        for index, start in enumerate((_START, middle, last)):
            _ingest(repository, batch_id=f"isolated-{index}", start=start)

    report = inspect_research_dataset(_request(end=end), database)
    market = report["markets"][0]

    assert market["traded_minutes"] == 3
    assert market["unresolved_minutes"] == 477
    assert market["no_trade_minutes"] == 0
    assert market["missing_intervals"] == [
        {
            "start_time_utc": _time(_START + timedelta(minutes=1)),
            "end_time_utc": _time(middle),
        },
        {
            "start_time_utc": _time(middle + timedelta(minutes=1)),
            "end_time_utc": _time(last),
        },
    ]
