"""Application-level catalog eligibility tests."""

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from cassandra_trader.adapters.storage.sqlite import (
    MarketCatalogSnapshotMemberInput,
    MarketCatalogSnapshotStatus,
    SqliteMinuteCandleRepository,
)
from cassandra_trader.apps.market_eligibility import evaluate_market_eligibility_as_of
from cassandra_trader.domain.market import TradingPair
from cassandra_trader.domain.market_eligibility import (
    MarketEligibilityDecisionStatus,
    MarketEligibilityEvaluationStatus,
    MarketEligibilityReason,
)

_BASE_TIME = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)


def _record_snapshot(
    repository: SqliteMinuteCandleRepository,
    snapshot_id: str,
    *,
    observed_at: datetime,
    available_at: datetime,
    warning: bool,
    status: MarketCatalogSnapshotStatus = MarketCatalogSnapshotStatus.SUCCEEDED,
) -> None:
    pair = TradingPair("KRW-BTC", "비트코인", "Bitcoin", warning)
    members = (
        MarketCatalogSnapshotMemberInput(
            response_order=0,
            pair=pair,
            raw_payload={"market": pair.code, "market_event": {"warning": warning}},
        ),
    )
    repository.record_market_catalog_snapshot(
        snapshot_id=snapshot_id,
        source="upbit",
        endpoint="/v1/market/all",
        request_parameters={"is_details": True},
        quote_currency="KRW",
        observed_at=observed_at,
        available_at=available_at,
        status=status,
        members=members if status is MarketCatalogSnapshotStatus.SUCCEEDED else (),
        error_type=None if status is MarketCatalogSnapshotStatus.SUCCEEDED else "TimeoutError",
        error_message=None if status is MarketCatalogSnapshotStatus.SUCCEEDED else "timed out",
    )


def test_point_in_time_warning_changes_only_after_later_snapshot_is_available(
    tmp_path: Path,
) -> None:
    database = tmp_path / "catalog.db"

    with SqliteMinuteCandleRepository(database) as repository:
        _record_snapshot(
            repository,
            "snapshot-normal",
            observed_at=_BASE_TIME,
            available_at=_BASE_TIME + timedelta(seconds=1),
            warning=False,
        )
        _record_snapshot(
            repository,
            "snapshot-warning",
            observed_at=_BASE_TIME + timedelta(minutes=10),
            available_at=_BASE_TIME + timedelta(minutes=10, microseconds=500_000),
            warning=True,
        )

    before_warning = evaluate_market_eligibility_as_of(
        "KRW",
        _BASE_TIME + timedelta(minutes=10),
        database,
        timedelta(hours=1),
    )
    after_warning = evaluate_market_eligibility_as_of(
        "KRW",
        _BASE_TIME + timedelta(minutes=10, microseconds=500_000),
        database,
        timedelta(hours=1),
    )

    assert before_warning.snapshot_id == "snapshot-normal"
    assert before_warning.decisions[0].status is MarketEligibilityDecisionStatus.ELIGIBLE
    assert after_warning.snapshot_id == "snapshot-warning"
    assert after_warning.decisions[0].status is MarketEligibilityDecisionStatus.EXCLUDED
    assert after_warning.decisions[0].reasons == (MarketEligibilityReason.WARNING,)


def test_future_and_failed_snapshots_do_not_replace_previous_success(tmp_path: Path) -> None:
    database = tmp_path / "catalog.db"
    as_of = _BASE_TIME + timedelta(minutes=5)

    with SqliteMinuteCandleRepository(database) as repository:
        _record_snapshot(
            repository,
            "snapshot-success",
            observed_at=_BASE_TIME,
            available_at=_BASE_TIME + timedelta(seconds=1),
            warning=False,
        )
        _record_snapshot(
            repository,
            "snapshot-failed",
            observed_at=_BASE_TIME + timedelta(minutes=4),
            available_at=_BASE_TIME + timedelta(minutes=4, seconds=1),
            warning=False,
            status=MarketCatalogSnapshotStatus.FAILED,
        )
        _record_snapshot(
            repository,
            "snapshot-future",
            observed_at=_BASE_TIME + timedelta(minutes=10),
            available_at=_BASE_TIME + timedelta(minutes=10, seconds=1),
            warning=True,
        )

    result = evaluate_market_eligibility_as_of(
        "KRW",
        as_of,
        database,
        timedelta(hours=1),
    )

    assert result.status is MarketEligibilityEvaluationStatus.RESOLVED
    assert result.snapshot_id == "snapshot-success"
    assert result.eligible_count == 1


def test_before_first_snapshot_is_unresolved_without_network_fallback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    database = tmp_path / "catalog.db"

    with SqliteMinuteCandleRepository(database):
        pass

    def unexpected_network(*args, **kwargs):
        raise AssertionError("eligibility lookup must not use the network")

    monkeypatch.setattr(
        "cassandra_trader.apps.market_universe.AsyncUpbit",
        unexpected_network,
    )

    result = evaluate_market_eligibility_as_of(
        "KRW",
        _BASE_TIME,
        database,
        timedelta(hours=1),
    )

    assert result.status is MarketEligibilityEvaluationStatus.UNRESOLVED
    assert result.reasons == (MarketEligibilityReason.UNIVERSE_UNRESOLVED,)
    assert result.decisions == ()


@pytest.mark.parametrize("maximum_age", [timedelta(0), timedelta(seconds=-1)])
def test_invalid_maximum_age_is_rejected_before_database_query(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    maximum_age: timedelta,
) -> None:
    called = False

    def unexpected_resolver(*args, **kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(
        "cassandra_trader.apps.market_eligibility.resolve_market_universe_as_of",
        unexpected_resolver,
    )

    with pytest.raises(ValueError, match="maximum_snapshot_age must be positive"):
        evaluate_market_eligibility_as_of(
            "KRW",
            _BASE_TIME,
            tmp_path / "missing.db",
            maximum_age,
        )

    assert called is False
    assert not (tmp_path / "missing.db").exists()


def test_application_service_does_not_write_eligibility_results(tmp_path: Path) -> None:
    database = tmp_path / "catalog.db"

    with SqliteMinuteCandleRepository(database) as repository:
        _record_snapshot(
            repository,
            "snapshot-1",
            observed_at=_BASE_TIME,
            available_at=_BASE_TIME + timedelta(seconds=1),
            warning=False,
        )

    evaluate_market_eligibility_as_of(
        "KRW",
        _BASE_TIME + timedelta(minutes=1),
        database,
        timedelta(hours=1),
    )

    with sqlite3.connect(database) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }

    assert not any("eligibility" in table for table in tables)
