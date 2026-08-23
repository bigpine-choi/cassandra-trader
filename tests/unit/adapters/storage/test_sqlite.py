import hashlib
import json
import sqlite3
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from cassandra_trader.adapters.storage.sqlite import (
    MinuteCandleBackfillRunStatus,
    RawMinuteCandleInput,
    SqliteMinuteCandleRepository,
)
from cassandra_trader.domain.candle import MinuteCandle

_START = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
_RECEIVED = datetime(2025, 1, 1, 12, 2, tzinfo=UTC)


def _candle(
    *,
    market: str = "KRW-BTC",
    close: str = "105",
    open_price: str = "100",
    high: str = "110",
    low: str = "90",
    base_volume: str = "1.5",
    quote_volume: str = "150",
) -> MinuteCandle:
    return MinuteCandle(
        market=market,
        start_time_utc=_START,
        end_time_utc=_START + timedelta(minutes=1),
        open=Decimal(open_price),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        base_volume=Decimal(base_volume),
        quote_volume=Decimal(quote_volume),
    )


def _record(
    *,
    candle: MinuteCandle | None = None,
    response_order: int = 0,
    raw_market: str = "KRW-BTC",
    raw_close: str = "105.00000000",
) -> RawMinuteCandleInput:
    payload = {
        "market": raw_market,
        "candle_date_time_utc": "2025-01-01T12:00:00",
        "candle_date_time_kst": "2025-01-01T21:00:00",
        "opening_price": "100.00000000",
        "high_price": "110.00000000",
        "low_price": "90.00000000",
        "trade_price": raw_close,
        "timestamp": 1735732800123,
        "candle_acc_trade_price": "150.00000000",
        "candle_acc_trade_volume": "1.50000000",
    }

    return RawMinuteCandleInput(
        response_order=response_order,
        market=raw_market,
        source_candle_time="2025-01-01T12:00:00Z",
        raw_payload=payload,
        candle=candle or _candle(close=Decimal(raw_close).__str__()),
    )


def _ingest(
    repository: SqliteMinuteCandleRepository,
    *,
    batch_id: str,
    records: tuple[RawMinuteCandleInput, ...],
    received_at: datetime = _RECEIVED,
    ingested_at: datetime | None = None,
):
    return repository.ingest_batch(
        batch_id=batch_id,
        source="upbit",
        endpoint="/v1/candles/minutes/1",
        request_parameters={"market": "KRW-BTC", "count": len(records)},
        received_at=received_at,
        ingested_at=ingested_at or received_at + timedelta(seconds=1),
        records=records,
    )


def _count(database: Path, table: str) -> int:
    with sqlite3.connect(database) as connection:
        return connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def _create_v1_database(database: Path) -> None:
    with sqlite3.connect(database) as connection:
        connection.executescript(
            """
            CREATE TABLE schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL
            );
            CREATE TABLE raw_ingestion_batches (
                batch_id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                endpoint TEXT NOT NULL,
                request_parameters_json TEXT NOT NULL,
                received_at TEXT NOT NULL,
                ingested_at TEXT NOT NULL
            );
            CREATE TABLE raw_minute_candles (
                raw_record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id TEXT NOT NULL REFERENCES raw_ingestion_batches(batch_id),
                response_order INTEGER NOT NULL,
                market TEXT NOT NULL,
                source_candle_time TEXT NOT NULL,
                raw_json TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL
            );
            CREATE TABLE canonical_minute_candles (
                canonical_record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                market TEXT NOT NULL,
                start_time_utc TEXT NOT NULL,
                end_time_utc TEXT NOT NULL,
                open TEXT NOT NULL,
                high TEXT NOT NULL,
                low TEXT NOT NULL,
                close TEXT NOT NULL,
                base_volume TEXT NOT NULL,
                quote_volume TEXT NOT NULL,
                revision INTEGER NOT NULL,
                content_sha256 TEXT NOT NULL,
                source_raw_record_id INTEGER NOT NULL REFERENCES raw_minute_candles(raw_record_id),
                available_at TEXT NOT NULL
            );
            INSERT INTO schema_migrations VALUES (1, '2025-01-01T00:00:00Z');
            INSERT INTO raw_ingestion_batches VALUES (
                'legacy-batch', 'upbit', '/v1/candles/minutes/1', '{}',
                '2025-01-01T12:02:00Z', '2025-01-01T12:02:01Z'
            );
            INSERT INTO raw_minute_candles VALUES (
                1, 'legacy-batch', 0, 'KRW-BTC', '2025-01-01T12:00:00Z', '{}', 'raw-hash'
            );
            INSERT INTO canonical_minute_candles VALUES (
                1, 'KRW-BTC', '2025-01-01T12:00:00Z', '2025-01-01T12:01:00Z',
                '100', '110', '90', '105', '1.5', '150', 1, 'canonical-hash', 1,
                '2025-01-01T12:02:01Z'
            );
            """
        )


def _record_for_candle(candle: MinuteCandle, response_order: int) -> RawMinuteCandleInput:
    source_time = candle.start_time_utc.isoformat().replace("+00:00", "Z")

    return RawMinuteCandleInput(
        response_order=response_order,
        market=candle.market,
        source_candle_time=source_time,
        raw_payload={
            "market": candle.market,
            "candle_date_time_utc": source_time.removesuffix("Z"),
            "opening_price": str(candle.open),
            "high_price": str(candle.high),
            "low_price": str(candle.low),
            "trade_price": str(candle.close),
            "candle_acc_trade_volume": str(candle.base_volume),
            "candle_acc_trade_price": str(candle.quote_volume),
        },
        candle=candle,
    )


def test_stores_first_raw_and_canonical_batch(tmp_path: Path) -> None:
    database = tmp_path / "nested" / "cassandra.db"
    record = _record()

    with SqliteMinuteCandleRepository(database) as repository:
        result = _ingest(repository, batch_id="batch-1", records=(record,))

    assert database.exists()
    assert result.raw_count == 1
    assert result.new_canonical_count == 1
    assert result.revised_canonical_count == 0
    assert result.unchanged_canonical_count == 0
    assert _count(database, "schema_migrations") == 3
    assert _count(database, "raw_ingestion_batches") == 1
    assert _count(database, "raw_minute_candles") == 1
    assert _count(database, "canonical_minute_candles") == 1

    with sqlite3.connect(database) as connection:
        source_candle_time, raw_json, payload_sha256 = connection.execute(
            "SELECT source_candle_time, raw_json, payload_sha256 FROM raw_minute_candles"
        ).fetchone()

    assert source_candle_time == "2025-01-01T12:00:00Z"
    assert json.loads(raw_json) == record.raw_payload
    assert json.loads(raw_json)["timestamp"] == 1735732800123
    assert payload_sha256 == hashlib.sha256(raw_json.encode()).hexdigest()


def test_available_at_uses_ingested_at_after_validation(tmp_path: Path) -> None:
    database = tmp_path / "cassandra.db"
    ingested_at = _RECEIVED + timedelta(seconds=30)

    with SqliteMinuteCandleRepository(database) as repository:
        _ingest(
            repository,
            batch_id="batch-1",
            records=(_record(),),
            received_at=_RECEIVED,
            ingested_at=ingested_at,
        )
        stored = repository.get_current_minute_candle("KRW-BTC", _START)

    assert stored is not None
    assert stored.available_at == ingested_at
    with sqlite3.connect(database) as connection:
        stored_text = connection.execute(
            "SELECT available_at FROM canonical_minute_candles"
        ).fetchone()[0]
    assert stored_text == "2025-01-01T12:02:30.000000Z"


def test_rejects_received_at_after_ingested_at(tmp_path: Path) -> None:
    database = tmp_path / "cassandra.db"

    with SqliteMinuteCandleRepository(database) as repository:
        with pytest.raises(ValueError, match="received_at"):
            _ingest(
                repository,
                batch_id="batch-1",
                records=(_record(),),
                received_at=_RECEIVED,
                ingested_at=_RECEIVED - timedelta(seconds=1),
            )

    assert _count(database, "raw_ingestion_batches") == 0


def test_reopening_database_does_not_repeat_migration(tmp_path: Path) -> None:
    database = tmp_path / "cassandra.db"

    with SqliteMinuteCandleRepository(database):
        pass

    with SqliteMinuteCandleRepository(database):
        pass

    with sqlite3.connect(database) as connection:
        versions = connection.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall()

    assert versions == [(1,), (2,), (3,)]


def test_existing_v1_database_preserves_canonical_data_when_v2_and_v3_are_applied(
    tmp_path: Path,
) -> None:
    database = tmp_path / "legacy.db"
    _create_v1_database(database)

    with SqliteMinuteCandleRepository(database) as repository:
        stored = repository.get_current_minute_candle("KRW-BTC", _START)

    assert stored is not None
    assert stored.revision == 1
    assert stored.candle.close == Decimal("105")
    assert _count(database, "canonical_minute_candles") == 1
    assert _count(database, "minute_candle_backfill_runs") == 0
    with sqlite3.connect(database) as connection:
        versions = connection.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall()
    assert versions == [(1,), (2,), (3,)]


def test_backfill_run_evidence_enforces_status_contract_and_is_append_only(
    tmp_path: Path,
) -> None:
    database = tmp_path / "cassandra.db"
    end = _START + timedelta(minutes=3)

    with SqliteMinuteCandleRepository(database) as repository:
        repository.record_minute_candle_backfill_run(
            run_id="run-success",
            market="KRW-BTC",
            requested_start=_START,
            requested_end=end,
            observed_at=_RECEIVED,
            available_at=_RECEIVED + timedelta(seconds=1),
            status=MinuteCandleBackfillRunStatus.SUCCEEDED,
        )
        runs = repository.list_minute_candle_backfill_runs(
            "KRW-BTC",
            _START,
            end,
            _RECEIVED + timedelta(minutes=1),
        )

        with pytest.raises(ValueError, match="SUCCEEDED"):
            repository.record_minute_candle_backfill_run(
                run_id="bad-success",
                market="KRW-BTC",
                requested_start=_START,
                requested_end=end,
                observed_at=_RECEIVED,
                status=MinuteCandleBackfillRunStatus.SUCCEEDED,
                error_type="TimeoutError",
            )

        with pytest.raises(ValueError, match="FAILED"):
            repository.record_minute_candle_backfill_run(
                run_id="bad-failure",
                market="KRW-BTC",
                requested_start=_START,
                requested_end=end,
                observed_at=_RECEIVED,
                status=MinuteCandleBackfillRunStatus.FAILED,
            )

        with pytest.raises(ValueError, match="observed_at"):
            repository.record_minute_candle_backfill_run(
                run_id="time-travel",
                market="KRW-BTC",
                requested_start=_START,
                requested_end=end,
                observed_at=_RECEIVED + timedelta(seconds=1),
                available_at=_RECEIVED,
                status=MinuteCandleBackfillRunStatus.SUCCEEDED,
            )

    assert len(runs) == 1
    assert runs[0].run_id == "run-success"
    assert runs[0].status is MinuteCandleBackfillRunStatus.SUCCEEDED
    assert runs[0].error_type is None
    assert runs[0].error_message is None

    with sqlite3.connect(database) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint"):
            connection.execute(
                """
                INSERT INTO minute_candle_backfill_runs (
                    run_id, market, requested_start, requested_end,
                    observed_at, available_at, status, error_type, error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "invalid-db-time",
                    "KRW-BTC",
                    "2025-01-01T12:00:00Z",
                    "2025-01-01T12:03:00Z",
                    "2025-01-01T12:02:01.000000Z",
                    "2025-01-01T12:02:00.000000Z",
                    "SUCCEEDED",
                    None,
                    None,
                ),
            )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute(
                "UPDATE minute_candle_backfill_runs SET market = ? WHERE run_id = ?",
                ("KRW-XRP", "run-success"),
            )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute(
                "DELETE FROM minute_candle_backfill_runs WHERE run_id = ?",
                ("run-success",),
            )


def test_replaying_same_recorded_batch_is_idempotent(tmp_path: Path) -> None:
    database = tmp_path / "cassandra.db"
    record = _record()

    with SqliteMinuteCandleRepository(database) as repository:
        first = _ingest(repository, batch_id="batch-1", records=(record,))
        replay = _ingest(repository, batch_id="batch-1", records=(record,))

    assert first.new_canonical_count == 1
    assert replay.batch_replayed is True
    assert replay.new_canonical_count == 0
    assert _count(database, "raw_ingestion_batches") == 1
    assert _count(database, "raw_minute_candles") == 1
    assert _count(database, "canonical_minute_candles") == 1


def test_new_batch_preserves_same_raw_payload_without_new_revision(tmp_path: Path) -> None:
    database = tmp_path / "cassandra.db"
    record = _record()

    with SqliteMinuteCandleRepository(database) as repository:
        _ingest(repository, batch_id="batch-1", records=(record,))
        observed_again = _ingest(
            repository,
            batch_id="batch-2",
            records=(record,),
            received_at=_RECEIVED + timedelta(minutes=1),
        )

    assert observed_again.raw_count == 1
    assert observed_again.unchanged_canonical_count == 1
    assert observed_again.new_canonical_count == 0
    assert observed_again.revised_canonical_count == 0
    assert _count(database, "raw_ingestion_batches") == 2
    assert _count(database, "raw_minute_candles") == 2
    assert _count(database, "canonical_minute_candles") == 1


def test_changed_content_adds_revision_and_preserves_history(tmp_path: Path) -> None:
    database = tmp_path / "cassandra.db"

    with SqliteMinuteCandleRepository(database) as repository:
        _ingest(repository, batch_id="batch-1", records=(_record(),))
        changed = _ingest(
            repository,
            batch_id="batch-2",
            records=(_record(candle=_candle(close="106"), raw_close="106.00000000"),),
            received_at=_RECEIVED + timedelta(minutes=1),
        )
        revisions = repository.list_minute_candle_revisions("KRW-BTC", _START)

    assert changed.revised_canonical_count == 1
    assert [revision.revision for revision in revisions] == [1, 2]
    assert [revision.candle.close for revision in revisions] == [Decimal("105"), Decimal("106")]
    assert revisions[0].source_raw_record_id != revisions[1].source_raw_record_id


def test_current_query_returns_highest_revision(tmp_path: Path) -> None:
    database = tmp_path / "cassandra.db"

    with SqliteMinuteCandleRepository(database) as repository:
        _ingest(repository, batch_id="batch-1", records=(_record(),))
        _ingest(
            repository,
            batch_id="batch-2",
            records=(_record(candle=_candle(close="106"), raw_close="106.00000000"),),
            received_at=_RECEIVED + timedelta(minutes=1),
        )
        current = repository.get_current_minute_candle("KRW-BTC", _START)

    assert current is not None
    assert current.revision == 2
    assert current.candle.close == Decimal("106")
    assert current.available_at == _RECEIVED + timedelta(minutes=1, seconds=1)


def test_decimal_and_utc_round_trip(tmp_path: Path) -> None:
    database = tmp_path / "cassandra.db"
    precise = _candle(
        open_price="0.123456789012345678901",
        high="0.223456789012345678901",
        low="0.1",
        close="0.2",
        base_volume="0.00000000000000000001",
        quote_volume="123456789.123456789123456789123456789",
    )

    with SqliteMinuteCandleRepository(database) as repository:
        _ingest(repository, batch_id="batch-1", records=(_record(candle=precise),))
        stored = repository.get_current_minute_candle("KRW-BTC", _START)

    assert stored is not None
    assert stored.candle == precise
    assert stored.candle.start_time_utc.tzinfo is UTC
    assert stored.candle.end_time_utc.tzinfo is UTC
    assert stored.available_at.tzinfo is UTC


def test_batch_rolls_back_on_mid_transaction_error(tmp_path: Path) -> None:
    database = tmp_path / "cassandra.db"
    mismatched = _record(
        candle=_candle(market="KRW-ETH"),
        response_order=1,
        raw_market="KRW-BTC",
    )

    with SqliteMinuteCandleRepository(database) as repository:
        with pytest.raises(ValueError, match="market must match"):
            _ingest(
                repository,
                batch_id="batch-rollback",
                records=(_record(), mismatched),
            )

    assert _count(database, "raw_ingestion_batches") == 0
    assert _count(database, "raw_minute_candles") == 0
    assert _count(database, "canonical_minute_candles") == 0


def test_as_of_range_query_excludes_future_revision_and_selects_latest(tmp_path: Path) -> None:
    database = tmp_path / "cassandra.db"
    first = _candle(close="105")
    revised = _candle(close="106")

    with SqliteMinuteCandleRepository(database) as repository:
        _ingest(
            repository,
            batch_id="batch-1",
            records=(_record_for_candle(first, 0),),
            received_at=_RECEIVED,
            ingested_at=_RECEIVED + timedelta(seconds=1),
        )
        _ingest(
            repository,
            batch_id="batch-2",
            records=(_record_for_candle(revised, 0),),
            received_at=_RECEIVED + timedelta(minutes=1),
            ingested_at=_RECEIVED + timedelta(minutes=1, seconds=1),
        )
        before_revision = repository.list_minute_candles_as_of(
            "KRW-BTC",
            _START,
            _START + timedelta(minutes=1),
            _RECEIVED + timedelta(seconds=30),
        )
        after_revision = repository.list_minute_candles_as_of(
            "KRW-BTC",
            _START,
            _START + timedelta(minutes=1),
            _RECEIVED + timedelta(minutes=2),
        )

    assert len(before_revision) == 1
    assert before_revision[0].revision == 1
    assert before_revision[0].candle.close == Decimal("105")
    assert len(after_revision) == 1
    assert after_revision[0].revision == 2
    assert after_revision[0].candle.close == Decimal("106")


def test_as_of_range_query_returns_minutes_in_start_order(tmp_path: Path) -> None:
    database = tmp_path / "cassandra.db"
    starts = (
        _START + timedelta(minutes=2),
        _START,
        _START + timedelta(minutes=1),
    )
    records = tuple(
        _record_for_candle(
            replace(
                _candle(),
                start_time_utc=start,
                end_time_utc=start + timedelta(minutes=1),
            ),
            response_order,
        )
        for response_order, start in enumerate(starts)
    )

    with SqliteMinuteCandleRepository(database) as repository:
        _ingest(repository, batch_id="batch-1", records=records)
        stored = repository.list_minute_candles_as_of(
            "KRW-BTC",
            _START,
            _START + timedelta(minutes=3),
            _RECEIVED + timedelta(seconds=2),
        )

    assert [item.candle.start_time_utc for item in stored] == sorted(starts)


def test_canonical_coverage_counts_logical_minutes_as_of_revision_time(
    tmp_path: Path,
) -> None:
    database = tmp_path / "cassandra.db"
    second_start = _START + timedelta(minutes=1)
    second = replace(
        _candle(),
        start_time_utc=second_start,
        end_time_utc=second_start + timedelta(minutes=1),
    )

    with SqliteMinuteCandleRepository(database) as repository:
        _ingest(
            repository,
            batch_id="batch-1",
            records=(_record_for_candle(_candle(close="105"), 0),),
            ingested_at=_RECEIVED + timedelta(seconds=1),
        )
        _ingest(
            repository,
            batch_id="batch-2",
            records=(_record_for_candle(_candle(close="106"), 0),),
            received_at=_RECEIVED + timedelta(minutes=1),
            ingested_at=_RECEIVED + timedelta(minutes=1, seconds=1),
        )
        _ingest(
            repository,
            batch_id="batch-3",
            records=(_record_for_candle(second, 0),),
            received_at=_RECEIVED + timedelta(minutes=2),
            ingested_at=_RECEIVED + timedelta(minutes=2, seconds=1),
        )

        before_second = repository.get_canonical_minute_candle_coverage(
            "KRW-BTC",
            _START,
            _START + timedelta(minutes=3),
            _RECEIVED + timedelta(minutes=2),
        )
        after_second = repository.get_canonical_minute_candle_coverage(
            "KRW-BTC",
            _START,
            _START + timedelta(minutes=3),
            _RECEIVED + timedelta(minutes=3),
        )

    assert before_second.expected_slots == 3
    assert before_second.observed_slots == 1
    assert before_second.absent_slots == 2
    assert before_second.earliest_candle_time == _START
    assert before_second.latest_candle_time == _START
    assert after_second.expected_slots == 3
    assert after_second.observed_slots == 2
    assert after_second.absent_slots == 1
    assert after_second.observed_slots + after_second.absent_slots == after_second.expected_slots
    assert after_second.earliest_candle_time == _START
    assert after_second.latest_candle_time == second_start


def test_canonical_coverage_uses_exact_half_open_range_without_synthesizing(
    tmp_path: Path,
) -> None:
    database = tmp_path / "cassandra.db"
    before = replace(
        _candle(),
        start_time_utc=_START - timedelta(minutes=1),
        end_time_utc=_START,
    )
    at_end = replace(
        _candle(),
        start_time_utc=_START + timedelta(minutes=3),
        end_time_utc=_START + timedelta(minutes=4),
    )

    with SqliteMinuteCandleRepository(database) as repository:
        _ingest(
            repository,
            batch_id="boundary-batch",
            records=(
                _record_for_candle(before, 0),
                _record_for_candle(_candle(), 1),
                _record_for_candle(at_end, 2),
            ),
        )
        coverage = repository.get_canonical_minute_candle_coverage(
            "KRW-BTC",
            _START,
            _START + timedelta(minutes=3),
            _RECEIVED + timedelta(minutes=1),
        )
        stored = repository.list_minute_candles_as_of(
            "KRW-BTC",
            _START,
            _START + timedelta(minutes=3),
            _RECEIVED + timedelta(minutes=1),
        )

    assert coverage.expected_slots == 3
    assert coverage.observed_slots == 1
    assert coverage.absent_slots == 2
    assert len(stored) == 1


def test_canonical_availability_compares_mixed_utc_precision_exactly(
    tmp_path: Path,
) -> None:
    database = tmp_path / "candles.db"
    half_second = _RECEIVED + timedelta(microseconds=500_000)

    with SqliteMinuteCandleRepository(database) as repository:
        _ingest(
            repository,
            batch_id="half-second",
            records=(_record(),),
            received_at=_RECEIVED,
            ingested_at=half_second,
        )
        before = repository.list_minute_candles_as_of(
            "KRW-BTC",
            _START,
            _START + timedelta(minutes=1),
            _RECEIVED,
        )
        at_boundary = repository.list_minute_candles_as_of(
            "KRW-BTC",
            _START,
            _START + timedelta(minutes=1),
            half_second,
        )
        coverage_before = repository.get_canonical_minute_candle_coverage(
            "KRW-BTC",
            _START,
            _START + timedelta(minutes=1),
            _RECEIVED,
        )
        coverage_at_boundary = repository.get_canonical_minute_candle_coverage(
            "KRW-BTC",
            _START,
            _START + timedelta(minutes=1),
            half_second,
        )

    assert before == ()
    assert len(at_boundary) == 1
    assert coverage_before.observed_slots == 0
    assert coverage_before.absent_slots == 1
    assert coverage_at_boundary.observed_slots == 1
    assert coverage_at_boundary.absent_slots == 0


def test_legacy_second_precision_canonical_is_available_with_fractional_as_of(
    tmp_path: Path,
) -> None:
    database = tmp_path / "legacy.db"
    _create_v1_database(database)
    as_of = datetime(2025, 1, 1, 12, 2, 1, 500_000, tzinfo=UTC)

    with SqliteMinuteCandleRepository(database) as repository:
        stored = repository.list_minute_candles_as_of(
            "KRW-BTC",
            _START,
            _START + timedelta(minutes=1),
            as_of,
        )

    assert len(stored) == 1
    assert stored[0].available_at == datetime(2025, 1, 1, 12, 2, 1, tzinfo=UTC)


def test_backfill_evidence_availability_uses_exact_half_second_boundary(
    tmp_path: Path,
) -> None:
    database = tmp_path / "candles.db"
    end = _START + timedelta(minutes=1)
    half_second = _RECEIVED + timedelta(microseconds=500_000)

    with SqliteMinuteCandleRepository(database) as repository:
        repository.record_minute_candle_backfill_run(
            run_id="half-second-evidence",
            market="KRW-BTC",
            requested_start=_START,
            requested_end=end,
            observed_at=_RECEIVED,
            available_at=half_second,
            status=MinuteCandleBackfillRunStatus.SUCCEEDED,
        )
        before = repository.list_minute_candle_backfill_runs(
            "KRW-BTC",
            _START,
            end,
            _RECEIVED,
        )
        at_boundary = repository.list_minute_candle_backfill_runs(
            "KRW-BTC",
            _START,
            end,
            half_second,
        )

    assert before == ()
    assert len(at_boundary) == 1
    assert at_boundary[0].available_at == half_second
    with sqlite3.connect(database) as connection:
        observed_text, available_text = connection.execute(
            """
            SELECT observed_at, available_at
            FROM minute_candle_backfill_runs
            WHERE run_id = 'half-second-evidence'
            """
        ).fetchone()
    assert observed_text == "2025-01-01T12:02:00.000000Z"
    assert available_text == "2025-01-01T12:02:00.500000Z"
