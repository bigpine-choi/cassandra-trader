import hashlib
import json
import sqlite3
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

_START = datetime(2025, 1, 1, tzinfo=UTC)
_RECEIVED = _START + timedelta(minutes=2)
_AVAILABLE = _RECEIVED + timedelta(seconds=1)


def _ingest(repository: SqliteMinuteCandleRepository, batch: str, close: str = "100"):
    candle = MinuteCandle(
        market="KRW-BTC",
        start_time_utc=_START,
        end_time_utc=_START + timedelta(minutes=1),
        open=Decimal("100"),
        high=Decimal("110"),
        low=Decimal("90"),
        close=Decimal(close),
        base_volume=Decimal("1"),
        quote_volume=Decimal(close),
    )
    return repository.ingest_batch(
        batch_id=batch,
        source="upbit",
        endpoint="/v1/candles/minutes/1",
        request_parameters={"market": "KRW-BTC", "count": 1},
        received_at=_RECEIVED,
        ingested_at=_AVAILABLE,
        records=(
            RawMinuteCandleInput(
                response_order=0,
                market="KRW-BTC",
                source_candle_time="2025-01-01T00:00:00Z",
                raw_payload={"trade_price": close},
                candle=candle,
            ),
        ),
    )


def test_read_only_missing_database_does_not_create_file_or_parent(tmp_path: Path) -> None:
    database = tmp_path / "missing" / "candles.db"

    with pytest.raises(sqlite3.OperationalError):
        SqliteMinuteCandleRepository(database, read_only=True)

    assert not database.parent.exists()


def test_read_only_reads_special_character_path_without_migration_or_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "candles ?# 한글.db"
    with SqliteMinuteCandleRepository(database) as writer:
        _ingest(writer, "initial")
    original = database.read_bytes()

    def forbidden_migration(self):
        pytest.fail("read-only access must not migrate")

    monkeypatch.setattr(SqliteMinuteCandleRepository, "_migrate", forbidden_migration)
    with SqliteMinuteCandleRepository(database, read_only=True) as reader:
        candles = reader.list_minute_candles_as_of(
            "KRW-BTC", _START, _START + timedelta(minutes=1), _AVAILABLE
        )
        assert [stored.candle.close for stored in candles] == [Decimal("100")]
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            reader._connection.execute("CREATE TABLE forbidden (value INTEGER)")

    assert database.read_bytes() == original


@pytest.mark.parametrize("versions", [(), (1,), (1, 2, 4), (1, 2, 3, 4, 5)])
def test_read_only_rejects_unsupported_schema_without_migrating(
    tmp_path: Path, versions: tuple[int, ...]
) -> None:
    database = tmp_path / "unsupported.db"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT)"
        )
        connection.executemany(
            "INSERT INTO schema_migrations VALUES (?, '2025-01-01T00:00:00Z')",
            [(version,) for version in versions],
        )
    original = database.read_bytes()

    with pytest.raises(RuntimeError, match="fully migrated schema version"):
        SqliteMinuteCandleRepository(database, read_only=True)

    assert database.read_bytes() == original


def test_invalid_database_releases_connection_on_constructor_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "empty.db"
    with sqlite3.connect(database):
        pass
    connections: list[sqlite3.Connection] = []
    connect = sqlite3.connect

    def tracked_connect(*args, **kwargs):
        connection = connect(*args, **kwargs)
        connections.append(connection)
        return connection

    monkeypatch.setattr(sqlite3, "connect", tracked_connect)
    with pytest.raises(RuntimeError, match="supported database schema"):
        SqliteMinuteCandleRepository(database, read_only=True)

    assert len(connections) == 1
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        connections[0].execute("SELECT 1")


def test_constructor_pins_consistent_snapshot_across_concurrent_writer_commits(
    tmp_path: Path,
) -> None:
    database = tmp_path / "candles.db"
    with SqliteMinuteCandleRepository(database) as writer:
        _ingest(writer, "initial")
        with SqliteMinuteCandleRepository(database, read_only=True) as reader:
            # No reader candle/evidence query has run yet. The constructor's
            # schema read must already have pinned the snapshot.
            _ingest(writer, "revision", "105")
            writer.record_minute_candle_backfill_run(
                run_id="later-evidence",
                market="KRW-BTC",
                requested_start=_START,
                requested_end=_START + timedelta(minutes=1),
                observed_at=_RECEIVED,
                available_at=_AVAILABLE,
                status=MinuteCandleBackfillRunStatus.SUCCEEDED,
            )

            stored = reader.get_current_minute_candle("KRW-BTC", _START)
            assert stored is not None
            assert stored.revision == 1
            assert stored.candle.close == Decimal("100")
            assert (
                reader.list_minute_candle_backfill_runs(
                    "KRW-BTC", _START, _START + timedelta(minutes=1), _AVAILABLE
                )
                == ()
            )
            provenance = reader.get_minute_candle_source_provenance(stored.source_raw_record_id)
            assert provenance is not None
            assert provenance.batch_id == "initial"
            revised = writer.get_current_minute_candle("KRW-BTC", _START)
            assert revised is not None
            assert reader.list_minute_candle_source_provenance(
                (stored.source_raw_record_id, revised.source_raw_record_id)
            ) == (provenance,)

            # A rejected public write must not end the read transaction.
            with pytest.raises(sqlite3.OperationalError, match="read-only"):
                _ingest(reader, "forbidden", "110")
            assert reader.get_current_minute_candle("KRW-BTC", _START) == stored

        with SqliteMinuteCandleRepository(database, read_only=True) as later_reader:
            revised = later_reader.get_current_minute_candle("KRW-BTC", _START)
            assert revised is not None
            assert revised.revision == 2
            assert revised.candle.close == Decimal("105")
            assert (
                len(
                    later_reader.list_minute_candle_backfill_runs(
                        "KRW-BTC", _START, _START + timedelta(minutes=1), _AVAILABLE
                    )
                )
                == 1
            )


def test_provenance_preserves_raw_hash_batch_metadata_and_absent_lookup(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"
    with SqliteMinuteCandleRepository(database) as writer:
        _ingest(writer, "initial")
    with SqliteMinuteCandleRepository(database, read_only=True) as reader:
        stored = reader.get_current_minute_candle("KRW-BTC", _START)
        assert stored is not None
        lineage = reader.get_minute_candle_source_provenance(stored.source_raw_record_id)
        assert lineage is not None
        assert lineage.raw_record_id == stored.source_raw_record_id
        assert lineage.batch_id == "initial"
        assert lineage.source == "upbit"
        assert lineage.endpoint == "/v1/candles/minutes/1"
        assert json.loads(lineage.request_parameters_json) == {"market": "KRW-BTC", "count": 1}
        assert lineage.received_at == _RECEIVED
        assert lineage.ingested_at == _AVAILABLE
        assert lineage.response_order == 0
        assert lineage.market == "KRW-BTC"
        assert lineage.source_candle_time == _START
        assert lineage.payload_sha256 == hashlib.sha256(b'{"trade_price":"100"}').hexdigest()
        assert reader.get_minute_candle_source_provenance(999) is None


def test_bulk_provenance_sorts_deduplicates_and_omits_absent_records(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"
    with SqliteMinuteCandleRepository(database) as writer:
        _ingest(writer, "first", "100")
        _ingest(writer, "second", "101")
        _ingest(writer, "third", "102")
    with SqliteMinuteCandleRepository(database, read_only=True) as reader:
        ids = tuple(
            stored.source_raw_record_id
            for stored in reader.list_minute_candle_revisions("KRW-BTC", _START)
        )
        records = reader.list_minute_candle_source_provenance((ids[2], ids[0], 999, ids[2], ids[1]))
        assert tuple(record.raw_record_id for record in records) == ids
        assert tuple(record.batch_id for record in records) == ("first", "second", "third")
        assert reader.list_minute_candle_source_provenance(()) == ()
        # Exceed even older SQLite's 999-variable limit without dropping rows.
        assert reader.list_minute_candle_source_provenance(tuple(range(1, 1002))) == records


@pytest.mark.parametrize("record_id", [0, -1, True, "1"])
def test_provenance_rejects_invalid_record_ids(tmp_path: Path, record_id: object) -> None:
    with SqliteMinuteCandleRepository(tmp_path / "candles.db") as repository:
        with pytest.raises(ValueError, match="positive integer"):
            repository.get_minute_candle_source_provenance(record_id)


def test_context_exit_on_error_closes_reader_and_explicit_close_is_idempotent(
    tmp_path: Path,
) -> None:
    database = tmp_path / "candles.db"
    with SqliteMinuteCandleRepository(database):
        pass
    with pytest.raises(ValueError, match="abort export"):
        with SqliteMinuteCandleRepository(database, read_only=True) as reader:
            raise ValueError("abort export")

    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        reader.get_current_minute_candle("KRW-BTC", _START)
    reader.close()


def test_read_only_flag_must_be_boolean_before_creating_database(tmp_path: Path) -> None:
    database = tmp_path / "missing" / "candles.db"
    with pytest.raises(ValueError, match="read_only must be a boolean"):
        SqliteMinuteCandleRepository(database, read_only="yes")
    assert not database.parent.exists()
