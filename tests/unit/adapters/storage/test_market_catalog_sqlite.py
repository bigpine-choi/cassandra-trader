import json
import sqlite3
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from cassandra_trader.adapters.storage.sqlite import (
    MarketCatalogSnapshotMemberInput,
    MarketCatalogSnapshotStatus,
    SqliteMinuteCandleRepository,
    market_catalog_content_hash,
)
from cassandra_trader.domain.market import TradingPair

_OBSERVED = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
_AVAILABLE = _OBSERVED + timedelta(microseconds=500_000)


def _member(
    market: str,
    response_order: int,
    *,
    korean_name: str | None = None,
    english_name: str | None = None,
    is_warning: bool = False,
    reverse_raw_keys: bool = False,
) -> MarketCatalogSnapshotMemberInput:
    pair = TradingPair(
        code=market,
        korean_name=korean_name or f"한글-{market}",
        english_name=english_name or f"English-{market}",
        is_warning=is_warning,
    )
    raw_items = [
        ("market", market),
        ("korean_name", pair.korean_name),
        ("english_name", pair.english_name),
        ("market_event", {"warning": is_warning, "caution": {"PRICE_FLUCTUATIONS": False}}),
    ]

    if reverse_raw_keys:
        raw_items.reverse()

    return MarketCatalogSnapshotMemberInput(
        response_order=response_order,
        pair=pair,
        raw_payload=dict(raw_items),
    )


def _record_success(
    repository: SqliteMinuteCandleRepository,
    snapshot_id: str,
    *,
    observed_at: datetime = _OBSERVED,
    available_at: datetime = _AVAILABLE,
    members: tuple[MarketCatalogSnapshotMemberInput, ...] | None = None,
):
    return repository.record_market_catalog_snapshot(
        snapshot_id=snapshot_id,
        source="upbit",
        endpoint="/v1/market/all",
        request_parameters={"is_details": True},
        quote_currency="KRW",
        observed_at=observed_at,
        available_at=available_at,
        status=MarketCatalogSnapshotStatus.SUCCEEDED,
        members=(
            members
            if members is not None
            else (_member("KRW-BTC", 1), _member("KRW-ADA", 0, is_warning=True))
        ),
    )


def _create_v2_database(database: Path) -> None:
    with sqlite3.connect(database) as connection:
        connection.executescript(
            """
            PRAGMA foreign_keys = ON;
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
            CREATE TABLE minute_candle_backfill_runs (
                run_id TEXT PRIMARY KEY,
                market TEXT NOT NULL,
                requested_start TEXT NOT NULL,
                requested_end TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                available_at TEXT NOT NULL,
                status TEXT NOT NULL,
                error_type TEXT,
                error_message TEXT
            );
            INSERT INTO schema_migrations VALUES (1, '2025-01-01T00:00:00Z');
            INSERT INTO schema_migrations VALUES (2, '2025-01-02T00:00:00Z');
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
            INSERT INTO minute_candle_backfill_runs VALUES (
                'legacy-run', 'KRW-BTC', '2025-01-01T12:00:00Z',
                '2025-01-01T12:01:00Z', '2025-01-01T12:02:00.000000Z',
                '2025-01-01T12:02:01.000000Z', 'SUCCEEDED', NULL, NULL
            );
            """
        )


def _table_count(database: Path, table: str) -> int:
    with sqlite3.connect(database) as connection:
        return int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def test_new_database_applies_v1_through_v3_once(tmp_path: Path) -> None:
    database = tmp_path / "catalog.db"

    with SqliteMinuteCandleRepository(database):
        pass
    with SqliteMinuteCandleRepository(database):
        pass

    with sqlite3.connect(database) as connection:
        versions = connection.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall()

    assert versions == [(1,), (2,), (3,)]


def test_existing_v2_data_is_preserved_when_only_v3_is_applied(tmp_path: Path) -> None:
    database = tmp_path / "legacy-v2.db"
    _create_v2_database(database)

    with SqliteMinuteCandleRepository(database) as repository:
        candle = repository.get_current_minute_candle(
            "KRW-BTC", datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
        )

    assert candle is not None
    assert str(candle.candle.close) == "105"
    assert _table_count(database, "minute_candle_backfill_runs") == 1
    assert _table_count(database, "market_catalog_snapshots") == 0
    with sqlite3.connect(database) as connection:
        versions = connection.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall()
    assert versions == [(1,), (2,), (3,)]


def test_success_snapshot_and_members_are_inserted_atomically(tmp_path: Path) -> None:
    database = tmp_path / "catalog.db"

    with SqliteMinuteCandleRepository(database) as repository:
        stored = _record_success(repository, "snapshot-1")

        with pytest.raises(ValueError, match="response_order"):
            _record_success(
                repository,
                "snapshot-rollback",
                members=(_member("KRW-BTC", 0), _member("KRW-ETH", 0)),
            )

    assert stored.market_count == 2
    assert [member.pair.code for member in stored.members] == ["KRW-ADA", "KRW-BTC"]
    assert stored.members[0].pair.is_warning is True
    assert _table_count(database, "market_catalog_snapshots") == 1
    assert _table_count(database, "market_catalog_snapshot_members") == 2
    with sqlite3.connect(database) as connection:
        observed_at, available_at, request_json = connection.execute(
            """
            SELECT observed_at, available_at, request_parameters_json
            FROM market_catalog_snapshots
            WHERE snapshot_id = 'snapshot-1'
            """
        ).fetchone()
    assert observed_at == "2026-08-23T12:00:00.000000Z"
    assert available_at == "2026-08-23T12:00:00.500000Z"
    assert json.loads(request_json) == {"is_details": True}


def test_snapshot_tables_are_sealed_and_append_only(tmp_path: Path) -> None:
    database = tmp_path / "catalog.db"

    with SqliteMinuteCandleRepository(database) as repository:
        _record_success(repository, "snapshot-1")

    with sqlite3.connect(database) as connection:
        connection.execute("PRAGMA foreign_keys = ON")

        for statement in (
            "UPDATE market_catalog_snapshots SET source = 'other' WHERE snapshot_id = 'snapshot-1'",
            "DELETE FROM market_catalog_snapshots WHERE snapshot_id = 'snapshot-1'",
            "UPDATE market_catalog_snapshot_members SET market = 'KRW-XRP' WHERE snapshot_id = 'snapshot-1'",
            "DELETE FROM market_catalog_snapshot_members WHERE snapshot_id = 'snapshot-1'",
        ):
            with pytest.raises(sqlite3.IntegrityError, match="append-only"):
                connection.execute(statement)

        with pytest.raises(sqlite3.IntegrityError, match="sealed"):
            connection.execute(
                """
                INSERT INTO market_catalog_snapshot_members (
                    snapshot_id, response_order, market, korean_name,
                    english_name, is_warning, raw_payload_json
                ) VALUES ('snapshot-1', 3, 'KRW-XRP', '리플', 'XRP', 0, '{}')
                """
            )


def test_deferred_foreign_key_and_sealing_hold_at_real_commit_boundary(
    tmp_path: Path,
) -> None:
    database = tmp_path / "catalog.db"

    with SqliteMinuteCandleRepository(database) as repository:
        connection = repository._connection
        assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1

        connection.execute("BEGIN")
        connection.execute(
            """
            INSERT INTO market_catalog_snapshot_members (
                snapshot_id, response_order, market, korean_name,
                english_name, is_warning, raw_payload_json
            ) VALUES ('orphan', 0, 'KRW-BTC', '비트코인', 'Bitcoin', 0, '{}')
            """
        )

        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            connection.commit()

        connection.rollback()
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM market_catalog_snapshot_members WHERE snapshot_id = 'orphan'"
            ).fetchone()[0]
            == 0
        )

        connection.execute("BEGIN")
        connection.execute(
            """
            INSERT INTO market_catalog_snapshot_members (
                snapshot_id, response_order, market, korean_name,
                english_name, is_warning, raw_payload_json
            ) VALUES ('bad-count', 0, 'KRW-BTC', '비트코인', 'Bitcoin', 0, '{}')
            """
        )

        with pytest.raises(sqlite3.IntegrityError, match="member contract"):
            connection.execute(
                """
                INSERT INTO market_catalog_snapshots (
                    snapshot_id, source, endpoint, request_parameters_json,
                    quote_currency, observed_at, available_at, status,
                    market_count, content_hash, error_type, error_message
                ) VALUES (
                    'bad-count', 'upbit', '/v1/market/all', '{}', 'KRW',
                    '2026-08-23T12:00:00.000000Z', '2026-08-23T12:00:01.000000Z',
                    'SUCCEEDED', 2,
                    'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                    NULL, NULL
                )
                """
            )

        connection.rollback()
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM market_catalog_snapshot_members WHERE snapshot_id = 'bad-count'"
            ).fetchone()[0]
            == 0
        )
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM market_catalog_snapshots WHERE snapshot_id = 'bad-count'"
            ).fetchone()[0]
            == 0
        )

        connection.execute("BEGIN")
        connection.execute(
            """
            INSERT INTO market_catalog_snapshot_members (
                snapshot_id, response_order, market, korean_name,
                english_name, is_warning, raw_payload_json
            ) VALUES ('failed-with-member', 0, 'KRW-BTC', '비트코인', 'Bitcoin', 0, '{}')
            """
        )

        with pytest.raises(sqlite3.IntegrityError, match="member contract"):
            connection.execute(
                """
                INSERT INTO market_catalog_snapshots (
                    snapshot_id, source, endpoint, request_parameters_json,
                    quote_currency, observed_at, available_at, status,
                    market_count, content_hash, error_type, error_message
                ) VALUES (
                    'failed-with-member', 'upbit', '/v1/market/all', '{}', 'KRW',
                    '2026-08-23T12:00:00.000000Z', '2026-08-23T12:00:01.000000Z',
                    'FAILED', 0, NULL, 'TimeoutError', 'timed out'
                )
                """
            )

        connection.rollback()
        assert (
            connection.execute(
                """
            SELECT COUNT(*) FROM market_catalog_snapshot_members
            WHERE snapshot_id = 'failed-with-member'
            """
            ).fetchone()[0]
            == 0
        )


def test_status_error_member_and_time_invariants_are_enforced(tmp_path: Path) -> None:
    database = tmp_path / "catalog.db"

    with SqliteMinuteCandleRepository(database) as repository:
        with pytest.raises(ValueError, match="non-empty"):
            _record_success(repository, "empty", members=())

        with pytest.raises(ValueError, match="FAILED"):
            repository.record_market_catalog_snapshot(
                snapshot_id="bad-failure",
                source="upbit",
                endpoint="/v1/market/all",
                request_parameters={"is_details": True},
                quote_currency="KRW",
                observed_at=_OBSERVED,
                available_at=_AVAILABLE,
                status=MarketCatalogSnapshotStatus.FAILED,
                members=(_member("KRW-BTC", 0),),
                error_type="TimeoutError",
            )

        with pytest.raises(ValueError, match="error type"):
            repository.record_market_catalog_snapshot(
                snapshot_id="bad-failure",
                source="upbit",
                endpoint="/v1/market/all",
                request_parameters={"is_details": True},
                quote_currency="KRW",
                observed_at=_OBSERVED,
                available_at=_AVAILABLE,
                status=MarketCatalogSnapshotStatus.FAILED,
                members=(),
            )

        with pytest.raises(ValueError, match="observed_at"):
            _record_success(
                repository,
                "time-travel",
                observed_at=_AVAILABLE,
                available_at=_OBSERVED,
            )

    with sqlite3.connect(database) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint"):
            connection.execute(
                """
                INSERT INTO market_catalog_snapshots (
                    snapshot_id, source, endpoint, request_parameters_json,
                    quote_currency, observed_at, available_at, status,
                    market_count, content_hash, error_type, error_message
                ) VALUES (
                    'invalid-time', 'upbit', '/v1/market/all', '{}', 'KRW',
                    '2026-08-23T12:00:01.000000Z', '2026-08-23T12:00:00.000000Z',
                    'FAILED', 0, NULL, 'TimeoutError', NULL
                )
                """
            )

        with pytest.raises(sqlite3.IntegrityError, match="member contract"):
            connection.execute(
                """
                INSERT INTO market_catalog_snapshots (
                    snapshot_id, source, endpoint, request_parameters_json,
                    quote_currency, observed_at, available_at, status,
                    market_count, content_hash, error_type, error_message
                ) VALUES (
                    'missing-member', 'upbit', '/v1/market/all', '{}', 'KRW',
                    '2026-08-23T12:00:00.000000Z', '2026-08-23T12:00:01.000000Z',
                    'SUCCEEDED', 1,
                    'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                    NULL, NULL
                )
                """
            )


def test_content_hash_is_order_and_json_key_independent_but_metadata_sensitive() -> None:
    btc = _member("KRW-BTC", 0)
    ada = _member("KRW-ADA", 1, is_warning=True)
    reordered = (
        replace(
            ada,
            response_order=8,
            raw_payload=_member("KRW-ADA", 8, is_warning=True, reverse_raw_keys=True).raw_payload,
        ),
        replace(
            btc,
            response_order=9,
            raw_payload=_member("KRW-BTC", 9, reverse_raw_keys=True).raw_payload,
        ),
    )
    baseline = market_catalog_content_hash(
        source="upbit",
        quote_currency="KRW",
        members=(btc, ada),
    )

    assert (
        market_catalog_content_hash(
            source="upbit",
            quote_currency="KRW",
            members=reordered,
        )
        == baseline
    )
    assert (
        market_catalog_content_hash(
            source="upbit",
            quote_currency="KRW",
            members=(replace(btc, pair=replace(btc.pair, korean_name="새 이름")), ada),
        )
        != baseline
    )
    assert (
        market_catalog_content_hash(
            source="upbit",
            quote_currency="KRW",
            members=(btc, replace(ada, pair=replace(ada.pair, is_warning=False))),
        )
        != baseline
    )
    assert (
        market_catalog_content_hash(source="upbit", quote_currency="KRW", members=(btc,))
        != baseline
    )
    assert (
        market_catalog_content_hash(
            source="other",
            quote_currency="KRW",
            members=(btc, ada),
        )
        != baseline
    )
    assert (
        market_catalog_content_hash(
            source="upbit",
            quote_currency="USDT",
            members=(btc, ada),
        )
        != baseline
    )


def test_repeated_identical_snapshots_have_same_content_hash(tmp_path: Path) -> None:
    database = tmp_path / "catalog.db"

    with SqliteMinuteCandleRepository(database) as repository:
        first = _record_success(repository, "snapshot-1")
        second = _record_success(
            repository,
            "snapshot-2",
            observed_at=_OBSERVED + timedelta(minutes=1),
            available_at=_AVAILABLE + timedelta(minutes=1),
            members=(
                _member("KRW-ADA", 10, is_warning=True, reverse_raw_keys=True),
                _member("KRW-BTC", 11, reverse_raw_keys=True),
            ),
        )

    assert first.content_hash == second.content_hash


def test_as_of_query_uses_exact_availability_boundary_and_ignores_failure(
    tmp_path: Path,
) -> None:
    database = tmp_path / "catalog.db"

    with SqliteMinuteCandleRepository(database) as repository:
        first = _record_success(repository, "success-1")
        repository.record_market_catalog_snapshot(
            snapshot_id="later-failure",
            source="upbit",
            endpoint="/v1/market/all",
            request_parameters={"is_details": True},
            quote_currency="KRW",
            observed_at=_OBSERVED + timedelta(minutes=10),
            available_at=_AVAILABLE + timedelta(minutes=10),
            status=MarketCatalogSnapshotStatus.FAILED,
            members=(),
            error_type="TimeoutError",
            error_message="timed out",
        )
        future = _record_success(
            repository,
            "future-success",
            observed_at=_OBSERVED + timedelta(minutes=20),
            available_at=_AVAILABLE + timedelta(minutes=20),
        )
        just_before = repository.get_market_catalog_snapshot_as_of(
            "KRW", _AVAILABLE - timedelta(microseconds=1)
        )
        at_boundary = repository.get_market_catalog_snapshot_as_of("KRW", _AVAILABLE)
        after_failure = repository.get_market_catalog_snapshot_as_of(
            "KRW", _AVAILABLE + timedelta(minutes=15)
        )
        after_future = repository.get_market_catalog_snapshot_as_of(
            "KRW", _AVAILABLE + timedelta(minutes=20)
        )

    assert just_before is None
    assert at_boundary == first
    assert after_failure == first
    assert after_future == future
    assert [member.pair.code for member in first.members] == ["KRW-ADA", "KRW-BTC"]
    assert first.members[0].pair.is_warning is True


def test_as_of_query_selects_latest_observed_then_available_and_snapshot_id(
    tmp_path: Path,
) -> None:
    database = tmp_path / "catalog.db"
    observed = _OBSERVED + timedelta(hours=1)

    with SqliteMinuteCandleRepository(database) as repository:
        _record_success(
            repository,
            "snapshot-a",
            observed_at=observed,
            available_at=observed + timedelta(seconds=1),
        )
        _record_success(
            repository,
            "snapshot-b",
            observed_at=observed,
            available_at=observed + timedelta(seconds=2),
        )
        selected_by_available = repository.get_market_catalog_snapshot_as_of(
            "KRW", observed + timedelta(seconds=3)
        )
        _record_success(
            repository,
            "snapshot-z",
            observed_at=observed,
            available_at=observed + timedelta(seconds=2),
        )
        selected_by_id = repository.get_market_catalog_snapshot_as_of(
            "KRW", observed + timedelta(seconds=3)
        )

    assert selected_by_available is not None
    assert selected_by_available.snapshot_id == "snapshot-b"
    assert selected_by_id is not None
    assert selected_by_id.snapshot_id == "snapshot-z"


def test_as_of_query_prioritizes_observed_at_over_availability_order(
    tmp_path: Path,
) -> None:
    database = tmp_path / "catalog.db"
    first_observed = _OBSERVED + timedelta(hours=1)

    with SqliteMinuteCandleRepository(database) as repository:
        _record_success(
            repository,
            "observed-first-available-last",
            observed_at=first_observed,
            available_at=first_observed + timedelta(minutes=20),
        )
        _record_success(
            repository,
            "observed-last-available-first",
            observed_at=first_observed + timedelta(minutes=10),
            available_at=first_observed + timedelta(minutes=11),
        )
        selected = repository.get_market_catalog_snapshot_as_of(
            "KRW", first_observed + timedelta(minutes=21)
        )

    assert selected is not None
    assert selected.snapshot_id == "observed-last-available-first"
