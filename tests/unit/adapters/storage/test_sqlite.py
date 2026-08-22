import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from cassandra_trader.adapters.storage.sqlite import (
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
    assert _count(database, "schema_migrations") == 1
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

    assert versions == [(1,)]


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
