"""SQLite persistence for raw and canonical minute candles."""

import hashlib
import json
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from cassandra_trader.domain.candle import MinuteCandle

_SCHEMA_VERSION = 1
_SCHEMA_SQL = """
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
    response_order INTEGER NOT NULL CHECK (response_order >= 0),
    market TEXT NOT NULL,
    source_candle_time TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    UNIQUE (batch_id, response_order)
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
    revision INTEGER NOT NULL CHECK (revision >= 1),
    content_sha256 TEXT NOT NULL,
    source_raw_record_id INTEGER NOT NULL REFERENCES raw_minute_candles(raw_record_id),
    available_at TEXT NOT NULL,
    UNIQUE (market, start_time_utc, revision)
);

CREATE INDEX canonical_minute_candles_current
ON canonical_minute_candles (market, start_time_utc, revision DESC);

CREATE TRIGGER raw_ingestion_batches_no_update
BEFORE UPDATE ON raw_ingestion_batches
BEGIN
    SELECT RAISE(ABORT, 'raw ingestion batches are append-only');
END;

CREATE TRIGGER raw_ingestion_batches_no_delete
BEFORE DELETE ON raw_ingestion_batches
BEGIN
    SELECT RAISE(ABORT, 'raw ingestion batches are append-only');
END;

CREATE TRIGGER raw_minute_candles_no_update
BEFORE UPDATE ON raw_minute_candles
BEGIN
    SELECT RAISE(ABORT, 'raw minute candles are append-only');
END;

CREATE TRIGGER raw_minute_candles_no_delete
BEFORE DELETE ON raw_minute_candles
BEGIN
    SELECT RAISE(ABORT, 'raw minute candles are append-only');
END;

CREATE TRIGGER canonical_minute_candles_no_update
BEFORE UPDATE ON canonical_minute_candles
BEGIN
    SELECT RAISE(ABORT, 'canonical minute candle revisions are immutable');
END;

CREATE TRIGGER canonical_minute_candles_no_delete
BEFORE DELETE ON canonical_minute_candles
BEGIN
    SELECT RAISE(ABORT, 'canonical minute candle revisions are immutable');
END;
"""


@dataclass(frozen=True, slots=True)
class RawMinuteCandleInput:
    """One source response item paired with its canonical representation."""

    response_order: int
    market: str
    source_candle_time: str
    raw_payload: Mapping[str, object]
    candle: MinuteCandle | None


@dataclass(frozen=True, slots=True)
class IngestionStats:
    """Outcome of one atomic raw and canonical batch ingestion."""

    raw_count: int
    new_canonical_count: int
    revised_canonical_count: int
    unchanged_canonical_count: int
    batch_replayed: bool = False


@dataclass(frozen=True, slots=True)
class StoredMinuteCandle:
    """A canonical candle revision loaded from SQLite."""

    candle: MinuteCandle
    revision: int
    content_sha256: str
    source_raw_record_id: int
    available_at: datetime


class SqliteMinuteCandleRepository:
    """Persist raw observations and immutable canonical candle revisions."""

    def __init__(self, database: str | Path) -> None:
        self.database = Path(database).expanduser()
        self.database.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self.database)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys = ON")
        self._connection.execute("PRAGMA journal_mode = WAL")
        self._migrate()

    def close(self) -> None:
        """Close the SQLite connection."""

        self._connection.close()

    def __enter__(self) -> "SqliteMinuteCandleRepository":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def ingest_batch(
        self,
        *,
        batch_id: str,
        source: str,
        endpoint: str,
        request_parameters: Mapping[str, object],
        received_at: datetime,
        records: Sequence[RawMinuteCandleInput],
        ingested_at: datetime | None = None,
    ) -> IngestionStats:
        """Atomically append a raw batch and any necessary canonical revisions."""

        effective_ingested_at = ingested_at or datetime.now(UTC)
        received_text = _utc_text(received_at)
        ingested_text = _utc_text(effective_ingested_at)

        if received_at > effective_ingested_at:
            raise ValueError("received_at must not be after ingested_at")

        request_json = _json_text(request_parameters)

        new_count = 0
        revised_count = 0
        unchanged_count = 0

        with self._connection:
            existing_batch = self._connection.execute(
                """
                SELECT source, endpoint, request_parameters_json, received_at
                FROM raw_ingestion_batches
                WHERE batch_id = ?
                """,
                (batch_id,),
            ).fetchone()

            if existing_batch is not None:
                self._verify_replayed_batch(
                    existing_batch=existing_batch,
                    batch_id=batch_id,
                    source=source,
                    endpoint=endpoint,
                    request_json=request_json,
                    received_text=received_text,
                    records=records,
                )
                return IngestionStats(
                    raw_count=len(records),
                    new_canonical_count=0,
                    revised_canonical_count=0,
                    unchanged_canonical_count=0,
                    batch_replayed=True,
                )

            self._connection.execute(
                """
                INSERT INTO raw_ingestion_batches (
                    batch_id, source, endpoint, request_parameters_json,
                    received_at, ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    batch_id,
                    source,
                    endpoint,
                    request_json,
                    received_text,
                    ingested_text,
                ),
            )

            for record in records:
                raw_json = _json_text(record.raw_payload)
                payload_sha256 = _sha256(raw_json)
                source_candle_time = _utc_text(_parse_utc(record.source_candle_time))
                raw_cursor = self._connection.execute(
                    """
                    INSERT INTO raw_minute_candles (
                        batch_id, response_order, market, source_candle_time,
                        raw_json, payload_sha256
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        batch_id,
                        record.response_order,
                        record.market,
                        source_candle_time,
                        raw_json,
                        payload_sha256,
                    ),
                )

                if record.candle is None:
                    continue

                if record.market != record.candle.market:
                    raise ValueError("raw and canonical market must match")

                content = _canonical_content(record.candle)

                if source_candle_time != content["start_time_utc"]:
                    raise ValueError("source and canonical candle times must match")

                content_sha256 = _sha256(_json_text(content))
                current = self._connection.execute(
                    """
                    SELECT revision, content_sha256
                    FROM canonical_minute_candles
                    WHERE market = ? AND start_time_utc = ?
                    ORDER BY revision DESC
                    LIMIT 1
                    """,
                    (record.candle.market, content["start_time_utc"]),
                ).fetchone()

                if current is not None and current["content_sha256"] == content_sha256:
                    unchanged_count += 1
                    continue

                revision = 1 if current is None else current["revision"] + 1
                self._connection.execute(
                    """
                    INSERT INTO canonical_minute_candles (
                        market, start_time_utc, end_time_utc,
                        open, high, low, close, base_volume, quote_volume,
                        revision, content_sha256, source_raw_record_id, available_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        content["market"],
                        content["start_time_utc"],
                        content["end_time_utc"],
                        content["open"],
                        content["high"],
                        content["low"],
                        content["close"],
                        content["base_volume"],
                        content["quote_volume"],
                        revision,
                        content_sha256,
                        raw_cursor.lastrowid,
                        ingested_text,
                    ),
                )

                if current is None:
                    new_count += 1
                else:
                    revised_count += 1

        return IngestionStats(
            raw_count=len(records),
            new_canonical_count=new_count,
            revised_canonical_count=revised_count,
            unchanged_canonical_count=unchanged_count,
        )

    def get_current_minute_candle(
        self,
        market: str,
        start_time_utc: datetime,
    ) -> StoredMinuteCandle | None:
        """Return the highest canonical revision for one logical candle."""

        row = self._connection.execute(
            """
            SELECT *
            FROM canonical_minute_candles
            WHERE market = ? AND start_time_utc = ?
            ORDER BY revision DESC
            LIMIT 1
            """,
            (market, _utc_text(start_time_utc)),
        ).fetchone()

        return None if row is None else _stored_candle(row)

    def list_minute_candle_revisions(
        self,
        market: str,
        start_time_utc: datetime,
    ) -> tuple[StoredMinuteCandle, ...]:
        """Return every immutable revision for one logical candle."""

        rows = self._connection.execute(
            """
            SELECT *
            FROM canonical_minute_candles
            WHERE market = ? AND start_time_utc = ?
            ORDER BY revision
            """,
            (market, _utc_text(start_time_utc)),
        ).fetchall()

        return tuple(_stored_candle(row) for row in rows)

    def _migrate(self) -> None:
        self._connection.execute("BEGIN IMMEDIATE")

        try:
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                )
                """
            )
            applied = self._connection.execute(
                "SELECT 1 FROM schema_migrations WHERE version = ?",
                (_SCHEMA_VERSION,),
            ).fetchone()

            if applied is None:
                for statement in _schema_statements():
                    self._connection.execute(statement)

                self._connection.execute(
                    "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
                    (_SCHEMA_VERSION, _utc_text(datetime.now(UTC))),
                )

            self._connection.commit()
        except Exception:
            self._connection.rollback()
            raise

    def _verify_replayed_batch(
        self,
        *,
        existing_batch: sqlite3.Row,
        batch_id: str,
        source: str,
        endpoint: str,
        request_json: str,
        received_text: str,
        records: Sequence[RawMinuteCandleInput],
    ) -> None:
        metadata = (
            existing_batch["source"],
            existing_batch["endpoint"],
            existing_batch["request_parameters_json"],
            existing_batch["received_at"],
        )
        expected_metadata = (source, endpoint, request_json, received_text)

        if metadata != expected_metadata:
            raise ValueError("batch_id already exists with different metadata")

        stored = self._connection.execute(
            """
            SELECT response_order, payload_sha256
            FROM raw_minute_candles
            WHERE batch_id = ?
            ORDER BY response_order
            """,
            (batch_id,),
        ).fetchall()
        expected = sorted(
            (
                record.response_order,
                _sha256(_json_text(record.raw_payload)),
            )
            for record in records
        )
        observed = [(row["response_order"], row["payload_sha256"]) for row in stored]

        if observed != expected:
            raise ValueError("batch_id already exists with different raw payloads")


def _canonical_content(candle: MinuteCandle) -> dict[str, str]:
    return {
        "market": candle.market,
        "start_time_utc": _utc_text(candle.start_time_utc),
        "end_time_utc": _utc_text(candle.end_time_utc),
        "open": _decimal_text(candle.open),
        "high": _decimal_text(candle.high),
        "low": _decimal_text(candle.low),
        "close": _decimal_text(candle.close),
        "base_volume": _decimal_text(candle.base_volume),
        "quote_volume": _decimal_text(candle.quote_volume),
    }


def _stored_candle(row: sqlite3.Row) -> StoredMinuteCandle:
    candle = MinuteCandle(
        market=row["market"],
        start_time_utc=_parse_utc(row["start_time_utc"]),
        end_time_utc=_parse_utc(row["end_time_utc"]),
        open=Decimal(row["open"]),
        high=Decimal(row["high"]),
        low=Decimal(row["low"]),
        close=Decimal(row["close"]),
        base_volume=Decimal(row["base_volume"]),
        quote_volume=Decimal(row["quote_volume"]),
    )

    return StoredMinuteCandle(
        candle=candle,
        revision=row["revision"],
        content_sha256=row["content_sha256"],
        source_raw_record_id=row["source_raw_record_id"],
        available_at=_parse_utc(row["available_at"]),
    )


def _decimal_text(value: Decimal) -> str:
    if not value.is_finite():
        raise ValueError("canonical decimals must be finite")

    if value == 0:
        return "0"

    text = format(value, "f")

    if "." in text:
        text = text.rstrip("0").rstrip(".")

    return text


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("timestamps must be timezone-aware UTC")

    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))

    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError("stored timestamp is not timezone-aware UTC")

    return parsed.astimezone(UTC)


def _json_text(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _schema_statements() -> tuple[str, ...]:
    statements: list[str] = []
    buffer: list[str] = []

    for line in _SCHEMA_SQL.splitlines():
        buffer.append(line)
        candidate = "\n".join(buffer).strip()

        if candidate and sqlite3.complete_statement(candidate):
            statements.append(candidate)
            buffer.clear()

    if any(line.strip() for line in buffer):
        raise RuntimeError("incomplete SQLite migration statement")

    return tuple(statements)
