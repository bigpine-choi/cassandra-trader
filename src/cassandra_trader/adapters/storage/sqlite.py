"""SQLite persistence for raw and canonical minute candles."""

import hashlib
import json
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from pathlib import Path

from cassandra_trader.domain.candle import MinuteCandle
from cassandra_trader.domain.market import TradingPair, normalize_quote_currency

_SCHEMA_VERSION = 3
_SCHEMA_V1_SQL = """
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

_SCHEMA_V2_SQL = """
CREATE TABLE minute_candle_backfill_runs (
    run_id TEXT PRIMARY KEY,
    market TEXT NOT NULL,
    requested_start TEXT NOT NULL,
    requested_end TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    available_at TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('SUCCEEDED', 'FAILED')),
    error_type TEXT,
    error_message TEXT,
    CHECK (requested_start < requested_end),
    CHECK (
        length(observed_at) = 27
        AND substr(observed_at, 20, 1) = '.'
        AND substr(observed_at, 27, 1) = 'Z'
    ),
    CHECK (
        length(available_at) = 27
        AND substr(available_at, 20, 1) = '.'
        AND substr(available_at, 27, 1) = 'Z'
    ),
    CHECK (observed_at <= available_at),
    CHECK (
        (status = 'SUCCEEDED' AND error_type IS NULL AND error_message IS NULL)
        OR
        (status = 'FAILED' AND error_type IS NOT NULL AND length(trim(error_type)) > 0)
    )
);

CREATE INDEX minute_candle_backfill_runs_lookup
ON minute_candle_backfill_runs (
    market,
    status,
    (CASE
        WHEN instr(available_at, '.') = 0
        THEN substr(available_at, 1, 19) || '.000000Z'
        ELSE available_at
    END),
    requested_start,
    requested_end,
    observed_at
);

CREATE TRIGGER minute_candle_backfill_runs_no_update
BEFORE UPDATE ON minute_candle_backfill_runs
BEGIN
    SELECT RAISE(ABORT, 'minute candle backfill runs are append-only');
END;

CREATE TRIGGER minute_candle_backfill_runs_no_delete
BEFORE DELETE ON minute_candle_backfill_runs
BEGIN
    SELECT RAISE(ABORT, 'minute candle backfill runs are append-only');
END;
"""

_SCHEMA_V3_SQL = """
CREATE TABLE market_catalog_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    request_parameters_json TEXT NOT NULL,
    quote_currency TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    available_at TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('SUCCEEDED', 'FAILED')),
    market_count INTEGER NOT NULL CHECK (market_count >= 0),
    content_hash TEXT,
    error_type TEXT,
    error_message TEXT,
    CHECK (length(trim(snapshot_id)) > 0),
    CHECK (length(trim(source)) > 0),
    CHECK (length(trim(endpoint)) > 0),
    CHECK (
        length(observed_at) = 27
        AND substr(observed_at, 20, 1) = '.'
        AND substr(observed_at, 27, 1) = 'Z'
    ),
    CHECK (
        length(available_at) = 27
        AND substr(available_at, 20, 1) = '.'
        AND substr(available_at, 27, 1) = 'Z'
    ),
    CHECK (observed_at <= available_at),
    CHECK (
        (
            status = 'SUCCEEDED'
            AND market_count > 0
            AND content_hash IS NOT NULL
            AND length(content_hash) = 64
            AND content_hash NOT GLOB '*[^0-9a-f]*'
            AND error_type IS NULL
            AND error_message IS NULL
        )
        OR
        (
            status = 'FAILED'
            AND market_count = 0
            AND content_hash IS NULL
            AND error_type IS NOT NULL
            AND length(trim(error_type)) > 0
        )
    )
);

CREATE TABLE market_catalog_snapshot_members (
    snapshot_id TEXT NOT NULL,
    response_order INTEGER NOT NULL CHECK (response_order >= 0),
    market TEXT NOT NULL,
    korean_name TEXT NOT NULL,
    english_name TEXT NOT NULL,
    is_warning INTEGER NOT NULL CHECK (is_warning IN (0, 1)),
    raw_payload_json TEXT NOT NULL,
    PRIMARY KEY (snapshot_id, response_order),
    UNIQUE (snapshot_id, market),
    FOREIGN KEY (snapshot_id) REFERENCES market_catalog_snapshots(snapshot_id)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX market_catalog_snapshots_as_of
ON market_catalog_snapshots (
    quote_currency,
    status,
    available_at,
    observed_at DESC,
    snapshot_id DESC
);

CREATE INDEX market_catalog_snapshot_members_market
ON market_catalog_snapshot_members (snapshot_id, market);

CREATE TRIGGER market_catalog_snapshots_validate_members
BEFORE INSERT ON market_catalog_snapshots
WHEN (
    (NEW.status = 'SUCCEEDED' AND (
        SELECT COUNT(*) FROM market_catalog_snapshot_members
        WHERE snapshot_id = NEW.snapshot_id
    ) != NEW.market_count)
    OR
    (NEW.status = 'FAILED' AND EXISTS (
        SELECT 1 FROM market_catalog_snapshot_members
        WHERE snapshot_id = NEW.snapshot_id
    ))
)
BEGIN
    SELECT RAISE(ABORT, 'market catalog snapshot member contract violated');
END;

CREATE TRIGGER market_catalog_snapshot_members_no_insert_after_header
BEFORE INSERT ON market_catalog_snapshot_members
WHEN EXISTS (
    SELECT 1 FROM market_catalog_snapshots
    WHERE snapshot_id = NEW.snapshot_id
)
BEGIN
    SELECT RAISE(ABORT, 'market catalog snapshot is sealed');
END;

CREATE TRIGGER market_catalog_snapshots_no_update
BEFORE UPDATE ON market_catalog_snapshots
BEGIN
    SELECT RAISE(ABORT, 'market catalog snapshots are append-only');
END;

CREATE TRIGGER market_catalog_snapshots_no_delete
BEFORE DELETE ON market_catalog_snapshots
BEGIN
    SELECT RAISE(ABORT, 'market catalog snapshots are append-only');
END;

CREATE TRIGGER market_catalog_snapshot_members_no_update
BEFORE UPDATE ON market_catalog_snapshot_members
BEGIN
    SELECT RAISE(ABORT, 'market catalog snapshot members are append-only');
END;

CREATE TRIGGER market_catalog_snapshot_members_no_delete
BEFORE DELETE ON market_catalog_snapshot_members
BEGIN
    SELECT RAISE(ABORT, 'market catalog snapshot members are append-only');
END;
"""

_MIGRATIONS = (
    (1, _SCHEMA_V1_SQL),
    (2, _SCHEMA_V2_SQL),
    (3, _SCHEMA_V3_SQL),
)


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


@dataclass(frozen=True, slots=True)
class CanonicalMinuteCandleCoverage:
    """Logical canonical minute coverage for one half-open UTC range."""

    market: str
    requested_start: datetime
    requested_end: datetime
    as_of: datetime
    expected_slots: int
    observed_slots: int
    absent_slots: int
    earliest_candle_time: datetime | None
    latest_candle_time: datetime | None


class MinuteCandleBackfillRunStatus(StrEnum):
    """Terminal outcome of one bounded minute-candle backfill run."""

    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class StoredMinuteCandleBackfillRun:
    """Immutable bounded backfill evidence loaded from SQLite."""

    run_id: str
    market: str
    requested_start: datetime
    requested_end: datetime
    observed_at: datetime
    available_at: datetime
    status: MinuteCandleBackfillRunStatus
    error_type: str | None
    error_message: str | None


class MarketCatalogSnapshotStatus(StrEnum):
    """Terminal outcome of one current-market catalog request."""

    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class MarketCatalogSnapshotMemberInput:
    """One raw market response item and its normalized trading pair."""

    response_order: int
    pair: TradingPair
    raw_payload: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class StoredMarketCatalogSnapshotMember:
    """One immutable member loaded from a market catalog snapshot."""

    response_order: int
    pair: TradingPair
    raw_payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class StoredMarketCatalogSnapshot:
    """One append-only market catalog snapshot and its members."""

    snapshot_id: str
    source: str
    endpoint: str
    request_parameters: dict[str, object]
    quote_currency: str
    observed_at: datetime
    available_at: datetime
    status: MarketCatalogSnapshotStatus
    market_count: int
    content_hash: str | None
    error_type: str | None
    error_message: str | None
    members: tuple[StoredMarketCatalogSnapshotMember, ...]

    @property
    def pairs(self) -> tuple[TradingPair, ...]:
        """Return snapshot pairs in deterministic market-code order."""

        return tuple(member.pair for member in self.members)


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
        ingested_text = _utc_availability_text(effective_ingested_at)

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

    def list_minute_candles_as_of(
        self,
        market: str,
        start_time_utc: datetime,
        end_time_utc: datetime,
        as_of: datetime,
    ) -> tuple[StoredMinuteCandle, ...]:
        """Return the highest available revision per minute in a UTC range."""

        start_text = _utc_text(start_time_utc)
        end_text = _utc_text(end_time_utc)
        as_of_text = _utc_availability_text(as_of)
        available_at_key = _utc_comparison_key_sql("available_at")

        if start_time_utc >= end_time_utc:
            raise ValueError("start_time_utc must be before end_time_utc")

        rows = self._connection.execute(
            f"""
            WITH available_revisions AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY market, start_time_utc
                           ORDER BY revision DESC
                       ) AS revision_rank
                FROM canonical_minute_candles
                WHERE market = ?
                  AND start_time_utc >= ?
                  AND start_time_utc < ?
                  AND {available_at_key} <= ?
            )
            SELECT *
            FROM available_revisions
            WHERE revision_rank = 1
            ORDER BY start_time_utc
            """,
            (market, start_text, end_text, as_of_text),
        ).fetchall()

        return tuple(_stored_candle(row) for row in rows)

    def get_canonical_minute_candle_coverage(
        self,
        market: str,
        start_time_utc: datetime,
        end_time_utc: datetime,
        as_of: datetime,
    ) -> CanonicalMinuteCandleCoverage:
        """Summarize logical canonical slots available as of one UTC cutoff."""

        _validate_minute_range(start_time_utc, end_time_utc)
        start_text = _utc_text(start_time_utc)
        end_text = _utc_text(end_time_utc)
        as_of_text = _utc_availability_text(as_of)
        available_at_key = _utc_comparison_key_sql("available_at")
        expected_slots = int((end_time_utc - start_time_utc) // timedelta(minutes=1))
        row = self._connection.execute(
            f"""
            SELECT COUNT(DISTINCT start_time_utc) AS observed_slots,
                   MIN(start_time_utc) AS earliest_candle_time,
                   MAX(start_time_utc) AS latest_candle_time
            FROM canonical_minute_candles
            WHERE market = ?
              AND start_time_utc >= ?
              AND start_time_utc < ?
              AND {available_at_key} <= ?
            """,
            (market, start_text, end_text, as_of_text),
        ).fetchone()
        observed_slots = int(row["observed_slots"])

        return CanonicalMinuteCandleCoverage(
            market=market,
            requested_start=start_time_utc,
            requested_end=end_time_utc,
            as_of=as_of,
            expected_slots=expected_slots,
            observed_slots=observed_slots,
            absent_slots=expected_slots - observed_slots,
            earliest_candle_time=(
                None
                if row["earliest_candle_time"] is None
                else _parse_utc(row["earliest_candle_time"])
            ),
            latest_candle_time=(
                None if row["latest_candle_time"] is None else _parse_utc(row["latest_candle_time"])
            ),
        )

    def record_minute_candle_backfill_run(
        self,
        *,
        run_id: str,
        market: str,
        requested_start: datetime,
        requested_end: datetime,
        observed_at: datetime,
        status: MinuteCandleBackfillRunStatus,
        available_at: datetime | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
    ) -> None:
        """Append terminal evidence for one bounded minute-candle backfill."""

        if not run_id.strip():
            raise ValueError("run_id must not be empty")

        if not market.strip():
            raise ValueError("market must not be empty")

        _validate_minute_range(requested_start, requested_end)
        effective_available_at = available_at or datetime.now(UTC)
        status = MinuteCandleBackfillRunStatus(status)
        observed_text = _utc_availability_text(observed_at)
        available_text = _utc_availability_text(effective_available_at)

        if observed_text > available_text:
            raise ValueError("observed_at must not be after available_at")

        if status is MinuteCandleBackfillRunStatus.SUCCEEDED and (
            error_type is not None or error_message is not None
        ):
            raise ValueError("SUCCEEDED backfill run must not include error information")

        if status is MinuteCandleBackfillRunStatus.FAILED and (
            error_type is None or not error_type.strip()
        ):
            raise ValueError("FAILED backfill run must include an error type")

        with self._connection:
            self._connection.execute(
                """
                INSERT INTO minute_candle_backfill_runs (
                    run_id, market, requested_start, requested_end,
                    observed_at, available_at, status, error_type, error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    market,
                    _utc_text(requested_start),
                    _utc_text(requested_end),
                    observed_text,
                    available_text,
                    status.value,
                    error_type,
                    error_message,
                ),
            )

    def list_minute_candle_backfill_runs(
        self,
        market: str,
        start_time_utc: datetime,
        end_time_utc: datetime,
        as_of: datetime,
        *,
        status: MinuteCandleBackfillRunStatus | None = None,
    ) -> tuple[StoredMinuteCandleBackfillRun, ...]:
        """Return point-in-time backfill evidence overlapping one UTC range."""

        _validate_minute_range(start_time_utc, end_time_utc)
        parameters: list[str] = [
            market,
            _utc_text(end_time_utc),
            _utc_text(start_time_utc),
            _utc_availability_text(as_of),
        ]
        status_clause = ""

        if status is not None:
            normalized_status = MinuteCandleBackfillRunStatus(status)
            status_clause = "AND status = ?"
            parameters.append(normalized_status.value)

        available_at_key = _utc_comparison_key_sql("available_at")
        rows = self._connection.execute(
            f"""
            SELECT *
            FROM minute_candle_backfill_runs
            WHERE market = ?
              AND requested_start < ?
              AND requested_end > ?
              AND {available_at_key} <= ?
              {status_clause}
            ORDER BY {available_at_key}, run_id
            """,
            parameters,
        ).fetchall()

        return tuple(_stored_backfill_run(row) for row in rows)

    def record_market_catalog_snapshot(
        self,
        *,
        snapshot_id: str,
        source: str,
        endpoint: str,
        request_parameters: Mapping[str, object],
        quote_currency: str,
        observed_at: datetime,
        available_at: datetime,
        status: MarketCatalogSnapshotStatus,
        members: Sequence[MarketCatalogSnapshotMemberInput],
        error_type: str | None = None,
        error_message: str | None = None,
    ) -> StoredMarketCatalogSnapshot:
        """Atomically append one complete catalog snapshot or failed request evidence."""

        if not snapshot_id.strip():
            raise ValueError("snapshot_id must not be empty")

        if not source.strip():
            raise ValueError("source must not be empty")

        if not endpoint.strip():
            raise ValueError("endpoint must not be empty")

        normalized_quote = normalize_quote_currency(quote_currency)
        normalized_status = MarketCatalogSnapshotStatus(status)
        observed_text = _utc_availability_text(observed_at)
        available_text = _utc_availability_text(available_at)
        member_items = tuple(members)

        if observed_text > available_text:
            raise ValueError("observed_at must not be after available_at")

        if normalized_status is MarketCatalogSnapshotStatus.SUCCEEDED:
            if not member_items:
                raise ValueError(
                    "SUCCEEDED market catalog snapshot requires a non-empty member set"
                )

            if error_type is not None or error_message is not None:
                raise ValueError(
                    "SUCCEEDED market catalog snapshot must not include error information"
                )
        else:
            if member_items:
                raise ValueError("FAILED market catalog snapshot must not include members")

            if error_type is None or not error_type.strip():
                raise ValueError("FAILED market catalog snapshot must include an error type")

        response_orders: set[int] = set()
        markets: set[str] = set()
        serialized_members: list[tuple[MarketCatalogSnapshotMemberInput, str]] = []

        for member in member_items:
            if (
                not isinstance(member.response_order, int)
                or isinstance(member.response_order, bool)
                or member.response_order < 0
            ):
                raise ValueError("response_order must be a non-negative integer")

            if member.response_order in response_orders:
                raise ValueError("duplicate response_order in market catalog snapshot")

            if member.pair.code in markets:
                raise ValueError("duplicate market in market catalog snapshot")

            if member.pair.quote_currency != normalized_quote:
                raise ValueError("snapshot member quote currency must match quote_currency")

            response_orders.add(member.response_order)
            markets.add(member.pair.code)
            serialized_members.append((member, _json_text(member.raw_payload)))

        content_hash = (
            market_catalog_content_hash(
                source=source,
                quote_currency=normalized_quote,
                members=member_items,
            )
            if normalized_status is MarketCatalogSnapshotStatus.SUCCEEDED
            else None
        )
        request_json = _json_text(request_parameters)

        with self._connection:
            for member, raw_json in serialized_members:
                self._connection.execute(
                    """
                    INSERT INTO market_catalog_snapshot_members (
                        snapshot_id, response_order, market, korean_name,
                        english_name, is_warning, raw_payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        snapshot_id,
                        member.response_order,
                        member.pair.code,
                        member.pair.korean_name,
                        member.pair.english_name,
                        int(member.pair.is_warning),
                        raw_json,
                    ),
                )

            self._connection.execute(
                """
                INSERT INTO market_catalog_snapshots (
                    snapshot_id, source, endpoint, request_parameters_json,
                    quote_currency, observed_at, available_at, status,
                    market_count, content_hash, error_type, error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot_id,
                    source,
                    endpoint,
                    request_json,
                    normalized_quote,
                    observed_text,
                    available_text,
                    normalized_status.value,
                    len(member_items),
                    content_hash,
                    error_type,
                    error_message,
                ),
            )

        stored = self.get_market_catalog_snapshot(snapshot_id)

        if stored is None:
            raise RuntimeError("stored market catalog snapshot could not be reloaded")

        return stored

    def get_market_catalog_snapshot(
        self,
        snapshot_id: str,
    ) -> StoredMarketCatalogSnapshot | None:
        """Return one immutable catalog snapshot by identifier."""

        row = self._connection.execute(
            "SELECT * FROM market_catalog_snapshots WHERE snapshot_id = ?",
            (snapshot_id,),
        ).fetchone()

        return None if row is None else self._load_market_catalog_snapshot(row)

    def get_market_catalog_snapshot_as_of(
        self,
        quote_currency: str,
        as_of: datetime,
    ) -> StoredMarketCatalogSnapshot | None:
        """Return the deterministically latest successful snapshot available by a cutoff."""

        normalized_quote = normalize_quote_currency(quote_currency)
        as_of_text = _utc_availability_text(as_of)
        row = self._connection.execute(
            """
            SELECT *
            FROM market_catalog_snapshots
            WHERE quote_currency = ?
              AND status = 'SUCCEEDED'
              AND available_at <= ?
            ORDER BY observed_at DESC, available_at DESC, snapshot_id DESC
            LIMIT 1
            """,
            (normalized_quote, as_of_text),
        ).fetchone()

        return None if row is None else self._load_market_catalog_snapshot(row)

    def _load_market_catalog_snapshot(
        self,
        row: sqlite3.Row,
    ) -> StoredMarketCatalogSnapshot:
        member_rows = self._connection.execute(
            """
            SELECT *
            FROM market_catalog_snapshot_members
            WHERE snapshot_id = ?
            ORDER BY market
            """,
            (row["snapshot_id"],),
        ).fetchall()
        members = tuple(
            StoredMarketCatalogSnapshotMember(
                response_order=member["response_order"],
                pair=TradingPair(
                    code=member["market"],
                    korean_name=member["korean_name"],
                    english_name=member["english_name"],
                    is_warning=bool(member["is_warning"]),
                ),
                raw_payload=dict(json.loads(member["raw_payload_json"])),
            )
            for member in member_rows
        )

        return StoredMarketCatalogSnapshot(
            snapshot_id=row["snapshot_id"],
            source=row["source"],
            endpoint=row["endpoint"],
            request_parameters=dict(json.loads(row["request_parameters_json"])),
            quote_currency=row["quote_currency"],
            observed_at=_parse_utc(row["observed_at"]),
            available_at=_parse_utc(row["available_at"]),
            status=MarketCatalogSnapshotStatus(row["status"]),
            market_count=row["market_count"],
            content_hash=row["content_hash"],
            error_type=row["error_type"],
            error_message=row["error_message"],
            members=members,
        )

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
            applied_versions = {
                row["version"]
                for row in self._connection.execute(
                    "SELECT version FROM schema_migrations"
                ).fetchall()
            }

            if any(version > _SCHEMA_VERSION for version in applied_versions):
                raise RuntimeError("database schema is newer than this application")

            for version, migration_sql in _MIGRATIONS:
                if version in applied_versions:
                    continue

                for statement in _schema_statements(migration_sql):
                    self._connection.execute(statement)

                self._connection.execute(
                    "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
                    (version, _utc_text(datetime.now(UTC))),
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


def market_catalog_content_hash(
    *,
    source: str,
    quote_currency: str,
    members: Sequence[MarketCatalogSnapshotMemberInput],
) -> str:
    """Hash canonical snapshot contents independent of run metadata and response order."""

    if not source.strip():
        raise ValueError("source must not be empty")

    canonical_members = sorted(
        (
            {
                "market": member.pair.code,
                "korean_name": member.pair.korean_name,
                "english_name": member.pair.english_name,
                "is_warning": member.pair.is_warning,
                "raw_payload": member.raw_payload,
            }
            for member in members
        ),
        key=lambda member: member["market"],
    )
    canonical_snapshot = {
        "source": source,
        "quote_currency": normalize_quote_currency(quote_currency),
        "members": canonical_members,
    }
    return _sha256(_json_text(canonical_snapshot))


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


def _stored_backfill_run(row: sqlite3.Row) -> StoredMinuteCandleBackfillRun:
    return StoredMinuteCandleBackfillRun(
        run_id=row["run_id"],
        market=row["market"],
        requested_start=_parse_utc(row["requested_start"]),
        requested_end=_parse_utc(row["requested_end"]),
        observed_at=_parse_utc(row["observed_at"]),
        available_at=_parse_utc(row["available_at"]),
        status=MinuteCandleBackfillRunStatus(row["status"]),
        error_type=row["error_type"],
        error_message=row["error_message"],
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


def _utc_availability_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("timestamps must be timezone-aware UTC")

    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _utc_comparison_key_sql(column: str) -> str:
    return (
        f"CASE WHEN instr({column}, '.') = 0 "
        f"THEN substr({column}, 1, 19) || '.000000Z' ELSE {column} END"
    )


def _validate_minute_range(start: datetime, end: datetime) -> None:
    _utc_text(start)
    _utc_text(end)

    if start.second != 0 or start.microsecond != 0:
        raise ValueError("start_time_utc must align to a one-minute UTC boundary")

    if end.second != 0 or end.microsecond != 0:
        raise ValueError("end_time_utc must align to a one-minute UTC boundary")

    if start >= end:
        raise ValueError("start_time_utc must be before end_time_utc")


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))

    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError("stored timestamp is not timezone-aware UTC")

    return parsed.astimezone(UTC)


def _json_text(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _schema_statements(migration_sql: str) -> tuple[str, ...]:
    statements: list[str] = []
    buffer: list[str] = []

    for line in migration_sql.splitlines():
        buffer.append(line)
        candidate = "\n".join(buffer).strip()

        if candidate and sqlite3.complete_statement(candidate):
            statements.append(candidate)
            buffer.clear()

    if any(line.strip() for line in buffer):
        raise RuntimeError("incomplete SQLite migration statement")

    return tuple(statements)
