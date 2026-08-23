import hashlib
import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from cassandra_trader.adapters.storage import sqlite as sqlite_adapter
from cassandra_trader.adapters.storage.sqlite import (
    MicrostructureCaptureSessionInput,
    MicrostructureSessionMarketInput,
    MicrostructureTerminalStatus,
    SqliteMinuteCandleRepository,
)
from cassandra_trader.domain.microstructure import (
    OrderbookEvent,
    OrderbookUnit,
    TradeEvent,
    TradeSide,
)
from cassandra_trader.domain.public_stream import StreamType

_WINDOW_END = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)
_AS_OF = _WINDOW_END + timedelta(minutes=10)
_OBSERVED = _AS_OF - timedelta(minutes=5)
_AVAILABLE = _AS_OF - timedelta(minutes=4)
_STARTED = _AS_OF + timedelta(seconds=1)
_EVENT_OBSERVED = _STARTED + timedelta(microseconds=500_000)
_EVENT_AVAILABLE = _EVENT_OBSERVED + timedelta(microseconds=1)


def _session(session_id: str = "session-1") -> MicrostructureCaptureSessionInput:
    return MicrostructureCaptureSessionInput(
        session_id=session_id,
        source="upbit",
        quote_currency="KRW",
        window_start=_WINDOW_END - timedelta(days=30),
        window_end=_WINDOW_END,
        as_of=_AS_OF,
        lookback=timedelta(days=30),
        maximum_snapshot_age=timedelta(hours=1),
        top_n=100,
        top_n_policy_name="liquidity-top-n",
        top_n_policy_version="v1",
        liquidity_policy_name="trailing-quote-volume",
        liquidity_policy_version="v1",
        catalog_policy_name="warning-exclusion",
        catalog_policy_version="v1",
        snapshot_id="snapshot-1",
        snapshot_observed_at=_OBSERVED,
        snapshot_available_at=_AVAILABLE,
        content_hash="a" * 64,
        catalog_candidate_count=2,
        catalog_excluded_count=0,
        liquidity_candidate_count=2,
        liquidity_resolved_count=2,
        liquidity_unresolved_count=0,
        orderbook_units=15,
        requested_duration=timedelta(seconds=60),
        started_at=_STARTED,
    )


def _markets() -> tuple[MicrostructureSessionMarketInput, ...]:
    return (
        MicrostructureSessionMarketInput("KRW-BTC", 1, Decimal("100.123456789123456789")),
        MicrostructureSessionMarketInput("KRW-ETH", 2, Decimal("90.000000000000000001")),
    )


def _trade(*, sequential_id: int = 7, observed_at: datetime = _EVENT_OBSERVED) -> TradeEvent:
    return TradeEvent(
        market="KRW-BTC",
        trade_price=Decimal("100473000.123456789123456789"),
        trade_volume=Decimal("0.000123456789123456789"),
        side=TradeSide.BID,
        trade_timestamp_utc=observed_at - timedelta(milliseconds=2),
        message_timestamp_utc=observed_at - timedelta(milliseconds=1),
        sequential_id=sequential_id,
        best_ask_price=Decimal("100474000"),
        best_ask_size=Decimal("1.2"),
        best_bid_price=Decimal("100473000"),
        best_bid_size=Decimal("2.3"),
        stream_type=StreamType.REALTIME,
        observed_at=observed_at,
    )


def _orderbook(observed_at: datetime = _EVENT_OBSERVED) -> OrderbookEvent:
    return OrderbookEvent(
        market="KRW-BTC",
        message_timestamp_utc=observed_at - timedelta(milliseconds=1),
        total_ask_size=Decimal("4.7"),
        total_bid_size=Decimal("2.6"),
        grouping_level=Decimal(0),
        stream_type=StreamType.SNAPSHOT,
        units=(
            OrderbookUnit(
                ask_price=Decimal("137002000.000000001"),
                bid_price=Decimal("137001000.000000001"),
                ask_size=Decimal("0.106238690000000001"),
                bid_size=Decimal("0.036568120000000001"),
            ),
            OrderbookUnit(
                ask_price=Decimal("137023000.000000002"),
                bid_price=Decimal("137000000.000000002"),
                ask_size=Decimal("0.061440790000000002"),
                bid_size=Decimal("0.335432840000000002"),
            ),
        ),
        observed_at=observed_at,
    )


def _counts(database: Path) -> dict[str, int]:
    tables = (
        "microstructure_capture_sessions",
        "microstructure_capture_session_markets",
        "microstructure_raw_events",
        "microstructure_trade_events",
        "microstructure_orderbook_events",
        "microstructure_orderbook_levels",
        "microstructure_capture_terminals",
    )
    with sqlite3.connect(database) as connection:
        return {
            table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in tables
        }


def test_new_database_applies_v1_through_v4_idempotently(tmp_path: Path) -> None:
    database = tmp_path / "new.db"
    with SqliteMinuteCandleRepository(database):
        pass
    with SqliteMinuteCandleRepository(database):
        pass
    with sqlite3.connect(database) as connection:
        versions = connection.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall()
        indexes = {
            row[1] for row in connection.execute("PRAGMA index_list(microstructure_raw_events)")
        }
    assert versions == [(1,), (2,), (3,), (4,)]
    assert "microstructure_raw_session_payload" in indexes


def test_existing_v3_database_is_migrated_without_losing_canonical_data(tmp_path: Path) -> None:
    database = tmp_path / "legacy-v3.db"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
        )
        connection.executescript(sqlite_adapter._SCHEMA_V1_SQL)
        connection.executescript(sqlite_adapter._SCHEMA_V2_SQL)
        connection.executescript(sqlite_adapter._SCHEMA_V3_SQL)
        connection.executemany(
            "INSERT INTO schema_migrations VALUES (?, ?)",
            ((1, "2026-01-01T00:00:00Z"), (2, "2026-01-02T00:00:00Z"), (3, "2026-01-03T00:00:00Z")),
        )
        connection.execute(
            "INSERT INTO raw_ingestion_batches VALUES (?, ?, ?, ?, ?, ?)",
            (
                "legacy",
                "upbit",
                "/v1/candles/minutes/1",
                "{}",
                _AS_OF.isoformat(),
                _AS_OF.isoformat(),
            ),
        )
        connection.execute(
            "INSERT INTO raw_minute_candles VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                1,
                "legacy",
                0,
                "KRW-BTC",
                _WINDOW_END.isoformat().replace("+00:00", "Z"),
                "{}",
                "raw",
            ),
        )
        connection.execute(
            "INSERT INTO canonical_minute_candles VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                1,
                "KRW-BTC",
                _WINDOW_END.isoformat().replace("+00:00", "Z"),
                (_WINDOW_END + timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
                "1",
                "1",
                "1",
                "1",
                "1",
                "1",
                1,
                "hash",
                1,
                _AS_OF.isoformat(),
            ),
        )

    with SqliteMinuteCandleRepository(database) as repository:
        candle = repository.get_current_minute_candle("KRW-BTC", _WINDOW_END)
    assert candle is not None
    assert candle.candle.close == Decimal(1)
    assert _counts(database)["microstructure_capture_sessions"] == 0


def test_session_raw_and_normalized_events_are_atomic_exact_and_append_only(tmp_path: Path) -> None:
    database = tmp_path / "events.db"
    trade_raw = b'{"type":"trade","precision":"100.123456789123456789"}'
    orderbook_raw = b'{"type":"orderbook","snapshot":true}'
    with SqliteMinuteCandleRepository(database) as repository:
        repository.create_microstructure_capture_session(_session(), _markets())
        first = repository.record_microstructure_event(
            event_id="event-1",
            session_id="session-1",
            receive_ordinal=1,
            raw_payload=trade_raw,
            event=_trade(),
            available_at=_EVENT_AVAILABLE,
        )
        repeated_raw = repository.record_microstructure_event(
            event_id="event-2",
            session_id="session-1",
            receive_ordinal=2,
            raw_payload=trade_raw,
            event=_trade(),
            available_at=_EVENT_AVAILABLE,
        )
        repeated_sequence = repository.record_microstructure_event(
            event_id="event-3",
            session_id="session-1",
            receive_ordinal=3,
            raw_payload=trade_raw + b" ",
            event=_trade(),
            available_at=_EVENT_AVAILABLE,
        )
        orderbook = repository.record_microstructure_event(
            event_id="event-4",
            session_id="session-1",
            receive_ordinal=4,
            raw_payload=orderbook_raw,
            event=_orderbook(),
            available_at=_EVENT_AVAILABLE,
        )
        with pytest.raises(sqlite3.IntegrityError):
            repository.record_microstructure_event(
                event_id="event-4",
                session_id="session-1",
                receive_ordinal=5,
                raw_payload=b"different",
                event=_orderbook(),
                available_at=_EVENT_AVAILABLE,
            )

    assert first.is_duplicate is False
    assert repeated_raw.is_duplicate is True
    assert repeated_sequence.is_duplicate is True
    assert orderbook.is_duplicate is False
    counts = _counts(database)
    assert counts["microstructure_raw_events"] == 4
    assert counts["microstructure_trade_events"] == 3
    assert counts["microstructure_orderbook_events"] == 1
    assert counts["microstructure_orderbook_levels"] == 2
    with sqlite3.connect(database) as connection:
        raw, digest, price = connection.execute(
            """
            SELECT r.raw_payload, r.payload_sha256, t.trade_price
            FROM microstructure_raw_events r
            JOIN microstructure_trade_events t USING (event_id)
            WHERE r.receive_ordinal = 1
            """
        ).fetchone()
        levels = connection.execute(
            "SELECT ask_price FROM microstructure_orderbook_levels ORDER BY level_ordinal"
        ).fetchall()
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "UPDATE microstructure_raw_events SET market = 'KRW-ETH' WHERE event_id = 'event-1'"
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute("DELETE FROM microstructure_capture_sessions")
    assert raw == trade_raw
    assert digest == hashlib.sha256(trade_raw).hexdigest()
    assert Decimal(price) == Decimal("100473000.123456789123456789")
    assert [Decimal(row[0]) for row in levels] == [
        Decimal("137002000.000000001"),
        Decimal("137023000.000000002"),
    ]


def test_same_orderbook_timestamp_frames_are_both_preserved(tmp_path: Path) -> None:
    database = tmp_path / "orderbooks.db"
    with SqliteMinuteCandleRepository(database) as repository:
        repository.create_microstructure_capture_session(_session(), _markets())
        for ordinal, raw in enumerate((b"snapshot-a", b"snapshot-b"), start=1):
            repository.record_microstructure_event(
                event_id=f"event-{ordinal}",
                session_id="session-1",
                receive_ordinal=ordinal,
                raw_payload=raw,
                event=_orderbook(),
                available_at=_EVENT_AVAILABLE,
            )
    assert _counts(database)["microstructure_raw_events"] == 2


def test_event_available_at_exact_boundary_and_reverse_time_are_enforced(tmp_path: Path) -> None:
    database = tmp_path / "availability.db"
    with SqliteMinuteCandleRepository(database) as repository:
        repository.create_microstructure_capture_session(_session(), _markets())
        repository.record_microstructure_event(
            event_id="event-exact",
            session_id="session-1",
            receive_ordinal=1,
            raw_payload=b"exact",
            event=_trade(),
            available_at=_EVENT_OBSERVED,
        )
        with pytest.raises(ValueError, match="observed_at"):
            repository.record_microstructure_event(
                event_id="event-reversed",
                session_id="session-1",
                receive_ordinal=2,
                raw_payload=b"reversed",
                event=_trade(),
                available_at=_EVENT_OBSERVED - timedelta(microseconds=1),
            )
    assert _counts(database)["microstructure_raw_events"] == 1


def test_terminal_counts_and_timestamp_invariant_are_enforced(tmp_path: Path) -> None:
    database = tmp_path / "terminal.db"
    with SqliteMinuteCandleRepository(database) as repository:
        repository.create_microstructure_capture_session(_session(), _markets())
        repository.record_microstructure_lifecycle_event(
            session_id="session-1",
            lifecycle_ordinal=1,
            status="CONNECTED",
            attempt=0,
            delay_seconds=None,
            detail=None,
            observed_at=_EVENT_OBSERVED,
            available_at=_EVENT_AVAILABLE,
        )
        repository.record_microstructure_event(
            event_id="event-1",
            session_id="session-1",
            receive_ordinal=1,
            raw_payload=b"trade",
            event=_trade(),
            available_at=_EVENT_AVAILABLE,
        )
        with pytest.raises(sqlite3.IntegrityError, match="terminal counts"):
            repository.record_microstructure_terminal(
                session_id="session-1",
                status=MicrostructureTerminalStatus.COMPLETED,
                ended_at=_EVENT_AVAILABLE,
                available_at=_EVENT_AVAILABLE,
                elapsed_seconds=Decimal("1"),
                received_trade_count=1,
                persisted_trade_count=0,
                received_orderbook_count=0,
                persisted_orderbook_count=0,
                duplicate_count=0,
                lifecycle_count=1,
                reconnect_count=0,
            )
        with pytest.raises(ValueError, match="ended_at"):
            repository.record_microstructure_terminal(
                session_id="session-1",
                status=MicrostructureTerminalStatus.COMPLETED,
                ended_at=_EVENT_AVAILABLE,
                available_at=_EVENT_OBSERVED,
                elapsed_seconds=Decimal("1"),
                received_trade_count=1,
                persisted_trade_count=1,
                received_orderbook_count=0,
                persisted_orderbook_count=0,
                duplicate_count=0,
                lifecycle_count=1,
                reconnect_count=0,
            )
        repository.record_microstructure_terminal(
            session_id="session-1",
            status=MicrostructureTerminalStatus.COMPLETED,
            ended_at=_EVENT_AVAILABLE,
            available_at=_EVENT_AVAILABLE,
            elapsed_seconds=Decimal("1"),
            received_trade_count=1,
            persisted_trade_count=1,
            received_orderbook_count=0,
            persisted_orderbook_count=0,
            duplicate_count=0,
            lifecycle_count=1,
            reconnect_count=0,
        )
        with pytest.raises(sqlite3.IntegrityError, match="terminal"):
            repository.record_microstructure_lifecycle_event(
                session_id="session-1",
                lifecycle_ordinal=2,
                status="DISCONNECTED",
                attempt=0,
                delay_seconds=None,
                detail=None,
                observed_at=_EVENT_AVAILABLE,
                available_at=_EVENT_AVAILABLE,
            )
        with pytest.raises(sqlite3.IntegrityError, match="terminal"):
            repository.record_microstructure_event(
                event_id="event-late",
                session_id="session-1",
                receive_ordinal=2,
                raw_payload=b"late",
                event=_trade(),
                available_at=_EVENT_AVAILABLE,
            )
    assert _counts(database)["microstructure_capture_terminals"] == 1
    assert _counts(database)["microstructure_raw_events"] == 1


def test_session_count_mismatch_and_late_member_are_rejected_at_commit(tmp_path: Path) -> None:
    database = tmp_path / "sealing.db"
    with SqliteMinuteCandleRepository(database) as repository:
        repository.create_microstructure_capture_session(_session(), _markets())
    with sqlite3.connect(database) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        with pytest.raises(sqlite3.IntegrityError, match="sealed"):
            connection.execute(
                "INSERT INTO microstructure_capture_session_markets VALUES (?, ?, ?, ?)",
                ("session-1", "KRW-XRP", 3, "1"),
            )
    assert _counts(database)["microstructure_capture_session_markets"] == 2


def test_session_rejects_non_ascii_selected_market_before_transaction(tmp_path: Path) -> None:
    database = tmp_path / "invalid-market.db"
    invalid = (MicrostructureSessionMarketInput("KRW-비트", 1, Decimal("1")),)

    with SqliteMinuteCandleRepository(database) as repository:
        with pytest.raises(ValueError, match="ASCII KRW market"):
            repository.create_microstructure_capture_session(_session(), invalid)

    assert _counts(database)["microstructure_capture_sessions"] == 0
    assert _counts(database)["microstructure_capture_session_markets"] == 0
