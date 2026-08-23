import asyncio
import sqlite3
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from cassandra_trader.adapters.storage.sqlite import SqliteMinuteCandleRepository
from cassandra_trader.adapters.upbit.microstructure_stream import (
    MicrostructureFrame,
    parse_microstructure_message,
)
from cassandra_trader.adapters.upbit.public_stream import LifecycleStatus, StreamLifecycleEvent
from cassandra_trader.apps.microstructure_recorder import (
    MicrostructureCaptureStatus,
    capture_market_payload,
    capture_summary_payload,
    record_microstructure,
    record_microstructure_with_dependencies,
)
from cassandra_trader.domain.liquidity_top_n import evaluate_liquidity_top_n
from cassandra_trader.domain.market import MarketUniverseResult, MarketUniverseStatus, TradingPair
from cassandra_trader.domain.market_eligibility import evaluate_market_eligibility
from cassandra_trader.domain.market_liquidity import evaluate_market_liquidity
from cassandra_trader.domain.minute_slot import MinuteSlotSummary

_WINDOW_END = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)
_AS_OF = _WINDOW_END + timedelta(minutes=10)
_STARTED = _AS_OF + timedelta(seconds=1)
_MAXIMUM_AGE = timedelta(hours=1)
_DURATION = timedelta(seconds=60)


class SequenceClock:
    def __init__(self, *values: datetime) -> None:
        self.values = list(values)

    def __call__(self) -> datetime:
        if len(self.values) > 1:
            return self.values.pop(0)
        return self.values[0]


class SequenceMonotonic:
    def __init__(self, *values: float) -> None:
        self.values = list(values)

    def __call__(self) -> float:
        if len(self.values) > 1:
            return self.values.pop(0)
        return self.values[0]


def _selection(*, empty: bool = False, unresolved: bool = False):
    if unresolved:
        universe = MarketUniverseResult(
            status=MarketUniverseStatus.UNRESOLVED,
            quote_currency="KRW",
            as_of=_AS_OF,
            snapshot_id=None,
            observed_at=None,
            available_at=None,
            content_hash=None,
            pairs=(),
        )
        eligibility = evaluate_market_eligibility(universe, _MAXIMUM_AGE)
        liquidity = evaluate_market_liquidity(eligibility, _WINDOW_END, ())
        return evaluate_liquidity_top_n(liquidity, 100)

    pairs = (
        TradingPair("KRW-BTC", "비트코인", "Bitcoin", empty),
        TradingPair("KRW-ETH", "이더리움", "Ethereum", empty),
    )
    universe = MarketUniverseResult(
        status=MarketUniverseStatus.AVAILABLE,
        quote_currency="KRW",
        as_of=_AS_OF,
        snapshot_id="snapshot-1",
        observed_at=_AS_OF - timedelta(minutes=5),
        available_at=_AS_OF - timedelta(minutes=4),
        content_hash="a" * 64,
        pairs=pairs,
    )
    eligibility = evaluate_market_eligibility(universe, _MAXIMUM_AGE)
    summaries = (
        ()
        if empty
        else tuple(
            MinuteSlotSummary(
                market=pair.code,
                start_time_utc=_WINDOW_END - timedelta(days=30),
                end_time_utc=_WINDOW_END,
                as_of=_AS_OF,
                expected_count=43_200,
                traded_count=1,
                no_trade_count=43_199,
                unresolved_count=0,
                quote_volume=Decimal("20.000000000000000001")
                if pair.code == "KRW-BTC"
                else Decimal("10.000000000000000002"),
            )
            for pair in pairs
        )
    )
    return evaluate_liquidity_top_n(
        evaluate_market_liquidity(eligibility, _WINDOW_END, summaries),
        100,
    )


def _trade_frame(raw: bytes | None = None) -> MicrostructureFrame:
    payload = raw or (
        b'{"type":"trade","code":"KRW-BTC","trade_price":100.123456789123456789,'
        b'"trade_volume":0.00000000123456789,"ask_bid":"BID",'
        b'"trade_timestamp":1787573401001,"timestamp":1787573401002,'
        b'"sequential_id":7,"best_ask_price":101,"best_ask_size":1.2,'
        b'"best_bid_price":100,"best_bid_size":2.3,"stream_type":"REALTIME"}'
    )
    return parse_microstructure_message(payload, observed_at=_STARTED + timedelta(seconds=1))


def _orderbook_frame() -> MicrostructureFrame:
    return parse_microstructure_message(
        b'{"type":"orderbook","code":"KRW-ETH","timestamp":1787573401003,'
        b'"total_ask_size":4.7,"total_bid_size":2.6,"orderbook_units":['
        b'{"ask_price":11,"bid_price":10,"ask_size":1,"bid_size":2}],'
        b'"stream_type":"SNAPSHOT","level":0}',
        observed_at=_STARTED + timedelta(seconds=1),
    )


async def _blocked_duration(_: float) -> None:
    await asyncio.Event().wait()


def _stream_factory(frames: tuple[MicrostructureFrame, ...], calls: list | None = None):
    def factory(markets, units, lifecycle_callback):
        if calls is not None:
            calls.append((markets, units))

        async def stream() -> AsyncIterator[MicrostructureFrame]:
            for frame in frames:
                yield frame

        return stream()

    return factory


def test_fixed_selection_events_duplicates_and_json_payloads(tmp_path: Path) -> None:
    database = tmp_path / "capture.db"
    calls = []
    ids = iter(("event-1", "event-2", "event-3"))
    frames = (_trade_frame(), _trade_frame(), _orderbook_frame())
    with SqliteMinuteCandleRepository(database) as repository:
        result = asyncio.run(
            record_microstructure_with_dependencies(
                _selection(),
                repository,
                _DURATION,
                orderbook_units=15,
                stream_factory=_stream_factory(frames, calls),
                clock=SequenceClock(
                    _STARTED,
                    _STARTED + timedelta(seconds=2),
                    _STARTED + timedelta(seconds=3),
                    _STARTED + timedelta(seconds=4),
                    _STARTED + timedelta(seconds=5),
                    _STARTED + timedelta(seconds=5, microseconds=1),
                ),
                monotonic=SequenceMonotonic(10.0, 13.0),
                duration_sleep=_blocked_duration,
                session_id_factory=lambda: "session-1",
                event_id_factory=lambda: next(ids),
                event_limit=3,
            )
        )

    assert result.status is MicrostructureCaptureStatus.COMPLETED
    assert calls == [(("KRW-BTC", "KRW-ETH"), 15)]
    assert [item.market for item in result.markets] == ["KRW-BTC", "KRW-ETH"]
    assert result.received_trade_count == result.persisted_trade_count == 2
    assert result.received_orderbook_count == result.persisted_orderbook_count == 1
    assert result.duplicate_count == 1
    assert capture_market_payload(result.markets[0])["quote_volume"] == ("20.000000000000000001")
    summary = capture_summary_payload(result)
    assert summary["session_id"] == "session-1"
    assert summary["selected_count"] == 2
    assert summary["elapsed_seconds"] == "3.0"
    with sqlite3.connect(database) as connection:
        assert (
            connection.execute("SELECT COUNT(*) FROM microstructure_raw_events").fetchone()[0] == 3
        )


def test_resolved_empty_selection_completes_without_stream_connection(tmp_path: Path) -> None:
    database = tmp_path / "empty.db"

    def unexpected_stream(*args):
        raise AssertionError("empty selection must not connect")

    with SqliteMinuteCandleRepository(database) as repository:
        result = asyncio.run(
            record_microstructure_with_dependencies(
                _selection(empty=True),
                repository,
                _DURATION,
                orderbook_units=15,
                stream_factory=unexpected_stream,
                clock=SequenceClock(
                    _STARTED,
                    _STARTED + timedelta(seconds=1),
                    _STARTED + timedelta(seconds=1, microseconds=1),
                ),
                monotonic=SequenceMonotonic(1.0, 1.0),
                duration_sleep=_blocked_duration,
                session_id_factory=lambda: "session-empty",
                event_id_factory=lambda: "unused",
            )
        )

    assert result.status is MicrostructureCaptureStatus.COMPLETED
    assert result.markets == ()
    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT selected_count FROM microstructure_capture_sessions"
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT status FROM microstructure_capture_terminals"
        ).fetchone() == ("COMPLETED",)


def test_unresolved_selection_stops_before_session_and_stream(tmp_path: Path) -> None:
    database = tmp_path / "missing.db"
    calls = 0

    def evaluator(*args):
        nonlocal calls
        calls += 1
        return _selection(unresolved=True)

    class UnexpectedAdapter:
        def stream(self, *args, **kwargs):
            raise AssertionError("unresolved selection must not connect")

    result = asyncio.run(
        record_microstructure(
            "KRW",
            _WINDOW_END,
            _AS_OF,
            database,
            _MAXIMUM_AGE,
            100,
            _DURATION,
            selection_evaluator=evaluator,
            stream_adapter=UnexpectedAdapter(),  # type: ignore[arg-type]
        )
    )

    assert calls == 1
    assert result.status is MicrostructureCaptureStatus.UNRESOLVED
    assert result.session_id is None
    assert not database.exists()


def test_selection_is_evaluated_once_and_fixed_for_connection_and_session(tmp_path: Path) -> None:
    database = tmp_path / "fixed.db"
    selection = _selection()
    evaluator_calls = 0
    adapter_calls = []

    def evaluator(*args):
        nonlocal evaluator_calls
        evaluator_calls += 1
        if evaluator_calls > 1:
            raise AssertionError("selection must not be reevaluated")
        return selection

    class Adapter:
        def stream(self, markets, *, orderbook_units, lifecycle_callback):
            adapter_calls.append((tuple(markets), orderbook_units))

            async def events() -> AsyncIterator[MicrostructureFrame]:
                await lifecycle_callback(StreamLifecycleEvent(LifecycleStatus.CONNECTED, 0))
                try:
                    yield _trade_frame()
                finally:
                    await lifecycle_callback(StreamLifecycleEvent(LifecycleStatus.DISCONNECTED, 0))

            return events()

    result = asyncio.run(
        record_microstructure(
            "KRW",
            _WINDOW_END,
            _AS_OF,
            database,
            _MAXIMUM_AGE,
            100,
            _DURATION,
            selection_evaluator=evaluator,
            stream_adapter=Adapter(),  # type: ignore[arg-type]
            clock=SequenceClock(
                _STARTED,
                _STARTED + timedelta(microseconds=1),
                _STARTED + timedelta(microseconds=2),
                _STARTED + timedelta(seconds=1),
                _STARTED + timedelta(seconds=2),
                _STARTED + timedelta(seconds=2, microseconds=1),
                _STARTED + timedelta(seconds=3),
                _STARTED + timedelta(seconds=3, microseconds=1),
            ),
            monotonic=SequenceMonotonic(0.0, 3.0),
            duration_sleep=_blocked_duration,
            session_id_factory=lambda: "session-fixed",
            event_id_factory=lambda: "event-fixed",
            event_limit=1,
        )
    )

    assert evaluator_calls == 1
    assert adapter_calls == [(("KRW-BTC", "KRW-ETH"), 15)]
    assert result.lifecycle_count == 2
    assert result.reconnect_count == 0
    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT snapshot_id FROM microstructure_capture_sessions"
        ).fetchone() == ("snapshot-1",)
        assert connection.execute(
            "SELECT status FROM microstructure_connection_lifecycle_events ORDER BY lifecycle_ordinal"
        ).fetchall() == [("CONNECTED",), ("DISCONNECTED",)]


def test_duration_stop_uses_injected_monotonic_boundary(tmp_path: Path) -> None:
    database = tmp_path / "duration.db"

    async def immediate_duration(_: float) -> None:
        return None

    async def never_stream() -> AsyncIterator[MicrostructureFrame]:
        await asyncio.Event().wait()
        yield _trade_frame()

    with SqliteMinuteCandleRepository(database) as repository:
        result = asyncio.run(
            record_microstructure_with_dependencies(
                _selection(),
                repository,
                _DURATION,
                orderbook_units=15,
                stream_factory=lambda *args: never_stream(),
                clock=SequenceClock(
                    _STARTED,
                    _STARTED + _DURATION,
                    _STARTED + _DURATION + timedelta(microseconds=1),
                ),
                monotonic=SequenceMonotonic(10.0, 70.0),
                duration_sleep=immediate_duration,
                session_id_factory=lambda: "session-duration",
                event_id_factory=lambda: "unused",
            )
        )

    assert result.status is MicrostructureCaptureStatus.COMPLETED
    assert result.elapsed_seconds == Decimal("60.0")
    assert result.received_trade_count == 0


def test_writer_failure_preserves_committed_prefix_and_records_failed_terminal(
    tmp_path: Path,
) -> None:
    database = tmp_path / "writer.db"

    class FailingRepository(SqliteMinuteCandleRepository):
        calls = 0

        def record_microstructure_event(self, **kwargs):
            self.calls += 1
            if self.calls == 2:
                raise sqlite3.OperationalError("disk full")
            return super().record_microstructure_event(**kwargs)

    with FailingRepository(database) as repository:
        result = asyncio.run(
            record_microstructure_with_dependencies(
                _selection(),
                repository,
                _DURATION,
                orderbook_units=15,
                stream_factory=_stream_factory((_trade_frame(), _orderbook_frame())),
                clock=SequenceClock(
                    _STARTED,
                    _STARTED + timedelta(seconds=1),
                    _STARTED + timedelta(seconds=2),
                    _STARTED + timedelta(seconds=3),
                    _STARTED + timedelta(seconds=3, microseconds=1),
                ),
                monotonic=SequenceMonotonic(0.0, 1.0),
                duration_sleep=_blocked_duration,
                session_id_factory=lambda: "session-writer",
                event_id_factory=iter(("event-1", "event-2")).__next__,
                event_limit=2,
            )
        )

    assert result.status is MicrostructureCaptureStatus.FAILED
    assert result.error_type == "OperationalError"
    assert result.received_trade_count == result.persisted_trade_count == 1
    assert result.received_orderbook_count == 1
    assert result.persisted_orderbook_count == 0
    with sqlite3.connect(database) as connection:
        assert (
            connection.execute("SELECT COUNT(*) FROM microstructure_raw_events").fetchone()[0] == 1
        )
        assert connection.execute(
            "SELECT status FROM microstructure_capture_terminals"
        ).fetchone() == ("FAILED",)


def test_cancellation_is_recorded_then_propagated(tmp_path: Path) -> None:
    database = tmp_path / "cancel.db"

    async def cancelling_stream() -> AsyncIterator[MicrostructureFrame]:
        raise asyncio.CancelledError("stop")
        yield _trade_frame()

    with SqliteMinuteCandleRepository(database) as repository:
        with pytest.raises(asyncio.CancelledError, match="stop"):
            asyncio.run(
                record_microstructure_with_dependencies(
                    _selection(),
                    repository,
                    _DURATION,
                    orderbook_units=15,
                    stream_factory=lambda *args: cancelling_stream(),
                    clock=SequenceClock(
                        _STARTED,
                        _STARTED + timedelta(seconds=1),
                        _STARTED + timedelta(seconds=1, microseconds=1),
                    ),
                    monotonic=SequenceMonotonic(0.0, 1.0),
                    duration_sleep=_blocked_duration,
                    session_id_factory=lambda: "session-cancel",
                    event_id_factory=lambda: "unused",
                )
            )
    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT status FROM microstructure_capture_terminals"
        ).fetchone() == ("CANCELLED",)


def test_cancellation_is_not_masked_by_terminal_clock_failure(tmp_path: Path) -> None:
    database = tmp_path / "cancel-clock.db"

    async def cancelling_stream() -> AsyncIterator[MicrostructureFrame]:
        raise asyncio.CancelledError("stop")
        yield _trade_frame()

    def failing_clock() -> datetime:
        raise RuntimeError("clock unavailable")

    clock_values = iter((_STARTED,))

    def clock() -> datetime:
        try:
            return next(clock_values)
        except StopIteration:
            return failing_clock()

    with SqliteMinuteCandleRepository(database) as repository:
        with pytest.raises(asyncio.CancelledError, match="stop") as caught:
            asyncio.run(
                record_microstructure_with_dependencies(
                    _selection(),
                    repository,
                    _DURATION,
                    orderbook_units=15,
                    stream_factory=lambda *args: cancelling_stream(),
                    clock=clock,
                    monotonic=SequenceMonotonic(0.0, 1.0),
                    duration_sleep=_blocked_duration,
                    session_id_factory=lambda: "session-cancel-clock",
                    event_id_factory=lambda: "unused",
                )
            )

    assert any("failed to read terminal clocks" in note for note in caught.value.__notes__)
    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM microstructure_capture_terminals"
        ).fetchone() == (0,)


def test_stream_cleanup_failure_is_noted_without_masking_cancellation(tmp_path: Path) -> None:
    database = tmp_path / "cancel-cleanup.db"

    class CancellingStream:
        def __aiter__(self):
            return self

        async def __anext__(self) -> MicrostructureFrame:
            raise asyncio.CancelledError("stop")

        async def aclose(self) -> None:
            raise RuntimeError("stream close failed")

    with SqliteMinuteCandleRepository(database) as repository:
        with pytest.raises(asyncio.CancelledError, match="stop") as caught:
            asyncio.run(
                record_microstructure_with_dependencies(
                    _selection(),
                    repository,
                    _DURATION,
                    orderbook_units=15,
                    stream_factory=lambda *args: CancellingStream(),  # type: ignore[arg-type]
                    clock=SequenceClock(
                        _STARTED,
                        _STARTED + timedelta(seconds=1),
                        _STARTED + timedelta(seconds=1, microseconds=1),
                    ),
                    monotonic=SequenceMonotonic(0.0, 1.0),
                    duration_sleep=_blocked_duration,
                    session_id_factory=lambda: "session-cancel-cleanup",
                    event_id_factory=lambda: "unused",
                )
            )

    assert any("failed to close microstructure stream" in note for note in caught.value.__notes__)
    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT status FROM microstructure_capture_terminals"
        ).fetchone() == ("CANCELLED",)


@pytest.mark.parametrize(
    "changes",
    [
        {"quote_currency": "BTC"},
        {"window_end": _WINDOW_END + timedelta(microseconds=1)},
        {"as_of": _WINDOW_END - timedelta(microseconds=1)},
        {"top_n": 0},
        {"top_n": True},
        {"duration": timedelta(0)},
        {"duration": timedelta(days=1, microseconds=1)},
        {"orderbook_units": 10},
    ],
)
def test_invalid_request_is_rejected_before_selection_and_database(
    tmp_path: Path,
    changes: dict,
) -> None:
    arguments = {
        "quote_currency": "KRW",
        "window_end": _WINDOW_END,
        "as_of": _AS_OF,
        "database": tmp_path / "missing.db",
        "maximum_snapshot_age": _MAXIMUM_AGE,
        "top_n": 100,
        "duration": _DURATION,
        "orderbook_units": 15,
    }
    arguments.update(changes)
    called = False

    def evaluator(*args):
        nonlocal called
        called = True
        return _selection()

    with pytest.raises(ValueError):
        asyncio.run(record_microstructure(**arguments, selection_evaluator=evaluator))
    assert called is False
    assert not arguments["database"].exists()
