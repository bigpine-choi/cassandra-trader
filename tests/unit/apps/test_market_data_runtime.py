import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from cassandra_trader.adapters.storage.sqlite import SqliteMinuteCandleRepository
from cassandra_trader.adapters.upbit.public_data import (
    MinuteCandleObservation,
    MinuteCandleResponseBatch,
)
from cassandra_trader.adapters.upbit.public_stream import (
    LifecycleStatus,
    StreamLifecycleEvent,
)
from cassandra_trader.apps.market_data_runtime import (
    RuntimeRecoveryEvent,
    RuntimeRecoveryKind,
    latest_finalizable_minute_boundary,
    run_market_data_with_dependencies,
)
from cassandra_trader.apps.minute_reconciliation import (
    MinuteReconciliationResult,
    MinuteReconciliationStatus,
)
from cassandra_trader.domain.candle import MinuteCandle
from cassandra_trader.domain.public_stream import (
    MinuteCandleUpdate,
    PublicMarketEvent,
    StreamType,
)

_INITIAL = datetime(2025, 1, 1, 17, 33, 3, tzinfo=UTC)


class MutableClock:
    def __init__(self, current: datetime = _INITIAL) -> None:
        self.current = current

    def __call__(self) -> datetime:
        return self.current

    def advance(self, seconds: float) -> None:
        self.current += timedelta(seconds=seconds)


def _observation(start: datetime) -> MinuteCandleObservation:
    start_text = start.isoformat().replace("+00:00", "Z")
    candle = MinuteCandle(
        market="KRW-BTC",
        start_time_utc=start,
        end_time_utc=start + timedelta(minutes=1),
        open=Decimal("10"),
        high=Decimal("11"),
        low=Decimal("9"),
        close=Decimal("10.5"),
        base_volume=Decimal("1"),
        quote_volume=Decimal("10"),
    )
    return MinuteCandleObservation(
        response_order=0,
        market="KRW-BTC",
        source_candle_time=start_text,
        raw_payload={"market": "KRW-BTC", "candle_date_time_utc": start_text},
        candle=candle,
    )


class RuntimeFetcher:
    def __init__(self, clock: MutableClock, *, return_single: bool = True) -> None:
        self.clock = clock
        self.return_single = return_single
        self.calls: list[tuple[str, int, datetime, datetime]] = []

    async def __call__(
        self,
        market: str,
        count: int,
        *,
        as_of: datetime,
        to: datetime,
    ) -> MinuteCandleResponseBatch:
        self.calls.append((market, count, as_of, to))
        observations = ()

        if count == 1 and self.return_single:
            observations = (_observation(to - timedelta(minutes=1)),)

        return MinuteCandleResponseBatch(
            endpoint="/v1/candles/minutes/1",
            request_parameters={
                "market": market,
                "count": count,
                "to": to.isoformat().replace("+00:00", "Z"),
            },
            received_at=self.clock(),
            observations=observations,
        )


class WaitingStream:
    def __init__(self) -> None:
        self.closed = False
        self.connected = asyncio.Event()

    def factory(self, lifecycle_callback) -> AsyncIterator[PublicMarketEvent]:
        async def events() -> AsyncIterator[PublicMarketEvent]:
            await lifecycle_callback(
                StreamLifecycleEvent(status=LifecycleStatus.CONNECTED, attempt=0)
            )
            self.connected.set()

            try:
                while True:
                    await asyncio.Event().wait()
                    raise AssertionError("unreachable")
                    yield
            finally:
                self.closed = True
                await lifecycle_callback(
                    StreamLifecycleEvent(status=LifecycleStatus.DISCONNECTED, attempt=0)
                )

        return events()


def test_three_second_finalization_boundary() -> None:
    before = datetime(2025, 1, 1, 17, 33, 2, 999999, tzinfo=UTC)
    at_boundary = datetime(2025, 1, 1, 17, 33, 3, tzinfo=UTC)

    assert latest_finalizable_minute_boundary(before, 3) == datetime(2025, 1, 1, 17, 32, tzinfo=UTC)
    assert latest_finalizable_minute_boundary(at_boundary, 3) == datetime(
        2025, 1, 1, 17, 33, tzinfo=UTC
    )


def test_startup_lookback_and_max_reconciliation_close_stream(tmp_path: Path) -> None:
    clock = MutableClock()
    fetcher = RuntimeFetcher(clock)
    stream = WaitingStream()
    reconciliations: list[MinuteReconciliationResult] = []
    runtime_events: list[RuntimeRecoveryEvent] = []

    async def advance_sleep(delay: float) -> None:
        clock.advance(delay)

    with SqliteMinuteCandleRepository(tmp_path / "candles.db") as repository:
        result = asyncio.run(
            run_market_data_with_dependencies(
                "KRW-BTC",
                repository,
                fetch_page=fetcher,
                stream_factory=stream.factory,
                startup_lookback_minutes=10,
                finalization_delay_seconds=3,
                max_reconciliations=1,
                clock=clock,
                sleep=advance_sleep,
                reconciliation_callback=reconciliations.append,
                runtime_event_callback=runtime_events.append,
            )
        )

    startup_call = fetcher.calls[0]
    assert startup_call[1] == 200
    assert startup_call[3] == datetime(2025, 1, 1, 17, 33, tzinfo=UTC)
    assert result.startup_backfill.requested_start == datetime(2025, 1, 1, 17, 23, tzinfo=UTC)
    assert result.startup_backfill.requested_end == datetime(2025, 1, 1, 17, 33, tzinfo=UTC)
    assert result.reconciliations_completed == 1
    assert reconciliations[0].start_time_utc == datetime(2025, 1, 1, 17, 33, tzinfo=UTC)
    assert reconciliations[0].status is MinuteReconciliationStatus.CONFIRMED
    assert [event.kind for event in runtime_events[:2]] == [
        RuntimeRecoveryKind.STARTUP_STARTED,
        RuntimeRecoveryKind.STARTUP_COMPLETED,
    ]
    assert stream.closed is True


def test_scheduler_catches_up_delayed_minutes_once_in_order(tmp_path: Path) -> None:
    clock = MutableClock()
    fetcher = RuntimeFetcher(clock)
    stream = WaitingStream()
    starts: list[datetime] = []
    first_sleep = True

    async def delayed_sleep(delay: float) -> None:
        nonlocal first_sleep

        if first_sleep:
            first_sleep = False
            clock.advance(delay + 120)
        else:
            clock.advance(delay)

    def record(result: MinuteReconciliationResult) -> None:
        starts.append(result.start_time_utc)

    with SqliteMinuteCandleRepository(tmp_path / "candles.db") as repository:
        asyncio.run(
            run_market_data_with_dependencies(
                "KRW-BTC",
                repository,
                fetch_page=fetcher,
                stream_factory=stream.factory,
                max_reconciliations=3,
                clock=clock,
                sleep=delayed_sleep,
                reconciliation_callback=record,
            )
        )

    assert starts == [
        datetime(2025, 1, 1, 17, 33, tzinfo=UTC),
        datetime(2025, 1, 1, 17, 34, tzinfo=UTC),
        datetime(2025, 1, 1, 17, 35, tzinfo=UTC),
    ]
    assert len(starts) == len(set(starts))


def test_reconnect_backfills_gap_from_last_boundary(tmp_path: Path) -> None:
    clock = MutableClock()
    fetcher = RuntimeFetcher(clock)
    runtime_events: list[RuntimeRecoveryEvent] = []
    closed = False

    def stream_factory(lifecycle_callback) -> AsyncIterator[PublicMarketEvent]:
        async def events() -> AsyncIterator[PublicMarketEvent]:
            nonlocal closed
            await lifecycle_callback(
                StreamLifecycleEvent(status=LifecycleStatus.CONNECTED, attempt=0)
            )
            await lifecycle_callback(
                StreamLifecycleEvent(status=LifecycleStatus.DISCONNECTED, attempt=0)
            )
            clock.current = datetime(2025, 1, 1, 17, 35, 3, tzinfo=UTC)
            await lifecycle_callback(
                StreamLifecycleEvent(status=LifecycleStatus.CONNECTED, attempt=1)
            )

            try:
                while True:
                    await asyncio.Event().wait()
                    yield
            finally:
                closed = True

        return events()

    async def advance_sleep(delay: float) -> None:
        clock.advance(delay)

    with SqliteMinuteCandleRepository(tmp_path / "candles.db") as repository:
        asyncio.run(
            run_market_data_with_dependencies(
                "KRW-BTC",
                repository,
                fetch_page=fetcher,
                stream_factory=stream_factory,
                max_reconciliations=1,
                clock=clock,
                sleep=advance_sleep,
                runtime_event_callback=runtime_events.append,
            )
        )

    backfill_calls = [call for call in fetcher.calls if call[1] == 200]
    assert len(backfill_calls) == 2
    assert backfill_calls[1][3] == datetime(2025, 1, 1, 17, 35, tzinfo=UTC)
    reconnect_events = [
        event for event in runtime_events if event.kind.name.startswith("RECONNECT")
    ]
    assert [event.kind for event in reconnect_events] == [
        RuntimeRecoveryKind.RECONNECT_STARTED,
        RuntimeRecoveryKind.RECONNECT_COMPLETED,
    ]
    assert reconnect_events[0].start_time_utc == datetime(2025, 1, 1, 17, 33, tzinfo=UTC)
    assert reconnect_events[0].end_time_utc == datetime(2025, 1, 1, 17, 35, tzinfo=UTC)
    assert closed is True


def test_reconnect_without_gap_skips_rest_backfill(tmp_path: Path) -> None:
    clock = MutableClock()
    fetcher = RuntimeFetcher(clock)

    def stream_factory(lifecycle_callback) -> AsyncIterator[PublicMarketEvent]:
        async def events() -> AsyncIterator[PublicMarketEvent]:
            await lifecycle_callback(
                StreamLifecycleEvent(status=LifecycleStatus.CONNECTED, attempt=0)
            )
            await lifecycle_callback(
                StreamLifecycleEvent(status=LifecycleStatus.DISCONNECTED, attempt=0)
            )
            await lifecycle_callback(
                StreamLifecycleEvent(status=LifecycleStatus.CONNECTED, attempt=1)
            )

            while True:
                await asyncio.Event().wait()
                yield

        return events()

    async def advance_sleep(delay: float) -> None:
        clock.advance(delay)

    with SqliteMinuteCandleRepository(tmp_path / "candles.db") as repository:
        asyncio.run(
            run_market_data_with_dependencies(
                "KRW-BTC",
                repository,
                fetch_page=fetcher,
                stream_factory=stream_factory,
                max_reconciliations=1,
                clock=clock,
                sleep=advance_sleep,
            )
        )

    assert len([call for call in fetcher.calls if call[1] == 200]) == 1


def test_provisional_websocket_update_is_never_written_directly(tmp_path: Path) -> None:
    clock = MutableClock()
    fetcher = RuntimeFetcher(clock, return_single=False)
    target = datetime(2025, 1, 1, 17, 33, tzinfo=UTC)

    def stream_factory(lifecycle_callback) -> AsyncIterator[PublicMarketEvent]:
        async def events() -> AsyncIterator[PublicMarketEvent]:
            await lifecycle_callback(
                StreamLifecycleEvent(status=LifecycleStatus.CONNECTED, attempt=0)
            )
            yield MinuteCandleUpdate(
                market="KRW-BTC",
                start_time_utc=target,
                end_time_utc=target + timedelta(minutes=1),
                open=Decimal("100"),
                high=Decimal("110"),
                low=Decimal("90"),
                close=Decimal("105"),
                base_volume=Decimal("5"),
                quote_volume=Decimal("500"),
                event_time_utc=target + timedelta(seconds=30),
                received_at=target + timedelta(seconds=31),
                stream_type=StreamType.REALTIME,
            )

            while True:
                await asyncio.Event().wait()
                yield

        return events()

    async def advance_sleep(delay: float) -> None:
        clock.advance(delay)

    reconciliations: list[MinuteReconciliationResult] = []
    with SqliteMinuteCandleRepository(tmp_path / "candles.db") as repository:
        asyncio.run(
            run_market_data_with_dependencies(
                "KRW-BTC",
                repository,
                fetch_page=fetcher,
                stream_factory=stream_factory,
                max_reconciliations=1,
                clock=clock,
                sleep=advance_sleep,
                reconciliation_callback=reconciliations.append,
            )
        )
        stored = repository.get_current_minute_candle("KRW-BTC", target)

    assert reconciliations[0].status is MinuteReconciliationStatus.NOT_RETURNED
    assert stored is None


def test_stream_failure_cancels_scheduler_sibling(tmp_path: Path) -> None:
    clock = MutableClock()
    fetcher = RuntimeFetcher(clock)
    sleep_started = asyncio.Event()
    scheduler_cancelled = False

    async def blocking_sleep(_delay: float) -> None:
        nonlocal scheduler_cancelled
        sleep_started.set()

        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            scheduler_cancelled = True
            raise

    def stream_factory(_lifecycle_callback) -> AsyncIterator[PublicMarketEvent]:
        async def events() -> AsyncIterator[PublicMarketEvent]:
            await sleep_started.wait()
            raise RuntimeError("stream failed")
            yield

        return events()

    with SqliteMinuteCandleRepository(tmp_path / "candles.db") as repository:
        with pytest.raises(RuntimeError, match="stream failed"):
            asyncio.run(
                run_market_data_with_dependencies(
                    "KRW-BTC",
                    repository,
                    fetch_page=fetcher,
                    stream_factory=stream_factory,
                    clock=clock,
                    sleep=blocking_sleep,
                )
            )

    assert scheduler_cancelled is True


def test_scheduler_failure_closes_stream_sibling(tmp_path: Path) -> None:
    clock = MutableClock()
    stream = WaitingStream()

    async def failing_fetcher(
        market: str,
        count: int,
        *,
        as_of: datetime,
        to: datetime,
    ) -> MinuteCandleResponseBatch:
        if count == 1:
            raise RuntimeError("REST reconciliation failed")

        return MinuteCandleResponseBatch(
            endpoint="/v1/candles/minutes/1",
            request_parameters={"market": market, "count": count},
            received_at=clock(),
            observations=(),
        )

    async def advance_sleep(delay: float) -> None:
        clock.advance(delay)

    with SqliteMinuteCandleRepository(tmp_path / "candles.db") as repository:
        with pytest.raises(RuntimeError, match="REST reconciliation failed"):
            asyncio.run(
                run_market_data_with_dependencies(
                    "KRW-BTC",
                    repository,
                    fetch_page=failing_fetcher,
                    stream_factory=stream.factory,
                    clock=clock,
                    sleep=advance_sleep,
                )
            )

    assert stream.closed is True


def test_runtime_cancellation_closes_stream_and_scheduler(tmp_path: Path) -> None:
    async def scenario(repository: SqliteMinuteCandleRepository) -> tuple[bool, bool]:
        clock = MutableClock()
        fetcher = RuntimeFetcher(clock)
        stream = WaitingStream()
        sleep_started = asyncio.Event()
        sleep_cancelled = False

        async def blocking_sleep(_delay: float) -> None:
            nonlocal sleep_cancelled
            sleep_started.set()

            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                sleep_cancelled = True
                raise

        task = asyncio.create_task(
            run_market_data_with_dependencies(
                "KRW-BTC",
                repository,
                fetch_page=fetcher,
                stream_factory=stream.factory,
                clock=clock,
                sleep=blocking_sleep,
            )
        )
        await stream.connected.wait()
        await sleep_started.wait()
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        return stream.closed, sleep_cancelled

    with SqliteMinuteCandleRepository(tmp_path / "candles.db") as repository:
        stream_closed, scheduler_cancelled = asyncio.run(scenario(repository))

    assert stream_closed is True
    assert scheduler_cancelled is True
