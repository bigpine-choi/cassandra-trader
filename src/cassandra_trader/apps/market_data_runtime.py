"""Single-market realtime stream and authoritative REST reconciliation runtime."""

import asyncio
import inspect
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import cast

from upbit import AsyncUpbit

from cassandra_trader.adapters.storage.sqlite import SqliteMinuteCandleRepository
from cassandra_trader.adapters.upbit.public_data import UpbitPublicDataAdapter
from cassandra_trader.adapters.upbit.public_stream import (
    LifecycleCallback,
    LifecycleStatus,
    StreamLifecycleEvent,
    UpbitPublicStreamAdapter,
    normalize_markets,
)
from cassandra_trader.apps.minute_candle_backfill import (
    FetchMinuteCandlePage,
    MinuteCandleBackfillResult,
    backfill_minute_candles_into_repository,
)
from cassandra_trader.apps.minute_reconciliation import (
    MinuteReconciliationResult,
    reconcile_completed_minute,
)
from cassandra_trader.domain.public_stream import PublicMarketEvent

_DEFAULT_STARTUP_LOOKBACK_MINUTES = 10
_DEFAULT_FINALIZATION_DELAY_SECONDS = 3

type Clock = Callable[[], datetime]
type Sleep = Callable[[float], Awaitable[None]]
type StreamFactory = Callable[[LifecycleCallback], AsyncIterator[PublicMarketEvent]]
type ReconciliationCallback = Callable[[MinuteReconciliationResult], object]
type RuntimeEventCallback = Callable[["RuntimeRecoveryEvent"], object]


class RuntimeRecoveryKind(StrEnum):
    """Observable startup and reconnect recovery transitions."""

    STARTUP_STARTED = "STARTUP_RECOVERY_STARTED"
    STARTUP_COMPLETED = "STARTUP_RECOVERY_COMPLETED"
    RECONNECT_STARTED = "RECONNECT_RECOVERY_STARTED"
    RECONNECT_COMPLETED = "RECONNECT_RECOVERY_COMPLETED"


@dataclass(frozen=True, slots=True)
class RuntimeRecoveryEvent:
    """One observable rolling-lookback or reconnect recovery transition."""

    kind: RuntimeRecoveryKind
    market: str
    start_time_utc: datetime
    end_time_utc: datetime
    result: MinuteCandleBackfillResult | None = None


@dataclass(frozen=True, slots=True)
class MarketDataRuntimeResult:
    """Finite runtime result returned when max reconciliations is configured."""

    startup_backfill: MinuteCandleBackfillResult
    reconciliations_completed: int
    last_reconciled_boundary: datetime


@dataclass(slots=True)
class _RuntimeState:
    last_reconciled_boundary: datetime
    disconnected_at: datetime | None = None


async def run_market_data(
    market: str,
    database: str | Path,
    *,
    startup_lookback_minutes: int = _DEFAULT_STARTUP_LOOKBACK_MINUTES,
    finalization_delay_seconds: int = _DEFAULT_FINALIZATION_DELAY_SECONDS,
    max_reconciliations: int | None = None,
    reconciliation_callback: ReconciliationCallback | None = None,
    lifecycle_callback: LifecycleCallback | None = None,
    runtime_event_callback: RuntimeEventCallback | None = None,
) -> MarketDataRuntimeResult:
    """Own the REST client, SQLite repository, and public stream for one market."""

    normalized_market = normalize_markets([market])[0]

    async with AsyncUpbit(
        environment="kr",
        timeout=10.0,
        max_retries=2,
    ) as rest_client:
        rest_adapter = UpbitPublicDataAdapter(
            list_minute_candles=rest_client.candles.list_minutes,
        )
        stream_adapter = UpbitPublicStreamAdapter()

        def stream_factory(callback: LifecycleCallback) -> AsyncIterator[PublicMarketEvent]:
            return stream_adapter.stream(
                [normalized_market],
                lifecycle_callback=callback,
            )

        with SqliteMinuteCandleRepository(database) as repository:
            return await run_market_data_with_dependencies(
                normalized_market,
                repository,
                fetch_page=rest_adapter.fetch_minute_candle_batch,
                stream_factory=stream_factory,
                startup_lookback_minutes=startup_lookback_minutes,
                finalization_delay_seconds=finalization_delay_seconds,
                max_reconciliations=max_reconciliations,
                reconciliation_callback=reconciliation_callback,
                lifecycle_callback=lifecycle_callback,
                runtime_event_callback=runtime_event_callback,
            )


async def run_market_data_with_dependencies(
    market: str,
    repository: SqliteMinuteCandleRepository,
    *,
    fetch_page: FetchMinuteCandlePage,
    stream_factory: StreamFactory,
    startup_lookback_minutes: int = _DEFAULT_STARTUP_LOOKBACK_MINUTES,
    finalization_delay_seconds: int = _DEFAULT_FINALIZATION_DELAY_SECONDS,
    max_reconciliations: int | None = None,
    clock: Clock | None = None,
    sleep: Sleep = asyncio.sleep,
    reconciliation_callback: ReconciliationCallback | None = None,
    lifecycle_callback: LifecycleCallback | None = None,
    runtime_event_callback: RuntimeEventCallback | None = None,
) -> MarketDataRuntimeResult:
    """Run the single-market runtime with injectable network and time boundaries."""

    normalized_market = normalize_markets([market])[0]
    _validate_options(
        startup_lookback_minutes,
        finalization_delay_seconds,
        max_reconciliations,
    )
    effective_clock = clock or (lambda: datetime.now(UTC))
    now = effective_clock()
    latest_boundary = latest_finalizable_minute_boundary(
        now,
        finalization_delay_seconds,
    )
    startup_start = latest_boundary - timedelta(minutes=startup_lookback_minutes)
    await _notify_runtime_event(
        runtime_event_callback,
        RuntimeRecoveryEvent(
            kind=RuntimeRecoveryKind.STARTUP_STARTED,
            market=normalized_market,
            start_time_utc=startup_start,
            end_time_utc=latest_boundary,
        ),
    )
    startup_result = await backfill_minute_candles_into_repository(
        normalized_market,
        startup_start,
        latest_boundary,
        repository,
        fetch_page=fetch_page,
        observed_at=now,
    )
    await _notify_runtime_event(
        runtime_event_callback,
        RuntimeRecoveryEvent(
            kind=RuntimeRecoveryKind.STARTUP_COMPLETED,
            market=normalized_market,
            start_time_utc=startup_start,
            end_time_utc=latest_boundary,
            result=startup_result,
        ),
    )

    state = _RuntimeState(last_reconciled_boundary=latest_boundary)
    recovery_requested = asyncio.Event()

    async def handle_lifecycle(event: StreamLifecycleEvent) -> None:
        await _notify_callback(lifecycle_callback, event)

        if event.status is LifecycleStatus.DISCONNECTED:
            state.disconnected_at = effective_clock()
            return

        if event.status is not LifecycleStatus.CONNECTED or state.disconnected_at is None:
            return

        state.disconnected_at = None
        recovery_requested.set()

    async def consume_stream() -> None:
        events = stream_factory(handle_lifecycle)

        try:
            async for _event in events:
                pass
        finally:
            await events.aclose()

        raise RuntimeError("public market-data stream ended unexpectedly")

    async def schedule_reconciliations() -> int:
        completed = 0

        while True:
            if recovery_requested.is_set():
                recovery_requested.clear()
                recovery_start = state.last_reconciled_boundary
                recovery_observed_at = effective_clock()
                recovery_end = latest_finalizable_minute_boundary(
                    recovery_observed_at,
                    finalization_delay_seconds,
                )

                if recovery_start < recovery_end:
                    await _notify_runtime_event(
                        runtime_event_callback,
                        RuntimeRecoveryEvent(
                            kind=RuntimeRecoveryKind.RECONNECT_STARTED,
                            market=normalized_market,
                            start_time_utc=recovery_start,
                            end_time_utc=recovery_end,
                        ),
                    )
                    recovery_result = await backfill_minute_candles_into_repository(
                        normalized_market,
                        recovery_start,
                        recovery_end,
                        repository,
                        fetch_page=fetch_page,
                        observed_at=recovery_observed_at,
                    )
                    state.last_reconciled_boundary = recovery_end
                    await _notify_runtime_event(
                        runtime_event_callback,
                        RuntimeRecoveryEvent(
                            kind=RuntimeRecoveryKind.RECONNECT_COMPLETED,
                            market=normalized_market,
                            start_time_utc=recovery_start,
                            end_time_utc=recovery_end,
                            result=recovery_result,
                        ),
                    )

            latest = latest_finalizable_minute_boundary(
                effective_clock(),
                finalization_delay_seconds,
            )

            while state.last_reconciled_boundary < latest:
                target_start = state.last_reconciled_boundary
                observed_at = effective_clock()
                result = await reconcile_completed_minute(
                    normalized_market,
                    target_start,
                    repository,
                    fetch_page,
                    observed_at,
                )
                state.last_reconciled_boundary = target_start + timedelta(minutes=1)

                completed += 1
                await _notify_callback(reconciliation_callback, result)

                if max_reconciliations is not None and completed >= max_reconciliations:
                    return completed

            next_due = (
                state.last_reconciled_boundary
                + timedelta(minutes=1)
                + timedelta(seconds=finalization_delay_seconds)
            )
            delay = max((next_due - effective_clock()).total_seconds(), 0.0)
            await _wait_for_scheduler_wakeup(delay, sleep, recovery_requested)

    stream_task = asyncio.create_task(consume_stream())
    scheduler_task = asyncio.create_task(schedule_reconciliations())
    tasks = (stream_task, scheduler_task)

    try:
        done, _pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

        for task in done:
            error = task.exception()

            if error is not None:
                raise error

        if scheduler_task in done:
            reconciliations_completed = scheduler_task.result()
            return MarketDataRuntimeResult(
                startup_backfill=startup_result,
                reconciliations_completed=reconciliations_completed,
                last_reconciled_boundary=state.last_reconciled_boundary,
            )

        raise RuntimeError("public market-data stream ended unexpectedly")
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)


def latest_finalizable_minute_boundary(now: datetime, delay_seconds: int) -> datetime:
    """Return floor(now - finalization delay) on a UTC one-minute boundary."""

    _validate_utc(now, "now")

    if delay_seconds < 0:
        raise ValueError("delay_seconds must be non-negative")

    delayed = now - timedelta(seconds=delay_seconds)
    return delayed.replace(second=0, microsecond=0)


async def _wait_for_scheduler_wakeup(
    delay: float,
    sleep: Sleep,
    recovery_requested: asyncio.Event,
) -> None:
    if recovery_requested.is_set():
        return

    sleep_task = asyncio.create_task(sleep(delay))
    recovery_task = asyncio.create_task(recovery_requested.wait())
    tasks = (sleep_task, recovery_task)

    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)


def _validate_options(
    startup_lookback_minutes: int,
    finalization_delay_seconds: int,
    max_reconciliations: int | None,
) -> None:
    if startup_lookback_minutes < 1:
        raise ValueError("startup_lookback_minutes must be positive")

    if finalization_delay_seconds < 0:
        raise ValueError("finalization_delay_seconds must be non-negative")

    if max_reconciliations is not None and max_reconciliations < 1:
        raise ValueError("max_reconciliations must be positive")


async def _notify_callback(callback: Callable[[object], object] | None, value: object) -> None:
    if callback is None:
        return

    result = callback(value)

    if inspect.isawaitable(result):
        await cast(Awaitable[object], result)


async def _notify_runtime_event(
    callback: RuntimeEventCallback | None,
    event: RuntimeRecoveryEvent,
) -> None:
    await _notify_callback(cast(Callable[[object], object] | None, callback), event)


def _validate_utc(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be timezone-aware UTC")
