"""Bounded run-once recorder for selected public trade and orderbook frames."""

import asyncio
import math
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from uuid import uuid4

from cassandra_trader.adapters.storage.sqlite import (
    MicrostructureCaptureSessionInput,
    MicrostructureSessionMarketInput,
    MicrostructureTerminalStatus,
    SqliteMinuteCandleRepository,
)
from cassandra_trader.adapters.upbit.microstructure_stream import (
    MicrostructureFrame,
    UpbitMicrostructureStreamAdapter,
    validate_orderbook_units,
)
from cassandra_trader.adapters.upbit.public_stream import (
    LifecycleCallback,
    LifecycleStatus,
    StreamLifecycleEvent,
)
from cassandra_trader.apps.liquidity_top_n import evaluate_liquidity_top_n_as_of
from cassandra_trader.domain.liquidity_top_n import (
    LiquidityTopNDecisionStatus,
    LiquidityTopNEvaluationStatus,
    LiquidityTopNResult,
    validate_top_n,
)
from cassandra_trader.domain.market import normalize_quote_currency
from cassandra_trader.domain.market_eligibility import validate_maximum_snapshot_age
from cassandra_trader.domain.microstructure import OrderbookEvent, TradeEvent

_MAXIMUM_CAPTURE_DURATION = timedelta(days=1)

type Clock = Callable[[], datetime]
type MonotonicClock = Callable[[], float]
type DurationSleep = Callable[[float], Awaitable[None]]
type IdFactory = Callable[[], str]
type StreamFactory = Callable[
    [tuple[str, ...], int, LifecycleCallback], AsyncIterator[MicrostructureFrame]
]
type SelectionEvaluator = Callable[
    [str, datetime, datetime, str | Path, timedelta, int], LiquidityTopNResult
]


class MicrostructureCaptureStatus(StrEnum):
    """Public result status for one bounded recorder request."""

    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    UNRESOLVED = "UNRESOLVED"


@dataclass(frozen=True, slots=True)
class MicrostructureSelectedMarketResult:
    """One fixed selected market and its exact upstream metric."""

    market: str
    liquidity_rank: int
    quote_volume: Decimal


@dataclass(frozen=True, slots=True)
class MicrostructureCaptureResult:
    """Fixed selection metadata and terminal counts for one recorder request."""

    status: MicrostructureCaptureStatus
    selection: LiquidityTopNResult
    markets: tuple[MicrostructureSelectedMarketResult, ...]
    session_id: str | None
    requested_duration: timedelta
    orderbook_units: int
    started_at: datetime | None
    ended_at: datetime | None
    elapsed_seconds: Decimal | None
    received_trade_count: int
    persisted_trade_count: int
    received_orderbook_count: int
    persisted_orderbook_count: int
    duplicate_count: int
    lifecycle_count: int
    reconnect_count: int
    error_type: str | None
    error_message: str | None

    def __post_init__(self) -> None:
        validate_capture_duration(self.requested_duration)
        validate_orderbook_units(self.orderbook_units)
        if self.markets != tuple(sorted(self.markets, key=lambda item: item.market)):
            raise ValueError("capture markets must be ordered by market code")
        if len({item.market for item in self.markets}) != len(self.markets):
            raise ValueError("capture markets must not contain duplicates")
        expected = tuple(
            MicrostructureSelectedMarketResult(
                decision.market,
                decision.liquidity_rank,
                decision.quote_volume,
            )
            for decision in self.selection.decisions
            if decision.status is LiquidityTopNDecisionStatus.SELECTED
            and decision.liquidity_rank is not None
            and decision.quote_volume is not None
        )
        if self.markets != expected:
            raise ValueError("capture markets must exactly match fixed top-N selection")
        counts = (
            self.received_trade_count,
            self.persisted_trade_count,
            self.received_orderbook_count,
            self.persisted_orderbook_count,
            self.duplicate_count,
            self.lifecycle_count,
            self.reconnect_count,
        )
        if any(
            not isinstance(count, int) or isinstance(count, bool) or count < 0 for count in counts
        ):
            raise ValueError("capture counts must be non-negative integers")
        if self.persisted_trade_count > self.received_trade_count or (
            self.persisted_orderbook_count > self.received_orderbook_count
        ):
            raise ValueError("persisted counts must not exceed received counts")
        if self.status is MicrostructureCaptureStatus.UNRESOLVED:
            if (
                self.selection.status is not LiquidityTopNEvaluationStatus.UNRESOLVED
                or self.markets
                or self.session_id is not None
                or self.started_at is not None
                or self.ended_at is not None
                or self.elapsed_seconds is not None
                or any(counts)
                or self.error_type is not None
                or self.error_message is not None
            ):
                raise ValueError("UNRESOLVED capture must stop before session creation")
            return
        if self.selection.status is not LiquidityTopNEvaluationStatus.RESOLVED:
            raise ValueError("terminal capture requires a resolved selection")
        if not self.session_id or self.started_at is None or self.ended_at is None:
            raise ValueError("terminal capture requires session identity and timestamps")
        _require_utc(self.started_at, "started_at")
        _require_utc(self.ended_at, "ended_at")
        if self.ended_at < self.started_at:
            raise ValueError("ended_at must not be before started_at")
        if (
            self.elapsed_seconds is None
            or not self.elapsed_seconds.is_finite()
            or self.elapsed_seconds < 0
        ):
            raise ValueError("terminal capture requires non-negative elapsed_seconds")
        if self.status is MicrostructureCaptureStatus.COMPLETED:
            if self.error_type is not None or self.error_message is not None:
                raise ValueError("COMPLETED capture must not contain an error")
        elif self.status is MicrostructureCaptureStatus.FAILED:
            if not self.error_type or self.error_message is None:
                raise ValueError("FAILED capture requires structured error information")
        else:
            raise ValueError(f"unsupported capture status: {self.status}")


@dataclass(slots=True)
class _CaptureCounts:
    received_trade: int = 0
    persisted_trade: int = 0
    received_orderbook: int = 0
    persisted_orderbook: int = 0
    duplicates: int = 0
    lifecycle: int = 0
    reconnects: int = 0
    receive_ordinal: int = 0


async def record_microstructure(
    quote_currency: str,
    window_end: datetime,
    as_of: datetime,
    database: str | Path,
    maximum_snapshot_age: timedelta,
    top_n: int,
    duration: timedelta,
    *,
    orderbook_units: int = 15,
    selection_evaluator: SelectionEvaluator | None = None,
    stream_adapter: UpbitMicrostructureStreamAdapter | None = None,
    clock: Clock | None = None,
    monotonic: MonotonicClock | None = None,
    duration_sleep: DurationSleep = asyncio.sleep,
    session_id_factory: IdFactory | None = None,
    event_id_factory: IdFactory | None = None,
    event_limit: int | None = None,
) -> MicrostructureCaptureResult:
    """Evaluate top-N once, then own one repository for the bounded capture."""

    normalized_quote, normalized_end, cutoff = validate_microstructure_request(
        quote_currency,
        window_end,
        as_of,
        maximum_snapshot_age,
        top_n,
        duration,
        orderbook_units,
        event_limit,
    )
    evaluator = selection_evaluator or evaluate_liquidity_top_n_as_of
    selection = evaluator(
        normalized_quote,
        normalized_end,
        cutoff,
        database,
        maximum_snapshot_age,
        top_n,
    )
    if selection.status is LiquidityTopNEvaluationStatus.UNRESOLVED:
        return _unresolved_result(selection, duration, orderbook_units)

    effective_clock = clock or (lambda: datetime.now(UTC))
    effective_monotonic = monotonic or __import__("time").monotonic
    adapter = stream_adapter or UpbitMicrostructureStreamAdapter()

    def stream_factory(
        markets: tuple[str, ...],
        units: int,
        callback: LifecycleCallback,
    ) -> AsyncIterator[MicrostructureFrame]:
        return adapter.stream(
            markets,
            orderbook_units=units,
            lifecycle_callback=callback,
        )

    with _owned_repository(database) as repository:
        return await record_microstructure_with_dependencies(
            selection,
            repository,
            duration,
            orderbook_units=orderbook_units,
            stream_factory=stream_factory,
            clock=effective_clock,
            monotonic=effective_monotonic,
            duration_sleep=duration_sleep,
            session_id_factory=session_id_factory or (lambda: uuid4().hex),
            event_id_factory=event_id_factory or (lambda: uuid4().hex),
            event_limit=event_limit,
        )


async def record_microstructure_with_dependencies(
    selection: LiquidityTopNResult,
    repository: SqliteMinuteCandleRepository,
    duration: timedelta,
    *,
    orderbook_units: int,
    stream_factory: StreamFactory,
    clock: Clock,
    monotonic: MonotonicClock,
    duration_sleep: DurationSleep,
    session_id_factory: IdFactory,
    event_id_factory: IdFactory,
    event_limit: int | None = None,
) -> MicrostructureCaptureResult:
    """Capture through injected time, stream, and already-open storage boundaries."""

    validate_capture_duration(duration)
    validate_orderbook_units(orderbook_units)
    _validate_event_limit(event_limit)
    if selection.status is not LiquidityTopNEvaluationStatus.RESOLVED:
        raise ValueError("capture dependencies require a resolved top-N selection")
    markets = _selected_market_results(selection)
    session_id = session_id_factory()
    started_at = _require_utc(clock(), "started_at")
    if started_at < selection.as_of:
        raise ValueError("started_at must not be before selection as_of")
    start_monotonic = _finite_monotonic(monotonic(), "start monotonic")
    repository.create_microstructure_capture_session(
        _session_input(
            selection,
            session_id,
            duration,
            orderbook_units,
            started_at,
        ),
        tuple(
            MicrostructureSessionMarketInput(
                item.market,
                item.liquidity_rank,
                item.quote_volume,
            )
            for item in markets
        ),
    )
    counts = _CaptureCounts()
    error: Exception | None = None
    cancelled: asyncio.CancelledError | None = None

    async def lifecycle_callback(event: StreamLifecycleEvent) -> None:
        observed = _require_utc(clock(), "lifecycle observed_at")
        available = _require_utc(clock(), "lifecycle available_at")
        lifecycle_ordinal = counts.lifecycle + 1
        repository.record_microstructure_lifecycle_event(
            session_id=session_id,
            lifecycle_ordinal=lifecycle_ordinal,
            status=event.status.value,
            attempt=event.attempt,
            delay_seconds=(
                None if event.delay_seconds is None else Decimal(str(event.delay_seconds))
            ),
            detail=event.detail,
            observed_at=observed,
            available_at=available,
        )
        counts.lifecycle = lifecycle_ordinal
        if event.status is LifecycleStatus.RECONNECTING:
            counts.reconnects += 1

    try:
        if markets:
            stream = stream_factory(
                tuple(item.market for item in markets),
                orderbook_units,
                lifecycle_callback,
            )
            await _consume_bounded(
                stream,
                duration,
                duration_sleep,
                event_limit,
                lambda frame: _persist_frame(
                    frame,
                    repository,
                    session_id,
                    counts,
                    clock,
                    event_id_factory,
                ),
            )
    except asyncio.CancelledError as caught:
        cancelled = caught
    except Exception as caught:
        error = caught

    try:
        end_monotonic = _finite_monotonic(monotonic(), "end monotonic")
        ended_at = _require_utc(clock(), "ended_at")
        available_at = _require_utc(clock(), "terminal available_at")
    except BaseException as finalization_error:
        if cancelled is not None:
            cancelled.add_note(f"failed to read terminal clocks: {finalization_error}")
            raise cancelled from finalization_error
        if error is not None:
            error.add_note(f"failed to read terminal clocks: {finalization_error}")
            raise error from finalization_error
        raise

    if end_monotonic < start_monotonic:
        clock_error = ValueError("monotonic clock moved backward")
        if cancelled is not None:
            cancelled.add_note(str(clock_error))
        elif error is not None:
            error.add_note(str(clock_error))
        else:
            error = clock_error
    elapsed_seconds = Decimal(str(max(0.0, end_monotonic - start_monotonic)))
    if ended_at < started_at or available_at < ended_at:
        terminal_clock_error = ValueError(
            "terminal clocks must satisfy started_at <= ended_at <= available_at"
        )
        if cancelled is not None:
            cancelled.add_note(str(terminal_clock_error))
            raise cancelled
        elif error is not None:
            error.add_note(str(terminal_clock_error))
            raise error
        else:
            raise terminal_clock_error

    if cancelled is not None:
        _record_terminal_preserving(
            repository,
            session_id,
            MicrostructureTerminalStatus.CANCELLED,
            ended_at,
            available_at,
            elapsed_seconds,
            counts,
            cancelled,
        )
        raise cancelled

    if error is not None:
        _record_terminal_preserving(
            repository,
            session_id,
            MicrostructureTerminalStatus.FAILED,
            ended_at,
            available_at,
            elapsed_seconds,
            counts,
            error,
        )
        return _terminal_result(
            MicrostructureCaptureStatus.FAILED,
            selection,
            markets,
            session_id,
            duration,
            orderbook_units,
            started_at,
            ended_at,
            elapsed_seconds,
            counts,
            error,
        )

    repository.record_microstructure_terminal(
        session_id=session_id,
        status=MicrostructureTerminalStatus.COMPLETED,
        ended_at=ended_at,
        available_at=available_at,
        elapsed_seconds=elapsed_seconds,
        received_trade_count=counts.received_trade,
        persisted_trade_count=counts.persisted_trade,
        received_orderbook_count=counts.received_orderbook,
        persisted_orderbook_count=counts.persisted_orderbook,
        duplicate_count=counts.duplicates,
        lifecycle_count=counts.lifecycle,
        reconnect_count=counts.reconnects,
    )
    return _terminal_result(
        MicrostructureCaptureStatus.COMPLETED,
        selection,
        markets,
        session_id,
        duration,
        orderbook_units,
        started_at,
        ended_at,
        elapsed_seconds,
        counts,
        None,
    )


async def _consume_bounded(
    stream: AsyncIterator[MicrostructureFrame],
    duration: timedelta,
    duration_sleep: DurationSleep,
    event_limit: int | None,
    persist: Callable[[MicrostructureFrame], None],
) -> None:
    deadline = asyncio.create_task(duration_sleep(duration.total_seconds()))
    body_error: BaseException | None = None
    try:
        received = 0
        while event_limit is None or received < event_limit:
            next_event = asyncio.create_task(anext(stream))
            done, _ = await asyncio.wait(
                (deadline, next_event), return_when=asyncio.FIRST_COMPLETED
            )
            if deadline in done:
                await deadline
                next_event.cancel()
                try:
                    await next_event
                except (asyncio.CancelledError, StopAsyncIteration):
                    pass
                break
            try:
                frame = await next_event
            except StopAsyncIteration as error:
                raise RuntimeError("microstructure stream ended before duration") from error
            persist(frame)
            received += 1
    except BaseException as caught:
        body_error = caught
    finally:
        if not deadline.done():
            deadline.cancel()
            try:
                await deadline
            except BaseException as deadline_error:
                if not isinstance(deadline_error, asyncio.CancelledError):
                    if body_error is None:
                        body_error = deadline_error
                    else:
                        body_error.add_note(f"failed to stop capture deadline: {deadline_error}")
        close = getattr(stream, "aclose", None)
        if close is not None:
            try:
                await close()
            except BaseException as close_error:
                if body_error is None:
                    body_error = close_error
                else:
                    body_error.add_note(f"failed to close microstructure stream: {close_error}")
    if body_error is not None:
        raise body_error


def _persist_frame(
    frame: MicrostructureFrame,
    repository: SqliteMinuteCandleRepository,
    session_id: str,
    counts: _CaptureCounts,
    clock: Clock,
    event_id_factory: IdFactory,
) -> None:
    if isinstance(frame.event, TradeEvent):
        counts.received_trade += 1
    elif isinstance(frame.event, OrderbookEvent):
        counts.received_orderbook += 1
    else:
        raise ValueError("unsupported microstructure frame event")
    counts.receive_ordinal += 1
    available_at = _require_utc(clock(), "event available_at")
    result = repository.record_microstructure_event(
        event_id=event_id_factory(),
        session_id=session_id,
        receive_ordinal=counts.receive_ordinal,
        raw_payload=frame.raw_payload,
        event=frame.event,
        available_at=available_at,
    )
    if isinstance(frame.event, TradeEvent):
        counts.persisted_trade += 1
    else:
        counts.persisted_orderbook += 1
    if result.is_duplicate:
        counts.duplicates += 1


def validate_microstructure_request(
    quote_currency: str,
    window_end: datetime,
    as_of: datetime,
    maximum_snapshot_age: timedelta,
    top_n: int,
    duration: timedelta,
    orderbook_units: int,
    event_limit: int | None = None,
) -> tuple[str, datetime, datetime]:
    """Validate every caller-controlled value before repository or network access."""

    normalized_quote = normalize_quote_currency(quote_currency)
    if normalized_quote != "KRW":
        raise ValueError("microstructure capture requires KRW quote currency")
    normalized_end = _require_utc(window_end, "window_end")
    cutoff = _require_utc(as_of, "as_of")
    if normalized_end.second or normalized_end.microsecond:
        raise ValueError("window_end must align to a one-minute UTC boundary")
    if cutoff < normalized_end:
        raise ValueError("as_of must not be before window_end")
    try:
        normalized_end - timedelta(days=30)
    except OverflowError as error:
        raise ValueError("window_end does not support a 30-day lookback") from error
    validate_maximum_snapshot_age(maximum_snapshot_age)
    validate_top_n(top_n)
    validate_capture_duration(duration)
    validate_orderbook_units(orderbook_units)
    _validate_event_limit(event_limit)
    return normalized_quote, normalized_end, cutoff


def validate_capture_duration(duration: timedelta) -> None:
    if (
        not isinstance(duration, timedelta)
        or duration <= timedelta(0)
        or duration > _MAXIMUM_CAPTURE_DURATION
    ):
        raise ValueError("duration must be a positive timedelta no greater than 24 hours")


def _validate_event_limit(event_limit: int | None) -> None:
    if event_limit is not None and (
        not isinstance(event_limit, int) or isinstance(event_limit, bool) or event_limit < 1
    ):
        raise ValueError("event_limit must be a positive integer when provided")


def _selected_market_results(
    selection: LiquidityTopNResult,
) -> tuple[MicrostructureSelectedMarketResult, ...]:
    return tuple(
        MicrostructureSelectedMarketResult(
            decision.market,
            decision.liquidity_rank,
            decision.quote_volume,
        )
        for decision in selection.decisions
        if decision.status is LiquidityTopNDecisionStatus.SELECTED
        and decision.liquidity_rank is not None
        and decision.quote_volume is not None
    )


def _session_input(
    selection: LiquidityTopNResult,
    session_id: str,
    duration: timedelta,
    orderbook_units: int,
    started_at: datetime,
) -> MicrostructureCaptureSessionInput:
    if (
        selection.snapshot_id is None
        or selection.observed_at is None
        or selection.available_at is None
        or selection.content_hash is None
    ):
        raise ValueError("resolved top-N selection requires snapshot metadata")
    return MicrostructureCaptureSessionInput(
        session_id=session_id,
        source="upbit",
        quote_currency=selection.quote_currency,
        window_start=selection.window_start,
        window_end=selection.window_end,
        as_of=selection.as_of,
        lookback=selection.lookback,
        maximum_snapshot_age=selection.maximum_snapshot_age,
        top_n=selection.top_n,
        top_n_policy_name=selection.policy_name,
        top_n_policy_version=selection.policy_version,
        liquidity_policy_name=selection.liquidity_policy_name,
        liquidity_policy_version=selection.liquidity_policy_version,
        catalog_policy_name=selection.catalog_policy_name,
        catalog_policy_version=selection.catalog_policy_version,
        snapshot_id=selection.snapshot_id,
        snapshot_observed_at=selection.observed_at,
        snapshot_available_at=selection.available_at,
        content_hash=selection.content_hash,
        catalog_candidate_count=selection.catalog_candidate_count,
        catalog_excluded_count=selection.catalog_excluded_count,
        liquidity_candidate_count=selection.liquidity_candidate_count,
        liquidity_resolved_count=selection.liquidity_resolved_count,
        liquidity_unresolved_count=selection.liquidity_unresolved_count,
        orderbook_units=orderbook_units,
        requested_duration=duration,
        started_at=started_at,
    )


def _record_terminal_preserving(
    repository: SqliteMinuteCandleRepository,
    session_id: str,
    status: MicrostructureTerminalStatus,
    ended_at: datetime,
    available_at: datetime,
    elapsed_seconds: Decimal,
    counts: _CaptureCounts,
    error: BaseException,
) -> None:
    try:
        repository.record_microstructure_terminal(
            session_id=session_id,
            status=status,
            ended_at=ended_at,
            available_at=available_at,
            elapsed_seconds=elapsed_seconds,
            received_trade_count=counts.received_trade,
            persisted_trade_count=counts.persisted_trade,
            received_orderbook_count=counts.received_orderbook,
            persisted_orderbook_count=counts.persisted_orderbook,
            duplicate_count=counts.duplicates,
            lifecycle_count=counts.lifecycle,
            reconnect_count=counts.reconnects,
            error_type=type(error).__name__,
            error_message=str(error),
        )
    except BaseException as terminal_error:
        error.add_note(f"failed to record terminal evidence: {terminal_error}")


def _terminal_result(
    status: MicrostructureCaptureStatus,
    selection: LiquidityTopNResult,
    markets: tuple[MicrostructureSelectedMarketResult, ...],
    session_id: str,
    duration: timedelta,
    orderbook_units: int,
    started_at: datetime,
    ended_at: datetime,
    elapsed_seconds: Decimal,
    counts: _CaptureCounts,
    error: Exception | None,
) -> MicrostructureCaptureResult:
    return MicrostructureCaptureResult(
        status=status,
        selection=selection,
        markets=markets,
        session_id=session_id,
        requested_duration=duration,
        orderbook_units=orderbook_units,
        started_at=started_at,
        ended_at=ended_at,
        elapsed_seconds=elapsed_seconds,
        received_trade_count=counts.received_trade,
        persisted_trade_count=counts.persisted_trade,
        received_orderbook_count=counts.received_orderbook,
        persisted_orderbook_count=counts.persisted_orderbook,
        duplicate_count=counts.duplicates,
        lifecycle_count=counts.lifecycle,
        reconnect_count=counts.reconnects,
        error_type=None if error is None else type(error).__name__,
        error_message=None if error is None else str(error),
    )


def _unresolved_result(
    selection: LiquidityTopNResult,
    duration: timedelta,
    orderbook_units: int,
) -> MicrostructureCaptureResult:
    return MicrostructureCaptureResult(
        status=MicrostructureCaptureStatus.UNRESOLVED,
        selection=selection,
        markets=(),
        session_id=None,
        requested_duration=duration,
        orderbook_units=orderbook_units,
        started_at=None,
        ended_at=None,
        elapsed_seconds=None,
        received_trade_count=0,
        persisted_trade_count=0,
        received_orderbook_count=0,
        persisted_orderbook_count=0,
        duplicate_count=0,
        lifecycle_count=0,
        reconnect_count=0,
        error_type=None,
        error_message=None,
    )


@contextmanager
def _owned_repository(database: str | Path):
    repository = SqliteMinuteCandleRepository(database)
    try:
        yield repository
    except BaseException as error:
        try:
            repository.close()
        except BaseException as close_error:
            error.add_note(f"failed to close SQLite repository: {close_error}")
        raise
    else:
        repository.close()


def capture_market_payload(result: MicrostructureSelectedMarketResult) -> dict[str, object]:
    """Return one deterministic selected-market JSON record."""

    return {
        "type": "market",
        "market": result.market,
        "liquidity_rank": result.liquidity_rank,
        "quote_volume": str(result.quote_volume),
    }


def capture_summary_payload(result: MicrostructureCaptureResult) -> dict[str, object]:
    """Return deterministic terminal metadata without raw market events."""

    selection = result.selection
    return {
        "type": "summary",
        "status": result.status.value,
        "session_id": result.session_id,
        "quote_currency": selection.quote_currency,
        "window_start": _utc_text(selection.window_start),
        "window_end": _utc_text(selection.window_end),
        "as_of": _utc_text(selection.as_of),
        "maximum_snapshot_age_seconds": _duration_seconds(selection.maximum_snapshot_age),
        "top_n": selection.top_n,
        "top_n_policy_name": selection.policy_name,
        "top_n_policy_version": selection.policy_version,
        "liquidity_policy_name": selection.liquidity_policy_name,
        "liquidity_policy_version": selection.liquidity_policy_version,
        "catalog_policy_name": selection.catalog_policy_name,
        "catalog_policy_version": selection.catalog_policy_version,
        "snapshot_id": selection.snapshot_id,
        "observed_at": _optional_utc_text(selection.observed_at),
        "available_at": _optional_utc_text(selection.available_at),
        "content_hash": selection.content_hash,
        "catalog_candidate_count": selection.catalog_candidate_count,
        "catalog_excluded_count": selection.catalog_excluded_count,
        "liquidity_candidate_count": selection.liquidity_candidate_count,
        "liquidity_resolved_count": selection.liquidity_resolved_count,
        "liquidity_unresolved_count": selection.liquidity_unresolved_count,
        "selected_count": len(result.markets),
        "duration_seconds": _duration_seconds(result.requested_duration),
        "orderbook_units": result.orderbook_units,
        "started_at": _optional_utc_text(result.started_at),
        "ended_at": _optional_utc_text(result.ended_at),
        "elapsed_seconds": None if result.elapsed_seconds is None else str(result.elapsed_seconds),
        "received_trade_count": result.received_trade_count,
        "persisted_trade_count": result.persisted_trade_count,
        "received_orderbook_count": result.received_orderbook_count,
        "persisted_orderbook_count": result.persisted_orderbook_count,
        "duplicate_count": result.duplicate_count,
        "lifecycle_count": result.lifecycle_count,
        "reconnect_count": result.reconnect_count,
        "error": (
            None
            if result.error_type is None
            else {"type": result.error_type, "message": result.error_message}
        ),
    }


def _finite_monotonic(value: float, name: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value):
        raise ValueError(f"{name} must be finite")
    return float(value)


def _require_utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be timezone-aware UTC")
    return value.astimezone(UTC)


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _optional_utc_text(value: datetime | None) -> str | None:
    return None if value is None else _utc_text(value)


def _duration_seconds(value: timedelta) -> int | float:
    microseconds = (value.days * 86_400 + value.seconds) * 1_000_000 + value.microseconds
    return microseconds // 1_000_000 if microseconds % 1_000_000 == 0 else microseconds / 1_000_000
