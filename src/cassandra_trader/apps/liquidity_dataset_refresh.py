"""Incremental run-once refresh for the rolling liquidity dataset."""

import asyncio
import time
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from uuid import uuid4

from upbit import AsyncUpbit

from cassandra_trader.adapters.storage.sqlite import (
    MinuteCandleBackfillRunStatus,
    SqliteMinuteCandleRepository,
    StoredMarketCatalogSnapshot,
    StoredMinuteCandleBackfillRun,
)
from cassandra_trader.adapters.upbit.public_data import UpbitPublicDataAdapter
from cassandra_trader.apps.liquidity_dataset_bootstrap import (
    BootstrapNetworkPolicy,
    Clock,
    MonotonicClock,
    ResilientMinuteCandleFetcher,
    RetryDecider,
    Sleep,
    is_transient_minute_candle_error,
)
from cassandra_trader.apps.market_universe import (
    MarketCatalogCaptureResult,
    SnapshotIdFactory,
    capture_market_catalog_into_repository,
)
from cassandra_trader.apps.minute_candle_backfill import (
    FetchMinuteCandlePage,
    MinuteCandleBackfillResult,
    backfill_minute_candles_into_repository,
)
from cassandra_trader.apps.minute_slots import (
    fully_covered_minute_evidence_intervals,
    summarize_minute_slots_from_repository,
)
from cassandra_trader.apps.multi_market_dataset import normalize_krw_markets
from cassandra_trader.domain.market import (
    MarketUniverseResult,
    MarketUniverseStatus,
    normalize_quote_currency,
)
from cassandra_trader.domain.market_eligibility import (
    MARKET_ELIGIBILITY_POLICY_NAME,
    MARKET_ELIGIBILITY_POLICY_VERSION,
    MarketEligibilityEvaluationStatus,
    MarketEligibilityReason,
    MarketEligibilityResult,
    evaluate_market_eligibility,
    validate_maximum_snapshot_age,
)
from cassandra_trader.domain.market_liquidity import MARKET_LIQUIDITY_LOOKBACK
from cassandra_trader.domain.minute_slot import MinuteSlotSummary

_EXPECTED_MINUTE_COUNT = 43_200


class LiquidityDatasetRefreshStatus(StrEnum):
    """Terminal status for one catalog-and-candle refresh cycle."""

    SUCCEEDED = "SUCCEEDED"
    PARTIAL_FAILURE = "PARTIAL_FAILURE"
    CATALOG_FAILED = "CATALOG_FAILED"
    UNRESOLVED = "UNRESOLVED"


class LiquidityDatasetRefreshMarketStatus(StrEnum):
    """Terminal status for one analysis-universe market."""

    UPDATED = "UPDATED"
    SKIPPED_COMPLETE = "SKIPPED_COMPLETE"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class MinuteBackfillRange:
    """One exact minute-aligned half-open range planned for bounded backfill."""

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        for name, value in (("start", self.start), ("end", self.end)):
            _require_utc(value, name)
            if value.second != 0 or value.microsecond != 0:
                raise ValueError(f"{name} must align to a one-minute UTC boundary")
        if self.start >= self.end:
            raise ValueError("start must be before end")


@dataclass(frozen=True, slots=True)
class LiquidityDatasetRefreshMarketResult:
    """Plan, completed gap backfills, and terminal coverage for one market."""

    market: str
    status: LiquidityDatasetRefreshMarketStatus
    initial_summary: MinuteSlotSummary
    planned_ranges: tuple[MinuteBackfillRange, ...]
    backfills: tuple[MinuteCandleBackfillResult, ...]
    final_summary: MinuteSlotSummary
    error_type: str | None
    error_message: str | None

    def __post_init__(self) -> None:
        if self.initial_summary.market != self.market or self.final_summary.market != self.market:
            raise ValueError("market summaries must match the refresh market")
        if (
            self.initial_summary.start_time_utc != self.final_summary.start_time_utc
            or self.initial_summary.end_time_utc != self.final_summary.end_time_utc
        ):
            raise ValueError("initial and final summaries must cover the same range")
        if not isinstance(self.planned_ranges, tuple) or not isinstance(self.backfills, tuple):
            raise ValueError("planned ranges and backfills must be immutable tuples")
        if self.planned_ranges != tuple(sorted(self.planned_ranges, key=lambda item: item.start)):
            raise ValueError("planned ranges must be chronological")
        if any(
            current.end > following.start
            for current, following in zip(
                self.planned_ranges,
                self.planned_ranges[1:],
                strict=False,
            )
        ):
            raise ValueError("planned ranges must not overlap")
        if len(self.backfills) > len(self.planned_ranges):
            raise ValueError("completed backfills cannot exceed planned ranges")
        for planned, backfill in zip(self.planned_ranges, self.backfills, strict=False):
            if backfill.requested_start != planned.start or backfill.requested_end != planned.end:
                raise ValueError("backfill results must match the planned range prefix")

        if self.status is LiquidityDatasetRefreshMarketStatus.UPDATED:
            if (
                not self.planned_ranges
                or len(self.backfills) != len(self.planned_ranges)
                or self.final_summary.unresolved_count
                or self.error_type is not None
                or self.error_message is not None
            ):
                raise ValueError("UPDATED requires completed gaps and resolved final coverage")
        elif self.status is LiquidityDatasetRefreshMarketStatus.SKIPPED_COMPLETE:
            if (
                self.initial_summary.unresolved_count
                or self.planned_ranges
                or self.backfills
                or self.final_summary.unresolved_count
                or self.error_type is not None
                or self.error_message is not None
            ):
                raise ValueError("SKIPPED_COMPLETE requires complete coverage without backfill")
        elif self.status is LiquidityDatasetRefreshMarketStatus.FAILED:
            if not self.error_type or self.error_message is None:
                raise ValueError("FAILED requires structured error information")
        else:
            raise ValueError(f"unsupported refresh market status: {self.status}")


@dataclass(frozen=True, slots=True)
class LiquidityDatasetRefreshSummary:
    """Reproducible metadata and aggregate outcome for one refresh cycle."""

    status: LiquidityDatasetRefreshStatus
    reasons: tuple[MarketEligibilityReason, ...]
    quote_currency: str
    maximum_snapshot_age: timedelta
    catalog_policy_name: str
    catalog_policy_version: str
    snapshot_id: str | None
    observed_at: datetime | None
    available_at: datetime | None
    content_hash: str | None
    planning_as_of: datetime | None
    window_start: datetime | None
    window_end: datetime | None
    coverage_as_of: datetime | None
    catalog_candidate_count: int
    catalog_excluded_count: int
    candidate_count: int
    updated_count: int
    skipped_count: int
    failed_count: int
    requested_gap_count: int
    pages_fetched: int
    error_type: str | None
    error_message: str | None

    def __post_init__(self) -> None:
        if self.quote_currency != "KRW":
            raise ValueError("liquidity dataset refresh requires KRW quote currency")
        validate_maximum_snapshot_age(self.maximum_snapshot_age)
        if (
            self.catalog_policy_name != MARKET_ELIGIBILITY_POLICY_NAME
            or self.catalog_policy_version != MARKET_ELIGIBILITY_POLICY_VERSION
        ):
            raise ValueError("catalog policy identifier must match the eligibility contract")
        if not isinstance(self.reasons, tuple) or any(
            not isinstance(reason, MarketEligibilityReason) for reason in self.reasons
        ):
            raise ValueError("reasons must be an immutable eligibility-reason tuple")
        counts = (
            self.catalog_candidate_count,
            self.catalog_excluded_count,
            self.candidate_count,
            self.updated_count,
            self.skipped_count,
            self.failed_count,
            self.requested_gap_count,
            self.pages_fetched,
        )
        if any(
            not isinstance(count, int) or isinstance(count, bool) or count < 0 for count in counts
        ):
            raise ValueError("refresh counts must be non-negative integers")
        if self.candidate_count != self.updated_count + self.skipped_count + self.failed_count:
            raise ValueError("market outcome counts must add up to candidate_count")

        snapshot_fields = (self.snapshot_id, self.observed_at, self.available_at)
        if any(value is None for value in snapshot_fields):
            if any(value is not None for value in snapshot_fields):
                raise ValueError(
                    "snapshot identity and timestamps must be entirely present or absent"
                )
        else:
            _require_utc(self.observed_at, "observed_at")
            _require_utc(self.available_at, "available_at")
            if self.observed_at > self.available_at:
                raise ValueError("observed_at must not be after available_at")

        cycle_fields = (
            self.planning_as_of,
            self.window_start,
            self.window_end,
            self.coverage_as_of,
        )
        if self.status is LiquidityDatasetRefreshStatus.CATALOG_FAILED:
            if (
                self.candidate_count
                or self.reasons
                or self.content_hash is not None
                or any(value is not None for value in cycle_fields)
                or not self.error_type
                or self.error_message is None
            ):
                raise ValueError("CATALOG_FAILED must stop before planning the candle cycle")
            return

        if any(value is None for value in snapshot_fields + cycle_fields):
            raise ValueError("completed catalog capture requires all cycle timestamps")
        if self.content_hash is None or len(self.content_hash) != 64:
            raise ValueError("successful catalog capture requires a content hash")
        for name, value in (
            ("planning_as_of", self.planning_as_of),
            ("window_start", self.window_start),
            ("window_end", self.window_end),
            ("coverage_as_of", self.coverage_as_of),
        ):
            _require_utc(value, name)
        if self.window_end - self.window_start != MARKET_LIQUIDITY_LOOKBACK:
            raise ValueError("refresh window must span exactly 30 days")
        if self.window_start.second or self.window_start.microsecond:
            raise ValueError("window_start must align to a minute boundary")
        if self.window_end.second or self.window_end.microsecond:
            raise ValueError("window_end must align to a minute boundary")
        if not (
            self.window_end
            <= self.observed_at
            <= self.available_at
            <= self.planning_as_of
            <= self.coverage_as_of
        ):
            raise ValueError("refresh timestamps must be monotonically ordered")

        if self.status is LiquidityDatasetRefreshStatus.UNRESOLVED:
            if self.candidate_count or not self.reasons or self.error_type is not None:
                raise ValueError("UNRESOLVED must fail closed before market selection")
        elif self.status is LiquidityDatasetRefreshStatus.SUCCEEDED:
            if self.failed_count or self.reasons or self.error_type is not None:
                raise ValueError("SUCCEEDED refresh must not contain failures")
            if self.catalog_candidate_count != self.catalog_excluded_count + self.candidate_count:
                raise ValueError("resolved catalog counts must match analysis candidates")
        elif self.status is LiquidityDatasetRefreshStatus.PARTIAL_FAILURE:
            if self.failed_count < 1 or self.reasons or self.error_type is not None:
                raise ValueError("PARTIAL_FAILURE requires one or more failed markets")
            if self.catalog_candidate_count != self.catalog_excluded_count + self.candidate_count:
                raise ValueError("resolved catalog counts must match analysis candidates")
        else:
            raise ValueError(f"unsupported refresh status: {self.status}")


@dataclass(frozen=True, slots=True)
class LiquidityDatasetRefreshResult:
    """Market-code ordered records and their one terminal summary."""

    markets: tuple[LiquidityDatasetRefreshMarketResult, ...]
    summary: LiquidityDatasetRefreshSummary

    def __post_init__(self) -> None:
        if self.markets != tuple(sorted(self.markets, key=lambda item: item.market)):
            raise ValueError("refresh markets must be ordered by market code")
        if len({item.market for item in self.markets}) != len(self.markets):
            raise ValueError("refresh markets must not contain duplicates")
        if len(self.markets) != self.summary.candidate_count:
            raise ValueError("market records must match candidate_count")
        actual_counts = (
            sum(
                item.status is LiquidityDatasetRefreshMarketStatus.UPDATED for item in self.markets
            ),
            sum(
                item.status is LiquidityDatasetRefreshMarketStatus.SKIPPED_COMPLETE
                for item in self.markets
            ),
            sum(item.status is LiquidityDatasetRefreshMarketStatus.FAILED for item in self.markets),
        )
        if actual_counts != (
            self.summary.updated_count,
            self.summary.skipped_count,
            self.summary.failed_count,
        ):
            raise ValueError("summary counts must match market statuses")
        if (
            sum(_attempted_gap_count(item) for item in self.markets)
            != self.summary.requested_gap_count
        ):
            raise ValueError("summary requested_gap_count must match attempted market gaps")
        if (
            sum(backfill.pages_fetched for item in self.markets for backfill in item.backfills)
            != self.summary.pages_fetched
        ):
            raise ValueError("summary pages_fetched must match completed backfills")
        for item in self.markets:
            if (
                item.initial_summary.as_of != self.summary.planning_as_of
                or item.final_summary.as_of != self.summary.coverage_as_of
                or item.initial_summary.start_time_utc != self.summary.window_start
                or item.initial_summary.end_time_utc != self.summary.window_end
            ):
                raise ValueError("market summaries must match the cycle window and cutoffs")


@dataclass(frozen=True, slots=True)
class _Attempt:
    market: str
    initial_summary: MinuteSlotSummary
    planned_ranges: tuple[MinuteBackfillRange, ...]
    backfills: tuple[MinuteCandleBackfillResult, ...]
    error: Exception | None


class IncompleteRefreshCoverageError(RuntimeError):
    """Raised internally when successful gap requests did not resolve the plan."""


def plan_incremental_backfill_ranges(
    runs: tuple[StoredMinuteCandleBackfillRun, ...],
    window_start: datetime,
    window_end: datetime,
    as_of: datetime,
) -> tuple[MinuteBackfillRange, ...]:
    """Return the chronological complement of successful minute evidence."""

    covered = fully_covered_minute_evidence_intervals(
        runs,
        window_start,
        window_end,
        as_of,
    )
    gaps: list[MinuteBackfillRange] = []
    cursor = window_start

    for covered_start, covered_end in covered:
        if cursor < covered_start:
            gaps.append(MinuteBackfillRange(cursor, covered_start))
        cursor = max(cursor, covered_end)

    if cursor < window_end:
        gaps.append(MinuteBackfillRange(cursor, window_end))

    return tuple(gaps)


async def refresh_liquidity_dataset(
    quote_currency: str,
    database: str | Path,
    maximum_snapshot_age: timedelta,
    *,
    network_policy: BootstrapNetworkPolicy | None = None,
    clock: Clock | None = None,
    monotonic: MonotonicClock | None = None,
    sleep: Sleep = asyncio.sleep,
    retry_decider: RetryDecider | None = None,
    snapshot_id_factory: SnapshotIdFactory | None = None,
) -> LiquidityDatasetRefreshResult:
    """Capture the current catalog and incrementally refresh eligible KRW markets."""

    normalized_quote, policy = _validated_inputs(
        quote_currency,
        maximum_snapshot_age,
        network_policy,
    )
    effective_clock = clock or _utc_now
    with _owned_repository(database) as repository:
        async with _owned_upbit_client() as client:
            adapter = UpbitPublicDataAdapter(
                client.trading_pairs.list,
                client.candles.list_minutes,
                clock=effective_clock,
            )
            return await _refresh_into_repository(
                normalized_quote,
                maximum_snapshot_age,
                repository,
                adapter,
                adapter.fetch_minute_candle_batch,
                policy,
                effective_clock,
                monotonic or time.monotonic,
                sleep,
                retry_decider or is_transient_minute_candle_error,
                snapshot_id_factory,
            )


async def refresh_liquidity_dataset_with_ports(
    quote_currency: str,
    database: str | Path,
    maximum_snapshot_age: timedelta,
    *,
    catalog_adapter: UpbitPublicDataAdapter,
    fetch_page: FetchMinuteCandlePage,
    network_policy: BootstrapNetworkPolicy | None = None,
    clock: Clock | None = None,
    monotonic: MonotonicClock | None = None,
    sleep: Sleep = asyncio.sleep,
    retry_decider: RetryDecider | None = None,
    snapshot_id_factory: SnapshotIdFactory | None = None,
) -> LiquidityDatasetRefreshResult:
    """Run a refresh through injected catalog and candle network ports."""

    normalized_quote, policy = _validated_inputs(
        quote_currency,
        maximum_snapshot_age,
        network_policy,
    )
    effective_clock = clock or _utc_now
    with _owned_repository(database) as repository:
        return await _refresh_into_repository(
            normalized_quote,
            maximum_snapshot_age,
            repository,
            catalog_adapter,
            fetch_page,
            policy,
            effective_clock,
            monotonic or time.monotonic,
            sleep,
            retry_decider or is_transient_minute_candle_error,
            snapshot_id_factory,
        )


async def _refresh_into_repository(
    quote_currency: str,
    maximum_snapshot_age: timedelta,
    repository: SqliteMinuteCandleRepository,
    catalog_adapter: UpbitPublicDataAdapter,
    fetch_page: FetchMinuteCandlePage,
    network_policy: BootstrapNetworkPolicy,
    clock: Clock,
    monotonic: MonotonicClock,
    sleep: Sleep,
    retry_decider: RetryDecider,
    snapshot_id_factory: SnapshotIdFactory | None,
) -> LiquidityDatasetRefreshResult:
    snapshot_id = (snapshot_id_factory or (lambda: uuid4().hex))()

    try:
        capture = await capture_market_catalog_into_repository(
            catalog_adapter,
            repository,
            quote_currency=quote_currency,
            clock=clock,
            snapshot_id_factory=lambda: snapshot_id,
        )
    except asyncio.CancelledError:
        raise
    except Exception as error:
        stored = repository.get_market_catalog_snapshot(snapshot_id)
        return LiquidityDatasetRefreshResult(
            markets=(),
            summary=_catalog_failure_summary(
                quote_currency,
                maximum_snapshot_age,
                stored,
                error,
            ),
        )

    window_end = _floor_minute(capture.observed_at)
    try:
        window_start = window_end - MARKET_LIQUIDITY_LOOKBACK
    except OverflowError as error:
        raise ValueError("captured observation does not support a 30-day lookback") from error
    if int((window_end - window_start) // timedelta(minutes=1)) != _EXPECTED_MINUTE_COUNT:
        raise RuntimeError("refresh window must contain exactly 43,200 minute slots")
    planning_as_of = _require_utc(clock(), "planning_as_of")
    if planning_as_of < capture.available_at:
        raise ValueError("planning_as_of must not be before catalog available_at")

    eligibility = evaluate_market_eligibility(
        MarketUniverseResult(
            status=MarketUniverseStatus.AVAILABLE,
            quote_currency=quote_currency,
            as_of=planning_as_of,
            snapshot_id=capture.snapshot_id,
            observed_at=capture.observed_at,
            available_at=capture.available_at,
            content_hash=capture.content_hash,
            pairs=capture.pairs,
        ),
        maximum_snapshot_age,
    )
    if eligibility.status is MarketEligibilityEvaluationStatus.UNRESOLVED:
        return LiquidityDatasetRefreshResult(
            markets=(),
            summary=_cycle_summary(
                status=LiquidityDatasetRefreshStatus.UNRESOLVED,
                eligibility=eligibility,
                capture=capture,
                window_start=window_start,
                window_end=window_end,
                planning_as_of=planning_as_of,
                coverage_as_of=planning_as_of,
                attempts=(),
            ),
        )

    if not eligibility.eligible_pairs:
        return LiquidityDatasetRefreshResult(
            markets=(),
            summary=_cycle_summary(
                status=LiquidityDatasetRefreshStatus.SUCCEEDED,
                eligibility=eligibility,
                capture=capture,
                window_start=window_start,
                window_end=window_end,
                planning_as_of=planning_as_of,
                coverage_as_of=planning_as_of,
                attempts=(),
            ),
        )

    resilient_fetcher = ResilientMinuteCandleFetcher(
        fetch_page,
        network_policy,
        sleep=sleep,
        monotonic=monotonic,
        clock=clock,
        retry_decider=retry_decider,
    )
    attempts = await _update_markets(
        normalize_krw_markets(pair.code for pair in eligibility.eligible_pairs),
        window_start,
        window_end,
        planning_as_of,
        repository,
        resilient_fetcher,
    )
    coverage_as_of = _require_utc(clock(), "coverage_as_of")
    if coverage_as_of < planning_as_of:
        raise ValueError("coverage_as_of must not be before planning_as_of")
    results = tuple(
        _market_result(
            attempt,
            window_start,
            window_end,
            coverage_as_of,
            repository,
        )
        for attempt in attempts
    )
    status = (
        LiquidityDatasetRefreshStatus.PARTIAL_FAILURE
        if any(result.status is LiquidityDatasetRefreshMarketStatus.FAILED for result in results)
        else LiquidityDatasetRefreshStatus.SUCCEEDED
    )
    return LiquidityDatasetRefreshResult(
        markets=results,
        summary=_cycle_summary(
            status=status,
            eligibility=eligibility,
            capture=capture,
            window_start=window_start,
            window_end=window_end,
            planning_as_of=planning_as_of,
            coverage_as_of=coverage_as_of,
            attempts=results,
        ),
    )


async def _update_markets(
    markets: tuple[str, ...],
    window_start: datetime,
    window_end: datetime,
    planning_as_of: datetime,
    repository: SqliteMinuteCandleRepository,
    fetch_page: FetchMinuteCandlePage,
) -> tuple[_Attempt, ...]:
    attempts: list[_Attempt] = []

    for market in markets:
        initial = summarize_minute_slots_from_repository(
            market,
            window_start,
            window_end,
            planning_as_of,
            repository,
        )
        if initial.unresolved_count == 0:
            attempts.append(_Attempt(market, initial, (), (), None))
            continue
        runs = repository.list_minute_candle_backfill_runs(
            market,
            window_start,
            window_end,
            planning_as_of,
            status=MinuteCandleBackfillRunStatus.SUCCEEDED,
        )
        planned_ranges = plan_incremental_backfill_ranges(
            runs,
            window_start,
            window_end,
            planning_as_of,
        )
        backfills: list[MinuteCandleBackfillResult] = []
        error: Exception | None = None

        for planned in planned_ranges:
            try:
                backfills.append(
                    await backfill_minute_candles_into_repository(
                        market,
                        planned.start,
                        planned.end,
                        repository,
                        fetch_page=fetch_page,
                        observed_at=planning_as_of,
                    )
                )
            except Exception as caught:
                error = caught
                break

        attempts.append(_Attempt(market, initial, planned_ranges, tuple(backfills), error))

    return tuple(attempts)


def _market_result(
    attempt: _Attempt,
    window_start: datetime,
    window_end: datetime,
    coverage_as_of: datetime,
    repository: SqliteMinuteCandleRepository,
) -> LiquidityDatasetRefreshMarketResult:
    final = summarize_minute_slots_from_repository(
        attempt.market,
        window_start,
        window_end,
        coverage_as_of,
        repository,
    )
    error = attempt.error
    if error is None and final.unresolved_count:
        error = IncompleteRefreshCoverageError(
            f"{final.unresolved_count} minute slots remain unresolved after planned backfills"
        )
    if error is not None:
        status = LiquidityDatasetRefreshMarketStatus.FAILED
    elif not attempt.planned_ranges:
        status = LiquidityDatasetRefreshMarketStatus.SKIPPED_COMPLETE
    else:
        status = LiquidityDatasetRefreshMarketStatus.UPDATED
    return LiquidityDatasetRefreshMarketResult(
        market=attempt.market,
        status=status,
        initial_summary=attempt.initial_summary,
        planned_ranges=attempt.planned_ranges,
        backfills=attempt.backfills,
        final_summary=final,
        error_type=None if error is None else type(error).__name__,
        error_message=None if error is None else str(error),
    )


def refresh_market_payload(result: LiquidityDatasetRefreshMarketResult) -> dict[str, object]:
    """Return one deterministic JSON-ready refresh market record."""

    return {
        "type": "market",
        "market": result.market,
        "status": result.status.value,
        "initial_slot_summary": _slot_summary_payload(result.initial_summary),
        "planned_gap_count": len(result.planned_ranges),
        "planned_ranges": [
            {"start": _utc_text(item.start), "end": _utc_text(item.end)}
            for item in result.planned_ranges
        ],
        "backfills": [_backfill_payload(backfill) for backfill in result.backfills],
        "final_slot_summary": _slot_summary_payload(result.final_summary),
        "error": (
            None
            if result.error_type is None
            else {"type": result.error_type, "message": result.error_message}
        ),
    }


def refresh_summary_payload(summary: LiquidityDatasetRefreshSummary) -> dict[str, object]:
    """Return one deterministic JSON-ready refresh summary record."""

    return {
        "type": "summary",
        "status": summary.status.value,
        "reasons": [reason.value for reason in summary.reasons],
        "catalog_policy_name": summary.catalog_policy_name,
        "catalog_policy_version": summary.catalog_policy_version,
        "quote_currency": summary.quote_currency,
        "maximum_snapshot_age_seconds": _duration_seconds(summary.maximum_snapshot_age),
        "snapshot_id": summary.snapshot_id,
        "observed_at": _optional_utc_text(summary.observed_at),
        "available_at": _optional_utc_text(summary.available_at),
        "content_hash": summary.content_hash,
        "planning_as_of": _optional_utc_text(summary.planning_as_of),
        "window_start": _optional_utc_text(summary.window_start),
        "window_end": _optional_utc_text(summary.window_end),
        "coverage_as_of": _optional_utc_text(summary.coverage_as_of),
        "catalog_candidate_count": summary.catalog_candidate_count,
        "catalog_excluded_count": summary.catalog_excluded_count,
        "candidate_count": summary.candidate_count,
        "updated_count": summary.updated_count,
        "skipped_count": summary.skipped_count,
        "failed_count": summary.failed_count,
        "requested_gap_count": summary.requested_gap_count,
        "pages_fetched": summary.pages_fetched,
        "error": (
            None
            if summary.error_type is None
            else {"type": summary.error_type, "message": summary.error_message}
        ),
    }


def _cycle_summary(
    *,
    status: LiquidityDatasetRefreshStatus,
    eligibility: MarketEligibilityResult,
    capture: MarketCatalogCaptureResult,
    window_start: datetime,
    window_end: datetime,
    planning_as_of: datetime,
    coverage_as_of: datetime,
    attempts: tuple[LiquidityDatasetRefreshMarketResult, ...],
) -> LiquidityDatasetRefreshSummary:
    return LiquidityDatasetRefreshSummary(
        status=status,
        reasons=eligibility.reasons,
        quote_currency=eligibility.quote_currency,
        maximum_snapshot_age=eligibility.maximum_snapshot_age,
        catalog_policy_name=eligibility.policy_name,
        catalog_policy_version=eligibility.policy_version,
        snapshot_id=capture.snapshot_id,
        observed_at=capture.observed_at,
        available_at=capture.available_at,
        content_hash=capture.content_hash,
        planning_as_of=planning_as_of,
        window_start=window_start,
        window_end=window_end,
        coverage_as_of=coverage_as_of,
        catalog_candidate_count=eligibility.candidate_count,
        catalog_excluded_count=eligibility.excluded_count,
        candidate_count=len(attempts),
        updated_count=sum(
            attempt.status is LiquidityDatasetRefreshMarketStatus.UPDATED for attempt in attempts
        ),
        skipped_count=sum(
            attempt.status is LiquidityDatasetRefreshMarketStatus.SKIPPED_COMPLETE
            for attempt in attempts
        ),
        failed_count=sum(
            attempt.status is LiquidityDatasetRefreshMarketStatus.FAILED for attempt in attempts
        ),
        requested_gap_count=sum(_attempted_gap_count(attempt) for attempt in attempts),
        pages_fetched=sum(
            backfill.pages_fetched for attempt in attempts for backfill in attempt.backfills
        ),
        error_type=None,
        error_message=None,
    )


def _catalog_failure_summary(
    quote_currency: str,
    maximum_snapshot_age: timedelta,
    stored: StoredMarketCatalogSnapshot | None,
    error: Exception,
) -> LiquidityDatasetRefreshSummary:
    return LiquidityDatasetRefreshSummary(
        status=LiquidityDatasetRefreshStatus.CATALOG_FAILED,
        reasons=(),
        quote_currency=quote_currency,
        maximum_snapshot_age=maximum_snapshot_age,
        catalog_policy_name=MARKET_ELIGIBILITY_POLICY_NAME,
        catalog_policy_version=MARKET_ELIGIBILITY_POLICY_VERSION,
        snapshot_id=None if stored is None else stored.snapshot_id,
        observed_at=None if stored is None else stored.observed_at,
        available_at=None if stored is None else stored.available_at,
        content_hash=None,
        planning_as_of=None,
        window_start=None,
        window_end=None,
        coverage_as_of=None,
        catalog_candidate_count=0,
        catalog_excluded_count=0,
        candidate_count=0,
        updated_count=0,
        skipped_count=0,
        failed_count=0,
        requested_gap_count=0,
        pages_fetched=0,
        error_type=type(error).__name__,
        error_message=str(error),
    )


def _slot_summary_payload(summary: MinuteSlotSummary) -> dict[str, object]:
    return {
        "expected_count": summary.expected_count,
        "traded_count": summary.traded_count,
        "no_trade_count": summary.no_trade_count,
        "unresolved_count": summary.unresolved_count,
        "quote_volume": str(summary.quote_volume),
        "as_of": _utc_text(summary.as_of),
    }


def _attempted_gap_count(result: LiquidityDatasetRefreshMarketResult) -> int:
    completed = len(result.backfills)
    if completed == len(result.planned_ranges):
        return completed
    return completed + 1


def _backfill_payload(result: MinuteCandleBackfillResult) -> dict[str, object]:
    return {
        "pages_fetched": result.pages_fetched,
        "raw_received": result.raw_received,
        "canonical_new": result.canonical_new,
        "canonical_revised": result.canonical_revised,
        "canonical_unchanged": result.canonical_unchanged,
        "earliest_candle_time": _optional_utc_text(result.earliest_candle_time),
        "latest_candle_time": _optional_utc_text(result.latest_candle_time),
        "requested_start": _utc_text(result.requested_start),
        "requested_end": _utc_text(result.requested_end),
    }


def _validated_inputs(
    quote_currency: str,
    maximum_snapshot_age: timedelta,
    network_policy: BootstrapNetworkPolicy | None,
) -> tuple[str, BootstrapNetworkPolicy]:
    validate_maximum_snapshot_age(maximum_snapshot_age)
    normalized_quote = normalize_quote_currency(quote_currency)
    if normalized_quote != "KRW":
        raise ValueError("liquidity dataset refresh requires KRW quote currency")
    policy = network_policy if network_policy is not None else BootstrapNetworkPolicy()
    if not isinstance(policy, BootstrapNetworkPolicy):
        raise ValueError("network_policy must be a BootstrapNetworkPolicy")
    return normalized_quote, policy


def _floor_minute(value: datetime) -> datetime:
    return value.replace(second=0, microsecond=0)


@contextmanager
def _owned_repository(
    database: str | Path,
) -> Iterator[SqliteMinuteCandleRepository]:
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


@asynccontextmanager
async def _owned_upbit_client() -> AsyncIterator[AsyncUpbit]:
    client = AsyncUpbit(environment="kr", timeout=10.0, max_retries=0)
    entered_client = await client.__aenter__()
    try:
        yield entered_client
    except BaseException as error:
        try:
            await client.__aexit__(type(error), error, error.__traceback__)
        except BaseException as close_error:
            error.add_note(f"failed to close Upbit client: {close_error}")
        raise
    else:
        await client.__aexit__(None, None, None)


def _require_utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be timezone-aware UTC")
    return value.astimezone(UTC)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _optional_utc_text(value: datetime | None) -> str | None:
    return None if value is None else _utc_text(value)


def _duration_seconds(value: timedelta) -> int | float:
    microseconds = (value.days * 86_400 + value.seconds) * 1_000_000 + value.microseconds
    if microseconds % 1_000_000 == 0:
        return microseconds // 1_000_000
    return microseconds / 1_000_000
