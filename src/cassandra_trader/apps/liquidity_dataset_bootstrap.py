"""Resumable sequential bootstrap for the full KRW analysis universe."""

import asyncio
import math
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from enum import StrEnum
from pathlib import Path

from upbit import APIConnectionError, APIStatusError, AsyncUpbit

from cassandra_trader.adapters.storage.sqlite import SqliteMinuteCandleRepository
from cassandra_trader.adapters.upbit.public_data import UpbitPublicDataAdapter
from cassandra_trader.apps.market_eligibility import evaluate_market_eligibility_as_of
from cassandra_trader.apps.minute_candle_backfill import (
    FetchMinuteCandlePage,
    MinuteCandleBackfillResult,
    backfill_minute_candles_into_repository,
)
from cassandra_trader.apps.minute_slots import summarize_minute_slots_from_repository
from cassandra_trader.apps.multi_market_dataset import normalize_krw_markets
from cassandra_trader.domain.market import normalize_quote_currency
from cassandra_trader.domain.market_eligibility import (
    MarketEligibilityEvaluationStatus,
    MarketEligibilityReason,
    MarketEligibilityResult,
    validate_maximum_snapshot_age,
)
from cassandra_trader.domain.market_liquidity import MARKET_LIQUIDITY_LOOKBACK
from cassandra_trader.domain.minute_slot import MinuteSlotSummary

type Clock = Callable[[], datetime]
type MonotonicClock = Callable[[], float]
type Sleep = Callable[[float], Awaitable[None]]
type RetryDecider = Callable[[Exception], bool]

_EXPECTED_MINUTE_COUNT = 43_200


class LiquidityDatasetBootstrapStatus(StrEnum):
    """Terminal status for the complete bootstrap request."""

    SUCCEEDED = "SUCCEEDED"
    PARTIAL_FAILURE = "PARTIAL_FAILURE"
    UNRESOLVED = "UNRESOLVED"


class LiquidityDatasetMarketStatus(StrEnum):
    """Terminal status for one analysis-universe market."""

    SUCCEEDED = "SUCCEEDED"
    SKIPPED_COMPLETE = "SKIPPED_COMPLETE"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class BootstrapNetworkPolicy:
    """Bounded sequential rate and retry policy for public candle requests."""

    minimum_interval_seconds: float = 0.125
    maximum_attempts: int = 4
    initial_backoff_seconds: float = 1.0
    maximum_delay_seconds: float = 30.0

    def __post_init__(self) -> None:
        _validate_finite_number(
            self.minimum_interval_seconds,
            "minimum_interval_seconds",
            minimum=0,
            maximum=60,
        )
        if (
            not isinstance(self.maximum_attempts, int)
            or isinstance(self.maximum_attempts, bool)
            or not 1 <= self.maximum_attempts <= 10
        ):
            raise ValueError("maximum_attempts must be an integer between 1 and 10")
        _validate_finite_number(
            self.initial_backoff_seconds,
            "initial_backoff_seconds",
            minimum=0,
            maximum=300,
        )
        _validate_finite_number(
            self.maximum_delay_seconds,
            "maximum_delay_seconds",
            minimum=0,
            maximum=300,
            inclusive_minimum=False,
        )


@dataclass(frozen=True, slots=True)
class LiquidityDatasetMarketResult:
    """Initial checkpoint, terminal coverage, and one market outcome."""

    market: str
    status: LiquidityDatasetMarketStatus
    initial_summary: MinuteSlotSummary
    final_summary: MinuteSlotSummary
    backfill: MinuteCandleBackfillResult | None
    error_type: str | None
    error_message: str | None

    def __post_init__(self) -> None:
        if self.initial_summary.market != self.market or self.final_summary.market != self.market:
            raise ValueError("market summaries must match the bootstrap market")
        if (
            self.initial_summary.start_time_utc != self.final_summary.start_time_utc
            or self.initial_summary.end_time_utc != self.final_summary.end_time_utc
        ):
            raise ValueError("initial and final summaries must cover the same range")
        if self.status is LiquidityDatasetMarketStatus.SUCCEEDED:
            if (
                self.backfill is None
                or self.error_type is not None
                or self.error_message is not None
            ):
                raise ValueError("SUCCEEDED market requires backfill without an error")
            if self.final_summary.unresolved_count != 0:
                raise ValueError("SUCCEEDED market requires complete final slot coverage")
        elif self.status is LiquidityDatasetMarketStatus.SKIPPED_COMPLETE:
            if (
                self.initial_summary.unresolved_count != 0
                or self.backfill is not None
                or self.error_type is not None
                or self.error_message is not None
            ):
                raise ValueError("SKIPPED_COMPLETE requires a complete initial checkpoint")
            if self.final_summary.unresolved_count != 0:
                raise ValueError("SKIPPED_COMPLETE must remain complete at the final cutoff")
        elif self.status is LiquidityDatasetMarketStatus.FAILED:
            if self.backfill is not None or not self.error_type or self.error_message is None:
                raise ValueError("FAILED market requires an error and no completed backfill")
        else:
            raise ValueError(f"unsupported bootstrap market status: {self.status}")


@dataclass(frozen=True, slots=True)
class LiquidityDatasetBootstrapSummary:
    """Reproducible request, snapshot, timing, and aggregate bootstrap outcome."""

    status: LiquidityDatasetBootstrapStatus
    reasons: tuple[MarketEligibilityReason, ...]
    quote_currency: str
    window_start: datetime
    window_end: datetime
    universe_as_of: datetime
    resume_as_of: datetime
    final_coverage_as_of: datetime
    maximum_snapshot_age: timedelta
    catalog_policy_name: str
    catalog_policy_version: str
    snapshot_id: str | None
    observed_at: datetime | None
    available_at: datetime | None
    content_hash: str | None
    catalog_candidate_count: int
    catalog_excluded_count: int
    candidate_count: int
    succeeded_count: int
    skipped_count: int
    failed_count: int

    def __post_init__(self) -> None:
        if self.quote_currency != "KRW":
            raise ValueError("liquidity dataset bootstrap requires KRW quote currency")
        for name, value in (
            ("window_start", self.window_start),
            ("window_end", self.window_end),
            ("universe_as_of", self.universe_as_of),
            ("resume_as_of", self.resume_as_of),
            ("final_coverage_as_of", self.final_coverage_as_of),
        ):
            _require_utc(value, name)
        if self.window_end - self.window_start != MARKET_LIQUIDITY_LOOKBACK:
            raise ValueError("bootstrap window must span exactly 30 days")
        if not (
            self.window_end <= self.universe_as_of <= self.resume_as_of <= self.final_coverage_as_of
        ):
            raise ValueError("bootstrap timestamps must be monotonically ordered")
        counts = (
            self.catalog_candidate_count,
            self.catalog_excluded_count,
            self.candidate_count,
            self.succeeded_count,
            self.skipped_count,
            self.failed_count,
        )
        if any(
            not isinstance(count, int) or isinstance(count, bool) or count < 0 for count in counts
        ):
            raise ValueError("bootstrap counts must be non-negative integers")
        if self.candidate_count != (self.succeeded_count + self.skipped_count + self.failed_count):
            raise ValueError("bootstrap market counts must add up to candidate_count")
        if self.status is LiquidityDatasetBootstrapStatus.SUCCEEDED:
            if self.failed_count or self.reasons:
                raise ValueError("SUCCEEDED bootstrap must not contain failures or reasons")
            if self.catalog_candidate_count != (self.catalog_excluded_count + self.candidate_count):
                raise ValueError("resolved bootstrap counts must match catalog eligibility")
        elif self.status is LiquidityDatasetBootstrapStatus.PARTIAL_FAILURE:
            if self.failed_count < 1 or self.reasons:
                raise ValueError("PARTIAL_FAILURE bootstrap requires failed markets")
            if self.catalog_candidate_count != (self.catalog_excluded_count + self.candidate_count):
                raise ValueError("resolved bootstrap counts must match catalog eligibility")
        elif self.status is LiquidityDatasetBootstrapStatus.UNRESOLVED:
            if self.candidate_count or not self.reasons:
                raise ValueError("UNRESOLVED bootstrap must fail before selecting candidates")
        else:
            raise ValueError(f"unsupported bootstrap status: {self.status}")


@dataclass(frozen=True, slots=True)
class LiquidityDatasetBootstrapResult:
    """Deterministically ordered market records followed by one summary."""

    markets: tuple[LiquidityDatasetMarketResult, ...]
    summary: LiquidityDatasetBootstrapSummary

    def __post_init__(self) -> None:
        if self.markets != tuple(sorted(self.markets, key=lambda item: item.market)):
            raise ValueError("bootstrap markets must be ordered by market code")
        if len({item.market for item in self.markets}) != len(self.markets):
            raise ValueError("bootstrap markets must not contain duplicates")
        if len(self.markets) != self.summary.candidate_count:
            raise ValueError("bootstrap market records must match candidate_count")
        actual_counts = (
            sum(item.status is LiquidityDatasetMarketStatus.SUCCEEDED for item in self.markets),
            sum(
                item.status is LiquidityDatasetMarketStatus.SKIPPED_COMPLETE
                for item in self.markets
            ),
            sum(item.status is LiquidityDatasetMarketStatus.FAILED for item in self.markets),
        )
        if actual_counts != (
            self.summary.succeeded_count,
            self.summary.skipped_count,
            self.summary.failed_count,
        ):
            raise ValueError("bootstrap summary counts must match market statuses")
        for item in self.markets:
            if (
                item.initial_summary.start_time_utc != self.summary.window_start
                or item.initial_summary.end_time_utc != self.summary.window_end
                or item.initial_summary.as_of != self.summary.resume_as_of
                or item.final_summary.as_of != self.summary.final_coverage_as_of
            ):
                raise ValueError("bootstrap market summaries must match summary cutoffs")


@dataclass(frozen=True, slots=True)
class _Request:
    quote_currency: str
    universe_as_of: datetime
    window_start: datetime
    window_end: datetime
    resume_as_of: datetime
    maximum_snapshot_age: timedelta


@dataclass(frozen=True, slots=True)
class _Attempt:
    market: str
    status: LiquidityDatasetMarketStatus
    initial_summary: MinuteSlotSummary
    backfill: MinuteCandleBackfillResult | None
    error: Exception | None


class _ResilientFetcher:
    def __init__(
        self,
        fetch_page: FetchMinuteCandlePage,
        policy: BootstrapNetworkPolicy,
        *,
        sleep: Sleep,
        monotonic: MonotonicClock,
        clock: Clock,
        retry_decider: RetryDecider,
    ) -> None:
        self._fetch_page = fetch_page
        self._policy = policy
        self._sleep = sleep
        self._monotonic = monotonic
        self._clock = clock
        self._retry_decider = retry_decider
        self._last_request_at: float | None = None

    async def __call__(self, *args: object, **kwargs: object):
        for attempt in range(1, self._policy.maximum_attempts + 1):
            await self._wait_for_rate_limit()
            try:
                return await self._fetch_page(*args, **kwargs)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                if attempt >= self._policy.maximum_attempts or not self._retry_decider(error):
                    raise
                delay = _retry_delay(error, attempt, self._policy, self._clock)
                await self._sleep(delay)
        raise RuntimeError("retry loop terminated unexpectedly")

    async def _wait_for_rate_limit(self) -> None:
        now = self._monotonic()
        if not isinstance(now, (int, float)) or isinstance(now, bool) or not math.isfinite(now):
            raise ValueError("monotonic clock must return a finite number")
        if self._last_request_at is not None:
            remaining = self._policy.minimum_interval_seconds - (now - self._last_request_at)
            if remaining > 0:
                await self._sleep(remaining)
                now = self._monotonic()
        self._last_request_at = float(now)


async def bootstrap_liquidity_dataset(
    quote_currency: str,
    universe_as_of: datetime,
    window_end: datetime,
    database: str | Path,
    maximum_snapshot_age: timedelta,
    *,
    network_policy: BootstrapNetworkPolicy | None = None,
    clock: Clock | None = None,
    monotonic: MonotonicClock | None = None,
    sleep: Sleep = asyncio.sleep,
    retry_decider: RetryDecider | None = None,
) -> LiquidityDatasetBootstrapResult:
    """Bootstrap fresh non-warning KRW markets with one client and repository."""

    effective_clock = clock or _utc_now
    request = _validated_request(
        quote_currency,
        universe_as_of,
        window_end,
        maximum_snapshot_age,
        effective_clock,
    )
    policy = network_policy if network_policy is not None else BootstrapNetworkPolicy()
    if not isinstance(policy, BootstrapNetworkPolicy):
        raise ValueError("network_policy must be a BootstrapNetworkPolicy")

    with SqliteMinuteCandleRepository(database) as repository:
        eligibility = _eligibility(request, database, repository)
        early_result = _early_result(request, eligibility)
        if early_result is not None:
            return early_result

        async with AsyncUpbit(environment="kr", timeout=10.0, max_retries=0) as client:
            adapter = UpbitPublicDataAdapter(
                list_minute_candles=client.candles.list_minutes,
            )
            fetcher = _ResilientFetcher(
                adapter.fetch_minute_candle_batch,
                policy,
                sleep=sleep,
                monotonic=monotonic or time.monotonic,
                clock=effective_clock,
                retry_decider=retry_decider or _is_transient_error,
            )
            return await _run_bootstrap(
                request,
                eligibility,
                repository,
                fetcher,
                effective_clock,
            )


async def bootstrap_liquidity_dataset_with_fetcher(
    quote_currency: str,
    universe_as_of: datetime,
    window_end: datetime,
    database: str | Path,
    maximum_snapshot_age: timedelta,
    *,
    fetch_page: FetchMinuteCandlePage,
    network_policy: BootstrapNetworkPolicy | None = None,
    clock: Clock | None = None,
    monotonic: MonotonicClock | None = None,
    sleep: Sleep = asyncio.sleep,
    retry_decider: RetryDecider | None = None,
) -> LiquidityDatasetBootstrapResult:
    """Bootstrap through an injected page fetcher without real sleep or network."""

    effective_clock = clock or _utc_now
    request = _validated_request(
        quote_currency,
        universe_as_of,
        window_end,
        maximum_snapshot_age,
        effective_clock,
    )
    policy = network_policy if network_policy is not None else BootstrapNetworkPolicy()
    if not isinstance(policy, BootstrapNetworkPolicy):
        raise ValueError("network_policy must be a BootstrapNetworkPolicy")

    with SqliteMinuteCandleRepository(database) as repository:
        eligibility = _eligibility(request, database, repository)
        early_result = _early_result(request, eligibility)
        if early_result is not None:
            return early_result
        fetcher = _ResilientFetcher(
            fetch_page,
            policy,
            sleep=sleep,
            monotonic=monotonic or time.monotonic,
            clock=effective_clock,
            retry_decider=retry_decider or _is_transient_error,
        )
        return await _run_bootstrap(
            request,
            eligibility,
            repository,
            fetcher,
            effective_clock,
        )


def _eligibility(
    request: _Request,
    database: str | Path,
    repository: SqliteMinuteCandleRepository,
) -> MarketEligibilityResult:
    return evaluate_market_eligibility_as_of(
        request.quote_currency,
        request.universe_as_of,
        database,
        request.maximum_snapshot_age,
        repository=repository,
    )


def _early_result(
    request: _Request,
    eligibility: MarketEligibilityResult,
) -> LiquidityDatasetBootstrapResult | None:
    if eligibility.status is MarketEligibilityEvaluationStatus.UNRESOLVED:
        return LiquidityDatasetBootstrapResult(
            markets=(),
            summary=_summary(
                request,
                eligibility,
                status=LiquidityDatasetBootstrapStatus.UNRESOLVED,
                final_coverage_as_of=request.resume_as_of,
                attempts=(),
            ),
        )
    if eligibility.eligible_pairs:
        return None
    return LiquidityDatasetBootstrapResult(
        markets=(),
        summary=_summary(
            request,
            eligibility,
            status=LiquidityDatasetBootstrapStatus.SUCCEEDED,
            final_coverage_as_of=request.resume_as_of,
            attempts=(),
        ),
    )


async def _run_bootstrap(
    request: _Request,
    eligibility: MarketEligibilityResult,
    repository: SqliteMinuteCandleRepository,
    fetch_page: FetchMinuteCandlePage,
    clock: Clock,
) -> LiquidityDatasetBootstrapResult:
    markets = normalize_krw_markets(pair.code for pair in eligibility.eligible_pairs)
    attempts: list[_Attempt] = []

    for market in markets:
        initial = summarize_minute_slots_from_repository(
            market,
            request.window_start,
            request.window_end,
            request.resume_as_of,
            repository,
        )
        if initial.unresolved_count == 0:
            attempts.append(
                _Attempt(
                    market,
                    LiquidityDatasetMarketStatus.SKIPPED_COMPLETE,
                    initial,
                    None,
                    None,
                )
            )
            continue
        try:
            backfill = await backfill_minute_candles_into_repository(
                market,
                request.window_start,
                request.window_end,
                repository,
                fetch_page=fetch_page,
                observed_at=request.resume_as_of,
            )
        except Exception as error:
            attempts.append(
                _Attempt(
                    market,
                    LiquidityDatasetMarketStatus.FAILED,
                    initial,
                    None,
                    error,
                )
            )
        else:
            attempts.append(
                _Attempt(
                    market,
                    LiquidityDatasetMarketStatus.SUCCEEDED,
                    initial,
                    backfill,
                    None,
                )
            )

    final_coverage_as_of = _require_utc(clock(), "final_coverage_as_of")
    if final_coverage_as_of < request.resume_as_of:
        raise ValueError("final_coverage_as_of must not be before resume_as_of")
    results = tuple(
        _market_result(attempt, request, final_coverage_as_of, repository) for attempt in attempts
    )
    failed_count = sum(result.status is LiquidityDatasetMarketStatus.FAILED for result in results)
    status = (
        LiquidityDatasetBootstrapStatus.PARTIAL_FAILURE
        if failed_count
        else LiquidityDatasetBootstrapStatus.SUCCEEDED
    )
    return LiquidityDatasetBootstrapResult(
        markets=results,
        summary=_summary(
            request,
            eligibility,
            status=status,
            final_coverage_as_of=final_coverage_as_of,
            attempts=results,
        ),
    )


def _market_result(
    attempt: _Attempt,
    request: _Request,
    final_coverage_as_of: datetime,
    repository: SqliteMinuteCandleRepository,
) -> LiquidityDatasetMarketResult:
    final = summarize_minute_slots_from_repository(
        attempt.market,
        request.window_start,
        request.window_end,
        final_coverage_as_of,
        repository,
    )
    return LiquidityDatasetMarketResult(
        market=attempt.market,
        status=attempt.status,
        initial_summary=attempt.initial_summary,
        final_summary=final,
        backfill=attempt.backfill,
        error_type=None if attempt.error is None else type(attempt.error).__name__,
        error_message=None if attempt.error is None else str(attempt.error),
    )


def _summary(
    request: _Request,
    eligibility: MarketEligibilityResult,
    *,
    status: LiquidityDatasetBootstrapStatus,
    final_coverage_as_of: datetime,
    attempts: tuple[LiquidityDatasetMarketResult, ...],
) -> LiquidityDatasetBootstrapSummary:
    return LiquidityDatasetBootstrapSummary(
        status=status,
        reasons=eligibility.reasons,
        quote_currency=request.quote_currency,
        window_start=request.window_start,
        window_end=request.window_end,
        universe_as_of=request.universe_as_of,
        resume_as_of=request.resume_as_of,
        final_coverage_as_of=final_coverage_as_of,
        maximum_snapshot_age=request.maximum_snapshot_age,
        catalog_policy_name=eligibility.policy_name,
        catalog_policy_version=eligibility.policy_version,
        snapshot_id=eligibility.snapshot_id,
        observed_at=eligibility.observed_at,
        available_at=eligibility.available_at,
        content_hash=eligibility.content_hash,
        catalog_candidate_count=eligibility.candidate_count,
        catalog_excluded_count=eligibility.excluded_count,
        candidate_count=len(attempts),
        succeeded_count=sum(
            attempt.status is LiquidityDatasetMarketStatus.SUCCEEDED for attempt in attempts
        ),
        skipped_count=sum(
            attempt.status is LiquidityDatasetMarketStatus.SKIPPED_COMPLETE for attempt in attempts
        ),
        failed_count=sum(
            attempt.status is LiquidityDatasetMarketStatus.FAILED for attempt in attempts
        ),
    )


def bootstrap_market_payload(result: LiquidityDatasetMarketResult) -> dict[str, object]:
    """Return one stable JSON-ready bootstrap market record."""

    return {
        "type": "market",
        "market": result.market,
        "status": result.status.value,
        "initial_slot_summary": _slot_summary_payload(result.initial_summary),
        "final_slot_summary": _slot_summary_payload(result.final_summary),
        "backfill": _backfill_payload(result.backfill),
        "error": (
            None
            if result.error_type is None
            else {"type": result.error_type, "message": result.error_message}
        ),
    }


def bootstrap_summary_payload(
    summary: LiquidityDatasetBootstrapSummary,
) -> dict[str, object]:
    """Return one stable JSON-ready bootstrap summary record."""

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
        "window_start": _utc_text(summary.window_start),
        "window_end": _utc_text(summary.window_end),
        "universe_as_of": _utc_text(summary.universe_as_of),
        "resume_as_of": _utc_text(summary.resume_as_of),
        "coverage_as_of": _utc_text(summary.final_coverage_as_of),
        "catalog_candidate_count": summary.catalog_candidate_count,
        "catalog_excluded_count": summary.catalog_excluded_count,
        "candidate_count": summary.candidate_count,
        "succeeded_count": summary.succeeded_count,
        "skipped_count": summary.skipped_count,
        "failed_count": summary.failed_count,
    }


def _slot_summary_payload(summary: MinuteSlotSummary) -> dict[str, object]:
    return {
        "expected_count": summary.expected_count,
        "traded_count": summary.traded_count,
        "no_trade_count": summary.no_trade_count,
        "unresolved_count": summary.unresolved_count,
        "quote_volume": str(summary.quote_volume),
        "as_of": _utc_text(summary.as_of),
    }


def _backfill_payload(result: MinuteCandleBackfillResult | None) -> dict[str, object] | None:
    if result is None:
        return None
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


def _validated_request(
    quote_currency: str,
    universe_as_of: datetime,
    window_end: datetime,
    maximum_snapshot_age: timedelta,
    clock: Clock,
) -> _Request:
    validate_maximum_snapshot_age(maximum_snapshot_age)
    normalized_quote = normalize_quote_currency(quote_currency)
    if normalized_quote != "KRW":
        raise ValueError("liquidity dataset bootstrap requires KRW quote currency")
    cutoff = _require_utc(universe_as_of, "universe_as_of")
    normalized_end = _require_utc(window_end, "window_end")
    if normalized_end.second != 0 or normalized_end.microsecond != 0:
        raise ValueError("window_end must align to a one-minute UTC boundary")
    resume_as_of = _require_utc(clock(), "resume_as_of")
    if cutoff < normalized_end:
        raise ValueError("window_end must not be after universe_as_of")
    if normalized_end > resume_as_of:
        raise ValueError("window_end must not be after execution time")
    if cutoff > resume_as_of:
        raise ValueError("universe_as_of must not be after execution time")
    try:
        window_start = normalized_end - MARKET_LIQUIDITY_LOOKBACK
    except OverflowError as error:
        raise ValueError("window_end does not support a 30-day lookback") from error
    expected = int((normalized_end - window_start) // timedelta(minutes=1))
    if expected != _EXPECTED_MINUTE_COUNT:
        raise RuntimeError("30-day bootstrap window must contain exactly 43,200 minutes")
    return _Request(
        quote_currency=normalized_quote,
        universe_as_of=cutoff,
        window_start=window_start,
        window_end=normalized_end,
        resume_as_of=resume_as_of,
        maximum_snapshot_age=maximum_snapshot_age,
    )


def _is_transient_error(error: Exception) -> bool:
    if isinstance(error, (TimeoutError, ConnectionError, APIConnectionError)):
        return True
    return isinstance(error, APIStatusError) and (
        error.status_code == 429 or error.status_code >= 500
    )


def _retry_delay(
    error: Exception,
    attempt: int,
    policy: BootstrapNetworkPolicy,
    clock: Clock,
) -> float:
    retry_after = _retry_after_seconds(error, clock)
    if retry_after is not None:
        return min(retry_after, policy.maximum_delay_seconds)
    backoff = policy.initial_backoff_seconds * (2 ** (attempt - 1))
    return min(backoff, policy.maximum_delay_seconds)


def _retry_after_seconds(error: Exception, clock: Clock) -> float | None:
    response = getattr(error, "response", None)
    headers = getattr(response, "headers", None)
    if headers is None:
        return None
    value = headers.get("Retry-After")
    if value is None:
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        try:
            retry_at = parsedate_to_datetime(str(value))
        except (TypeError, ValueError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=UTC)
        seconds = (retry_at.astimezone(UTC) - _require_utc(clock(), "clock")).total_seconds()
    if not math.isfinite(seconds) or seconds < 0:
        return None
    return seconds


def _validate_finite_number(
    value: float,
    name: str,
    *,
    minimum: float,
    maximum: float,
    inclusive_minimum: bool = True,
) -> None:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
        or value > maximum
        or (value < minimum if inclusive_minimum else value <= minimum)
    ):
        qualifier = "at least" if inclusive_minimum else "greater than"
        raise ValueError(f"{name} must be finite, {qualifier} {minimum}, and at most {maximum}")


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
