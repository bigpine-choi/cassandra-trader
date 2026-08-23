"""Sequential multi-market minute-candle dataset backfill."""

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path

from upbit import AsyncUpbit

from cassandra_trader.adapters.storage.sqlite import (
    CanonicalMinuteCandleCoverage,
    SqliteMinuteCandleRepository,
)
from cassandra_trader.adapters.upbit.public_data import UpbitPublicDataAdapter
from cassandra_trader.apps.minute_candle_backfill import (
    FetchMinuteCandlePage,
    MinuteCandleBackfillResult,
    backfill_minute_candles_into_repository,
)


class DatasetMarketStatus(StrEnum):
    """Outcome of one market in a dataset backfill."""

    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class DatasetMarketResult:
    """Backfill, coverage, and error outcome for one normalized market."""

    market: str
    status: DatasetMarketStatus
    backfill: MinuteCandleBackfillResult | None
    coverage: CanonicalMinuteCandleCoverage | None
    error_type: str | None
    error_message: str | None


@dataclass(frozen=True, slots=True)
class DatasetBackfillSummary:
    """Aggregate outcome for one multi-market request."""

    requested_start: datetime
    requested_end: datetime
    observed_at: datetime
    coverage_as_of: datetime
    total: int
    succeeded: int
    failed: int


@dataclass(frozen=True, slots=True)
class DatasetBackfillResult:
    """Deterministically ordered market results and aggregate summary."""

    markets: tuple[DatasetMarketResult, ...]
    summary: DatasetBackfillSummary


@dataclass(frozen=True, slots=True)
class _BackfillAttempt:
    market: str
    backfill: MinuteCandleBackfillResult | None
    error: Exception | None


async def backfill_multi_market_dataset(
    markets: Iterable[str],
    start: datetime,
    end: datetime,
    database: str | Path,
    *,
    observed_at: datetime | None = None,
) -> DatasetBackfillResult:
    """Backfill normalized KRW markets with one client and one repository."""

    normalized_markets = normalize_krw_markets(markets)
    _validate_range(start, end)
    effective_observed_at = observed_at or datetime.now(UTC)
    _validate_utc(effective_observed_at, "observed_at")

    with SqliteMinuteCandleRepository(database) as repository:
        async with AsyncUpbit(
            environment="kr",
            timeout=10.0,
            max_retries=2,
        ) as client:
            adapter = UpbitPublicDataAdapter(
                list_minute_candles=client.candles.list_minutes,
            )
            return await _backfill_normalized_markets(
                normalized_markets,
                start,
                end,
                repository,
                fetch_page=adapter.fetch_minute_candle_batch,
                observed_at=effective_observed_at,
            )


async def backfill_multi_market_dataset_with_fetcher(
    markets: Iterable[str],
    start: datetime,
    end: datetime,
    database: str | Path,
    *,
    fetch_page: FetchMinuteCandlePage,
    observed_at: datetime | None = None,
    coverage_as_of: datetime | None = None,
) -> DatasetBackfillResult:
    """Backfill through an injected network boundary for deterministic tests."""

    normalized_markets = normalize_krw_markets(markets)
    _validate_range(start, end)
    effective_observed_at = observed_at or datetime.now(UTC)
    _validate_utc(effective_observed_at, "observed_at")

    if coverage_as_of is not None:
        _validate_utc(coverage_as_of, "coverage_as_of")

    with SqliteMinuteCandleRepository(database) as repository:
        return await _backfill_normalized_markets(
            normalized_markets,
            start,
            end,
            repository,
            fetch_page=fetch_page,
            observed_at=effective_observed_at,
            coverage_as_of=coverage_as_of,
        )


async def _backfill_normalized_markets(
    markets: tuple[str, ...],
    start: datetime,
    end: datetime,
    repository: SqliteMinuteCandleRepository,
    *,
    fetch_page: FetchMinuteCandlePage,
    observed_at: datetime,
    coverage_as_of: datetime | None = None,
) -> DatasetBackfillResult:
    attempts: list[_BackfillAttempt] = []

    for market in markets:
        try:
            backfill = await backfill_minute_candles_into_repository(
                market,
                start,
                end,
                repository,
                fetch_page=fetch_page,
                observed_at=observed_at,
            )
        except Exception as error:
            attempts.append(_BackfillAttempt(market, None, error))
        else:
            attempts.append(_BackfillAttempt(market, backfill, None))

    effective_coverage_as_of = coverage_as_of or datetime.now(UTC)
    _validate_utc(effective_coverage_as_of, "coverage_as_of")
    results = tuple(
        _market_result(
            attempt,
            repository,
            start,
            end,
            effective_coverage_as_of,
        )
        for attempt in attempts
    )
    succeeded = sum(result.status is DatasetMarketStatus.SUCCEEDED for result in results)

    return DatasetBackfillResult(
        markets=results,
        summary=DatasetBackfillSummary(
            requested_start=start,
            requested_end=end,
            observed_at=observed_at,
            coverage_as_of=effective_coverage_as_of,
            total=len(results),
            succeeded=succeeded,
            failed=len(results) - succeeded,
        ),
    )


def _market_result(
    attempt: _BackfillAttempt,
    repository: SqliteMinuteCandleRepository,
    start: datetime,
    end: datetime,
    coverage_as_of: datetime,
) -> DatasetMarketResult:
    try:
        coverage = repository.get_canonical_minute_candle_coverage(
            attempt.market,
            start,
            end,
            coverage_as_of,
        )
    except Exception as coverage_error:
        error = attempt.error or coverage_error
        return DatasetMarketResult(
            market=attempt.market,
            status=DatasetMarketStatus.FAILED,
            backfill=attempt.backfill,
            coverage=None,
            error_type=type(error).__name__,
            error_message=str(error),
        )

    if attempt.error is not None:
        return DatasetMarketResult(
            market=attempt.market,
            status=DatasetMarketStatus.FAILED,
            backfill=None,
            coverage=coverage,
            error_type=type(attempt.error).__name__,
            error_message=str(attempt.error),
        )

    return DatasetMarketResult(
        market=attempt.market,
        status=DatasetMarketStatus.SUCCEEDED,
        backfill=attempt.backfill,
        coverage=coverage,
        error_type=None,
        error_message=None,
    )


def normalize_krw_markets(markets: Iterable[str]) -> tuple[str, ...]:
    """Normalize, validate, deduplicate, and sort explicit Upbit KRW markets."""

    normalized = {market.strip().upper() for market in markets}

    if not normalized or "" in normalized:
        raise ValueError("at least one non-empty market is required")

    for market in normalized:
        quote, separator, base = market.partition("-")

        if (
            separator != "-"
            or quote != "KRW"
            or not base
            or not base.isascii()
            or not base.isalnum()
        ):
            raise ValueError(f"market must be an Upbit KRW market: {market}")

    return tuple(sorted(normalized))


def market_result_payload(result: DatasetMarketResult) -> dict[str, object]:
    """Return a stable JSON-ready mapping for one market result."""

    return {
        "type": "market",
        "market": result.market,
        "status": result.status.value,
        "backfill": _backfill_payload(result.backfill),
        "coverage": _coverage_payload(result.coverage),
        "error": (
            None
            if result.error_type is None
            else {"type": result.error_type, "message": result.error_message}
        ),
    }


def summary_payload(summary: DatasetBackfillSummary) -> dict[str, object]:
    """Return a stable JSON-ready mapping for a dataset summary."""

    return {
        "type": "summary",
        "requested_start": _utc_text(summary.requested_start),
        "requested_end": _utc_text(summary.requested_end),
        "observed_at": _utc_text(summary.observed_at),
        "coverage_as_of": _utc_text(summary.coverage_as_of),
        "total": summary.total,
        "succeeded": summary.succeeded,
        "failed": summary.failed,
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


def _coverage_payload(
    coverage: CanonicalMinuteCandleCoverage | None,
) -> dict[str, object] | None:
    if coverage is None:
        return None

    return {
        "expected_slots": coverage.expected_slots,
        "observed_slots": coverage.observed_slots,
        "absent_slots": coverage.absent_slots,
        "earliest_candle_time": _optional_utc_text(coverage.earliest_candle_time),
        "latest_candle_time": _optional_utc_text(coverage.latest_candle_time),
        "requested_start": _utc_text(coverage.requested_start),
        "requested_end": _utc_text(coverage.requested_end),
        "as_of": _utc_text(coverage.as_of),
    }


def _validate_range(start: datetime, end: datetime) -> None:
    _validate_minute_boundary(start, "start")
    _validate_minute_boundary(end, "end")

    if start >= end:
        raise ValueError("start must be before end")


def _validate_minute_boundary(value: datetime, name: str) -> None:
    _validate_utc(value, name)

    if value.second != 0 or value.microsecond != 0:
        raise ValueError(f"{name} must align to a one-minute UTC boundary")


def _validate_utc(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be timezone-aware UTC")


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _optional_utc_text(value: datetime | None) -> str | None:
    return None if value is None else _utc_text(value)
