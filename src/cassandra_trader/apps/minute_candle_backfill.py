"""Application service for paginated historical minute-candle ingestion."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

from upbit import AsyncUpbit

from cassandra_trader.adapters.storage.sqlite import (
    RawMinuteCandleInput,
    SqliteMinuteCandleRepository,
)
from cassandra_trader.adapters.upbit.public_data import (
    MinuteCandleObservation,
    MinuteCandleResponseBatch,
    UpbitPublicDataAdapter,
)

type FetchMinuteCandlePage = Callable[..., Awaitable[MinuteCandleResponseBatch]]

_PAGE_SIZE = 200


@dataclass(frozen=True, slots=True)
class MinuteCandleBackfillResult:
    """Aggregate outcome of a paginated historical backfill."""

    pages_fetched: int
    raw_received: int
    canonical_new: int
    canonical_revised: int
    canonical_unchanged: int
    earliest_candle_time: datetime | None
    latest_candle_time: datetime | None
    requested_start: datetime
    requested_end: datetime


async def backfill_minute_candles(
    market: str,
    start: datetime,
    end: datetime,
    database: str | Path,
) -> MinuteCandleBackfillResult:
    """Backfill one UTC range while reusing a single public Upbit client."""

    async with AsyncUpbit(
        environment="kr",
        timeout=10.0,
        max_retries=2,
    ) as client:
        adapter = UpbitPublicDataAdapter(
            list_minute_candles=client.candles.list_minutes,
        )
        return await backfill_minute_candles_with_fetcher(
            market,
            start,
            end,
            database,
            fetch_page=adapter.fetch_minute_candle_batch,
        )


async def backfill_minute_candles_with_fetcher(
    market: str,
    start: datetime,
    end: datetime,
    database: str | Path,
    *,
    fetch_page: FetchMinuteCandlePage,
    observed_at: datetime | None = None,
) -> MinuteCandleBackfillResult:
    """Fetch and persist pages using an injected network boundary."""

    with SqliteMinuteCandleRepository(database) as repository:
        return await backfill_minute_candles_into_repository(
            market,
            start,
            end,
            repository,
            fetch_page=fetch_page,
            observed_at=observed_at,
        )


async def backfill_minute_candles_into_repository(
    market: str,
    start: datetime,
    end: datetime,
    repository: SqliteMinuteCandleRepository,
    *,
    fetch_page: FetchMinuteCandlePage,
    observed_at: datetime | None = None,
) -> MinuteCandleBackfillResult:
    """Backfill a range into an already-open SQLite repository."""

    _validate_range(start, end)
    effective_observed_at = observed_at or datetime.now(UTC)
    _validate_utc(effective_observed_at, "observed_at")

    cursor = end
    pages_fetched = 0
    raw_received = 0
    canonical_new = 0
    canonical_revised = 0
    canonical_unchanged = 0
    candle_times: set[datetime] = set()

    while True:
        batch = await fetch_page(
            market,
            _PAGE_SIZE,
            as_of=effective_observed_at,
            to=cursor,
        )
        pages_fetched += 1
        raw_received += len(batch.observations)
        records = tuple(
            _raw_record(observation, start, end, effective_observed_at)
            for observation in batch.observations
        )
        stats = repository.ingest_batch(
            batch_id=str(uuid4()),
            source="upbit",
            endpoint=batch.endpoint,
            request_parameters=batch.request_parameters,
            received_at=batch.received_at,
            records=records,
        )
        canonical_new += stats.new_canonical_count
        canonical_revised += stats.revised_canonical_count
        canonical_unchanged += stats.unchanged_canonical_count

        candle_times.update(
            record.candle.start_time_utc for record in records if record.candle is not None
        )

        if not batch.observations:
            break

        ordered_observations = sorted(
            batch.observations,
            key=_observation_start,
        )
        earliest = _observation_start(ordered_observations[0])

        if earliest <= start:
            break

        if earliest >= cursor:
            raise RuntimeError("minute-candle cursor did not move backward")

        cursor = earliest

    return MinuteCandleBackfillResult(
        pages_fetched=pages_fetched,
        raw_received=raw_received,
        canonical_new=canonical_new,
        canonical_revised=canonical_revised,
        canonical_unchanged=canonical_unchanged,
        earliest_candle_time=min(candle_times) if candle_times else None,
        latest_candle_time=max(candle_times) if candle_times else None,
        requested_start=start,
        requested_end=end,
    )


def _raw_record(
    observation: MinuteCandleObservation,
    start: datetime,
    end: datetime,
    observed_at: datetime,
) -> RawMinuteCandleInput:
    candle = observation.candle

    if candle is not None and not (
        start <= candle.start_time_utc < end and candle.end_time_utc <= observed_at
    ):
        candle = None

    return RawMinuteCandleInput(
        response_order=observation.response_order,
        market=observation.market,
        source_candle_time=observation.source_candle_time,
        raw_payload=observation.raw_payload,
        candle=candle,
    )


def _observation_start(observation: MinuteCandleObservation) -> datetime:
    value = datetime.fromisoformat(observation.source_candle_time.replace("Z", "+00:00"))
    _validate_utc(value, "source_candle_time")
    return value.astimezone(UTC)


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
