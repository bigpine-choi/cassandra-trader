"""REST reconciliation for completed one-minute candles."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from uuid import uuid4

from cassandra_trader.adapters.storage.sqlite import (
    RawMinuteCandleInput,
    SqliteMinuteCandleRepository,
)
from cassandra_trader.adapters.upbit.public_data import MinuteCandleResponseBatch
from cassandra_trader.adapters.upbit.public_stream import normalize_markets
from cassandra_trader.apps.minute_candle_backfill import FetchMinuteCandlePage


class MinuteReconciliationStatus(StrEnum):
    """Outcome of one authoritative REST minute reconciliation."""

    CONFIRMED = "CONFIRMED"
    NOT_RETURNED = "NOT_RETURNED"


@dataclass(frozen=True, slots=True)
class MinuteReconciliationResult:
    """Persistence outcome for one completed UTC minute."""

    market: str
    start_time_utc: datetime
    end_time_utc: datetime
    status: MinuteReconciliationStatus
    raw_received: int
    canonical_new: int
    canonical_revised: int
    canonical_unchanged: int
    available_at: datetime


async def reconcile_completed_minute(
    market: str,
    start_time_utc: datetime,
    repository: SqliteMinuteCandleRepository,
    fetch_page: FetchMinuteCandlePage,
    observed_at: datetime,
    *,
    ingested_at: datetime | None = None,
) -> MinuteReconciliationResult:
    """Confirm exactly one completed minute through REST and persist its raw batch."""

    normalized_market = normalize_markets([market])[0]
    _validate_minute_boundary(start_time_utc, "start_time_utc")
    _validate_utc(observed_at, "observed_at")
    end_time_utc = start_time_utc + timedelta(minutes=1)

    if observed_at < end_time_utc:
        raise ValueError("observed_at must be at or after the target minute end")

    batch = await fetch_page(
        normalized_market,
        1,
        as_of=observed_at,
        to=end_time_utc,
    )
    effective_ingested_at = ingested_at or datetime.now(UTC)
    _validate_utc(effective_ingested_at, "ingested_at")
    records = _target_records(
        batch,
        normalized_market,
        start_time_utc,
        end_time_utc,
    )
    stats = repository.ingest_batch(
        batch_id=str(uuid4()),
        source="upbit",
        endpoint=batch.endpoint,
        request_parameters=batch.request_parameters,
        received_at=batch.received_at,
        records=records,
        ingested_at=effective_ingested_at,
    )
    confirmed = any(record.candle is not None for record in records)

    return MinuteReconciliationResult(
        market=normalized_market,
        start_time_utc=start_time_utc,
        end_time_utc=end_time_utc,
        status=(
            MinuteReconciliationStatus.CONFIRMED
            if confirmed
            else MinuteReconciliationStatus.NOT_RETURNED
        ),
        raw_received=stats.raw_count,
        canonical_new=stats.new_canonical_count,
        canonical_revised=stats.revised_canonical_count,
        canonical_unchanged=stats.unchanged_canonical_count,
        available_at=effective_ingested_at,
    )


def _target_records(
    batch: MinuteCandleResponseBatch,
    market: str,
    start: datetime,
    end: datetime,
) -> tuple[RawMinuteCandleInput, ...]:
    records: list[RawMinuteCandleInput] = []

    for observation in batch.observations:
        candle = observation.candle

        if candle is not None and not (
            candle.market == market
            and candle.start_time_utc == start
            and candle.end_time_utc == end
        ):
            candle = None

        records.append(
            RawMinuteCandleInput(
                response_order=observation.response_order,
                market=observation.market,
                source_candle_time=observation.source_candle_time,
                raw_payload=observation.raw_payload,
                candle=candle,
            )
        )

    return tuple(records)


def _validate_minute_boundary(value: datetime, name: str) -> None:
    _validate_utc(value, name)

    if value.second != 0 or value.microsecond != 0:
        raise ValueError(f"{name} must align to a one-minute UTC boundary")


def _validate_utc(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be timezone-aware UTC")
