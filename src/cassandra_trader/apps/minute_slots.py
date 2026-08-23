"""Resolve point-in-time semantic status for canonical minute slots."""

from datetime import datetime, timedelta
from pathlib import Path

from cassandra_trader.adapters.storage.sqlite import (
    MinuteCandleBackfillRunStatus,
    SqliteMinuteCandleRepository,
    StoredMinuteCandleBackfillRun,
)
from cassandra_trader.domain.minute_slot import MinuteSlotStatus, ResolvedMinuteSlot


def resolve_minute_slots(
    market: str,
    start_time_utc: datetime,
    end_time_utc: datetime,
    as_of: datetime,
    database: str | Path,
) -> tuple[ResolvedMinuteSlot, ...]:
    """Resolve every minute in one exact half-open UTC range."""

    with SqliteMinuteCandleRepository(database) as repository:
        return resolve_minute_slots_from_repository(
            market,
            start_time_utc,
            end_time_utc,
            as_of,
            repository,
        )


def resolve_minute_slots_from_repository(
    market: str,
    start_time_utc: datetime,
    end_time_utc: datetime,
    as_of: datetime,
    repository: SqliteMinuteCandleRepository,
) -> tuple[ResolvedMinuteSlot, ...]:
    """Resolve slots through an already-open repository."""

    _validate_range(start_time_utc, end_time_utc)
    _validate_utc(as_of, "as_of")
    canonical = {
        stored.candle.start_time_utc: stored
        for stored in repository.list_minute_candles_as_of(
            market,
            start_time_utc,
            end_time_utc,
            as_of,
        )
    }
    successful_runs = repository.list_minute_candle_backfill_runs(
        market,
        start_time_utc,
        end_time_utc,
        as_of,
        status=MinuteCandleBackfillRunStatus.SUCCEEDED,
    )
    successful_intervals = _merge_successful_intervals(
        successful_runs,
        start_time_utc,
        end_time_utc,
    )
    slots: list[ResolvedMinuteSlot] = []
    slot_start = start_time_utc
    interval_index = 0

    while slot_start < end_time_utc:
        slot_end = slot_start + timedelta(minutes=1)
        stored = canonical.get(slot_start)

        while (
            interval_index < len(successful_intervals)
            and successful_intervals[interval_index][1] <= slot_start
        ):
            interval_index += 1

        covered_by_success = (
            interval_index < len(successful_intervals)
            and successful_intervals[interval_index][0] <= slot_start
            and successful_intervals[interval_index][1] >= slot_end
        )

        if stored is not None:
            slots.append(
                ResolvedMinuteSlot(
                    market=market,
                    start_time_utc=slot_start,
                    end_time_utc=slot_end,
                    status=MinuteSlotStatus.TRADED,
                    candle=stored.candle,
                    canonical_revision=stored.revision,
                    canonical_available_at=stored.available_at,
                )
            )
        elif covered_by_success:
            slots.append(
                ResolvedMinuteSlot(
                    market=market,
                    start_time_utc=slot_start,
                    end_time_utc=slot_end,
                    status=MinuteSlotStatus.NO_TRADE,
                    candle=None,
                    canonical_revision=None,
                    canonical_available_at=None,
                )
            )
        else:
            slots.append(
                ResolvedMinuteSlot(
                    market=market,
                    start_time_utc=slot_start,
                    end_time_utc=slot_end,
                    status=MinuteSlotStatus.UNRESOLVED,
                    candle=None,
                    canonical_revision=None,
                    canonical_available_at=None,
                )
            )

        slot_start = slot_end

    return tuple(slots)


def _merge_successful_intervals(
    runs: tuple[StoredMinuteCandleBackfillRun, ...],
    requested_start: datetime,
    requested_end: datetime,
) -> tuple[tuple[datetime, datetime], ...]:
    clipped = sorted(
        (
            max(run.requested_start, requested_start),
            min(run.requested_end, run.observed_at, requested_end),
        )
        for run in runs
        if max(run.requested_start, requested_start)
        < min(run.requested_end, run.observed_at, requested_end)
    )
    merged: list[tuple[datetime, datetime]] = []

    for interval_start, interval_end in clipped:
        if not merged or interval_start > merged[-1][1]:
            merged.append((interval_start, interval_end))
            continue

        previous_start, previous_end = merged[-1]
        merged[-1] = (previous_start, max(previous_end, interval_end))

    return tuple(merged)


def _validate_range(start: datetime, end: datetime) -> None:
    _validate_utc(start, "start_time_utc")
    _validate_utc(end, "end_time_utc")

    if start.second != 0 or start.microsecond != 0:
        raise ValueError("start_time_utc must align to a one-minute UTC boundary")

    if end.second != 0 or end.microsecond != 0:
        raise ValueError("end_time_utc must align to a one-minute UTC boundary")

    if start >= end:
        raise ValueError("start_time_utc must be before end_time_utc")


def _validate_utc(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be timezone-aware UTC")
