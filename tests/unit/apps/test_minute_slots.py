from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from cassandra_trader.adapters.storage.sqlite import (
    MinuteCandleBackfillRunStatus,
    RawMinuteCandleInput,
    SqliteMinuteCandleRepository,
)
from cassandra_trader.apps.minute_slots import resolve_minute_slots
from cassandra_trader.domain.candle import MinuteCandle
from cassandra_trader.domain.minute_slot import MinuteSlotStatus

_START = datetime(2026, 8, 22, 8, 0, tzinfo=UTC)
_AVAILABLE = datetime(2026, 8, 23, tzinfo=UTC)
_LATE = _AVAILABLE + timedelta(hours=1)


def _candle(start: datetime = _START, close: str = "10.5") -> MinuteCandle:
    return MinuteCandle(
        market="KRW-BTC",
        start_time_utc=start,
        end_time_utc=start + timedelta(minutes=1),
        open=Decimal("10"),
        high=Decimal("11"),
        low=Decimal("9"),
        close=Decimal(close),
        base_volume=Decimal("1"),
        quote_volume=Decimal("10.5"),
    )


def _ingest_candle(
    repository: SqliteMinuteCandleRepository,
    candle: MinuteCandle,
    *,
    batch_id: str,
    available_at: datetime,
) -> None:
    start_text = candle.start_time_utc.isoformat().replace("+00:00", "Z")
    repository.ingest_batch(
        batch_id=batch_id,
        source="upbit",
        endpoint="/v1/candles/minutes/1",
        request_parameters={"market": candle.market, "count": 1},
        received_at=available_at,
        ingested_at=available_at,
        records=(
            RawMinuteCandleInput(
                response_order=0,
                market=candle.market,
                source_candle_time=start_text,
                raw_payload={"market": candle.market, "candle_date_time_utc": start_text},
                candle=candle,
            ),
        ),
    )


def _record_run(
    repository: SqliteMinuteCandleRepository,
    *,
    run_id: str,
    start: datetime,
    end: datetime,
    observed_at: datetime,
    available_at: datetime,
    status: MinuteCandleBackfillRunStatus = MinuteCandleBackfillRunStatus.SUCCEEDED,
) -> None:
    repository.record_minute_candle_backfill_run(
        run_id=run_id,
        market="KRW-BTC",
        requested_start=start,
        requested_end=end,
        observed_at=observed_at,
        available_at=available_at,
        status=status,
        error_type="TimeoutError" if status is MinuteCandleBackfillRunStatus.FAILED else None,
        error_message="request timed out"
        if status is MinuteCandleBackfillRunStatus.FAILED
        else None,
    )


def test_successful_range_resolves_traded_and_no_trade_without_synthetic_candles(
    tmp_path: Path,
) -> None:
    database = tmp_path / "candles.db"
    end = _START + timedelta(minutes=3)
    traded_start = _START + timedelta(minutes=1)

    with SqliteMinuteCandleRepository(database) as repository:
        _ingest_candle(
            repository,
            _candle(traded_start),
            batch_id="candle",
            available_at=_AVAILABLE,
        )
        _record_run(
            repository,
            run_id="success",
            start=_START,
            end=end,
            observed_at=end,
            available_at=_AVAILABLE,
        )

    slots = resolve_minute_slots("KRW-BTC", _START, end, _AVAILABLE, database)

    assert [slot.start_time_utc for slot in slots] == [
        _START,
        traded_start,
        _START + timedelta(minutes=2),
    ]
    assert [slot.status for slot in slots] == [
        MinuteSlotStatus.NO_TRADE,
        MinuteSlotStatus.TRADED,
        MinuteSlotStatus.NO_TRADE,
    ]
    assert slots[1].candle == _candle(traded_start)
    assert slots[1].canonical_revision == 1
    assert slots[0].candle is None
    assert slots[0].canonical_revision is None
    assert slots[2].candle is None
    assert slots[2].canonical_revision is None


def test_absent_slot_without_successful_evidence_is_unresolved(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"
    end = _START + timedelta(minutes=1)

    with SqliteMinuteCandleRepository(database):
        pass

    slots = resolve_minute_slots("KRW-BTC", _START, end, _AVAILABLE, database)

    assert len(slots) == 1
    assert slots[0].status is MinuteSlotStatus.UNRESOLVED
    assert slots[0].candle is None


def test_failed_run_does_not_resolve_absence_or_invalidate_prior_success(
    tmp_path: Path,
) -> None:
    database = tmp_path / "candles.db"
    end = _START + timedelta(minutes=2)

    with SqliteMinuteCandleRepository(database) as repository:
        _record_run(
            repository,
            run_id="failed-only",
            start=_START,
            end=_START + timedelta(minutes=1),
            observed_at=end,
            available_at=_AVAILABLE,
            status=MinuteCandleBackfillRunStatus.FAILED,
        )
        _record_run(
            repository,
            run_id="prior-success",
            start=_START + timedelta(minutes=1),
            end=end,
            observed_at=end,
            available_at=_AVAILABLE,
        )
        _record_run(
            repository,
            run_id="later-failure",
            start=_START + timedelta(minutes=1),
            end=end,
            observed_at=end,
            available_at=_LATE,
            status=MinuteCandleBackfillRunStatus.FAILED,
        )

    slots = resolve_minute_slots("KRW-BTC", _START, end, _LATE, database)

    assert [slot.status for slot in slots] == [
        MinuteSlotStatus.UNRESOLVED,
        MinuteSlotStatus.NO_TRADE,
    ]


def test_evidence_only_applies_after_available_at_and_completed_observation(
    tmp_path: Path,
) -> None:
    database = tmp_path / "candles.db"
    end = _START + timedelta(minutes=3)

    with SqliteMinuteCandleRepository(database) as repository:
        _record_run(
            repository,
            run_id="late-evidence",
            start=_START,
            end=end,
            observed_at=_START + timedelta(minutes=2),
            available_at=_LATE,
        )

    before_available = resolve_minute_slots("KRW-BTC", _START, end, _AVAILABLE, database)
    after_available = resolve_minute_slots("KRW-BTC", _START, end, _LATE, database)

    assert all(slot.status is MinuteSlotStatus.UNRESOLVED for slot in before_available)
    assert [slot.status for slot in after_available] == [
        MinuteSlotStatus.NO_TRADE,
        MinuteSlotStatus.NO_TRADE,
        MinuteSlotStatus.UNRESOLVED,
    ]


def test_overlapping_successful_ranges_combine_across_requested_range(
    tmp_path: Path,
) -> None:
    database = tmp_path / "candles.db"
    middle = _START + timedelta(minutes=2)
    end = _START + timedelta(minutes=4)

    with SqliteMinuteCandleRepository(database) as repository:
        _record_run(
            repository,
            run_id="left",
            start=_START,
            end=middle + timedelta(minutes=1),
            observed_at=end,
            available_at=_AVAILABLE,
        )
        _record_run(
            repository,
            run_id="right",
            start=middle,
            end=end,
            observed_at=end,
            available_at=_AVAILABLE,
        )

    slots = resolve_minute_slots("KRW-BTC", _START, end, _AVAILABLE, database)

    assert len(slots) == 4
    assert all(slot.status is MinuteSlotStatus.NO_TRADE for slot in slots)


def test_adjacent_successful_ranges_merge_but_preserve_middle_unresolved_gap(
    tmp_path: Path,
) -> None:
    database = tmp_path / "candles.db"
    end = _START + timedelta(minutes=4)

    with SqliteMinuteCandleRepository(database) as repository:
        _record_run(
            repository,
            run_id="first",
            start=_START,
            end=_START + timedelta(minutes=1),
            observed_at=end,
            available_at=_AVAILABLE,
        )
        _record_run(
            repository,
            run_id="adjacent",
            start=_START + timedelta(minutes=1),
            end=_START + timedelta(minutes=2),
            observed_at=end,
            available_at=_AVAILABLE,
        )
        _record_run(
            repository,
            run_id="after-gap",
            start=_START + timedelta(minutes=3),
            end=end,
            observed_at=end,
            available_at=_AVAILABLE,
        )

    slots = resolve_minute_slots("KRW-BTC", _START, end, _AVAILABLE, database)

    assert [slot.status for slot in slots] == [
        MinuteSlotStatus.NO_TRADE,
        MinuteSlotStatus.NO_TRADE,
        MinuteSlotStatus.UNRESOLVED,
        MinuteSlotStatus.NO_TRADE,
    ]


def test_invalid_evidence_time_cannot_create_historical_no_trade(
    tmp_path: Path,
) -> None:
    database = tmp_path / "candles.db"
    end = _START + timedelta(minutes=1)

    with SqliteMinuteCandleRepository(database) as repository:
        with pytest.raises(ValueError, match="observed_at"):
            _record_run(
                repository,
                run_id="invalid-time",
                start=_START,
                end=end,
                observed_at=_LATE,
                available_at=_AVAILABLE,
            )

    slots = resolve_minute_slots("KRW-BTC", _START, end, _LATE, database)

    assert slots[0].status is MinuteSlotStatus.UNRESOLVED


def test_resolver_selects_latest_canonical_revision_available_as_of(
    tmp_path: Path,
) -> None:
    database = tmp_path / "candles.db"
    end = _START + timedelta(minutes=1)

    with SqliteMinuteCandleRepository(database) as repository:
        _ingest_candle(
            repository,
            _candle(close="10.5"),
            batch_id="revision-1",
            available_at=_AVAILABLE,
        )
        _ingest_candle(
            repository,
            replace(_candle(), close=Decimal("10.75")),
            batch_id="revision-2",
            available_at=_LATE,
        )

    before_revision = resolve_minute_slots("KRW-BTC", _START, end, _AVAILABLE, database)
    after_revision = resolve_minute_slots("KRW-BTC", _START, end, _LATE, database)

    assert before_revision[0].status is MinuteSlotStatus.TRADED
    assert before_revision[0].canonical_revision == 1
    assert before_revision[0].candle is not None
    assert before_revision[0].candle.close == Decimal("10.5")
    assert after_revision[0].canonical_revision == 2
    assert after_revision[0].candle is not None
    assert after_revision[0].candle.close == Decimal("10.75")
