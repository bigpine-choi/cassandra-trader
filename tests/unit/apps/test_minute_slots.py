from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from cassandra_trader.adapters.storage.sqlite import (
    MinuteCandleBackfillRunStatus,
    RawMinuteCandleInput,
    SqliteMinuteCandleRepository,
    StoredMinuteCandle,
    StoredMinuteCandleBackfillRun,
)
from cassandra_trader.apps.minute_slots import (
    resolve_minute_slots,
    resolve_minute_slots_from_repository,
    summarize_minute_slots,
    summarize_minute_slots_from_repository,
)
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


def test_thirty_day_summary_counts_43200_no_trade_slots_without_materializing_slots(
    tmp_path: Path,
) -> None:
    database = tmp_path / "candles.db"
    end = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
    start = end - timedelta(days=30)
    available_at = end + timedelta(seconds=1)

    with SqliteMinuteCandleRepository(database) as repository:
        _record_run(
            repository,
            run_id="thirty-days",
            start=start,
            end=end,
            observed_at=end,
            available_at=available_at,
        )

    summary = summarize_minute_slots("KRW-BTC", start, end, available_at, database)

    assert summary.expected_count == 43_200
    assert summary.traded_count == 0
    assert summary.no_trade_count == 43_200
    assert summary.unresolved_count == 0
    assert summary.quote_volume == Decimal(0)


def test_summary_matches_resolver_and_counts_canonical_before_absence_evidence(
    tmp_path: Path,
) -> None:
    database = tmp_path / "candles.db"
    end = _START + timedelta(minutes=4)
    first = replace(_candle(_START + timedelta(minutes=1)), quote_volume=Decimal("10.123456789"))
    second = replace(_candle(_START + timedelta(minutes=3)), quote_volume=Decimal("0.000000011"))

    with SqliteMinuteCandleRepository(database) as repository:
        _ingest_candle(repository, first, batch_id="first", available_at=_AVAILABLE)
        _ingest_candle(repository, second, batch_id="second", available_at=_AVAILABLE)
        _record_run(
            repository,
            run_id="complete",
            start=_START,
            end=end,
            observed_at=end,
            available_at=_AVAILABLE,
        )

    summary = summarize_minute_slots("KRW-BTC", _START, end, _AVAILABLE, database)
    slots = resolve_minute_slots("KRW-BTC", _START, end, _AVAILABLE, database)

    assert summary.traded_count == sum(slot.status is MinuteSlotStatus.TRADED for slot in slots)
    assert summary.no_trade_count == sum(slot.status is MinuteSlotStatus.NO_TRADE for slot in slots)
    assert summary.unresolved_count == sum(
        slot.status is MinuteSlotStatus.UNRESOLVED for slot in slots
    )
    assert summary.traded_count == 2
    assert summary.no_trade_count == 2
    assert summary.quote_volume == Decimal("10.123456800")


def test_summary_merges_fractional_overlapping_and_adjacent_evidence_inward() -> None:
    end = _START + timedelta(minutes=4)
    runs = (
        StoredMinuteCandleBackfillRun(
            run_id="fractional-left",
            market="KRW-BTC",
            requested_start=_START + timedelta(seconds=30),
            requested_end=_START + timedelta(minutes=2),
            observed_at=_START + timedelta(minutes=2),
            available_at=_AVAILABLE,
            status=MinuteCandleBackfillRunStatus.SUCCEEDED,
            error_type=None,
            error_message=None,
        ),
        StoredMinuteCandleBackfillRun(
            run_id="overlap",
            market="KRW-BTC",
            requested_start=_START + timedelta(minutes=1, seconds=30),
            requested_end=end,
            observed_at=_START + timedelta(minutes=3, seconds=30),
            available_at=_AVAILABLE,
            status=MinuteCandleBackfillRunStatus.SUCCEEDED,
            error_type=None,
            error_message=None,
        ),
        StoredMinuteCandleBackfillRun(
            run_id="adjacent",
            market="KRW-BTC",
            requested_start=_START + timedelta(minutes=3, seconds=30),
            requested_end=end,
            observed_at=end,
            available_at=_AVAILABLE,
            status=MinuteCandleBackfillRunStatus.SUCCEEDED,
            error_type=None,
            error_message=None,
        ),
    )

    class FakeRepository:
        def list_minute_candles_as_of(self, *args, **kwargs):
            return ()

        def list_minute_candle_backfill_runs(self, *args, **kwargs):
            return runs

    summary = summarize_minute_slots_from_repository(
        "KRW-BTC",
        _START,
        end,
        _AVAILABLE,
        FakeRepository(),  # type: ignore[arg-type]
    )

    assert summary.expected_count == 4
    assert summary.traded_count == 0
    assert summary.no_trade_count == 3
    assert summary.unresolved_count == 1


def test_summary_respects_canonical_revision_and_future_evidence_availability(
    tmp_path: Path,
) -> None:
    database = tmp_path / "candles.db"
    end = _START + timedelta(minutes=2)
    revised = replace(_candle(), quote_volume=Decimal("20.000000001"))

    with SqliteMinuteCandleRepository(database) as repository:
        _ingest_candle(repository, _candle(), batch_id="revision-1", available_at=_AVAILABLE)
        _ingest_candle(repository, revised, batch_id="revision-2", available_at=_LATE)
        _record_run(
            repository,
            run_id="future-success",
            start=_START,
            end=end,
            observed_at=end,
            available_at=_LATE,
        )
        _record_run(
            repository,
            run_id="failed",
            start=_START,
            end=end,
            observed_at=end,
            available_at=_AVAILABLE,
            status=MinuteCandleBackfillRunStatus.FAILED,
        )

    earlier = summarize_minute_slots("KRW-BTC", _START, end, _AVAILABLE, database)
    later = summarize_minute_slots("KRW-BTC", _START, end, _LATE, database)

    assert earlier.traded_count == 1
    assert earlier.no_trade_count == 0
    assert earlier.unresolved_count == 1
    assert earlier.quote_volume == Decimal("10.5")
    assert later.traded_count == 1
    assert later.no_trade_count == 1
    assert later.unresolved_count == 0
    assert later.quote_volume == Decimal("20.000000001")


def test_summary_is_differentially_equivalent_to_slot_resolver_across_boundaries() -> None:
    """Keep the optimized interval arithmetic identical to the slot contract."""

    end = _START + timedelta(minutes=6)

    def stored(
        minute: int,
        quote_volume: str,
        revision: int = 1,
        available_at: datetime = _AVAILABLE,
    ) -> StoredMinuteCandle:
        start = _START + timedelta(minutes=minute)
        candle = replace(_candle(start), quote_volume=Decimal(quote_volume))
        return StoredMinuteCandle(
            candle=candle,
            revision=revision,
            content_sha256=f"{revision:064x}",
            source_raw_record_id=revision,
            available_at=available_at,
        )

    def run(
        run_id: str,
        start_minutes: float,
        end_minutes: float,
        *,
        observed_minutes: float | None = None,
        available_at: datetime = _AVAILABLE,
        status: MinuteCandleBackfillRunStatus = MinuteCandleBackfillRunStatus.SUCCEEDED,
    ) -> StoredMinuteCandleBackfillRun:
        return StoredMinuteCandleBackfillRun(
            run_id=run_id,
            market="KRW-BTC",
            requested_start=_START + timedelta(minutes=start_minutes),
            requested_end=_START + timedelta(minutes=end_minutes),
            observed_at=_START
            + timedelta(minutes=end_minutes if observed_minutes is None else observed_minutes),
            available_at=available_at,
            status=status,
            error_type=("TimeoutError" if status is MinuteCandleBackfillRunStatus.FAILED else None),
            error_message=(
                "request timed out" if status is MinuteCandleBackfillRunStatus.FAILED else None
            ),
        )

    cases = (
        ("canonical-only", _AVAILABLE, (stored(0, "1.25"), stored(5, "2.75")), ()),
        ("successful-evidence-only", _AVAILABLE, (), (run("full", 0, 6),)),
        (
            "canonical-overlapping-evidence",
            _AVAILABLE,
            (stored(2, "3.5"),),
            (run("full-with-canonical", 0, 6),),
        ),
        (
            "failed-evidence",
            _AVAILABLE,
            (),
            (run("failed", 0, 6, status=MinuteCandleBackfillRunStatus.FAILED),),
        ),
        (
            "overlapping-and-adjacent-evidence",
            _AVAILABLE,
            (stored(2, "4"),),
            (run("overlap-left", 0, 2.5), run("overlap-right", 2, 4), run("adjacent", 4, 6)),
        ),
        ("partial-first-and-last", _AVAILABLE, (), (run("partial-edges", 0.5, 5.5),)),
        (
            "fractional-observed-at-clip",
            _AVAILABLE,
            (),
            (run("observed-clip", 0, 6, observed_minutes=3.5),),
        ),
        (
            "before-later-canonical-revision",
            _AVAILABLE,
            (stored(1, "5", 1), stored(1, "9", 2, _LATE)),
            (),
        ),
        (
            "after-later-canonical-revision",
            _LATE,
            (stored(1, "5", 1), stored(1, "9", 2, _LATE)),
            (),
        ),
        (
            "exact-one-minute-gap",
            _AVAILABLE,
            (),
            (run("gap-left", 0, 2), run("gap-right", 3, 6)),
        ),
        (
            "range-boundary-touching-evidence",
            _AVAILABLE,
            (),
            (
                run("outside-left", -2, 0),
                run("inside-left", 0, 1),
                run("inside-right", 5, 6),
                run("outside-right", 6, 8),
            ),
        ),
    )

    class DifferentialRepository:
        def __init__(
            self,
            canonical: tuple[StoredMinuteCandle, ...],
            runs: tuple[StoredMinuteCandleBackfillRun, ...],
        ) -> None:
            self.canonical = canonical
            self.runs = runs

        def list_minute_candles_as_of(
            self,
            market: str,
            start_time_utc: datetime,
            end_time_utc: datetime,
            as_of: datetime,
        ) -> tuple[StoredMinuteCandle, ...]:
            latest: dict[datetime, StoredMinuteCandle] = {}

            for item in self.canonical:
                candle = item.candle
                if (
                    candle.market == market
                    and start_time_utc <= candle.start_time_utc < end_time_utc
                    and item.available_at <= as_of
                    and (
                        candle.start_time_utc not in latest
                        or item.revision > latest[candle.start_time_utc].revision
                    )
                ):
                    latest[candle.start_time_utc] = item

            return tuple(latest[start] for start in sorted(latest))

        def list_minute_candle_backfill_runs(
            self,
            market: str,
            start_time_utc: datetime,
            end_time_utc: datetime,
            as_of: datetime,
            *,
            status: MinuteCandleBackfillRunStatus | None = None,
        ) -> tuple[StoredMinuteCandleBackfillRun, ...]:
            return tuple(
                item
                for item in self.runs
                if item.market == market
                and item.requested_start < end_time_utc
                and item.requested_end > start_time_utc
                and item.available_at <= as_of
                and (status is None or item.status is status)
            )

    for case, as_of, canonical, runs in cases:
        repository = DifferentialRepository(canonical, runs)
        slots = resolve_minute_slots_from_repository(
            "KRW-BTC",
            _START,
            end,
            as_of,
            repository,  # type: ignore[arg-type]
        )
        summary = summarize_minute_slots_from_repository(
            "KRW-BTC",
            _START,
            end,
            as_of,
            repository,  # type: ignore[arg-type]
        )
        traded_slots = tuple(slot for slot in slots if slot.status is MinuteSlotStatus.TRADED)

        assert summary.expected_count == len(slots), case
        assert summary.traded_count == len(traded_slots), case
        assert summary.no_trade_count == sum(
            slot.status is MinuteSlotStatus.NO_TRADE for slot in slots
        ), case
        assert summary.unresolved_count == sum(
            slot.status is MinuteSlotStatus.UNRESOLVED for slot in slots
        ), case
        assert summary.quote_volume == sum(
            (slot.candle.quote_volume for slot in traded_slots if slot.candle is not None),
            start=Decimal(0),
        ), case
        assert summary.expected_count == (
            summary.traded_count + summary.no_trade_count + summary.unresolved_count
        ), case
