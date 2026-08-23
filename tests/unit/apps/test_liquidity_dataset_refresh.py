import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from cassandra_trader.adapters.storage.sqlite import (
    MarketCatalogSnapshotMemberInput,
    MarketCatalogSnapshotStatus,
    MinuteCandleBackfillRunStatus,
    SqliteMinuteCandleRepository,
    StoredMinuteCandleBackfillRun,
)
from cassandra_trader.adapters.upbit.public_data import (
    MinuteCandleResponseBatch,
    TradingPairObservation,
    TradingPairResponseBatch,
)
from cassandra_trader.apps.liquidity_dataset_bootstrap import BootstrapNetworkPolicy
from cassandra_trader.apps.liquidity_dataset_refresh import (
    IncompleteRefreshCoverageError,
    LiquidityDatasetRefreshMarketStatus,
    LiquidityDatasetRefreshStatus,
    MinuteBackfillRange,
    plan_incremental_backfill_ranges,
    refresh_liquidity_dataset,
    refresh_liquidity_dataset_with_ports,
    refresh_market_payload,
    refresh_summary_payload,
)
from cassandra_trader.domain.market import TradingPair
from cassandra_trader.domain.minute_slot import MinuteSlotSummary

_START = datetime(2026, 7, 25, 12, 0, tzinfo=UTC)
_END = _START + timedelta(days=30)
_AS_OF = _END + timedelta(minutes=1)
_NOW = datetime.now(UTC)
_OBSERVED = _NOW - timedelta(seconds=30)
_AVAILABLE = _NOW - timedelta(seconds=20)
_PLANNING = _NOW
_COVERAGE = _NOW + timedelta(minutes=2)
_CYCLE_END = _OBSERVED.replace(second=0, microsecond=0)
_CYCLE_START = _CYCLE_END - timedelta(days=30)
_MAXIMUM_AGE = timedelta(hours=1)


class SequenceClock:
    def __init__(self, *values: datetime) -> None:
        self.values = list(values)

    def __call__(self) -> datetime:
        if len(self.values) > 1:
            return self.values.pop(0)
        return self.values[0]


class CatalogAdapter:
    def __init__(
        self,
        pairs: tuple[TradingPair, ...],
        error: Exception | None = None,
        *,
        observed_at: datetime = _OBSERVED,
    ) -> None:
        self.pairs = pairs
        self.error = error
        self.observed_at = observed_at
        self.calls = 0

    async def fetch_trading_pair_batch(self, quote_currency: str) -> TradingPairResponseBatch:
        self.calls += 1
        if self.error is not None:
            raise self.error
        observations = tuple(
            TradingPairObservation(
                response_order=index,
                market=pair.code,
                raw_payload={"market": pair.code, "warning": pair.is_warning},
                pair=pair,
            )
            for index, pair in enumerate(self.pairs)
        )
        return TradingPairResponseBatch(
            endpoint="/v1/market/all",
            request_parameters={"is_details": True},
            received_at=self.observed_at,
            observations=observations,
        )


class EmptyFetcher:
    def __init__(self, failing: tuple[str, ...] = ()) -> None:
        self.failing = set(failing)
        self.calls: list[tuple[str, datetime, datetime]] = []

    async def __call__(self, market, count, *, as_of, to):
        self.calls.append((market, as_of, to))
        if market in self.failing:
            raise TimeoutError(f"timed out: {market}")
        return MinuteCandleResponseBatch(
            endpoint="/v1/candles/minutes/1",
            request_parameters={"market": market, "count": count, "to": to.isoformat()},
            received_at=_PLANNING,
            observations=(),
        )


def _pair(market: str, *, warning: bool = False) -> TradingPair:
    return TradingPair(market, market, market, warning)


def _record_success(
    database: Path,
    market: str,
    start: datetime,
    end: datetime,
    *,
    run_id: str = "existing",
) -> None:
    with SqliteMinuteCandleRepository(database) as repository:
        repository.record_minute_candle_backfill_run(
            run_id=run_id,
            market=market,
            requested_start=start,
            requested_end=end,
            observed_at=end,
            available_at=_PLANNING - timedelta(microseconds=1),
            status=MinuteCandleBackfillRunStatus.SUCCEEDED,
        )


def _run_cycle(
    database: Path,
    pairs: tuple[TradingPair, ...],
    fetcher: EmptyFetcher,
    *,
    adapter: CatalogAdapter | None = None,
    clock: SequenceClock | None = None,
    snapshot_id: str = "snapshot-refresh",
    maximum_age: timedelta = _MAXIMUM_AGE,
):
    return asyncio.run(
        refresh_liquidity_dataset_with_ports(
            "KRW",
            database,
            maximum_age,
            catalog_adapter=adapter or CatalogAdapter(pairs),
            fetch_page=fetcher,
            network_policy=BootstrapNetworkPolicy(
                minimum_interval_seconds=0,
                maximum_attempts=1,
            ),
            clock=clock or SequenceClock(_AVAILABLE, _PLANNING, _COVERAGE),
            monotonic=lambda: 0.0,
            sleep=lambda delay: _completed_sleep(),
            snapshot_id_factory=lambda: snapshot_id,
        )
    )


async def _completed_sleep() -> None:
    return None


def _run(
    start: datetime,
    end: datetime,
    *,
    observed_at: datetime | None = None,
    available_at: datetime | None = None,
    status: MinuteCandleBackfillRunStatus = MinuteCandleBackfillRunStatus.SUCCEEDED,
    run_id: str = "run",
) -> StoredMinuteCandleBackfillRun:
    return StoredMinuteCandleBackfillRun(
        run_id=run_id,
        market="KRW-BTC",
        requested_start=start,
        requested_end=end,
        observed_at=observed_at or end,
        available_at=available_at or _AS_OF,
        status=status,
        error_type="TimeoutError" if status is MinuteCandleBackfillRunStatus.FAILED else None,
        error_message="timed out" if status is MinuteCandleBackfillRunStatus.FAILED else None,
    )


def test_first_cycle_plans_the_complete_window() -> None:
    assert plan_incremental_backfill_ranges((), _START, _END, _AS_OF) == (
        MinuteBackfillRange(_START, _END),
    )


def test_complete_window_has_no_gap() -> None:
    assert (
        plan_incremental_backfill_ranges(
            (_run(_START, _END),),
            _START,
            _END,
            _AS_OF,
        )
        == ()
    )


def test_moving_window_plans_only_the_new_tail() -> None:
    for minutes in (1, 7):
        shifted_start = _START + timedelta(minutes=minutes)
        shifted_end = _END + timedelta(minutes=minutes)

        assert plan_incremental_backfill_ranges(
            (_run(_START, _END),),
            shifted_start,
            shifted_end,
            _AS_OF + timedelta(minutes=minutes),
        ) == (MinuteBackfillRange(_END, shifted_end),)


def test_overlapping_and_adjacent_evidence_merge_around_internal_gaps() -> None:
    minute = timedelta(minutes=1)
    runs = (
        _run(_START, _START + 10 * minute, run_id="first"),
        _run(_START + 8 * minute, _START + 20 * minute, run_id="overlap"),
        _run(_START + 20 * minute, _START + 30 * minute, run_id="adjacent"),
        _run(_START + 31 * minute, _START + 40 * minute, run_id="after-single"),
        _run(_START + 43 * minute, _END, run_id="after-multiple"),
    )

    assert plan_incremental_backfill_ranges(runs, _START, _END, _AS_OF) == (
        MinuteBackfillRange(_START + 30 * minute, _START + 31 * minute),
        MinuteBackfillRange(_START + 40 * minute, _START + 43 * minute),
    )


def test_fractional_observed_at_does_not_expand_evidence_to_a_partial_minute() -> None:
    minute = timedelta(minutes=1)
    run = _run(
        _START,
        _END,
        observed_at=_START + 10 * minute + timedelta(seconds=30),
    )

    assert plan_incremental_backfill_ranges((run,), _START, _END, _AS_OF) == (
        MinuteBackfillRange(_START + 10 * minute, _END),
    )


def test_future_and_failed_evidence_do_not_cover_gaps() -> None:
    runs = (
        _run(
            _START,
            _END,
            available_at=_AS_OF + timedelta(microseconds=1),
            run_id="future",
        ),
        _run(
            _START,
            _END,
            status=MinuteCandleBackfillRunStatus.FAILED,
            run_id="failed",
        ),
    )

    assert plan_incremental_backfill_ranges(runs, _START, _END, _AS_OF) == (
        MinuteBackfillRange(_START, _END),
    )


def test_first_cycle_updates_the_complete_trailing_window(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"
    fetcher = EmptyFetcher()

    result = _run_cycle(database, (_pair("KRW-BTC"),), fetcher)

    assert result.summary.status is LiquidityDatasetRefreshStatus.SUCCEEDED
    assert result.summary.window_start == _CYCLE_START
    assert result.summary.window_end == _CYCLE_END
    assert result.summary.planning_as_of == _PLANNING
    assert result.summary.coverage_as_of == _COVERAGE
    assert result.markets[0].status is LiquidityDatasetRefreshMarketStatus.UPDATED
    assert result.markets[0].planned_ranges == (MinuteBackfillRange(_CYCLE_START, _CYCLE_END),)
    assert fetcher.calls == [("KRW-BTC", _PLANNING, _CYCLE_END)]
    assert result.markets[0].final_summary.expected_count == 43_200
    assert result.markets[0].final_summary.unresolved_count == 0
    market_payload = refresh_market_payload(result.markets[0])
    summary_payload = refresh_summary_payload(result.summary)
    assert market_payload["planned_ranges"] == [
        {
            "start": _CYCLE_START.isoformat().replace("+00:00", "Z"),
            "end": _CYCLE_END.isoformat().replace("+00:00", "Z"),
        }
    ]
    assert market_payload["error"] is None
    assert summary_payload["snapshot_id"] == "snapshot-refresh"
    assert summary_payload["planning_as_of"] == _PLANNING.isoformat().replace("+00:00", "Z")
    assert summary_payload["coverage_as_of"] == _COVERAGE.isoformat().replace("+00:00", "Z")


@pytest.mark.parametrize("offset", [timedelta(0), timedelta(microseconds=1)])
def test_cycle_window_includes_the_minute_ending_at_exact_observation_boundary(
    tmp_path: Path,
    offset: timedelta,
) -> None:
    observed_at = datetime(2026, 8, 23, 12, 0, tzinfo=UTC) + offset
    available_at = observed_at + timedelta(microseconds=1)
    planning_as_of = available_at + timedelta(microseconds=1)

    result = _run_cycle(
        tmp_path / f"boundary-{offset.microseconds}.db",
        (_pair("KRW-BTC", warning=True),),
        EmptyFetcher(),
        adapter=CatalogAdapter(
            (_pair("KRW-BTC", warning=True),),
            observed_at=observed_at,
        ),
        clock=SequenceClock(available_at, planning_as_of),
    )

    assert result.summary.status is LiquidityDatasetRefreshStatus.SUCCEEDED
    assert result.summary.window_end == datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
    assert result.summary.window_start == datetime(2026, 7, 24, 12, 0, tzinfo=UTC)


def test_complete_window_skips_without_candle_api_call(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"
    _record_success(database, "KRW-BTC", _CYCLE_START, _CYCLE_END)
    fetcher = EmptyFetcher()

    result = _run_cycle(database, (_pair("KRW-BTC"),), fetcher)

    assert fetcher.calls == []
    assert result.markets[0].status is LiquidityDatasetRefreshMarketStatus.SKIPPED_COMPLETE
    assert result.markets[0].planned_ranges == ()
    assert result.summary.skipped_count == 1


def test_cycle_plans_only_new_tail_from_previous_window(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"
    old_start = _CYCLE_START - timedelta(minutes=1)
    old_end = _CYCLE_END - timedelta(minutes=1)
    _record_success(database, "KRW-BTC", old_start, old_end)
    fetcher = EmptyFetcher()

    result = _run_cycle(database, (_pair("KRW-BTC"),), fetcher)

    assert result.markets[0].planned_ranges == (MinuteBackfillRange(old_end, _CYCLE_END),)
    assert fetcher.calls == [("KRW-BTC", _PLANNING, _CYCLE_END)]


def test_internal_evidence_gaps_are_the_only_requested_ranges(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"
    gap_start = _CYCLE_START + timedelta(minutes=10)
    gap_end = gap_start + timedelta(minutes=3)
    _record_success(database, "KRW-BTC", _CYCLE_START, gap_start, run_id="before")
    _record_success(database, "KRW-BTC", gap_end, _CYCLE_END, run_id="after")
    fetcher = EmptyFetcher()

    result = _run_cycle(database, (_pair("KRW-BTC"),), fetcher)

    assert result.markets[0].planned_ranges == (MinuteBackfillRange(gap_start, gap_end),)
    assert fetcher.calls == [("KRW-BTC", _PLANNING, gap_end)]


def test_partial_gap_failure_preserves_completed_gap_for_next_cycle(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"
    minute = timedelta(minutes=1)
    first_gap = MinuteBackfillRange(_CYCLE_START + 10 * minute, _CYCLE_START + 11 * minute)
    second_gap = MinuteBackfillRange(_CYCLE_START + 20 * minute, _CYCLE_START + 21 * minute)
    _record_success(database, "KRW-BTC", _CYCLE_START, first_gap.start, run_id="before")
    _record_success(database, "KRW-BTC", first_gap.end, second_gap.start, run_id="middle")
    _record_success(database, "KRW-BTC", second_gap.end, _CYCLE_END, run_id="after")

    class SecondGapFails(EmptyFetcher):
        async def __call__(self, market, count, *, as_of, to):
            if to == second_gap.end:
                self.calls.append((market, as_of, to))
                raise TimeoutError("second gap failed")
            return await super().__call__(market, count, as_of=as_of, to=to)

    first = _run_cycle(
        database,
        (_pair("KRW-BTC"),),
        SecondGapFails(),
        snapshot_id="snapshot-partial",
    )
    next_planning = datetime.now(UTC)
    second_fetcher = EmptyFetcher()
    second = _run_cycle(
        database,
        (_pair("KRW-BTC"),),
        second_fetcher,
        snapshot_id="snapshot-resume",
        clock=SequenceClock(_AVAILABLE, next_planning, next_planning + timedelta(minutes=1)),
    )

    assert first.markets[0].status is LiquidityDatasetRefreshMarketStatus.FAILED
    assert first.markets[0].backfills[0].requested_start == first_gap.start
    assert first.markets[0].final_summary.unresolved_count == 1
    assert second.markets[0].planned_ranges == (second_gap,)
    assert second.markets[0].status is LiquidityDatasetRefreshMarketStatus.UPDATED
    assert second_fetcher.calls == [("KRW-BTC", next_planning, second_gap.end)]


def test_requested_gap_count_excludes_unattempted_ranges_after_failure(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"
    minute = timedelta(minutes=1)
    first_gap = MinuteBackfillRange(_CYCLE_START + 10 * minute, _CYCLE_START + 11 * minute)
    second_gap = MinuteBackfillRange(_CYCLE_START + 20 * minute, _CYCLE_START + 21 * minute)
    _record_success(database, "KRW-BTC", _CYCLE_START, first_gap.start, run_id="before")
    _record_success(database, "KRW-BTC", first_gap.end, second_gap.start, run_id="middle")
    _record_success(database, "KRW-BTC", second_gap.end, _CYCLE_END, run_id="after")

    class FirstGapFails(EmptyFetcher):
        async def __call__(self, market, count, *, as_of, to):
            self.calls.append((market, as_of, to))
            raise TimeoutError("first gap failed")

    fetcher = FirstGapFails()
    result = _run_cycle(database, (_pair("KRW-BTC"),), fetcher)

    assert result.markets[0].planned_ranges == (first_gap, second_gap)
    assert fetcher.calls == [("KRW-BTC", _PLANNING, first_gap.end)]
    assert result.summary.requested_gap_count == 1
    assert refresh_summary_payload(result.summary)["requested_gap_count"] == 1


def test_catalog_failure_stops_before_candle_api(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"
    fetcher = EmptyFetcher()
    adapter = CatalogAdapter((), TimeoutError("catalog timed out"))

    result = _run_cycle(
        database,
        (),
        fetcher,
        adapter=adapter,
        clock=SequenceClock(_OBSERVED, _AVAILABLE),
    )

    assert result.summary.status is LiquidityDatasetRefreshStatus.CATALOG_FAILED
    assert result.summary.error_type == "TimeoutError"
    assert result.summary.snapshot_id == "snapshot-refresh"
    assert result.markets == ()
    assert fetcher.calls == []


def test_cycle_uses_the_captured_snapshot_instead_of_a_concurrent_future_snapshot(
    tmp_path: Path,
) -> None:
    database = tmp_path / "candles.db"
    future_pair = _pair("KRW-XRP")
    with SqliteMinuteCandleRepository(database) as repository:
        repository.record_market_catalog_snapshot(
            snapshot_id="snapshot-concurrent",
            source="upbit",
            endpoint="/v1/market/all",
            request_parameters={"is_details": True},
            quote_currency="KRW",
            observed_at=_AVAILABLE,
            available_at=_AVAILABLE,
            status=MarketCatalogSnapshotStatus.SUCCEEDED,
            members=(
                MarketCatalogSnapshotMemberInput(
                    response_order=0,
                    pair=future_pair,
                    raw_payload={"market": future_pair.code},
                ),
            ),
        )
    fetcher = EmptyFetcher()

    result = _run_cycle(
        database,
        (_pair("KRW-BTC"),),
        fetcher,
        snapshot_id="snapshot-captured",
    )

    assert result.summary.snapshot_id == "snapshot-captured"
    assert [item.market for item in result.markets] == ["KRW-BTC"]
    assert [call[0] for call in fetcher.calls] == ["KRW-BTC"]


def test_stale_catalog_and_all_warning_catalog_do_not_call_candle_api(
    tmp_path: Path,
) -> None:
    stale_database = tmp_path / "stale.db"
    stale_fetcher = EmptyFetcher()
    stale_planning = _OBSERVED + timedelta(hours=2)
    stale = _run_cycle(
        stale_database,
        (_pair("KRW-BTC"),),
        stale_fetcher,
        clock=SequenceClock(_AVAILABLE, stale_planning),
    )

    warning_database = tmp_path / "warning.db"
    warning_fetcher = EmptyFetcher()
    warning = _run_cycle(
        warning_database,
        (_pair("KRW-ADA", warning=True),),
        warning_fetcher,
    )

    assert stale.summary.status is LiquidityDatasetRefreshStatus.UNRESOLVED
    assert stale_fetcher.calls == []
    assert warning.summary.status is LiquidityDatasetRefreshStatus.SUCCEEDED
    assert warning.summary.candidate_count == 0
    assert warning.summary.catalog_excluded_count == 1
    assert warning_fetcher.calls == []


def test_market_failure_isolated_and_later_market_continues(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"
    fetcher = EmptyFetcher(("KRW-BTC",))

    result = _run_cycle(
        database,
        (_pair("KRW-ETH"), _pair("KRW-BTC")),
        fetcher,
    )

    assert [item.market for item in result.markets] == ["KRW-BTC", "KRW-ETH"]
    assert [item.status for item in result.markets] == [
        LiquidityDatasetRefreshMarketStatus.FAILED,
        LiquidityDatasetRefreshMarketStatus.UPDATED,
    ]
    assert [call[0] for call in fetcher.calls] == ["KRW-BTC", "KRW-ETH"]
    assert result.summary.status is LiquidityDatasetRefreshStatus.PARTIAL_FAILURE
    assert result.summary.failed_count == 1


def test_successful_cycle_repeats_without_candle_api_calls(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"
    first_fetcher = EmptyFetcher()
    first = _run_cycle(
        database,
        (_pair("KRW-BTC"),),
        first_fetcher,
        snapshot_id="snapshot-first",
    )
    second_fetcher = EmptyFetcher()
    second_planning = datetime.now(UTC) + timedelta(minutes=1)
    second_coverage = second_planning + timedelta(minutes=1)
    second = _run_cycle(
        database,
        (_pair("KRW-BTC"),),
        second_fetcher,
        snapshot_id="snapshot-second",
        clock=SequenceClock(_AVAILABLE, second_planning, second_coverage),
    )

    assert first.markets[0].status is LiquidityDatasetRefreshMarketStatus.UPDATED
    assert second.markets[0].status is LiquidityDatasetRefreshMarketStatus.SKIPPED_COMPLETE
    assert second_fetcher.calls == []


def test_warning_and_removed_markets_are_not_queried_or_deleted(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"
    _record_success(database, "KRW-XRP", _CYCLE_START, _CYCLE_END, run_id="removed")
    _record_success(database, "KRW-ETH", _CYCLE_START, _CYCLE_END, run_id="warning")
    fetcher = EmptyFetcher()

    result = _run_cycle(
        database,
        (_pair("KRW-BTC"), _pair("KRW-ETH", warning=True)),
        fetcher,
    )

    assert [item.market for item in result.markets] == ["KRW-BTC"]
    assert [call[0] for call in fetcher.calls] == ["KRW-BTC"]
    with SqliteMinuteCandleRepository(database) as repository:
        assert repository.list_minute_candle_backfill_runs(
            "KRW-XRP",
            _CYCLE_START,
            _CYCLE_END,
            _COVERAGE,
        )
        assert repository.list_minute_candle_backfill_runs(
            "KRW-ETH",
            _CYCLE_START,
            _CYCLE_END,
            _COVERAGE,
        )


def test_canonical_complete_summary_takes_precedence_over_gap_planner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    database = tmp_path / "candles.db"
    fetcher = EmptyFetcher()

    def complete_summary(market, start, end, as_of, repository):
        return MinuteSlotSummary(
            market=market,
            start_time_utc=start,
            end_time_utc=end,
            as_of=as_of,
            expected_count=43_200,
            traded_count=43_200,
            no_trade_count=0,
            unresolved_count=0,
            quote_volume=Decimal("43200"),
        )

    monkeypatch.setattr(
        "cassandra_trader.apps.liquidity_dataset_refresh.summarize_minute_slots_from_repository",
        complete_summary,
    )
    result = _run_cycle(database, (_pair("KRW-BTC"),), fetcher)

    assert result.markets[0].status is LiquidityDatasetRefreshMarketStatus.SKIPPED_COMPLETE
    assert fetcher.calls == []


def test_unresolved_final_coverage_is_not_reported_as_updated(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    database = tmp_path / "candles.db"
    fetcher = EmptyFetcher()
    calls = 0

    def unresolved_summary(market, start, end, as_of, repository):
        nonlocal calls
        calls += 1
        return MinuteSlotSummary(
            market=market,
            start_time_utc=start,
            end_time_utc=end,
            as_of=as_of,
            expected_count=43_200,
            traded_count=0,
            no_trade_count=0,
            unresolved_count=43_200,
            quote_volume=Decimal(0),
        )

    monkeypatch.setattr(
        "cassandra_trader.apps.liquidity_dataset_refresh.summarize_minute_slots_from_repository",
        unresolved_summary,
    )
    result = _run_cycle(database, (_pair("KRW-BTC"),), fetcher)

    assert calls == 2
    assert result.markets[0].status is LiquidityDatasetRefreshMarketStatus.FAILED
    assert result.markets[0].error_type == IncompleteRefreshCoverageError.__name__
    assert result.summary.status is LiquidityDatasetRefreshStatus.PARTIAL_FAILURE


def test_cancelled_error_propagates_without_becoming_market_failure(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"

    class CancellingFetcher:
        async def __call__(self, *args, **kwargs):
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            refresh_liquidity_dataset_with_ports(
                "KRW",
                database,
                _MAXIMUM_AGE,
                catalog_adapter=CatalogAdapter((_pair("KRW-BTC"),)),
                fetch_page=CancellingFetcher(),
                network_policy=BootstrapNetworkPolicy(minimum_interval_seconds=0),
                clock=SequenceClock(_AVAILABLE, _PLANNING, _COVERAGE),
                monotonic=lambda: 0.0,
                snapshot_id_factory=lambda: "snapshot-cancelled",
            )
        )

    with SqliteMinuteCandleRepository(database) as repository:
        assert (
            repository.list_minute_candle_backfill_runs(
                "KRW-BTC",
                _CYCLE_START,
                _CYCLE_END,
                _COVERAGE,
            )
            == ()
        )


def test_reversed_coverage_clock_does_not_create_success_result(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"

    with pytest.raises(ValueError, match="coverage_as_of must not be before planning_as_of"):
        _run_cycle(
            database,
            (_pair("KRW-BTC"),),
            EmptyFetcher(),
            clock=SequenceClock(
                _AVAILABLE,
                _PLANNING,
                _PLANNING - timedelta(microseconds=1),
            ),
        )


@pytest.mark.parametrize(
    ("quote_currency", "maximum_age", "network_policy"),
    [
        ("BTC", _MAXIMUM_AGE, None),
        ("KRW", timedelta(0), None),
        ("KRW", _MAXIMUM_AGE, "invalid"),
    ],
)
def test_invalid_inputs_are_rejected_before_database_and_network(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    quote_currency: str,
    maximum_age: timedelta,
    network_policy: object,
) -> None:
    def unexpected_repository(*args, **kwargs):
        raise AssertionError("invalid input must not open the database")

    class UnexpectedClient:
        def __init__(self, *args, **kwargs):
            raise AssertionError("invalid input must not create a network client")

    monkeypatch.setattr(
        "cassandra_trader.apps.liquidity_dataset_refresh.SqliteMinuteCandleRepository",
        unexpected_repository,
    )
    monkeypatch.setattr(
        "cassandra_trader.apps.liquidity_dataset_refresh.AsyncUpbit",
        UnexpectedClient,
    )

    with pytest.raises(ValueError):
        asyncio.run(
            refresh_liquidity_dataset(
                quote_currency,
                tmp_path / "missing.db",
                maximum_age,
                network_policy=network_policy,
            )
        )


@pytest.mark.parametrize("cancelled", [False, True])
def test_one_owned_client_and_repository_are_reused_and_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    cancelled: bool,
) -> None:
    database = tmp_path / "candles.db"
    repository_count = 0
    repository_close_count = 0
    client_count = 0
    client_close_count = 0
    candle_calls = 0
    real_repository = SqliteMinuteCandleRepository

    class TrackingRepository(real_repository):
        def __init__(self, path):
            nonlocal repository_count
            repository_count += 1
            super().__init__(path)

        def close(self) -> None:
            nonlocal repository_close_count
            repository_close_count += 1
            super().close()

    class FakeClient:
        def __init__(self, *args, **kwargs):
            nonlocal client_count
            client_count += 1

            async def list_pairs(**parameters):
                return [
                    SimpleNamespace(
                        market="KRW-BTC",
                        korean_name="비트코인",
                        english_name="Bitcoin",
                        market_event=None,
                        market_warning=None,
                    )
                ]

            async def list_minutes(unit, **parameters):
                nonlocal candle_calls
                candle_calls += 1
                if cancelled:
                    raise asyncio.CancelledError
                return []

            self.trading_pairs = SimpleNamespace(list=list_pairs)
            self.candles = SimpleNamespace(list_minutes=list_minutes)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            nonlocal client_close_count
            client_close_count += 1

    monkeypatch.setattr(
        "cassandra_trader.apps.liquidity_dataset_refresh.SqliteMinuteCandleRepository",
        TrackingRepository,
    )
    monkeypatch.setattr(
        "cassandra_trader.apps.liquidity_dataset_refresh.AsyncUpbit",
        FakeClient,
    )
    operation = refresh_liquidity_dataset(
        "KRW",
        database,
        _MAXIMUM_AGE,
        network_policy=BootstrapNetworkPolicy(minimum_interval_seconds=0),
        clock=SequenceClock(
            _OBSERVED,
            _AVAILABLE,
            _PLANNING,
            _PLANNING,
            _COVERAGE,
        ),
        monotonic=lambda: 0.0,
        snapshot_id_factory=lambda: "snapshot-owned-resources",
    )

    if cancelled:
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(operation)
    else:
        result = asyncio.run(operation)
        assert result.summary.status is LiquidityDatasetRefreshStatus.SUCCEEDED

    assert repository_count == 1
    assert repository_close_count == 1
    assert client_count == 1
    assert client_close_count == 1
    assert candle_calls == 1


def test_cleanup_failures_do_not_replace_original_cancellation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    database = tmp_path / "candles.db"
    real_repository = SqliteMinuteCandleRepository

    class CloseFailingRepository(real_repository):
        def close(self) -> None:
            super().close()
            raise RuntimeError("repository close failed")

    class CloseFailingClient:
        def __init__(self, *args, **kwargs):
            async def list_pairs(**parameters):
                return [
                    SimpleNamespace(
                        market="KRW-BTC",
                        korean_name="비트코인",
                        english_name="Bitcoin",
                        market_event=None,
                        market_warning=None,
                    )
                ]

            async def list_minutes(unit, **parameters):
                raise asyncio.CancelledError("original cancellation")

            self.trading_pairs = SimpleNamespace(list=list_pairs)
            self.candles = SimpleNamespace(list_minutes=list_minutes)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            raise RuntimeError("client close failed")

    monkeypatch.setattr(
        "cassandra_trader.apps.liquidity_dataset_refresh.SqliteMinuteCandleRepository",
        CloseFailingRepository,
    )
    monkeypatch.setattr(
        "cassandra_trader.apps.liquidity_dataset_refresh.AsyncUpbit",
        CloseFailingClient,
    )

    with pytest.raises(asyncio.CancelledError, match="original cancellation") as caught:
        asyncio.run(
            refresh_liquidity_dataset(
                "KRW",
                database,
                _MAXIMUM_AGE,
                network_policy=BootstrapNetworkPolicy(minimum_interval_seconds=0),
                clock=SequenceClock(_OBSERVED, _AVAILABLE, _PLANNING),
                monotonic=lambda: 0.0,
                snapshot_id_factory=lambda: "snapshot-cleanup-failure",
            )
        )

    assert caught.value.__notes__ == [
        "failed to close Upbit client: client close failed",
        "failed to close SQLite repository: repository close failed",
    ]
