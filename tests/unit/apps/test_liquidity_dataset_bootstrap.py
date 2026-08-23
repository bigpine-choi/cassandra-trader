import asyncio
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from email.utils import format_datetime
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from upbit import APIStatusError, RateLimitError

from cassandra_trader.adapters.storage.sqlite import (
    MarketCatalogSnapshotMemberInput,
    MarketCatalogSnapshotStatus,
    SqliteMinuteCandleRepository,
)
from cassandra_trader.adapters.upbit.public_data import (
    MinuteCandleObservation,
    MinuteCandleResponseBatch,
)
from cassandra_trader.apps.liquidity_dataset_bootstrap import (
    BootstrapNetworkPolicy,
    LiquidityDatasetBootstrapStatus,
    LiquidityDatasetMarketStatus,
    bootstrap_liquidity_dataset,
    bootstrap_liquidity_dataset_with_fetcher,
)
from cassandra_trader.domain.candle import MinuteCandle
from cassandra_trader.domain.market import TradingPair

_NOW = datetime.now(UTC)
_WINDOW_END = _NOW.replace(second=0, microsecond=0) - timedelta(minutes=2)
_WINDOW_START = _WINDOW_END - timedelta(days=30)
_UNIVERSE_AS_OF = _WINDOW_END + timedelta(minutes=1)
_RESUME_AS_OF = _NOW
_FINAL_AS_OF = _NOW + timedelta(minutes=1)
_MAXIMUM_AGE = timedelta(hours=1)


class SequenceClock:
    def __init__(self, *values: datetime) -> None:
        self.values = list(values)

    def __call__(self) -> datetime:
        if len(self.values) > 1:
            return self.values.pop(0)
        return self.values[0]


class EmptyFetcher:
    def __init__(
        self,
        failing: Iterable[str] = (),
        *,
        expected_as_of: datetime = _RESUME_AS_OF,
    ) -> None:
        self.failing = set(failing)
        self.expected_as_of = expected_as_of
        self.calls: list[str] = []

    async def __call__(self, market, count, *, as_of, to):
        assert count == 200
        assert as_of == self.expected_as_of
        self.calls.append(market)
        if market in self.failing:
            raise TimeoutError(f"timed out: {market}")
        return _empty_batch(market, to)


def _pair(market: str, *, warning: bool = False) -> TradingPair:
    return TradingPair(market, market, market, warning)


def _record_catalog(database: Path, *pairs: TradingPair, stale: bool = False) -> None:
    ordered = tuple(sorted(pairs, key=lambda pair: pair.code))
    observed_at = _UNIVERSE_AS_OF - (timedelta(hours=2) if stale else timedelta(minutes=1))
    with SqliteMinuteCandleRepository(database) as repository:
        repository.record_market_catalog_snapshot(
            snapshot_id="snapshot-bootstrap",
            source="upbit",
            endpoint="/v1/market/all",
            request_parameters={"is_details": True},
            quote_currency="KRW",
            observed_at=observed_at,
            available_at=observed_at + timedelta(microseconds=1),
            status=MarketCatalogSnapshotStatus.SUCCEEDED,
            members=tuple(
                MarketCatalogSnapshotMemberInput(
                    response_order=index,
                    pair=pair,
                    raw_payload={"market": pair.code, "warning": pair.is_warning},
                )
                for index, pair in enumerate(ordered)
            ),
        )


def _empty_batch(market: str, to: datetime) -> MinuteCandleResponseBatch:
    return MinuteCandleResponseBatch(
        endpoint="/v1/candles/minutes/1",
        request_parameters={"market": market, "count": 200, "to": to.isoformat()},
        received_at=_RESUME_AS_OF,
        observations=(),
    )


def _batch(market: str, starts: Iterable[datetime]) -> MinuteCandleResponseBatch:
    observations = tuple(_observation(market, start, index) for index, start in enumerate(starts))
    return MinuteCandleResponseBatch(
        endpoint="/v1/candles/minutes/1",
        request_parameters={"market": market, "count": 200},
        received_at=_RESUME_AS_OF,
        observations=observations,
    )


def _observation(
    market: str,
    start: datetime,
    response_order: int,
) -> MinuteCandleObservation:
    start_text = start.isoformat().replace("+00:00", "Z")
    return MinuteCandleObservation(
        response_order=response_order,
        market=market,
        source_candle_time=start_text,
        raw_payload={"market": market, "candle_date_time_utc": start_text},
        candle=MinuteCandle(
            market=market,
            start_time_utc=start,
            end_time_utc=start + timedelta(minutes=1),
            open=Decimal("1"),
            high=Decimal("1"),
            low=Decimal("1"),
            close=Decimal("1"),
            base_volume=Decimal("1"),
            quote_volume=Decimal("1"),
        ),
    )


def _run(
    database: Path,
    fetcher,
    *,
    policy: BootstrapNetworkPolicy | None = None,
    sleep=asyncio.sleep,
    resume_as_of: datetime = _RESUME_AS_OF,
    final_as_of: datetime = _FINAL_AS_OF,
):
    return asyncio.run(
        bootstrap_liquidity_dataset_with_fetcher(
            "KRW",
            _UNIVERSE_AS_OF,
            _WINDOW_END,
            database,
            _MAXIMUM_AGE,
            fetch_page=fetcher,
            network_policy=policy or BootstrapNetworkPolicy(minimum_interval_seconds=0),
            clock=SequenceClock(resume_as_of, final_as_of),
            monotonic=lambda: 0.0,
            sleep=sleep,
        )
    )


def test_targets_fresh_non_warning_markets_in_deterministic_order(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"
    _record_catalog(
        database,
        _pair("KRW-XRP"),
        _pair("KRW-ADA", warning=True),
        _pair("KRW-BTC"),
    )
    fetcher = EmptyFetcher()

    result = _run(database, fetcher)

    assert fetcher.calls == ["KRW-BTC", "KRW-XRP"]
    assert [market.market for market in result.markets] == ["KRW-BTC", "KRW-XRP"]
    assert all(market.status is LiquidityDatasetMarketStatus.SUCCEEDED for market in result.markets)
    assert result.summary.status is LiquidityDatasetBootstrapStatus.SUCCEEDED
    assert result.summary.window_start == _WINDOW_START
    assert result.summary.window_end == _WINDOW_END
    assert result.summary.candidate_count == 2
    assert result.summary.catalog_candidate_count == 3
    assert result.summary.catalog_excluded_count == 1
    assert result.summary.snapshot_id == "snapshot-bootstrap"
    assert all(market.initial_summary.expected_count == 43_200 for market in result.markets)
    assert all(market.final_summary.expected_count == 43_200 for market in result.markets)
    assert all(market.final_summary.unresolved_count == 0 for market in result.markets)
    assert {market.initial_summary.as_of for market in result.markets} == {_RESUME_AS_OF}
    assert {market.final_summary.as_of for market in result.markets} == {_FINAL_AS_OF}


def test_completed_markets_are_skipped_and_second_run_makes_no_api_calls(
    tmp_path: Path,
) -> None:
    database = tmp_path / "candles.db"
    _record_catalog(database, _pair("KRW-BTC"), _pair("KRW-ETH"))
    first_fetcher = EmptyFetcher()

    _run(database, first_fetcher)
    second_resume = datetime.now(UTC)
    second_fetcher = EmptyFetcher(expected_as_of=second_resume)
    second = _run(
        database,
        second_fetcher,
        resume_as_of=second_resume,
        final_as_of=second_resume + timedelta(minutes=1),
    )

    assert first_fetcher.calls == ["KRW-BTC", "KRW-ETH"]
    assert second_fetcher.calls == []
    assert [market.status for market in second.markets] == [
        LiquidityDatasetMarketStatus.SKIPPED_COMPLETE,
        LiquidityDatasetMarketStatus.SKIPPED_COMPLETE,
    ]
    assert all(market.backfill is None for market in second.markets)
    assert second.summary.skipped_count == 2
    assert second.summary.failed_count == 0


def test_failure_isolated_and_only_incomplete_market_is_retried(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"
    _record_catalog(database, _pair("KRW-BTC"), _pair("KRW-ETH"))
    failing = EmptyFetcher({"KRW-BTC"})
    policy = BootstrapNetworkPolicy(minimum_interval_seconds=0, maximum_attempts=1)

    first = _run(database, failing, policy=policy)
    second_resume = datetime.now(UTC)
    retry = EmptyFetcher(expected_as_of=second_resume)
    second = _run(
        database,
        retry,
        policy=policy,
        resume_as_of=second_resume,
        final_as_of=second_resume + timedelta(minutes=1),
    )

    assert [market.status for market in first.markets] == [
        LiquidityDatasetMarketStatus.FAILED,
        LiquidityDatasetMarketStatus.SUCCEEDED,
    ]
    assert first.markets[0].backfill is None
    assert first.markets[0].error_type == "TimeoutError"
    assert first.summary.status is LiquidityDatasetBootstrapStatus.PARTIAL_FAILURE
    assert retry.calls == ["KRW-BTC"]
    assert [market.status for market in second.markets] == [
        LiquidityDatasetMarketStatus.SUCCEEDED,
        LiquidityDatasetMarketStatus.SKIPPED_COMPLETE,
    ]


def test_partial_second_page_failure_preserves_candles_and_resumes_market(
    tmp_path: Path,
) -> None:
    database = tmp_path / "candles.db"
    _record_catalog(database, _pair("KRW-BTC"), _pair("KRW-ETH"))
    calls: list[str] = []
    btc_calls = 0

    async def partial_fetch(market, count, *, as_of, to):
        nonlocal btc_calls
        calls.append(market)
        if market == "KRW-BTC":
            btc_calls += 1
            if btc_calls == 2:
                raise TimeoutError("second page failed")
            return _batch(
                market,
                (_WINDOW_START + timedelta(minutes=index) for index in range(1, 201)),
            )
        return _empty_batch(market, to)

    first = _run(
        database,
        partial_fetch,
        policy=BootstrapNetworkPolicy(minimum_interval_seconds=0, maximum_attempts=1),
    )
    second_resume = datetime.now(UTC)
    retry = EmptyFetcher(expected_as_of=second_resume)
    second = _run(
        database,
        retry,
        policy=BootstrapNetworkPolicy(minimum_interval_seconds=0, maximum_attempts=1),
        resume_as_of=second_resume,
        final_as_of=second_resume + timedelta(minutes=1),
    )

    assert calls == ["KRW-BTC", "KRW-BTC", "KRW-ETH"]
    assert first.markets[0].status is LiquidityDatasetMarketStatus.FAILED
    assert first.markets[0].final_summary.traded_count == 200
    assert first.markets[0].final_summary.no_trade_count == 0
    assert first.markets[0].final_summary.unresolved_count == 43_000
    assert first.markets[1].status is LiquidityDatasetMarketStatus.SUCCEEDED
    assert retry.calls == ["KRW-BTC"]
    assert second.markets[0].status is LiquidityDatasetMarketStatus.SUCCEEDED
    assert second.markets[1].status is LiquidityDatasetMarketStatus.SKIPPED_COMPLETE


def test_transient_error_retries_then_succeeds_without_real_sleep(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"
    _record_catalog(database, _pair("KRW-BTC"))
    calls = 0
    sleeps: list[float] = []

    async def fetch_page(market, count, *, as_of, to):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError("temporary")
        return _empty_batch(market, to)

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    result = _run(
        database,
        fetch_page,
        policy=BootstrapNetworkPolicy(
            minimum_interval_seconds=0,
            maximum_attempts=2,
            initial_backoff_seconds=2,
            maximum_delay_seconds=5,
        ),
        sleep=sleep,
    )

    assert calls == 2
    assert sleeps == [2]
    assert result.markets[0].status is LiquidityDatasetMarketStatus.SUCCEEDED


def test_retry_after_is_respected_and_capped(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"
    _record_catalog(database, _pair("KRW-BTC"))
    calls = 0
    sleeps: list[float] = []

    async def fetch_page(market, count, *, as_of, to):
        nonlocal calls
        calls += 1
        if calls == 1:
            request = httpx.Request("GET", "https://api.upbit.com/v1/candles/minutes/1")
            response = httpx.Response(429, headers={"Retry-After": "9"}, request=request)
            raise RateLimitError("rate limited", response=response, body=None)
        return _empty_batch(market, to)

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    result = _run(
        database,
        fetch_page,
        policy=BootstrapNetworkPolicy(
            minimum_interval_seconds=0,
            maximum_attempts=2,
            maximum_delay_seconds=5,
        ),
        sleep=sleep,
    )

    assert result.markets[0].status is LiquidityDatasetMarketStatus.SUCCEEDED
    assert sleeps == [5]


def test_retry_after_http_date_is_respected_and_capped(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"
    _record_catalog(database, _pair("KRW-BTC"))
    calls = 0
    sleeps: list[float] = []
    retry_clock = _RESUME_AS_OF.replace(microsecond=0) + timedelta(seconds=1)

    async def fetch_page(market, count, *, as_of, to):
        nonlocal calls
        calls += 1
        if calls == 1:
            request = httpx.Request("GET", "https://api.upbit.com/v1/candles/minutes/1")
            response = httpx.Response(
                429,
                headers={
                    "Retry-After": format_datetime(
                        retry_clock + timedelta(seconds=9),
                        usegmt=True,
                    )
                },
                request=request,
            )
            raise RateLimitError("rate limited", response=response, body=None)
        return _empty_batch(market, to)

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    result = asyncio.run(
        bootstrap_liquidity_dataset_with_fetcher(
            "KRW",
            _UNIVERSE_AS_OF,
            _WINDOW_END,
            database,
            _MAXIMUM_AGE,
            fetch_page=fetch_page,
            network_policy=BootstrapNetworkPolicy(
                minimum_interval_seconds=0,
                maximum_attempts=2,
                maximum_delay_seconds=5,
            ),
            clock=SequenceClock(_RESUME_AS_OF, retry_clock, _FINAL_AS_OF),
            monotonic=lambda: 0.0,
            sleep=sleep,
        )
    )

    assert result.markets[0].status is LiquidityDatasetMarketStatus.SUCCEEDED
    assert sleeps == [5]


@pytest.mark.parametrize(
    ("status_code", "expected_calls", "expected_status"),
    [
        (400, 1, LiquidityDatasetMarketStatus.FAILED),
        (500, 2, LiquidityDatasetMarketStatus.SUCCEEDED),
    ],
)
def test_only_transient_http_statuses_are_retried(
    tmp_path: Path,
    status_code: int,
    expected_calls: int,
    expected_status: LiquidityDatasetMarketStatus,
) -> None:
    database = tmp_path / "candles.db"
    _record_catalog(database, _pair("KRW-BTC"))
    calls = 0

    async def fetch_page(market, count, *, as_of, to):
        nonlocal calls
        calls += 1
        if calls == 1:
            request = httpx.Request("GET", "https://api.upbit.com/v1/candles/minutes/1")
            response = httpx.Response(status_code, request=request)
            raise APIStatusError("HTTP failure", response=response, body=None)
        return _empty_batch(market, to)

    result = _run(
        database,
        fetch_page,
        policy=BootstrapNetworkPolicy(
            minimum_interval_seconds=0,
            maximum_attempts=2,
            initial_backoff_seconds=0,
        ),
        sleep=lambda delay: _completed_sleep(),
    )

    assert calls == expected_calls
    assert result.markets[0].status is expected_status


def test_exponential_backoff_is_capped(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"
    _record_catalog(database, _pair("KRW-BTC"))
    calls = 0
    sleeps: list[float] = []

    async def fetch_page(market, count, *, as_of, to):
        nonlocal calls
        calls += 1
        if calls < 4:
            raise ConnectionError("temporary")
        return _empty_batch(market, to)

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    result = _run(
        database,
        fetch_page,
        policy=BootstrapNetworkPolicy(
            minimum_interval_seconds=0,
            maximum_attempts=4,
            initial_backoff_seconds=3,
            maximum_delay_seconds=5,
        ),
        sleep=sleep,
    )

    assert result.markets[0].status is LiquidityDatasetMarketStatus.SUCCEEDED
    assert sleeps == [3, 5, 5]


def test_rate_limiter_waits_between_page_requests(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"
    _record_catalog(database, _pair("KRW-BTC"))
    calls = 0
    sleeps: list[float] = []
    monotonic_values = iter((0.0, 0.02, 0.1))

    async def fetch_page(market, count, *, as_of, to):
        nonlocal calls
        calls += 1
        if calls == 1:
            return _batch(market, (_WINDOW_START + timedelta(minutes=1),))
        return _empty_batch(market, to)

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    result = asyncio.run(
        bootstrap_liquidity_dataset_with_fetcher(
            "KRW",
            _UNIVERSE_AS_OF,
            _WINDOW_END,
            database,
            _MAXIMUM_AGE,
            fetch_page=fetch_page,
            network_policy=BootstrapNetworkPolicy(minimum_interval_seconds=0.1),
            clock=SequenceClock(_RESUME_AS_OF, _FINAL_AS_OF),
            monotonic=lambda: next(monotonic_values),
            sleep=sleep,
        )
    )

    assert result.markets[0].status is LiquidityDatasetMarketStatus.SUCCEEDED
    assert calls == 2
    assert sleeps == [pytest.approx(0.08)]


def test_every_retry_attempt_passes_through_rate_limiter(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"
    _record_catalog(database, _pair("KRW-BTC"))
    calls = 0
    sleeps: list[float] = []
    monotonic_values = iter((0.0, 0.02, 0.1))

    async def fetch_page(market, count, *, as_of, to):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError("temporary")
        return _empty_batch(market, to)

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    result = asyncio.run(
        bootstrap_liquidity_dataset_with_fetcher(
            "KRW",
            _UNIVERSE_AS_OF,
            _WINDOW_END,
            database,
            _MAXIMUM_AGE,
            fetch_page=fetch_page,
            network_policy=BootstrapNetworkPolicy(
                minimum_interval_seconds=0.1,
                maximum_attempts=2,
                initial_backoff_seconds=0,
            ),
            clock=SequenceClock(_RESUME_AS_OF, _FINAL_AS_OF),
            monotonic=lambda: next(monotonic_values),
            sleep=sleep,
        )
    )

    assert calls == 2
    assert result.markets[0].status is LiquidityDatasetMarketStatus.SUCCEEDED
    assert sleeps == [0, pytest.approx(0.08)]


@pytest.mark.parametrize(
    "arguments",
    [
        {"minimum_interval_seconds": True},
        {"minimum_interval_seconds": -1},
        {"minimum_interval_seconds": float("nan")},
        {"minimum_interval_seconds": float("inf")},
        {"maximum_attempts": False},
        {"maximum_attempts": 0},
        {"maximum_attempts": 11},
        {"initial_backoff_seconds": float("nan")},
        {"maximum_delay_seconds": 0},
        {"maximum_delay_seconds": 301},
    ],
)
def test_network_policy_rejects_unsafe_values(arguments: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        BootstrapNetworkPolicy(**arguments)


@pytest.mark.parametrize("error", [ValueError("bad schema"), TypeError("bad payload")])
def test_validation_errors_are_not_retried(
    tmp_path: Path,
    error: Exception,
) -> None:
    database = tmp_path / "candles.db"
    _record_catalog(database, _pair("KRW-BTC"))
    calls = 0

    async def fetch_page(*args, **kwargs):
        nonlocal calls
        calls += 1
        raise error

    result = _run(database, fetch_page)

    assert calls == 1
    assert result.markets[0].status is LiquidityDatasetMarketStatus.FAILED


def test_retry_exhaustion_marks_market_failed_and_continues(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"
    _record_catalog(database, _pair("KRW-BTC"), _pair("KRW-ETH"))
    fetcher = EmptyFetcher({"KRW-BTC"})

    result = _run(
        database,
        fetcher,
        policy=BootstrapNetworkPolicy(minimum_interval_seconds=0, maximum_attempts=3),
        sleep=lambda delay: _completed_sleep(),
    )

    assert fetcher.calls == ["KRW-BTC", "KRW-BTC", "KRW-BTC", "KRW-ETH"]
    assert result.markets[0].status is LiquidityDatasetMarketStatus.FAILED
    assert result.markets[1].status is LiquidityDatasetMarketStatus.SUCCEEDED


async def _completed_sleep() -> None:
    return None


def test_cancelled_error_propagates_without_failed_evidence(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"
    _record_catalog(database, _pair("KRW-BTC"))

    async def fetch_page(*args, **kwargs):
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        _run(database, fetch_page)

    with SqliteMinuteCandleRepository(database) as repository:
        runs = repository.list_minute_candle_backfill_runs(
            "KRW-BTC", _WINDOW_START, _WINDOW_END, _FINAL_AS_OF
        )
    assert runs == ()


def test_cancelled_error_during_backoff_propagates_without_failed_evidence(
    tmp_path: Path,
) -> None:
    database = tmp_path / "candles.db"
    _record_catalog(database, _pair("KRW-BTC"))

    async def fetch_page(*args, **kwargs):
        raise TimeoutError("temporary")

    async def cancelled_sleep(delay: float) -> None:
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        _run(
            database,
            fetch_page,
            policy=BootstrapNetworkPolicy(
                minimum_interval_seconds=0,
                maximum_attempts=2,
            ),
            sleep=cancelled_sleep,
        )

    with SqliteMinuteCandleRepository(database) as repository:
        runs = repository.list_minute_candle_backfill_runs(
            "KRW-BTC", _WINDOW_START, _WINDOW_END, _FINAL_AS_OF
        )
    assert runs == ()


def test_reversed_final_wall_clock_fails_without_a_success_result(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"
    _record_catalog(database, _pair("KRW-BTC"))

    with pytest.raises(
        ValueError,
        match="final_coverage_as_of must not be before resume_as_of",
    ):
        _run(
            database,
            EmptyFetcher(),
            final_as_of=_RESUME_AS_OF - timedelta(microseconds=1),
        )


@pytest.mark.parametrize("missing", [True, False])
def test_unresolved_or_stale_universe_fails_closed_before_candle_api(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    missing: bool,
) -> None:
    database = tmp_path / "candles.db"
    if not missing:
        _record_catalog(database, _pair("KRW-BTC"), stale=True)

    class UnexpectedClient:
        def __init__(self, *args, **kwargs):
            raise AssertionError("candle client must not be created")

    def unexpected_summary(*args, **kwargs):
        raise AssertionError("candle storage must not be queried")

    monkeypatch.setattr(
        "cassandra_trader.apps.liquidity_dataset_bootstrap.AsyncUpbit", UnexpectedClient
    )
    monkeypatch.setattr(
        "cassandra_trader.apps.liquidity_dataset_bootstrap.summarize_minute_slots_from_repository",
        unexpected_summary,
    )

    result = asyncio.run(
        bootstrap_liquidity_dataset(
            "KRW",
            _UNIVERSE_AS_OF,
            _WINDOW_END,
            database,
            _MAXIMUM_AGE,
            clock=SequenceClock(_RESUME_AS_OF, _FINAL_AS_OF),
        )
    )

    assert result.summary.status is LiquidityDatasetBootstrapStatus.UNRESOLVED
    assert result.markets == ()
    assert result.summary.candidate_count == 0


def test_fresh_all_warning_universe_completes_without_client(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    database = tmp_path / "candles.db"
    _record_catalog(database, _pair("KRW-ADA", warning=True))

    class UnexpectedClient:
        def __init__(self, *args, **kwargs):
            raise AssertionError("empty bootstrap must not create a client")

    monkeypatch.setattr(
        "cassandra_trader.apps.liquidity_dataset_bootstrap.AsyncUpbit", UnexpectedClient
    )
    result = asyncio.run(
        bootstrap_liquidity_dataset(
            "KRW",
            _UNIVERSE_AS_OF,
            _WINDOW_END,
            database,
            _MAXIMUM_AGE,
            clock=SequenceClock(_RESUME_AS_OF, _FINAL_AS_OF),
        )
    )

    assert result.summary.status is LiquidityDatasetBootstrapStatus.SUCCEEDED
    assert result.summary.candidate_count == 0
    assert result.markets == ()


def test_one_client_and_repository_are_reused_for_all_markets(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    database = tmp_path / "candles.db"
    _record_catalog(database, _pair("KRW-BTC"), _pair("KRW-ETH"))
    repository_count = 0
    repository_close_count = 0
    client_count = 0
    client_close_count = 0
    api_calls: list[str] = []
    real_repository = SqliteMinuteCandleRepository

    class TrackingRepository(real_repository):
        def close(self) -> None:
            nonlocal repository_close_count
            repository_close_count += 1
            super().close()

    def repository_factory(path):
        nonlocal repository_count
        repository_count += 1
        return TrackingRepository(path)

    class FakeClient:
        def __init__(self, *args, **kwargs):
            nonlocal client_count
            client_count += 1

            async def list_minutes(unit, **parameters):
                api_calls.append(parameters["market"])
                return []

            self.candles = SimpleNamespace(list_minutes=list_minutes)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            nonlocal client_close_count
            client_close_count += 1
            return None

    monkeypatch.setattr(
        "cassandra_trader.apps.liquidity_dataset_bootstrap.SqliteMinuteCandleRepository",
        repository_factory,
    )
    monkeypatch.setattr("cassandra_trader.apps.liquidity_dataset_bootstrap.AsyncUpbit", FakeClient)

    result = asyncio.run(
        bootstrap_liquidity_dataset(
            "KRW",
            _UNIVERSE_AS_OF,
            _WINDOW_END,
            database,
            _MAXIMUM_AGE,
            network_policy=BootstrapNetworkPolicy(minimum_interval_seconds=0),
            clock=SequenceClock(_RESUME_AS_OF, _FINAL_AS_OF),
        )
    )

    assert result.summary.succeeded_count == 2
    assert repository_count == 1
    assert repository_close_count == 1
    assert client_count == 1
    assert client_close_count == 1
    assert api_calls == ["KRW-BTC", "KRW-ETH"]


def test_owned_client_and_repository_are_closed_when_cancelled(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    database = tmp_path / "candles.db"
    _record_catalog(database, _pair("KRW-BTC"))
    repository_closed = False
    client_closed = False
    real_repository = SqliteMinuteCandleRepository

    class TrackingRepository(real_repository):
        def close(self) -> None:
            nonlocal repository_closed
            repository_closed = True
            super().close()

    class CancellingClient:
        def __init__(self, *args, **kwargs):
            async def list_minutes(unit, **parameters):
                raise asyncio.CancelledError

            self.candles = SimpleNamespace(list_minutes=list_minutes)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            nonlocal client_closed
            client_closed = True

    monkeypatch.setattr(
        "cassandra_trader.apps.liquidity_dataset_bootstrap.SqliteMinuteCandleRepository",
        TrackingRepository,
    )
    monkeypatch.setattr(
        "cassandra_trader.apps.liquidity_dataset_bootstrap.AsyncUpbit",
        CancellingClient,
    )

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            bootstrap_liquidity_dataset(
                "KRW",
                _UNIVERSE_AS_OF,
                _WINDOW_END,
                database,
                _MAXIMUM_AGE,
                network_policy=BootstrapNetworkPolicy(minimum_interval_seconds=0),
                clock=SequenceClock(_RESUME_AS_OF, _FINAL_AS_OF),
            )
        )

    assert client_closed is True
    assert repository_closed is True


@pytest.mark.parametrize(
    ("quote, universe_as_of, window_end"),
    [
        ("BTC", _UNIVERSE_AS_OF, _WINDOW_END),
        ("KRW", _UNIVERSE_AS_OF.replace(tzinfo=None), _WINDOW_END),
        ("KRW", _UNIVERSE_AS_OF, _WINDOW_END + timedelta(seconds=1)),
        ("KRW", _WINDOW_END - timedelta(minutes=1), _WINDOW_END),
        ("KRW", _RESUME_AS_OF + timedelta(minutes=1), _WINDOW_END),
    ],
)
def test_invalid_request_is_rejected_before_database_access(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    quote: str,
    universe_as_of: datetime,
    window_end: datetime,
) -> None:
    def unexpected_repository(*args, **kwargs):
        raise AssertionError("invalid input must not open the database")

    monkeypatch.setattr(
        "cassandra_trader.apps.liquidity_dataset_bootstrap.SqliteMinuteCandleRepository",
        unexpected_repository,
    )

    with pytest.raises(ValueError):
        asyncio.run(
            bootstrap_liquidity_dataset(
                quote,
                universe_as_of,
                window_end,
                tmp_path / "missing.db",
                _MAXIMUM_AGE,
                clock=SequenceClock(_RESUME_AS_OF, _FINAL_AS_OF),
            )
        )
