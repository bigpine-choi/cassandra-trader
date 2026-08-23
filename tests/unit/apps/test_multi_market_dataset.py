import asyncio
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from cassandra_trader.adapters.storage.sqlite import SqliteMinuteCandleRepository
from cassandra_trader.adapters.upbit.public_data import (
    MinuteCandleObservation,
    MinuteCandleResponseBatch,
)
from cassandra_trader.apps.minute_slots import resolve_minute_slots
from cassandra_trader.apps.multi_market_dataset import (
    DatasetMarketStatus,
    backfill_multi_market_dataset,
    backfill_multi_market_dataset_with_fetcher,
)
from cassandra_trader.domain.candle import MinuteCandle
from cassandra_trader.domain.minute_slot import MinuteSlotStatus

_START = datetime(2026, 8, 22, 8, 0, tzinfo=UTC)
_END = _START + timedelta(minutes=3)
_OBSERVED_AT = datetime(2026, 8, 23, tzinfo=UTC)
_COVERAGE_AS_OF = datetime(2100, 1, 1, tzinfo=UTC)


class MarketFetcher:
    def __init__(self, *, failing_market: str | None = None) -> None:
        self.failing_market = failing_market
        self.calls: list[tuple[str, datetime, datetime]] = []

    async def __call__(
        self,
        market: str,
        count: int,
        *,
        as_of: datetime,
        to: datetime,
    ) -> MinuteCandleResponseBatch:
        assert count == 200
        self.calls.append((market, as_of, to))

        if market == self.failing_market:
            raise TimeoutError(f"timed out fetching {market}")

        return _batch(market, (_START, _START + timedelta(minutes=2)))


def _batch(market: str, starts: Iterable[datetime]) -> MinuteCandleResponseBatch:
    observations = tuple(
        _observation(market, start, response_order) for response_order, start in enumerate(starts)
    )
    return MinuteCandleResponseBatch(
        endpoint="/v1/candles/minutes/1",
        request_parameters={"market": market, "count": 200},
        received_at=_OBSERVED_AT,
        observations=observations,
    )


def _observation(
    market: str,
    start: datetime,
    response_order: int,
) -> MinuteCandleObservation:
    start_text = start.isoformat().replace("+00:00", "Z")
    candle = MinuteCandle(
        market=market,
        start_time_utc=start,
        end_time_utc=start + timedelta(minutes=1),
        open=Decimal("10"),
        high=Decimal("11"),
        low=Decimal("9"),
        close=Decimal("10.5"),
        base_volume=Decimal("1"),
        quote_volume=Decimal("10.5"),
    )
    return MinuteCandleObservation(
        response_order=response_order,
        market=market,
        source_candle_time=start_text,
        raw_payload={"market": market, "candle_date_time_utc": start_text},
        candle=candle,
    )


def _run(
    database: Path,
    markets: list[str],
    fetcher: MarketFetcher,
):
    return asyncio.run(
        backfill_multi_market_dataset_with_fetcher(
            markets,
            _START,
            _END,
            database,
            fetch_page=fetcher,
            observed_at=_OBSERVED_AT,
            coverage_as_of=_COVERAGE_AS_OF,
        )
    )


def test_normalizes_deduplicates_sorts_and_keeps_market_data_separate(
    tmp_path: Path,
) -> None:
    database = tmp_path / "candles.db"
    fetcher = MarketFetcher()

    result = _run(database, [" krw-xrp ", "KRW-BTC", "krw-xrp"], fetcher)

    assert [item.market for item in result.markets] == ["KRW-BTC", "KRW-XRP"]
    assert [call[0] for call in fetcher.calls] == ["KRW-BTC", "KRW-XRP"]
    assert {call[1] for call in fetcher.calls} == {_OBSERVED_AT}
    assert result.summary.succeeded == 2
    assert result.summary.failed == 0
    with SqliteMinuteCandleRepository(database) as repository:
        btc = repository.list_minute_candles_as_of("KRW-BTC", _START, _END, _COVERAGE_AS_OF)
        xrp = repository.list_minute_candles_as_of("KRW-XRP", _START, _END, _COVERAGE_AS_OF)
        btc_runs = repository.list_minute_candle_backfill_runs(
            "KRW-BTC", _START, _END, _COVERAGE_AS_OF
        )
        xrp_runs = repository.list_minute_candle_backfill_runs(
            "KRW-XRP", _START, _END, _COVERAGE_AS_OF
        )
    assert {item.candle.market for item in btc} == {"KRW-BTC"}
    assert {item.candle.market for item in xrp} == {"KRW-XRP"}
    assert len(btc) == len(xrp) == 2
    assert len(btc_runs) == len(xrp_runs) == 1


@pytest.mark.parametrize("markets", [[], ["BTC-USDT"], ["KRW-"], ["KRW-비트"]])
def test_rejects_empty_or_non_krw_markets_before_fetch(
    tmp_path: Path,
    markets: list[str],
) -> None:
    fetcher = MarketFetcher()

    with pytest.raises(ValueError):
        _run(tmp_path / "candles.db", markets, fetcher)

    assert fetcher.calls == []


def test_continues_after_one_market_failure_and_reports_neutral_absence(
    tmp_path: Path,
) -> None:
    fetcher = MarketFetcher(failing_market="KRW-BTC")

    result = _run(tmp_path / "candles.db", ["KRW-XRP", "KRW-BTC"], fetcher)

    failed, succeeded = result.markets
    assert [call[0] for call in fetcher.calls] == ["KRW-BTC", "KRW-XRP"]
    assert failed.status is DatasetMarketStatus.FAILED
    assert failed.backfill is None
    assert failed.coverage is not None
    assert failed.coverage.observed_slots == 0
    assert failed.coverage.absent_slots == 3
    assert failed.error_type == "TimeoutError"
    assert failed.error_message == "timed out fetching KRW-BTC"
    assert succeeded.status is DatasetMarketStatus.SUCCEEDED
    assert succeeded.coverage is not None
    assert succeeded.coverage.observed_slots == 2
    assert succeeded.coverage.absent_slots == 1
    assert result.summary.succeeded == 1
    assert result.summary.failed == 1


def test_second_page_failure_keeps_partial_coverage_and_continues(
    tmp_path: Path,
) -> None:
    end = _START + timedelta(minutes=201)
    calls: list[str] = []
    btc_calls = 0

    async def fetch_page(
        market: str,
        count: int,
        *,
        as_of: datetime,
        to: datetime,
    ) -> MinuteCandleResponseBatch:
        nonlocal btc_calls
        assert count == 200
        assert as_of == _OBSERVED_AT
        calls.append(market)

        if market == "KRW-BTC":
            btc_calls += 1

            if btc_calls == 2:
                raise TimeoutError("second page timed out")

            starts = (_START + timedelta(minutes=offset) for offset in range(1, 201))
            return _batch(market, starts)

        assert to == end
        return _batch(market, (_START, end - timedelta(minutes=1)))

    result = asyncio.run(
        backfill_multi_market_dataset_with_fetcher(
            ["KRW-XRP", "KRW-BTC"],
            _START,
            end,
            tmp_path / "candles.db",
            fetch_page=fetch_page,
            observed_at=_OBSERVED_AT,
            coverage_as_of=_COVERAGE_AS_OF,
        )
    )

    failed, succeeded = result.markets
    assert calls == ["KRW-BTC", "KRW-BTC", "KRW-XRP"]
    assert failed.market == "KRW-BTC"
    assert failed.status is DatasetMarketStatus.FAILED
    assert failed.backfill is None
    assert failed.coverage is not None
    assert failed.coverage.expected_slots == 201
    assert failed.coverage.observed_slots == 200
    assert failed.coverage.absent_slots == 1
    assert failed.error_type == "TimeoutError"
    assert failed.error_message == "second page timed out"
    assert succeeded.market == "KRW-XRP"
    assert succeeded.status is DatasetMarketStatus.SUCCEEDED
    assert succeeded.backfill is not None
    assert result.summary.succeeded == 1
    assert result.summary.failed == 1

    slots = resolve_minute_slots(
        "KRW-BTC",
        _START,
        end,
        _COVERAGE_AS_OF,
        tmp_path / "candles.db",
    )
    assert slots[0].status is MinuteSlotStatus.UNRESOLVED
    assert all(slot.status is MinuteSlotStatus.TRADED for slot in slots[1:])


def test_repeated_range_has_no_new_or_revised_canonical_rows(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"

    first = _run(database, ["KRW-BTC", "KRW-XRP"], MarketFetcher())
    second = _run(database, ["KRW-XRP", "KRW-BTC"], MarketFetcher())

    assert all(item.backfill is not None for item in first.markets)
    assert all(item.backfill is not None for item in second.markets)
    assert [item.backfill.canonical_new for item in second.markets if item.backfill] == [0, 0]
    assert [item.backfill.canonical_revised for item in second.markets if item.backfill] == [0, 0]
    assert [item.backfill.canonical_unchanged for item in second.markets if item.backfill] == [
        2,
        2,
    ]


def test_production_service_reuses_one_client_and_repository(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    real_repository = SqliteMinuteCandleRepository
    fetcher = MarketFetcher()
    client_count = 0
    repository_count = 0

    class FakeClient:
        def __init__(self, **kwargs: object) -> None:
            nonlocal client_count
            client_count += 1
            assert kwargs == {"environment": "kr", "timeout": 10.0, "max_retries": 2}
            self.candles = SimpleNamespace(list_minutes=object())

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args: object) -> None:
            return None

    class FakeAdapter:
        def __init__(self, *, list_minute_candles: object) -> None:
            assert list_minute_candles is not None
            self.fetch_minute_candle_batch = fetcher

    def repository_factory(database: str | Path) -> SqliteMinuteCandleRepository:
        nonlocal repository_count
        repository_count += 1
        return real_repository(database)

    monkeypatch.setattr("cassandra_trader.apps.multi_market_dataset.AsyncUpbit", FakeClient)
    monkeypatch.setattr(
        "cassandra_trader.apps.multi_market_dataset.UpbitPublicDataAdapter",
        FakeAdapter,
    )
    monkeypatch.setattr(
        "cassandra_trader.apps.multi_market_dataset.SqliteMinuteCandleRepository",
        repository_factory,
    )

    result = asyncio.run(
        backfill_multi_market_dataset(
            ["KRW-XRP", "KRW-BTC"],
            _START,
            _END,
            tmp_path / "candles.db",
            observed_at=_OBSERVED_AT,
        )
    )

    assert client_count == 1
    assert repository_count == 1
    assert [call[0] for call in fetcher.calls] == ["KRW-BTC", "KRW-XRP"]
    assert result.summary.succeeded == 2
