from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from cassandra_trader.adapters.storage.sqlite import (
    MarketCatalogSnapshotMemberInput,
    MarketCatalogSnapshotStatus,
    MinuteCandleBackfillRunStatus,
    RawMinuteCandleInput,
    SqliteMinuteCandleRepository,
)
from cassandra_trader.apps.market_liquidity import evaluate_market_liquidity_as_of
from cassandra_trader.domain.candle import MinuteCandle
from cassandra_trader.domain.market import (
    MarketUniverseResult,
    MarketUniverseStatus,
    TradingPair,
)
from cassandra_trader.domain.market_eligibility import evaluate_market_eligibility
from cassandra_trader.domain.market_liquidity import (
    MarketLiquidityEvaluationStatus,
    MarketLiquidityReason,
    MarketLiquidityStatus,
)
from cassandra_trader.domain.minute_slot import MinuteSlotSummary

_WINDOW_END = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
_WINDOW_START = _WINDOW_END - timedelta(days=30)
_AS_OF = _WINDOW_END + timedelta(minutes=10)
_MAXIMUM_AGE = timedelta(hours=1)


def _pair(market: str, *, warning: bool = False) -> TradingPair:
    return TradingPair(market, f"한글-{market}", f"English-{market}", warning)


def _record_catalog(
    repository: SqliteMinuteCandleRepository,
    *pairs: TradingPair,
) -> None:
    members = tuple(
        MarketCatalogSnapshotMemberInput(
            response_order=index,
            pair=pair,
            raw_payload={"market": pair.code, "market_event": {"warning": pair.is_warning}},
        )
        for index, pair in enumerate(pairs)
    )
    repository.record_market_catalog_snapshot(
        snapshot_id="snapshot-1",
        source="upbit",
        endpoint="/v1/market/all",
        request_parameters={"is_details": True},
        quote_currency="KRW",
        observed_at=_WINDOW_END + timedelta(minutes=5),
        available_at=_WINDOW_END + timedelta(minutes=6),
        status=MarketCatalogSnapshotStatus.SUCCEEDED,
        members=members,
    )


def _record_evidence(repository: SqliteMinuteCandleRepository, market: str) -> None:
    repository.record_minute_candle_backfill_run(
        run_id=f"evidence-{market}",
        market=market,
        requested_start=_WINDOW_START,
        requested_end=_WINDOW_END,
        observed_at=_WINDOW_END,
        available_at=_WINDOW_END + timedelta(minutes=1),
        status=MinuteCandleBackfillRunStatus.SUCCEEDED,
    )


def _ingest_candle(
    repository: SqliteMinuteCandleRepository,
    market: str,
    quote_volume: str,
) -> None:
    start = _WINDOW_START + timedelta(minutes=1)
    candle = MinuteCandle(
        market=market,
        start_time_utc=start,
        end_time_utc=start + timedelta(minutes=1),
        open=Decimal("1"),
        high=Decimal("1"),
        low=Decimal("1"),
        close=Decimal("1"),
        base_volume=Decimal("1"),
        quote_volume=Decimal(quote_volume),
    )
    start_text = start.isoformat().replace("+00:00", "Z")
    repository.ingest_batch(
        batch_id=f"batch-{market}",
        source="upbit",
        endpoint="/v1/candles/minutes/1",
        request_parameters={"market": market, "count": 1},
        received_at=_WINDOW_END + timedelta(minutes=2),
        ingested_at=_WINDOW_END + timedelta(minutes=2),
        records=(
            RawMinuteCandleInput(
                response_order=0,
                market=market,
                source_candle_time=start_text,
                raw_payload={"market": market, "candle_date_time_utc": start_text},
                candle=candle,
            ),
        ),
    )


def test_application_ranks_only_fresh_non_warning_catalog_candidates(tmp_path: Path) -> None:
    database = tmp_path / "liquidity.db"

    with SqliteMinuteCandleRepository(database) as repository:
        _record_catalog(
            repository,
            _pair("KRW-ADA", warning=True),
            _pair("KRW-BTC"),
            _pair("KRW-ETH"),
        )

        for market, volume in (("KRW-BTC", "20.000000001"), ("KRW-ETH", "10.5")):
            _record_evidence(repository, market)
            _ingest_candle(repository, market, volume)

    result = evaluate_market_liquidity_as_of(
        "KRW",
        _WINDOW_END,
        _AS_OF,
        database,
        _MAXIMUM_AGE,
    )

    assert result.status is MarketLiquidityEvaluationStatus.RESOLVED
    assert [metric.market for metric in result.metrics] == ["KRW-BTC", "KRW-ETH"]
    assert [metric.rank for metric in result.metrics] == [1, 2]
    assert [metric.quote_volume for metric in result.metrics] == [
        Decimal("20.000000001"),
        Decimal("10.5"),
    ]
    assert result.catalog_candidate_count == 3
    assert result.catalog_excluded_count == 1


def test_one_unresolved_market_prevents_all_application_ranks(tmp_path: Path) -> None:
    database = tmp_path / "liquidity.db"

    with SqliteMinuteCandleRepository(database) as repository:
        _record_catalog(repository, _pair("KRW-BTC"), _pair("KRW-ETH"))
        _record_evidence(repository, "KRW-BTC")
        _ingest_candle(repository, "KRW-BTC", "20")

    result = evaluate_market_liquidity_as_of(
        "KRW",
        _WINDOW_END,
        _AS_OF,
        database,
        _MAXIMUM_AGE,
    )

    assert result.status is MarketLiquidityEvaluationStatus.UNRESOLVED
    assert result.reasons == (MarketLiquidityReason.LIQUIDITY_DATA_UNRESOLVED,)
    assert result.metrics[0].status is MarketLiquidityStatus.RESOLVED
    assert result.metrics[1].status is MarketLiquidityStatus.UNRESOLVED
    assert all(metric.rank is None for metric in result.metrics)


@pytest.mark.parametrize("stale", [False, True])
def test_unresolved_or_stale_catalog_does_not_open_liquidity_repository(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    stale: bool,
) -> None:
    universe = (
        MarketUniverseResult(
            status=MarketUniverseStatus.AVAILABLE,
            quote_currency="KRW",
            as_of=_AS_OF,
            snapshot_id="snapshot-stale",
            observed_at=_AS_OF - timedelta(hours=2),
            available_at=_AS_OF - timedelta(hours=2) + timedelta(seconds=1),
            content_hash="f" * 64,
            pairs=(_pair("KRW-BTC"),),
        )
        if stale
        else MarketUniverseResult(
            status=MarketUniverseStatus.UNRESOLVED,
            quote_currency="KRW",
            as_of=_AS_OF,
            snapshot_id=None,
            observed_at=None,
            available_at=None,
            content_hash=None,
            pairs=(),
        )
    )
    eligibility = evaluate_market_eligibility(universe, _MAXIMUM_AGE)

    monkeypatch.setattr(
        "cassandra_trader.apps.market_liquidity.evaluate_market_eligibility_as_of",
        lambda *args, **kwargs: eligibility,
    )

    def unexpected_repository(*args, **kwargs):
        raise AssertionError("unresolved catalog must not scan liquidity data")

    monkeypatch.setattr(
        "cassandra_trader.apps.market_liquidity.SqliteMinuteCandleRepository",
        unexpected_repository,
    )

    result = evaluate_market_liquidity_as_of(
        "KRW",
        _WINDOW_END,
        _AS_OF,
        tmp_path / "missing.db",
        _MAXIMUM_AGE,
    )

    assert result.status is MarketLiquidityEvaluationStatus.UNRESOLVED
    assert result.metrics == ()

    if stale:
        assert result.snapshot_id == "snapshot-stale"
        assert result.catalog_candidate_count == 1
    else:
        assert result.snapshot_id is None
        assert result.catalog_candidate_count == 0


def test_all_warning_catalog_resolves_empty_without_liquidity_repository(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    universe = MarketUniverseResult(
        status=MarketUniverseStatus.AVAILABLE,
        quote_currency="KRW",
        as_of=_AS_OF,
        snapshot_id="snapshot-warning",
        observed_at=_AS_OF - timedelta(minutes=5),
        available_at=_AS_OF - timedelta(minutes=4),
        content_hash="a" * 64,
        pairs=(_pair("KRW-ADA", warning=True),),
    )
    eligibility = evaluate_market_eligibility(universe, _MAXIMUM_AGE)
    monkeypatch.setattr(
        "cassandra_trader.apps.market_liquidity.evaluate_market_eligibility_as_of",
        lambda *args, **kwargs: eligibility,
    )

    def unexpected_repository(*args, **kwargs):
        raise AssertionError("all-warning catalog has no liquidity candidates")

    monkeypatch.setattr(
        "cassandra_trader.apps.market_liquidity.SqliteMinuteCandleRepository",
        unexpected_repository,
    )

    result = evaluate_market_liquidity_as_of(
        "KRW",
        _WINDOW_END,
        _AS_OF,
        tmp_path / "missing.db",
        _MAXIMUM_AGE,
    )

    assert result.status is MarketLiquidityEvaluationStatus.RESOLVED
    assert result.metrics == ()
    assert result.catalog_excluded_count == 1


def test_application_reuses_one_repository_and_skips_warning_markets(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    universe = MarketUniverseResult(
        status=MarketUniverseStatus.AVAILABLE,
        quote_currency="KRW",
        as_of=_AS_OF,
        snapshot_id="snapshot-reuse",
        observed_at=_AS_OF - timedelta(minutes=5),
        available_at=_AS_OF - timedelta(minutes=4),
        content_hash="b" * 64,
        pairs=(
            _pair("KRW-ADA", warning=True),
            _pair("KRW-BTC"),
            _pair("KRW-ETH"),
        ),
    )
    eligibility = evaluate_market_eligibility(universe, _MAXIMUM_AGE)
    repositories: list[object] = []
    summary_calls: list[tuple[str, object]] = []

    monkeypatch.setattr(
        "cassandra_trader.apps.market_liquidity.evaluate_market_eligibility_as_of",
        lambda *args, **kwargs: eligibility,
    )

    class FakeRepository:
        def __init__(self, database: str | Path) -> None:
            self.database = database
            repositories.append(self)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

    def fake_summary(
        market: str,
        start: datetime,
        end: datetime,
        as_of: datetime,
        repository: object,
    ) -> MinuteSlotSummary:
        summary_calls.append((market, repository))
        return MinuteSlotSummary(
            market=market,
            start_time_utc=start,
            end_time_utc=end,
            as_of=as_of,
            expected_count=43_200,
            traded_count=0,
            no_trade_count=43_200,
            unresolved_count=0,
            quote_volume=Decimal(0),
        )

    monkeypatch.setattr(
        "cassandra_trader.apps.market_liquidity.SqliteMinuteCandleRepository",
        FakeRepository,
    )
    monkeypatch.setattr(
        "cassandra_trader.apps.market_liquidity.summarize_minute_slots_from_repository",
        fake_summary,
    )

    result = evaluate_market_liquidity_as_of(
        "KRW",
        _WINDOW_END,
        _AS_OF,
        tmp_path / "missing.db",
        _MAXIMUM_AGE,
    )

    assert result.status is MarketLiquidityEvaluationStatus.RESOLVED
    assert len(repositories) == 1
    assert summary_calls == [
        ("KRW-BTC", repositories[0]),
        ("KRW-ETH", repositories[0]),
    ]


@pytest.mark.parametrize(
    ("quote_currency", "window_end", "as_of", "maximum_age"),
    [
        ("USDT", _WINDOW_END, _AS_OF, _MAXIMUM_AGE),
        ("KRW", _WINDOW_END.replace(tzinfo=None), _AS_OF, _MAXIMUM_AGE),
        ("KRW", _WINDOW_END + timedelta(microseconds=1), _AS_OF, _MAXIMUM_AGE),
        ("KRW", _WINDOW_END, _AS_OF.replace(tzinfo=None), _MAXIMUM_AGE),
        ("KRW", _WINDOW_END, _WINDOW_END - timedelta(microseconds=1), _MAXIMUM_AGE),
        ("KRW", _WINDOW_END, _AS_OF, timedelta(0)),
        ("KRW", _WINDOW_END, _AS_OF, timedelta(microseconds=-1)),
        (
            "KRW",
            datetime.min.replace(tzinfo=UTC),
            datetime.min.replace(tzinfo=UTC),
            _MAXIMUM_AGE,
        ),
    ],
)
def test_invalid_inputs_are_rejected_before_catalog_or_database_query(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    quote_currency: str,
    window_end: datetime,
    as_of: datetime,
    maximum_age: timedelta,
) -> None:
    called = False

    def unexpected_eligibility(*args, **kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(
        "cassandra_trader.apps.market_liquidity.evaluate_market_eligibility_as_of",
        unexpected_eligibility,
    )

    with pytest.raises(ValueError):
        evaluate_market_liquidity_as_of(
            quote_currency,
            window_end,
            as_of,
            tmp_path / "missing.db",
            maximum_age,
        )

    assert called is False
    assert not (tmp_path / "missing.db").exists()
