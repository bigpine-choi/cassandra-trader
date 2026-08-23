from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from cassandra_trader.domain.market import (
    MarketUniverseResult,
    MarketUniverseStatus,
    TradingPair,
)
from cassandra_trader.domain.market_eligibility import evaluate_market_eligibility
from cassandra_trader.domain.market_liquidity import (
    MARKET_LIQUIDITY_LOOKBACK,
    MARKET_LIQUIDITY_POLICY_NAME,
    MARKET_LIQUIDITY_POLICY_VERSION,
    MarketLiquidityEvaluationStatus,
    MarketLiquidityMetric,
    MarketLiquidityReason,
    MarketLiquidityResult,
    MarketLiquidityStatus,
    evaluate_market_liquidity,
)
from cassandra_trader.domain.minute_slot import MinuteSlotSummary

_WINDOW_END = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
_WINDOW_START = _WINDOW_END - timedelta(days=30)
_AS_OF = _WINDOW_END + timedelta(minutes=10)
_MAXIMUM_AGE = timedelta(hours=1)


def _pair(market: str, *, warning: bool = False) -> TradingPair:
    return TradingPair(market, f"한글-{market}", f"English-{market}", warning)


def _eligibility(*pairs: TradingPair):
    universe = MarketUniverseResult(
        status=MarketUniverseStatus.AVAILABLE,
        quote_currency="KRW",
        as_of=_AS_OF,
        snapshot_id="snapshot-1",
        observed_at=_AS_OF - timedelta(minutes=5),
        available_at=_AS_OF - timedelta(minutes=4),
        content_hash="a" * 64,
        pairs=tuple(sorted(pairs, key=lambda pair: pair.code)),
    )
    return evaluate_market_eligibility(universe, _MAXIMUM_AGE)


def _unresolved_eligibility():
    universe = MarketUniverseResult(
        status=MarketUniverseStatus.UNRESOLVED,
        quote_currency="KRW",
        as_of=_AS_OF,
        snapshot_id=None,
        observed_at=None,
        available_at=None,
        content_hash=None,
        pairs=(),
    )
    return evaluate_market_eligibility(universe, _MAXIMUM_AGE)


def _summary(
    market: str,
    quote_volume: str,
    *,
    traded_count: int = 1,
    unresolved_count: int = 0,
) -> MinuteSlotSummary:
    return MinuteSlotSummary(
        market=market,
        start_time_utc=_WINDOW_START,
        end_time_utc=_WINDOW_END,
        as_of=_AS_OF,
        expected_count=43_200,
        traded_count=traded_count,
        no_trade_count=43_200 - traded_count - unresolved_count,
        unresolved_count=unresolved_count,
        quote_volume=Decimal(quote_volume),
    )


def test_resolved_liquidity_ranks_by_volume_then_market_but_stores_market_order() -> None:
    eligibility = _eligibility(_pair("KRW-ADA"), _pair("KRW-BTC"), _pair("KRW-ETH"))

    result = evaluate_market_liquidity(
        eligibility,
        _WINDOW_END,
        (
            _summary("KRW-ADA", "10.000000001"),
            _summary("KRW-BTC", "20.123456789"),
            _summary("KRW-ETH", "20.123456789"),
        ),
    )

    assert result.status is MarketLiquidityEvaluationStatus.RESOLVED
    assert result.policy_name == MARKET_LIQUIDITY_POLICY_NAME
    assert result.policy_version == MARKET_LIQUIDITY_POLICY_VERSION
    assert result.lookback == MARKET_LIQUIDITY_LOOKBACK
    assert result.maximum_snapshot_age == eligibility.maximum_snapshot_age
    assert result.snapshot_id == eligibility.snapshot_id
    assert result.observed_at == eligibility.observed_at
    assert result.available_at == eligibility.available_at
    assert result.content_hash == eligibility.content_hash
    assert result.catalog_policy_name == eligibility.policy_name
    assert result.catalog_policy_version == eligibility.policy_version
    assert [metric.market for metric in result.metrics] == ["KRW-ADA", "KRW-BTC", "KRW-ETH"]
    assert [metric.rank for metric in result.metrics] == [3, 1, 2]
    assert [metric.quote_volume for metric in result.metrics] == [
        Decimal("10.000000001"),
        Decimal("20.123456789"),
        Decimal("20.123456789"),
    ]
    assert result.candidate_count == 3
    assert result.resolved_market_count == 3
    assert result.unresolved_market_count == 0


def test_one_unresolved_candidate_suppresses_every_rank_and_partial_volume() -> None:
    eligibility = _eligibility(_pair("KRW-ADA"), _pair("KRW-BTC"))

    result = evaluate_market_liquidity(
        eligibility,
        _WINDOW_END,
        (
            _summary("KRW-ADA", "10"),
            _summary("KRW-BTC", "999", unresolved_count=1),
        ),
    )

    assert result.status is MarketLiquidityEvaluationStatus.UNRESOLVED
    assert result.reasons == (MarketLiquidityReason.LIQUIDITY_DATA_UNRESOLVED,)
    assert result.metrics[0].status is MarketLiquidityStatus.RESOLVED
    assert result.metrics[0].quote_volume == Decimal("10")
    assert result.metrics[1].status is MarketLiquidityStatus.UNRESOLVED
    assert result.metrics[1].quote_volume is None
    assert all(metric.rank is None for metric in result.metrics)
    assert result.resolved_market_count == 1
    assert result.unresolved_market_count == 1


def test_all_warning_fresh_catalog_is_a_resolved_empty_liquidity_result() -> None:
    eligibility = _eligibility(
        _pair("KRW-ADA", warning=True),
        _pair("KRW-BTC", warning=True),
    )

    result = evaluate_market_liquidity(eligibility, _WINDOW_END, ())

    assert result.status is MarketLiquidityEvaluationStatus.RESOLVED
    assert result.metrics == ()
    assert result.catalog_candidate_count == 2
    assert result.catalog_excluded_count == 2
    assert result.candidate_count == 0


def test_unresolved_catalog_fails_closed_without_metrics_or_snapshot_metadata() -> None:
    result = evaluate_market_liquidity(_unresolved_eligibility(), _WINDOW_END, ())

    assert result.status is MarketLiquidityEvaluationStatus.UNRESOLVED
    assert result.reasons == (MarketLiquidityReason.CATALOG_ELIGIBILITY_UNRESOLVED,)
    assert result.metrics == ()
    assert result.snapshot_id is None
    assert result.observed_at is None
    assert result.available_at is None
    assert result.content_hash is None


def test_metric_and_result_reject_contradictory_counts_volume_rank_and_order() -> None:
    with pytest.raises(ValueError):
        MarketLiquidityMetric(
            market="KRW-BTC",
            status=MarketLiquidityStatus.RESOLVED,
            expected_count=43_200,
            traded_count=1,
            no_trade_count=43_198,
            unresolved_count=1,
            quote_volume=Decimal("10"),
            rank=1,
            reasons=(),
        )

    with pytest.raises(ValueError):
        MarketLiquidityMetric(
            market="KRW-BTC",
            status=MarketLiquidityStatus.UNRESOLVED,
            expected_count=43_200,
            traded_count=1,
            no_trade_count=43_198,
            unresolved_count=1,
            quote_volume=Decimal("10"),
            rank=None,
            reasons=(MarketLiquidityReason.LIQUIDITY_DATA_UNRESOLVED,),
        )

    for changes in (
        {"expected_count": True},
        {"rank": True},
    ):
        values = {
            "market": "KRW-BTC",
            "status": MarketLiquidityStatus.RESOLVED,
            "expected_count": 43_200,
            "traded_count": 1,
            "no_trade_count": 43_199,
            "unresolved_count": 0,
            "quote_volume": Decimal("10"),
            "rank": 1,
            "reasons": (),
        }
        values.update(changes)

        with pytest.raises(ValueError):
            MarketLiquidityMetric(**values)  # type: ignore[arg-type]

    eligibility = _eligibility(_pair("KRW-ADA"), _pair("KRW-BTC"))
    resolved = evaluate_market_liquidity(
        eligibility,
        _WINDOW_END,
        (_summary("KRW-ADA", "1"), _summary("KRW-BTC", "2")),
    )

    for changes in (
        {"metrics": tuple(reversed(resolved.metrics))},
        {"metrics": (resolved.metrics[0], resolved.metrics[0])},
        {"status": MarketLiquidityEvaluationStatus.UNRESOLVED},
        {"window_start": _WINDOW_START + timedelta(minutes=1)},
        {"as_of": _AS_OF.replace(tzinfo=None)},
    ):
        with pytest.raises(ValueError):
            replace(resolved, **changes)


def test_liquidity_counts_are_read_only_properties() -> None:
    fields = MarketLiquidityResult.__dataclass_fields__

    assert "candidate_count" not in fields
    assert "resolved_market_count" not in fields
    assert "unresolved_market_count" not in fields
