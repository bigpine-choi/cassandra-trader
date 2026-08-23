from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from cassandra_trader.domain.liquidity_top_n import (
    LIQUIDITY_TOP_N_POLICY_NAME,
    LIQUIDITY_TOP_N_POLICY_VERSION,
    LiquidityTopNDecision,
    LiquidityTopNDecisionStatus,
    LiquidityTopNEvaluationStatus,
    LiquidityTopNReason,
    LiquidityTopNResult,
    evaluate_liquidity_top_n,
    validate_top_n,
)
from cassandra_trader.domain.market import (
    MarketUniverseResult,
    MarketUniverseStatus,
    TradingPair,
)
from cassandra_trader.domain.market_eligibility import evaluate_market_eligibility
from cassandra_trader.domain.market_liquidity import evaluate_market_liquidity
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


def _summary(
    market: str,
    quote_volume: str,
    *,
    unresolved_count: int = 0,
) -> MinuteSlotSummary:
    return MinuteSlotSummary(
        market=market,
        start_time_utc=_WINDOW_START,
        end_time_utc=_WINDOW_END,
        as_of=_AS_OF,
        expected_count=43_200,
        traded_count=1,
        no_trade_count=43_199 - unresolved_count,
        unresolved_count=unresolved_count,
        quote_volume=Decimal(quote_volume),
    )


def _resolved_liquidity():
    eligibility = _eligibility(_pair("KRW-ADA"), _pair("KRW-BTC"), _pair("KRW-ETH"))
    return evaluate_market_liquidity(
        eligibility,
        _WINDOW_END,
        (
            _summary("KRW-ADA", "20.000000001"),
            _summary("KRW-BTC", "20.000000001"),
            _summary("KRW-ETH", "10.5"),
        ),
    )


@pytest.mark.parametrize(
    ("top_n", "selected", "excluded"),
    [
        (1, ("KRW-ADA",), ("KRW-BTC", "KRW-ETH")),
        (2, ("KRW-ADA", "KRW-BTC"), ("KRW-ETH",)),
        (3, ("KRW-ADA", "KRW-BTC", "KRW-ETH"), ()),
        (5, ("KRW-ADA", "KRW-BTC", "KRW-ETH"), ()),
    ],
)
def test_resolved_top_n_uses_existing_ordinal_rank_without_expanding_ties(
    top_n: int,
    selected: tuple[str, ...],
    excluded: tuple[str, ...],
) -> None:
    liquidity = _resolved_liquidity()

    result = evaluate_liquidity_top_n(liquidity, top_n)

    assert result.status is LiquidityTopNEvaluationStatus.RESOLVED
    assert result.policy_name == LIQUIDITY_TOP_N_POLICY_NAME
    assert result.policy_version == LIQUIDITY_TOP_N_POLICY_VERSION
    assert result.selected_markets == selected
    assert (
        tuple(
            decision.market
            for decision in result.decisions
            if decision.status is LiquidityTopNDecisionStatus.EXCLUDED
        )
        == excluded
    )
    assert result.selected_count == len(selected)
    assert result.excluded_count == len(excluded)
    assert result.unresolved_count == 0
    assert [decision.market for decision in result.decisions] == [
        "KRW-ADA",
        "KRW-BTC",
        "KRW-ETH",
    ]
    assert [decision.liquidity_rank for decision in result.decisions] == [1, 2, 3]
    assert [decision.quote_volume for decision in result.decisions] == [
        Decimal("20.000000001"),
        Decimal("20.000000001"),
        Decimal("10.5"),
    ]
    assert all(
        decision.reasons
        == (
            ()
            if decision.status is LiquidityTopNDecisionStatus.SELECTED
            else (LiquidityTopNReason.OUTSIDE_TOP_N,)
        )
        for decision in result.decisions
    )


@pytest.mark.parametrize("top_n", [0, -1, True, False, 1.0, "1"])
def test_top_n_must_be_a_non_bool_positive_integer(top_n: object) -> None:
    with pytest.raises(ValueError):
        validate_top_n(top_n)  # type: ignore[arg-type]

    with pytest.raises(ValueError):
        evaluate_liquidity_top_n(_resolved_liquidity(), top_n)  # type: ignore[arg-type]


def test_globally_unresolved_liquidity_suppresses_all_selection_and_partial_rank() -> None:
    liquidity = evaluate_market_liquidity(
        _eligibility(_pair("KRW-ADA"), _pair("KRW-BTC")),
        _WINDOW_END,
        (
            _summary("KRW-ADA", "10"),
            _summary("KRW-BTC", "999", unresolved_count=1),
        ),
    )

    result = evaluate_liquidity_top_n(liquidity, 1)

    assert result.status is LiquidityTopNEvaluationStatus.UNRESOLVED
    assert result.reasons == (LiquidityTopNReason.LIQUIDITY_UNRESOLVED,)
    assert result.selected_markets == ()
    assert result.selected_count == 0
    assert result.excluded_count == 0
    assert result.unresolved_count == 2
    assert all(
        decision.status is LiquidityTopNDecisionStatus.UNRESOLVED
        and decision.liquidity_rank is None
        and decision.reasons == (LiquidityTopNReason.LIQUIDITY_UNRESOLVED,)
        for decision in result.decisions
    )
    assert result.decisions[0].quote_volume == Decimal("10")
    assert result.decisions[1].quote_volume is None


def test_unresolved_catalog_and_all_warning_catalog_keep_fail_closed_empty_semantics() -> None:
    missing_universe = MarketUniverseResult(
        status=MarketUniverseStatus.UNRESOLVED,
        quote_currency="KRW",
        as_of=_AS_OF,
        snapshot_id=None,
        observed_at=None,
        available_at=None,
        content_hash=None,
        pairs=(),
    )
    unresolved_liquidity = evaluate_market_liquidity(
        evaluate_market_eligibility(missing_universe, _MAXIMUM_AGE),
        _WINDOW_END,
        (),
    )
    all_warning_liquidity = evaluate_market_liquidity(
        _eligibility(_pair("KRW-ADA", warning=True), _pair("KRW-BTC", warning=True)),
        _WINDOW_END,
        (),
    )

    unresolved = evaluate_liquidity_top_n(unresolved_liquidity, 30)
    empty = evaluate_liquidity_top_n(all_warning_liquidity, 30)

    assert unresolved.status is LiquidityTopNEvaluationStatus.UNRESOLVED
    assert unresolved.decisions == ()
    assert unresolved.selected_count == 0
    assert unresolved.excluded_count == 0
    assert unresolved.unresolved_count == 0
    assert unresolved.snapshot_id is None
    assert empty.status is LiquidityTopNEvaluationStatus.RESOLVED
    assert empty.decisions == ()
    assert empty.selected_markets == ()
    assert empty.selected_count == 0
    assert empty.excluded_count == 0
    assert empty.unresolved_count == 0
    assert empty.catalog_candidate_count == 2
    assert empty.catalog_excluded_count == 2


def test_result_preserves_all_upstream_metadata_and_exposes_read_only_counts() -> None:
    liquidity = _resolved_liquidity()
    result = evaluate_liquidity_top_n(liquidity, 2)

    assert result.liquidity_policy_name == liquidity.policy_name
    assert result.liquidity_policy_version == liquidity.policy_version
    assert result.catalog_policy_name == liquidity.catalog_policy_name
    assert result.catalog_policy_version == liquidity.catalog_policy_version
    assert result.quote_currency == liquidity.quote_currency
    assert result.window_start == liquidity.window_start
    assert result.window_end == liquidity.window_end
    assert result.as_of == liquidity.as_of
    assert result.lookback == liquidity.lookback
    assert result.maximum_snapshot_age == liquidity.maximum_snapshot_age
    assert result.snapshot_id == liquidity.snapshot_id
    assert result.observed_at == liquidity.observed_at
    assert result.available_at == liquidity.available_at
    assert result.content_hash == liquidity.content_hash
    assert result.catalog_candidate_count == liquidity.catalog_candidate_count
    assert result.catalog_excluded_count == liquidity.catalog_excluded_count
    assert result.liquidity_candidate_count == liquidity.candidate_count
    assert result.liquidity_resolved_count == liquidity.resolved_market_count
    assert result.liquidity_unresolved_count == liquidity.unresolved_market_count

    fields = LiquidityTopNResult.__dataclass_fields__
    assert "selected_markets" not in fields
    assert "liquidity_candidate_count" not in fields
    assert "selected_count" not in fields
    assert "excluded_count" not in fields
    assert "unresolved_count" not in fields


def test_decision_and_result_reject_contradictory_state_reason_rank_volume_and_order() -> None:
    selected = LiquidityTopNDecision(
        market="KRW-BTC",
        status=LiquidityTopNDecisionStatus.SELECTED,
        quote_volume=Decimal("10.000000001"),
        liquidity_rank=1,
        reasons=(),
    )

    for changes in (
        {"status": LiquidityTopNDecisionStatus.EXCLUDED},
        {"status": LiquidityTopNDecisionStatus.UNRESOLVED},
        {"quote_volume": None},
        {"quote_volume": 10.0},
        {"liquidity_rank": None},
        {"liquidity_rank": True},
        {"liquidity_rank": 0},
        {"liquidity_rank": -1},
        {"quote_volume": Decimal("-1")},
        {"quote_volume": Decimal("NaN")},
        {"quote_volume": Decimal("Infinity")},
        {"reasons": (LiquidityTopNReason.OUTSIDE_TOP_N,)},
    ):
        with pytest.raises(ValueError):
            replace(selected, **changes)

    result = evaluate_liquidity_top_n(_resolved_liquidity(), 2)

    for changes in (
        {"top_n": True},
        {"decisions": tuple(reversed(result.decisions))},
        {"decisions": (result.decisions[0], result.decisions[0])},
        {
            "decisions": (
                result.decisions[0],
                replace(result.decisions[1], liquidity_rank=1),
                result.decisions[2],
            )
        },
        {
            "decisions": (
                result.decisions[0],
                result.decisions[1],
                replace(result.decisions[2], liquidity_rank=4),
            )
        },
        {
            "decisions": (
                replace(result.decisions[0], liquidity_rank=2),
                replace(result.decisions[1], liquidity_rank=1),
                result.decisions[2],
            )
        },
        {
            "decisions": (
                result.decisions[0],
                replace(
                    result.decisions[1],
                    status=LiquidityTopNDecisionStatus.EXCLUDED,
                    reasons=(LiquidityTopNReason.OUTSIDE_TOP_N,),
                ),
                result.decisions[2],
            )
        },
        {
            "decisions": (
                result.decisions[0],
                result.decisions[1],
                replace(
                    result.decisions[2],
                    status=LiquidityTopNDecisionStatus.SELECTED,
                    reasons=(),
                ),
            )
        },
        {"status": LiquidityTopNEvaluationStatus.UNRESOLVED},
        {"reasons": (LiquidityTopNReason.LIQUIDITY_UNRESOLVED,)},
        {"window_start": _WINDOW_START + timedelta(minutes=1)},
        {"as_of": _WINDOW_END - timedelta(microseconds=1)},
        {"as_of": _AS_OF.replace(tzinfo=None)},
        {"catalog_candidate_count": result.catalog_candidate_count + 1},
        {"liquidity_resolved_count": True},
        {"liquidity_resolved_count": 2, "liquidity_unresolved_count": 1},
        {"liquidity_policy_name": "changed"},
        {"catalog_policy_version": "changed"},
    ):
        with pytest.raises(ValueError):
            replace(result, **changes)


def test_unresolved_decision_rejects_rank_and_non_liquidity_reason() -> None:
    decision = LiquidityTopNDecision(
        market="KRW-BTC",
        status=LiquidityTopNDecisionStatus.UNRESOLVED,
        quote_volume=Decimal("10"),
        liquidity_rank=None,
        reasons=(LiquidityTopNReason.LIQUIDITY_UNRESOLVED,),
    )

    for changes in (
        {"liquidity_rank": 1},
        {"reasons": (LiquidityTopNReason.OUTSIDE_TOP_N,)},
    ):
        with pytest.raises(ValueError):
            replace(decision, **changes)


@pytest.mark.parametrize(
    ("volumes", "top_n"),
    [
        ((("KRW-BTC", "1"),), 1),
        ((("KRW-ETH", "10"), ("KRW-ADA", "20"), ("KRW-BTC", "20")), 1),
        ((("KRW-ETH", "10"), ("KRW-ADA", "20"), ("KRW-BTC", "20")), 3),
        ((("KRW-ETH", "10"), ("KRW-ADA", "20"), ("KRW-BTC", "20")), 10),
    ],
)
def test_resolved_boundary_matrix_matches_reference_rank_rule(
    volumes: tuple[tuple[str, str], ...],
    top_n: int,
) -> None:
    ordered = tuple(sorted(volumes))
    liquidity = evaluate_market_liquidity(
        _eligibility(*(_pair(market) for market, _ in ordered)),
        _WINDOW_END,
        tuple(_summary(market, volume) for market, volume in ordered),
    )

    result = evaluate_liquidity_top_n(liquidity, top_n)

    assert tuple(decision.market for decision in result.decisions) == tuple(
        market for market, _ in ordered
    )
    assert all(
        decision.status
        is (
            LiquidityTopNDecisionStatus.SELECTED
            if decision.liquidity_rank is not None and decision.liquidity_rank <= top_n
            else LiquidityTopNDecisionStatus.EXCLUDED
        )
        for decision in result.decisions
    )
    assert result.selected_count == min(top_n, result.liquidity_candidate_count)
    assert result.unresolved_count == 0

    if result.liquidity_candidate_count > 1:
        with pytest.raises(ValueError):
            replace(liquidity, metrics=tuple(reversed(liquidity.metrics)))


def test_all_upstream_unresolved_metrics_suppress_every_selection() -> None:
    liquidity = evaluate_market_liquidity(
        _eligibility(_pair("KRW-ADA"), _pair("KRW-BTC")),
        _WINDOW_END,
        (
            _summary("KRW-ADA", "10", unresolved_count=1),
            _summary("KRW-BTC", "20", unresolved_count=1),
        ),
    )

    result = evaluate_liquidity_top_n(liquidity, 1)

    assert result.status is LiquidityTopNEvaluationStatus.UNRESOLVED
    assert result.selected_count == 0
    assert result.excluded_count == 0
    assert result.unresolved_count == 2
    assert result.liquidity_resolved_count == 0
    assert result.liquidity_unresolved_count == 2
    assert all(decision.quote_volume is None for decision in result.decisions)

    with pytest.raises(ValueError):
        replace(
            result,
            liquidity_resolved_count=2,
            liquidity_unresolved_count=0,
        )
