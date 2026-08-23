from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from cassandra_trader.domain.market import (
    MarketUniverseResult,
    MarketUniverseStatus,
    TradingPair,
)
from cassandra_trader.domain.market_eligibility import (
    MARKET_ELIGIBILITY_POLICY_NAME,
    MARKET_ELIGIBILITY_POLICY_VERSION,
    MarketEligibilityDecision,
    MarketEligibilityDecisionStatus,
    MarketEligibilityEvaluationStatus,
    MarketEligibilityReason,
    MarketEligibilityResult,
    evaluate_market_eligibility,
)

_AS_OF = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
_MAXIMUM_AGE = timedelta(hours=1)
_ADA = TradingPair("KRW-ADA", "에이다", "Cardano", True)
_BTC = TradingPair("KRW-BTC", "비트코인", "Bitcoin", False)


def _available_universe(
    *,
    observed_at: datetime | None = None,
    available_at: datetime | None = None,
    pairs: tuple[TradingPair, ...] = (_ADA, _BTC),
) -> MarketUniverseResult:
    observation = observed_at or _AS_OF - timedelta(minutes=30)
    availability = available_at or observation + timedelta(seconds=1)
    return MarketUniverseResult(
        status=MarketUniverseStatus.AVAILABLE,
        quote_currency="KRW",
        as_of=_AS_OF,
        snapshot_id="snapshot-1",
        observed_at=observation,
        available_at=availability,
        content_hash="a" * 64,
        pairs=pairs,
    )


def _unresolved_universe() -> MarketUniverseResult:
    return MarketUniverseResult(
        status=MarketUniverseStatus.UNRESOLVED,
        quote_currency="KRW",
        as_of=_AS_OF,
        snapshot_id=None,
        observed_at=None,
        available_at=None,
        content_hash=None,
        pairs=(),
    )


def test_fresh_universe_resolves_warning_and_normal_markets_deterministically() -> None:
    result = evaluate_market_eligibility(_available_universe(), _MAXIMUM_AGE)

    assert result.status is MarketEligibilityEvaluationStatus.RESOLVED
    assert result.policy_name == MARKET_ELIGIBILITY_POLICY_NAME
    assert result.policy_version == MARKET_ELIGIBILITY_POLICY_VERSION
    assert result.maximum_snapshot_age == _MAXIMUM_AGE
    assert result.reasons == ()
    assert [decision.market for decision in result.decisions] == ["KRW-ADA", "KRW-BTC"]
    assert result.decisions[0].status is MarketEligibilityDecisionStatus.EXCLUDED
    assert result.decisions[0].reasons == (MarketEligibilityReason.WARNING,)
    assert result.decisions[0].is_warning is True
    assert result.decisions[1].status is MarketEligibilityDecisionStatus.ELIGIBLE
    assert result.decisions[1].reasons == ()
    assert result.decisions[1].is_warning is False
    assert result.eligible_pairs == (_BTC,)
    assert result.candidate_count == 2
    assert result.eligible_count == 1
    assert result.excluded_count == 1
    assert result.unresolved_count == 0


def test_exact_maximum_age_is_fresh_but_one_microsecond_older_is_stale() -> None:
    boundary = evaluate_market_eligibility(
        _available_universe(observed_at=_AS_OF - _MAXIMUM_AGE),
        _MAXIMUM_AGE,
    )
    stale = evaluate_market_eligibility(
        _available_universe(observed_at=_AS_OF - _MAXIMUM_AGE - timedelta(microseconds=1)),
        _MAXIMUM_AGE,
    )

    assert boundary.status is MarketEligibilityEvaluationStatus.RESOLVED
    assert boundary.snapshot_age == _MAXIMUM_AGE
    assert stale.status is MarketEligibilityEvaluationStatus.UNRESOLVED
    assert stale.reasons == (MarketEligibilityReason.STALE_UNIVERSE,)
    assert stale.snapshot_age == _MAXIMUM_AGE + timedelta(microseconds=1)
    assert stale.eligible_pairs == ()
    assert stale.eligible_count == 0
    assert stale.excluded_count == 0
    assert stale.unresolved_count == 2
    assert all(
        decision.status is MarketEligibilityDecisionStatus.UNRESOLVED
        and decision.reasons == (MarketEligibilityReason.STALE_UNIVERSE,)
        for decision in stale.decisions
    )
    assert stale.decisions[0].is_warning is True


def test_freshness_uses_observed_at_instead_of_available_at() -> None:
    result = evaluate_market_eligibility(
        _available_universe(
            observed_at=_AS_OF - timedelta(hours=2),
            available_at=_AS_OF,
        ),
        _MAXIMUM_AGE,
    )

    assert result.status is MarketEligibilityEvaluationStatus.UNRESOLVED
    assert result.snapshot_age == timedelta(hours=2)


def test_unresolved_universe_fails_closed_without_synthetic_candidates() -> None:
    result = evaluate_market_eligibility(_unresolved_universe(), _MAXIMUM_AGE)

    assert result.status is MarketEligibilityEvaluationStatus.UNRESOLVED
    assert result.reasons == (MarketEligibilityReason.UNIVERSE_UNRESOLVED,)
    assert result.snapshot_id is None
    assert result.observed_at is None
    assert result.available_at is None
    assert result.content_hash is None
    assert result.snapshot_age is None
    assert result.decisions == ()
    assert result.candidate_count == 0
    assert result.eligible_pairs == ()


@pytest.mark.parametrize("maximum_age", [timedelta(0), timedelta(microseconds=-1)])
def test_maximum_snapshot_age_must_be_positive(maximum_age: timedelta) -> None:
    with pytest.raises(ValueError, match="maximum_snapshot_age must be positive"):
        evaluate_market_eligibility(_available_universe(), maximum_age)


def test_decision_rejects_status_reason_and_warning_contradictions() -> None:
    invalid_factories = (
        lambda: MarketEligibilityDecision(
            pair=_ADA,
            status=MarketEligibilityDecisionStatus.ELIGIBLE,
            reasons=(),
        ),
        lambda: MarketEligibilityDecision(
            pair=_BTC,
            status=MarketEligibilityDecisionStatus.EXCLUDED,
            reasons=(MarketEligibilityReason.WARNING,),
        ),
        lambda: MarketEligibilityDecision(
            pair=_BTC,
            status=MarketEligibilityDecisionStatus.UNRESOLVED,
            reasons=(MarketEligibilityReason.UNIVERSE_UNRESOLVED,),
        ),
        lambda: MarketEligibilityDecision(
            pair=_BTC,
            status=MarketEligibilityDecisionStatus.ELIGIBLE,
            reasons=[],  # type: ignore[arg-type]
        ),
    )

    for factory in invalid_factories:
        with pytest.raises(ValueError):
            factory()


def test_result_rejects_order_duplicate_quote_count_and_status_contradictions() -> None:
    valid = evaluate_market_eligibility(_available_universe(), _MAXIMUM_AGE)
    eth = TradingPair("KRW-ETH", "이더리움", "Ethereum", False)
    btc_decision = valid.decisions[1]

    invalid_changes = (
        {"reasons": []},
        {"decisions": tuple(reversed(valid.decisions))},
        {"decisions": (btc_decision, btc_decision)},
        {
            "decisions": (
                MarketEligibilityDecision(
                    pair=TradingPair("USDT-BTC", "비트코인", "Bitcoin", False),
                    status=MarketEligibilityDecisionStatus.ELIGIBLE,
                    reasons=(),
                ),
            )
        },
        {
            "status": MarketEligibilityEvaluationStatus.RESOLVED,
            "decisions": (
                MarketEligibilityDecision(
                    pair=eth,
                    status=MarketEligibilityDecisionStatus.UNRESOLVED,
                    reasons=(MarketEligibilityReason.STALE_UNIVERSE,),
                ),
            ),
        },
        {"status": MarketEligibilityEvaluationStatus.UNRESOLVED},
    )

    for changes in invalid_changes:
        with pytest.raises(ValueError):
            replace(valid, **changes)


def test_unresolved_source_and_stale_result_metadata_invariants() -> None:
    missing = evaluate_market_eligibility(_unresolved_universe(), _MAXIMUM_AGE)
    stale = evaluate_market_eligibility(
        _available_universe(observed_at=_AS_OF - timedelta(hours=2)),
        _MAXIMUM_AGE,
    )

    with pytest.raises(ValueError):
        replace(missing, snapshot_id="synthetic")

    with pytest.raises(ValueError):
        replace(stale, snapshot_id=None)

    with pytest.raises(ValueError):
        replace(stale, reasons=(MarketEligibilityReason.UNIVERSE_UNRESOLVED,))


def test_result_rejects_non_utc_or_inconsistent_snapshot_metadata() -> None:
    valid = evaluate_market_eligibility(_available_universe(), _MAXIMUM_AGE)

    for changes in (
        {"as_of": _AS_OF.replace(tzinfo=None)},
        {"observed_at": valid.observed_at.replace(tzinfo=None)},
        {"available_at": valid.available_at.replace(tzinfo=None)},
        {"available_at": _AS_OF + timedelta(microseconds=1)},
    ):
        with pytest.raises(ValueError):
            replace(valid, **changes)


def test_count_properties_are_derived_from_decisions() -> None:
    fields = MarketEligibilityResult.__dataclass_fields__

    assert "candidate_count" not in fields
    assert "eligible_count" not in fields
    assert "excluded_count" not in fields
    assert "unresolved_count" not in fields
    assert "eligible_pairs" not in fields
