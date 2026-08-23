"""Catalog-level market eligibility from one point-in-time universe."""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum

from cassandra_trader.domain.market import (
    MarketUniverseResult,
    MarketUniverseStatus,
    TradingPair,
    normalize_quote_currency,
)

MARKET_ELIGIBILITY_POLICY_NAME = "catalog-eligibility"
MARKET_ELIGIBILITY_POLICY_VERSION = "v1"


class MarketEligibilityEvaluationStatus(StrEnum):
    """Whether catalog-level eligibility could be resolved."""

    RESOLVED = "RESOLVED"
    UNRESOLVED = "UNRESOLVED"


class MarketEligibilityDecisionStatus(StrEnum):
    """Catalog-level decision for one point-in-time market candidate."""

    ELIGIBLE = "ELIGIBLE"
    EXCLUDED = "EXCLUDED"
    UNRESOLVED = "UNRESOLVED"


class MarketEligibilityReason(StrEnum):
    """Evidence-backed reason for exclusion or unresolved evaluation."""

    WARNING = "WARNING"
    UNIVERSE_UNRESOLVED = "UNIVERSE_UNRESOLVED"
    STALE_UNIVERSE = "STALE_UNIVERSE"


@dataclass(frozen=True, slots=True)
class MarketEligibilityDecision:
    """One immutable decision tied to its catalog snapshot pair metadata."""

    pair: TradingPair
    status: MarketEligibilityDecisionStatus
    reasons: tuple[MarketEligibilityReason, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.pair, TradingPair):
            raise ValueError("eligibility decision requires a trading pair")

        if not isinstance(self.reasons, tuple) or any(
            not isinstance(reason, MarketEligibilityReason) for reason in self.reasons
        ):
            raise ValueError("eligibility decision reasons must be an immutable reason tuple")

        if self.status is MarketEligibilityDecisionStatus.ELIGIBLE:
            if self.pair.is_warning or self.reasons:
                raise ValueError("ELIGIBLE requires a non-warning pair without reasons")
        elif self.status is MarketEligibilityDecisionStatus.EXCLUDED:
            if not self.pair.is_warning or self.reasons != (MarketEligibilityReason.WARNING,):
                raise ValueError("EXCLUDED requires a warning pair and WARNING reason")
        elif self.status is MarketEligibilityDecisionStatus.UNRESOLVED:
            if self.reasons != (MarketEligibilityReason.STALE_UNIVERSE,):
                raise ValueError("candidate UNRESOLVED requires STALE_UNIVERSE reason")
        else:
            raise ValueError(f"unsupported eligibility decision status: {self.status}")

    @property
    def market(self) -> str:
        """Return the normalized market code from the source snapshot."""

        return self.pair.code

    @property
    def is_warning(self) -> bool:
        """Return the warning flag observed in the source snapshot."""

        return self.pair.is_warning


@dataclass(frozen=True, slots=True)
class MarketEligibilityResult:
    """Reproducible catalog eligibility at one information cutoff."""

    status: MarketEligibilityEvaluationStatus
    quote_currency: str
    as_of: datetime
    maximum_snapshot_age: timedelta
    snapshot_id: str | None
    observed_at: datetime | None
    available_at: datetime | None
    content_hash: str | None
    decisions: tuple[MarketEligibilityDecision, ...]
    reasons: tuple[MarketEligibilityReason, ...]
    policy_name: str = field(init=False, default=MARKET_ELIGIBILITY_POLICY_NAME)
    policy_version: str = field(init=False, default=MARKET_ELIGIBILITY_POLICY_VERSION)

    def __post_init__(self) -> None:
        if self.quote_currency != normalize_quote_currency(self.quote_currency):
            raise ValueError("quote_currency must be normalized")

        _validate_utc(self.as_of, "as_of")
        validate_maximum_snapshot_age(self.maximum_snapshot_age)

        if not isinstance(self.decisions, tuple) or any(
            not isinstance(decision, MarketEligibilityDecision) for decision in self.decisions
        ):
            raise ValueError("eligibility decisions must be an immutable decision tuple")

        if not isinstance(self.reasons, tuple) or any(
            not isinstance(reason, MarketEligibilityReason) for reason in self.reasons
        ):
            raise ValueError("eligibility reasons must be an immutable reason tuple")

        if self.decisions != tuple(sorted(self.decisions, key=lambda item: item.market)):
            raise ValueError("eligibility decisions must be ordered by market code")

        if len({decision.market for decision in self.decisions}) != len(self.decisions):
            raise ValueError("eligibility decisions must not contain duplicate markets")

        if any(decision.pair.quote_currency != self.quote_currency for decision in self.decisions):
            raise ValueError("decision quote currency must match the result")

        metadata_is_complete = self._validate_snapshot_metadata()

        if self.status is MarketEligibilityEvaluationStatus.RESOLVED:
            if self.reasons:
                raise ValueError("RESOLVED eligibility must not include evaluation reasons")

            if not metadata_is_complete or not self.decisions:
                raise ValueError("RESOLVED eligibility requires snapshot metadata and decisions")

            if self.snapshot_age is None or self.snapshot_age > self.maximum_snapshot_age:
                raise ValueError("RESOLVED eligibility requires a fresh universe snapshot")

            if self.unresolved_count != 0:
                raise ValueError("RESOLVED eligibility must not include unresolved decisions")
        elif self.status is MarketEligibilityEvaluationStatus.UNRESOLVED:
            if self.eligible_count != 0:
                raise ValueError("UNRESOLVED eligibility must not include eligible markets")

            if self.reasons == (MarketEligibilityReason.UNIVERSE_UNRESOLVED,):
                if metadata_is_complete or self.decisions:
                    raise ValueError(
                        "unresolved source universe must not include snapshot data or decisions"
                    )
            elif self.reasons == (MarketEligibilityReason.STALE_UNIVERSE,):
                if not metadata_is_complete or not self.decisions:
                    raise ValueError("stale universe requires snapshot metadata and decisions")

                if self.snapshot_age is None or self.snapshot_age <= self.maximum_snapshot_age:
                    raise ValueError("STALE_UNIVERSE requires age above the maximum")

                if self.unresolved_count != self.candidate_count:
                    raise ValueError("stale universe requires every candidate to be unresolved")
            else:
                raise ValueError("UNRESOLVED eligibility requires one unresolved reason")
        else:
            raise ValueError(f"unsupported eligibility evaluation status: {self.status}")

    def _validate_snapshot_metadata(self) -> bool:
        metadata = (
            self.snapshot_id,
            self.observed_at,
            self.available_at,
            self.content_hash,
        )
        present = tuple(value is not None for value in metadata)

        if any(present) and not all(present):
            raise ValueError("snapshot metadata must be entirely present or absent")

        if not all(present):
            return False

        if not isinstance(self.snapshot_id, str) or not self.snapshot_id.strip():
            raise ValueError("snapshot_id must not be empty")

        if not isinstance(self.observed_at, datetime) or not isinstance(
            self.available_at, datetime
        ):
            raise ValueError("snapshot timestamps must be datetimes")

        _validate_utc(self.observed_at, "observed_at")
        _validate_utc(self.available_at, "available_at")

        if self.observed_at > self.available_at:
            raise ValueError("observed_at must not be after available_at")

        if self.available_at > self.as_of:
            raise ValueError("available_at must not be after as_of")

        if not isinstance(self.content_hash, str) or len(self.content_hash) != 64:
            raise ValueError("content_hash must be a 64-character string")

        return True

    @property
    def snapshot_age(self) -> timedelta | None:
        """Return freshness age measured from observation, not availability."""

        if self.observed_at is None:
            return None

        return self.as_of - self.observed_at

    @property
    def candidate_count(self) -> int:
        """Return the number of source snapshot candidates."""

        return len(self.decisions)

    @property
    def eligible_count(self) -> int:
        """Return the number of catalog-level eligible candidates."""

        return sum(
            decision.status is MarketEligibilityDecisionStatus.ELIGIBLE
            for decision in self.decisions
        )

    @property
    def excluded_count(self) -> int:
        """Return the number of warning-excluded candidates."""

        return sum(
            decision.status is MarketEligibilityDecisionStatus.EXCLUDED
            for decision in self.decisions
        )

    @property
    def unresolved_count(self) -> int:
        """Return the number of candidates blocked by stale source evidence."""

        return sum(
            decision.status is MarketEligibilityDecisionStatus.UNRESOLVED
            for decision in self.decisions
        )

    @property
    def eligible_pairs(self) -> tuple[TradingPair, ...]:
        """Derive eligible pairs without duplicating mutable result state."""

        return tuple(
            decision.pair
            for decision in self.decisions
            if decision.status is MarketEligibilityDecisionStatus.ELIGIBLE
        )


def evaluate_market_eligibility(
    universe: MarketUniverseResult,
    maximum_snapshot_age: timedelta,
) -> MarketEligibilityResult:
    """Apply the versioned catalog eligibility policy to one resolved universe."""

    validate_maximum_snapshot_age(maximum_snapshot_age)

    if universe.status is MarketUniverseStatus.UNRESOLVED:
        return MarketEligibilityResult(
            status=MarketEligibilityEvaluationStatus.UNRESOLVED,
            quote_currency=universe.quote_currency,
            as_of=universe.as_of,
            maximum_snapshot_age=maximum_snapshot_age,
            snapshot_id=None,
            observed_at=None,
            available_at=None,
            content_hash=None,
            decisions=(),
            reasons=(MarketEligibilityReason.UNIVERSE_UNRESOLVED,),
        )

    if universe.snapshot_age is None:
        raise ValueError("AVAILABLE universe must include snapshot age")

    metadata = {
        "quote_currency": universe.quote_currency,
        "as_of": universe.as_of,
        "maximum_snapshot_age": maximum_snapshot_age,
        "snapshot_id": universe.snapshot_id,
        "observed_at": universe.observed_at,
        "available_at": universe.available_at,
        "content_hash": universe.content_hash,
    }

    if universe.snapshot_age > maximum_snapshot_age:
        decisions = tuple(
            MarketEligibilityDecision(
                pair=pair,
                status=MarketEligibilityDecisionStatus.UNRESOLVED,
                reasons=(MarketEligibilityReason.STALE_UNIVERSE,),
            )
            for pair in universe.pairs
        )
        return MarketEligibilityResult(
            status=MarketEligibilityEvaluationStatus.UNRESOLVED,
            decisions=decisions,
            reasons=(MarketEligibilityReason.STALE_UNIVERSE,),
            **metadata,
        )

    decisions = tuple(
        MarketEligibilityDecision(
            pair=pair,
            status=(
                MarketEligibilityDecisionStatus.EXCLUDED
                if pair.is_warning
                else MarketEligibilityDecisionStatus.ELIGIBLE
            ),
            reasons=(MarketEligibilityReason.WARNING,) if pair.is_warning else (),
        )
        for pair in universe.pairs
    )
    return MarketEligibilityResult(
        status=MarketEligibilityEvaluationStatus.RESOLVED,
        decisions=decisions,
        reasons=(),
        **metadata,
    )


def validate_maximum_snapshot_age(value: timedelta) -> None:
    """Reject missing, zero, or negative freshness policy windows."""

    if not isinstance(value, timedelta) or value <= timedelta(0):
        raise ValueError("maximum_snapshot_age must be positive")


def _validate_utc(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be timezone-aware UTC")
