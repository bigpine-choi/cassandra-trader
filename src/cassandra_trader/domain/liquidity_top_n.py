"""Versioned top-N shortlist policy over point-in-time liquidity ranks."""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from cassandra_trader.domain.market import normalize_quote_currency
from cassandra_trader.domain.market_eligibility import (
    MARKET_ELIGIBILITY_POLICY_NAME,
    MARKET_ELIGIBILITY_POLICY_VERSION,
    validate_maximum_snapshot_age,
)
from cassandra_trader.domain.market_liquidity import (
    MARKET_LIQUIDITY_LOOKBACK,
    MARKET_LIQUIDITY_POLICY_NAME,
    MARKET_LIQUIDITY_POLICY_VERSION,
    MarketLiquidityEvaluationStatus,
    MarketLiquidityMetric,
    MarketLiquidityResult,
)

LIQUIDITY_TOP_N_POLICY_NAME = "liquidity-top-n"
LIQUIDITY_TOP_N_POLICY_VERSION = "v1"


class LiquidityTopNEvaluationStatus(StrEnum):
    """Whether a complete upstream liquidity rank was available."""

    RESOLVED = "RESOLVED"
    UNRESOLVED = "UNRESOLVED"


class LiquidityTopNDecisionStatus(StrEnum):
    """Top-N shortlist decision for one liquidity candidate."""

    SELECTED = "SELECTED"
    EXCLUDED = "EXCLUDED"
    UNRESOLVED = "UNRESOLVED"


class LiquidityTopNReason(StrEnum):
    """Stable exclusion and fail-closed reasons."""

    OUTSIDE_TOP_N = "OUTSIDE_TOP_N"
    LIQUIDITY_UNRESOLVED = "LIQUIDITY_UNRESOLVED"


@dataclass(frozen=True, slots=True)
class LiquidityTopNDecision:
    """One immutable decision tied to an upstream liquidity metric."""

    market: str
    status: LiquidityTopNDecisionStatus
    quote_volume: Decimal | None
    liquidity_rank: int | None
    reasons: tuple[LiquidityTopNReason, ...]

    def __post_init__(self) -> None:
        _validate_krw_market(self.market)

        if not isinstance(self.reasons, tuple) or any(
            not isinstance(reason, LiquidityTopNReason) for reason in self.reasons
        ):
            raise ValueError("top-N decision reasons must be an immutable reason tuple")

        if self.quote_volume is not None and (
            not isinstance(self.quote_volume, Decimal)
            or not self.quote_volume.is_finite()
            or self.quote_volume < 0
        ):
            raise ValueError("top-N quote_volume must be a finite non-negative Decimal")

        if self.liquidity_rank is not None and (
            not isinstance(self.liquidity_rank, int)
            or isinstance(self.liquidity_rank, bool)
            or self.liquidity_rank < 1
        ):
            raise ValueError("liquidity_rank must be a positive integer")

        if self.status is LiquidityTopNDecisionStatus.SELECTED:
            if self.quote_volume is None or self.liquidity_rank is None or self.reasons:
                raise ValueError("SELECTED requires volume and rank without reasons")
        elif self.status is LiquidityTopNDecisionStatus.EXCLUDED:
            if (
                self.quote_volume is None
                or self.liquidity_rank is None
                or self.reasons != (LiquidityTopNReason.OUTSIDE_TOP_N,)
            ):
                raise ValueError("EXCLUDED requires volume, rank, and OUTSIDE_TOP_N")
        elif self.status is LiquidityTopNDecisionStatus.UNRESOLVED:
            if self.liquidity_rank is not None or self.reasons != (
                LiquidityTopNReason.LIQUIDITY_UNRESOLVED,
            ):
                raise ValueError("UNRESOLVED requires no rank and LIQUIDITY_UNRESOLVED")
        else:
            raise ValueError(f"unsupported top-N decision status: {self.status}")


@dataclass(frozen=True, slots=True)
class LiquidityTopNResult:
    """Reproducible fail-closed liquidity shortlist evaluation."""

    status: LiquidityTopNEvaluationStatus
    top_n: int
    liquidity_policy_name: str
    liquidity_policy_version: str
    catalog_policy_name: str
    catalog_policy_version: str
    quote_currency: str
    window_start: datetime
    window_end: datetime
    as_of: datetime
    lookback: timedelta
    maximum_snapshot_age: timedelta
    snapshot_id: str | None
    observed_at: datetime | None
    available_at: datetime | None
    content_hash: str | None
    catalog_candidate_count: int
    catalog_excluded_count: int
    liquidity_resolved_count: int
    liquidity_unresolved_count: int
    decisions: tuple[LiquidityTopNDecision, ...]
    reasons: tuple[LiquidityTopNReason, ...]
    policy_name: str = field(init=False, default=LIQUIDITY_TOP_N_POLICY_NAME)
    policy_version: str = field(init=False, default=LIQUIDITY_TOP_N_POLICY_VERSION)

    def __post_init__(self) -> None:
        validate_top_n(self.top_n)

        if (
            self.liquidity_policy_name != MARKET_LIQUIDITY_POLICY_NAME
            or self.liquidity_policy_version != MARKET_LIQUIDITY_POLICY_VERSION
        ):
            raise ValueError("unsupported upstream liquidity policy")

        if (
            self.catalog_policy_name != MARKET_ELIGIBILITY_POLICY_NAME
            or self.catalog_policy_version != MARKET_ELIGIBILITY_POLICY_VERSION
        ):
            raise ValueError("unsupported upstream catalog policy")

        if self.quote_currency != normalize_quote_currency(self.quote_currency):
            raise ValueError("quote_currency must be normalized")

        if self.quote_currency != "KRW":
            raise ValueError("liquidity top-N requires KRW quote currency")

        _validate_utc(self.window_start, "window_start")
        _validate_utc(self.window_end, "window_end")
        _validate_utc(self.as_of, "as_of")

        if self.window_end.second != 0 or self.window_end.microsecond != 0:
            raise ValueError("window_end must align to a one-minute UTC boundary")

        if self.as_of < self.window_end:
            raise ValueError("as_of must not be before window_end")

        if self.lookback != MARKET_LIQUIDITY_LOOKBACK:
            raise ValueError("lookback must preserve the upstream liquidity lookback")

        if self.window_start != self.window_end - self.lookback:
            raise ValueError("window_start must match the liquidity lookback")

        validate_maximum_snapshot_age(self.maximum_snapshot_age)

        if not isinstance(self.decisions, tuple) or any(
            not isinstance(decision, LiquidityTopNDecision) for decision in self.decisions
        ):
            raise ValueError("top-N decisions must be an immutable decision tuple")

        if self.decisions != tuple(sorted(self.decisions, key=lambda item: item.market)):
            raise ValueError("top-N decisions must be ordered by market code")

        if len({decision.market for decision in self.decisions}) != len(self.decisions):
            raise ValueError("top-N decisions must not contain duplicate markets")

        if not isinstance(self.reasons, tuple) or any(
            not isinstance(reason, LiquidityTopNReason) for reason in self.reasons
        ):
            raise ValueError("top-N result reasons must be an immutable reason tuple")

        for count in (
            self.catalog_candidate_count,
            self.catalog_excluded_count,
            self.liquidity_resolved_count,
            self.liquidity_unresolved_count,
        ):
            if not isinstance(count, int) or isinstance(count, bool) or count < 0:
                raise ValueError("upstream counts must be non-negative integers")

        if self.catalog_excluded_count > self.catalog_candidate_count:
            raise ValueError("catalog_excluded_count must not exceed candidate count")

        if self.liquidity_candidate_count != (
            self.liquidity_resolved_count + self.liquidity_unresolved_count
        ):
            raise ValueError("upstream liquidity counts must add up to candidate count")

        metadata_is_complete = self._validate_snapshot_metadata()

        if metadata_is_complete and self.catalog_candidate_count < 1:
            raise ValueError("snapshot metadata requires at least one catalog candidate")

        if not metadata_is_complete and (
            self.catalog_candidate_count != 0 or self.catalog_excluded_count != 0
        ):
            raise ValueError("missing snapshot must not contain catalog counts")

        if self.status is LiquidityTopNEvaluationStatus.RESOLVED:
            self._validate_resolved(metadata_is_complete)
        elif self.status is LiquidityTopNEvaluationStatus.UNRESOLVED:
            self._validate_unresolved(metadata_is_complete)
        else:
            raise ValueError(f"unsupported top-N evaluation status: {self.status}")

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

        if self.observed_at > self.available_at or self.available_at > self.as_of:
            raise ValueError("snapshot timestamps must satisfy observed <= available <= as_of")

        if not isinstance(self.content_hash, str) or len(self.content_hash) != 64:
            raise ValueError("content_hash must be a 64-character string")

        return True

    def _validate_resolved(self, metadata_is_complete: bool) -> None:
        if self.reasons or not metadata_is_complete:
            raise ValueError("RESOLVED top-N requires snapshot metadata without reasons")

        if any(
            decision.status is LiquidityTopNDecisionStatus.UNRESOLVED for decision in self.decisions
        ):
            raise ValueError("RESOLVED top-N must not contain unresolved decisions")

        if self.catalog_candidate_count != (
            self.liquidity_candidate_count + self.catalog_excluded_count
        ):
            raise ValueError("catalog and liquidity candidate counts must agree")

        if (
            self.liquidity_resolved_count != self.liquidity_candidate_count
            or self.liquidity_unresolved_count != 0
        ):
            raise ValueError("RESOLVED top-N requires fully resolved upstream liquidity")

        if self.selected_count != min(self.top_n, self.liquidity_candidate_count):
            raise ValueError("RESOLVED top-N must select min(top_n, candidate_count)")

        ordered = sorted(
            self.decisions,
            key=lambda decision: (
                -_required_quote_volume(decision),
                decision.market,
            ),
        )
        expected_ranks = {decision.market: rank for rank, decision in enumerate(ordered, start=1)}

        if any(
            decision.liquidity_rank != expected_ranks[decision.market]
            for decision in self.decisions
        ):
            raise ValueError("resolved top-N must preserve ordinal liquidity ranks")

        for decision in self.decisions:
            expected_status = (
                LiquidityTopNDecisionStatus.SELECTED
                if decision.liquidity_rank is not None and decision.liquidity_rank <= self.top_n
                else LiquidityTopNDecisionStatus.EXCLUDED
            )

            if decision.status is not expected_status:
                raise ValueError("top-N decision must match its upstream liquidity rank")

    def _validate_unresolved(self, metadata_is_complete: bool) -> None:
        if self.reasons != (LiquidityTopNReason.LIQUIDITY_UNRESOLVED,):
            raise ValueError("UNRESOLVED top-N requires LIQUIDITY_UNRESOLVED")

        if any(
            decision.status is not LiquidityTopNDecisionStatus.UNRESOLVED
            for decision in self.decisions
        ):
            raise ValueError("UNRESOLVED top-N must suppress every selection")

        if self.selected_count != 0 or self.excluded_count != 0:
            raise ValueError("UNRESOLVED top-N must not select or exclude candidates")

        if self.decisions:
            if (
                not metadata_is_complete
                or self.liquidity_unresolved_count < 1
                or self.catalog_candidate_count
                != (self.liquidity_candidate_count + self.catalog_excluded_count)
            ):
                raise ValueError("unresolved liquidity candidates must preserve catalog counts")
        elif metadata_is_complete and self.catalog_excluded_count != 0:
            raise ValueError("unresolved empty liquidity must not claim catalog exclusions")

    @property
    def selected_markets(self) -> tuple[str, ...]:
        """Return selected market codes in deterministic market order."""

        return tuple(
            decision.market
            for decision in self.decisions
            if decision.status is LiquidityTopNDecisionStatus.SELECTED
        )

    @property
    def liquidity_candidate_count(self) -> int:
        """Return the number of upstream liquidity candidates."""

        return len(self.decisions)

    @property
    def selected_count(self) -> int:
        """Return shortlisted candidate count."""

        return sum(
            decision.status is LiquidityTopNDecisionStatus.SELECTED for decision in self.decisions
        )

    @property
    def excluded_count(self) -> int:
        """Return candidates outside the requested top-N."""

        return sum(
            decision.status is LiquidityTopNDecisionStatus.EXCLUDED for decision in self.decisions
        )

    @property
    def unresolved_count(self) -> int:
        """Return candidates suppressed by unresolved upstream liquidity."""

        return sum(
            decision.status is LiquidityTopNDecisionStatus.UNRESOLVED for decision in self.decisions
        )


def evaluate_liquidity_top_n(
    liquidity: MarketLiquidityResult,
    top_n: int,
) -> LiquidityTopNResult:
    """Apply top-N selection without recomputing or extending liquidity ranks."""

    validate_top_n(top_n)

    if not isinstance(liquidity, MarketLiquidityResult):
        raise ValueError("top-N policy requires a MarketLiquidityResult")

    metadata = {
        "top_n": top_n,
        "liquidity_policy_name": liquidity.policy_name,
        "liquidity_policy_version": liquidity.policy_version,
        "catalog_policy_name": liquidity.catalog_policy_name,
        "catalog_policy_version": liquidity.catalog_policy_version,
        "quote_currency": liquidity.quote_currency,
        "window_start": liquidity.window_start,
        "window_end": liquidity.window_end,
        "as_of": liquidity.as_of,
        "lookback": liquidity.lookback,
        "maximum_snapshot_age": liquidity.maximum_snapshot_age,
        "snapshot_id": liquidity.snapshot_id,
        "observed_at": liquidity.observed_at,
        "available_at": liquidity.available_at,
        "content_hash": liquidity.content_hash,
        "catalog_candidate_count": liquidity.catalog_candidate_count,
        "catalog_excluded_count": liquidity.catalog_excluded_count,
        "liquidity_resolved_count": liquidity.resolved_market_count,
        "liquidity_unresolved_count": liquidity.unresolved_market_count,
    }

    if liquidity.status is MarketLiquidityEvaluationStatus.UNRESOLVED:
        decisions = tuple(
            LiquidityTopNDecision(
                market=metric.market,
                status=LiquidityTopNDecisionStatus.UNRESOLVED,
                quote_volume=metric.quote_volume,
                liquidity_rank=None,
                reasons=(LiquidityTopNReason.LIQUIDITY_UNRESOLVED,),
            )
            for metric in liquidity.metrics
        )
        return LiquidityTopNResult(
            status=LiquidityTopNEvaluationStatus.UNRESOLVED,
            decisions=decisions,
            reasons=(LiquidityTopNReason.LIQUIDITY_UNRESOLVED,),
            **metadata,
        )

    decisions = tuple(_resolved_decision(metric, top_n) for metric in liquidity.metrics)
    return LiquidityTopNResult(
        status=LiquidityTopNEvaluationStatus.RESOLVED,
        decisions=decisions,
        reasons=(),
        **metadata,
    )


def validate_top_n(value: int) -> None:
    """Reject non-integer, bool, and non-positive shortlist sizes."""

    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError("top_n must be a positive integer")


def _resolved_decision(
    metric: MarketLiquidityMetric,
    top_n: int,
) -> LiquidityTopNDecision:
    if metric.rank is None or metric.quote_volume is None:
        raise ValueError("resolved liquidity metric requires rank and quote_volume")

    selected = metric.rank <= top_n
    return LiquidityTopNDecision(
        market=metric.market,
        status=(
            LiquidityTopNDecisionStatus.SELECTED
            if selected
            else LiquidityTopNDecisionStatus.EXCLUDED
        ),
        quote_volume=metric.quote_volume,
        liquidity_rank=metric.rank,
        reasons=() if selected else (LiquidityTopNReason.OUTSIDE_TOP_N,),
    )


def _required_quote_volume(decision: LiquidityTopNDecision) -> Decimal:
    if decision.quote_volume is None:
        raise ValueError("resolved top-N decision requires quote_volume")

    return decision.quote_volume


def _validate_krw_market(market: str) -> None:
    quote, separator, base = market.partition("-")

    if (
        quote != "KRW"
        or not separator
        or "-" in base
        or not base.isascii()
        or not base.isalnum()
        or market != market.upper()
    ):
        raise ValueError("market must be an uppercase ASCII KRW pair")


def _validate_utc(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be timezone-aware UTC")
