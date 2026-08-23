"""Point-in-time trailing quote-volume liquidity policy."""

from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from cassandra_trader.domain.market import normalize_quote_currency
from cassandra_trader.domain.market_eligibility import (
    MARKET_ELIGIBILITY_POLICY_NAME,
    MARKET_ELIGIBILITY_POLICY_VERSION,
    MarketEligibilityEvaluationStatus,
    MarketEligibilityResult,
    validate_maximum_snapshot_age,
)
from cassandra_trader.domain.minute_slot import MinuteSlotSummary

MARKET_LIQUIDITY_POLICY_NAME = "trailing-quote-volume"
MARKET_LIQUIDITY_POLICY_VERSION = "v1"
MARKET_LIQUIDITY_LOOKBACK = timedelta(days=30)
MARKET_LIQUIDITY_EXPECTED_COUNT = 43_200


class MarketLiquidityEvaluationStatus(StrEnum):
    """Whether every catalog candidate has a complete liquidity metric."""

    RESOLVED = "RESOLVED"
    UNRESOLVED = "UNRESOLVED"


class MarketLiquidityStatus(StrEnum):
    """Point-in-time data completeness for one market's liquidity metric."""

    RESOLVED = "RESOLVED"
    UNRESOLVED = "UNRESOLVED"


class MarketLiquidityReason(StrEnum):
    """Fail-closed reasons for liquidity evaluation."""

    CATALOG_ELIGIBILITY_UNRESOLVED = "CATALOG_ELIGIBILITY_UNRESOLVED"
    LIQUIDITY_DATA_UNRESOLVED = "LIQUIDITY_DATA_UNRESOLVED"


@dataclass(frozen=True, slots=True)
class MarketLiquidityMetric:
    """Trailing 30-day quote-volume metric for one eligible KRW market."""

    market: str
    status: MarketLiquidityStatus
    expected_count: int
    traded_count: int
    no_trade_count: int
    unresolved_count: int
    quote_volume: Decimal | None
    rank: int | None
    reasons: tuple[MarketLiquidityReason, ...]

    def __post_init__(self) -> None:
        _validate_krw_market(self.market)

        if not isinstance(self.reasons, tuple) or any(
            not isinstance(reason, MarketLiquidityReason) for reason in self.reasons
        ):
            raise ValueError("liquidity metric reasons must be an immutable reason tuple")

        counts = (
            self.expected_count,
            self.traded_count,
            self.no_trade_count,
            self.unresolved_count,
        )

        if any(
            not isinstance(count, int) or isinstance(count, bool) or count < 0 for count in counts
        ):
            raise ValueError("liquidity counts must be non-negative integers")

        if self.expected_count != MARKET_LIQUIDITY_EXPECTED_COUNT:
            raise ValueError("expected_count must equal 43200")

        if self.expected_count != (self.traded_count + self.no_trade_count + self.unresolved_count):
            raise ValueError("liquidity counts must add up to expected_count")

        if self.rank is not None and (
            not isinstance(self.rank, int) or isinstance(self.rank, bool) or self.rank < 1
        ):
            raise ValueError("liquidity rank must be a positive integer")

        if self.status is MarketLiquidityStatus.RESOLVED:
            if self.unresolved_count != 0 or self.reasons:
                raise ValueError("RESOLVED metric must not contain unresolved slots or reasons")

            if not isinstance(self.quote_volume, Decimal):
                raise ValueError("RESOLVED metric requires Decimal quote_volume")

            if not self.quote_volume.is_finite() or self.quote_volume < 0:
                raise ValueError("quote_volume must be finite and non-negative")
        elif self.status is MarketLiquidityStatus.UNRESOLVED:
            if (
                self.unresolved_count < 1
                or self.quote_volume is not None
                or self.rank is not None
                or self.reasons != (MarketLiquidityReason.LIQUIDITY_DATA_UNRESOLVED,)
            ):
                raise ValueError(
                    "UNRESOLVED metric requires unresolved slots without volume or rank"
                )
        else:
            raise ValueError(f"unsupported market liquidity status: {self.status}")


@dataclass(frozen=True, slots=True)
class MarketLiquidityResult:
    """Fail-closed, reproducible trailing liquidity evaluation."""

    status: MarketLiquidityEvaluationStatus
    quote_currency: str
    window_start: datetime
    window_end: datetime
    as_of: datetime
    maximum_snapshot_age: timedelta
    snapshot_id: str | None
    observed_at: datetime | None
    available_at: datetime | None
    content_hash: str | None
    catalog_candidate_count: int
    catalog_excluded_count: int
    metrics: tuple[MarketLiquidityMetric, ...]
    reasons: tuple[MarketLiquidityReason, ...]
    policy_name: str = field(init=False, default=MARKET_LIQUIDITY_POLICY_NAME)
    policy_version: str = field(init=False, default=MARKET_LIQUIDITY_POLICY_VERSION)
    catalog_policy_name: str = field(init=False, default=MARKET_ELIGIBILITY_POLICY_NAME)
    catalog_policy_version: str = field(init=False, default=MARKET_ELIGIBILITY_POLICY_VERSION)
    lookback: timedelta = field(init=False, default=MARKET_LIQUIDITY_LOOKBACK)

    def __post_init__(self) -> None:
        if self.quote_currency != normalize_quote_currency(self.quote_currency):
            raise ValueError("quote_currency must be normalized")

        if self.quote_currency != "KRW":
            raise ValueError("trailing quote-volume policy requires KRW quote currency")

        _validate_window(self.window_end, self.as_of)
        validate_maximum_snapshot_age(self.maximum_snapshot_age)

        if self.window_start != self.window_end - MARKET_LIQUIDITY_LOOKBACK:
            raise ValueError("window_start must be exactly 30 days before window_end")

        if not isinstance(self.metrics, tuple) or any(
            not isinstance(metric, MarketLiquidityMetric) for metric in self.metrics
        ):
            raise ValueError("liquidity metrics must be an immutable metric tuple")

        if not isinstance(self.reasons, tuple) or any(
            not isinstance(reason, MarketLiquidityReason) for reason in self.reasons
        ):
            raise ValueError("liquidity reasons must be an immutable reason tuple")

        if self.metrics != tuple(sorted(self.metrics, key=lambda metric: metric.market)):
            raise ValueError("liquidity metrics must be ordered by market code")

        if len({metric.market for metric in self.metrics}) != len(self.metrics):
            raise ValueError("liquidity metrics must not contain duplicate markets")

        for count in (self.catalog_candidate_count, self.catalog_excluded_count):
            if not isinstance(count, int) or isinstance(count, bool) or count < 0:
                raise ValueError("catalog counts must be non-negative integers")

        if self.catalog_excluded_count > self.catalog_candidate_count:
            raise ValueError("catalog_excluded_count must not exceed candidate count")

        metadata_is_complete = self._validate_snapshot_metadata()

        if metadata_is_complete and self.catalog_candidate_count < 1:
            raise ValueError("catalog snapshot metadata requires at least one candidate")

        if not metadata_is_complete and (
            self.catalog_candidate_count != 0 or self.catalog_excluded_count != 0
        ):
            raise ValueError("missing catalog snapshot must not contain catalog counts")

        if self.status is MarketLiquidityEvaluationStatus.RESOLVED:
            if self.reasons or not metadata_is_complete:
                raise ValueError("RESOLVED liquidity requires snapshot metadata without reasons")

            if self.unresolved_market_count != 0:
                raise ValueError("RESOLVED liquidity must not contain unresolved metrics")

            if self.catalog_candidate_count != (self.candidate_count + self.catalog_excluded_count):
                raise ValueError("resolved catalog and liquidity candidate counts must agree")

            self._validate_resolved_ranks()
        elif self.status is MarketLiquidityEvaluationStatus.UNRESOLVED:
            if any(metric.rank is not None for metric in self.metrics):
                raise ValueError("UNRESOLVED liquidity must not publish any ranks")

            if self.reasons == (MarketLiquidityReason.CATALOG_ELIGIBILITY_UNRESOLVED,):
                if self.metrics or self.catalog_excluded_count != 0:
                    raise ValueError("unresolved catalog must not produce liquidity metrics")

                if not metadata_is_complete and self.catalog_candidate_count != 0:
                    raise ValueError("missing catalog snapshot must not contain candidates")
            elif self.reasons == (MarketLiquidityReason.LIQUIDITY_DATA_UNRESOLVED,):
                if not metadata_is_complete or self.unresolved_market_count < 1:
                    raise ValueError(
                        "unresolved liquidity data requires snapshot and unresolved metrics"
                    )

                if self.catalog_candidate_count != (
                    self.candidate_count + self.catalog_excluded_count
                ):
                    raise ValueError("catalog and liquidity candidate counts must agree")
            else:
                raise ValueError("UNRESOLVED liquidity requires one fail-closed reason")
        else:
            raise ValueError(f"unsupported liquidity evaluation status: {self.status}")

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

    def _validate_resolved_ranks(self) -> None:
        ordered = sorted(
            self.metrics,
            key=lambda metric: (
                -_required_quote_volume(metric),
                metric.market,
            ),
        )
        expected_ranks = {metric.market: rank for rank, metric in enumerate(ordered, start=1)}

        if any(metric.rank != expected_ranks[metric.market] for metric in self.metrics):
            raise ValueError("resolved liquidity ranks must follow volume and market ordering")

    @property
    def candidate_count(self) -> int:
        """Return catalog-eligible liquidity candidates."""

        return len(self.metrics)

    @property
    def resolved_market_count(self) -> int:
        """Return candidates with complete 30-day evidence."""

        return sum(metric.status is MarketLiquidityStatus.RESOLVED for metric in self.metrics)

    @property
    def unresolved_market_count(self) -> int:
        """Return candidates with at least one unresolved minute."""

        return sum(metric.status is MarketLiquidityStatus.UNRESOLVED for metric in self.metrics)


def evaluate_market_liquidity(
    eligibility: MarketEligibilityResult,
    window_end: datetime,
    summaries: tuple[MinuteSlotSummary, ...],
) -> MarketLiquidityResult:
    """Apply the versioned liquidity policy to precomputed point-in-time summaries."""

    _validate_window(window_end, eligibility.as_of)

    if not isinstance(summaries, tuple) or any(
        not isinstance(summary, MinuteSlotSummary) for summary in summaries
    ):
        raise ValueError("minute summaries must be an immutable summary tuple")

    metadata = {
        "quote_currency": eligibility.quote_currency,
        "window_start": window_end - MARKET_LIQUIDITY_LOOKBACK,
        "window_end": window_end,
        "as_of": eligibility.as_of,
        "maximum_snapshot_age": eligibility.maximum_snapshot_age,
        "snapshot_id": eligibility.snapshot_id,
        "observed_at": eligibility.observed_at,
        "available_at": eligibility.available_at,
        "content_hash": eligibility.content_hash,
        "catalog_candidate_count": eligibility.candidate_count,
        "catalog_excluded_count": eligibility.excluded_count,
    }

    if eligibility.status is MarketEligibilityEvaluationStatus.UNRESOLVED:
        if summaries:
            raise ValueError("unresolved catalog eligibility must not include minute summaries")

        return MarketLiquidityResult(
            status=MarketLiquidityEvaluationStatus.UNRESOLVED,
            metrics=(),
            reasons=(MarketLiquidityReason.CATALOG_ELIGIBILITY_UNRESOLVED,),
            **metadata,
        )

    expected_markets = tuple(pair.code for pair in eligibility.eligible_pairs)

    if tuple(summary.market for summary in summaries) != expected_markets:
        raise ValueError("minute summaries must match eligible markets in market-code order")

    window_start = window_end - MARKET_LIQUIDITY_LOOKBACK

    for summary in summaries:
        if (
            summary.start_time_utc != window_start
            or summary.end_time_utc != window_end
            or summary.as_of != eligibility.as_of
            or summary.expected_count != MARKET_LIQUIDITY_EXPECTED_COUNT
        ):
            raise ValueError("minute summary does not match the liquidity evaluation window")

    metrics = tuple(_metric_from_summary(summary) for summary in summaries)

    if any(metric.status is MarketLiquidityStatus.UNRESOLVED for metric in metrics):
        return MarketLiquidityResult(
            status=MarketLiquidityEvaluationStatus.UNRESOLVED,
            metrics=metrics,
            reasons=(MarketLiquidityReason.LIQUIDITY_DATA_UNRESOLVED,),
            **metadata,
        )

    rank_by_market = {
        metric.market: rank
        for rank, metric in enumerate(
            sorted(
                metrics,
                key=lambda metric: (-_required_quote_volume(metric), metric.market),
            ),
            start=1,
        )
    }
    ranked = tuple(replace(metric, rank=rank_by_market[metric.market]) for metric in metrics)
    return MarketLiquidityResult(
        status=MarketLiquidityEvaluationStatus.RESOLVED,
        metrics=ranked,
        reasons=(),
        **metadata,
    )


def _metric_from_summary(summary: MinuteSlotSummary) -> MarketLiquidityMetric:
    if summary.unresolved_count:
        return MarketLiquidityMetric(
            market=summary.market,
            status=MarketLiquidityStatus.UNRESOLVED,
            expected_count=summary.expected_count,
            traded_count=summary.traded_count,
            no_trade_count=summary.no_trade_count,
            unresolved_count=summary.unresolved_count,
            quote_volume=None,
            rank=None,
            reasons=(MarketLiquidityReason.LIQUIDITY_DATA_UNRESOLVED,),
        )

    return MarketLiquidityMetric(
        market=summary.market,
        status=MarketLiquidityStatus.RESOLVED,
        expected_count=summary.expected_count,
        traded_count=summary.traded_count,
        no_trade_count=summary.no_trade_count,
        unresolved_count=0,
        quote_volume=summary.quote_volume,
        rank=None,
        reasons=(),
    )


def _required_quote_volume(metric: MarketLiquidityMetric) -> Decimal:
    if metric.quote_volume is None:
        raise ValueError("resolved liquidity metric requires quote_volume")

    return metric.quote_volume


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


def _validate_window(window_end: datetime, as_of: datetime) -> None:
    _validate_utc(window_end, "window_end")
    _validate_utc(as_of, "as_of")

    if window_end.second != 0 or window_end.microsecond != 0:
        raise ValueError("window_end must align to a one-minute UTC boundary")

    if as_of < window_end:
        raise ValueError("as_of must not be before window_end")


def _validate_utc(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be timezone-aware UTC")
