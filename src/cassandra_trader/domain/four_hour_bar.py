"""Deterministic four-hour candle aggregation."""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from cassandra_trader.domain.candle import MinuteCandle
from cassandra_trader.domain.minute_slot import MinuteSlotStatus, ResolvedMinuteSlot

_EXPECTED_MINUTE_COUNT = 240


class FourHourBarStatus(StrEnum):
    """Outcome status for a four-hour aggregation attempt."""

    COMPLETE = "COMPLETE"
    NO_TRADE = "NO_TRADE"
    INCOMPLETE = "INCOMPLETE"


class FourHourBarReason(StrEnum):
    """Reason an aggregation did not produce a tradable candle."""

    MISSING_MINUTES = "MISSING_MINUTES"
    INTERVAL_NOT_COMPLETE = "INTERVAL_NOT_COMPLETE"


@dataclass(frozen=True, slots=True)
class FourHourCandle:
    """Canonical completed four-hour candle aligned to UTC boundaries."""

    market: str
    start_time_utc: datetime
    end_time_utc: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    base_volume: Decimal
    quote_volume: Decimal

    def __post_init__(self) -> None:
        _validate_market(self.market)
        _validate_utc(self.start_time_utc, "start_time_utc")
        _validate_utc(self.end_time_utc, "end_time_utc")

        if (
            self.start_time_utc.hour % 4 != 0
            or self.start_time_utc.minute != 0
            or self.start_time_utc.second != 0
            or self.start_time_utc.microsecond != 0
        ):
            raise ValueError("start_time_utc must align to a four-hour UTC boundary")

        if self.end_time_utc != self.start_time_utc + timedelta(hours=4):
            raise ValueError("end_time_utc must equal start_time_utc plus four hours")

        prices = (self.open, self.high, self.low, self.close)
        volumes = (self.base_volume, self.quote_volume)

        if any(not value.is_finite() or value < 0 for value in prices):
            raise ValueError("prices must be finite and non-negative")

        if any(not value.is_finite() or value < 0 for value in volumes):
            raise ValueError("volumes must be finite and non-negative")

        if self.low > min(self.open, self.close) or self.high < max(self.open, self.close):
            raise ValueError("OHLC values must satisfy low <= open/close <= high")


@dataclass(frozen=True, slots=True)
class FourHourBarResult:
    """Evidence-backed four-hour aggregation result and slot counts."""

    status: FourHourBarStatus
    market: str
    start_time_utc: datetime
    end_time_utc: datetime
    expected_count: int
    resolved_count: int
    traded_count: int
    no_trade_count: int
    unresolved_count: int
    unresolved_minute_starts: tuple[datetime, ...]
    candle: FourHourCandle | None
    reason: FourHourBarReason | None

    def __post_init__(self) -> None:
        _validate_market(self.market)
        _validate_four_hour_start(self.start_time_utc)
        _validate_utc(self.end_time_utc, "end_time_utc")

        if self.end_time_utc != self.start_time_utc + timedelta(hours=4):
            raise ValueError("end_time_utc must equal start_time_utc plus four hours")

        counts = (
            self.expected_count,
            self.resolved_count,
            self.traded_count,
            self.no_trade_count,
            self.unresolved_count,
        )

        if any(
            not isinstance(count, int) or isinstance(count, bool) or count < 0 for count in counts
        ):
            raise ValueError("four-hour slot counts must be non-negative integers")

        if self.expected_count != _EXPECTED_MINUTE_COUNT:
            raise ValueError("expected_count must equal 240")

        if self.resolved_count != self.traded_count + self.no_trade_count:
            raise ValueError("resolved_count must equal traded_count plus no_trade_count")

        if self.expected_count != self.resolved_count + self.unresolved_count:
            raise ValueError("resolved and unresolved counts must equal expected_count")

        if self.unresolved_count != len(self.unresolved_minute_starts):
            raise ValueError("unresolved_count must match unresolved_minute_starts")

        if self.unresolved_minute_starts != tuple(sorted(set(self.unresolved_minute_starts))):
            raise ValueError("unresolved_minute_starts must be unique and ordered")

        for start in self.unresolved_minute_starts:
            _validate_utc(start, "unresolved_minute_start")

            if not self.start_time_utc <= start < self.end_time_utc:
                raise ValueError("unresolved minute start falls outside the four-hour interval")

            if start.second != 0 or start.microsecond != 0:
                raise ValueError("unresolved minute start must align to a minute boundary")

        if self.status is FourHourBarStatus.COMPLETE:
            if self.candle is None or self.traded_count < 1 or self.unresolved_count != 0:
                raise ValueError(
                    "COMPLETE requires traded slots, no unresolved slots, and a candle"
                )

            if self.reason is not None:
                raise ValueError("COMPLETE must not include an incomplete reason")
        elif self.status is FourHourBarStatus.NO_TRADE:
            if (
                self.candle is not None
                or self.resolved_count != _EXPECTED_MINUTE_COUNT
                or self.traded_count != 0
                or self.no_trade_count != _EXPECTED_MINUTE_COUNT
                or self.unresolved_count != 0
                or self.reason is not None
            ):
                raise ValueError("NO_TRADE requires 240 resolved no-trade slots and no candle")
        elif self.status is FourHourBarStatus.INCOMPLETE:
            if self.candle is not None or self.reason is None or self.unresolved_count < 1:
                raise ValueError("INCOMPLETE requires unresolved slots, a reason, and no candle")
        else:
            raise ValueError(f"unsupported four-hour bar status: {self.status}")

        if self.candle is not None and (
            self.candle.market != self.market
            or self.candle.start_time_utc != self.start_time_utc
            or self.candle.end_time_utc != self.end_time_utc
        ):
            raise ValueError("four-hour candle must match its result interval")

    @property
    def actual_count(self) -> int:
        """Compatibility alias for the number of actual TRADED candles."""

        return self.traded_count

    @property
    def missing_minute_starts(self) -> tuple[datetime, ...]:
        """Compatibility alias for unresolved minute starts."""

        return self.unresolved_minute_starts


def aggregate_resolved_four_hour_bar(
    *,
    market: str,
    start_time_utc: datetime,
    as_of: datetime,
    minute_slots: Sequence[ResolvedMinuteSlot],
) -> FourHourBarResult:
    """Aggregate one four-hour interval from exactly 240 resolved minute slots."""

    _validate_market(market)
    _validate_four_hour_start(start_time_utc)
    _validate_utc(as_of, "as_of")
    end_time_utc = start_time_utc + timedelta(hours=4)
    expected_starts = tuple(
        start_time_utc + timedelta(minutes=offset) for offset in range(_EXPECTED_MINUTE_COUNT)
    )
    by_start: dict[datetime, ResolvedMinuteSlot] = {}

    for slot in minute_slots:
        if slot.market != market:
            raise ValueError("minute slot market does not match aggregation market")

        if not start_time_utc <= slot.start_time_utc < end_time_utc:
            raise ValueError("minute slot falls outside the four-hour interval")

        if slot.start_time_utc in by_start:
            raise ValueError("duplicate minute slot start_time_utc")

        by_start[slot.start_time_utc] = slot

    if set(by_start) != set(expected_starts):
        raise ValueError("minute_slots must contain exactly 240 contiguous minute slots")

    ordered_slots = tuple(by_start[start] for start in expected_starts)
    unresolved_starts = tuple(
        slot.start_time_utc
        for slot in ordered_slots
        if slot.status is MinuteSlotStatus.UNRESOLVED or slot.end_time_utc > as_of
    )
    unresolved_start_set = set(unresolved_starts)
    traded_slots = tuple(
        slot
        for slot in ordered_slots
        if slot.start_time_utc not in unresolved_start_set
        and slot.status is MinuteSlotStatus.TRADED
    )
    no_trade_count = sum(
        slot.start_time_utc not in unresolved_start_set and slot.status is MinuteSlotStatus.NO_TRADE
        for slot in ordered_slots
    )
    traded_count = len(traded_slots)
    unresolved_count = len(unresolved_starts)
    resolved_count = traded_count + no_trade_count

    if end_time_utc > as_of or unresolved_count:
        return FourHourBarResult(
            status=FourHourBarStatus.INCOMPLETE,
            market=market,
            start_time_utc=start_time_utc,
            end_time_utc=end_time_utc,
            expected_count=_EXPECTED_MINUTE_COUNT,
            resolved_count=resolved_count,
            traded_count=traded_count,
            no_trade_count=no_trade_count,
            unresolved_count=unresolved_count,
            unresolved_minute_starts=unresolved_starts,
            candle=None,
            reason=(
                FourHourBarReason.INTERVAL_NOT_COMPLETE
                if end_time_utc > as_of
                else FourHourBarReason.MISSING_MINUTES
            ),
        )

    if traded_count == 0:
        return FourHourBarResult(
            status=FourHourBarStatus.NO_TRADE,
            market=market,
            start_time_utc=start_time_utc,
            end_time_utc=end_time_utc,
            expected_count=_EXPECTED_MINUTE_COUNT,
            resolved_count=resolved_count,
            traded_count=0,
            no_trade_count=no_trade_count,
            unresolved_count=0,
            unresolved_minute_starts=(),
            candle=None,
            reason=None,
        )

    traded_candles = tuple(slot.candle for slot in traded_slots if slot.candle is not None)
    candle = FourHourCandle(
        market=market,
        start_time_utc=start_time_utc,
        end_time_utc=end_time_utc,
        open=traded_candles[0].open,
        high=max(minute_candle.high for minute_candle in traded_candles),
        low=min(minute_candle.low for minute_candle in traded_candles),
        close=traded_candles[-1].close,
        base_volume=sum(
            (minute_candle.base_volume for minute_candle in traded_candles),
            start=Decimal(0),
        ),
        quote_volume=sum(
            (minute_candle.quote_volume for minute_candle in traded_candles),
            start=Decimal(0),
        ),
    )

    return FourHourBarResult(
        status=FourHourBarStatus.COMPLETE,
        market=market,
        start_time_utc=start_time_utc,
        end_time_utc=end_time_utc,
        expected_count=_EXPECTED_MINUTE_COUNT,
        resolved_count=resolved_count,
        traded_count=traded_count,
        no_trade_count=no_trade_count,
        unresolved_count=0,
        unresolved_minute_starts=(),
        candle=candle,
        reason=None,
    )


def aggregate_four_hour_bar(
    *,
    market: str,
    start_time_utc: datetime,
    as_of: datetime,
    minute_candles: Sequence[MinuteCandle],
) -> FourHourBarResult:
    """Compatibility wrapper treating absent canonical minutes as UNRESOLVED."""

    _validate_market(market)
    _validate_utc(start_time_utc, "start_time_utc")
    _validate_utc(as_of, "as_of")

    if (
        start_time_utc.hour % 4 != 0
        or start_time_utc.minute != 0
        or start_time_utc.second != 0
        or start_time_utc.microsecond != 0
    ):
        raise ValueError("start_time_utc must align to a four-hour UTC boundary")

    end_time_utc = start_time_utc + timedelta(hours=4)
    by_start: dict[datetime, MinuteCandle] = {}

    for candle in minute_candles:
        if candle.market != market:
            raise ValueError("minute candle market does not match aggregation market")

        if not start_time_utc <= candle.start_time_utc < end_time_utc:
            raise ValueError("minute candle falls outside the four-hour interval")

        if candle.start_time_utc in by_start:
            raise ValueError("duplicate minute candle start_time_utc")

        by_start[candle.start_time_utc] = candle

    expected_starts = tuple(
        start_time_utc + timedelta(minutes=offset) for offset in range(_EXPECTED_MINUTE_COUNT)
    )
    slots = tuple(
        ResolvedMinuteSlot(
            market=market,
            start_time_utc=start,
            end_time_utc=start + timedelta(minutes=1),
            status=(MinuteSlotStatus.TRADED if start in by_start else MinuteSlotStatus.UNRESOLVED),
            candle=by_start.get(start),
            canonical_revision=1 if start in by_start else None,
            canonical_available_at=as_of if start in by_start else None,
        )
        for start in expected_starts
    )

    return aggregate_resolved_four_hour_bar(
        market=market,
        start_time_utc=start_time_utc,
        as_of=as_of,
        minute_slots=slots,
    )


def _validate_four_hour_start(start_time_utc: datetime) -> None:
    _validate_utc(start_time_utc, "start_time_utc")

    if (
        start_time_utc.hour % 4 != 0
        or start_time_utc.minute != 0
        or start_time_utc.second != 0
        or start_time_utc.microsecond != 0
    ):
        raise ValueError("start_time_utc must align to a four-hour UTC boundary")


def _validate_market(market: str) -> None:
    quote, separator, base = market.partition("-")

    if not separator or not quote.isalnum() or not base.isalnum() or market != market.upper():
        raise ValueError(f"Invalid market code: {market}")


def _validate_utc(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be timezone-aware UTC")
