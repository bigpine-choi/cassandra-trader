from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from cassandra_trader.domain.candle import MinuteCandle
from cassandra_trader.domain.four_hour_bar import (
    FourHourBarReason,
    FourHourBarStatus,
    aggregate_four_hour_bar,
    aggregate_resolved_four_hour_bar,
)
from cassandra_trader.domain.minute_slot import MinuteSlotStatus, ResolvedMinuteSlot

_START = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)
_END = _START + timedelta(hours=4)


def _minute(offset: int, *, market: str = "KRW-BTC") -> MinuteCandle:
    start = _START + timedelta(minutes=offset)
    open_price = Decimal(100 + offset)

    return MinuteCandle(
        market=market,
        start_time_utc=start,
        end_time_utc=start + timedelta(minutes=1),
        open=open_price,
        high=open_price + Decimal(2),
        low=open_price - Decimal(1),
        close=open_price + Decimal(1),
        base_volume=Decimal("0.1"),
        quote_volume=Decimal("0.00000000000000000001"),
    )


def _complete_minutes() -> tuple[MinuteCandle, ...]:
    return tuple(_minute(offset) for offset in range(240))


def _slot(offset: int, status: MinuteSlotStatus) -> ResolvedMinuteSlot:
    start = _START + timedelta(minutes=offset)
    candle = _minute(offset) if status is MinuteSlotStatus.TRADED else None

    return ResolvedMinuteSlot(
        market="KRW-BTC",
        start_time_utc=start,
        end_time_utc=start + timedelta(minutes=1),
        status=status,
        candle=candle,
        canonical_revision=1 if candle is not None else None,
        canonical_available_at=_END if candle is not None else None,
    )


def _slots(
    *,
    no_trade: set[int] | None = None,
    unresolved: set[int] | None = None,
) -> tuple[ResolvedMinuteSlot, ...]:
    no_trade = no_trade or set()
    unresolved = unresolved or set()
    result: list[ResolvedMinuteSlot] = []

    for offset in range(240):
        if offset in unresolved:
            status = MinuteSlotStatus.UNRESOLVED
        elif offset in no_trade:
            status = MinuteSlotStatus.NO_TRADE
        else:
            status = MinuteSlotStatus.TRADED

        result.append(_slot(offset, status))

    return tuple(result)


def test_aggregates_240_minutes_with_exact_ohlcv_and_boundaries() -> None:
    result = aggregate_four_hour_bar(
        market="KRW-BTC",
        start_time_utc=_START,
        as_of=_END,
        minute_candles=_complete_minutes(),
    )

    assert result.status is FourHourBarStatus.COMPLETE
    assert result.actual_count == 240
    assert result.missing_minute_starts == ()
    assert result.resolved_count == 240
    assert result.traded_count == 240
    assert result.no_trade_count == 0
    assert result.unresolved_count == 0
    assert result.candle is not None
    assert result.candle.start_time_utc == _START
    assert result.candle.end_time_utc == _END
    assert result.candle.open == Decimal("100")
    assert result.candle.high == Decimal("341")
    assert result.candle.low == Decimal("99")
    assert result.candle.close == Decimal("340")
    assert result.candle.base_volume == Decimal("24.0")
    assert result.candle.quote_volume == Decimal("0.00000000000000000240")


def test_resolved_mix_aggregates_only_real_traded_candles() -> None:
    no_trade = {0, 42, 239}

    result = aggregate_resolved_four_hour_bar(
        market="KRW-BTC",
        start_time_utc=_START,
        as_of=_END,
        minute_slots=_slots(no_trade=no_trade),
    )

    assert result.status is FourHourBarStatus.COMPLETE
    assert result.resolved_count == 240
    assert result.traded_count == 237
    assert result.no_trade_count == 3
    assert result.unresolved_count == 0
    assert result.actual_count == 237
    assert result.candle is not None
    assert result.candle.open == Decimal("101")
    assert result.candle.close == Decimal("339")
    assert result.candle.high == Decimal("340")
    assert result.candle.low == Decimal("100")
    assert result.candle.base_volume == Decimal("23.7")
    assert result.candle.quote_volume == Decimal("0.00000000000000000237")


def test_shuffled_slots_use_chronological_traded_open_and_close() -> None:
    result = aggregate_resolved_four_hour_bar(
        market="KRW-BTC",
        start_time_utc=_START,
        as_of=_END,
        minute_slots=tuple(reversed(_slots(no_trade={0, 42, 239}))),
    )

    assert result.status is FourHourBarStatus.COMPLETE
    assert result.candle is not None
    assert result.candle.open == Decimal("101")
    assert result.candle.close == Decimal("339")


@pytest.mark.parametrize(
    "minute_slots, error",
    [
        (_slots()[:-1], "exactly 240 contiguous"),
        ((*_slots()[:-1], _slot(0, MinuteSlotStatus.TRADED)), "duplicate"),
        ((*_slots()[:-1], _slot(240, MinuteSlotStatus.TRADED)), "outside"),
    ],
)
def test_rejects_structurally_invalid_resolved_slot_ranges(
    minute_slots: tuple[ResolvedMinuteSlot, ...],
    error: str,
) -> None:
    with pytest.raises(ValueError, match=error):
        aggregate_resolved_four_hour_bar(
            market="KRW-BTC",
            start_time_utc=_START,
            as_of=_END,
            minute_slots=minute_slots,
        )


def test_all_resolved_no_trade_returns_observed_no_trade_without_candle() -> None:
    result = aggregate_resolved_four_hour_bar(
        market="KRW-BTC",
        start_time_utc=_START,
        as_of=_END,
        minute_slots=_slots(no_trade=set(range(240))),
    )

    assert result.status is FourHourBarStatus.NO_TRADE
    assert result.expected_count == 240
    assert result.resolved_count == 240
    assert result.traded_count == 0
    assert result.no_trade_count == 240
    assert result.unresolved_count == 0
    assert result.actual_count == 0
    assert result.missing_minute_starts == ()
    assert result.candle is None
    assert result.reason is None


def test_one_unresolved_slot_prevents_partial_candle_and_reports_exact_counts() -> None:
    unresolved = _START + timedelta(minutes=42)

    result = aggregate_resolved_four_hour_bar(
        market="KRW-BTC",
        start_time_utc=_START,
        as_of=_END,
        minute_slots=_slots(no_trade={10}, unresolved={42}),
    )

    assert result.status is FourHourBarStatus.INCOMPLETE
    assert result.expected_count == 240
    assert result.resolved_count == 239
    assert result.traded_count == 238
    assert result.no_trade_count == 1
    assert result.unresolved_count == 1
    assert result.unresolved_minute_starts == (unresolved,)
    assert result.actual_count == 238
    assert result.missing_minute_starts == (unresolved,)
    assert result.candle is None


def test_result_rejects_inconsistent_status_candle_and_counts() -> None:
    complete = aggregate_resolved_four_hour_bar(
        market="KRW-BTC",
        start_time_utc=_START,
        as_of=_END,
        minute_slots=_slots(),
    )

    with pytest.raises(ValueError, match="COMPLETE"):
        replace(complete, candle=None)

    with pytest.raises(ValueError, match="resolved_count"):
        replace(complete, resolved_count=239)

    with pytest.raises(ValueError, match="integers"):
        replace(complete, expected_count=240.0)

    with pytest.raises(ValueError, match="NO_TRADE"):
        replace(
            complete,
            status=FourHourBarStatus.NO_TRADE,
            traded_count=0,
            no_trade_count=240,
        )

    incomplete = aggregate_resolved_four_hour_bar(
        market="KRW-BTC",
        start_time_utc=_START,
        as_of=_END,
        minute_slots=_slots(unresolved={42}),
    )

    with pytest.raises(ValueError, match="INCOMPLETE"):
        replace(
            incomplete,
            resolved_count=240,
            traded_count=240,
            unresolved_count=0,
            unresolved_minute_starts=(),
        )


@pytest.mark.parametrize(
    "start_time",
    [
        datetime(2026, 8, 22, 13, 0, tzinfo=UTC),
        datetime(2026, 8, 22, 12, 1, tzinfo=UTC),
        datetime(2026, 8, 22, 12, 0),
        datetime(2026, 8, 22, 12, 0, tzinfo=timezone(timedelta(hours=9))),
    ],
)
def test_rejects_invalid_four_hour_utc_boundary(start_time: datetime) -> None:
    with pytest.raises(ValueError):
        aggregate_four_hour_bar(
            market="KRW-BTC",
            start_time_utc=start_time,
            as_of=_END,
            minute_candles=(),
        )


def test_one_missing_minute_returns_structured_incomplete() -> None:
    minutes = tuple(candle for index, candle in enumerate(_complete_minutes()) if index != 42)

    result = aggregate_four_hour_bar(
        market="KRW-BTC",
        start_time_utc=_START,
        as_of=_END,
        minute_candles=minutes,
    )

    assert result.status is FourHourBarStatus.INCOMPLETE
    assert result.reason is FourHourBarReason.MISSING_MINUTES
    assert result.expected_count == 240
    assert result.actual_count == 239
    assert result.missing_minute_starts == (_START + timedelta(minutes=42),)
    assert result.resolved_count == 239
    assert result.traded_count == 239
    assert result.no_trade_count == 0
    assert result.unresolved_count == 1
    assert result.candle is None


def test_rejects_duplicate_minute_start() -> None:
    minutes = (*_complete_minutes(), _minute(0))

    with pytest.raises(ValueError, match="duplicate"):
        aggregate_four_hour_bar(
            market="KRW-BTC",
            start_time_utc=_START,
            as_of=_END,
            minute_candles=minutes,
        )


def test_rejects_market_mismatch() -> None:
    minutes = list(_complete_minutes())
    minutes[10] = _minute(10, market="KRW-ETH")

    with pytest.raises(ValueError, match="market"):
        aggregate_four_hour_bar(
            market="KRW-BTC",
            start_time_utc=_START,
            as_of=_END,
            minute_candles=minutes,
        )


def test_rejects_minute_outside_interval() -> None:
    minutes = (
        *_complete_minutes(),
        replace(_minute(0), start_time_utc=_END, end_time_utc=_END + timedelta(minutes=1)),
    )

    with pytest.raises(ValueError, match="outside"):
        aggregate_four_hour_bar(
            market="KRW-BTC",
            start_time_utc=_START,
            as_of=_END,
            minute_candles=minutes,
        )


def test_in_progress_interval_never_produces_candle() -> None:
    result = aggregate_four_hour_bar(
        market="KRW-BTC",
        start_time_utc=_START,
        as_of=_END - timedelta(microseconds=1),
        minute_candles=_complete_minutes(),
    )

    assert result.status is FourHourBarStatus.INCOMPLETE
    assert result.reason is FourHourBarReason.INTERVAL_NOT_COMPLETE
    assert result.actual_count == 239
    assert result.resolved_count == 239
    assert result.traded_count == 239
    assert result.unresolved_count == 1
    assert result.missing_minute_starts == (_END - timedelta(minutes=1),)
    assert result.candle is None
