from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from cassandra_trader.domain.candle import MinuteCandle
from cassandra_trader.domain.minute_slot import MinuteSlotStatus, ResolvedMinuteSlot

_START = datetime(2026, 8, 22, 8, 0, tzinfo=UTC)
_AVAILABLE = datetime(2026, 8, 23, tzinfo=UTC)


def _candle() -> MinuteCandle:
    return MinuteCandle(
        market="KRW-BTC",
        start_time_utc=_START,
        end_time_utc=_START + timedelta(minutes=1),
        open=Decimal("10"),
        high=Decimal("11"),
        low=Decimal("9"),
        close=Decimal("10.5"),
        base_volume=Decimal("1"),
        quote_volume=Decimal("10.5"),
    )


@pytest.mark.parametrize(
    "start",
    [
        _START.replace(tzinfo=None),
        _START.astimezone(timezone(timedelta(hours=9))),
        _START + timedelta(seconds=1),
    ],
)
def test_resolved_slot_rejects_invalid_utc_minute_start(start: datetime) -> None:
    with pytest.raises(ValueError):
        ResolvedMinuteSlot(
            market="KRW-BTC",
            start_time_utc=start,
            end_time_utc=start + timedelta(minutes=1),
            status=MinuteSlotStatus.UNRESOLVED,
            candle=None,
            canonical_revision=None,
            canonical_available_at=None,
        )


def test_resolved_slot_rejects_non_utc_end() -> None:
    with pytest.raises(ValueError, match="end_time_utc"):
        ResolvedMinuteSlot(
            market="KRW-BTC",
            start_time_utc=_START,
            end_time_utc=(_START + timedelta(minutes=1)).replace(tzinfo=None),
            status=MinuteSlotStatus.UNRESOLVED,
            candle=None,
            canonical_revision=None,
            canonical_available_at=None,
        )


@pytest.mark.parametrize(
    ("revision", "available_at"),
    [
        (0, _AVAILABLE),
        (1, _AVAILABLE.replace(tzinfo=None)),
        (1, _AVAILABLE.astimezone(timezone(timedelta(hours=9)))),
    ],
)
def test_traded_slot_rejects_invalid_revision_or_availability(
    revision: int,
    available_at: datetime,
) -> None:
    with pytest.raises(ValueError):
        ResolvedMinuteSlot(
            market="KRW-BTC",
            start_time_utc=_START,
            end_time_utc=_START + timedelta(minutes=1),
            status=MinuteSlotStatus.TRADED,
            candle=_candle(),
            canonical_revision=revision,
            canonical_available_at=available_at,
        )
