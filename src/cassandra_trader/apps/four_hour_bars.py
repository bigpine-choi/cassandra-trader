"""Application service for deriving four-hour bars from stored minute candles."""

from datetime import datetime, timedelta
from pathlib import Path

from cassandra_trader.apps.minute_slots import resolve_minute_slots
from cassandra_trader.domain.four_hour_bar import (
    FourHourBarResult,
    aggregate_resolved_four_hour_bar,
)


def derive_four_hour_bar(
    market: str,
    start_time_utc: datetime,
    as_of: datetime,
    database: str | Path,
) -> FourHourBarResult:
    """Resolve point-in-time minute slots and derive one UTC four-hour bar."""

    end_time_utc = start_time_utc + timedelta(hours=4)

    minute_slots = resolve_minute_slots(
        market,
        start_time_utc,
        end_time_utc,
        as_of,
        database,
    )

    return aggregate_resolved_four_hour_bar(
        market=market,
        start_time_utc=start_time_utc,
        as_of=as_of,
        minute_slots=minute_slots,
    )
