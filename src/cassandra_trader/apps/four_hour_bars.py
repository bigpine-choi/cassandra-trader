"""Application service for deriving four-hour bars from stored minute candles."""

from datetime import datetime, timedelta
from pathlib import Path

from cassandra_trader.adapters.storage.sqlite import SqliteMinuteCandleRepository
from cassandra_trader.domain.four_hour_bar import (
    FourHourBarResult,
    aggregate_four_hour_bar,
)


def derive_four_hour_bar(
    market: str,
    start_time_utc: datetime,
    as_of: datetime,
    database: str | Path,
) -> FourHourBarResult:
    """Load as-of minute revisions and derive one UTC four-hour bar."""

    end_time_utc = start_time_utc + timedelta(hours=4)

    with SqliteMinuteCandleRepository(database) as repository:
        stored_candles = repository.list_minute_candles_as_of(
            market,
            start_time_utc,
            end_time_utc,
            as_of,
        )

    return aggregate_four_hour_bar(
        market=market,
        start_time_utc=start_time_utc,
        as_of=as_of,
        minute_candles=tuple(stored.candle for stored in stored_candles),
    )
