"""Application service for retrieving recent completed minute candles."""

from upbit import AsyncUpbit

from cassandra_trader.adapters.upbit.public_data import UpbitPublicDataAdapter
from cassandra_trader.domain.candle import MinuteCandle


async def fetch_recent_minute_candles(
    market: str,
    count: int,
) -> tuple[MinuteCandle, ...]:
    """Fetch recent completed one-minute candles from Upbit public data."""

    async with AsyncUpbit(
        environment="kr",
        timeout=10.0,
        max_retries=2,
    ) as client:
        adapter = UpbitPublicDataAdapter(
            list_minute_candles=client.candles.list_minutes,
        )

        return await adapter.list_minute_candles(market, count)
