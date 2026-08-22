"""Application service for retrieving recent completed minute candles."""

from pathlib import Path
from uuid import uuid4

from upbit import AsyncUpbit

from cassandra_trader.adapters.storage.sqlite import (
    IngestionStats,
    RawMinuteCandleInput,
    SqliteMinuteCandleRepository,
)
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


async def sync_recent_minute_candles(
    market: str,
    count: int,
    database: str | Path,
) -> IngestionStats:
    """Fetch and atomically persist one raw and canonical candle batch."""

    async with AsyncUpbit(
        environment="kr",
        timeout=10.0,
        max_retries=2,
    ) as client:
        adapter = UpbitPublicDataAdapter(
            list_minute_candles=client.candles.list_minutes,
        )
        batch = await adapter.fetch_minute_candle_batch(market, count)

    records = tuple(
        RawMinuteCandleInput(
            response_order=observation.response_order,
            market=observation.market,
            source_candle_time=observation.source_candle_time,
            raw_payload=observation.raw_payload,
            candle=observation.candle,
        )
        for observation in batch.observations
    )

    with SqliteMinuteCandleRepository(database) as repository:
        return repository.ingest_batch(
            batch_id=str(uuid4()),
            source="upbit",
            endpoint=batch.endpoint,
            request_parameters=batch.request_parameters,
            received_at=batch.received_at,
            records=records,
        )
