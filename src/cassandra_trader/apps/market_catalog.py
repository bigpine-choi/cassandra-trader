"""Application service for retrieving the Upbit market catalog."""

from upbit import AsyncUpbit

from cassandra_trader.adapters.upbit.public_data import UpbitPublicDataAdapter
from cassandra_trader.domain.market import TradingPair


async def fetch_krw_trading_pairs() -> tuple[TradingPair, ...]:
    """Fetch the current KRW trading-pair catalog from Upbit."""

    async with AsyncUpbit(
        environment="kr",
        timeout=10.0,
        max_retries=2,
    ) as client:
        adapter = UpbitPublicDataAdapter(client.trading_pairs.list)

        return await adapter.list_trading_pairs("KRW")
