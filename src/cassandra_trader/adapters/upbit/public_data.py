"""Read-only adapter for Upbit public market data."""

from collections.abc import Awaitable, Callable

from upbit.types import TradingPairListResponse

from cassandra_trader.domain.market import TradingPair

type ListTradingPairs = Callable[..., Awaitable[TradingPairListResponse]]


class UpbitPublicDataAdapter:
    """Translate Upbit SDK responses into Cassandra domain objects."""

    def __init__(self, list_trading_pairs: ListTradingPairs) -> None:
        self._list_trading_pairs = list_trading_pairs

    async def list_trading_pairs(
        self,
        quote_currency: str = "KRW",
    ) -> tuple[TradingPair, ...]:
        """Return sorted trading pairs for the requested quote currency."""

        normalized_quote = quote_currency.strip().upper()

        if not normalized_quote:
            raise ValueError("quote_currency must not be empty")

        raw_pairs = await self._list_trading_pairs(is_details=True)
        prefix = f"{normalized_quote}-"

        pairs = [
            TradingPair(
                code=raw.market,
                korean_name=raw.korean_name,
                english_name=raw.english_name,
                is_warning=bool(
                    (raw.market_event and raw.market_event.warning)
                    or raw.market_warning == "CAUTION"
                ),
            )
            for raw in raw_pairs
            if raw.market.startswith(prefix)
        ]

        return tuple(sorted(pairs, key=lambda pair: pair.code))
