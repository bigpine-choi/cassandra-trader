"""Read-only adapter for Upbit public market data."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from upbit.types import CandleListMinutesResponse, TradingPairListResponse

from cassandra_trader.domain.candle import MinuteCandle
from cassandra_trader.domain.market import TradingPair

type ListTradingPairs = Callable[..., Awaitable[TradingPairListResponse]]
type ListMinuteCandles = Callable[..., Awaitable[CandleListMinutesResponse]]


@dataclass(frozen=True, slots=True)
class MinuteCandleObservation:
    """One raw Upbit response item and its completed canonical candle, if any."""

    response_order: int
    market: str
    source_candle_time: str
    raw_payload: dict[str, object]
    candle: MinuteCandle | None


@dataclass(frozen=True, slots=True)
class MinuteCandleResponseBatch:
    """One recorded Upbit minute-candle REST response."""

    endpoint: str
    request_parameters: dict[str, str | int]
    received_at: datetime
    observations: tuple[MinuteCandleObservation, ...]


class UpbitPublicDataAdapter:
    """Translate Upbit SDK responses into Cassandra domain objects."""

    def __init__(
        self,
        list_trading_pairs: ListTradingPairs | None = None,
        list_minute_candles: ListMinuteCandles | None = None,
    ) -> None:
        self._list_trading_pairs = list_trading_pairs
        self._list_minute_candles = list_minute_candles

    async def list_trading_pairs(
        self,
        quote_currency: str = "KRW",
    ) -> tuple[TradingPair, ...]:
        """Return sorted trading pairs for the requested quote currency."""

        normalized_quote = quote_currency.strip().upper()

        if not normalized_quote:
            raise ValueError("quote_currency must not be empty")

        if self._list_trading_pairs is None:
            raise RuntimeError("trading-pair API is not configured")

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

    async def list_minute_candles(
        self,
        market: str,
        count: int = 1,
        *,
        as_of: datetime | None = None,
    ) -> tuple[MinuteCandle, ...]:
        """Return recent completed one-minute candles in chronological order."""

        batch = await self.fetch_minute_candle_batch(
            market,
            count,
            as_of=as_of,
        )

        unique: dict[tuple[str, datetime], MinuteCandle] = {}

        for observation in batch.observations:
            if observation.candle is not None:
                candle = observation.candle
                unique.setdefault((candle.market, candle.start_time_utc), candle)

        ordered = sorted(unique.values(), key=lambda candle: candle.start_time_utc)

        return tuple(ordered[-count:])

    async def fetch_minute_candle_batch(
        self,
        market: str,
        count: int = 1,
        *,
        as_of: datetime | None = None,
    ) -> MinuteCandleResponseBatch:
        """Fetch raw response fields alongside completed canonical candles."""

        normalized_market = _normalize_market(market)

        if not 1 <= count <= 200:
            raise ValueError("count must be between 1 and 200")

        if self._list_minute_candles is None:
            raise RuntimeError("minute-candle API is not configured")

        observed_at = as_of or datetime.now(UTC)

        if observed_at.tzinfo is None:
            raise ValueError("as_of must be timezone-aware")

        observed_at = observed_at.astimezone(UTC)
        complete_before = observed_at.replace(second=0, microsecond=0)
        request_parameters: dict[str, str | int] = {
            "market": normalized_market,
            "count": count,
            "to": complete_before.isoformat().replace("+00:00", "Z"),
        }
        raw_candles = await self._list_minute_candles(
            1,
            **request_parameters,
        )
        received_at = datetime.now(UTC)
        observations: list[MinuteCandleObservation] = []

        for response_order, raw in enumerate(raw_candles):
            source_candle_time = raw.candle_date_time_utc
            start_time = _parse_upbit_utc(source_candle_time)
            mapped = MinuteCandle(
                market=raw.market,
                start_time_utc=start_time,
                end_time_utc=start_time + timedelta(minutes=1),
                open=Decimal(raw.opening_price),
                high=Decimal(raw.high_price),
                low=Decimal(raw.low_price),
                close=Decimal(raw.trade_price),
                base_volume=Decimal(raw.candle_acc_trade_volume),
                quote_volume=Decimal(raw.candle_acc_trade_price),
            )
            observations.append(
                MinuteCandleObservation(
                    response_order=response_order,
                    market=raw.market,
                    source_candle_time=start_time.isoformat().replace("+00:00", "Z"),
                    raw_payload=_sdk_model_payload(raw),
                    candle=mapped if mapped.end_time_utc <= observed_at else None,
                )
            )

        return MinuteCandleResponseBatch(
            endpoint="/v1/candles/minutes/1",
            request_parameters=request_parameters,
            received_at=received_at,
            observations=tuple(observations),
        )


def _normalize_market(market: str) -> str:
    normalized_market = market.strip().upper()
    quote, separator, base = normalized_market.partition("-")

    if not separator or not quote.isalnum() or not base.isalnum():
        raise ValueError(f"Invalid market code: {market}")

    return normalized_market


def _parse_upbit_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)

    return parsed.astimezone(UTC)


def _sdk_model_payload(model: Any) -> dict[str, object]:
    model_dump = getattr(model, "model_dump", None)

    if callable(model_dump):
        return dict(model_dump(mode="json"))

    return dict(vars(model))
