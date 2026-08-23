"""Read-only adapter for Upbit public market data."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from upbit.types import CandleListMinutesResponse, TradingPairListResponse

from cassandra_trader.domain.candle import MinuteCandle
from cassandra_trader.domain.market import TradingPair, normalize_quote_currency

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


@dataclass(frozen=True, slots=True)
class TradingPairObservation:
    """One raw Upbit market response item and its normalized trading pair."""

    response_order: int
    market: str
    raw_payload: dict[str, object]
    pair: TradingPair


@dataclass(frozen=True, slots=True)
class TradingPairResponseBatch:
    """One recorded Upbit trading-pair REST response for a quote currency."""

    endpoint: str
    request_parameters: dict[str, bool]
    received_at: datetime
    observations: tuple[TradingPairObservation, ...]


class UpbitPublicDataAdapter:
    """Translate Upbit SDK responses into Cassandra domain objects."""

    def __init__(
        self,
        list_trading_pairs: ListTradingPairs | None = None,
        list_minute_candles: ListMinuteCandles | None = None,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._list_trading_pairs = list_trading_pairs
        self._list_minute_candles = list_minute_candles
        self._clock = clock or (lambda: datetime.now(UTC))

    async def list_trading_pairs(
        self,
        quote_currency: str = "KRW",
    ) -> tuple[TradingPair, ...]:
        """Return sorted trading pairs for the requested quote currency."""

        batch = await self.fetch_trading_pair_batch(quote_currency)
        pairs: dict[str, TradingPair] = {}

        for observation in batch.observations:
            if observation.market in pairs:
                raise ValueError(f"duplicate market in trading-pair response: {observation.market}")

            pairs[observation.market] = observation.pair

        return tuple(pairs[market] for market in sorted(pairs))

    async def fetch_trading_pair_batch(
        self,
        quote_currency: str = "KRW",
    ) -> TradingPairResponseBatch:
        """Fetch raw market details alongside normalized pairs for one quote."""

        normalized_quote = normalize_quote_currency(quote_currency)

        if self._list_trading_pairs is None:
            raise RuntimeError("trading-pair API is not configured")

        request_parameters = {"is_details": True}
        raw_pairs = await self._list_trading_pairs(**request_parameters)
        received_at = _as_utc(self._clock(), "clock")
        observations: list[TradingPairObservation] = []

        for response_order, raw in enumerate(raw_pairs):
            pair = TradingPair(
                code=raw.market,
                korean_name=raw.korean_name,
                english_name=raw.english_name,
                is_warning=bool(
                    (raw.market_event and raw.market_event.warning)
                    or raw.market_warning == "CAUTION"
                ),
            )

            if pair.quote_currency != normalized_quote:
                continue

            observations.append(
                TradingPairObservation(
                    response_order=response_order,
                    market=pair.code,
                    raw_payload=_sdk_model_payload(raw),
                    pair=pair,
                )
            )

        return TradingPairResponseBatch(
            endpoint="/v1/market/all",
            request_parameters=request_parameters,
            received_at=received_at,
            observations=tuple(sorted(observations, key=lambda item: item.market)),
        )

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
        to: datetime | None = None,
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
        request_to = to or complete_before

        if request_to.tzinfo is None:
            raise ValueError("to must be timezone-aware")

        request_to = request_to.astimezone(UTC)
        request_parameters: dict[str, str | int] = {
            "market": normalized_market,
            "count": count,
            "to": request_to.isoformat().replace("+00:00", "Z"),
        }
        raw_candles = await self._list_minute_candles(
            1,
            **request_parameters,
        )
        received_at = _as_utc(self._clock(), "clock")
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

    if (
        not separator
        or not quote.isascii()
        or not quote.isalnum()
        or not base.isascii()
        or not base.isalnum()
    ):
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

    return {key: _json_compatible(value) for key, value in vars(model).items()}


def _json_compatible(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_compatible(item) for key, item in value.items()}

    if isinstance(value, (list, tuple)):
        return [_json_compatible(item) for item in value]

    if hasattr(value, "__dict__"):
        return {key: _json_compatible(item) for key, item in vars(value).items()}

    return value


def _as_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware")

    return value.astimezone(UTC)
