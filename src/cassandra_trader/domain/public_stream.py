"""Domain events emitted by the public market-data stream."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum


class StreamType(StrEnum):
    """Upbit stream delivery category."""

    SNAPSHOT = "SNAPSHOT"
    REALTIME = "REALTIME"


@dataclass(frozen=True, slots=True)
class TickerEvent:
    """Exchange-independent realtime ticker event."""

    market: str
    trade_price: Decimal
    signed_change_rate: Decimal
    acc_trade_price_24h: Decimal
    trade_timestamp_utc: datetime
    received_at: datetime
    stream_type: StreamType

    def __post_init__(self) -> None:
        _validate_market(self.market)
        _validate_decimal(self.trade_price, "trade_price", non_negative=True)
        _validate_decimal(self.signed_change_rate, "signed_change_rate")
        _validate_decimal(
            self.acc_trade_price_24h,
            "acc_trade_price_24h",
            non_negative=True,
        )
        _validate_utc(self.trade_timestamp_utc, "trade_timestamp_utc")
        _validate_utc(self.received_at, "received_at")

        if not isinstance(self.stream_type, StreamType):
            raise ValueError("stream_type must be a StreamType")


@dataclass(frozen=True, slots=True)
class MinuteCandleUpdate:
    """Provisional one-minute candle update from a realtime stream."""

    market: str
    start_time_utc: datetime
    end_time_utc: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    base_volume: Decimal
    quote_volume: Decimal
    event_time_utc: datetime
    received_at: datetime
    stream_type: StreamType

    def __post_init__(self) -> None:
        _validate_market(self.market)
        _validate_utc(self.start_time_utc, "start_time_utc")
        _validate_utc(self.end_time_utc, "end_time_utc")
        _validate_utc(self.event_time_utc, "event_time_utc")
        _validate_utc(self.received_at, "received_at")

        if self.start_time_utc.second != 0 or self.start_time_utc.microsecond != 0:
            raise ValueError("start_time_utc must align to a one-minute boundary")

        if self.end_time_utc != self.start_time_utc + timedelta(minutes=1):
            raise ValueError("end_time_utc must equal start_time_utc plus one minute")

        prices = (self.open, self.high, self.low, self.close)
        volumes = (self.base_volume, self.quote_volume)

        for name, value in zip(("open", "high", "low", "close"), prices, strict=True):
            _validate_decimal(value, name, non_negative=True)

        for name, value in zip(
            ("base_volume", "quote_volume"),
            volumes,
            strict=True,
        ):
            _validate_decimal(value, name, non_negative=True)

        if self.low > min(self.open, self.close) or self.high < max(self.open, self.close):
            raise ValueError("OHLC values must satisfy low <= open/close <= high")

        if not isinstance(self.stream_type, StreamType):
            raise ValueError("stream_type must be a StreamType")


type PublicMarketEvent = TickerEvent | MinuteCandleUpdate


def _validate_market(market: str) -> None:
    quote, separator, base = market.partition("-")

    if not separator or not quote.isalnum() or not base.isalnum() or market != market.upper():
        raise ValueError(f"Invalid market code: {market}")


def _validate_decimal(value: Decimal, name: str, *, non_negative: bool = False) -> None:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"{name} must be a finite Decimal")

    if non_negative and value < 0:
        raise ValueError(f"{name} must be non-negative")


def _validate_utc(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be timezone-aware UTC")
