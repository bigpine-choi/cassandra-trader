"""Immutable public trade and orderbook observations."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from cassandra_trader.domain.public_stream import StreamType


class TradeSide(StrEnum):
    """Aggressor side reported by Upbit."""

    ASK = "ASK"
    BID = "BID"


@dataclass(frozen=True, slots=True)
class TradeEvent:
    """One normalized public trade observation."""

    market: str
    trade_price: Decimal
    trade_volume: Decimal
    side: TradeSide
    trade_timestamp_utc: datetime
    message_timestamp_utc: datetime
    sequential_id: int
    best_ask_price: Decimal
    best_ask_size: Decimal
    best_bid_price: Decimal
    best_bid_size: Decimal
    stream_type: StreamType
    observed_at: datetime

    def __post_init__(self) -> None:
        _validate_market(self.market)
        for name, value in (
            ("trade_price", self.trade_price),
            ("trade_volume", self.trade_volume),
            ("best_ask_price", self.best_ask_price),
            ("best_ask_size", self.best_ask_size),
            ("best_bid_price", self.best_bid_price),
            ("best_bid_size", self.best_bid_size),
        ):
            _validate_non_negative_decimal(value, name)
        if not isinstance(self.side, TradeSide):
            raise ValueError("side must be ASK or BID")
        if (
            not isinstance(self.sequential_id, int)
            or isinstance(self.sequential_id, bool)
            or self.sequential_id < 0
        ):
            raise ValueError("sequential_id must be a non-negative integer")
        _validate_utc(self.trade_timestamp_utc, "trade_timestamp_utc")
        _validate_utc(self.message_timestamp_utc, "message_timestamp_utc")
        _validate_utc(self.observed_at, "observed_at")
        if not isinstance(self.stream_type, StreamType):
            raise ValueError("stream_type must be a StreamType")


@dataclass(frozen=True, slots=True)
class OrderbookUnit:
    """One ask/bid pair in the source-provided order."""

    ask_price: Decimal
    bid_price: Decimal
    ask_size: Decimal
    bid_size: Decimal

    def __post_init__(self) -> None:
        for name, value in (
            ("ask_price", self.ask_price),
            ("bid_price", self.bid_price),
            ("ask_size", self.ask_size),
            ("bid_size", self.bid_size),
        ):
            _validate_non_negative_decimal(value, name)


@dataclass(frozen=True, slots=True)
class OrderbookEvent:
    """One normalized public orderbook snapshot or realtime observation."""

    market: str
    message_timestamp_utc: datetime
    total_ask_size: Decimal
    total_bid_size: Decimal
    grouping_level: Decimal
    stream_type: StreamType
    units: tuple[OrderbookUnit, ...]
    observed_at: datetime

    def __post_init__(self) -> None:
        _validate_market(self.market)
        _validate_utc(self.message_timestamp_utc, "message_timestamp_utc")
        _validate_utc(self.observed_at, "observed_at")
        for name, value in (
            ("total_ask_size", self.total_ask_size),
            ("total_bid_size", self.total_bid_size),
            ("grouping_level", self.grouping_level),
        ):
            _validate_non_negative_decimal(value, name)
        if not isinstance(self.stream_type, StreamType):
            raise ValueError("stream_type must be a StreamType")
        if (
            not isinstance(self.units, tuple)
            or not self.units
            or any(not isinstance(unit, OrderbookUnit) for unit in self.units)
        ):
            raise ValueError("units must be a non-empty immutable OrderbookUnit tuple")


type MicrostructureEvent = TradeEvent | OrderbookEvent


def _validate_market(market: str) -> None:
    if not isinstance(market, str) or market != market.upper():
        raise ValueError("market must be an uppercase ASCII market code")
    quote, separator, base = market.partition("-")
    if (
        not separator
        or not quote.isascii()
        or not base.isascii()
        or not quote.isalnum()
        or not base.isalnum()
    ):
        raise ValueError("market must be an uppercase ASCII market code")


def _validate_non_negative_decimal(value: Decimal, name: str) -> None:
    if (
        not isinstance(value, Decimal)
        or isinstance(value, bool)
        or not value.is_finite()
        or value < 0
    ):
        raise ValueError(f"{name} must be a finite non-negative Decimal")


def _validate_utc(value: datetime, name: str) -> None:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be timezone-aware UTC")
