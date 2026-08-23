from datetime import UTC, datetime, timedelta

import pytest

from cassandra_trader.domain.market import (
    MarketUniverseResult,
    MarketUniverseStatus,
    TradingPair,
    normalize_quote_currency,
)


def test_trading_pair_requires_uppercase_ascii_market_code() -> None:
    for code in ("krw-btc", "KRW-비트", "ＫＲＷ-BTC", "KRW-BTC-ETH"):
        with pytest.raises(ValueError, match="trading-pair code"):
            TradingPair(code, "이름", "Name", False)


def test_quote_currency_is_trimmed_uppercased_and_ascii_validated() -> None:
    assert normalize_quote_currency(" krw ") == "KRW"

    for value in ("", "ＫＲＷ", "KR원", "KRW-"):
        with pytest.raises(ValueError, match="quote_currency"):
            normalize_quote_currency(value)


def test_available_universe_rejects_negative_snapshot_age() -> None:
    as_of = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
    pair = TradingPair("KRW-BTC", "비트코인", "Bitcoin", False)

    with pytest.raises(ValueError, match="available_at"):
        MarketUniverseResult(
            status=MarketUniverseStatus.AVAILABLE,
            quote_currency="KRW",
            as_of=as_of,
            snapshot_id="snapshot-1",
            observed_at=as_of,
            available_at=as_of + timedelta(microseconds=1),
            content_hash="a" * 64,
            pairs=(pair,),
        )
