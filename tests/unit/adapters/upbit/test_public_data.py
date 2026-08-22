import asyncio
from types import SimpleNamespace
from typing import cast

import pytest
from upbit.types import TradingPairListResponse

from cassandra_trader.adapters.upbit.public_data import UpbitPublicDataAdapter


def _raw_pair(
    market: str,
    korean_name: str,
    english_name: str,
    *,
    event_warning: bool = False,
    legacy_warning: str = "NONE",
) -> SimpleNamespace:
    return SimpleNamespace(
        market=market,
        korean_name=korean_name,
        english_name=english_name,
        market_event=SimpleNamespace(warning=event_warning),
        market_warning=legacy_warning,
    )


def test_list_trading_pairs_filters_normalizes_and_sorts() -> None:
    calls: list[bool] = []

    async def fake_list(*, is_details: bool) -> TradingPairListResponse:
        calls.append(is_details)

        return cast(
            TradingPairListResponse,
            [
                _raw_pair("KRW-BTC", "비트코인", "Bitcoin"),
                _raw_pair(
                    "KRW-ADA",
                    "에이다",
                    "Cardano",
                    legacy_warning="CAUTION",
                ),
                _raw_pair("BTC-ETH", "이더리움", "Ethereum"),
            ],
        )

    adapter = UpbitPublicDataAdapter(fake_list)

    pairs = asyncio.run(adapter.list_trading_pairs(" krw "))

    assert calls == [True]
    assert [pair.code for pair in pairs] == ["KRW-ADA", "KRW-BTC"]
    assert pairs[0].is_warning is True
    assert pairs[1].is_warning is False
    assert pairs[1].quote_currency == "KRW"
    assert pairs[1].base_currency == "BTC"


def test_list_trading_pairs_rejects_empty_quote_currency() -> None:
    async def fake_list(*, is_details: bool) -> TradingPairListResponse:
        raise AssertionError("API must not be called")

    adapter = UpbitPublicDataAdapter(fake_list)

    with pytest.raises(ValueError, match="quote_currency"):
        asyncio.run(adapter.list_trading_pairs("   "))
