import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import cast

import pytest
from upbit.types import CandleListMinutesResponse, TradingPairListResponse

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


def _raw_candle(
    start_time: str,
    *,
    market: str = "KRW-BTC",
    opening_price: str = "100",
    high_price: str = "110",
    low_price: str = "90",
    trade_price: str = "105",
    base_volume: str = "1.5",
    quote_volume: str = "150",
) -> SimpleNamespace:
    return SimpleNamespace(
        market=market,
        candle_date_time_utc=start_time,
        opening_price=opening_price,
        high_price=high_price,
        low_price=low_price,
        trade_price=trade_price,
        candle_acc_trade_volume=base_volume,
        candle_acc_trade_price=quote_volume,
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


def test_list_minute_candles_maps_sdk_response() -> None:
    calls: list[tuple[int, str, int, str]] = []

    async def fake_list(
        unit: int,
        *,
        market: str,
        count: int,
        to: str,
    ) -> CandleListMinutesResponse:
        calls.append((unit, market, count, to))
        return cast(CandleListMinutesResponse, [_raw_candle("2025-01-01T12:00:00")])

    adapter = UpbitPublicDataAdapter(list_minute_candles=fake_list)

    candles = asyncio.run(
        adapter.list_minute_candles(
            " krw-btc ",
            1,
            as_of=datetime(2025, 1, 1, 12, 2, 30, tzinfo=UTC),
        )
    )

    assert calls == [(1, "KRW-BTC", 1, "2025-01-01T12:02:00Z")]
    assert len(candles) == 1
    assert candles[0].market == "KRW-BTC"
    assert candles[0].start_time_utc == datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    assert candles[0].end_time_utc == datetime(2025, 1, 1, 12, 1, tzinfo=UTC)
    assert candles[0].open == Decimal("100")
    assert candles[0].high == Decimal("110")
    assert candles[0].low == Decimal("90")
    assert candles[0].close == Decimal("105")
    assert candles[0].base_volume == Decimal("1.5")
    assert candles[0].quote_volume == Decimal("150")


def test_list_minute_candles_sorts_oldest_first() -> None:
    async def fake_list(
        unit: int,
        *,
        market: str,
        count: int,
        to: str,
    ) -> CandleListMinutesResponse:
        return cast(
            CandleListMinutesResponse,
            [
                _raw_candle("2025-01-01T12:01:00"),
                _raw_candle("2025-01-01T12:00:00"),
            ],
        )

    adapter = UpbitPublicDataAdapter(list_minute_candles=fake_list)

    candles = asyncio.run(
        adapter.list_minute_candles(
            "KRW-BTC",
            2,
            as_of=datetime(2025, 1, 1, 12, 3, tzinfo=UTC),
        )
    )

    assert [candle.start_time_utc.minute for candle in candles] == [0, 1]


def test_list_minute_candles_excludes_incomplete_current_minute() -> None:
    async def fake_list(
        unit: int,
        *,
        market: str,
        count: int,
        to: str,
    ) -> CandleListMinutesResponse:
        return cast(
            CandleListMinutesResponse,
            [
                _raw_candle("2025-01-01T12:02:00"),
                _raw_candle("2025-01-01T12:01:00"),
            ],
        )

    adapter = UpbitPublicDataAdapter(list_minute_candles=fake_list)

    candles = asyncio.run(
        adapter.list_minute_candles(
            "KRW-BTC",
            2,
            as_of=datetime(2025, 1, 1, 12, 2, 30, tzinfo=UTC),
        )
    )

    assert [candle.start_time_utc.minute for candle in candles] == [1]


def test_list_minute_candles_removes_duplicates() -> None:
    async def fake_list(
        unit: int,
        *,
        market: str,
        count: int,
        to: str,
    ) -> CandleListMinutesResponse:
        return cast(
            CandleListMinutesResponse,
            [
                _raw_candle("2025-01-01T12:00:00", trade_price="105"),
                _raw_candle("2025-01-01T12:00:00", trade_price="104"),
            ],
        )

    adapter = UpbitPublicDataAdapter(list_minute_candles=fake_list)

    candles = asyncio.run(
        adapter.list_minute_candles(
            "KRW-BTC",
            2,
            as_of=datetime(2025, 1, 1, 12, 2, tzinfo=UTC),
        )
    )

    assert len(candles) == 1
    assert candles[0].close == Decimal("105")


@pytest.mark.parametrize(
    ("market", "count"),
    [("", 1), ("BTC", 1), ("KRW-BTC", 0), ("KRW-BTC", 201)],
)
def test_list_minute_candles_rejects_invalid_request(market: str, count: int) -> None:
    async def fake_list(
        unit: int,
        *,
        market: str,
        count: int,
        to: str,
    ) -> CandleListMinutesResponse:
        raise AssertionError("API must not be called")

    adapter = UpbitPublicDataAdapter(list_minute_candles=fake_list)

    with pytest.raises(ValueError):
        asyncio.run(adapter.list_minute_candles(market, count))
