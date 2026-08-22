from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from cassandra_trader.cli import main
from cassandra_trader.domain.candle import MinuteCandle
from cassandra_trader.domain.market import TradingPair


def test_main(capsys: pytest.CaptureFixture[str]) -> None:
    main([])

    captured = capsys.readouterr()

    assert captured.out == "Cassandra Trader\n"


def test_markets_command(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def fake_fetch_krw_trading_pairs() -> tuple[TradingPair, ...]:
        return (
            TradingPair(
                code="KRW-BTC",
                korean_name="비트코인",
                english_name="Bitcoin",
                is_warning=False,
            ),
            TradingPair(
                code="KRW-ETH",
                korean_name="이더리움",
                english_name="Ethereum",
                is_warning=False,
            ),
        )

    monkeypatch.setattr(
        "cassandra_trader.cli.fetch_krw_trading_pairs",
        fake_fetch_krw_trading_pairs,
    )

    main(["markets", "--limit", "1"])

    captured = capsys.readouterr()

    assert captured.out == "KRW trading pairs: 2\nKRW-BTC      비트코인\n"


def test_candles_command(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def fake_fetch_recent_minute_candles(
        market: str,
        count: int,
    ) -> tuple[MinuteCandle, ...]:
        assert market == "KRW-BTC"
        assert count == 1
        start = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)

        return (
            MinuteCandle(
                market=market,
                start_time_utc=start,
                end_time_utc=start + timedelta(minutes=1),
                open=Decimal("100"),
                high=Decimal("110"),
                low=Decimal("90"),
                close=Decimal("105"),
                base_volume=Decimal("1.5"),
                quote_volume=Decimal("150"),
            ),
        )

    monkeypatch.setattr(
        "cassandra_trader.cli.fetch_recent_minute_candles",
        fake_fetch_recent_minute_candles,
    )

    main(["candles", "krw-btc", "--count", "1"])

    captured = capsys.readouterr()

    assert captured.out == (
        "KRW-BTC completed 1m candles: 1\n"
        "2025-01-01T12:00:00Z O=100 H=110 L=90 C=105 "
        "base_volume=1.5 quote_volume=150\n"
    )


@pytest.mark.parametrize(
    "arguments",
    [
        ["candles", "BTC", "--count", "1"],
        ["candles", "KRW-BTC", "--count", "0"],
        ["candles", "KRW-BTC", "--count", "201"],
    ],
)
def test_candles_command_rejects_invalid_arguments(arguments: list[str]) -> None:
    with pytest.raises(SystemExit):
        main(arguments)
