import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from cassandra_trader.apps.liquidity_top_n import evaluate_liquidity_top_n_as_of
from cassandra_trader.domain.liquidity_top_n import LiquidityTopNEvaluationStatus
from cassandra_trader.domain.market import (
    MarketUniverseResult,
    MarketUniverseStatus,
    TradingPair,
)
from cassandra_trader.domain.market_eligibility import evaluate_market_eligibility
from cassandra_trader.domain.market_liquidity import evaluate_market_liquidity
from cassandra_trader.domain.minute_slot import MinuteSlotSummary

_WINDOW_END = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
_WINDOW_START = _WINDOW_END - timedelta(days=30)
_AS_OF = _WINDOW_END + timedelta(minutes=10)
_MAXIMUM_AGE = timedelta(hours=1)


def _liquidity_result():
    universe = MarketUniverseResult(
        status=MarketUniverseStatus.AVAILABLE,
        quote_currency="KRW",
        as_of=_AS_OF,
        snapshot_id="snapshot-1",
        observed_at=_AS_OF - timedelta(minutes=5),
        available_at=_AS_OF - timedelta(minutes=4),
        content_hash="a" * 64,
        pairs=(TradingPair("KRW-BTC", "비트코인", "Bitcoin", False),),
    )
    eligibility = evaluate_market_eligibility(universe, _MAXIMUM_AGE)
    return evaluate_market_liquidity(
        eligibility,
        _WINDOW_END,
        (
            MinuteSlotSummary(
                market="KRW-BTC",
                start_time_utc=_WINDOW_START,
                end_time_utc=_WINDOW_END,
                as_of=_AS_OF,
                expected_count=43_200,
                traded_count=1,
                no_trade_count=43_199,
                unresolved_count=0,
                quote_volume=Decimal("10.5"),
            ),
        ),
    )


def test_application_reuses_existing_liquidity_service_without_writes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    liquidity = _liquidity_result()
    database = tmp_path / "liquidity.db"
    calls: list[tuple[object, ...]] = []

    def fake_liquidity(*args):
        calls.append(args)
        return liquidity

    monkeypatch.setattr(
        "cassandra_trader.apps.liquidity_top_n.evaluate_market_liquidity_as_of",
        fake_liquidity,
    )

    result = evaluate_liquidity_top_n_as_of(
        "KRW",
        _WINDOW_END,
        _AS_OF,
        database,
        _MAXIMUM_AGE,
        1,
    )

    assert result.status is LiquidityTopNEvaluationStatus.RESOLVED
    assert result.selected_markets == ("KRW-BTC",)
    assert calls == [("KRW", _WINDOW_END, _AS_OF, database, _MAXIMUM_AGE)]
    assert not database.exists()


@pytest.mark.parametrize("top_n", [0, -1, True])
def test_invalid_top_n_is_rejected_before_liquidity_or_database_access(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    top_n: object,
) -> None:
    called = False

    def unexpected_liquidity(*args, **kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(
        "cassandra_trader.apps.liquidity_top_n.evaluate_market_liquidity_as_of",
        unexpected_liquidity,
    )
    database = tmp_path / "missing.db"

    with pytest.raises(ValueError):
        evaluate_liquidity_top_n_as_of(
            "KRW",
            _WINDOW_END,
            _AS_OF,
            database,
            _MAXIMUM_AGE,
            top_n,  # type: ignore[arg-type]
        )

    assert called is False
    assert not database.exists()


@pytest.mark.parametrize("error", [TimeoutError("timed out"), asyncio.CancelledError()])
def test_application_propagates_upstream_errors_without_success_result(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    error: BaseException,
) -> None:
    calls = 0

    def failed_liquidity(*args, **kwargs):
        nonlocal calls
        calls += 1
        raise error

    monkeypatch.setattr(
        "cassandra_trader.apps.liquidity_top_n.evaluate_market_liquidity_as_of",
        failed_liquidity,
    )

    with pytest.raises(type(error)):
        evaluate_liquidity_top_n_as_of(
            "KRW",
            _WINDOW_END,
            _AS_OF,
            tmp_path / "missing.db",
            _MAXIMUM_AGE,
            1,
        )

    assert calls == 1
