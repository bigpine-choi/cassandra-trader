import asyncio
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from cassandra_trader.adapters.storage.sqlite import (
    MarketCatalogSnapshotMemberInput,
    MarketCatalogSnapshotStatus,
    SqliteMinuteCandleRepository,
)
from cassandra_trader.adapters.upbit.public_data import (
    TradingPairObservation,
    TradingPairResponseBatch,
)
from cassandra_trader.apps.market_universe import (
    capture_market_catalog_into_repository,
    resolve_market_universe_as_of,
)
from cassandra_trader.domain.market import MarketUniverseStatus, TradingPair

_OBSERVED = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
_AVAILABLE = _OBSERVED + timedelta(seconds=1)


def _observation(
    market: str,
    response_order: int,
    *,
    warning: bool = False,
) -> TradingPairObservation:
    pair = TradingPair(
        code=market,
        korean_name=f"한글-{market}",
        english_name=f"English-{market}",
        is_warning=warning,
    )
    return TradingPairObservation(
        response_order=response_order,
        market=market,
        raw_payload={
            "market": market,
            "korean_name": pair.korean_name,
            "english_name": pair.english_name,
            "market_event": {"warning": warning},
        },
        pair=pair,
    )


def _batch(*observations: TradingPairObservation) -> TradingPairResponseBatch:
    return TradingPairResponseBatch(
        endpoint="/v1/market/all",
        request_parameters={"is_details": True},
        received_at=_OBSERVED,
        observations=tuple(observations),
    )


class _FakeAdapter:
    def __init__(
        self,
        batch: TradingPairResponseBatch | None = None,
        error: BaseException | None = None,
    ) -> None:
        self.batch = batch
        self.error = error
        self.calls: list[str] = []

    async def fetch_trading_pair_batch(self, quote_currency: str) -> TradingPairResponseBatch:
        self.calls.append(quote_currency)

        if self.error is not None:
            raise self.error

        assert self.batch is not None
        return self.batch


def _snapshot_rows(database: Path) -> list[tuple[str, str, str | None]]:
    with sqlite3.connect(database) as connection:
        return connection.execute(
            """
            SELECT snapshot_id, status, error_type
            FROM market_catalog_snapshots
            ORDER BY snapshot_id
            """
        ).fetchall()


def test_capture_persists_complete_sorted_snapshot_with_injected_clock_and_id(
    tmp_path: Path,
) -> None:
    database = tmp_path / "catalog.db"
    adapter = _FakeAdapter(
        _batch(
            _observation("KRW-BTC", 1),
            _observation("KRW-ADA", 0, warning=True),
        )
    )

    with SqliteMinuteCandleRepository(database) as repository:
        result = asyncio.run(
            capture_market_catalog_into_repository(
                adapter,
                repository,
                quote_currency=" krw ",
                clock=lambda: _AVAILABLE,
                snapshot_id_factory=lambda: "snapshot-fixed",
            )
        )

    assert adapter.calls == ["KRW"]
    assert result.snapshot_id == "snapshot-fixed"
    assert result.observed_at == _OBSERVED
    assert result.available_at == _AVAILABLE
    assert result.market_count == 2
    assert len(result.content_hash) == 64
    assert [pair.code for pair in result.pairs] == ["KRW-ADA", "KRW-BTC"]
    assert result.pairs[0].is_warning is True


@pytest.mark.parametrize(
    "batch",
    [
        _batch(),
        _batch(_observation("KRW-BTC", 0), _observation("KRW-BTC", 1)),
    ],
)
def test_empty_or_duplicate_response_is_failed_evidence_not_success(
    tmp_path: Path,
    batch: TradingPairResponseBatch,
) -> None:
    database = tmp_path / "catalog.db"
    adapter = _FakeAdapter(batch)

    with SqliteMinuteCandleRepository(database) as repository:
        with pytest.raises(ValueError):
            asyncio.run(
                capture_market_catalog_into_repository(
                    adapter,
                    repository,
                    clock=lambda: _AVAILABLE,
                    snapshot_id_factory=lambda: "snapshot-invalid",
                )
            )

    assert _snapshot_rows(database) == [("snapshot-invalid", "FAILED", "ValueError")]


def test_network_error_records_failed_snapshot_then_reraises_original(tmp_path: Path) -> None:
    database = tmp_path / "catalog.db"
    error = TimeoutError("request timed out")
    adapter = _FakeAdapter(error=error)
    times = iter((_OBSERVED, _AVAILABLE))

    with SqliteMinuteCandleRepository(database) as repository:
        with pytest.raises(TimeoutError) as captured:
            asyncio.run(
                capture_market_catalog_into_repository(
                    adapter,
                    repository,
                    clock=lambda: next(times),
                    snapshot_id_factory=lambda: "snapshot-failed",
                )
            )

    assert captured.value is error
    assert _snapshot_rows(database) == [("snapshot-failed", "FAILED", "TimeoutError")]


def test_available_clock_before_received_at_records_failure_not_success(tmp_path: Path) -> None:
    database = tmp_path / "catalog.db"
    adapter = _FakeAdapter(_batch(_observation("KRW-BTC", 0)))
    early = _OBSERVED - timedelta(seconds=1)

    with SqliteMinuteCandleRepository(database) as repository:
        with pytest.raises(ValueError, match="observed_at"):
            asyncio.run(
                capture_market_catalog_into_repository(
                    adapter,
                    repository,
                    clock=lambda: early,
                    snapshot_id_factory=lambda: "snapshot-clock-error",
                )
            )

    assert _snapshot_rows(database) == [("snapshot-clock-error", "FAILED", "ValueError")]


def test_cancelled_error_is_not_recorded_as_failed_snapshot(tmp_path: Path) -> None:
    database = tmp_path / "catalog.db"
    adapter = _FakeAdapter(error=asyncio.CancelledError())

    with SqliteMinuteCandleRepository(database) as repository:
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(
                capture_market_catalog_into_repository(
                    adapter,
                    repository,
                    clock=lambda: _AVAILABLE,
                    snapshot_id_factory=lambda: "snapshot-cancelled",
                )
            )

    assert _snapshot_rows(database) == []


def test_resolver_never_backfills_before_first_success_and_is_point_in_time(
    tmp_path: Path,
) -> None:
    database = tmp_path / "catalog.db"
    pair = TradingPair("KRW-BTC", "비트코인", "Bitcoin", False)
    member = MarketCatalogSnapshotMemberInput(
        response_order=0,
        pair=pair,
        raw_payload={"market": pair.code, "market_event": {"warning": False}},
    )

    with SqliteMinuteCandleRepository(database) as repository:
        repository.record_market_catalog_snapshot(
            snapshot_id="snapshot-1",
            source="upbit",
            endpoint="/v1/market/all",
            request_parameters={"is_details": True},
            quote_currency="KRW",
            observed_at=_OBSERVED,
            available_at=_AVAILABLE,
            status=MarketCatalogSnapshotStatus.SUCCEEDED,
            members=(member,),
        )

    before = resolve_market_universe_as_of("KRW", _OBSERVED, database)
    available = resolve_market_universe_as_of("KRW", _AVAILABLE, database)

    assert before.status is MarketUniverseStatus.UNRESOLVED
    assert before.pairs == ()
    assert before.snapshot_id is None
    assert available.status is MarketUniverseStatus.AVAILABLE
    assert available.snapshot_id == "snapshot-1"
    assert available.observed_at == _OBSERVED
    assert available.available_at == _AVAILABLE
    assert available.pairs == (pair,)
    assert available.snapshot_age == timedelta(seconds=1)
