import asyncio
import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from cassandra_trader.adapters.storage.sqlite import SqliteMinuteCandleRepository
from cassandra_trader.adapters.upbit.public_data import (
    MinuteCandleObservation,
    MinuteCandleResponseBatch,
)
from cassandra_trader.apps.minute_reconciliation import (
    MinuteReconciliationStatus,
    reconcile_completed_minute,
)
from cassandra_trader.domain.candle import MinuteCandle
from cassandra_trader.domain.public_stream import MinuteCandleUpdate, StreamType

_START = datetime(2026, 8, 22, 17, 32, tzinfo=UTC)
_END = _START + timedelta(minutes=1)
_OBSERVED_AT = _END + timedelta(seconds=3)
_INGESTED_AT = _END + timedelta(seconds=4)


def _observation(
    start: datetime,
    *,
    close: str = "10.5",
    response_order: int = 0,
) -> MinuteCandleObservation:
    candle = MinuteCandle(
        market="KRW-BTC",
        start_time_utc=start,
        end_time_utc=start + timedelta(minutes=1),
        open=Decimal("10"),
        high=Decimal("20"),
        low=Decimal("5"),
        close=Decimal(close),
        base_volume=Decimal("1.25"),
        quote_volume=Decimal("12.5"),
    )
    start_text = start.isoformat().replace("+00:00", "Z")
    return MinuteCandleObservation(
        response_order=response_order,
        market="KRW-BTC",
        source_candle_time=start_text,
        raw_payload={
            "market": "KRW-BTC",
            "candle_date_time_utc": start_text,
            "trade_price": close,
        },
        candle=candle,
    )


def _batch(
    observations: tuple[MinuteCandleObservation, ...],
    *,
    received_at: datetime = _OBSERVED_AT,
) -> MinuteCandleResponseBatch:
    return MinuteCandleResponseBatch(
        endpoint="/v1/candles/minutes/1",
        request_parameters={
            "market": "KRW-BTC",
            "count": 1,
            "to": _END.isoformat().replace("+00:00", "Z"),
        },
        received_at=received_at,
        observations=observations,
    )


class FakeFetcher:
    def __init__(self, batches: list[MinuteCandleResponseBatch]) -> None:
        self.batches = iter(batches)
        self.calls: list[tuple[str, int, datetime, datetime]] = []

    async def __call__(
        self,
        market: str,
        count: int,
        *,
        as_of: datetime,
        to: datetime,
    ) -> MinuteCandleResponseBatch:
        self.calls.append((market, count, as_of, to))
        return next(self.batches)


def _reconcile(
    repository: SqliteMinuteCandleRepository,
    fetcher: FakeFetcher,
):
    return asyncio.run(
        reconcile_completed_minute(
            "KRW-BTC",
            _START,
            repository,
            fetcher,
            _OBSERVED_AT,
            ingested_at=_INGESTED_AT,
        )
    )


def test_exact_rest_candle_is_confirmed_and_persisted(tmp_path: Path) -> None:
    fetcher = FakeFetcher([_batch((_observation(_START),))])

    with SqliteMinuteCandleRepository(tmp_path / "candles.db") as repository:
        result = _reconcile(repository, fetcher)
        stored = repository.get_current_minute_candle("KRW-BTC", _START)

    assert fetcher.calls == [("KRW-BTC", 1, _OBSERVED_AT, _END)]
    assert result.status is MinuteReconciliationStatus.CONFIRMED
    assert result.raw_received == 1
    assert result.canonical_new == 1
    assert result.canonical_revised == 0
    assert result.canonical_unchanged == 0
    assert result.available_at == _INGESTED_AT
    assert stored is not None
    assert stored.candle.close == Decimal("10.5")


def test_empty_rest_response_is_not_returned(tmp_path: Path) -> None:
    fetcher = FakeFetcher([_batch(())])

    with SqliteMinuteCandleRepository(tmp_path / "candles.db") as repository:
        result = _reconcile(repository, fetcher)
        stored = repository.get_current_minute_candle("KRW-BTC", _START)

    assert result.status is MinuteReconciliationStatus.NOT_RETURNED
    assert result.raw_received == 0
    assert result.canonical_new == 0
    assert stored is None


def test_older_rest_candle_is_raw_only_and_target_is_not_synthesized(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"
    older_start = _START - timedelta(minutes=1)
    fetcher = FakeFetcher([_batch((_observation(older_start),))])

    with SqliteMinuteCandleRepository(database) as repository:
        result = _reconcile(repository, fetcher)
        target = repository.get_current_minute_candle("KRW-BTC", _START)
        older = repository.get_current_minute_candle("KRW-BTC", older_start)

    with sqlite3.connect(database) as connection:
        raw_count = connection.execute("SELECT COUNT(*) FROM raw_minute_candles").fetchone()[0]

    assert result.status is MinuteReconciliationStatus.NOT_RETURNED
    assert result.raw_received == 1
    assert target is None
    assert older is None
    assert raw_count == 1


def test_websocket_values_do_not_override_authoritative_rest_candle(tmp_path: Path) -> None:
    provisional = MinuteCandleUpdate(
        market="KRW-BTC",
        start_time_utc=_START,
        end_time_utc=_END,
        open=Decimal("100"),
        high=Decimal("120"),
        low=Decimal("90"),
        close=Decimal("110"),
        base_volume=Decimal("99"),
        quote_volume=Decimal("9999"),
        event_time_utc=_END - timedelta(milliseconds=1),
        received_at=_END,
        stream_type=StreamType.REALTIME,
    )
    fetcher = FakeFetcher([_batch((_observation(_START, close="10.75"),))])

    with SqliteMinuteCandleRepository(tmp_path / "candles.db") as repository:
        result = _reconcile(repository, fetcher)
        stored = repository.get_current_minute_candle("KRW-BTC", _START)

    assert provisional.close == Decimal("110")
    assert result.status is MinuteReconciliationStatus.CONFIRMED
    assert stored is not None
    assert stored.candle.close == Decimal("10.75")


def test_repeat_is_unchanged_and_modified_rest_content_creates_revision(
    tmp_path: Path,
) -> None:
    fetcher = FakeFetcher(
        [
            _batch((_observation(_START, close="10.5"),)),
            _batch((_observation(_START, close="10.5"),)),
            _batch((_observation(_START, close="11.5"),)),
        ]
    )

    with SqliteMinuteCandleRepository(tmp_path / "candles.db") as repository:
        first = _reconcile(repository, fetcher)
        repeated = _reconcile(repository, fetcher)
        modified = _reconcile(repository, fetcher)
        revisions = repository.list_minute_candle_revisions("KRW-BTC", _START)

    assert first.canonical_new == 1
    assert repeated.canonical_unchanged == 1
    assert repeated.canonical_revised == 0
    assert modified.canonical_revised == 1
    assert [revision.revision for revision in revisions] == [1, 2]
    assert [revision.candle.close for revision in revisions] == [
        Decimal("10.5"),
        Decimal("11.5"),
    ]


def test_default_available_at_is_captured_after_rest_receive(tmp_path: Path) -> None:
    received_at = datetime.now(UTC)
    fetcher = FakeFetcher([_batch((_observation(_START),), received_at=received_at)])

    with SqliteMinuteCandleRepository(tmp_path / "candles.db") as repository:
        result = asyncio.run(
            reconcile_completed_minute(
                "KRW-BTC",
                _START,
                repository,
                fetcher,
                _OBSERVED_AT,
            )
        )

    assert result.available_at >= received_at
