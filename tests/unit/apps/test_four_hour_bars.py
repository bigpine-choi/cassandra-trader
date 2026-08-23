from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from cassandra_trader.adapters.storage.sqlite import (
    MinuteCandleBackfillRunStatus,
    RawMinuteCandleInput,
    SqliteMinuteCandleRepository,
)
from cassandra_trader.apps.four_hour_bars import derive_four_hour_bar
from cassandra_trader.domain.candle import MinuteCandle
from cassandra_trader.domain.four_hour_bar import FourHourBarStatus

_START = datetime(2026, 8, 22, 8, 0, tzinfo=UTC)
_END = _START + timedelta(hours=4)
_EARLY = _END + timedelta(hours=1)
_LATE = _END + timedelta(hours=2)


def _candle(*, close: str = "10.5") -> MinuteCandle:
    return MinuteCandle(
        market="KRW-BTC",
        start_time_utc=_START,
        end_time_utc=_START + timedelta(minutes=1),
        open=Decimal("10"),
        high=Decimal("12"),
        low=Decimal("9"),
        close=Decimal(close),
        base_volume=Decimal("1"),
        quote_volume=Decimal("10.5"),
    )


def _ingest(
    repository: SqliteMinuteCandleRepository,
    *,
    batch_id: str,
    candle: MinuteCandle,
    available_at: datetime,
) -> None:
    start_text = candle.start_time_utc.isoformat().replace("+00:00", "Z")
    repository.ingest_batch(
        batch_id=batch_id,
        source="upbit",
        endpoint="/v1/candles/minutes/1",
        request_parameters={"market": candle.market, "count": 1},
        received_at=available_at,
        ingested_at=available_at,
        records=(
            RawMinuteCandleInput(
                response_order=0,
                market=candle.market,
                source_candle_time=start_text,
                raw_payload={"market": candle.market, "candle_date_time_utc": start_text},
                candle=candle,
            ),
        ),
    )


def _evidence(
    repository: SqliteMinuteCandleRepository,
    *,
    status: MinuteCandleBackfillRunStatus,
    available_at: datetime,
) -> None:
    repository.record_minute_candle_backfill_run(
        run_id=f"run-{status.value.lower()}",
        market="KRW-BTC",
        requested_start=_START,
        requested_end=_END,
        observed_at=_END,
        available_at=available_at,
        status=status,
        error_type="TimeoutError" if status is MinuteCandleBackfillRunStatus.FAILED else None,
        error_message="request timed out"
        if status is MinuteCandleBackfillRunStatus.FAILED
        else None,
    )


def test_derive_all_no_trade_from_successful_resolved_range(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"

    with SqliteMinuteCandleRepository(database) as repository:
        _evidence(
            repository,
            status=MinuteCandleBackfillRunStatus.SUCCEEDED,
            available_at=_EARLY,
        )

    result = derive_four_hour_bar("KRW-BTC", _START, _EARLY, database)

    assert result.status is FourHourBarStatus.NO_TRADE
    assert result.resolved_count == 240
    assert result.no_trade_count == 240
    assert result.candle is None


def test_later_canonical_changes_only_later_no_trade_slot(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"

    with SqliteMinuteCandleRepository(database) as repository:
        _evidence(
            repository,
            status=MinuteCandleBackfillRunStatus.SUCCEEDED,
            available_at=_EARLY,
        )
        _ingest(repository, batch_id="later-canonical", candle=_candle(), available_at=_LATE)

    earlier = derive_four_hour_bar("KRW-BTC", _START, _EARLY, database)
    later = derive_four_hour_bar("KRW-BTC", _START, _LATE, database)

    assert earlier.status is FourHourBarStatus.NO_TRADE
    assert earlier.traded_count == 0
    assert earlier.no_trade_count == 240
    assert later.status is FourHourBarStatus.COMPLETE
    assert later.traded_count == 1
    assert later.no_trade_count == 239
    assert later.candle is not None
    assert later.candle.open == Decimal("10")
    assert later.candle.close == Decimal("10.5")


def test_failed_evidence_leaves_partial_canonical_range_incomplete(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"

    with SqliteMinuteCandleRepository(database) as repository:
        _ingest(repository, batch_id="partial", candle=_candle(), available_at=_EARLY)
        _evidence(
            repository,
            status=MinuteCandleBackfillRunStatus.FAILED,
            available_at=_EARLY,
        )

    result = derive_four_hour_bar("KRW-BTC", _START, _EARLY, database)

    assert result.status is FourHourBarStatus.INCOMPLETE
    assert result.traded_count == 1
    assert result.no_trade_count == 0
    assert result.unresolved_count == 239
    assert result.unresolved_minute_starts[0] == _START + timedelta(minutes=1)
    assert result.unresolved_minute_starts[-1] == _END - timedelta(minutes=1)
    assert result.candle is None


def test_later_revision_changes_only_later_as_of_four_hour_result(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"

    with SqliteMinuteCandleRepository(database) as repository:
        _evidence(
            repository,
            status=MinuteCandleBackfillRunStatus.SUCCEEDED,
            available_at=_EARLY,
        )
        _ingest(repository, batch_id="revision-1", candle=_candle(), available_at=_EARLY)
        revised = replace(_candle(), close=Decimal("11"))
        _ingest(repository, batch_id="revision-2", candle=revised, available_at=_LATE)

    earlier = derive_four_hour_bar("KRW-BTC", _START, _EARLY, database)
    later = derive_four_hour_bar("KRW-BTC", _START, _LATE, database)

    assert earlier.status is FourHourBarStatus.COMPLETE
    assert earlier.candle is not None
    assert earlier.candle.close == Decimal("10.5")
    assert later.status is FourHourBarStatus.COMPLETE
    assert later.candle is not None
    assert later.candle.close == Decimal("11")
    assert earlier.traded_count == later.traded_count == 1
    assert earlier.no_trade_count == later.no_trade_count == 239
