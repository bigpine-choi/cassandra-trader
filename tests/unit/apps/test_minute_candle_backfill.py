import asyncio
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from cassandra_trader.adapters.storage.sqlite import (
    MinuteCandleBackfillRunStatus,
    SqliteMinuteCandleRepository,
)
from cassandra_trader.adapters.upbit.public_data import (
    MinuteCandleObservation,
    MinuteCandleResponseBatch,
)
from cassandra_trader.apps.minute_candle_backfill import (
    backfill_minute_candles_with_fetcher,
)
from cassandra_trader.domain.candle import MinuteCandle

_START = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
_END = _START + timedelta(hours=4)
_OBSERVED_AT = datetime(2025, 1, 2, tzinfo=UTC)


class FakePageFetcher:
    def __init__(self, pages: list[tuple[MinuteCandleObservation, ...]]) -> None:
        self._pages = iter(pages)
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
        observations = next(self._pages)
        return MinuteCandleResponseBatch(
            endpoint="/v1/candles/minutes/1",
            request_parameters={
                "market": market,
                "count": count,
                "to": to.isoformat().replace("+00:00", "Z"),
            },
            received_at=_OBSERVED_AT,
            observations=observations,
        )


def _observation(start: datetime, response_order: int) -> MinuteCandleObservation:
    start_text = start.isoformat().replace("+00:00", "Z")
    candle = MinuteCandle(
        market="KRW-BTC",
        start_time_utc=start,
        end_time_utc=start + timedelta(minutes=1),
        open=Decimal("10"),
        high=Decimal("11"),
        low=Decimal("9"),
        close=Decimal("10.5"),
        base_volume=Decimal("1.25"),
        quote_volume=Decimal("12.5"),
    )
    return MinuteCandleObservation(
        response_order=response_order,
        market="KRW-BTC",
        source_candle_time=start_text,
        raw_payload={"market": "KRW-BTC", "candle_date_time_utc": start_text},
        candle=candle,
    )


def _page(starts: list[datetime]) -> tuple[MinuteCandleObservation, ...]:
    return tuple(_observation(start, order) for order, start in enumerate(starts))


def _run_backfill(
    database: Path,
    fetcher: FakePageFetcher,
    *,
    start: datetime = _START,
    end: datetime = _END,
    observed_at: datetime = _OBSERVED_AT,
):
    return asyncio.run(
        backfill_minute_candles_with_fetcher(
            "KRW-BTC",
            start,
            end,
            database,
            fetch_page=fetcher,
            observed_at=observed_at,
        )
    )


def test_backfill_collects_two_pages_and_moves_cursor(tmp_path: Path) -> None:
    newest_page = [_START + timedelta(minutes=offset) for offset in range(40, 240)]
    oldest_page = [_START + timedelta(minutes=offset) for offset in range(40)]
    newest_page = newest_page[::2] + newest_page[1::2]
    oldest_page.reverse()
    fetcher = FakePageFetcher([_page(newest_page), _page(oldest_page)])

    result = _run_backfill(tmp_path / "candles.db", fetcher)

    assert [call[1] for call in fetcher.calls] == [200, 200]
    assert [call[3] for call in fetcher.calls] == [_END, _START + timedelta(minutes=40)]
    assert result.pages_fetched == 2
    assert result.raw_received == 240
    assert result.canonical_new == 240
    assert result.canonical_revised == 0
    assert result.canonical_unchanged == 0
    assert result.earliest_candle_time == _START
    assert result.latest_candle_time == _END - timedelta(minutes=1)
    assert result.requested_start == _START
    assert result.requested_end == _END

    with SqliteMinuteCandleRepository(tmp_path / "candles.db") as repository:
        runs = repository.list_minute_candle_backfill_runs(
            "KRW-BTC",
            _START,
            _END,
            datetime(2100, 1, 1, tzinfo=UTC),
        )
    assert len(runs) == 1
    assert runs[0].status is MinuteCandleBackfillRunStatus.SUCCEEDED
    assert runs[0].requested_start == _START
    assert runs[0].requested_end == _END
    assert runs[0].observed_at == _OBSERVED_AT
    assert runs[0].error_type is None


def test_backfill_applies_exact_half_open_canonical_range(tmp_path: Path) -> None:
    starts = [_END]
    starts.extend(_START + timedelta(minutes=offset) for offset in reversed(range(240)))
    starts.append(_START - timedelta(minutes=1))
    fetcher = FakePageFetcher([_page(starts)])

    result = _run_backfill(tmp_path / "candles.db", fetcher)

    assert result.raw_received == 242
    assert result.canonical_new == 240
    assert result.earliest_candle_time == _START
    assert result.latest_candle_time == _END - timedelta(minutes=1)


def test_page_boundary_duplicate_does_not_duplicate_canonical(tmp_path: Path) -> None:
    boundary = _START + timedelta(minutes=40)
    newest_page = [_START + timedelta(minutes=offset) for offset in range(40, 240)]
    oldest_page = [boundary]
    oldest_page.extend(_START + timedelta(minutes=offset) for offset in range(40))
    database = tmp_path / "candles.db"

    result = _run_backfill(
        database,
        FakePageFetcher([_page(newest_page), _page(oldest_page)]),
    )

    assert result.raw_received == 241
    assert result.canonical_new == 240
    assert result.canonical_unchanged == 1
    with SqliteMinuteCandleRepository(database) as repository:
        stored = repository.list_minute_candles_as_of(
            "KRW-BTC",
            _START,
            _END,
            datetime(2100, 1, 1, tzinfo=UTC),
        )
    assert len(stored) == 240


def test_repeated_range_is_canonical_unchanged(tmp_path: Path) -> None:
    database = tmp_path / "candles.db"
    starts = [_START + timedelta(minutes=offset) for offset in reversed(range(240))]

    first = _run_backfill(database, FakePageFetcher([_page(starts)]))
    second = _run_backfill(database, FakePageFetcher([_page(starts)]))

    assert first.canonical_new == 240
    assert second.canonical_new == 0
    assert second.canonical_revised == 0
    assert second.canonical_unchanged == 240


def test_empty_response_returns_structured_result(tmp_path: Path) -> None:
    result = _run_backfill(tmp_path / "candles.db", FakePageFetcher([()]))

    assert result.pages_fetched == 1
    assert result.raw_received == 0
    assert result.canonical_new == 0
    assert result.canonical_revised == 0
    assert result.canonical_unchanged == 0
    assert result.earliest_candle_time is None
    assert result.latest_candle_time is None


def test_cursor_that_does_not_move_backward_fails(tmp_path: Path) -> None:
    fetcher = FakePageFetcher([_page([_END])])

    with pytest.raises(RuntimeError, match="cursor did not move backward"):
        _run_backfill(tmp_path / "candles.db", fetcher)


def test_second_page_failure_records_failed_run_and_reraises_original_error(
    tmp_path: Path,
) -> None:
    database = tmp_path / "candles.db"
    failure = TimeoutError("second page timed out")
    call_count = 0

    async def fetch_page(
        market: str,
        count: int,
        *,
        as_of: datetime,
        to: datetime,
    ) -> MinuteCandleResponseBatch:
        nonlocal call_count
        call_count += 1

        if call_count == 2:
            raise failure

        return MinuteCandleResponseBatch(
            endpoint="/v1/candles/minutes/1",
            request_parameters={"market": market, "count": count},
            received_at=_OBSERVED_AT,
            observations=_page([_START + timedelta(minutes=offset) for offset in range(40, 240)]),
        )

    with pytest.raises(TimeoutError) as caught:
        asyncio.run(
            backfill_minute_candles_with_fetcher(
                "KRW-BTC",
                _START,
                _END,
                database,
                fetch_page=fetch_page,
                observed_at=_OBSERVED_AT,
            )
        )

    assert caught.value is failure
    with SqliteMinuteCandleRepository(database) as repository:
        runs = repository.list_minute_candle_backfill_runs(
            "KRW-BTC",
            _START,
            _END,
            datetime(2100, 1, 1, tzinfo=UTC),
        )
        stored = repository.list_minute_candles_as_of(
            "KRW-BTC",
            _START,
            _END,
            datetime(2100, 1, 1, tzinfo=UTC),
        )

    assert len(stored) == 200
    assert len(runs) == 1
    assert runs[0].status is MinuteCandleBackfillRunStatus.FAILED
    assert runs[0].error_type == "TimeoutError"
    assert runs[0].error_message == "second page timed out"


def test_cancelled_backfill_propagates_control_flow_without_terminal_evidence(
    tmp_path: Path,
) -> None:
    database = tmp_path / "candles.db"

    async def cancelled_fetch(*args, **kwargs) -> MinuteCandleResponseBatch:
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            backfill_minute_candles_with_fetcher(
                "KRW-BTC",
                _START,
                _END,
                database,
                fetch_page=cancelled_fetch,
                observed_at=_OBSERVED_AT,
            )
        )

    with SqliteMinuteCandleRepository(database) as repository:
        runs = repository.list_minute_candle_backfill_runs(
            "KRW-BTC",
            _START,
            _END,
            datetime(2100, 1, 1, tzinfo=UTC),
        )
    assert runs == ()


def test_missing_minutes_are_not_synthesized(tmp_path: Path) -> None:
    missing = _START + timedelta(minutes=42)
    starts = [
        _START + timedelta(minutes=offset)
        for offset in reversed(range(240))
        if _START + timedelta(minutes=offset) != missing
    ]
    database = tmp_path / "candles.db"

    result = _run_backfill(database, FakePageFetcher([_page(starts)]))

    assert result.raw_received == 239
    assert result.canonical_new == 239
    with SqliteMinuteCandleRepository(database) as repository:
        stored = repository.list_minute_candles_as_of(
            "KRW-BTC",
            _START,
            _END,
            datetime(2100, 1, 1, tzinfo=UTC),
        )
    assert len(stored) == 239
    assert missing not in {item.candle.start_time_utc for item in stored}


def test_incomplete_future_minutes_are_not_canonicalized(tmp_path: Path) -> None:
    end = _START + timedelta(minutes=4)
    starts = [_START + timedelta(minutes=offset) for offset in reversed(range(4))]
    fetcher = FakePageFetcher([_page(starts)])

    result = _run_backfill(
        tmp_path / "candles.db",
        fetcher,
        end=end,
        observed_at=_START + timedelta(minutes=2),
    )

    assert result.raw_received == 4
    assert result.canonical_new == 2
    assert result.latest_candle_time == _START + timedelta(minutes=1)


@pytest.mark.parametrize(
    ("start", "end", "message"),
    [
        (_START.replace(tzinfo=None), _END, "start must be timezone-aware UTC"),
        (
            _START.astimezone(timezone(timedelta(hours=9))),
            _END,
            "start must be timezone-aware UTC",
        ),
        (_START + timedelta(seconds=1), _END, "start must align"),
        (_START, _END + timedelta(microseconds=1), "end must align"),
        (_END, _START, "start must be before end"),
        (_START, _START, "start must be before end"),
    ],
)
def test_backfill_rejects_invalid_range(
    tmp_path: Path,
    start: datetime,
    end: datetime,
    message: str,
) -> None:
    fetcher = FakePageFetcher([()])

    with pytest.raises(ValueError, match=message):
        _run_backfill(tmp_path / "candles.db", fetcher, start=start, end=end)

    assert fetcher.calls == []
