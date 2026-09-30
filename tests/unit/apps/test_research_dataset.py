"""Input contracts and failure atomicity for exploratory research exports."""

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal, DecimalException
from pathlib import Path

import pytest

from cassandra_trader.adapters.storage.sqlite import (
    MinuteCandleBackfillRunStatus,
    RawMinuteCandleInput,
    SqliteMinuteCandleRepository,
)
from cassandra_trader.apps import research_dataset
from cassandra_trader.apps.research_dataset import export_research_dataset
from cassandra_trader.domain.candle import MinuteCandle
from cassandra_trader.domain.research_dataset import ResearchDatasetRequest

_START = datetime(2026, 8, 22, 8, tzinfo=UTC)
_END = _START + timedelta(hours=4)


def _request(**overrides: object) -> ResearchDatasetRequest:
    values = {
        "markets": ("KRW-BTC",),
        "start_time_utc": _START,
        "end_time_utc": _END,
        "as_of": _END,
        "maximum_snapshot_age": timedelta(hours=1),
    }
    return ResearchDatasetRequest(**(values | overrides))


def test_request_normalizes_explicit_selection_and_is_immutable() -> None:
    request = _request(markets=(" krw-eth ", "KRW-BTC", "krw-eth"))
    assert request.markets == ("KRW-BTC", "KRW-ETH")
    with pytest.raises(AttributeError):
        request.markets = ("KRW-ADA",)


@pytest.mark.parametrize(
    "overrides",
    [
        {"markets": ()},
        {"markets": ["KRW-BTC"]},
        {"markets": ("BTC-ETH",)},
        {"markets": ("KRW-ß",)},
        {"markets": ("KRW-ＢＴＣ",)},
        {"markets": ("KRW-BTC/USDT",)},
        {"start_time_utc": _START.replace(tzinfo=None)},
        {"as_of": _END.astimezone(timezone(timedelta(hours=9)))},
        {"start_time_utc": _START + timedelta(minutes=1)},
        {"end_time_utc": _END + timedelta(microseconds=1)},
        {"end_time_utc": _START},
        {"as_of": _END - timedelta(microseconds=1)},
        {"maximum_snapshot_age": timedelta(0)},
    ],
)
def test_invalid_request_is_rejected_before_any_io(overrides: dict) -> None:
    with pytest.raises(ValueError):
        _request(**overrides)


def test_precision_failure_leaves_no_published_or_staging_dataset(tmp_path: Path) -> None:
    database = tmp_path / "source.db"
    output = tmp_path / "dataset"
    with SqliteMinuteCandleRepository(database) as repository:
        candle = MinuteCandle(
            market="KRW-BTC",
            start_time_utc=_START,
            end_time_utc=_START + timedelta(minutes=1),
            open=Decimal("10"),
            high=Decimal("10"),
            low=Decimal("10"),
            close=Decimal("10"),
            base_volume=Decimal("1." + "1" * 60),
            quote_volume=Decimal("10"),
        )
        repository.ingest_batch(
            batch_id="too-precise",
            source="upbit",
            endpoint="/v1/candles/minutes/1",
            request_parameters={"market": "KRW-BTC"},
            received_at=_END,
            ingested_at=_END,
            records=(
                RawMinuteCandleInput(
                    response_order=0,
                    market="KRW-BTC",
                    source_candle_time=_START.isoformat(),
                    raw_payload={"market": "KRW-BTC"},
                    candle=candle,
                ),
            ),
        )
        repository.record_minute_candle_backfill_run(
            run_id="coverage",
            market="KRW-BTC",
            requested_start=_START,
            requested_end=_END,
            observed_at=_END,
            available_at=_END,
            status=MinuteCandleBackfillRunStatus.SUCCEEDED,
        )
    source = database.read_bytes()

    with pytest.raises(DecimalException):
        export_research_dataset(_request(), database, output)

    assert not output.exists()
    assert not list(tmp_path.glob(".dataset.staging-*"))
    assert database.read_bytes() == source


def test_concurrent_empty_destination_is_preserved_and_staging_removed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "source.db"
    output = tmp_path / "dataset"
    with SqliteMinuteCandleRepository(database):
        pass
    publish = research_dataset._publish_new_directory
    created_stat = None

    def concurrent_publish(staging: Path, destination: Path) -> None:
        nonlocal created_stat
        destination.mkdir()
        created_stat = destination.stat()
        publish(staging, destination)

    monkeypatch.setattr(research_dataset, "_publish_new_directory", concurrent_publish)
    with pytest.raises(FileExistsError):
        export_research_dataset(_request(), database, output)

    assert created_stat is not None
    assert output.stat().st_ino == created_stat.st_ino
    assert list(output.iterdir()) == []
    assert not list(tmp_path.glob(".dataset.staging-*"))


def test_dangling_output_symlink_is_rejected_without_touching_target(tmp_path: Path) -> None:
    database = tmp_path / "source.db"
    output = tmp_path / "dataset"
    target = tmp_path / "uncreated-target"
    with SqliteMinuteCandleRepository(database):
        pass
    try:
        output.symlink_to(target, target_is_directory=True)
    except OSError:
        pytest.skip("symlink creation is unavailable")

    with pytest.raises(ValueError, match="symlinks"):
        export_research_dataset(_request(), database, output)

    assert output.is_symlink()
    assert not target.exists()


@pytest.mark.parametrize("suffix", ["-wal", "-shm", "-journal"])
def test_source_sidecar_names_cannot_be_export_destinations(tmp_path: Path, suffix: str) -> None:
    database = tmp_path / "source.db"
    with SqliteMinuteCandleRepository(database):
        pass
    output = tmp_path / f"source.db{suffix}"
    with pytest.raises((ValueError, FileExistsError)):
        export_research_dataset(_request(), database, output)
    assert not output.is_dir()


@pytest.mark.parametrize("suffix", ["-wal", "-shm", "-journal"])
@pytest.mark.parametrize("through_alias", [False, True])
def test_source_sidecar_descendants_never_create_reserved_directories(
    tmp_path: Path, suffix: str, through_alias: bool
) -> None:
    database = tmp_path / "source.db"
    with SqliteMinuteCandleRepository(database):
        pass
    source_argument = database
    if through_alias:
        source_argument = tmp_path / "alias.db"
        try:
            source_argument.symlink_to(database)
        except OSError:
            pytest.skip("symlink creation is unavailable")
    reserved = Path(str(source_argument) + suffix)
    assert not reserved.exists()

    with pytest.raises(ValueError, match="conflicts"):
        export_research_dataset(_request(), source_argument, reserved / "exports" / "new")

    assert not reserved.exists()


def test_snapshot_age_identity_preserves_microseconds_beyond_float_precision(
    tmp_path: Path,
) -> None:
    database = tmp_path / "source.db"
    with SqliteMinuteCandleRepository(database):
        pass
    first_age = timedelta(days=999999998, microseconds=1)
    second_age = first_age + timedelta(microseconds=1)
    assert first_age.total_seconds() == second_age.total_seconds()

    first = export_research_dataset(
        _request(maximum_snapshot_age=first_age), database, tmp_path / "first"
    )
    second = export_research_dataset(
        _request(maximum_snapshot_age=second_age), database, tmp_path / "second"
    )

    first_exact = first["parameters"]["maximum_snapshot_age_microseconds"]
    second_exact = second["parameters"]["maximum_snapshot_age_microseconds"]
    assert first_exact == 999999998 * 86400 * 1000000 + 1
    assert second_exact == first_exact + 1
    assert first["dataset_id"] != second["dataset_id"]
