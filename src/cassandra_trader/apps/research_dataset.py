"""Read-only research diagnostics and deterministic, atomic dataset exports."""

import ctypes
import errno
import hashlib
import json
import os
import shutil
import sys
import tempfile
from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from decimal import Context, Inexact, Rounded, localcontext
from pathlib import Path
from typing import BinaryIO

from cassandra_trader.adapters.storage.sqlite import (
    MinuteCandleBackfillRunStatus,
    SqliteMinuteCandleRepository,
    StoredMinuteCandle,
    StoredMinuteCandleBackfillRun,
    StoredMinuteCandleSourceProvenance,
)
from cassandra_trader.apps.market_eligibility import evaluate_market_eligibility_as_of
from cassandra_trader.apps.minute_slots import resolve_minute_slots_from_repository
from cassandra_trader.domain.four_hour_bar import aggregate_resolved_four_hour_bar
from cassandra_trader.domain.market_eligibility import MarketEligibilityDecisionStatus
from cassandra_trader.domain.minute_slot import MinuteSlotStatus, ResolvedMinuteSlot
from cassandra_trader.domain.research_dataset import (
    RESEARCH_DATASET_ALGORITHM_VERSION,
    RESEARCH_DATASET_SCHEMA_VERSION,
    ResearchDatasetRequest,
)

_FOUR_HOURS = timedelta(hours=4)
_EVIDENCE_GRADE = "NON_PIT_EXPLORATORY"
_EVIDENCE_LIMITATIONS = [
    "EXPLICIT_MARKETS_NOT_HISTORICAL_UNIVERSE",
    "COMMON_CUTOFF_NOT_PER_DECISION_AVAILABILITY",
    "HISTORICAL_LISTING_AND_DELISTING_NOT_RECONSTRUCTED",
]


def inspect_research_dataset(
    request: ResearchDatasetRequest,
    database: str | Path,
) -> dict[str, object]:
    """Report coverage in the requested range without writing to the database."""

    _validate_request(request)
    with SqliteMinuteCandleRepository(database, read_only=True) as repository:
        return _build(request, database, repository)


def export_research_dataset(
    request: ResearchDatasetRequest,
    database: str | Path,
    output_directory: str | Path,
) -> dict[str, object]:
    """Publish a new four-file dataset; existing destinations are never replaced.

    Canonical minute values are aggregated with a fresh Decimal Context of
    precision 50 and traps for Inexact and Rounded. Unsupported precision fails
    the whole export, instead of depending on a caller's arithmetic context.
    Memory used for candle rows is bounded to one four-hour market interval.
    """

    _validate_request(request)
    destination = _validate_destination(database, output_directory)
    staging: Path | None = None
    try:
        # Open and validate the source before creating any export directories.
        with SqliteMinuteCandleRepository(database, read_only=True) as repository:
            destination.parent.mkdir(parents=True, exist_ok=True)
            staging = Path(
                tempfile.mkdtemp(prefix=f".{destination.name}.staging-", dir=destination.parent)
            )
            with (
                (staging / "bars.jsonl").open("wb") as bars,
                (staging / "lineage.jsonl").open("wb") as lineage,
            ):
                readiness = _build(
                    request,
                    database,
                    repository,
                    write_bar=lambda row: bars.write(_json_bytes(row)),
                    write_lineage=lambda row: lineage.write(_json_bytes(row)),
                )
                _sync_file(bars)
                _sync_file(lineage)
            _write_json(staging / "readiness.json", readiness)
            manifest = _manifest(request, readiness, staging)
            _write_json(staging / "manifest.json", manifest)
        _publish_new_directory(staging, destination)
        staging = None
        return manifest
    finally:
        if staging is not None:
            shutil.rmtree(staging)


def _validate_request(request: ResearchDatasetRequest) -> None:
    if not isinstance(request, ResearchDatasetRequest):
        raise ValueError("request must be a ResearchDatasetRequest")


def _build(
    request: ResearchDatasetRequest,
    database: str | Path,
    repository: SqliteMinuteCandleRepository,
    *,
    write_bar: Callable[[dict[str, object]], object] | None = None,
    write_lineage: Callable[[dict[str, object]], object] | None = None,
) -> dict[str, object]:
    eligibility = evaluate_market_eligibility_as_of(
        "KRW", request.as_of, database, request.maximum_snapshot_age, repository=repository
    )
    decisions = {decision.market: decision for decision in eligibility.decisions}
    markets: list[dict[str, object]] = []
    arithmetic = Context(prec=50)
    arithmetic.traps[Inexact] = True
    arithmetic.traps[Rounded] = True
    for market in request.markets:
        decision = decisions.get(market)
        catalog_reasons = [reason.value for reason in eligibility.reasons]
        if decision is None:
            if eligibility.snapshot_id is not None:
                catalog_reasons.append("MARKET_NOT_IN_CATALOG")
            catalog_status = "UNRESOLVED"
        else:
            catalog_reasons.extend(reason.value for reason in decision.reasons)
            catalog_status = decision.status.value
            if decision.is_warning and "WARNING" not in catalog_reasons:
                catalog_reasons.append("WARNING")
        catalog_reasons = sorted(set(catalog_reasons))
        summary: dict[str, object] = {
            "market": market,
            "expected_minutes": int(
                (request.end_time_utc - request.start_time_utc) // timedelta(minutes=1)
            ),
            "traded_minutes": 0,
            "no_trade_minutes": 0,
            "unresolved_minutes": 0,
            "missing_intervals": [],
            "first_candle_time_utc": None,
            "latest_candle_time_utc": None,
            "latest_available_at": None,
            "complete_bars": 0,
            "no_trade_bars": 0,
            "incomplete_bars": 0,
            "catalog_status": catalog_status,
            "catalog_reasons": catalog_reasons,
        }
        missing: list[dict[str, str]] = []
        latest_available: datetime | None = None
        start = request.start_time_utc
        while start < request.end_time_utc:
            end = start + _FOUR_HOURS
            slots = resolve_minute_slots_from_repository(
                market, start, end, request.as_of, repository
            )
            with localcontext(arithmetic):
                bar = aggregate_resolved_four_hour_bar(
                    market=market, start_time_utc=start, as_of=request.as_of, minute_slots=slots
                )
            canonical = repository.list_minute_candles_as_of(market, start, end, request.as_of)
            no_trade_intervals = _slot_intervals(slots, MinuteSlotStatus.NO_TRADE)
            evidence = _no_trade_evidence(
                repository, market, start, end, request.as_of, no_trade_intervals
            )
            for stored in canonical:
                if summary["first_candle_time_utc"] is None:
                    summary["first_candle_time_utc"] = _utc(stored.candle.start_time_utc)
                summary["latest_candle_time_utc"] = _utc(stored.candle.start_time_utc)
                latest_available = _latest(latest_available, stored.available_at)
            for run in evidence:
                latest_available = _latest(latest_available, run.available_at)
            for interval in _slot_intervals(slots, MinuteSlotStatus.UNRESOLVED):
                _append_interval(missing, interval[0], interval[1])
            for name, count in (
                ("traded_minutes", bar.traded_count),
                ("no_trade_minutes", bar.no_trade_count),
                ("unresolved_minutes", bar.unresolved_count),
                (f"{bar.status.value.lower()}_bars", 1),
            ):
                summary[name] += count
            if write_bar is not None:
                write_bar(
                    {
                        "market": market,
                        "start_time_utc": _utc(start),
                        "end_time_utc": _utc(end),
                        "status": bar.status.value,
                        "reason": None if bar.reason is None else bar.reason.value,
                        "expected_count": bar.expected_count,
                        "resolved_count": bar.resolved_count,
                        "traded_count": bar.traded_count,
                        "no_trade_count": bar.no_trade_count,
                        "unresolved_count": bar.unresolved_count,
                        "candle": None
                        if bar.candle is None
                        else {
                            name: str(getattr(bar.candle, name))
                            for name in (
                                "open",
                                "high",
                                "low",
                                "close",
                                "base_volume",
                                "quote_volume",
                            )
                        },
                    }
                )
            if write_lineage is not None:
                provenance = {
                    source.raw_record_id: source
                    for source in repository.list_minute_candle_source_provenance(
                        tuple(stored.source_raw_record_id for stored in canonical)
                    )
                }
                write_lineage(
                    {
                        "market": market,
                        "start_time_utc": _utc(start),
                        "end_time_utc": _utc(end),
                        "canonical_minutes": [
                            _canonical_lineage(stored, provenance.get(stored.source_raw_record_id))
                            for stored in canonical
                        ],
                        "no_trade_intervals": [
                            {"start_time_utc": _utc(left), "end_time_utc": _utc(right)}
                            for left, right in no_trade_intervals
                        ],
                        "no_trade_evidence": [_run_lineage(run) for run in evidence],
                    }
                )
            start = end
        summary["missing_intervals"] = missing
        summary["latest_available_at"] = _optional_utc(latest_available)
        summary["data_complete"] = summary["unresolved_minutes"] == 0
        summary["catalog_ready"] = catalog_status == MarketEligibilityDecisionStatus.ELIGIBLE
        summary["blockers"] = catalog_reasons + (
            ["UNRESOLVED_MINUTES"] if summary["unresolved_minutes"] else []
        )
        markets.append(summary)

    complete = all(item["data_complete"] for item in markets)
    return {
        **_metadata(request),
        "status": "COMPLETE" if complete else "INCOMPLETE",
        "data_complete": complete,
        "catalog_ready": all(item["catalog_ready"] for item in markets),
        "catalog": {
            "status": eligibility.status.value,
            "snapshot_id": eligibility.snapshot_id,
            "content_hash": eligibility.content_hash,
            "observed_at": _optional_utc(eligibility.observed_at),
            "available_at": _optional_utc(eligibility.available_at),
            "reasons": [reason.value for reason in eligibility.reasons],
            "scope": "COMMON_CUTOFF_DIAGNOSTIC_ONLY",
        },
        "markets": markets,
    }


def _no_trade_evidence(
    repository: SqliteMinuteCandleRepository,
    market: str,
    start: datetime,
    end: datetime,
    as_of: datetime,
    intervals: list[tuple[datetime, datetime]],
) -> tuple[StoredMinuteCandleBackfillRun, ...]:
    if not intervals:
        return ()
    runs = repository.list_minute_candle_backfill_runs(
        market, start, end, as_of, status=MinuteCandleBackfillRunStatus.SUCCEEDED
    )
    return tuple(
        run
        for run in runs
        if any(
            max(left, run.requested_start) < min(right, run.requested_end, run.observed_at)
            for left, right in intervals
        )
    )


def _canonical_lineage(
    stored: StoredMinuteCandle, provenance: StoredMinuteCandleSourceProvenance | None
) -> dict[str, object]:
    if provenance is None:
        raise ValueError(f"missing source provenance for raw record {stored.source_raw_record_id}")
    source = asdict(provenance)
    source["request_parameters"] = json.loads(source.pop("request_parameters_json"))
    for key, value in source.items():
        if isinstance(value, datetime):
            source[key] = _utc(value)
    return {
        "start_time_utc": _utc(stored.candle.start_time_utc),
        "end_time_utc": _utc(stored.candle.end_time_utc),
        "revision": stored.revision,
        "content_sha256": stored.content_sha256,
        "source_raw_record_id": stored.source_raw_record_id,
        "available_at": _utc(stored.available_at),
        "source": source,
    }


def _run_lineage(run: StoredMinuteCandleBackfillRun) -> dict[str, object]:
    return {
        "run_id": run.run_id,
        "market": run.market,
        "requested_start": _utc(run.requested_start),
        "requested_end": _utc(run.requested_end),
        "observed_at": _utc(run.observed_at),
        "available_at": _utc(run.available_at),
        "status": run.status.value,
    }


def _slot_intervals(
    slots: tuple[ResolvedMinuteSlot, ...], status: MinuteSlotStatus
) -> list[tuple[datetime, datetime]]:
    intervals: list[tuple[datetime, datetime]] = []
    for slot in slots:
        if slot.status is status:
            if intervals and intervals[-1][1] == slot.start_time_utc:
                intervals[-1] = (intervals[-1][0], slot.end_time_utc)
            else:
                intervals.append((slot.start_time_utc, slot.end_time_utc))
    return intervals


def _append_interval(intervals: list[dict[str, str]], start: datetime, end: datetime) -> None:
    if intervals and intervals[-1]["end_time_utc"] == _utc(start):
        intervals[-1]["end_time_utc"] = _utc(end)
    else:
        intervals.append({"start_time_utc": _utc(start), "end_time_utc": _utc(end)})


def _latest(previous: datetime | None, value: datetime) -> datetime:
    return value if previous is None else max(previous, value)


def _metadata(request: ResearchDatasetRequest) -> dict[str, object]:
    return {
        "schema_version": RESEARCH_DATASET_SCHEMA_VERSION,
        "algorithm_version": RESEARCH_DATASET_ALGORITHM_VERSION,
        "parameters": {
            "markets": list(request.markets),
            "start_time_utc": _utc(request.start_time_utc),
            "end_time_utc": _utc(request.end_time_utc),
            "as_of": _utc(request.as_of),
            "maximum_snapshot_age_seconds": request.maximum_snapshot_age.total_seconds(),
            "maximum_snapshot_age_microseconds": request.maximum_snapshot_age
            // timedelta(microseconds=1),
        },
        "selection_scope": "EXPLICIT_MARKETS",
        "evidence_grade": _EVIDENCE_GRADE,
        "confirmatory_ready": False,
        "evidence_limitations": list(_EVIDENCE_LIMITATIONS),
        "arithmetic_policy": "DECIMAL_PRECISION_50_TRAP_INEXACT_AND_ROUNDED",
    }


def _manifest(
    request: ResearchDatasetRequest, readiness: dict[str, object], directory: Path
) -> dict[str, object]:
    manifest = {
        **_metadata(request),
        "status": readiness["status"],
        "data_complete": readiness["data_complete"],
        "catalog_ready": readiness["catalog_ready"],
        "bar_count": len(request.markets)
        * int((request.end_time_utc - request.start_time_utc) // _FOUR_HOURS),
        "files": {
            filename: _file_fingerprint(directory / filename)
            for filename in ("bars.jsonl", "lineage.jsonl", "readiness.json")
        },
    }
    manifest["dataset_id"] = hashlib.sha256(_json_bytes(manifest)).hexdigest()
    return manifest


def _file_fingerprint(path: Path) -> dict[str, object]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as file:
        while chunk := file.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    return {"sha256": digest.hexdigest(), "size_bytes": size}


def _json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    ).encode("utf-8")


def _utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _optional_utc(value: datetime | None) -> str | None:
    return None if value is None else _utc(value)


def _write_json(path: Path, value: object) -> None:
    with path.open("wb") as file:
        file.write(_json_bytes(value))
        _sync_file(file)


def _sync_file(file: BinaryIO) -> None:
    file.flush()
    os.fsync(file.fileno())


def _validate_destination(database: str | Path, output_directory: str | Path) -> Path:
    source = Path(database).expanduser().resolve()
    source_alias = Path(os.path.abspath(Path(database).expanduser()))
    destination = Path(output_directory).expanduser().absolute()
    # Reject symlinks including dangling links before resolve can hide them.
    if any(part.is_symlink() for part in (destination, *destination.parents)):
        raise ValueError("output_directory must not contain symlinks")
    destination = destination.resolve()
    if destination.exists():
        raise FileExistsError(f"output_directory already exists: {destination}")
    reserved = {
        Path(str(path) + suffix)
        for path in (source, source_alias)
        for suffix in ("", "-wal", "-shm", "-journal")
    }
    if any(
        destination == path or path in destination.parents or destination in path.parents
        for path in reserved
    ):
        raise ValueError("output_directory conflicts with the source database")
    return destination


def _publish_new_directory(staging: Path, destination: Path) -> None:
    """Use an atomic no-replace rename, including races with empty directories."""

    if os.name == "nt":
        # Windows rename rejects every existing target, even an empty directory.
        os.rename(staging, destination)
        return
    if not sys.platform.startswith("linux"):
        raise OSError("atomic no-replace dataset publication requires Linux or Windows")
    libc = ctypes.CDLL(None, use_errno=True)
    rename = getattr(libc, "renameat2", None)
    if rename is None:
        raise OSError("atomic no-replace dataset publication requires renameat2 support")
    rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    rename.restype = ctypes.c_int
    if rename(-100, os.fsencode(staging), -100, os.fsencode(destination), 1) != 0:
        error = ctypes.get_errno()
        if error == errno.EEXIST:
            raise FileExistsError(error, os.strerror(error), str(destination))
        raise OSError(error, os.strerror(error), str(destination))
