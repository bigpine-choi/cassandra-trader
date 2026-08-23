"""Capture append-only market catalogs and resolve point-in-time universes."""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

from upbit import AsyncUpbit

from cassandra_trader.adapters.storage.sqlite import (
    MarketCatalogSnapshotMemberInput,
    MarketCatalogSnapshotStatus,
    SqliteMinuteCandleRepository,
    StoredMarketCatalogSnapshot,
)
from cassandra_trader.adapters.upbit.public_data import (
    TradingPairObservation,
    UpbitPublicDataAdapter,
)
from cassandra_trader.domain.market import (
    MarketUniverseResult,
    MarketUniverseStatus,
    TradingPair,
    normalize_quote_currency,
)

type Clock = Callable[[], datetime]
type SnapshotIdFactory = Callable[[], str]


@dataclass(frozen=True, slots=True)
class MarketCatalogCaptureResult:
    """Metadata for one successfully persisted complete catalog snapshot."""

    snapshot_id: str
    quote_currency: str
    observed_at: datetime
    available_at: datetime
    market_count: int
    content_hash: str
    pairs: tuple[TradingPair, ...]


async def capture_krw_market_catalog(
    database: str | Path,
    *,
    clock: Clock | None = None,
    snapshot_id_factory: SnapshotIdFactory | None = None,
) -> MarketCatalogCaptureResult:
    """Fetch and atomically persist one current Upbit KRW market snapshot."""

    effective_clock = clock or _utc_now

    async with AsyncUpbit(
        environment="kr",
        timeout=10.0,
        max_retries=2,
    ) as client:
        adapter = UpbitPublicDataAdapter(
            client.trading_pairs.list,
            clock=effective_clock,
        )

        with SqliteMinuteCandleRepository(database) as repository:
            return await capture_market_catalog_into_repository(
                adapter,
                repository,
                quote_currency="KRW",
                clock=effective_clock,
                snapshot_id_factory=snapshot_id_factory,
            )


async def capture_market_catalog_into_repository(
    adapter: UpbitPublicDataAdapter,
    repository: SqliteMinuteCandleRepository,
    *,
    quote_currency: str = "KRW",
    clock: Clock | None = None,
    snapshot_id_factory: SnapshotIdFactory | None = None,
) -> MarketCatalogCaptureResult:
    """Capture through injected ports while recording terminal request evidence."""

    normalized_quote = normalize_quote_currency(quote_currency)
    effective_clock = clock or _utc_now
    effective_id_factory = snapshot_id_factory or (lambda: uuid4().hex)
    snapshot_id = effective_id_factory()
    observed_at: datetime | None = None

    try:
        batch = await adapter.fetch_trading_pair_batch(normalized_quote)
        observed_at = _require_utc(batch.received_at, "received_at")
        members = _validated_members(batch.observations, normalized_quote)
        available_at = _require_utc(effective_clock(), "available_at")
        stored = repository.record_market_catalog_snapshot(
            snapshot_id=snapshot_id,
            source="upbit",
            endpoint=batch.endpoint,
            request_parameters=batch.request_parameters,
            quote_currency=normalized_quote,
            observed_at=observed_at,
            available_at=available_at,
            status=MarketCatalogSnapshotStatus.SUCCEEDED,
            members=members,
        )
    except asyncio.CancelledError:
        raise
    except Exception as error:
        failure_observed_at = observed_at or _require_utc(effective_clock(), "observed_at")
        failure_available_at = _require_utc(effective_clock(), "available_at")

        if failure_available_at < failure_observed_at:
            failure_available_at = failure_observed_at

        try:
            repository.record_market_catalog_snapshot(
                snapshot_id=snapshot_id,
                source="upbit",
                endpoint="/v1/market/all",
                request_parameters={"is_details": True},
                quote_currency=normalized_quote,
                observed_at=failure_observed_at,
                available_at=failure_available_at,
                status=MarketCatalogSnapshotStatus.FAILED,
                members=(),
                error_type=type(error).__name__,
                error_message=str(error),
            )
        except Exception:
            pass

        raise

    return _capture_result(stored)


def resolve_market_universe_as_of(
    quote_currency: str,
    as_of: datetime,
    database: str | Path,
) -> MarketUniverseResult:
    """Resolve a universe from complete snapshots available by one cutoff."""

    normalized_quote = normalize_quote_currency(quote_currency)
    cutoff = _require_utc(as_of, "as_of")

    with SqliteMinuteCandleRepository(database) as repository:
        snapshot = repository.get_market_catalog_snapshot_as_of(normalized_quote, cutoff)

    if snapshot is None:
        return MarketUniverseResult(
            status=MarketUniverseStatus.UNRESOLVED,
            quote_currency=normalized_quote,
            as_of=cutoff,
            snapshot_id=None,
            observed_at=None,
            available_at=None,
            content_hash=None,
            pairs=(),
        )

    return MarketUniverseResult(
        status=MarketUniverseStatus.AVAILABLE,
        quote_currency=normalized_quote,
        as_of=cutoff,
        snapshot_id=snapshot.snapshot_id,
        observed_at=snapshot.observed_at,
        available_at=snapshot.available_at,
        content_hash=snapshot.content_hash,
        pairs=snapshot.pairs,
    )


def _validated_members(
    observations: tuple[TradingPairObservation, ...],
    quote_currency: str,
) -> tuple[MarketCatalogSnapshotMemberInput, ...]:
    if not observations:
        raise ValueError(f"{quote_currency} market catalog response must not be empty")

    members: list[MarketCatalogSnapshotMemberInput] = []
    markets: set[str] = set()
    response_orders: set[int] = set()

    for observation in observations:
        pair = observation.pair

        if observation.market != pair.code:
            raise ValueError("trading-pair observation market must match its normalized pair")

        if pair.quote_currency != quote_currency:
            raise ValueError("trading-pair observation quote currency does not match request")

        if pair.code in markets:
            raise ValueError(f"duplicate market in catalog response: {pair.code}")

        if observation.response_order in response_orders:
            raise ValueError("duplicate response_order in catalog response")

        markets.add(pair.code)
        response_orders.add(observation.response_order)
        members.append(
            MarketCatalogSnapshotMemberInput(
                response_order=observation.response_order,
                pair=pair,
                raw_payload=observation.raw_payload,
            )
        )

    return tuple(members)


def _capture_result(snapshot: StoredMarketCatalogSnapshot) -> MarketCatalogCaptureResult:
    if snapshot.content_hash is None:
        raise RuntimeError("successful market catalog snapshot is missing content_hash")

    return MarketCatalogCaptureResult(
        snapshot_id=snapshot.snapshot_id,
        quote_currency=snapshot.quote_currency,
        observed_at=snapshot.observed_at,
        available_at=snapshot.available_at,
        market_count=snapshot.market_count,
        content_hash=snapshot.content_hash,
        pairs=snapshot.pairs,
    )


def _require_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be timezone-aware UTC")

    return value.astimezone(UTC)


def _utc_now() -> datetime:
    return datetime.now(UTC)
