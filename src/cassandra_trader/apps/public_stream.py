"""Application service for consuming public market-data domain events."""

from collections.abc import AsyncIterator

from cassandra_trader.adapters.upbit.public_stream import (
    LifecycleCallback,
    UpbitPublicStreamAdapter,
)
from cassandra_trader.domain.public_stream import PublicMarketEvent


def stream_public_events(
    markets: list[str] | tuple[str, ...],
    *,
    lifecycle_callback: LifecycleCallback | None = None,
    adapter: UpbitPublicStreamAdapter | None = None,
) -> AsyncIterator[PublicMarketEvent]:
    """Yield only internal domain events from the public Upbit stream."""

    effective_adapter = adapter or UpbitPublicStreamAdapter()
    return effective_adapter.stream(
        markets,
        lifecycle_callback=lifecycle_callback,
    )
