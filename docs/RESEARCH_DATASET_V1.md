# Research Dataset v1

## Scope

This is the first RD1 implementation slice: offline data diagnostics and a
reproducible, exploratory four-hour panel. It consumes an existing SQLite schema
v4 database. It does not fetch market data, construct labels or features, select
investments, fit models, or submit orders.

`ResearchDatasetRequest` fixes explicit KRW markets, UTC `[start, end)`, one
common `as_of`, and catalog maximum age. Markets are normalized, deduplicated and
sorted; both bounds must be four-hour boundaries and `as_of >= end`. Processing
order is market code followed by bar start. No wall clock or output path enters
the content identity.

## Commands

```bash
uv run cassandra-trader research-readiness KRW-BTC KRW-ETH \
  --start 2026-08-24T00:00:00Z --end 2026-08-25T00:00:00Z \
  --as-of 2026-08-25T00:10:00Z \
  --maximum-snapshot-age-seconds 3600 --database candles.db

uv run cassandra-trader export-research-dataset KRW-BTC KRW-ETH \
  --start 2026-08-24T00:00:00Z --end 2026-08-25T00:00:00Z \
  --as-of 2026-08-25T00:10:00Z \
  --maximum-snapshot-age-seconds 3600 --database candles.db \
  --output artifacts/research/20260825-v1
```

The dates are examples, not evidence that these data exist on a user's machine.
Use a cutoff by which the source data had actually been ingested. Backfilling an
old interval today does not make it visible to an earlier `as_of`.

Both commands print one JSON object to stdout. Invalid arguments use argparse
exit code 2. Operational errors produce one JSON error on stderr and exit 1.
Otherwise exit 0 means requested-range data completeness and requested-market
catalog eligibility, and exit 2 means either is missing. A diagnostic export is
still published when it returns 2; incomplete bars and ineligible markets are
retained for inspection.

## Output contract

| File | Contents |
|---|---|
| `bars.jsonl` | Every requested market/four-hour interval, status, slot counts and OHLCV or null |
| `lineage.jsonl` | Matching intervals, canonical revision/hash/availability and raw/batch provenance, no-trade intervals and successful backfill evidence |
| `readiness.json` | Per-market coverage, merged unresolved intervals, first/latest canonical candle start and latest used availability within the request, bar counts, catalog diagnostics and blockers |
| `manifest.json` | Parameters, schema/algorithm/arithmetic versions, evidence limitations, row count, file byte sizes/SHA-256 hashes and dataset identity |

Manifest `dataset_id` is SHA-256 of the canonical manifest JSON before adding
the `dataset_id` member. JSON uses sorted keys, compact separators, ASCII escape
encoding and a final LF. The three data file hashes cover their exact bytes.
Decimals are strings; UTC timestamps include six fractional digits and `Z`.
Catalog maximum age also has an exact integer `maximum_snapshot_age_microseconds`
parameter; the seconds field is a display convenience. Per-market
`first_candle_time_utc`/`latest_candle_time_utc` refer to candle starts, not collector
receive times. `latest_available_at` is the latest evidence used in the requested
range, not a whole-database last-write time.
One explicit precision-50 Decimal context traps both rounding and inexact
arithmetic. Inputs that cannot aggregate exactly under this policy fail without
publishing a dataset. Caller precision, rounding and traps do not affect output.

`COMPLETE`, `NO_TRADE` and `INCOMPLETE` retain the existing 240-minute resolver
semantics. An unresolved minute makes the bar incomplete with null OHLCV.
All-no-trade intervals also have null OHLCV. Actual traded minutes alone contribute
to complete OHLCV; no forward fill or synthetic volume is added.

The source must remain available to retrieve canonical minute values from the
recorded hashes/IDs. Lineage preserves references and provenance, not a second
copy of every raw response. It is not a standalone raw-data archive.

## Readiness and evidence limits

- `data_complete`: all requested minute slots have traded or successful no-trade
  evidence. This includes all-no-trade data; it is not usable-price or label
  sufficiency.
- `catalog_ready`: every requested market is eligible in the stored catalog
  snapshot at the common cutoff. Missing/stale snapshots, warning markets and
  absent requested markets are reported distinctly.
- `status`: the requested candle/evidence completeness state, independent of
  catalog eligibility. Check both readiness flags for command success.
- `selection_scope=EXPLICIT_MARKETS`: no full-analysis-universe readiness claim,
  liquidity rank, top-N selection or 90-day listing-age determination is made.
- `evidence_grade=NON_PIT_EXPLORATORY` and `confirmatory_ready=false` always.
  The common cutoff respects actual input availability, but does not reconstruct
  each historical decision's information set or historical universe. A fresh
  catalog today is not historical membership evidence.

Confirmatory OOS, lockbox and model promotion remain blocked until historical
listing/status evidence, per-decision availability/revision policy, labels and
time-ordered validation are implemented. No reconstructed availability model is
invented by this export.

## Storage and publication

Read-only access uses SQLite `mode=ro`, a pinned read transaction and schema
validation. It creates no source database, changes no journal mode and runs no
migration. Concurrent commits are invisible within one inspection/export.
An old schema must be migrated explicitly through the existing write path first.

Output files are streamed by four-hour market interval into a sibling staging
directory. Missing-interval diagnostics and market summaries are retained in
memory; minute candle rows are not accumulated for the whole dataset. Completed
files are flushed before an atomic, no-replace directory rename. Existing output
directories, files and symlinks are rejected; source DB sidecar paths are reserved.
Handled failures remove the staging directory. Abrupt process termination may
leave a hidden staging directory, but no partially published final directory.

Publication currently supports Linux with `renameat2(RENAME_NOREPLACE)` and
Windows `rename`. Other platforms fail without publishing. This is atomic
visibility, not a guarantee against arbitrary hardware/power-loss failures.
Long read snapshots can delay WAL checkpoint reclamation while a collector is
writing; use bounded research requests and monitor source storage growth.

## Next RD1 work

Historical listing/status evidence and 90-day eligibility; versioned historical
availability contracts; executable 7-day primary and 1/3/14-day auxiliary labels;
global time-axis splits with overlap purging; and experiment registration remain
separate implementation tasks. Formal training storage may later use
Parquet/DuckDB; this initial JSONL slice adds no new runtime dependency.
