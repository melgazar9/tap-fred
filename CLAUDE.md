# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Critical Warning

DO NOT assume this code is production ready. Previous AI sessions repeatedly declared "production ready" while real bugs existed. Be brutally honest about code quality. Verify every claim against the FRED API documentation at https://fred.stlouisfed.org/docs/api/fred/.

## What This Is

A Meltano Singer tap for the Federal Reserve Economic Data (FRED) API. Extracts economic time series data with support for ALFRED (vintage/historical) mode for backtesting without look-ahead bias. Built with the [Meltano Singer SDK](https://sdk.meltano.com).

## Build & Test Commands

```bash
uv sync                                    # Install dependencies
uv run pytest                              # Run all tests
uv run pytest tests/test_core.py           # Run specific test file
uv run pytest -k point_in_time             # Run tests matching pattern
uv run tap-fred --help                     # CLI entrypoint
meltano invoke tap-fred --discover         # Validate Singer discovery
ruff check . --fix && ruff format .        # Lint and format
mypy tap_fred/                             # Type checking
```

**CRITICAL**: Never use inline `TAP_FRED_*` environment variables in bash commands. Always modify `meltano.yml` config section and run:
```bash
meltano el --state-id <test-name> tap-fred target-jsonl --select <stream>
```

## Architecture

### Base Class Hierarchy (client.py)

```
FREDStream (ABC, RESTStream)          # Core: rate limiting, retry, pagination, API key redaction
├── TreeTraversalFREDStream           # BFS tree traversal (categories)
├── SeriesBasedFREDStream             # Per-series-ID iteration
├── ReleaseBasedFREDStream            # Partitioned by release_id
├── CategoryBasedFREDStream           # Partitioned by category_id
├── SourceBasedFREDStream             # Partitioned by source_id
├── TagBasedFREDStream                # Partitioned by tag_name
└── PointInTimePartitionStream        # (resource_id, vintage_date) partitions for backtesting
```

### Key Files

- **tap.py** - `TapFRED` class: config schema, thread-safe resource ID caching (categories, releases, sources, tags, series, vintage dates), wildcard discovery via `_cache_resource_ids()`
- **client.py** - All base stream classes, sliding window rate limiting (`_throttle`), exponential backoff retry (`_make_request`), pagination (`_paginate_records`)
- **helpers.py** - `clean_json_keys()` (camelCase->snake_case), `generate_surrogate_key()`
- **streams/** - 32 FRED API endpoint implementations across 6 domain files

### Data Modes

- **FRED** (default): Current revised data. `realtime_start`/`realtime_end` default to today.
- **ALFRED**: Vintage historical data. Set `data_mode: ALFRED` with `realtime_start`/`realtime_end` to get data as-known-on a specific date.
- **Point-in-Time**: Set `point_in_time_mode: true` to create separate partitions per vintage date for backtesting.

### Caching Pattern

All resource types use the same tap-level thread-safe caching via `_cache_resource_ids()`:
1. Check class-level cache attribute (e.g., `_cached_release_ids`)
2. Acquire lock, double-check
3. If `["*"]`, use discovery stream; otherwise use config values directly
4. Cache result for all dependent streams

### Conditional Stream Registration

Streams requiring specific config are only registered in `discover_streams()` when their config is present:
- **tag_names required**: `CategoryRelatedTagsStream`, `RelatedTagsStream`, `ReleaseRelatedTagsStream`
- **series_search_related_tags_params with both `series_search_text` and `tag_names`**: `SeriesSearchRelatedTagsStream`
- **geofred_regional_params required**: `GeoFREDRegionalDataStream`
- **geofred_series_ids required**: `GeoFREDSeriesDataStream`

### Error Handling

- `strict_mode` config (default: `false`): When true, non-retriable errors fail the sync at both `_make_request` (4xx) and `_safe_partition_extraction` (partition-level). When false, errors are logged and skipped.
- Per-stream `_skipped_partitions` list tracks all skipped partitions with stream name, partition key/value, and error type.
- `finalize_state_progress_markers()` emits an aggregated WARNING-level skip summary after each stream completes (count + details of all skipped partitions).
- Partition-level errors in `_safe_partition_extraction` log individual ERROR per skip, then re-raise if `strict_mode=true`.

### Pagination Completeness Validation

`_paginate_records` captures the API-reported `count` from the first page and compares against total extracted records at the end. Three known behaviors:

| Endpoint group | `count` accuracy | Notes |
|---|---|---|
| releases, sources, observations, vintage dates | **Exact** | count == extractable records |
| tags, related_tags, search_tags | **Approximate** | count is cached/stale; actual records are ~0.5-1% fewer |
| series/search | **Misleading** | Reports 80K+ but API hard-caps at offset 4001 (max 5,000 accessible). Undocumented. |

Page-level errors mid-pagination (429, 500, offset cap 400) are handled:
- **Permissive mode**: logs warning with extracted-so-far count, stops pagination, continues sync
- **Strict mode**: re-raises the error, fails the stream

### Incremental Replication Strategy

| Stream | replication_key | Behavior |
|---|---|---|
| `series_observations` | `realtime_start` | Meaningful in **point-in-time mode** only. In FRED/single-vintage ALFRED mode, all records share the same `realtime_start`, so the bookmark doesn't filter — effectively FULL_TABLE. |
| `series_vintage_dates` | `date` | New vintage dates always have later dates, so bookmark works correctly. |
| `series_updates` | `last_updated` | Large stream (90K+). SDK-side filtering emits only series updated since last bookmark. API still fetches all records (FRED has no server-side "updated since" filter). |
| All other streams | None (FULL_TABLE) | Metadata streams with small datasets. FRED API has no "updated since" parameter on these endpoints. Re-fetching everything each run is correct. |

## Migration Notes

### PK Change for series_observations
`primary_keys` changed from `["series_id", "date"]` to `["series_id", "date", "realtime_start", "realtime_end"]`. This prevents vintage data from being overwritten during point-in-time backfills. Downstream targets doing upsert-by-PK will need to account for this wider key. Per FRED docs, `realtime_start` and `realtime_end` together define the version identity of an observation.
