# tap-fred

Meltano Singer tap for the Federal Reserve Economic Data (FRED) API with ALFRED vintage data support for backtesting.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Features

- **32 FRED API endpoints** across 6 domain categories (series, categories, releases, sources, tags, GeoFRED)
- **FRED Mode**: Current revised economic data for analysis and reporting
- **ALFRED Mode**: Historical vintage data for backtesting without look-ahead bias
- **Point-in-Time Mode**: Per-vintage-date partitions with data leakage prevention
- **Incremental Loading**: State management with partition-aware bookmarking
- **Wildcard Discovery**: Extract all resources with `["*"]` configuration
- **Rate Limiting**: Sliding window throttling with exponential backoff retry
- **Error Handling**: Configurable strict/permissive modes with partition-level skip tracking

## Quick Start

### 1. Get FRED API Key
Register at https://fred.stlouisfed.org/docs/api/api_key.html

### 2. Install
```bash
pipx install meltano
cd your-project
meltano install extractor tap-fred
```

### 3. Configure
```yaml
# meltano.yml
extractors:
- name: tap-fred
  config:
    api_key: ${FRED_API_KEY}
    series_ids: ["GDP", "UNRATE"]
```

### 4. Run
```bash
export FRED_API_KEY=your_api_key_here
meltano el --state-id my-job tap-fred target-jsonl --select series_observations
```

**Important**: Always use `--state-id` for proper incremental replication.

## Configuration

### Core Settings

| Setting | Type | Required | Default | Description |
|---------|------|----------|---------|-------------|
| `api_key` | string | Yes | | FRED API key |
| `series_ids` | array/string | Yes | | Series IDs to extract (`["GDP", "UNRATE"]` or `["*"]`) |
| `start_date` | date | No | | Earliest observation date |
| `api_url` | string | No | `https://api.stlouisfed.org/fred` | FRED API base URL |
| `max_requests_per_minute` | integer | No | `60` | Rate limit (FRED allows up to 120) |
| `min_throttle_seconds` | float | No | `1.0` | Min seconds between requests |
| `strict_mode` | boolean | No | `false` | If true, fail on any non-retriable error; if false, skip and log |

### ALFRED / Vintage Mode

| Setting | Type | Description |
|---------|------|-------------|
| `data_mode` | string | `"FRED"` (default) or `"ALFRED"` |
| `realtime_start` | date | Vintage start date (YYYY-MM-DD) |
| `realtime_end` | date | Vintage end date (YYYY-MM-DD) |

### Point-in-Time Mode

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `point_in_time_mode` | boolean | `false` | Create per-vintage-date partitions |
| `point_in_time_start` | date | | Filter vintage dates from this date |
| `point_in_time_end` | date | | Filter vintage dates up to this date |

### Resource Selection

| Setting | Type | Description |
|---------|------|-------------|
| `category_ids` | array | Category IDs (`["18", "32992"]` or `["*"]`) |
| `release_ids` | array | Release IDs (`["53", "151"]` or `["*"]`) |
| `source_ids` | array | Source IDs (`["1", "3"]` or `["*"]`) |
| `tag_names` | array | Tag names (`["gdp", "inflation"]` or `["*"]`) |

### GeoFRED Configuration

| Setting | Type | Description |
|---------|------|-------------|
| `geofred_regional_params` | array | Parameter sets for regional data extraction |
| `geofred_series_ids` | array | GeoFRED series IDs to extract |

## Data Modes

### FRED Mode (Default)
```yaml
config:
  data_mode: "FRED"
  series_ids: ["GDP", "UNRATE"]
```
Returns current revised data — "what we know now about the past."

### ALFRED Mode
```yaml
config:
  data_mode: "ALFRED"
  series_ids: ["GDP", "UNRATE"]
  realtime_start: "2020-06-01"
  realtime_end: "2020-06-01"
```
Returns vintage data as it existed on the specified date — "what we knew then." Prevents look-ahead bias in backtesting.

### Point-in-Time Mode
```yaml
config:
  data_mode: "ALFRED"
  series_ids: ["GDP", "DGS10"]
  point_in_time_mode: true
  point_in_time_start: "2020-01-01"
  point_in_time_end: "2020-03-31"
  strict_mode: true
```
Creates separate partitions for each vintage date within the range. Each partition captures the complete observation history as known on that specific vintage date.

**Data leakage prevention**: If a series has no vintage dates in the requested range, it is skipped entirely — the tap will never fall back to current revised data in PIT mode. A runtime guard also verifies that every returned record's `realtime_start` matches the requested `vintage_date`.

## Available Streams

### Series Streams

| Stream | Endpoint | PK | Replication Key |
|--------|----------|------|-----------------|
| `series_observations` | `/fred/series/observations` | `series_id, date, realtime_start, realtime_end` | `realtime_start` |
| `series` | `/fred/series` | `id` | — |
| `series_categories` | `/fred/series/categories` | `id` | — |
| `series_release` | `/fred/series/release` | `id` | — |
| `series_search` | `/fred/series/search` | `id` | — |
| `series_search_tags` | `/fred/series/search/tags` | `name` | — |
| `series_search_related_tags` | `/fred/series/search/related_tags` | `name` | — |
| `series_tags` | `/fred/series/tags` | `name` | — |
| `series_updates` | `/fred/series/updates` | `id` | `last_updated` |
| `series_vintage_dates` | `/fred/series/vintagedates` | `series_id, date` | `date` |

### Category Streams

| Stream | Endpoint | PK |
|--------|----------|----|
| `categories` | `/fred/category` | `id` |
| `category_children` | `/fred/category/children` | `id, parent_id` |
| `category_related` | `/fred/category/related` | `id` |
| `category_series` | `/fred/category/series` | `id` |
| `category_tags` | `/fred/category/tags` | `name` |
| `category_related_tags` | `/fred/category/related_tags` | `name` |

### Release Streams

| Stream | Endpoint | PK |
|--------|----------|----|
| `releases` | `/fred/releases` | `id` |
| `release` | `/fred/release` | `id` |
| `release_dates` | `/fred/release/dates` | `release_id, date` |
| `release_series` | `/fred/release/series` | `id` |
| `release_sources` | `/fred/release/sources` | `id` |
| `release_tags` | `/fred/release/tags` | `name` |
| `release_related_tags` | `/fred/release/related_tags` | `name` |
| `release_tables` | `/fred/release/tables` | `element_id` |

### Source Streams

| Stream | Endpoint | PK |
|--------|----------|----|
| `sources` | `/fred/sources` | `id` |
| `source` | `/fred/source` | `id` |
| `source_releases` | `/fred/source/releases` | `id` |

### Tag Streams

| Stream | Endpoint | PK |
|--------|----------|----|
| `tags` | `/fred/tags` | `name` |
| `related_tags` | `/fred/related_tags` | `name` |
| `tags_series` | `/fred/tags/series` | `id` |

### GeoFRED Streams

| Stream | Endpoint | PK |
|--------|----------|----|
| `geofred_regional_data` | `/geofred/regional/data` | `region, series_group, date, region_type` |
| `geofred_series_data` | `/geofred/series/data` | `region, series_id, date` |

### Conditional Stream Registration

Some streams require specific configuration to be registered:

- **`tag_names` required**: `category_related_tags`, `related_tags`, `release_related_tags`, `tags_series`
- **`series_search_related_tags_params` with both `series_search_text` and `tag_names`**: `series_search_related_tags`
- **`geofred_regional_params`**: `geofred_regional_data`
- **`geofred_series_ids`**: `geofred_series_data`

## Migration Notes

### PK Change for series_observations

`primary_keys` changed from `["series_id", "date"]` to `["series_id", "date", "realtime_start", "realtime_end"]`. This prevents vintage data from being overwritten during point-in-time backfills. Downstream targets doing upsert-by-PK will need to account for this wider key.

## Operations

### Strict vs Permissive Mode

- **`strict_mode: false`** (default): Non-retriable API errors (4xx) are logged and the partition is skipped. A skip summary is emitted after each stream completes.
- **`strict_mode: true`**: Any non-retriable error fails the entire sync. Recommended for production to catch unexpected API changes.

RuntimeError (data leakage guards) always propagates regardless of strict_mode.

### Rate Limiting

The tap uses a sliding window rate limiter shared across all streams. Default: 60 requests/minute with 1.0s minimum between requests. FRED allows up to 120/min but conservative defaults reduce 429 risk.

### Retry Logic

Automatic exponential backoff (5s–300s) with jitter on retriable errors: 429 (rate limit), 500, 502, 503, 504.

### Chunked Backfills

For large wildcard backfills (`series_ids: ["*"]`), use narrow `point_in_time_start`/`point_in_time_end` windows with separate `--state-id` values per chunk. This keeps individual runs manageable and allows resuming from failures.

### Restart Behavior

- State commits only after successful pipeline completion (Singer SDK guarantee).
- If the process is killed mid-sync, the next run re-processes from the last committed bookmark — no data loss, but some records may be re-emitted.
- Point-in-time partitions with existing bookmarks are skipped entirely (no API call), making re-runs efficient.

### Post-Run Checks

1. Check for skip summary warnings in logs (partitions that returned errors).
2. Verify record counts against expectations for high-volume streams.
3. For PIT mode: confirm `realtime_start` values in output match requested vintage dates.

## Development

### Prerequisites
- Python 3.9+
- [uv](https://docs.astral.sh/uv/)

### Setup
```bash
git clone <repo-url>
cd tap-fred
uv sync
uv run pytest
uv run tap-fred --help
```

### Testing
```bash
uv run pytest                              # All tests
uv run pytest tests/test_integration.py    # Integration tests (mocked API)
uv run pytest tests/test_comprehensive.py  # Config/schema tests
uv run pytest -k point_in_time            # Pattern matching
```

## License

[Your License Here]
