# tap-fred

Production-ready Meltano extractor for the Federal Reserve Economic Data (FRED) API with ALFRED vintage data support for institutional-grade backtesting.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Features

- **FRED Mode**: Current revised economic data for analysis and reporting
- **ALFRED Mode**: Historical vintage data for backtesting without look-ahead bias
- **319+ Economic Releases**: All major indicators from BEA, BLS, Census, Fed, and more
- **Incremental Loading**: State management with configurable replication keys
- **Production Ready**: Rate limiting, retry logic, error handling, and API key security
- **Wildcard Support**: Extract all series IDs with `series_ids: "*"`

## Quick Start

### 1. Get FRED API Key
Register at https://fred.stlouisfed.org/docs/api/api_key.html

### 2. Install Meltano
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
    series_ids: ["GDP", "UNRATE"]  # Or "*" for all series
    data_mode: "FRED"  # Use "ALFRED" for vintage data
```

### 4. Run Extraction
```bash
# Set API key
export FRED_API_KEY=your_api_key_here

# Extract current data
meltano el tap-fred target-jsonl --select series_observations

# Extract vintage data (backtesting mode)
meltano config tap-fred set data_mode ALFRED
meltano config tap-fred set realtime_start 2020-06-01
meltano config tap-fred set realtime_end 2020-06-01
meltano el tap-fred target-jsonl --select series_observations
```

## Configuration

### Core Settings

| Setting | Type | Required | Description |
|---------|------|----------|-------------|
| `api_key` | string | ✅ | FRED API key |
| `series_ids` | array/string | ✅ | Series IDs to extract (or "*" for all) |
| `data_mode` | string | | "FRED" (default) or "ALFRED" |
| `start_date` | date | | Earliest observation date |
| `api_url` | string | | FRED API base URL |
| `min_throttle_seconds` | float | | Rate limiting (default: 0.01) |

### ALFRED Mode (Vintage Data)

| Setting | Type | Description |
|---------|------|-------------|
| `realtime_start` | date | Data as it existed on this date |
| `realtime_end` | date | End of vintage period (use same as start for point-in-time) |

## FRED vs ALFRED Modes

### FRED Mode (Default)
```yaml
config:
  data_mode: "FRED"
  series_ids: ["GDP", "UNRATE"]
```
- **Use for**: Current analysis, reporting, research
- **Data**: Current revised values - "what we know now about the past"
- **Output**: Latest economic data with all historical revisions

### ALFRED Mode (Backtesting)
```yaml
config:
  data_mode: "ALFRED"
  series_ids: ["GDP", "UNRATE"] 
  realtime_start: "2020-06-01"
  realtime_end: "2020-06-01"
```
- **Use for**: Backtesting, trading algorithms, machine learning models
- **Data**: Vintage historical data - "what we knew then"
- **Output**: Only data available on the specified date
- **Prevents**: Look-ahead bias in backtesting

### Example Output Comparison

**FRED Mode (Current Revised Data):**
```json
{"realtime_start": "2025-08-27", "realtime_end": "2025-08-27", "date": "2020-01-01", "value": 21727.657, "series_id": "GDP"}
```

**ALFRED Mode (Vintage Data):**
```json
{"realtime_start": "2020-06-01", "realtime_end": "2020-06-01", "date": "2020-01-01", "value": 21534.907, "series_id": "GDP"}
```

## Data Reliability for Backtesting

### ✅ Verified: No Intraday Updates

FRED/ALFRED data follows predetermined government release schedules and does NOT update intraday:

**Major Release Times:**
- **BEA** (GDP, Personal Income): 8:30 AM ET
- **BLS** (Unemployment, CPI): 8:30 AM ET  
- **Fed** (Interest Rates): 3:15 PM CT

**Backtesting Guarantee:**
A vintage record with `realtime_start: "2020-06-01"` represents exactly what was known on June 1, 2020 - whether you traded at 9 AM, 3 PM, or 11:59 PM that day.

### Data Sources (319 Releases)
- **Federal Agencies**: BEA, BLS, Census Bureau, Treasury, Federal Reserve
- **Regional Fed Banks**: Chicago Fed, NY Fed, Atlanta Fed, etc.
- **Private Sources**: ADP, Conference Board, and more

## Available Streams

Currently supported stream (more can be added following existing patterns):

### series_observations
- **Description**: Economic data points for specified series
- **Primary Keys**: `["series_id", "date"]`
- **Replication Key**: `date` (incremental loading)
- **Schema**: 
  - `series_id` (string): FRED series identifier
  - `date` (date): Observation date
  - `value` (number): Economic data value
  - `realtime_start` (date): Vintage start date
  - `realtime_end` (date): Vintage end date

## Development

### Prerequisites
- Python 3.9+
- [uv](https://docs.astral.sh/uv/)

### Setup
```bash
# Clone and install
git clone <repo-url>
cd tap-fred
uv sync

# Test
uv run pytest

# Run directly
uv run tap-fred --help
```

### Testing with Meltano
```bash
# Install Meltano and plugins
pipx install meltano
cd tap-fred
meltano install

# Test discovery
meltano invoke tap-fred --discover

# Run extraction
meltano el tap-fred target-jsonl --select series_observations
```

## Production Deployment

### Environment Variables
```bash
export FRED_API_KEY=your_api_key_here
```

### Rate Limiting
```yaml
config:
  min_throttle_seconds: 1.0  # 60 requests/minute (under 120/min FRED limit)
```

### Incremental Loading
The extractor automatically manages state for incremental loading:
- Uses `date` as replication key
- Tracks last extracted date per series
- Resumes from last extraction point

### Error Handling
- Exponential backoff with jitter
- Retry on 429, 500, 502, 503, 504 errors
- API key redaction in logs
- Thread-safe series caching

## License

[Your License Here]

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes following existing patterns
4. Add tests
5. Submit a pull request

For more details, see `.claude/CLAUDE.md` for complete technical documentation.