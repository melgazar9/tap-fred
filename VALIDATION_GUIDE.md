# Live Validation Guide

Validation steps that require a real FRED API key and live API access. Run these to produce evidence artifacts for production sign-off.

## Prerequisites

```bash
export FRED_API_KEY=your_key_here
```

## 1. Strict Point-in-Time Evidence

This proves the data leakage prevention works against the real FRED API.

### Config (modify meltano.yml)

```yaml
config:
  api_key: ${FRED_API_KEY}
  series_ids: ["GDP", "DGS10"]
  data_mode: "ALFRED"
  point_in_time_mode: true
  point_in_time_start: "2025-01-01"
  point_in_time_end: "2025-01-10"
  strict_mode: true
  max_requests_per_minute: 60
  min_throttle_seconds: 1.0
select:
- series_observations.*
- series_vintage_dates.*
```

### Run

```bash
# Clean output
rm -f output/*.jsonl

# Run extraction
meltano el --state-id strict-pit-test tap-fred target-jsonl --select series_observations 2>&1 | tee output/strict_pit_run.log
```

### What to verify

1. **GDP skipped**: Look for log line: `Skipping series_id='GDP' — no vintage dates in point-in-time range`
   - GDP is quarterly — no vintage dates exist in a 10-day window (Jan 1-10)
2. **DGS10 records**: Check output file, every record must have `realtime_start` == the partition's `vintage_date`
   ```bash
   cat output/series_observations.jsonl | python3 -c "
   import sys, json
   for line in sys.stdin:
       r = json.loads(line)
       assert r['realtime_start'] == r.get('vintage_date', r['realtime_start']), f'LEAKAGE: {r}'
   print('All records pass leakage check')
   "
   ```
3. **Zero skipped partitions**: No `skipped partition(s)` warnings in log (strict mode would crash on errors)

## 2. Idempotency / Replay Evidence

Proves that re-running with the same state-id doesn't duplicate data.

### Run 1 (baseline)

```bash
rm -f output/*.jsonl
meltano el --state-id replay-test tap-fred target-jsonl --select series_observations 2>&1 | tee output/replay_run1.log
wc -l output/series_observations.jsonl > output/replay_run1_count.txt
cat output/replay_run1_count.txt
```

### Run 2 (replay with same state-id)

```bash
rm -f output/*.jsonl
meltano el --state-id replay-test tap-fred target-jsonl --select series_observations 2>&1 | tee output/replay_run2.log
wc -l output/series_observations.jsonl > output/replay_run2_count.txt
cat output/replay_run2_count.txt
```

### What to verify

- **PIT mode**: Run 2 should emit 0 records (all vintage partitions already bookmarked)
- **Standard mode**: Run 2 should emit 0 or very few records (only truly new data since Run 1)
- Check log for `Skipping partition` or `bookmark` messages confirming skip behavior

## 3. Reconciliation Report

Compares extracted counts against FRED API metadata for high-volume streams.

### Config (modify meltano.yml for this test)

```yaml
config:
  api_key: ${FRED_API_KEY}
  series_ids: ["GDP"]
  data_mode: "FRED"
  point_in_time_mode: false
  strict_mode: false
  release_ids: ["*"]
  source_ids: ["*"]
  category_ids: ["18", "32992"]
select:
- releases.*
- sources.*
- category_series.*
```

### Run

```bash
rm -f output/*.jsonl

# Run each stream separately to get clean counts
meltano el --state-id recon-releases tap-fred target-jsonl --select releases 2>&1 | tee output/recon_releases.log
meltano el --state-id recon-sources tap-fred target-jsonl --select sources 2>&1 | tee output/recon_sources.log

# Count extracted records
echo "=== Reconciliation Counts ==="
echo "releases: $(wc -l < output/releases.jsonl) records"
echo "sources: $(wc -l < output/sources.jsonl) records"
```

### Compare against API

```bash
# Get API-reported totals
curl -s "https://api.stlouisfed.org/fred/releases?api_key=${FRED_API_KEY}&file_type=json&limit=1" | python3 -c "import sys,json; print(f\"API releases count: {json.load(sys.stdin).get('count', 'N/A')}\")"
curl -s "https://api.stlouisfed.org/fred/sources?api_key=${FRED_API_KEY}&file_type=json&limit=1" | python3 -c "import sys,json; print(f\"API sources count: {json.load(sys.stdin).get('count', 'N/A')}\")"
```

### What to verify

- Extracted count should match or be close to API-reported count
- Any deficit should be explainable (API offset caps, pagination limits)
- Document any known API limitations (e.g., FRED's undocumented 5000-offset cap on search endpoints)

## Acceptance Gates Checklist

After running the above, fill in:

| Gate | Status | Evidence |
|------|--------|----------|
| README matches runtime behavior | | |
| 75/75 unit/integration tests green | | `uv run pytest tests/test_integration.py tests/test_comprehensive.py` |
| GDP skipped in strict PIT (no fallback) | | `strict_pit_run.log` |
| DGS10 realtime_start == vintage_date | | output file check |
| Zero unexplained skipped partitions | | log inspection |
| Run 2 emits 0 records (idempotent) | | `replay_run2_count.txt` |
| Reconciliation counts match API | | comparison output |
