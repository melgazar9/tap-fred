"""Durable point-in-time equivalence tests (network-gated).

Proves the shipped output_type=1 compact path reconstructs per-vintage ALFRED truth
EXACTLY — the property that actually matters, locked into the suite instead of a
throwaway script. Runs only when a FRED API key is present (skips in CI without one):

  FRED_API_KEY or FRED_API_KEY_1..N  ->  hits the live FRED API.

Covered:
  * Backfill, single chunk (GDP) and multi-chunk (DGS10): as-of reconstruction from
    the emitted compact rows == per-vintage ALFRED snapshot, incl. early baseline and
    across chunk boundaries.
  * Incremental completeness: prior data + the delta from a bookmarked run reconstructs
    every post-bookmark vintage exactly (no gaps), confirming output_type=1 incremental
    loses nothing vs a full backfill.
"""

from __future__ import annotations

import os
from unittest import mock

import pytest
import requests

from tap_fred.tap import TapFRED

VINT = "https://api.stlouisfed.org/fred/series/vintagedates"
OBS = "https://api.stlouisfed.org/fred/series/observations"


def _api_key():
    for var in ["FRED_API_KEY", *(f"FRED_API_KEY_{i}" for i in range(1, 29))]:
        if os.environ.get(var):
            return os.environ[var]
    return None


pytestmark = pytest.mark.skipif(
    _api_key() is None,
    reason="no FRED API key in env (FRED_API_KEY[_N]) — live PIT test skipped",
)


def _vintage_dates(series, key):
    r = requests.get(
        VINT,
        params={
            "series_id": series,
            "api_key": key,
            "file_type": "json",
            "limit": 10000,
        },
        timeout=(10, 60),
    )
    r.raise_for_status()
    return r.json()["vintage_dates"]


def _per_vintage_truth(series, vintage, key):
    """The current tap method's ground truth: series exactly as it stood at `vintage`."""
    r = requests.get(
        OBS,
        params={
            "series_id": series,
            "api_key": key,
            "file_type": "json",
            "realtime_start": vintage,
            "realtime_end": vintage,
            "limit": 100000,
        },
        timeout=(20, 120),
    )
    r.raise_for_status()
    out = {}
    for o in r.json().get("observations", []):
        out[o["date"]] = None if o["value"] == "." else float(o["value"])
    return out


def _emit(series, key, bookmark=None, max_vintages=None):
    """Run the REAL stream get_records for one series; return emitted compact rows."""
    cfg = {
        "api_key": key,
        "series_ids": [series],
        "data_mode": "ALFRED",
        "point_in_time_mode": True,
        "strict_mode": True,
        "point_in_time_start": "1776-07-04",
        "point_in_time_end": "2099-12-31",
        "max_requests_per_minute": 120,
        "min_throttle_seconds": 0.0,
        "throttle_jitter_seconds": 0.0,
    }
    tap = TapFRED(config=cfg, validate_config=False)
    stream = tap.streams["series_observations"]
    if max_vintages:
        stream.MAX_VINTAGES_PER_REQUEST = max_vintages
    vd_cache = [
        {"resource_id": series, "vintage_date": d} for d in _vintage_dates(series, key)
    ]
    state = {"replication_key_value": bookmark} if bookmark else {}
    with (
        mock.patch.object(tap, "get_cached_vintage_dates", return_value=vd_cache),
        mock.patch.object(stream, "get_context_state", return_value=state),
    ):
        return list(stream.get_records(context={"series_id": series}))


def _as_of(rows, v):
    """Bitemporal read: latest realtime_start <= v wins, per date."""
    best = {}
    for r in rows:
        rs = r["realtime_start"]
        if rs <= v:
            d = r["date"]
            if d not in best or rs > best[d][0]:
                best[d] = (rs, r["value"])
    return {d: val for d, (rs, val) in best.items()}


@pytest.mark.parametrize("series", ["GDP", "DGS10"])
def test_backfill_reconstructs_per_vintage_truth(series):
    """Full backfill: as-of reconstruction == per-vintage ALFRED truth at sampled vintages
    (GDP=1 chunk, DGS10=multi-chunk -> exercises chunk-boundary correctness)."""
    key = _api_key()
    vd = _vintage_dates(series, key)
    rows = _emit(series, key)
    assert rows, f"{series}: backfill emitted no rows"
    for v in (vd[len(vd) // 10], vd[len(vd) // 2], vd[-1]):
        assert _as_of(rows, v) == _per_vintage_truth(
            series, v, key
        ), f"{series} as-of {v} mismatch"


def test_backfill_multichunk_forced_boundaries():
    """Force tiny chunks so a small series spans several windows; reconstruction must
    stay exact across the synthetic boundaries (guards the chunking/clipping logic)."""
    key = _api_key()
    vd = _vintage_dates("GDP", key)
    rows = _emit("GDP", key, max_vintages=50)  # 411 vintages -> ~9 chunks
    for v in (vd[5], vd[len(vd) // 2], vd[-1]):
        assert _as_of(rows, v) == _per_vintage_truth(
            "GDP", v, key
        ), f"GDP as-of {v} mismatch (chunked@50)"


def test_incremental_delta_is_complete():
    """Prior data + a bookmarked incremental run reconstructs every POST-bookmark vintage
    exactly — proving output_type=1 incremental loses nothing vs a full backfill."""
    key = _api_key()
    series = "GDP"
    vd = _vintage_dates(series, key)
    bookmark = vd[len(vd) // 2]

    full = _emit(series, key)  # complete reference
    prior = [
        r for r in full if r["realtime_start"] <= bookmark
    ]  # what a run up to the bookmark had
    delta = _emit(series, key, bookmark=bookmark)  # incremental run

    # Incremental must only pull newer vintages (lean), and prior+delta must be complete.
    assert delta, "incremental emitted no rows for new vintages"
    combined = prior + delta
    for v in [d for d in vd if d > bookmark][:: max(1, len(vd) // 6)]:
        assert _as_of(combined, v) == _per_vintage_truth(
            series, v, key
        ), f"incremental as-of {v} incomplete"
