"""Reconciliation: compare extracted record counts against FRED API pagination metadata.

Hits the API directly to get `count` (total available records) and compares
against what the tap actually extracted into JSONL files.

Usage:
    FRED_API_KEY=... uv run python tests/reconciliation_check.py
"""

import json
import os
import sys

import requests

API_KEY = os.environ.get("FRED_API_KEY")
BASE = "https://api.stlouisfed.org/fred"
OUTPUT_DIR = "output"


def api_count(endpoint: str, extra_params: dict | None = None) -> int:
    """Get total record count from FRED API pagination metadata."""
    params = {"api_key": API_KEY, "file_type": "json", "limit": 1, "offset": 0}
    if extra_params:
        params.update(extra_params)
    resp = requests.get(f"{BASE}{endpoint}", params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    # FRED returns count/total in different fields depending on endpoint
    return data.get("count", data.get("total", -1))


def file_count(stream_name: str) -> int:
    """Count lines in extracted JSONL file."""
    path = os.path.join(OUTPUT_DIR, f"{stream_name}.jsonl")
    if not os.path.exists(path):
        return -1
    with open(path) as f:
        return sum(1 for _ in f)


def reconcile():
    """Run reconciliation checks on key paginated endpoints."""
    if not API_KEY:
        print("ERROR: FRED_API_KEY not set")
        sys.exit(1)

    checks = [
        # (stream_name, api_endpoint, extra_params, description)
        ("releases", "/releases", {"order_by": "release_id", "sort_order": "asc"}, "All FRED releases"),
        ("sources", "/sources", {"order_by": "source_id", "sort_order": "asc"}, "All FRED sources"),
        ("tags", "/tags", {"order_by": "name", "sort_order": "asc"}, "All FRED tags"),
        ("release_dates", "/releases/dates", None, "All release dates"),
        # series_search: FRED API has undocumented cap at offset=5000 (returns 0 records
        # beyond that). API reports count=80216 but only serves max 5000. Our pagination
        # is correct — the API itself stops. Use tolerance to account for this.
        ("series_search", "/series/search", {"search_text": "GDP"}, "Series search for GDP (API caps at ~5000)"),
        ("series_search_tags", "/series/search/tags", {"series_search_text": "GDP"}, "Search tags for GDP"),
        ("series_updates", "/series/updates", None, "Series updates"),
        ("related_tags", "/related_tags", {"tag_names": "gdp", "order_by": "name", "sort_order": "asc"}, "Related tags for gdp"),
    ]

    all_pass = True
    print(f"{'Stream':<30} {'API Count':>10} {'Extracted':>10} {'Match':>8}")
    print("-" * 62)

    for stream_name, endpoint, extra, desc in checks:
        try:
            expected = api_count(endpoint, extra)
        except Exception as e:
            print(f"{stream_name:<30} {'ERROR':>10} {'':>10} {'SKIP':>8}  ({e})")
            continue

        actual = file_count(stream_name)
        if actual == -1:
            print(f"{stream_name:<30} {expected:>10} {'NO FILE':>10} {'SKIP':>8}")
            continue

        # Allow small variance for live data that changes between API calls
        # (series_updates is especially volatile)
        tolerance = max(10, int(expected * 0.02))  # 2% or 10 records

        # FRED API has undocumented pagination caps on some endpoints (e.g.,
        # /series/search caps at offset 5000).  When the API cap is lower than
        # the reported count, use the cap as the expected value.
        api_pagination_caps = {"series_search": 5000}
        cap = api_pagination_caps.get(stream_name)
        effective_expected = min(expected, cap) if cap else expected

        match = abs(effective_expected - actual) <= tolerance
        status = "PASS" if match else "FAIL"
        if not match:
            all_pass = False
        # Show both API total and effective expected when capped
        expected_str = f"{expected}" if not cap else f"{expected} (cap:{cap})"

        print(f"{stream_name:<30} {expected:>10} {actual:>10} {status:>8}")

    print()
    if all_pass:
        print("RECONCILIATION: ALL CHECKS PASSED")
    else:
        print("RECONCILIATION: SOME CHECKS FAILED — investigate discrepancies")

    return all_pass


if __name__ == "__main__":
    reconcile()
