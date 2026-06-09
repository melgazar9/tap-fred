"""Standalone FRED series discovery — single source of truth.

Pure-stdlib module with no Singer SDK dependency so it can be called as a
subprocess from any environment (dagster, scripts, CI).

Usage as CLI::

    tap-fred-discover-series --api-key $FRED_API_KEY
    tap-fred-discover-series --api-key $FRED_API_KEY --api-url https://api.stlouisfed.org/fred

Usage as library (inside tap-fred venv)::

    from tap_fred.discovery import discover_all_series_ids
    ids = discover_all_series_ids(api_key="...", api_url="...")
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque

logger = logging.getLogger(__name__)

_PAGE_LIMIT = 1000


def _redact_url(url: str) -> str:
    """Strip api_key value from URLs before logging."""
    return re.sub(r"(api_key=)[^&]+", r"\1<REDACTED>", url)


class Throttle:
    """Paces FRED API calls for a single key.

    Sleeps before every call: a fixed ``min_interval`` between calls plus a random
    ``[0, jitter_seconds]`` buffer before each one. FRED is strict on rate limits;
    the jitter de-periodizes the request stream and de-syncs concurrent keys so a
    fleet of workers never hits the API in lockstep.
    """

    def __init__(self, min_interval: float, jitter_seconds: float = 0.0) -> None:
        self.min_interval = max(min_interval, 0.0)
        self.jitter_seconds = max(jitter_seconds, 0.0)
        self.calls = 0

    def wait(self) -> None:
        """Sleep the pacing delay, then count the call. Jitter applies before every call;
        the min interval applies only between calls (not before the first)."""
        delay = random.uniform(0.0, self.jitter_seconds)
        if self.calls > 0:
            delay += self.min_interval
        if delay > 0:
            time.sleep(delay)
        self.calls += 1


def _fred_get(
    api_url: str,
    endpoint: str,
    params: dict,
    api_key: str,
    throttle: Throttle,
    max_attempts: int = 8,
) -> dict:
    """Single FRED API GET with a jittered pre-call buffer and retry.

    FRED rate-limits a heavy walk with 403 *as well as* 429, so both are treated as
    retryable throttle signals with escalating backoff (60s, 120s, 240s, … capped at
    300s) — enough patience (~25min over the default attempts) to ride out sustained
    throttling. A 403/429 that still survives ``max_attempts`` is re-raised (genuinely
    forbidden, or a real outage) so discovery fails closed and surfaces it rather than
    returning partial/empty data. Other 4xx raise immediately; 5xx and network errors
    back off exponentially.
    """
    params = {**params, "api_key": api_key, "file_type": "json"}
    url = f"{api_url}/{endpoint}?{urllib.parse.urlencode(params)}"
    safe_url = _redact_url(url)

    # Jittered buffer before every call (FRED is strict on rate limits).
    throttle.wait()

    for attempt in range(max_attempts):
        last = attempt == max_attempts - 1
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
            return data
        except urllib.error.HTTPError as e:
            throttled = e.code in (403, 429)
            if (not throttled and e.code < 500) or last:
                raise  # non-retryable 4xx, or retries exhausted -> fail closed
            # Throttle (403/429): escalating backoff capped at 300s to ride out sustained
            # rate-limiting. 5xx: shorter exponential.
            wait = (
                min(300.0, 60 * (2**attempt)) + random.uniform(1, 5)
                if throttled
                else (2**attempt) * 5 + random.uniform(0, 2)
            )
            logger.warning(
                "Retryable %d on %s, waiting %.0fs (attempt %d/%d)",
                e.code,
                safe_url,
                wait,
                attempt + 1,
                max_attempts,
            )
            time.sleep(wait)
        except (urllib.error.URLError, TimeoutError) as e:
            if last:
                raise
            wait = (2**attempt) * 5
            logger.warning(
                "Request failed on %s (%s), retrying in %.0fs", safe_url, e, wait
            )
            time.sleep(wait)
    return {}  # unreachable: final attempt always returns or raises


def _paginate_series(
    api_url: str,
    endpoint: str,
    resource_key: str,
    resource_id,
    api_key: str,
    throttle: Throttle,
) -> set[str]:
    """Paginate an endpoint that returns seriess[], collecting series IDs."""
    ids: set[str] = set()
    offset = 0
    while True:
        data = _fred_get(
            api_url,
            endpoint,
            {resource_key: resource_id, "limit": _PAGE_LIMIT, "offset": offset},
            api_key,
            throttle,
        )
        seriess = data.get("seriess", [])
        if not seriess:
            break
        ids.update(s["id"] for s in seriess if "id" in s)
        if len(seriess) < _PAGE_LIMIT:
            break
        offset += _PAGE_LIMIT
    return ids


class DiscoveryIncompleteError(RuntimeError):
    """Raised when discovery cannot guarantee completeness."""

    def __init__(self, failures: list[dict], series_found: int):
        self.failures = failures
        self.series_found = series_found
        summary = "; ".join(f"{f['phase']}/{f['resource_id']}" for f in failures[:10])
        if len(failures) > 10:
            summary += f" ... and {len(failures) - 10} more"
        super().__init__(
            f"Discovery incomplete: {len(failures)} unrecovered failure(s) "
            f"({series_found} series found so far). Failed: {summary}"
        )


def _discover_via_releases(
    api_url: str,
    api_key: str,
    throttle: Throttle,
) -> tuple[set[str], list[dict]]:
    """Phase 1: releases -> release/series. Returns (series_ids, failures)."""
    release_ids = []
    offset = 0
    while True:
        data = _fred_get(
            api_url,
            "releases",
            {"limit": _PAGE_LIMIT, "offset": offset},
            api_key,
            throttle,
        )
        releases = data.get("releases", [])
        if not releases:
            break
        release_ids.extend(r["id"] for r in releases)
        if len(releases) < _PAGE_LIMIT:
            break
        offset += _PAGE_LIMIT

    logger.info(f"Found {len(release_ids)} FRED releases, fetching series for each...")
    series_ids: set[str] = set()
    failures: list[dict] = []
    for idx, rid in enumerate(release_ids):
        try:
            series_ids |= _paginate_series(
                api_url,
                "release/series",
                "release_id",
                rid,
                api_key,
                throttle,
            )
        except Exception as e:
            logger.error(f"Failed to get series for release {rid}: {e}")
            failures.append(
                {"phase": "release/series", "resource_id": rid, "error": str(e)}
            )
        if (idx + 1) % 50 == 0:
            logger.info(
                f"Release progress: {idx + 1}/{len(release_ids)}, {len(series_ids)} series so far"
            )

    return series_ids, failures


def _discover_via_categories(
    api_url: str,
    api_key: str,
    throttle: Throttle,
) -> tuple[set[str], list[dict]]:
    """Phase 2: BFS category tree -> category/series. Returns (series_ids, failures)."""
    visited: set[int] = set()
    queue: deque[int] = deque([0])
    category_ids: list[int] = []
    failures: list[dict] = []

    while queue:
        cat_id = queue.popleft()
        if cat_id in visited:
            continue
        visited.add(cat_id)
        try:
            data = _fred_get(
                api_url,
                "category/children",
                {"category_id": cat_id},
                api_key,
                throttle,
            )
        except Exception as e:
            logger.error(f"Failed to fetch children for category {cat_id}: {e}")
            failures.append(
                {"phase": "category/children", "resource_id": cat_id, "error": str(e)}
            )
            continue
        for child in data.get("categories", []):
            child_id = child.get("id")
            if child_id is not None and child_id not in visited:
                category_ids.append(child_id)
                queue.append(child_id)

    logger.info(
        f"Discovered {len(category_ids)} FRED categories via BFS, fetching series for each..."
    )
    series_ids: set[str] = set()
    for idx, cid in enumerate(category_ids):
        try:
            series_ids |= _paginate_series(
                api_url,
                "category/series",
                "category_id",
                cid,
                api_key,
                throttle,
            )
        except Exception as e:
            logger.error(f"Failed to get series for category {cid}: {e}")
            failures.append(
                {"phase": "category/series", "resource_id": cid, "error": str(e)}
            )
        if (idx + 1) % 100 == 0:
            logger.info(
                f"Category progress: {idx + 1}/{len(category_ids)}, {len(series_ids)} series so far"
            )

    return series_ids, failures


def discover_all_series_ids(
    api_key: str,
    api_url: str = "https://api.stlouisfed.org/fred",
    rate_limit_rpm: int = 60,
    jitter_seconds: float = 1.0,
) -> list[str]:
    """Discover all FRED series IDs via releases and categories.

    This is the single source of truth for FRED series discovery.
    Both the tap's wildcard mode and external orchestration tools
    should call this function (directly or via the CLI entry point).

    Fails closed: if any release or category cannot be fully traversed
    after retries, raises DiscoveryIncompleteError rather than returning
    a partial result. Callers should not cache partial output.

    Parameters
    ----------
    api_key : str
        FRED API key.
    api_url : str
        FRED API base URL.
    rate_limit_rpm : int
        Max requests per minute (default 60); sets the min interval between calls.
    jitter_seconds : float
        Max random buffer (seconds) added before every call on top of the min interval.
        FRED is strict, so jitter de-periodizes the stream and de-syncs concurrent keys.

    Returns
    -------
    list[str]
        Sorted, deduplicated list of all discoverable FRED series IDs.

    Raises
    ------
    DiscoveryIncompleteError
        If any resource could not be fetched after retries.
    """
    throttle = Throttle(
        min_interval=60.0 / max(rate_limit_rpm, 1), jitter_seconds=jitter_seconds
    )

    logger.info("Discovering FRED series IDs via releases + categories...")

    # Phase 1: releases
    series_ids, release_failures = _discover_via_releases(api_url, api_key, throttle)
    release_count = len(series_ids)
    logger.info(f"Release-based discovery: {release_count} unique series")

    # Phase 2: categories
    cat_ids, cat_failures = _discover_via_categories(api_url, api_key, throttle)
    new_from_categories = len(cat_ids - series_ids)
    series_ids |= cat_ids
    logger.info(
        f"Category-based discovery: {new_from_categories} additional series not found via releases"
    )

    # Fail closed: do not return partial results
    all_failures = release_failures + cat_failures
    if all_failures:
        raise DiscoveryIncompleteError(all_failures, len(series_ids))

    result = sorted(series_ids)
    logger.info(
        f"Total unique series discovered: {len(result)} ({throttle.calls} API requests)"
    )
    return result


def main() -> None:
    """CLI entry point: outputs JSON array of series IDs to stdout."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Discover all FRED series IDs (releases + categories BFS)",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("FRED_API_KEY"),
        help="FRED API key (default: $FRED_API_KEY env var)",
    )
    parser.add_argument(
        "--api-url",
        default="https://api.stlouisfed.org/fred",
        help="FRED API base URL",
    )
    parser.add_argument(
        "--rate-limit-rpm",
        type=int,
        default=60,
        help="Max requests per minute (default: 60)",
    )
    parser.add_argument(
        "--jitter-seconds",
        type=float,
        default=float(os.environ.get("TAP_FRED_THROTTLE_JITTER_SECONDS", "1.0")),
        help="Max random buffer (seconds) before every call (default: $TAP_FRED_THROTTLE_JITTER_SECONDS or 1.0)",
    )
    args = parser.parse_args()

    if not args.api_key:
        parser.error("--api-key is required (or set FRED_API_KEY env var)")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,  # Logs to stderr, JSON to stdout
    )

    try:
        series_ids = discover_all_series_ids(
            api_key=args.api_key,
            api_url=args.api_url,
            rate_limit_rpm=args.rate_limit_rpm,
            jitter_seconds=args.jitter_seconds,
        )
    except DiscoveryIncompleteError as e:
        logger.error(str(e))
        sys.exit(1)

    # JSON to stdout — parseable by callers
    sys.stdout.write(json.dumps(series_ids))
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
