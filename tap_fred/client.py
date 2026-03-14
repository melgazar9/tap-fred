"""REST client handling, including FREDStream base class. v2024.09.04"""

from __future__ import annotations

import logging
import random
import re
import time
import typing as t
from abc import ABC

import backoff
import jsonpath_ng
import requests
from singer_sdk.helpers.types import Context
from singer_sdk.streams import RESTStream

from tap_fred.helpers import clean_json_keys, coerce_str_to_bool, generate_surrogate_key


class FREDStream(RESTStream, ABC):
    """FRED stream class."""

    records_jsonpath = "$.observations[*]"
    rest_method = "GET"
    _add_surrogate_key = False
    _paginate = False  # Set to True in streams that need pagination

    def __init__(self, tap) -> None:
        super().__init__(tap)
        self._max_requests_per_minute = int(
            self.config.get("max_requests_per_minute", 60)
        )
        self._min_interval = float(self.config.get("min_throttle_seconds", 1.0))
        # Use shared rate limiter from tap so all streams respect the same budget
        self._throttle_lock = tap._shared_throttle_lock
        self._request_timestamps = tap._shared_request_timestamps
        self._skipped_partitions: list[dict] = []  # Track skipped partitions per stream

        # Initialize configurable parameters
        self.query_params = {}
        self._parse_config_params()

    @property
    def url_base(self) -> str:
        """Return the API URL root."""
        return self.config["api_url"]

    @property
    def authenticator(self):
        """FRED uses API key in URL params, not headers."""
        return None

    @property
    def http_headers(self) -> dict:
        """Return HTTP headers."""
        headers = {}
        if user_agent := self.config.get("user_agent"):
            headers["User-Agent"] = user_agent
        return headers

    @staticmethod
    def redact_api_key(msg):
        return re.sub(r"(api_key=)[^&\s]+", r"\1<REDACTED>", msg)

    def _throttle(self) -> None:
        """Throttle requests using sliding window rate limiting to enforce max requests per minute.

        This implementation:
        1. Tracks request timestamps in a sliding 60-second window
        2. Removes old requests outside the window
        3. Waits if we've hit the rate limit
        4. Also enforces minimum interval between requests
        """
        with self._throttle_lock:
            now = time.time()
            window_start = now - 60.0  # 60-second sliding window

            # Remove old request timestamps outside the window
            while (
                self._request_timestamps and self._request_timestamps[0] < window_start
            ):
                self._request_timestamps.popleft()

            # Check if we've hit the rate limit
            if len(self._request_timestamps) >= self._max_requests_per_minute:
                # Calculate how long to wait until oldest request falls outside window
                oldest_request = self._request_timestamps[0]
                wait_time = oldest_request + 60.0 - now
                if wait_time > 0:
                    logging.info(
                        f"Rate limit reached ({self._max_requests_per_minute}/min). Waiting {wait_time:.1f}s"
                    )
                    time.sleep(wait_time + random.uniform(0.1, 0.5))  # Add small jitter
                    now = time.time()

            # Also enforce minimum interval between consecutive requests
            if self._request_timestamps:
                last_request = self._request_timestamps[-1]
                min_wait = last_request + self._min_interval - now
                if min_wait > 0:
                    time.sleep(min_wait + random.uniform(0, 0.1))
                    now = time.time()

            # Record this request timestamp
            self._request_timestamps.append(now)

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException,),
        base=5,
        max_value=300,
        jitter=backoff.full_jitter,
        max_tries=5,
        max_time=300,  # Allow enough time for 429 rate-limit recovery (60s waits)
        giveup=lambda e: (
            # Give up on HTTP errors except for specific retryable ones
            isinstance(e, requests.exceptions.HTTPError)
            and e.response is not None
            and (
                400 <= e.response.status_code <= 599  # Give up on all HTTP errors
                and e.response.status_code != 429  # Except rate limits
                and e.response.status_code != 500  # Except internal server error
                and e.response.status_code != 502  # Except bad gateway
                and e.response.status_code != 503  # Except service unavailable
                and e.response.status_code != 504  # Except gateway timeout
            )
        ),
        on_backoff=lambda details: logging.warning(
            f"API request failed, retrying in {details['wait']:.1f}s "
            f"(attempt {details['tries']}): {details['exception']}"
        ),
    )
    def _make_request(self, url: str, params: dict) -> dict:
        """Single centralized method for all API requests."""
        log_url = self.redact_api_key(url)
        log_params = {
            k: ("<REDACTED>" if k == "api_key" else v) for k, v in params.items()
        }
        # Enhanced logging for point-in-time debugging
        if "realtime_start" in params or "realtime_end" in params:
            vintage_info = []
            if "realtime_start" in params:
                vintage_info.append(f"realtime_start={params['realtime_start']}")
            if "realtime_end" in params:
                vintage_info.append(f"realtime_end={params['realtime_end']}")
            vintage_str = ", ".join(vintage_info)
            logging.info(
                f"Stream {self.name}: Point-in-time API call [{vintage_str}] -> {log_url} with params: {log_params}"
            )
        else:
            logging.info(
                f"Stream {self.name}: Requesting: {log_url} with params: {log_params}"
            )

        try:
            self._throttle()
            response = self.requests_session.get(url, params=params, timeout=(20, 60))
            response.raise_for_status()
            data = response.json()

            if "error_code" in data:
                error_msg = f"FRED API Error {data['error_code']}: {data.get('error_message', 'Unknown error')}"
                logging.error(error_msg)
                raise requests.exceptions.HTTPError(error_msg)

            return data

        except requests.exceptions.RequestException as e:
            redacted_url = self.redact_api_key(str(e.request.url if e.request else url))

            # Handle timeout exceptions - let backoff retry them
            if isinstance(
                e,
                (
                    requests.exceptions.ReadTimeout,
                    requests.exceptions.ConnectTimeout,
                    requests.exceptions.Timeout,
                ),
            ):
                logging.warning(
                    f"Request timeout for {redacted_url}, will retry if attempts remain"
                )
                raise e

            if isinstance(e, requests.exceptions.HTTPError) and e.response is not None:
                status_code = e.response.status_code

                if status_code == 429:
                    # Rate limited — wait for FRED's 60-second window to reset, then retry
                    logging.warning(
                        f"Rate limited (429) for {redacted_url}. "
                        f"Waiting 60s for rate window to reset."
                    )
                    time.sleep(60)
                    raise e

                if status_code >= 500:
                    logging.warning(
                        f"Server error {status_code} for {redacted_url}, will retry if attempts remain"
                    )
                    raise e

                # Non-retryable client errors (400-499 except 429)
                response_body = e.response.text[:200] if e.response.text else "no body"
                logging.warning(
                    f"Client error {status_code} for {redacted_url}: "
                    f"{response_body}. Skipping - data may be missing."
                )
                self._skipped_partitions.append(
                    {
                        "stream": self.name,
                        "url": redacted_url,
                        "status_code": status_code,
                    }
                )
                if self.config.get("strict_mode", False):
                    raise
                return {}

            error_message = (
                f"{e.response.status_code} Client Error: {e.response.reason} for url: {redacted_url}"
                if e.response and hasattr(e, "request")
                else self.redact_api_key(str(e))
            )
            raise requests.exceptions.HTTPError(error_message)

    def _fetch_with_retry(self, url: str, query_params: dict) -> dict:
        """Fetch data with retry logic and rate limiting."""
        return self._make_request(url, query_params)

    def get_url(self, context: Context | None = None) -> str:
        return f"{self.url_base}{self.path}"

    def _safe_partition_extraction(
        self, generator: t.Iterable[dict], resource_id: str, resource_id_key: str
    ) -> t.Iterable[dict]:
        """Safely extract records from a partition with graceful error handling.

        Wraps partition extraction to log errors and continue instead of
        failing the entire sync when a single partition has issues.
        """
        try:
            yield from generator
        except requests.exceptions.HTTPError as e:
            status_code = (
                e.response.status_code
                if hasattr(e, "response") and e.response
                else "unknown"
            )
            self.logger.error(
                f"Failed to extract {resource_id_key}={resource_id} after retries "
                f"(HTTP {status_code}). Skipping this partition and continuing with others."
            )
            self.logger.debug(f"Error details for {resource_id}: {e!s}")
            self._skipped_partitions.append(
                {
                    "stream": self.name,
                    "partition_key": resource_id_key,
                    "partition_value": resource_id,
                    "error": f"HTTP {status_code}",
                }
            )
            if self.config.get("strict_mode", False):
                raise
        except RuntimeError:
            # Data leakage guards raise RuntimeError — ALWAYS propagate.
            # Silently swallowing a data leakage error would corrupt
            # backtesting results.  This is never a recoverable error.
            raise
        except Exception as e:
            self.logger.error(
                f"Unexpected error extracting {resource_id_key}={resource_id}: {type(e).__name__}. "
                f"Skipping this partition and continuing with others."
            )
            self.logger.debug(f"Error details for {resource_id}: {e!s}")
            self._skipped_partitions.append(
                {
                    "stream": self.name,
                    "partition_key": resource_id_key,
                    "partition_value": resource_id,
                    "error": type(e).__name__,
                }
            )
            if self.config.get("strict_mode", False):
                raise

    def finalize_state_progress_markers(self, state: dict | None = None) -> None:
        """Emit aggregated skip summary after all partitions have been synced."""
        super().finalize_state_progress_markers(state)
        if self._skipped_partitions:
            self.logger.warning(
                f"Stream '{self.name}' completed with "
                f"{len(self._skipped_partitions)} skipped partition(s):"
            )
            for skip_info in self._skipped_partitions:
                self.logger.warning(f"  - {skip_info}")

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Retrieve records from the API - uses pagination if _paginate flag is True."""
        # Check if this is a partition-based stream
        resource_id_key = getattr(self, "_resource_id_key", None)
        is_partition = context and resource_id_key and resource_id_key in context

        def extract_records():
            if self._paginate:
                yield from self._paginate_records(context)
            else:
                url = self.get_url()
                params = self.query_params.copy()
                if context:
                    params.update(context)

                response_data = self._make_request(url, params)
                jsonpath_expr = jsonpath_ng.parse(self.records_jsonpath)
                matches = [match.value for match in jsonpath_expr.find(response_data)]

                for record in matches:
                    record = self.post_process(record, context)
                    yield record

        # Wrap in safe extraction if partition, otherwise extract directly
        if is_partition:
            resource_id = context.get(resource_id_key)
            yield from self._safe_partition_extraction(
                extract_records(), resource_id, resource_id_key
            )
        else:
            yield from extract_records()

    def _fetch_and_process_records(
        self, url: str, params: dict, context: Context | None
    ) -> t.Iterable[dict]:
        """Common method to fetch data and process records with consistent handling."""
        response_data = self._make_request(url, params)
        records = response_data.get(self._get_records_key(), [])

        for record in records:
            yield self.post_process(record, context)

    def _get_records_key(self) -> str:
        """Override in subclasses to specify the JSON key containing records."""
        # Default implementation - subclasses should override this
        return "data"

    # Fields that FRED returns as strings but should be integers
    _INTEGER_FIELDS = frozenset(
        {
            "id",
            "parent_id",
            "category_id",
            "release_id",
            "source_id",
            "popularity",
            "series_count",
            "group_popularity",
        }
    )

    def post_process(self, record: dict, context: Context | None = None) -> dict:
        """Transform raw FRED API response record into schema-compliant format.

        Handles: snake_case keys, FRED missing-value marker ("."), type coercion
        for integer fields, press_release boolean, and partition context injection.
        """
        record = clean_json_keys(record)

        # Replace FRED's "." missing-value marker with None
        record = {k: (None if v == "." else v) for k, v in record.items()}

        # Coerce known integer fields
        for field in self._INTEGER_FIELDS & record.keys():
            if record[field] is not None:
                try:
                    record[field] = int(record[field])
                except (ValueError, TypeError):
                    pass

        # Coerce FRED's string booleans (e.g., press_release: "true" -> True)
        if "press_release" in record:
            record["press_release"] = coerce_str_to_bool(record["press_release"])

        # Inject partition context IDs
        if context:
            for key in ("category_id", "release_id", "source_id"):
                if key in context and context[key] is not None:
                    record[key] = int(context[key])
            if "series_id" in context and context["series_id"] is not None:
                record["series_id"] = str(context["series_id"])

        if self._add_surrogate_key:
            record["surrogate_key"] = generate_surrogate_key(record)

        return record

    def _parse_config_params(self) -> None:
        """Parse configurable parameters from config."""
        # Get stream-specific config first, then fall back to defaults
        stream_params = self.config.get(f"{self.name}_params")
        default_params = self.config.get("default_params")

        # Use stream-specific params if available, otherwise defaults
        cfg_params = stream_params if stream_params is not None else default_params

        if isinstance(cfg_params, list):
            cfg_params = cfg_params[0] if cfg_params else {}
        if not isinstance(cfg_params, dict):
            cfg_params = {}

        self.query_params = cfg_params.get("query_params", {}).copy()

        # Always ensure api_key is in query_params
        self.query_params["api_key"] = self.config["api_key"]
        self.query_params["file_type"] = "json"

        # Map start_date config to observation_start API param for series_observations
        start_date = self.config.get("start_date")
        if start_date and self.name == "series_observations":
            self.query_params["observation_start"] = self._format_date(start_date)

        # Add realtime parameters based on data_mode
        self._add_realtime_params()

    @staticmethod
    def _format_date(date_value) -> str:
        """Format date parameter consistently."""
        if hasattr(date_value, "strftime"):
            return date_value.strftime("%Y-%m-%d")
        return str(date_value)[:10]

    def _add_realtime_params(self) -> None:
        """Add ALFRED realtime parameters for point-in-time accuracy."""
        data_mode = self.config.get("data_mode", "FRED")

        if data_mode == "ALFRED":
            realtime_start = self.config.get("realtime_start")
            realtime_end = self.config.get("realtime_end")

            if realtime_start:
                self.query_params["realtime_start"] = realtime_start

            if realtime_end:
                self.query_params["realtime_end"] = realtime_end
            elif realtime_start:
                # If only start date provided, use same date for end (point-in-time)
                self.query_params["realtime_end"] = realtime_start

        # Skip realtime params for series_vintage_dates to avoid API errors
        if self.name == "series_vintage_dates":
            self.query_params.pop("realtime_start", None)
            self.query_params.pop("realtime_end", None)

    def _paginate_records(self, context: Context | None) -> t.Iterable[dict]:
        """Shared pagination logic for FRED streams that use offset/limit.

        Captures the API-reported ``count`` from the first page response and
        validates extracted totals against it.  Handles three failure modes:
          1. API offset cap (e.g. /series/search hard-caps at 5 000)
          2. Transient page-level errors (429 / 500 mid-pagination)
          3. Approximate ``count`` values on tag-related endpoints
        """
        url = self.get_url()
        offset = 0
        limit = self._get_pagination_limit()
        total_yielded = 0
        api_reported_count: int | None = None

        while True:
            params = self.query_params.copy()
            if context:
                params.update(context)
            params.update({"limit": limit, "offset": offset})

            # --- fetch one page -------------------------------------------------
            try:
                response_data = self._make_request(url, params)
            except requests.exceptions.HTTPError as page_err:
                # Detect FRED's undocumented offset cap
                # (e.g. /series/search returns 400 at offset > 4000)
                resp = getattr(page_err, "response", None)
                body = resp.text[:300] if resp is not None and resp.text else ""
                if (
                    resp is not None
                    and resp.status_code == 400
                    and "maximum" in body.lower()
                ):
                    self.logger.warning(
                        f"Stream '{self.name}': API offset cap reached at offset={offset} "
                        f"({total_yielded} records extracted). Response: {body.strip()}"
                    )
                    break
                # Any other page-level error
                self.logger.error(
                    f"Stream '{self.name}': Page fetch failed at offset={offset} "
                    f"after retries ({page_err}). "
                    f"Extracted {total_yielded} records before failure."
                )
                if self.config.get("strict_mode", False):
                    raise
                break
            except Exception as page_err:
                self.logger.error(
                    f"Stream '{self.name}': Unexpected error at offset={offset} "
                    f"({type(page_err).__name__}: {page_err}). "
                    f"Extracted {total_yielded} records before failure."
                )
                if self.config.get("strict_mode", False):
                    raise
                break

            # Empty dict means _make_request handled a 4xx in permissive mode
            if not response_data:
                self.logger.warning(
                    f"Stream '{self.name}': Empty response at offset={offset}. "
                    f"Extracted {total_yielded} records before stopping."
                )
                break

            # --- capture API-reported total from the first page -----------------
            if api_reported_count is None:
                api_reported_count = response_data.get("count")
                if api_reported_count is not None:
                    self.logger.info(
                        f"Stream '{self.name}': API reports {api_reported_count} "
                        f"total records available."
                    )

            # --- yield records from this page -----------------------------------
            records = response_data.get(self._get_records_key(), [])
            page_yielded = 0
            for record in records:
                yield self.post_process(record, context)
                page_yielded += 1
            total_yielded += page_yielded

            if page_yielded == 0 or page_yielded < limit:
                break

            offset += limit

        # --- completeness validation --------------------------------------------
        self._log_pagination_completeness(total_yielded, api_reported_count)

    def _log_pagination_completeness(
        self, total_yielded: int, api_reported_count: int | None
    ) -> None:
        """Compare extracted record count against API-reported total.

        The FRED API ``count`` field is *approximate* on tag-related endpoints
        and *exact* on most others.  A shortfall does not always indicate data
        loss — it may be an API offset cap or a stale ``count`` cache.
        """
        if api_reported_count is None:
            return

        if total_yielded >= api_reported_count:
            self.logger.info(
                f"Stream '{self.name}': Pagination complete — "
                f"{total_yielded}/{api_reported_count} records extracted."
            )
            return

        shortfall = api_reported_count - total_yielded
        pct = (shortfall / api_reported_count) * 100 if api_reported_count else 0
        self.logger.warning(
            f"Stream '{self.name}': Extracted {total_yielded}/{api_reported_count} "
            f"records ({shortfall} fewer, {pct:.1f}% gap). Possible causes: "
            f"API pagination offset cap, approximate count field, or data "
            f"changed during extraction."
        )

    def _get_pagination_limit(self) -> int:
        """Get appropriate pagination limit based on endpoint and FRED API documentation."""
        # Stream-specific limits from config
        stream_limit_key = f"{self.name}_limit"
        if stream_limit_key in self.config:
            return int(self.config[stream_limit_key])

        # Endpoint-specific defaults based on FRED API documentation
        endpoint_defaults = {
            "releases": 1000,
            "sources": 1000,
            "tags": 1000,
            "series_search": 1000,
            "series_observations": 100000,  # Special case - much higher limit
        }

        for endpoint_prefix, default_limit in endpoint_defaults.items():
            if self.name.startswith(endpoint_prefix):
                return default_limit

        # General default for other endpoints
        return 1000


class TreeTraversalFREDStream(FREDStream):
    """Base class for FRED streams that traverse hierarchical tree structures."""

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Traverse tree structure starting from root nodes."""
        url = self.get_url()
        processed_nodes = set()
        nodes_to_process = self._get_root_nodes()

        while nodes_to_process:
            node_id = nodes_to_process.pop(0)

            if node_id in processed_nodes:
                continue

            params = self.query_params.copy()
            params.update(self._get_node_params(node_id))

            for record in self._fetch_and_process_records(url, params, context):
                yield record

                child_id = self._get_child_id(record)
                if child_id and child_id not in processed_nodes:
                    nodes_to_process.append(child_id)

            processed_nodes.add(node_id)

    def _get_root_nodes(self) -> list[int]:
        """Return starting node IDs for tree traversal."""
        category_ids = self.config.get("category_ids", [])

        # Handle empty or None
        if not category_ids:
            return [0]  # FRED root category

        # Handle wildcard
        if "*" in category_ids:
            return [0]

        # Handle list of specific IDs
        try:
            return [int(cid) for cid in category_ids]
        except (ValueError, TypeError):
            logging.warning(
                f"Invalid category_ids {category_ids}, using root category 0"
            )
            return [0]

    @staticmethod
    def _get_node_params(node_id) -> dict:
        """Override to specify URL parameters for accessing a node."""
        return {"category_id": node_id}

    @staticmethod
    def _get_child_id(record: dict):
        """Override to extract child ID from a record for further traversal."""
        return record.get("id")


class SeriesBasedFREDStream(FREDStream):
    """Base class for FRED streams that operate on series data."""

    _resource_id_key = "series_id"  # Generic resource ID key for series-based streams

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Process records for each configured series ID."""
        series_ids = self._get_series_ids()

        for series_id in series_ids:
            yield from self._get_series_records(series_id, context)

    def _get_series_ids(self) -> list[str]:
        """Get series IDs from tap configuration."""
        cached_series = self._tap.get_cached_series_ids()
        return [item["series_id"] for item in cached_series]

    def _get_series_records(
        self, series_id: str, context: Context | None
    ) -> t.Iterable[dict]:
        """Override to implement series-specific record retrieval."""
        raise NotImplementedError("Subclasses must implement _get_series_records")


class ReleaseBasedFREDStream(FREDStream):
    """Base class for all release-related streams."""

    _resource_id_key = "release_id"  # Generic resource ID key for release-based streams

    @property
    def partitions(self):
        """Generate partitions from cached release IDs."""
        cached_ids = self._tap.get_cached_release_ids()
        return [{"release_id": item["release_id"]} for item in cached_ids]


class CategoryBasedFREDStream(FREDStream):
    """Base class for all category-related streams."""

    _resource_id_key = (
        "category_id"  # Generic resource ID key for category-based streams
    )

    @property
    def partitions(self):
        """Generate partitions from cached category IDs."""
        cached_ids = self._tap.get_cached_category_ids()
        return [{"category_id": item["category_id"]} for item in cached_ids]


class SourceBasedFREDStream(FREDStream):
    """Base class for all source-related streams."""

    _resource_id_key = "source_id"  # Generic resource ID key for source-based streams

    @property
    def partitions(self):
        """Generate partitions from cached source IDs."""
        cached_ids = self._tap.get_cached_source_ids()
        return [{"source_id": item["source_id"]} for item in cached_ids]


class TagBasedFREDStream(FREDStream):
    """Base class for all tag-related streams."""

    _resource_id_key = "tag_name"  # Generic resource ID key for tag-based streams

    @property
    def partitions(self):
        """Generate partitions from cached tag names."""
        cached_ids = self._tap.get_cached_tag_names()
        return [{"tag_name": item["tag_name"]} for item in cached_ids]


class PointInTimePartitionStream(FREDStream):
    """Base class for streams with point-in-time vintage date partitioning.

    Handles the common pattern of:
    1. Getting resource IDs (series_ids, category_ids, etc.)
    2. Getting revision dates for each resource ID
    3. Creating partitions for each (resource_id, vintage_date) combination
    """

    # Subclasses must define these attributes
    _resource_type: str = None  # e.g., "series", "category", "release"
    _resource_id_key: str = None  # e.g., "series_id", "category_id", "release_id"

    @property
    def partitions(self):
        """Return partitions based on resource IDs and vintage dates (if point-in-time mode)."""
        cache_method = getattr(self._tap, f"get_cached_{self._resource_type}_ids")
        resource_ids = cache_method()
        if not resource_ids:
            return []
        if isinstance(resource_ids[0], dict):
            # Extract the ID from dict format
            resource_ids = [item[self._resource_id_key] for item in resource_ids]

        # Check if point-in-time mode is enabled
        point_in_time_mode = self.config.get("point_in_time_mode", False)
        if not point_in_time_mode:
            # Standard resource-based partitions
            return [
                {self._resource_id_key: resource_id} for resource_id in resource_ids
            ]

        # Point-in-time mode: create partitions for each (resource_id, vintage_date)
        partitions = []
        for resource_id in resource_ids:
            vintage_dates = self._get_vintage_dates_for_resource(resource_id)

            if not vintage_dates:
                # SKIP — never fall back to current data in point-in-time mode.
                # Falling back would silently introduce today's revised data into
                # a vintage dataset, contaminating backtesting results.
                self.logger.warning(
                    f"Skipping {self._resource_id_key}='{resource_id}' — no vintage "
                    f"dates in point-in-time range "
                    f"[{self.config.get('point_in_time_start', 'unbounded')}, "
                    f"{self.config.get('point_in_time_end', 'unbounded')}]. "
                    f"Will NOT fall back to current data (prevents data leakage)."
                )
                continue
            # Create partition for each vintage date
            for vintage_date in vintage_dates:
                partitions.append(
                    {
                        self._resource_id_key: resource_id,
                        "vintage_date": vintage_date,
                    }
                )
        return partitions

    def _get_vintage_dates_for_resource(self, resource_id: str) -> list[str]:
        """Get vintage dates for a specific resource ID using proper API-based caching.

        IMPORTANT: This uses generic "resource_id" field, not hardcoded "series_id".
        Works for ANY resource type (series, categories, releases, etc.) that has vintage dates.
        """
        # Use the tap's consistent caching pattern for vintage dates
        cached_vintage_dates = self._tap.get_cached_vintage_dates()

        # Filter vintage dates for THIS specific resource only (avoid cross-product)
        # Uses generic "resource_id" field from cache, not hardcoded "series_id"
        vintage_dates = [
            item["vintage_date"]
            for item in cached_vintage_dates
            if item.get("resource_id") == resource_id
        ]

        # Apply point_in_time_start and point_in_time_end filtering
        point_in_time_start = self.config.get("point_in_time_start")
        point_in_time_end = self.config.get("point_in_time_end")

        if point_in_time_start:
            vintage_dates = [d for d in vintage_dates if d >= point_in_time_start]

        if point_in_time_end:
            vintage_dates = [d for d in vintage_dates if d <= point_in_time_end]

        return sorted(vintage_dates)

    def _get_resource_records(
        self, resource_id: str, context: Context | None
    ) -> t.Iterable[dict]:
        """Retrieve records for a specific resource ID."""
        url = self.get_url()
        params = self.query_params.copy()
        params.update(
            {
                self._resource_id_key: resource_id,
                "sort_order": "asc",
            }
        )

        # Handle point-in-time mode with vintage-specific queries
        if context and "vintage_date" in context:
            vintage_date = context["vintage_date"]
            # Set both realtime_start and realtime_end to the specific vintage date
            params["realtime_start"] = vintage_date
            params["realtime_end"] = vintage_date
            self.logger.info(
                f"Point-in-time query for {resource_id} using vintage date {vintage_date}"
            )

        response_data = self._fetch_with_retry(url, params)

        if not response_data:
            self.logger.warning(
                f"Empty API response for {self._resource_id_key}='{resource_id}' "
                f"(likely a 4xx error in permissive mode). No records extracted."
            )
            return

        records_key = self._get_records_key()
        records = response_data.get(records_key, [])

        for record in records:
            record[self._resource_id_key] = resource_id
            # Add vintage_date to record for tracking
            if context and "vintage_date" in context:
                record["vintage_date"] = context["vintage_date"]
            record = self.post_process(record, context)

            # Runtime data-leakage guard: in point-in-time mode, every record's
            # realtime_start MUST equal the requested vintage_date.  A mismatch
            # means the FRED API returned data from a different vintage — this
            # would silently corrupt backtesting results.
            if context and "vintage_date" in context:
                expected = str(context["vintage_date"])[:10]
                actual = str(record.get("realtime_start", ""))[:10]
                if actual != expected:
                    raise RuntimeError(
                        f"DATA LEAKAGE: {self._resource_id_key}='{resource_id}' "
                        f"vintage_date='{expected}' but record has "
                        f"realtime_start='{actual}'. Aborting to prevent "
                        f"data contamination."
                    )

            yield record

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Retrieve records for a specific resource ID from context (partition support).

        Point-in-time optimization: vintage date partitions contain immutable
        data (FRED never revises a historical vintage snapshot).  When a
        partition already has a bookmark from a previous sync we skip the API
        call entirely — the Singer SDK preserves the existing bookmark even
        when zero records are yielded.
        """
        if context is None or self._resource_id_key not in context:
            yield from super().get_records(context)
            return

        # Skip already-synced vintage partitions (immutable data)
        # SAFETY: In PIT mode, all records share the same realtime_start
        # (= vintage_date), so partial extraction still sets bookmark =
        # vintage_date. If extraction fails BEFORE any records are emitted,
        # no bookmark exists and the partition retries on next run.
        # IMPORTANT: Check state["replication_key_value"] directly, NOT
        # get_starting_replication_key_value() which falls back to start_date
        # config and would incorrectly skip all new partitions.
        if context.get("vintage_date"):
            state = self.get_context_state(context)
            bookmark = state.get("replication_key_value")
            if bookmark is not None:
                self.logger.debug(
                    f"Skipping completed partition {self._resource_id_key}="
                    f"{context[self._resource_id_key]}, vintage_date="
                    f"{context['vintage_date']} (bookmark={bookmark})"
                )
                return

        resource_id = context[self._resource_id_key]
        yield from self._safe_partition_extraction(
            self._get_resource_records(resource_id, context),
            resource_id,
            self._resource_id_key,
        )
