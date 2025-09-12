"""REST client handling, including FREDStream base class. v2024.09.04"""

from __future__ import annotations

import logging
import random
import re
import threading
import time
import typing as t
from abc import ABC
from collections import deque
import backoff
import jsonpath_ng
import requests
from singer_sdk.helpers.types import Context
from singer_sdk.streams import RESTStream

from tap_fred.helpers import clean_json_keys, generate_surrogate_key


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
        self._throttle_lock = threading.Lock()
        self._request_timestamps = (
            deque()
        )  # Track request timestamps for sliding window

        # Initialize configurable parameters
        self.path_params = {}
        self.query_params = {}
        self.other_params = {}
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
        """
        Throttle requests using sliding window rate limiting to enforce max requests per minute.

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
        max_tries=3,  # Reduced from 12 - give up faster
        max_time=30,  # Reduced from 1800 - give up faster
        giveup=lambda e: (
            # Give up on HTTP errors except for specific retryable ones
            isinstance(e, requests.exceptions.HTTPError)
            and e.response is not None
            and (
                400 <= e.response.status_code <= 599  # Give up on all HTTP errors
                and e.response.status_code != 429  # Except rate limits
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
        """Single centralized method for all API requests - DRY principle."""
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

            if isinstance(e, requests.exceptions.HTTPError) and e.response is not None:
                status_code = e.response.status_code

                if status_code >= 500:
                    logging.warning(
                        f"Server error {status_code} for {redacted_url}, skipping"
                    )
                    return {}

                if 400 <= status_code < 500 and status_code not in [429, 502, 503, 504]:
                    logging.warning(
                        f"Client error {status_code} for {redacted_url}, skipping"
                    )
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

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Retrieve records from the API - uses pagination if _paginate flag is True."""
        if self._paginate:
            yield from self._paginate_records(context)
        else:
            # Single API call for simple streams
            url = self.get_url()
            params = self.query_params.copy()
            if context:
                params.update(context)

            # Ensure ALFRED parameters are added for all streams
            self._add_alfred_params(params, context)
            response_data = self._make_request(url, params)

            # Extract records using JSONPath
            jsonpath_expr = jsonpath_ng.parse(self.records_jsonpath)
            matches = [match.value for match in jsonpath_expr.find(response_data)]

            for record in matches:
                record = self.post_process(record, context)
                yield record

    def _fetch_and_process_records(
        self, url: str, params: dict, context: Context | None
    ) -> t.Iterable[dict]:
        """Common method to fetch data and process records with consistent handling."""
        self._add_alfred_params(params, context)
        response_data = self._make_request(url, params)
        records = response_data.get(self._get_records_key(), [])

        for record in records:
            yield self.post_process(record, context)

    def _get_records_key(self) -> str:
        """Override in subclasses to specify the JSON key containing records."""
        # Default implementation - subclasses should override this
        return "data"

    def post_process(self, record: dict, context: Context | None = None) -> dict:
        """Transform raw data to match expected structure."""
        # Clean JSON keys to snake_case
        record = clean_json_keys(record)

        for key, value in record.items():
            if value == ".":
                record[key] = None

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

        if cfg_params is None:
            cfg_params = {}

        # Support both single dict and list of dicts (use first item for now)
        if isinstance(cfg_params, list) and len(cfg_params) > 0:
            cfg_params = cfg_params[0]
        elif isinstance(cfg_params, list):
            cfg_params = {}

        if isinstance(cfg_params, dict):
            self.path_params = cfg_params.get("path_params", {}).copy()
            self.query_params = cfg_params.get("query_params", {}).copy()
            self.other_params = cfg_params.get("other_params", {}).copy()

        # Always ensure api_key is in query_params
        self.query_params["api_key"] = self.config["api_key"]
        self.query_params["file_type"] = "json"

        # Add realtime parameters based on data_mode
        self._add_realtime_params()

    def _is_stream_enabled(self) -> bool:
        """Check if this stream should be enabled."""
        # Check stream type-based enabling
        if self.name.startswith(("series", "series_")):
            if not self.config.get("enable_series_streams", True):
                return False
        elif self.name.startswith(("category", "categories")):
            if not self.config.get("enable_metadata_streams", True):
                return False
        elif self.name.startswith(("release", "releases")):
            if not self.config.get("enable_metadata_streams", True):
                return False
        elif self.name.startswith(("source", "sources", "tag", "tags", "related_tag")):
            if not self.config.get("enable_metadata_streams", True):
                return False
        elif self.name.startswith("geofred"):
            if not self.config.get("enable_geofred_streams", True):
                return False

        # Check specific ID-based filtering
        if self.name.startswith("category") and "category_ids" in self.config:
            category_ids = self.config.get("category_ids")
            if category_ids and category_ids != ["*"]:
                # This stream should filter based on specific category IDs
                # Implementation would depend on specific stream needs
                pass

        if (
            self.name.startswith(("release", "releases"))
            and "release_ids" in self.config
        ):
            release_ids = self.config.get("release_ids")
            if release_ids and release_ids != ["*"]:
                # This stream should filter based on specific release IDs
                pass

        if self.name.startswith(("source", "sources")) and "source_ids" in self.config:
            source_ids = self.config.get("source_ids")
            if source_ids and source_ids != ["*"]:
                # This stream should filter based on specific source IDs
                pass

        if self.name.startswith(("tag", "tags")) and "tag_names" in self.config:
            tag_names = self.config.get("tag_names")
            if tag_names and tag_names != ["*"]:
                # This stream should filter based on specific tag names
                pass

        return True

    def _format_date(self, date_value) -> str:
        """Format date parameter consistently."""
        if hasattr(date_value, "strftime"):
            return date_value.strftime("%Y-%m-%d")
        return str(date_value)[:10]

    def _add_alfred_params(self, params: dict, context: dict = None) -> None:
        """Add ALFRED realtime parameters to params if in ALFRED mode.

        Parameter Usage:
        - realtime_start/realtime_end: Global config used for regular ALFRED mode
        - point_in_time_start/point_in_time_end: Filter vintage dates when point_in_time_mode=true
        - vintage_date in context: Individual partition vintage date (overrides global config)
        """
        # Handle point-in-time mode with vintage dates from context
        if context and "vintage_date" in context:
            # Point-in-time mode: use vintage_date for realtime parameters (ALFRED mode)
            vintage_date = context["vintage_date"]
            params.update(
                {
                    "realtime_start": vintage_date,
                    "realtime_end": vintage_date,
                }
            )
            return

        # Regular ALFRED mode: use global config
        if self.config["data_mode"].upper() != "ALFRED":
            return

        realtime_start = self.config.get("realtime_start")  # Optional
        realtime_end = self.config.get("realtime_end")  # Optional

        if realtime_start:
            params["realtime_start"] = self._format_date(realtime_start)
        if realtime_end:
            params["realtime_end"] = self._format_date(realtime_end)
        elif realtime_start:
            params["realtime_end"] = params["realtime_start"]

    def _add_realtime_params(self) -> None:
        """Add realtime parameters for FRED vs ALFRED mode."""
        data_mode = self.config["data_mode"].upper()

        if data_mode == "ALFRED":
            self._add_alfred_params(self.query_params)
            if self.config.get("realtime_start") and not self.config.get(
                "point_in_time_mode"
            ):
                realtime_end = (
                    self.config.get("realtime_end") or self.config["realtime_start"]
                )
                logging.info(
                    f"ALFRED mode: Using vintage data from {self.config['realtime_start']} to {realtime_end}"
                )
            elif self.config.get("point_in_time_mode"):
                logging.info(
                    "ALFRED mode: Point-in-time partitions will set vintage dates per partition"
                )
        else:
            logging.info("FRED mode: Using current revised data")

    def _paginate_records(self, context: Context | None) -> t.Iterable[dict]:
        """Shared pagination logic for FRED streams that use offset/limit."""
        url = self.get_url()
        offset = 0
        # Get limit from config or use endpoint-specific defaults from FRED API docs
        limit = self._get_pagination_limit()

        while True:
            params = self.query_params.copy()
            params.update(
                {
                    "limit": limit,
                    "offset": offset,
                }
            )

            records_yielded = 0
            for record in self._fetch_and_process_records(url, params, context):
                yield record
                records_yielded += 1

            if records_yielded == 0:
                break

            if records_yielded < limit:
                break

            offset += limit

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

    def _get_node_params(self, node_id) -> dict:
        """Override to specify URL parameters for accessing a node."""
        return {"category_id": node_id}

    def _get_child_id(self, record: dict):
        """Override to extract child ID from a record for further traversal."""
        return record.get("id")

    def _get_records_key(self) -> str:
        """Override to specify the JSON key containing records."""
        return "categories"


class SeriesBasedFREDStream(FREDStream):
    """Base class for FRED streams that operate on series data."""

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

    @property
    def partitions(self):
        """Generate partitions from cached release IDs."""
        cached_ids = self._tap.get_cached_release_ids()
        return [{"release_id": item["release_id"]} for item in cached_ids]


class CategoryBasedFREDStream(FREDStream):
    """Base class for all category-related streams."""

    @property
    def partitions(self):
        """Generate partitions from cached category IDs."""
        cached_ids = self._tap.get_cached_category_ids()
        return [{"category_id": item["category_id"]} for item in cached_ids]


class SourceBasedFREDStream(FREDStream):
    """Base class for all source-related streams."""

    @property
    def partitions(self):
        """Generate partitions from cached source IDs."""
        cached_ids = self._tap.get_cached_source_ids()
        return [{"source_id": item["source_id"]} for item in cached_ids]


class TagBasedFREDStream(FREDStream):
    """Base class for all tag-related streams."""

    @property
    def partitions(self):
        """Generate partitions from cached tag names."""
        cached_ids = self._tap.get_cached_tag_names()
        return [{"tag_name": item["tag_name"]} for item in cached_ids]


class PointInTimePartitionStream(FREDStream):
    """Base class for streams with point-in-time vintage date partitioning.

    This DRY base class handles the common pattern of:
    1. Getting resource IDs (series_ids, category_ids, etc.)
    2. Getting revision dates for each resource ID
    3. Creating partitions for each (resource_id, vintage_date) combination
    """

    # Subclasses must define these attributes
    _resource_type: str = None  # e.g., "series", "category", "release"
    _resource_id_key: str = None  # e.g., "series_id", "category_id", "release_id"

    @property
    def partitions(self):
        """Return partitions based on point-in-time mode or regular resource IDs."""
        if self.config.get("point_in_time_mode", False):
            logging.info(
                f"Stream {self.name}: Point-in-time mode enabled - creating vintage date partitions"
            )
            # Use revision dates for partitions
            revision_cache_method = getattr(
                self._tap, f"get_cached_{self._resource_type}_revision_dates"
            )
            revision_data = revision_cache_method()
            partitions = []

            for item in revision_data:
                resource_id = item[self._resource_id_key]
                vintage_dates_count = len(item["revision_dates"])
                logging.info(
                    f"Stream {self.name}: Resource {resource_id} has {vintage_dates_count} vintage dates"
                )
                for vintage_date in item["revision_dates"]:
                    partitions.append(
                        {
                            self._resource_id_key: resource_id,
                            "vintage_date": vintage_date,
                        }
                    )

            total_partitions = len(partitions)
            logging.info(
                f"Stream {self.name}: Created {total_partitions} point-in-time partitions total"
            )
            return partitions
        else:
            # Use regular resource-based partitions
            cache_method = getattr(self._tap, f"get_cached_{self._resource_type}_ids")
            resource_ids = cache_method()
            if isinstance(resource_ids[0], dict):
                # Extract the ID from dict format
                resource_ids = [item[self._resource_id_key] for item in resource_ids]
            return [
                {self._resource_id_key: resource_id} for resource_id in resource_ids
            ]

    def _get_resource_records(
        self, resource_id: str, context: Context | None
    ) -> t.Iterable[dict]:
        """Retrieve records for a specific resource ID, using vintage dates in point-in-time mode."""
        url = self.get_url()
        params = self.query_params.copy()
        params.update(
            {
                self._resource_id_key: resource_id,
                "sort_order": "asc",
            }
        )

        # Point-in-time mode and regular ALFRED mode handled by _add_alfred_params
        self._add_alfred_params(params, context)

        response_data = self._fetch_with_retry(url, params)
        records_key = self._get_records_key()
        records = response_data.get(records_key, [])

        for record in records:
            record[self._resource_id_key] = resource_id
            record = self.post_process(record, context)
            yield record

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Retrieve records for a specific resource ID from context (partition support)."""
        if context is None or self._resource_id_key not in context:
            # Fall back to base class behavior for non-partition usage
            yield from super().get_records(context)
            return

        if (
            self.replication_key
            and self.replication_method == "INCREMENTAL"
            and context.get("vintage_date")
        ):
            starting_replication_key = self.get_starting_replication_key_value(context)
            vintage_date = context["vintage_date"]

            # In point-in-time mode, if the vintage_date <= starting_replication_key,
            # this partition was already processed (realtime_start = vintage_date)
            if starting_replication_key and vintage_date <= starting_replication_key:
                logging.info(
                    f"Stream {self.name}: Skipping completed partition "
                    f"{context[self._resource_id_key]}@{vintage_date} "
                    f"(bookmark: {starting_replication_key})"
                )
                return

        # Use partition-specific logic
        yield from self._get_resource_records(context[self._resource_id_key], context)
