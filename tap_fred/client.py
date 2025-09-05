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
        self._min_interval = float(
            self.config.get("min_throttle_seconds", 1.0)
        )  # Default to 1 second for 60/min
        self._throttle_lock = threading.Lock()
        self._request_timestamps = (
            deque()
        )  # Track request timestamps for sliding window

        # Initialize configurable parameters like tap-fmp
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
        logging.info(
            f"Stream {self.name}: Requesting: {log_url} with params: {log_params}"
        )

        try:
            self._throttle()
            response = self.requests_session.get(
                url, params=params, timeout=(20, 60)
            )
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
        """Backwards compatibility wrapper - delegates to _make_request."""
        return self._make_request(url, query_params)

    def _add_date_filtering(self, params: dict, context: Context | None) -> None:
        """Add incremental date filtering to request parameters - only for INCREMENTAL streams."""
        # Only add date filtering for incremental streams, not FULL_TABLE streams
        if self.replication_method == "INCREMENTAL" and hasattr(
            self, "_replication_key_starting_name"
        ):
            if self.replication_key:
                replication_key_value = self.get_starting_replication_key_value(context)
                if replication_key_value:
                    params[self._replication_key_starting_name] = self._format_date(
                        replication_key_value
                    )
                elif start_date := self.config.get("start_date"):
                    params[self._replication_key_starting_name] = self._format_date(
                        start_date
                    )
            elif start_date := self.config.get("start_date"):
                params[self._replication_key_starting_name] = self._format_date(
                    start_date
                )

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
            self._add_date_filtering(params, context)

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
        self._add_alfred_params(params)
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
        """Parse configurable parameters from config like tap-fmp."""
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
            self.path_params = cfg_params.get("path_params", {})
            self.query_params = cfg_params.get("query_params", {})
            self.other_params = cfg_params.get("other_params", {})

        # Always ensure api_key is in query_params
        self.query_params["api_key"] = self.config["api_key"]
        self.query_params["file_type"] = "json"

        # Add realtime parameters based on data_mode
        self._add_realtime_params()

    def _is_stream_enabled(self) -> bool:
        """Check if this stream should be enabled based on tap-fmp style configuration."""
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
            if category_ids and category_ids != "*" and category_ids != ["*"]:
                # This stream should filter based on specific category IDs
                # Implementation would depend on specific stream needs
                pass

        if (
            self.name.startswith(("release", "releases"))
            and "release_ids" in self.config
        ):
            release_ids = self.config.get("release_ids")
            if release_ids and release_ids != "*" and release_ids != ["*"]:
                # This stream should filter based on specific release IDs
                pass

        if self.name.startswith(("source", "sources")) and "source_ids" in self.config:
            source_ids = self.config.get("source_ids")
            if source_ids and source_ids != "*" and source_ids != ["*"]:
                # This stream should filter based on specific source IDs
                pass

        if self.name.startswith(("tag", "tags")) and "tag_names" in self.config:
            tag_names = self.config.get("tag_names")
            if tag_names and tag_names != "*" and tag_names != ["*"]:
                # This stream should filter based on specific tag names
                pass

        return True

    def _format_date(self, date_value) -> str:
        """Format date parameter consistently."""
        if hasattr(date_value, "strftime"):
            return date_value.strftime("%Y-%m-%d")
        return str(date_value)[:10]

    def _add_alfred_params(self, params: dict) -> None:
        """Add ALFRED realtime parameters to params if in ALFRED mode."""
        if self.config.get("data_mode", "FRED").upper() != "ALFRED":
            return

        realtime_start = self.config.get("realtime_start")
        realtime_end = self.config.get("realtime_end")

        if realtime_start:
            params["realtime_start"] = self._format_date(realtime_start)
        if realtime_end:
            params["realtime_end"] = self._format_date(realtime_end)
        elif realtime_start:
            params["realtime_end"] = params["realtime_start"]

    def _add_realtime_params(self) -> None:
        """Add realtime parameters for FRED vs ALFRED mode."""
        data_mode = self.config.get("data_mode", "FRED").upper()

        if data_mode == "ALFRED":
            self._add_alfred_params(self.query_params)
            logging.info(
                f"ALFRED mode: Using vintage data from {self.query_params.get('realtime_start')} to "
                f"{self.query_params.get('realtime_end')}"
            )
        else:
            logging.info("FRED mode: Using current revised data")

    def _fetch_all_series_ids(self) -> list[str]:
        """Fetch ALL series IDs from FRED API when wildcard is specified."""
        logging.info("Fetching all series IDs from FRED API...")
        all_series = set()

        releases_url = f"{self.config['api_url']}/releases"
        releases_params = {
            "api_key": self.config["api_key"],
            "file_type": "json",
        }
        
        # Get limit from FRED API docs - releases endpoint allows 1-1000
        releases_params["limit"] = self.config.get("releases_limit", 1000)

        try:
            releases_data = self._make_request(releases_url, releases_params)
            releases = releases_data.get("releases", [])
            logging.info(f"Found {len(releases)} releases")

            for release in releases:
                release_id = release["id"]
                release_name = release.get("name", f"Release {release_id}")
                logging.info(
                    f"Fetching series for release {release_id}: {release_name}"
                )

                # Use pagination for series within releases
                offset = 0
                # Get limit from FRED API docs - release/series endpoint allows 1-1000
                limit = self.config.get("release_series_limit", 1000)

                while True:
                    series_url = f"{self.config['api_url']}/release/series"
                    series_params = {
                        "release_id": release_id,
                        "api_key": self.config["api_key"],
                        "file_type": "json",
                        "limit": limit,
                        "offset": offset,
                    }

                    try:
                        series_data = self._make_request(series_url, series_params)

                        if "error_code" in series_data:
                            logging.warning(
                                f"FRED API Error for release {release_id}: {series_data.get('error_message', 'Unknown error')}"
                            )
                            break

                        series_list = series_data.get("seriess", [])
                        if not series_list:
                            break

                        for series in series_list:
                            series_id = series.get("id")
                            if series_id:
                                all_series.add(series_id)

                        logging.info(
                            f"  Found {len(series_list)} series (total: {len(all_series)})"
                        )

                        if len(series_list) < limit:
                            break

                        offset += limit

                    except Exception as e:
                        logging.warning(
                            f"Failed to fetch series for release {release_id}: {e}"
                        )
                        break

        except Exception as e:
            logging.error(f"Failed to fetch releases: {e}")
            logging.info("Falling back to search method...")
            return self._fetch_all_series_via_search()

        logging.info(f"Total series IDs found: {len(all_series)}")
        return list(all_series)

    def _fetch_all_series_via_search(self) -> list[str]:
        """Fallback method to fetch series IDs via search API with pagination."""
        all_series = set()
        offset = 0
        # Get limit from FRED API docs - series/search endpoint allows 1-1000
        limit = self.config.get("series_search_limit", 1000)

        while True:
            search_url = f"{self.config['api_url']}/series/search"
            search_params = {
                "search_text": "",
                "api_key": self.config["api_key"],
                "file_type": "json",
                "limit": limit,
                "offset": offset,
            }

            try:
                search_data = self._make_request(search_url, search_params)

                if "error_code" in search_data:
                    logging.error(
                        f"FRED API Error in search: {search_data.get('error_message', 'Unknown error')}"
                    )
                    break

                series_list = search_data.get("seriess", [])
                if not series_list:
                    break

                for series in series_list:
                    series_id = series.get("id")
                    if series_id:
                        all_series.add(series_id)

                logging.info(
                    f"Search found {len(series_list)} series (total: {len(all_series)})"
                )

                if len(series_list) < limit:
                    break

                offset += limit

            except Exception as e:
                logging.warning(f"Search failed at offset {offset}: {e}")
                break

        logging.info(f"Search method found {len(all_series)} series IDs")
        return list(all_series)

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
        return self.tap.get_cached_series_ids()

    def _get_series_records(
        self, series_id: str, context: Context | None
    ) -> t.Iterable[dict]:
        """Override to implement series-specific record retrieval."""
        raise NotImplementedError("Subclasses must implement _get_series_records")
