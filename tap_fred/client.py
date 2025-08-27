"""REST client handling, including FREDStream base class."""

from __future__ import annotations

import logging
import random
import re
import threading
import time
import typing as t
from abc import ABC
from datetime import datetime
import backoff
import requests
from singer_sdk.helpers.types import Context
from singer_sdk.streams import RESTStream

from tap_fred.helpers import clean_json_keys, generate_surrogate_key


class FREDStream(RESTStream, ABC):
    """FRED stream class."""

    records_jsonpath = "$.observations[*]"
    rest_method = "GET"
    _add_surrogate_key = False

    def __init__(self, tap) -> None:
        super().__init__(tap)
        self._min_interval = float(self.config.get("min_throttle_seconds", 0.01))
        self._throttle_lock = threading.Lock()
        self._last_call_ts = 0.0

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
        """Throttle requests to prevent rate limiting."""
        with self._throttle_lock:
            now = time.time()
            wait = self._last_call_ts + self._min_interval - now
            if wait > 0:
                time.sleep(wait + random.uniform(0, 0.1))
            self._last_call_ts = now

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException,),
        base=5,
        max_value=300,
        jitter=backoff.full_jitter,
        max_tries=12,
        max_time=1800,
        giveup=lambda e: (
            isinstance(e, requests.exceptions.HTTPError)
            and e.response is not None
            and e.response.status_code not in (429, 500, 502, 503, 504)
        ),
        on_backoff=lambda details: logging.warning(
            f"API request failed, retrying in {details['wait']:.1f}s "
            f"(attempt {details['tries']}): {details['exception']}"
        ),
    )
    def _fetch_with_retry(self, url: str, query_params: dict) -> dict:
        """Centralized API call with retry logic."""
        log_url = self.redact_api_key(url)
        log_params = {
            k: ("<REDACTED>" if k == "api_key" else v) for k, v in query_params.items()
        }
        logging.info(
            f"Stream {self.name}: Requesting: {log_url} with params: {log_params}"
        )

        try:
            self._throttle()
            response = self.requests_session.get(
                url, params=query_params, timeout=(20, 60)
            )
            response.raise_for_status()

            data = response.json()

            if "error_code" in data:
                error_msg = f"FRED API Error {data['error_code']}: {data.get('error_message', 'Unknown error')}"
                logging.error(error_msg)
                raise requests.exceptions.HTTPError(error_msg)

            observations = data.get("observations", [])
            logging.info(f"Stream {self.name}: Records returned: {len(observations)}")
            return data

        except requests.exceptions.RequestException as e:
            redacted_url = self.redact_api_key(str(e.request.url if e.request else url))
            error_message = (
                f"{e.response.status_code} Client Error: {e.response.reason} for url: {redacted_url}"
                if e.response and hasattr(e, "request")
                else self.redact_api_key(str(e))
            )
            raise requests.exceptions.HTTPError(error_message)

    def _add_date_filtering(self, params: dict, context: Context | None) -> None:
        """Add incremental date filtering to request parameters."""
        if self.replication_key:
            replication_key_value = self.get_starting_replication_key_value(context)
            if replication_key_value:
                if isinstance(replication_key_value, datetime):
                    params["observation_start"] = replication_key_value.strftime(
                        "%Y-%m-%d"
                    )
                else:
                    params["observation_start"] = str(replication_key_value)
        elif start_date := self.config.get("start_date"):
            if isinstance(start_date, datetime):
                params["observation_start"] = start_date.strftime("%Y-%m-%d")
            else:
                params["observation_start"] = str(start_date)[:10]

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Retrieve records from the API."""
        url = f"{self.url_base}{self.path}"

        params = self.query_params.copy()

        self._add_date_filtering(params, context)

        response_data = self._fetch_with_retry(url, params)
        for record in response_data:
            record = self.post_process(record, context)
            yield record

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

    def _add_realtime_params(self) -> None:
        """Add realtime parameters for FRED vs ALFRED mode."""
        data_mode = self.config.get("data_mode", "FRED").upper()

        if data_mode == "ALFRED":
            # ALFRED mode: Use vintage data with specific realtime period
            realtime_start = self.config.get("realtime_start")
            realtime_end = self.config.get("realtime_end")

            if realtime_start:
                if hasattr(realtime_start, "strftime"):
                    self.query_params["realtime_start"] = realtime_start.strftime(
                        "%Y-%m-%d"
                    )
                else:
                    self.query_params["realtime_start"] = str(realtime_start)[:10]

            if realtime_end:
                if hasattr(realtime_end, "strftime"):
                    self.query_params["realtime_end"] = realtime_end.strftime(
                        "%Y-%m-%d"
                    )
                else:
                    self.query_params["realtime_end"] = str(realtime_end)[:10]

            # If only one realtime param is set, use it for both start and end (point-in-time)
            if realtime_start and not realtime_end:
                self.query_params["realtime_end"] = self.query_params["realtime_start"]
            elif realtime_end and not realtime_start:
                self.query_params["realtime_start"] = self.query_params["realtime_end"]

            logging.info(
                f"ALFRED mode: Using vintage data from {self.query_params.get('realtime_start')} to"
                f"{self.query_params.get('realtime_end')}"
            )

        else:
            # FRED mode (default): Use current revised data - no realtime params needed
            logging.info("FRED mode: Using current revised data")

    def _fetch_all_series_ids(self) -> list[str]:
        """Fetch ALL series IDs from FRED API when wildcard is specified."""
        logging.info("Fetching all series IDs from FRED API...")
        all_series = set()  # Use set to avoid duplicates

        releases_url = f"{self.config['api_url']}/releases"
        releases_params = {
            "api_key": self.config["api_key"],
            "file_type": "json",
            "limit": 1000,
        }

        try:
            self._throttle()
            response = self.requests_session.get(
                releases_url, params=releases_params, timeout=(20, 60)
            )
            response.raise_for_status()
            releases_data = response.json()

            if "error_code" in releases_data:
                error_msg = (
                    f"FRED API Error {releases_data['error_code']}: "
                    f"{releases_data.get('error_message', 'Unknown error')}"
                )
                logging.error(error_msg)
                raise requests.exceptions.HTTPError(error_msg)

            releases = releases_data.get("releases", [])
            logging.info(f"Found {len(releases)} releases")

            # For each release, get all series
            for release in releases:
                release_id = release["id"]
                release_name = release.get("name", f"Release {release_id}")
                logging.info(
                    f"Fetching series for release {release_id}: {release_name}"
                )

                # Get series for this release with pagination
                offset = 0
                limit = 1000

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
                        self._throttle()
                        response = self.requests_session.get(
                            series_url, params=series_params, timeout=(20, 60)
                        )
                        response.raise_for_status()
                        series_data = response.json()

                        # Handle FRED API errors
                        if "error_code" in series_data:
                            logging.warning(
                                f"FRED API Error for release {release_id}: {series_data.get('error_message', 'Unknown error')}"
                            )
                            break

                        series_list = series_data.get("seriess", [])
                        if not series_list:
                            break

                        # Extract series IDs
                        for series in series_list:
                            series_id = series.get("id")
                            if series_id:
                                all_series.add(series_id)

                        logging.info(
                            f"  Found {len(series_list)} series (total: {len(all_series)})"
                        )

                        # Check if we got less than limit, meaning no more pages
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
            # Fallback to search method
            logging.info("Falling back to search method...")
            return self._fetch_all_series_via_search()

        logging.info(f"Total series IDs found: {len(all_series)}")
        return list(all_series)  # Convert set back to list

    def _fetch_all_series_via_search(self) -> list[str]:
        """Fallback method to fetch series IDs via search API with pagination."""
        all_series = set()  # Use set to avoid duplicates
        offset = 0
        limit = 1000

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
                self._throttle()
                response = self.requests_session.get(
                    search_url, params=search_params, timeout=(20, 60)
                )
                response.raise_for_status()
                search_data = response.json()

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

                # Check if we got less than limit, meaning no more pages
                if len(series_list) < limit:
                    break

                offset += limit

            except Exception as e:
                logging.warning(f"Search failed at offset {offset}: {e}")
                break

        logging.info(f"Search method found {len(all_series)} series IDs")
        return list(all_series)  # Convert set back to list
