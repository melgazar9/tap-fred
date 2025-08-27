"""FRED tap class."""

from __future__ import annotations

from threading import Lock

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_fred.client import FREDStream

from tap_fred.streams.series_streams import SeriesObservationsStream


class TapFRED(Tap):
    """FRED tap class."""

    name = "tap-fred"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            secret=True,
            description="API key for FRED (Federal Reserve Economic Data) service",
        ),
        th.Property(
            "series_ids",
            th.OneOf(th.StringType, th.ArrayType(th.StringType)),
            required=True,
            description="FRED series IDs to replicate (e.g., 'GDP' or ['GDP', 'UNRATE'] or '*' for all)",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest observation date to sync",
        ),
        th.Property(
            "api_url",
            th.StringType,
            default="https://api.stlouisfed.org/fred",
            description="The url for the FRED API service",
        ),
        th.Property(
            "user_agent",
            th.StringType,
            description="Custom User-Agent header to send with each request",
        ),
        th.Property(
            "data_mode",
            th.StringType,
            default="FRED",
            description="Data mode: 'FRED' for current revised data, 'ALFRED' for vintage historical data",
        ),
        th.Property(
            "realtime_start",
            th.DateTimeType,
            description="Real-time start date for ALFRED vintage data (YYYY-MM-DD). Only used when data_mode='ALFRED'",
        ),
        th.Property(
            "realtime_end",
            th.DateTimeType,
            description="Real-time end date for ALFRED vintage data (YYYY-MM-DD). Only used when data_mode='ALFRED'",
        ),
    ).to_dict()

    _series_cache = {}
    _cache_lock = Lock()

    def get_cached_series_ids(self) -> list[str]:
        """Get cached series IDs using FREDStream client method."""
        with self._cache_lock:
            if not self._series_cache:
                series_ids = self.config["series_ids"]

                if series_ids == "*" or series_ids == ["*"]:
                    # Use FREDStream client to fetch all series IDs
                    temp_stream = SeriesObservationsStream(self)
                    series_ids = temp_stream._fetch_all_series_ids()
                elif isinstance(series_ids, str):
                    series_ids = [series_ids]
                elif not isinstance(series_ids, list):
                    raise ValueError("series_ids must be a string or list of strings")

                self._series_cache = series_ids

            return self._series_cache

    def discover_streams(self) -> list[FREDStream]:
        """Return a list of discovered streams."""
        return [
            SeriesObservationsStream(self),
        ]


if __name__ == "__main__":
    TapFRED.cli()
