"""FRED tap class."""

from __future__ import annotations

import json
from threading import Lock

from singer_sdk import Tap
from singer_sdk import typing as th
from singer_sdk.exceptions import ConfigValidationError

from tap_fred.client import FREDStream

from tap_fred.streams import (
    # Series streams
    SeriesStream,
    SeriesObservationsStream,
    SeriesCategoriesStream,
    SeriesReleaseStream,
    SeriesSearchStream,
    SeriesSearchTagsStream,
    SeriesSearchRelatedTagsStream,
    SeriesTagsStream,
    SeriesUpdatesStream,
    SeriesVintageDatesStream,
    # Category streams
    CategoryStream,
    CategoryChildrenStream,
    CategoryRelatedStream,
    CategorySeriesStream,
    CategoryTagsStream,
    CategoryRelatedTagsStream,
    # Release streams
    ReleasesStream,
    ReleaseStream,
    ReleaseDatesStream,
    ReleaseSeriesStream,
    ReleaseSourcesStream,
    ReleaseTagsStream,
    ReleaseRelatedTagsStream,
    ReleaseTablesStream,
    # Source streams
    SourcesStream,
    SourceStream,
    SourceReleasesStream,
    # Tag streams
    TagsStream,
    RelatedTagsStream,
    TagsSeriesStream,
    # Maps/GeoFRED streams
    GeoFREDRegionalDataStream,
    GeoFREDSeriesDataStream,
)


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
        th.Property(
            "max_requests_per_minute",
            th.IntegerType,
            default=60,
            description="Maximum number of API requests per minute (default: 60, FRED allows up to 120)",
        ),
        th.Property(
            "min_throttle_seconds",
            th.NumberType,
            default=1.0,
            description="Minimum seconds between consecutive API requests (default: 1.0 for 60/min rate)",
        ),
        th.Property(
            "category_ids",
            th.ArrayType(th.StringType),
            description="Specific category IDs to process (e.g., ['125', '13'] or ['*'] for all)",
        ),
        th.Property(
            "release_ids",
            th.ArrayType(th.StringType),
            description="Specific release IDs to process (e.g., ['53', '151'] or ['*'] for all)",
        ),
        th.Property(
            "source_ids",
            th.ArrayType(th.StringType),
            description="Specific source IDs to process (e.g., ['1', '3'] or ['*'] for all)",
        ),
        th.Property(
            "tag_names",
            th.ArrayType(th.StringType),
            description="Specific tag names to process (e.g., ['gdp', 'inflation'] or ['*'] for all)",
        ),
        th.Property(
            "enable_series_streams",
            th.BooleanType,
            default=True,
            description="Enable series-related streams (default: true)",
        ),
        th.Property(
            "enable_metadata_streams",
            th.BooleanType,
            default=True,
            description="Enable metadata streams (categories, releases, sources, tags) (default: true)",
        ),
        th.Property(
            "enable_geofred_streams",
            th.BooleanType,
            default=True,
            description="Enable geographic/regional data streams (default: true)",
        ),
    ).to_dict()

    _series_cache = {}
    
    # Category caching following tap-fmp pattern
    _cached_category_ids: list[dict] | None = None
    _category_ids_stream_instance = None
    _category_ids_lock = Lock()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._validate_dates()

    def _validate_dates(self):
        """Validate that realtime_end is not before realtime_start."""
        realtime_start = self.config.get("realtime_start")
        realtime_end = self.config.get("realtime_end")

        if realtime_start and realtime_end and realtime_end < realtime_start:
            raise ConfigValidationError("realtime_end cannot be before realtime_start")

    def get_cached_series_ids(self) -> list[str]:
        """Get cached series IDs using FREDStream client method."""
        with self._cache_lock:
            if not self._series_cache:
                series_ids = self.config["series_ids"]

                if series_ids == "*" or series_ids == ["*"]:
                    series_id_stream = SeriesObservationsStream(self)
                    series_ids = series_id_stream._fetch_all_series_ids()
                elif isinstance(series_ids, str):
                    if series_ids.startswith("[") and series_ids.endswith("]"):
                        try:
                            series_ids = json.loads(series_ids)
                        except json.JSONDecodeError:
                            series_ids = [series_ids]
                    else:
                        series_ids = [series_ids]
                elif not isinstance(series_ids, list):
                    raise ValueError("series_ids must be a string or list of strings")

                self._series_cache = series_ids

            return self._series_cache

    def get_category_ids_stream(self):
        """Get CategoryStream instance for caching - follows tap-fmp pattern."""
        if self._category_ids_stream_instance is None:
            from tap_fred.streams.category_streams import CategoryStream
            self._category_ids_stream_instance = CategoryStream(self)
        return self._category_ids_stream_instance

    def get_cached_category_ids(self):
        """Thread-safe category ID caching following tap-fmp pattern."""
        if self._cached_category_ids is None:
            with self._category_ids_lock:
                if self._cached_category_ids is None:
                    self.logger.info("Fetching and caching category IDs...")
                    stream = self.get_category_ids_stream()
                    data = list(stream.get_records(context=None))
                    self._cached_category_ids = sorted(data, key=lambda x: x.get("id", 0))
                    self.logger.info(f"Cached {len(self._cached_category_ids)} category IDs.")
        return self._cached_category_ids

    def discover_streams(self) -> list[FREDStream]:
        """Return a list of discovered streams - complete FRED API coverage with configurable selection."""
        return [
            # Core data stream - series observations (economic data points)
            # SeriesObservationsStream(self),
            # # Series-related streams
            # SeriesStream(self),
            # SeriesCategoriesStream(self),
            # SeriesReleaseStream(self),
            # SeriesSearchStream(self),
            # SeriesSearchTagsStream(self),
            # SeriesSearchRelatedTagsStream(self),
            # SeriesTagsStream(self),
            # SeriesUpdatesStream(self),
            # SeriesVintageDatesStream(self),
            # # Category streams (hierarchical metadata)
            CategoryStream(self),
            # CategoryChildrenStream(self),
            # CategoryRelatedStream(self),
            # CategorySeriesStream(self),
            # CategoryTagsStream(self),
            # CategoryRelatedTagsStream(self),
            # # Release streams (publication metadata)
            # ReleasesStream(self),
            # ReleaseStream(self),
            # ReleaseDatesStream(self),
            # ReleaseSeriesStream(self),
            # ReleaseSourcesStream(self),
            # ReleaseTagsStream(self),
            # ReleaseRelatedTagsStream(self),
            # ReleaseTablesStream(self),
            # # Source streams (data provider metadata)
            # SourcesStream(self),
            # SourceStream(self),
            # SourceReleasesStream(self),
            # # Tag streams (topic/keyword metadata)
            # TagsStream(self),
            # RelatedTagsStream(self),
            # TagsSeriesStream(self),
            # # Geographic/regional data streams
            # GeoFREDRegionalDataStream(self),
            # GeoFREDSeriesDataStream(self),
        ]


if __name__ == "__main__":
    TapFRED.cli()
