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

    # Category caching
    _cached_category_ids: list[dict] | None = None
    _category_ids_stream_instance = None
    _category_ids_lock = Lock()
    
    # Release caching
    _cached_release_ids: list[dict] | None = None
    _release_ids_stream_instance = None
    _release_ids_lock = Lock()
    
    # Source caching
    _cached_source_ids: list[dict] | None = None
    _source_ids_stream_instance = None
    _source_ids_lock = Lock()
    
    # Tag names caching
    _cached_tag_names: list[dict] | None = None
    _tag_names_stream_instance = None
    _tag_names_lock = Lock()

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
        """Get series IDs from configuration - simplified for current implementation."""
        series_ids = self.config["series_ids"]
        
        if isinstance(series_ids, str):
            if series_ids.startswith("[") and series_ids.endswith("]"):
                try:
                    series_ids = json.loads(series_ids)
                except json.JSONDecodeError:
                    series_ids = [series_ids]
            else:
                series_ids = [series_ids]
        elif not isinstance(series_ids, list):
            raise ValueError("series_ids must be a string or list of strings")
        
        return series_ids

    def get_category_ids_stream(self):
        """Get CategoryStream instance for caching"""
        if self._category_ids_stream_instance is None:
            self._category_ids_stream_instance = CategoryStream(self)
        return self._category_ids_stream_instance


    def _cache_resource_ids(self, resource_type: str, discovery_stream_getter, individual_stream_class=None):
        """Generic method to cache resource IDs following consistent pattern.
        
        Args:
            resource_type: "category", "release", "source", or "tag_name"  
            discovery_stream_getter: Function that returns discovery stream instance
            individual_stream_class: Class for individual resource fetching (None for tags)
        """
        # Handle special case for tag_name vs tag_names
        if resource_type == "tag_name":
            cache_attr = "_cached_tag_names"
            lock_attr = "_tag_names_lock"
            config_key = "tag_names"
            id_key = "tag_name"
            data_key = "name"  # Tags use "name" field instead of "id"
        else:
            cache_attr = f"_cached_{resource_type}_ids"
            lock_attr = f"_{resource_type}_ids_lock"
            config_key = f"{resource_type}_ids"
            id_key = f"{resource_type}_id"
            data_key = "id"
        
        # Validate attributes exist
        if not hasattr(self, cache_attr) or not hasattr(self, lock_attr):
            raise AttributeError(f"Missing cache attributes for {resource_type}")
        
        cached_data = getattr(self, cache_attr)
        if cached_data is None:
            with getattr(self, lock_attr):
                cached_data = getattr(self, cache_attr)
                if cached_data is None:
                    config_ids = self.config.get(config_key, ["*"])
                    
                    if config_ids == ["*"]:
                        # Wildcard: use discovery stream
                        self.logger.info(f"Fetching and caching {resource_type} IDs via wildcard discovery...")
                        stream = discovery_stream_getter()
                        data = list(stream.get_records(context=None))
                        if data_key == "id":
                            cached_data = [{id_key: item["id"]} for item in sorted(data, key=lambda x: x.get("id", 0))]
                        else:  # For tags using "name"
                            cached_data = [{id_key: item["name"]} for item in data]
                    else:
                        # Explicit config: call individual endpoints or use config directly
                        self.logger.info(f"Fetching and caching specified {resource_type} IDs from meltano.yml config: {config_ids}")
                        if individual_stream_class:
                            stream = individual_stream_class(self)
                            cached_items = []
                            for item_id in config_ids:
                                context = {id_key: int(item_id)}
                                records = list(stream.get_records(context))
                                cached_items.extend(records)
                            cached_data = [{id_key: item["id"]} for item in cached_items]
                        else:
                            # For resources like tags that don't need individual endpoints
                            cached_data = [{id_key: item_id} for item_id in config_ids]
                    
                    setattr(self, cache_attr, cached_data)
                    self.logger.info(f"Cached {len(cached_data)} {resource_type} IDs.")
        return cached_data

    def get_cached_category_ids(self):
        """Return cached category IDs in consistent format [{'category_id': int}]."""
        from tap_fred.streams import CategoryStream
        return self._cache_resource_ids("category", self.get_category_ids_stream, CategoryStream)

    def get_release_ids_stream(self):
        """Get ReleasesStream instance for caching"""
        if self._release_ids_stream_instance is None:
            self._release_ids_stream_instance = ReleasesStream(self)
        return self._release_ids_stream_instance

    def get_cached_release_ids(self):
        """Return cached release IDs in consistent format [{'release_id': int}]."""
        from tap_fred.streams import ReleaseStream
        return self._cache_resource_ids("release", self.get_release_ids_stream, ReleaseStream)

    def get_source_ids_stream(self):
        """Get SourcesStream instance for caching"""
        if self._source_ids_stream_instance is None:
            self._source_ids_stream_instance = SourcesStream(self)
        return self._source_ids_stream_instance

    def get_cached_source_ids(self):
        """Return cached source IDs in consistent format [{'source_id': int}]."""
        from tap_fred.streams import SourceStream
        return self._cache_resource_ids("source", self.get_source_ids_stream, SourceStream)

    def get_tag_names_stream(self):
        """Get TagsStream instance for caching"""
        if self._tag_names_stream_instance is None:
            from tap_fred.streams import TagsStream
            self._tag_names_stream_instance = TagsStream(self)
        return self._tag_names_stream_instance

    def get_cached_tag_names(self):
        """Return cached tag names in consistent format [{'tag_name': str}]."""
        return self._cache_resource_ids("tag_name", self.get_tag_names_stream, None)

    def discover_streams(self) -> list[FREDStream]:
        """Return a list of discovered streams - complete FRED API coverage with configurable selection."""
        discovered_streams = [
            # Core data stream - series observations (economic data points)
            SeriesObservationsStream(self),
            # Series-related streams
            SeriesStream(self),
            SeriesCategoriesStream(self),
            SeriesReleaseStream(self),
            SeriesSearchStream(self),
            SeriesSearchTagsStream(self),
            SeriesTagsStream(self),
            SeriesUpdatesStream(self),
            SeriesVintageDatesStream(self),
            # Category streams (hierarchical metadata)
            CategoryStream(self),
            CategoryChildrenStream(self),
            CategoryRelatedStream(self),
            CategorySeriesStream(self),
            CategoryTagsStream(self),
            # Release streams (publication metadata)
            ReleasesStream(self),
            ReleaseStream(self),
            ReleaseDatesStream(self),
            ReleaseSeriesStream(self),
            ReleaseSourcesStream(self),
            ReleaseTagsStream(self),
            ReleaseTablesStream(self),
            # Source streams (data provider metadata)
            SourcesStream(self),
            SourceStream(self),
            SourceReleasesStream(self),
            # Tag streams (topic/keyword metadata)
            TagsStream(self),
            TagsSeriesStream(self),
        ]
        
        # Conditionally add streams that require specific configuration
        
        # SeriesSearchRelatedTagsStream requires search_text AND tag_names
        if self.config.get("search_text") and self.config.get("tag_names"):
            try:
                discovered_streams.append(SeriesSearchRelatedTagsStream(self))
            except ValueError as e:
                self.logger.info(f"Skipping SeriesSearchRelatedTagsStream: {e}")
                
        # CategoryRelatedTagsStream requires tag_names
        if self.config.get("tag_names"):
            try:
                discovered_streams.append(CategoryRelatedTagsStream(self))
            except ValueError as e:
                self.logger.info(f"Skipping CategoryRelatedTagsStream: {e}")
                
        # RelatedTagsStream requires tag_names
        if self.config.get("tag_names"):
            try:
                discovered_streams.append(RelatedTagsStream(self))
            except ValueError as e:
                self.logger.info(f"Skipping RelatedTagsStream: {e}")
                
        # ReleaseRelatedTagsStream requires release_ids AND tag_names
        if self.config.get("release_ids") and self.config.get("tag_names"):
            try:
                discovered_streams.append(ReleaseRelatedTagsStream(self))
            except ValueError as e:
                self.logger.info(f"Skipping ReleaseRelatedTagsStream: {e}")
        
        # GeoFRED streams require specific configuration - always try to add them
        try:
            discovered_streams.append(GeoFREDRegionalDataStream(self))
        except ValueError as e:
            self.logger.info(f"Skipping GeoFREDRegionalDataStream: {e}")
            
        try:
            discovered_streams.append(GeoFREDSeriesDataStream(self))
        except ValueError as e:
            self.logger.info(f"Skipping GeoFREDSeriesDataStream: {e}")
        
        return discovered_streams


if __name__ == "__main__":
    TapFRED.cli()
