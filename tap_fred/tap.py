"""FRED tap class."""

from __future__ import annotations

from threading import Lock

from singer_sdk import Tap
from singer_sdk import typing as th

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
        th.Property(
            "point_in_time_mode",
            th.BooleanType,
            default=False,
            description="Enable point-in-time vintage date partitioning for backtesting",
        ),
        th.Property(
            "point_in_time_start",
            th.DateTimeType,
            description="Filter vintage dates from this date (YYYY-MM-DD)",
        ),
        th.Property(
            "point_in_time_end",
            th.DateTimeType,
            description="Filter vintage dates up to this date (YYYY-MM-DD)",
        ),
    ).to_dict()

    # Category caching
    _cached_category_ids: list[dict] | None = None
    _category_ids_lock = Lock()

    # Release caching
    _cached_release_ids: list[dict] | None = None
    _release_ids_lock = Lock()

    # Source caching
    _cached_source_ids: list[dict] | None = None
    _source_ids_lock = Lock()

    # Tag names caching
    _cached_tag_names: list[dict] | None = None
    _tag_names_lock = Lock()

    # Series IDs caching
    _cached_series_ids: list[dict] | None = None
    _series_ids_lock = Lock()

    # Vintage dates caching
    _cached_vintage_dates: list[dict] | None = None
    _vintage_dates_lock = Lock()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_cached_series_ids(self):
        """Return cached series IDs in consistent format [{'series_id': str}]."""
        return self._cache_resource_ids("series", SeriesStream(self))

    def get_cached_vintage_dates(self):
        """Return cached vintage dates in consistent format [{'vintage_date': str}]."""
        return self._cache_resource_ids("vintage_date", SeriesVintageDatesStream(self))

    def _discover_all_series_ids(self) -> list[str]:
        """Discover all FRED series IDs by fetching from release series endpoints."""
        series_ids = set()

        # Get all release IDs first
        cached_releases = self.get_cached_release_ids()
        self.logger.info(f"Discovering series from {len(cached_releases)} releases...")

        # For each release, get all its series
        stream = ReleaseSeriesStream(self)
        for release_item in cached_releases:
            release_id = release_item["release_id"]
            try:
                # Call the stream's partition-based method directly
                url = stream.get_url()
                params = stream.query_params.copy()
                params["release_id"] = release_id

                response_data = stream._fetch_with_retry(url, params)
                release_series = response_data.get("seriess", [])
                release_series_ids = [
                    record["id"] for record in release_series if "id" in record
                ]
                series_ids.update(release_series_ids)
                self.logger.info(
                    f"Release {release_id}: Found {len(release_series_ids)} series"
                )
            except Exception as e:
                self.logger.warning(
                    f"Failed to get series for release {release_id}: {e}"
                )
                continue

        discovered_series = sorted(list(series_ids))
        self.logger.info(f"Total unique series discovered: {len(discovered_series)}")
        return discovered_series

    def _cache_resource_ids(self, resource_type: str, discovery_stream):
        """Generic method to cache resource IDs following consistent pattern.

        Args:
            resource_type: "category", "release", "source", "tag_name", or "series"
            discovery_stream: Stream instance for discovery
        """
        # Handle special cases
        if resource_type == "tag_name":
            cache_attr = "_cached_tag_names"
            lock_attr = "_tag_names_lock"
            config_key = "tag_names"
            id_key = "tag_name"
            data_key = "name"  # Tags use "name" field instead of "id"
        elif resource_type == "vintage_date":
            cache_attr = "_cached_vintage_dates"
            lock_attr = "_vintage_dates_lock"
            config_key = "vintage_dates"
            id_key = "vintage_date"
            data_key = "date"
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
                        self.logger.info(
                            f"Fetching and caching {resource_type} IDs via wildcard discovery..."
                        )

                        if resource_type == "series":
                            # Special case: series discovery requires complex multi-endpoint approach
                            series_ids_list = self._discover_all_series_ids()
                            cached_data = [
                                {id_key: series_id} for series_id in series_ids_list
                            ]
                        else:
                            # Standard discovery for other resource types
                            data = list(discovery_stream.get_records(context=None))
                            if data_key == "id":
                                cached_data = [
                                    {id_key: item["id"]}
                                    for item in sorted(
                                        data, key=lambda x: x.get("id", 0)
                                    )
                                ]
                            elif data_key == "date":  # For vintage dates using "date"
                                # Preserve parent resource ID for per-resource vintage date filtering
                                # Extract parent resource ID key from discovery stream (generic pattern)
                                parent_id_key = getattr(discovery_stream, '_resource_id_key', None)
                                if not parent_id_key:
                                    raise ValueError(
                                        f"Discovery stream for {resource_type} must define _resource_id_key "
                                        f"for vintage date caching"
                                    )

                                cached_data = [
                                    {
                                        id_key: item["date"],
                                        "resource_id": item[parent_id_key]  # Generic "resource_id" field
                                    }
                                    for item in data
                                ]
                            else:  # For tags using "name"
                                cached_data = [{id_key: item["name"]} for item in data]
                    else:
                        # Explicit config: call individual endpoints or use config directly
                        self.logger.info(
                            f"Fetching and caching specified {resource_type} IDs from meltano.yml config: {config_ids}"
                        )

                        # Direct config usage - all resource types use config directly
                        cached_data = [{id_key: item_id} for item_id in config_ids]

                    setattr(self, cache_attr, cached_data)
                    self.logger.info(f"Cached {len(cached_data)} {resource_type} IDs.")
        return cached_data

    def get_cached_category_ids(self):
        """Return cached category IDs in consistent format [{'category_id': int}]."""
        return self._cache_resource_ids("category", CategoryStream(self))

    def get_cached_release_ids(self):
        """Return cached release IDs in consistent format [{'release_id': int}]."""
        return self._cache_resource_ids("release", ReleasesStream(self))

    def get_cached_source_ids(self):
        """Return cached source IDs in consistent format [{'source_id': int}]."""
        return self._cache_resource_ids("source", SourcesStream(self))

    def get_cached_tag_names(self):
        """Return cached tag names in consistent format [{'tag_name': str}]."""
        return self._cache_resource_ids("tag_name", TagsStream(self))

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
            # Conditional streams that require specific parameters
            SeriesSearchRelatedTagsStream(self),
            CategoryRelatedTagsStream(self),
            RelatedTagsStream(self),
            ReleaseRelatedTagsStream(self),
            # GeoFRED streams (geographic/regional data)
            GeoFREDRegionalDataStream(self),
            GeoFREDSeriesDataStream(self),
        ]

        return discovered_streams


if __name__ == "__main__":
    TapFRED.cli()
