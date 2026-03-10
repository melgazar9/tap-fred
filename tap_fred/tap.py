"""FRED tap class."""

from __future__ import annotations

from collections import deque
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
        th.Property(
            "strict_mode",
            th.BooleanType,
            default=False,
            description="If true, fail on any non-retriable API error. If false (default), warn and skip.",
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
        # Shared rate limiter state must be initialized BEFORE super().__init__()
        # because discover_streams() runs during parent init and creates stream instances
        self._shared_request_timestamps: deque = deque()
        self._shared_throttle_lock = Lock()
        super().__init__(*args, **kwargs)
        self._validate_tap_config()

    def _validate_tap_config(self) -> None:
        """Validate config at startup to catch misconfigurations early."""
        # Fix 6a: Normalize string series_ids to list to prevent char-by-char iteration
        series_ids = self.config.get("series_ids")
        if isinstance(series_ids, str):
            self._config["series_ids"] = [series_ids]

        if isinstance(series_ids, list) and len(series_ids) == 0:
            raise ValueError(
                "series_ids is an empty list. Provide at least one series ID "
                "or use ['*'] for wildcard discovery."
            )

        pit_start = self.config.get("point_in_time_start")
        pit_end = self.config.get("point_in_time_end")
        if pit_start and pit_end and str(pit_start)[:10] > str(pit_end)[:10]:
            raise ValueError(
                f"point_in_time_start ({pit_start}) is after "
                f"point_in_time_end ({pit_end}). This produces zero partitions."
            )

        realtime_start = self.config.get("realtime_start")
        realtime_end = self.config.get("realtime_end")
        if realtime_start and realtime_end and str(realtime_start)[:10] > str(realtime_end)[:10]:
            raise ValueError(
                f"realtime_start ({realtime_start}) is after "
                f"realtime_end ({realtime_end})."
            )

        data_mode = self.config.get("data_mode", "FRED")
        pit_mode = self.config.get("point_in_time_mode", False)
        if data_mode == "ALFRED" and not realtime_start and not pit_mode:
            self.logger.warning(
                "data_mode=ALFRED without realtime_start and point_in_time_mode=false. "
                "FRED API will default to today's date (equivalent to FRED mode)."
            )

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

        # For each release, get all its series (paginated per FRED docs: limit 1-1000)
        stream = ReleaseSeriesStream(self)
        for release_item in cached_releases:
            release_id = release_item["release_id"]
            try:
                url = stream.get_url()
                offset = 0
                limit = 1000
                release_total = 0
                while True:
                    params = stream.query_params.copy()
                    params["release_id"] = release_id
                    params["limit"] = limit
                    params["offset"] = offset

                    response_data = stream._fetch_with_retry(url, params)
                    release_series = response_data.get("seriess", [])
                    if not release_series:
                        break
                    release_series_ids = [
                        record["id"] for record in release_series if "id" in record
                    ]
                    series_ids.update(release_series_ids)
                    release_total += len(release_series_ids)
                    if len(release_series) < limit:
                        break
                    offset += limit
                self.logger.info(
                    f"Release {release_id}: Found {release_total} series"
                )
            except Exception as e:
                self.logger.warning(
                    f"Failed to get series for release {release_id}: {e}"
                )
                continue

        discovered_series = sorted(series_ids)
        self.logger.info(f"Total unique series discovered: {len(discovered_series)}")
        return discovered_series

    # Maps resource_type -> (cache_attr, lock_attr, config_key, id_key, data_key)
    _RESOURCE_ATTR_MAP = {
        "tag_name": ("_cached_tag_names", "_tag_names_lock", "tag_names", "tag_name", "name"),
        "vintage_date": ("_cached_vintage_dates", "_vintage_dates_lock", "vintage_dates", "vintage_date", "date"),
    }

    def _cache_resource_ids(self, resource_type: str, discovery_stream):
        """Thread-safe cache of resource IDs (wildcard discovery or explicit config).

        Args:
            resource_type: "category", "release", "source", "tag_name", "series", or "vintage_date"
            discovery_stream: Stream instance used for wildcard discovery
        """
        cache_attr, lock_attr, config_key, id_key, data_key = self._RESOURCE_ATTR_MAP.get(
            resource_type,
            (
                f"_cached_{resource_type}_ids",
                f"_{resource_type}_ids_lock",
                f"{resource_type}_ids",
                f"{resource_type}_id",
                "id",
            ),
        )

        # Validate attributes exist
        if not hasattr(self, cache_attr) or not hasattr(self, lock_attr):
            raise AttributeError(f"Missing cache attributes for {resource_type}")

        cached_data = getattr(self, cache_attr)
        if cached_data is None:
            with getattr(self, lock_attr):
                cached_data = getattr(self, cache_attr)
                if cached_data is None:
                    config_ids = self.config.get(config_key, ["*"])
                    # Belt-and-suspenders: normalize string to list
                    if isinstance(config_ids, str):
                        config_ids = [config_ids]

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
        ]

        # Conditionally register SeriesSearchRelatedTagsStream behind its required params
        search_related_params = self.config.get("series_search_related_tags_params", {})
        search_related_qp = search_related_params.get("query_params", {})
        if search_related_qp.get("series_search_text") and search_related_qp.get("tag_names"):
            discovered_streams.append(SeriesSearchRelatedTagsStream(self))

        # Conditionally register streams that require tag_names config
        if self.config.get("tag_names"):
            discovered_streams.extend([
                CategoryRelatedTagsStream(self),
                RelatedTagsStream(self),
                ReleaseRelatedTagsStream(self),
                TagsSeriesStream(self),
            ])

        # Conditionally register GeoFRED streams that require specific config
        if self.config.get("geofred_regional_params"):
            discovered_streams.append(GeoFREDRegionalDataStream(self))
        if self.config.get("geofred_series_ids"):
            discovered_streams.append(GeoFREDSeriesDataStream(self))

        return discovered_streams


if __name__ == "__main__":
    TapFRED.cli()
