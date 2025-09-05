"""FRED Series streams - /fred/series and /fred/series/observations."""

from __future__ import annotations

import typing as t
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_fred.client import SeriesBasedFREDStream, FREDStream


class SeriesStream(SeriesBasedFREDStream):
    """Stream for FRED series metadata - fetches ALL series from /fred/series endpoint."""

    name = "series"
    path = "/series"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = None
    records_jsonpath = "$.seriess[*]"
    _add_surrogate_key = False

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Series ID"),
        th.Property("title", th.StringType, description="Series title"),
        th.Property(
            "observation_start", th.DateType, description="First observation date"
        ),
        th.Property(
            "observation_end", th.DateType, description="Last observation date"
        ),
        th.Property("frequency", th.StringType, description="Data frequency"),
        th.Property(
            "frequency_short", th.StringType, description="Short frequency code"
        ),
        th.Property("units", th.StringType, description="Data units"),
        th.Property("units_short", th.StringType, description="Short units code"),
        th.Property(
            "seasonal_adjustment", th.StringType, description="Seasonal adjustment"
        ),
        th.Property(
            "seasonal_adjustment_short",
            th.StringType,
            description="Short seasonal adjustment code",
        ),
        th.Property(
            "last_updated", th.DateTimeType, description="Last update timestamp"
        ),
        th.Property("popularity", th.IntegerType, description="Series popularity rank"),
        th.Property("notes", th.StringType, description="Series notes"),
    ).to_dict()

    def _get_series_records(
        self, series_id: str, context: Context | None
    ) -> t.Iterable[dict]:
        """Get series metadata for a specific series ID."""
        url = self.get_url()
        params = self.query_params.copy()
        params.update({"series_id": series_id})
        self._add_alfred_params(params)

        response_data = self._fetch_with_retry(url, params)
        series_list = response_data.get("seriess", [])

        for series in series_list:
            yield self.post_process(series, context)


class SeriesPartitionStream(SeriesBasedFREDStream):
    """Base stream class for FRED series partitioned streams."""

    @property
    def partitions(self):
        """Return partitions based on series IDs."""
        series_ids = self._tap.get_cached_series_ids()
        return [{"series_id": series_id} for series_id in series_ids]

    def _get_series_records(
        self, series_id: str, context: Context | None
    ) -> t.Iterable[dict]:
        """Retrieve records for a specific series ID - override base class method."""
        # Create partition context for this series
        partition_context = {"series_id": series_id}
        if context:
            partition_context.update(context)

        url = self.get_url()
        params = self.query_params.copy()
        params.update(
            {
                "series_id": series_id,
                "sort_order": "asc",
            }
        )

        self._add_alfred_params(params)
        self._add_date_filtering(params, partition_context)

        response_data = self._fetch_with_retry(url, params)
        records = response_data.get("observations", [])
        for record in records:
            record["series_id"] = series_id
            record = self.post_process(record, partition_context)
            yield record

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Retrieve records for a specific series ID from context (partition support)."""
        if context is None or "series_id" not in context:
            # Fall back to base class behavior for non-partition usage
            yield from super().get_records(context)
            return

        # Use partition-specific logic
        yield from self._get_series_records(context["series_id"], context)


class SeriesObservationsStream(SeriesPartitionStream):
    """Stream for FRED series observations (economic data points)."""

    name = "series_observations"
    path = "/series/observations"
    primary_keys: t.ClassVar[list[str]] = ["series_id", "date"]
    replication_key = "date"
    _add_surrogate_key = False

    schema = th.PropertiesList(
        th.Property("series_id", th.StringType, description="FRED series identifier"),
        th.Property("date", th.DateType, description="Observation date"),
        th.Property(
            "value",
            th.NumberType,
            description="Economic data value (null for missing data)",
        ),
        th.Property(
            "realtime_start",
            th.DateType,
            description="Real-time start date for this observation",
        ),
        th.Property(
            "realtime_end",
            th.DateType,
            description="Real-time end date for this observation",
        ),
    ).to_dict()

    def post_process(self, record: dict, context: t.Any = None) -> dict:
        """Transform raw data to match expected structure."""
        record = super().post_process(record, context)

        if "value" in record and record["value"] is not None:
            try:
                record["value"] = float(record["value"])
            except (ValueError, TypeError):
                record["value"] = None

        return record


class SeriesCategoriesStream(SeriesBasedFREDStream):
    """Stream for FRED series categories - /fred/series/categories endpoint."""

    name = "series_categories"
    path = "/series/categories"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = None
    records_jsonpath = "$.categories[*]"
    _add_surrogate_key = False

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType, description="Category ID"),
        th.Property("name", th.StringType, description="Category name"),
        th.Property("parent_id", th.IntegerType, description="Parent category ID"),
        th.Property("notes", th.StringType, description="Category notes/description"),
        th.Property(
            "series_id", th.StringType, description="Series ID this category belongs to"
        ),
    ).to_dict()

    def _get_series_records(
        self, series_id: str, context: Context | None
    ) -> t.Iterable[dict]:
        """Get categories for a specific series ID."""
        url = self.get_url()
        params = self.query_params.copy()
        params.update({"series_id": series_id})
        self._add_alfred_params(params)

        response_data = self._fetch_with_retry(url, params)
        categories = response_data.get("categories", [])

        for category in categories:
            category["series_id"] = series_id
            yield self.post_process(category, context)


class SeriesReleaseStream(SeriesBasedFREDStream):
    """Stream for FRED series release - /fred/series/release endpoint."""

    name = "series_release"
    path = "/series/release"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = None
    records_jsonpath = "$.releases[*]"
    _add_surrogate_key = False

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType, description="Release ID"),
        th.Property("realtime_start", th.DateType, description="Real-time start date"),
        th.Property("realtime_end", th.DateType, description="Real-time end date"),
        th.Property("name", th.StringType, description="Release name"),
        th.Property("press_release", th.BooleanType, description="Press release flag"),
        th.Property("link", th.StringType, description="Release URL link"),
        th.Property("notes", th.StringType, description="Release notes/description"),
        th.Property(
            "series_id", th.StringType, description="Series ID this release belongs to"
        ),
    ).to_dict()

    def _get_series_records(
        self, series_id: str, context: Context | None
    ) -> t.Iterable[dict]:
        """Get release for a specific series ID."""
        url = self.get_url()
        params = self.query_params.copy()
        params.update({"series_id": series_id})
        self._add_alfred_params(params)

        response_data = self._fetch_with_retry(url, params)
        releases = response_data.get("releases", [])

        for release in releases:
            release["series_id"] = series_id
            # Convert press_release to boolean
            if "press_release" in release:
                release["press_release"] = release["press_release"] == "true"
            yield self.post_process(release, context)


class SeriesSearchStream(FREDStream):
    """Stream for FRED series search - /fred/series/search endpoint."""

    name = "series_search"
    path = "/series/search"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = None
    records_jsonpath = "$.seriess[*]"
    _add_surrogate_key = False
    _paginate = True

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Series ID"),
        th.Property("title", th.StringType, description="Series title"),
        th.Property(
            "observation_start", th.DateType, description="First observation date"
        ),
        th.Property(
            "observation_end", th.DateType, description="Last observation date"
        ),
        th.Property("frequency", th.StringType, description="Data frequency"),
        th.Property("units", th.StringType, description="Data units"),
        th.Property(
            "seasonal_adjustment", th.StringType, description="Seasonal adjustment"
        ),
        th.Property(
            "last_updated", th.DateTimeType, description="Last update timestamp"
        ),
        th.Property("popularity", th.IntegerType, description="Series popularity rank"),
        th.Property("notes", th.StringType, description="Series notes"),
    ).to_dict()

    def __init__(self, tap) -> None:
        super().__init__(tap)
        # Add search-specific query parameters
        self.query_params.update(
            {
                "search_text": "",  # Can be configured
            }
        )

    def _get_records_key(self) -> str:
        return "seriess"


class SeriesSearchTagsStream(FREDStream):
    """Stream for FRED series search tags - /fred/series/search/tags endpoint."""

    name = "series_search_tags"
    path = "/series/search/tags"
    primary_keys: t.ClassVar[list[str]] = ["name"]
    replication_key = None
    records_jsonpath = "$.tags[*]"
    _add_surrogate_key = False
    _paginate = True

    schema = th.PropertiesList(
        th.Property("name", th.StringType, description="Tag name"),
        th.Property("group_id", th.StringType, description="Tag group identifier"),
        th.Property("notes", th.StringType, description="Tag notes/description"),
        th.Property("created", th.DateTimeType, description="Tag creation timestamp"),
        th.Property("popularity", th.IntegerType, description="Tag popularity score"),
        th.Property(
            "series_count", th.IntegerType, description="Number of series with this tag"
        ),
    ).to_dict()

    def __init__(self, tap) -> None:
        super().__init__(tap)
        # Add search-specific query parameters
        self.query_params.update(
            {
                "series_search_text": "",  # Can be configured
            }
        )

    def _get_records_key(self) -> str:
        return "tags"


class SeriesSearchRelatedTagsStream(FREDStream):
    """Stream for FRED series search related tags - /fred/series/search/related_tags endpoint.

    Note: This endpoint requires both series_search_text and tag_names parameters.
    Stream will only be enabled if tag_names are configured in the tap config.
    """

    name = "series_search_related_tags"
    path = "/series/search/related_tags"
    primary_keys: t.ClassVar[list[str]] = ["name"]
    replication_key = None
    records_jsonpath = "$.tags[*]"
    _add_surrogate_key = False
    _paginate = True

    schema = th.PropertiesList(
        th.Property("name", th.StringType, description="Tag name"),
        th.Property("group_id", th.StringType, description="Tag group identifier"),
        th.Property("notes", th.StringType, description="Tag notes/description"),
        th.Property("created", th.DateTimeType, description="Tag creation timestamp"),
        th.Property("popularity", th.IntegerType, description="Tag popularity score"),
        th.Property(
            "series_count", th.IntegerType, description="Number of series with this tag"
        ),
    ).to_dict()

    def __init__(self, tap) -> None:
        super().__init__(tap)

        # Get required parameters from config - NO DEFAULTS!
        search_text = self.config.get("search_text")
        tag_names = self.config.get("tag_names")
        
        if not search_text:
            raise ValueError(
                "SeriesSearchRelatedTagsStream requires search_text to be configured. "
                "No defaults are provided - search text must be explicitly configured."
            )
            
        if not tag_names:
            raise ValueError(
                "SeriesSearchRelatedTagsStream requires tag_names to be configured. "
                "No defaults are provided - tag names must be explicitly configured."
            )

        # Convert tag_names to string format
        if isinstance(tag_names, list):
            tag_names_str = ";".join(tag_names)
        elif isinstance(tag_names, str):
            tag_names_str = tag_names
        else:
            raise ValueError("tag_names must be a list or string")

        self.query_params.update(
            {
                "series_search_text": search_text,
                "tag_names": tag_names_str,
            }
        )

    def _get_records_key(self) -> str:
        return "tags"


class SeriesTagsStream(SeriesBasedFREDStream):
    """Stream for FRED series tags - /fred/series/tags endpoint."""

    name = "series_tags"
    path = "/series/tags"
    primary_keys: t.ClassVar[list[str]] = ["name"]
    replication_key = None
    records_jsonpath = "$.tags[*]"
    _add_surrogate_key = False

    schema = th.PropertiesList(
        th.Property("name", th.StringType, description="Tag name"),
        th.Property("group_id", th.StringType, description="Tag group identifier"),
        th.Property("notes", th.StringType, description="Tag notes/description"),
        th.Property("created", th.DateTimeType, description="Tag creation timestamp"),
        th.Property("popularity", th.IntegerType, description="Tag popularity score"),
        th.Property(
            "series_count", th.IntegerType, description="Number of series with this tag"
        ),
        th.Property(
            "series_id", th.StringType, description="Series ID these tags belong to"
        ),
    ).to_dict()

    def _get_series_records(
        self, series_id: str, context: Context | None
    ) -> t.Iterable[dict]:
        """Get tags for a specific series ID."""
        url = self.get_url()
        params = self.query_params.copy()
        params.update({"series_id": series_id})
        self._add_alfred_params(params)

        response_data = self._fetch_with_retry(url, params)
        tags = response_data.get("tags", [])

        for tag in tags:
            tag["series_id"] = series_id
            yield self.post_process(tag, context)


class SeriesUpdatesStream(FREDStream):
    """Stream for FRED series updates - /fred/series/updates endpoint."""

    name = "series_updates"
    path = "/series/updates"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = None
    records_jsonpath = "$.seriess[*]"
    _add_surrogate_key = False
    _paginate = True

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Series ID"),
        th.Property("title", th.StringType, description="Series title"),
        th.Property(
            "observation_start", th.DateType, description="First observation date"
        ),
        th.Property(
            "observation_end", th.DateType, description="Last observation date"
        ),
        th.Property("frequency", th.StringType, description="Data frequency"),
        th.Property("units", th.StringType, description="Data units"),
        th.Property(
            "seasonal_adjustment", th.StringType, description="Seasonal adjustment"
        ),
        th.Property(
            "last_updated", th.DateTimeType, description="Last update timestamp"
        ),
        th.Property("popularity", th.IntegerType, description="Series popularity rank"),
        th.Property("notes", th.StringType, description="Series notes"),
    ).to_dict()

    def _get_records_key(self) -> str:
        return "seriess"


class SeriesVintageDatesStream(SeriesBasedFREDStream):
    """Stream for FRED series vintage dates - /fred/series/vintagedates endpoint."""

    name = "series_vintage_dates"
    path = "/series/vintagedates"
    primary_keys: t.ClassVar[list[str]] = ["date"]
    replication_key = None
    records_jsonpath = "$.vintage_dates[*]"
    _add_surrogate_key = False

    schema = th.PropertiesList(
        th.Property("date", th.DateType, description="Vintage date"),
        th.Property(
            "series_id", th.StringType, description="Series ID for vintage dates"
        ),
    ).to_dict()

    def _get_series_records(
        self, series_id: str, context: Context | None
    ) -> t.Iterable[dict]:
        """Get vintage dates for a specific series ID."""
        url = self.get_url()
        params = self.query_params.copy()
        params.update({"series_id": series_id})
        self._add_alfred_params(params)

        response_data = self._fetch_with_retry(url, params)
        vintage_dates = response_data.get("vintage_dates", [])

        for date in vintage_dates:
            record = {"date": date, "series_id": series_id}
            yield self.post_process(record, context)
