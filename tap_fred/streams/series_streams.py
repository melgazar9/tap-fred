"""FRED Series streams - /fred/series and /fred/series/observations."""

from __future__ import annotations

import typing as t
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_fred.client import (
    SeriesBasedFREDStream,
    FREDStream,
    PointInTimePartitionStream,
)
from tap_fred.helpers import join_tag_names


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

        response_data = self._fetch_with_retry(url, params)
        series_list = response_data.get("seriess", [])

        for series in series_list:
            yield self.post_process(series, context)


class SeriesObservationsStream(PointInTimePartitionStream):
    """Stream for FRED series observations (economic data points)."""

    name = "series_observations"
    path = "/series/observations"
    primary_keys: t.ClassVar[list[str]] = ["series_id", "date", "realtime_start", "realtime_end"]
    _add_surrogate_key = False

    # Point-in-time partitioning configuration
    _resource_type = "series"
    _resource_id_key = "series_id"

    # Incremental replication by realtime_start (vintage publication date)
    replication_key = "realtime_start"

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

    def _get_records_key(self) -> str:
        """Return the JSON key that contains the records list."""
        return "observations"

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

        response_data = self._fetch_with_retry(url, params)
        releases = response_data.get("releases", [])

        for release in releases:
            release["series_id"] = series_id
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

        stream_params = self.config.get("series_search_related_tags_params", {})
        tag_names = stream_params.get("query_params", {}).get("tag_names")

        if tag_names:
            self.query_params["tag_names"] = join_tag_names(tag_names)

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
        """Get tags for a specific series ID.

        Note: Tags are metadata that don't have vintage dates, so we skip
        ALFRED parameters to avoid server timeouts.
        """
        url = self.get_url()
        params = self.query_params.copy()
        params.update({"series_id": series_id})

        # Skip ALFRED params for metadata endpoints - use direct _make_request
        # instead of _fetch_with_retry to avoid automatic ALFRED parameter addition
        response_data = self._make_request(url, params)
        tags = response_data.get("tags", [])

        for tag in tags:
            tag["series_id"] = series_id
            yield self.post_process(tag, context)


class SeriesUpdatesStream(FREDStream):
    """Stream for FRED series updates - /fred/series/updates endpoint.

    Uses ``last_updated`` as replication key so subsequent runs only emit
    series that were updated since the previous sync's bookmark.
    """

    name = "series_updates"
    path = "/series/updates"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "last_updated"
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
    replication_key = "date"
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
        """Get vintage dates for a specific series ID.

        Paginated per FRED docs: /fred/series/vintagedates supports limit (max 10000) and offset.
        """
        url = self.get_url()
        offset = 0
        limit = 10000  # FRED docs max for this endpoint

        while True:
            params = self.query_params.copy()
            params.update({"series_id": series_id, "limit": limit, "offset": offset})

            # Skip realtime parameters - let FRED use defaults for complete vintage data
            # API has issues with realtime parameters on vintagedates endpoint

            response_data = self._fetch_with_retry(url, params)
            vintage_dates = response_data.get("vintage_dates", [])

            if not vintage_dates:
                break

            for date in vintage_dates:
                record = {"date": date, "series_id": series_id}
                yield self.post_process(record, context)

            if len(vintage_dates) < limit:
                break
            offset += limit
