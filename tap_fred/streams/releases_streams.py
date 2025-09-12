"""FRED Releases streams - /fred/releases endpoints."""

from __future__ import annotations

import typing as t
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_fred.client import FREDStream, ReleaseBasedFREDStream


class ReleasesStream(ReleaseBasedFREDStream):
    """Stream for FRED releases - /fred/releases endpoint.

    Uses pagination per FRED API documentation (limit 1-1000, default 1000).
    """

    name = "releases"
    path = "/releases"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = None
    records_jsonpath = "$.releases[*]"
    _paginate = True

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType, description="Release ID"),
        th.Property("realtime_start", th.DateType, description="Real-time start date"),
        th.Property("realtime_end", th.DateType, description="Real-time end date"),
        th.Property("name", th.StringType, description="Release name"),
        th.Property("press_release", th.BooleanType, description="Press release flag"),
        th.Property("link", th.StringType, description="Release URL link"),
        th.Property("notes", th.StringType, description="Release notes/description"),
    ).to_dict()

    def __init__(self, tap) -> None:
        super().__init__(tap)

        # Get order_by from config - FRED API allows: release_id, name, press_release, realtime_start, realtime_end
        order_by = self.config.get("releases_order_by", "release_id")
        sort_order = self.config.get("releases_sort_order", "asc")

        self.query_params.update(
            {
                "order_by": order_by,
                "sort_order": sort_order,
            }
        )

    def _get_records_key(self) -> str:
        return "releases"

    def post_process(self, record: dict, context: Context | None = None) -> dict:
        """Transform raw data to match expected structure."""
        # Apply business logic BEFORE calling super()
        if "press_release" in record and isinstance(record["press_release"], str):
            record["press_release"] = record["press_release"].lower() == "true"

        return super().post_process(record, context)


class ReleaseStream(FREDStream):
    """Stream for individual FRED release - /fred/release endpoint.

    Requires release_ids to be configured. Each release ID becomes a partition.
    """

    name = "release"
    path = "/release"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = None
    records_jsonpath = "$.releases[*]"

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType, description="Release ID"),
        th.Property("realtime_start", th.DateType, description="Real-time start date"),
        th.Property("realtime_end", th.DateType, description="Real-time end date"),
        th.Property("name", th.StringType, description="Release name"),
        th.Property("press_release", th.BooleanType, description="Press release flag"),
        th.Property("link", th.StringType, description="Release URL link"),
        th.Property("notes", th.StringType, description="Release notes/description"),
    ).to_dict()

    def _get_records_key(self) -> str:
        return "releases"

    def post_process(self, record: dict, context: Context | None = None) -> dict:
        """Transform raw data to match expected structure."""
        # Apply business logic BEFORE calling super()
        if "press_release" in record and isinstance(record["press_release"], str):
            record["press_release"] = record["press_release"].lower() == "true"

        return super().post_process(record, context)


class ReleaseDatesStream(FREDStream):
    """Stream for FRED release dates - /fred/releases/dates endpoint.

    Uses pagination and configurable date range filtering.
    """

    name = "release_dates"
    path = "/releases/dates"
    primary_keys: t.ClassVar[list[str]] = ["release_id", "date"]
    replication_key = None
    records_jsonpath = "$.release_dates[*]"
    _paginate = True

    schema = th.PropertiesList(
        th.Property("release_id", th.IntegerType, description="Release ID"),
        th.Property("release_name", th.StringType, description="Release name"),
        th.Property("date", th.DateType, description="Release date"),
    ).to_dict()

    def _get_records_key(self) -> str:
        return "release_dates"


class ReleaseSeriesStream(ReleaseBasedFREDStream):
    """Stream for FRED release series - /fred/release/series endpoint.

    Requires release_ids to be configured. Each release ID becomes a partition.
    Uses pagination per FRED API documentation.
    """

    name = "release_series"
    path = "/release/series"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = None
    records_jsonpath = "$.seriess[*]"
    _paginate = True

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Series ID"),
        th.Property("realtime_start", th.DateType, description="Real-time start date"),
        th.Property("realtime_end", th.DateType, description="Real-time end date"),
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
        th.Property(
            "group_popularity", th.IntegerType, description="Group popularity score"
        ),
        th.Property("notes", th.StringType, description="Series notes"),
        th.Property(
            "release_id",
            th.IntegerType,
            description="Release ID this series belongs to",
        ),
    ).to_dict()

    def _get_records_key(self) -> str:
        return "seriess"


class ReleaseSourcesStream(ReleaseBasedFREDStream):
    """Stream for FRED release sources - /fred/release/sources endpoint.

    Requires release_ids to be configured. Each release ID becomes a partition.
    """

    name = "release_sources"
    path = "/release/sources"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = None
    records_jsonpath = "$.sources[*]"

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType, description="Source ID"),
        th.Property("realtime_start", th.DateType, description="Real-time start date"),
        th.Property("realtime_end", th.DateType, description="Real-time end date"),
        th.Property("name", th.StringType, description="Source name"),
        th.Property("link", th.StringType, description="Source URL link"),
        th.Property("notes", th.StringType, description="Source notes/description"),
        th.Property(
            "release_id",
            th.IntegerType,
            description="Release ID this source belongs to",
        ),
    ).to_dict()

    def _get_records_key(self) -> str:
        return "sources"


class ReleaseTagsStream(ReleaseBasedFREDStream):
    """Stream for FRED release tags - /fred/release/tags endpoint.

    Requires release_ids to be configured. Each release ID becomes a partition.
    """

    name = "release_tags"
    path = "/release/tags"
    primary_keys: t.ClassVar[list[str]] = ["name"]
    replication_key = None
    records_jsonpath = "$.tags[*]"

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
            "release_id", th.IntegerType, description="Release ID these tags belong to"
        ),
    ).to_dict()

    def _get_records_key(self) -> str:
        return "tags"


class ReleaseRelatedTagsStream(FREDStream):
    """Stream for FRED release related tags - /fred/release/related_tags endpoint.

    Requires both release_ids and tag_names to be configured.
    Each combination becomes a partition.
    """

    name = "release_related_tags"
    path = "/release/related_tags"
    primary_keys: t.ClassVar[list[str]] = ["name", "release_id"]
    replication_key = None
    records_jsonpath = "$.tags[*]"

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
            "release_id", th.IntegerType, description="Release ID for related tags"
        ),
    ).to_dict()

    @property
    def partitions(self):
        """Generate partitions from release_ids and tag_names configuration."""
        release_ids = self.config.get("release_ids")
        tag_names = self.config.get("tag_names")

        if not release_ids:
            raise ValueError(
                "ReleaseRelatedTagsStream requires release_ids to be configured. "
                "No defaults are provided - all release IDs must be explicitly configured."
            )

        if not tag_names:
            raise ValueError(
                "ReleaseRelatedTagsStream requires tag_names to be configured. "
                "No defaults are provided - all tag names must be explicitly configured."
            )

        # Resolve wildcard release_ids
        if release_ids == ["*"]:
            release_ids = self._tap.get_cached_release_ids()

        # Create partitions for each combination of release_id and tag_names
        partitions = []
        for release_id in release_ids:
            tag_names_str = (
                ";".join(tag_names) if isinstance(tag_names, list) else str(tag_names)
            )
            partitions.append(
                {
                    "release_id": int(release_id),
                    "tag_names": tag_names_str,
                }
            )

        return partitions

    def _get_records_key(self) -> str:
        return "tags"


class ReleaseTablesStream(ReleaseBasedFREDStream):
    """Stream for FRED release tables - /fred/release/tables endpoint.

    Requires release_ids to be configured. Each release ID becomes a partition.
    """

    name = "release_tables"
    path = "/release/tables"
    primary_keys: t.ClassVar[list[str]] = ["element_id"]
    replication_key = None
    records_jsonpath = "$.elements[*]"

    schema = th.PropertiesList(
        th.Property("element_id", th.StringType, description="Element ID"),
        th.Property("release_id", th.IntegerType, description="Release ID"),
        th.Property("series_id", th.StringType, description="Series ID"),
        th.Property("parent_id", th.StringType, description="Parent element ID"),
        th.Property("line", th.StringType, description="Line description"),
        th.Property("type", th.StringType, description="Element type"),
        th.Property("name", th.StringType, description="Element name"),
        th.Property("level", th.StringType, description="Element level"),
    ).to_dict()

    def _get_records_key(self) -> str:
        return "elements"
