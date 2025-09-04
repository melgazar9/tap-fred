"""FRED Releases streams - /fred/releases."""

from __future__ import annotations

import typing as t
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_fred.client import FREDStream


class ReleasesStream(FREDStream):
    """Stream for FRED releases - /fred/releases endpoint."""

    name = "releases"
    path = "/releases"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = None
    records_jsonpath = "$.releases[*]"
    _add_surrogate_key = False
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
        # Add releases-specific query parameters
        self.query_params.update(
            {
                "sort_order": "asc",
                "order_by": "release_id",
            }
        )

    def post_process(self, record: dict, context: Context | None = None) -> dict:
        """Transform raw data to match expected structure."""
        record = super().post_process(record, context)

        # Convert press_release to boolean
        if "press_release" in record:
            record["press_release"] = record["press_release"] == "true"

        return record


class ReleaseStream(FREDStream):
    """Stream for individual FRED release - /fred/release endpoint."""

    name = "release"
    path = "/release"
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
    ).to_dict()

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get specific releases - requires release_id parameter."""
        # This stream needs specific release IDs - would need configuration
        # For now, fall back to getting all releases via releases endpoint
        releases_stream = ReleasesStream(self.tap)
        for record in releases_stream.get_records(context):
            yield record

    def post_process(self, record: dict, context: Context | None = None) -> dict:
        """Transform raw data to match expected structure."""
        record = super().post_process(record, context)

        # Convert press_release to boolean
        if "press_release" in record:
            record["press_release"] = record["press_release"] == "true"

        return record


class ReleaseDatesStream(FREDStream):
    """Stream for FRED release dates - /fred/releases/dates endpoint."""

    name = "release_dates"
    path = "/releases/dates"
    primary_keys: t.ClassVar[list[str]] = ["release_id", "date"]
    replication_key = None
    records_jsonpath = "$.release_dates[*]"
    _add_surrogate_key = False

    schema = th.PropertiesList(
        th.Property("release_id", th.IntegerType, description="Release ID"),
        th.Property("release_name", th.StringType, description="Release name"),
        th.Property("date", th.DateType, description="Release date"),
    ).to_dict()

    def _get_records_key(self) -> str:
        return "release_dates"


class SingleReleaseDatesStream(FREDStream):
    """Stream for FRED single release dates - /fred/release/dates endpoint."""

    name = "single_release_dates"
    path = "/release/dates"
    primary_keys: t.ClassVar[list[str]] = ["release_id", "date"]
    replication_key = None
    records_jsonpath = "$.release_dates[*]"
    _add_surrogate_key = False

    schema = th.PropertiesList(
        th.Property("release_id", th.IntegerType, description="Release ID"),
        th.Property("date", th.DateType, description="Release date"),
    ).to_dict()

    def _get_records_key(self) -> str:
        return "release_dates"

    def _get_release_ids(self) -> list[str]:
        """Get release IDs from tap configuration."""
        release_ids = self.config.get("release_ids", [])
        if not release_ids or release_ids == "*" or release_ids == ["*"]:
            # If wildcard, we need to fetch all release IDs first
            # For now, return empty to avoid infinite loop
            return []

        if isinstance(release_ids, str):
            return [release_ids]
        elif isinstance(release_ids, list):
            return [str(rid) for rid in release_ids]

        return []

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get records for each configured release ID."""
        release_ids = self._get_release_ids()

        for release_id in release_ids:
            url = self.get_url()
            params = self.query_params.copy()
            params["release_id"] = release_id
            self._add_alfred_params(params)

            response_data = self._fetch_with_retry(url, params)
            records = response_data.get(self._get_records_key(), [])

            for record in records:
                yield self.post_process(record, context)


class ReleaseSeriesStream(FREDStream):
    """Stream for FRED release series - /fred/release/series endpoint."""

    name = "release_series"
    path = "/release/series"
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
        th.Property("units", th.StringType, description="Data units"),
        th.Property(
            "seasonal_adjustment", th.StringType, description="Seasonal adjustment"
        ),
        th.Property(
            "last_updated", th.DateTimeType, description="Last update timestamp"
        ),
        th.Property("popularity", th.IntegerType, description="Series popularity rank"),
        th.Property("notes", th.StringType, description="Series notes"),
        th.Property(
            "release_id",
            th.IntegerType,
            description="Release ID this series belongs to",
        ),
    ).to_dict()

    def _get_records_key(self) -> str:
        return "seriess"


class ReleaseSourcesStream(FREDStream):
    """Stream for FRED release sources - /fred/release/sources endpoint."""

    name = "release_sources"
    path = "/release/sources"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = None
    records_jsonpath = "$.sources[*]"
    _add_surrogate_key = False

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


class ReleaseTagsStream(FREDStream):
    """Stream for FRED release tags - /fred/release/tags endpoint."""

    name = "release_tags"
    path = "/release/tags"
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
            "release_id", th.IntegerType, description="Release ID these tags belong to"
        ),
    ).to_dict()

    def _get_records_key(self) -> str:
        return "tags"


class ReleaseRelatedTagsStream(FREDStream):
    """Stream for FRED release related tags - /fred/release/related_tags endpoint."""

    name = "release_related_tags"
    path = "/release/related_tags"
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
            "release_id", th.IntegerType, description="Release ID for related tags"
        ),
    ).to_dict()

    def __init__(self, tap) -> None:
        super().__init__(tap)

        # Get tag_names from config - required parameter for this endpoint
        tag_names = self.config.get("tag_names")
        if isinstance(tag_names, list):
            tag_names_str = ";".join(tag_names)
        elif isinstance(tag_names, str):
            tag_names_str = tag_names
        else:
            tag_names_str = "quarterly"  # Default common tag
            import logging

            logging.info(
                f"Stream {self.name}: No tag_names configured, using default 'quarterly'"
            )

        # Add release-specific query parameters
        self.query_params.update(
            {
                "sort_order": "asc",
                "order_by": "name",
                "tag_names": tag_names_str,
            }
        )

    def _get_records_key(self) -> str:
        return "tags"


class ReleaseTablesStream(FREDStream):
    """Stream for FRED release tables - /fred/release/tables endpoint."""

    name = "release_tables"
    path = "/release/tables"
    primary_keys: t.ClassVar[list[str]] = ["element_id"]
    replication_key = None
    records_jsonpath = "$.elements[*]"
    _add_surrogate_key = False

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
