"""FRED Tags streams - /fred/tags."""

from __future__ import annotations

import typing as t
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_fred.client import FREDStream, TagBasedFREDStream


class TagsStream(FREDStream):
    """Stream for FRED tags - /fred/tags endpoint."""

    name = "tags"
    path = "/tags"
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
        # Add tags-specific query parameters
        self.query_params.update(
            {
                "sort_order": "asc",
                "order_by": "name",
            }
        )

    def post_process(self, record: dict, context: Context | None = None) -> dict:
        """Transform raw data to match expected structure."""
        record = super().post_process(record, context)

        # Convert numeric fields
        if "popularity" in record and record["popularity"] is not None:
            try:
                record["popularity"] = int(record["popularity"])
            except (ValueError, TypeError):
                record["popularity"] = None

        if "series_count" in record and record["series_count"] is not None:
            try:
                record["series_count"] = int(record["series_count"])
            except (ValueError, TypeError):
                record["series_count"] = None

        return record


class RelatedTagsStream(FREDStream):
    """Stream for FRED related tags - /fred/related_tags endpoint."""

    name = "related_tags"
    path = "/related_tags"
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

        # Get tag_names from config - required parameter for this endpoint
        tag_names = self.config.get("tag_names")
        
        if not tag_names:
            raise ValueError(
                "RelatedTagsStream requires tag_names to be configured. "
                "No defaults are provided - all tag names must be explicitly configured."
            )
            
        if isinstance(tag_names, list):
            tag_names_str = ";".join(tag_names)
        elif isinstance(tag_names, str):
            tag_names_str = tag_names
        else:
            raise ValueError("tag_names must be a list or string")

        # Add related tags-specific query parameters
        self.query_params.update(
            {
                "sort_order": "asc",
                "order_by": "name",
                "tag_names": tag_names_str,
            }
        )

    def post_process(self, record: dict, context: Context | None = None) -> dict:
        """Transform raw data to match expected structure."""
        record = super().post_process(record, context)

        # Convert numeric fields
        if "popularity" in record and record["popularity"] is not None:
            try:
                record["popularity"] = int(record["popularity"])
            except (ValueError, TypeError):
                record["popularity"] = None

        if "series_count" in record and record["series_count"] is not None:
            try:
                record["series_count"] = int(record["series_count"])
            except (ValueError, TypeError):
                record["series_count"] = None

        return record


class TagsSeriesStream(FREDStream):
    """Stream for FRED tags series - /fred/tags/series endpoint."""

    name = "tags_series"
    path = "/tags/series"
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
            "tag_names", th.StringType, description="Tag names this series belongs to"
        ),
    ).to_dict()

    def __init__(self, tap) -> None:
        super().__init__(tap)
        # Add tags series-specific query parameters
        self.query_params.update(
            {
                "sort_order": "asc",
                "order_by": "series_id",
                "tag_names": "",  # Can be configured
            }
        )

    def _get_records_key(self) -> str:
        return "seriess"
