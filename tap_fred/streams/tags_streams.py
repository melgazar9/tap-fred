"""FRED Tags streams - /fred/tags."""

from __future__ import annotations

import typing as t

from singer_sdk import typing as th

from tap_fred.client import FREDStream
from tap_fred.helpers import join_tag_names


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

    def _get_records_key(self) -> str:
        return "tags"

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

        tag_names = self.config.get("tag_names")
        if not tag_names:
            raise ValueError(
                "RelatedTagsStream requires tag_names to be configured."
            )

        self.query_params.update({
            "sort_order": "asc",
            "order_by": "name",
            "tag_names": join_tag_names(tag_names),
        })

    def _get_records_key(self) -> str:
        return "tags"


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

        tag_names = self.config.get("tag_names")
        if not tag_names:
            raise ValueError(
                "TagsSeriesStream requires tag_names to be configured."
            )

        self.query_params.update({
            "sort_order": "asc",
            "order_by": "series_id",
            "tag_names": join_tag_names(tag_names),
        })

    def _get_records_key(self) -> str:
        return "seriess"
