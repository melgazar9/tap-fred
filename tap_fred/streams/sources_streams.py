"""FRED Sources streams - /fred/sources endpoints."""

from __future__ import annotations

import typing as t

from singer_sdk import typing as th

from tap_fred.client import FREDStream, SourceBasedFREDStream


class SourcesStream(FREDStream):
    """Stream for FRED sources - /fred/sources endpoint.

    List-all endpoint - returns all sources. Does NOT accept source_id filter.
    Uses pagination per FRED API documentation (limit 1-1000, default 1000).
    """

    name = "sources"
    path = "/sources"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = None
    records_jsonpath = "$.sources[*]"
    _paginate = True

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType, description="Source ID"),
        th.Property("realtime_start", th.DateType, description="Real-time start date"),
        th.Property("realtime_end", th.DateType, description="Real-time end date"),
        th.Property("name", th.StringType, description="Source name"),
        th.Property("link", th.StringType, description="Source URL link"),
        th.Property("notes", th.StringType, description="Source notes/description"),
    ).to_dict()

    def __init__(self, tap) -> None:
        super().__init__(tap)

        # Get order_by from config - FRED API allows: source_id, name, realtime_start, realtime_end
        order_by = self.config.get("sources_order_by", "source_id")
        sort_order = self.config.get("sources_sort_order", "asc")

        self.query_params.update(
            {
                "order_by": order_by,
                "sort_order": sort_order,
            }
        )

    def _get_records_key(self) -> str:
        return "sources"


class SourceStream(SourceBasedFREDStream):
    """Stream for individual FRED source - /fred/source endpoint.

    Requires source_ids to be configured. Each source ID becomes a partition.
    """

    name = "source"
    path = "/source"
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
    ).to_dict()

    def _get_records_key(self) -> str:
        return "sources"


class SourceReleasesStream(SourceBasedFREDStream):
    """Stream for FRED source releases - /fred/source/releases endpoint.

    Requires source_ids to be configured. Each source ID becomes a partition.
    Uses pagination per FRED API documentation.
    """

    name = "source_releases"
    path = "/source/releases"
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
        th.Property(
            "source_id", th.IntegerType, description="Source ID this release belongs to"
        ),
    ).to_dict()

    def __init__(self, tap) -> None:
        super().__init__(tap)

        # Get order_by from config - FRED API allows: release_id, name, press_release, realtime_start, realtime_end
        order_by = self.config.get("source_releases_order_by", "release_id")
        sort_order = self.config.get("source_releases_sort_order", "asc")

        self.query_params.update(
            {
                "order_by": order_by,
                "sort_order": sort_order,
            }
        )

    def _get_records_key(self) -> str:
        return "releases"
