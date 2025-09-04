"""FRED Sources streams - /fred/sources."""

from __future__ import annotations

import typing as t
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_fred.client import FREDStream


class SourcesStream(FREDStream):
    """Stream for FRED sources - /fred/sources endpoint."""

    name = "sources"
    path = "/sources"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = None
    records_jsonpath = "$.sources[*]"
    _add_surrogate_key = False
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
        # Add sources-specific query parameters
        self.query_params.update(
            {
                "sort_order": "asc",
                "order_by": "source_id",
            }
        )


class SourceStream(FREDStream):
    """Stream for individual FRED source - /fred/source endpoint."""

    name = "source"
    path = "/source"
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
    ).to_dict()

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Get specific sources - requires source_id parameter."""
        # This stream needs specific source IDs - would need configuration
        # For now, fall back to getting all sources via sources endpoint
        sources_stream = SourcesStream(self.tap)
        for record in sources_stream.get_records(context):
            yield record

    def __init__(self, tap) -> None:
        super().__init__(tap)
        # Add sources-specific query parameters
        self.query_params.update(
            {
                "sort_order": "asc",
                "order_by": "source_id",
            }
        )


class SourceReleasesStream(FREDStream):
    """Stream for FRED source releases - /fred/source/releases endpoint."""

    name = "source_releases"
    path = "/source/releases"
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
            "source_id", th.IntegerType, description="Source ID this release belongs to"
        ),
    ).to_dict()

    def __init__(self, tap) -> None:
        super().__init__(tap)
        # Add sources-specific query parameters
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
