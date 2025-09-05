"""FRED Sources streams - /fred/sources endpoints."""

from __future__ import annotations

import typing as t
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_fred.client import FREDStream


class SourcesStream(FREDStream):
    """Stream for FRED sources - /fred/sources endpoint.
    
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
        
        self.query_params.update({
            "order_by": order_by,
            "sort_order": sort_order,
        })

    def _get_records_key(self) -> str:
        return "sources"


class SourceStream(FREDStream):
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

    @property
    def partitions(self):
        """Generate partitions from source_ids configuration."""
        source_ids = self.config.get("source_ids")
        
        if not source_ids:
            raise ValueError(
                "SourceStream requires source_ids to be configured. "
                "No defaults are provided - all source IDs must be explicitly configured."
            )
        
        if source_ids == ["*"]:
            # Use cached source IDs from tap level
            cached_ids = self._tap.get_cached_source_ids()
            return [{"source_id": int(sid)} for sid in cached_ids]
        else:
            return [{"source_id": int(sid)} for sid in source_ids if sid != "*"]

    def _get_records_key(self) -> str:
        return "sources"


class SourceReleasesStream(FREDStream):
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
        th.Property("source_id", th.IntegerType, description="Source ID this release belongs to"),
    ).to_dict()

    def __init__(self, tap) -> None:
        super().__init__(tap)
        
        # Get order_by from config - FRED API allows: release_id, name, press_release, realtime_start, realtime_end
        order_by = self.config.get("source_releases_order_by", "release_id")
        sort_order = self.config.get("source_releases_sort_order", "asc")
        
        self.query_params.update({
            "order_by": order_by,
            "sort_order": sort_order,
        })

    @property
    def partitions(self):
        """Generate partitions from source_ids configuration."""
        source_ids = self.config.get("source_ids")
        
        if not source_ids:
            raise ValueError(
                "SourceReleasesStream requires source_ids to be configured. "
                "No defaults are provided - all source IDs must be explicitly configured."
            )
        
        if source_ids == ["*"]:
            # Use cached source IDs from tap level
            cached_ids = self._tap.get_cached_source_ids()
            return [{"source_id": int(sid)} for sid in cached_ids]
        else:
            return [{"source_id": int(sid)} for sid in source_ids if sid != "*"]

    def _get_records_key(self) -> str:
        return "releases"

    def post_process(self, record: dict, context: Context | None = None) -> dict:
        """Transform raw data to match expected structure."""
        # Apply business logic BEFORE calling super()
        if "press_release" in record and isinstance(record["press_release"], str):
            record["press_release"] = record["press_release"].lower() == "true"

        return super().post_process(record, context)