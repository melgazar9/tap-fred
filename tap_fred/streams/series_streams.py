"""FRED Series Observations stream - /fred/series/observations."""

from __future__ import annotations

import typing as t
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_fred.client import FREDStream


class SeriesPartitionStream(FREDStream):
    """Base stream class for FRED series partitioned streams."""

    @property
    def partitions(self):
        """Return partitions based on series IDs."""
        series_ids = self._tap.get_cached_series_ids()
        return [{"series_id": series_id} for series_id in series_ids]

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Retrieve records for a specific series ID from context."""
        if context is None or "series_id" not in context:
            # No partitions available, skip processing
            return

        url = f"{self.url_base}{self.path}"
        params = {
            "api_key": self.config["api_key"],
            "file_type": "json",
            "series_id": context["series_id"],
            "sort_order": "asc",
        }

        # Add realtime parameters for ALFRED mode BEFORE date filtering
        data_mode = self.config.get("data_mode", "FRED").upper()
        if data_mode == "ALFRED":
            realtime_start = self.config.get("realtime_start")
            realtime_end = self.config.get("realtime_end")
            if realtime_start:
                params["realtime_start"] = realtime_start
            if realtime_end:
                params["realtime_end"] = realtime_end
            elif realtime_start and not realtime_end:
                params["realtime_end"] = realtime_start

        self._add_date_filtering(params, context)

        response_data = self._fetch_with_retry(url, params)
        records = response_data.get("observations", [])
        for record in records:
            record["series_id"] = context["series_id"]
            record = self.post_process(record, context)
            yield record


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
