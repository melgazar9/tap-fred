"""GeoFRED Maps streams - /geofred/regional/data and /geofred/series/data."""

from __future__ import annotations

import logging
import typing as t
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_fred.client import FREDStream


class GeoFREDRegionalDataStream(FREDStream):
    """Stream for GeoFRED regional data - /geofred/regional/data endpoint."""

    name = "geofred_regional_data"
    path = "/regional/data"
    primary_keys: t.ClassVar[list[str]] = ["region", "series_id", "date"]
    replication_key = "date"
    _add_surrogate_key = False

    @property
    def url_base(self) -> str:
        """Return the GeoFRED API URL root."""
        return "https://api.stlouisfed.org/geofred"

    schema = th.PropertiesList(
        th.Property("region", th.StringType, description="Region name"),
        th.Property("code", th.StringType, description="Region code"),
        th.Property("value", th.NumberType, description="Economic data value"),
        th.Property("series_id", th.StringType, description="FRED series identifier"),
        th.Property("date", th.DateType, description="Observation date"),
        th.Property("series_group", th.StringType, description="Series group ID"),
        th.Property(
            "region_type", th.StringType, description="Region type (state, county, msa)"
        ),
        th.Property(
            "season",
            th.StringType,
            description="Seasonality (SA, NSA, SSA, SAAR, NSAAR)",
        ),
        th.Property("units", th.StringType, description="Data units"),
    ).to_dict()

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Retrieve records from the GeoFRED API."""
        url = self.get_url()

        # GeoFRED requires specific parameters - use defaults for comprehensive data
        # Users can override these via stream-specific configuration
        base_params = {
            "api_key": self.config["api_key"],
            "file_type": "json",
        }

        # Get configurable regional data parameters
        regional_params = self.config.get("geofred_regional_params", {})

        # Default comprehensive parameters if not specified
        if not regional_params:
            # Use popular series groups and region types for comprehensive coverage
            default_configs = [
                {
                    "series_group": "882",
                    "region_type": "state",
                    "season": "NSA",
                    "units": "Dollars",
                    "frequency": "a",
                },
                {
                    "series_group": "882",
                    "region_type": "county",
                    "season": "NSA",
                    "units": "Dollars",
                    "frequency": "a",
                },
                {
                    "series_group": "882",
                    "region_type": "msa",
                    "season": "NSA",
                    "units": "Dollars",
                    "frequency": "a",
                },
            ]

            for config in default_configs:
                params = {**base_params, **config}

                # GeoFRED uses 'date' parameter, not 'start_date'
                if start_date := self.config.get("start_date"):
                    params["date"] = start_date
                else:
                    params["date"] = "2013-01-01"  # Use example date from documentation

                response_data = self._fetch_with_retry(url, params)
                data_array = response_data.get("data", [])

                for record in data_array:
                    # Enrich record with request parameters
                    record["series_group"] = config["series_group"]
                    record["region_type"] = config["region_type"]
                    record["season"] = config["season"]
                    record["units"] = response_data.get("meta", {}).get("units", "")

                    # Set date from request if not in response
                    if "date" not in record:
                        record["date"] = params.get("date")

                    record = self.post_process(record, context)
                    yield record
        else:
            # Use user-specified parameters
            for param_set in regional_params:
                params = {**base_params, **param_set}

                response_data = self._fetch_with_retry(url, params)
                data_array = response_data.get("data", [])

                for record in data_array:
                    record = self.post_process(record, context)
                    yield record


class GeoFREDSeriesDataStream(FREDStream):
    """Stream for GeoFRED series data - /geofred/series/data endpoint."""

    name = "geofred_series_data"
    path = "/series/data"
    primary_keys: t.ClassVar[list[str]] = ["region", "series_id", "date"]
    replication_key = "date"
    _add_surrogate_key = False

    @property
    def url_base(self) -> str:
        """Return the GeoFRED API URL root."""
        return "https://api.stlouisfed.org/geofred"

    schema = th.PropertiesList(
        th.Property("region", th.StringType, description="Region name"),
        th.Property("code", th.StringType, description="Region code"),
        th.Property("value", th.NumberType, description="Economic data value"),
        th.Property("series_id", th.StringType, description="FRED series identifier"),
        th.Property("date", th.DateType, description="Observation date"),
        th.Property("title", th.StringType, description="Series title"),
        th.Property("region_type", th.StringType, description="Region type"),
        th.Property("seasonality", th.StringType, description="Seasonality"),
        th.Property("units", th.StringType, description="Data units"),
        th.Property("frequency", th.StringType, description="Data frequency"),
    ).to_dict()

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Retrieve records from the GeoFRED series data API."""
        url = f"{self.url_base}{self.path}"

        # Get series IDs that have geographic data
        geofred_series_ids = self.config.get("geofred_series_ids", [])

        # Default geographic series if none specified
        if not geofred_series_ids:
            # Popular series with geographic data
            geofred_series_ids = [
                "WIPCPI",  # Consumer Price Index by region
                "SMU17266400000000001A",  # Employment data
                # Add more default geographic series as needed
            ]

        for series_id in geofred_series_ids:
            params = {
                "api_key": self.config["api_key"],
                "file_type": "json",
                "series_id": series_id,
            }

            # Add date range for historical data (GeoFRED uses 'date' parameter)
            if start_date := self.config.get("start_date"):
                params["date"] = start_date
            else:
                params["date"] = "2013-01-01"  # Default historical date

            try:
                response_data = self._fetch_with_retry(url, params)
                meta = response_data.get("meta", {})
                data_array = response_data.get("data", [])

                for record in data_array:
                    # Enrich with metadata
                    record["series_id"] = series_id
                    record["title"] = meta.get("title", "")
                    record["region_type"] = meta.get("region_type", "")
                    record["seasonality"] = meta.get("seasonality", "")
                    record["units"] = meta.get("units", "")
                    record["frequency"] = meta.get("frequency", "")

                    # Set date from request if not in response
                    if "date" not in record:
                        record["date"] = params.get("date")

                    record = self.post_process(record, context)
                    yield record

            except Exception as e:
                # Log warning but continue with other series
                logging.warning(
                    f"Failed to fetch GeoFRED data for series {series_id}: {e}"
                )
                continue
