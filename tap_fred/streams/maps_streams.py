"""GeoFRED Maps streams - /geofred/regional/data and /geofred/series/data."""

from __future__ import annotations

import typing as t
import jsonpath_ng
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_fred.client import FREDStream


class GeoFREDRegionalDataStream(FREDStream):
    """Stream for GeoFRED regional data - /geofred/regional/data endpoint.

    Requires geofred_regional_params to be configured in tap config.
    Each parameter set becomes a partition for parallel processing.
    """

    name = "geofred_regional_data"
    path = "/regional/data"
    primary_keys: t.ClassVar[list[str]] = [
        "region",
        "series_group",
        "date",
        "region_type",
    ]
    replication_key = "date"
    records_jsonpath = "$.data[*]"

    schema = th.PropertiesList(
        th.Property("region", th.StringType, description="Region name"),
        th.Property("code", th.StringType, description="Region code"),
        th.Property("value", th.NumberType, description="Economic data value"),
        th.Property("series_group", th.StringType, description="Series group ID"),
        th.Property("date", th.DateType, description="Observation date"),
        th.Property(
            "region_type", th.StringType, description="Region type (state, county, msa)"
        ),
        th.Property(
            "season",
            th.StringType,
            description="Seasonality (SA, NSA, SSA, SAAR, NSAAR)",
        ),
        th.Property("units", th.StringType, description="Data units"),
        th.Property("frequency", th.StringType, description="Data frequency"),
    ).to_dict()

    @property
    def url_base(self) -> str:
        """Return the GeoFRED API URL root - derived from main API URL."""
        return f"{self.config['api_url'].replace('/fred', '')}/geofred"

    @property
    def partitions(self):
        """Generate partitions from geofred_regional_params configuration.

        Each parameter set in the array becomes a partition for parallel processing.
        Supports wildcard "*" for comprehensive discovery.
        """
        regional_params = self.config.get("geofred_regional_params")

        if not regional_params:
            raise ValueError(
                "GeoFREDRegionalDataStream requires geofred_regional_params to be configured. "
                "No defaults are provided - all parameters must be explicitly configured."
            )

        if not isinstance(regional_params, list):
            raise ValueError(
                "geofred_regional_params must be an array of parameter sets"
            )

        # Expand wildcard parameters
        expanded_partitions = []
        for param_set in regional_params:
            expanded_partitions.extend(self._expand_wildcard_params(param_set))

        return expanded_partitions

    def _expand_wildcard_params(self, param_set):
        """Expand wildcard parameters to all possible combinations."""
        series_groups = self._expand_series_groups(param_set.get("series_group"))
        region_types = self._expand_region_types(param_set.get("region_type"))
        seasons = self._expand_seasons(param_set.get("season"))

        # Validate required parameters that have no wildcard support
        if not param_set.get("date"):
            raise ValueError(
                "GeoFRED API requires 'date' parameter in YYYY-MM-DD format. "
                "Add 'date' to each geofred_regional_params entry."
            )

        if not param_set.get("units"):
            raise ValueError(
                "GeoFRED API requires 'units' parameter (e.g., 'Dollars', 'Percent'). "
                "Add 'units' to each geofred_regional_params entry."
            )

        # Generate all combinations including required parameters
        partitions = []
        for series_group in series_groups:
            for region_type in region_types:
                for season in seasons:
                    partitions.append(
                        {
                            "series_group": series_group,
                            "region_type": region_type,
                            "season": season,
                            "date": param_set["date"],
                            "units": param_set["units"],
                            # Optional parameters if provided
                            "frequency": param_set.get(
                                "frequency", "a"
                            ),  # Default to annual
                        }
                    )

        return partitions

    @staticmethod
    def _expand_series_groups(series_group):
        """Expand series_group wildcard to available options."""
        if series_group != "*":
            return [series_group]

        # Wildcard not supported for GeoFRED - requires explicit configuration
        raise ValueError(
            "GeoFRED wildcard '*' for series_group is not supported. "
            "GeoFRED API requires explicit series_group values. "
            "Configure specific series_group IDs (e.g., '882', '883') instead of '*'."
        )

    @staticmethod
    def _expand_region_types(region_type):
        """Expand region_type wildcard to all supported types."""
        if region_type != "*":
            return [region_type]

        # Expand wildcard to all documented GeoFRED region types
        return [
            "bea",
            "msa",
            "frb",
            "necta",
            "state",
            "country",
            "county",
            "censusregion",
        ]

    @staticmethod
    def _expand_seasons(season):
        """Expand season wildcard to all supported seasons."""
        if season != "*":
            return [season]

        # Expand wildcard to all documented GeoFRED season values
        return ["SA", "NSA", "SSA", "SAAR", "NSAAR"]

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Retrieve records using partition-based parameter sets."""
        if not context:
            raise ValueError("GeoFREDRegionalDataStream requires partition context")

        url = self.get_url()
        params = self.query_params.copy()

        # Add partition-specific parameters
        params.update(context)

        response_data = self._make_request(url, params)

        # Extract records using JSONPath
        jsonpath_expr = jsonpath_ng.parse(self.records_jsonpath)
        matches = [match.value for match in jsonpath_expr.find(response_data)]

        for record in matches:
            # Enrich record with partition parameters for identification
            record.update(
                {
                    "series_group": context.get("series_group"),
                    "region_type": context.get("region_type"),
                    "season": context.get("season"),
                    "units": context.get("units"),
                    "frequency": context.get("frequency"),
                }
            )

            yield self.post_process(record, context)


class GeoFREDSeriesDataStream(FREDStream):
    """Stream for GeoFRED series data - /geofred/series/data endpoint.

    Requires geofred_series_ids to be configured in tap config.
    Each series ID becomes a partition for parallel processing.
    """

    name = "geofred_series_data"
    path = "/series/data"
    primary_keys: t.ClassVar[list[str]] = ["region", "series_id", "date"]
    replication_key = "date"
    records_jsonpath = "$.data[*]"

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

    @property
    def url_base(self) -> str:
        """Return the GeoFRED API URL root - derived from main API URL."""
        return f"{self.config['api_url'].replace('/fred', '')}/geofred"

    @property
    def partitions(self):
        """Generate partitions from geofred_series_ids configuration.

        Each series ID becomes a partition for parallel processing.
        Supports wildcard "*" for comprehensive discovery.
        """
        geofred_series_ids = self.config.get("geofred_series_ids")

        if not geofred_series_ids:
            raise ValueError(
                "GeoFREDSeriesDataStream requires geofred_series_ids to be configured. "
                "No defaults are provided - all series IDs must be explicitly configured."
            )

        if not isinstance(geofred_series_ids, list):
            raise ValueError("geofred_series_ids must be an array of series IDs")

        # Handle wildcard discovery
        if geofred_series_ids == ["*"]:
            discovered_series_ids = self._discover_geofred_series_ids()
            return [{"series_id": series_id} for series_id in discovered_series_ids]

        return [{"series_id": series_id} for series_id in geofred_series_ids]

    def _discover_geofred_series_ids(self):
        """Discover all available GeoFRED series IDs."""
        try:
            # Use main FRED API to discover all series tagged with 'geography' or 'regional'
            url = f"{self.config['api_url']}/series/search"
            params = self.query_params.copy()
            params.update(
                {
                    "search_text": "geography regional",
                    "limit": 100000,  # Maximum to get comprehensive list
                    "order_by": "series_id",
                    "sort_order": "asc",
                }
            )

            response_data = self._make_request(url, params)
            series_list = response_data.get("seriess", [])

            # Extract series IDs from search results
            discovered_series_ids = [
                series.get("id") for series in series_list if series.get("id")
            ]

            if discovered_series_ids:
                self.logger.info(
                    f"Discovered {len(discovered_series_ids)} GeoFRED series IDs"
                )
                return discovered_series_ids
            else:
                raise ValueError(
                    "No GeoFRED series IDs discovered via search. "
                    "Configure explicit geofred_series_ids instead of using wildcard '*'."
                )

        except Exception as e:
            raise ValueError(
                f"Failed to discover GeoFRED series IDs: {e}. "
                "Configure explicit geofred_series_ids instead of using wildcard '*'."
            )

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Retrieve records for a specific series ID from partition context."""
        if not context or "series_id" not in context:
            raise ValueError(
                "GeoFREDSeriesDataStream requires series_id in partition context"
            )

        series_id = context["series_id"]
        url = self.get_url()
        params = self.query_params.copy()
        params["series_id"] = series_id

        response_data = self._make_request(url, params)
        meta = response_data.get("meta", {})

        # Extract records using JSONPath
        jsonpath_expr = jsonpath_ng.parse(self.records_jsonpath)
        matches = [match.value for match in jsonpath_expr.find(response_data)]

        for record in matches:
            # Enrich with metadata from API response
            record.update(
                {
                    "series_id": series_id,
                    "title": meta.get("title", ""),
                    "region_type": meta.get("region_type", ""),
                    "seasonality": meta.get("seasonality", ""),
                    "units": meta.get("units", ""),
                    "frequency": meta.get("frequency", ""),
                }
            )

            yield self.post_process(record, context)
