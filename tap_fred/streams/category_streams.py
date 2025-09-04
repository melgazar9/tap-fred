"""FRED Categories stream - /fred/category endpoint."""

from __future__ import annotations

import typing as t
from singer_sdk import typing as th
from collections import deque
import requests.exceptions
from singer_sdk.helpers.types import Context
from tap_fred.client import TreeTraversalFREDStream, FREDStream


class CategoryStream(FREDStream):
    """Stream for FRED categories - fetches individual categories using /fred/category endpoint.

    Follows tap-fmp partitions pattern with thread-safe category ID caching at the tap level.
    """

    name = "categories"
    path = "/category"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    records_jsonpath = "$.categories[*]"

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType, description="Category ID"),
        th.Property("name", th.StringType, description="Category name"),
        th.Property("parent_id", th.IntegerType, description="Parent category ID"),
        th.Property("notes", th.StringType, description="Category notes/description"),
    ).to_dict()

    @property
    def partitions(self):
        """Generate partitions for each category ID following tap-fmp pattern."""
        category_ids = self._tap.config.get("category_ids", ["*"])

        # Handle wildcard - use streaming discovery, not batch caching
        if category_ids == ["*"]:
            # For wildcard, override partitions and implement streaming in get_records
            return None  # This will cause get_records to be called instead of partition processing
        else:
            # For specific IDs, use direct partitions
            return [{"category_id": int(cid)} for cid in category_ids if cid != "*"]

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Override to handle streaming wildcard discovery."""
        if self.partitions is None:  # Wildcard case
            # Stream records as we discover categories, don't batch everything
            self.logger.info("Streaming category discovery for wildcard selection...")

            discovered_ids = []  # Cache discovered IDs for other streams
            all_category_ids = set()
            queue = deque([0])  # Start with root category ID per FRED docs
            processed = set()
            failed_count = 0
            max_consecutive_failures = 10

            while queue and failed_count < max_consecutive_failures:
                category_id = queue.popleft()
                if category_id in processed:
                    continue
                processed.add(category_id)
                all_category_ids.add(category_id)
                discovered_ids.append(category_id)

                # Fetch and IMMEDIATELY yield this category record
                url = self.get_url()
                params = self.query_params.copy()
                params["category_id"] = category_id

                try:
                    response_data = self._fetch_with_retry(url, params)
                    categories = response_data.get("categories", [])

                    # Yield records immediately as we find them
                    for category in categories:
                        yield self.post_process(category, context)

                    # Then discover children for further traversal
                    children_url = f"{self.url_base}/category/children"
                    children_params = self.query_params.copy()
                    children_params["category_id"] = category_id

                    try:
                        children_response = self._fetch_with_retry(
                            children_url, children_params
                        )
                        children = children_response.get("categories", [])

                        for child in children:
                            child_id = child.get("id")
                            if child_id and child_id not in processed:
                                queue.append(child_id)
                                all_category_ids.add(child_id)
                    except Exception as e:
                        self.logger.warning(
                            f"Failed to get children for category {category_id}: {e}"
                        )

                except Exception as e:
                    self.logger.warning(f"Failed to get category {category_id}: {e}")
                    failed_count += 1
                    continue

                failed_count = 0  # Reset on success

            # Cache discovered IDs for other streams to use
            with self._tap._cache_lock:
                self._tap._category_cache = discovered_ids
            self.logger.info(
                f"Cached {len(discovered_ids)} category IDs for other streams"
            )

        else:
            # For specific category IDs, use normal partition processing
            yield from super().get_records(context)


class CategoryChildrenStream(TreeTraversalFREDStream):
    """Stream for FRED category relationships - /fred/category/children endpoint."""

    name = "category_children"
    path = "/category/children"
    primary_keys: t.ClassVar[list[str]] = ["id", "parent_id"]
    records_jsonpath = "$.categories[*]"

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType, description="Category ID"),
        th.Property("name", th.StringType, description="Category name"),
        th.Property("parent_id", th.IntegerType, description="Parent category ID"),
    ).to_dict()

    def _fetch_all_category_ids(self) -> list[int]:
        """Fetch ALL category IDs using tree traversal - follows FRED API documentation."""
        self.logger.info(
            "Fetching all category IDs from FRED API using tree traversal..."
        )
        all_category_ids = set()
        queue = deque([0])  # Start with root category ID per FRED docs
        processed = set()
        failed_count = 0
        max_consecutive_failures = 10  # Prevent infinite loops on systemic API issues

        while queue and failed_count < max_consecutive_failures:
            category_id = queue.popleft()
            if category_id in processed:
                continue
            processed.add(category_id)
            all_category_ids.add(category_id)

            url = self.get_url()
            params = self.query_params.copy()
            params["category_id"] = category_id

            try:
                response_data = self._fetch_with_retry(url, params)
                children = response_data.get("categories", [])

                for child in children:
                    child_id = child.get("id")
                    if child_id and child_id not in processed:
                        queue.append(child_id)
                        all_category_ids.add(child_id)

            except requests.exceptions.HTTPError as e:
                if e.response and e.response.status_code >= 500:
                    self.logger.info(
                        f"Server error for category {category_id}: {e.response.status_code}, skipping"
                    )
                    failed_count += 1
                else:
                    self.logger.warning(f"HTTP error for category {category_id}: {e}")
                    failed_count += 1
                continue
            except Exception as e:
                self.logger.warning(
                    f"Failed to get children for category {category_id}: {e}"
                )
                failed_count += 1
                continue

            # Reset failure counter on successful API call
            failed_count = 0

        sorted_ids = sorted(all_category_ids)
        self.logger.info(
            f"Discovered {len(sorted_ids)} total category IDs (processed: {len(processed)}, failed: {failed_count})"
        )
        return sorted_ids


class CategoryRelatedStream(TreeTraversalFREDStream):
    """Stream for FRED category related categories - /fred/category/related endpoint."""

    name = "category_related"
    path = "/category/related"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    records_jsonpath = "$.categories[*]"

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType, description="Category ID"),
        th.Property("name", th.StringType, description="Category name"),
        th.Property("parent_id", th.IntegerType, description="Parent category ID"),
    ).to_dict()


class CategorySeriesStream(TreeTraversalFREDStream):
    """Stream for FRED series in categories - /fred/category/series endpoint."""

    name = "category_series"
    path = "/category/series"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    records_jsonpath = "$.series[*]"

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
            "category_id",
            th.IntegerType,
            description="Category ID this series belongs to",
        ),
    ).to_dict()

    def _get_records_key(self) -> str:
        return "seriess"

    def _get_child_id(self, record: dict):
        """Don't traverse child nodes - we only want series for specified categories."""
        return None


class CategoryTagsStream(TreeTraversalFREDStream):
    """Stream for FRED category tags - /fred/category/tags endpoint."""

    name = "category_tags"
    path = "/category/tags"
    primary_keys: t.ClassVar[list[str]] = ["name"]
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
            "category_id",
            th.IntegerType,
            description="Category ID these tags belong to",
        ),
    ).to_dict()

    def _get_records_key(self) -> str:
        return "tags"


class CategoryRelatedTagsStream(TreeTraversalFREDStream):
    """Stream for FRED category related tags - /fred/category/related_tags endpoint.

    Note: This endpoint requires tag_names parameter. Stream will only be enabled
    if tag_names are configured in the tap config.
    """

    name = "category_related_tags"
    path = "/category/related_tags"
    primary_keys: t.ClassVar[list[str]] = ["name"]
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
            "category_id", th.IntegerType, description="Category ID for related tags"
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
            self.logger.info(
                f"Stream {self.name}: No tag_names configured, using default 'quarterly'"
            )

        # Add tag_names to query_params so TreeTraversalFREDStream can use it
        self.query_params.update(
            {
                "tag_names": tag_names_str,
            }
        )

    def _get_records_key(self) -> str:
        return "tags"
