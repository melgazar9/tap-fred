"""FRED Categories stream - /fred/category endpoint."""

from __future__ import annotations

import typing as t
from singer_sdk import typing as th
from singer_sdk.helpers.types import Context
from tap_fred.client import TreeTraversalFREDStream, FREDStream, CategoryBasedFREDStream


class CategoryStream(FREDStream):
    """Stream for FRED categories - fetches individual categories using /fred/category endpoint.
    
    Thread-safe category ID caching at the tap level.
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

    def _get_records_key(self) -> str:
        """JSON key containing the records in API response."""
        return "categories"

    @property
    def partitions(self):
        """Generate partitions for each category ID."""
        category_ids = self._tap.config.get("category_ids", ["*"])

        # For specific IDs, use direct partitions
        if category_ids != ["*"]:
            return [{"category_id": int(cid)} for cid in category_ids if cid != "*"]
        
        # For wildcard, return None to use custom get_records()
        return None
    
    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Override to handle wildcard discovery using category/children tree traversal."""
        category_ids = self._tap.config.get("category_ids", ["*"])
        
        if category_ids == ["*"]:
            # For wildcard, use tree traversal via /category/children to discover all categories
            from collections import deque
            
            discovered_ids = set()
            queue = deque([0])  # Start with root category
            processed = set()
            
            while queue:
                category_id = queue.popleft()
                if category_id in processed:
                    continue
                
                # First, get the category itself using /fred/category
                category_url = self.get_url()  # /fred/category
                category_params = self.query_params.copy()
                category_params["category_id"] = category_id
                
                try:
                    category_data = self._make_request(category_url, category_params)
                    categories = category_data.get("categories", [])
                    
                    for category in categories:
                        discovered_ids.add(category["id"])
                        yield self.post_process(category, context)
                        
                except Exception as e:
                    self.logger.warning(f"Failed to fetch category {category_id}: {e}")
                    continue

                processed.add(category_id)
                
                # Then get children using /fred/category/children
                children_url = f"{self.url_base}/category/children"
                children_params = self.query_params.copy()
                children_params["category_id"] = category_id
                
                try:
                    children_data = self._make_request(children_url, children_params)
                    children = children_data.get("categories", [])
                    
                    for child in children:
                        child_id = child.get("id")
                        if child_id and child_id not in processed and child_id not in discovered_ids:
                            queue.append(child_id)
                            
                except Exception as e:
                    self.logger.warning(f"Failed to fetch children for category {category_id}: {e}")
                    continue
        else:
            # For specific IDs, use normal partition processing
            yield from super().get_records(context)


class CategoryChildrenStream(CategoryBasedFREDStream):
    """Stream for FRED category relationships - /fred/category/children endpoint.
    
    Uses cached category IDs from CategoryStream instead of separate tree traversal.
    """

    name = "category_children"
    path = "/category/children"
    primary_keys: t.ClassVar[list[str]] = ["id", "parent_id"]
    records_jsonpath = "$.categories[*]"

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType, description="Child category ID"),
        th.Property("name", th.StringType, description="Child category name"),
        th.Property("parent_id", th.IntegerType, description="Parent category ID"),
    ).to_dict()

    def _get_records_key(self) -> str:
        """JSON key containing the records in API response."""
        return "categories"

    def post_process(self, row: dict, context: Context | None = None) -> dict:
        """Add parent_id from partition context to create proper parent-child relationships."""
        # Get the parent category ID from the partition context
        if context and "category_id" in context:
            parent_id = context["category_id"]
            # The returned children have their own IDs, but we need to track the parent relationship
            row["parent_id"] = parent_id
        
        return super().post_process(row, context)


class CategoryRelatedStream(CategoryBasedFREDStream):
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

    def _get_records_key(self) -> str:
        """JSON key containing the records in API response."""
        return "categories"


class CategorySeriesStream(CategoryBasedFREDStream):
    """Stream for FRED series in categories - /fred/category/series endpoint."""

    name = "category_series"
    path = "/category/series"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    records_jsonpath = "$.seriess[*]"

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


class CategoryTagsStream(CategoryBasedFREDStream):
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


class CategoryRelatedTagsStream(CategoryBasedFREDStream):
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
        if not tag_names:
            raise ValueError("CategoryRelatedTagsStream requires tag_names to be configured")
            
        if isinstance(tag_names, list):
            tag_names_str = ";".join(tag_names)
        elif isinstance(tag_names, str):
            tag_names_str = tag_names
        else:
            raise ValueError("tag_names must be a list or string")

        self.query_params.update(
            {
                "tag_names": tag_names_str,
            }
        )

    def _get_records_key(self) -> str:
        return "tags"