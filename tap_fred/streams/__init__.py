"""FRED streams."""

# Series streams
# Category streams
from tap_fred.streams.category_streams import (
    CategoryChildrenStream,
    CategoryRelatedStream,
    CategoryRelatedTagsStream,
    CategorySeriesStream,
    CategoryStream,
    CategoryTagsStream,
)

# Maps/GeoFRED streams
from tap_fred.streams.maps_streams import (
    GeoFREDRegionalDataStream,
    GeoFREDSeriesDataStream,
)

# Release streams
from tap_fred.streams.releases_streams import (
    ReleaseDatesStream,
    ReleaseRelatedTagsStream,
    ReleaseSeriesStream,
    ReleaseSourcesStream,
    ReleasesStream,
    ReleaseStream,
    ReleaseTablesStream,
    ReleaseTagsStream,
)
from tap_fred.streams.series_streams import (
    SeriesCategoriesStream,
    SeriesObservationsStream,
    SeriesReleaseStream,
    SeriesSearchRelatedTagsStream,
    SeriesSearchStream,
    SeriesSearchTagsStream,
    SeriesStream,
    SeriesTagsStream,
    SeriesUpdatesStream,
    SeriesVintageDatesStream,
)

# Source streams
from tap_fred.streams.sources_streams import (
    SourceReleasesStream,
    SourcesStream,
    SourceStream,
)

# Tag streams
from tap_fred.streams.tags_streams import (
    RelatedTagsStream,
    TagsSeriesStream,
    TagsStream,
)

__all__ = [
    # Series streams
    "SeriesStream",
    "SeriesObservationsStream",
    "SeriesCategoriesStream",
    "SeriesReleaseStream",
    "SeriesSearchStream",
    "SeriesSearchTagsStream",
    "SeriesSearchRelatedTagsStream",
    "SeriesTagsStream",
    "SeriesUpdatesStream",
    "SeriesVintageDatesStream",
    # Category streams
    "CategoryStream",
    "CategoryChildrenStream",
    "CategoryRelatedStream",
    "CategorySeriesStream",
    "CategoryTagsStream",
    "CategoryRelatedTagsStream",
    # Release streams
    "ReleasesStream",
    "ReleaseStream",
    "ReleaseDatesStream",
    "ReleaseSeriesStream",
    "ReleaseSourcesStream",
    "ReleaseTagsStream",
    "ReleaseRelatedTagsStream",
    "ReleaseTablesStream",
    # Source streams
    "SourcesStream",
    "SourceStream",
    "SourceReleasesStream",
    # Tag streams
    "TagsStream",
    "RelatedTagsStream",
    "TagsSeriesStream",
    # Maps/GeoFRED streams
    "GeoFREDRegionalDataStream",
    "GeoFREDSeriesDataStream",
]
