"""FRED streams."""

# Series streams
from tap_fred.streams.series_streams import (
    SeriesStream,
    SeriesObservationsStream,
    SeriesCategoriesStream,
    SeriesReleaseStream,
    SeriesSearchStream,
    SeriesSearchTagsStream,
    SeriesSearchRelatedTagsStream,
    SeriesTagsStream,
    SeriesUpdatesStream,
    SeriesVintageDatesStream,
)

# Category streams
from tap_fred.streams.category_streams import (
    CategoryStream,
    CategoryChildrenStream,
    CategoryRelatedStream,
    CategorySeriesStream,
    CategoryTagsStream,
    CategoryRelatedTagsStream,
)

# Release streams
from tap_fred.streams.releases_streams import (
    ReleasesStream,
    ReleaseStream,
    ReleaseDatesStream,
    SingleReleaseDatesStream,
    ReleaseSeriesStream,
    ReleaseSourcesStream,
    ReleaseTagsStream,
    ReleaseRelatedTagsStream,
    ReleaseTablesStream,
)

# Source streams
from tap_fred.streams.sources_streams import (
    SourcesStream,
    SourceStream,
    SourceReleasesStream,
)

# Tag streams
from tap_fred.streams.tags_streams import (
    TagsStream,
    RelatedTagsStream,
    TagsSeriesStream,
)

# Maps/GeoFRED streams
from tap_fred.streams.maps_streams import (
    GeoFREDRegionalDataStream,
    GeoFREDSeriesDataStream,
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
    "SingleReleaseDatesStream",
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
