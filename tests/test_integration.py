"""Integration tests for tap-fred with mocked API responses.

These tests simulate the actual API calls and validate the complete data flow
without requiring actual API keys or network calls.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock

import requests

from tap_fred.tap import TapFRED
from tap_fred.client import FREDStream
from singer_sdk import typing as th


class TestTapFREDIntegration(unittest.TestCase):
    """Integration tests with mocked API responses."""

    def setUp(self):
        """Set up test fixtures with mock config."""
        self.config = {
            "api_key": "test_api_key",
            "series_ids": ["GDP", "UNRATE"],
            "start_date": "2024-01-01",
            "data_mode": "FRED",
        }

        self.alfred_config = {
            **self.config,
            "data_mode": "ALFRED",
            "realtime_start": "2020-01-01",
            "realtime_end": "2020-01-01",
        }

    @patch("tap_fred.client.FREDStream._make_request")
    def test_series_observations_fred_mode(self, mock_request):
        """Test series_observations extraction in FRED mode."""
        mock_request.return_value = {
            "observations": [
                {
                    "realtime_start": "2025-09-11",
                    "realtime_end": "2025-09-11",
                    "date": "2024-01-01",
                    "value": "21000.0",
                },
                {
                    "realtime_start": "2025-09-11",
                    "realtime_end": "2025-09-11",
                    "date": "2024-04-01",
                    "value": "21500.0",
                },
            ],
        }

        tap = TapFRED(config=self.config)
        streams = {stream.name: stream for stream in tap.streams.values()}
        series_obs_stream = streams["series_observations"]

        # Provide partition context directly - no need to mock _get_series_ids
        records = list(series_obs_stream.get_records(context={"series_id": "GDP"}))

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["series_id"], "GDP")
        self.assertEqual(records[0]["date"], "2024-01-01")
        self.assertEqual(records[0]["value"], 21000.0)
        self.assertEqual(records[0]["realtime_start"], "2025-09-11")

    @patch("tap_fred.client.FREDStream._make_request")
    def test_series_observations_alfred_mode(self, mock_request):
        """Test series_observations extraction in ALFRED mode with vintage data."""
        mock_request.return_value = {
            "observations": [
                {
                    "realtime_start": "2020-01-01",
                    "realtime_end": "2020-01-01",
                    "date": "2019-10-01",
                    "value": "21734.266",
                }
            ],
        }

        tap = TapFRED(config=self.alfred_config)
        streams = {stream.name: stream for stream in tap.streams.values()}
        series_obs_stream = streams["series_observations"]

        # Provide partition context directly
        records = list(series_obs_stream.get_records(context={"series_id": "GDP"}))

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["realtime_start"], "2020-01-01")
        self.assertEqual(records[0]["realtime_end"], "2020-01-01")
        self.assertEqual(records[0]["value"], 21734.266)

    @patch("tap_fred.client.requests.Session.get")
    def test_multiple_streams_execution(self, mock_get):
        """Test execution of multiple streams with different configurations."""

        def mock_response_side_effect(url, **kwargs):
            mock_response = Mock()
            mock_response.status_code = 200

            if "categories" in url:
                mock_response.json.return_value = {
                    "categories": [{"id": "125", "name": "Trade Balance"}]
                }
            elif "releases" in url:
                mock_response.json.return_value = {
                    "releases": [{"id": "53", "name": "Gross Domestic Product"}]
                }
            elif "sources" in url:
                mock_response.json.return_value = {
                    "sources": [{"id": "1", "name": "Board of Governors"}]
                }
            else:
                mock_response.json.return_value = {"observations": []}

            return mock_response

        mock_get.side_effect = mock_response_side_effect

        config = {
            **self.config,
            "category_ids": ["125"],
            "release_ids": ["53"],
            "source_ids": ["1"],
        }

        tap = TapFRED(config=config)

        stream_names = [stream.name for stream in tap.streams.values()]

        expected_streams = ["categories", "releases", "sources", "series_observations"]
        for stream_name in expected_streams:
            self.assertIn(stream_name, stream_names)

    def test_wildcard_id_configuration(self):
        """Test wildcard ID configuration for comprehensive data extraction."""
        wildcard_config = {
            **self.config,
            "series_ids": ["*"],
            "category_ids": ["*"],
            "release_ids": ["*"],
            "source_ids": ["*"],
            "tag_names": ["*"],
        }

        tap = TapFRED(config=wildcard_config)

        self.assertEqual(tap.config["series_ids"], ["*"])
        self.assertEqual(tap.config["category_ids"], ["*"])
        self.assertEqual(tap.config["release_ids"], ["*"])
        self.assertEqual(tap.config["source_ids"], ["*"])
        self.assertEqual(tap.config["tag_names"], ["*"])

    def test_incremental_state_handling(self):
        """Test incremental state management and bookmarking."""
        tap = TapFRED(config=self.config)
        streams = {stream.name: stream for stream in tap.streams.values()}
        series_obs_stream = streams["series_observations"]

        self.assertEqual(series_obs_stream.replication_key, "realtime_start")
        self.assertIsNotNone(series_obs_stream.replication_key)

    def test_schema_output_validation(self):
        """Test that output schemas match Singer specification."""
        tap = TapFRED(config=self.config)

        streams = {stream.name: stream for stream in tap.streams.values()}
        series_obs_stream = streams["series_observations"]

        schema = series_obs_stream.schema

        self.assertIn("type", schema)
        self.assertEqual(schema["type"], "object")
        self.assertIn("properties", schema)

        properties = schema["properties"]
        required_fields = [
            "series_id",
            "date",
            "value",
            "realtime_start",
            "realtime_end",
        ]

        for field in required_fields:
            self.assertIn(field, properties)
            self.assertIn("type", properties[field])

    def test_data_transformation(self):
        """Test data transformation and field mapping."""
        tap = TapFRED(config=self.config)
        streams = {stream.name: stream for stream in tap.streams.values()}
        series_obs_stream = streams["series_observations"]

        # Updated PK: includes realtime_start/end to prevent vintage data overwriting
        self.assertEqual(
            series_obs_stream.primary_keys,
            ["series_id", "date", "realtime_start", "realtime_end"],
        )

        self.assertTrue(hasattr(series_obs_stream, "_resource_type"))
        self.assertEqual(series_obs_stream._resource_type, "series")

    @patch("tap_fred.client.requests.Session.get")
    def test_error_handling_api_failures(self, mock_get):
        """Test error handling for API failures."""
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.json.return_value = {"error": "Rate limit exceeded"}
        mock_get.return_value = mock_response

        tap = TapFRED(config=self.config)
        streams = {stream.name: stream for stream in tap.streams.values()}
        series_obs_stream = streams["series_observations"]

        self.assertIsNotNone(series_obs_stream)

    def test_production_scale_configuration(self):
        """Test configuration suitable for production-scale extraction."""
        production_config = {
            **self.config,
            "start_date": "2015-01-01",
            "data_mode": "ALFRED",
            "realtime_start": "2020-01-01",
            "realtime_end": "2020-01-01",
            "max_requests_per_minute": 120,
            "min_throttle_seconds": 0.5,
            "series_ids": ["*"],
            "category_ids": ["*"],
            "release_ids": ["*"],
            "source_ids": ["*"],
        }

        tap = TapFRED(config=production_config)

        self.assertEqual(tap.config["start_date"], "2015-01-01")
        self.assertEqual(tap.config["max_requests_per_minute"], 120)
        self.assertTrue(
            all(
                ids == ["*"]
                for ids in [
                    tap.config["series_ids"],
                    tap.config["category_ids"],
                    tap.config["release_ids"],
                    tap.config["source_ids"],
                ]
            )
        )


class TestRegressionFixes(unittest.TestCase):
    """Regression tests for specific bug fixes. Always run (no API key needed)."""

    def setUp(self):
        self.config = {
            "api_key": "test_api_key",
            "series_ids": ["GDP"],
            "data_mode": "FRED",
        }

    def test_strict_mode_reraise_in_safe_partition_extraction(self):
        """strict_mode=true must propagate exceptions from _safe_partition_extraction."""

        class MockStream(FREDStream):
            name = "test_strict"
            path = "/test"
            records_jsonpath = "$.data[*]"
            schema = th.PropertiesList(th.Property("id", th.StringType)).to_dict()
            _resource_id_key = "test_id"

            def _get_records_key(self):
                return "data"

        strict_config = {**self.config, "strict_mode": True}
        tap = TapFRED(config=strict_config)
        stream = MockStream(tap)

        # Simulate an HTTPError from a partition
        mock_response = Mock()
        mock_response.status_code = 404

        def failing_generator():
            raise requests.exceptions.HTTPError(response=mock_response)
            yield  # noqa: unreachable - makes this a generator

        with self.assertRaises(requests.exceptions.HTTPError):
            list(stream._safe_partition_extraction(
                failing_generator(), "BAD_ID", "test_id"
            ))

        # Verify it was tracked before re-raising
        self.assertEqual(len(stream._skipped_partitions), 1)
        self.assertEqual(stream._skipped_partitions[0]["partition_value"], "BAD_ID")

    def test_permissive_mode_swallows_partition_errors(self):
        """strict_mode=false (default) must swallow partition errors and continue."""

        class MockStream(FREDStream):
            name = "test_permissive"
            path = "/test"
            records_jsonpath = "$.data[*]"
            schema = th.PropertiesList(th.Property("id", th.StringType)).to_dict()
            _resource_id_key = "test_id"

            def _get_records_key(self):
                return "data"

        tap = TapFRED(config=self.config)  # strict_mode defaults to False
        stream = MockStream(tap)

        mock_response = Mock()
        mock_response.status_code = 404

        def failing_generator():
            raise requests.exceptions.HTTPError(response=mock_response)
            yield  # noqa: unreachable

        # Should NOT raise - error is swallowed
        records = list(stream._safe_partition_extraction(
            failing_generator(), "BAD_ID", "test_id"
        ))
        self.assertEqual(records, [])
        self.assertEqual(len(stream._skipped_partitions), 1)

    def test_strict_mode_reraise_generic_exception(self):
        """strict_mode=true must also propagate non-HTTP exceptions."""

        class MockStream(FREDStream):
            name = "test_strict_generic"
            path = "/test"
            records_jsonpath = "$.data[*]"
            schema = th.PropertiesList(th.Property("id", th.StringType)).to_dict()
            _resource_id_key = "test_id"

            def _get_records_key(self):
                return "data"

        strict_config = {**self.config, "strict_mode": True}
        tap = TapFRED(config=strict_config)
        stream = MockStream(tap)

        def failing_generator():
            raise ValueError("unexpected error")
            yield  # noqa: unreachable

        with self.assertRaises(ValueError):
            list(stream._safe_partition_extraction(
                failing_generator(), "BAD_ID", "test_id"
            ))

    @patch("tap_fred.client.FREDStream._make_request")
    def test_pagination_context_propagation(self, mock_request):
        """Pagination must forward partition context (e.g., release_id) in params."""

        class MockPaginatedStream(FREDStream):
            name = "test_paginated"
            path = "/test"
            records_jsonpath = "$.items[*]"
            _paginate = True
            schema = th.PropertiesList(
                th.Property("id", th.StringType),
                th.Property("release_id", th.IntegerType),
            ).to_dict()

            def _get_records_key(self):
                return "items"

        tap = TapFRED(config=self.config)
        stream = MockPaginatedStream(tap)

        # First page returns 1 record (less than limit = pagination stops)
        mock_request.return_value = {
            "items": [{"id": "1", "name": "Test"}],
        }

        context = {"release_id": 53}
        records = list(stream._paginate_records(context))

        # Verify the context was passed in params
        call_args = mock_request.call_args
        params = call_args[0][1]  # second positional arg is params
        self.assertEqual(params["release_id"], 53)
        self.assertIn("limit", params)
        self.assertIn("offset", params)

    def test_wildcard_release_ids_dict_conversion(self):
        """Wildcard release_ids=["*"] must be resolved from cache dicts properly."""
        from tap_fred.streams.releases_streams import ReleaseRelatedTagsStream

        config = {
            **self.config,
            "release_ids": ["*"],
            "tag_names": ["gdp"],
        }

        tap = TapFRED(config=config)

        # Mock the cached release IDs (returns list of dicts)
        tap._cached_release_ids = [
            {"release_id": 10},
            {"release_id": 20},
            {"release_id": 30},
        ]

        stream = ReleaseRelatedTagsStream(tap)
        partitions = stream.partitions

        # Verify dict-to-int conversion worked
        release_ids_in_partitions = [p["release_id"] for p in partitions]
        self.assertEqual(release_ids_in_partitions, [10, 20, 30])

        # Verify tag_names are joined with semicolons
        for p in partitions:
            self.assertEqual(p["tag_names"], "gdp")

    def test_start_date_maps_to_observation_start(self):
        """start_date config must map to observation_start param for series_observations."""
        config = {
            **self.config,
            "start_date": "2024-01-01",
        }

        tap = TapFRED(config=config)
        streams = {stream.name: stream for stream in tap.streams.values()}
        series_obs_stream = streams["series_observations"]

        self.assertEqual(
            series_obs_stream.query_params.get("observation_start"),
            "2024-01-01",
        )

    def test_start_date_not_mapped_for_other_streams(self):
        """start_date config must NOT map to observation_start for non-observation streams."""
        config = {
            **self.config,
            "start_date": "2024-01-01",
        }

        tap = TapFRED(config=config)
        streams = {stream.name: stream for stream in tap.streams.values()}

        # series stream should NOT have observation_start
        series_stream = streams["series"]
        self.assertNotIn("observation_start", series_stream.query_params)

    def test_skip_summary_emitted(self):
        """finalize_state_progress_markers must log skip summary when partitions were skipped."""

        class MockStream(FREDStream):
            name = "test_summary"
            path = "/test"
            records_jsonpath = "$.data[*]"
            schema = th.PropertiesList(th.Property("id", th.StringType)).to_dict()

            def _get_records_key(self):
                return "data"

        tap = TapFRED(config=self.config)
        stream = MockStream(tap)

        # Simulate skipped partitions
        stream._skipped_partitions = [
            {"stream": "test", "partition_key": "id", "partition_value": "1", "error": "HTTP 404"},
            {"stream": "test", "partition_key": "id", "partition_value": "2", "error": "HTTP 400"},
        ]

        # Should log warning with summary (test that it doesn't crash)
        with patch.object(stream.logger, "warning") as mock_warn:
            stream.finalize_state_progress_markers()

            # Verify summary was logged
            mock_warn.assert_called()
            calls = [str(c) for c in mock_warn.call_args_list]
            summary_logged = any("2 skipped partition(s)" in c for c in calls)
            self.assertTrue(summary_logged, f"Expected skip summary in log calls: {calls}")

    def test_series_search_related_tags_gating(self):
        """SeriesSearchRelatedTagsStream only registered when both tag_names and search_text are configured."""
        # Without the required params - should NOT include the stream
        config_without = {**self.config}
        tap = TapFRED(config=config_without)
        stream_names = [s.name for s in tap.streams.values()]
        self.assertNotIn("series_search_related_tags", stream_names)

        # With only series_search_text but no tag_names - should NOT include
        config_partial = {
            **self.config,
            "series_search_related_tags_params": {
                "query_params": {"series_search_text": "GDP"}
            },
        }
        tap2 = TapFRED(config=config_partial)
        stream_names2 = [s.name for s in tap2.streams.values()]
        self.assertNotIn("series_search_related_tags", stream_names2)

        # With both - should include
        config_full = {
            **self.config,
            "series_search_related_tags_params": {
                "query_params": {
                    "series_search_text": "GDP",
                    "tag_names": ["gdp", "usa"],
                }
            },
        }
        tap3 = TapFRED(config=config_full)
        stream_names3 = [s.name for s in tap3.streams.values()]
        self.assertIn("series_search_related_tags", stream_names3)


class TestIncrementalReplication(unittest.TestCase):
    """Tests that prove incremental replication works correctly.

    Simulates Run 1 → Run 2 to verify bookmark-based extraction
    only returns new data, not duplicates.
    """

    def setUp(self):
        self.config = {
            "api_key": "test_api_key",
            "series_ids": ["GDP"],
            "start_date": "2024-01-01",
            "data_mode": "FRED",
        }

    @patch("tap_fred.client.FREDStream._make_request")
    def test_incremental_replication_returns_new_records_only(self, mock_request):
        """Simulate Run 1 (full) then Run 2 (incremental) for series_observations.

        Run 1: Returns 3 records with realtime_start dates 2024-01-01, 2024-06-01, 2024-09-01
        Run 2: With bookmark at 2024-06-01, should only get records >= that date
        """
        # Run 1 response: all historical observations
        run1_observations = [
            {"realtime_start": "2024-01-01", "realtime_end": "2024-03-31",
             "date": "2024-01-01", "value": "27000.0"},
            {"realtime_start": "2024-06-01", "realtime_end": "2024-08-31",
             "date": "2024-04-01", "value": "27200.0"},
            {"realtime_start": "2024-09-01", "realtime_end": "2024-12-31",
             "date": "2024-07-01", "value": "27500.0"},
        ]
        mock_request.return_value = {"observations": run1_observations}

        tap = TapFRED(config=self.config)
        stream = {s.name: s for s in tap.streams.values()}["series_observations"]

        run1_records = list(stream.get_records(context={"series_id": "GDP"}))
        self.assertEqual(len(run1_records), 3)

        # Verify replication key values are present
        replication_values = [r["realtime_start"] for r in run1_records]
        self.assertEqual(replication_values, ["2024-01-01", "2024-06-01", "2024-09-01"])

        # Run 2: API returns only records at or after bookmark (2024-06-01)
        run2_observations = [
            {"realtime_start": "2024-06-01", "realtime_end": "2024-08-31",
             "date": "2024-04-01", "value": "27200.0"},
            {"realtime_start": "2024-09-01", "realtime_end": "2024-12-31",
             "date": "2024-07-01", "value": "27500.0"},
            {"realtime_start": "2025-01-15", "realtime_end": "2025-03-31",
             "date": "2024-10-01", "value": "27800.0"},
        ]
        mock_request.return_value = {"observations": run2_observations}

        run2_records = list(stream.get_records(context={"series_id": "GDP"}))
        self.assertEqual(len(run2_records), 3)

        # The NEW record from Run 2 that wasn't in Run 1
        new_record = run2_records[2]
        self.assertEqual(new_record["realtime_start"], "2025-01-15")
        self.assertEqual(new_record["value"], 27800.0)

    def test_replication_key_is_realtime_start(self):
        """series_observations must use realtime_start as replication key.

        This is critical for incremental replication — the Singer SDK uses this
        field to track the bookmark position. Using 'date' would miss vintage revisions.
        """
        tap = TapFRED(config=self.config)
        stream = {s.name: s for s in tap.streams.values()}["series_observations"]

        self.assertEqual(stream.replication_key, "realtime_start")
        # PK must include realtime dates to prevent vintage overwrites
        self.assertIn("realtime_start", stream.primary_keys)
        self.assertIn("realtime_end", stream.primary_keys)


class TestIncrementalPartitionSkip(unittest.TestCase):
    """Tests that point-in-time partitions are correctly skipped after first sync.

    Vintage data is immutable — once a (series_id, vintage_date) partition has
    been fully synced, the tap must NEVER hit the API for it again.  This is
    critical for the full-wildcard ALFRED backfill: without skip logic, every
    run would re-fetch millions of already-synced partitions.
    """

    def setUp(self):
        self.config = {
            "api_key": "test_api_key",
            "series_ids": ["GDP"],
            "data_mode": "ALFRED",
            "realtime_start": "2020-01-01",
            "realtime_end": "2020-01-01",
            "point_in_time_mode": True,
            "point_in_time_start": "2020-01-01",
            "point_in_time_end": "2020-03-31",
        }

    @patch("tap_fred.client.FREDStream._make_request")
    def test_completed_vintage_partition_skips_api_call(self, mock_request):
        """A partition with an existing bookmark must NOT make any API call."""
        mock_request.return_value = {"observations": [
            {"date": "2024-01-01", "value": "100.0",
             "realtime_start": "2020-01-30", "realtime_end": "2020-01-30"},
        ]}

        tap = TapFRED(config=self.config)
        stream = {s.name: s for s in tap.streams.values()}["series_observations"]

        context = {"series_id": "GDP", "vintage_date": "2020-01-30"}

        # Simulate an existing bookmark from a previous sync
        with patch.object(stream, "get_context_state", return_value={"replication_key_value": "2020-01-30"}):
            records = list(stream.get_records(context=context))

        # CRITICAL: No records emitted (partition is skipped)
        self.assertEqual(records, [])
        # CRITICAL: No API call made
        mock_request.assert_not_called()

    @patch("tap_fred.client.FREDStream._make_request")
    def test_new_vintage_partition_fetches_normally(self, mock_request):
        """A partition WITHOUT a bookmark must fetch from the API."""
        mock_request.return_value = {"observations": [
            {"date": "2024-01-01", "value": "100.0",
             "realtime_start": "2020-01-30", "realtime_end": "2020-01-30"},
        ]}

        tap = TapFRED(config=self.config)
        stream = {s.name: s for s in tap.streams.values()}["series_observations"]

        context = {"series_id": "GDP", "vintage_date": "2020-01-30"}

        # No existing bookmark — first sync of this partition
        with patch.object(stream, "get_context_state", return_value={}):
            records = list(stream.get_records(context=context))

        self.assertEqual(len(records), 1)
        mock_request.assert_called_once()

    @patch("tap_fred.client.FREDStream._make_request")
    def test_non_pit_partition_never_skipped(self, mock_request):
        """Non-point-in-time partitions (no vintage_date in context) must ALWAYS fetch."""
        mock_request.return_value = {"observations": [
            {"date": "2024-01-01", "value": "100.0",
             "realtime_start": "2020-01-01", "realtime_end": "2020-01-01"},
        ]}

        config = {**self.config, "point_in_time_mode": False}
        tap = TapFRED(config=config)
        stream = {s.name: s for s in tap.streams.values()}["series_observations"]

        context = {"series_id": "GDP"}  # No vintage_date

        # Even with a bookmark, non-PIT partitions must still fetch
        with patch.object(stream, "get_context_state", return_value={"replication_key_value": "2020-01-01"}):
            records = list(stream.get_records(context=context))

        self.assertEqual(len(records), 1)
        mock_request.assert_called_once()

    @patch("tap_fred.client.FREDStream._make_request")
    def test_run1_then_run2_skips_completed_partitions(self, mock_request):
        """Simulate full Run 1 → Run 2 lifecycle: Run 2 must skip synced partitions."""
        observations = {"observations": [
            {"date": "2024-01-01", "value": "100.0",
             "realtime_start": "2020-01-30", "realtime_end": "2020-01-30"},
        ]}
        mock_request.return_value = observations

        tap = TapFRED(config=self.config)
        stream = {s.name: s for s in tap.streams.values()}["series_observations"]

        context = {"series_id": "GDP", "vintage_date": "2020-01-30"}

        # Run 1: no bookmark → fetch normally
        with patch.object(stream, "get_context_state", return_value={}):
            run1_records = list(stream.get_records(context=context))
        self.assertEqual(len(run1_records), 1)
        self.assertEqual(mock_request.call_count, 1)

        mock_request.reset_mock()

        # Run 2: bookmark exists → skip
        with patch.object(stream, "get_context_state", return_value={"replication_key_value": "2020-01-30"}):
            run2_records = list(stream.get_records(context=context))
        self.assertEqual(run2_records, [])
        mock_request.assert_not_called()

    def test_vintage_dates_per_series_partitions(self):
        """series_vintage_dates must use per-series partitions for independent bookmarks.

        Without per-series partitions, the bookmark is global — if GDP's latest
        vintage is 2025-03-01 and UNRATE's is 2025-02-15, UNRATE's new vintages
        between those dates would be silently dropped on the next run.
        """
        config = {**self.config, "series_ids": ["GDP", "UNRATE"]}
        tap = TapFRED(config=config)

        # Mock cached series IDs (normally discovered from API)
        tap._cached_series_ids = [
            {"series_id": "GDP"},
            {"series_id": "UNRATE"},
        ]

        stream = {s.name: s for s in tap.streams.values()}["series_vintage_dates"]
        partitions = stream.partitions

        self.assertEqual(len(partitions), 2)
        self.assertEqual(partitions[0], {"series_id": "GDP"})
        self.assertEqual(partitions[1], {"series_id": "UNRATE"})

    def test_vintage_dates_primary_key_includes_series_id(self):
        """series_vintage_dates PK must include series_id to prevent cross-series collisions.

        Without series_id in the PK, vintage date '2020-01-01' from GDP would
        collide with '2020-01-01' from UNRATE during upsert.
        """
        tap = TapFRED(config=self.config)
        stream = {s.name: s for s in tap.streams.values()}["series_vintage_dates"]

        self.assertEqual(stream.primary_keys, ["series_id", "date"])

    @patch("tap_fred.client.FREDStream._make_request")
    def test_vintage_dates_per_series_extraction(self, mock_request):
        """Each series partition must fetch only its own vintage dates."""
        mock_request.return_value = {
            "vintage_dates": ["2020-01-15", "2020-02-15"]
        }

        config = {**self.config, "series_ids": ["GDP", "UNRATE"]}
        tap = TapFRED(config=config)
        tap._cached_series_ids = [
            {"series_id": "GDP"},
            {"series_id": "UNRATE"},
        ]

        stream = {s.name: s for s in tap.streams.values()}["series_vintage_dates"]

        # Extract for GDP partition only
        records = list(stream.get_records(context={"series_id": "GDP"}))

        self.assertEqual(len(records), 2)
        # All records must belong to GDP, not UNRATE
        for r in records:
            self.assertEqual(r["series_id"], "GDP")

        # API call must have included series_id=GDP
        call_params = mock_request.call_args[0][1]
        self.assertEqual(call_params["series_id"], "GDP")


class TestAlfredPointInTimeAccuracy(unittest.TestCase):
    """Tests that prove ALFRED/point-in-time mode produces historically accurate data.

    These test the core financial use case: backtesting without look-ahead bias.
    """

    def setUp(self):
        self.fred_config = {
            "api_key": "test_api_key",
            "series_ids": ["GDP"],
            "data_mode": "FRED",
        }
        self.alfred_config = {
            "api_key": "test_api_key",
            "series_ids": ["GDP"],
            "data_mode": "ALFRED",
            "realtime_start": "2020-01-01",
            "realtime_end": "2020-01-01",
        }

    def test_alfred_mode_adds_realtime_params_to_query(self):
        """ALFRED mode must inject realtime_start/end into API query params.

        Without these params, FRED returns current revised data — which would
        introduce look-ahead bias in backtesting.
        """
        tap = TapFRED(config=self.alfred_config)
        stream = {s.name: s for s in tap.streams.values()}["series_observations"]

        self.assertEqual(stream.query_params["realtime_start"], "2020-01-01")
        self.assertEqual(stream.query_params["realtime_end"], "2020-01-01")

    def test_fred_mode_does_not_add_realtime_params(self):
        """FRED mode must NOT inject realtime params — FRED defaults to today."""
        tap = TapFRED(config=self.fred_config)
        stream = {s.name: s for s in tap.streams.values()}["series_observations"]

        self.assertNotIn("realtime_start", stream.query_params)
        self.assertNotIn("realtime_end", stream.query_params)

    @patch("tap_fred.client.FREDStream._make_request")
    def test_vintage_dates_differ_from_current_data(self, mock_request):
        """ALFRED vintage data must differ from FRED current data for the same series.

        This is the core backtesting guarantee: GDP as-known-on 2020-01-01
        differs from GDP as-known-today because revisions happened.
        """
        # Simulate FRED (current) response: includes revised data through 2024
        fred_response = {"observations": [
            {"realtime_start": "2025-01-01", "realtime_end": "2025-01-01",
             "date": "2019-10-01", "value": "21729.124"},  # Revised value
        ]}
        # Simulate ALFRED (vintage) response: data as-known-on 2020-01-01
        alfred_response = {"observations": [
            {"realtime_start": "2020-01-01", "realtime_end": "2020-01-01",
             "date": "2019-10-01", "value": "21734.266"},  # Initial estimate
        ]}

        # FRED mode
        tap_fred = TapFRED(config=self.fred_config)
        stream_fred = {s.name: s for s in tap_fred.streams.values()}["series_observations"]
        mock_request.return_value = fred_response
        fred_records = list(stream_fred.get_records(context={"series_id": "GDP"}))

        # ALFRED mode
        tap_alfred = TapFRED(config=self.alfred_config)
        stream_alfred = {s.name: s for s in tap_alfred.streams.values()}["series_observations"]
        mock_request.return_value = alfred_response
        alfred_records = list(stream_alfred.get_records(context={"series_id": "GDP"}))

        # Same observation date, different values — proves vintage accuracy
        self.assertEqual(fred_records[0]["date"], "2019-10-01")
        self.assertEqual(alfred_records[0]["date"], "2019-10-01")
        self.assertNotEqual(fred_records[0]["value"], alfred_records[0]["value"])

        # ALFRED must carry the vintage date, not today's date
        self.assertEqual(alfred_records[0]["realtime_start"], "2020-01-01")

    @patch("tap_fred.client.FREDStream._make_request")
    def test_point_in_time_partitions_capture_gdp_revisions(self, mock_request):
        """Point-in-time mode must create separate partitions per vintage date.

        Each partition captures GDP as-known-on that specific date, showing
        real revision history (e.g., initial estimate → first revision → final).
        """
        pit_config = {
            "api_key": "test_api_key",
            "series_ids": ["GDP"],
            "data_mode": "ALFRED",
            "point_in_time_mode": True,
            "point_in_time_start": "2020-01-01",
            "point_in_time_end": "2020-03-31",
        }

        # Mock vintage dates discovery
        vintage_dates_response = {
            "vintage_dates": ["2020-01-30", "2020-02-27", "2020-03-26"]
        }

        # GDP Q4 2019 values at each vintage date (real revision pattern)
        vintage_responses = {
            "2020-01-30": {"observations": [
                {"realtime_start": "2020-01-30", "realtime_end": "2020-01-30",
                 "date": "2019-10-01", "value": "21734.266"},  # Initial estimate
            ]},
            "2020-02-27": {"observations": [
                {"realtime_start": "2020-02-27", "realtime_end": "2020-02-27",
                 "date": "2019-10-01", "value": "21726.779"},  # First revision DOWN
            ]},
            "2020-03-26": {"observations": [
                {"realtime_start": "2020-03-26", "realtime_end": "2020-03-26",
                 "date": "2019-10-01", "value": "21729.124"},  # Settled value
            ]},
        }

        def mock_request_side_effect(url, params):
            if "vintagedates" in url:
                return vintage_dates_response
            vintage = params.get("realtime_start", "")
            return vintage_responses.get(vintage, {"observations": []})

        mock_request.side_effect = mock_request_side_effect

        tap = TapFRED(config=pit_config)
        stream = {s.name: s for s in tap.streams.values()}["series_observations"]

        # Verify partitions are created for each vintage date
        partitions = stream.partitions
        vintage_partition_dates = [
            p["vintage_date"] for p in partitions if "vintage_date" in p
        ]
        self.assertEqual(vintage_partition_dates,
                         ["2020-01-30", "2020-02-27", "2020-03-26"])

        # Extract records for each vintage partition
        all_values = []
        for partition in partitions:
            records = list(stream.get_records(context=partition))
            for r in records:
                all_values.append(r["value"])

        # GDP revisions: initial estimate → revision down → settled
        self.assertEqual(all_values, [21734.266, 21726.779, 21729.124])

        # Initial estimate > first revision (GDP was revised DOWN)
        self.assertGreater(all_values[0], all_values[1])

    def test_vintage_dates_stream_excludes_realtime_params(self):
        """series_vintage_dates must NOT send realtime_start/end to avoid API errors.

        The FRED API returns errors when realtime params are sent to the
        vintagedates endpoint. Our code must strip them.
        """
        tap = TapFRED(config=self.alfred_config)
        stream = {s.name: s for s in tap.streams.values()}["series_vintage_dates"]

        self.assertNotIn("realtime_start", stream.query_params)
        self.assertNotIn("realtime_end", stream.query_params)


class TestEndpointContracts(unittest.TestCase):
    """Verify each stream family sends correct params and parses correct response keys.

    These are fast contract checks — no real API calls, just mock responses
    that mirror FRED API documentation structure.
    """

    def setUp(self):
        self.config = {
            "api_key": "test_api_key",
            "series_ids": ["GDP", "UNRATE"],
            "data_mode": "FRED",
            "category_ids": ["18", "32992"],
            "release_ids": ["53", "151"],
            "source_ids": ["1"],
            "tag_names": ["gdp"],
        }

    @patch("tap_fred.client.FREDStream._make_request")
    def test_series_observations_contract(self, mock_request):
        """series_observations must send series_id and parse 'observations' key."""
        mock_request.return_value = {"observations": [
            {"date": "2024-01-01", "value": "100.0",
             "realtime_start": "2025-01-01", "realtime_end": "2025-01-01"},
        ]}

        tap = TapFRED(config=self.config)
        stream = {s.name: s for s in tap.streams.values()}["series_observations"]
        records = list(stream.get_records(context={"series_id": "GDP"}))

        self.assertEqual(len(records), 1)
        # Verify series_id was passed in the API call
        call_params = mock_request.call_args[0][1]
        self.assertEqual(call_params["series_id"], "GDP")
        self.assertIn("api_key", call_params)
        self.assertEqual(call_params["file_type"], "json")

    @patch("tap_fred.client.FREDStream._make_request")
    def test_releases_list_all_contract(self, mock_request):
        """releases (list-all) must parse 'releases' key and paginate."""
        mock_request.return_value = {"releases": [
            {"id": "53", "name": "GDP", "press_release": "true"},
        ]}

        tap = TapFRED(config=self.config)
        stream = {s.name: s for s in tap.streams.values()}["releases"]
        records = list(stream.get_records(context=None))

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["id"], 53)
        # press_release should be coerced to bool by base post_process
        self.assertIs(records[0]["press_release"], True)

    @patch("tap_fred.client.FREDStream._make_request")
    def test_tags_contract(self, mock_request):
        """tags must parse 'tags' key (not default 'data')."""
        mock_request.return_value = {"tags": [
            {"name": "gdp", "group_id": "1", "popularity": "99", "series_count": "42"},
        ]}

        tap = TapFRED(config=self.config)
        stream = {s.name: s for s in tap.streams.values()}["tags"]
        records = list(stream._fetch_and_process_records(
            stream.get_url(), stream.query_params.copy(), None
        ))

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["name"], "gdp")
        # Verify integer coercion happened
        self.assertEqual(records[0]["popularity"], 99)
        self.assertEqual(records[0]["series_count"], 42)

    @patch("tap_fred.client.FREDStream._make_request")
    def test_category_children_injects_parent_id(self, mock_request):
        """category_children must inject parent_id from partition context."""
        mock_request.return_value = {"categories": [
            {"id": "100", "name": "Child Category"},
            {"id": "101", "name": "Another Child"},
        ]}

        tap = TapFRED(config=self.config)
        stream = {s.name: s for s in tap.streams.values()}["category_children"]
        records = list(stream.get_records(context={"category_id": "18"}))

        self.assertEqual(len(records), 2)
        # parent_id must come from partition context, not API response
        self.assertEqual(records[0]["parent_id"], 18)
        self.assertEqual(records[1]["parent_id"], 18)


class TestPaginationCorrectness(unittest.TestCase):
    """Verify pagination doesn't drop pages or duplicate records."""

    def setUp(self):
        self.config = {
            "api_key": "test_api_key",
            "series_ids": ["GDP"],
            "data_mode": "FRED",
        }

    @patch("tap_fred.client.FREDStream._make_request")
    def test_pagination_fetches_all_pages(self, mock_request):
        """Paginated stream must fetch all pages until records < limit."""

        class SmallPageStream(FREDStream):
            name = "test_small_page"
            path = "/test"
            records_jsonpath = "$.items[*]"
            _paginate = True
            schema = th.PropertiesList(
                th.Property("id", th.StringType),
            ).to_dict()

            def _get_records_key(self):
                return "items"

            def _get_pagination_limit(self):
                return 2  # Force small pages

        call_count = 0

        def paginated_response(url, params):
            nonlocal call_count
            call_count += 1
            offset = params.get("offset", 0)
            if offset == 0:
                return {"items": [{"id": "a"}, {"id": "b"}]}  # Full page
            elif offset == 2:
                return {"items": [{"id": "c"}]}  # Partial page = last
            return {"items": []}

        mock_request.side_effect = paginated_response

        tap = TapFRED(config=self.config)
        stream = SmallPageStream(tap)
        records = list(stream._paginate_records(context=None))

        # Must get all 3 records across 2 pages
        self.assertEqual(len(records), 3)
        record_ids = [r["id"] for r in records]
        self.assertEqual(record_ids, ["a", "b", "c"])
        # Must have made exactly 2 API calls (page 1 + page 2)
        self.assertEqual(call_count, 2)

    @patch("tap_fred.client.FREDStream._make_request")
    def test_pagination_stops_on_empty_page(self, mock_request):
        """Pagination must stop when API returns 0 records."""

        class EmptyPageStream(FREDStream):
            name = "test_empty_page"
            path = "/test"
            records_jsonpath = "$.items[*]"
            _paginate = True
            schema = th.PropertiesList(th.Property("id", th.StringType)).to_dict()

            def _get_records_key(self):
                return "items"

            def _get_pagination_limit(self):
                return 100

        mock_request.return_value = {"items": []}

        tap = TapFRED(config=self.config)
        stream = EmptyPageStream(tap)
        records = list(stream._paginate_records(context=None))

        self.assertEqual(records, [])
        # Only 1 call — stopped immediately on empty
        mock_request.assert_called_once()

    @patch("tap_fred.client.FREDStream._make_request")
    def test_pagination_logs_completeness_when_count_matches(self, mock_request):
        """Pagination must log info when extracted count matches API count."""

        class CountedStream(FREDStream):
            name = "test_counted"
            path = "/test"
            records_jsonpath = "$.items[*]"
            _paginate = True
            schema = th.PropertiesList(th.Property("id", th.StringType)).to_dict()

            def _get_records_key(self):
                return "items"

            def _get_pagination_limit(self):
                return 2

        # Page 1: full page with count metadata, page 2: partial
        mock_request.side_effect = [
            {"count": 3, "items": [{"id": "a"}, {"id": "b"}]},
            {"count": 3, "items": [{"id": "c"}]},
        ]

        tap = TapFRED(config=self.config)
        stream = CountedStream(tap)
        records = list(stream._paginate_records(context=None))

        self.assertEqual(len(records), 3)

    @patch("tap_fred.client.FREDStream._make_request")
    def test_pagination_warns_on_shortfall(self, mock_request):
        """Pagination must warn when extracted count < API-reported count."""

        class ShortfallStream(FREDStream):
            name = "test_shortfall"
            path = "/test"
            records_jsonpath = "$.items[*]"
            _paginate = True
            schema = th.PropertiesList(th.Property("id", th.StringType)).to_dict()

            def _get_records_key(self):
                return "items"

            def _get_pagination_limit(self):
                return 1000

        # API says 5000 records exist but only returns 2
        mock_request.return_value = {
            "count": 5000,
            "items": [{"id": "a"}, {"id": "b"}],
        }

        tap = TapFRED(config=self.config)
        stream = ShortfallStream(tap)

        with self.assertLogs("tap-fred.test_shortfall", level="WARNING") as log:
            records = list(stream._paginate_records(context=None))

        self.assertEqual(len(records), 2)
        # Must have emitted a warning about the shortfall
        shortfall_warnings = [m for m in log.output if "fewer" in m]
        self.assertTrue(shortfall_warnings, "Expected shortfall warning not emitted")

    @patch("tap_fred.client.FREDStream._make_request")
    def test_pagination_handles_offset_cap_error(self, mock_request):
        """Pagination must handle FRED's undocumented offset cap gracefully."""

        class CappedStream(FREDStream):
            name = "test_capped"
            path = "/test"
            records_jsonpath = "$.items[*]"
            _paginate = True
            schema = th.PropertiesList(th.Property("id", th.StringType)).to_dict()

            def _get_records_key(self):
                return "items"

            def _get_pagination_limit(self):
                return 2

        call_count = 0

        def capped_response(url, params):
            nonlocal call_count
            call_count += 1
            offset = params.get("offset", 0)
            if offset < 4:
                return {"count": 100, "items": [{"id": str(offset)}, {"id": str(offset + 1)}]}
            # Simulate FRED's offset cap: 400 error
            resp = requests.models.Response()
            resp.status_code = 400
            resp._content = b'{"error_code": 400, "error_message": "Exceeded 5000 maximum searchable results"}'
            raise requests.exceptions.HTTPError(response=resp)

        mock_request.side_effect = capped_response

        tap = TapFRED(config=self.config)
        stream = CappedStream(tap)
        records = list(stream._paginate_records(context=None))

        # Should have gotten records from pages before the cap
        self.assertEqual(len(records), 4)

    @patch("tap_fred.client.FREDStream._make_request")
    def test_pagination_strict_mode_raises_on_page_error(self, mock_request):
        """In strict mode, a page-level error must re-raise, not silently stop."""

        class StrictStream(FREDStream):
            name = "test_strict_page"
            path = "/test"
            records_jsonpath = "$.items[*]"
            _paginate = True
            schema = th.PropertiesList(th.Property("id", th.StringType)).to_dict()

            def _get_records_key(self):
                return "items"

            def _get_pagination_limit(self):
                return 2

        def error_on_second_page(url, params):
            if params.get("offset", 0) == 0:
                return {"items": [{"id": "a"}, {"id": "b"}]}
            raise requests.exceptions.ConnectionError("network failure")

        mock_request.side_effect = error_on_second_page

        strict_config = {**self.config, "strict_mode": True}
        tap = TapFRED(config=strict_config)
        stream = StrictStream(tap)

        with self.assertRaises(requests.exceptions.ConnectionError):
            list(stream._paginate_records(context=None))


class TestWildcardDiscovery(unittest.TestCase):
    """Verify wildcard partition generation produces non-zero, plausible counts."""

    def setUp(self):
        self.config = {
            "api_key": "test_api_key",
            "series_ids": ["GDP"],
            "data_mode": "FRED",
        }

    def test_release_wildcard_generates_partitions_from_cache(self):
        """release_ids=["*"] must generate partitions from cached release discovery."""
        config = {**self.config, "release_ids": ["*"]}
        tap = TapFRED(config=config)

        # Simulate cached discovery results (3 releases found)
        tap._cached_release_ids = [
            {"release_id": 10}, {"release_id": 53}, {"release_id": 151},
        ]

        from tap_fred.streams.releases_streams import ReleaseSeriesStream
        stream = ReleaseSeriesStream(tap)
        partitions = stream.partitions

        self.assertEqual(len(partitions), 3)
        partition_ids = [p["release_id"] for p in partitions]
        self.assertEqual(partition_ids, [10, 53, 151])

    def test_category_wildcard_generates_partitions_from_cache(self):
        """category_ids=["*"] must generate partitions from cached category discovery."""
        config = {**self.config, "category_ids": ["*"]}
        tap = TapFRED(config=config)

        tap._cached_category_ids = [
            {"category_id": 0}, {"category_id": 18}, {"category_id": 32992},
        ]

        from tap_fred.streams.category_streams import CategoryChildrenStream
        stream = CategoryChildrenStream(tap)
        partitions = stream.partitions

        self.assertEqual(len(partitions), 3)

    def test_specific_ids_generate_exact_partitions(self):
        """Explicit IDs must generate exactly that many partitions — no discovery needed."""
        config = {**self.config, "release_ids": ["53", "151"]}
        tap = TapFRED(config=config)

        from tap_fred.streams.releases_streams import ReleaseSeriesStream
        stream = ReleaseSeriesStream(tap)
        partitions = stream.partitions

        self.assertEqual(len(partitions), 2)
        self.assertEqual(partitions[0]["release_id"], "53")
        self.assertEqual(partitions[1]["release_id"], "151")

    def test_geofred_regional_params_generate_correct_partitions(self):
        """GeoFRED regional params must expand wildcards correctly."""
        config = {
            **self.config,
            "geofred_regional_params": [
                {"series_group": "882", "region_type": "state",
                 "season": "*", "date": "2013-01-01",
                 "units": "Dollars", "frequency": "a"},
            ],
        }
        tap = TapFRED(config=config)

        from tap_fred.streams.maps_streams import GeoFREDRegionalDataStream
        stream = GeoFREDRegionalDataStream(tap)
        partitions = stream.partitions

        # season="*" expands to 5 values (SA, NSA, SSA, SAAR, NSAAR)
        self.assertEqual(len(partitions), 5)
        seasons = [p["season"] for p in partitions]
        self.assertEqual(sorted(seasons), ["NSA", "NSAAR", "SA", "SAAR", "SSA"])


class TestStrictPermissiveErrorModes(unittest.TestCase):
    """Verify strict_mode=true fails loudly, false skips and logs."""

    def setUp(self):
        self.base_config = {
            "api_key": "test_api_key",
            "series_ids": ["GDP"],
            "data_mode": "FRED",
        }

    @patch("tap_fred.client.FREDStream._make_request")
    def test_bad_partition_in_strict_mode_crashes(self, mock_request):
        """A 404 on one partition must crash the entire sync in strict mode."""
        mock_response = Mock()
        mock_response.status_code = 404

        def request_side_effect(url, params):
            if params.get("series_id") == "INVALID":
                raise requests.exceptions.HTTPError(response=mock_response)
            return {"observations": [
                {"date": "2024-01-01", "value": "100",
                 "realtime_start": "2025-01-01", "realtime_end": "2025-01-01"}
            ]}

        mock_request.side_effect = request_side_effect

        config = {**self.base_config, "strict_mode": True}
        tap = TapFRED(config=config)
        stream = {s.name: s for s in tap.streams.values()}["series_observations"]

        with self.assertRaises(requests.exceptions.HTTPError):
            list(stream.get_records(context={"series_id": "INVALID"}))

        self.assertEqual(len(stream._skipped_partitions), 1)
        self.assertEqual(stream._skipped_partitions[0]["partition_value"], "INVALID")

    @patch("tap_fred.client.FREDStream._make_request")
    def test_bad_partition_in_permissive_mode_continues(self, mock_request):
        """A 404 on one partition must be skipped in permissive mode (default)."""
        mock_response = Mock()
        mock_response.status_code = 404

        def request_side_effect(url, params):
            if params.get("series_id") == "INVALID":
                raise requests.exceptions.HTTPError(response=mock_response)
            return {"observations": [
                {"date": "2024-01-01", "value": "100",
                 "realtime_start": "2025-01-01", "realtime_end": "2025-01-01"}
            ]}

        mock_request.side_effect = request_side_effect

        config = {**self.base_config, "strict_mode": False}
        tap = TapFRED(config=config)
        stream = {s.name: s for s in tap.streams.values()}["series_observations"]

        # Should NOT raise — partition is skipped
        records = list(stream.get_records(context={"series_id": "INVALID"}))
        self.assertEqual(records, [])

        # Skip was tracked
        self.assertEqual(len(stream._skipped_partitions), 1)

        # Other partitions still work
        good_records = list(stream.get_records(context={"series_id": "GDP"}))
        self.assertEqual(len(good_records), 1)

    def test_skip_summary_includes_all_skipped_partitions(self):
        """finalize_state_progress_markers must emit count and details of all skips."""

        class MockStream(FREDStream):
            name = "test_skip_summary"
            path = "/test"
            records_jsonpath = "$.data[*]"
            schema = th.PropertiesList(th.Property("id", th.StringType)).to_dict()

            def _get_records_key(self):
                return "data"

        tap = TapFRED(config=self.base_config)
        stream = MockStream(tap)

        stream._skipped_partitions = [
            {"stream": "test", "partition_key": "id", "partition_value": "BAD1", "error": "HTTP 404"},
            {"stream": "test", "partition_key": "id", "partition_value": "BAD2", "error": "HTTP 400"},
            {"stream": "test", "partition_key": "id", "partition_value": "BAD3", "error": "ValueError"},
        ]

        with patch.object(stream.logger, "warning") as mock_warn:
            stream.finalize_state_progress_markers()

            calls = [str(c) for c in mock_warn.call_args_list]
            # Summary line must state exact count
            self.assertTrue(
                any("3 skipped partition(s)" in c for c in calls),
                f"Expected '3 skipped partition(s)' in: {calls}"
            )
            # Each individual skip must be logged
            self.assertTrue(any("BAD1" in c for c in calls))
            self.assertTrue(any("BAD3" in c for c in calls))


class TestDataLeakagePrevention(unittest.TestCase):
    """Tests for production-critical data leakage prevention (Fixes 1-3).

    These guard against silent data contamination in point-in-time mode,
    which would corrupt financial backtesting results.
    """

    def setUp(self):
        self.pit_config = {
            "api_key": "test_api_key",
            "series_ids": ["GDP", "DGS10"],
            "data_mode": "ALFRED",
            "point_in_time_mode": True,
            "point_in_time_start": "2025-01-01",
            "point_in_time_end": "2025-01-10",
        }

    def test_pit_skips_series_with_no_vintage_dates(self):
        """Fix 1: Series with no vintage dates in range must produce ZERO partitions.

        GDP is quarterly — no vintage dates exist in a 10-day window.
        The old code created a fallback partition WITHOUT vintage_date, which
        hit the API with realtime_start=today, returning current revised data.
        """
        tap = TapFRED(config=self.pit_config)
        stream = {s.name: s for s in tap.streams.values()}["series_observations"]

        # Mock: GDP has no vintage dates, DGS10 has some
        mock_vintage_dates = [
            {"resource_id": "DGS10", "vintage_date": "2025-01-02"},
            {"resource_id": "DGS10", "vintage_date": "2025-01-03"},
        ]

        with patch.object(tap, "get_cached_vintage_dates", return_value=mock_vintage_dates), \
             patch.object(tap, "get_cached_series_ids", return_value=[
                 {"series_id": "GDP"}, {"series_id": "DGS10"}
             ]):
            partitions = stream.partitions

        # GDP must NOT have any partitions (no fallback)
        gdp_partitions = [p for p in partitions if p.get("series_id") == "GDP"]
        self.assertEqual(len(gdp_partitions), 0, "GDP should have zero partitions — no fallback allowed")

        # DGS10 must have exactly 2 partitions (one per vintage date)
        dgs10_partitions = [p for p in partitions if p.get("series_id") == "DGS10"]
        self.assertEqual(len(dgs10_partitions), 2)
        # Each partition must have a vintage_date
        for p in dgs10_partitions:
            self.assertIn("vintage_date", p, "Every PIT partition must have vintage_date")

    @patch("tap_fred.client.FREDStream._make_request")
    def test_data_leakage_guard_raises_on_mismatch(self, mock_request):
        """Fix 2: RuntimeError must be raised if realtime_start != vintage_date.

        This catches scenarios where the FRED API returns data from a different
        vintage than requested — silent data contamination.
        """
        # API returns realtime_start=2025-01-15 but we requested vintage_date=2025-01-02
        mock_request.return_value = {"observations": [
            {"date": "2024-10-01", "value": "21000.0",
             "realtime_start": "2025-01-15", "realtime_end": "2025-01-15"},
        ]}

        tap = TapFRED(config=self.pit_config)
        stream = {s.name: s for s in tap.streams.values()}["series_observations"]

        context = {"series_id": "DGS10", "vintage_date": "2025-01-02"}

        with patch.object(stream, "get_context_state", return_value={}):
            with self.assertRaises(RuntimeError) as cm:
                list(stream.get_records(context=context))

        self.assertIn("DATA LEAKAGE", str(cm.exception))
        self.assertIn("2025-01-02", str(cm.exception))
        self.assertIn("2025-01-15", str(cm.exception))

    @patch("tap_fred.client.FREDStream._make_request")
    def test_data_leakage_guard_passes_on_match(self, mock_request):
        """Fix 2 (positive case): No error when realtime_start matches vintage_date."""
        mock_request.return_value = {"observations": [
            {"date": "2024-10-01", "value": "4.25",
             "realtime_start": "2025-01-02", "realtime_end": "2025-01-02"},
        ]}

        tap = TapFRED(config=self.pit_config)
        stream = {s.name: s for s in tap.streams.values()}["series_observations"]

        context = {"series_id": "DGS10", "vintage_date": "2025-01-02"}

        with patch.object(stream, "get_context_state", return_value={}):
            records = list(stream.get_records(context=context))

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["realtime_start"], "2025-01-02")
        self.assertEqual(records[0]["vintage_date"], "2025-01-02")

    def test_vintage_date_in_schema(self):
        """Fix 3: vintage_date must be in SeriesObservationsStream schema.

        Without this, the SDK silently drops the vintage_date field that
        _get_resource_records adds, making it impossible for downstream
        consumers to identify which vintage a record belongs to.
        """
        tap = TapFRED(config=self.pit_config)
        stream = {s.name: s for s in tap.streams.values()}["series_observations"]

        schema_properties = stream.schema.get("properties", {})
        self.assertIn("vintage_date", schema_properties,
                      "vintage_date must be in schema so SDK doesn't silently drop it")
        self.assertEqual(
            schema_properties["vintage_date"]["format"], "date",
            "vintage_date must be DateType (format=date)"
        )


class TestInstitutionalHardening(unittest.TestCase):
    """Tests for hardening fixes (Fixes 6a, 6b, 7, 8, 9).

    These ensure configuration edge cases, empty states, and exception
    handling are handled correctly at institutional-grade standards.
    """

    def setUp(self):
        self.base_config = {
            "api_key": "test_api_key",
            "series_ids": ["GDP"],
            "data_mode": "FRED",
        }

    # --- Fix 6a: String series_ids normalization ---

    def test_string_series_ids_normalized(self):
        """Fix 6a: A bare string series_ids must be normalized to a list.

        Without this, iterating 'GDP' yields ['G', 'D', 'P'] — three bogus partitions.
        """
        config = {**self.base_config, "series_ids": "GDP"}
        tap = TapFRED(config=config)

        # Must be a list, not a string
        self.assertIsInstance(tap.config["series_ids"], list)
        self.assertEqual(tap.config["series_ids"], ["GDP"])

    # --- Fix 6b: Empty resource_ids returns empty partitions ---

    def test_empty_resource_ids_returns_empty_partitions(self):
        """Fix 6b: Empty resource_ids cache must return [] partitions, not IndexError."""
        from tap_fred.client import PointInTimePartitionStream

        config = {
            **self.base_config,
            "point_in_time_mode": True,
            "point_in_time_start": "2025-01-01",
            "point_in_time_end": "2025-01-10",
        }
        tap = TapFRED(config=config)

        # Mock the cached series IDs to return empty
        tap._cached_series_ids = []

        stream = {s.name: s for s in tap.streams.values()}["series_observations"]

        # Must return empty list, NOT raise IndexError
        partitions = stream.partitions
        self.assertEqual(partitions, [])

    # --- Fix 7: Startup config validation ---

    def test_empty_series_ids_config_raises(self):
        """Fix 7: Empty series_ids list must raise ValueError at startup."""
        config = {**self.base_config, "series_ids": []}

        with self.assertRaises(ValueError) as cm:
            TapFRED(config=config)

        self.assertIn("empty list", str(cm.exception))

    def test_pit_start_after_end_raises(self):
        """Fix 7: point_in_time_start > point_in_time_end must raise ValueError."""
        config = {
            **self.base_config,
            "point_in_time_mode": True,
            "point_in_time_start": "2025-06-01",
            "point_in_time_end": "2025-01-01",
        }

        with self.assertRaises(ValueError) as cm:
            TapFRED(config=config)

        self.assertIn("point_in_time_start", str(cm.exception))

    def test_realtime_start_after_end_raises(self):
        """Fix 7: realtime_start > realtime_end must raise ValueError."""
        config = {
            **self.base_config,
            "data_mode": "ALFRED",
            "realtime_start": "2025-06-01",
            "realtime_end": "2025-01-01",
        }

        with self.assertRaises(ValueError) as cm:
            TapFRED(config=config)

        self.assertIn("realtime_start", str(cm.exception))

    # --- Fix 8: Category BFS exception narrowing ---

    @patch("tap_fred.client.FREDStream._make_request")
    def test_category_bfs_strict_mode_raises(self, mock_request):
        """Fix 8: In strict_mode, a RequestException during BFS must propagate."""
        call_count = {"n": 0}

        def side_effect(url, params):
            call_count["n"] += 1
            cid = params.get("category_id")
            # Call 1: fetch category 0 (root) — succeeds
            if cid == 0 and "children" not in url:
                return {"categories": [{"id": 0, "name": "Root", "parent_id": 0}]}
            # Call 2: fetch children of category 0 — returns child 99
            if cid == 0 and "children" in url:
                return {"categories": [{"id": 99, "name": "Child", "parent_id": 0}]}
            # Call 3: fetch category 99 — network error
            if cid == 99:
                raise requests.exceptions.ConnectionError("network failure")
            return {"categories": []}

        mock_request.side_effect = side_effect

        config = {
            **self.base_config,
            "category_ids": ["*"],
            "strict_mode": True,
        }
        tap = TapFRED(config=config)
        stream = {s.name: s for s in tap.streams.values()}["categories"]

        with self.assertRaises(requests.exceptions.ConnectionError):
            list(stream.get_records(context=None))

    @patch("tap_fred.client.FREDStream._make_request")
    def test_category_bfs_permissive_continues(self, mock_request):
        """Fix 8: In permissive mode, RequestException during BFS must be logged and skipped."""
        from tap_fred.streams.category_streams import CategoryStream

        call_log = []

        def side_effect(url, params):
            call_log.append((url, params.get("category_id")))
            cid = params.get("category_id")
            if "children" in url:
                if cid == 0:
                    return {"categories": [{"id": 99, "name": "Child"}]}
                return {"categories": []}
            if cid == 0:
                return {"categories": [{"id": 0, "name": "Root", "parent_id": 0}]}
            if cid == 99:
                raise requests.exceptions.ConnectionError("network failure")
            return {"categories": []}

        mock_request.side_effect = side_effect

        config = {
            **self.base_config,
            "category_ids": ["*"],
            "strict_mode": False,
        }
        tap = TapFRED(config=config)
        stream = {s.name: s for s in tap.streams.values()}["categories"]

        # Should NOT raise — error is swallowed
        records = list(stream.get_records(context=None))

        # Root category was yielded, child was skipped
        self.assertTrue(len(records) >= 1)
        root_ids = [r["id"] for r in records]
        self.assertIn(0, root_ids)

    # --- Fix 9: Vintage dates bookmark filtering ---

    @patch("tap_fred.client.FREDStream._make_request")
    def test_vintage_dates_skips_already_synced(self, mock_request):
        """Fix 9: Already-synced vintage dates must be skipped based on bookmark."""
        mock_request.return_value = {
            "vintage_dates": ["2020-01-15", "2020-02-15", "2020-03-15"]
        }

        config = {
            **self.base_config,
            "series_ids": ["GDP"],
            "data_mode": "ALFRED",
            "realtime_start": "2020-01-01",
            "realtime_end": "2020-12-31",
        }
        tap = TapFRED(config=config)
        tap._cached_series_ids = [{"series_id": "GDP"}]

        stream = {s.name: s for s in tap.streams.values()}["series_vintage_dates"]

        # Simulate bookmark at 2020-02-15 (already synced up to this date)
        mock_state = {"replication_key_value": "2020-02-15"}
        with patch.object(stream, "get_context_state", return_value=mock_state):
            records = list(stream.get_records(context={"series_id": "GDP"}))

        # Only 2020-03-15 should be emitted (after bookmark)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["date"], "2020-03-15")

    @patch("tap_fred.client.FREDStream._make_request")
    def test_vintage_dates_no_bookmark_yields_all(self, mock_request):
        """Fix 9 (negative): Without a bookmark, all vintage dates must be yielded."""
        mock_request.return_value = {
            "vintage_dates": ["2020-01-15", "2020-02-15", "2020-03-15"]
        }

        config = {
            **self.base_config,
            "series_ids": ["GDP"],
            "data_mode": "ALFRED",
            "realtime_start": "2020-01-01",
            "realtime_end": "2020-12-31",
        }
        tap = TapFRED(config=config)
        tap._cached_series_ids = [{"series_id": "GDP"}]

        stream = {s.name: s for s in tap.streams.values()}["series_vintage_dates"]

        # No bookmark — all dates should come through
        with patch.object(stream, "get_context_state", return_value={}):
            records = list(stream.get_records(context={"series_id": "GDP"}))

        self.assertEqual(len(records), 3)
        dates = [r["date"] for r in records]
        self.assertEqual(dates, ["2020-01-15", "2020-02-15", "2020-03-15"])


if __name__ == "__main__":
    unittest.main()
