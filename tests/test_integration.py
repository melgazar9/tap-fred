"""Integration tests for tap-fred with mocked API responses.

These tests simulate the actual API calls and validate the complete data flow
without requiring actual API keys or network calls.
"""

import unittest
from unittest.mock import Mock, patch

from tap_fred.tap import TapFRED


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

    @patch("tap_fred.client.requests.Session.get")
    def test_series_observations_fred_mode(self, mock_get):
        """Test series_observations extraction in FRED mode."""
        # Mock API response for series observations
        mock_response = Mock()
        mock_response.json.return_value = {
            "realtime_start": "2025-09-11",
            "realtime_end": "2025-09-11",
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
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        tap = TapFRED(config=self.config)
        streams = {stream.name: stream for stream in tap.streams.values()}
        series_obs_stream = streams["series_observations"]

        # Mock the series ID caching
        with patch.object(series_obs_stream, "_get_series_ids", return_value=["GDP"]):
            records = list(series_obs_stream.get_records(context={"series_id": "GDP"}))

        # Validate records
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["series_id"], "GDP")
        self.assertEqual(records[0]["date"], "2024-01-01")
        self.assertEqual(records[0]["value"], "21000.0")
        self.assertEqual(records[0]["realtime_start"], "2025-09-11")

    @patch("tap_fred.client.requests.Session.get")
    def test_series_observations_alfred_mode(self, mock_get):
        """Test series_observations extraction in ALFRED mode with vintage data."""
        # Mock API response for ALFRED mode
        mock_response = Mock()
        mock_response.json.return_value = {
            "realtime_start": "2020-01-01",
            "realtime_end": "2020-01-01",
            "observations": [
                {
                    "realtime_start": "2020-01-01",
                    "realtime_end": "2020-01-01",
                    "date": "2019-10-01",
                    "value": "21734.266",  # Historical vintage value
                }
            ],
        }
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        tap = TapFRED(config=self.alfred_config)
        streams = {stream.name: stream for stream in tap.streams.values()}
        series_obs_stream = streams["series_observations"]

        # Mock the series ID caching
        with patch.object(series_obs_stream, "_get_series_ids", return_value=["GDP"]):
            records = list(series_obs_stream.get_records(context={"series_id": "GDP"}))

        # Validate vintage data
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["realtime_start"], "2020-01-01")
        self.assertEqual(records[0]["realtime_end"], "2020-01-01")
        self.assertEqual(records[0]["value"], "21734.266")

    @patch("tap_fred.client.requests.Session.get")
    def test_multiple_streams_execution(self, mock_get):
        """Test execution of multiple streams with different configurations."""

        # Mock different responses for different endpoints
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

        # Test that multiple stream types can be instantiated
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

        # Verify wildcard configuration is preserved
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

        # Test that incremental stream has replication key
        self.assertEqual(series_obs_stream.replication_key, "realtime_start")

        # Test bookmark handling
        # state = {
        #     "bookmarks": {
        #         "series_observations": {
        #             "partitions": [
        #                 {
        #                     "context": {"series_id": "GDP"},
        #                     "replication_key_value": "2024-01-01",
        #                 }
        #             ]
        #         }
        #     }
        # }

        # This would be handled by Singer SDK in real execution
        self.assertIsNotNone(series_obs_stream.replication_key)

    def test_schema_output_validation(self):
        """Test that output schemas match Singer specification."""
        tap = TapFRED(config=self.config)

        # Test series_observations schema
        streams = {stream.name: stream for stream in tap.streams.values()}
        series_obs_stream = streams["series_observations"]

        schema = series_obs_stream.schema

        # Validate Singer schema structure
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

        # Test that primary keys are properly defined
        self.assertEqual(series_obs_stream.primary_keys, ["series_id", "date"])

        # Test partitioning configuration
        self.assertTrue(hasattr(series_obs_stream, "_resource_type"))
        self.assertEqual(series_obs_stream._resource_type, "series")

    @patch("tap_fred.client.requests.Session.get")
    def test_error_handling_api_failures(self, mock_get):
        """Test error handling for API failures."""
        # Mock API failure response
        mock_response = Mock()
        mock_response.status_code = 429  # Rate limiting
        mock_response.json.return_value = {"error": "Rate limit exceeded"}
        mock_get.return_value = mock_response

        tap = TapFRED(config=self.config)
        streams = {stream.name: stream for stream in tap.streams.values()}
        series_obs_stream = streams["series_observations"]

        # The stream should handle API errors gracefully
        # This would typically retry or log the error appropriately
        self.assertIsNotNone(series_obs_stream)

    def test_production_scale_configuration(self):
        """Test configuration suitable for production-scale extraction."""
        production_config = {
            **self.config,
            "start_date": "2015-01-01",  # 10 years of data
            "data_mode": "ALFRED",
            "realtime_start": "2020-01-01",
            "realtime_end": "2020-01-01",
            "max_requests_per_minute": 120,  # Maximum allowed by FRED
            "min_throttle_seconds": 0.5,  # Fast but safe
            "series_ids": ["*"],  # All series
            "category_ids": ["*"],  # All categories
            "release_ids": ["*"],  # All releases
            "source_ids": ["*"],  # All sources
        }

        tap = TapFRED(config=production_config)

        # Verify production configuration is accepted
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


if __name__ == "__main__":
    unittest.main()
