"""Comprehensive integration tests for tap-fred functionality.

These tests validate the scenarios tested manually to ensure they continue working
without needing manual testing every time.
"""

import unittest

from tap_fred.tap import TapFRED


class TestTapFREDComprehensive(unittest.TestCase):
    """Comprehensive tests covering all major tap-fred functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.base_config = {
            "api_key": "test_key",
            "series_ids": ["GDP", "UNRATE"],
            "category_ids": ["125", "32992"],
            "release_ids": ["53", "151"],
            "source_ids": ["1", "3"],
            "tag_names": ["gdp", "unemployment"],
            "start_date": "2024-01-01",
        }

    def test_fred_mode_config(self):
        """Test FRED mode configuration."""
        config = {
            **self.base_config,
            "data_mode": "FRED",
        }
        tap = TapFRED(config=config)

        # Verify FRED mode is properly configured
        self.assertEqual(tap.config.get("data_mode"), "FRED")
        self.assertIsNone(tap.config.get("realtime_start"))
        self.assertIsNone(tap.config.get("realtime_end"))

    def test_alfred_mode_config(self):
        """Test ALFRED mode configuration with point-in-time parameters."""
        config = {
            **self.base_config,
            "data_mode": "ALFRED",
            "realtime_start": "2020-01-01",
            "realtime_end": "2020-01-01",
        }
        tap = TapFRED(config=config)

        # Verify ALFRED mode is properly configured
        self.assertEqual(tap.config.get("data_mode"), "ALFRED")
        self.assertEqual(tap.config.get("realtime_start"), "2020-01-01")
        self.assertEqual(tap.config.get("realtime_end"), "2020-01-01")

    def test_stream_discovery(self):
        """Test that all expected streams are discovered."""
        config = {**self.base_config, "data_mode": "FRED"}
        tap = TapFRED(config=config)

        # Get all stream maps
        stream_maps = tap.get_stream_maps()
        stream_names = {stream.stream for stream in stream_maps}

        # Verify we have the core streams
        expected_core_streams = {
            "series_observations",
            "categories",
            "releases",
            "sources",
            "series",
            "tags",
            "geofred_regional_data",
            "geofred_series_data",
        }

        for stream_name in expected_core_streams:
            self.assertIn(
                stream_name,
                stream_names,
                f"Stream '{stream_name}' not found in discovered streams",
            )

    def test_total_stream_count(self):
        """Test that we have exactly 32 streams as documented."""
        config = {**self.base_config, "data_mode": "FRED"}
        tap = TapFRED(config=config)

        stream_maps = tap.get_stream_maps()
        total_streams = len(stream_maps)

        # Verify we have 32 streams total as documented in CLAUDE.md
        self.assertEqual(
            total_streams, 32, f"Expected 32 streams, but found {total_streams}"
        )

    def test_schema_validation_series_observations(self):
        """Test series_observations stream schema matches expected structure."""
        config = {**self.base_config, "data_mode": "FRED"}
        tap = TapFRED(config=config)

        # Get series_observations stream
        streams = {stream.name: stream for stream in tap.streams.values()}
        series_obs_stream = streams.get("series_observations")

        self.assertIsNotNone(series_obs_stream, "series_observations stream not found")

        # Check schema has required fields
        schema_properties = series_obs_stream.schema.get("properties", {})
        required_fields = {
            "series_id",
            "date",
            "value",
            "realtime_start",
            "realtime_end",
        }

        for field in required_fields:
            self.assertIn(
                field,
                schema_properties,
                f"Required field '{field}' missing from schema",
            )

    def test_incremental_replication_key(self):
        """Test that incremental streams have proper replication keys."""
        config = {**self.base_config, "data_mode": "FRED"}
        tap = TapFRED(config=config)

        streams = {stream.name: stream for stream in tap.streams.values()}
        series_obs_stream = streams.get("series_observations")

        self.assertIsNotNone(series_obs_stream)
        self.assertEqual(series_obs_stream.replication_key, "realtime_start")

    def test_wildcard_configuration(self):
        """Test wildcard configuration for resource discovery."""
        config = {
            **self.base_config,
            "data_mode": "FRED",
            "series_ids": ["*"],
            "category_ids": ["*"],
            "release_ids": ["*"],
            "source_ids": ["*"],
            "tag_names": ["*"],
        }
        tap = TapFRED(config=config)

        # Verify wildcard configs are preserved
        self.assertEqual(tap.config.get("series_ids"), ["*"])
        self.assertEqual(tap.config.get("category_ids"), ["*"])
        self.assertEqual(tap.config.get("release_ids"), ["*"])
        self.assertEqual(tap.config.get("source_ids"), ["*"])
        self.assertEqual(tap.config.get("tag_names"), ["*"])

    def test_production_date_ranges(self):
        """Test production-scale date range configuration."""
        config = {
            **self.base_config,
            "data_mode": "ALFRED",
            "start_date": "2015-01-01",  # 10 years of data
            "realtime_start": "2020-01-01",
            "realtime_end": "2020-01-01",
        }
        tap = TapFRED(config=config)

        # Verify historical date configuration
        self.assertEqual(tap.config.get("start_date"), "2015-01-01")

    def test_point_in_time_mode(self):
        """Test point-in-time mode configuration for backtesting."""
        config = {
            **self.base_config,
            "data_mode": "ALFRED",
            "point_in_time_mode": True,
            "point_in_time_start": "2020-01-01",
            "point_in_time_end": "2021-12-31",
            "realtime_start": "2020-01-01",
            "realtime_end": "2023-12-31",
        }
        tap = TapFRED(config=config)

        # Verify point-in-time configuration
        self.assertTrue(tap.config.get("point_in_time_mode"))
        self.assertEqual(tap.config.get("point_in_time_start"), "2020-01-01")
        self.assertEqual(tap.config.get("point_in_time_end"), "2021-12-31")

    def test_rate_limiting_configuration(self):
        """Test rate limiting configuration."""
        config = {
            **self.base_config,
            "data_mode": "FRED",
            "max_requests_per_minute": 120,
            "min_throttle_seconds": 0.5,
        }
        tap = TapFRED(config=config)

        # Verify rate limiting config
        self.assertEqual(tap.config.get("max_requests_per_minute"), 120)
        self.assertEqual(tap.config.get("min_throttle_seconds"), 0.5)

    def test_stream_selection_categories(self):
        """Test different stream categories are properly organized."""
        config = {**self.base_config, "data_mode": "FRED"}
        tap = TapFRED(config=config)

        stream_maps = tap.get_stream_maps()
        stream_names = {stream.stream for stream in stream_maps}

        # Test Series streams (10 expected)
        series_streams = {name for name in stream_names if name.startswith("series")}
        self.assertGreaterEqual(
            len(series_streams), 8, "Not enough series streams found"
        )

        # Test Category streams (6 expected)
        category_streams = {
            name
            for name in stream_names
            if name.startswith("category") or name == "categories"
        }
        self.assertGreaterEqual(
            len(category_streams), 5, "Not enough category streams found"
        )

        # Test Release streams (8 expected)
        release_streams = {name for name in stream_names if name.startswith("release")}
        self.assertGreaterEqual(
            len(release_streams), 7, "Not enough release streams found"
        )

        # Test Source streams (3 expected)
        source_streams = {name for name in stream_names if name.startswith("source")}
        self.assertGreaterEqual(
            len(source_streams), 2, "Not enough source streams found"
        )

        # Test Tag streams (3 expected)
        tag_streams = {
            name
            for name in stream_names
            if "tag" in name
            and not name.startswith("category")
            and not name.startswith("release")
        }
        self.assertGreaterEqual(len(tag_streams), 2, "Not enough tag streams found")

    def test_config_validation(self):
        """Test configuration validation and error handling."""
        # Test missing required config
        with self.assertRaises(Exception):
            TapFRED(config={})  # Missing api_key and series_ids

        # Test invalid data mode
        invalid_config = {
            **self.base_config,
            "data_mode": "INVALID_MODE",
        }
        # Should not raise exception, but mode might be ignored or defaulted
        tap = TapFRED(config=invalid_config)
        self.assertIsNotNone(tap)

    def test_schema_field_types(self):
        """Test that schema field types are correctly defined."""
        config = {**self.base_config, "data_mode": "FRED"}
        tap = TapFRED(config=config)

        streams = {stream.name: stream for stream in tap.streams.values()}
        series_obs_stream = streams.get("series_observations")

        schema_props = series_obs_stream.schema.get("properties", {})

        # Test specific field types
        self.assertIn("type", schema_props.get("series_id", {}))
        self.assertIn("type", schema_props.get("date", {}))
        self.assertIn("type", schema_props.get("value", {}))
        self.assertIn("type", schema_props.get("realtime_start", {}))
        self.assertIn("type", schema_props.get("realtime_end", {}))


if __name__ == "__main__":
    unittest.main()
