"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import unittest

from singer_sdk.testing import get_tap_test_class

from tap_fred.tap import TapFRED
from tap_fred.client import FREDStream

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "api_key": "test_key",
    "series_ids": ["GDP"],
    "data_mode": "ALFRED",
    "point_in_time_mode": True,
    "realtime_start": "2020-01-01",
    "realtime_end": "2020-01-01",
}


# Run standard built-in tap tests from the SDK:
TestTapFRED = get_tap_test_class(
    tap_class=TapFRED,
    config=SAMPLE_CONFIG,
)


class TestConfigAccess(unittest.TestCase):
    """Test config access patterns and point-in-time mode functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.tap = TapFRED(config=SAMPLE_CONFIG)

    def test_point_in_time_mode_config_access(self):
        """Test that point_in_time_mode is accessed correctly via self.config."""

        # Create a mock stream that inherits from FREDStream
        class MockFREDStream(FREDStream):
            name = "test_stream"
            path = "/test"
            records_jsonpath = "$.data[*]"

            def _get_records_key(self):
                return "data"

        # Create stream instance
        stream = MockFREDStream(self.tap)

        # Test that config access works correctly
        self.assertTrue(stream.config.get("point_in_time_mode", False))
        self.assertEqual(stream.config.get("data_mode"), "ALFRED")
        self.assertEqual(stream.config.get("realtime_start"), "2020-01-01")

    def test_alfred_params_added_correctly(self):
        """Test that ALFRED parameters are added correctly in different scenarios."""

        class MockFREDStream(FREDStream):
            name = "test_stream"
            path = "/test"
            records_jsonpath = "$.data[*]"

            def _get_records_key(self):
                return "data"

        stream = MockFREDStream(self.tap)

        # Test regular ALFRED mode (no context)
        params = {}
        stream._add_alfred_params(params, None)
        self.assertEqual(params.get("realtime_start"), "2020-01-01")
        self.assertEqual(params.get("realtime_end"), "2020-01-01")

        # Test point-in-time mode with vintage_date in context
        params = {}
        context = {"vintage_date": "2019-06-15"}
        stream._add_alfred_params(params, context)
        self.assertEqual(params.get("realtime_start"), "2019-06-15")
        self.assertEqual(params.get("realtime_end"), "2019-06-15")

    def test_parameter_documentation_clarity(self):
        """Test that parameter usage is clear and documented."""
        # Test that both parameter sets are understood correctly
        config_with_both = {
            **SAMPLE_CONFIG,
            "point_in_time_start": "2019-01-01",
            "point_in_time_end": "2021-01-01",
        }

        tap = TapFRED(config=config_with_both)

        # realtime_start/end should be used for global ALFRED mode
        self.assertEqual(tap.config.get("realtime_start"), "2020-01-01")
        self.assertEqual(tap.config.get("realtime_end"), "2020-01-01")

        # point_in_time_start/end should be used for filtering vintage dates
        self.assertEqual(tap.config.get("point_in_time_start"), "2019-01-01")
        self.assertEqual(tap.config.get("point_in_time_end"), "2021-01-01")
