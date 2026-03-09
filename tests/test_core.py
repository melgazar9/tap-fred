"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import os
import unittest

import pytest
from singer_sdk import typing as th
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


# SDK integration tests require a real API key (sync_all() makes live HTTP requests).
# All mocked/unit tests below always run regardless of API key availability.
if os.environ.get("FRED_API_KEY"):
    _sdk_config = {**SAMPLE_CONFIG, "api_key": os.environ["FRED_API_KEY"]}
    TestTapFRED = get_tap_test_class(
        tap_class=TapFRED,
        config=_sdk_config,
    )
else:

    @pytest.mark.skip(reason="FRED_API_KEY not set - SDK integration tests require a real API key")
    class TestTapFRED:
        pass


class TestConfigAccess(unittest.TestCase):
    """Test config access patterns and point-in-time mode functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.tap = TapFRED(config=SAMPLE_CONFIG)

    def test_point_in_time_mode_config_access(self):
        """Test that point_in_time_mode is accessed correctly via self.config."""

        class MockFREDStream(FREDStream):
            name = "test_stream"
            path = "/test"
            records_jsonpath = "$.data[*]"

            schema = th.PropertiesList(
                th.Property("id", th.StringType),
            ).to_dict()

            def _get_records_key(self):
                return "data"

        stream = MockFREDStream(self.tap)

        self.assertTrue(stream.config.get("point_in_time_mode", False))
        self.assertEqual(stream.config.get("data_mode"), "ALFRED")
        self.assertEqual(stream.config.get("realtime_start"), "2020-01-01")

    def test_realtime_params_added_correctly(self):
        """Test that realtime parameters are added correctly in ALFRED mode."""

        class MockFREDStream(FREDStream):
            name = "test_stream"
            path = "/test"
            records_jsonpath = "$.data[*]"

            schema = th.PropertiesList(
                th.Property("id", th.StringType),
            ).to_dict()

            def _get_records_key(self):
                return "data"

        stream = MockFREDStream(self.tap)

        # ALFRED mode should add realtime params during __init__
        self.assertEqual(stream.query_params.get("realtime_start"), "2020-01-01")
        self.assertEqual(stream.query_params.get("realtime_end"), "2020-01-01")

    def test_parameter_documentation_clarity(self):
        """Test that parameter usage is clear and documented."""
        config_with_both = {
            **SAMPLE_CONFIG,
            "point_in_time_start": "2019-01-01",
            "point_in_time_end": "2021-01-01",
        }

        tap = TapFRED(config=config_with_both)

        self.assertEqual(tap.config.get("realtime_start"), "2020-01-01")
        self.assertEqual(tap.config.get("realtime_end"), "2020-01-01")
        self.assertEqual(tap.config.get("point_in_time_start"), "2019-01-01")
        self.assertEqual(tap.config.get("point_in_time_end"), "2021-01-01")
