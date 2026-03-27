"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import os
import unittest

import pytest
from singer_sdk import typing as th
from singer_sdk.testing import get_tap_test_class

from tap_fred.client import FREDStream
from tap_fred.tap import TapFRED

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

    @pytest.mark.skip(
        reason="FRED_API_KEY not set - SDK integration tests require a real API key"
    )
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


class TestSanitizeResourceId(unittest.TestCase):
    """Test _sanitize_resource_id strips JSON array encoding from IDs."""

    def test_plain_string_unchanged(self):
        assert FREDStream._sanitize_resource_id("GDP") == "GDP"

    def test_json_array_single_element(self):
        assert FREDStream._sanitize_resource_id('["GDP"]') == "GDP"

    def test_json_array_with_long_id(self):
        assert (
            FREDStream._sanitize_resource_id('["CLVMNACSCAB1GQEA19"]')
            == "CLVMNACSCAB1GQEA19"
        )

    def test_multi_element_array_untouched(self):
        # Multi-element arrays should not be silently collapsed
        assert (
            FREDStream._sanitize_resource_id('["GDP","UNRATE"]') == '["GDP","UNRATE"]'
        )

    def test_numeric_string(self):
        assert FREDStream._sanitize_resource_id("125") == "125"

    def test_whitespace_stripped(self):
        assert FREDStream._sanitize_resource_id("  GDP  ") == "GDP"

    def test_non_json_brackets_unchanged(self):
        assert FREDStream._sanitize_resource_id("[not json") == "[not json"


class TestSeriesIdNormalization(unittest.TestCase):
    """Test that series_ids config is properly normalized regardless of input format.

    Reproduces the production bug where '["GDP"]' (JSON string from env vars)
    was wrapped as ['["GDP"]'] instead of decoded to ["GDP"].
    """

    def test_json_string_decoded_to_list(self):
        """String '["GDP"]' from env var must decode to list ["GDP"], not ['["GDP"]']."""
        config = {**SAMPLE_CONFIG, "series_ids": '["GDP"]'}
        tap = TapFRED(config=config)
        assert tap.config["series_ids"] == ["GDP"]

    def test_json_string_multi_element(self):
        config = {**SAMPLE_CONFIG, "series_ids": '["GDP","UNRATE"]'}
        tap = TapFRED(config=config)
        assert tap.config["series_ids"] == ["GDP", "UNRATE"]

    def test_plain_string_wrapped(self):
        """Plain string 'GDP' wraps as ['GDP']."""
        config = {**SAMPLE_CONFIG, "series_ids": "GDP"}
        tap = TapFRED(config=config)
        assert tap.config["series_ids"] == ["GDP"]

    def test_list_passthrough(self):
        """Already a list — no change needed."""
        config = {**SAMPLE_CONFIG, "series_ids": ["GDP", "UNRATE"]}
        tap = TapFRED(config=config)
        assert tap.config["series_ids"] == ["GDP", "UNRATE"]

    def test_cache_sanitizes_double_encoded_elements(self):
        """If list element is '["GDP"]' (double-encoded), cache must unwrap it."""
        config = {**SAMPLE_CONFIG, "series_ids": ['["GDP"]', "UNRATE"]}
        tap = TapFRED(config=config)
        cached = tap.get_cached_series_ids()
        ids = [item["series_id"] for item in cached]
        assert ids == ["GDP", "UNRATE"]

    def test_wildcard_unchanged(self):
        config = {**SAMPLE_CONFIG, "series_ids": ["*"]}
        tap = TapFRED(config=config)
        assert tap.config["series_ids"] == ["*"]
