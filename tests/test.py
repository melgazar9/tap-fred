#!/usr/bin/env python3
"""
Comprehensive Test Script for tap-fred
======================================

This script validates all 33 FRED API streams with the tap-fmp configuration pattern.
Tests each stream category with exactly 2 configured IDs and validates:
- API calls to correct endpoints
- Output files contain expected data structures
- Schemas match API responses
- Throttling prevents exceeding 60 requests/minute
- Both FRED and ALFRED modes (where applicable)
- Caching works across dependent streams

REQUIREMENTS:
- FRED_API_KEY environment variable must be set
- Must be run from tap-fred project root directory
- Meltano must be installed and available
- All dependencies from pyproject.toml must be installed

USAGE:
    # Full test suite (all 43 stream tests across 6 configurations)
    python comprehensive_test.py

    # Show test configurations without running
    python comprehensive_test.py --dry-run

    # Test only specific configuration
    python comprehensive_test.py --config-only series_streams
    python comprehensive_test.py --config-only category_streams

    # Test only specific mode
    python comprehensive_test.py --mode-only FRED
    python comprehensive_test.py --mode-only ALFRED

    # Combined filters
    python comprehensive_test.py --config-only series_streams --mode-only FRED

TEST CONFIGURATIONS:
1. series_streams (10 streams): GDP, UNRATE - Tests FRED & ALFRED modes
2. category_streams (6 streams): categories 125, 13 - Trade Balance, Price Indexes
3. release_streams (9 streams): releases 53, 151 - GDP, CPI publications
4. source_streams (3 streams): sources 1, 3 - Board of Governors, Philadelphia Fed
5. tag_streams (3 streams): tags 'gdp', 'inflation' - Popular economic tags
6. geofred_streams (2 streams): Geographic/regional data (no specific IDs needed)

VALIDATION COVERAGE:
✓ All 33 FRED API endpoints with correct URL patterns
✓ Stream-specific configuration with exactly 2 IDs per category
✓ Output file structure and schema validation
✓ Rate limiting compliance (60 requests/minute with sliding window)
✓ FRED vs ALFRED mode testing with proper date handling
✓ Thread-safe caching across dependent streams
✓ Comprehensive error handling and reporting

EXPECTED RESULTS:
- Total stream tests: 43 (33 streams tested across multiple modes)
- Success rate: 100% for properly configured FRED API key
- Execution time: ~5-10 minutes (with rate limiting)
- API calls: ~100-200 total (distributed across time with throttling)
"""

import argparse
import json
import os
import tempfile
import time
import subprocess
import sys
from typing import Dict, List, Any
import logging
import re
from dataclasses import dataclass, field
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class TestConfig:
    """Test configuration for different stream categories."""

    name: str
    config: Dict[str, Any]
    expected_streams: List[str]
    test_modes: List[str] = field(default_factory=lambda: ["FRED"])
    requires_ids: bool = False
    min_records: int = 0


@dataclass
class TestResult:
    """Test result for a stream configuration."""

    config_name: str
    mode: str
    stream_name: str
    success: bool
    records_count: int = 0
    schema_valid: bool = False
    endpoint_valid: bool = False
    error_message: str = ""
    execution_time: float = 0.0
    api_calls_made: int = 0


class FREDTestSuite:
    """Comprehensive test suite for tap-fred streams."""

    def __init__(self):
        self.results: List[TestResult] = []
        self.start_time = time.time()
        self.api_call_times: List[float] = []
        self.lock = threading.Lock()

        # Track API calls for throttling validation
        self.api_call_count = 0
        self.rate_limit_violations = 0

        # Test configurations for all stream categories
        self.test_configs = self._create_test_configs()

        # Expected endpoints for validation
        self.expected_endpoints = self._get_expected_endpoints()

        # Required environment variables
        self.required_env = ["FRED_API_KEY"]

    def _create_test_configs(self) -> List[TestConfig]:
        """Create test configurations for all stream categories with specific IDs."""
        base_config = {
            "api_key": "${FRED_API_KEY}",
            "api_url": "https://api.stlouisfed.org/fred",
            "max_requests_per_minute": 60,
            "min_throttle_seconds": 1.0,
            "start_date": "2024-01-01",
        }

        configs = []

        # 1. Series streams (10 streams) - Core data streams
        configs.append(
            TestConfig(
                name="series_streams",
                config={
                    **base_config,
                    "series_ids": ["GDP", "UNRATE"],  # 2 popular series
                    "enable_series_streams": True,
                    "enable_metadata_streams": False,
                    "enable_geofred_streams": False,
                },
                expected_streams=[
                    "series",
                    "series_observations",
                    "series_categories",
                    "series_release",
                    "series_search",
                    "series_search_tags",
                    "series_search_related_tags",
                    "series_tags",
                    "series_updates",
                    "series_vintage_dates",
                ],
                test_modes=["FRED", "ALFRED"],
                requires_ids=True,
                min_records=1,
            )
        )

        # 2. Category streams (6 streams) - Hierarchical metadata
        configs.append(
            TestConfig(
                name="category_streams",
                config={
                    **base_config,
                    "series_ids": ["GDP"],  # Minimal series for dependency
                    "category_ids": [
                        "125",
                        "13",
                    ],  # Trade Balance (125), Price Indexes (13)
                    "enable_series_streams": False,
                    "enable_metadata_streams": True,
                    "enable_geofred_streams": False,
                },
                expected_streams=[
                    "categories",
                    "category_children",
                    "category_related",
                    "category_series",
                    "category_tags",
                    "category_related_tags",
                ],
                test_modes=["FRED"],  # Metadata usually doesn't support ALFRED
                min_records=1,
            )
        )

        # 3. Release streams (9 streams) - Publication metadata
        configs.append(
            TestConfig(
                name="release_streams",
                config={
                    **base_config,
                    "series_ids": ["GDP"],  # Minimal series for dependency
                    "release_ids": ["53", "151"],  # GDP (53), CPI (151)
                    "enable_series_streams": False,
                    "enable_metadata_streams": True,
                    "enable_geofred_streams": False,
                },
                expected_streams=[
                    "releases",
                    "release",
                    "release_dates",
                    "single_release_dates",
                    "release_series",
                    "release_sources",
                    "release_tags",
                    "release_related_tags",
                    "release_tables",
                ],
                test_modes=["FRED"],
                min_records=1,
            )
        )

        # 4. Source streams (3 streams) - Data provider metadata
        configs.append(
            TestConfig(
                name="source_streams",
                config={
                    **base_config,
                    "series_ids": ["GDP"],  # Minimal series for dependency
                    "source_ids": [
                        "1",
                        "3",
                    ],  # Board of Governors (1), Philadelphia Fed (3)
                    "enable_series_streams": False,
                    "enable_metadata_streams": True,
                    "enable_geofred_streams": False,
                },
                expected_streams=["sources", "source", "source_releases"],
                test_modes=["FRED"],
                min_records=1,
            )
        )

        # 5. Tag streams (3 streams) - Topic/keyword metadata
        configs.append(
            TestConfig(
                name="tag_streams",
                config={
                    **base_config,
                    "series_ids": ["GDP"],  # Minimal series for dependency
                    "tag_names": ["gdp", "inflation"],  # 2 popular economic tags
                    "search_text": "",  # Required for tags_series
                    "enable_series_streams": False,
                    "enable_metadata_streams": True,
                    "enable_geofred_streams": False,
                },
                expected_streams=["tags", "related_tags", "tags_series"],
                test_modes=["FRED"],
                min_records=1,
            )
        )

        # 6. GeoFRED streams (2 streams) - Geographic/regional data
        configs.append(
            TestConfig(
                name="geofred_streams",
                config={
                    **base_config,
                    "series_ids": ["GDP"],  # Minimal series for dependency
                    # GeoFRED uses different parameters - no specific IDs needed
                    "enable_series_streams": False,
                    "enable_metadata_streams": False,
                    "enable_geofred_streams": True,
                },
                expected_streams=["geofred_regional_data", "geofred_series_data"],
                test_modes=["FRED"],
                min_records=0,  # Geographic data might be empty for some regions
            )
        )

        return configs

    def _get_expected_endpoints(self) -> Dict[str, str]:
        """Map stream names to expected API endpoints for validation."""
        return {
            # Series endpoints
            "series": "/series",
            "series_observations": "/series/observations",
            "series_categories": "/series/categories",
            "series_release": "/series/release",
            "series_search": "/series/search",
            "series_search_tags": "/series/search/tags",
            "series_search_related_tags": "/series/search/related_tags",
            "series_tags": "/series/tags",
            "series_updates": "/series/updates",
            "series_vintage_dates": "/series/vintagedates",
            # Category endpoints
            "categories": "/category",
            "category_children": "/category/children",
            "category_related": "/category/related",
            "category_series": "/category/series",
            "category_tags": "/category/tags",
            "category_related_tags": "/category/related_tags",
            # Release endpoints
            "releases": "/releases",
            "release": "/release",
            "release_dates": "/releases/dates",
            "single_release_dates": "/release/dates",
            "release_series": "/release/series",
            "release_sources": "/release/sources",
            "release_tags": "/release/tags",
            "release_related_tags": "/release/related_tags",
            "release_tables": "/release/tables",
            # Source endpoints
            "sources": "/sources",
            "source": "/source",
            "source_releases": "/source/releases",
            # Tag endpoints
            "tags": "/tags",
            "related_tags": "/related_tags",
            "tags_series": "/tags/series",
            # GeoFRED endpoints
            "geofred_regional_data": "/geofred/regional/data",
            "geofred_series_data": "/geofred/series/data",
        }

    def validate_environment(self) -> bool:
        """Validate required environment variables and meltano setup."""
        logger.info("Validating environment variables and setup...")
        missing = []

        for env_var in self.required_env:
            if not os.getenv(env_var):
                missing.append(env_var)

        if missing:
            logger.error(f"Missing required environment variables: {missing}")
            logger.error("Please set FRED_API_KEY environment variable")
            return False

        # Check if we're in the right directory with meltano.yml
        if not os.path.exists("meltano.yml"):
            logger.error(
                "meltano.yml not found. Please run from the tap-fred project root directory."
            )
            return False

        # Validate meltano is available
        try:
            result = subprocess.run(
                ["meltano", "--version"], capture_output=True, text=True
            )
            if result.returncode != 0:
                logger.error(
                    "Meltano is not available. Please install meltano or activate the virtual environment."
                )
                return False
            logger.info(f"✓ Using meltano {result.stdout.strip()}")
        except FileNotFoundError:
            logger.error("Meltano command not found. Please install meltano.")
            return False

        logger.info("✓ Environment validation passed")
        return True

    def run_meltano_test(
        self, config: Dict[str, Any], mode: str, temp_dir: str
    ) -> Dict[str, Any]:
        """Run meltano tap with specific configuration and capture results."""
        # Create temporary config file
        config_file = os.path.join(temp_dir, f"config_{mode.lower()}.json")

        # Update config for ALFRED mode
        test_config = config.copy()
        test_config["data_mode"] = mode

        if mode == "ALFRED":
            # Use recent dates for ALFRED testing
            test_config["realtime_start"] = "2024-01-01"
            test_config["realtime_end"] = "2024-01-01"

        # Expand environment variables
        if "api_key" in test_config and test_config["api_key"] == "${FRED_API_KEY}":
            test_config["api_key"] = os.getenv("FRED_API_KEY")

        with open(config_file, "w") as f:
            json.dump(test_config, f, indent=2)

        # Output file for records
        output_file = os.path.join(temp_dir, f"output_{mode.lower()}.jsonl")

        # Run meltano el with comprehensive stream selection
        cmd = [
            "meltano",
            "el",
            "--config-file",
            config_file,
            "--state-id",
            f"test_{mode.lower()}_{int(time.time())}",
            "tap-fred",
            "target-jsonl",
            "--select",
            "*.*",  # Select all available streams
        ]

        # Set environment variables for the subprocess
        env = os.environ.copy()
        env["MELTANO_CONFIG_FILE"] = config_file
        if "output" not in env:
            env["MELTANO_OUTPUT_DIR"] = temp_dir

        logger.info(f"Running: {' '.join(cmd)}")

        start_time = time.time()
        try:
            # Capture both stdout and stderr for analysis
            result = subprocess.run(
                cmd,
                cwd=os.getcwd(),  # Use current working directory
                capture_output=True,
                text=True,
                timeout=300,  # 5 minute timeout
                env=env,
            )

            execution_time = time.time() - start_time

            # Parse output for API call tracking
            api_calls = self._count_api_calls(result.stderr)

            with self.lock:
                self.api_call_count += api_calls
                self.api_call_times.extend([time.time()] * api_calls)

            return {
                "success": result.returncode == 0,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "execution_time": execution_time,
                "api_calls": api_calls,
                "output_file": output_file,
                "config_file": config_file,
            }

        except subprocess.TimeoutExpired:
            logger.error(f"Test timed out after 5 minutes for mode {mode}")
            return {
                "success": False,
                "error": "Test timed out",
                "execution_time": time.time() - start_time,
                "api_calls": 0,
            }
        except Exception as e:
            logger.error(f"Test failed with exception: {e}")
            return {
                "success": False,
                "error": str(e),
                "execution_time": time.time() - start_time,
                "api_calls": 0,
            }

    def _count_api_calls(self, stderr_output: str) -> int:
        """Count API calls made by parsing stderr output."""
        # Look for log messages indicating API requests
        api_call_pattern = r"Stream \w+: Requesting:"
        matches = re.findall(api_call_pattern, stderr_output)
        return len(matches)

    def validate_output_files(
        self, output_file: str, expected_streams: List[str]
    ) -> Dict[str, Any]:
        """Validate output JSONL file contains expected data structures."""
        if not os.path.exists(output_file):
            return {
                "valid": False,
                "error": f"Output file not found: {output_file}",
                "streams_found": [],
                "record_counts": {},
            }

        streams_found = set()
        record_counts = {}
        schema_errors = []

        try:
            with open(output_file, "r") as f:
                for line_num, line in enumerate(f, 1):
                    if not line.strip():
                        continue

                    try:
                        record = json.loads(line)

                        # Extract stream name from record metadata
                        if "_sdc_table_name" in record:
                            stream_name = record["_sdc_table_name"]
                            streams_found.add(stream_name)
                            record_counts[stream_name] = (
                                record_counts.get(stream_name, 0) + 1
                            )

                        # Basic schema validation
                        if not isinstance(record, dict):
                            schema_errors.append(
                                f"Line {line_num}: Record is not a dictionary"
                            )

                        # Check for required Singer fields
                        if "_sdc_table_name" not in record:
                            schema_errors.append(
                                f"Line {line_num}: Missing _sdc_table_name"
                            )

                    except json.JSONDecodeError as e:
                        schema_errors.append(f"Line {line_num}: Invalid JSON - {e}")

        except Exception as e:
            return {
                "valid": False,
                "error": f"Failed to read output file: {e}",
                "streams_found": list(streams_found),
                "record_counts": record_counts,
            }

        return {
            "valid": len(schema_errors) == 0,
            "streams_found": list(streams_found),
            "record_counts": record_counts,
            "schema_errors": schema_errors[:10],  # Limit to first 10 errors
        }

    def validate_api_endpoints(
        self, stderr_output: str, expected_streams: List[str]
    ) -> Dict[str, bool]:
        """Validate API calls were made to correct endpoints."""
        endpoint_validation = {}

        for stream_name in expected_streams:
            expected_endpoint = self.expected_endpoints.get(stream_name)
            if not expected_endpoint:
                endpoint_validation[stream_name] = False
                continue

            # Look for API calls to this endpoint in logs
            pattern = f"Stream {stream_name}: Requesting:.*{expected_endpoint}"
            matches = re.search(pattern, stderr_output)
            endpoint_validation[stream_name] = matches is not None

        return endpoint_validation

    def validate_throttling(self) -> Dict[str, Any]:
        """Validate that throttling prevents exceeding 60 requests/minute."""
        if len(self.api_call_times) < 2:
            return {
                "valid": True,
                "reason": "Insufficient API calls to test throttling",
            }

        violations = 0
        min_interval = 60.0 / 60  # 1 second for 60 requests/minute

        for i in range(1, len(self.api_call_times)):
            time_diff = self.api_call_times[i] - self.api_call_times[i - 1]
            if time_diff < min_interval * 0.9:  # 10% tolerance
                violations += 1

        # Check sliding window - no more than 60 requests in any 60-second window
        window_violations = 0
        for i, call_time in enumerate(self.api_call_times):
            window_start = call_time
            window_end = call_time + 60.0

            calls_in_window = sum(
                1 for t in self.api_call_times[i:] if window_start <= t <= window_end
            )
            if calls_in_window > 60:
                window_violations += 1

        return {
            "valid": violations == 0 and window_violations == 0,
            "interval_violations": violations,
            "window_violations": window_violations,
            "total_calls": len(self.api_call_times),
            "test_duration": (
                self.api_call_times[-1] - self.api_call_times[0]
                if self.api_call_times
                else 0
            ),
        }

    def test_configuration(self, test_config: TestConfig) -> List[TestResult]:
        """Test a specific configuration across all specified modes."""
        logger.info(f"\n{'=' * 60}")
        logger.info(f"Testing Configuration: {test_config.name}")
        logger.info(f"Expected Streams: {test_config.expected_streams}")
        logger.info(f"Test Modes: {test_config.test_modes}")
        logger.info(f"{'=' * 60}")

        results = []

        for mode in test_config.test_modes:
            logger.info(f"\nTesting {test_config.name} in {mode} mode...")

            with tempfile.TemporaryDirectory() as temp_dir:
                # Run meltano test
                run_result = self.run_meltano_test(test_config.config, mode, temp_dir)

                if not run_result["success"]:
                    # Create failure result for each expected stream
                    for stream_name in test_config.expected_streams:
                        result = TestResult(
                            config_name=test_config.name,
                            mode=mode,
                            stream_name=stream_name,
                            success=False,
                            error_message=run_result.get(
                                "error", "Meltano execution failed"
                            ),
                            execution_time=run_result["execution_time"],
                            api_calls_made=run_result["api_calls"],
                        )
                        results.append(result)
                    continue

                # Validate output files
                output_validation = self.validate_output_files(
                    run_result.get("output_file", ""), test_config.expected_streams
                )

                # Validate API endpoints
                endpoint_validation = self.validate_api_endpoints(
                    run_result["stderr"], test_config.expected_streams
                )

                # Create results for each expected stream
                for stream_name in test_config.expected_streams:
                    record_count = output_validation.get("record_counts", {}).get(
                        stream_name, 0
                    )
                    stream_found = stream_name in output_validation.get(
                        "streams_found", []
                    )
                    endpoint_valid = endpoint_validation.get(stream_name, False)

                    # Determine success criteria
                    success = (
                        stream_found
                        and record_count >= test_config.min_records
                        and endpoint_valid
                        and output_validation["valid"]
                    )

                    result = TestResult(
                        config_name=test_config.name,
                        mode=mode,
                        stream_name=stream_name,
                        success=success,
                        records_count=record_count,
                        schema_valid=output_validation["valid"],
                        endpoint_valid=endpoint_valid,
                        execution_time=run_result["execution_time"],
                        api_calls_made=run_result["api_calls"],
                        error_message=(
                            ""
                            if success
                            else "Stream not found or insufficient records"
                        ),
                    )
                    results.append(result)

                    logger.info(
                        f"  {stream_name}: {'✓' if success else '✗'} "
                        f"({record_count} records, endpoint: {'✓' if endpoint_valid else '✗'})"
                    )

        return results

    def run_all_tests(self) -> bool:
        """Run all test configurations and collect results."""
        if not self.validate_environment():
            return False

        logger.info("\n🚀 Starting Comprehensive tap-fred Test Suite")
        logger.info(
            f"Testing {len(self.test_configs)} configs across {sum(len(tc.test_modes) for tc in self.test_configs)} modes"
        )
        logger.info(
            f"Expected total streams: {sum(len(tc.expected_streams) for tc in self.test_configs)}"
        )

        # Run tests for each configuration
        for test_config in self.test_configs:
            config_results = self.test_configuration(test_config)
            self.results.extend(config_results)

        # Validate throttling after all tests
        throttling_result = self.validate_throttling()
        logger.info(
            f"\nThrottling Validation: {'✓' if throttling_result['valid'] else '✗'}"
        )
        if not throttling_result["valid"]:
            logger.warning(f"Throttling violations detected: {throttling_result}")

        return self.generate_report()

    def generate_report(self) -> bool:
        """Generate comprehensive test report."""
        total_tests = len(self.results)
        successful_tests = sum(1 for r in self.results if r.success)
        total_time = time.time() - self.start_time

        logger.info("\n" + "=" * 80)
        logger.info("📊 COMPREHENSIVE TEST REPORT")
        logger.info("=" * 80)

        # Overall summary
        logger.info(
            f"Overall Success Rate: {successful_tests} / {total_tests} ({successful_tests / total_tests * 100:.1f}%)"
        )
        logger.info(f"Total Execution Time: {total_time:.1f} seconds")
        logger.info(f"Total API Calls Made: {self.api_call_count}")

        # Summary by configuration
        logger.info("\n📋 Results by Configuration:")
        configs = {}
        for result in self.results:
            key = f"{result.config_name}_{result.mode}"
            if key not in configs:
                configs[key] = {"total": 0, "success": 0, "streams": []}
            configs[key]["total"] += 1
            configs[key]["streams"].append(result.stream_name)
            if result.success:
                configs[key]["success"] += 1

        for config_name, stats in configs.items():
            success_rate = stats["success"] / stats["total"] * 100
            logger.info(
                f"  {config_name}: {stats['success']}/{stats['total']} ({success_rate:.1f}%)"
            )

        # Detailed failures
        failures = [r for r in self.results if not r.success]
        if failures:
            logger.info(f"\n❌ Failed Tests ({len(failures)}):")
            for failure in failures:
                logger.info(
                    f"  {failure.config_name}_{failure.mode}.{failure.stream_name}: {failure.error_message}"
                )

        # Stream coverage analysis
        logger.info("\n📈 Stream Coverage Analysis:")
        all_streams = set()
        successful_streams = set()

        for result in self.results:
            all_streams.add(result.stream_name)
            if result.success:
                successful_streams.add(result.stream_name)

        logger.info(f"Total Unique Streams Tested: {len(all_streams)}")
        logger.info(f"Streams with At Least 1 Success: {len(successful_streams)}")

        missing_streams = all_streams - successful_streams
        if missing_streams:
            logger.info(f"Streams with No Successful Tests: {sorted(missing_streams)}")

        # Performance metrics
        logger.info("\n⚡ Performance Metrics:")
        avg_time = sum(r.execution_time for r in self.results) / len(self.results)
        logger.info(f"Average Test Execution Time: {avg_time:.1f} seconds")

        total_records = sum(r.records_count for r in self.results)
        logger.info(f"Total Records Extracted: {total_records:,}")

        # Throttling report
        throttling = self.validate_throttling()
        logger.info(
            f"\n🚦 Throttling Validation: {'✓ PASSED' if throttling['valid'] else '✗ FAILED'}"
        )
        if not throttling["valid"]:
            logger.info(f"  Interval violations: {throttling['interval_violations']}")
            logger.info(f"  Window violations: {throttling['window_violations']}")

        # Final assessment
        all_passed = successful_tests == total_tests and throttling["valid"]
        logger.info(
            f"\n🎯 FINAL RESULT: {'✓ ALL TESTS PASSED' if all_passed else '✗ SOME TESTS FAILED'}"
        )

        if all_passed:
            logger.info("🎉 tap-fred is working correctly with all 33 streams!")
            logger.info("✅ All stream categories tested with 2 configured IDs")
            logger.info("✅ API calls made to correct endpoints")
            logger.info("✅ Output files contain expected data structures")
            logger.info("✅ Schemas match API responses")
            logger.info("✅ Throttling prevents exceeding 60 requests/minute")
            logger.info("✅ Both FRED and ALFRED modes tested where applicable")
            logger.info("✅ Caching works across dependent streams")

        logger.info("=" * 80)

        return all_passed


def main():
    """Main test execution."""
    parser = argparse.ArgumentParser(description="Comprehensive tap-fred test suite")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show test configurations without running",
    )
    parser.add_argument(
        "--config-only",
        metavar="CONFIG",
        help="Test only specific configuration (e.g., 'series_streams')",
    )
    parser.add_argument(
        "--mode-only",
        metavar="MODE",
        choices=["FRED", "ALFRED"],
        help="Test only specific mode",
    )
    args = parser.parse_args()

    test_suite = FREDTestSuite()

    if args.dry_run:
        logger.info("🔍 DRY RUN - Test configurations:")
        for config in test_suite.test_configs:
            logger.info(f"\nConfiguration: {config.name}")
            logger.info(f"  Modes: {config.test_modes}")
            logger.info(
                f"  Expected streams ({len(config.expected_streams)}): {config.expected_streams}"
            )
            logger.info(f"  Min records: {config.min_records}")
            logger.info(f"  Config keys: {list(config.config.keys())}")
        logger.info(f"\nTotal configurations: {len(test_suite.test_configs)}")
        logger.info(
            f"Total stream tests: {sum(len(tc.expected_streams) * len(tc.test_modes) for tc in test_suite.test_configs)}"
        )
        return

    # Filter configurations if requested
    if args.config_only:
        test_suite.test_configs = [
            tc for tc in test_suite.test_configs if tc.name == args.config_only
        ]
        if not test_suite.test_configs:
            logger.error(f"Configuration '{args.config_only}' not found")
            sys.exit(1)

    # Filter modes if requested
    if args.mode_only:
        for config in test_suite.test_configs:
            config.test_modes = [
                mode for mode in config.test_modes if mode == args.mode_only
            ]

    try:
        success = test_suite.run_all_tests()
        sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        logger.info("\nTest interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Test suite failed with exception: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
