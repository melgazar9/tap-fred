"""Tests for the per-call jitter/buffer before every FRED API call.

FRED is strict on rate limits, so both call paths must apply a randomized buffer
before EVERY request (per key):
  * discovery.py  -> Throttle.wait() (used by _fred_get on releases/categories/series)
  * client.py     -> FREDStream._throttle() (used by the streams: observations, vintage, ...)

These run fully offline (urlopen / time.sleep / random mocked).
"""

from __future__ import annotations

import urllib.error
from unittest import mock

import pytest
import requests

from tap_fred import discovery
from tap_fred.discovery import Throttle
from tap_fred.tap import TapFRED


class TestDiscoveryThrottle:
    def test_jitter_applied_before_every_call(self):
        """Every wait() sleeps a jitter buffer; the min interval is added only between calls."""
        throttle = Throttle(min_interval=0.5, jitter_seconds=0.2)
        with (
            mock.patch.object(discovery.random, "uniform", return_value=0.13) as rnd,
            mock.patch.object(discovery.time, "sleep") as sleep,
        ):
            throttle.wait()  # first call: jitter only, no min_interval
            throttle.wait()  # second call: min_interval + jitter
            throttle.wait()  # third call: min_interval + jitter

        # jitter requested on every call, always over [0, jitter_seconds]
        assert rnd.call_count == 3
        assert all(c.args == (0.0, 0.2) for c in rnd.call_args_list)
        # first sleeps just jitter; later calls add the min interval
        assert [round(c.args[0], 6) for c in sleep.call_args_list] == [0.13, 0.63, 0.63]
        assert throttle.calls == 3

    def test_zero_jitter_still_paces_between_calls(self):
        """jitter_seconds=0 -> no buffer before the first call, min interval between later calls."""
        throttle = Throttle(min_interval=0.5, jitter_seconds=0.0)
        with mock.patch.object(discovery.time, "sleep") as sleep:
            throttle.wait()  # first call: nothing
            throttle.wait()  # second call: min_interval only
        assert [c.args[0] for c in sleep.call_args_list] == [0.5]

    def test_fred_get_buffers_before_each_request(self):
        """_fred_get must pace via the throttle before hitting urlopen, on every call."""
        throttle = Throttle(min_interval=0.5, jitter_seconds=0.2)
        resp = mock.MagicMock()
        resp.read.return_value = b'{"ok": 1}'
        resp.__enter__.return_value = resp
        order = []
        with (
            mock.patch.object(discovery.random, "uniform", return_value=0.1),
            mock.patch.object(
                discovery.time,
                "sleep",
                side_effect=lambda s: order.append(("sleep", round(s, 6))),
            ),
            mock.patch.object(
                discovery.urllib.request,
                "urlopen",
                side_effect=lambda *a, **k: order.append(("call", None)) or resp,
            ),
        ):
            discovery._fred_get("https://api", "releases", {}, "key", throttle)
            discovery._fred_get("https://api", "releases", {}, "key", throttle)

        # a sleep precedes each urlopen, and the buffer grows once the min interval kicks in
        assert order == [("sleep", 0.1), ("call", None), ("sleep", 0.6), ("call", None)]


class TestFredGetThrottleRetry:
    """FRED rate-limits with 403 as well as 429 — _fred_get must back off and retry both."""

    @staticmethod
    def _ok_resp():
        resp = mock.MagicMock()
        resp.read.return_value = b'{"ok": 1}'
        resp.__enter__.return_value = resp
        return resp

    @staticmethod
    def _http_error(code):
        return urllib.error.HTTPError("https://api/x", code, "err", {}, None)

    def test_403_retries_then_succeeds(self):
        throttle = Throttle(min_interval=0.0, jitter_seconds=0.0)
        seq = [self._http_error(403), self._http_error(403), self._ok_resp()]
        with (
            mock.patch.object(discovery.time, "sleep") as sleep,
            mock.patch.object(discovery.urllib.request, "urlopen", side_effect=seq),
        ):
            out = discovery._fred_get("https://api", "releases", {}, "key", throttle)
        assert out == {"ok": 1}
        assert sleep.call_count == 2  # two backoffs before the success

    def test_persistent_403_raises_after_max_attempts(self):
        throttle = Throttle(min_interval=0.0, jitter_seconds=0.0)
        with (
            mock.patch.object(discovery.time, "sleep") as sleep,
            mock.patch.object(
                discovery.urllib.request, "urlopen", side_effect=self._http_error(403)
            ),
        ):
            with pytest.raises(urllib.error.HTTPError) as exc:
                discovery._fred_get(
                    "https://api", "releases", {}, "key", throttle, max_attempts=3
                )
        assert exc.value.code == 403
        assert sleep.call_count == 2  # backs off on attempts 0 and 1, raises on the 3rd

    def test_non_throttle_4xx_raises_immediately(self):
        throttle = Throttle(min_interval=0.0, jitter_seconds=0.0)
        with (
            mock.patch.object(discovery.time, "sleep") as sleep,
            mock.patch.object(
                discovery.urllib.request, "urlopen", side_effect=self._http_error(400)
            ),
        ):
            with pytest.raises(urllib.error.HTTPError):
                discovery._fred_get("https://api", "releases", {}, "key", throttle)
        assert sleep.call_count == 0  # a 400 is not retried


class TestStreamThrottleJitter:
    @staticmethod
    def _stream(jitter, min_throttle=0.0, rpm=100000):
        cfg = {
            "api_key": "dummy",
            "api_url": "https://api.stlouisfed.org/fred",
            "max_requests_per_minute": rpm,
            "min_throttle_seconds": min_throttle,
            "throttle_jitter_seconds": jitter,
            "series_ids": ["GDP"],
        }
        tap = TapFRED(config=cfg, validate_config=False)
        return tap.streams["series_observations"]

    def test_unconditional_jitter_before_every_call(self):
        """With min interval/rate cap inert, the jitter buffer must still fire on EVERY call."""
        stream = self._stream(jitter=0.5)
        import tap_fred.client as client_mod

        with (
            mock.patch.object(client_mod.random, "uniform", return_value=0.27) as rnd,
            mock.patch.object(client_mod.time, "sleep") as sleep,
        ):
            stream._throttle()
            stream._throttle()
            stream._throttle()

        # exactly one jitter sleep per call (min-interval/rate-cap paths are inert here)
        assert sleep.call_count == 3
        assert all(c.args[0] == 0.27 for c in sleep.call_args_list)
        assert all(c.args == (0, 0.5) for c in rnd.call_args_list)

    def test_no_jitter_when_disabled(self):
        """throttle_jitter_seconds=0 -> no jitter sleep (paths inert => no sleeps at all)."""
        stream = self._stream(jitter=0.0)
        import tap_fred.client as client_mod

        with mock.patch.object(client_mod.time, "sleep") as sleep:
            stream._throttle()
            stream._throttle()
        assert sleep.call_count == 0


class TestStreamRetriesOn403:
    """FRED throttles the streams with 403 (not just 429); the requests-based client must
    retry it like a rate-limit, not treat it as a fatal/skip error."""

    @staticmethod
    def _stream():
        cfg = {
            "api_key": "dummy",
            "api_url": "https://api.stlouisfed.org/fred",
            "max_requests_per_minute": 100000,
            "min_throttle_seconds": 0.0,
            "throttle_jitter_seconds": 0.0,
            "series_ids": ["GDP"],
            "strict_mode": True,
        }
        return TapFRED(config=cfg, validate_config=False).streams["series_observations"]

    @staticmethod
    def _resp(status, json_data=None):
        resp = mock.MagicMock()
        resp.status_code = status
        resp.reason = "Forbidden"
        resp.text = ""
        if status >= 400:
            resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
                response=resp
            )
        else:
            resp.raise_for_status.return_value = None
            resp.json.return_value = json_data or {}
        return resp

    def test_403_retried_then_succeeds(self):
        stream = self._stream()
        seq = [self._resp(403), self._resp(403), self._resp(200, {"observations": []})]
        with (
            mock.patch("time.sleep"),
            mock.patch.object(stream.requests_session, "get", side_effect=seq) as get,
        ):
            out = stream._make_request("https://api/x", {"api_key": "dummy"})
        assert out == {"observations": []}
        assert get.call_count == 3  # two 403s ridden through, then success

    def test_400_not_retried(self):
        stream = self._stream()
        with (
            mock.patch("time.sleep"),
            mock.patch.object(
                stream.requests_session,
                "get",
                side_effect=lambda *a, **k: self._resp(400),
            ) as get,
        ):
            with pytest.raises(requests.exceptions.HTTPError):
                stream._make_request("https://api/x", {"api_key": "dummy"})
        assert get.call_count == 1  # a genuine 400 is fatal, no retry
