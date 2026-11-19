"""Readiness probes for trigger-gated jobs (roadmap #10).

A queued job can wait on a DAG parent (existing ``trigger``) and/or a readiness
probe: a TCP port accepting connections, an HTTP status, a captured log line, or
a file appearing. Probes run on the daemon host, are pure (no state), and are
evaluated by the single queued-job dispatcher.

Kept dependency-free (``socket`` / ``urllib`` / ``os``); the server owns
persistence and throttling.
"""

from __future__ import annotations

import functools
import os
import socket
import urllib.error
from typing import Any

PROBE_TYPES = {"port", "http", "log_line", "file"}
_PROBE_COMMON_FIELDS = {"timeout_seconds", "interval_seconds"}
_DEFAULT_EXPECT_STATUS = 200
_PORT_TIMEOUT_SECONDS = 0.3
_HTTP_TIMEOUT_SECONDS = 1.0

# Readiness probes connect DIRECTLY: a system/registry proxy would otherwise
# route a localhost check (or any internal endpoint) through an unexpected hop.
@functools.cache
def _http_opener() -> Any:
    # Lazy: urllib.request (~30ms) and build_opener's SSL context (~20ms)
    # would otherwise be paid by every process that imports vanth.server.
    from urllib.request import ProxyHandler, build_opener

    return build_opener(ProxyHandler({}))


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field} must be an integer >= 1")
    return value


def validate_probe(probe: Any) -> dict[str, Any]:
    """Validate and normalize a probe object; raise ValueError on bad input."""
    if not isinstance(probe, dict):
        raise ValueError("trigger.probe must be an object")
    kind = probe.get("type")
    if kind not in PROBE_TYPES:
        raise ValueError(f"probe.type must be one of {sorted(PROBE_TYPES)}")
    out: dict[str, Any] = {"type": kind}
    if kind == "port":
        host = probe.get("host", "127.0.0.1")
        port = probe.get("port")
        if not isinstance(host, str) or not host:
            raise ValueError("port probe requires a non-empty host (or omit it for 127.0.0.1)")
        if isinstance(port, bool) or not isinstance(port, int) or not (1 <= port <= 65535):
            raise ValueError("port probe requires port in 1..65535")
        out.update(host=host, port=port)
        allowed = {"type", "host", "port"}
    elif kind == "http":
        url = probe.get("url")
        expect = probe.get("expect_status", _DEFAULT_EXPECT_STATUS)
        if not isinstance(url, str) or not url.startswith(("http://", "https://")):
            raise ValueError("http probe requires an http(s) url")
        if isinstance(expect, bool) or not isinstance(expect, int) or not (100 <= expect <= 599):
            raise ValueError("http probe expect_status must be in 100..599")
        out.update(url=url, expect_status=expect)
        allowed = {"type", "url", "expect_status"}
    elif kind == "log_line":
        job_id = probe.get("job_id")
        pattern = probe.get("pattern")
        stream = probe.get("stream", "all")
        if not isinstance(job_id, str) or not job_id:
            raise ValueError("log_line probe requires a target job_id")
        if not isinstance(pattern, str) or not pattern:
            raise ValueError("log_line probe requires a non-empty pattern")
        if stream not in {"stdout", "stderr", "all"}:
            raise ValueError("log_line probe stream must be stdout, stderr, or all")
        out.update(job_id=job_id, pattern=pattern, stream=stream)
        allowed = {"type", "job_id", "pattern", "stream"}
    else:  # file
        path = probe.get("path")
        if not isinstance(path, str) or not path:
            raise ValueError("file probe requires a non-empty path")
        out.update(path=path)
        allowed = {"type", "path"}
    unknown = set(probe) - allowed - _PROBE_COMMON_FIELDS
    if unknown:
        raise ValueError(f"unknown probe fields: {sorted(unknown)}")
    for key in _PROBE_COMMON_FIELDS:
        if key in probe:
            out[key] = _positive_int(probe[key], f"probe.{key}")
    return out


def evaluate_probe(probe: dict[str, Any], *, log_text: str | None = None) -> bool:
    """Return True when the probe's condition is currently satisfied.

    ``log_text`` is the captured log tail supplied by the caller for a
    ``log_line`` probe (this module never reads job state).
    """
    kind = probe["type"]
    if kind == "port":
        try:
            with socket.create_connection((probe.get("host", "127.0.0.1"), probe["port"]), timeout=_PORT_TIMEOUT_SECONDS):
                return True
        except OSError:
            return False
    if kind == "http":
        from .outbound import OutboundDenied, check_outbound_url

        try:
            check_outbound_url(probe["url"])
        except OutboundDenied:
            return False
        from urllib.request import Request

        request = Request(probe["url"], method="GET")
        expected = int(probe.get("expect_status", _DEFAULT_EXPECT_STATUS))
        try:
            with _http_opener().open(request, timeout=_HTTP_TIMEOUT_SECONDS) as response:
                return int(response.status) == expected
        except urllib.error.HTTPError as exc:
            return int(exc.code) == expected
        except Exception:
            return False
    if kind == "file":
        return os.path.exists(probe["path"])
    if kind == "log_line":
        return probe["pattern"] in (log_text or "")
    return False
