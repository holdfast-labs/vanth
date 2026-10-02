from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import math
import os
import re
import secrets
import shutil
import struct
import subprocess
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from logging.handlers import RotatingFileHandler

# mcp's FastMCP has a Settings model with a `lifespan` field whose annotation
# contains an unresolved forward reference; pydantic-settings >=2.15 warns about
# it on import and on every console-script invocation (including the ops CLI,
# which never touches MCP). The warning is upstream noise — suppress it so a
# fresh install doesn't print a scary traceback-shaped message.
try:
    from pydantic_settings.exceptions import IncompleteFieldDefinitionWarning as _IncompleteFieldWarning

    warnings.filterwarnings("ignore", category=_IncompleteFieldWarning)
except Exception:
    pass

from mcp.server.fastmcp import FastMCP

from .client import VanthClient
from .codex_bridge import CodexActiveWriterError, send_delivery_to_codex
from .migrations import LATEST_SCHEMA_VERSION, configure_connection, migrate
from .opencode_bridge import OpenCodeSessionNotFound, send_delivery_to_opencode
from .outbound import OutboundDenied, check_outbound_url
from .paths import canonical_home
from .probes import evaluate_probe, validate_probe
from .runtime_info import capture_run_metadata, serialize_run_metadata
from .schedules import compute_next_fire, next_cron_fires, validate_schedule_spec, validate_timezone

EVENT_PREFIX = "AGENT_EVENT "
DEFAULT_MAX_EVENT_BYTES = 65536
DEFAULT_MAX_EVENT_LINE_BYTES = 1024 * 1024
DEFAULT_MAX_LOG_BYTES = 10 * 1024 * 1024

# Sentinel distinguishing "no worker-pid guard requested" from "the observed
# worker pid is NULL" in identity-guarded writes (review rc36 P1). Binding
# None asserts `worker_pid IS NULL`; a stale snapshot that saw a NULL worker
# must NOT fall through to an unguarded CAS, or a runner that published a live
# pid after the snapshot could be orphaned.
_UNSET = object()
DEFAULT_MAX_ERROR_BYTES = 4096
TERMINAL_STATUSES = {"completed", "failed", "timeout", "cancelled", "orphaned"}
# last_error prefix for wakes the dispatch loop abandoned because no relay ever
# connected (see ``expire_stale_deliveries``); treated as a dead letter signal.
EXPIRED_DELIVERY_ERROR = "expired:"
# Upper bound for the relay-poll liveness-write interval, kept well under the
# relay stale (90s) and subscription TTL (300s) windows.
_MAX_RELAY_POLL_HEARTBEAT = 30.0


def _env_flag_default_on(name: str) -> bool:
    """True unless the env var is set to an explicit falsy value."""
    value = os.environ.get(name, "").strip().lower()
    if value == "":
        return True
    return value not in {"0", "false", "no", "off"}


def _same_directory(a: str | None, b: str | None) -> bool:
    if not a or not b:
        return False
    try:
        return os.path.normcase(os.path.abspath(a)) == os.path.normcase(os.path.abspath(b))
    except (OSError, ValueError):
        return False


def _default_wake_min_seconds() -> int:
    try:
        return max(0, int(os.environ.get("VANTH_DEFAULT_WAKE_MIN_SECONDS", "60")))
    except ValueError:
        return 60
# Who requested a stop, carried on the resulting terminal event (review #9).
# "tool" = an MCP tool call, "user" = the human CLI/API, "watchdog" =
# recovery/heartbeat reconciliation, "timeout" = the runner's timeout,
# "policy" = a policy reaction, "remote" = a remote dispatcher.
STOP_ACTORS = {"user", "tool", "watchdog", "timeout", "daemon", "policy", "remote"}
DEFAULT_STOP_ACTOR = "user"


def stop_event_data(row: Any, *, default_actor: str = "watchdog", default_reason: str | None = None) -> dict[str, Any]:
    """Build terminal-event ``data`` carrying who/why a run ended.

    Reads the persisted ``stop_actor``/``stop_reason`` from a job row (missing
    on pre-v14 rows) and falls back to the emitter's defaults, so every
    ``cancelled``/``orphaned``/``timeout`` event is attributable.
    """
    actor = reason = None
    try:
        actor = row["stop_actor"]
        reason = row["stop_reason"]
    except (KeyError, IndexError):
        pass
    return {"actor": actor or default_actor, "reason": reason or default_reason}


def mask_secrets(line: bytes, secrets: list[str] | None) -> bytes:
    """Replace declared-secret values with ``***`` in one captured log line.

    The ``::add-mask::`` pattern: a job names secret env vars, the runner
    resolves their values from the job's merged environment, and every value is
    scrubbed from stdout/stderr before it is written to a durable log file or
    parsed into a structured event. This helper masks a complete byte buffer;
    the stream reader additionally retains boundary bytes between pipe reads.
    """
    if not secrets:
        return line
    for value in secrets:
        if value:
            try:
                needle = value.encode("utf-8")
            except UnicodeEncodeError:
                continue
            if needle:
                line = line.replace(needle, b"***")
    return line


def validate_policy(policy: dict[str, Any] | None) -> dict[str, Any] | None:
    """Validate the per-job policy block (dead-man's switch + failure reactions).

    Shape:
      {
        "schedule": {"expected_interval_seconds": int>=1, "grace_period_seconds": int>=0},
        "on_failure": {"after_n": int>=1, "action": "alert"|"disable"|"run_job",
                       "job_id": str (only for run_job)},
        "restart": {"max_retries": int>=1, "backoff_seconds": int>=0,
                    "backoff_max_seconds": int>=0},
        "retention": {"events_seconds"|"metrics_seconds"|"deliveries_seconds": int>=1}
      }
    """
    if policy is None:
        return None
    if not isinstance(policy, dict):
        raise ValueError("policy must be an object")
    cleaned: dict[str, Any] = {}
    schedule = policy.get("schedule")
    if schedule is not None:
        if not isinstance(schedule, dict):
            raise ValueError("policy.schedule must be an object")
        interval = schedule.get("expected_interval_seconds")
        if isinstance(interval, bool) or not isinstance(interval, int) or interval < 1:
            raise ValueError("policy.schedule.expected_interval_seconds must be an integer >= 1")
        grace = schedule.get("grace_period_seconds", 0)
        if isinstance(grace, bool) or not isinstance(grace, int) or grace < 0:
            raise ValueError("policy.schedule.grace_period_seconds must be an integer >= 0")
        cleaned["schedule"] = {"expected_interval_seconds": interval, "grace_period_seconds": grace}
    on_failure = policy.get("on_failure")
    if on_failure is not None:
        if not isinstance(on_failure, dict):
            raise ValueError("policy.on_failure must be an object")
        after_n = on_failure.get("after_n")
        if isinstance(after_n, bool) or not isinstance(after_n, int) or after_n < 1:
            raise ValueError("policy.on_failure.after_n must be an integer >= 1")
        action = on_failure.get("action")
        # isinstance first: an unhashable value (list/dict) in a set membership
        # test raises TypeError and would surface as an internal 500.
        if not isinstance(action, str) or action not in {"alert", "disable", "run_job"}:
            raise ValueError("policy.on_failure.action must be one of: alert, disable, run_job")
        entry: dict[str, Any] = {"after_n": after_n, "action": action}
        if action == "run_job":
            job_id = on_failure.get("job_id")
            if not isinstance(job_id, str) or not job_id:
                raise ValueError("policy.on_failure.job_id is required when action is run_job")
            entry["job_id"] = job_id
        cleaned["on_failure"] = entry
    restart = policy.get("restart")
    if restart is not None:
        if not isinstance(restart, dict):
            raise ValueError("policy.restart must be an object")
        max_retries = restart.get("max_retries")
        if isinstance(max_retries, bool) or not isinstance(max_retries, int) or max_retries < 1:
            raise ValueError("policy.restart.max_retries must be an integer >= 1")
        backoff = restart.get("backoff_seconds", 0)
        if isinstance(backoff, bool) or not isinstance(backoff, int) or backoff < 0:
            raise ValueError("policy.restart.backoff_seconds must be an integer >= 0")
        backoff_max = restart.get("backoff_max_seconds", 0)
        if isinstance(backoff_max, bool) or not isinstance(backoff_max, int) or backoff_max < 0:
            raise ValueError("policy.restart.backoff_max_seconds must be an integer >= 0")
        cleaned["restart"] = {
            "max_retries": max_retries,
            "backoff_seconds": backoff,
            "backoff_max_seconds": backoff_max,
        }
    retention = policy.get("retention")
    if retention is not None:
        if not isinstance(retention, dict):
            raise ValueError("policy.retention must be an object")
        cleaned_retention: dict[str, int] = {}
        for stream, key in (("events_seconds", "events"), ("metrics_seconds", "metrics"), ("deliveries_seconds", "deliveries")):
            value = retention.get(stream)
            if value is None:
                continue
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"policy.retention.{stream} must be an integer >= 1")
            cleaned_retention[key] = value
        if not cleaned_retention:
            raise ValueError("policy.retention must set at least one of events_seconds, metrics_seconds, deliveries_seconds")
        cleaned["retention"] = cleaned_retention
    if not cleaned:
        raise ValueError("policy must contain a schedule or on_failure block")
    return cleaned


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _ms_to_iso(ms: int) -> str:
    """Convert epoch milliseconds to the RFC3339 text Vanth stores."""
    try:
        ms = int(ms)
    except (TypeError, ValueError):
        raise ValueError("timestamp must be epoch milliseconds (int)") from None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _downsample(points: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    """Uniformly reduce a series to at most ``limit`` points.

    When the series is larger than ``limit``, points are sampled by index to
    keep an even spread across the whole series (same idea as the Go
    monitor's downsample). The first and last points are always retained.
    """
    n = len(points)
    if n <= limit or limit <= 0:
        return points
    if limit == 1:
        return [points[0]]
    indices = sorted(set(int(round(i * (n - 1) / (limit - 1))) for i in range(limit)))
    return [points[i] for i in indices]


def _parse_iso(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _runtime_seconds(started_at: str | None, ended_at: str | None) -> float | None:
    start = _parse_iso(started_at) if started_at else None
    if start is None:
        return None
    end = _parse_iso(ended_at) if ended_at else datetime.now(timezone.utc)
    seconds = (end - start).total_seconds()
    return max(0.0, seconds)


def _elapsed_seconds(start: str | None, end: str | None) -> float | None:
    if not start or not end:
        return None
    start_dt = _parse_iso(start)
    end_dt = _parse_iso(end)
    if start_dt is None or end_dt is None:
        return None
    seconds = (end_dt - start_dt).total_seconds()
    return seconds if seconds >= 0 else None


def _percentile(values: list[float], p: float) -> float | None:
    """Linear-interpolated percentile (p in [0, 1]); None for an empty list."""
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = p * (len(ordered) - 1)
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (rank - low)


def _round3(value: float | None) -> float | None:
    return round(value, 3) if value is not None else None


def _duration_trend(ordered: list[float]) -> dict[str, Any]:
    """Compare the p50 runtime of a job's older half against its newer half.

    Catches a backup that "crept 40min -> 2h over 6 weeks": needs at least 6
    runs, then flags ``regressing``/``improving`` at a 1.5x/0.67x ratio.
    ``ordered`` must be chronological.
    """
    unknown = {"direction": "unknown", "factor": None, "recent_p50": None, "baseline_p50": None}
    if len(ordered) < 6:
        return unknown
    half = len(ordered) // 2
    baseline = _percentile(ordered[:half], 0.5)
    recent = _percentile(ordered[half:], 0.5)
    if baseline is None or recent is None or baseline <= 0:
        return unknown
    factor = recent / baseline
    if factor >= 1.5:
        direction = "regressing"
    elif factor <= 0.67:
        direction = "improving"
    else:
        direction = "stable"
    return {
        "direction": direction,
        "factor": round(factor, 3),
        "recent_p50": _round3(recent),
        "baseline_p50": _round3(baseline),
    }


def parse_agent_event_line(line: str) -> dict[str, Any] | None:
    if not line.startswith(EVENT_PREFIX):
        return None
    try:
        payload = json.loads(line[len(EVENT_PREFIX) :])
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict) or not isinstance(payload.get("type"), str):
        return None
    if payload.get("message") is not None and not isinstance(payload["message"], str):
        return None
    if payload.get("level") is not None and not isinstance(payload["level"], str):
        return None
    return payload


def normalize_event_payload(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data")
    if not isinstance(data, dict):
        data = {}
    if payload["type"] == "progress":
        current = data.get("current")
        total = data.get("total")
        if "percent" not in data and isinstance(current, (int, float)) and isinstance(total, (int, float)):
            try:
                data["percent"] = round((current / total) * 100, 2) if total else 0
            except OverflowError:
                # Preserve the raw event when its derived chart value cannot
                # be represented as a float.
                pass
        if isinstance(data.get("percent"), (int, float)):
            data["percent"] = max(0, min(100, data["percent"]))
    return {
        "type": payload["type"],
        "level": payload.get("level", "info"),
        "message": payload.get("message"),
        "data": data,
    }


ATTENTION_EVENTS = {"needs_input", "permission_required", "blocked", "decision_requested"}
WAKE_TARGET_TYPES = {"local_command", "codex_cli_thread", "codex_thread", "codex_desktop", "opencode_thread", "webhook"}
# Wake target types delivered by a CLIENT-side relay long-polling ``/relay/poll``
# (the daemon never spawns a transport for these): Codex Desktop via the host
# pipe, OpenCode via its in-process plugin. ``_RELAY_IDENTITY_KEYS`` maps each to
# the payload field(s) holding the destination identity, because the relay SQL
# eligibility filter reads the identity straight out of ``payload_json``.
RELAY_CLIENT_TYPES = {"codex_desktop", "opencode_thread"}
RELAY_IDENTITY_KEYS = {
    "codex_desktop": ("thread_id", "threadId"),
    "opencode_thread": ("session_id", "sessionId"),
}
# The OpenCode plugin's long-poll identity (``vanth.ts``): ``opencode-<pid>-<rand>``.
# It is NOT a wake destination — an ``opencode_thread`` delivery is matched on the
# destination ``session_id`` (``ses_...``), so a target carrying a client id is
# never claimed and stays pending forever with no error. Callers repeatedly copy
# this from ``vanth doctor``, so it is rejected at target creation.
OPENCODE_CLIENT_ID_RE = re.compile(r"^opencode-\d+-[a-z0-9]+$")
# Durable approval/decision requests (roadmap Tier-1). A decision is its own
# small state machine keyed by ``decision_id``; the job row is untouched, so a
# job can keep running (or stay queued) while a human decides.
DECISION_PENDING = "pending"
DECISION_STATUSES = {DECISION_PENDING, "resolved", "withdrawn", "expired"}
DEFAULT_DECISION_OPTIONS = ["approve", "deny"]
# Authoritative decision transitions are exempt from the per-job structured
# event cap: the cap bounds telemetry, and dropping a decision event would
# break the durability contract (no wake, no waitable signal) with no way for
# a retry to repair it. The exemption is per-CALL (`exempt_from_cap`), NOT per
# event type: job stdout can emit any event type via AGENT_EVENT, so keying on
# the type would let a job forge `decision_requested` lines and bypass the cap.
# Bound decision input so the lifecycle event payload cannot be truncated by
# `max_event_bytes` (which would strip decision_id/choice and break wake
# delivery and `job_wait`). Human-paced, so the limits are generous.
MAX_DECISION_PROMPT_CHARS = 10000
MAX_DECISION_ACTOR_CHARS = 200
MAX_DECISION_OPTIONS = 50
MAX_DECISION_OPTION_CHARS = 200


class _DecisionNoOp(Exception):
    """Internal: a decision mutation found nothing to change (idempotent retry)."""


def resolve_wake_target_identity(
    targets: list[dict[str, Any]],
    origin_thread_id: str | None,
) -> list[dict[str, Any]]:
    """Copy caller-owned wake-target dicts and inject the inherited thread id.

    Shared by ``start`` and ``wake_now`` so both resolve target identity the
    same way (review P0-1b). ``origin_thread_id`` is resolved in the MCP process
    that owns the calling task (never inferred inside the persistent daemon). A
    copy is returned — the caller's dicts are never mutated.

    ``codex_cli_thread``/``codex_thread``/``codex_desktop`` inherit the calling
    thread id as their thread id (the CLI app-server targets an unloaded CLI
    task; Desktop wake targets the visible Desktop task of the calling thread).
    ``opencode_thread`` does NOT auto-inherit: OpenCode does not inject
    ``OPENCODE_SESSION_ID`` into MCP subprocesses (review P1-1), so an inherited
    value would be wrong; callers must pass an explicit ``session_id`` until a
    client plugin supplies it.
    """
    copied = [dict(target) for target in targets]
    if origin_thread_id:
        for target in copied:
            target_type = target.get("type")
            if target_type in {"codex_cli_thread", "codex_thread", "codex_desktop"} and not (target.get("thread_id") or target.get("threadId")):
                target["thread_id"] = origin_thread_id
            # opencode_thread intentionally NOT auto-inherited (review P1-1).
    return copied


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """urllib handler that refuses 3xx redirects (webhook credential safety).

    urllib's default HTTPRedirectHandler follows redirects and re-sends the
    request headers, which would forward Authorization tokens to a different
    origin. Raising here surfaces the redirect as an HTTPError so webhook
    delivery fails instead of leaking credentials (review P1-4).
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(req.full_url, code, msg, headers, fp)


_NO_REDIRECT_OPENER = urllib.request.build_opener(_NoRedirectHandler)


def canonicalize_wake_target(target: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of a wake-target dict with its identity key canonicalized.

    Wake targets accept several aliases for the destination id (``threadId``,
    ``sessionId``, and — for ``opencode_thread``/codex types — each other's key),
    but every consumer reads exactly one: the relay eligibility SQL filters on
    ``$.target.session_id`` for ``opencode_thread`` and ``$.target.thread_id`` for
    ``codex_desktop``, and the bridges read the same. Canonicalizing ONCE at
    persistence guarantees a delivery registered with any alias is matchable; an
    alias left in place makes the delivery silently unclaimable. A copy is
    returned — the caller's dict is never mutated.
    """
    copied = dict(target)
    target_type = copied.get("type")
    # isinstance guards the set membership below: an unhashable type (e.g. a
    # list) would raise TypeError and surface as an internal 500.
    if isinstance(target_type, str) and target_type == "opencode_thread":
        identity = copied.get("session_id") or copied.get("sessionId") or copied.get("thread_id") or copied.get("threadId")
        if identity:
            copied["session_id"] = identity
        for alias in ("sessionId", "thread_id", "threadId"):
            copied.pop(alias, None)
    elif isinstance(target_type, str) and target_type in {"codex_desktop", "codex_thread", "codex_cli_thread"}:
        identity = copied.get("thread_id") or copied.get("threadId") or copied.get("session_id") or copied.get("sessionId")
        if identity:
            copied["thread_id"] = identity
        for alias in ("threadId", "session_id", "sessionId"):
            copied.pop(alias, None)
    else:
        if "threadId" in copied and "thread_id" not in copied:
            copied["thread_id"] = copied["threadId"]
        copied.pop("threadId", None)
    return copied


def validate_wake_targets(targets: list[dict[str, Any]] | None) -> None:
    if targets is None:
        return
    if not isinstance(targets, list):
        raise ValueError("wake_targets must be a list")
    for target in targets:
        if not isinstance(target, dict):
            raise ValueError("each wake target must be an object")
        target_type = target.get("type")
        # isinstance first: an unhashable value (list/dict) in a set membership
        # test raises TypeError and would surface as an internal 500.
        if not isinstance(target_type, str) or target_type not in WAKE_TARGET_TYPES:
            raise ValueError(f"unsupported wake target type: {target_type!r}")
        events = target.get("events", target.get("notify_on", []))
        if not isinstance(events, list) or not events or not all(isinstance(event, str) for event in events):
            # events=[] is rejected (review P2): an empty list would be
            # interpreted as a wildcard and wake on every event, contradicting
            # the documented non-empty requirement.
            raise ValueError("wake target events must be a non-empty list of strings")
        command = target.get("command")
        if command is not None and not (
            (isinstance(command, str) and command) or (isinstance(command, list) and command)
        ):
            raise ValueError("wake target command must be a non-empty string or argv list")
        if target_type == "local_command" and command is None:
            raise ValueError("local_command target requires command")
        if target_type == "webhook":
            url = target.get("url")
            if not isinstance(url, str) or not url:
                raise ValueError("webhook target requires url")
            try:
                scheme = urllib.parse.urlparse(url).scheme
            except ValueError as exc:
                raise ValueError("webhook target url must be a valid URL") from exc
            if scheme not in {"http", "https"}:
                raise ValueError("webhook target url must be an http(s) URL")
            try:
                check_outbound_url(url)
            except OutboundDenied as exc:
                raise ValueError(f"webhook target url is blocked by policy: {exc}") from exc
            headers = target.get("headers")
            if headers is not None and not isinstance(headers, dict):
                raise ValueError("webhook target headers must be an object")
            if headers is not None:
                for key, value in headers.items():
                    if not isinstance(key, str) or not isinstance(value, str):
                        raise ValueError("webhook target headers must be string key/value pairs")
        if target_type == "codex_desktop" and command is not None:
            # codex_desktop is relay-only: _dispatch_delivery returns before the
            # command branch, so a command is ignored and the identity is still
            # required. Without this, such a target is silently never claimed.
            identity = target.get("thread_id") or target.get("threadId") or target.get("session_id") or target.get("sessionId")
            if not isinstance(identity, str) or not identity:
                raise ValueError("codex_desktop target requires thread_id (it is delivered by the Desktop relay; command is ignored)")
        if target_type == "opencode_thread" and command is None:
            # OpenCode cannot auto-inherit a session id (review P1-1): the id is
            # never injected by the client, so a missing one must be rejected
            # here rather than silently defaulted to a wrong/absent value.
            # thread_id/threadId is accepted as a legacy alias for session_id.
            session_id = target.get("session_id") or target.get("sessionId") or target.get("thread_id") or target.get("threadId")
            if not isinstance(session_id, str) or not session_id:
                raise ValueError(
                    "opencode_thread target requires session_id — the OpenCode session id "
                    "(`ses_...`, from `opencode session list` or the destination shown by "
                    "`vanth doctor`), NOT the relay client id (`opencode-<pid>-<rand>`); "
                    "omit it to resolve the live plugin relay for the job's directory"
                )
            # ``attach`` is OPTIONAL: a plain TUI session exposes no server URL
            # (review P0-3), so without it the wake is delivered by the
            # in-process OpenCode plugin relay instead of an external
            # `opencode run --attach`. When set it must be a usable URL.
            attach = target.get("attach")
            if attach is not None and (not isinstance(attach, str) or not attach):
                raise ValueError("opencode_thread attach must be a non-empty string when set")
        elif target_type not in {"local_command", "webhook"} and command is None:
            thread_id = target.get("thread_id") or target.get("threadId") or target.get("session_id") or target.get("sessionId")
            if not isinstance(thread_id, str) or not thread_id:
                raise ValueError(f"{target_type} target requires thread_id")
        for key, minimum in (("timeout_seconds", 1), ("max_attempts", 1), ("retry_delay_seconds", 0)):
            if key in target and (not isinstance(target[key], int) or isinstance(target[key], bool) or target[key] < minimum):
                raise ValueError(f"wake target {key} must be an integer >= {minimum}")


#: Every identity field a relay-delivered target may carry: the canonical
#: ``RELAY_IDENTITY_KEYS`` names plus their camelCase aliases and the legacy
#: ``thread_id`` accepted for ``opencode_thread``.
_RELAY_IDENTITY_FIELDS = ("session_id", "sessionId", "thread_id", "threadId")


def wake_target_identity(target: dict[str, Any]) -> str | None:
    """The relay destination identity of a relay-delivered wake target.

    Returns ``None`` for a target the daemon does not deliver through a client
    relay — a non-relay type, or one carrying its own ``command``.
    """
    target_type = target.get("type")
    # isinstance first: an unhashable value (list/dict) in the dict-membership
    # test raises TypeError and would surface as an internal 500 (field-level
    # shape errors must stay a 400).
    if not isinstance(target_type, str) or target_type not in RELAY_IDENTITY_KEYS or target.get("command"):
        return None
    for field in _RELAY_IDENTITY_FIELDS:
        value = target.get(field)
        if isinstance(value, str) and value:
            return value
    return None


def is_relay_client_id(target_type: Any, identity: str, client_ids: set[str]) -> bool:
    """Whether ``identity`` is a relay CLIENT id, which is never a destination.

    The OpenCode plugin's long-poll identity is ``opencode-<pid>-<rand>`` and is
    matched by shape; any other registered client id is matched against the live
    subscription set.
    """
    return (
        target_type == "opencode_thread" and bool(OPENCODE_CLIENT_ID_RE.match(identity))
    ) or identity in client_ids


def validate_limit(value: int, name: str, maximum: int = 1000) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1 or value > maximum:
        raise ValueError(f"{name} must be an integer between 1 and {maximum}")
    return value


class JobManager:
    def __init__(self, home: str | Path | None = None, *, recover: bool = True) -> None:
        self.home = canonical_home(home)
        self.max_event_bytes = int(
            os.environ.get("VANTH_MAX_EVENT_BYTES")
            or os.environ.get("AGENT_BG_MAX_EVENT_BYTES")
            or DEFAULT_MAX_EVENT_BYTES
        )
        self.max_event_line_bytes = int(os.environ.get("VANTH_MAX_EVENT_LINE_BYTES", DEFAULT_MAX_EVENT_LINE_BYTES))
        self.max_log_bytes = int(os.environ.get("VANTH_MAX_LOG_BYTES", DEFAULT_MAX_LOG_BYTES))
        self.delivery_lease_margin = int(os.environ.get("VANTH_DELIVERY_LEASE_MARGIN", "5"))
        self.launch_claim_timeout = max(5, int(os.environ.get("VANTH_LAUNCH_CLAIM_TIMEOUT", "30")))
        self.heartbeat_interval = float(os.environ.get("VANTH_RUNNER_HEARTBEAT_INTERVAL", "1"))
        self.heartbeat_stale_after = float(os.environ.get("VANTH_RUNNER_HEARTBEAT_STALE_AFTER", "10"))
        self.recovery_kill_timeout = max(0, int(os.environ.get("VANTH_RECOVERY_KILL_TIMEOUT", "10")))
        self.logs = self.home / "logs"
        self.events_dir = self.home / "events"
        self.logs.mkdir(parents=True, exist_ok=True)
        self.events_dir.mkdir(parents=True, exist_ok=True)
        self.backup_path = None
        self.db = sqlite3.connect(self.home / "jobs.sqlite", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        configure_connection(self.db)
        self.db_lock = threading.RLock()
        self._closed = False
        self._close_lock = threading.Lock()
        self.processes: dict[str, subprocess.Popen[bytes]] = {}
        self.reader_threads: dict[str, list[threading.Thread]] = {}
        self.conditions: dict[str, threading.Condition] = {}
        self._log_truncated: set[tuple[str, str]] = set()
        self._capture_failed: set[str] = set()
        self.sqlite_contentions = 0
        # Retries of the event-capture persist, per job (drives write_contended
        # for that job); kept separate from sqlite_contentions so unrelated
        # _retry_locked callers (terminal transitions, deliveries, relay polls)
        # and other jobs cannot mis-attribute contention.
        self.event_contentions_by_job: dict[str, int] = {}
        self._contention_reported: set[str] = set()
        self._metric_ingest_keys: set[str] = set()
        self._delivery_threads: set[threading.Thread] = set()
        self._delivery_threads_lock = threading.Lock()
        self.max_delivery_concurrency = max(1, int(os.environ.get("VANTH_DELIVERY_MAX_CONCURRENT", "4")))
        self.max_running_jobs = max(0, int(os.environ.get("VANTH_MAX_RUNNING_JOBS", "0")))
        self.max_retention_seconds = max(0, int(os.environ.get("VANTH_RETENTION_SECONDS", "0")))
        self.retention_interval_seconds = max(0, int(os.environ.get("VANTH_RETENTION_INTERVAL_SECONDS", "3600")))
        self.retention_dry_run = os.environ.get("VANTH_RETENTION_DRY_RUN", "1") != "0"
        self._last_retention_run: float | None = None
        self.shutdown_requested = threading.Event()
        self.max_events_per_job = max(1, int(os.environ.get("VANTH_MAX_EVENTS_PER_JOB", "100000")))
        self._events_truncated: set[str] = set()
        # Readiness-probe throttle: job_id -> monotonic time of the last probe
        # attempt (roadmap #10). Pruned to the current queued set each dispatch.
        self._probe_last_attempt: dict[str, float] = {}
        # Max actual probe I/O calls per dispatch pass, so a batch of blocked
        # HTTP/port probes cannot stall the maintenance loop (deliveries,
        # recovery, schedules). Throttled jobs cost nothing, so successive
        # passes drain the whole queue.
        self.probe_budget = max(1, int(os.environ.get("VANTH_PROBE_BUDGET", "8")))
        self.logger = logging.getLogger(f"vanth.manager.{id(self)}")
        self.logger.setLevel(os.environ.get("VANTH_LOG_LEVEL", "INFO").upper())
        self.logger.propagate = False
        if not self.logger.handlers:
            handler = RotatingFileHandler(
                self.logs / "daemon.log",
                maxBytes=int(os.environ.get("VANTH_LOG_MAX_BYTES", str(5 * 1024 * 1024))),
                backupCount=int(os.environ.get("VANTH_LOG_BACKUP_COUNT", "3")),
                encoding="utf-8",
            )
            handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s pid=%(process)d component=manager %(message)s"))
            self.logger.addHandler(handler)
        self.dispatch_enabled = recover
        self.dispatcher_stop = threading.Event()
        self.dispatcher_thread: threading.Thread | None = None
        self.alert_thread: threading.Thread | None = None
        self._started_monotonic = time.monotonic()
        self._last_delivery_expiry = 0.0
        self._last_wal_checkpoint = 0.0
        # Operator alerts: edge-triggered condition state + throttle (review B3).
        self._alert_state: dict[str, bool] = {}
        self._last_alert_check: float | None = None
        self.backup_path = migrate(self.db, self.home)
        if recover:
            with self.db_lock:
                self._reconcile_invalid_wake_targets()
            self._recover_jobs()
            self._reconcile_running_jobs()
        if recover:
            self._dispatch_due_deliveries()
            self.dispatcher_thread = threading.Thread(target=self._dispatch_loop, daemon=True)
            self.dispatcher_thread.start()
            # Alerts run on their OWN thread: a slow alert POST (or DNS) must
            # never stall delivery dispatch, recovery, or schedule firing.
            if os.environ.get("VANTH_ALERT_WEBHOOK", "").strip():
                self.alert_thread = threading.Thread(target=self._alert_loop, name="vanth-alerts", daemon=True)
                self.alert_thread.start()

    def _pid_alive(self, pid: int | None) -> bool:
        if not pid:
            return False
        try:
            if sys.platform == "win32":
                result = subprocess.run(
                    ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    text=True,
                    timeout=2,
                )
                return str(pid) in result.stdout
            os.kill(pid, 0)
            return True
        except Exception:
            return False

    def _recover_jobs(self) -> None:
        rows = self.db.execute(
            "SELECT job_id, worker_pid, stop_requested_at, stop_actor, stop_reason, claim_token "
            "FROM jobs WHERE status='running'"
        ).fetchall()
        for row in rows:
            if self._pid_alive(row["worker_pid"]):
                continue
            job_id = row["job_id"]
            # Review rc34 P2: revalidate OWNERSHIP immediately before the kill.
            # The snapshot above is stale by the time we act; re-read the current
            # row and only proceed if it is still running under OUR run identity
            # (token for launch-path rows, worker_pid for legacy rows). This
            # couples the kill decision to a still-current identity so a newer
            # run that took ownership is never terminated.
            current = self._row(
                "SELECT status, pid, worker_pid, claim_token FROM jobs WHERE job_id=?", (job_id,)
            )
            if current is None or current["status"] != "running":
                continue
            if row["claim_token"]:
                if current["claim_token"] != row["claim_token"]:
                    continue
            elif current["worker_pid"] != row["worker_pid"]:
                continue
            workload_pid = current["pid"]
            if workload_pid:
                if not self._terminate_pid(int(workload_pid), force=True, deadline=time.monotonic() + self.recovery_kill_timeout):
                    # Keep the job running: the workload could not be killed and
                    # may still be alive; orphaning it would leave a live,
                    # untracked workload. A later pass retries.
                    self.logger.error("recovery could not terminate workload job_id=%s pid=%s", job_id, workload_pid)
                    continue
            terminal = "cancelled" if row["stop_requested_at"] else "orphaned"
            # The guarded terminal transition is run-IDENTITY guarded (rc33
            # P1-5); it returns 0 if a newer run took ownership between the
            # revalidation above and this write.
            self._terminal_event(
                job_id, terminal, claim_token=row["claim_token"],
                worker_pid=row["worker_pid"] if not row["claim_token"] else _UNSET,
                message="Job runner was not alive during recovery",
                data=stop_event_data(row, default_actor="watchdog", default_reason="runner not alive during recovery"),
            )

    def _reconcile_running_jobs(self) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=self.heartbeat_stale_after)
        cutoff_text = cutoff.isoformat().replace("+00:00", "Z")
        rows = self.db.execute(
            "SELECT job_id, worker_pid, pid, stop_requested_at, stop_actor, stop_reason, claim_token "
            "FROM jobs WHERE status='running' AND (runner_heartbeat_at IS NULL OR runner_heartbeat_at < ?)",
            (cutoff_text,),
        ).fetchall()
        for row in rows:
            if self._pid_alive(row["worker_pid"]):
                continue
            job_id = row["job_id"]
            # Review rc34 P2: revalidate ownership immediately before the kill
            # (see _recover_jobs) so a newer run that took ownership is never
            # terminated.
            current = self._row(
                "SELECT status, pid, worker_pid, claim_token FROM jobs WHERE job_id=?", (job_id,)
            )
            if current is None or current["status"] != "running":
                continue
            if row["claim_token"]:
                if current["claim_token"] != row["claim_token"]:
                    continue
            elif current["worker_pid"] != row["worker_pid"]:
                continue
            if current["pid"]:
                if not self._terminate_pid(int(current["pid"]), force=True, deadline=time.monotonic() + self.recovery_kill_timeout):
                    self.logger.error("heartbeat reconciliation could not terminate workload job_id=%s pid=%s", job_id, current["pid"])
                    continue
            terminal = "cancelled" if row["stop_requested_at"] else "orphaned"
            # Review rc33 P1-5: the terminal transition is run-IDENTITY guarded.
            # It only finalizes the row if it is STILL running under OUR recorded
            # worker/token, so an old reconciliation pass can never orphan a
            # newer run.
            self._terminal_event(
                job_id, terminal, claim_token=row["claim_token"],
                worker_pid=row["worker_pid"] if not row["claim_token"] else _UNSET,
                message="Runner heartbeat is stale and the runner is not alive",
                data=stop_event_data(row, default_actor="watchdog", default_reason="runner heartbeat stale"),
            )

    def begin_shutdown(self) -> None:
        self.shutdown_requested.set()
        for condition in self.conditions.values():
            with condition:
                condition.notify_all()

    def _transition_terminal(
        self,
        job_id: str,
        status: str,
        exit_code: int | None = None,
        *,
        claim_token: str | None = None,
        worker_pid: int | None | object = _UNSET,
        require_launching: bool = False,
        expected_worker_pid: int | None | object = _UNSET,
        transaction: bool = True,
    ) -> bool:
        if status not in TERMINAL_STATUSES:
            raise ValueError(f"invalid terminal status: {status}")
        self._ensure_open()
        stamp = now_iso()

        def transition() -> bool:
            with self.db_lock:
                if claim_token:
                    # Claim-OWNED transition (review rc32 P1-3): only the run
                    # holding this token may move the row to a terminal state.
                    # By default this guards both the 'running' (normal) and
                    # 'launching' (runner failed before publishing) states so a
                    # stale run can never clobber a newer launch, and a run that
                    # finished while the row was still 'launching' still records
                    # its terminal outcome (no more rejected transitions).
                    #
                    # require_launching (review rc33 P1-4): stale-claim recovery
                    # must NOT orphan a runner that already promoted to
                    # 'running'. Only the atomic launching-only transition
                    # establishes ownership; after it wins, processes are
                    # reconciled. If the runner promoted first, the guard returns
                    # 0 and the live run is left alone.
                    #
                    # expected_worker_pid (review rc36 P1): stale-claim recovery
                    # snapshots a dead/null worker_pid; requiring that observed
                    # worker identity in the CAS means a runner that became live
                    # AFTER the snapshot (worker_pid changed to a live value) is
                    # never orphaned. The sentinel default (vs explicit None)
                    # distinguishes "no worker guard requested" from "the
                    # snapshot saw worker_pid IS NULL" — an explicit None binds
                    # `worker_pid IS NULL` so a runner that published a live pid
                    # after a NULL-observing snapshot is never orphaned.
                    if require_launching:
                        status_guard = "status='launching'"
                    else:
                        status_guard = "status IN ('running','launching')"
                    if expected_worker_pid is not _UNSET:
                        status_guard += " AND worker_pid IS ?"
                        args: tuple[Any, ...] = (status, exit_code, stamp, stamp, job_id, claim_token, expected_worker_pid)
                    else:
                        args = (status, exit_code, stamp, stamp, job_id, claim_token)
                    changed = self.db.execute(
                        "UPDATE jobs SET status=?, exit_code=?, ended_at=?, updated_at=?, stop_requested_at=NULL "
                        f"WHERE job_id=? AND claim_token=? AND {status_guard}",
                        args,
                    ).rowcount
                elif require_launching:
                    guard = " AND worker_pid IS ?" if expected_worker_pid is not _UNSET else ""
                    args = (status, exit_code, stamp, stamp, job_id)
                    if expected_worker_pid is not _UNSET:
                        args += (expected_worker_pid,)
                    changed = self.db.execute(
                        "UPDATE jobs SET status=?, exit_code=?, ended_at=?, updated_at=?, stop_requested_at=NULL "
                        "WHERE job_id=? AND status='launching' AND claim_token IS NULL" + guard, args,
                    ).rowcount
                elif worker_pid is not _UNSET:
                    # No-token RUN-IDENTITY guarded transition (review rc33
                    # P1-5): only finalize a stale row if the recorded worker is
                    # still OUR process. A newer restart that took ownership has
                    # a different worker_pid (and status may now be running under
                    # a new run), so this guard cannot orphan a newer run.
                    # An explicit None (the recovery/reconcile snapshot observed
                    # worker_pid IS NULL) binds `worker_pid IS NULL` (review rc36
                    # P1): a runner that published a live pid after that NULL
                    # snapshot is never orphaned by an unguarded fall-through.
                    changed = self.db.execute(
                        "UPDATE jobs SET status=?, exit_code=?, ended_at=?, updated_at=?, stop_requested_at=NULL "
                        "WHERE job_id=? AND status='running' AND worker_pid IS ?",
                        (status, exit_code, stamp, stamp, job_id, worker_pid),
                    ).rowcount
                else:
                    changed = self.db.execute(
                        "UPDATE jobs SET status=?, exit_code=?, ended_at=?, updated_at=?, stop_requested_at=NULL WHERE job_id=? AND status='running'",
                        (status, exit_code, stamp, stamp, job_id),
                    ).rowcount
                if transaction:
                    self.db.commit()
            return bool(changed)

        return self._retry_locked(transition) if transaction else transition()

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("JobManager is closed")

    def _retry_locked(self, fn, *args, attempts: int = 5, event_job: str | None = None, **kwargs):
        """Run fn, retrying transient SQLite write-lock contention.

        The per-process db_lock serializes threads inside one process, but
        runners and the daemon are separate processes sharing one database.
        A short retry loop keeps a transient ``database is locked`` from
        killing a runner thread or abandoning a critical write.

        Rollback on failure belongs to the transaction's own body (under
        ``db_lock``); doing it here would run unlocked on the shared connection
        and could discard another thread's in-flight transaction.
        """
        for attempt in range(attempts):
            try:
                return fn(*args, **kwargs)
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() or attempt == attempts - 1:
                    raise
                self.sqlite_contentions += 1
                if event_job is not None:
                    self.event_contentions_by_job[event_job] = self.event_contentions_by_job.get(event_job, 0) + 1
                time.sleep(0.02 * (attempt + 1))
        raise RuntimeError("unreachable")  # pragma: no cover

    def _dispatch_loop(self) -> None:
        while not self.dispatcher_stop.wait(float(os.environ.get("VANTH_DELIVERY_POLL_INTERVAL", "0.2"))):
            try:
                self._dispatch_due_deliveries()
                self._reconcile_running_jobs()
                self._recover_stale_launch_claims()
                self._fire_due_schedules()
                self._dispatch_queued_jobs()
                self._watch_policies()
                self._maybe_auto_cleanup()
                self._expire_decisions()
                self.relay_expire_stale(stale_after_seconds=int(os.environ.get("VANTH_RELAY_SUBSCRIPTION_TTL", "300")))
                # Sweep at most once a minute: the UPDATE takes the write lock, so
                # running it on every 0.2s pass would add needless contention.
                delivery_ttl = self._delivery_ttl_seconds()
                if delivery_ttl > 0 and time.monotonic() - self._last_delivery_expiry >= min(delivery_ttl, 60):
                    self._last_delivery_expiry = time.monotonic()
                    self.expire_stale_deliveries(ttl_seconds=delivery_ttl)
                # Reclaim WAL space periodically. Auto-checkpoint can be starved by
                # a long-lived read snapshot, letting the -wal file grow without
                # bound; PASSIVE never blocks a concurrent reader/writer.
                if time.monotonic() - self._last_wal_checkpoint >= self._wal_checkpoint_seconds():
                    self._last_wal_checkpoint = time.monotonic()
                    self._checkpoint_wal()
            except Exception:
                self.logger.exception("maintenance iteration failed")

    def _checkpoint_wal(self) -> None:
        try:
            with self.db_lock:
                self.db.execute("PRAGMA wal_checkpoint(PASSIVE)")
        except sqlite3.Error as exc:
            self.logger.warning("wal checkpoint failed: %s", exc)

    def _wal_checkpoint_seconds(self) -> float:
        try:
            value = float(os.environ.get("VANTH_WAL_CHECKPOINT_SECONDS", "300"))
        except ValueError:
            return 300.0
        return value if value > 0 else 300.0

    def _watch_policies(self) -> None:
        """Evaluate per-job policy blocks (dead-man's switch + failure reactions).

        Two independent watches per job:

        - ``schedule``: a recurring job pattern. ``last_started_at`` is the
          most recent time this job entered a running state. If the expected
          interval (plus grace) elapses with no new start, emit
          ``schedule_missed`` once per missed window. If a job started but has
          been running longer than interval+grace, emit ``job_stuck`` once.
        - ``on_failure``: count consecutive terminal failures (from the
          ``failure_threshold``-committed state, see below); when the count
          reaches ``after_n``, fire the configured action once, then reset so
          a subsequent streak re-triggers.
        """
        try:
            now = datetime.now(timezone.utc)
            with self.db_lock:
                rows = self.db.execute(
                    "SELECT job_id, status, started_at, updated_at, policy_json, tags_json, name FROM jobs "
                    "WHERE policy_json IS NOT NULL AND policy_disabled=0"
                ).fetchall()
            for row in rows:
                try:
                    policy = json.loads(row["policy_json"] or "null")
                except (TypeError, ValueError):
                    continue
                if not isinstance(policy, dict):
                    continue
                schedule = policy.get("schedule")
                if isinstance(schedule, dict):
                    self._watch_schedule(row, schedule, now)
                on_failure = policy.get("on_failure")
                if isinstance(on_failure, dict):
                    self._watch_on_failure(row, on_failure)
                restart = policy.get("restart")
                if isinstance(restart, dict):
                    self._watch_restart(row, restart)
                retention = policy.get("retention")
                if isinstance(retention, dict):
                    self._apply_retention(row["job_id"], retention)
        except Exception:
            self.logger.exception("policy watch iteration failed")

    def _policy_state(self, job_id: str) -> dict[str, Any]:
        row = self._row("SELECT policy_state_json FROM jobs WHERE job_id=?", (job_id,))
        if not row:
            return {}
        try:
            state = json.loads(row["policy_state_json"] or "{}")
        except (TypeError, ValueError):
            return {}
        return state if isinstance(state, dict) else {}

    def _save_policy_state(self, job_id: str, state: dict[str, Any]) -> None:
        def write() -> None:
            with self.db_lock:
                self.db.execute(
                    "UPDATE jobs SET policy_state_json=?, updated_at=? WHERE job_id=?",
                    (json.dumps(state, separators=(",", ":")), now_iso(), job_id),
                )
                self.db.commit()
        self._retry_locked(write)

    def _watch_schedule(self, row: sqlite3.Row, schedule: dict[str, Any], now: datetime) -> None:
        job_id = row["job_id"]
        interval = int(schedule["expected_interval_seconds"])
        grace = int(schedule.get("grace_period_seconds", 0))
        horizon = interval + grace
        state = self._policy_state(job_id)
        # Only track jobs that are actually recurring: enabled + completed/
        # failed/running histories. Queued/cancelled/timeout jobs pause the
        # watch (timeout means the run itself failed; on_failure handles it).
        status = row["status"]
        if status in {"queued", "cancelled", "launching"}:
            return
        last_start = row["started_at"]
        try:
            last_start_dt = datetime.fromisoformat(last_start.replace("Z", "+00:00")) if last_start else None
        except ValueError:
            last_start_dt = None
        if last_start_dt is None:
            return
        elapsed = (now - last_start_dt).total_seconds()
        # Per-run rearm (review P2-1): when the job row's started_at changes
        # (e.g. an automatic restart reused the same row), the dead-man flags
        # belong to the PREVIOUS run and must not suppress the new run's
        # schedule_missed/job_stuck. Detect the change and clear them.
        if state.get("observed_started_at") != last_start:
            for key in ("stuck_emitted", "missed_emitted_at_elapsed"):
                if key in state:
                    state.pop(key)
            state["observed_started_at"] = last_start
            self._save_policy_state(job_id, state)
        if status == "running":
            # Started but running far past its expected cadence -> stuck.
            if elapsed > horizon and not state.get("stuck_emitted"):
                self._emit(
                    job_id,
                    "job_stuck",
                    message=f"Job running for {int(elapsed)}s, past expected interval {interval}s + grace {grace}s",
                    data={"elapsed_seconds": int(elapsed), "expected_interval_seconds": interval, "grace_period_seconds": grace},
                    level="warning",
                )
                state["stuck_emitted"] = True
                self._save_policy_state(job_id, state)
            return
        # Terminal (completed/failed/orphaned): watch for a missed next run.
        if elapsed > horizon and not state.get("missed_emitted_at_elapsed"):
            self._emit(
                job_id,
                "schedule_missed",
                message=f"Expected re-run within {interval}s (+{grace}s grace) but no start for {int(elapsed)}s",
                data={"elapsed_seconds": int(elapsed), "expected_interval_seconds": interval, "grace_period_seconds": grace},
                level="warning",
            )
            # Re-arm only when the job starts again (started_at changes);
            # record elapsed to avoid duplicate emissions within this window.
            state["missed_emitted_at_elapsed"] = int(elapsed)
            self._save_policy_state(job_id, state)
        elif elapsed <= interval:
            # Fresh run happened; clear sticky flags (keep run-tracking +
            # failure-streak + retention bookkeeping).
            if state.get("missed_emitted_at_elapsed") or state.get("stuck_emitted"):
                for key in ("missed_emitted_at_elapsed", "stuck_emitted"):
                    state.pop(key, None)
                self._save_policy_state(job_id, state)

    def _watch_on_failure(self, row: sqlite3.Row, on_failure: dict[str, Any]) -> None:
        job_id = row["job_id"]
        after_n = int(on_failure["after_n"])
        action = on_failure["action"]
        state = self._policy_state(job_id)
        streak = int(state.get("failure_streak", 0))
        status = row["status"]
        if status == "failed":
            # Count each failed EXECUTION exactly once (review P1-2 / P2): the
            # failure event is the unit. A failed row that stays failed across
            # many watcher ticks is not re-counted, but an automatic RESTART that
            # reuses the same job row emits a NEW failed event, so the streak is
            # measured between two event-sequence watermarks. The upper bound is
            # the row we just read: a failure committed between the two queries
            # must not be counted here AND again on the next tick.
            last_terminal = self.db.execute(
                "SELECT event_id, seq FROM events WHERE job_id=? AND type='failed' ORDER BY seq DESC LIMIT 1",
                (job_id,),
            ).fetchone()
            if last_terminal is None:
                return
            last_seq = int(last_terminal["seq"])
            watermark = state.get("failure_streak_after_seq")
            if watermark is None:
                # Migration from the pre-1.9.1 marker: resolve the legacy event
                # id to its job-local sequence so already-counted failures are
                # not counted a second time after upgrading.
                legacy_id = state.get("last_failure_event_id")
                if legacy_id:
                    legacy = self.db.execute(
                        "SELECT seq FROM events WHERE job_id=? AND event_id=?", (job_id, legacy_id)
                    ).fetchone()
                    if legacy is not None:
                        watermark = int(legacy["seq"])
            if watermark is not None and last_seq <= int(watermark):
                # Every failed execution up to here is already counted. A prior
                # tick may have committed the streak but failed (or crashed)
                # before completing the reaction: retry it instead of dropping it.
                if streak >= after_n and state.get("reacted_at_streak") != streak:
                    self._react_to_failure(row, on_failure, streak)
                    state["reacted_at_streak"] = streak
                    self._save_policy_state(job_id, state)
                return
            # Count EVERY failed execution in the interval, not just the latest:
            # with a fast restart two failures can land between watcher ticks and
            # incrementing by one per tick undercounts (Windows CI saw a final
            # streak of 2, not 3). With no watermark (fresh streak / first-ever
            # failure) count from the beginning so a pre-existing backlog is not
            # collapsed to one.
            if watermark is None:
                pending = int(
                    self.db.execute(
                        "SELECT COUNT(*) FROM events WHERE job_id=? AND type='failed' AND seq<=?",
                        (job_id, last_seq),
                    ).fetchone()[0]
                )
            else:
                pending = int(
                    self.db.execute(
                        "SELECT COUNT(*) FROM events WHERE job_id=? AND type='failed' AND seq>? AND seq<=?",
                        (job_id, int(watermark), last_seq),
                    ).fetchone()[0]
                )
            new_streak = streak + max(1, pending)
            react = new_streak >= after_n and state.get("reacted_at_streak") != new_streak
            state["failure_streak"] = new_streak
            state["failure_streak_after_seq"] = last_seq
            # Persist the streak BEFORE reacting: _react_to_failure emits the
            # failure_threshold event, and a waiter that observes that event must
            # already see the updated policy state (reading between the event
            # commit and the state save saw no failure_streak). The reaction is
            # marked complete only AFTER it succeeds, so a crash/error retries it.
            # ponytail: at-least-once reaction delivery — a crash between the side
            # effect and the marker save can repeat it. Harmless in practice
            # (disable is filtered out of the scan, run_job refuses a busy target,
            # alerts are advisory); a durable outbox + idempotency key per streak
            # is the upgrade path if a reaction becomes side-effect-heavy.
            self._save_policy_state(job_id, state)
            if react:
                self._react_to_failure(row, on_failure, new_streak)
                state["reacted_at_streak"] = new_streak
                self._save_policy_state(job_id, state)
        elif status in {"completed", "timeout", "cancelled", "orphaned"}:
            # A non-failure terminal outcome resets the run identity: the NEXT
            # failure is a fresh run. timeout keeps its existing semantics
            # (handled by the failed branch when it maps to a failed event;
            # otherwise it continues the streak as before). Only reset when we
            # actually have a streak to clear, so the watcher stays a no-op for
            # jobs that never failed.
            if state.get("failure_streak") or state.get("failure_streak_after_seq") is not None:
                state["failure_streak"] = 0
                state.pop("reacted_at_streak", None)
                # Move the watermark past the failures that preceded this success
                # so the next streak counts only failures after the reset.
                latest_failed = self.db.execute(
                    "SELECT seq FROM events WHERE job_id=? AND type='failed' ORDER BY seq DESC LIMIT 1",
                    (job_id,),
                ).fetchone()
                state["failure_streak_after_seq"] = int(latest_failed["seq"]) if latest_failed else 0
                self._save_policy_state(job_id, state)

    def _watch_restart(self, row: sqlite3.Row, restart: dict[str, Any]) -> None:
        """Relaunch failed jobs with linear-until-cap backoff, up to max_retries.

        Restart bookkeeping lives in policy_state (restart_attempts,
        restart_after). ``restart_after`` is the single persisted deadline for
        the NEXT relaunch; the dispatcher claims it exactly once per iteration
        when it is due. No in-memory timers: a daemon restart cannot lose or
        double-schedule a pending relaunch, and polling never consumes the
        budget. A successful completion resets the attempt counter; exhausting
        the budget emits a ``gave_up`` event (level=error) that flows to wake
        targets. Disabled jobs are skipped.
        """
        job_id = row["job_id"]
        status = row["status"]
        if status not in {"failed", "completed"}:
            return
        state = self._policy_state(job_id)
        max_retries = int(restart["max_retries"])
        backoff_base = int(restart.get("backoff_seconds", 0))
        backoff_max = int(restart.get("backoff_max_seconds", 0))
        if status == "completed":
            if state.get("restart_attempts"):
                state["restart_attempts"] = 0
                state.pop("restart_after", None)
                self._save_policy_state(job_id, state)
            return
        attempts = int(state.get("restart_attempts", 0))
        restart_after = state.get("restart_after")
        if restart_after:
            try:
                due_at = datetime.fromisoformat(str(restart_after).replace("Z", "+00:00"))
            except ValueError:
                due_at = None
            if due_at is None or datetime.now(timezone.utc) < due_at:
                return  # pending relaunch not due yet; budget already claimed
            # The persisted deadline is due: launch the already-budgeted
            # relaunch. No further budget is consumed here.
            self._launch_due_restart(job_id, attempts, restart_after, max_retries, state)
            return
        if attempts >= max_retries:
            if not state.get("gave_up"):
                state["gave_up"] = True
                self._save_policy_state(job_id, state)
                self._emit(
                    job_id,
                    "gave_up",
                    message=f"Job failed and the restart budget is exhausted ({attempts}/{max_retries} retries used)",
                    data={"restart_attempts": attempts, "max_retries": max_retries},
                    level="error",
                )
            return
        # Fresh failure, no pending deadline: claim the budget and persist the
        # single restart deadline. Subsequent ticks return above while
        # restart_after is still in the future, so polling cannot consume more
        # of the budget (review P1-1).
        delay = min(backoff_base * (attempts + 1), backoff_max) if backoff_max else backoff_base * (attempts + 1)
        state["restart_attempts"] = attempts + 1
        state.pop("gave_up", None)
        state["last_restart_delay_seconds"] = delay
        state["restart_after"] = (datetime.now(timezone.utc) + timedelta(seconds=delay)).isoformat().replace("+00:00", "Z")
        self._save_policy_state(job_id, state)
        if delay == 0:
            # No backoff: relaunch immediately on this tick.
            self._launch_due_restart(job_id, attempts + 1, state["restart_after"], max_retries, state)

    def _launch_due_restart(self, job_id: str, attempt: int, restart_after: str, max_retries: int, state: dict[str, Any]) -> None:
        """Atomically claim and launch a due restart deadline (once).

        Inside one write transaction we verify the job is still failed, the
        deadline is unchanged, and the job is not disabled, then clear
        ``restart_after`` and launch. Any concurrent tick that read the same
        deadline finds it cleared and skips.
        """
        launch_info = self._claim_due_restart(job_id, restart_after)
        if launch_info is None:
            self.logger.warning("restart launch refused job_id=%s attempt=%s", job_id, attempt)
            return
        delay = int(state.get("last_restart_delay_seconds", 0))
        self._launch_prepared(launch_info)
        self._emit(
            job_id,
            "restarted",
            message=f"Restart attempt {attempt}/{max_retries} after failure (delay {delay}s)",
            data={"restart_attempt": attempt, "max_retries": max_retries, "delay_seconds": delay},
        )

    def _claim_due_restart(self, job_id: str, restart_after: str) -> dict[str, Any] | None:
        """Atomically claim a due restart deadline and prepare its launch.

        One dispatcher tick wins the claim: within a single guarded UPDATE the
        job is verified still-failed/not-disabled with the deadline unchanged,
        the deadline is cleared, and the row is claimed ``launching`` with a
        durable claim_token. The deadline clear and the launch claim are ONE
        transaction (review rc32 P1-4): a crash between them previously left a
        failed job with its attempt consumed and no pending deadline, so
        ``max_retries=1`` immediately became ``gave_up``. Now either both the
        claim and the deadline-clear commit, or neither does — and if the claim
        cannot be won (disabled, already claimed, or deadline changed), the
        deadline stays intact for the next tick. Returns the launch dict, or
        ``None`` when the job is no longer eligible.
        """
        token = self._claim_launch(
            job_id,
            require_policy_state={"restart_after": restart_after},
            policy_state_updates={"restart_after": None},
        )
        if token is None:
            return None
        return self._build_launch(job_id, token)

    def _apply_retention(self, job_id: str, retention: dict[str, int]) -> None:
        """Prune old per-stream rows for one job (policy.retention block).

        Throttled (review P1-6): instead of running DELETE+commit every 0.2s
        dispatch tick, a per-job ``retention_next_at`` deadline in policy_state
        limits pruning to at most once per ``retention_min_interval_seconds``
        (default 60s, overridable via env). Deletion runs inside a single
        transaction with rollback-on-error (no partial deletion on failure),
        and deleting deliveries cascades to their delivery_attempts so no rows
        are orphaned. Terminal events (needed for status history) are kept —
        only non-terminal, non-metric bookkeeping rows expire.
        """
        state = self._policy_state(job_id)
        min_interval = int(os.environ.get("VANTH_RETENTION_MIN_INTERVAL_SECONDS", "60"))
        next_at = state.get("retention_next_at")
        if next_at:
            try:
                if datetime.now(timezone.utc) < datetime.fromisoformat(str(next_at).replace("Z", "+00:00")):
                    return  # throttled; not due yet
            except ValueError:
                pass
        cutoff_base = datetime.now(timezone.utc)
        deleted_total = 0
        with self.db_lock:
            try:
                for column in ("events", "metrics", "deliveries"):
                    seconds = retention.get(column)
                    if not seconds:
                        continue
                    cutoff = (cutoff_base - timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")
                    if column == "events":
                        cur = self.db.execute(
                            "DELETE FROM events WHERE job_id=? AND created_at < ? AND type NOT IN ('completed','failed','timeout','cancelled','orphaned')",
                            (job_id, cutoff),
                        )
                    elif column == "metrics":
                        cur = self.db.execute(
                            "DELETE FROM metric_series WHERE job_id=? AND created_at < ?",
                            (job_id, cutoff),
                        )
                    else:
                        # Cascade: drop settled deliveries AND their attempts so
                        # delivery_attempts are never orphaned by pruning.
                        self.db.execute(
                            "DELETE FROM delivery_attempts WHERE delivery_id IN "
                            "(SELECT delivery_id FROM deliveries WHERE job_id=? AND created_at < ? AND status IN ('delivered','failed'))",
                            (job_id, cutoff),
                        )
                        cur = self.db.execute(
                            "DELETE FROM deliveries WHERE job_id=? AND created_at < ? AND status IN ('delivered','failed')",
                            (job_id, cutoff),
                        )
                    deleted_total += cur.rowcount
                self.db.commit()
            except Exception:
                # A DELETE opened an implicit transaction; roll back so partial
                # deletion is never committed (review P1-6).
                self.db.rollback()
                raise
        # Record the throttle deadline even when nothing was deleted (idempotent
        # no-op runs still must not burn a write transaction every tick).
        state["retention_next_at"] = (cutoff_base + timedelta(seconds=min_interval)).isoformat().replace("+00:00", "Z")
        self._save_policy_state(job_id, state)
        if deleted_total:
            self.logger.info("retention pruned job_id=%s rows=%s", job_id, deleted_total)

    def _react_to_failure(self, row: sqlite3.Row, on_failure: dict[str, Any], streak: int) -> None:
        job_id = row["job_id"]
        action = on_failure["action"]
        message = f"Failure streak reached {streak} (after_n={on_failure['after_n']}); policy action: {action}"
        data = {"failure_streak": streak, "after_n": on_failure["after_n"], "action": action}
        # Commit any open write transaction BEFORE emitting: _emit opens its
        # own BEGIN IMMEDIATE and the connection must be idle for that.
        with self.db_lock:
            try:
                self.db.commit()
            except sqlite3.Error:
                pass
        if action == "alert":
            self._emit(job_id, "failure_threshold", message=message, data=data, level="error")
            return
        if action == "disable":
            def disable() -> int:
                with self.db_lock:
                    changed = self.db.execute(
                        "UPDATE jobs SET policy_disabled=1, updated_at=? WHERE job_id=?",
                        (now_iso(), job_id),
                    ).rowcount
                    self.db.commit()
                    return changed
            changed = bool(self._retry_locked(disable))
            with self.db_lock:
                try:
                    self.db.commit()
                except sqlite3.Error:
                    pass
            if changed:
                self._emit(job_id, "failure_threshold", message=message + "; job disabled", data={**data, "disabled": True}, level="error")
            return
        if action == "run_job":
            target_job = on_failure.get("job_id")
            if not target_job:
                return
            try:
                with self.db_lock:
                    try:
                        self.db.commit()
                    except sqlite3.Error:
                        pass
                launch = self.prepare_launch(target_job)
            except ValueError as exc:
                self._emit(job_id, "failure_threshold", message=f"{message}; reaction job launch failed: {exc}", data={**data, "reaction_job_id": target_job}, level="error")
                return
            if launch is None:
                self._emit(job_id, "failure_threshold", message=f"{message}; reaction job {target_job} is not idle", data={**data, "reaction_job_id": target_job}, level="warning")
                return
            self._launch_prepared(launch)
            with self.db_lock:
                try:
                    self.db.commit()
                except sqlite3.Error:
                    pass
            self._emit(job_id, "failure_threshold", message=f"{message}; launched reaction job {target_job}", data={**data, "reaction_job_id": target_job})

    def _dispatch_queued_jobs(self) -> None:
        """Launch queued jobs whose gates are satisfied, in priority order.

        A queued job is gated by its trigger (a DAG parent status) and/or its
        pool (not paused, under ``max_parallel``). Eligible jobs launch
        highest-priority first and consume the global concurrent-job quota. A
        trigger job whose parent ended in a different terminal status is
        cancelled. One method covers trigger DAGs and pools — no second queue.
        """
        try:
            with self.db_lock:
                rows = self.db.execute(
                    "SELECT job_id, trigger_json, pool, priority, paused, created_at "
                    "FROM jobs WHERE status='queued' ORDER BY priority DESC, created_at ASC"
                ).fetchall()
                if not rows:
                    return
                parents: dict[str, tuple[str | None, str | None]] = {}
                triggers: dict[str, dict[str, Any]] = {}
                for row in rows:
                    trigger = json.loads(row["trigger_json"] or "null")
                    if isinstance(trigger, dict):
                        triggers[row["job_id"]] = trigger
                        if trigger.get("job_id"):
                            parents.setdefault(trigger["job_id"], (None, None))
                for parent in parents:
                    status_row = self.db.execute(
                        "SELECT status, ended_at FROM jobs WHERE job_id=?", (parent,)
                    ).fetchone()
                    parents[parent] = (status_row["status"], status_row["ended_at"]) if status_row else (None, None)

            # Keep the probe throttle bounded to the jobs still queued.
            queued_ids = {row["job_id"] for row in rows}
            self._probe_last_attempt = {
                key: value for key, value in self._probe_last_attempt.items() if key in queued_ids
            }
            # If the global quota is already exhausted, nothing can launch, so
            # skip probe I/O entirely this pass (DAG cancels still run).
            capacity_open = (not self.max_running_jobs) or (self._running_count() < self.max_running_jobs)
            probe_budget = [self.probe_budget]

            to_cancel: list[tuple[str, str, str]] = []
            to_cancel_orphan: list[tuple[str, str]] = []
            to_cancel_probe: list[tuple[str, dict[str, Any]]] = []
            to_launch: list[tuple[str, str | None]] = []
            for row in rows:
                job_id = row["job_id"]
                trigger = triggers.get(job_id)
                probe = trigger.get("probe") if trigger else None
                # The readiness deadline runs from when the dependency gate is
                # satisfied (the parent's end), or from queue creation when there
                # is no DAG gate.
                probe_start = row["created_at"]
                if trigger and trigger.get("job_id"):
                    parent, target = trigger["job_id"], trigger["status"]
                    parent_status, parent_ended = parents.get(parent, (None, None))
                    if parent_status is None:
                        # The parent row is gone (pruned/removed): the gate can
                        # never fire, so cancel rather than waiting forever with
                        # no event (the queued job would otherwise never move).
                        to_cancel_orphan.append((job_id, parent))
                        continue
                    if parent_status in TERMINAL_STATUSES and parent_status != target:
                        # Cancellation is independent of pause: a held job whose
                        # trigger can never fire must not linger.
                        to_cancel.append((job_id, parent, target))
                        continue
                    if parent_status != target:
                        continue  # DAG gate not satisfied yet
                    if parent_ended:
                        probe_start = parent_ended
                if probe is not None and self._probe_timed_out(probe, probe_start):
                    to_cancel_probe.append((job_id, probe))
                    continue
                if row["paused"]:
                    continue  # held; capacity/pause are enforced again at claim
                if probe is not None:
                    if not capacity_open:
                        continue  # global quota full; keep waiting without I/O
                    if not self._probe_ready(job_id, probe, probe_budget):
                        continue  # readiness gate not satisfied yet
                to_launch.append((job_id, row["pool"]))

            for job_id, pool in to_launch:
                # Capacity, current pool pause/max, and the claim are one guarded
                # UPDATE (see _launch_queued_if_capacity), so a concurrent direct
                # start(), dispatcher, or pool reconfiguration cannot slip past.
                if self._launch_queued_if_capacity(job_id, pool=pool):
                    self._probe_last_attempt.pop(job_id, None)
            for job_id, probe in to_cancel_probe:
                reason = f"readiness probe ({probe['type']}) did not become ready within {probe['timeout_seconds']}s"
                if self._cancel_queued(
                    job_id, actor="daemon", reason=reason, message=f"Queued job cancelled: {reason}",
                    data={"actor": "daemon", "reason": reason, "probe": probe},
                ):
                    self._probe_last_attempt.pop(job_id, None)
            for job_id, parent, status in to_cancel:
                reason = "trigger parent reached an incompatible terminal status"
                self._cancel_queued(
                    job_id, actor="daemon", reason=reason,
                    message=f"Trigger parent {parent} reached a different terminal status than {status}",
                    data={"actor": "daemon", "reason": reason,
                          "trigger": {"job_id": parent, "status": status},
                          "parent_status": parents.get(parent, (None, None))[0]},
                )
            for job_id, parent in to_cancel_orphan:
                reason = f"trigger parent {parent} no longer exists"
                self._cancel_queued(
                    job_id, actor="daemon", reason=reason, message=f"Queued job cancelled: {reason}",
                    data={"actor": "daemon", "reason": reason, "trigger": {"job_id": parent}},
                )
        except Exception:
            self.logger.exception("queued-job dispatch failed")

    def _probe_timed_out(self, probe: dict[str, Any], created_at: str | None) -> bool:
        timeout = probe.get("timeout_seconds")
        if not timeout:
            return False
        created = _parse_iso(created_at) if created_at else None
        if created is None:
            return False
        return (datetime.now(timezone.utc) - created).total_seconds() >= int(timeout)

    def _probe_ready(self, job_id: str, probe: dict[str, Any], budget: list[int]) -> bool:
        """Evaluate a readiness probe, throttled per job and budgeted per pass.

        Returns False when the probe is not yet satisfied, inside its throttle
        window, or the per-pass I/O budget is exhausted (so the caller keeps the
        job queued). A throttled check consumes no budget, so successive passes
        drain the whole queue without stalling the maintenance loop.
        """
        interval = float(probe.get("interval_seconds", 1))
        now = time.monotonic()
        last = self._probe_last_attempt.get(job_id)
        if last is not None and now - last < interval:
            return False
        if budget[0] <= 0:
            return False
        budget[0] -= 1
        self._probe_last_attempt[job_id] = now
        try:
            if probe["type"] == "log_line":
                return evaluate_probe(probe, log_text=self._probe_log_text(probe))
            return evaluate_probe(probe)
        except Exception:
            self.logger.exception("readiness probe failed job_id=%s type=%s", job_id, probe.get("type"))
            return False

    def _probe_log_text(self, probe: dict[str, Any], max_bytes: int = 262144) -> str:
        """Bounded tail of a target job's captured log for a log_line probe.

        The path is resolved and confined under the logs directory as defense in
        depth; the target ``job_id`` is validated to be a real job at trigger
        creation, so it is always a generated ``job_<hex>`` id.
        """
        job_id = probe["job_id"]
        stream = probe.get("stream", "all")
        streams = ["stdout", "stderr"] if stream == "all" else [stream]
        logs_root = self.logs.resolve()
        chunks: list[str] = []
        for name in streams:
            path = (self.logs / f"{job_id}.{name}.log").resolve()
            if not path.is_relative_to(logs_root):
                self.logger.warning("log_line probe path escaped the logs dir; ignoring")
                continue
            try:
                size = path.stat().st_size
                with path.open("rb") as handle:
                    if size > max_bytes:
                        handle.seek(size - max_bytes)
                    chunks.append(handle.read().decode("utf-8", errors="replace"))
            except OSError:
                continue
        return "\n".join(chunks)

    def _launch_queued_if_capacity(self, job_id: str, *, pool: str | None) -> bool:
        """Atomically enforce the global/pool cap and claim a queued job.

        Every gate is a subquery INSIDE the guarded UPDATE, so the read and the
        claim are one SQLite statement under the write lock: a concurrent direct
        ``start()``, another dispatcher, or a pool reconfiguration/pause cannot
        slip a job in past a cap between the check and the claim (review rc37
        P1 parity, extended to pools). The job's own ``paused``/``policy_disabled``
        and the pool's CURRENT ``paused``/``max_parallel`` are all re-read here.
        """
        token = "claim_" + uuid.uuid4().hex[:16]

        def claim() -> int:
            with self.db_lock:
                if pool:
                    changed = self.db.execute(
                        "UPDATE jobs SET status='launching', claim_token=?, updated_at=? "
                        "WHERE job_id=? AND status='queued' AND paused=0 AND policy_disabled=0 "
                        "AND (? = 0 OR (SELECT COUNT(*) FROM jobs WHERE status IN ('running','launching')) < ?) "
                        "AND COALESCE((SELECT paused FROM pools WHERE pool=?), 0) = 0 "
                        "AND (COALESCE((SELECT max_parallel FROM pools WHERE pool=?), 0) = 0 "
                        "     OR (SELECT COUNT(*) FROM jobs WHERE pool=? AND status IN ('running','launching')) "
                        "        < (SELECT max_parallel FROM pools WHERE pool=?))",
                        (
                            token, now_iso(), job_id,
                            self.max_running_jobs, self.max_running_jobs,
                            pool, pool, pool, pool,
                        ),
                    ).rowcount
                else:
                    changed = self.db.execute(
                        "UPDATE jobs SET status='launching', claim_token=?, updated_at=? "
                        "WHERE job_id=? AND status='queued' AND paused=0 AND policy_disabled=0 "
                        "AND (? = 0 OR (SELECT COUNT(*) FROM jobs WHERE status IN ('running','launching')) < ?)",
                        (token, now_iso(), job_id, self.max_running_jobs, self.max_running_jobs),
                    ).rowcount
                self.db.commit()
                return changed

        if not self._retry_locked(claim):
            return False
        self._launch_prepared(self._build_launch(job_id, token))
        return True

    @staticmethod
    def _iso_utc(value: datetime) -> str:
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    def _schedule_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "schedule_id": row["schedule_id"],
            "name": row["name"],
            "cron": row["cron"],
            "interval_seconds": row["interval_seconds"],
            "timezone": row["timezone"],
            "command": row["command"],
            "cwd": row["cwd"],
            "env": json.loads(row["env_json"] or "{}"),
            "timeout_seconds": row["timeout_seconds"],
            "tags": json.loads(row["tags_json"] or "[]"),
            "notes": row["notes"],
            "secret_env": json.loads(row["secret_env_json"] or "[]"),
            "overlap": row["overlap"],
            "enabled": bool(row["enabled"]),
            "next_fire_at": row["next_fire_at"],
            "last_fired_at": row["last_fired_at"],
            "fire_count": row["fire_count"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def create_schedule(
        self,
        command: str,
        *,
        name: str | None = None,
        cron: str | None = None,
        interval_seconds: int | None = None,
        timezone_name: str = "UTC",
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        timeout_seconds: int | None = None,
        tags: list[str] | None = None,
        notes: str | None = None,
        secret_env: list[str] | None = None,
        overlap: str = "skip",
        enabled: bool = True,
    ) -> dict[str, Any]:
        """Create a cron/interval schedule that launches a fresh job per fire."""
        self._ensure_open()
        if not isinstance(command, str) or not command.strip():
            raise ValueError("command must be a non-empty string")
        cron = cron.strip() if isinstance(cron, str) and cron.strip() else None
        validate_schedule_spec(cron=cron, interval_seconds=interval_seconds)
        tz = validate_timezone(timezone_name)
        if timeout_seconds is not None and (isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, int) or timeout_seconds < 1):
            raise ValueError("timeout_seconds must be an integer >= 1")
        if not isinstance(overlap, str) or overlap not in {"skip", "allow"}:
            raise ValueError("overlap must be 'skip' or 'allow'")
        if env is not None and not isinstance(env, dict):
            raise ValueError("env must be an object of string values")
        secret_env = self._validate_secret_env(secret_env)
        schedule_id = "sched_" + uuid.uuid4().hex[:12]
        now = now_iso()
        next_at = (
            self._iso_utc(compute_next_fire(cron=cron, interval_seconds=interval_seconds, timezone_name=tz))
            if enabled
            else None
        )
        with self.db_lock:
            self.db.execute(
                """
                INSERT INTO schedules(schedule_id, name, cron, interval_seconds, timezone, command, cwd, env_json,
                  timeout_seconds, tags_json, notes, secret_env_json, overlap, enabled, next_fire_at, last_fired_at,
                  fire_count, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, ?, ?)
                """,
                (
                    schedule_id,
                    name,
                    cron,
                    interval_seconds,
                    tz,
                    command,
                    cwd,
                    json.dumps(env or {}, separators=(",", ":")),
                    timeout_seconds,
                    json.dumps(tags or [], separators=(",", ":")),
                    notes,
                    json.dumps(secret_env, separators=(",", ":")) if secret_env else None,
                    overlap,
                    1 if enabled else 0,
                    next_at,
                    now,
                    now,
                ),
            )
            self.db.commit()
        return self.get_schedule(schedule_id)

    def get_schedule(self, schedule_id: str) -> dict[str, Any]:
        row = self._row("SELECT * FROM schedules WHERE schedule_id=?", (schedule_id,))
        if not row:
            raise ValueError(f"Unknown schedule_id: {schedule_id}")
        return self._schedule_dict(row)

    def list_schedules(self) -> dict[str, Any]:
        self._ensure_open()
        with self.db_lock:
            rows = self.db.execute("SELECT * FROM schedules ORDER BY created_at ASC").fetchall()
        return {"schedules": [self._schedule_dict(row) for row in rows], "count": len(rows)}

    def update_schedule(self, schedule_id: str, **changes: Any) -> dict[str, Any]:
        """Edit a schedule in place (never its id) and recompute the next fire."""
        self._ensure_open()
        row = self._row("SELECT * FROM schedules WHERE schedule_id=?", (schedule_id,))
        if not row:
            raise ValueError(f"Unknown schedule_id: {schedule_id}")
        allowed = {
            "name", "cron", "interval_seconds", "timezone", "command", "cwd", "env",
            "timeout_seconds", "tags", "notes", "secret_env", "overlap", "enabled",
        }
        unknown = set(changes) - allowed
        if unknown:
            raise ValueError(f"unknown schedule fields: {sorted(unknown)}")
        merged = {**self._schedule_dict(row), **changes}
        command = merged.get("command")
        if not isinstance(command, str) or not command.strip():
            raise ValueError("command must be a non-empty string")
        cron = merged.get("cron")
        cron = cron.strip() if isinstance(cron, str) and cron.strip() else None
        interval = merged.get("interval_seconds")
        if interval is not None and (isinstance(interval, bool) or not isinstance(interval, int) or interval < 1):
            raise ValueError("interval_seconds must be an integer >= 1")
        validate_schedule_spec(cron=cron, interval_seconds=interval)
        tz = validate_timezone(merged.get("timezone") or "UTC")
        overlap = merged.get("overlap", "skip")
        if not isinstance(overlap, str) or overlap not in {"skip", "allow"}:
            raise ValueError("overlap must be 'skip' or 'allow'")
        timeout = merged.get("timeout_seconds")
        if timeout is not None and (isinstance(timeout, bool) or not isinstance(timeout, int) or timeout < 1):
            raise ValueError("timeout_seconds must be an integer >= 1")
        env = merged.get("env") or {}
        if not isinstance(env, dict):
            raise ValueError("env must be an object of string values")
        secret_env = self._validate_secret_env(merged.get("secret_env"))
        enabled = bool(merged.get("enabled", True))
        now = now_iso()
        next_at = (
            self._iso_utc(compute_next_fire(cron=cron, interval_seconds=interval, timezone_name=tz))
            if enabled
            else None
        )
        with self.db_lock:
            self.db.execute(
                "UPDATE schedules SET name=?, cron=?, interval_seconds=?, timezone=?, command=?, cwd=?, env_json=?, "
                "timeout_seconds=?, tags_json=?, notes=?, secret_env_json=?, overlap=?, enabled=?, next_fire_at=?, updated_at=? "
                "WHERE schedule_id=?",
                (
                    merged.get("name"),
                    cron,
                    interval,
                    tz,
                    command,
                    merged.get("cwd"),
                    json.dumps(env, separators=(",", ":")),
                    timeout,
                    json.dumps(merged.get("tags") or [], separators=(",", ":")),
                    merged.get("notes"),
                    json.dumps(secret_env, separators=(",", ":")) if secret_env else None,
                    overlap,
                    1 if enabled else 0,
                    next_at,
                    now,
                    schedule_id,
                ),
            )
            self.db.commit()
        return self.get_schedule(schedule_id)

    def delete_schedule(self, schedule_id: str) -> dict[str, Any]:
        self._ensure_open()
        with self.db_lock:
            changed = self.db.execute("DELETE FROM schedules WHERE schedule_id=?", (schedule_id,)).rowcount
            self.db.commit()
        if not changed:
            raise ValueError(f"Unknown schedule_id: {schedule_id}")
        return {"result": "ok", "schedule_id": schedule_id}

    def schedule_next_fires(self, schedule_id: str, count: int = 5) -> dict[str, Any]:
        self._ensure_open()
        validate_limit(count, "count", 50)
        row = self._row("SELECT * FROM schedules WHERE schedule_id=?", (schedule_id,))
        if not row:
            raise ValueError(f"Unknown schedule_id: {schedule_id}")
        schedule = self._schedule_dict(row)
        now = datetime.now(timezone.utc)
        if schedule["cron"]:
            fires = [self._iso_utc(dt) for dt in next_cron_fires(schedule["cron"], timezone_name=schedule["timezone"], after=now, count=count)]
        else:
            fires = [self._iso_utc(compute_next_fire(interval_seconds=schedule["interval_seconds"], after=now))]
        return {"schedule_id": schedule_id, "timezone": schedule["timezone"], "next_fires": fires}

    def _fire_due_schedules(self) -> None:
        """Launch a fresh job for every enabled schedule whose fire time passed."""
        try:
            now = now_iso()
            with self.db_lock:
                due = self.db.execute(
                    "SELECT * FROM schedules WHERE enabled=1 AND next_fire_at IS NOT NULL AND next_fire_at <= ? "
                    "ORDER BY next_fire_at ASC",
                    (now,),
                ).fetchall()
            for row in due:
                self._fire_schedule(row)
        except Exception:
            self.logger.exception("schedule dispatch failed")

    def _fire_schedule(self, row: sqlite3.Row) -> None:
        schedule_id = row["schedule_id"]
        due_at = row["next_fire_at"]
        fired_at = now_iso()
        now_dt = _parse_iso(fired_at) or datetime.now(timezone.utc)
        try:
            next_at = self._iso_utc(
                compute_next_fire(
                    cron=row["cron"],
                    interval_seconds=row["interval_seconds"],
                    timezone_name=row["timezone"],
                    after=now_dt,
                )
            )
        except ValueError:
            self.logger.exception("schedule %s has no next fire; disabling it", schedule_id)
            with self.db_lock:
                self.db.execute(
                    "UPDATE schedules SET enabled=0, next_fire_at=NULL, updated_at=? WHERE schedule_id=?",
                    (fired_at, schedule_id),
                )
                self.db.commit()
            return
        skip = False
        if row["overlap"] != "allow":
            with self.db_lock:
                active = self.db.execute(
                    "SELECT 1 FROM jobs WHERE schedule_id=? AND status NOT IN "
                    "('completed','failed','timeout','cancelled','orphaned') LIMIT 1",
                    (schedule_id,),
                ).fetchone()
            skip = active is not None

        # Atomically CLAIM the occurrence before acting: the guarded UPDATE
        # advances next_fire_at only while it still equals the value we read, so
        # a second daemon firing the same due row claims zero rows and returns.
        # A fire is therefore at-most-once across managers (a genuinely missed
        # run is surfaced by the dead-man's switch).
        def claim() -> int:
            with self.db_lock:
                if skip:
                    changed = self.db.execute(
                        "UPDATE schedules SET next_fire_at=?, updated_at=? "
                        "WHERE schedule_id=? AND enabled=1 AND next_fire_at=?",
                        (next_at, fired_at, schedule_id, due_at),
                    ).rowcount
                else:
                    changed = self.db.execute(
                        "UPDATE schedules SET last_fired_at=?, next_fire_at=?, fire_count=fire_count+1, updated_at=? "
                        "WHERE schedule_id=? AND enabled=1 AND next_fire_at=?",
                        (fired_at, next_at, fired_at, schedule_id, due_at),
                    ).rowcount
                self.db.commit()
                return changed

        if not self._retry_locked(claim):
            return  # another manager already claimed this fire
        if skip:
            self.logger.warning("schedule %s fire skipped: previous run is still active", schedule_id)
            return
        tags = json.loads(row["tags_json"] or "[]")
        if "scheduled" not in tags:
            tags.append("scheduled")
        try:
            result = asyncio.run(
                self.start(
                    command=row["command"],
                    cwd=row["cwd"],
                    name=row["name"] or schedule_id,
                    env=json.loads(row["env_json"] or "{}") or None,
                    timeout_seconds=row["timeout_seconds"],
                    tags=tags,
                    notes=row["notes"],
                    secret_env=json.loads(row["secret_env_json"] or "[]") or None,
                    schedule_id=schedule_id,
                )
            )
            self.logger.info("schedule %s fired job %s", schedule_id, result.get("job_id"))
        except Exception:
            self.logger.exception("schedule %s failed to launch a job", schedule_id)

    def pool_configure(self, pool: str, *, max_parallel: int = 0, paused: bool | None = None) -> dict[str, Any]:
        """Create/update a pool's concurrency cap and paused flag."""
        self._ensure_open()
        pool = self._validate_pool(pool)
        if pool is None:
            raise ValueError("pool must be a non-empty string")
        if isinstance(max_parallel, bool) or not isinstance(max_parallel, int) or max_parallel < 0:
            raise ValueError("max_parallel must be an integer >= 0")
        now = now_iso()
        with self.db_lock:
            existing = self.db.execute("SELECT * FROM pools WHERE pool=?", (pool,)).fetchone()
            if existing is None:
                self.db.execute(
                    "INSERT INTO pools(pool, max_parallel, paused, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                    (pool, max_parallel, 1 if paused else 0, now, now),
                )
            else:
                self.db.execute(
                    "UPDATE pools SET max_parallel=?, paused=?, updated_at=? WHERE pool=?",
                    (max_parallel, (1 if paused else 0) if paused is not None else existing["paused"], now, pool),
                )
            self.db.commit()
        return self.pool_get(pool)

    def pool_get(self, pool: str) -> dict[str, Any]:
        row = self._row("SELECT * FROM pools WHERE pool=?", (pool,))
        if not row:
            raise ValueError(f"Unknown pool: {pool}")
        return {
            "pool": row["pool"],
            "max_parallel": row["max_parallel"],
            "paused": bool(row["paused"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def pool_list(self) -> dict[str, Any]:
        self._ensure_open()
        with self.db_lock:
            pools = [dict(row) for row in self.db.execute("SELECT * FROM pools ORDER BY pool").fetchall()]
            counts = {
                (row["pool"], row["status"]): row["c"]
                for row in self.db.execute(
                    "SELECT pool, status, COUNT(*) AS c FROM jobs WHERE pool IS NOT NULL GROUP BY pool, status"
                ).fetchall()
            }
        out = []
        for pool in pools:
            name = pool["pool"]
            out.append(
                {
                    "pool": name,
                    "max_parallel": pool["max_parallel"],
                    "paused": bool(pool["paused"]),
                    "queued": counts.get((name, "queued"), 0),
                    "running": counts.get((name, "running"), 0) + counts.get((name, "launching"), 0),
                    "updated_at": pool["updated_at"],
                }
            )
        return {"pools": out, "count": len(out)}

    def job_pause(self, job_id: str) -> dict[str, Any]:
        """Hold a queued job so the dispatcher will not launch it."""
        self._ensure_open()
        with self.db_lock:
            changed = self.db.execute(
                "UPDATE jobs SET paused=1, updated_at=? WHERE job_id=? AND status='queued' AND paused=0",
                (now_iso(), job_id),
            ).rowcount
            self.db.commit()
        if changed:
            return {"result": "ok", "job_id": job_id, "paused": True}
        row = self._row("SELECT status, paused FROM jobs WHERE job_id=?", (job_id,))
        if not row:
            raise ValueError(f"Unknown job_id: {job_id}")
        if row["status"] != "queued":
            raise ValueError(f"only a queued job can be paused (status={row['status']})")
        return {"result": "ok", "job_id": job_id, "paused": True}

    def job_resume(self, job_id: str) -> dict[str, Any]:
        """Release a paused queued job back to the dispatcher."""
        self._ensure_open()
        with self.db_lock:
            changed = self.db.execute(
                "UPDATE jobs SET paused=0, updated_at=? WHERE job_id=? AND status='queued' AND paused=1",
                (now_iso(), job_id),
            ).rowcount
            self.db.commit()
        if changed:
            return {"result": "ok", "job_id": job_id, "paused": False}
        row = self._row("SELECT status, paused FROM jobs WHERE job_id=?", (job_id,))
        if not row:
            raise ValueError(f"Unknown job_id: {job_id}")
        if row["status"] != "queued":
            raise ValueError(f"only a queued job can be resumed (status={row['status']})")
        return {"result": "ok", "job_id": job_id, "paused": False}

    def _running_count(self) -> int:
        # Direct starts are inserted 'launching' and promoted by the runner
        # (review rc36 P1); both states occupy the concurrent-job quota.
        with self.db_lock:
            return self.db.execute("SELECT COUNT(*) FROM jobs WHERE status IN ('running','launching')").fetchone()[0]

    def _alert_loop(self) -> None:
        try:
            interval = max(1.0, float(os.environ.get("VANTH_ALERT_INTERVAL", "30")))
        except ValueError:
            interval = 30.0
        while not self.dispatcher_stop.wait(interval):
            try:
                self._check_alerts()
            except Exception:
                self.logger.exception("alert check failed")

    def _check_alerts(self) -> None:
        """Edge-triggered operator alerts to ``VANTH_ALERT_WEBHOOK`` (review B3).

        Fires only when a condition transitions, so a persistent problem alerts
        once (and recovery alerts once), not every maintenance tick.
        """
        url = os.environ.get("VANTH_ALERT_WEBHOOK", "").strip()
        if not url:
            return
        try:
            interval = max(1.0, float(os.environ.get("VANTH_ALERT_INTERVAL", "30")))
        except ValueError:
            interval = 30.0
        now = time.monotonic()
        if self._last_alert_check is not None and now - self._last_alert_check < interval:
            return
        self._last_alert_check = now
        try:
            conditions = self._alert_conditions()
        except Exception:
            self.logger.exception("alert condition evaluation failed")
            return
        for key, condition in conditions.items():
            if condition["active"] == self._alert_state.get(key, False):
                continue
            try:
                self._post_alert(url, key, condition)
            except Exception:
                # Do NOT advance the state on a failed send: the transition is
                # retried on the next pass instead of being silently lost.
                self.logger.exception("alert delivery failed condition=%s", key)
                continue
            self._alert_state[key] = condition["active"]

    def _recent_jobs_without_wake(self, window_seconds: int = 86400) -> int:
        """Jobs created in the window with no wake target (polling-only).

        A visibility signal, not an error: agents that never attach a wake must
        use ``job_wait``/``job_status`` and can miss outcomes across turns.
        """
        if window_seconds <= 0:
            return 0
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=window_seconds)).isoformat().replace("+00:00", "Z")
        with self.db_lock:
            return int(
                self.db.execute(
                    "SELECT COUNT(*) FROM jobs j WHERE j.created_at >= ? AND NOT EXISTS "
                    "(SELECT 1 FROM wake_targets w WHERE w.job_id = j.job_id)",
                    (cutoff,),
                ).fetchone()[0]
            )

    def _dead_letter_count(self) -> int:
        """Truly exhausted deliveries, plus wakes the dispatch loop expired.

        A bare ``status='failed'`` also matches transient failures awaiting retry
        and administrative drains, so it overcounts (review P2). ``attempts >=
        max_attempts`` covers retry exhaustion; ``last_error`` beginning with
        ``expired:`` covers a wake no relay ever picked up (attempts stays 0).
        """
        with self.db_lock:
            return int(
                self.db.execute(
                    "SELECT COUNT(*) FROM deliveries WHERE status='failed' "
                    "AND (attempts >= COALESCE(json_extract(payload_json, '$.target.max_attempts'), 1) "
                    "OR last_error LIKE ?)",
                    (f"{EXPIRED_DELIVERY_ERROR}%",),
                ).fetchone()[0]
            )

    def _alert_conditions(self) -> dict[str, dict[str, Any]]:
        failed = self._dead_letter_count()
        try:
            threshold = int(os.environ.get("VANTH_ALERT_DISK_FREE_BYTES", "0"))
        except ValueError:
            threshold = 0
        free = shutil.disk_usage(self.home).free
        return {
            "dead_letters": {
                "active": failed > 0,
                "severity": "critical" if failed > 0 else "ok",
                "message": f"{failed} dead-lettered deliveries" if failed else "dead-letter queue empty",
                "details": {"failed_deliveries": failed},
            },
            "disk_low": {
                "active": bool(threshold and free < threshold),
                "severity": "warning",
                "message": f"free disk {free} below threshold {threshold}" if threshold and free < threshold else "disk free within threshold",
                "details": {"free_bytes": free, "threshold_bytes": threshold},
            },
        }

    def _post_alert(self, url: str, key: str, condition: dict[str, Any]) -> None:
        check_outbound_url(url)
        body = json.dumps(
            {
                "type": "vanth_alert",
                "condition": key,
                "active": condition["active"],
                "severity": condition["severity"],
                "message": condition["message"],
                "details": condition.get("details") or {},
                "at": now_iso(),
            },
            separators=(",", ":"),
        ).encode("utf-8")
        request = urllib.request.Request(
            url, data=body, headers={"Content-Type": "application/json"}, method="POST"
        )
        with _NO_REDIRECT_OPENER.open(request, timeout=5) as response:
            if response.status not in (200, 201, 202, 204):
                raise RuntimeError(f"alert webhook returned HTTP {response.status}")

    def _maybe_auto_cleanup(self) -> dict[str, Any] | None:
        if self.max_retention_seconds <= 0:
            return
        if self._last_retention_run is not None and time.monotonic() - self._last_retention_run < self.retention_interval_seconds:
            return
        self._last_retention_run = time.monotonic()
        try:
            return self.cleanup(older_than_seconds=self.max_retention_seconds, dry_run=self.retention_dry_run)
        except Exception:
            self.logger.exception("automatic retention cleanup failed")
            return None

    def _dispatch_due_deliveries(self) -> None:
        self._ensure_open()
        with self.db_lock:
            rows = self.db.execute(
                """
                SELECT * FROM deliveries
                WHERE (status IN ('pending', 'retrying') AND (next_attempt_at IS NULL OR next_attempt_at <= ?))
                   OR (status='dispatching' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?)
                """,
                (now_iso(), now_iso()),
            ).fetchall()
        for row in rows:
            try:
                payload = json.loads(row["payload_json"] or "{}")
            except (TypeError, json.JSONDecodeError):
                payload = {}
            if payload.get("target", {}).get("auto_dispatch") is False:
                continue
            if row["target_type"] == "codex_desktop":
                # Desktop wake is relayed by the CLIENT (review rc36 P0); the
                # daemon never dispatches it (no CLI fallback, no second
                # app-server). The relay poll claims and acks these.
                continue
            with self._delivery_threads_lock:
                if len(self._delivery_threads) >= self.max_delivery_concurrency:
                    continue
                thread = threading.Thread(target=self._dispatch_delivery, args=(self._delivery_dict(row),), daemon=True)
                self._delivery_threads.add(thread)
            try:
                thread.start()
            except RuntimeError:
                # Can't start a thread: drop it from the in-flight set so it does
                # not permanently count against max_delivery_concurrency (the
                # worker's own finally-discard never runs).
                with self._delivery_threads_lock:
                    self._delivery_threads.discard(thread)
                self.logger.warning("delivery dispatch thread failed to start delivery_id=%s", row["delivery_id"])

    def close(self) -> None:
        with self._close_lock:
            if self._closed:
                return
            self.begin_shutdown()
            self._closed = True
            self.dispatcher_stop.set()
            if self.dispatcher_thread and self.dispatcher_thread is not threading.current_thread():
                self.dispatcher_thread.join(timeout=2)
            deadline = time.monotonic() + float(os.environ.get("VANTH_SHUTDOWN_TIMEOUT", "10"))
            with self._delivery_threads_lock:
                workers = list(self._delivery_threads)
            for thread in workers:
                remaining = max(0, deadline - time.monotonic())
                if remaining:
                    thread.join(timeout=remaining)
            with self.db_lock:
                try:
                    # Reclaim the WAL on a clean shutdown, but don't let a
                    # lingering cross-process reader stall close() for the full
                    # busy_timeout: shorten it for this final checkpoint.
                    self.db.execute("PRAGMA busy_timeout=1000")
                    self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                except sqlite3.Error:
                    pass
                self.db.close()
            for handler in self.logger.handlers[:]:
                handler.close()
                self.logger.removeHandler(handler)

    @property
    def specs_dir(self) -> Path:
        path = self.home / "specs"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _condition(self, job_id: str) -> threading.Condition:
        self.conditions.setdefault(job_id, threading.Condition())
        return self.conditions[job_id]

    def _row(self, sql: str, args: tuple[Any, ...]) -> sqlite3.Row | None:
        self._ensure_open()
        with self.db_lock:
            return self.db.execute(sql, args).fetchone()

    def _event_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "event_id": row["event_id"],
            "job_id": row["job_id"],
            "seq": row["seq"],
            "type": row["type"],
            "level": row["level"],
            "message": row["message"],
            "data": json.loads(row["data_json"] or "{}"),
            "source": row["source"],
            "created_at": row["created_at"],
        }

    def _emit(
        self,
        job_id: str,
        event_type: str,
        *,
        message: str | None = None,
        data: dict[str, Any] | None = None,
        level: str = "info",
        source: str = "server",
        mutate: Callable[[sqlite3.Connection], None] | None = None,
        exempt_from_cap: bool = False,
    ) -> dict[str, Any]:
        self._ensure_open()
        payload = normalize_event_payload({"type": event_type, "message": message, "data": data or {}, "level": level})
        data_json = json.dumps(payload["data"], separators=(",", ":"))
        if len(data_json.encode()) > self.max_event_bytes:
            payload["data"] = {"truncated": True, "max_bytes": self.max_event_bytes}
            payload["message"] = payload["message"] or "Event payload exceeded max bytes"
            payload["level"] = "warning"
            data_json = json.dumps(payload["data"], separators=(",", ":"))
        with self.db_lock:
            for attempt in range(10):
                try:
                    event = self._emit_transactional(
                        job_id, payload, data_json, event_type, level, source, message, mutate, exempt_from_cap
                    )
                    break
                except sqlite3.OperationalError as exc:
                    if "locked" not in str(exc).lower() or attempt == 9:
                        raise
                    self.logger.warning("event write contended, retrying job_id=%s attempt=%s", job_id, attempt + 1)
                    time.sleep(min(0.5, 0.05 * (attempt + 1)))
            else:  # pragma: no cover - loop always breaks
                raise RuntimeError("event write failed")
        if event is not None and event.get("persisted") is not False:
            self._append_event_mirror(event, job_id)
        with self._condition(job_id):
            self._condition(job_id).notify_all()
        return event

    def _emit_transactional(
        self,
        job_id: str,
        payload: dict[str, Any],
        data_json: str,
        event_type: str,
        level: str,
        source: str,
        message: str | None,
        mutate: Callable[[sqlite3.Connection], None] | None = None,
        exempt_from_cap: bool = False,
        transaction: bool = True,
    ) -> dict[str, Any]:
        """Persist a state mutation and its event + deliveries in ONE transaction.

        ``mutate`` runs inside the write transaction, after the event-cap check
        and before the event row is inserted, so a decision state change and the
        event/wake it owes either both land or neither does (a crash can never
        leave a resolved decision with no event, which a retry could not
        repair). ``mutate`` may raise ``_DecisionNoOp`` to abort cleanly.
        ``exempt_from_cap`` is set only by authoritative decision transitions.
        """
        if transaction:
            self.db.execute("BEGIN IMMEDIATE")
        try:
            if event_type not in TERMINAL_STATUSES and not exempt_from_cap:
                count = self.db.execute("SELECT COUNT(*) FROM events WHERE job_id=?", (job_id,)).fetchone()[0]
                if count >= self.max_events_per_job:
                    if transaction:
                        self.db.rollback()
                    if job_id not in self._events_truncated:
                        self._events_truncated.add(job_id)
                        self.logger.warning("structured event cap reached job_id=%s max_events=%s", job_id, self.max_events_per_job)
                    return {
                        "event_id": None,
                        "job_id": job_id,
                        "seq": count + 1,
                        "type": event_type,
                        "level": "warning",
                        "message": "Structured event cap reached",
                        "data": {"max_events": self.max_events_per_job, "truncated": True},
                        "source": source,
                        "created_at": now_iso(),
                        "persisted": False,
                    }
            if mutate is not None:
                try:
                    mutate(self.db)
                except _DecisionNoOp:
                    self.db.rollback()
                    return {
                        "event_id": None,
                        "job_id": job_id,
                        "seq": 0,
                        "type": event_type,
                        "level": level,
                        "message": message,
                        "data": payload["data"],
                        "source": source,
                        "created_at": now_iso(),
                        "persisted": False,
                        "noop": True,
                    }
            row = self.db.execute("SELECT COALESCE(MAX(seq), 0) + 1 AS seq FROM events WHERE job_id=?", (job_id,)).fetchone()
            seq = int(row["seq"])
            created_at = now_iso()
            event_id = "evt_" + uuid.uuid4().hex[:16]
            self.db.execute(
                """
                INSERT INTO events(event_id, job_id, seq, type, level, message, data_json, source, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    job_id,
                    seq,
                    payload["type"],
                    payload["level"],
                    payload["message"],
                    data_json,
                    source,
                    created_at,
                ),
            )
            event = {
                "event_id": event_id,
                "job_id": job_id,
                "seq": seq,
                "type": payload["type"],
                "level": payload["level"],
                "message": payload["message"],
                "data": payload["data"],
                "source": source,
                "created_at": created_at,
            }
            self._enqueue_deliveries_uncommitted(event)
            self._persist_metric_series_uncommitted(event)
            if transaction:
                self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return event

    def _emit_capture_batch(self, job_id: str, payloads: list[dict[str, Any]], source: str) -> None:
        """Commit available pipe events together; never hold a transaction while reading."""
        prepared = []
        for payload in payloads:
            encoded = json.dumps(payload["data"], separators=(",", ":"))
            if len(encoded.encode()) > self.max_event_bytes:
                payload["data"] = {"truncated": True, "max_bytes": self.max_event_bytes}
                payload["message"] = payload["message"] or "Event payload exceeded max bytes"
                payload["level"] = "warning"
                encoded = json.dumps(payload["data"], separators=(",", ":"))
            prepared.append((payload, encoded))
        for offset in range(0, len(prepared), 64):
            def persist():
                with self.db_lock:
                    self.db.execute("BEGIN IMMEDIATE")
                    try:
                        events = [self._emit_transactional(
                            job_id, payload, encoded, payload["type"], payload["level"], source,
                            payload["message"], transaction=False,
                        ) for payload, encoded in prepared[offset:offset + 64]]
                        self.db.commit()
                        return events
                    except BaseException:
                        self.db.rollback()
                        raise
            started = time.monotonic()
            contentions = self.event_contentions_by_job.get(job_id, 0)
            events = self._retry_locked(persist, event_job=job_id)
            elapsed = time.monotonic() - started
            for event in events:
                if event.get("persisted") is not False:
                    self._append_event_mirror(event, job_id)
            with self._condition(job_id):
                self._condition(job_id).notify_all()
            with self.db_lock:
                retry_count = self.event_contentions_by_job.get(job_id, 0) - contentions
                # Only emit a job event for *actual* lock contention. A slow but
                # uncontended batch (loaded CI runner, stalled disk) is logged,
                # not injected into the job's event stream, so it cannot perturb
                # sequence-sensitive consumers.
                report = retry_count > 0 and job_id not in self._contention_reported
                if report:
                    self._contention_reported.add(job_id)
            if report:
                self._emit_safely(job_id, "write_contended", message="Captured events waited for SQLite persistence",
                                  data={"retry_count": retry_count, "write_seconds": round(elapsed, 3)}, level="warning")
            elif elapsed > 1:
                self.logger.warning("event capture slow job_id=%s stream=%s seconds=%.3f", job_id, source, elapsed)

    def _append_event_mirror(self, event: dict[str, Any], job_id: str) -> None:
        try:
            row = self._row("SELECT events_path FROM jobs WHERE job_id=?", (job_id,))
            if not row:
                return
            with Path(row["events_path"]).open("a", encoding="utf-8") as f:
                f.write(json.dumps(event, separators=(",", ":")) + "\n")
        except OSError:
            self.logger.exception("event mirror write failed job_id=%s event_id=%s", job_id, event.get("event_id"))
        except (sqlite3.Error, RuntimeError):
            self.logger.exception("event mirror path lookup failed job_id=%s", job_id)

    def _enqueue_deliveries(self, event: dict[str, Any]) -> list[dict[str, Any]]:
        deliveries = self._enqueue_deliveries_uncommitted(event)
        self.db.commit()
        return deliveries

    def _enqueue_deliveries_uncommitted(self, event: dict[str, Any]) -> list[dict[str, Any]]:
        deliveries = []
        targets = self.db.execute("SELECT * FROM wake_targets WHERE job_id=?", (event["job_id"],)).fetchall()
        for target in targets:
            events = json.loads(target["events_json"] or "[]")
            if events and event["type"] not in events:
                continue
            delivery_id = "del_" + uuid.uuid4().hex[:16]
            payload = self._delivery_payload(event, target, delivery_id)
            self.db.execute(
                """
                INSERT OR IGNORE INTO deliveries(
                  delivery_id, event_id, target_id, job_id, target_type, status, payload_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
                """,
                (
                    delivery_id,
                    event["event_id"],
                    target["target_id"],
                    event["job_id"],
                    target["type"],
                    json.dumps(payload, separators=(",", ":")),
                    now_iso(),
                ),
            )
            row = self._row("SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,))
            if row:
                deliveries.append(self._delivery_dict(row))
        return deliveries

    def _is_finite_number(self, value: Any) -> bool:
        if isinstance(value, bool):
            return False
        if isinstance(value, (int, float)):
            try:
                number = float(value)
            except OverflowError:
                return False
            return number == number and number not in (float("inf"), float("-inf"))
        return False

    def _metric_x(self, data: dict[str, Any], seq: int) -> float:
        step = data.get("_step")
        if self._is_finite_number(step):
            return float(step)
        return float(seq)

    def _persist_metric_series_uncommitted(self, event: dict[str, Any]) -> None:
        """Mirror scalar fields of metric/progress events into metric_series.

        Mirrors the Go monitor's transform: numeric `metric` payload fields
        become series named after the field; `progress` events produce
        ``progress.current`` / ``progress.total`` / ``progress.percent``.
        ``_step`` (when finite numeric) is the x value, otherwise the event
        sequence. Keys prefixed with ``_`` and the ``stage``/``phase`` keys are
        skipped as series names.
        """
        if event.get("persisted") is False or not event.get("event_id"):
            return
        event_type = event["type"]
        data = event["data"]
        if event_type not in ("metric", "progress"):
            return
        stage = data.get("stage") or data.get("phase")
        if not isinstance(stage, str):
            stage = None
        rows: list[tuple[str, str, float, float]] = []
        if event_type == "metric":
            for key, value in data.items():
                if key.startswith("_") or key in ("stage", "phase"):
                    continue
                if not self._is_finite_number(value):
                    continue
                rows.append((key, float(value), float(self._metric_x(data, event["seq"])), stage))
        else:
            current = data.get("current")
            total = data.get("total")
            percent = data.get("percent")
            if self._is_finite_number(current):
                rows.append(("progress.current", float(current), float(self._metric_x(data, event["seq"])), stage))
            if self._is_finite_number(total):
                rows.append(("progress.total", float(total), float(self._metric_x(data, event["seq"])), stage))
            if self._is_finite_number(percent):
                rows.append(("progress.percent", float(percent), float(self._metric_x(data, event["seq"])), stage))
        x = float(self._metric_x(data, event["seq"]))
        for metric, y, seq_value, series_stage in rows:
            series_id = "ser_" + uuid.uuid4().hex[:16]
            self.db.execute(
                """
                INSERT INTO metric_series(series_id, job_id, metric, x, y, stage, event_id, seq, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    series_id,
                    event["job_id"],
                    metric,
                    x,
                    y,
                    series_stage,
                    event["event_id"],
                    event["seq"],
                    event["created_at"],
                ),
            )
        if rows:
            self.logger.debug("persisted %d metric points job_id=%s event_id=%s", len(rows), event["job_id"], event["event_id"])

    def _claim_delivery(self, delivery_id: str, *, claim_client_id: str | None = None) -> dict[str, Any] | None:
        # Retry the whole BEGIN IMMEDIATE transaction on transient cross-process
        # contention. The rollback runs while holding db_lock (so it can never
        # discard another thread's in-flight transaction) and before the retry.
        def attempt() -> dict[str, Any] | None:
            with self.db_lock:
                try:
                    return self._claim_delivery_locked(delivery_id, claim_client_id=claim_client_id)
                except sqlite3.Error:
                    self.db.rollback()
                    raise

        return self._retry_locked(attempt)

    def _claim_delivery_locked(self, delivery_id: str, *, claim_client_id: str | None = None) -> dict[str, Any] | None:
        with self.db_lock:
            self.db.execute("BEGIN IMMEDIATE")
            now = now_iso()
            row = self.db.execute("SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,)).fetchone()
            if not row:
                self.db.rollback()
                return None
            target = json.loads(row["payload_json"] or "{}").get("target", {})
            # The lease must cover the adapter's effective timeout (review
            # P1-3). Thread bridges wait up to 300s by default, so a 30s lease
            # would let the dispatcher reclaim and re-send a wake whose turn is
            # still running. Per-target timeout_seconds overrides; thread
            # targets without one use the bridge default (300).
            target_type = row["target_type"]
            default_timeout = 300 if target_type in {"codex_thread", "codex_cli_thread", "codex_desktop", "opencode_thread"} else 30
            timeout = int(target.get("timeout_seconds", default_timeout))
            if target_type == "codex_desktop":
                # Desktop wake lease is derived from the SAME end-to-end deadline
                # as the helper subprocess (review rc39 P1): the helper hard
                # deadline + margin. Because the helper deadline already includes
                # the process buffer, lease > helper holds for every margin>=1 —
                # a stalled helper can never outlive its claim.
                from .codex_pipe import claim_lease_seconds

                lease_seconds = claim_lease_seconds(timeout, self.delivery_lease_margin)
            else:
                lease_seconds = timeout + max(1, self.delivery_lease_margin)
            due = (
                row["status"] in {"pending", "retrying"}
                and (row["next_attempt_at"] is None or row["next_attempt_at"] <= now)
            ) or (row["status"] == "dispatching" and row["lease_expires_at"] and row["lease_expires_at"] <= now)
            if not due:
                self.db.rollback()
                return None
            token = secrets.token_urlsafe(24)
            lease_expires = (datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)).isoformat().replace("+00:00", "Z")
            attempt = int(row["attempts"]) + 1
            reclaimed = int(row["status"] == "dispatching")
            # claim_client_id binds the claim to a specific relay so only that
            # client can ack it (review rc37 P1). The daemon dispatcher passes
            # None (no relay binding).
            changed = self.db.execute(
                """
                UPDATE deliveries SET status='dispatching', attempts=?, claim_token=?, claimed_at=?, lease_expires_at=?,
                  claim_client_id=?
                WHERE delivery_id=? AND (status IN ('pending','retrying') OR (status='dispatching' AND lease_expires_at<=?))
                """,
                (attempt, token, now, lease_expires, claim_client_id, delivery_id, now),
            ).rowcount
            if not changed:
                self.db.rollback()
                return None
            if reclaimed:
                self.db.execute(
                    "UPDATE delivery_attempts SET status='reclaimed', ended_at=?, error=? WHERE delivery_id=? AND ended_at IS NULL",
                    (now, "delivery lease expired", delivery_id),
                )
            self.db.execute(
                """
                INSERT INTO delivery_attempts(attempt_id, delivery_id, attempt, claim_token, target_type,
                  started_at, status, reclaimed, created_at)
                VALUES (?, ?, ?, ?, ?, ?, 'dispatching', ?, ?)
                """,
                (
                    "att_" + uuid.uuid4().hex[:16], delivery_id, attempt, token,
                    row["target_type"], now, reclaimed, now,
                ),
            )
            self.db.commit()
            claimed = self.db.execute("SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,)).fetchone()
        return self._delivery_dict(claimed)

    def _dispatch_delivery(self, delivery: dict[str, Any]) -> None:
        try:
            target = delivery["payload"].get("target", {})
            command = target.get("command")
            target_type = target.get("type")
            if target_type == "codex_desktop":
                # Desktop wake is delivered by the CLIENT-side relay (review rc36
                # P0), never by the daemon: the daemon does not know the rotating
                # CODEX_APP_TOOLS_PIPE_PATH and must not spawn a second
                # app-server. The relay poll claims and acks these. If no relay
                # is subscribed, the delivery stays pending until one connects
                # (or fails the lease and is retried); it is never routed to the
                # CLI thread bridge.
                return
            if not command and target_type in {"codex_cli_thread", "codex_thread", "opencode_thread"} and target.get("auto_dispatch") is False:
                return
            if not command and target_type == "opencode_thread" and not target.get("attach"):
                # Delivered by the in-process OpenCode plugin relay over the same
                # client-side lease protocol as codex_desktop. The daemon must NOT
                # spawn `opencode run --session` here: without --attach that starts
                # an isolated backend whose writes the live TUI never sees (review
                # P0-3). With no relay subscribed the delivery stays pending until
                # one connects, then is retried/claimed.
                return
            if not command and target_type not in {"codex_cli_thread", "codex_thread", "opencode_thread", "webhook"}:
                return
            delivery = self._claim_delivery(delivery["delivery_id"])
            if not delivery:
                return
            payload = delivery["payload"]
            if not command and target_type == "webhook":
                try:
                    self._dispatch_webhook(payload)
                    self._complete_delivery(delivery, "delivered")
                except Exception as exc:
                    self._complete_delivery(delivery, "failed", str(exc))
                return
            if not command and target_type in {"codex_cli_thread", "codex_thread"}:
                try:
                    send_delivery_to_codex(payload)
                    self._complete_delivery(delivery, "delivered")
                except CodexActiveWriterError as exc:
                    # Desktop owns the active writer: permanently non-retryable.
                    # (review P0-2).
                    effective_delivery = {
                        **delivery,
                        "payload": {
                            **delivery["payload"],
                            "target": {
                                **delivery["payload"].get("target", {}),
                                "max_attempts": 1,
                            },
                        },
                    }
                    self._complete_delivery(effective_delivery, "failed", str(exc))
                except Exception as exc:
                    self._complete_delivery(delivery, "failed", str(exc))
                return
            if not command and target_type == "opencode_thread":
                try:
                    send_delivery_to_opencode(payload)
                    self._complete_delivery(delivery, "delivered")
                except OpenCodeSessionNotFound as exc:
                    effective_delivery = {
                        **delivery,
                        "payload": {
                            **delivery["payload"],
                            "target": {
                                **delivery["payload"].get("target", {}),
                                "max_attempts": 1,
                            },
                        },
                    }
                    self._complete_delivery(effective_delivery, "failed", str(exc))
                except Exception as exc:
                    self._complete_delivery(delivery, "failed", str(exc))
                return
            try:
                proc = subprocess.run(
                    command,
                    input=json.dumps(payload),
                    text=True,
                    shell=isinstance(command, str),
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                    timeout=int(target.get("timeout_seconds", 30)),
                )
                if proc.returncode == 0:
                    self._complete_delivery(delivery, "delivered")
                else:
                    self._complete_delivery(delivery, "failed", (proc.stderr or "").strip())
            except Exception as exc:
                self._complete_delivery(delivery, "failed", str(exc))
        finally:
            with self._delivery_threads_lock:
                self._delivery_threads.discard(threading.current_thread())

    def _complete_delivery(
        self,
        delivery: dict[str, Any],
        status: str,
        error: str | None = None,
        *,
        require_claim_client_id: str | None = None,
    ) -> dict[str, Any]:
        """Complete a dispatching delivery with an atomic ownership CAS.

        The ownership condition is part of the SAME single UPDATE (no
        separate SELECT-then-UPDATE window), so a lease reclaimed between
        validation and completion is impossible. When ``require_claim_client_id``
        is given, the CAS also requires ``claim_client_id=?`` (the relay ack
        path, review rc39 P1) and zero affected rows raises — the caller must
        not report success for a delivery it no longer owns.
        """
        if not isinstance(status, str) or status not in {"delivered", "failed"}:
            raise ValueError("delivery completion status must be delivered or failed")
        target = delivery["payload"].get("target", {})
        attempt = int(delivery["attempts"])
        final_status = status
        next_attempt_at = None
        if status == "failed" and attempt < int(target.get("max_attempts", 1)):
            # Backoff grows per attempt (5s, 15s, 45s... capped at 5 min) so a
            # busy target session gets progressively longer to finish its
            # current turn instead of being retried into the same wall.
            delay = min(int(target.get("retry_delay_seconds", 5)) * (3 ** (attempt - 1)), 300)
            final_status = "retrying"
            next_attempt_at = (datetime.now(timezone.utc) + timedelta(seconds=delay)).isoformat().replace("+00:00", "Z")
        delivered_at = now_iso() if final_status == "delivered" else None
        error = (error or "")[:DEFAULT_MAX_ERROR_BYTES] or None
        with self.db_lock:
            stamp = now_iso()
            if require_claim_client_id is not None:
                changed = self.db.execute(
                    """
                    UPDATE deliveries SET status=?, delivered_at=COALESCE(?, delivered_at), last_error=?, next_attempt_at=?,
                      claim_token=NULL, claimed_at=NULL, lease_expires_at=NULL, claim_client_id=NULL
                    WHERE delivery_id=? AND status='dispatching' AND claim_token=? AND claim_client_id=?
                    """,
                    (
                        final_status, delivered_at, error, next_attempt_at,
                        delivery["delivery_id"], delivery["claim_token"], require_claim_client_id,
                    ),
                ).rowcount
            else:
                changed = self.db.execute(
                    """
                    UPDATE deliveries SET status=?, delivered_at=COALESCE(?, delivered_at), last_error=?, next_attempt_at=?,
                      claim_token=NULL, claimed_at=NULL, lease_expires_at=NULL, claim_client_id=NULL
                    WHERE delivery_id=? AND status='dispatching' AND claim_token=?
                    """,
                    (final_status, delivered_at, error, next_attempt_at, delivery["delivery_id"], delivery["claim_token"]),
                ).rowcount
            if changed:
                self.db.execute(
                    """
                    UPDATE delivery_attempts SET status=?, error=?, ended_at=?
                    WHERE delivery_id=? AND claim_token=? AND ended_at IS NULL
                    """,
                    (final_status, error, stamp, delivery["delivery_id"], delivery["claim_token"]),
                )
            self.db.commit()
            if require_claim_client_id is not None and not changed:
                raise ValueError(
                    "delivery is not claimed by this relay (lease reclaimed, delivered, or owned by another client)"
                )
            row = self.db.execute("SELECT * FROM deliveries WHERE delivery_id=?", (delivery["delivery_id"],)).fetchone()
        return self._delivery_dict(row)

    def _dispatch_webhook(self, payload: dict[str, Any]) -> None:
        """POST a delivery payload to an HTTP(S) webhook endpoint.

        Raises on non-2xx status or transport error; the caller marks the
        delivery failed (and retries per max_attempts/retry_delay_seconds).
        Redirects are NOT followed (review P1-4): urllib's default redirect
        handler would forward configured headers — including ``Authorization``
        — to a cross-origin destination. A 3xx here fails the delivery instead
        of leaking credentials. The JSON body omits configured header secrets.
        """
        target = payload["target"]
        url = target["url"]
        # Re-check at delivery time (policy may have changed and the host is
        # resolved fresh); a denied destination fails the delivery, never leaks.
        check_outbound_url(url)
        headers = dict(target.get("headers") or {})
        headers.setdefault("Content-Type", "application/json")
        # Never duplicate configured header secrets into the JSON payload.
        body_payload = dict(payload)
        body_payload["target"] = dict(target)
        body_payload["target"].pop("headers", None)
        body = json.dumps(body_payload, separators=(",", ":")).encode("utf-8")
        request = urllib.request.Request(
            url,
            data=body,
            headers=headers,
            method="POST",
        )
        timeout = float(target.get("timeout_seconds", 30))
        try:
            # Disable automatic redirects: cross-origin redirects could exfiltrate
            # the configured headers (Authorization bearer tokens, service tokens).
            with _NO_REDIRECT_OPENER.open(request, timeout=timeout) as response:
                status = response.getcode()
        except urllib.error.HTTPError as exc:
            if exc.code in (301, 302, 303, 307, 308):
                raise RuntimeError(f"webhook redirect not followed (HTTP {exc.code}); credentials would leak cross-origin") from exc
            raise RuntimeError(f"webhook returned HTTP {exc.code}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"webhook request failed: {exc.reason}") from exc
        if status in (301, 302, 303, 307, 308):
            raise RuntimeError(f"webhook redirect not followed (HTTP {status}); credentials would leak cross-origin")
        if status not in (200, 201, 202, 204):
            raise RuntimeError(f"webhook returned HTTP {status}")

    def _delivery_payload(self, event: dict[str, Any], target: sqlite3.Row, delivery_id: str) -> dict[str, Any]:
        config = json.loads(target["config_json"] or "{}")
        prompt = config.get("prompt")
        if not prompt:
            prompt = (
                "vanth event\n"
                f"delivery_id: {delivery_id}\n"
                f"job_id: {event['job_id']}\n"
                f"event: {event['type']}\n"
                f"message: {event.get('message') or ''}\n"
                f"data: {json.dumps(event.get('data') or {}, separators=(',', ':'))}\n\n"
                "Continue from this event. Use vanth job_status/job_events/job_tail for details instead of polling."
            )
        # Canonicalize the target key once so the persisted payload's thread id
        # is always `thread_id` (the relay SQL filter reads only that path).
        target_dict = canonicalize_wake_target({"type": target["type"], **config})
        return {
            "target": target_dict,
            "event": event,
            "prompt": prompt,
            "delivery_id": delivery_id,
        }

    # --- Client relay (review rc36 P0) ---
    # Codex Desktop wake is delivered by an outbound relay owned by the CLIENT
    # integration (the MCP process that inherited CODEX_APP_TOOLS_PIPE_PATH),
    # never by the persistent daemon. The daemon stores the durable deliveries
    # and exposes them to the relay via long-poll; the relay acknowledges only
    # after the client-native prompt admission succeeds. A disconnect leaves the
    # delivery pending; a reconnect resumes from the last acknowledged
    # delivery id. This keeps the rotating Desktop pipe entirely inside the
    # Codex MCP process.

    def relay_register(self, client_id: str, client_type: str, destinations: list[dict[str, Any]]) -> dict[str, Any]:
        """Register (or refresh) a client relay subscription.

        ``destinations`` is a list of identities the client can wake: for
        ``codex_desktop`` a ``{"thread_id": ...}``, for ``opencode_thread`` a
        ``{"session_id": ..., "directory": ...}``. The daemon uses it only to
        route relayed deliveries; the client's pipe/credentials/URLs never leave
        the client process.
        """
        self._ensure_open()
        if not isinstance(client_id, str) or not client_id:
            raise ValueError("client_id must be a non-empty string")
        if client_type not in RELAY_CLIENT_TYPES:
            raise ValueError(
                f"unsupported relay client type: {client_type!r} (expected one of {sorted(RELAY_CLIENT_TYPES)})"
            )
        if not isinstance(destinations, list):
            raise ValueError("destinations must be a list")
        if not all(isinstance(item, dict) for item in destinations):
            raise ValueError("destinations must be a list of objects")
        now = now_iso()
        with self.db_lock:
            self.db.execute(
                """
                INSERT INTO relay_subscriptions(client_id, client_type, destinations_json, updated_at, last_poll_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(client_id) DO UPDATE SET
                  client_type=excluded.client_type,
                  destinations_json=excluded.destinations_json,
                  updated_at=excluded.updated_at,
                  last_poll_at=excluded.last_poll_at
                """,
                (client_id, client_type, json.dumps(destinations, separators=(",", ":")), now, now),
            )
            self.db.commit()
        return {"result": "ok", "client_id": client_id, "client_type": client_type}

    def relay_unregister(self, client_id: str) -> dict[str, Any]:
        self._ensure_open()
        with self.db_lock:
            self.db.execute("DELETE FROM relay_subscriptions WHERE client_id=?", (client_id,))
            self.db.commit()
        return {"result": "ok", "client_id": client_id}

    def _relay_heartbeat_seconds(self) -> float:
        try:
            value = float(os.environ.get("VANTH_RELAY_POLL_HEARTBEAT_SECONDS", "5"))
        except ValueError:
            return 5.0
        # Clamp below the stale/TTL windows so a large value can never make an
        # actively polling relay look dead or expire its own subscription.
        if not math.isfinite(value) or value <= 0:
            return 5.0
        return min(value, _MAX_RELAY_POLL_HEARTBEAT)

    def _touch_relay_subscription(self, client_id: str) -> None:
        # Called only when the stored last_poll_at is already stale, so the
        # UPDATE is unconditional here (the caller gates it).
        with self.db_lock:
            try:
                self.db.execute(
                    "UPDATE relay_subscriptions SET last_poll_at=? WHERE client_id=?",
                    (now_iso(), client_id),
                )
                self.db.commit()
            except sqlite3.Error:
                self.db.rollback()
                raise

    def relay_poll(self, client_id: str, timeout_seconds: float = 30.0) -> list[dict[str, Any]]:
        """Long-poll for due codex_desktop deliveries addressed to this client.

        Returns a list of delivery dicts (each carries the full payload AND an
        opaque ``lease_token``). The relay claims each one (marks it
        ``dispatching`` with a lease bound to this client) so a delivery is only
        handed to ONE relay at a time. The relay delivers via the pipe and calls
        ``relay_ack`` with BOTH its client_id and the opaque lease token — only
        the claiming client can complete a delivery (review rc37 P1). Polls
        block up to ``timeout_seconds`` (bounded) so a client can reconnect with
        backoff. Stale subscriptions (no poll within the grace window) are
        expired server-side.
        """
        self._ensure_open()
        client_row = self._row(
            "SELECT client_id, client_type, destinations_json, last_poll_at FROM relay_subscriptions WHERE client_id=?",
            (client_id,),
        )
        if not client_row:
            raise ValueError(f"Unknown relay client_id: {client_id}")
        client_type = client_row["client_type"]
        identity_keys = RELAY_IDENTITY_KEYS.get(client_type)
        if identity_keys is None:
            raise ValueError(f"unknown relay client type: {client_type!r}")
        try:
            destinations = json.loads(client_row["destinations_json"] or "[]")
        except (TypeError, ValueError):
            destinations = []
        # Collect destinations under EVERY identity alias the client type accepts,
        # not just the canonical one: a relay that registered with the legacy
        # alias would otherwise poll with an empty identity set and never be
        # offered its deliveries (silent permanent pending).
        identities: set[str] = set()
        for item in destinations:
            if not isinstance(item, dict):
                continue
            for key in identity_keys:
                value = item.get(key)
                if isinstance(value, str) and value:
                    identities.add(value)
        # Update last_poll_at for liveness tracking, but only once per heartbeat
        # interval: a no-match UPDATE still takes the write lock, so skipping it
        # entirely when the stored value is fresh is what actually reduces
        # write-lock pressure. The write is retried on transient contention.
        heartbeat_cutoff = (datetime.now(timezone.utc) - timedelta(seconds=self._relay_heartbeat_seconds())).isoformat().replace("+00:00", "Z")
        if not client_row["last_poll_at"] or client_row["last_poll_at"] < heartbeat_cutoff:
            self._retry_locked(self._touch_relay_subscription, client_id)
        deadline = time.monotonic() + max(1.0, min(timeout_seconds, 60.0))
        poll_interval = float(os.environ.get("VANTH_RELAY_POLL_INTERVAL", "0.5"))
        while True:
            due = self._relay_due_deliveries(
                identities, target_type=client_type, identity_keys=identity_keys, claim_client_id=client_id
            )
            if due:
                return due
            if time.monotonic() >= deadline:
                return []
            time.sleep(poll_interval)

    def _relay_due_deliveries(
        self,
        identities: set[str],
        *,
        target_type: str = "codex_desktop",
        identity_keys: tuple[str, ...] = ("thread_id", "threadId"),
        claim_client_id: str | None = None,
    ) -> list[dict[str, Any]]:
        now = now_iso()
        if not identities or not identity_keys:
            return []
        # Build the SQL so destinations are filtered BEFORE ORDER BY/LIMIT,
        # otherwise 20 older deliveries for OTHER destinations could starve a
        # matching delivery forever (review rc37 P1). The identity field differs
        # per relay client type, so it comes from RELAY_IDENTITY_KEYS rather than
        # being hardcoded (codex_desktop thread_id vs opencode_thread session_id).
        placeholders = ",".join("?" for _ in identities)
        identity_predicate = " OR ".join(
            f"json_extract(payload_json, '$.target.{key}') IN ({placeholders})" for key in identity_keys
        )
        with self.db_lock:
            rows = self.db.execute(
                f"""
                SELECT * FROM deliveries
                WHERE target_type=?
                  AND ({identity_predicate})
                  AND ((status IN ('pending', 'retrying') AND (next_attempt_at IS NULL OR next_attempt_at <= ?))
                       OR (status='dispatching' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?))
                ORDER BY created_at
                LIMIT 20
                """,
                (target_type, *([*identities] * len(identity_keys)), now, now),
            ).fetchall()
        claimed = []
        for row in rows:
            delivery = self._claim_delivery(row["delivery_id"], claim_client_id=claim_client_id)
            if delivery:
                # Add an opaque lease token: the claim_token stored in the row is
                # opaque to the client; only relay_ack (which CAS's on it server
                # side) may use it.
                delivery["lease_token"] = delivery["claim_token"]
                claimed.append(delivery)
        return claimed

    def _delivery_thread_target(self, row: sqlite3.Row) -> dict[str, Any]:
        try:
            payload = json.loads(row["payload_json"] or "{}")
        except (TypeError, json.JSONDecodeError):
            payload = {}
        target = payload.get("target") or {}
        thread_id = target.get("thread_id") or target.get("threadId")
        return {"thread_id": thread_id}

    def relay_ack(self, client_id: str, delivery_id: str, status: str, error: str | None = None, lease_token: str | None = None) -> dict[str, Any]:
        """Acknowledge a relayed delivery after client-native admission.

        ``status`` is ``delivered`` (the prompt was accepted/processed) or
        ``failed``. On ``failed`` the normal retry/backoff semantics apply.

        The acknowledgement is OWNED by the claiming client (review rc37 P1):
        it CAS's on ``status='dispatching'``, the stored ``claim_token`` (the
        opaque ``lease_token`` returned by ``relay_poll``), AND ``claim_client_id``
        matching the caller. A different relay — or the same relay after its
        lease was reclaimed — affects zero rows. The current claim token is never
        reloaded on behalf of the acknowledger.
        """
        self._ensure_open()
        if not isinstance(status, str) or status not in {"delivered", "failed"}:
            raise ValueError("delivery completion status must be delivered or failed")
        if not isinstance(lease_token, str) or not lease_token:
            raise ValueError("relay_ack requires the opaque lease_token returned by relay_poll")
        # ONE atomic CAS: the ownership check AND the completion are a single
        # guarded UPDATE (including claim_client_id) inside one db_lock. There
        # is no SELECT-then-UPDATE window in which another relay could reclaim
        # the lease, and a zero-row guard raises instead of silently reporting
        # success (review rc39 P1).
        with self.db_lock:
            row = self.db.execute(
                "SELECT * FROM deliveries WHERE delivery_id=? AND status='dispatching' AND claim_token=? AND claim_client_id=?",
                (delivery_id, lease_token, client_id),
            ).fetchone()
            if not row:
                raise ValueError(
                    "delivery is not claimed by this relay (lease reclaimed, delivered, or owned by another client)"
                )
            delivery = self._delivery_dict(row)
            self._complete_delivery(delivery, status, error, require_claim_client_id=client_id)
        return {"result": "ok", "delivery_id": delivery_id, "status": status}

    def relay_release(self, client_id: str, delivery_id: str, lease_token: str | None = None) -> dict[str, Any]:
        """Release a claimed delivery back to pending WITHOUT consuming an attempt.

        Used when the relay cannot deliver for environmental reasons (a Desktop
        restart invalidated the pipe) rather than a delivery failure: the wake
        must survive for a re-provisioned relay instead of being failed — a
        ``failed`` ack is terminal at the default ``max_attempts=1``
        (self-review rc40). Ownership-CAS'd on delivery_id + dispatching +
        claim_token + claim_client_id; zero rows raises. The cancelled claim is
        removed from attempt bookkeeping and ``next_attempt_at`` is cleared so
        the delivery is immediately due for the next poll.
        """
        self._ensure_open()
        if not isinstance(lease_token, str) or not lease_token:
            raise ValueError("relay_release requires the opaque lease_token returned by relay_poll")
        with self.db_lock:
            changed = self.db.execute(
                """
                UPDATE deliveries SET status='pending', attempts=MAX(0, attempts - 1), next_attempt_at=NULL,
                  claim_token=NULL, claimed_at=NULL, lease_expires_at=NULL, claim_client_id=NULL
                WHERE delivery_id=? AND status='dispatching' AND claim_token=? AND claim_client_id=?
                """,
                (delivery_id, lease_token, client_id),
            ).rowcount
            if changed:
                self.db.execute(
                    "DELETE FROM delivery_attempts WHERE delivery_id=? AND claim_token=? AND ended_at IS NULL",
                    (delivery_id, lease_token),
                )
            self.db.commit()
            if not changed:
                raise ValueError(
                    "delivery is not claimed by this relay (lease reclaimed, delivered, or owned by another client)"
                )
        return {"result": "ok", "delivery_id": delivery_id, "status": "pending"}

    def relay_expire_stale(self, stale_after_seconds: int = 300) -> int:
        """Expire relay subscriptions that have not polled within the window.

        Called from the dispatch loop so a crashed relay's registration does not
        linger forever (review rc37 P1). Returns the number of rows removed.
        """
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=stale_after_seconds)).isoformat().replace("+00:00", "Z")
        with self.db_lock:
            changed = self.db.execute(
                "DELETE FROM relay_subscriptions WHERE last_poll_at IS NULL OR last_poll_at < ?",
                (cutoff,),
            ).rowcount
            self.db.commit()
        return changed

    def _delivery_ttl_seconds(self) -> int:
        try:
            return int(os.environ.get("VANTH_DELIVERY_TTL_SECONDS", "21600"))
        except ValueError:
            return 21600

    def expire_stale_deliveries(self, ttl_seconds: int = 21600) -> int:
        """Fail deliveries that were never dispatched within ``ttl_seconds``.

        A relay-addressed wake (``opencode_thread``/``codex_*``) is only handed
        to a client when one polls with a matching destination. If no relay ever
        completes it the row would otherwise stay ``pending`` forever with no
        error, no retry, and no alarm. ``attempts = 0`` identifies rows that were
        never claimed by a relay (the daemon-side dispatch returns without
        claiming relay targets); ``relay_release`` can also return a row to 0, so
        the same 6h age limit applies uniformly. Called from the dispatch loop so
        those wakes resolve to ``failed`` and surface in dead-letter/doctor counts
        instead of leaking. Returns the number of rows expired.
        """
        if ttl_seconds <= 0:
            return 0
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=ttl_seconds)).isoformat().replace("+00:00", "Z")
        with self.db_lock:
            changed = self.db.execute(
                "UPDATE deliveries SET status='failed', last_error=?, next_attempt_at=NULL, "
                "claim_token=NULL, claimed_at=NULL, lease_expires_at=NULL, claim_client_id=NULL "
                "WHERE status IN ('pending','retrying') AND attempts=0 AND created_at < ?",
                (f"{EXPIRED_DELIVERY_ERROR} no relay ever completed this wake within {ttl_seconds}s", cutoff),
            ).rowcount
            self.db.commit()
        if changed:
            self.logger.info("expired %d never-dispatched delivery(ies) older than %ds", changed, ttl_seconds)
        return changed

    async def start(
        self,
        command: str,
        cwd: str | None = None,
        name: str | None = None,
        env: dict[str, str] | None = None,
        timeout_seconds: int | None = None,
        notify_on: list[str] | None = None,
        wake_targets: list[dict[str, Any]] | None = None,
        origin_thread_id: str | None = None,
        tags: list[str] | None = None,
        notes: str | None = None,
        interactive: bool = False,
        trigger: dict[str, Any] | None = None,
        policy: dict[str, Any] | None = None,
        secret_env: list[str] | None = None,
        pool: str | None = None,
        priority: int = 0,
        schedule_id: str | None = None,
        idempotency_key: str | None = None,
        wake_default: bool = False,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        self._ensure_open()
        if not isinstance(wake_default, bool):
            raise ValueError("wake_default must be a boolean")
        request_hash = None
        if cwd is not None:
            if not isinstance(cwd, str) or not cwd.strip():
                raise ValueError("cwd must be a non-empty string")
            cwd = str(Path(cwd).expanduser().resolve())
        if not isinstance(dry_run, bool):
            raise ValueError("dry_run must be a boolean")
        if idempotency_key is not None:
            if not isinstance(idempotency_key, str) or not re.fullmatch(r"[A-Za-z0-9_-]{8,128}", idempotency_key):
                raise ValueError("idempotency_key must be 8..128 letters, digits, underscores or hyphens")
            request_spec = {
                "command": command, "cwd": cwd or os.getcwd(), "name": name,
                "env": env, "timeout_seconds": timeout_seconds, "notify_on": notify_on,
                "wake_targets": wake_targets, "origin_thread_id": origin_thread_id,
                "tags": tags, "notes": notes, "interactive": interactive,
                "trigger": trigger, "policy": policy, "secret_env": secret_env,
                "pool": pool, "priority": priority, "schedule_id": schedule_id,
            }
            try:
                request_hash = hashlib.sha256(json.dumps(request_spec, sort_keys=True, allow_nan=False).encode()).hexdigest()
            except (TypeError, ValueError) as exc:
                raise ValueError("job request must contain JSON-compatible values") from exc
            if not dry_run:
                replay = self._replay_local_start(idempotency_key, request_hash)
                if replay is not None:
                    return replay
        # Thread identity (review P1-4 / P2-2): the LAUNCHING thread is the
        # default wake destination, but the daemon cannot know it. The MCP
        # wrapper (job_start) resolves origin_thread_id from the calling task's
        # environment and passes it explicitly; the daemon NEVER infers it
        # from its own (persistent) environment, which would inherit the thread
        # that originally spawned the daemon. Explicit ids in the target always
        # win, so agents can still fan out to other threads. Caller-owned
        # wake-target dicts are copied before any inherited id or event is
        # injected — the caller's objects are never mutated.
        # Validate field SHAPES before any side effect (metadata capture, DB
        # insert, runner launch). These are shared by the MCP tool and the HTTP
        # boundary, so a bad type can no longer be persisted and fail later
        # inside the runner (or become a sqlite binding error / 500).
        if not isinstance(command, str) or not command.strip():
            raise ValueError("command must be a non-empty string")
        if cwd is not None and (not isinstance(cwd, str) or not cwd.strip()):
            raise ValueError("cwd must be a non-empty string")
        if name is not None and not isinstance(name, str):
            raise ValueError("name must be a string")
        if notes is not None and not isinstance(notes, str):
            raise ValueError("notes must be a string")
        if env is not None and (
            not isinstance(env, dict)
            or not all(isinstance(key, str) and isinstance(value, str) for key, value in env.items())
        ):
            raise ValueError("env must be an object of string key/value pairs")
        if tags is not None and (not isinstance(tags, list) or not all(isinstance(tag, str) for tag in tags)):
            raise ValueError("tags must be a list of strings")
        if notify_on is not None and (
            not isinstance(notify_on, list) or not all(isinstance(event, str) for event in notify_on)
        ):
            raise ValueError("notify_on must be a list of strings")
        if not isinstance(interactive, bool):
            raise ValueError("interactive must be a boolean")
        if origin_thread_id is not None and not isinstance(origin_thread_id, str):
            raise ValueError("origin_thread_id must be a string")
        # Best-effort default wake: attach an opencode_thread wake for the
        # caller's project only when a live plugin relay resolves; otherwise
        # skip silently (never fail the start). Explicit wake targets below are
        # still validated strictly.
        if wake_default and wake_targets is None:
            candidate = [{
                "type": "opencode_thread",
                "events": ["completed", "failed", "timeout", "cancelled", "orphaned"],
                "cwd": cwd or os.getcwd(),
            }]
            session_id = self._sole_relay_session(cwd or os.getcwd())
            if session_id:
                candidate[0]["session_id"] = session_id
                wake_targets = candidate
        # Shape-check the container/elements BEFORE identity resolution, which
        # calls dict(target) and would raise a bare TypeError on a null/non-object
        # element (misclassified as a 500 instead of a field-level 400).
        if wake_targets is not None and (
            not isinstance(wake_targets, list)
            or not all(isinstance(target, dict) for target in wake_targets)
        ):
            raise ValueError("wake_targets must be a list of objects")
        if wake_targets is not None:
            wake_targets = resolve_wake_target_identity(wake_targets, origin_thread_id)
            # An opencode_thread target with no session_id is addressed to the
            # live plugin relay in this project (see _resolve_relay_sessions).
            self._resolve_relay_sessions(wake_targets, cwd)
            self._reject_relay_client_id_targets(wake_targets)
        # Apply notify_on defaults BEFORE validation so a target with no explicit
        # events inherits the notify_on list and is not rejected as empty.
        if notify_on:
            for target in wake_targets or []:
                if "events" not in target and "notify_on" not in target:
                    target["events"] = notify_on
        validate_wake_targets(wake_targets)
        if timeout_seconds is not None and (isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, int) or timeout_seconds < 1):
            raise ValueError("timeout_seconds must be an integer >= 1")
        trigger = self._validate_trigger(trigger)
        policy = validate_policy(policy)
        secret_env = self._validate_secret_env(secret_env)
        pool = self._validate_pool(pool)
        priority = self._validate_priority(priority)
        if dry_run:
            resolved_cwd = Path(cwd or os.getcwd()).expanduser().resolve()
            if not resolved_cwd.is_dir():
                raise ValueError(f"cwd does not exist or is not a directory: {resolved_cwd}")
            return {
                "result": "preview", "command": command,
                "shell": os.environ.get("COMSPEC", "cmd.exe") if sys.platform == "win32" else "/bin/sh",
                "cwd": str(resolved_cwd), "env_names": sorted(env or {}),
                "secret_env": secret_env, "timeout_seconds": timeout_seconds,
                "interactive": interactive, "trigger": trigger, "policy": policy,
                "pool": pool, "priority": priority,
                **self._start_extras(wake_targets, notify_on),
            }
        # A trigger OR a pool gates launch: the job is created 'queued' and the
        # single dispatcher launches it once the trigger fires and the pool has
        # capacity (and is not paused).
        queued = trigger is not None or pool is not None
        job_id = "job_" + uuid.uuid4().hex[:12]
        stdout_path = self.logs / f"{job_id}.stdout.log"
        stderr_path = self.logs / f"{job_id}.stderr.log"
        events_path = self.events_dir / f"{job_id}.jsonl"
        created_at = now_iso()
        # The wake identity is the session id for opencode_thread and the thread
        # id otherwise (both are canonicalized by _resolve_relay_sessions above),
        # so `list --thread-id` can find an opencode job by the session it wakes.
        wake_thread_id = next(
            (
                target.get("session_id") if target.get("type") == "opencode_thread" else target.get("thread_id")
                for target in (wake_targets or [])
                if target.get("type") in {"codex_thread", "codex_cli_thread", "codex_desktop", "opencode_thread"}
            ),
            None,
        )
        run_info = capture_run_metadata(cwd=cwd, notes=notes)
        run_payload = {**run_info, "interactive": interactive}
        # Every direct launch carries a claim token (review rc36 P1): the row is
        # inserted 'launching' with the token atomically, and the runner
        # promotes it to 'running' exactly like the prepare_launch path. There
        # is NO 'running' pre-spawn window with worker_pid IS NULL, so a second
        # manager's recovery can never orphan a pre-spawn row, and a stale
        # starter can never mark a newer run failed (all writes are claim-token
        # guarded).
        direct_claim_token = None if queued else "claim_" + uuid.uuid4().hex[:16]
        with self.db_lock:
            try:
                # Concurrent-job quota is enforced ATOMICALLY with the row insert
                # (review rc37 P1): BEGIN IMMEDIATE acquires the write lock before
                # the count, so the count and the INSERT are ONE transaction. Two
                # manager processes (or threads) synchronized at a SELECT-then-insert
                # can no longer both pass VANTH_MAX_RUNNING_JOBS=1 and create two
                # 'launching' rows. Both 'launching' and 'running' reservations count.
                if idempotency_key is not None or (self.max_running_jobs and not queued):
                    self.db.execute("BEGIN IMMEDIATE")
                if idempotency_key is not None:
                    replay = self._replay_local_start(idempotency_key, request_hash)
                    if replay is not None:
                        self.db.commit()
                        return replay
                if self.max_running_jobs and not queued:
                    reserved = self.db.execute(
                        "SELECT COUNT(*) FROM jobs WHERE status IN ('running','launching')"
                    ).fetchone()[0]
                    if reserved >= self.max_running_jobs:
                        raise ValueError(f"concurrent job quota reached ({self.max_running_jobs} running jobs)")
                self.db.execute(
                    """
                INSERT INTO jobs(job_id, name, command, cwd, status, created_at, updated_at, started_at, runner_heartbeat_at,
                  timeout_seconds, notify_on, origin_thread_id, wake_thread_id, tags_json, env_json, notes, run_json,
                  stdout_path, stderr_path, events_path, trigger_json, policy_json, claim_token, secret_env_json,
                  pool, priority, schedule_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        job_id,
                        name,
                        command,
                        cwd,
                        "queued" if queued else "launching",
                        created_at,
                        created_at,
                        None if queued else created_at,
                        None if queued else created_at,
                        timeout_seconds,
                        json.dumps(notify_on or []),
                        origin_thread_id,
                        wake_thread_id,
                        json.dumps(tags or [], separators=(",", ":")),
                        json.dumps(env or {}, separators=(",", ":")),
                        notes,
                        serialize_run_metadata(run_payload),
                        str(stdout_path),
                        str(stderr_path),
                        str(events_path),
                        json.dumps(trigger, separators=(",", ":")) if trigger else None,
                        json.dumps(policy, separators=(",", ":")) if policy else None,
                        direct_claim_token,
                        json.dumps(secret_env, separators=(",", ":")) if secret_env else None,
                        pool,
                        priority,
                        schedule_id,
                    ),
                )
                # The job row and its wake targets commit in ONE transaction: a
                # crash between the two would leave an accepted job whose
                # promised notifications were never registered, and the caller
                # cannot repair that (the job exists, so a retry would duplicate
                # it). Rollback on ANY failure so a half-written acceptance can
                # never be swept into the DB by a later commit.
                self._insert_wake_targets(job_id, wake_targets or [], created_at)
                if idempotency_key is not None:
                    self.db.execute(
                        "INSERT INTO local_start_requests VALUES (?, ?, ?, ?)",
                        (idempotency_key, request_hash, job_id, created_at),
                    )
                self.db.commit()
            except BaseException:
                self.db.rollback()
                raise
        if queued:
            gates = []
            if trigger and trigger.get("job_id"):
                gates.append(f"{trigger['job_id']} reaches {trigger['status']}")
            if trigger and trigger.get("probe"):
                gates.append(f"readiness probe {trigger['probe']['type']!r} passes")
            if pool:
                gates.append(f"pool {pool!r} has capacity")
            message = "Job queued; will start when " + (", ".join(gates) if gates else "its gates pass")
            return {
                "job_id": job_id,
                "status": "queued",
                "trigger": trigger,
                "pool": pool,
                "priority": priority,
                "stdout_path": str(stdout_path),
                "stderr_path": str(stderr_path),
                "events_path": str(events_path),
                "message": message,
                **self._start_extras(wake_targets, notify_on),
            }
        claim_spec_path = self._write_spec(
            job_id,
            {
                "command": command,
                "cwd": cwd,
                "env": env or {},
                "timeout_seconds": timeout_seconds,
                "max_log_bytes": self.max_log_bytes,
                "stdout_path": str(stdout_path),
                "stderr_path": str(stderr_path),
                "interactive": interactive,
                "claim_token": direct_claim_token,
                "secret_env": secret_env or [],
            },
            spec_name=f"{job_id}-{direct_claim_token}.json",
        )
        result = self._launch(
            job_id, stdout_path, stderr_path, events_path, claim_spec_path, claim_token=direct_claim_token
        )
        return {**result, **self._start_extras(wake_targets, notify_on)}

    def _replay_local_start(self, key: str, request_hash: str) -> dict[str, Any] | None:
        with self.db_lock:
            request = self.db.execute(
                "SELECT request_hash, job_id FROM local_start_requests WHERE idempotency_key=?", (key,)
            ).fetchone()
            if request is None:
                return None
            if request["request_hash"] != request_hash:
                raise ValueError("idempotency_key was already used for a different job request")
            row = self.db.execute("SELECT * FROM jobs WHERE job_id=?", (request["job_id"],)).fetchone()
            if row is None:
                raise ValueError(f"idempotency_key refers to cleaned job {request['job_id']}; use a new key for new work")
            return {
                "job_id": row["job_id"], "status": row["status"],
                "stdout_path": row["stdout_path"], "stderr_path": row["stderr_path"],
                "events_path": row["events_path"], "idempotent_replay": True,
                "message": "Existing job returned; no new workload launched",
                **self._start_extras(self._wake_targets_for_job(row["job_id"]), None),
            }

    def _validate_trigger(self, trigger: dict[str, Any] | None) -> dict[str, Any] | None:
        """Validate a queue trigger: a DAG gate, a readiness probe, or both.

        A DAG gate is ``{"job_id": A, "status": S}`` (existing). A readiness gate
        is ``{"probe": {...}}`` (roadmap #10). When both are present they are
        ANDed. Unknown fields are rejected.
        """
        if trigger is None:
            return None
        if not isinstance(trigger, dict):
            raise ValueError("trigger must be an object")
        unknown = set(trigger) - {"job_id", "status", "probe"}
        if unknown:
            raise ValueError(f"unknown trigger fields: {sorted(unknown)}")
        job_id = trigger.get("job_id")
        status = trigger.get("status")
        probe = trigger.get("probe")
        normalized: dict[str, Any] = {}
        if job_id is not None or status is not None:
            if not isinstance(job_id, str) or not job_id:
                raise ValueError("trigger.job_id must be a non-empty string")
            if not isinstance(status, str) or status not in TERMINAL_STATUSES:
                raise ValueError(f"trigger.status must be one of {sorted(TERMINAL_STATUSES)}")
            if not self._row("SELECT job_id FROM jobs WHERE job_id=?", (job_id,)):
                raise ValueError(f"Unknown trigger job_id: {job_id}")
            normalized.update(job_id=job_id, status=status)
        if probe is not None:
            normalized_probe = validate_probe(probe)
            if normalized_probe["type"] == "log_line" and not self._row(
                "SELECT job_id FROM jobs WHERE job_id=?", (normalized_probe["job_id"],)
            ):
                raise ValueError(f"Unknown log_line probe job_id: {normalized_probe['job_id']}")
            if normalized_probe["type"] == "http":
                try:
                    check_outbound_url(normalized_probe["url"])
                except OutboundDenied as exc:
                    raise ValueError(f"http probe url is blocked by policy: {exc}") from exc
            normalized["probe"] = normalized_probe
        if not normalized:
            raise ValueError("trigger must include a job_id/status gate, a probe, or both")
        return normalized

    @staticmethod
    def _validate_secret_env(secret_env: list[str] | None) -> list[str]:
        """Validate the declared-secret env NAMES whose values are masked.

        Only environment variable names are accepted (never literal secret
        values). The runner resolves each name in the job's merged environment
        and replaces the value with ``***`` in captured logs and structured
        events, so a job can never leak a declared secret into durable state.
        """
        if secret_env is None:
            return []
        if not isinstance(secret_env, list):
            raise ValueError("secret_env must be a list of environment variable names")
        names: list[str] = []
        for name in secret_env:
            if not isinstance(name, str) or not name.strip():
                raise ValueError("secret_env entries must be non-empty strings")
            if name not in names:
                names.append(name)
        return names

    @staticmethod
    def _validate_pool(pool: str | None) -> str | None:
        if pool is None:
            return None
        if not isinstance(pool, str) or not pool.strip():
            raise ValueError("pool must be a non-empty string")
        if len(pool) > 64:
            raise ValueError("pool must be at most 64 characters")
        return pool.strip()

    @staticmethod
    def _validate_priority(priority: int) -> int:
        if isinstance(priority, bool) or not isinstance(priority, int):
            raise ValueError("priority must be an integer")
        if priority < -1000000 or priority > 1000000:
            raise ValueError("priority must be between -1000000 and 1000000")
        return priority

    def _write_spec(self, job_id: str, spec: dict[str, Any], *, spec_name: str | None = None) -> Path:
        """Write a job's run spec JSON and return its path.

        Shared by ``start`` and the remote dispatcher so both local and remote
        launches reuse one serialization path. ``spec_name`` overrides the
        default ``{job_id}.json``; the launch path passes a CLAIM-SPECIFIC name
        (``{job_id}-{claim_token}.json``) so a delayed runner from an old claim
        can never read the replacement token of a newer run (review rc33 P1-3).
        """
        path = self.specs_dir / (spec_name or f"{job_id}.json")
        path.write_text(
            json.dumps(spec, separators=(",", ":")),
            encoding="utf-8",
        )
        return path

    def _launch(
        self,
        job_id: str,
        stdout_path: Path,
        stderr_path: Path,
        events_path: Path,
        spec_path: Path,
        *,
        claim_token: str,
    ) -> dict[str, Any]:
        creationflags = 0
        if sys.platform == "win32":
            creationflags = subprocess.CREATE_NEW_PROCESS_GROUP | getattr(subprocess, "CREATE_BREAKAWAY_FROM_JOB", 0)
        runner_log = None
        try:
            try:
                runner_log = (self.logs / f"{job_id}.runner.log").open("ab")
                proc = subprocess.Popen(
                    [sys.executable, "-m", "vanth.runner", str(self.home), job_id, f"{job_id}-{claim_token}.json"],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=runner_log,
                    creationflags=creationflags,
                    start_new_session=sys.platform != "win32",
                )
            except (SystemExit, KeyboardInterrupt):
                raise
            except Exception as exc:
                self.processes.pop(job_id, None)
                # The row was claimed 'launching' by prepare_launch/start; the
                # spawn failed so fall it to 'failed' unconditionally (the
                # terminal guard's WHERE status='running' would skip a launching
                # row). The write is CLAIM-token guarded so a stale starter can
                # never mark a newer run failed (review rc36 P1).
                changed = self._terminal_event(
                    job_id, "failed", 1, claim_token=claim_token, require_launching=True,
                    message=f"Job runner failed to start: {exc}", data={"error": str(exc)},
                    level="error", source="server",
                )
                # Preserve a budgeted restart only after the owned failure and
                # its event were committed together.
                if changed:
                    self._restore_pending_restart_after(job_id, claim_token)
                Path(spec_path).unlink(missing_ok=True)
                return {
                    "job_id": job_id,
                    "status": "failed",
                    "exit_code": 1,
                    "message": f"Job runner failed to start: {exc}",
                    "worker_pid": None,
                    "stdout_path": str(stdout_path),
                    "stderr_path": str(stderr_path),
                    "events_path": str(events_path),
                }
        finally:
            if runner_log is not None:
                runner_log.close()
        self.processes[job_id] = proc
        threading.Thread(target=self._watch_runner, args=(job_id, proc, claim_token), daemon=True).start()
        # The runner (not this parent) performs the launching->running
        # promotion atomically, guarded by claim_token. The parent only
        # records worker_pid so stale-claim recovery can tell a live runner
        # from a claim abandoned before spawn (review rc32 P1-3). This write
        # is claim-token guarded: it cannot resurrect a run the runner has
        # already finished or that a newer launch owns.
        with self.db_lock:
            wrote = self.db.execute(
                "UPDATE jobs SET worker_pid=?, updated_at=? WHERE job_id=? AND claim_token=? AND status='launching'",
                (proc.pid, now_iso(), job_id, claim_token),
            ).rowcount
            self.db.commit()
            if wrote != 1:
                # The worker_pid write returned 0. That can mean two very
                # different things:
                #
                # 1. OWNED SUCCESS: our runner already promoted this same claim
                #    launching -> running (and possibly already finished it).
                #    The runner is a VALID run of the job — never terminate it.
                # 2. CLAIM LOSS: recovery reclaimed the claim, or a newer launch
                #    owns the row (claim_token mismatched). The runner is for a
                #    run that is no longer ours — terminate it.
                #
                # Distinguish by inspecting claim_token/status: same token with
                # status 'running' (or any terminal) is owned success; only a
                # mismatched token represents claim loss (review rc33 P1-1).
                current = self._row(
                    "SELECT status, claim_token, pid, worker_pid FROM jobs WHERE job_id=?", (job_id,)
                )
                ours = bool(
                    current
                    and current["claim_token"] == claim_token
                    and current["status"] in {"running", "launching"} | TERMINAL_STATUSES
                )
                if ours:
                    # The runner beat us to the promotion (or finished the job
                    # before we recorded worker_pid). This is the success path.
                    # The runner already cleared any pending restart intent in
                    # its promotion transaction (review rc34 P1-1); no clear
                    # here.
                    return {
                        "job_id": job_id,
                        "status": current["status"],
                        "worker_pid": current["worker_pid"] or proc.pid,
                        "pid": current["pid"],
                        "stdout_path": str(stdout_path),
                        "stderr_path": str(stderr_path),
                        "events_path": str(events_path),
                        "message": "Job started",
                    }
                # The claim was lost between spawn and this write. Never leave a
                # runner for a run that is no longer ours.
                self.processes.pop(job_id, None)
                try:
                    self._terminate_pid(proc.pid, force=True, deadline=time.monotonic() + self.recovery_kill_timeout)
                except Exception:
                    self.logger.exception("could not terminate leaked runner job_id=%s pid=%s", job_id, proc.pid)
                return {
                    "job_id": job_id,
                    "status": (current["status"] if current else "lost"),
                    "message": "Launch claim was lost before the runner published",
                    "worker_pid": proc.pid,
                    "stdout_path": str(stdout_path),
                    "stderr_path": str(stderr_path),
                    "events_path": str(events_path),
                }
            # The runner clears pending_restart_after in its token-guarded
            # promotion transaction (review rc34 P1-1). The parent must NOT
            # clear it here: the row may still be 'launching' and a crash before
            # promotion would lose the already-budgeted retry.
            return {
                "job_id": job_id,
                "status": "running",
                "worker_pid": proc.pid,
                "stdout_path": str(stdout_path),
                "stderr_path": str(stderr_path),
                "events_path": str(events_path),
                "message": "Job started",
            }

    def _claim_launch(
        self,
        job_id: str,
        *,
        require_policy_state: dict[str, Any] | None = None,
        policy_state_updates: dict[str, Any] | None = None,
    ) -> str | None:
        """Atomically claim a runnable job for launch and return the claim token.

        The claim is a single guarded UPDATE whose rowcount is authoritative:
        ``WHERE status IN ('queued','failed','orphaned') AND policy_disabled=0``.
        Exactly one caller (in this process or another) observes rowcount==1;
        every other caller gets 0 and returns None (review rc32 P1-2). The old
        SELECT-then-verify approach let two JobManager instances both believe
        they owned the claim because a post-UPDATE SELECT only confirmed the
        row's status, not who wrote it.

        ``require_policy_state`` guards the claim on persisted policy_state
        (e.g. ``{"restart_after": <deadline>}``); ``policy_state_updates`` are
        applied to policy_state in the SAME transaction as the claim. This makes
        the restart-deadline clear atomic with the launch claim (review rc32
        P1-4): a crash between the two can no longer consume the restart budget
        without establishing launch ownership.

        Review rc33 P1-6: if a restart claim is cleared (``restart_after`` ->
        ``None``) but the launch never spawns (spec-construction failure, disk
        error, or crash before spawn), the claimed-but-abandoned row would be
        recovered ``orphaned`` and restart policy (which only watches
        failed/completed rows) would lose the already-budgeted retry. To keep
        the retry intent durable, the claim transaction ALSO records the cleared
        deadline under ``pending_restart_after``; ``_restore_abandoned_claim``
        re-persists it if the claimed launch is abandoned pre-spawn.
        """
        def claim() -> str | None:
            with self.db_lock:
                row = self.db.execute(
                    "SELECT job_id, status, policy_disabled, policy_state_json FROM jobs WHERE job_id=?",
                    (job_id,),
                ).fetchone()
                if row is None:
                    return None
                if row["status"] not in {"queued", "failed", "orphaned"}:
                    return None
                if row["policy_disabled"]:
                    return None
                state: dict[str, Any] = {}
                try:
                    state = json.loads(row["policy_state_json"] or "{}")
                except (TypeError, ValueError):
                    state = {}
                if not isinstance(state, dict):
                    state = {}
                if require_policy_state is not None:
                    for key, value in require_policy_state.items():
                        if state.get(key) != value:
                            return None
                if policy_state_updates is not None:
                    # Record the pre-clear value so an abandoned claim can
                    # restore the restart intent (review rc33 P1-6).
                    cleared = {k: state.get(k) for k in policy_state_updates}
                    state.update(policy_state_updates)
                    for key, value in cleared.items():
                        if value is not None and key == "restart_after":
                            state["pending_restart_after"] = value
                token = "claim_" + uuid.uuid4().hex[:16]
                changed = self.db.execute(
                    "UPDATE jobs SET status='launching', claim_token=?, policy_state_json=?, updated_at=?, "
                    "worker_pid=NULL, pid=NULL, runner_heartbeat_at=NULL "
                    "WHERE job_id=? AND status IN ('queued','failed','orphaned') AND policy_disabled=0",
                    (token, json.dumps(state, separators=(",", ":")), now_iso(), job_id),
                ).rowcount
                if changed != 1:
                    # A concurrent caller won the claim (rowcount 0) — never
                    # double-claim. Commit to close the read transaction.
                    self.db.commit()
                    return None
                self.db.commit()
                return token
        return self._retry_locked(claim)

    def _build_launch(self, job_id: str, token: str) -> dict[str, Any]:
        """Read the run spec for a claimed job and write its spec file."""
        spec_row = self._row(
            "SELECT job_id, command, cwd, env_json, timeout_seconds, run_json, secret_env_json FROM jobs WHERE job_id=?",
            (job_id,),
        )
        interactive = False
        if spec_row:
            try:
                interactive = json.loads(spec_row["run_json"] or "{}").get("interactive") is True
            except (TypeError, ValueError):
                interactive = False
            if interactive:
                lock = self._lock_stdin(job_id)
                try:
                    owner = self._row("SELECT status, claim_token FROM jobs WHERE job_id=?", (job_id,))
                    if owner["status"] != "launching" or owner["claim_token"] != token:
                        raise RuntimeError("stdin reset refused: launch claim is no longer owned")
                    for suffix in ("in", "closed"):
                        (self.home / "stdin" / f"{job_id}.{suffix}").unlink(missing_ok=True)
                finally:
                    lock.release()
            spec = {
                "command": spec_row["command"],
                "cwd": spec_row["cwd"],
                "env": json.loads(spec_row["env_json"] or "{}"),
                "timeout_seconds": spec_row["timeout_seconds"],
                "max_log_bytes": self.max_log_bytes,
                "stdout_path": str(self.logs / f"{job_id}.stdout.log"),
                "stderr_path": str(self.logs / f"{job_id}.stderr.log"),
                "interactive": interactive,
                # Declared-secret masking must survive a queued (trigger/pool)
                # launch, which does not go through start()'s direct spec path.
                "secret_env": json.loads(spec_row["secret_env_json"] or "[]"),
                # The runner uses this token to atomically promote the claim
                # (launching -> running) and to guard every terminal transition
                # so a stale run can never touch a newer launch.
                "claim_token": token,
            }
        else:
            spec = {
                "command": "",
                "cwd": None,
                "env": {},
                "timeout_seconds": None,
                "max_log_bytes": self.max_log_bytes,
                "stdout_path": str(self.logs / f"{job_id}.stdout.log"),
                "stderr_path": str(self.logs / f"{job_id}.stderr.log"),
                "interactive": False,
                "secret_env": [],
                "claim_token": token,
            }
        spec_path = self._write_spec(job_id, spec, spec_name=f"{job_id}-{token}.json")
        return {
            "job_id": job_id,
            "stdout_path": Path(spec["stdout_path"]),
            "stderr_path": Path(spec["stderr_path"]),
            "events_path": self.events_dir / f"{job_id}.jsonl",
            "spec_path": spec_path,
            "claim_token": token,
            "spec_file": f"{job_id}-{token}.json",
        }

    def prepare_launch(self, job_id: str) -> dict[str, Any] | None:
        """Prepare a job already inserted in a runnable state for launch.

        Writes the spec JSON (command/cwd/env/timeout/etc. from the row) and
        returns a launch dict. This is the shared explicit operation used by the
        local dispatcher and the remote dispatcher so a remote launch never
        calls ``JobManager.start`` directly. Returns ``None`` when the job is
        unknown or not launchable.

        The runnable check is ATOMIC (review P1-3 / P1-1 / rc32 P1-2): under one
        guarded UPDATE the row's status and policy_disabled flag are verified
        and the row is claimed as ``launching`` with a durable claim_token. The
        UPDATE's rowcount==1 is authoritative, so across processes exactly one
        caller owns the claim (no SELECT-then-verify ambiguity). ``launching``
        is NOT runnable, so a serialized second call (or a concurrent one)
        cannot claim the same row twice. A crash between claim and spawn leaves
        the row ``launching``; the dispatch loop's stale-claim recovery reclaims
        it after a grace period (see ``_recover_stale_launch_claims``).
        """
        self._ensure_open()
        token = self._claim_launch(job_id)
        if token is None:
            # We did NOT acquire the claim — another process owns the row (or
            # it is no longer runnable). Never mutate the row here: marking a
            # non-null 'launching' claim orphaned could corrupt a LIVE claim
            # owned by another manager (review rc34 P1-3). A claim that was
            # lost to shutdown is recovered by the dispatch loop's stale-claim
            # recovery instead.
            return None
        return self._build_launch(job_id, token)

    def _restore_pending_restart_after(self, job_id: str, claim_token: str | None, status: str | None = None) -> bool:
        """Restore a restart deadline that was cleared atomically with a claim
        whose launch never spawned (review rc33 P1-6).

        ``_claim_due_restart`` records the pre-clear ``restart_after`` deadline
        under ``pending_restart_after`` in policy_state. If the claimed launch is
        abandoned pre-spawn (spec-construction failure, disk error, spawn
        failure, or crash before spawn -> stale-claim recovery), the row ends
        ``failed`` or ``orphaned`` and restart policy (which watches
        failed/completed rows) would lose the already-budgeted retry. This
        restores the deadline so the budgeted relaunch still happens. It is a
        guarded no-op when the claim is no longer ours or no deadline was
        pending. ``status`` optionally constrains the row status (e.g.
        ``'orphaned'`` for stale-claim recovery).
        """
        self._ensure_open()

        def restore() -> bool:
            with self.db_lock:
                # Review rc36 P1: a SINGLE guarded UPDATE moves the pending
                # deadline back to restart_after. The WHERE clause re-checks the
                # full run identity (claim_token + status + pending set) in the
                # SAME statement that writes, so two managers cannot interleave a
                # SELECT/UPDATE (cross-process TOCTOU): stale restore can never
                # replace a newer claim's state because the guarded write affects
                # zero rows when the token/status no longer match.
                where = "job_id=?"
                args: list[Any] = [job_id]
                if claim_token:
                    where += " AND claim_token=?"
                    args.append(claim_token)
                if status is not None:
                    where += " AND status=?"
                    args.append(status)
                where += " AND json_extract(policy_state_json, '$.pending_restart_after') IS NOT NULL"
                changed = self.db.execute(
                    "UPDATE jobs SET "
                    "policy_state_json=json_set(json_remove(policy_state_json, '$.pending_restart_after'), "
                    "  '$.restart_after', json_extract(policy_state_json, '$.pending_restart_after')), "
                    "updated_at=? "
                    f"WHERE {where}",
                    (now_iso(), *args),
                ).rowcount
                self.db.commit()
                return bool(changed)
        return self._retry_locked(restore)

    def _clear_pending_restart_after(self, job_id: str, claim_token: str | None = None) -> None:
        """Clear the abandoned-claim restart intent once a launch is confirmed
        (runner promoted or the parent recorded worker_pid). Without this, a
        stale ``pending_restart_after`` from an earlier claim could be restored
        for a LATER abandoned claim whose own deadline differs (review rc33
        P1-6).

        Review rc34 P1-1: the clear REQUIRES the owning claim token. A delayed
        runner from a newer claim must never erase the newer claim's intent, so
        the update is guarded by ``claim_token`` AND the token-guarded
        ``status='running'`` promotion (it only runs after the launch is
        confirmed live). With no token (the start() path), a legacy no-op is
        applied (no pending intent exists there anyway).
        """
        self._ensure_open()

        def clear() -> None:
            with self.db_lock:
                if not claim_token:
                    # Only a claim-OWNED launch clears the pending restart
                    # intent (review rc34 P1-1). The no-token start() path has
                    # no pending_restart_after and must never clear another
                    # claim's.
                    return
                # Review rc36 P1: a SINGLE guarded UPDATE removes the pending
                # intent. The WHERE re-checks claim_token in the SAME statement
                # that writes (cross-process CAS), so a stale clear from an old
                # claim can never erase a newer claim's pending deadline — the
                # guarded write affects zero rows when the token no longer
                # matches.
                changed = self.db.execute(
                    "UPDATE jobs SET policy_state_json=json_remove(policy_state_json, '$.pending_restart_after'), updated_at=? "
                    "WHERE job_id=? AND claim_token=? AND json_extract(policy_state_json, '$.pending_restart_after') IS NOT NULL",
                    (now_iso(), job_id, claim_token),
                ).rowcount
                self.db.commit()
                return
        self._retry_locked(clear)

    def _abandon_launch_claim(
        self, job_id: str, claim_token: str | None, expected_worker_pid: int | None | object = _UNSET,
        *, message: str = "Launch claim went stale before the runner published its workload",
    ) -> tuple[bool, str]:
        """Win the launching-only ownership CAS before cleanup; commit event with state.

        Cleanup runs while the winning write transaction owns the claim. If
        cleanup fails, roll back instead of announcing a terminal state for a
        live workload. Preserve a budgeted restart deadline after commit.
        """
        with self.db_lock:
            row = self.db.execute("SELECT policy_state_json FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        pending = None
        if row:
            try:
                state = json.loads(row["policy_state_json"] or "{}")
                pending = state.get("pending_restart_after") if isinstance(state, dict) else None
            except (TypeError, ValueError):
                pass
        target = "failed" if pending else "orphaned"

        def cleanup_owned_workload() -> bool:
            owned = self._row("SELECT pid FROM jobs WHERE job_id=?", (job_id,))
            if owned and owned["pid"]:
                if not self._terminate_pid(int(owned["pid"]), force=True,
                                           deadline=time.monotonic() + self.recovery_kill_timeout):
                    self.logger.error("abandoned claim could not terminate workload job_id=%s pid=%s", job_id, owned["pid"])
                    return False
            return True

        recovered = self._terminal_event(
            job_id, target, claim_token=claim_token, require_launching=True,
            expected_worker_pid=expected_worker_pid,
            message=message + ("; restart intent preserved" if pending else ""),
            data={"claim_token": claim_token}, level="error" if pending else "info",
            before_commit=cleanup_owned_workload,
        )
        if recovered and pending:
            self._restore_pending_restart_after(job_id, claim_token, status=target)
        return recovered, target

    def _recover_stale_launch_claims(self) -> None:
        """Reclaim ``launching`` rows whose claim went stale (crash between
        claim and spawn). Called from the dispatch loop; a row left
        ``launching`` past the grace window means the claiming process died
        before spawning, so it is recovered to ``orphaned`` (the runner never
        started) so it can be relaunched.

        Review rc32 P1-3: recovery reconciles process ownership FIRST. A row
        whose runner (worker_pid) is still alive is a launch that is mid-flight
        (the runner is about to publish its workload) — never orphan a live
        launch. A row with no live runner may still have a live workload pid to
        terminate before the row is freed, and the recovery emits an
        ``orphaned`` event through the normal terminal-transition path so waits,
        wake targets, and feeds are notified.

        Review rc33 P1-4: recovery NEVER orphans a run the runner already
        promoted. The terminal transition is an ATOMIC launching-only,
        token-guarded UPDATE (``status='launching' AND claim_token=?``). Only
        after recovery WINS that transition (rowcount 1) does it know the runner
        cannot still promote — then (and only then) it reconciles/kills any
        workload PID. If the runner promoted between the stale snapshot and the
        transition, the guard returns 0 and the live workload is left alone.
        """
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=self.launch_claim_timeout)).isoformat().replace("+00:00", "Z")
        try:
            rows_to_recover: list[sqlite3.Row] = []
            with self.db_lock:
                stale = self.db.execute(
                    "SELECT job_id, worker_pid, pid, claim_token FROM jobs WHERE status='launching' AND updated_at < ?",
                    (cutoff,),
                ).fetchall()
                for row in stale:
                    if row["worker_pid"] and self._pid_alive(row["worker_pid"]):
                        # The runner process is alive: this launch will publish
                        # momentarily. Leave it alone.
                        continue
                    rows_to_recover.append(row)
            for row in rows_to_recover:
                job_id = row["job_id"]
                # ATOMIC ownership win first: the runner can promote the same
                # token launching -> running between the snapshot above and this
                # transition. The launching-only, token-guarded UPDATE is what
                # decides: if the runner already owns 'running', it returns 0 and
                # we leave the live run alone (review rc33 P1-4). A claim that
                # carried a pending restart deadline recovers as 'failed' with
                # the deadline restored so restart policy relaunches (P1-6).
                # The observed worker identity is included in the CAS so a
                # runner that became live after the snapshot is never orphaned
                # (review rc36 P1).
                self._abandon_launch_claim(
                    job_id, row["claim_token"], expected_worker_pid=row["worker_pid"]
                )
        except Exception:
            self.logger.exception("stale launch claim recovery failed")

    def _launch_prepared(self, launch: dict[str, Any]) -> dict[str, Any]:
        """Launch a job prepared by :meth:`prepare_launch` (wraps ``_launch``).

        Shared by ``_fire_triggered_jobs`` and the remote dispatcher. Keeps the
        existing ``_launch`` behavior byte-for-byte for local jobs.
        """
        return self._launch(
            launch["job_id"],
            launch["stdout_path"],
            launch["stderr_path"],
            launch["events_path"],
            launch["spec_path"],
            claim_token=launch.get("claim_token"),
        )

    async def rerun(self, job_id: str, **overrides: Any) -> dict[str, Any]:
        return await asyncio.get_running_loop().run_in_executor(
            None, lambda: self.rerun_sync(job_id, **overrides)
        )

    def rerun_sync(
        self,
        job_id: str,
        *,
        command: str | None = None,
        env: dict[str, str] | None = None,
        timeout_seconds: int | None = None,
        name: str | None = None,
        tags: list[str] | None = None,
        notes: str | None = None,
        cwd: str | None = None,
        interactive: bool | None = None,
        secret_env: list[str] | None = None,
    ) -> dict[str, Any]:
        self._ensure_open()
        row = self._row(
            "SELECT job_id, command, cwd, env_json, timeout_seconds, notify_on, origin_thread_id, wake_thread_id, "
            "tags_json, name, notes, run_json, policy_json, secret_env_json, pool, priority FROM jobs WHERE job_id=?",
            (job_id,),
        )
        if not row:
            raise ValueError(f"Unknown job_id: {job_id}")
        # A job disabled by policy (on_failure disable action) must not be
        # relaunched through the public rerun API (review P1-3).
        disabled = self._row("SELECT policy_disabled FROM jobs WHERE job_id=?", (job_id,))
        if disabled and disabled["policy_disabled"]:
            raise ValueError("job is disabled by policy (on_failure disable); clear the flag to relaunch")
        # Carry the failure streak across reruns: the policy state of the
        # source job seeds the new job, so after_n counts consecutive failures
        # of the *logical* job, not one runner instance.
        prior_state = self._policy_state(job_id)
        targets = []
        for target in self.db.execute("SELECT * FROM wake_targets WHERE job_id=?", (job_id,)).fetchall():
            config = json.loads(target["config_json"] or "{}")
            targets.append({
                "type": target["type"],
                "events": json.loads(target["events_json"] or "[]"),
                **config,
            })
        stored_tags = json.loads(row["tags_json"] or "[]")
        stored_notify_on = json.loads(row["notify_on"] or "[]")
        stored_interactive = json.loads(row["run_json"] or "{}").get("interactive") is True
        stored_env = json.loads(row["env_json"] or "{}")
        merged_env = stored_env
        if env is not None:
            if not isinstance(env, dict):
                raise ValueError("env must be an object of string values")
            merged_env = {**stored_env, **env}
        result = asyncio.run(self.start(
            command=command if command is not None else row["command"],
            cwd=cwd if cwd is not None else row["cwd"],
            name=name if name is not None else row["name"],
            env=merged_env,
            timeout_seconds=timeout_seconds if timeout_seconds is not None else row["timeout_seconds"],
            notify_on=stored_notify_on or None,
            wake_targets=targets or None,
            origin_thread_id=row["origin_thread_id"],
            tags=stored_tags if tags is None else tags,
            notes=notes if notes is not None else row["notes"],
            interactive=stored_interactive if not isinstance(interactive, bool) else interactive,
            policy=json.loads(row["policy_json"] or "null"),
            secret_env=secret_env if secret_env is not None else (json.loads(row["secret_env_json"] or "null") or None),
            pool=row["pool"],
            priority=row["priority"] or 0,
        ))
        if prior_state and json.loads(row["policy_json"] or "null"):
            # Carry ONLY the failure streak: reacted_* dedup markers belong to
            # the previous runner instance and must not suppress the next
            # failure's reaction. The counting watermark is NOT carried — event
            # ``seq`` is per-job, so the rerun's fresh sequence counts its own
            # failures from the start (carrying the old seq would suppress them).
            self._save_policy_state(result["job_id"], {"failure_streak": int(prior_state.get("failure_streak", 0))})
        return result

    def _watch_runner(
        self,
        job_id: str,
        proc: subprocess.Popen[bytes],
        claim_token: str | None = None,
    ) -> None:
        proc.wait()
        try:
            row = self._row(
                "SELECT status, pid, worker_pid, stop_requested_at, stop_actor, stop_reason, claim_token "
                "FROM jobs WHERE job_id=?",
                (job_id,),
            )
            if not row:
                return
            if claim_token and row["status"] == "launching" and row["claim_token"] == claim_token:
                # Our runner exited before it ever published its workload —
                # either the child spawn failed inside the runner or the runner
                # was killed. The claim is stale; recover it NOW (no grace
                # period wait) so the job can be relaunched promptly. Guarded by
                # claim_token so a concurrent recovery can't double-orphan. A
                # restart claim preserves its budgeted retry intent (P1-6).
                # Review rc34 P2: establish ownership FIRST (win the atomic
                # launching-only transition) before terminating any workload
                # PID, so a PID reused by a newer run is never killed.
                self._abandon_launch_claim(
                    job_id, claim_token, expected_worker_pid=row["worker_pid"],
                    message="Job runner exited before its workload was published",
                )
                return
            if row["status"] != "running":
                return
            # A restart policy can relaunch this job with a NEW runner while a
            # stale watcher from the previous run is still waking up. If a newer
            # launch owns the row, leave it alone. Ownership is claim_token
            # based when available (the launch path); on the start() path the
            # recorded worker_pid must still be OUR runner process. Note: on
            # Windows the runner's os.getpid() (real python) can differ from the
            # Popen pid (launcher shim), so the pid guard only applies to the
            # no-token path where worker_pid is the parent-observed Popen pid.
            watcher_pid = getattr(proc, "pid", None)
            if claim_token:
                if row["claim_token"] and row["claim_token"] != claim_token:
                    return
            elif row["worker_pid"] and watcher_pid and int(row["worker_pid"]) != int(watcher_pid):
                return
            # The runner process has exited; terminate its workload child. The
            # kill decision is coupled to the still-current run identity
            # revalidated above (review rc34 P2). If the kill fails, keep the
            # job running: the workload may still be alive and orphaning it
            # would leave a live, untracked process.
            if row["pid"]:
                if not self._terminate_pid(int(row["pid"]), force=True, deadline=time.monotonic() + self.recovery_kill_timeout):
                    self.logger.error("runner watcher could not terminate workload job_id=%s pid=%s", job_id, row["pid"])
                    return
            terminal = "cancelled" if row["stop_requested_at"] else "orphaned"
            self._terminal_event(
                job_id, terminal, claim_token=claim_token,
                worker_pid=row["worker_pid"] if not claim_token else _UNSET,
                message="Job runner exited before recording a terminal status",
                data=stop_event_data(row, default_actor="watchdog", default_reason="runner exited before terminal status"),
            )
        except (sqlite3.Error, RuntimeError):
            return

    def _insert_wake_targets(
        self,
        job_id: str,
        targets: list[dict[str, Any]],
        created_at: str,
        remote_id: str | None = None,
    ) -> list[str]:
        inserted = []
        for target in targets:
            target = canonicalize_wake_target(target)
            target_type = target.get("type")
            events = target.get("events") or target.get("notify_on") or []
            if not isinstance(target_type, str):
                continue
            config = {key: value for key, value in target.items() if key not in {"type", "events", "notify_on"}}
            target_id = "target_" + uuid.uuid4().hex[:12]
            self.db.execute(
                """
                INSERT INTO wake_targets(target_id, job_id, remote_id, type, events_json, config_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    target_id,
                    job_id,
                    remote_id,
                    target_type,
                    json.dumps(events, separators=(",", ":")),
                    json.dumps(config, separators=(",", ":")),
                    created_at,
                ),
            )
            inserted.append(target_id)
        return inserted

    def register_remote_wake_targets(
        self, remote_id: str, remote_job_id: str, targets: list[dict[str, Any]]
    ) -> list[str]:
        """Register local wake targets for a job running on a paired host."""
        self._ensure_open()
        binding_id = self._remote_binding_id(remote_id, remote_job_id)
        try:
            resolved = resolve_wake_target_identity(targets, None)
            self._resolve_relay_sessions(resolved, None)
            self._reject_relay_client_id_targets(resolved)
            validate_wake_targets(resolved)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"remote wake was NOT registered: {exc}") from exc
        with self.db_lock:
            existing = self.db.execute(
                "SELECT target_id FROM wake_targets WHERE job_id=? ORDER BY rowid", (binding_id,)
            ).fetchall()
            if existing:
                return [row["target_id"] for row in existing]
            try:
                inserted = self._insert_wake_targets(binding_id, resolved, now_iso(), remote_id=remote_id)
                self.db.commit()
            except BaseException:
                self.db.rollback()
                raise
        return inserted

    @staticmethod
    def _remote_binding_id(remote_id: str, remote_job_id: str) -> str:
        """Composite wake-target/event key for a job on a paired host.

        A colon separator keeps it distinct from any local ``job_<hex>`` id, so a
        remote job id can never collide with a local one. Generated ids never
        contain ``:``; a caller-supplied id that does would make the composite
        ambiguous (and ``remote_wake_bindings`` unrecoverable), so reject it.
        """
        for value, label in ((remote_id, "remote_id"), (remote_job_id, "remote_job_id")):
            if not isinstance(value, str) or not value:
                raise ValueError(f"{label} must be a non-empty string")
            if ":" in value:
                raise ValueError(f"{label} must not contain ':'")
        return f"remote:{remote_id}:{remote_job_id}"

    def emit_remote_terminal(
        self,
        remote_id: str,
        remote_job_id: str,
        status: str,
        *,
        exit_code: int | None = None,
    ) -> dict[str, Any] | None:
        """Emit one terminal event for a remote job's local wake binding."""
        if status not in TERMINAL_STATUSES:
            return None
        binding_id = self._remote_binding_id(remote_id, remote_job_id)
        with self.db_lock:
            if self.db.execute(
                "SELECT 1 FROM events WHERE job_id=? AND type IN ({}) LIMIT 1".format(
                    ",".join("?" for _ in TERMINAL_STATUSES)
                ),
                (binding_id, *TERMINAL_STATUSES),
            ).fetchone():
                # Already fired (the feed re-delivers, or a crash landed between
                # the event commit and the delete below). Drop the binding anyway
                # so remote_wake_bindings stops re-polling the host forever.
                self.db.execute("DELETE FROM wake_targets WHERE job_id=?", (binding_id,))
                self.db.execute("DELETE FROM remote_event_cursors WHERE binding_id=?", (binding_id,))
                self.db.commit()
                return None
            event = self._emit(
                binding_id,
                status,
                message=f"Remote job {remote_id}/{remote_job_id} {status}",
                data={"remote_id": remote_id, "remote_job_id": remote_job_id, "exit_code": exit_code},
                source="remote",
            )
            self.db.execute("DELETE FROM wake_targets WHERE job_id=?", (binding_id,))
            self.db.execute("DELETE FROM remote_event_cursors WHERE binding_id=?", (binding_id,))
            self.db.commit()
            return event

    def remote_event_cursor(self, remote_id: str, remote_job_id: str) -> int | None:
        binding_id = self._remote_binding_id(remote_id, remote_job_id)
        with self.db_lock:
            row = self.db.execute(
                "SELECT next_seq FROM remote_event_cursors WHERE binding_id=?", (binding_id,)
            ).fetchone()
        return None if row is None else int(row["next_seq"])

    def set_remote_event_cursor(self, remote_id: str, remote_job_id: str, next_seq: int) -> None:
        binding_id = self._remote_binding_id(remote_id, remote_job_id)
        with self.db_lock:
            self._remote_cursor_upsert(self.db, binding_id, next_seq)
            self.db.commit()

    @staticmethod
    def _remote_cursor_upsert(db: sqlite3.Connection, binding_id: str, next_seq: int) -> None:
        """Advance a binding's event cursor, never rewinding it."""
        db.execute(
            "INSERT INTO remote_event_cursors(binding_id, next_seq, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(binding_id) DO UPDATE SET next_seq=max(next_seq, excluded.next_seq), "
            "updated_at=excluded.updated_at",
            (binding_id, int(next_seq), now_iso()),
        )

    def emit_remote_event(
        self, remote_id: str, remote_job_id: str, event: dict[str, Any], next_seq: int
    ) -> dict[str, Any] | None:
        binding_id = self._remote_binding_id(remote_id, remote_job_id)
        seq = int(event["seq"])
        with self.db_lock:
            if not self.db.execute("SELECT 1 FROM wake_targets WHERE job_id=? LIMIT 1", (binding_id,)).fetchone():
                return None
            row = self.db.execute(
                "SELECT next_seq FROM remote_event_cursors WHERE binding_id=?", (binding_id,)
            ).fetchone()
            if row is not None and seq <= int(row["next_seq"]):
                return None
            parsed = json.loads(event.get("data_json") or "{}")
            if not isinstance(parsed, dict):
                parsed = {}
            data = {
                **parsed,
                "remote_seq": seq,
                "remote_id": remote_id,
                "remote_job_id": remote_job_id,
            }

            def advance_cursor(db: sqlite3.Connection) -> None:
                # Runs under the same db_lock as the guard above, so no other
                # writer can have advanced the cursor in between.
                self._remote_cursor_upsert(db, binding_id, next_seq)

            result = self._emit(
                binding_id,
                event["type"],
                message=event.get("message"),
                data=data,
                level=event.get("level") or "info",
                source="remote",
                mutate=advance_cursor,
            )
            if result is not None and result.get("persisted") is False:
                # The per-job event cap dropped the event, so `mutate` never ran
                # and the cursor would never advance — the backlog would refetch
                # the same page forever and starve the terminal wake. The event is
                # dropped by design (matching local behaviour), so skip it.
                # A `noop` result means the cursor already covered this seq.
                if not result.get("noop"):
                    self._remote_cursor_upsert(self.db, binding_id, next_seq)
                    self.db.commit()
                return None
            return result

    def drop_remote_wake_binding(self, remote_id: str, remote_job_id: str, *, reason: str = "remote job no longer exists") -> bool:
        """Settle a binding whose remote job is gone (deleted/forgotten on the host).

        The wake can never fire, so remove the binding and its cursor and fail any
        owed deliveries — rather than re-polling the host forever for a shadow that
        will never come back.
        """
        binding_id = self._remote_binding_id(remote_id, remote_job_id)
        with self.db_lock:
            if not self.db.execute("SELECT 1 FROM wake_targets WHERE job_id=? LIMIT 1", (binding_id,)).fetchone():
                return False
            self.db.execute(
                "UPDATE deliveries SET status='failed', last_error=?, next_attempt_at=NULL, "
                "claim_token=NULL, claimed_at=NULL, lease_expires_at=NULL "
                "WHERE job_id=? AND status IN ('pending','dispatching')",
                (reason, binding_id),
            )
            self.db.execute("DELETE FROM wake_targets WHERE job_id=?", (binding_id,))
            self.db.execute("DELETE FROM remote_event_cursors WHERE binding_id=?", (binding_id,))
            self.db.commit()
        return True

    def remote_wake_bindings(self) -> list[dict[str, Any]]:
        with self.db_lock:
            rows = self.db.execute(
                "SELECT remote_id, job_id, target_id, events_json FROM wake_targets "
                "WHERE remote_id IS NOT NULL ORDER BY rowid"
            ).fetchall()
        return [
            {
                "remote_id": row["remote_id"],
                "remote_job_id": row["job_id"].rsplit(":", 1)[-1],
                "binding_id": row["job_id"],
                "target_id": row["target_id"],
                "events": json.loads(row["events_json"] or "[]"),
            }
            for row in rows
        ]

    def _start_extras(self, wake_targets: list[dict[str, Any]] | None, notify_on: list[str] | None) -> dict[str, Any]:
        """Wake identity echo + advisory warnings for a start response."""
        extras = self._wake_start_info(wake_targets or [])
        if not wake_targets:
            # Purely advisory; does not change the start. Agents overwhelmingly
            # default to polling, so nudge toward a wake at the point of launch.
            # Names job_start because a rerun does not itself accept wake args.
            extras["wake_recommended"] = (
                "No wake target attached: completion will not resume a session. Pass wake_me=True "
                "(or a wake_targets entry) on job_start when you need to be woken instead of polling."
            )
        if notify_on and not wake_targets:
            # notify_on is only a default for a wake target's events; on its own
            # it notifies nobody. Stored (not an error) for compatibility, but
            # the caller is told so a silent no-op is not mistaken for a wake.
            extras["warnings"] = [
                "notify_on has no effect without wake_targets: it only sets the default events of a wake target. "
                "Pass wake_me=True (or a wake_targets entry) to actually be woken."
            ]
        return extras

    def _wake_start_info(self, targets: list[dict[str, Any]]) -> dict[str, Any]:
        """Echo the resolved wake identity back to the caller.

        ``wake_addressable`` is ``True`` when every relay-delivered target has a
        destination identity, ``False`` when one does not, and ``None`` when
        there are no relay-delivered targets (nothing to address).
        """
        info = []
        relay_addressable = []
        for target in targets:
            target_type = target.get("type")
            entry: dict[str, Any] = {
                "type": target_type,
                "events": target.get("events") or target.get("notify_on") or [],
            }
            if target_type in RELAY_IDENTITY_KEYS:
                identity_key = "session_id" if target_type == "opencode_thread" else "thread_id"
                identity = target.get(identity_key) or target.get("sessionId" if identity_key == "session_id" else "threadId")
                entry[identity_key] = identity
                if not target.get("command"):
                    relay_addressable.append(bool(identity))
            info.append(entry)
        return {
            "wake_targets": info,
            "wake_addressable": (all(relay_addressable) if relay_addressable else None),
        }

    def _latest_relay_session(self, directory: str | None) -> str | None:
        """Newest OpenCode plugin-relay session registered for ``directory``.

        The plugin publishes the session it lives in (plus its project
        directory) whenever it observes a hook, so a wake target that names no
        session can be resolved to the client actually running in the caller's
        project. A registration with no directory matches any caller.
        """
        try:
            rows = self.db.execute(
                "SELECT destinations_json, last_poll_at FROM relay_subscriptions WHERE client_type='opencode_thread'"
            ).fetchall()
        except sqlite3.Error:
            return None
        wanted = (directory if isinstance(directory, str) else "").rstrip("\\/").lower()
        best: tuple[str, str] | None = None
        for row in rows:
            try:
                destinations = json.loads(row["destinations_json"] or "[]")
            except (TypeError, ValueError):
                continue
            for item in destinations if isinstance(destinations, list) else []:
                if not isinstance(item, dict):
                    continue
                session_id = item.get("session_id")
                if not isinstance(session_id, str) or not session_id:
                    continue
                item_dir = (item.get("directory") or "").rstrip("\\/").lower()
                if wanted and item_dir and item_dir != wanted:
                    continue
                stamp = row["last_poll_at"] or ""
                if best is None or stamp > best[0]:
                    best = (stamp, session_id)
        return best[1] if best else None

    def _sole_relay_session(self, directory: str | None) -> str | None:
        """The single OpenCode session registered for ``directory``, else None.

        Used only for the *best-effort default* wake: with several agents on one
        cwd (a directory-less registration matches any cwd too) the newest
        session is not necessarily the caller's, so an ambiguous match is
        skipped rather than waking the wrong session. Explicit ``wake_me`` keeps
        the newest-match behavior via ``_latest_relay_session``.
        """
        try:
            rows = self.db.execute(
                "SELECT destinations_json FROM relay_subscriptions WHERE client_type='opencode_thread'"
            ).fetchall()
        except sqlite3.Error:
            return None
        wanted = (directory if isinstance(directory, str) else "").rstrip("\\/").lower()
        sessions: set[str] = set()
        for row in rows:
            try:
                destinations = json.loads(row["destinations_json"] or "[]")
            except (TypeError, ValueError):
                continue
            for item in destinations if isinstance(destinations, list) else []:
                if not isinstance(item, dict):
                    continue
                session_id = item.get("session_id")
                if not isinstance(session_id, str) or not session_id:
                    continue
                item_dir = (item.get("directory") or "").rstrip("\\/").lower()
                if wanted and item_dir and item_dir != wanted:
                    continue
                sessions.add(session_id)
        return sessions.pop() if len(sessions) == 1 else None

    def _resolve_relay_sessions(self, targets: list[dict[str, Any]] | None, directory: str | None) -> None:
        """Fill in a missing ``opencode_thread`` session id from live relays.

        A target naming neither ``session_id`` nor ``attach`` is delivered by the
        in-process OpenCode plugin relay; resolving it here (rather than at
        delivery time) keeps the stored target concrete and addressable, and
        makes an unregistered client fail at creation with an actionable error
        (``validate_wake_targets``) instead of silently never waking.

        The identity alias is canonicalized IN PLACE first, so an alias-only
        target (e.g. ``thread_id`` on an ``opencode_thread`` target) is treated as
        already addressed rather than having a different session injected, and
        every later step sees the key the relay SQL actually reads.
        """
        for target in targets or []:
            if not isinstance(target, dict):
                continue
            canonical = canonicalize_wake_target(target)
            if canonical != target:
                target.clear()
                target.update(canonical)
        for target in targets or []:
            if target.get("type") != "opencode_thread" or target.get("attach"):
                continue
            if target.get("session_id"):
                continue
            session_id = self._latest_relay_session(target.get("cwd") or directory)
            if session_id:
                target["session_id"] = session_id

    def _reject_relay_client_id_targets(self, targets: list[dict[str, Any]] | None) -> None:
        """Refuse a relay-delivered wake addressed to a relay CLIENT id.

        The relay ``client_id`` (``opencode-<pid>-<rand>`` for the OpenCode
        plugin, ``mcp-<pid>-<thread>`` for Codex Desktop — both printed by ``vanth
        doctor``) is the long-poll identity, not a destination. A relay-delivered
        target (``opencode_thread``/``codex_desktop`` with no ``command``) is
        claimed by matching the payload's destination identity, so a target naming
        a client id is never claimed: the delivery sits ``pending`` forever with
        no error and no retry. Reject it at creation instead.
        """
        client_ids = self._relay_client_ids()
        for target in targets or []:
            if not isinstance(target, dict):
                continue
            identity = wake_target_identity(target)
            if identity is None:
                continue
            target_type = target.get("type")
            if is_relay_client_id(target_type, identity, client_ids):
                raise ValueError(self._relay_client_id_error(target_type, identity))

    @staticmethod
    def _relay_client_id_error(target_type: str, identity: str) -> str:
        return (
            f"{target_type} target id {identity!r} is a relay client id, not a wake destination; "
            "pass the destination id shown by `vanth doctor` (the session/thread id), "
            "not the relay's client id"
        )

    def _reconcile_invalid_wake_targets(self) -> None:
        """Remove pre-1.11 relay client-id wakes that can never be claimed.

        Creation-time rejection (``_reject_relay_client_id_targets``) landed in
        1.11.0, but targets written before it still sit in the table and their
        deliveries stay ``pending`` forever (never claimed, never errored). Sweep
        them once at startup so the zombie rows and their pending deliveries
        become visible as ``failed``.
        """
        client_ids = self._relay_client_ids()
        try:
            rows = self.db.execute("SELECT target_id, type, config_json FROM wake_targets").fetchall()
        except sqlite3.Error:
            # Defensive: a pre-wake_targets DB must not break manager construction.
            return
        invalid = []
        for row in rows:
            try:
                config = json.loads(row["config_json"] or "{}")
            except (TypeError, ValueError):
                continue
            if not isinstance(config, dict):
                continue
            identity = wake_target_identity({**config, "type": row["type"]})
            if identity is None:
                continue
            if is_relay_client_id(row["type"], identity, client_ids):
                invalid.append((row["target_id"], self._relay_client_id_error(row["type"], identity)))
        for target_id, message in invalid:
            self.db.execute(
                "UPDATE deliveries SET status='failed', last_error=?, next_attempt_at=NULL, "
                "claim_token=NULL, claimed_at=NULL, lease_expires_at=NULL "
                "WHERE target_id=? AND status IN ('pending','dispatching')",
                (message, target_id),
            )
            binding = self.db.execute("SELECT job_id FROM wake_targets WHERE target_id=?", (target_id,)).fetchone()
            self.db.execute("DELETE FROM wake_targets WHERE target_id=?", (target_id,))
            if binding:
                self.db.execute("DELETE FROM remote_event_cursors WHERE binding_id=?", (binding["job_id"],))
        if invalid:
            self.db.commit()
            self.logger.info("reconciled %d undeliverable wake target(s)", len(invalid))

    def _relay_client_ids(self) -> set[str]:
        try:
            rows = self.db.execute("SELECT client_id FROM relay_subscriptions").fetchall()
        except sqlite3.Error:
            return set()
        return {row["client_id"] for row in rows}

    def _read_stream(
        self,
        job_id: str,
        stream,
        path: Path,
        source: str,
        mask_values: list[str] | None = None,
    ) -> None:
        if stream is None:
            return
        max_bytes = self.max_log_bytes
        written = 0
        f = None
        values = {variant for value in (mask_values or []) if value for variant in (
            value, value.replace("\r\n", "\n"), value.replace("\r\n", "\n").replace("\n", "\r\n"),
            value.replace("\n", "\r\n"),  # Text-mode Windows stdout also translates existing CRLF.
        )}
        needles = sorted({encoded for value in values for encoded in (
            value.encode(), json.dumps(value, ensure_ascii=False)[1:-1].encode(),
            json.dumps(value)[1:-1].encode(),
        )}, key=len, reverse=True)
        pattern = re.compile(b"|".join(re.escape(value) for value in needles)) if needles else None
        prefix_tables = []
        for needle in needles:
            table = [0] * len(needle)
            length = 0
            for index in range(1, len(needle)):
                while length and needle[index] != needle[length]:
                    length = table[length - 1]
                if needle[index] == needle[length]:
                    length += 1
                table[index] = length
            prefix_tables.append((needle, table))
        pending = b""
        line = bytearray()
        oversized = False
        event_line = False
        event_capture_failed = False

        def capture_error(exc: OSError) -> None:
            self._capture_failed.add(job_id)
            self.logger.error("log capture failed job_id=%s stream=%s error=%s", job_id, source, exc)
            self._emit_safely(job_id, "log_capture_failed", message=f"{source} log storage failed",
                              data={"stream": source, "error": str(exc)}, level="error")

        try:
            try:
                written = path.stat().st_size if path.exists() else 0
                f = path.open("ab")
            except OSError as exc:
                capture_error(exc)
            read = getattr(stream, "read1", stream.read)
            while True:
                chunk = read(65536)
                pending += chunk
                hold = 0
                if chunk:
                    # Only a suffix that could finish a secret needs another read.
                    for needle, table in prefix_tables:
                        length = 0
                        for byte in pending[-(len(needle) - 1):] if len(needle) > 1 else b"":
                            while length and byte != needle[length]:
                                length = table[length - 1]
                            if byte == needle[length]:
                                length += 1
                        hold = max(hold, length)
                cut = len(pending) - hold
                if pattern:
                    for match in pattern.finditer(pending):
                        if match.start() < cut < match.end():
                            cut = match.end()
                    safe = pattern.sub(b"***", pending[:cut])
                else:
                    safe = pending[:cut]
                pending = pending[cut:]
                if f is not None and written < max_bytes:
                    try:
                        output = safe[:max_bytes - written]
                        f.write(output)
                        f.flush()
                        written += len(output)
                    except OSError as exc:
                        capture_error(exc)
                        try:
                            f.close()
                        except OSError:
                            pass
                        f = None
                if written >= max_bytes and (job_id, source) not in self._log_truncated:
                    self._log_truncated.add((job_id, source))
                    self._emit_safely(job_id, "log_truncated", message=f"{source} log reached its configured byte cap",
                                      data={"stream": source, "max_bytes": max_bytes}, level="warning")
                events = []
                for part in safe.splitlines(keepends=True):
                    if not oversized:
                        line.extend(part)
                        if len(line) > self.max_event_line_bytes:
                            event_line = line.startswith(EVENT_PREFIX.encode())
                            oversized = True
                            line.clear()
                    if part.endswith(b"\n"):
                        if oversized:
                            if event_line:
                                self._emit_safely(job_id, "event_rejected",
                                                  message="AGENT_EVENT line exceeded the configured byte limit",
                                                  data={"max_bytes": self.max_event_line_bytes},
                                                  level="warning", source=source)
                        else:
                            try:
                                payload = parse_agent_event_line(line.decode(errors="replace").rstrip("\r\n"))
                                if payload:
                                    events.append(normalize_event_payload(payload))
                            except Exception:
                                pass
                        line.clear()
                        oversized = False
                        event_line = False
                if not chunk and oversized and event_line:
                    self._emit_safely(job_id, "event_rejected",
                                      message="AGENT_EVENT line exceeded the configured byte limit",
                                      data={"max_bytes": self.max_event_line_bytes}, level="warning", source=source)
                if not chunk and line and not oversized:
                    try:
                        payload = parse_agent_event_line(line.decode(errors="replace").rstrip("\r\n"))
                        if payload:
                            events.append(normalize_event_payload(payload))
                    except Exception:
                        pass
                if events and not event_capture_failed:
                    try:
                        self._emit_capture_batch(job_id, events, source)
                    except (sqlite3.Error, RuntimeError) as exc:
                        event_capture_failed = True
                        self._capture_failed.add(job_id)
                        self.logger.exception("event capture failed; draining pipe job_id=%s stream=%s", job_id, source)
                        self._emit_safely(job_id, "log_capture_failed", message=f"{source} event persistence failed",
                                          data={"stream": source, "error": str(exc)}, level="error")
                if not chunk:
                    break
        except (OSError, sqlite3.Error, RuntimeError) as exc:
            self._capture_failed.add(job_id)
            self.logger.exception("capture failed job_id=%s stream=%s", job_id, source)
            self._emit_safely(job_id, "log_capture_failed", message=f"{source} capture failed",
                              data={"stream": source, "error": str(exc)}, level="error")
            proc = self.processes.get(job_id)
            if proc is not None:
                self._kill_process(proc, force=True)
        finally:
            if f is not None:
                try:
                    f.close()
                except OSError as exc:
                    capture_error(exc)

    def _emit_safely(
        self,
        job_id: str,
        event_type: str,
        *,
        message: str | None = None,
        data: dict[str, Any] | None = None,
        level: str = "info",
        source: str = "server",
    ) -> None:
        try:
            self._emit(job_id, event_type, message=message, data=data, level=level, source=source)
        except (sqlite3.Error, RuntimeError):
            self.logger.exception("structured event persisted failed job_id=%s type=%s", job_id, event_type)

    def _watch(self, job_id: str, proc: subprocess.Popen[bytes], timeout_seconds: int | None) -> None:
        deadline = time.monotonic() + timeout_seconds if timeout_seconds is not None else None
        try:
            exit_code = proc.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            self._kill_process(proc, force=True)
            exit_code = proc.wait()
            self._readers_done(job_id, timeout=1)
            self._finish(job_id, "timeout", exit_code)
            return
        status = self._row("SELECT status FROM jobs WHERE job_id=?", (job_id,))["status"]
        if status == "cancelled":
            return
        drained = self._readers_done(job_id, timeout=max(0, deadline - time.monotonic()) if deadline else 30)
        terminal = "timeout" if not drained and deadline else (
            "completed" if exit_code == 0 and job_id not in self._capture_failed else "failed"
        )
        self._finish(job_id, terminal, exit_code)

    def _readers_done(self, job_id: str, timeout: float = 30.0) -> bool:
        deadline = time.monotonic() + max(0.0, timeout)
        threads = self.reader_threads.pop(job_id, [])
        for thread in threads:
            thread.join(timeout=max(0.0, deadline - time.monotonic()))
        if any(thread.is_alive() for thread in threads):
            self._capture_failed.add(job_id)
            self._emit_safely(job_id, "pipe_drain_timeout", message="Output pipes remained open after workload exit",
                              data={"drain_timeout_seconds": timeout,
                                    "next_action": "Ensure child processes close inherited stdout/stderr."}, level="error")
            proc = self.processes.get(job_id)
            if proc is not None:
                self._kill_process(proc, force=True)
            return False
        return True

    def _kill_process(self, proc: subprocess.Popen[bytes], force: bool) -> None:
        if sys.platform != "win32" and proc.poll() is not None:
            # The reaped leader's PID may already belong to another process.
            # Descendants can still hold its original process group and pipes.
            try:
                os.killpg(proc.pid, 9 if force else 15)
            except (ProcessLookupError, PermissionError):
                pass
            return
        self._kill_pid(proc.pid, force)

    def _kill_pid(self, pid: int, force: bool, *, timeout_seconds: float = 10.0) -> None:
        if sys.platform == "win32":
            args = ["taskkill", "/PID", str(pid), "/T"]
            if force:
                args.append("/F")
            subprocess.run(
                args,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=max(0.1, timeout_seconds),
            )
            return
        signal_number = 9 if force else 15
        try:
            os.killpg(pid, signal_number)
        except (ProcessLookupError, PermissionError):
            try:
                os.kill(pid, signal_number)
            except ProcessLookupError:
                return

    def _terminate_pid(self, pid: int, force: bool, deadline: float) -> bool:
        if not pid or not self._pid_alive(pid):
            return True
        # Even an immediate force-stop needs bounded process-launch grace for
        # taskkill itself; a zero grace must not time out before it can start.
        try:
            self._kill_pid(pid, force=force, timeout_seconds=max(1.0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            return False
        while self._pid_alive(pid) and time.monotonic() < deadline:
            time.sleep(0.05)
        if self._pid_alive(pid):
            try:
                self._kill_pid(pid, force=True, timeout_seconds=max(1.0, min(deadline + 1, time.monotonic() + 1) - time.monotonic()))
            except subprocess.TimeoutExpired:
                return False
            force_deadline = min(deadline + 1, time.monotonic() + 1)
            while self._pid_alive(pid) and time.monotonic() < force_deadline:
                time.sleep(0.05)
        return not self._pid_alive(pid)

    def _cancel_queued(self, job_id: str, *, actor: str, reason: str,
                       message: str, data: dict[str, Any]) -> bool:
        def cancel(db: sqlite3.Connection) -> None:
            changed = db.execute(
                "UPDATE jobs SET status='cancelled', stop_actor=?, stop_reason=?, ended_at=?, updated_at=? "
                "WHERE job_id=? AND status='queued'",
                (actor, reason, now_iso(), now_iso(), job_id),
            ).rowcount
            if not changed:
                raise _DecisionNoOp()

        event = self._emit(job_id, "cancelled", message=message, data=data, mutate=cancel)
        return event.get("persisted") is not False

    def _terminal_event(
        self, job_id: str, status: str, exit_code: int | None = None, *,
        claim_token: str | None = None, worker_pid: int | None | object = _UNSET,
        require_launching: bool = False, expected_worker_pid: int | None | object = _UNSET,
        message: str | None = None, data: dict[str, Any] | None = None,
        level: str = "info", source: str = "server", before_commit: Callable[[], bool] | None = None,
    ) -> bool:
        """Commit owned terminal state, event, and wakes together or none of them."""
        def transition(db: sqlite3.Connection) -> None:
            if not self._transition_terminal(
                job_id, status, exit_code, claim_token=claim_token, worker_pid=worker_pid,
                require_launching=require_launching, expected_worker_pid=expected_worker_pid, transaction=False,
            ):
                raise _DecisionNoOp()
            if before_commit is not None and not before_commit():
                raise _DecisionNoOp()

        event = self._emit(job_id, status, message=message, data=data, level=level,
                           source=source, mutate=transition)
        return event.get("persisted") is not False

    def _finish(self, job_id: str, status: str, exit_code: int | None = None, *, claim_token: str | None = None) -> None:
        row = self._row("SELECT stop_requested_at, stop_actor, stop_reason, timeout_seconds FROM jobs WHERE job_id=?", (job_id,))
        if row and row["stop_requested_at"]:
            status = "cancelled"
        data: dict[str, Any] = {"exit_code": exit_code} if exit_code is not None else {}
        if status == "cancelled":
            data.update(stop_event_data(row or {}, default_actor="user", default_reason="stop requested"))
        elif status == "timeout":
            data.update({"actor": "timeout", "reason": f"exceeded timeout of {row['timeout_seconds']}s" if row and row["timeout_seconds"] else "exceeded configured timeout"})
        self._terminal_event(job_id, status, exit_code, claim_token=claim_token, data=data)
        self.processes.pop(job_id, None)

    def _event_query(self, job_id: str, types: list[str] | None, since_event_id: str | None, limit: int,
                     reverse: bool = False) -> list[dict[str, Any]]:
        since_seq = None
        if since_event_id:
            row = self._row("SELECT seq FROM events WHERE job_id=? AND event_id=?", (job_id, since_event_id))
            since_seq = int(row["seq"]) if row else None
        args: list[Any] = [job_id]
        if reverse:
            if since_seq is not None:
                where = "job_id=? AND seq<?"
                args.append(since_seq)
            else:
                where = "job_id=?"
            order = "seq DESC"
        else:
            where = "job_id=? AND seq>?"
            args.append(since_seq if since_seq is not None else 0)
            order = "seq"
        if types:
            where += " AND type IN (%s)" % ",".join("?" for _ in types)
            args.extend(types)
        with self.db_lock:
            rows = self.db.execute(
                f"SELECT * FROM events WHERE {where} ORDER BY {order} LIMIT ?",
                (*args, limit),
            ).fetchall()
        return [self._event_dict(row) for row in rows]

    async def wait(
        self,
        job_id: str,
        filters: list[str],
        since_event_id: str | None = None,
        timeout_seconds: int = 3600,
        return_progress: bool = False,
        metric_ge: dict[str, float] | None = None,
    ) -> dict[str, Any]:
        return await asyncio.get_running_loop().run_in_executor(
            None,
            self.wait_sync,
            job_id,
            filters,
            since_event_id,
            timeout_seconds,
            return_progress,
            metric_ge,
        )

    def wait_sync(
        self,
        job_id: str,
        filters: list[str],
        since_event_id: str | None = None,
        timeout_seconds: int = 3600,
        return_progress: bool = False,
        metric_ge: dict[str, float] | None = None,
    ) -> dict[str, Any]:
        return self._wait(job_id, filters, since_event_id, timeout_seconds, return_progress, metric_ge)

    def _wait(
        self,
        job_id: str,
        filters: list[str],
        since_event_id: str | None = None,
        timeout_seconds: int = 3600,
        return_progress: bool = False,
        metric_ge: dict[str, float] | None = None,
    ) -> dict[str, Any]:
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)) or timeout_seconds < 0 or timeout_seconds > 86400:
            raise ValueError("timeout_seconds must be between 0 and 86400")
        if not self._row("SELECT job_id FROM jobs WHERE job_id=?", (job_id,)):
            raise ValueError(f"Unknown job_id: {job_id}")
        if metric_ge is not None:
            if not isinstance(metric_ge, dict) or not metric_ge:
                raise ValueError("metric_ge must be a non-empty object of metric names to numeric thresholds")
            for metric, threshold in metric_ge.items():
                if not isinstance(metric, str) or not metric:
                    raise ValueError("metric_ge keys must be non-empty metric names")
                if isinstance(threshold, bool) or not isinstance(threshold, (int, float)):
                    raise ValueError(f"metric_ge threshold for {metric!r} must be a number")
        deadline = time.monotonic() + timeout_seconds
        while True:
            if self.shutdown_requested.is_set():
                return {"result": "shutdown", "job_id": job_id, "message": "Vanth is shutting down"}
            try:
                events = self._event_query(job_id, filters, since_event_id, 1)
            except RuntimeError:
                return {"result": "shutdown", "job_id": job_id, "message": "Vanth is shutting down"}
            # Return the EARLIEST matching signal (terminal / metric threshold /
            # progress), not a fixed precedence: with a streaming cursor the
            # caller expects events in order, and a threshold crossed before the
            # job finished must win over the terminal event that follows it.
            candidates: list[tuple[int, dict[str, Any]]] = []
            since_seq = 0
            if since_event_id:
                since_row = self._row(
                    "SELECT seq FROM events WHERE job_id=? AND event_id=?", (job_id, since_event_id)
                )
                if since_row is not None:
                    since_seq = int(since_row["seq"])
            if events:
                candidates.append((
                    int(events[0].get("seq") or 0),
                    {"result": "event", "job_id": job_id, "status": self.status(job_id)["status"], "event": events[0]},
                ))
            if metric_ge:
                try:
                    for metric, threshold in metric_ge.items():
                        # Cursor-aware and sample-consistent: the threshold is
                        # checked against the very sample whose event is returned.
                        value, metric_event = self._latest_metric_sample_after(job_id, metric, since_seq)
                        if metric_event is None or value is None or value < threshold:
                            continue
                        candidates.append((
                            int(metric_event.get("seq") or 0),
                            {
                                "result": "metric",
                                "job_id": job_id,
                                "status": self.status(job_id)["status"],
                                "metric": metric,
                                "threshold": threshold,
                                "value": value,
                                "event": metric_event,
                            },
                        ))
                except RuntimeError:
                    pass
            if return_progress and "progress" not in filters:
                try:
                    progress = self._event_query(job_id, ["progress"], since_event_id, 1)
                except RuntimeError:
                    progress = []
                if progress:
                    candidates.append((
                        int(progress[0].get("seq") or 0),
                        {
                            "result": "progress",
                            "job_id": job_id,
                            "event": progress[0],
                            "status": self.status(job_id)["status"],
                            "progress": progress[0].get("data"),
                        },
                    ))
            if candidates:
                candidates.sort(key=lambda item: item[0])
                return candidates[0][1]
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return {"result": "timeout", "job_id": job_id, "status": self.status(job_id)["status"], "message": "No matching event before timeout"}
            with self._condition(job_id):
                if not self._condition(job_id).wait(timeout=min(0.1, remaining)):
                    if remaining <= 0:
                        return {"result": "timeout", "job_id": job_id, "status": self.status(job_id)["status"], "message": "No matching event before timeout"}
                    continue
                else:
                    continue

    def status(self, job_id: str) -> dict[str, Any]:
        row = self._row("SELECT * FROM jobs WHERE job_id=?", (job_id,))
        if not row:
            raise ValueError(f"Unknown job_id: {job_id}")
        with self.db_lock:
            last = self.db.execute("SELECT * FROM events WHERE job_id=? ORDER BY seq DESC LIMIT 1", (job_id,)).fetchone()
            progress = self.db.execute(
                "SELECT * FROM events WHERE job_id=? AND type='progress' ORDER BY seq DESC LIMIT 1",
                (job_id,),
            ).fetchone()
        result = {
            "job_id": job_id,
            "status": row["status"],
            "command": row["command"],
            "cwd": row["cwd"],
            "timeout_seconds": row["timeout_seconds"],
            "pid": row["pid"],
            "worker_pid": row["worker_pid"],
            "name": row["name"],
            "origin_thread_id": row["origin_thread_id"],
            "wake_thread_id": row["wake_thread_id"],
            "tags": json.loads(row["tags_json"] or "[]"),
            "env": json.loads(row["env_json"] or "{}"),
            "notes": row["notes"],
            "run": json.loads(row["run_json"] or "{}"),
            "trigger": json.loads(row["trigger_json"] or "null"),
            "policy": json.loads(row["policy_json"] or "null"),
            "secret_env": json.loads(row["secret_env_json"] or "[]"),
            "stop_actor": row["stop_actor"],
            "stop_reason": row["stop_reason"],
            "pool": row["pool"],
            "priority": row["priority"],
            "paused": bool(row["paused"]),
            "schedule_id": row["schedule_id"],
            "runtime_seconds": _runtime_seconds(row["started_at"], row["ended_at"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "exit_code": row["exit_code"],
            "last_event": self._event_dict(last) if last else None,
        }
        result["progress"] = ({**json.loads(progress["data_json"] or "{}"), "updated_at": progress["created_at"]} if progress else None)
        if row["status"] in {"failed", "timeout", "orphaned", "cancelled"}:
            reason = row["status"]
            action = f"Inspect job_tail for {job_id} before rerunning"
            if reason == "failed":
                began = self._row(
                    "SELECT 1 FROM events WHERE job_id=? AND type='started' AND created_at>=? LIMIT 1",
                    (job_id, row["started_at"] or row["created_at"]),
                )
                reason = "workload_failed" if began else "startup_failed"
                capture = self._row(
                    "SELECT type FROM events WHERE job_id=? AND type IN ('log_capture_failed','pipe_drain_timeout') "
                    "AND created_at>=? ORDER BY seq DESC LIMIT 1",
                    (job_id, row["started_at"] or row["created_at"]),
                )
                if capture:
                    reason = capture["type"]
                    action = "Inspect job_doctor capture diagnostics and check disk space or descendant processes"
            elif reason == "timeout":
                action = "Inspect captured output and workload progress; increase the timeout only if expected"
            elif reason == "orphaned":
                action = "Inspect job_status and verify the previous workload has stopped before rerunning"
            elif reason == "cancelled":
                action = "Inspect stop_actor and stop_reason before deciding whether to rerun"
            result.update(failure_reason=reason, recommended_next_action=action)
        return result

    def status_batch(self, job_ids: list[str], limit: int = 500) -> dict[str, Any]:
        self._ensure_open()
        validate_limit(limit, "limit", 1000)
        if not isinstance(job_ids, list) or not job_ids:
            raise ValueError("job_ids must be a non-empty list of job ids")
        if any(isinstance(job_id, bool) or not isinstance(job_id, str) for job_id in job_ids):
            raise ValueError("job_ids must be a list of strings")
        if len(job_ids) > limit:
            raise ValueError(f"job_ids must contain at most {limit} ids")
        jobs = []
        unknown = []
        for job_id in job_ids:
            try:
                jobs.append(self.status(job_id))
            except ValueError:
                unknown.append(job_id)
                jobs.append({"job_id": job_id, "status": "unknown", "error": "Unknown job_id"})
        return {"jobs": jobs, "count": len(jobs), "unknown": unknown}

    def list(self, status: list[str] | None = None, limit: int = 50, thread_id: str | None = None,
             name: str | None = None, tags: list[str] | None = None) -> dict[str, Any]:
        self._ensure_open()
        validate_limit(limit, "limit")
        args: list[Any] = []
        filters = []
        if status:
            filters.append("status IN (%s)" % ",".join("?" for _ in status))
            args.extend(status)
        if thread_id:
            filters.append("(origin_thread_id=? OR wake_thread_id=?)")
            args.extend([thread_id, thread_id])
        if name:
            filters.append("name LIKE ?")
            args.append(f"%{name}%")
        for tag in tags or []:
            filters.append("tags_json LIKE ?")
            args.append(f'%"{tag}"%')
        where = "WHERE " + " AND ".join(filters) if filters else ""
        with self.db_lock:
            rows = self.db.execute(
                f"""
                SELECT job_id, name, status, created_at, started_at, ended_at, updated_at,
                       exit_code, origin_thread_id, wake_thread_id, tags_json
                FROM jobs {where} ORDER BY updated_at DESC LIMIT ?
                """,
                (*args, limit),
            ).fetchall()
        jobs = []
        for row in rows:
            item = dict(row)
            item["tags"] = json.loads(item.pop("tags_json") or "[]")
            # Derived here so presentation layers (CLI `vanth list`, MCP
            # job_list) never have to re-derive runtime or fall back to
            # `updated_at` (which a running job's heartbeat refreshes, making
            # any age computed from it collapse to ~0s).
            item["runtime_seconds"] = _runtime_seconds(row["started_at"], row["ended_at"])
            jobs.append(item)
        return {"jobs": jobs}

    def duration_stats(
        self,
        name: str | None = None,
        tags: list[str] | None = None,
        limit: int = 20,
        runs_per_group: int = 200,
        since_ms: int | None = None,
        slowest: int = 10,
    ) -> dict[str, Any]:
        """Per-logical-job duration, queue-time, and flakiness analytics.

        Groups terminal runs by ``name`` (falling back to the command) and
        reports p50/p95 runtime and queue time, success rate, a flaky score
        (a failed run that has a success both before and after it), each group's
        slowest runs, and a trend flag that catches a job creeping slower over
        time. ``slowest`` also returns the top-N slowest runs across all groups.
        """
        self._ensure_open()
        validate_limit(limit, "limit", 200)
        validate_limit(runs_per_group, "runs_per_group", 5000)
        validate_limit(slowest, "slowest", 100)
        where = [
            "status IN ('completed','failed','timeout','cancelled','orphaned')",
            "started_at IS NOT NULL",
            "ended_at IS NOT NULL",
        ]
        args: list[Any] = []
        if name:
            where.append("name LIKE ?")
            args.append(f"%{name}%")
        for tag in tags or []:
            where.append("tags_json LIKE ?")
            args.append(f'%"{tag}"%')
        if since_ms is not None:
            cutoff = datetime.fromtimestamp(since_ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")
            where.append("ended_at >= ?")
            args.append(cutoff)
        row_cap = min(50000, max(1000, runs_per_group * max(1, limit)))
        with self.db_lock:
            rows = self.db.execute(
                f"SELECT job_id, name, command, status, created_at, started_at, ended_at, exit_code "
                f"FROM jobs WHERE {' AND '.join(where)} ORDER BY created_at DESC LIMIT ?",
                (*args, row_cap),
            ).fetchall()
        groups: dict[str, list[Any]] = {}
        for row in rows:
            key = row["name"] or row["command"] or row["job_id"]
            groups.setdefault(key, []).append(row)
        out_groups: list[dict[str, Any]] = []
        all_runs: list[dict[str, Any]] = []
        for key, group in groups.items():
            group.sort(key=lambda r: r["created_at"] or "")
            durations: list[float] = []
            queues: list[float] = []
            results: list[dict[str, Any]] = []
            for row in group:
                duration = _elapsed_seconds(row["started_at"], row["ended_at"])
                if duration is not None:
                    durations.append(duration)
                queue = _elapsed_seconds(row["created_at"], row["started_at"])
                if queue is not None:
                    queues.append(queue)
                results.append(
                    {
                        "job_id": row["job_id"],
                        "status": row["status"],
                        "duration_seconds": _round3(duration),
                        "created_at": row["created_at"],
                        "ended_at": row["ended_at"],
                        "exit_code": row["exit_code"],
                    }
                )
                all_runs.append({**results[-1], "key": key})
            completed = sum(1 for row in group if row["status"] == "completed")
            failed = sum(1 for row in group if row["status"] in {"failed", "timeout"})
            flaky = 0
            for index, row in enumerate(group):
                if row["status"] not in {"failed", "timeout"}:
                    continue
                succeeded_before = any(g["status"] == "completed" for g in group[:index])
                succeeded_after = any(g["status"] == "completed" for g in group[index + 1 :])
                if succeeded_before and succeeded_after:
                    flaky += 1
            group_slowest = sorted(
                (r for r in results if r["duration_seconds"] is not None),
                key=lambda r: r["duration_seconds"],
                reverse=True,
            )[:slowest]
            out_groups.append(
                {
                    "key": key,
                    "runs": len(group),
                    "completed": completed,
                    "failed": failed,
                    "success_rate": round(completed / len(group), 4) if group else None,
                    "duration_seconds": {
                        "p50": _round3(_percentile(durations, 0.5)),
                        "p95": _round3(_percentile(durations, 0.95)),
                        "mean": _round3(sum(durations) / len(durations)) if durations else None,
                        "min": _round3(min(durations)) if durations else None,
                        "max": _round3(max(durations)) if durations else None,
                    },
                    "queue_seconds": {
                        "p50": _round3(_percentile(queues, 0.5)),
                        "p95": _round3(_percentile(queues, 0.95)),
                    },
                    "flaky_score": round(flaky / len(group), 4) if group else None,
                    "flaky_runs": flaky,
                    "trend": _duration_trend([d for d in durations]),
                    "slowest_runs": group_slowest,
                    "last_run": results[-1] if results else None,
                }
            )
        out_groups.sort(key=lambda g: (g["duration_seconds"]["p95"] or 0), reverse=True)
        global_slowest = sorted(
            (r for r in all_runs if r["duration_seconds"] is not None),
            key=lambda r: r["duration_seconds"],
            reverse=True,
        )[:slowest]
        return {"groups": out_groups[:limit], "slowest": global_slowest, "group_count": len(out_groups)}

    def events(self, job_id: str, since_event_id: str | None = None, types: list[str] | None = None, limit: int = 20,
               reverse: bool = False) -> dict[str, Any]:
        self._ensure_open()
        validate_limit(limit, "limit")
        if not self._row("SELECT job_id FROM jobs WHERE job_id=?", (job_id,)):
            raise ValueError(f"Unknown job_id: {job_id}")
        return {"events": self._event_query(job_id, types, since_event_id, limit, reverse=reverse)}

    def deliveries(self, job_id: str | None = None, status: str | None = None, limit: int = 20) -> dict[str, Any]:
        self._ensure_open()
        validate_limit(limit, "limit")
        where = []
        args: list[Any] = []
        if job_id:
            where.append("job_id=?")
            args.append(job_id)
        if status:
            where.append("status=?")
            args.append(status)
        sql_where = "WHERE " + " AND ".join(where) if where else ""
        with self.db_lock:
            rows = self.db.execute(
                f"SELECT * FROM deliveries {sql_where} ORDER BY created_at DESC LIMIT ?",
                (*args, limit),
            ).fetchall()
        return {"deliveries": [self._delivery_dict(row) for row in rows]}

    def metrics_query(self, job_id: str, metric: str | None = None, from_ms: int | None = None,
                      to_ms: int | None = None, limit: int = 1000) -> dict[str, Any]:
        """Return stored scalar series for one job.

        Series come from ``metric``/``progress`` AGENT_EVENT payloads mirrored
        into ``metric_series``. ``metric`` filters to one name (e.g.
        ``loss`` or ``progress.percent``); without it all metrics for the job
        are returned grouped by name. ``from_ms``/``to_ms`` filter by event
        timestamp (milliseconds since epoch). Points are ordered by seq.
        """
        self._ensure_open()
        validate_limit(limit, "limit", 10000)
        if not self._row("SELECT job_id FROM jobs WHERE job_id=?", (job_id,)):
            raise ValueError(f"Unknown job_id: {job_id}")
        where = ["job_id=?"]
        args: list[Any] = [job_id]
        if metric:
            where.append("metric=?")
            args.append(metric)
        if from_ms is not None:
            where.append("created_at >= ?")
            args.append(_ms_to_iso(from_ms))
        if to_ms is not None:
            where.append("created_at <= ?")
            args.append(_ms_to_iso(to_ms))
        with self.db_lock:
            rows = self.db.execute(
                f"""
                SELECT metric, x, y, stage, event_id, seq, created_at
                FROM metric_series WHERE {' AND '.join(where)}
                ORDER BY metric, seq ASC LIMIT ?
                """,
                (*args, limit),
            ).fetchall()
        series: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            point = {
                "x": row["x"],
                "y": row["y"],
                "stage": row["stage"],
                "event_id": row["event_id"],
                "seq": row["seq"],
                "at": row["created_at"],
            }
            series.setdefault(row["metric"], []).append(point)
        return {"job_id": job_id, "series": series, "metrics": list(series.keys())}

    def metric_compare(self, job_ids: list[str], metric: str, aggregation: str = "latest",
                       from_ms: int | None = None, to_ms: int | None = None) -> dict[str, Any]:
        """Compare one metric across jobs.

        ``aggregation`` is one of ``latest`` (last point), ``mean``, ``min``,
        ``max``, ``sum``, or ``count``. Returns per-job summary values plus the
        raw series points so agents can reason about the comparison.
        """
        self._ensure_open()
        valid_aggs = {"latest", "mean", "min", "max", "sum", "count"}
        if aggregation not in valid_aggs:
            raise ValueError(f"aggregation must be one of {sorted(valid_aggs)}")
        if not isinstance(job_ids, list) or not job_ids or len(job_ids) > 50:
            raise ValueError("job_ids must be a non-empty list of at most 50 ids")
        if not isinstance(metric, str) or not metric:
            raise ValueError("metric must be a non-empty string")
        for job_id in job_ids:
            if not self._row("SELECT job_id FROM jobs WHERE job_id=?", (job_id,)):
                raise ValueError(f"Unknown job_id: {job_id}")
        result: dict[str, Any] = {"metric": metric, "aggregation": aggregation, "jobs": {}}
        for job_id in job_ids:
            points = self.metrics_query(job_id, metric, from_ms, to_ms, limit=10000)["series"].get(metric, [])
            values = [p["y"] for p in points]
            summary: Any = None
            if values:
                if aggregation == "latest":
                    summary = values[-1]
                elif aggregation == "mean":
                    summary = sum(values) / len(values)
                elif aggregation == "min":
                    summary = min(values)
                elif aggregation == "max":
                    summary = max(values)
                elif aggregation == "sum":
                    summary = sum(values)
                elif aggregation == "count":
                    summary = len(values)
            result["jobs"][job_id] = {
                "value": summary,
                "points": len(points),
                "first": points[0] if points else None,
                "last": points[-1] if points else None,
            }
        return result

    def _latest_metric_value(self, job_id: str, metric: str) -> float | None:
        """Return the latest stored value for one job+metric, or None."""
        with self.db_lock:
            row = self.db.execute(
                "SELECT y FROM metric_series WHERE job_id=? AND metric=? ORDER BY seq DESC LIMIT 1",
                (job_id, metric),
            ).fetchone()
        return float(row["y"]) if row else None

    def _latest_metric_event(self, job_id: str, metric: str) -> dict[str, Any] | None:
        """Return the event that produced the latest point for one job+metric."""
        with self.db_lock:
            row = self.db.execute(
                """
                SELECT e.* FROM events e
                JOIN metric_series m ON m.event_id = e.event_id
                WHERE e.job_id=? AND m.metric=?
                ORDER BY m.seq DESC LIMIT 1
                """,
                (job_id, metric),
            ).fetchone()
        return self._event_dict(row) if row else None

    def _latest_metric_sample_after(
        self, job_id: str, metric: str, after_seq: int
    ) -> tuple[float | None, dict[str, Any] | None]:
        """Latest metric sample strictly after ``after_seq``, read atomically.

        Returns ``(value, event)`` for the SAME sample so the threshold check and
        the returned event cannot disagree: reading the value and the event in
        separate queries could pair a stale satisfying value with a newer event
        that no longer satisfies the threshold.
        """
        with self.db_lock:
            row = self.db.execute(
                """
                SELECT e.*, m.y AS metric_value FROM events e
                JOIN metric_series m ON m.event_id = e.event_id
                WHERE e.job_id=? AND m.metric=? AND e.seq > ?
                ORDER BY e.seq DESC LIMIT 1
                """,
                (job_id, metric, int(after_seq)),
            ).fetchone()
        if row is None:
            return None, None
        return float(row["metric_value"]), self._event_dict(row)

    def metric_ingest(self, job_id: str, metrics: list[dict[str, Any]], idempotency_key: str | None = None) -> dict[str, Any]:
        """Record scalar metric points for a job programmatically.

        Each point is ``{"name": str, "value": float, "ts_ms": int|None,
        "labels": dict|None}``. Points are mirrored into the same
        ``metric_series`` pipeline used by ``metric`` AGENT_EVENT payloads, so
        ``metrics_query``/``metric_compare`` see them immediately. A repeated
        ``idempotency_key`` is ignored.
        """
        self._ensure_open()
        if not self._row("SELECT job_id FROM jobs WHERE job_id=?", (job_id,)):
            raise ValueError(f"Unknown job_id: {job_id}")
        if not isinstance(metrics, list) or not metrics:
            raise ValueError("metrics must be a non-empty list")
        if len(metrics) > 1000:
            raise ValueError("metrics must contain at most 1000 points")
        if idempotency_key is not None and not isinstance(idempotency_key, str):
            raise ValueError("idempotency_key must be a string")
        points: list[tuple[str, float, int | None, dict[str, Any]]] = []
        for point in metrics:
            if not isinstance(point, dict):
                raise ValueError("each metric point must be an object")
            name = point.get("name")
            if not isinstance(name, str) or not name:
                raise ValueError("each metric point must have a non-empty string name")
            value = point.get("value")
            if not self._is_finite_number(value):
                raise ValueError(f"metric {name!r} value must be a finite number")
            ts_ms = point.get("ts_ms")
            if ts_ms is not None and (isinstance(ts_ms, bool) or not isinstance(ts_ms, int)):
                raise ValueError("ts_ms must be an integer epoch milliseconds")
            labels = point.get("labels")
            if labels is not None:
                if not isinstance(labels, dict) or not all(
                    isinstance(key, str) and isinstance(label_value, (str, int, float))
                    for key, label_value in labels.items()
                ):
                    raise ValueError(f"metric {name!r} labels must be a dict of scalar values")
            points.append((name, float(value), ts_ms, labels or {}))
        if idempotency_key is not None:
            if idempotency_key in self._metric_ingest_keys:
                return {
                    "result": "ok",
                    "job_id": job_id,
                    "ingested": 0,
                    "event_id": None,
                    "deduplicated": True,
                }
            self._metric_ingest_keys.add(idempotency_key)
        event_id: str | None = None
        for name, value, ts_ms, labels in points:
            data: dict[str, Any] = {name: value}
            if ts_ms is not None:
                data["_step"] = ts_ms
            data.update(labels)
            event = self._emit(job_id, "metric", data=data, message=f"metric ingest {name}", source="server")
            event_id = event.get("event_id")
            if not event_id:
                if idempotency_key is not None:
                    self._metric_ingest_keys.discard(idempotency_key)
                raise RuntimeError("metric ingest event was not persisted")
        return {"result": "ok", "job_id": job_id, "ingested": len(points), "event_id": event_id}

    def run_summary(self, job_id: str, include_stderr_excerpt: bool = False,
                    include_stdout_excerpt: bool = False) -> dict[str, Any]:
        """One-call summary of a job: status, runtime, progress, top metrics.

        Computes the latest value of every stored metric series plus the last
        progress event, so an agent can answer "did it work?" in a single call.
        """
        status = self.status(job_id)
        series = self.metrics_query(job_id, limit=10000)["series"]
        latest_metrics = {metric: points[-1]["y"] for metric, points in series.items() if points}
        # Build per-metric latest + a compact metric overview.
        overview = []
        for metric, points in sorted(series.items()):
            if not points:
                continue
            overview.append(
                {
                    "metric": metric,
                    "latest": points[-1]["y"],
                    "first": points[0]["y"],
                    "min": min(p["y"] for p in points),
                    "max": max(p["y"] for p in points),
                    "count": len(points),
                    "stage": points[-1].get("stage"),
                }
            )
        artifacts = self.artifacts(job_id)["artifacts"]
        summary = {
            "job_id": job_id,
            "status": status["status"],
            "name": status.get("name"),
            "runtime_seconds": status.get("runtime_seconds"),
            "exit_code": status.get("exit_code"),
            "progress": status.get("progress"),
            "notes": status.get("notes"),
            "metrics": overview,
            "latest_metrics": latest_metrics,
            "artifacts": artifacts,
        }
        if include_stderr_excerpt:
            summary["stderr_excerpt"] = self.tail(job_id, stream="stderr", max_bytes=2048)["content"]
        if include_stdout_excerpt:
            summary["stdout_excerpt"] = self.tail(job_id, stream="stdout", max_bytes=8192)["content"]
        for field in ("failure_reason", "recommended_next_action"):
            if field in status:
                summary[field] = status[field]
        return summary

    def diff_spec(self, base_job_id: str, other_job_id: str) -> dict[str, Any]:
        """Diff the run specs (command/env/cwd/timeout/etc) of two jobs."""
        self._ensure_open()
        base = self._row("SELECT * FROM jobs WHERE job_id=?", (base_job_id,))
        other = self._row("SELECT * FROM jobs WHERE job_id=?", (other_job_id,))
        if not base:
            raise ValueError(f"Unknown job_id: {base_job_id}")
        if not other:
            raise ValueError(f"Unknown job_id: {other_job_id}")

        fields = ["command", "cwd", "timeout_seconds", "name", "notes", "interactive"]
        changes: list[dict[str, Any]] = []
        for field in fields:
            if field == "interactive":
                base_value = json.loads(base["run_json"] or "{}").get("interactive") is True
                other_value = json.loads(other["run_json"] or "{}").get("interactive") is True
            else:
                base_value = base[field]
                other_value = other[field]
            if base_value != other_value:
                changes.append({
                    "field": field,
                    "base": base_value,
                    "other": other_value,
                })

        base_env = json.loads(base["env_json"] or "{}")
        other_env = json.loads(other["env_json"] or "{}")
        env_changes: list[dict[str, Any]] = []
        for key in sorted(set(base_env) | set(other_env)):
            if base_env.get(key) != other_env.get(key):
                env_changes.append({"key": key, "base": base_env.get(key), "other": other_env.get(key)})
        if env_changes:
            changes.append({"field": "env", "changes": env_changes})

        base_tags = json.loads(base["tags_json"] or "[]")
        other_tags = json.loads(other["tags_json"] or "[]")
        if sorted(base_tags) != sorted(other_tags):
            changes.append({"field": "tags", "base": base_tags, "other": other_tags})

        base_targets = self._wake_targets_for_job(base_job_id)
        other_targets = self._wake_targets_for_job(other_job_id)
        if base_targets != other_targets:
            changes.append({"field": "wake_targets", "base": base_targets, "other": other_targets})

        return {
            "base_job_id": base_job_id,
            "other_job_id": other_job_id,
            "identical": not changes,
            "changes": changes,
        }

    def _wake_targets_for_job(self, job_id: str) -> list[dict[str, Any]]:
        with self.db_lock:
            rows = self.db.execute(
                "SELECT type, events_json, config_json FROM wake_targets WHERE job_id=? ORDER BY created_at ASC",
                (job_id,),
            ).fetchall()
        return [
            {
                "type": row["type"],
                "events": json.loads(row["events_json"] or "[]"),
                **json.loads(row["config_json"] or "{}"),
            }
            for row in rows
        ]


    def artifacts(self, job_id: str, limit: int = 50) -> dict[str, Any]:
        self._ensure_open()
        validate_limit(limit, "limit", 1000)
        if not self._row("SELECT job_id FROM jobs WHERE job_id=?", (job_id,)):
            raise ValueError(f"Unknown job_id: {job_id}")
        with self.db_lock:
            rows = self.db.execute(
                "SELECT * FROM artifacts WHERE job_id=? ORDER BY created_at ASC LIMIT ?",
                (job_id, limit),
            ).fetchall()
        return {
            "artifacts": [
                {
                    "artifact_id": row["artifact_id"],
                    "job_id": row["job_id"],
                    "name": row["name"],
                    "uri": row["uri"],
                    "kind": row["kind"],
                    "size_bytes": row["size_bytes"],
                    "sha256": row["sha256"],
                    "meta": json.loads(row["meta_json"] or "{}"),
                    "created_at": row["created_at"],
                }
                for row in rows
            ]
        }

    def artifact_add(self, job_id: str, name: str, uri: str, kind: str | None = None,
                     size_bytes: int | None = None, sha256: str | None = None,
                     meta: dict[str, Any] | None = None) -> dict[str, Any]:
        self._ensure_open()
        if not isinstance(name, str) or not name:
            raise ValueError("name must be a non-empty string")
        if not isinstance(uri, str) or not uri:
            raise ValueError("uri must be a non-empty string")
        if not self._row("SELECT job_id FROM jobs WHERE job_id=?", (job_id,)):
            raise ValueError(f"Unknown job_id: {job_id}")
        artifact_id = "art_" + uuid.uuid4().hex[:16]
        created_at = now_iso()
        with self.db_lock:
            self.db.execute(
                """
                INSERT INTO artifacts(artifact_id, job_id, name, uri, kind, size_bytes, sha256, meta_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (artifact_id, job_id, name, uri, kind, size_bytes, sha256,
                 json.dumps(meta or {}, separators=(",", ":")), created_at),
            )
            self.db.commit()
        return {"artifact_id": artifact_id, "job_id": job_id, "name": name, "uri": uri,
                "kind": kind, "size_bytes": size_bytes, "sha256": sha256,
                "meta": meta or {}, "created_at": created_at}

    def artifact_read(self, artifact_id: str, max_bytes: int = 262144) -> dict[str, Any]:
        """Read the content of an artifact (file://, local path, or http(s)://).

        Returns base64-encoded content so binary and JSON artifacts round-trip
        cleanly through the MCP JSON transport. ``truncated`` is set when the
        artifact exceeds ``max_bytes``.

        HTTP(S) retrieval is disabled by default and must be opted into via
        ``VANTH_ALLOW_HTTP_ARTIFACT_READ=1``. This path is legacy and is never
        used by managed artifacts (see ``artifacts/``).
        """
        self._ensure_open()
        if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 256 or max_bytes > 1024 * 1024:
            raise ValueError("max_bytes must be an integer between 256 and 1048576")
        row = self._row(
            "SELECT artifact_id, job_id, name, uri, kind, size_bytes, sha256 FROM artifacts WHERE artifact_id=?",
            (artifact_id,),
        )
        if not row:
            raise ValueError(f"artifact content unavailable: unknown artifact_id: {artifact_id}")
        uri = row["uri"]
        parsed = urllib.parse.urlparse(uri)
        if parsed.scheme in {"http", "https"}:
            if os.environ.get("VANTH_ALLOW_HTTP_ARTIFACT_READ") != "1":
                raise ValueError(
                    "artifact content unavailable: http(s) retrieval is disabled; "
                    "set VANTH_ALLOW_HTTP_ARTIFACT_READ=1 to enable legacy retrieval"
                )
            try:
                with urllib.request.urlopen(uri, timeout=5) as response:
                    content = response.read(max_bytes + 1)
            except Exception as exc:
                raise ValueError(f"artifact content unavailable: {exc}") from None
        elif parsed.scheme in {"", "file"} or (len(parsed.scheme) == 1 and parsed.scheme.isalpha()):
            path = Path(uri)
            if parsed.scheme == "file":
                path = Path(urllib.request.url2pathname(parsed.path))
            if not path.is_absolute():
                path = Path(self.home) / path
            if not path.exists() or not path.is_file():
                raise ValueError(f"artifact content unavailable: {uri}")
            with path.open("rb") as handle:
                content = handle.read(max_bytes + 1)
        else:
            raise ValueError(f"artifact content unavailable: unsupported scheme {parsed.scheme!r}")
        truncated = len(content) > max_bytes
        content = content[:max_bytes]
        return {
            "artifact_id": row["artifact_id"],
            "name": row["name"],
            "kind": row["kind"],
            "uri": uri,
            "size_bytes": row["size_bytes"],
            "content_base64": base64.b64encode(content).decode("ascii"),
            "truncated": truncated,
            "bytes_read": len(content),
        }

    def dashboard(self, job_ids: list[str] | None = None, limit: int = 5000) -> dict[str, Any]:
        """Chart-data view for one or more jobs, mirroring the Go monitor.

        Returns every stored metric series (downsampled to ``limit`` points
        per series) plus the job list, so any client can render charts the way
        the terminal monitor does.
        """
        self._ensure_open()
        validate_limit(limit, "limit", 50000)
        jobs = self.list(limit=100)["jobs"]
        if job_ids is None:
            job_ids = [j["job_id"] for j in jobs]
        elif not isinstance(job_ids, list) or not job_ids or len(job_ids) > 50:
            raise ValueError("job_ids must be a non-empty list of at most 50 ids")
        for job_id in job_ids:
            if not self._row("SELECT job_id FROM jobs WHERE job_id=?", (job_id,)):
                raise ValueError(f"Unknown job_id: {job_id}")
        series: dict[str, dict[str, list[dict[str, Any]]]] = {}
        for job_id in job_ids:
            q = self.metrics_query(job_id, limit=limit)["series"]
            series[job_id] = {metric: _downsample(points, limit) for metric, points in q.items()}
        return {"jobs": jobs, "series": series, "series_count": sum(len(v) for v in series.values())}

    def _delivery_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "delivery_id": row["delivery_id"],
            "event_id": row["event_id"],
            "target_id": row["target_id"],
            "job_id": row["job_id"],
            "target_type": row["target_type"],
            "status": row["status"],
            "attempts": row["attempts"],
            "payload": json.loads(row["payload_json"] or "{}"),
            "created_at": row["created_at"],
            "next_attempt_at": row["next_attempt_at"],
            "delivered_at": row["delivered_at"],
            "last_error": row["last_error"],
            "claim_token": row["claim_token"],
            "claimed_at": row["claimed_at"],
            "lease_expires_at": row["lease_expires_at"],
        }

    def delivery_attempts(self, delivery_id: str, limit: int = 20) -> dict[str, Any]:
        validate_limit(limit, "limit")
        if not self._row("SELECT delivery_id FROM deliveries WHERE delivery_id=?", (delivery_id,)):
            raise ValueError(f"Unknown delivery_id: {delivery_id}")
        with self.db_lock:
            rows = self.db.execute(
                "SELECT * FROM delivery_attempts WHERE delivery_id=? ORDER BY attempt DESC LIMIT ?",
                (delivery_id, limit),
            ).fetchall()
        return {
            "attempts": [
                {
                    "attempt_id": row["attempt_id"],
                    "delivery_id": row["delivery_id"],
                    "attempt": row["attempt"],
                    "claim_token": row["claim_token"],
                    "target_type": row["target_type"],
                    "started_at": row["started_at"],
                    "ended_at": row["ended_at"],
                    "status": row["status"],
                    "error": row["error"],
                    "reclaimed": bool(row["reclaimed"]),
                    "created_at": row["created_at"],
                }
                for row in rows
            ]
        }

    def mark_delivery(self, delivery_id: str, status: str, error: str | None = None) -> dict[str, Any]:
        if not isinstance(status, str) or status not in {"pending", "retrying", "delivered", "failed"}:
            raise ValueError("invalid delivery status")
        error = (error or "")[:DEFAULT_MAX_ERROR_BYTES] or None
        delivered_at = now_iso() if status == "delivered" else None
        with self.db_lock:
            current = self._row("SELECT attempts FROM deliveries WHERE delivery_id=?", (delivery_id,))
            if not current:
                raise ValueError(f"Unknown delivery_id: {delivery_id}")
            attempt = int(current["attempts"]) + 1
            self.db.execute(
                """
                UPDATE deliveries
                SET status=?, attempts=?, delivered_at=COALESCE(?, delivered_at), last_error=?, next_attempt_at=NULL
                WHERE delivery_id=?
                """,
                (status, attempt, delivered_at, error, delivery_id),
            )
            self.db.execute(
                """
                INSERT INTO delivery_attempts(attempt_id, delivery_id, attempt, target_type, started_at, ended_at, status, error, created_at)
                SELECT ?, delivery_id, ?, target_type, ?, ?, ?, ?, ? FROM deliveries WHERE delivery_id=?
                """,
                ("att_" + uuid.uuid4().hex[:16], attempt, now_iso(), now_iso(), status, error, now_iso(), delivery_id),
            )
            self.db.commit()
            row = self._row("SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,))
        return self._delivery_dict(row)

    def retry_delivery(self, delivery_id: str) -> dict[str, Any]:
        row = self._row("SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,))
        if not row:
            raise ValueError(f"Unknown delivery_id: {delivery_id}")
        with self.db_lock:
            # Refresh ``created_at`` (the age the TTL predicate reads) rather than
            # bumping attempts: a relay-addressed wake with no subscriber is never
            # claimed, so attempts must stay 0 for the TTL to still apply. Bumping
            # it would exempt this row from expiry and re-open the leak.
            changed = self.db.execute(
                """
                UPDATE deliveries SET status='retrying', next_attempt_at=NULL, last_error=NULL,
                    created_at=?
                WHERE delivery_id=? AND status IN ('failed','retrying')
                """,
                (now_iso(), delivery_id),
            ).rowcount
            self.db.commit()
            row = self._row("SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,))
        delivery = self._delivery_dict(row)
        return delivery

    def clear_deliveries(
        self,
        job_id: str | None = None,
        status: str | None = None,
        older_than_seconds: int | None = None,
        stale_only: bool = False,
        limit: int = 1000,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Bulk-drain the wake/delivery queue (agent-controllable overflow valve).

        Matches deliveries by optional job_id, status, and age; ``stale_only``
        restricts to deliveries whose SOURCE EVENT is terminal — i.e.
        notifications that lost their moment (the "disk full, everything
        floods at once" case). Returns what was (or would be) drained.

        Default drain set is the unsettled queue (``pending``/``retrying``):
        a bulk drain must never rewrite already-delivered records as failed
        (review P2-1). To drain settled history you must pass an explicit
        ``status`` (e.g. ``status="delivered"``). Draining a ``dispatching``
        row finalizes its in-flight delivery_attempts entry first.
        """
        validate_limit(limit, "limit", 10000)
        if status is not None and (
            not isinstance(status, str)
            or status not in {"pending", "retrying", "dispatching", "delivered", "failed"}
        ):
            raise ValueError("invalid delivery status")
        if older_than_seconds is not None and (isinstance(older_than_seconds, bool) or not isinstance(older_than_seconds, int) or older_than_seconds < 0):
            raise ValueError("older_than_seconds must be a non-negative integer")
        where = []
        args: list[Any] = []
        if job_id is not None:
            if not isinstance(job_id, str) or not job_id:
                raise ValueError("job_id must be a non-empty string")
            where.append("d.job_id=?")
            args.append(job_id)
        if status is not None:
            where.append("d.status=?")
            args.append(status)
        else:
            # Safe default: only unsettled deliveries. Settled rows (delivered/
            # failed) are audit history and must be explicitly opted into.
            where.append("d.status IN ('pending','retrying')")
        if older_than_seconds is not None:
            cutoff = (datetime.now(timezone.utc) - timedelta(seconds=older_than_seconds)).isoformat().replace("+00:00", "Z")
            where.append("d.created_at < ?")
            args.append(cutoff)
        if stale_only:
            # Only drain deliveries whose event no longer matters: the source
            # job already reached a terminal state. We accept any terminal
            # event; freshness is bounded by the source event's created_at.
            where.append(
                "EXISTS (SELECT 1 FROM events e JOIN jobs j ON j.job_id=e.job_id "
                "WHERE e.event_id=d.event_id AND j.status IN ('completed','failed','timeout','cancelled','orphaned'))"
            )
        sql_where = ("WHERE " + " AND ".join(where)) if where else ""
        with self.db_lock:
            count_row = self.db.execute(
                f"SELECT COUNT(*) AS n FROM deliveries d {sql_where}", tuple(args)
            ).fetchone()
            matched = int(count_row["n"])
            if dry_run or matched == 0:
                return {"matched": matched, "drained": 0, "dry_run": dry_run}
            rows = self.db.execute(
                f"SELECT d.delivery_id, d.status FROM deliveries d {sql_where} LIMIT ?",
                (*args, limit),
            ).fetchall()
            ids = [r["delivery_id"] for r in rows]
            placeholders = ",".join("?" for _ in ids)
            # Finalize any in-flight delivery_attempts rows for dispatching
            # deliveries being drained (never leave them dangling 'dispatching').
            self.db.execute(
                f"UPDATE delivery_attempts SET status='failed', ended_at=?, error='drained via clear_deliveries' "
                f"WHERE delivery_id IN ({placeholders}) AND ended_at IS NULL",
                (now_iso(), *ids),
            )
            cur = self.db.execute(
                f"UPDATE deliveries SET status='failed', last_error='drained via clear_deliveries', next_attempt_at=NULL, claim_token=NULL, claimed_at=NULL, lease_expires_at=NULL "
                f"WHERE delivery_id IN ({placeholders})",
                tuple(ids),
            )
            self.db.commit()
        return {"matched": matched, "drained": int(cur.rowcount), "dry_run": False, "ids": ids[:25]}

    def tail(self, job_id: str, stream: str = "stdout", max_bytes: int = 8192, offset: int | None = None,
             follow: bool = False, timeout_seconds: float = 5.0, grep: str | None = None) -> dict[str, Any]:
        validate_limit(max_bytes, "max_bytes", max(self.max_log_bytes, 8192))
        if not isinstance(stream, str) or stream not in {"stdout", "stderr"}:
            raise ValueError("stream must be stdout or stderr")
        if offset is not None and (isinstance(offset, bool) or not isinstance(offset, int) or offset < 0):
            raise ValueError("offset must be a non-negative integer")
        if not isinstance(timeout_seconds, (int, float)) or isinstance(timeout_seconds, bool) or timeout_seconds < 0 or timeout_seconds > 86400:
            raise ValueError("timeout_seconds must be between 0 and 86400")
        if grep is not None and (not isinstance(grep, str) or not grep):
            raise ValueError("grep must be a non-empty string")
        row = self._row(f"SELECT {stream}_path AS path FROM jobs WHERE job_id=?", (job_id,))
        if not row:
            raise ValueError(f"Unknown job_id: {job_id}")
        path = Path(row["path"])
        size = path.stat().st_size if path.exists() else 0
        if not path.exists():
            result = {
                "job_id": job_id,
                "stream": stream,
                "offset": 0,
                "next_offset": 0,
                "size": 0,
                "truncated": False,
                "content": "",
            }
            return self._tail_follow_result(job_id, stream, result, follow, max_bytes, timeout_seconds, path)
        start = max(0, offset or 0)
        truncated = False
        with path.open("rb") as f:
            if offset is None and size > max_bytes:
                f.seek(-max_bytes, os.SEEK_END)
                start = f.tell()
                truncated = True
            else:
                if size - start > max_bytes:
                    start = max(0, size - max_bytes)
                    truncated = True
                f.seek(min(start, size))
            content = f.read(max_bytes).decode(errors="replace")
            cursor = f.tell()
        if grep:
            content = "".join(line for line in content.splitlines(keepends=True) if grep in line)
        result = {
            "job_id": job_id,
            "stream": stream,
            "offset": start,
            "next_offset": cursor,
            "size": size,
            "truncated": truncated,
            "content": content,
        }
        if not follow:
            return result
        return self._tail_follow_result(job_id, stream, result, follow, max_bytes, timeout_seconds, path)

    def _tail_follow_result(self, job_id: str, stream: str, result: dict[str, Any], follow: bool,
                            max_bytes: int, timeout_seconds: float, path: Path) -> dict[str, Any]:
        if not follow:
            return result
        deadline = time.monotonic() + timeout_seconds
        cursor = int(result["next_offset"])
        new_chunks: list[str] = []
        limit = 4 * max_bytes
        seen_bytes = 0
        while True:
            if self.shutdown_requested.is_set():
                break
            got = False
            if path.exists():
                size = path.stat().st_size
                if size > cursor:
                    with path.open("rb") as f:
                        f.seek(cursor)
                        raw = f.read(max(0, min(limit - seen_bytes, size - cursor)))
                    if raw:
                        got = True
                        chunk = raw.decode(errors="replace")
                        new_chunks.append(chunk)
                        seen_bytes += len(chunk)
                        cursor += len(raw)
                        if seen_bytes >= limit:
                            break
            current_status = self.status(job_id)["status"]
            if not got and current_status in TERMINAL_STATUSES:
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(0.1, remaining))
        return {
            **result,
            "followed": True,
            "new_bytes": seen_bytes,
            "next_offset": cursor,
            "content": result["content"] + "".join(new_chunks),
            "size": path.stat().st_size if path.exists() else result["size"],
        }

    def agent_view(self, thread_id: str | None = None, limit: int = 50) -> dict[str, Any]:
        jobs = []
        for row in self.list(limit=limit, thread_id=thread_id)["jobs"]:
            status = self.status(row["job_id"])
            deliveries = self.deliveries(row["job_id"], limit=100)["deliveries"]
            counts: dict[str, int] = {}
            for delivery in deliveries:
                counts[delivery["status"]] = counts.get(delivery["status"], 0) + 1
            priority = 0
            if status["status"] in {"failed", "timeout", "orphaned"}:
                priority += 100
            if counts.get("failed") or counts.get("pending") or counts.get("retrying"):
                priority += 50
            last_event = status.get("last_event") or {}
            if last_event.get("type") in ATTENTION_EVENTS:
                priority += 25
            jobs.append({**status, "delivery_counts": counts, "priority": priority})
        jobs.sort(key=lambda item: (item["priority"], item["updated_at"]), reverse=True)
        return {"jobs": jobs}

    def reap_orphans(self) -> dict[str, Any]:
        """Terminate MCP stdio server processes whose launching client is gone."""
        orphans = _orphaned_mcp_servers()
        reaped = []
        failed = []
        import signal as _signal

        for entry in orphans:
            pid = entry["pid"]
            try:
                if sys.platform == "win32":
                    subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
                else:
                    os.kill(pid, _signal.SIGKILL)
                reaped.append(pid)
            except Exception as exc:
                failed.append({"pid": pid, "error": str(exc)})
        return {"reaped": reaped, "failed": failed, "orphan_count": len(orphans)}

    def metrics_text(self) -> str:
        """Prometheus text exposition of daemon state (review B2, no deps)."""
        self._ensure_open()
        with self.db_lock:
            status_counts = {
                row["status"]: int(row["c"])
                for row in self.db.execute("SELECT status, COUNT(*) AS c FROM jobs GROUP BY status").fetchall()
            }
            delivery_counts = {
                row["status"]: int(row["c"])
                for row in self.db.execute("SELECT status, COUNT(*) AS c FROM deliveries GROUP BY status").fetchall()
            }
            pool_rows = self.db.execute(
                "SELECT pool, max_parallel, paused FROM pools ORDER BY pool"
            ).fetchall()
            pool_counts = {
                (row["pool"], row["status"]): int(row["c"])
                for row in self.db.execute(
                    "SELECT pool, status, COUNT(*) AS c FROM jobs WHERE pool IS NOT NULL GROUP BY pool, status"
                ).fetchall()
            }
            events_total = int(self.db.execute("SELECT COUNT(*) FROM events").fetchone()[0])
            schema = int(self.db.execute("PRAGMA user_version").fetchone()[0])
            stale_leases = int(
                self.db.execute(
                    "SELECT COUNT(*) FROM deliveries WHERE status='dispatching' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?",
                    (now_iso(),),
                ).fetchone()[0]
            )
        db_path = self.home / "jobs.sqlite"
        try:
            db_size = db_path.stat().st_size
        except OSError:
            db_size = 0
        disk_free = shutil.disk_usage(self.home).free
        running = status_counts.get("running", 0) + status_counts.get("launching", 0)

        def escape(value: object) -> str:
            return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")

        def emit(name: str, value: float, **labels: str) -> str:
            if labels:
                rendered = ",".join(f'{key}="{escape(val)}"' for key, val in sorted(labels.items()))
                return f"{name}{{{rendered}}} {value}"
            return f"{name} {value}"

        lines = [
            "# HELP vanth_up Daemon process is up.",
            "# TYPE vanth_up gauge",
            emit("vanth_up", 1),
            "# TYPE vanth_uptime_seconds gauge",
            emit("vanth_uptime_seconds", round(time.monotonic() - self._started_monotonic, 3)),
            "# TYPE vanth_schema_version gauge",
            emit("vanth_schema_version", schema),
            "# TYPE vanth_jobs gauge",
            emit("vanth_jobs", running, status="running_or_launching"),
            emit("vanth_jobs", status_counts.get("queued", 0), status="queued"),
            emit("vanth_jobs_count", sum(status_counts.values())),
            emit("vanth_events_count", events_total),
            emit("vanth_maintenance_alive", 1 if (self.dispatcher_thread and self.dispatcher_thread.is_alive()) else 0),
            emit("vanth_disk_free_bytes", disk_free),
            emit("vanth_db_size_bytes", db_size),
            emit("vanth_stale_delivery_leases", stale_leases),
            emit("vanth_dead_letters", self._dead_letter_count()),
            emit("vanth_jobs_without_wake", self._recent_jobs_without_wake()),
        ]
        for status, count in sorted(delivery_counts.items()):
            lines.append(emit("vanth_deliveries", count, status=status))
        for row in pool_rows:
            name = row["pool"]
            lines.append(emit("vanth_pool_max_parallel", int(row["max_parallel"]), pool=name))
            lines.append(emit("vanth_pool_paused", 1 if row["paused"] else 0, pool=name))
            lines.append(
                emit(
                    "vanth_pool_running",
                    pool_counts.get((name, "running"), 0) + pool_counts.get((name, "launching"), 0),
                    pool=name,
                )
            )
            lines.append(emit("vanth_pool_queued", pool_counts.get((name, "queued"), 0), pool=name))
        return "\n".join(lines) + "\n"

    def _artifact_integrity_report(self, verify: bool) -> dict[str, Any]:
        report: dict[str, Any] = {"requested": verify, "checked": 0, "complete": False, "issues": []}
        catalog_path = self.home / "artifacts.sqlite"
        if not verify:
            return report
        if not catalog_path.exists():
            return {**report, "complete": True}
        from .artifacts.lifecycle import Lifecycle
        from .artifacts.manifest import validate_manifest

        connection = None
        try:
            deadline = time.monotonic() + 3
            connection = sqlite3.connect(catalog_path.resolve().as_uri() + "?mode=ro", uri=True, timeout=1)
            manifests = connection.execute(
                "SELECT substr(CAST(manifest_json AS BLOB),1,1048577) FROM versions WHERE deleted_at IS NULL ORDER BY created_at DESC LIMIT 101"
            ).fetchall()
            shas = set()
            complete = len(manifests) <= 100
            manifest_budget = 16 * 1024 * 1024
            for row in manifests[:100]:
                if len(row[0]) > min(1048576, manifest_budget) or time.monotonic() >= deadline:
                    complete = False
                    continue
                manifest_budget -= len(row[0])
                validate_manifest(json.loads(row[0]))
                shas.update(Lifecycle._manifest_shas(row[0]))
                if len(shas) > 1000:
                    shas = set(sorted(shas)[:1000])
                    complete = False
                    break
            budget = 64 * 1024 * 1024
            for sha in sorted(shas):
                if not re.fullmatch(r"[0-9a-f]{64}", sha):
                    report["issues"].append({"type": "invalid_artifact_hash", "sha256": sha})
                    continue
                path = self.home / "artifacts-store" / "blobs" / sha[:2] / sha[2:4] / sha
                try:
                    size = path.stat().st_size
                    if size > budget or time.monotonic() >= deadline:
                        complete = False
                        continue
                    digest = hashlib.sha256()
                    with path.open("rb") as handle:
                        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                            digest.update(chunk)
                            if time.monotonic() >= deadline:
                                complete = False
                                break
                        else:
                            report["checked"] += 1
                            budget -= size
                            if digest.hexdigest() != sha:
                                report["issues"].append({"type": "corrupt_artifact_blob", "sha256": sha})
                except FileNotFoundError:
                    report["issues"].append({"type": "missing_artifact_blob", "sha256": sha})
                except OSError as exc:
                    report["issues"].append({"type": "artifact_read_failed", "sha256": sha, "error": str(exc)})
            report["complete"] = complete
            report["limits"] = {"versions": 100, "blobs": 1000, "manifest_bytes": 16 * 1024 * 1024,
                                "bytes": 64 * 1024 * 1024, "seconds": 3}
        except (sqlite3.Error, ValueError, TypeError, KeyError, AttributeError) as exc:
            report["issues"].append({"type": "artifact_catalog_check_failed", "error": str(exc)})
        finally:
            if connection is not None:
                connection.close()
        return report

    def doctor(self, verify_artifacts: bool = False) -> dict[str, Any]:
        self._ensure_open()
        tables = {
            row["name"]
            for row in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        required = {"jobs", "events", "wake_targets", "deliveries", "delivery_attempts", "cleanup_tombstones"}
        delivery_counts = {
            row["status"]: row["count"]
            for row in self.db.execute("SELECT status, COUNT(*) AS count FROM deliveries GROUP BY status").fetchall()
        }
        stale_delivery_ttl = self._delivery_ttl_seconds()
        stale_cutoff = (datetime.now(timezone.utc) - timedelta(seconds=stale_delivery_ttl)).isoformat().replace("+00:00", "Z")
        stale_pending_deliveries = self.db.execute(
            "SELECT COUNT(*) FROM deliveries WHERE status IN ('pending','retrying') AND attempts=0 AND created_at < ?",
            (stale_cutoff,),
        ).fetchone()[0]
        recent_jobs_without_wake = self._recent_jobs_without_wake()
        relay_client_ids = self._relay_client_ids()
        undeliverable_wakes = 0
        for row in self.db.execute("SELECT type, config_json FROM wake_targets").fetchall():
            try:
                config = json.loads(row["config_json"] or "{}")
            except (TypeError, ValueError):
                continue
            if not isinstance(config, dict):
                continue
            identity = wake_target_identity({**config, "type": row["type"]})
            if identity and is_relay_client_id(row["type"], identity, relay_client_ids):
                undeliverable_wakes += 1
        warnings = []
        integrity = self._artifact_integrity_report(verify_artifacts)
        if integrity["issues"]:
            warnings.append({"type": "artifact_integrity", "issues": integrity["issues"],
                             "detail": "restore missing blobs or republish corrupt artifacts"})
        diagnostics = self.db.execute(
            "SELECT job_id, type, message, created_at FROM events "
            "WHERE type IN ('pipe_drain_timeout','log_capture_failed','write_contended') ORDER BY created_at DESC LIMIT 20"
        ).fetchall()
        if diagnostics:
            warnings.append({"type": "capture_diagnostics", "count": len(diagnostics),
                             "detail": "inspect capture failures and inherited output pipes in the reported jobs"})
        contention_count = 0
        try:
            with (self.logs / "daemon.log").open("rb") as handle:
                handle.seek(0, os.SEEK_END)
                handle.seek(max(0, handle.tell() - 8192))
                contention_count = handle.read(8192).count(b"event write contended")
        except OSError:
            pass
        if contention_count:
            warnings.append({"type": "ingestion_contention", "count": contention_count,
                             "detail": "recent event writes contended; reduce parallel event rate or inspect ingestion latency"})
        if undeliverable_wakes:
            warnings.append(
                {
                    "type": "undeliverable_wake_targets",
                    "count": undeliverable_wakes,
                    "detail": "these wakes can never fire; inspect with `vanth deliveries --status pending`",
                }
            )
        if stale_pending_deliveries:
            warnings.append(
                {
                    "type": "stale_pending_deliveries",
                    "count": stale_pending_deliveries,
                    "detail": (f"wakes older than {stale_delivery_ttl}s that no relay completed "
                               "(expired automatically by the dispatch loop)"),
                }
            )
        missing = sorted(required - tables)
        if missing:
            warnings.append({"type": "missing_tables", "tables": missing})
        codex_bin = os.environ.get("VANTH_CODEX_BIN") or (r"C:\codex\codex.exe" if sys.platform == "win32" else "codex")
        codex_available = bool(Path(codex_bin).exists() if ("\\" in codex_bin or "/" in codex_bin) else shutil.which(codex_bin))
        if not codex_available:
            warnings.append({"type": "codex_unavailable", "command": codex_bin})
        opencode_bin = os.environ.get("VANTH_OPENCODE_BIN", "opencode")
        opencode_available = bool(shutil.which(opencode_bin) or Path(opencode_bin).exists())
        if not opencode_available:
            warnings.append({"type": "opencode_unavailable", "command": opencode_bin})
        orphaned_mcp = _orphaned_mcp_servers()
        if orphaned_mcp:
            warnings.append(
                {
                    "type": "orphaned_mcp_servers",
                    "count": len(orphaned_mcp),
                    "pids": [entry["pid"] for entry in orphaned_mcp],
                    "detail": "MCP stdio servers whose launching client is gone; "
                    "reap with `vanth doctor --reap-orphans`",
                }
            )
        quick_check = self.db.execute("PRAGMA quick_check").fetchone()[0]
        stale_leases = self.db.execute(
            "SELECT COUNT(*) FROM deliveries WHERE status='dispatching' AND lease_expires_at IS NOT NULL AND lease_expires_at<=?",
            (now_iso(),),
        ).fetchone()[0]
        dead_lettered = []
        for row in self.db.execute(
            "SELECT delivery_id, job_id, attempts, last_error, payload_json FROM deliveries WHERE status='failed' ORDER BY created_at DESC LIMIT 20"
        ).fetchall():
            target = json.loads(row["payload_json"] or "{}").get("target", {})
            max_attempts = int(target.get("max_attempts", 1))
            expired = (row["last_error"] or "").startswith(EXPIRED_DELIVERY_ERROR)
            if int(row["attempts"]) < max_attempts and not expired:
                continue
            dead_lettered.append(
                {
                    "delivery_id": row["delivery_id"],
                    "job_id": row["job_id"],
                    "attempts": int(row["attempts"]),
                    "last_error": row["last_error"],
                }
            )
        disk = shutil.disk_usage(self.home)
        running_jobs = self._running_count()
        # Optional agent adapters being absent is informational, not a health
        # problem: the daemon (and its jobs) are fully functional without them.
        # Orphaned MCP servers are likewise a host-hygiene advisory (reap them
        # explicitly with `vanth doctor --reap-orphans`), and depend on the
        # ambient process table, so they must not flip the health exit code.
        soft_warning_types = {"codex_unavailable", "opencode_unavailable", "orphaned_mcp_servers",
                              "capture_diagnostics", "ingestion_contention"}
        hard_warnings = [w for w in warnings if w.get("type") not in soft_warning_types]
        # A dead maintenance/dispatch loop is a HARD failure: queues stop
        # draining and wake deliveries stop progressing, yet `/ready` (which
        # trusts this flag) would keep reporting healthy and hide the outage.
        maintenance_alive = bool(self.dispatcher_thread and self.dispatcher_thread.is_alive())
        return {
            "ok": not hard_warnings and quick_check == "ok" and maintenance_alive,
            "ok_warnings": [w.get("type") for w in warnings],
            "home": str(self.home),
            "db_path": str(self.home / "jobs.sqlite"),
            "logs_dir": str(self.logs),
            "events_dir": str(self.events_dir),
            "tables": sorted(tables),
            "delivery_counts": delivery_counts,
            "pending_deliveries": delivery_counts.get("pending", 0),
            "stale_pending_deliveries": stale_pending_deliveries,
            "recent_jobs_without_wake": recent_jobs_without_wake,
            "undeliverable_wakes": undeliverable_wakes,
            "codex": {"command": codex_bin, "available": codex_available},
            "opencode": {"command": opencode_bin, "available": opencode_available},
            "schema_version": int(self.db.execute("PRAGMA user_version").fetchone()[0]),
            "quick_check": quick_check,
            "maintenance_alive": maintenance_alive,
            "relays": self.relay_status(),
            "stale_delivery_leases": stale_leases,
            # Unbounded count; ``dead_lettered`` below is only the most recent 20.
            "dead_letter_count": self._dead_letter_count(),
            "dead_lettered": dead_lettered,
            "running_jobs": running_jobs,
            "max_running_jobs": self.max_running_jobs,
            "retention": {
                "seconds": self.max_retention_seconds,
                "interval_seconds": self.retention_interval_seconds,
                "dry_run": self.retention_dry_run,
            },
            "disk_free_bytes": disk.free,
            "token_path": str(self.home / "token"),
            "warnings": warnings,
            "artifact_integrity": integrity,
            "capture_diagnostics": [dict(row) for row in diagnostics],
            "ingestion": {"recent_contention_log_lines": contention_count},
            "orphaned_mcp_servers": orphaned_mcp,
        }

    def relay_status(self) -> list[dict[str, Any]]:
        """Registered client relays and their liveness (wake-reachability view).

        ``codex_desktop``/``opencode_thread`` wakes are delivered by whichever
        client relay long-polls for the destination. With no relay registered (or
        a stale one) those deliveries stay pending forever, so doctor reports the
        subscription itself rather than only counting deliveries.
        """
        stale_after = float(os.environ.get("VANTH_RELAY_STALE_SECONDS", "90"))
        now = datetime.now(timezone.utc)
        statuses = []
        for row in self.db.execute(
            "SELECT client_id, client_type, destinations_json, updated_at, last_poll_at "
            "FROM relay_subscriptions ORDER BY client_type, client_id"
        ).fetchall():
            last_poll = _parse_iso(row["last_poll_at"]) if row["last_poll_at"] else None
            age = (now - last_poll).total_seconds() if last_poll else None
            try:
                destinations = json.loads(row["destinations_json"] or "[]")
            except (TypeError, ValueError):
                destinations = []
            statuses.append(
                {
                    "client_id": row["client_id"],
                    "client_type": row["client_type"],
                    "destinations": destinations if isinstance(destinations, list) else [],
                    "last_poll_age_seconds": age,
                    "live": age is not None and age <= stale_after,
                }
            )
        return statuses

    def _cleanup_rows(self, older_than_seconds: int) -> list[sqlite3.Row]:
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=older_than_seconds)).isoformat().replace("+00:00", "Z")
        with self.db_lock:
            return self.db.execute(
                "SELECT * FROM jobs WHERE status IN ('completed','failed','timeout','cancelled','orphaned') AND updated_at<=?",
                (cutoff,),
            ).fetchall()

    def cleanup(self, older_than_seconds: int, dry_run: bool = True) -> dict[str, Any]:
        self._ensure_open()
        if isinstance(older_than_seconds, bool) or not isinstance(older_than_seconds, int) or older_than_seconds < 0:
            raise ValueError("older_than_seconds must be a non-negative integer")
        rows = self._cleanup_rows(older_than_seconds)
        job_ids = [row["job_id"] for row in rows]
        deleted = list(job_ids)
        if not dry_run and job_ids:
            with self.db_lock:
                placeholders = ",".join("?" for _ in job_ids)
                self.db.execute("BEGIN IMMEDIATE")
                # Re-check terminal status INSIDE the transaction: a restart
                # recovery can claim a terminal row back to 'launching' between
                # the selection above and these DELETEs. Deleting the row — or its
                # wake targets / events / logs — out from under a live launch would
                # silently lose the job (and the launch would then find no target).
                still = [
                    r["job_id"]
                    for r in self.db.execute(
                        f"SELECT job_id FROM jobs WHERE job_id IN ({placeholders}) "
                        "AND status IN ('completed','failed','timeout','cancelled','orphaned')",
                        job_ids,
                    )
                ]
                deleted = still
                still_set = set(still)
                still_ph = ",".join("?" for _ in still)
                for row in rows:
                    job_id = row["job_id"]
                    if job_id not in still_set:
                        continue
                    artifacts = [
                        row["stdout_path"],
                        row["stderr_path"],
                        row["events_path"],
                        str(self.logs / f"{job_id}.runner.log"),
                        str(self.home / "specs" / f"{job_id}.json"),
                        str(self.home / "stdin" / f"{job_id}.in"),
                    ]
                    # Claim-specific specs (specs/{job_id}-{claim_token}.json)
                    # also belong to this job and must be cleaned up (review
                    # rc33 P1-3 introduced the claim-specific spec naming).
                    if (self.home / "specs").exists():
                        for spec_file in (self.home / "specs").glob(f"{job_id}-*.json"):
                            artifacts.append(str(spec_file))
                    self.db.execute(
                        "INSERT OR IGNORE INTO cleanup_tombstones(tombstone_id, job_id, artifacts_json, created_at) VALUES (?, ?, ?, ?)",
                        ("clean_" + uuid.uuid4().hex[:16], job_id, json.dumps(artifacts, separators=(",", ":")), now_iso()),
                    )
                self.db.execute(f"DELETE FROM delivery_attempts WHERE delivery_id IN (SELECT delivery_id FROM deliveries WHERE job_id IN ({still_ph}))", still)
                self.db.execute(f"DELETE FROM deliveries WHERE job_id IN ({still_ph})", still)
                self.db.execute(f"DELETE FROM wake_targets WHERE job_id IN ({still_ph})", still)
                self.db.execute(f"DELETE FROM decisions WHERE job_id IN ({still_ph})", still)
                self.db.execute(f"DELETE FROM events WHERE job_id IN ({still_ph})", still)
                self.db.execute(f"DELETE FROM jobs WHERE job_id IN ({still_ph})", still)
                self.db.commit()
        if not dry_run:
            self._prune_remote_wake_rows(older_than_seconds)
            self._drain_cleanup_tombstones()
        return {"dry_run": dry_run, "older_than_seconds": older_than_seconds, "jobs": deleted, "count": len(deleted)}

    def _prune_remote_wake_rows(self, older_than_seconds: int) -> int:
        """Prune SETTLED remote-wake rows, which have no ``jobs`` row to sweep.

        A remote binding's terminal event and delivery carry a
        ``remote:<remote_id>:<job_id>`` key with no local ``jobs`` row, so the
        per-job DELETEs above never reach them and they would accumulate forever.
        A ``pending``/``dispatching`` delivery is a wake still owed and is never
        pruned; only delivered/failed rows at or below the cutoff are.
        """
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=older_than_seconds)).isoformat().replace("+00:00", "Z")
        with self.db_lock:
            try:
                attempts = self.db.execute(
                    "DELETE FROM delivery_attempts WHERE delivery_id IN "
                    "(SELECT delivery_id FROM deliveries WHERE job_id LIKE 'remote:%' "
                    "AND created_at<=? AND status IN ('delivered','failed'))",
                    (cutoff,),
                ).rowcount
                deliveries = self.db.execute(
                    "DELETE FROM deliveries WHERE job_id LIKE 'remote:%' "
                    "AND created_at<=? AND status IN ('delivered','failed')",
                    (cutoff,),
                ).rowcount
                events = self.db.execute(
                    "DELETE FROM events WHERE job_id LIKE 'remote:%' AND created_at<=?",
                    (cutoff,),
                ).rowcount
                metrics = self.db.execute(
                    "DELETE FROM metric_series WHERE job_id LIKE 'remote:%' AND created_at<=?",
                    (cutoff,),
                ).rowcount
                cursors = self.db.execute(
                    "DELETE FROM remote_event_cursors WHERE NOT EXISTS "
                    "(SELECT 1 FROM wake_targets WHERE wake_targets.job_id=remote_event_cursors.binding_id "
                    "AND wake_targets.remote_id IS NOT NULL)"
                ).rowcount
                self.db.commit()
            except Exception:
                self.db.rollback()
                raise
        return attempts + deliveries + events + metrics + cursors

    def cleanup_preview(self, older_than_seconds: int) -> dict[str, Any]:
        """Dry-run preview of the jobs a cleanup would remove, without deleting."""
        self._ensure_open()
        if isinstance(older_than_seconds, bool) or not isinstance(older_than_seconds, int) or older_than_seconds < 0:
            raise ValueError("older_than_seconds must be a non-negative integer")
        jobs = [
            {
                "job_id": row["job_id"],
                "name": row["name"],
                "status": row["status"],
                "updated_at": row["updated_at"],
                "stdout_path": row["stdout_path"],
                "stderr_path": row["stderr_path"],
                "events_path": row["events_path"],
            }
            for row in self._cleanup_rows(older_than_seconds)
        ]
        return {"dry_run": True, "older_than_seconds": older_than_seconds, "jobs": jobs, "count": len(jobs)}

    def _drain_cleanup_tombstones(self) -> None:
        with self.db_lock:
            tombstones = self.db.execute("SELECT tombstone_id, artifacts_json FROM cleanup_tombstones").fetchall()
        for tombstone in tombstones:
            failed = False
            for raw_path in json.loads(tombstone["artifacts_json"] or "[]"):
                for attempt in range(3):
                    try:
                        Path(raw_path).unlink()
                        break
                    except FileNotFoundError:
                        break
                    except OSError:
                        if attempt == 2:
                            failed = True
                        else:
                            time.sleep(0.05)
            if not failed:
                with self.db_lock:
                    self.db.execute("DELETE FROM cleanup_tombstones WHERE tombstone_id=?", (tombstone["tombstone_id"],))
                    self.db.commit()

    async def stop(
        self,
        job_id: str,
        signal: str = "terminate",
        kill_after_seconds: int = 10,
        actor: str = DEFAULT_STOP_ACTOR,
        reason: str | None = None,
    ) -> dict[str, Any]:
        return await asyncio.get_running_loop().run_in_executor(
            None, self.stop_sync, job_id, signal, kill_after_seconds, actor, reason
        )

    def stop_sync(
        self,
        job_id: str,
        signal: str = "terminate",
        kill_after_seconds: int = 10,
        actor: str = DEFAULT_STOP_ACTOR,
        reason: str | None = None,
    ) -> dict[str, Any]:
        return self._stop(job_id, signal, kill_after_seconds, actor, reason)

    def _stop(
        self,
        job_id: str,
        signal: str = "terminate",
        kill_after_seconds: int = 10,
        actor: str = DEFAULT_STOP_ACTOR,
        reason: str | None = None,
    ) -> dict[str, Any]:
        if isinstance(kill_after_seconds, bool) or not isinstance(kill_after_seconds, int) or kill_after_seconds < 0 or kill_after_seconds > 86400:
            raise ValueError("kill_after_seconds must be between 0 and 86400")
        if actor not in STOP_ACTORS:
            raise ValueError(f"actor must be one of {sorted(STOP_ACTORS)}")
        if reason is not None:
            if not isinstance(reason, str) or not reason.strip():
                raise ValueError("reason must be a non-empty string when provided")
            reason = reason.strip()
        else:
            reason = "stop requested"
        proc = self.processes.get(job_id)
        # First read captures the FULL ownership identity (claim token) — the
        # stop-request update AND every terminal transition below CAS against
        # THIS identity, so a replacement launch that took over the row between
        # this read and the write is never touched (review rc39 P1).
        row = self._row(
            "SELECT status, worker_pid, pid, stop_requested_at, claim_token, trigger_json FROM jobs WHERE job_id=?", (job_id,)
        )
        if not row:
            raise ValueError(f"Job is not running in this server: {job_id}")
        if row and row["status"] == "queued":
            if self._cancel_queued(
                job_id, actor=actor, reason=reason, message="Queued job cancelled before its trigger fired",
                data={"actor": actor, "reason": reason},
            ):
                return {"job_id": job_id, "status": self.status(job_id)["status"], "message": "Queued job cancelled"}
            # The dispatcher claimed the row to 'launching' between our read and
            # this CAS. Re-read and fall through to the live-stop path below so we
            # stop the launch instead of reporting a cancellation that did not
            # happen (a zero-row CAS must never be reported as success).
            row = self._row(
                "SELECT status, worker_pid, pid, stop_requested_at, claim_token, trigger_json FROM jobs WHERE job_id=?",
                (job_id,),
            )
            if not row or row["status"] not in {"running", "launching"}:
                return {
                    "job_id": job_id,
                    "status": row["status"] if row else "unknown",
                    "message": "Job left the queue; stop is a no-op",
                }
        if row and row["status"] not in {"running", "launching"}:
            # Already-terminal stop is an idempotent no-op (review rc14 P1-11):
            # repeated/cleanup stops must not raise when the job finished
            # between a caller's observation and its stop request.
            return {
                "job_id": job_id,
                "status": row["status"],
                "message": f"Job already {row['status']}; stop is a no-op",
            }
        deadline = time.monotonic() + kill_after_seconds
        stop_token = now_iso()
        observed_claim_token = row["claim_token"]
        observed_worker_pid = int(row["worker_pid"]) if row["worker_pid"] else None
        observed_workload_pid = int(row["pid"]) if row["pid"] else None
        # A 'launching' row is stoppable too: the runner's launching->running
        # promotion is guarded by `stop_requested_at IS NULL`, so setting the
        # stop flag here prevents the launch from ever coming up (review rc36 P1
        # — every start is now a claim-token 'launching' row, so stop must not
        # reject the brief pre-promotion window).
        #
        # The stop-request update is OWNERSHIP-CAS'd on the observed claim token:
        # a finish+restart/recovery that installed claim B between the first read
        # and this write means the UPDATE affects zero rows and we return WITHOUT
        # setting the stop flag on the replacement launch (review rc39 P1).
        with self.db_lock:
            if observed_claim_token:
                requested = self.db.execute(
                    "UPDATE jobs SET stop_requested_at=?, stop_actor=?, stop_reason=? "
                    "WHERE job_id=? AND status IN ('running','launching') AND claim_token=?",
                    (stop_token, actor, reason, job_id, observed_claim_token),
                ).rowcount
            else:
                # No-token path (legacy/no-claim row): guard on the observed
                # worker identity so a replacement worker is never stopped.
                if observed_worker_pid is not None:
                    requested = self.db.execute(
                        "UPDATE jobs SET stop_requested_at=?, stop_actor=?, stop_reason=? "
                        "WHERE job_id=? AND status IN ('running','launching') AND worker_pid=?",
                        (stop_token, actor, reason, job_id, observed_worker_pid),
                    ).rowcount
                elif observed_workload_pid is not None:
                    requested = self.db.execute(
                        "UPDATE jobs SET stop_requested_at=?, stop_actor=?, stop_reason=? "
                        "WHERE job_id=? AND status IN ('running','launching') AND pid=?",
                        (stop_token, actor, reason, job_id, observed_workload_pid),
                    ).rowcount
                else:
                    requested = self.db.execute(
                        "UPDATE jobs SET stop_requested_at=?, stop_actor=?, stop_reason=? "
                        "WHERE job_id=? AND status IN ('running','launching') AND stop_requested_at IS NULL",
                        (stop_token, actor, reason, job_id),
                    ).rowcount
            self.db.commit()
        if not requested:
            return {"job_id": job_id, "status": self.status(job_id)["status"], "message": "Job is owned by a newer launch; stop is a no-op"}
        row = self._row("SELECT status, worker_pid, pid, stop_requested_at, claim_token FROM jobs WHERE job_id=?", (job_id,))
        if not row or row["status"] not in {"running", "launching"}:
            return {"job_id": job_id, "status": self.status(job_id)["status"], "message": "Job was already terminal"}
        # Ownership re-verification (self-review rc40): the stop-request CAS
        # succeeded on the observed identity, but a replacement launch may have
        # taken over the row between that CAS and this re-read. Every process
        # termination and transition below must target the OBSERVED owner only —
        # never kill B's workload/runner just because B now owns the row. Our
        # flag value is also cleared so B's runner is not poisoned by a stop
        # flag it never asked for (the clear only applies while the flag is
        # still exactly our token, so a newer stop's flag is never removed).
        def _ownership_changed() -> bool:
            if observed_claim_token:
                return row["claim_token"] != observed_claim_token
            if observed_worker_pid is not None:
                return row["worker_pid"] != observed_worker_pid
            if observed_workload_pid is not None:
                return row["pid"] != observed_workload_pid
            # The original legacy row exposed no ownership identity. If one
            # appears after our stop flag CAS, it may belong to a replacement;
            # fail safe rather than terminating an unverified process.
            return any(row[key] is not None for key in ("claim_token", "worker_pid", "pid"))

        if _ownership_changed():
            with self.db_lock:
                self.db.execute(
                    "UPDATE jobs SET stop_requested_at=NULL WHERE job_id=? AND stop_requested_at=?",
                    (job_id, stop_token),
                )
                self.db.commit()
            return {"job_id": job_id, "status": row["status"], "message": "Job is owned by a newer launch; stop is a no-op"}
        workload_pid = int(row["pid"]) if row["pid"] else None
        if workload_pid and not self._terminate_pid(workload_pid, signal == "kill", deadline):
            with self.db_lock:
                self.db.execute(
                    "UPDATE jobs SET stop_requested_at=NULL WHERE job_id=? AND status IN ('running','launching') AND stop_requested_at=?",
                    (job_id, stop_token),
                )
                self.db.commit()
            raise RuntimeError(f"Failed to stop workload process tree: {workload_pid}")
        # If the row is still 'launching' (the runner has not promoted yet), the
        # runner will observe stop_requested_at on publish and self-terminate;
        # record the cancelled transition now so the caller's stop returns
        # cancelled deterministically. The transition is claim-token guarded so a
        # newer launch that already owns the row is never cancelled by a stale
        # stop.
        if row["status"] == "launching":
            changed = self._terminal_event(
                job_id, "cancelled", claim_token=observed_claim_token, require_launching=True,
                expected_worker_pid=observed_worker_pid,
                message="Job cancelled while its runner was still launching",
                data={"actor": actor, "reason": reason},
            )
            if not changed:
                # The runner promoted between our snapshot and this write; fall
                # through to the normal running-stop path. PRESERVE the ORIGINAL
                # observed ownership identity: the final transition below is
                # claim-token guarded with the SNAPSHOT's token, so a
                # recovery/restart that installed a NEW claim (claim B) between
                # the snapshot and this write is never cancelled by this stale
                # stop — the CAS on claim A returns 0 and we return without
                # mutating the new launch (review rc38 P1 / rc39 P1).
                row = self._row(
                    "SELECT status, worker_pid, pid, stop_requested_at, claim_token FROM jobs WHERE job_id=?", (job_id,)
                )
            else:
                # Terminate the still-starting runner process so it does not keep
                # running against a terminal row.
                runner_pid = int(row["worker_pid"]) if row["worker_pid"] else None
                if runner_pid:
                    self._terminate_pid(runner_pid, force=True, deadline=deadline)
                if proc and proc.pid != runner_pid:
                    self._terminate_pid(proc.pid, force=True, deadline=deadline)
                self._readers_done(job_id)
                self.processes.pop(job_id, None)
                return {"job_id": job_id, "status": "cancelled", "message": "Job stopped"}
        changed = self._terminal_event(
            job_id, "cancelled", claim_token=observed_claim_token,
            worker_pid=observed_worker_pid if not observed_claim_token else _UNSET,
            data={"actor": actor, "reason": reason},
        )
        if not changed:
            return {"job_id": job_id, "status": self.status(job_id)["status"], "message": "Job was already terminal or owned by a newer launch"}
        failures = []
        runner_pid = int(row["worker_pid"]) if row["worker_pid"] else None
        if runner_pid and not self._terminate_pid(runner_pid, signal == "kill", deadline):
            failures.append(runner_pid)
        if proc and proc.pid != runner_pid:
            if not self._terminate_pid(proc.pid, signal == "kill", deadline):
                failures.append(proc.pid)
            try:
                proc.wait(timeout=max(0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                failures.append(proc.pid)
        self._readers_done(job_id)
        self.processes.pop(job_id, None)
        if failures:
            # The job is already terminal (cancelled); a lingering tree is a
            # host problem, not a failed stop. Retry the stragglers once more with
            # a fresh grace window, log what survived, then report.
            still = [
                pid for pid in failures
                if self._pid_alive(pid) and not self._terminate_pid(pid, True, time.monotonic() + 2.0)
            ]
            if still:
                self.logger.warning("job %s cancelled but process tree did not exit: %s", job_id, still)
                raise RuntimeError(f"Failed to stop process tree(s): {still}")
        return {"job_id": job_id, "status": "cancelled", "message": "Job stopped"}

    def add_wake_target(self, job_id: str, target: dict[str, Any]) -> dict[str, Any]:
        """Schedule a self-resume wake target against a job after the fact.

        Registers a target for FUTURE events. Use :meth:`wake_now` to surface a
        wake immediately (even if the triggering event already fired).
        """
        self._ensure_open()
        if not isinstance(target, dict):
            raise ValueError("target must be an object")
        # Apply the events default before validation (events must be non-empty).
        if "events" not in target and "notify_on" not in target:
            target = {**target, "events": ["completed", "failed"]}
        target = dict(target)
        # Resolve an unaddressed opencode_thread target to the live plugin relay
        # in the job's project before validation demands a session id.
        job_row = self._row("SELECT cwd FROM jobs WHERE job_id=?", (job_id,))
        if job_row:
            self._resolve_relay_sessions([target], job_row["cwd"])
            self._reject_relay_client_id_targets([target])
        validate_wake_targets([target])
        target_type = target.get("type")
        events = target.get("events")
        if events is None:
            events = target.get("notify_on") or ["completed", "failed"]
        if not events:
            raise ValueError("target events must not be empty")
        if not self._row("SELECT job_id FROM jobs WHERE job_id=?", (job_id,)):
            raise ValueError(f"Unknown job_id: {job_id}")
        config = {key: value for key, value in target.items() if key not in {"type", "events", "notify_on"}}
        with self.db_lock:
            inserted = self._insert_wake_targets(job_id, [{"type": target_type, "events": events, **config}], now_iso())
            self.db.commit()
        return {"result": "ok", "job_id": job_id, "target_id": inserted[0], "target_type": target_type, "events": events}

    def wake_now(self, job_id: str, target: dict[str, Any], origin_thread_id: str | None = None) -> dict[str, Any]:
        """Surface a wake NOW, regardless of whether the triggering event fired.

        ``daemon_wake`` historically only registered a target for a FUTURE event
        (review P0-1); calling it after a job completed did nothing. This is the
        genuine "wake immediately" operation: it registers the target and
        enqueues a synthetic delivery right away so the wake reaches the target
        session without waiting for a matching event.

        ``origin_thread_id`` is inherited the same way ``start`` inherits it
        (``codex_cli_thread``/``codex_thread`` get the calling thread id);
        ``opencode_thread`` requires an explicit ``session_id`` (review P1-1).
        """
        self._ensure_open()
        if not isinstance(target, dict):
            raise ValueError("target must be an object")
        targets = resolve_wake_target_identity([target], origin_thread_id)
        resolved = targets[0]
        if "events" not in resolved and "notify_on" not in resolved:
            resolved = {**resolved, "events": ["completed", "failed"]}
        # Resolve an unaddressed opencode_thread target to the live plugin relay
        # in the job's project before validation demands a session id. Resolving
        # only when the job exists preserves the existing error precedence for an
        # unknown job_id.
        job_row = self._row("SELECT cwd FROM jobs WHERE job_id=?", (job_id,))
        if job_row:
            self._resolve_relay_sessions([resolved], job_row["cwd"])
            self._reject_relay_client_id_targets([resolved])
        validate_wake_targets([resolved])
        target_type = resolved.get("type")
        events = resolved.get("events")
        if events is None:
            events = resolved.get("notify_on") or ["completed", "failed"]
        if not events:
            raise ValueError("target events must not be empty")
        if resolved.get("auto_dispatch") is False:
            # Review P0-1/P1: wake_now must actually dispatch. An
            # auto_dispatch:false target would leave a permanently-pending
            # delivery while wake_now reported woken:true. Reject it.
            raise ValueError("wake_now does not support auto_dispatch:false (the wake must be delivered immediately)")
        row = self._row("SELECT job_id, status FROM jobs WHERE job_id=?", (job_id,))
        if not row:
            raise ValueError(f"Unknown job_id: {job_id}")
        config = {key: value for key, value in resolved.items() if key not in {"type", "events", "notify_on"}}
        with self.db_lock:
            inserted = self._insert_wake_targets(job_id, [{"type": target_type, "events": events, **config}], now_iso())
            # Enqueue an immediate delivery so the wake fires NOW. Use a DISTINCT
            # synthetic event type (never fabricate 'completed'/'failed' for a
            # running or failed job — review P1). The payload carries the actual
            # job status. The target stays registered so future REAL events of
            # these types keep waking it.
            target_row = self.db.execute(
                "SELECT * FROM wake_targets WHERE target_id=?", (inserted[0],)
            ).fetchone()
            synthetic = {
                "event_id": "evt_synthetic_" + uuid.uuid4().hex[:12],
                "job_id": job_id,
                "seq": 0,
                "type": "wake_now",
                "level": "info",
                "message": "wake_now requested by caller",
                "data": {"synthetic": True, "requested_status": row["status"]},
                "source": "server",
                "created_at": now_iso(),
            }
            delivery_id = "del_" + uuid.uuid4().hex[:16]
            payload = self._delivery_payload(synthetic, target_row, delivery_id)
            self.db.execute(
                """
                INSERT OR IGNORE INTO deliveries(
                  delivery_id, event_id, target_id, job_id, target_type, status, payload_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
                """,
                (
                    delivery_id,
                    synthetic["event_id"],
                    target_row["target_id"],
                    job_id,
                    target_type,
                    json.dumps(payload, separators=(",", ":")),
                    now_iso(),
                ),
            )
            self.db.commit()
        return {
            "result": "ok",
            "job_id": job_id,
            "target_id": inserted[0],
            "target_type": target_type,
            "events": events,
            "woken": True,
            "synthetic_event_type": "wake_now",
            "requested_status": row["status"],
        }

    async def send(self, job_id: str, input: str, eof: bool = False) -> dict[str, Any]:
        return await asyncio.get_running_loop().run_in_executor(None, self.send_sync, job_id, input, eof)

    def _lock_stdin(self, job_id: str):
        from .daemon import DaemonLock
        lock = DaemonLock(self.home / "stdin" / f"{job_id}.lock")
        deadline = time.monotonic() + 10
        while not lock.acquire():
            if time.monotonic() >= deadline:
                raise TimeoutError("stdin channel is busy; retry the send")
            time.sleep(0.01)
        return lock

    def send_sync(self, job_id: str, input: str, eof: bool = False) -> dict[str, Any]:
        """Append a stdin record to a running interactive job's channel."""
        self._ensure_open()
        if not isinstance(input, str):
            raise ValueError("input must be a string")
        if not eof and not input:
            raise ValueError("input must be a non-empty string")
        row = self._row(
            "SELECT status, run_json FROM jobs WHERE job_id=?",
            (job_id,),
        )
        if not row:
            raise ValueError(f"Unknown job_id: {job_id}")
        run = json.loads(row["run_json"] or "{}")
        if run.get("interactive") is not True:
            raise ValueError("job is not interactive (started without interactive=True)")
        if row["status"] != "running":
            raise ValueError(f"job is not running: {row['status']}")
        channel_dir = self.home / "stdin"
        channel_dir.mkdir(parents=True, exist_ok=True)
        data = input.encode()
        # DaemonLock uses OS locks, so independent CLI/MCP processes serialize too.
        lock = self._lock_stdin(job_id)
        try:
            current = self._row("SELECT status FROM jobs WHERE job_id=?", (job_id,))
            if current["status"] != "running":
                raise ValueError(f"job is not running: {current['status']}")
            closed = channel_dir / f"{job_id}.closed"
            channel = channel_dir / f"{job_id}.in"
            eof_written = closed.exists()
            if not eof_written and channel.exists():
                # Recover a crash between durable EOF append and marker publication.
                with channel.open("rb") as existing:
                    existing.seek(0, os.SEEK_END)
                    size = existing.tell()
                    if size >= 8:
                        existing.seek(-8, os.SEEK_END)
                        if existing.read(8) == b"\0" * 8:
                            existing.seek(0)
                            while existing.tell() + 8 <= size:
                                length = struct.unpack("<Q", existing.read(8))[0]
                                if length == 0:
                                    eof_written = True
                                    break
                                if existing.tell() + length > size:
                                    break
                                existing.seek(length, os.SEEK_CUR)
            if eof_written:
                if eof and not data:
                    return {"job_id": job_id, "sent": 0, "eof": True}
                raise ValueError("stdin channel is already closed")
            record = (struct.pack("<Q", len(data)) + data) if data else b""
            if eof:
                record += struct.pack("<Q", 0)
            with channel.open("ab") as f:
                f.write(record)
                f.flush()
                os.fsync(f.fileno())
            if eof:
                closed.touch()
        finally:
            lock.release()
        return {"job_id": job_id, "sent": len(data), "eof": bool(eof)}

    def _decision_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "decision_id": row["decision_id"],
            "job_id": row["job_id"],
            "prompt": row["prompt"],
            "options": json.loads(row["options_json"] or "[]"),
            "choice": row["choice"],
            "status": row["status"],
            "resolved_by": row["resolved_by"],
            "created_at": row["created_at"],
            "expires_at": row["expires_at"],
            "resolved_at": row["resolved_at"],
        }

    def _decision_isdue(self, decision: dict[str, Any], now: datetime | None = None) -> bool:
        return _row_isdue(decision.get("expires_at"), decision.get("status"), now)

    def _reload_decision(self, decision_id: str) -> dict[str, Any]:
        row = self._row("SELECT * FROM decisions WHERE decision_id=?", (decision_id,))
        if row is None:  # pragma: no cover - the row was just written/committed
            raise ValueError(f"Unknown decision: {decision_id}")
        return self._decision_dict(row)

    def request_decision(
        self,
        job_id: str,
        prompt: str,
        options: list[str] | None = None,
        timeout_seconds: int | None = None,
    ) -> dict[str, Any]:
        """Persist a durable "needs a decision" request and wake the owner.

        The job-status eligibility check, the row insert, the
        ``decision_requested`` event and its wake deliveries all commit in one
        transaction, so a crash can never leave a pending decision with no wake
        (which a retry could not repair). The job keeps running; nothing about
        its status changes.
        """
        self._ensure_open()
        if not isinstance(prompt, str) or not prompt.strip():
            raise ValueError("prompt must be a non-empty string")
        if len(prompt) > MAX_DECISION_PROMPT_CHARS:
            raise ValueError(f"prompt must be at most {MAX_DECISION_PROMPT_CHARS} characters")
        normalized = _normalize_decision_options(options)
        if timeout_seconds is not None and (
            isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, int) or timeout_seconds < 1
        ):
            raise ValueError("timeout_seconds must be an integer >= 1")
        now = datetime.now(timezone.utc)
        decision_id = "dec_" + uuid.uuid4().hex[:16]
        created_at = now_iso()
        expires_at = (now + timedelta(seconds=timeout_seconds)).isoformat().replace("+00:00", "Z") if timeout_seconds else None

        def mutate(db: sqlite3.Connection) -> None:
            job = db.execute("SELECT status FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if job is None:
                raise ValueError(f"Unknown job_id: {job_id}")
            if job["status"] in TERMINAL_STATUSES:
                raise ValueError(f"job is already terminal: {job['status']}")
            db.execute(
                """
                INSERT INTO decisions(decision_id, job_id, prompt, options_json, choice, status, resolved_by, created_at, expires_at, resolved_at)
                VALUES (?, ?, ?, ?, NULL, ?, NULL, ?, ?, NULL)
                """,
                (decision_id, job_id, prompt, json.dumps(normalized, separators=(",", ":")), DECISION_PENDING, created_at, expires_at),
            )

        event_data = {"decision_id": decision_id, "prompt": prompt, "options": normalized, "expires_at": expires_at}
        if not self._decision_event_fits(event_data):
            raise ValueError("prompt/options are too large for the decision event payload; shorten them")

        self._emit(
            job_id,
            "decision_requested",
            message=prompt,
            data=event_data,
            mutate=mutate,
            exempt_from_cap=True,
        )
        return self._reload_decision(decision_id)

    def _decision_event_fits(self, data: dict[str, Any]) -> bool:
        """Whether a decision lifecycle payload survives ``max_event_bytes``.

        Character limits do not bound the *serialized* size (``json.dumps`` is
        ASCII-only, so one astral char is 12 bytes). Truncation would replace the
        whole data object, dropping ``decision_id``/``choice`` and breaking the
        wake and wait paths, so an oversized payload is rejected up front.
        """
        payload = normalize_event_payload({"type": "decision_event", "message": None, "data": data, "level": "info"})
        return len(json.dumps(payload["data"], separators=(",", ":")).encode()) <= self.max_event_bytes

    def _decision_for(self, job_id: str, token: str) -> sqlite3.Row:
        if not isinstance(token, str) or not token:
            raise ValueError("token must be a non-empty string")
        row = self._row("SELECT * FROM decisions WHERE decision_id=?", (token,))
        if not row or row["job_id"] != job_id:
            raise ValueError(f"Unknown decision for job {job_id}: {token}")
        return row

    def resolve_decision(self, job_id: str, token: str, choice: str, actor: str = "user") -> dict[str, Any]:
        """Record a human choice for a pending decision (idempotent per choice).

        Validation (deadline, status, choice) and the pending->resolved CAS all
        run inside the write transaction, so the deadline is enforced at the
        authoritative transition rather than on a stale read.
        """
        self._ensure_open()
        if not isinstance(choice, str) or not choice:
            raise ValueError("choice must be a non-empty string")
        if not isinstance(token, str) or not token:
            raise ValueError("token must be a non-empty string")
        actor = _normalize_decision_actor(actor)
        event_data = {"decision_id": token, "choice": choice, "actor": actor}
        if not self._decision_event_fits(event_data):
            raise ValueError("decision payload is too large for the event; shorten the choice")

        def mutate(db: sqlite3.Connection) -> None:
            row = db.execute("SELECT * FROM decisions WHERE decision_id=?", (token,)).fetchone()
            if row is None or row["job_id"] != job_id:
                raise ValueError(f"Unknown decision for job {job_id}: {token}")
            if row["status"] == "resolved":
                if row["choice"] == choice:
                    raise _DecisionNoOp()
                raise ValueError(f"decision already resolved as {row['choice']!r}")
            if row["status"] != DECISION_PENDING:
                raise ValueError(f"decision is {row['status']}")
            if _row_isdue(row["expires_at"], row["status"], datetime.now(timezone.utc)):
                raise ValueError("decision expired")
            allowed = json.loads(row["options_json"] or "[]")
            if choice not in allowed:
                raise ValueError(f"choice must be one of {allowed}")
            cursor = db.execute(
                "UPDATE decisions SET status='resolved', choice=?, resolved_by=?, resolved_at=? WHERE decision_id=? AND status=?",
                (choice, actor, now_iso(), token, DECISION_PENDING),
            )
            if cursor.rowcount != 1:
                raise ValueError("decision changed concurrently")

        event = self._emit(
            job_id,
            "decision_resolved",
            message=choice,
            data=event_data,
            mutate=mutate,
            exempt_from_cap=True,
        )
        if event is not None and event.get("noop"):
            row = self._decision_for(job_id, token)
            if row["choice"] != choice:  # pragma: no cover - noop only for equal choice
                raise ValueError(f"decision already resolved as {row['choice']!r}")
            return self._decision_dict(row)
        return self._reload_decision(token)

    def withdraw_decision(self, job_id: str, token: str, actor: str = "user") -> dict[str, Any]:
        """Cancel a pending decision (the requester no longer needs an answer)."""
        self._ensure_open()
        if not isinstance(token, str) or not token:
            raise ValueError("token must be a non-empty string")
        actor = _normalize_decision_actor(actor)
        event_data = {"decision_id": token, "actor": actor}
        if not self._decision_event_fits(event_data):
            raise ValueError("decision payload is too large for the event")

        def mutate(db: sqlite3.Connection) -> None:
            row = db.execute("SELECT * FROM decisions WHERE decision_id=?", (token,)).fetchone()
            if row is None or row["job_id"] != job_id:
                raise ValueError(f"Unknown decision for job {job_id}: {token}")
            if row["status"] != DECISION_PENDING:
                raise ValueError(f"decision is {row['status']}")
            if _row_isdue(row["expires_at"], row["status"], datetime.now(timezone.utc)):
                raise ValueError("decision expired")
            cursor = db.execute(
                "UPDATE decisions SET status='withdrawn', resolved_by=?, resolved_at=? WHERE decision_id=? AND status=?",
                (actor, now_iso(), token, DECISION_PENDING),
            )
            if cursor.rowcount != 1:
                raise ValueError("decision changed concurrently")

        self._emit(
            job_id,
            "decision_withdrawn",
            message="decision withdrawn",
            data=event_data,
            mutate=mutate,
            exempt_from_cap=True,
        )
        return self._reload_decision(token)

    def list_decisions(self, job_id: str | None = None, status: str | None = None, limit: int = 50) -> dict[str, Any]:
        self._ensure_open()
        if status is not None and status not in DECISION_STATUSES:
            raise ValueError(f"status must be one of {sorted(DECISION_STATUSES)}")
        sql = "SELECT * FROM decisions WHERE 1=1"
        args: list[Any] = []
        if job_id:
            sql += " AND job_id=?"
            args.append(job_id)
        if status:
            sql += " AND status=?"
            args.append(status)
        sql += " ORDER BY created_at DESC, decision_id DESC LIMIT ?"
        args.append(validate_limit(limit, "limit", maximum=500))
        with self.db_lock:
            rows = self.db.execute(sql, tuple(args)).fetchall()
        return {"decisions": [self._decision_dict(row) for row in rows], "count": len(rows)}

    def _expire_decisions(self) -> None:
        """Expire overdue pending decisions (maintenance loop; edge-triggered).

        Each expiry's state change and ``decision_expired`` event commit
        together, so an expiry is never recorded without its event.
        """
        now = datetime.now(timezone.utc)
        with self.db_lock:
            rows = self.db.execute(
                "SELECT decision_id, job_id, expires_at FROM decisions WHERE status=? AND expires_at IS NOT NULL",
                (DECISION_PENDING,),
            ).fetchall()
        for row in rows:
            if not _row_isdue(row["expires_at"], DECISION_PENDING, now):
                continue

            def mutate(db: sqlite3.Connection, decision_id: str = row["decision_id"], expires_at: str = row["expires_at"]) -> None:
                # Re-check under the write lock: a concurrent resolve/withdraw
                # (or a resolver that beat the deadline by a hair) wins.
                current = db.execute("SELECT expires_at, status FROM decisions WHERE decision_id=?", (decision_id,)).fetchone()
                if current is None or not _row_isdue(current["expires_at"], current["status"], datetime.now(timezone.utc)):
                    raise _DecisionNoOp()
                cursor = db.execute(
                    "UPDATE decisions SET status='expired', resolved_at=? WHERE decision_id=? AND status=?",
                    (now_iso(), decision_id, DECISION_PENDING),
                )
                if cursor.rowcount != 1:
                    raise _DecisionNoOp()

            self._emit(
                row["job_id"],
                "decision_expired",
                message="decision timed out",
                data={"decision_id": row["decision_id"]},
                mutate=mutate,
                exempt_from_cap=True,
            )


def _row_isdue(expires_at: Any, status: Any, now: datetime | None = None) -> bool:
    """Whether a stored deadline has passed. Malformed/non-pending -> False."""
    if status != DECISION_PENDING or not expires_at:
        return False
    parsed = _parse_iso(str(expires_at))
    if parsed is None:
        return False
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed <= (now or datetime.now(timezone.utc))


def _normalize_decision_actor(actor: Any) -> str:
    if not isinstance(actor, str) or not actor.strip():
        raise ValueError("actor must be a non-empty string")
    if len(actor) > MAX_DECISION_ACTOR_CHARS:
        raise ValueError(f"actor must be at most {MAX_DECISION_ACTOR_CHARS} characters")
    return actor


def _normalize_decision_options(options: list[str] | None) -> list[str]:
    choices = DEFAULT_DECISION_OPTIONS if options is None else options
    if not isinstance(choices, list) or not choices:
        raise ValueError("options must be a non-empty list of strings")
    if len(choices) > MAX_DECISION_OPTIONS:
        raise ValueError(f"options must contain at most {MAX_DECISION_OPTIONS} entries")
    normalized: list[str] = []
    seen: set[str] = set()
    for option in choices:
        if not isinstance(option, str) or not option.strip():
            raise ValueError("options must be non-empty strings")
        if len(option) > MAX_DECISION_OPTION_CHARS:
            raise ValueError(f"each option must be at most {MAX_DECISION_OPTION_CHARS} characters")
        if option not in seen:
            seen.add(option)
            normalized.append(option)
    return normalized


client: VanthClient | None = None
mcp = FastMCP("vanth")


_client_lock = threading.Lock()


def get_client() -> VanthClient:
    global client
    # Tools now run their blocking work in worker threads, so the lazy global
    # must be initialized under a lock: two concurrent first calls could
    # otherwise both construct a client and both run ensure() (double daemon
    # spawn / port-conflict RuntimeError).
    with _client_lock:
        if client is None:
            client = VanthClient()
            client.ensure()
        return client


def tool_error(message: str) -> dict[str, Any]:
    return {"result": "error", "error": message}


@mcp.tool()
def job_start(
    command: str,
    cwd: str | None = None,
    name: str | None = None,
    env: dict[str, str] | None = None,
    timeout_seconds: int | None = None,
    notify_on: list[str] | None = None,
    wake_targets: list[dict[str, Any]] | None = None,
    origin_thread_id: str | None = None,
    tags: list[str] | None = None,
    notes: str | None = None,
    interactive: bool = False,
    trigger: dict[str, Any] | None = None,
    policy: dict[str, Any] | None = None,
    secret_env: list[str] | None = None,
    pool: str | None = None,
    priority: int = 0,
    remote_id: str | None = None,
    idempotency_key: str | None = None,
    wake_me: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Core: Start a background job. For anything that may outlive this turn,
    pass ``wake_me=True`` so completion resumes this session instead of polling
    (``job_wait`` still tracks it meanwhile). Use ``job_wait``/``job_status`` to
    check progress.

    For a direct local job, this call also waits up to three seconds for the
    workload's ``started`` event. ``startup_confirmed`` reports whether that
    confirmation arrived; false means check later, not that startup failed.
    Queued jobs and remote submissions return without this local confirmation.

    ``pool`` queues the job behind a named concurrency pool instead of starting
    it immediately; ``priority`` (higher first) orders queued pool/trigger jobs.
    Configure pools with ``pool_configure`` and hold/release a queued job with
    ``job_pause`` / ``job_resume``.

    ``secret_env`` names env vars whose values are masked (``***``) in captured
    stdout/stderr and structured events — the GitHub ``::add-mask::`` pattern.
    The value is still kept in the job's own env in the owner-only database, as
    with any env var; masking protects emitted output. Local jobs only.

    ``trigger`` optionally gates launch: pass ``{"job_id": "A", "status":
    "completed"}`` to start only once job A reaches ``completed`` (the job stays
    ``queued`` until then, or is ``cancelled`` if A ends differently). Add a
    readiness ``probe`` (ANDed with the DAG gate when both are present):
    ``{"probe": {"type": "port", "host": "127.0.0.1", "port": 5432,
    "timeout_seconds": 120}}``. Probe types are ``port``, ``http``
    (``url`` + optional ``expect_status``), ``log_line`` (``job_id`` +
    ``pattern`` + optional ``stream``), and ``file`` (``path``). Each accepts
    optional ``timeout_seconds`` (cancel the queued job if never ready) and
    ``interval_seconds`` (probe cadence; default 1s).

    ``policy`` adds dead-man's-switch monitoring and failure reactions:
      - ``{"schedule": {"expected_interval_seconds": 3600, "grace_period_seconds": 300}}``
        emits ``schedule_missed`` when no new run starts within interval+grace
        and ``job_stuck`` when a run outlasts interval+grace.
      - ``{"on_failure": {"after_n": 3, "action": "alert"|"disable"|"run_job",
        "job_id": "cleanup_job"}}`` fires once the failure streak reaches N:
        ``alert`` emits ``failure_threshold``, ``disable`` stops the job from
        launching again, ``run_job`` launches another job. All three emit
        ``failure_threshold`` and flow to wake targets.
      - ``{"restart": {"max_retries": 3, "backoff_seconds": 5,
        "backoff_max_seconds": 60}}`` relaunches a failed job with linear
        backoff capped at the max, resetting the counter on success; when the
        budget is exhausted emits ``gave_up``.
      - ``{"retention": {"events_seconds": 604800, "metrics_seconds": 2592000}}``
        prunes that job's non-terminal events / metric points / settled
        deliveries older than the TTL (log-retention without the paywall).

    Pass ``remote_id`` to run the job on a paired remote host instead of the
    local daemon; discover ids with ``remote_list`` (pairing is interactive:
    `vanth remote pair user@host`). Remote mutations REQUIRE a caller-supplied
    ``idempotency_key`` (8..128 chars in ``[A-Za-z0-9_-]``) — that is what makes
    a lost response safe to retry — and the daemon rejects a missing one. Local
    starts optionally accept a key too: retry identical settings with the same
    key to recover the existing job, including after a daemon restart. Reusing
    a key for different settings is rejected. ``dry_run=True`` validates and
    previews a local start without launching work or creating a job.

    ``wake_targets`` resumes a client when the job emits a matching event. For
    ``opencode_thread``, pass the OpenCode ``ses_...`` session id (from
    ``opencode session list``, or the destination ``vanth doctor`` prints) — NOT
    the relay client id ``opencode-<pid>-<rand>``, which is the long-poll
    identity and can never be woken. Omit ``session_id`` to resolve the live
    plugin relay for the job's directory; ``attach`` is optional (only for a
    headless ``opencode serve``). The daemon rejects a client id in
    ``session_id`` rather than enqueueing a wake that would never be claimed.
    ``wake_me`` wakes the calling OpenCode thread on every terminal outcome
    (completed, failed, timeout, cancelled, or orphaned). Explicit
    ``wake_targets`` win when both are supplied.
    """
    # Thread identity is resolved HERE, in the MCP process that owns the
    # calling task (review P1-4). The persistent daemon's environment belongs
    # to whichever client originally spawned it, so resolving inside the
    # daemon would inherit a wrong/absent thread. Explicit ids always win.
    # opencode_thread targets are NOT auto-inherited (OpenCode never injects
    # OPENCODE_SESSION_ID — review P1-1); callers must pass session_id.
    origin_thread_id = (
        origin_thread_id
        or os.environ.get("CODEX_THREAD_ID")
    )
    cwd = cwd or os.getcwd()
    # Default: ask the daemon to attach the calling-session wake to an
    # agent-started local job that did not ask for one, so completion resumes the
    # session instead of relying on polling. Best-effort: the daemon skips it when
    # no live relay resolves, so it never fails a start. Skipped for
    # short/interactive/remote/preview jobs and when the job runs outside the
    # caller's directory. Opt out with VANTH_DEFAULT_WAKE_ME=0.
    wake_default = (
        not wake_me
        and wake_targets is None
        and not interactive
        and remote_id is None
        and not dry_run
        and (timeout_seconds is None or timeout_seconds >= _default_wake_min_seconds())
        and _env_flag_default_on("VANTH_DEFAULT_WAKE_ME")
        and _same_directory(cwd, os.getcwd())
    )
    copied_targets: list[dict[str, Any]] | None = None
    if wake_me and wake_targets is None:
        # Mirror the CLI shorthand: the events MUST be present (a wake target
        # with neither events nor notify_on is rejected as empty), and the
        # caller's directory pins relay resolution so an omitted session_id does
        # not resolve an unrelated project's relay.
        wake_targets = [{
            "type": "opencode_thread",
            "events": ["completed", "failed", "timeout", "cancelled", "orphaned"],
            "cwd": cwd or os.getcwd(),
        }]
    if wake_targets is not None:
        copied_targets = resolve_wake_target_identity(wake_targets, origin_thread_id)
    client = get_client()
    if dry_run and remote_id:
        raise ValueError("start preview is supported for local jobs only")
    started = client.post(
        "/jobs/preview" if dry_run else "/jobs",
        {
            "command": command,
            "cwd": cwd,
            "name": name,
            "env": env,
            "timeout_seconds": timeout_seconds,
            "notify_on": notify_on,
            "wake_targets": copied_targets,
            "origin_thread_id": origin_thread_id,
            "tags": tags,
            "notes": notes,
            "interactive": interactive,
            "trigger": trigger,
            "policy": policy,
            "secret_env": secret_env,
            "pool": pool,
            "priority": priority,
            "remote_id": remote_id,
            "idempotency_key": idempotency_key,
            "wake_default": wake_default,
        },
    )
    return client.confirm_local_start(started) if not remote_id and not dry_run else started


@mcp.tool()
async def job_start_and_wait(command: str, cwd: str | None = None, name: str | None = None,
                             env: dict[str, str] | None = None, timeout_seconds: int | None = None,
                             wait_timeout_seconds: int = 20, tags: list[str] | None = None,
                             notes: str | None = None, secret_env: list[str] | None = None,
                             idempotency_key: str | None = None) -> dict[str, Any]:
    """Run a short local job, wait for a terminal outcome, and return its summary.

    The wait lasts 1..300 seconds; if it does not reach a terminal event in an
    MCP-safe slice (``VANTH_MCP_WAIT_SLICE``, default 25 s) it returns
    ``{"wait": {"result": "still_running"}}`` and the ``job_id`` so the caller
    can call ``job_wait`` again. The job keeps running either way. Use
    ``job_start`` with a wake target for long work. ``timeout_seconds`` is the
    job's runtime limit, while ``wait_timeout_seconds`` only bounds this call.
    """
    if isinstance(wait_timeout_seconds, bool) or not isinstance(wait_timeout_seconds, int) or not 1 <= wait_timeout_seconds <= 300:
        raise ValueError("wait_timeout_seconds must be between 1 and 300")
    started = await asyncio.to_thread(
        job_start, command=command, cwd=cwd, name=name, env=env,
        timeout_seconds=timeout_seconds, tags=tags, notes=notes,
        secret_env=secret_env, idempotency_key=idempotency_key,
        # This tool already returns the outcome; opting out of the default wake
        # (wake_targets=[]) avoids a redundant terminal wake firing later.
        wake_targets=[],
    )
    job_id = started.get("job_id")
    if not job_id:
        return started
    waited = await job_wait(job_id, ["completed", "failed", "timeout", "cancelled", "orphaned"],
                            timeout_seconds=wait_timeout_seconds)
    summary = await asyncio.to_thread(job_run_summary, job_id,
                                      include_stderr_excerpt=True, include_stdout_excerpt=True)
    return {"job_id": job_id, "status": summary["status"], "wait": waited, "summary": summary}


@mcp.tool()
def job_rerun(job_id: str, command: str | None = None, env: dict[str, str] | None = None,
              timeout_seconds: int | None = None, name: str | None = None, tags: list[str] | None = None,
              notes: str | None = None, cwd: str | None = None, interactive: bool | None = None,
              secret_env: list[str] | None = None,
              remote_id: str | None = None, idempotency_key: str | None = None) -> dict[str, Any]:
    """Core: Start a new job from this job's settings, overriding only supplied fields.

    Use after inspecting a failed or completed run; this creates a new job ID.
    Remote reruns require ``idempotency_key``. Direct local reruns include the
    same bounded startup confirmation and failure guidance as ``job_start``.
    """
    payload = {key: value for key, value in {
        "command": command,
        "env": env,
        "timeout_seconds": timeout_seconds,
        "name": name,
        "tags": tags,
        "notes": notes,
        "cwd": cwd,
        "interactive": interactive,
        "secret_env": secret_env,
        "remote_id": remote_id,
        "idempotency_key": idempotency_key,
    }.items() if value is not None}
    client = get_client()
    started = client.post(f"/jobs/{job_id}/rerun", payload)
    return client.confirm_local_start(started) if not remote_id else started


@mcp.tool()
def job_status_batch(job_ids: list[str], limit: int = 500) -> dict[str, Any]:
    """Core: Get status for several job IDs in one call; use when tracking a batch of jobs."""
    return get_client().get("/status/batch", {"job_ids": ",".join(job_ids), "limit": limit})


@mcp.tool()
def job_status(job_id: str, remote_id: str | None = None) -> dict[str, Any]:
    """Core: Check one job's current state and summary; pass ``remote_id`` for a paired host."""
    return get_client().get(f"/jobs/{job_id}/status", {"remote_id": remote_id})


@mcp.tool()
def job_send(job_id: str, input: str, eof: bool = False) -> dict[str, Any]:
    """Core: Send stdin to an interactive job; set ``eof=True`` when input is complete."""
    return get_client().post(f"/jobs/{job_id}/send", {"input": input, "eof": eof})


@mcp.tool()
def job_request_decision(
    job_id: str,
    prompt: str,
    options: list[str] | None = None,
    timeout_seconds: int | None = None,
) -> dict[str, Any]:
    """Ask a human to decide something about a job and wait durably for the answer.

    Creates a durable ``decision`` (status ``pending``) and notifies the job's
    wake targets, so the owning thread learns a human is needed. The job keeps
    running — its status is untouched. ``options`` defaults to
    ``["approve", "deny"]``; ``timeout_seconds`` expires the request (the
    daemon emits ``decision_expired`` and the decision can no longer be
    resolved). Wait for the answer with
    ``job_wait(job_id, ["decision_resolved"])`` or poll ``job_decisions``.

    Resolve with ``job_resolve(job_id, decision_id, choice)``; cancel an
    unneeded request with ``job_withdraw_decision(job_id, decision_id)``.
    """
    payload = {"prompt": prompt, "options": options, "timeout_seconds": timeout_seconds}
    return get_client().post(f"/jobs/{job_id}/decision", payload)


@mcp.tool()
def job_resolve(job_id: str, token: str, choice: str) -> dict[str, Any]:
    """Answer a pending decision with one of its ``options`` (see job_request_decision)."""
    return get_client().post(f"/jobs/{job_id}/decision/{token}/resolve", {"choice": choice})


@mcp.tool()
def job_withdraw_decision(job_id: str, token: str) -> dict[str, Any]:
    """Withdraw a pending decision that no longer needs an answer."""
    return get_client().post(f"/jobs/{job_id}/decision/{token}/withdraw")


@mcp.tool()
def job_decisions(job_id: str | None = None, status: str | None = None, limit: int = 50) -> dict[str, Any]:
    """List decisions (newest first), optionally filtered by job and/or status.

    ``status`` is one of ``pending``, ``resolved``, ``withdrawn``, ``expired``.
    """
    return get_client().get("/decisions", {"job_id": job_id, "status": status, "limit": limit})


@mcp.tool()
def job_list(status: list[str] | None = None, limit: int = 50, thread_id: str | None = None,
             name: str | None = None, tags: list[str] | None = None,
             remote_id: str | None = None) -> dict[str, Any]:
    """List jobs. With ``remote_id``, list that paired host's jobs (its shadow).

    Discover paired hosts with ``remote_list``.
    """
    if remote_id:
        # The remote read path projects the controller's shadow and supports only
        # `limit`; silently dropping a filter would return jobs the caller did not
        # ask for (e.g. completed ones when filtering on "running").
        unsupported = [
            name
            for name, value in (("status", status), ("thread_id", thread_id), ("name", name), ("tags", tags))
            if value is not None
        ]
        if unsupported:
            raise ValueError(
                f"filters not supported for remote job lists: {', '.join(unsupported)}; "
                "fetch with job_list(remote_id=...) and filter locally"
            )
        return get_client().get(f"/remotes/{remote_id}/jobs", {"limit": limit})
    return get_client().get("/jobs", {"status": status, "limit": limit, "thread_id": thread_id, "name": name, "tags": tags})


@mcp.tool()
def remote_list() -> dict[str, Any]:
    """List paired remote execution hosts.

    Pass a returned ``remote_id`` to ``job_start``/``job_list``/``job_status``/
    ``job_tail``/``job_stop``/``job_rerun``/``job_wait`` to act on that host
    instead of the local daemon. Pairing itself is interactive and lives in the
    CLI (`vanth remote pair user@host`).
    """
    return get_client().get("/remotes")


@mcp.tool()
def remote_doctor(remote_id: str | None = None) -> dict[str, Any]:
    """Report SSH availability and remote state; omit ``remote_id`` for all hosts."""
    return get_client().get("/remotes/doctor", {"remote_id": remote_id})


@mcp.tool()
def job_view(thread_id: str | None = None, limit: int = 50) -> dict[str, Any]:
    """Core: Show recent jobs, optionally scoped to a thread; use to rediscover job IDs."""
    return get_client().get("/view", {"thread_id": thread_id, "limit": limit})


@mcp.tool()
def job_events(job_id: str, since_event_id: str | None = None, types: list[str] | None = None, limit: int = 20,
               reverse: bool = False) -> dict[str, Any]:
    """Core: Read a job's structured event history; pass ``since_event_id`` to continue from a prior result."""
    return get_client().get(f"/jobs/{job_id}/events", {"since_event_id": since_event_id, "types": types, "limit": limit, "reverse": reverse})


@mcp.tool()
def job_deliveries(job_id: str | None = None, status: str | None = None, limit: int = 20) -> dict[str, Any]:
    """Core: Inspect wake notification deliveries; filter by job or status when diagnosing a missed wake."""
    return get_client().get("/deliveries", {"job_id": job_id, "status": status, "limit": limit})


@mcp.tool()
def job_mark_delivery(delivery_id: str, status: str, error: str | None = None) -> dict[str, Any]:
    """Advanced: Record a delivery outcome manually; use after external delivery handling or to stop retries."""
    return get_client().post(f"/deliveries/{delivery_id}/mark", {"status": status, "error": error})


@mcp.tool()
def job_retry_delivery(delivery_id: str) -> dict[str, Any]:
    """Core: Retry a failed or retrying wake delivery; inspect ``job_delivery_attempts`` if it fails again."""
    return get_client().post(f"/deliveries/{delivery_id}/retry")


@mcp.tool()
def job_delivery_attempts(delivery_id: str, limit: int = 20) -> dict[str, Any]:
    """Core: Inspect attempt history for one wake delivery to find why it was not delivered."""
    return get_client().get(f"/deliveries/{delivery_id}/attempts", {"limit": limit})


@mcp.tool()
def job_clear_deliveries(
    job_id: str | None = None,
    status: str | None = None,
    older_than_seconds: int | None = None,
    stale_only: bool = False,
    limit: int = 1000,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Bulk-drain stale wake deliveries (notification queue overflow valve).

    Use when a flood of stale notifications arrives at once (e.g. after the
    machine was offline or a disk filled up). Marks matching deliveries as
    permanently failed so they stop retrying.

    Filters (combine freely):
      - ``job_id``: only deliveries for one job
      - ``status``: 'pending' | 'retrying' | 'dispatching' | 'delivered' | 'failed'
      - ``older_than_seconds``: only deliveries created before the cutoff
      - ``stale_only``: only deliveries whose source job is already terminal
      - ``limit``: drain at most N (default 1000)
      - ``dry_run``: preview counts without changing anything (default true)

    View the queue first with job_deliveries(status='retrying'/'pending').
    """
    return get_client().post(
        "/deliveries/clear",
        {
            "job_id": job_id,
            "status": status,
            "older_than_seconds": older_than_seconds,
            "stale_only": stale_only,
            "limit": limit,
            "dry_run": dry_run,
        },
    )


_MAX_MCP_WAIT_SLICE = 45.0


def _mcp_wait_slice_seconds() -> float:
    """Client-safe ceiling (seconds) for a single blocking MCP call.

    MCP clients cap a tool call at their own request timeout (the reference
    TypeScript SDK default is 60 s). A blocking tool must return before that or
    the client reports ``-32001`` and the result is lost; long waits are made
    resumable instead of blocking past this budget. Override with
    ``VANTH_MCP_WAIT_SLICE``; the value is clamped to ``_MAX_MCP_WAIT_SLICE`` so
    the socket deadline (slice + margin) stays under the client budget.
    """
    try:
        value = float(os.environ.get("VANTH_MCP_WAIT_SLICE", "25"))
    except ValueError:
        return 25.0
    if not math.isfinite(value) or value <= 0:
        return 25.0
    return min(value, _MAX_MCP_WAIT_SLICE)


def _job_wait_blocking(client: VanthClient, job_id: str, filters: list[str], since_event_id: str | None,
                       timeout_seconds: int, return_progress: bool,
                       metric_ge: dict[str, float] | None, remote_id: str | None) -> dict[str, Any]:
    # Blocking HTTP call. Always invoked via asyncio.to_thread so it never stalls
    # the MCP stdio event loop: a stalled loop makes every queued tool call
    # (job_status, job_tail) time out behind the wait. The +5 margin covers the
    # response round-trip only; the daemon returns at the deadline.
    return client.post(
        f"/jobs/{job_id}/wait",
        {"filters": filters, "since_event_id": since_event_id, "timeout_seconds": timeout_seconds,
         "return_progress": return_progress, "metric_ge": metric_ge, "remote_id": remote_id},
        timeout=float(timeout_seconds) + 5,
    )


@mcp.tool()
async def job_tail(job_id: str, stream: str = "stdout", max_bytes: int = 8192, offset: int | None = None,
                   follow: bool = False, timeout_seconds: float = 5.0, grep: str | None = None,
                   remote_id: str | None = None) -> dict[str, Any]:
    """Read a job's captured output.

    With ``remote_id`` the log is read from that paired host over the remote
    protocol (a single byte range; ``follow``/``grep`` do not apply, and
    ``offset``/``max_bytes`` select the range).
    """
    if remote_id:
        # A remote read is a single byte range: refuse the options it cannot
        # honour instead of silently ignoring them (callers would otherwise
        # believe they were following a live log). Validate BEFORE touching the
        # daemon so a rejection never performs I/O or spawns a daemon.
        if follow:
            raise ValueError("follow is not supported when reading a remote job's log; poll job_tail instead")
        if grep is not None:
            # `is not None`, not truthiness: grep="" was supplied and cannot be
            # honoured, so it must not be silently dropped.
            raise ValueError("grep is not supported when reading a remote job's log; filter the returned content")
        client = get_client()
        return await asyncio.to_thread(
            client.get,
            f"/remotes/{remote_id}/jobs/{job_id}/tail",
            {"stream": stream, "offset": offset or 0, "size": max_bytes},
            # A remote byte-range read has no reason to exceed the MCP slice;
            # cap the socket deadline so one call stays under the client budget.
            timeout=min(float(timeout_seconds), _mcp_wait_slice_seconds()) + 5,
        )
    # Bound a follow to the MCP-safe slice so the client does not time out; the
    # caller resumes from ``next_offset``.
    effective_timeout = float(timeout_seconds)
    if follow and effective_timeout > _mcp_wait_slice_seconds():
        effective_timeout = max(1.0, _mcp_wait_slice_seconds())
    client = get_client()
    return await asyncio.to_thread(
        client.get,
        f"/jobs/{job_id}/tail",
        {"stream": stream, "max_bytes": max_bytes, "offset": offset,
         "follow": follow, "timeout_seconds": effective_timeout, "grep": grep},
        timeout=effective_timeout + 5,
    )


@mcp.tool()
async def job_wait(
    job_id: str,
    filters: list[str],
    since_event_id: str | None = None,
    timeout_seconds: int = 3600,
    return_progress: bool = False,
    metric_ge: dict[str, float] | None = None,
    remote_id: str | None = None,
) -> dict[str, Any]:
    """Wait until a matching event fires or a metric crosses a threshold.

    ``filters`` is a list of event types (e.g. ``["completed", "failed"]``);
    the first matching event (after ``since_event_id``) is returned as
    ``{"result": "event", ...}``. ``metric_ge`` maps a metric name (e.g.
    ``loss``, ``progress.percent``) to a numeric threshold; when the latest
    stored value reaches it, the wait returns ``{"result": "metric", ...}``
    with the threshold and current value. ``return_progress`` streams progress
    events instead of blocking. Returns ``{"result": "timeout"}`` when
    ``timeout_seconds`` elapses.

    A single call blocks at most ``VANTH_MCP_WAIT_SLICE`` seconds (default 25),
    well under an MCP client's own request timeout. If the event has not fired
    by then, the result is ``{"result": "still_running", "job_id": ...,
    "status": ...}`` and the caller should call ``job_wait`` again (or
    ``job_status``); the job keeps running. Never raise the client's timeout to
    make a long wait work — just re-call.

    With ``remote_id`` the wait polls ``RemoteControl.status`` every 0.2s until
    the remote reports a terminal status or the timeout elapses (a real
    cross-machine event push arrives in Phase 4); ``since_event_id``,
    ``return_progress`` and ``metric_ge`` are ignored in that mode.
    """
    slice_seconds = _mcp_wait_slice_seconds()
    capped = timeout_seconds is not None and timeout_seconds > slice_seconds
    effective = max(1, int(slice_seconds)) if capped else timeout_seconds
    result = await asyncio.to_thread(
        _job_wait_blocking, get_client(), job_id, filters, since_event_id, effective,
        return_progress, metric_ge, remote_id,
    )
    # Only offer a resumable wait while the job can still produce the event. A
    # terminal job whose narrow filter never matched must stay a plain timeout,
    # or an agent would re-call forever on a status that can never change.
    if capped and result.get("result") == "timeout" and result.get("status") not in TERMINAL_STATUSES:
        return {
            "result": "still_running",
            "job_id": job_id,
            "status": result.get("status"),
            "waited_seconds": effective,
            "requested_timeout_seconds": timeout_seconds,
            "message": ("No matching event within the MCP wait slice; call job_wait again to keep "
                        "waiting, or job_status for the current state."),
        }
    return result


@mcp.tool()
async def job_stop(job_id: str, signal: str = "terminate", kill_after_seconds: int = 10,
                   reason: str | None = None,
                   remote_id: str | None = None, idempotency_key: str | None = None) -> dict[str, Any]:
    """Stop a running job.

    ``reason`` optionally records why the caller is stopping it; the resulting
    ``cancelled`` event carries ``{"actor": "tool", "reason": ...}`` so the kill
    is attributable after the fact.
    """
    # The daemon waits out the grace period, so the socket deadline covers it —
    # but never past the MCP client's own request timeout. Offloaded so it does
    # not stall other tool calls, and bounded so a long grace period returns an
    # acknowledgement instead of `-32001` (the daemon keeps stopping regardless).
    client = get_client()
    budget = _mcp_wait_slice_seconds()
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(
                client.post,
                f"/jobs/{job_id}/stop",
                {"signal": signal, "kill_after_seconds": kill_after_seconds,
                 "actor": "tool", "reason": reason,
                 "remote_id": remote_id, "idempotency_key": idempotency_key},
                timeout=budget + 1,
            ),
            timeout=budget,
        )
    except (asyncio.TimeoutError, TimeoutError):
        return {
            "result": "still_running",
            "job_id": job_id,
            "message": ("Stop requested; the process tree is still terminating. Re-check with "
                        "job_status or job_wait for the cancelled event."),
        }


@mcp.tool()
def job_pause(job_id: str) -> dict[str, Any]:
    """Hold a queued job so the dispatcher will not launch it (pool/trigger)."""
    return get_client().post(f"/jobs/{job_id}/pause", {})


@mcp.tool()
def job_resume(job_id: str) -> dict[str, Any]:
    """Release a paused queued job back to the dispatcher."""
    return get_client().post(f"/jobs/{job_id}/resume", {})


@mcp.tool()
def pool_configure(pool: str, max_parallel: int = 0, paused: bool | None = None) -> dict[str, Any]:
    """Create/update a concurrency pool. ``max_parallel`` 0 = unlimited."""
    return get_client().post("/pools", {"pool": pool, "max_parallel": max_parallel, "paused": paused})


@mcp.tool()
def pool_list() -> dict[str, Any]:
    """List concurrency pools with queued/running counts."""
    return get_client().get("/pools")


@mcp.tool()
def schedule_create(command: str, cron: str | None = None, interval_seconds: int | None = None,
                    name: str | None = None, timezone_name: str = "UTC", cwd: str | None = None,
                    env: dict[str, str] | None = None, timeout_seconds: int | None = None,
                    tags: list[str] | None = None, notes: str | None = None,
                    secret_env: list[str] | None = None, overlap: str = "skip",
                    enabled: bool = True) -> dict[str, Any]:
    """Create a schedule that launches a fresh job per fire.

    Pass exactly one of ``cron`` (5-field, or ``@daily`` etc.) or
    ``interval_seconds``. ``overlap`` is ``skip`` (default; skip the fire while
    the previous run is active) or ``allow``.
    """
    return get_client().post("/schedules", {
        "command": command, "cron": cron, "interval_seconds": interval_seconds, "name": name,
        "timezone_name": timezone_name, "cwd": cwd, "env": env, "timeout_seconds": timeout_seconds,
        "tags": tags, "notes": notes, "secret_env": secret_env, "overlap": overlap, "enabled": enabled,
    })


@mcp.tool()
def schedule_list() -> dict[str, Any]:
    """List all schedules."""
    return get_client().get("/schedules")


@mcp.tool()
def schedule_update(schedule_id: str, changes: dict[str, Any]) -> dict[str, Any]:
    """Edit a schedule in place (name/cron/interval_seconds/timezone/command/
    cwd/env/timeout_seconds/tags/notes/secret_env/overlap/enabled)."""
    return get_client().post(f"/schedules/{schedule_id}/update", changes)


@mcp.tool()
def schedule_delete(schedule_id: str) -> dict[str, Any]:
    """Delete a schedule (already-created jobs are unaffected)."""
    return get_client().post(f"/schedules/{schedule_id}/delete", {})


@mcp.tool()
def schedule_next(schedule_id: str, count: int = 5) -> dict[str, Any]:
    """Preview the next ``count`` fire times for a schedule."""
    return get_client().get(f"/schedules/{schedule_id}/next", {"count": count})


@mcp.tool()
def job_doctor(verify_artifacts: bool = False) -> dict[str, Any]:
    """Core: Check daemon and job-store health; use when Vanth tools fail or report inconsistent state."""
    return get_client().get("/doctor", {"verify_artifacts": verify_artifacts})


@mcp.tool()
def job_cleanup(older_than_seconds: int, dry_run: bool = True) -> dict[str, Any]:
    """Advanced: Preview or delete terminal jobs older than the given age; inspect the dry run before deleting."""
    return get_client().post("/cleanup", {"older_than_seconds": older_than_seconds, "dry_run": dry_run})


@mcp.tool()
def job_metrics_query(job_id: str, metric: str | None = None, from_ms: int | None = None,
                      to_ms: int | None = None, limit: int = 1000) -> dict[str, Any]:
    """Return stored scalar metric series for a job (loss, accuracy, progress.percent, ...)."""
    return get_client().get(f"/jobs/{job_id}/metrics", {"metric": metric, "from_ms": from_ms, "to_ms": to_ms, "limit": limit})


@mcp.tool()
def job_metric_compare(job_ids: list[str], metric: str, aggregation: str = "latest",
                       from_ms: int | None = None, to_ms: int | None = None) -> dict[str, Any]:
    """Compare one metric across jobs (e.g. val_loss across training runs)."""
    return get_client().get("/metrics/compare", {"job_ids": job_ids, "metric": metric, "aggregation": aggregation,
                                                 "from_ms": from_ms, "to_ms": to_ms})


@mcp.tool()
def job_duration_stats(name: str | None = None, tags: list[str] | None = None, limit: int = 20,
                       runs_per_group: int = 200, since_ms: int | None = None, slowest: int = 10) -> dict[str, Any]:
    """Duration, queue-time, and flakiness analytics grouped by logical job.

    Returns per-group p50/p95 runtime and queue time, success rate, a flaky
    score (failed runs with a success both before and after), the group's
    slowest runs, and a ``trend`` flag (``regressing``/``stable``/``improving``)
    that catches a job creeping slower over weeks. ``slowest`` is the top-N
    slowest runs across all groups.
    """
    return get_client().get("/analytics/durations", {
        "name": name, "tags": tags, "limit": limit, "runs_per_group": runs_per_group,
        "since_ms": since_ms, "slowest": slowest,
    })


@mcp.tool()
def job_run_summary(job_id: str, include_stderr_excerpt: bool = False,
                    include_stdout_excerpt: bool = False) -> dict[str, Any]:
    """One-call summary with optional bounded stderr (2 KiB) and stdout (8 KiB)."""
    return get_client().get(f"/jobs/{job_id}/summary", {
        "include_stderr_excerpt": include_stderr_excerpt,
        "include_stdout_excerpt": include_stdout_excerpt,
    })


@mcp.tool()
def job_diff(base_job_id: str, other_job_id: str) -> dict[str, Any]:
    """Diff the run specs of two jobs (command, env, cwd, timeout, tags, wake targets).

    Useful for comparing a job to its rerun, or two pipeline stages, to see
    exactly what changed. Returns a list of per-field changes with base/other
    values and `identical: true` when nothing differs.
    """
    return get_client().get(f"/jobs/{base_job_id}/diff", {"other": other_job_id})


@mcp.tool()
def job_artifact_add(job_id: str, name: str, uri: str, kind: str | None = None,
                     size_bytes: int | None = None, sha256: str | None = None,
                     meta: dict[str, Any] | None = None) -> dict[str, Any]:
    """Attach an artifact (checkpoint, CSV, rendered output) to a job."""
    return get_client().post(f"/jobs/{job_id}/artifacts", {"name": name, "uri": uri, "kind": kind,
                                                           "size_bytes": size_bytes, "sha256": sha256, "meta": meta})


@mcp.tool()
def job_artifacts(job_id: str, limit: int = 50) -> dict[str, Any]:
    """List artifacts attached to a job."""
    return get_client().get(f"/jobs/{job_id}/artifacts", {"limit": limit})


@mcp.tool()
def job_dashboard(job_ids: list[str] | None = None, limit: int = 5000) -> dict[str, Any]:
    """Chart-data view (downsampled series per job + job list) for any chart renderer."""
    return get_client().get("/dashboard", {"job_ids": job_ids, "limit": limit})


@mcp.tool()
def job_metric_ingest(job_id: str, metrics: list[dict[str, Any]], idempotency_key: str | None = None) -> dict[str, Any]:
    """Record scalar metric points for a job (loss, accuracy, ...) programmatically."""
    return get_client().post(f"/jobs/{job_id}/metrics", {"metrics": metrics, "idempotency_key": idempotency_key})


@mcp.tool()
def job_artifact_read(artifact_id: str, max_bytes: int = 262144) -> dict[str, Any]:
    """Fetch the content of an artifact (base64-encoded) for direct consumption."""
    return get_client().get(f"/artifacts/{artifact_id}/content", {"max_bytes": max_bytes})


@mcp.tool()
def artifact_put(path: str, name: str, idempotency_key: str | None = None) -> dict[str, Any]:
    """Publish a local file into the managed artifact store as an immutable version.

    ``name`` selects the file root; identical content re-published to the same
    root deduplicates onto the existing version.
    """
    return get_client().post("/artifacts/put", {"path": path, "name": name, "idempotency_key": idempotency_key})


@mcp.tool()
def artifact_put_dir(source_path: str, name: str, idempotency_key: str | None = None) -> dict[str, Any]:
    """Publish a local directory tree into the managed artifact store as an immutable v1 version.

    Capture refuses symlinks, reparse points, special files, and source
    mutation; identical trees re-published to the same root deduplicate.
    """
    return get_client().post("/artifacts/put-dir",
                             {"source_path": source_path, "name": name, "idempotency_key": idempotency_key})


@mcp.tool()
def artifact_resolve(name: str, alias: str | None = None, version_id: str | None = None) -> dict[str, Any]:
    """Resolve a root (latest), alias pin, or explicit version to one immutable version."""
    return get_client().get("/artifacts/resolve", {"name": name, "alias": alias, "version_id": version_id})


@mcp.tool()
def artifact_info(version_id: str) -> dict[str, Any]:
    """Manifest plus blob existence and verification flag for one artifact version."""
    return get_client().get(f"/artifacts/info/{version_id}")


@mcp.tool()
def artifact_materialize(version_id: str, dest_path: str, overwrite: bool = False) -> dict[str, Any]:
    """Write an artifact version's content to dest_path atomically (existing destinations fail unless overwrite)."""
    return get_client().post("/artifacts/materialize",
                             {"version_id": version_id, "dest_path": dest_path, "overwrite": overwrite})


@mcp.tool()
def artifact_verify(version_id: str) -> dict[str, Any]:
    """Re-hash the stored content of an artifact version and report ok/expected/actual."""
    return get_client().post("/artifacts/verify", {"version_id": version_id})


@mcp.tool()
def artifact_collection_create(name: str, idempotency_key: str | None = None) -> dict[str, Any]:
    """Create a named artifact collection for monotonic immutable version lists."""
    return get_client().post("/artifacts/collections", {"name": name, "idempotency_key": idempotency_key})


@mcp.tool()
def artifact_collection_append(collection: str, version_id: str, idempotency_key: str | None = None) -> dict[str, Any]:
    """Append an immutable version to a collection with a monotonic ordinal (duplicate append is a no-op)."""
    return get_client().post("/artifacts/collections/append",
                             {"collection": collection, "version_id": version_id, "idempotency_key": idempotency_key})


@mcp.tool()
def artifact_collection_get(name: str) -> dict[str, Any]:
    """Get a collection's ordered versions (by monotonic ordinal)."""
    return get_client().get(f"/artifacts/collections/{name}")


@mcp.tool()
def artifact_alias_set(alias_name: str, root_id: str, new_version_id: str,
                       expected_version_id: str | None = None, updated_by: str | None = None,
                       idempotency_key: str | None = None) -> dict[str, Any]:
    """Compare-and-swap an alias pin: moves only if it currently points at expected_version_id.

    Pass expected_version_id=None to create a new alias; any mismatch fails
    with ALIAS_CAS_MISMATCH and never silently moves the alias.
    """
    return get_client().post("/artifacts/alias-set",
                             {"alias_name": alias_name, "root_id": root_id, "new_version_id": new_version_id,
                              "expected_version_id": expected_version_id, "updated_by": updated_by,
                              "idempotency_key": idempotency_key})


@mcp.tool()
def artifact_link_lineage(producer_kind: str, producer_id: str, consumer_kind: str, consumer_id: str,
                          version_id: str, idempotency_key: str | None = None) -> dict[str, Any]:
    """Link a producer/consumer identity ('job'|'remote_job'|'version'|'alias') to one immutable version."""
    return get_client().post("/artifacts/lineage",
                             {"producer_kind": producer_kind, "producer_id": producer_id,
                              "consumer_kind": consumer_kind, "consumer_id": consumer_id,
                              "version_id": version_id, "idempotency_key": idempotency_key})


@mcp.tool()
def artifact_lineage_for(version_id: str) -> dict[str, Any]:
    """List all lineage links recorded against one immutable version."""
    return get_client().get(f"/artifacts/lineage/{version_id}")


@mcp.tool()
def artifact_delete_request(version_id: str, idempotency_key: str | None = None) -> dict[str, Any]:
    """Logically delete an artifact version (content stays until GC reclaims it); rejects aliased versions."""
    return get_client().post("/artifacts/delete-request",
                             {"version_id": version_id, "idempotency_key": idempotency_key})


@mcp.tool()
def artifact_restore(version_id: str, idempotency_key: str | None = None) -> dict[str, Any]:
    """Clear a pending delete request on an artifact version."""
    return get_client().post("/artifacts/restore-version",
                             {"version_id": version_id, "idempotency_key": idempotency_key})


@mcp.tool()
def artifact_pin(version_id: str, hold_reason: str, idempotency_key: str | None = None) -> dict[str, Any]:
    """Pin/hold an artifact version so GC can never reclaim it."""
    return get_client().post("/artifacts/pin", {"version_id": version_id, "hold_reason": hold_reason,
                                                "idempotency_key": idempotency_key})


@mcp.tool()
def artifact_unpin(version_id: str, idempotency_key: str | None = None) -> dict[str, Any]:
    """Remove a pin/hold from an artifact version."""
    return get_client().post("/artifacts/unpin", {"version_id": version_id,
                                                  "idempotency_key": idempotency_key})


@mcp.tool()
def artifact_gc(dry_run: bool = True, idempotency_key: str | None = None) -> dict[str, Any]:
    """Fenced garbage collection of unreachable versions/blobs; dry_run=True only reports candidates."""
    return get_client().post("/artifacts/gc", {"dry_run": dry_run, "idempotency_key": idempotency_key})


@mcp.tool()
def artifact_backup() -> dict[str, Any]:
    """Take a manual sqlite backup of the artifacts catalog."""
    return get_client().post("/artifacts/backup", {})


@mcp.tool()
def artifact_begin_restore(backup_path: str) -> dict[str, Any]:
    """Restore the artifacts catalog from a backup copy; rotates instance identity and locks mutations until complete-restore."""
    return get_client().post("/artifacts/begin-restore", {"backup_path": backup_path})


@mcp.tool()
def artifact_complete_restore() -> dict[str, Any]:
    """Clear the recovery_required marker after a restore so mutations are allowed again."""
    return get_client().post("/artifacts/complete-restore", {})


@mcp.tool()
def artifact_storage_profile_create(kind: str = "s3", config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Register a storage profile (immutable revisions; creates revision 1)."""
    return get_client().post("/artifacts/storage-profiles", {"kind": kind, "config": config})


@mcp.tool()
def artifact_storage_profile_get(profile_id: str) -> dict[str, Any]:
    """Get the latest revision of a storage profile (config + capabilities)."""
    return get_client().get(f"/artifacts/storage-profiles/{profile_id}")


@mcp.tool()
def artifact_storage_profile_probe(profile_id: str) -> dict[str, Any]:
    """Probe a storage profile's endpoint capabilities and store them on the latest revision."""
    return get_client().post(f"/artifacts/storage-profiles/{profile_id}/probe", {})


@mcp.tool()
def artifact_storage_profile_update(profile_id: str, config: dict[str, Any],
                                    idempotency_key: str | None = None) -> dict[str, Any]:
    """Insert the NEXT immutable revision of a storage profile; old revisions stay queryable."""
    return get_client().post(f"/artifacts/storage-profiles/{profile_id}/update",
                             {"config": config, "idempotency_key": idempotency_key})


@mcp.tool()
def artifact_push_remote(remote_id: str, version_id: str, idempotency_key: str | None = None) -> dict[str, Any]:
    """Publish a local managed artifact version to a paired remote (chunked, resumable; no credentials cross the wire)."""
    return get_client().post("/artifacts/push-remote",
                             {"remote_id": remote_id, "version_id": version_id,
                              "idempotency_key": idempotency_key})


@mcp.tool()
def artifact_pull_remote(remote_id: str, version_id: str, dest_path: str,
                         idempotency_key: str | None = None) -> dict[str, Any]:
    """Materialize a remote artifact version onto this machine via the controller broker (chunked, resumable)."""
    return get_client().post("/artifacts/pull-remote",
                             {"remote_id": remote_id, "version_id": version_id,
                              "dest_path": dest_path, "idempotency_key": idempotency_key})


def _build_wake_target(
    target: dict[str, Any] | None,
    events: list[str] | None,
    target_type: str | None,
    config: dict[str, Any],
) -> dict[str, Any]:
    """Resolve the daemon_wake shorthand into a full wake-target dict.

    When ``target`` is given it is returned unchanged (backward compatible).
    Otherwise a target is built from ``type`` / ``events`` / ``config``.
    ``type`` is required and must be a supported wake target type
    (local_command, codex_cli_thread, codex_thread, codex_desktop,
    opencode_thread, webhook);
    events default to ["completed", "failed"].
    """
    if target is not None:
        if not isinstance(target, dict):
            raise ValueError("target must be a dict")
        return target
    if target_type is None or not isinstance(target_type, str) or not target_type:
        raise ValueError("type is required when target is not provided")
    if target_type not in WAKE_TARGET_TYPES:
        raise ValueError(f"unsupported wake target type: {target_type!r}")
    if events is not None:
        if not isinstance(events, list) or not events or not all(isinstance(event, str) for event in events):
            raise ValueError("events must be a non-empty list of strings")
    return {"type": target_type, "events": events or ["completed", "failed"], **config}


# --- Python-compatibility API (review rc37 P1 / rc38 P1) ---
# The OLD public Python names ``daemon_wake(job_id, target=None, events=None,
# type=None, **config)`` / ``job_wake_now`` / ``job_add_wake_target`` are the
# UNDECORATED functions below, with the original signature: extra target config
# is passed as plain keyword arguments (``command=``, ``url=``, ``session_id=``,
# ...). FastMCP cannot bind ``**config`` (pydantic makes it required), so the
# MCP surface is the explicit-signature adapters below (``mcp_daemon_wake`` /
# ``mcp_job_wake_now`` / ``mcp_job_add_wake_target``) which accept an explicit
# ``config`` dict — but they are REGISTERED under the existing external MCP
# names ``daemon_wake`` / ``job_wake_now`` / ``job_add_wake_target`` (via
# FastMCP's ``name=`` argument) so agents keep calling the documented/original
# names (review rc38 P1). Importing ``daemon_wake`` from ``vanth.server`` and
# calling it with the original positional/keyword forms keeps working.


def daemon_wake(
    job_id: str,
    target: dict[str, Any] | None = None,
    events: list[str] | None = None,
    type: str | None = None,
    **config: Any,
) -> dict[str, Any]:
    """DEPRECATED: register a wake target. Kept for backward compatibility.

    Original signature: extra target config passed as plain keyword arguments
    (``command=``, ``url=``, ``session_id=``, ``thread_id=``, ...). ``type`` is
    required when ``target`` is not given. This registers a target for FUTURE
    events only (matching the original semantics). Use ``job_wake_now`` to
    surface a wake immediately, or ``job_add_wake_target`` to register a target.
    Caller-task inheritance resolves ``CODEX_THREAD_ID`` /
    ``VANTH_CODEX_DESKTOP_THREAD`` for ``codex_*`` targets without an explicit
    thread id.
    """
    origin_thread_id = _mcp_origin_thread_id()
    resolved = _build_wake_target(target, events, type, config)
    if origin_thread_id:
        resolved = resolve_wake_target_identity([resolved], origin_thread_id)[0]
    return get_client().post(f"/jobs/{job_id}/wake", {"target": resolved})


def job_wake_now(
    job_id: str,
    target: dict[str, Any] | None = None,
    events: list[str] | None = None,
    type: str | None = None,
    **config: Any,
) -> dict[str, Any]:
    """Surface a wake for a job IMMEDIATELY, even if the event already fired.

    Original signature: extra target config passed as plain keyword arguments.
    ``opencode_thread`` targets require an explicit ``session_id`` — the OpenCode
    ``ses_...`` id (from ``opencode session list``, or the destination ``vanth
    doctor`` prints), NOT the relay client id ``opencode-<pid>-<rand>``; ``attach``
    is optional. Caller-task inheritance resolves ``CODEX_THREAD_ID`` for
    ``codex_cli_thread``/``codex_thread``/``codex_desktop`` targets without an
    explicit thread id.
    """
    origin_thread_id = _mcp_origin_thread_id()
    resolved = _build_wake_target(target, events, type, config)
    if origin_thread_id:
        resolved = resolve_wake_target_identity([resolved], origin_thread_id)[0]
    return get_client().post(f"/jobs/{job_id}/wake-now", {"target": resolved})


def job_add_wake_target(
    job_id: str,
    target: dict[str, Any] | None = None,
    events: list[str] | None = None,
    type: str | None = None,
    **config: Any,
) -> dict[str, Any]:
    """Register a wake target against a job for FUTURE events.

    Original signature: extra target config passed as plain keyword arguments.
    ``opencode_thread`` targets need ``session_id`` — the OpenCode ``ses_...`` id
    (from ``opencode session list``, or the destination ``vanth doctor`` prints),
    NOT the relay client id ``opencode-<pid>-<rand>``; omit it to resolve the live
    plugin relay for the job's directory. ``attach`` is optional. Caller-task
    inheritance resolves ``CODEX_THREAD_ID``/``VANTH_CODEX_DESKTOP_THREAD`` for
    ``codex_cli_thread``/``codex_thread``/``codex_desktop`` targets without an
    explicit thread id.
    """
    origin_thread_id = _mcp_origin_thread_id()
    resolved = _build_wake_target(target, events, type, config)
    if origin_thread_id:
        resolved = resolve_wake_target_identity([resolved], origin_thread_id)[0]
    return get_client().post(f"/jobs/{job_id}/wake", {"target": resolved})


def _mcp_wake_payload(target, events, type, config) -> dict[str, Any]:
    return _build_wake_target(target, events, type, config or {})


def _mcp_origin_thread_id() -> str | None:
    """Caller task identity, resolved in the MCP process that owns the task.

    OpenCode injects no session id, so only the Codex identities are inherited
    (``resolve_wake_target_identity`` applies them to codex_* targets only).
    """
    return os.environ.get("CODEX_THREAD_ID") or os.environ.get("VANTH_CODEX_DESKTOP_THREAD")


@mcp.tool(name="job_add_wake_target")
def mcp_job_add_wake_target(
    job_id: str,
    target: dict[str, Any] | None = None,
    events: list[str] | None = None,
    type: str | None = None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Register a wake target against a job for FUTURE events.

    Pass a full target dict ({"type", "events", ...config}) as ``target``, or
    use the shorthand: ``type`` (required, one of local_command /
    codex_cli_thread / codex_thread / codex_desktop / opencode_thread / webhook)
    plus optional ``events`` and ``config`` (extra target config). Events default
    to ["completed", "failed"].

    This only schedules a target for events that will occur AFTER registration.
    To surface a wake immediately (even if the event already fired), use
    ``job_wake_now``. ``opencode_thread`` targets need ``session_id`` — the
    OpenCode ``ses_...`` id (from ``opencode session list``, or the destination
    ``vanth doctor`` prints), NOT the relay client id ``opencode-<pid>-<rand>``;
    omit it to resolve the live plugin relay for the job's directory. ``attach``
    is optional (only for a headless ``opencode serve``).
    ``codex_desktop`` targets wake a RUNNING Codex Desktop task through its
    native app-tools host pipe (requires the Desktop integration).

    Caller-task inheritance is resolved HERE, in the MCP process that owns the
    calling task: a ``codex_cli_thread``/``codex_thread``/``codex_desktop``
    target without an explicit ``thread_id`` inherits the calling Codex task
    identity (``CODEX_THREAD_ID`` / ``VANTH_CODEX_DESKTOP_THREAD``).

    Registered under the external MCP name ``job_add_wake_target`` (the rc37
    contract); ``mcp_`` is only an implementation-prefix for the Python callable
    (review rc38 P1 — clients must not learn prefix names).
    """
    origin_thread_id = _mcp_origin_thread_id()
    resolved = _mcp_wake_payload(target, events, type, config)
    if origin_thread_id:
        resolved = resolve_wake_target_identity([resolved], origin_thread_id)[0]
    return get_client().post(f"/jobs/{job_id}/wake", {"target": resolved})


@mcp.tool(name="job_wake_now")
def mcp_job_wake_now(
    job_id: str,
    target: dict[str, Any] | None = None,
    events: list[str] | None = None,
    type: str | None = None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Surface a wake for a job IMMEDIATELY, even if the event already fired.

    This is the genuine "wake now" operation (review P0-1): it registers the
    target AND enqueues a synthetic delivery right away, so the wake reaches the
    target session without waiting for a matching event. Use ``target`` as a
    full dict, or the shorthand: ``type`` (required) + ``events`` + ``config``
    (extra target config kwargs). ``opencode_thread`` targets need ``session_id``
    — the OpenCode ``ses_...`` id (from ``opencode session list``, or the
    destination ``vanth doctor`` prints), NOT the relay client id
    ``opencode-<pid>-<rand>``; omit it to resolve the live plugin relay for the
    job's directory. ``attach`` is optional (only for a headless ``opencode
    serve``).

    Caller-task inheritance is resolved HERE, in the MCP process that owns the
    calling task (review P0-2): a ``codex_cli_thread``/``codex_thread``/
    ``codex_desktop`` target without an explicit ``thread_id`` inherits the
    calling Codex task identity so the wake resumes the calling task.

    Registered under the external MCP name ``job_wake_now`` (the rc37
    contract); ``mcp_`` is only an implementation-prefix for the Python callable
    (review rc38 P1 — clients must not learn prefix names).
    """
    origin_thread_id = _mcp_origin_thread_id()
    resolved = _mcp_wake_payload(target, events, type, config)
    if origin_thread_id:
        resolved = resolve_wake_target_identity([resolved], origin_thread_id)[0]
    return get_client().post(f"/jobs/{job_id}/wake-now", {"target": resolved})


@mcp.tool(name="daemon_wake")
def mcp_daemon_wake(
    job_id: str,
    target: dict[str, Any] | None = None,
    events: list[str] | None = None,
    type: str | None = None,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """DEPRECATED: register a wake target. Kept for backward compatibility.

    Use ``job_add_wake_target`` to register a target for future events, or
    ``job_wake_now`` to surface a wake immediately. This alias registers the
    target only (matching the original semantics). ``config`` holds extra
    target config kwargs. Supported types include local_command /
    codex_cli_thread / codex_thread / codex_desktop / opencode_thread / webhook.

    Registered under the external MCP name ``daemon_wake`` (the rc37 contract);
    ``mcp_`` is only an implementation-prefix for the Python callable (review
    rc38 P1 — clients must not learn prefix names).
    """
    origin_thread_id = _mcp_origin_thread_id()
    resolved = _mcp_wake_payload(target, events, type, config)
    if origin_thread_id:
        resolved = resolve_wake_target_identity([resolved], origin_thread_id)[0]
    return get_client().post(f"/jobs/{job_id}/wake", {"target": resolved})


@mcp.tool()
def job_cleanup_preview(older_than_seconds: int) -> dict[str, Any]:
    """Dry-run preview of what job_cleanup would remove, without deleting anything."""
    return get_client().get("/cleanup/preview", {"older_than_seconds": older_than_seconds})


# Global flags that may precede the subcommand (`vanth --json list`); they do not
# name a command, so the dispatch gate must look past them.
_VANTH_CLI_GLOBAL_FLAGS = {"--json"}
_VANTH_CLI_SUBCOMMANDS = {
    "status", "doctor", "restart", "setup", "--help", "-h", "help",
    "start", "list", "ps", "logs", "tail", "stop", "rerun", "send", "sleep", "deliveries", "api",
    "artifacts", "prune", "backup", "restore", "wait", "diff", "wake",
    "autostart", "--version", "version", "remote",
}

_VANTH_SCRIPT_NAMES = {"vanth", "vanth.exe", "vanth-script.py", "vanth-script.pyw"}

# Interpreter options the MCP launch shape may carry. Valueless options are
# skipped; value-taking options consume the next token; ANYTHING else that
# starts with "-" (unknown option, long option, or an attached ``-c<payload>``)
# refuses the match — the reaper kills what it accepts, so ambiguity loses.
_PY_VALUELESS_OPTIONS = {
    "-O", "-OO", "-B", "-b", "-d", "-E", "-I", "-q", "-R", "-s", "-S", "-u", "-v",
}
_PY_VALUED_OPTIONS = {"-X", "-W"}


def _is_vanth_mcp_command(command_line: str) -> bool:
    """Whether a command line is a Vanth MCP stdio server (not a CLI command).

    A false positive here is not cosmetic: the orphan reaper terminates every
    reported process, so the matcher accepts only the exact supported launch
    shapes and rejects anything ambiguous:

    - ``<python> -m vanth.server`` / ``-m vanth.mcp`` (``-m`` must be the
      interpreter's launching argument; ``python unrelated.py -m vanth.server``,
      ``bash -lc 'python -m vanth.server'`` and ``python -c<payload>`` are
      rejected), and
    - the ``vanth`` console script (``vanth`` / ``python .../vanth``) run with
      no CLI subcommand — ``vanth logs --follow`` is the CLI, not MCP.

    A quoted ``argv0`` (Windows ``"C:\\Program Files\\Python\\python.exe" ...``)
    is split off before tokenizing; arguments after it are whitespace-split, so
    a quoting trick later in the line can only cause a REJECTION, never a false
    match. Missing a genuine exotic launch is the safe failure mode.
    """
    if command_line.startswith('"'):
        end = command_line.find('"', 1)
        if end == -1:
            return False
        tokens = [command_line[1:end]] + command_line[end + 1:].split()
    else:
        tokens = command_line.split()
    if not tokens:
        return False

    def _base(token: str) -> str:
        token = token.strip("\"'")
        return token.replace("\\", "/").rstrip("/").rsplit("/", 1)[-1].lower()

    def _is_cli_script(script_index: int) -> bool:
        rest = tokens[script_index + 1:]
        # Quoted subcommand (``vanth "logs"``) is still a CLI invocation.
        return bool(rest) and rest[0].strip("\"'") in _VANTH_CLI_SUBCOMMANDS

    if _base(tokens[0]).startswith("python"):
        # Walk the interpreter's own options to the launching argument:
        # ``-m module`` (the supported MCP shape), ``-c`` (never us), or the
        # first non-option token (a script path — matched only when it is the
        # ``vanth`` console script with no CLI subcommand).
        i = 1
        while i < len(tokens):
            tok = tokens[i]
            if tok == "-m":
                return i + 1 < len(tokens) and tokens[i + 1] in {"vanth.server", "vanth.mcp"}
            if tok == "-c" or tok.startswith("-c"):
                return False
            if tok in _PY_VALUED_OPTIONS:
                i += 2
                continue
            if tok in _PY_VALUELESS_OPTIONS:
                i += 1
                continue
            if tok.startswith("-"):
                return False
            if _base(tok) in _VANTH_SCRIPT_NAMES:
                return not _is_cli_script(i)
            return False
        return False
    if _base(tokens[0]) in _VANTH_SCRIPT_NAMES:
        return not _is_cli_script(0)
    return False


def _orphaned_mcp_servers() -> list[dict[str, Any]]:
    """Find MCP stdio server processes whose launching client is gone.

    Scans for ``vanth``/python processes running the Vanth MCP entrypoint and
    checks whether their parent process is still alive. A ``vanth`` process
    launched by a dead client is an orphan that the new watchdog is designed to
    prevent, but which older versions (and force-killed sessions) may still
    have left behind. Returns a list of ``{pid, started, parent_pid}`` entries.
    """
    from .process_watch import process_alive

    import subprocess as _sp

    candidates = []
    try:
        if sys.platform == "win32":
            # Get-CimInstance provides the COMMAND LINE (WMIC CSV does not, and
            # its columns are ordered alphabetically, so the old positional parse
            # both misread the fields and could not establish MCP identity).
            # JSON output avoids comma-splitting a command line containing commas.
            result = _sp.run(
                [
                    "powershell", "-NoProfile", "-NonInteractive", "-Command",
                    "Get-CimInstance Win32_Process | "
                    "Select-Object ProcessId,ParentProcessId,CreationDate,CommandLine | "
                    "ConvertTo-Json -Compress",
                ],
                stdout=_sp.PIPE, stderr=_sp.DEVNULL, text=True, timeout=15,
            )
            raw = (result.stdout or "").strip()
            records = json.loads(raw) if raw else []
            if isinstance(records, dict):
                records = [records]
            for rec in records:
                cmdline = rec.get("CommandLine") or ""
                if not _is_vanth_mcp_command(cmdline):
                    continue
                pid = rec.get("ProcessId")
                if isinstance(pid, bool) or not isinstance(pid, int):
                    continue
                ppid = rec.get("ParentProcessId")
                candidates.append(
                    {
                        "pid": pid,
                        "name": cmdline,
                        "started": str(rec.get("CreationDate") or ""),
                        "ppid": ppid if isinstance(ppid, int) and not isinstance(ppid, bool) else None,
                    }
                )
        else:
            result = _sp.run(["ps", "-eo", "pid=,ppid=,etime=,args="],
                             stdout=_sp.PIPE, stderr=_sp.DEVNULL, text=True, timeout=10)
            for line in result.stdout.splitlines():
                parts = line.split(None, 3)
                if len(parts) < 4:
                    continue
                pid, ppid, etime, args = parts
                if not _is_vanth_mcp_command(args):
                    continue
                candidates.append(
                    {
                        "pid": int(pid),
                        "name": args,
                        "started": etime,
                        "ppid": int(ppid) if ppid.isdigit() else None,
                    }
                )
    except Exception:
        return []
    orphans = []
    for entry in candidates:
        if entry["ppid"] in (0, 1, None):
            continue
        if process_alive(entry["ppid"]):
            continue
        orphans.append(
            {
                "pid": entry["pid"],
                "started": entry["started"],
                "parent_pid": entry["ppid"],
            }
        )
    return orphans


def _hint_setup() -> None:
    """Print a stderr hint on MCP startup when a known client still lacks the
    Vanth MCP entry. stdout is the JSON-RPC protocol, so hints go to stderr
    (harmless to the transport). Suppress with VANTH_NO_SETUP_HINT=1."""
    if os.environ.get("VANTH_NO_SETUP_HINT") in {"1", "true", "yes"}:
        return
    try:
        from .setup import client_config_paths, effective_state

        found = client_config_paths()
        missing = []
        for client, paths in found.items():
            # Only a definite "not configured" is worth hinting about: a
            # `disabled` or `unreadable` (commented JSONC) file must not
            # produce a false "run vanth setup" hint.
            if effective_state(client, paths) == "not-configured":
                missing.append(client)
        if missing:
            print(
                "vanth: MCP server not configured in " + ", ".join(missing)
                + " — run `vanth setup` to register (or set VANTH_NO_SETUP_HINT=1)",
                file=sys.stderr,
            )
    except Exception:
        pass


def main(argv: list[str] | None = None) -> None:
    from .cli import main as cli_main

    args = list(sys.argv[1:] if argv is None else argv)
    # Human-facing subcommands are dispatched to the CLI; anything else
    # (including no args) runs the MCP stdio server, which is what MCP
    # clients expect from `vanth` (bare). Leading global flags are skipped so
    # `vanth --json list` (the documented global form) routes here too: checking
    # only args[0] sent it to the MCP stdio server — a hang for an agent, and
    # "unknown command '--json'" in a terminal.
    first_non_flag = next((arg for arg in args if arg not in _VANTH_CLI_GLOBAL_FLAGS), None)
    if args and (first_non_flag in _VANTH_CLI_SUBCOMMANDS or first_non_flag is None):
        raise SystemExit(cli_main(args))
    # Interactive misuse guard (user report): bare `vanth` typed in a real
    # terminal would otherwise start the MCP stdio server and appear to
    # "hang" reading JSON-RPC from the keyboard. Real MCP clients always
    # run us with pipes, never a TTY on stdin.
    interactive = sys.stdin.isatty()
    if interactive and not args:
        print(
            "vanth: refusing to start the MCP stdio server in an interactive "
            "terminal.\n"
            "  - Terminal dashboard:            vanth-monitor\n"
            "  - Human subcommands:             vanth doctor | status | setup\n"
            "  - MCP clients launch `vanth` with pipes automatically.",
            file=sys.stderr,
        )
        raise SystemExit(2)
    if interactive:
        # An unknown human command (typo like `vanth statsu`) or a bare
        # invocation with redirected stdout must not silently hang inside the
        # MCP read loop. Route unknown interactive invocations to the CLI,
        # which prints usage/errors and exits (review P2-2).
        print(
            f"vanth: unknown command {args[0]!r}",
            file=sys.stderr,
        )
        raise SystemExit(2)
    _hint_setup()
    _run_mcp_server()


def _run_mcp_server() -> None:
    """Run the MCP stdio server under a parent-liveness + idle watchdog.

    Agent clients (codex/opencode) launch ``vanth`` as a stdio MCP server. If a
    session dies without closing stdin, or the client accumulates cached
    workers, those processes would otherwise linger forever holding
    ``vanth.exe``. The watchdog (see ``vanth.process_watch``) self-terminates
    the process when the parent dies or the process is idle, while never
    killing a blocking tool call (``job_wait``/``job_tail --follow``) mid-flight.
    """
    from .process_watch import start_watchdog

    thread, tracker = start_watchdog()

    # Review rc36 P0: start the client-side Desktop wake relay (a separate
    # localhost subscription to the daemon, never MCP stdio). It only runs when
    # this MCP process inherited both CODEX_THREAD_ID and
    # CODEX_APP_TOOLS_PIPE_PATH, i.e. when the visible Codex Desktop task can be
    # woken through its native app-tools host pipe. It is stopped with the MCP
    # process.
    desktop_relay = None
    try:
        from .relay import start_desktop_relay

        desktop_relay = start_desktop_relay(activity_tracker=tracker)
    except Exception:
        logging.getLogger("vanth").exception("failed to start Desktop wake relay")

    # Bump the in-flight counter around every tool call so a long-running
    # blocking call (job_wait with a filter) is never idle-reaped mid-call.
    original_call_tool = mcp.call_tool

    async def guarded_call_tool(name: str, arguments: dict[str, object]) -> object:
        with tracker:
            return await original_call_tool(name, arguments)

    mcp.call_tool = guarded_call_tool  # type: ignore[method-assign]

    try:
        mcp.run()
    finally:
        if desktop_relay is not None:
            desktop_relay.stop()
        if thread is not None and thread.is_alive():
            # The stdio loop ended (client closed stdin or sent exit): no need
            # for the watchdog anymore; it would only fire a redundant exit.
            pass
