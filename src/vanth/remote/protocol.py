"""Vanth remote protocol codec, canonical JSON, and validation (Phase 0).

Implements the v1 remote protocol: newline-delimited JSON frames over the
SSH forced-command helper's stdin/stdout, RFC 8785 canonical JSON, request
digests, an error registry, and deterministic rejection of malformed /
oversized / duplicate-key / unknown-kind / unknown-field frames.
"""

from __future__ import annotations

import json
import re
import hashlib
from typing import Any


FRAME_KINDS = ("hello", "request", "response", "error", "snapshot", "log_range")

DEFAULT_MAX_FRAME_BYTES = 8 * 1024 * 1024

VALID_REQUEST_METHODS = (
    "job.start", "job.stop", "job.rerun", "job.status", "job.snapshot", "job.log_range", "job.feed", "job.events",
    "artifact.transfer_init", "artifact.blob_chunk", "artifact.transfer_complete",
)

TRANSFER_DIRECTIONS = {"pull", "push"}

TRANSFER_INIT_ALLOWED = {
    "transfer_id", "direction", "root_name", "manifest_digest", "total_bytes",
    "sha256", "version_id",
}
BLOB_CHUNK_ALLOWED = {"transfer_id", "offset", "data_b64", "sha256", "size"}
TRANSFER_COMPLETE_ALLOWED = {
    "transfer_id", "root_name", "manifest_digest", "total_bytes", "sha256", "version_id",
}

IDEMPOTENCY_KEY_RE = re.compile(r"^[A-Za-z0-9_-]{8,128}$")

DIGEST_FIELDS = ("method", "payload", "idempotency_key")


class VanthRemoteProtocolError(Exception):
    """Protocol-level failure surfaced with a stable registry code.

    ``code`` is always a key of :data:`ERROR_REGISTRY`. ``message`` defaults
    to the registry message for the code and may carry details.
    """

    def __init__(self, code: str, message: str | None = None) -> None:
        if code not in ERROR_REGISTRY:
            raise ValueError(f"unknown error code: {code!r}")
        super().__init__(message or ERROR_REGISTRY[code][1])
        self.code = code


class _DuplicateKeyError(Exception):
    pass


def _no_duplicates_hook(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateKeyError(key)
        result[key] = value
    return result


# ---------------------------------------------------------------------------
# Error registry
# ---------------------------------------------------------------------------

ERROR_REGISTRY: dict[str, tuple[int, str]] = {
    "PROTOCOL_MALFORMED": (400, "frame is not valid JSON"),
    "PROTOCOL_OVERSIZED": (413, "frame exceeds the maximum allowed size"),
    "PROTOCOL_UNKNOWN_KIND": (400, "frame has an unknown kind"),
    "PROTOCOL_DUPLICATE_KEY": (400, "object contains a duplicate key"),
    "PROTOCOL_UNKNOWN_FIELD": (400, "object contains an unknown field"),
    "PROTOCOL_REPLAY_MISMATCH": (409, "idempotency key was reused with a different request"),
    "OPERATION_RETRY_PENDING": (503, "outcome not yet terminal; retry with the same idempotency key"),
    "STATE_EPOCH_MISMATCH": (409, "request was bound to a stale remote state epoch"),
    "INSTANCE_ID_MISMATCH": (409, "request was bound to a different remote instance"),
    "SNAPSHOT_EXPIRED": (409, "snapshot expired; restart sync"),
    "UNSUPPORTED_FEATURE": (501, "requested feature is not supported"),
    "AUTH_FAILED": (401, "authentication failed"),
    "INVALID_REQUEST": (422, "request payload is invalid"),
}


# ---------------------------------------------------------------------------
# Canonical JSON (RFC 8785)
# ---------------------------------------------------------------------------


def _number_to_string(value: float) -> str:
    """Render a float in shortest round-trip ES6-compatible form.

    CPython's ``repr(float)`` implements the same shortest-round-trip digit
    selection as ECMAScript's Number::toString, so the digit string from
    ``repr`` is authoritative; we only normalize the formatting differences.
    ES6 switches between fixed and exponential notation based on the decimal
    exponent of the shortest form:
      - ``-6 <= exp < 21``: fixed decimal notation (e.g. ``1000``, ``1e-5``
        -> ``0.00001``, ``1e16`` -> ``10000000000000000``).
      - otherwise: ``<mantissa>E<sign><exp>`` (e.g. ``1.5e-8`` -> ``1.5E-8``,
        ``1e21`` -> ``1E+21``).
    """
    if value == 0:
        return "0"
    text = repr(value)
    if "e" not in text and "E" not in text:
        return text.rstrip("0").rstrip(".") or "0"
    mantissa, exponent_text = text.split("e", 1)
    exponent = int(exponent_text)
    sign = "+" if exponent >= 0 else "-"
    if not (-6 <= exponent < 21):
        return f"{mantissa}E{sign}{abs(exponent)}"
    negative = mantissa.startswith("-")
    mantissa = mantissa.lstrip("-")
    if "." in mantissa:
        integer_part, fractional_part = mantissa.split(".", 1)
    else:
        integer_part, fractional_part = mantissa, ""
    all_digits = integer_part + fractional_part
    point = len(integer_part) + exponent
    if point <= 0:
        fixed = "0." + ("0" * -point) + all_digits
    elif point >= len(all_digits):
        fixed = all_digits + ("0" * (point - len(all_digits)))
    else:
        fixed = all_digits[:point] + "." + all_digits[point:]
        fixed = fixed.rstrip("0").rstrip(".")
    return ("-" if negative else "") + fixed


def _json_number(value: int | float) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise VanthRemoteProtocolError("PROTOCOL_MALFORMED", "canonical JSON rejects NaN and Infinity")
        return _number_to_string(value)
    raise VanthRemoteProtocolError("PROTOCOL_MALFORMED", f"canonical JSON cannot serialize {type(value).__name__}")


def _escape_string(value: str) -> str:
    """Minimal JSON string escaping (RFC 8785 §3.2.2.2).

    Only ``"``, ``\\``, and control characters < 0x20 are escaped; control
    characters use lowercase ``\\uXXXX`` escapes.
    """
    chars: list[str] = []
    for char in value:
        code = ord(char)
        if char == '"':
            chars.append('\\"')
        elif char == "\\":
            chars.append("\\\\")
        elif code < 0x20:
            chars.append(f"\\u{code:04x}")
        else:
            chars.append(char)
    return '"' + "".join(chars) + '"'


def _serialize(obj: Any) -> str:
    if obj is None:
        return "null"
    if isinstance(obj, bool):
        return "true" if obj else "false"
    if isinstance(obj, (int, float)):
        return _json_number(obj)
    if isinstance(obj, str):
        return _escape_string(obj)
    if isinstance(obj, (list, tuple)):
        return "[" + ",".join(_serialize(item) for item in obj) + "]"
    if isinstance(obj, dict):
        if not all(isinstance(key, str) for key in obj):
            raise VanthRemoteProtocolError(
                "PROTOCOL_MALFORMED", "canonical JSON object keys must be strings"
            )
        # RFC 8785 sorts keys by UTF-16 code units, not code points. Encoding
        # to big-endian UTF-16 makes the byte sort match that order (including
        # astral characters, whose surrogate units sort before U+E000..U+FFFF).
        result = []
        for key in sorted(obj.keys(), key=lambda k: k.encode("utf-16-be", "surrogatepass")):
            result.append(_escape_string(key) + ":" + _serialize(obj[key]))
        return "{" + ",".join(result) + "}"
    raise VanthRemoteProtocolError("PROTOCOL_MALFORMED", f"canonical JSON cannot serialize {type(obj).__name__}")


def canonical_json(obj: Any) -> str:
    """RFC 8785 canonical serializer: sorted keys, minimal escapes, ES6 numbers."""
    if isinstance(obj, dict):
        _check_duplicate_keys(obj)
    return _serialize(obj)


# ---------------------------------------------------------------------------
# Frame encoding / decoding
# ---------------------------------------------------------------------------

def encode_frame(obj: Any, *, max_bytes: int | None = None) -> bytes:
    """Serialize ``obj`` as one compact JSON frame terminated by ``\\n``.

    Raises ``VanthRemoteProtocolError(PROTOCOL_OVERSIZED)`` when the encoded
    frame exceeds ``max_bytes`` (default 8 MiB).
    """
    limit = DEFAULT_MAX_FRAME_BYTES if max_bytes is None else max_bytes
    if limit <= 0:
        raise ValueError("max_bytes must be a positive integer")
    text = json.dumps(obj, separators=(",", ":"))
    if len(text) > limit:
        raise VanthRemoteProtocolError("PROTOCOL_OVERSIZED")
    return (text + "\n").encode("utf-8")


def decode_frame(line: str, *, max_bytes: int | None = None, validate: bool = True) -> dict[str, Any]:
    """Decode and (by default) validate one newline-delimited JSON frame.

    Deterministically rejects malformed JSON, non-object frames, frames
    larger than ``max_bytes`` (default 8 MiB), duplicate keys, unknown
    ``kind`` values, and unknown per-kind fields. Each failure raises
    :class:`VanthRemoteProtocolError` with a stable registry code.
    """
    limit = DEFAULT_MAX_FRAME_BYTES if max_bytes is None else max_bytes
    if limit <= 0:
        raise ValueError("max_bytes must be a positive integer")
    if len(line) > limit + 1:
        raise VanthRemoteProtocolError("PROTOCOL_OVERSIZED")
    try:
        obj = json.loads(line, object_pairs_hook=_no_duplicates_hook)
    except _DuplicateKeyError:
        raise VanthRemoteProtocolError("PROTOCOL_DUPLICATE_KEY") from None
    except json.JSONDecodeError as exc:
        raise VanthRemoteProtocolError("PROTOCOL_MALFORMED", str(exc)) from None
    if not isinstance(obj, dict):
        raise VanthRemoteProtocolError("PROTOCOL_MALFORMED", "frame must be a JSON object")
    encoded = json.dumps(obj, separators=(",", ":"))
    if len(encoded) > limit:
        raise VanthRemoteProtocolError("PROTOCOL_OVERSIZED")
    if validate:
        return validate_frame(obj)
    return obj


# ---------------------------------------------------------------------------
# Frame validation
# ---------------------------------------------------------------------------

FRAME_SCHEMAS: dict[str, tuple[set[str], set[str]]] = {
    "hello": ({"version", "kind", "protocol", "agent", "remote_id", "state_epoch", "instance_id", "sent_at"},
              {"version", "kind", "protocol"}),
    "request": ({"version", "kind", "request_id", "idempotency_key", "method", "payload", "digest",
                  "expected_state_epoch", "expected_instance_id", "sent_at"},
              {"version", "kind", "method", "payload", "idempotency_key"}),
    "response": ({"version", "kind", "request_id", "method", "result", "sent_at"},
                 {"version", "kind", "method"}),
    "error": ({"version", "kind", "request_id", "method", "code", "message", "sent_at"},
              {"version", "kind", "code", "message"}),
    "snapshot": ({"version", "kind", "state_epoch", "cursor", "jobs", "events", "has_more",
                  "feed_epoch", "feed_boundary_seq", "sent_at"},
                 {"version", "kind", "state_epoch", "cursor", "jobs"}),
    "log_range": ({"version", "kind", "remote_job_id", "stream", "offset", "size", "content", "truncated", "sent_at"},
                  {"version", "kind", "remote_job_id", "stream", "offset", "content"}),
}

_NUMERIC_FIELDS = {"timeout_seconds", "kill_after_seconds"}
_ARRAY_FIELDS = {"notify_on", "wake_targets", "tags"}

START_OPTIONAL_FIELDS = {
    "command", "cwd", "name", "env", "timeout_seconds", "notify_on", "wake_targets",
    "origin_thread_id", "tags", "notes", "interactive", "trigger", "policy",
}
STOP_OPTIONAL_FIELDS = {"signal", "kill_after_seconds"}
RERUN_OPTIONAL_FIELDS = {"command", "env", "timeout_seconds", "name", "tags", "notes", "cwd", "interactive"}
STATUS_ALLOWED = {"job_id"}
SNAPSHOT_ALLOWED = {"cursor"}
LOG_RANGE_REQUIRED = {"remote_job_id"}
LOG_RANGE_ALLOWED = {"remote_job_id", "stream", "offset", "size"}

FEED_DEFAULT_LIMIT = 100
FEED_MAX_LIMIT = 500
FEED_MAX_WAIT_MS = 10000
FEED_ALLOWED = {"cursor", "limit", "wait_ms"}
EVENTS_DEFAULT_LIMIT = 200
EVENTS_MAX_LIMIT = 500
EVENTS_MAX_JOBS = 50
EVENTS_ALLOWED = {"cursors", "limit"}

VALID_LOG_STREAMS = {"stdout", "stderr"}

START_ALLOWED = set(START_OPTIONAL_FIELDS)
STOP_ALLOWED = {"job_id"} | STOP_OPTIONAL_FIELDS
RERUN_ALLOWED = {"job_id"} | RERUN_OPTIONAL_FIELDS

VALID_STOP_SIGNALS = {"terminate", "kill"}


def _check_duplicate_keys(obj: dict[str, Any]) -> None:
    seen: set[str] = set()
    for key in obj:
        if key in seen:
            raise VanthRemoteProtocolError("PROTOCOL_DUPLICATE_KEY", f"duplicate key: {key!r}")
        seen.add(key)


def _check_required_and_unknown(payload: Any, required: set[str], allowed: set[str], field: str) -> None:
    if not isinstance(payload, dict):
        raise VanthRemoteProtocolError("INVALID_REQUEST", f"{field} must be an object")
    missing = sorted(required - set(payload.keys()))
    if missing:
        raise VanthRemoteProtocolError("INVALID_REQUEST", f"{field} missing required fields: {', '.join(missing)}")
    unknown = sorted(set(payload.keys()) - allowed)
    if unknown:
        raise VanthRemoteProtocolError(
            "PROTOCOL_UNKNOWN_FIELD", f"unknown {field} field: {unknown[0]!r}"
        )


def _check_string_field(payload: dict[str, Any], key: str, *, required: bool = False) -> None:
    value = payload.get(key)
    if value is None:
        if required:
            raise VanthRemoteProtocolError("INVALID_REQUEST", f"{key} is required")
        return
    if not isinstance(value, str):
        raise VanthRemoteProtocolError("INVALID_REQUEST", f"{key} must be a string")
    if key == "idempotency_key" and not IDEMPOTENCY_KEY_RE.match(value):
        raise VanthRemoteProtocolError(
            "INVALID_REQUEST", "idempotency_key must be 8..128 chars in [A-Za-z0-9_-]"
        )
    if key == "command" and not value.strip():
        raise VanthRemoteProtocolError("INVALID_REQUEST", "command must be a non-empty string")


def _check_numeric_field(payload: dict[str, Any], key: str, *, minimum: int, required: bool = False) -> None:
    value = payload.get(key)
    if value is None:
        if required:
            raise VanthRemoteProtocolError("INVALID_REQUEST", f"{key} is required")
        return
    if isinstance(value, bool) or not isinstance(value, int):
        raise VanthRemoteProtocolError("INVALID_REQUEST", f"{key} must be an integer")
    if value < minimum:
        raise VanthRemoteProtocolError("INVALID_REQUEST", f"{key} must be >= {minimum}")


def _check_object_field(payload: dict[str, Any], key: str) -> None:
    value = payload.get(key)
    if value is None:
        return
    if not isinstance(value, dict):
        raise VanthRemoteProtocolError("INVALID_REQUEST", f"{key} must be an object")
    for env_key, env_value in value.items():
        if not isinstance(env_key, str) or not isinstance(env_value, str):
            raise VanthRemoteProtocolError(
                "INVALID_REQUEST", f"{key} must contain only string keys and string values"
            )


def _check_string_array_field(payload: dict[str, Any], key: str) -> None:
    value = payload.get(key)
    if value is None:
        return
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise VanthRemoteProtocolError("INVALID_REQUEST", f"{key} must be an array of strings")


def _check_object_array_field(payload: dict[str, Any], key: str) -> None:
    value = payload.get(key)
    if value is None:
        return
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise VanthRemoteProtocolError("INVALID_REQUEST", f"{key} must be an array of objects")


def _check_boolean_field(payload: dict[str, Any], key: str) -> None:
    value = payload.get(key)
    if value is None:
        return
    if not isinstance(value, bool):
        raise VanthRemoteProtocolError("INVALID_REQUEST", f"{key} must be a boolean")


def _check_hex64_field(payload: dict[str, Any], key: str, *, required: bool = False) -> None:
    """A 64-char lowercase hex digest field (sha256 / manifest_digest)."""
    value = payload.get(key)
    if value is None:
        if required:
            raise VanthRemoteProtocolError("INVALID_REQUEST", f"{key} is required")
        return
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise VanthRemoteProtocolError("INVALID_REQUEST", f"{key} must be a 64-char lowercase hex string")


def validate_request(method: str, payload: dict[str, Any]) -> None:
    """Validate an already-decoded request's method and payload."""
    if method not in VALID_REQUEST_METHODS:
        raise VanthRemoteProtocolError("UNSUPPORTED_FEATURE", f"unsupported request method: {method}")
    if method == "job.start":
        _check_required_and_unknown(payload, {"command"}, START_ALLOWED, "payload")
        _check_string_field(payload, "command")
        for field in ("cwd", "name", "notes", "origin_thread_id"):
            _check_string_field(payload, field)
        _check_object_field(payload, "env")
        _check_numeric_field(payload, "timeout_seconds", minimum=1)
        _check_string_array_field(payload, "notify_on")
        _check_object_array_field(payload, "wake_targets")
        _check_string_array_field(payload, "tags")
        _check_boolean_field(payload, "interactive")
        if "trigger" in payload and not isinstance(payload["trigger"], dict):
            raise VanthRemoteProtocolError("INVALID_REQUEST", "trigger must be an object")
        if "policy" in payload and not isinstance(payload["policy"], dict):
            raise VanthRemoteProtocolError("INVALID_REQUEST", "policy must be an object")
    elif method == "job.stop":
        _check_required_and_unknown(payload, {"job_id"}, STOP_ALLOWED, "payload")
        _check_string_field(payload, "job_id", required=True)
        signal = payload.get("signal")
        if signal is not None:
            if not isinstance(signal, str) or signal not in VALID_STOP_SIGNALS:
                raise VanthRemoteProtocolError(
                    "INVALID_REQUEST", "signal must be 'terminate' or 'kill'"
                )
        _check_numeric_field(payload, "kill_after_seconds", minimum=1)
    elif method == "job.rerun":
        _check_required_and_unknown(payload, {"job_id"}, RERUN_ALLOWED, "payload")
        _check_string_field(payload, "job_id", required=True)
        for field in ("command", "name", "notes", "cwd"):
            _check_string_field(payload, field)
        _check_object_field(payload, "env")
        _check_numeric_field(payload, "timeout_seconds", minimum=1)
        _check_string_array_field(payload, "tags")
        _check_boolean_field(payload, "interactive")
    elif method == "job.status":
        _check_required_and_unknown(payload, {"job_id"}, STATUS_ALLOWED, "payload")
        _check_string_field(payload, "job_id", required=True)
    elif method == "job.snapshot":
        _check_required_and_unknown(payload, set(), SNAPSHOT_ALLOWED, "payload")
        cursor = payload.get("cursor")
        if cursor is not None and not isinstance(cursor, dict):
            raise VanthRemoteProtocolError("INVALID_REQUEST", "cursor must be an object")
    elif method == "job.feed":
        _check_required_and_unknown(payload, set(), FEED_ALLOWED, "payload")
        cursor = payload.get("cursor")
        if cursor is not None and not isinstance(cursor, dict):
            raise VanthRemoteProtocolError("INVALID_REQUEST", "cursor must be an object")
        _check_numeric_field(payload, "limit", minimum=1)
        if (payload.get("limit") or FEED_DEFAULT_LIMIT) > FEED_MAX_LIMIT:
            raise VanthRemoteProtocolError("INVALID_REQUEST", f"limit must be <= {FEED_MAX_LIMIT}")
        _check_numeric_field(payload, "wait_ms", minimum=0)
        if (payload.get("wait_ms") or 0) > FEED_MAX_WAIT_MS:
            raise VanthRemoteProtocolError("INVALID_REQUEST", f"wait_ms must be <= {FEED_MAX_WAIT_MS}")
    elif method == "job.events":
        _check_required_and_unknown(payload, {"cursors"}, EVENTS_ALLOWED, "payload")
        cursors = payload["cursors"]
        if not isinstance(cursors, dict) or not cursors:
            raise VanthRemoteProtocolError("INVALID_REQUEST", "cursors must be a non-empty object")
        if len(cursors) > EVENTS_MAX_JOBS:
            raise VanthRemoteProtocolError("INVALID_REQUEST", f"cursors must contain at most {EVENTS_MAX_JOBS} jobs")
        for job_id, since in cursors.items():
            if not isinstance(job_id, str) or not job_id:
                raise VanthRemoteProtocolError("INVALID_REQUEST", "cursor job ids must be non-empty strings")
            if since is not None and (isinstance(since, bool) or not isinstance(since, int) or since < 0):
                raise VanthRemoteProtocolError("INVALID_REQUEST", "event cursors must be null or non-negative integers")
        _check_numeric_field(payload, "limit", minimum=1)
        if (payload.get("limit") or EVENTS_DEFAULT_LIMIT) > EVENTS_MAX_LIMIT:
            raise VanthRemoteProtocolError("INVALID_REQUEST", f"limit must be <= {EVENTS_MAX_LIMIT}")
    elif method == "job.log_range":
        _check_required_and_unknown(payload, LOG_RANGE_REQUIRED, LOG_RANGE_ALLOWED, "payload")
        _check_string_field(payload, "remote_job_id", required=True)
        stream = payload.get("stream")
        if stream is not None:
            if not isinstance(stream, str) or stream not in VALID_LOG_STREAMS:
                raise VanthRemoteProtocolError("INVALID_REQUEST", "stream must be 'stdout' or 'stderr'")
        for key in ("offset", "size"):
            value = payload.get(key)
            if value is None:
                continue
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise VanthRemoteProtocolError("INVALID_REQUEST", f"{key} must be a non-negative integer")
        size = payload.get("size")
        if size is not None and size > DEFAULT_MAX_FRAME_BYTES // 2:
            raise VanthRemoteProtocolError("INVALID_REQUEST", f"size must be <= {DEFAULT_MAX_FRAME_BYTES // 2}")
    elif method == "artifact.transfer_init":
        # Register/resume a durable bulk transfer on the remote side. For
        # "push" the controller already knows the content identity, so the
        # digest/size/sha fields are required up front; for "pull" they are
        # advisory placeholders — the authoritative values come from the
        # remote's own version row and are returned in the init response.
        direction = payload.get("direction")
        push_required = {"transfer_id", "direction", "root_name", "manifest_digest", "total_bytes", "sha256"}
        # Pull names the remote version to serve; without it the handler
        # could not resolve any content (rc18 review R8).
        pull_required = {"transfer_id", "direction", "version_id"}
        if direction not in TRANSFER_DIRECTIONS:
            raise VanthRemoteProtocolError(
                "INVALID_REQUEST", f"direction must be one of {sorted(TRANSFER_DIRECTIONS)}"
            )
        required = pull_required if direction == "pull" else push_required
        _check_required_and_unknown(payload, required, TRANSFER_INIT_ALLOWED, "payload")
        for field in ("transfer_id", "root_name", "manifest_digest"):
            _check_string_field(payload, field)
            value = payload.get(field)
            if isinstance(value, str) and (not value.strip() or any(ord(ch) < 0x20 for ch in value)):
                raise VanthRemoteProtocolError("INVALID_REQUEST", f"{field} contains forbidden characters")
        _check_string_field(payload, "version_id")
        _check_numeric_field(payload, "total_bytes", minimum=0)
        _check_hex64_field(payload, "sha256")
        tid = payload.get("transfer_id")
        if isinstance(tid, str) and not re.fullmatch(r"xfr_[0-9a-f]{32}", tid):
            raise VanthRemoteProtocolError("INVALID_REQUEST", "transfer_id must be 'xfr_' + 32 hex chars")
    elif method == "artifact.blob_chunk":
        # One chunk at a byte offset. Push chunks carry data_b64+sha256; pull
        # chunk REQUESTS omit them and carry a size instead (the response
        # carries the requested bytes).
        _check_required_and_unknown(payload, {"transfer_id", "offset"}, BLOB_CHUNK_ALLOWED, "payload")
        _check_string_field(payload, "transfer_id")
        tid = payload.get("transfer_id")
        if isinstance(tid, str) and not re.fullmatch(r"xfr_[0-9a-f]{32}", tid):
            raise VanthRemoteProtocolError("INVALID_REQUEST", "transfer_id must be 'xfr_' + 32 hex chars")
        _check_numeric_field(payload, "offset", minimum=0)
        size = payload.get("size")
        if size is not None:
            if isinstance(size, bool) or not isinstance(size, int) or size < 1 or size > 4 * 1024 * 1024:
                raise VanthRemoteProtocolError("INVALID_REQUEST", "size must be an integer in [1, 4194304]")
        data_b64 = payload.get("data_b64")
        if isinstance(data_b64, str) and len(data_b64) > 8 * 1024 * 1024:
            raise VanthRemoteProtocolError("PROTOCOL_OVERSIZED", "chunk data exceeds the maximum chunk size")
        _check_string_field(payload, "data_b64")
        _check_hex64_field(payload, "sha256")
    elif method == "artifact.transfer_complete":
        _check_required_and_unknown(
            payload,
            {"transfer_id", "root_name", "manifest_digest", "total_bytes", "sha256"},
            TRANSFER_COMPLETE_ALLOWED,
            "payload",
        )
        _check_string_field(payload, "transfer_id")
        tid = payload.get("transfer_id")
        if isinstance(tid, str) and not re.fullmatch(r"xfr_[0-9a-f]{32}", tid):
            raise VanthRemoteProtocolError("INVALID_REQUEST", "transfer_id must be 'xfr_' + 32 hex chars")
        _check_hex64_field(payload, "sha256")
        _check_string_field(payload, "root_name")
        _check_string_field(payload, "manifest_digest")
        _check_numeric_field(payload, "total_bytes", minimum=0)
        _check_string_field(payload, "version_id")


def validate_frame(frame: dict[str, Any]) -> dict[str, Any]:
    """Validate one decoded frame object against the v1 schema.

    Enforces the per-kind allowed/required field sets, rejecting unknown
    fields (``PROTOCOL_UNKNOWN_FIELD``). Returns the frame unchanged on
    success.
    """
    if not isinstance(frame, dict):
        raise VanthRemoteProtocolError("PROTOCOL_MALFORMED", "frame must be a JSON object")
    _check_duplicate_keys(frame)
    kind = frame.get("kind")
    if kind not in FRAME_KINDS:
        raise VanthRemoteProtocolError("PROTOCOL_UNKNOWN_KIND", f"unknown frame kind: {kind!r}")
    allowed, required = FRAME_SCHEMAS[kind]
    unknown = sorted(set(frame.keys()) - allowed)
    if unknown:
        raise VanthRemoteProtocolError("PROTOCOL_UNKNOWN_FIELD", f"unknown frame field: {unknown[0]!r}")
    missing = sorted(required - set(frame.keys()))
    if missing:
        raise VanthRemoteProtocolError("PROTOCOL_MALFORMED", f"missing required field: {missing[0]!r}")
    version = frame.get("version")
    if not isinstance(version, str):
        raise VanthRemoteProtocolError("PROTOCOL_MALFORMED", "version must be a string")
    if version != "1":
        raise VanthRemoteProtocolError("UNSUPPORTED_FEATURE", f"unsupported protocol version: {version!r}")
    sent_at = frame.get("sent_at")
    if sent_at is not None and not isinstance(sent_at, str):
        raise VanthRemoteProtocolError("PROTOCOL_MALFORMED", "sent_at must be an ISO-8601 string")
    for key in ("request_id", "remote_id", "agent", "stream", "remote_job_id", "instance_id", "expected_instance_id"):
        if key in frame and not isinstance(frame[key], str):
            raise VanthRemoteProtocolError("PROTOCOL_MALFORMED", f"{key} must be a string")
    if "state_epoch" in frame and (isinstance(frame["state_epoch"], bool) or not isinstance(frame["state_epoch"], int)):
        raise VanthRemoteProtocolError("PROTOCOL_MALFORMED", "state_epoch must be an integer")
    for int_field in ("feed_epoch", "feed_boundary_seq"):
        if int_field in frame and (
            isinstance(frame[int_field], bool) or not isinstance(frame[int_field], int)
        ):
            raise VanthRemoteProtocolError("PROTOCOL_MALFORMED", f"{int_field} must be an integer")
    if "expected_state_epoch" in frame and (
        isinstance(frame["expected_state_epoch"], bool) or not isinstance(frame["expected_state_epoch"], int)
        or frame["expected_state_epoch"] < 1
    ):
        raise VanthRemoteProtocolError("PROTOCOL_MALFORMED", "expected_state_epoch must be an integer >= 1")
    if "cursor" in frame and not isinstance(frame["cursor"], dict):
        raise VanthRemoteProtocolError("PROTOCOL_MALFORMED", "cursor must be an object")
    for key in ("has_more", "truncated"):
        if key in frame and not isinstance(frame[key], bool):
            raise VanthRemoteProtocolError("PROTOCOL_MALFORMED", f"{key} must be a boolean")
    for key in ("offset", "size"):
        if key in frame and (isinstance(frame[key], bool) or not isinstance(frame[key], int)):
            raise VanthRemoteProtocolError("PROTOCOL_MALFORMED", f"{key} must be an integer")
    if "jobs" in frame and not isinstance(frame["jobs"], list):
        raise VanthRemoteProtocolError("PROTOCOL_MALFORMED", "jobs must be an array")
    if "events" in frame and not isinstance(frame["events"], list):
        raise VanthRemoteProtocolError("PROTOCOL_MALFORMED", "events must be an array")

    if kind == "request":
        method = frame.get("method")
        if not isinstance(method, str):
            raise VanthRemoteProtocolError("PROTOCOL_MALFORMED", "method must be a string")
        if method not in VALID_REQUEST_METHODS:
            raise VanthRemoteProtocolError("UNSUPPORTED_FEATURE", f"unsupported request method: {method}")
        if not isinstance(frame["idempotency_key"], str):
            raise VanthRemoteProtocolError("INVALID_REQUEST", "idempotency_key must be a string")
        if not IDEMPOTENCY_KEY_RE.match(frame["idempotency_key"]):
            raise VanthRemoteProtocolError(
                "INVALID_REQUEST", "idempotency_key must be 8..128 chars in [A-Za-z0-9_-]"
            )
        payload = frame.get("payload")
        if not isinstance(payload, dict):
            raise VanthRemoteProtocolError("INVALID_REQUEST", "payload must be an object")
        _check_duplicate_keys(payload)
        validate_request(method, payload)
        digest = frame.get("digest")
        if digest is not None:
            if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
                raise VanthRemoteProtocolError("PROTOCOL_MALFORMED", "digest must be a 64-char lowercase hex string")
            expected = request_digest(method, payload, frame["idempotency_key"])
            if digest != expected:
                raise VanthRemoteProtocolError("PROTOCOL_REPLAY_MISMATCH", "digest does not match the request")
    elif kind == "error":
        code = frame.get("code")
        if not isinstance(code, str):
            raise VanthRemoteProtocolError("PROTOCOL_MALFORMED", "code must be a string")
        if code not in ERROR_REGISTRY:
            raise VanthRemoteProtocolError("PROTOCOL_MALFORMED", f"unknown error code: {code!r}")
    return frame


# ---------------------------------------------------------------------------
# Request digest
# ---------------------------------------------------------------------------

def request_digest(method: str, payload: dict[str, Any], idempotency_key: str) -> str:
    """Lowercase hex SHA-256 of the canonicalized request triple.

    ``payload`` must already be validated (a validated payload is always a
    plain dict with string keys, so canonicalization cannot fail on key
    type). ``method`` and ``idempotency_key`` are emitted verbatim.
    """
    canonical = canonical_json(
        {"method": method, "payload": payload, "idempotency_key": idempotency_key}
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
