import json
from typing import Any

EVENT_PREFIX = "AGENT_EVENT "


def build_event(
    event_type: str,
    message: str | None = None,
    data: dict[str, Any] | None = None,
    level: str | None = None,
) -> dict[str, Any]:
    """Build an AGENT_EVENT payload dict.

    The payload shape is the language-neutral wire format: a JSON object whose
    required ``type`` and optional ``data``/``message``/``level`` keys are
    parsed from any job's stdout/stderr line prefixed with ``AGENT_EVENT ``.
    """
    payload: dict[str, Any] = {"type": event_type, "data": dict(data or {})}
    if message is not None:
        payload["message"] = message
    if level is not None:
        payload["level"] = level
    return payload


def format_event(payload: dict[str, Any]) -> str:
    """Render a payload dict as the one-line stdout form jobs emit."""
    return EVENT_PREFIX + json.dumps(payload, separators=(",", ":"))


def emit_payload(payload: dict[str, Any]) -> None:
    print(format_event(payload), flush=True)


def agent_event(event_type: str, message: str | None = None, **data: object) -> None:
    emit_payload(build_event(event_type, message, data))


def progress(
    current: float,
    total: float | None = None,
    unit: str | None = None,
    stage: str | None = None,
    message: str | None = None,
) -> None:
    data: dict[str, object] = {"current": current}
    if total is not None:
        data["total"] = total
        data["percent"] = round((current / total) * 100, 2) if total else 0
    if unit is not None:
        data["unit"] = unit
    if stage is not None:
        data["stage"] = stage
    agent_event("progress", message, **data)
