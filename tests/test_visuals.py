"""Declarative monitor panels: the `panels` AGENT_EVENT type."""

from __future__ import annotations

import json

from vanth.server import JobManager, normalize_event_payload, now_iso, parse_agent_event_line, validate_panels


def _norm(line: str) -> dict:
    payload = parse_agent_event_line(line)
    assert payload is not None
    return normalize_event_payload(payload)


def test_valid_panels_round_trip_and_aliases():
    ev = _norm('AGENT_EVENT {"type":"panels","data":{"panels":['
               '{"kind":"gauge","metric":"gpu","max":100,"label":"GPU","unit":"%"},'
               '{"kind":"chart","metric":"loss"},'
               '{"kind":"NUMBER","metric":"tok_s","unit":"tok/s"},'
               '{"kind":"text","key":"phase","label":"phase"},'
               '{"kind":"status","text":"warming up"}]}}')
    assert ev["level"] == "info"
    assert ev["message"] is None
    assert ev["data"] == {"panels": [
        {"kind": "gauge", "metric": "gpu", "label": "GPU", "unit": "%", "max": 100},
        {"kind": "line", "metric": "loss"},
        {"kind": "stat", "metric": "tok_s", "unit": "tok/s"},
        {"kind": "text", "key": "phase", "label": "phase"},
        {"kind": "text", "text": "warming up"},
    ]}
    # Normalising again is a no-op (the capture and _emit paths may both run it).
    assert normalize_event_payload({"type": "panels", **ev})["data"] == ev["data"]


def test_top_level_panels_key_is_accepted():
    ev = _norm('AGENT_EVENT {"type":"panels","panels":[{"kind":"line","metric":"loss"}]}')
    assert ev["data"] == {"panels": [{"kind": "line", "metric": "loss"}]}


def test_invalid_specs_are_dropped_with_notes():
    raw = [
        "nope",
        {"kind": "pie", "metric": "x"},
        {"kind": "line"},
        {"kind": "text"},
        {"kind": "gauge", "metric": "m" * 65},
        {"kind": "gauge", "metric": "ok", "min": 5, "max": 1, "label": "L" * 100, "extra": {"a": 1}},
        {"kind": "stat", "metric": "nan", "max": float("nan"), "min": True},
    ]
    panels, invalid = validate_panels(raw)
    assert panels == [
        {"kind": "gauge", "metric": "ok", "label": "L" * 32},
        {"kind": "stat", "metric": "nan"},
    ]
    assert len(invalid) == 6
    assert invalid[0] == "panel 1: not an object"
    assert "unknown kind" in invalid[1]
    assert "min must be below max" in invalid[5]

    ev = normalize_event_payload({"type": "panels", "data": {"panels": raw}})
    assert ev["level"] == "warning"
    assert ev["message"].startswith("6 panel spec(s) ignored: panel 1")
    assert ev["data"]["invalid"] == invalid


def test_counts_and_shapes_are_bounded():
    panels, invalid = validate_panels([{"kind": "line", "metric": f"m{i}"} for i in range(50)])
    assert len(panels) == 8
    assert invalid == ["42 panel(s) beyond the 8-panel limit ignored"]
    assert validate_panels({"kind": "line"}) == ([], ["panels must be a list"])
    assert validate_panels(None) == ([], ["panels must be a list"])
    ev = normalize_event_payload({"type": "panels", "data": "garbage"})
    assert ev["data"] == {"panels": [], "invalid": ["panels must be a list"]}


def test_panels_event_is_stored_like_any_event(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    try:
        with manager.db_lock:
            manager.db.execute(
                "INSERT INTO jobs(job_id, name, command, status, created_at, updated_at, stdout_path, "
                "stderr_path, events_path) VALUES ('job_v', 'v', 'echo', 'running', ?, ?, ?, ?, ?)",
                (now_iso(), now_iso(), str(tmp_path / "o"), str(tmp_path / "e"), str(tmp_path / "ev.jsonl")),
            )
            manager.db.commit()
        manager._emit("job_v", "panels", data={"panels": [{"kind": "line", "metric": "loss"}, {"kind": "pie"}]},
                      source="stdout")
        with manager.db_lock:
            row = manager.db.execute(
                "SELECT level, message, data_json FROM events WHERE job_id='job_v' AND type='panels'").fetchone()
        assert row["level"] == "warning"
        assert "ignored" in row["message"]
        data = json.loads(row["data_json"])
        assert data["panels"] == [{"kind": "line", "metric": "loss"}]
        assert len(data["invalid"]) == 1
    finally:
        manager.close()
