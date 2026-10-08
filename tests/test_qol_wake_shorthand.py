"""Tests for the daemon_wake shorthand (_build_wake_target) + wake delivery.

Uses the same patterns as test_qol_mcp.py: a direct JobManager instance with a
tmp_path home and real jobs started via manager.start(...).
"""

import asyncio
import sys
import time

import pytest

from vanth.server import JobManager, _build_wake_target


import shellcmd


def cmd(code: str) -> str:
    return shellcmd.join([sys.executable, "-c", code])


def wait_event(manager: JobManager, job_id: str, event_type: str) -> dict:
    return asyncio.run(manager.wait(job_id, [event_type], timeout_seconds=10))


def start_job(manager, code, **kwargs):
    return manager.start(cmd(code), **kwargs)["job_id"]


def test_build_wake_target_passthrough():
    target = {"type": "codex_thread", "events": ["completed"], "session_id": "s1"}
    assert _build_wake_target(target, None, None, {}) is target


def test_build_wake_target_shorthand():
    assert _build_wake_target(None, ["checkpoint"], "local_command", {}) == {
        "type": "local_command",
        "events": ["checkpoint"],
    }
    assert _build_wake_target(None, None, "opencode_thread", {"session_id": "s1", "attach": "http://127.0.0.1:4096"}) == {
        "type": "opencode_thread",
        "events": ["completed", "failed"],
        "session_id": "s1",
        "attach": "http://127.0.0.1:4096",
    }
    assert _build_wake_target(None, None, "local_command", {"command": ["echo", "hi"]}) == {
        "type": "local_command",
        "events": ["completed", "failed"],
        "command": ["echo", "hi"],
    }


def test_build_wake_target_validation():
    with pytest.raises(ValueError, match="type is required"):
        _build_wake_target(None, None, None, {})
    with pytest.raises(ValueError, match="unsupported wake target type"):
        _build_wake_target(None, None, "http", {})
    with pytest.raises(ValueError, match="non-empty"):
        _build_wake_target(None, [], "local_command", {})
    with pytest.raises(ValueError, match="non-empty"):
        _build_wake_target(None, ["x", 3], "local_command", {})
    with pytest.raises(ValueError, match="type is required"):
        _build_wake_target(None, None, "", {})
    with pytest.raises(ValueError, match="dict"):
        _build_wake_target("http", None, None, {})


def test_wake_shorthand_enqueues_delivery_on_completion(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(manager, "print('wake shorthand')")
        result = manager.add_wake_target(
            job_id,
            _build_wake_target(None, ["completed"], "local_command", {"command": [sys.executable, "-c", "import sys; sys.exit(0)"]}),
        )
        assert result["result"] == "ok"
        assert result["job_id"] == job_id
        assert result["target_id"].startswith("target_")
        assert result["target_type"] == "local_command"
        assert result["events"] == ["completed"]

        wait_event(manager, job_id, "completed")
        deadline = time.monotonic() + 10
        deliveries = []
        while time.monotonic() < deadline:
            deliveries = manager.deliveries(job_id)["deliveries"]
            if any(d["target_type"] == "local_command" for d in deliveries):
                break
            time.sleep(0.05)
        assert any(d["target_type"] == "local_command" for d in deliveries)
    finally:
        manager.close()


def test_add_wake_target_rejects_unsupported_type(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(manager, "print('reject')")
        with pytest.raises(ValueError, match="unsupported wake target type"):
            manager.add_wake_target(job_id, {"type": "http", "events": ["completed"]})
    finally:
        manager.close()


def test_python_compat_wrappers_accept_legacy_kwargs(monkeypatch, tmp_path):
    """Review rc37 P1: the old public Python names daemon_wake / job_wake_now /
    job_add_wake_target keep the original signature
    ``(job_id, target=None, events=None, type=None, **config)`` — extra target
    config is passed as plain keyword arguments. The MCP surface is the
    explicit-config adapters (mcp_daemon_wake etc.) registered under the
    EXISTING external MCP names daemon_wake / job_wake_now / job_add_wake_target
    (review rc38 P1). Each Python function delegates through the same
    _build_wake_target logic and posts to the daemon."""
    from vanth.server import (
        daemon_wake,
        job_add_wake_target,
        job_wake_now,
    )

    posted = {}

    class FakeClient:
        def post(self, path, payload):
            posted["path"] = path
            posted["payload"] = payload
            return {"result": "ok", "job_id": "job_1", "target_id": "target_1", "target_type": payload["target"]["type"], "events": payload["target"].get("events")}

    import vanth.server as server_mod
    original = server_mod.get_client
    server_mod.get_client = lambda: FakeClient()
    try:
        # Original kwargs style: daemon_wake(job_id, type=..., command=...).
        result = daemon_wake("job_1", type="local_command", command=["echo", "hi"])
        assert result["result"] == "ok"
        assert posted["path"] == "/jobs/job_1/wake"
        assert posted["payload"]["target"]["type"] == "local_command"
        assert posted["payload"]["target"]["command"] == ["echo", "hi"]
        assert posted["payload"]["target"]["events"] == ["completed", "failed"]

        result = job_add_wake_target("job_1", type="webhook", url="http://x/", events=["checkpoint"])
        assert posted["payload"]["target"]["type"] == "webhook"
        assert posted["payload"]["target"]["url"] == "http://x/"
        assert posted["payload"]["target"]["events"] == ["checkpoint"]

        # job_wake_now posts to /wake-now.
        job_wake_now("job_1", type="local_command", command=["echo", "hi"])
        assert posted["path"] == "/jobs/job_1/wake-now"

        # Original full-target positional form still works.
        daemon_wake("job_1", {"type": "local_command", "command": ["echo", "hi"], "events": ["completed"]})
        assert posted["payload"]["target"]["type"] == "local_command"
        assert posted["payload"]["target"]["command"] == ["echo", "hi"]

        # Missing type still raises through _build_wake_target.
        with pytest.raises(ValueError, match="type is required"):
            daemon_wake("job_1", command=["echo", "hi"])
    finally:
        server_mod.get_client = original


def test_wake_now_response_contract(tmp_path):
    """Review rc36 P2: wake_now's response must carry the literal
    ``synthetic_event_type: "wake_now"`` (never a fabricated completed/failed)
    plus the real job status as ``requested_status``."""
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(manager, "print('x')")
        result = manager.wake_now(
            job_id,
            {"type": "local_command", "events": ["completed"], "command": [sys.executable, "-c", "import sys; sys.exit(0)"]},
        )
        assert result["woken"] is True
        assert result["synthetic_event_type"] == "wake_now"
        assert result["requested_status"] == manager.status(job_id)["status"]
    finally:
        manager.close()
