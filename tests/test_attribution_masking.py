"""Review #9: stop attribution (actor/reason) and declared-secret masking."""

from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path

import pytest

from vanth.server import JobManager, mask_secrets


import shellcmd


def cmd(code: str) -> str:
    return shellcmd.join([sys.executable, "-c", code])


def _wait_status(manager: JobManager, job_id: str, want: set[str], timeout: float = 15.0) -> str:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = manager.status(job_id)["status"]
        if status in want:
            return status
        time.sleep(0.05)
    raise AssertionError(f"job {job_id} did not reach {want} (last={status})")


def _wait_event(manager: JobManager, job_id: str, event_type: str, timeout: float = 15.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        events = manager.events(job_id, types=[event_type], limit=10)["events"]
        if events:
            return events[-1]
        time.sleep(0.05)
    raise AssertionError(f"no {event_type} event for {job_id}")


def test_mask_secrets_replaces_declared_values():
    assert mask_secrets(b"token=s3cr3t\n", ["s3cr3t"]) == b"token=***\n"
    # Non-declared text and empty/None lists are untouched.
    assert mask_secrets(b"hello", None) == b"hello"
    assert mask_secrets(b"hello", []) == b"hello"
    assert mask_secrets(b"hello", ["", "world"]) == b"hello"
    # Every occurrence is masked.
    assert mask_secrets(b"a s3cr3t b s3cr3t", ["s3cr3t"]) == b"a *** b ***"


def test_secret_env_masked_in_logs_and_events(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        try:
            code = (
                "import os,json;"
                "tok=os.environ['API_TOKEN'];"
                "print('plain '+tok, flush=True);"
                "print('token='+tok, file=__import__('sys').stderr, flush=True);"
                "print('AGENT_EVENT '+json.dumps({'type':'metric','data':{'token':tok},'message':'leak '+tok}), flush=True)"
            )
            started = manager.start(cmd(code), env={"API_TOKEN": "s3cr3t-value"}, secret_env=["API_TOKEN"])
            job_id = started["job_id"]
            _wait_status(manager, job_id, {"completed", "failed"})

            stdout = Path(started["stdout_path"]).read_text(encoding="utf-8")
            stderr = Path(started["stderr_path"]).read_text(encoding="utf-8")
            assert "s3cr3t-value" not in stdout, stdout
            assert "s3cr3t-value" not in stderr, stderr
            assert "***" in stdout and "***" in stderr

            events = manager.events(job_id, types=["metric"], limit=10)["events"]
            assert events, "metric event should be recorded"
            blob = json.dumps(events)
            assert "s3cr3t-value" not in blob, blob
            assert events[0]["data"]["token"] == "***"

            status = manager.status(job_id)
            assert status["secret_env"] == ["API_TOKEN"]
        finally:
            manager.close()

    asyncio.run(main())


def test_stop_event_carries_actor_and_reason(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        try:
            started = manager.start(cmd("import time; time.sleep(30)"))
            job_id = started["job_id"]
            _wait_status(manager, job_id, {"running"})
            result = manager.stop_sync(job_id, actor="tool", reason="agent changed its mind")
            assert result["status"] == "cancelled", result

            data = _wait_event(manager, job_id, "cancelled")["data"]
            assert data["actor"] == "tool"
            assert data["reason"] == "agent changed its mind"

            status = manager.status(job_id)
            assert status["stop_actor"] == "tool"
            assert status["stop_reason"] == "agent changed its mind"
        finally:
            manager.close()

    asyncio.run(main())


def test_stop_default_actor_is_user(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        try:
            started = manager.start(cmd("import time; time.sleep(30)"))
            job_id = started["job_id"]
            _wait_status(manager, job_id, {"running"})
            manager.stop_sync(job_id)
            data = _wait_event(manager, job_id, "cancelled")["data"]
            assert data["actor"] == "user"
            assert data["reason"] == "stop requested"
        finally:
            manager.close()

    asyncio.run(main())


def test_invalid_stop_actor_rejected(tmp_path):
    manager = JobManager(tmp_path)
    try:
        with pytest.raises(ValueError, match="actor must be one of"):
            manager.stop_sync("job_missing", actor="bogus")
    finally:
        manager.close()


def test_queued_cancel_carries_attribution(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        try:
            parent = manager.start(cmd("import time; time.sleep(30)"))
            child = manager.start(
                cmd("print('never')"),
                trigger={"job_id": parent["job_id"], "status": "completed"},
            )
            assert child["status"] == "queued"
            child_id = child["job_id"]
            result = manager.stop_sync(child_id, actor="user", reason="cancelled before trigger")
            assert "cancelled" in result["message"].lower()
            assert _wait_event(manager, child_id, "cancelled")["data"] == {
                "actor": "user",
                "reason": "cancelled before trigger",
            }
            manager.stop_sync(parent["job_id"])
        finally:
            manager.close()

    asyncio.run(main())


def test_timeout_event_carries_timeout_actor(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        try:
            started = manager.start(cmd("import time; time.sleep(30)"), timeout_seconds=1)
            job_id = started["job_id"]
            _wait_status(manager, job_id, {"timeout"}, timeout=20)
            data = _wait_event(manager, job_id, "timeout", timeout=20)["data"]
            assert data["actor"] == "timeout"
            assert "exceeded" in (data["reason"] or "")
        finally:
            manager.close()

    asyncio.run(main())


def test_rerun_preserves_secret_env(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        try:
            code = "import os; print('tok='+os.environ.get('API_TOKEN',''), flush=True)"
            started = manager.start(cmd(code), env={"API_TOKEN": "s3cr3t-value"}, secret_env=["API_TOKEN"])
            job_id = started["job_id"]
            _wait_status(manager, job_id, {"completed", "failed"})
            rerun = await manager.rerun(job_id)
            new_id = rerun["job_id"]
            _wait_status(manager, new_id, {"completed", "failed"})
            assert manager.status(new_id)["secret_env"] == ["API_TOKEN"]
            stdout = Path(rerun["stdout_path"]).read_text(encoding="utf-8")
            assert "s3cr3t-value" not in stdout
            assert "***" in stdout
        finally:
            manager.close()

    asyncio.run(main())
