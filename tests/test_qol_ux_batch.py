import asyncio
import json
import sys
import time

import pytest

from vanth.server import JobManager


import shellcmd


def cmd(code: str) -> str:
    return shellcmd.join([sys.executable, "-c", code])


def start_job(manager, code, **kwargs):
    return manager.start(cmd(code), **kwargs)["job_id"]


def wait_terminal(manager, job_id, timeout=20):
    return asyncio.run(manager.wait(job_id, ["completed", "failed", "cancelled", "timeout"], timeout_seconds=timeout))


# ---- job_wait metric_ge ----

def test_wait_metric_ge_returns_when_threshold_crossed(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        code = (
            "import json,time;"
            "f=lambda i:(print('AGENT_EVENT '+json.dumps({'type':'metric','data':{'loss':i}}), flush=True), "
            "time.sleep(0.05));"
            "[f(i) for i in (0.2, 0.5, 0.8, 1.0)]"
        )
        job_id = start_job(manager, code)
        result = asyncio.run(manager.wait(job_id, ["completed"], metric_ge={"loss": 0.5}, timeout_seconds=15))
        assert result["result"] == "metric"
        assert result["metric"] == "loss"
        assert result["threshold"] == 0.5
        assert result["value"] >= 0.5
    finally:
        manager.close()


def test_wait_metric_ge_ignored_when_none(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(manager, "print('plain')")
        result = asyncio.run(manager.wait(job_id, ["completed"], timeout_seconds=10))
        assert result["result"] == "event"
        assert result["event"]["type"] == "completed"
    finally:
        manager.close()


def test_wait_metric_ge_invalid_raises(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(manager, "print('x')")
        asyncio.run(manager.wait(job_id, ["started"], timeout_seconds=10))
        with pytest.raises(ValueError):
            asyncio.run(manager.wait(job_id, ["completed"], metric_ge={"loss": "high"}, timeout_seconds=1))
        with pytest.raises(ValueError):
            asyncio.run(manager.wait(job_id, ["completed"], metric_ge={}, timeout_seconds=1))
    finally:
        manager.close()


# ---- job DAG / trigger ----

def test_trigger_starts_child_when_parent_completes(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        parent = start_job(manager, "print('parent done')")
        child = manager.start(
            cmd("print('child done')"),
            trigger={"job_id": parent, "status": "completed"},
        )
        assert child["status"] == "queued"
        child_id = child["job_id"]
        assert manager.status(child_id)["status"] == "queued"
        assert manager.status(child_id)["trigger"] == {"job_id": parent, "status": "completed"}
        assert manager.status(parent)["status"] in {"running", "launching", "completed", "failed"}
        wait_terminal(manager, child_id)
        assert manager.status(child_id)["status"] == "completed"
        # child should have run after parent
        parent_status = manager.status(parent)["status"]
        assert parent_status == "completed"
    finally:
        manager.close()


def test_trigger_cancels_child_when_parent_ends_differently(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        parent = start_job(manager, "import sys; sys.exit(1)")
        wait_terminal(manager, parent)
        child = manager.start(
            cmd("print('never runs')"),
            trigger={"job_id": parent, "status": "completed"},
        )
        child_id = child["job_id"]
        wait_terminal(manager, child_id)
        assert manager.status(child_id)["status"] == "cancelled"
    finally:
        manager.close()


def test_trigger_unknown_parent_raises(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        with pytest.raises(ValueError):
            manager.start(cmd("x"), trigger={"job_id": "job_nope", "status": "completed"})
    finally:
        manager.close()


def test_trigger_child_cancelled_when_parent_row_is_removed(tmp_path):
    """A pruned/removed parent can never satisfy the gate, so the child must be
    cancelled rather than left `queued` forever with no event (silent stuck)."""
    manager = JobManager(tmp_path / "state")
    try:
        # Parent still running, so the DAG gate cannot be satisfied yet.
        parent = start_job(manager, "import time; time.sleep(30)")
        child = manager.start(
            cmd("print('never runs')"),
            trigger={"job_id": parent, "status": "completed"},
        )
        child_id = child["job_id"]
        assert manager.status(child_id)["status"] == "queued"
        with manager.db_lock:
            manager.db.execute("DELETE FROM jobs WHERE job_id=?", (parent,))
            manager.db.commit()
        manager._dispatch_queued_jobs()
        assert manager.status(child_id)["status"] == "cancelled"
    finally:
        manager.close()


def test_stop_queued_job_cancels_without_running(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        parent = start_job(manager, "import time; time.sleep(30)")
        child = manager.start(
            cmd("print('never')"),
            trigger={"job_id": parent, "status": "completed"},
        )
        child_id = child["job_id"]
        stopped = manager.stop_sync(child_id)
        assert stopped["status"] == "cancelled"
        assert manager.status(child_id)["status"] == "cancelled"
    finally:
        manager.close()


# ---- tail grep ----

def test_tail_grep_filters_lines(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        code = "print('hello world', flush=True); print('goodbye world', flush=True)"
        job_id = start_job(manager, code)
        asyncio.run(manager.wait(job_id, ["completed"], timeout_seconds=10))
        all_content = manager.tail(job_id)["content"]
        assert "hello world" in all_content and "goodbye world" in all_content
        filtered = manager.tail(job_id, grep="hello")["content"]
        assert "hello world" in filtered
        assert "goodbye world" not in filtered
    finally:
        manager.close()


def test_tail_grep_empty_result(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(manager, "print('alpha', flush=True)")
        asyncio.run(manager.wait(job_id, ["completed"], timeout_seconds=10))
        filtered = manager.tail(job_id, grep="zzz_none")["content"]
        assert filtered == ""
    finally:
        manager.close()


# ---- job diff ----

def test_diff_identical_jobs(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        code = "print('same')"
        job_id = start_job(manager, code, name="same")
        other = start_job(manager, code, name="same")
        diff = manager.diff_spec(job_id, other)
        assert diff["identical"] is True
        assert diff["changes"] == []
    finally:
        manager.close()


def test_diff_detects_command_and_env_changes(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        base = start_job(manager, "print('a')", env={"X": "1"}, name="base")
        other = start_job(manager, "print('b')", env={"X": "2"}, name="base")
        diff = manager.diff_spec(base, other)
        assert diff["identical"] is False
        fields = {c["field"] for c in diff["changes"]}
        assert "command" in fields
        assert "env" in fields
        env_change = next(c for c in diff["changes"] if c["field"] == "env")
        assert any(e["key"] == "X" and e["base"] == "1" and e["other"] == "2" for e in env_change["changes"])
    finally:
        manager.close()


def test_diff_unknown_job_raises(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(manager, "print('x')")
        with pytest.raises(ValueError):
            manager.diff_spec(job_id, "job_nope")
    finally:
        manager.close()
