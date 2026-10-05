"""Tests for the v1.1 agent-facing feature set.

Covers: job_status/job_view exposing command/cwd/env/timeout, job_list name and
tag filters, job_events reverse (latest-N) paging, job_rerun relaunching a job
with its original command/env/targets, and daemon discovery metadata.
"""

import asyncio
import sys

import pytest

from vanth.server import JobManager


import shellcmd


def cmd(code: str) -> str:
    return shellcmd.join([sys.executable, "-c", code])


def wait_event(manager: JobManager, job_id: str, event_type: str) -> dict:
    return asyncio.run(manager.wait(job_id, [event_type], timeout_seconds=10))


def start_job(manager, code, **kwargs):
    return asyncio.run(manager.start(cmd(code), **kwargs))["job_id"]


def test_status_exposes_command_cwd_env_and_timeout(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(
            manager,
            "print('hi')",
            name="visible job",
            cwd=str(tmp_path),
            env={"VANTH_TEST_ENV": "present", "SECOND": "two"},
            timeout_seconds=30,
            tags=["inspect"],
        )
        wait_event(manager, job_id, "completed")
        status = manager.status(job_id)
        assert status["command"] == cmd("print('hi')")
        assert status["cwd"] == str(tmp_path)
        assert status["env"] == {"VANTH_TEST_ENV": "present", "SECOND": "two"}
        assert status["timeout_seconds"] == 30
        assert "inspect" in status["tags"]
    finally:
        manager.close()


def test_view_exposes_command_and_env(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(manager, "print('view')", name="view job", env={"K": "v"})
        wait_event(manager, job_id, "completed")
        view = manager.agent_view()["jobs"]
        entry = next(j for j in view if j["job_id"] == job_id)
        assert entry["command"] == cmd("print('view')")
        assert entry["env"] == {"K": "v"}
    finally:
        manager.close()


def test_list_filters_by_name_and_tags(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_a = start_job(manager, "print('a')", name="alpha run", tags=["gpu", "train"])
        job_b = start_job(manager, "print('b')", name="beta run", tags=["cpu"])
        wait_event(manager, job_a, "completed")
        wait_event(manager, job_b, "completed")

        by_name = manager.list(name="alpha")["jobs"]
        assert [j["job_id"] for j in by_name] == [job_a]

        by_tag = manager.list(tags=["gpu"])["jobs"]
        assert [j["job_id"] for j in by_tag] == [job_a]

        multi = manager.list(tags=["gpu", "train"])["jobs"]
        assert [j["job_id"] for j in multi] == [job_a]

        no_match = manager.list(tags=["nonexistent"])["jobs"]
        assert no_match == []
    finally:
        manager.close()


def test_events_reverse_returns_latest_first(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        code = (
            "import json;"
            "f=lambda i:(print('AGENT_EVENT '+json.dumps({'type':'metric','data':{'i':i}}), flush=True),);"
            "[f(i) for i in range(10)]"
        )
        job_id = start_job(manager, code)
        wait_event(manager, job_id, "metric")
        deadline = __import__("time").monotonic() + 10
        while __import__("time").monotonic() < deadline and len(manager.events(job_id, limit=100)["events"]) < 12:
            __import__("time").sleep(0.05)

        reverse = manager.events(job_id, limit=5, reverse=True)["events"]
        seqs = [e["seq"] for e in reverse]
        assert len(seqs) == 5
        assert seqs == sorted(seqs, reverse=True), seqs

        forward = manager.events(job_id, limit=5)["events"]
        assert [e["seq"] for e in forward] == list(range(1, 6))

        types_only = manager.events(job_id, types=["completed"], limit=5, reverse=True)["events"]
        assert {e["type"] for e in types_only} == {"completed"}
    finally:
        manager.close()


def test_rerun_relaunches_failed_job_with_original_config(tmp_path):
    manager = JobManager(tmp_path / "state")
    calls = tmp_path / "rerun_calls.txt"
    try:
        code = (
            "from pathlib import Path; import os,sys; "
            "Path(os.environ['RERUN_MARK']).touch(); "
            "print('AGENT_EVENT '+__import__('json').dumps({'type':'checkpoint'}), flush=True); "
            "sys.exit(0)"
        )
        target = {"type": "local_command", "events": ["checkpoint"], "command": [sys.executable, "-c", "pass"]}
        job_id = start_job(
            manager, code,
            name="rerun me",
            cwd=str(tmp_path),
            env={"RERUN_MARK": str(calls)},
            tags=["rerun"],
            wake_targets=[target],
            timeout_seconds=15,
        )
        wait_event(manager, job_id, "completed")

        reran = manager.rerun_sync(job_id)
        assert reran["status"] == "running"
        assert reran["job_id"] != job_id
        assert manager.status(reran["job_id"])["env"] == {"RERUN_MARK": str(calls)}
        assert manager.status(reran["job_id"])["tags"] == ["rerun"]
        assert manager.status(reran["job_id"])["cwd"] == str(tmp_path)
        wait_event(manager, reran["job_id"], "checkpoint")
        deliveries = manager.deliveries(reran["job_id"])["deliveries"]
        assert len(deliveries) == 1
        assert deliveries[0]["target_type"] == "local_command"
    finally:
        manager.close()


def test_rerun_unknown_job_is_error(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        with pytest.raises(ValueError, match="Unknown job_id"):
            manager.rerun_sync("job_missing")
    finally:
        manager.close()


def test_rerun_preserves_wake_targets_and_origin(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(
            manager,
            "print('x')",
            name="targeted",
            origin_thread_id="thread_origin",
            wake_targets=[
                {"type": "codex_thread", "thread_id": "thread_wake", "events": ["completed"], "auto_dispatch": False}
            ],
        )
        wait_event(manager, job_id, "completed")
        reran = manager.rerun_sync(job_id)
        status = manager.status(reran["job_id"])
        assert status["origin_thread_id"] == "thread_origin"
        assert status["wake_thread_id"] == "thread_wake"
    finally:
        manager.close()


def test_run_overview_metadata_captured_at_start(tmp_path):
    import platform
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(
            manager, "print('run')", name="run overview",
            cwd=str(tmp_path), notes="this run is special",
            env={"MODE": "probe"}, tags=["sav-transfer"],
        )
        wait_event(manager, job_id, "completed")
        status = manager.status(job_id)
        run = status["run"]
        assert status["notes"] == "this run is special"
        assert run["hostname"] == platform.node()
        assert run["os"] == platform.system()
        # toolchain describes the JOB, not the daemon's interpreter
        assert run["toolchain"]["language"] == "python"
        assert "python_version" not in run
        assert run["cwd"] == str(tmp_path)
        assert run["author"] is not None
        assert status["runtime_seconds"] is not None and status["runtime_seconds"] >= 0
    finally:
        manager.close()


def test_rerun_preserves_notes(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(manager, "print('x')", name="noted", notes="keep this note")
        wait_event(manager, job_id, "completed")
        reran = manager.rerun_sync(job_id)
        assert manager.status(reran["job_id"])["notes"] == "keep this note"
    finally:
        manager.close()


def test_runtime_seconds_increases_for_running_job(tmp_path):
    import time as _time
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(manager, "import time; time.sleep(1.2)")
        wait_event(manager, job_id, "started")
        first = manager.status(job_id)["runtime_seconds"]
        _time.sleep(0.5)
        second = manager.status(job_id)["runtime_seconds"]
        assert second >= first
        wait_event(manager, job_id, "completed")
        done = manager.status(job_id)["runtime_seconds"]
        assert done >= second
    finally:
        manager.close()
