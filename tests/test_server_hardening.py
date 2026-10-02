import asyncio
import inspect
import json
import os
import subprocess
import sys
import threading
import time

import pytest

from vanth.server import JobManager


import shellcmd


def cmd(code: str) -> str:
    return shellcmd.join([sys.executable, "-c", code])


def test_terminal_events_carry_a_message_and_normalize_exit_code(tmp_path):
    """A terminal status must explain itself: failed/timeout events previously
    had an empty message, and a Windows termination status (4294967295) showed
    as a huge exit code."""
    async def main():
        manager = JobManager(tmp_path)
        started = await manager.start(cmd("import sys; sys.exit(7)"))
        # Wait for the terminal event to be persisted.
        await manager.wait(started["job_id"], ["failed"], timeout_seconds=30)
        events = manager.events(started["job_id"])["events"]
        failed = [e for e in events if e["type"] == "failed"]
        assert failed, events
        assert failed[0]["message"], "failed event must carry a message"
        assert "7" in failed[0]["message"], failed[0]["message"]
        assert manager.status(started["job_id"])["exit_code"] == 7
        manager.close()

    asyncio.run(main())


def test_timeout_event_message_names_the_timeout(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        started = await manager.start(cmd("import time; time.sleep(30)"), timeout_seconds=1)
        await manager.wait(started["job_id"], ["timeout"], timeout_seconds=30)
        events = manager.events(started["job_id"])["events"]
        timeout = [e for e in events if e["type"] == "timeout"]
        assert timeout, events
        assert "timeout" in (timeout[0]["message"] or "").lower(), timeout[0]["message"]
        manager.close()

    asyncio.run(main())


def test_display_exit_code_normalizes_windows_termination_status():
    from vanth.server import JobManager

    assert JobManager._display_exit_code(None) is None
    assert JobManager._display_exit_code(0) == 0
    assert JobManager._display_exit_code(1) == 1
    assert JobManager._display_exit_code(-9) == -9
    assert JobManager._display_exit_code(4294967295) == -1


def test_malformed_event_does_not_kill_reader(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        code = (
            "import json; "
            "print('AGENT_EVENT '+json.dumps({'type':'checkpoint','message':{'bad':1}}), flush=True); "
            "print('AGENT_EVENT '+json.dumps({'type':'checkpoint','message':'after bad'}), flush=True)"
        )
        started = await manager.start(cmd(code))
        result = await manager.wait(started["job_id"], ["checkpoint"], timeout_seconds=5)
        assert result["event"]["message"] == "after bad"
        manager.close()

    asyncio.run(main())


def test_oversized_event_line_is_rejected_and_reader_continues(tmp_path, monkeypatch):
    async def main():
        monkeypatch.setenv("VANTH_MAX_EVENT_LINE_BYTES", "256")
        manager = JobManager(tmp_path)
        code = (
            "import json; "
            "print('AGENT_EVENT '+json.dumps({'type':'checkpoint','message':'x'*1000}), flush=True); "
            "print('AGENT_EVENT '+json.dumps({'type':'checkpoint','message':'after big'}), flush=True)"
        )
        started = await manager.start(cmd(code))
        rejected = await manager.wait(started["job_id"], ["event_rejected"], timeout_seconds=5)
        valid = await manager.wait(started["job_id"], ["checkpoint"], timeout_seconds=5)
        assert rejected["event"]["data"] == {"max_bytes": 256}
        assert valid["event"]["message"] == "after big"
        manager.close()

    asyncio.run(main())


@pytest.mark.parametrize(
    "target",
    [
        {"type": "local_command", "events": 7, "command": ["ignored"]},
        {"type": "unknown", "events": []},
        {"type": "local_command", "events": []},
        {"type": "local_command", "events": [], "command": ["ignored"], "max_attempts": "bad"},
    ],
)
def test_invalid_wake_targets_fail_before_launch(tmp_path, target):
    manager = JobManager(tmp_path)
    with pytest.raises(ValueError):
        asyncio.run(manager.start(cmd("pass"), wake_targets=[target]))
    assert manager.list()["jobs"] == []
    manager.close()


def test_recovery_does_not_overwrite_concurrent_completion(tmp_path):
    class SlowDeadCheck(JobManager):
        def _pid_alive(self, pid):
            time.sleep(1)
            return False

    async def start():
        manager = JobManager(tmp_path)
        started = await manager.start(cmd("import time; time.sleep(.3)"))
        await manager.wait(started["job_id"], ["started"], timeout_seconds=5)
        manager.close()
        return started["job_id"]

    job_id = asyncio.run(start())
    recovered = SlowDeadCheck(tmp_path)
    result = recovered.wait_sync(job_id, ["completed"], timeout_seconds=5)
    assert result["result"] == "event"
    assert recovered.status(job_id)["status"] == "completed"
    assert recovered.events(job_id, types=["orphaned"])["events"] == []
    recovered.close()


def test_unknown_job_events_is_an_error(tmp_path):
    manager = JobManager(tmp_path)
    with pytest.raises(ValueError, match="Unknown job_id"):
        manager.events("job_missing")
    manager.close()


@pytest.mark.skipif(sys.platform != "win32", reason="Windows recovered process-tree behavior")
def test_stop_after_restart_kills_runner_and_workload(tmp_path):
    async def start():
        manager = JobManager(tmp_path)
        started = await manager.start(cmd("import time; time.sleep(30)"))
        await manager.wait(started["job_id"], ["started"], timeout_seconds=5)
        status = manager.status(started["job_id"])
        manager.close()
        return started["job_id"], status["worker_pid"], status["pid"]

    job_id, worker_pid, pid = asyncio.run(start())
    recovered = JobManager(tmp_path)
    stopped = recovered.stop_sync(job_id, kill_after_seconds=0)
    assert stopped["status"] == "cancelled"
    # Process teardown is asynchronous on Windows; give the OS a bounded window
    # to reap the runner/workload before asserting they are gone.
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and (recovered._pid_alive(worker_pid) or recovered._pid_alive(pid)):
        time.sleep(0.05)
    assert not recovered._pid_alive(worker_pid)
    assert not recovered._pid_alive(pid)
    recovered.close()


def test_live_manager_detects_runner_disappearance(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        started = await manager.start(cmd("import time; time.sleep(30)"))
        await manager.wait(started["job_id"], ["started"], timeout_seconds=5)
        status = manager.status(started["job_id"])
        manager._kill_pid(status["worker_pid"], force=True)
        orphaned = await manager.wait(started["job_id"], ["orphaned"], timeout_seconds=10)
        assert orphaned["result"] == "event"
        assert manager.status(started["job_id"])["status"] == "orphaned"
        assert not manager._pid_alive(status["pid"])
        manager.close()

    asyncio.run(main())


def test_runner_popen_failure_marks_job_failed(tmp_path, monkeypatch):
    real_popen = subprocess.Popen

    def failing_popen(argv, *args, **kwargs):
        if "-m" in argv and "vanth.runner" in argv:
            raise OSError("venv python missing")
        return real_popen(argv, *args, **kwargs)

    monkeypatch.setattr(subprocess, "Popen", failing_popen)
    manager = JobManager(tmp_path)

    async def main():
        started = await manager.start(cmd("import time; time.sleep(30)"))
        status = manager.status(started["job_id"])
        events = manager.events(started["job_id"], types=["failed"])["events"]
        assert status["status"] == "failed"
        assert status["exit_code"] == 1
        assert started["status"] == "failed"
        assert started["exit_code"] == 1
        assert started["worker_pid"] is None
        assert started["message"].startswith("Job runner failed to start:")
        assert events and events[0]["source"] == "server"
        assert events[0]["message"].startswith("Job runner failed to start:")
        assert not (manager.home / "specs" / f"{started['job_id']}.json").exists()
        assert started["job_id"] not in manager.processes

    asyncio.run(main())
    manager.close()


def test_stale_stop_does_not_cancel_replacement_launch(tmp_path):
    """Review rc39 P1: the real _stop interleaving must never cancel a
    replacement launch owner.

    _stop's FIRST read observes claim A. Between that read and the stop-request
    write, a finish+restart/recovery installs claim B (and promotes to
    'running'). The stop-request UPDATE is ownership-CAS'd on claim A, so it
    affects zero rows and _stop returns WITHOUT setting the stop flag or
    transitioning B. This drives the real _stop path (not _transition_terminal
    in isolation)."""
    from vanth.server import now_iso

    manager = JobManager(tmp_path, recover=False)
    try:
        job_id = "job_stale_stop"
        stdout_path = manager.logs / f"{job_id}.stdout.log"
        stderr_path = manager.logs / f"{job_id}.stderr.log"
        events_path = manager.events_dir / f"{job_id}.jsonl"
        # Insert a 'launching' row owned by claim A.
        claim_a = "claim_A"
        stamp = now_iso()
        with manager.db_lock:
            manager.db.execute(
                "INSERT INTO jobs(job_id, command, status, created_at, updated_at, stdout_path, stderr_path, "
                "events_path, claim_token, worker_pid, pid) VALUES (?, ?, 'launching', ?, ?, ?, ?, ?, ?, NULL, NULL)",
                (job_id, "true", stamp, stamp, str(stdout_path), str(stderr_path), str(events_path), claim_a),
            )
            manager.db.commit()

        # _stop snapshots the row AFTER the first read; to reproduce the race we
        # swap claim A -> claim B between the observation and the stop-request
        # write. Patch _row to return the claim-A snapshot on the FIRST call
        # (the observation) and the real (claim-B) row thereafter, so the
        # stop-request UPDATE runs while the row is already owned by B.
        original_row = manager._row
        calls = {"n": 0}

        def racy_row(sql, params=()):
            calls["n"] += 1
            if calls["n"] == 1 and "claim_token" in sql:
                # The first ownership read returns the ORIGINAL claim-A row, even
                # though the DB already moved on to B.
                row = original_row("SELECT status, worker_pid, pid, stop_requested_at, claim_token FROM jobs WHERE job_id=?", (job_id,))
                if row is None:
                    return None
                return dict((k, (claim_a if k == "claim_token" else row[k])) for k in row.keys())
            return original_row(sql, params)

        # Install claim B and promote to 'running' BEFORE the stop request.
        claim_b = "claim_B"
        with manager.db_lock:
            manager.db.execute(
                "UPDATE jobs SET status='running', claim_token=?, worker_pid=?, pid=? WHERE job_id=? AND claim_token=?",
                (claim_b, os.getpid(), os.getpid(), job_id, claim_a),
            )
            manager.db.commit()

        manager._row = racy_row
        try:
            result = manager.stop_sync(job_id, kill_after_seconds=0)
        finally:
            manager._row = original_row
        # The stale stop must NOT set the stop flag or cancel B.
        assert result["status"] != "cancelled", f"stale stop must not cancel B: {result}"
        assert "newer launch" in result.get("message", ""), f"unexpected message: {result}"
        row = manager._row("SELECT status, claim_token, stop_requested_at FROM jobs WHERE job_id=?", (job_id,))
        assert row["status"] == "running", "the replacement launch must remain running"
        assert row["claim_token"] == claim_b, "the replacement owner must be untouched"
        assert row["stop_requested_at"] is None, "the stop flag must not be set on the replacement launch"
    finally:
        manager.close()


def test_stale_stop_never_kills_replacement_processes(tmp_path):
    """Self-review rc40: even when the stop-request CAS wins on A and B takes
    over before the re-read, _stop must not terminate B's workload/runner, must
    not transition B, and must clear its own flag value so B's runner is not
    poisoned."""
    import subprocess as _sp

    from vanth.server import now_iso

    manager = JobManager(tmp_path, recover=False)
    sleeper = _sp.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        job_id = "job_stale_stop_pids"
        stdout_path = manager.logs / f"{job_id}.stdout.log"
        stderr_path = manager.logs / f"{job_id}.stderr.log"
        events_path = manager.events_dir / f"{job_id}.jsonl"
        claim_a = "claim_A2"
        stamp = now_iso()
        with manager.db_lock:
            manager.db.execute(
                "INSERT INTO jobs(job_id, command, status, created_at, updated_at, stdout_path, stderr_path, "
                "events_path, claim_token, worker_pid, pid) VALUES (?, ?, 'launching', ?, ?, ?, ?, ?, ?, NULL, NULL)",
                (job_id, "true", stamp, stamp, str(stdout_path), str(stderr_path), str(events_path), claim_a),
            )
            manager.db.commit()

        # On the SECOND _row call (the post-CAS re-read), install claim B with
        # a LIVE workload/runner pid first, then return the real row.
        claim_b = "claim_B2"
        original_row = manager._row
        calls = {"n": 0}

        def racy_row(sql, params=()):
            calls["n"] += 1
            if calls["n"] == 2:
                with manager.db_lock:
                    manager.db.execute(
                        "UPDATE jobs SET status='running', claim_token=?, worker_pid=?, pid=? WHERE job_id=?",
                        (claim_b, sleeper.pid, sleeper.pid, job_id),
                    )
                    manager.db.commit()
            return original_row(sql, params)

        manager._row = racy_row
        try:
            result = manager.stop_sync(job_id, kill_after_seconds=0)
        finally:
            manager._row = original_row
        assert "newer launch" in result.get("message", ""), f"unexpected: {result}"
        # B's live process must survive the stale stop.
        assert manager._pid_alive(sleeper.pid), "stale stop must not kill the replacement's process"
        row = manager._row("SELECT status, claim_token, stop_requested_at FROM jobs WHERE job_id=?", (job_id,))
        assert row["status"] == "running"
        assert row["claim_token"] == claim_b
        assert row["stop_requested_at"] is None, "our flag value must be cleared so B is not poisoned"
    finally:
        try:
            sleeper.terminate()
            sleeper.wait(timeout=5)
        except Exception:
            pass
        manager.close()


def test_identityless_legacy_stop_never_kills_new_owner(tmp_path):
    """A legacy row with no claim or PID cannot authenticate a PID that appears
    after the stop CAS; treat it as a replacement and fail safe."""
    import subprocess as _sp

    from vanth.server import now_iso

    manager = JobManager(tmp_path, recover=False)
    sleeper = _sp.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        job_id = "job_identityless_stop"
        stamp = now_iso()
        with manager.db_lock:
            manager.db.execute(
                "INSERT INTO jobs(job_id, command, status, created_at, updated_at, stdout_path, stderr_path, "
                "events_path, claim_token, worker_pid, pid) VALUES (?, ?, 'launching', ?, ?, ?, ?, ?, NULL, NULL, NULL)",
                (
                    job_id,
                    "true",
                    stamp,
                    stamp,
                    str(manager.logs / f"{job_id}.stdout.log"),
                    str(manager.logs / f"{job_id}.stderr.log"),
                    str(manager.events_dir / f"{job_id}.jsonl"),
                ),
            )
            manager.db.commit()

        original_row = manager._row
        calls = {"n": 0}

        def racy_row(sql, params=()):
            calls["n"] += 1
            if calls["n"] == 2:
                with manager.db_lock:
                    manager.db.execute(
                        "UPDATE jobs SET status='running', claim_token=?, worker_pid=?, pid=? WHERE job_id=?",
                        ("claim_new", sleeper.pid, sleeper.pid, job_id),
                    )
                    manager.db.commit()
            return original_row(sql, params)

        manager._row = racy_row
        try:
            result = manager.stop_sync(job_id, kill_after_seconds=0)
        finally:
            manager._row = original_row

        assert "newer launch" in result.get("message", "")
        assert manager._pid_alive(sleeper.pid)
        row = manager._row(
            "SELECT status, claim_token, stop_requested_at FROM jobs WHERE job_id=?",
            (job_id,),
        )
        assert row["status"] == "running"
        assert row["claim_token"] == "claim_new"
        assert row["stop_requested_at"] is None
    finally:
        try:
            sleeper.terminate()
            sleeper.wait(timeout=5)
        except Exception:
            pass
        manager.close()


def test_job_start_mcp_tool_is_not_a_coroutine_function():
    from vanth.server import mcp

    tool = mcp._tool_manager._tools["job_start"]
    assert inspect.iscoroutinefunction(tool.fn) is False


def test_launch_claim_clears_stale_workload_pid(tmp_path):
    """A re-claim must not carry the previous run's workload pid.

    The row keeps its old `pid` after a failure, so an abandoned re-claim would
    force-kill whatever process now holds that (recycled) pid. Only the runner
    may publish a workload pid, at promotion."""
    from vanth.server import now_iso

    manager = JobManager(tmp_path, recover=False)
    try:
        job_id = "job_reclaim_pid"
        stamp = now_iso()
        with manager.db_lock:
            manager.db.execute(
                "INSERT INTO jobs(job_id, command, status, created_at, updated_at, stdout_path, stderr_path, "
                "events_path, pid, worker_pid, runner_heartbeat_at) "
                "VALUES (?, ?, 'failed', ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    job_id,
                    "true",
                    stamp,
                    stamp,
                    str(manager.logs / f"{job_id}.stdout.log"),
                    str(manager.logs / f"{job_id}.stderr.log"),
                    str(manager.events_dir / f"{job_id}.jsonl"),
                    os.getpid(),
                    os.getpid(),
                    stamp,
                ),
            )
            manager.db.commit()

        token = manager._claim_launch(job_id)
        assert token
        row = manager._row(
            "SELECT status, pid, worker_pid, runner_heartbeat_at FROM jobs WHERE job_id=?", (job_id,)
        )
        assert row["status"] == "launching"
        assert row["pid"] is None
        assert row["worker_pid"] is None
        assert row["runner_heartbeat_at"] is None
    finally:
        manager.close()


def test_launch_retries_transient_lock_on_worker_pid_write(tmp_path, monkeypatch):
    """_launch's post-spawn worker_pid write must go through _retry_locked.

    An uncaught 'database is locked' there would abort start() after the runner
    already spawned, leaking a live runner the caller never learned about. Assert
    the write is retried (the fn passed to _retry_locked raises once, then the
    real write runs)."""
    import sqlite3
    import vanth.server as server_module

    manager = JobManager(tmp_path, recover=False)
    try:
        token = "claim_" + "a" * 16
        with manager.db_lock:
            manager.db.execute(
                "INSERT INTO jobs(job_id, command, status, created_at, updated_at, stdout_path, stderr_path, "
                "events_path, claim_token) VALUES ('job_lock', 'true', 'launching', ?, ?, ?, ?, ?, ?)",
                (
                    server_module.now_iso(), server_module.now_iso(),
                    str(manager.logs / "job_lock.stdout.log"),
                    str(manager.logs / "job_lock.stderr.log"),
                    str(manager.events_dir / "job_lock.jsonl"),
                    token,
                ),
            )
            manager.db.commit()

        real_retry = manager._retry_locked
        state = {"locked_once": False, "retries": 0}

        def retry_then_inject(fn, *args, **kwargs):
            def flaky():
                if not state["locked_once"]:
                    state["locked_once"] = True
                    raise sqlite3.OperationalError("database is locked")
                return fn()
            return real_retry(flaky, *args, **kwargs)

        monkeypatch.setattr(manager, "_retry_locked", retry_then_inject)

        released = threading.Event()

        class FakeProc:
            pid = 4242

            def wait(self, timeout=None):
                released.wait(5)
                return 0

            def poll(self):
                return None

        monkeypatch.setattr(server_module.subprocess, "Popen", lambda *a, **k: FakeProc())
        try:
            result = manager._launch(
                "job_lock",
                manager.logs / "job_lock.stdout.log",
                manager.logs / "job_lock.stderr.log",
                manager.events_dir / "job_lock.jsonl",
                manager.specs_dir / "job_lock.json",
                claim_token=token,
            )
        finally:
            released.set()
        assert state["locked_once"], "worker_pid write should not have raised before retry"
        assert result["status"] == "running"
        assert result["worker_pid"] == 4242
    finally:
        manager.close()


def test_close_checkpoints_and_truncates_wal(tmp_path):
    from vanth.server import now_iso

    manager = JobManager(tmp_path / "state", recover=False)
    for i in range(500):
        manager.db.execute(
            "INSERT INTO events(event_id,job_id,seq,type,created_at) VALUES (?,?,?,?,?)",
            (f"evt_{i}", "job_x", i, "log", now_iso()),
        )
    manager.db.commit()
    wal = tmp_path / "state" / "jobs.sqlite-wal"
    assert wal.exists() and wal.stat().st_size > 0

    manager.close()

    # A clean close truncates the WAL (file removed or zero-length).
    assert (not wal.exists()) or wal.stat().st_size == 0


def test_checkpoint_wal_runs_without_error(tmp_path):
    manager = JobManager(tmp_path / "state", recover=False)
    try:
        manager._checkpoint_wal()
    finally:
        manager.close()
