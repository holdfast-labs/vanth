"""Production storage fault injection with isolated state and real SQLite locks."""
import errno
import json
import sqlite3
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from vanth.server import JobManager, now_iso


@pytest.fixture
def manager(tmp_path):
    value = JobManager(tmp_path, recover=False)
    stamp = now_iso()
    value.db.execute(
        "INSERT INTO jobs(job_id,command,status,created_at,updated_at,stdout_path,stderr_path,events_path) "
        "VALUES ('job_fault','unused','running',?,?,?,?,?)",
        (stamp, stamp, str(value.logs / "fault.stdout"), str(value.logs / "fault.stderr"),
         str(value.events_dir / "fault.jsonl")))
    value.db.commit()
    try:
        yield value
    finally:
        value.close()


def test_real_sqlite_write_contention_release_preserves_one_terminal_event(manager, monkeypatch, caplog):
    manager.db.execute("PRAGMA busy_timeout=10")
    blocker = sqlite3.connect(manager.home / "jobs.sqlite")
    blocker.execute("BEGIN IMMEDIATE")
    entered, finished = threading.Event(), threading.Event()
    errors = []
    original = manager._emit_transactional

    def observe_attempt(*args, **kwargs):
        entered.set()
        return original(*args, **kwargs)
    monkeypatch.setattr(manager, "_emit_transactional", observe_attempt)

    def complete():
        try:
            manager._finish("job_fault", "completed", 0)
        except BaseException as exc:
            errors.append(exc)
        finally:
            finished.set()

    worker = threading.Thread(target=complete, name="fault-terminal-writer", daemon=True)
    worker.start()
    try:
        assert entered.wait(2)
        assert not finished.wait(.06)
        blocker.commit()
        assert finished.wait(3)
        assert not errors
        assert "event write contended" in caplog.text
        assert manager.status("job_fault")["status"] == "completed"
        assert manager.status("job_fault")["exit_code"] == 0
        assert [(row["seq"], row["type"]) for row in manager.db.execute(
            "SELECT seq,type FROM events WHERE job_id='job_fault'")] == [(1, "completed")]
        manager._finish("job_fault", "completed", 0)
        with sqlite3.connect(manager.home / "jobs.sqlite") as observer:
            assert observer.execute("SELECT status FROM jobs WHERE job_id='job_fault'").fetchone()[0] == "completed"
            assert observer.execute("SELECT COUNT(*) FROM events WHERE job_id='job_fault'").fetchone()[0] == 1
    finally:
        blocker.rollback()
        worker.join(timeout=3)
        blocker.close()


def test_terminal_real_lock_retry_exhaustion_keeps_job_retryable(manager, monkeypatch):
    from vanth import server

    manager.db.execute("PRAGMA busy_timeout=1")
    monkeypatch.setattr(server.time, "sleep", lambda duration: None)
    blocker = sqlite3.connect(manager.home / "jobs.sqlite")
    blocker.execute("BEGIN IMMEDIATE")
    try:
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            manager._finish("job_fault", "completed", 0)
        assert manager.status("job_fault")["status"] == "running"
        assert manager.db.execute("SELECT COUNT(*) FROM events WHERE job_id='job_fault'").fetchone()[0] == 0
        assert not (manager.events_dir / "fault.jsonl").exists()
    finally:
        blocker.rollback()
        blocker.close()
    manager._finish("job_fault", "completed", 0)
    manager._finish("job_fault", "completed", 0)
    assert manager.status("job_fault")["status"] == "completed"
    assert manager.db.execute("SELECT COUNT(*) FROM events WHERE job_id='job_fault' AND type='completed'").fetchone()[0] == 1


def test_terminal_event_insert_retry_exhaustion_rolls_back_state_and_mirror(manager, monkeypatch):
    from vanth import server

    original = manager._enqueue_deliveries_uncommitted
    attempts = []
    monkeypatch.setattr(server.time, "sleep", lambda duration: None)

    def fail_after_event_insert(event):
        attempts.append(event["event_id"])
        # The state and event exist only inside the uncommitted transaction;
        # another connection must still see the original running job.
        with sqlite3.connect(manager.home / "jobs.sqlite") as observer:
            assert observer.execute("SELECT status FROM jobs WHERE job_id='job_fault'").fetchone()[0] == "running"
            assert observer.execute("SELECT COUNT(*) FROM events WHERE job_id='job_fault'").fetchone()[0] == 0
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(manager, "_enqueue_deliveries_uncommitted", fail_after_event_insert)
    with pytest.raises(sqlite3.OperationalError, match="locked"):
        manager._finish("job_fault", "completed", 0)
    assert len(attempts) == 10
    assert manager.status("job_fault")["status"] == "running"
    assert manager.db.execute("SELECT ended_at FROM jobs WHERE job_id='job_fault'").fetchone()[0] is None
    assert manager.db.execute("SELECT COUNT(*) FROM events WHERE job_id='job_fault'").fetchone()[0] == 0
    assert not (manager.events_dir / "fault.jsonl").exists()
    monkeypatch.setattr(manager, "_enqueue_deliveries_uncommitted", original)
    manager._finish("job_fault", "completed", 0)
    manager._finish("job_fault", "completed", 0)
    assert manager.status("job_fault")["status"] == "completed"
    assert manager.db.execute("SELECT COUNT(*) FROM events WHERE job_id='job_fault' AND type='completed'").fetchone()[0] == 1
    mirror = [json.loads(line) for line in (manager.events_dir / "fault.jsonl").read_text().splitlines()]
    assert [event["type"] for event in mirror] == ["completed"]


@pytest.mark.parametrize("phase", ["open", "write"])
@pytest.mark.parametrize("fault_errno", [errno.EACCES, errno.ENOSPC], ids=["permission-denied", "disk-full"])
def test_log_sink_faults_drain_pipes_persist_diagnostic_and_fail_job(manager, monkeypatch, phase, fault_errno):
    sink_path = manager.logs / "fault.stdout"
    original_open = Path.open
    reads = []
    chunks = [b"AGENT_EVENT " + json.dumps({"type": "metric", "data": {"i": i}}).encode() + b"\n"
              for i in range(3)]
    error_class = PermissionError if fault_errno == errno.EACCES else OSError

    class Stream:
        def read1(self, size):
            data = chunks.pop(0) if chunks else b""
            reads.append(data)
            return data
        read = read1

    class FailingSink:
        def __init__(self, handle):
            self.handle = handle
            self.writes = 0
        def write(self, data):
            self.writes += 1
            if self.writes == 2:
                raise error_class(fault_errno, "injected log write fault")
            return self.handle.write(data)
        def flush(self):
            self.handle.flush()
        def close(self):
            self.handle.close()

    def fault_open(path, *args, **kwargs):
        if path == sink_path:
            if phase == "open":
                raise error_class(fault_errno, "injected log open fault")
            return FailingSink(original_open(path, *args, **kwargs))
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", fault_open)
    manager._read_stream("job_fault", Stream(), sink_path, "stdout")
    assert len(reads) == 4 and reads[-1] == b"", "reader must keep draining through EOF after storage failure"
    assert "job_fault" in manager._capture_failed
    metric_rows = manager.db.execute("SELECT data_json FROM events WHERE job_id='job_fault' AND type='metric' ORDER BY seq").fetchall()
    assert [json.loads(row[0])["i"] for row in metric_rows] == [0, 1, 2]
    diagnostic = manager.db.execute("SELECT data_json FROM events WHERE job_id='job_fault' AND type='log_capture_failed'").fetchall()
    assert len(diagnostic) == 1
    assert "injected log" in json.loads(diagnostic[0][0])["error"]
    assert json.loads(diagnostic[0][0])["stream"] == "stdout"
    # Workload success is insufficient when durable log capture failed.
    manager._watch("job_fault", SimpleNamespace(wait=lambda timeout=None: 0), timeout_seconds=None)
    assert manager.status("job_fault")["status"] == "failed"
    assert manager.db.execute("SELECT COUNT(*) FROM events WHERE job_id='job_fault' AND type='failed'").fetchone()[0] == 1


@pytest.mark.parametrize("state,guards", [
    (("running", "claim_new", 9), {"claim_token": "claim_old"}),
    (("running", None, 9), {"worker_pid": 7}),
    (("launching", "claim_old", 9), {"claim_token": "claim_old", "require_launching": True, "expected_worker_pid": 7}),
    (("running", "claim_old", 7), {"claim_token": "claim_old", "require_launching": True, "expected_worker_pid": 7}),
])
def test_atomic_terminal_event_refuses_replacement_claim_worker_and_promoted_launch(manager, state, guards):
    manager.db.execute("UPDATE jobs SET status=?,claim_token=?,worker_pid=? WHERE job_id='job_fault'", state)
    manager.db.commit()
    cleanup = []
    assert not manager._terminal_event("job_fault", "orphaned", before_commit=lambda: cleanup.append(True) or True, **guards)
    assert not cleanup, "losing ownership must never run cleanup"
    assert manager.db.execute("SELECT status,claim_token,worker_pid FROM jobs WHERE job_id='job_fault'").fetchone()[:] == state
    assert manager.db.execute("SELECT COUNT(*) FROM events WHERE job_id='job_fault'").fetchone()[0] == 0
    assert not (manager.events_dir / "fault.jsonl").exists()


def test_abandoned_claim_cleanup_failure_rolls_back_terminal_state_and_event(manager, monkeypatch):
    manager.db.execute("UPDATE jobs SET status='launching',claim_token='claim_owned',worker_pid=7,pid=42 WHERE job_id='job_fault'")
    manager.db.commit()
    calls = []

    def failed_cleanup(pid, **kwargs):
        calls.append(pid)
        # The ownership CAS is won before cleanup, but no terminal state is
        # visible to another process until cleanup and event persistence succeed.
        assert manager.status("job_fault")["status"] == "orphaned"
        with sqlite3.connect(manager.home / "jobs.sqlite") as observer:
            assert observer.execute("SELECT status FROM jobs WHERE job_id='job_fault'").fetchone()[0] == "launching"
        return False

    monkeypatch.setattr(manager, "_terminate_pid", failed_cleanup)
    recovered, target = manager._abandon_launch_claim("job_fault", "claim_owned", expected_worker_pid=7)
    assert not recovered and target == "orphaned"
    assert calls == [42]
    assert manager.status("job_fault")["status"] == "launching"
    assert manager.db.execute("SELECT COUNT(*) FROM events WHERE job_id='job_fault'").fetchone()[0] == 0
    monkeypatch.setattr(manager, "_terminate_pid", lambda *args, **kwargs: True)
    assert manager._abandon_launch_claim("job_fault", "claim_owned", expected_worker_pid=7)[0]
    assert manager.status("job_fault")["status"] == "orphaned"
    assert manager.db.execute("SELECT COUNT(*) FROM events WHERE job_id='job_fault' AND type='orphaned'").fetchone()[0] == 1


def test_legacy_abandoned_claim_worker_guard_preserves_newer_owner(manager, monkeypatch):
    manager.db.execute("UPDATE jobs SET status='launching',worker_pid=9,pid=42 WHERE job_id='job_fault'")
    manager.db.commit()
    killed = []
    monkeypatch.setattr(manager, "_terminate_pid", lambda pid, **kwargs: killed.append(pid) or True)
    assert not manager._abandon_launch_claim("job_fault", None, expected_worker_pid=7)[0]
    assert not killed
    assert manager.status("job_fault")["status"] == "launching"
    assert manager._abandon_launch_claim("job_fault", None, expected_worker_pid=9)[0]
    assert killed == [42]
    assert manager.status("job_fault")["status"] == "orphaned"


def test_taskkill_timeout_rolls_back_owned_abandoned_claim(manager, monkeypatch):
    import subprocess
    from vanth import server

    manager.db.execute("UPDATE jobs SET status='launching',claim_token='claim_owned',worker_pid=7,pid=42 WHERE job_id='job_fault'")
    manager.db.commit()
    manager.recovery_kill_timeout = .25
    monkeypatch.setattr(server.sys, "platform", "win32")
    monkeypatch.setattr(manager, "_pid_alive", lambda pid: True)
    observed_timeouts = []

    def stalled_taskkill(args, **kwargs):
        assert args[0] == "taskkill" and args[2] == "42"
        observed_timeouts.append(kwargs["timeout"])
        raise subprocess.TimeoutExpired(args, kwargs["timeout"])

    monkeypatch.setattr(server.subprocess, "run", stalled_taskkill)
    assert not manager._abandon_launch_claim("job_fault", "claim_owned", expected_worker_pid=7)[0]
    assert observed_timeouts and 0 < observed_timeouts[0] <= 1.0
    assert manager.status("job_fault")["status"] == "launching"
    assert manager.db.execute("SELECT COUNT(*) FROM events WHERE job_id='job_fault'").fetchone()[0] == 0


def test_parent_runner_log_open_failure_records_owned_failure_and_event(manager, monkeypatch):
    manager.db.execute("UPDATE jobs SET status='launching',claim_token='claim_owned' WHERE job_id='job_fault'")
    manager.db.commit()
    original = Path.open

    def fail_runner_log(path, *args, **kwargs):
        if path.name == "job_fault.runner.log":
            raise PermissionError(errno.EACCES, "injected runner log permission denied")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", fail_runner_log)
    result = manager._launch("job_fault", manager.logs / "fault.stdout", manager.logs / "fault.stderr",
                             manager.events_dir / "fault.jsonl", manager.specs_dir / "unused.json", claim_token="claim_owned")
    assert result["status"] == "failed"
    assert result["worker_pid"] is None and "job_fault" not in manager.processes
    assert manager.status("job_fault")["status"] == "failed"
    assert manager.db.execute("SELECT COUNT(*) FROM events WHERE job_id='job_fault' AND type='failed'").fetchone()[0] == 1


def test_queued_cancel_persistence_failure_rolls_back_metadata_and_allows_retry(manager, monkeypatch):
    from vanth import server

    manager.db.execute("UPDATE jobs SET status='queued' WHERE job_id='job_fault'")
    manager.db.commit()
    original = manager._enqueue_deliveries_uncommitted
    monkeypatch.setattr(server.time, "sleep", lambda duration: None)
    monkeypatch.setattr(manager, "_enqueue_deliveries_uncommitted", lambda event: (_ for _ in ()).throw(sqlite3.OperationalError("database is locked")))
    with pytest.raises(sqlite3.OperationalError, match="locked"):
        manager.stop_sync("job_fault", actor="tool", reason="test cancellation")
    assert manager.status("job_fault")["status"] == "queued"
    assert manager.status("job_fault")["stop_actor"] is None
    assert manager.db.execute("SELECT COUNT(*) FROM events WHERE job_id='job_fault'").fetchone()[0] == 0
    monkeypatch.setattr(manager, "_enqueue_deliveries_uncommitted", original)
    assert manager.stop_sync("job_fault", actor="tool", reason="test cancellation")["status"] == "cancelled"
    assert manager.db.execute("SELECT COUNT(*) FROM events WHERE job_id='job_fault' AND type='cancelled'").fetchone()[0] == 1
