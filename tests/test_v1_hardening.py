import asyncio
import json
import os
import shlex
import sqlite3
import subprocess
import sys
import threading
import time

import pytest

from vanth.client import VanthClient
from vanth.runner import _publish_workload
from vanth.server import JobManager


import shellcmd


def cmd(code: str) -> str:
    return shellcmd.join([sys.executable, "-c", code])


def wait_event(manager: JobManager, job_id: str, event_type: str) -> dict:
    return asyncio.run(manager.wait(job_id, [event_type], timeout_seconds=10))


def test_future_schema_is_rejected_and_existing_schema_migrates_with_backup(tmp_path):
    home = tmp_path / "state"
    manager = JobManager(home)
    stamp = "2026-01-01T00:00:00Z"
    manager.db.execute(
        "INSERT INTO jobs(job_id, command, status, created_at, updated_at, stdout_path, stderr_path, events_path) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("job_old", "echo old", "completed", stamp, stamp, "out", "err", "events"),
    )
    manager.db.execute("PRAGMA user_version=0")
    manager.db.commit()
    manager.close()

    migrated = JobManager(home)
    from vanth.migrations import LATEST_SCHEMA_VERSION

    assert migrated.status("job_old")["status"] == "completed"
    assert migrated.doctor()["schema_version"] == LATEST_SCHEMA_VERSION
    assert len(list((home / "backups").glob("*.sqlite"))) == 1
    migrated.close()

    db = sqlite3.connect(home / "jobs.sqlite")
    db.execute("PRAGMA user_version=999")
    db.commit()
    db.close()
    with pytest.raises(RuntimeError, match="newer"):
        JobManager(home)


def test_manual_delivery_is_not_polled_or_threaded(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        started = manager.start(
                cmd("print('AGENT_EVENT {\\\"type\\\":\\\"checkpoint\\\"}', flush=True)"),
                wake_targets=[{"type": "codex_thread", "thread_id": "thread", "events": ["checkpoint"], "auto_dispatch": False}],
            
        )
        wait_event(manager, started["job_id"], "checkpoint")
        for _ in range(5):
            manager._dispatch_due_deliveries()
            time.sleep(0.02)
        assert not [thread for thread in manager._delivery_threads if thread.is_alive()]
    finally:
        manager.close()


def test_expired_delivery_reclaim_rejects_stale_completion(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        started = manager.start(
                cmd("print('AGENT_EVENT {\\\"type\\\":\\\"checkpoint\\\"}', flush=True)"),
                wake_targets=[{"type": "codex_thread", "thread_id": "thread", "events": ["checkpoint"], "auto_dispatch": False}],
            
        )
        wait_event(manager, started["job_id"], "checkpoint")
        delivery = manager.deliveries(started["job_id"])["deliveries"][0]
        first = manager._claim_delivery(delivery["delivery_id"])
        manager.db.execute("UPDATE deliveries SET lease_expires_at='1970-01-01T00:00:00Z' WHERE delivery_id=?", (delivery["delivery_id"],))
        manager.db.commit()
        second = manager._claim_delivery(delivery["delivery_id"])
        assert first["claim_token"] != second["claim_token"]
        assert manager._complete_delivery(first, "delivered")["status"] == "dispatching"
        assert manager._complete_delivery(second, "delivered")["status"] == "delivered"
        attempts = manager.delivery_attempts(delivery["delivery_id"])["attempts"]
        assert {attempt["status"] for attempt in attempts} == {"reclaimed", "delivered"}
    finally:
        manager.close()


def test_shutdown_wakes_active_wait(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        started = manager.start(cmd("import time; time.sleep(30)"))
        result = {}

        def wait() -> None:
            result.update(manager.wait_sync(started["job_id"], ["completed"], timeout_seconds=30))

        thread = threading.Thread(target=wait)
        thread.start()
        time.sleep(0.1)
        manager.begin_shutdown()
        thread.join(timeout=2)
        assert result["result"] == "shutdown"
    finally:
        manager.close()


def test_cleanup_removes_all_job_artifacts_and_is_idempotent(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        started = manager.start(cmd("print('done')"))
        wait_event(manager, started["job_id"], "completed")
        runner_log = manager.logs / f"{started['job_id']}.runner.log"
        spec = manager.home / "specs" / f"{started['job_id']}.json"
        runner_log.write_text("diagnostic", encoding="utf-8")
        spec.write_text("{}", encoding="utf-8")
        # Under heavy suite load the runner can still hold handles when the
        # status flips; converge with a deadline instead of a single shot.
        import time as _time

        deadline = _time.monotonic() + 30
        while _time.monotonic() < deadline:
            manager.cleanup(0, dry_run=False)
            if not runner_log.exists() and not spec.exists():
                break
            _time.sleep(0.05)
        assert not runner_log.exists() and not spec.exists()
        assert manager.cleanup(0, dry_run=False)["count"] == 0
    finally:
        manager.close()


def test_client_readiness_requires_matching_authenticated_home(tmp_path):
    client = VanthClient("http://127.0.0.1:1", tmp_path / "state")
    client.get = lambda path, params=None: {"result": "error", "error": "Unauthorized"}
    assert not client._ready()


def test_cleanup_dry_run_does_not_drain_existing_tombstones(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        artifact = manager.logs / "held.log"
        artifact.write_text("keep", encoding="utf-8")
        manager.db.execute(
            "INSERT INTO cleanup_tombstones(tombstone_id, job_id, artifacts_json, created_at) VALUES (?, ?, ?, ?)",
            ("clean_existing", "job_old", json.dumps([str(artifact)]), "2026-01-01T00:00:00Z"),
        )
        manager.db.commit()
        manager.cleanup(0, dry_run=True)
        assert artifact.exists()
        assert manager.db.execute("SELECT 1 FROM cleanup_tombstones WHERE tombstone_id='clean_existing'").fetchone()
    finally:
        manager.close()


def test_stop_failure_leaves_running_job_retryable(tmp_path, monkeypatch):
    manager = JobManager(tmp_path / "state")
    try:
        stamp = "2026-01-01T00:00:00Z"
        manager.db.execute(
            "INSERT INTO jobs(job_id, command, status, pid, worker_pid, created_at, updated_at, stdout_path, stderr_path, events_path) VALUES (?, ?, 'running', ?, ?, ?, ?, ?, ?, ?)",
            (
                "job_stop_retry",
                "sleep 30",
                999991,
                999992,
                stamp,
                stamp,
                str(manager.logs / "stop.out"),
                str(manager.logs / "stop.err"),
                str(manager.events_dir / "stop.jsonl"),
            ),
        )
        manager.db.commit()
        original = manager._terminate_pid
        monkeypatch.setattr(manager, "_terminate_pid", lambda *args, **kwargs: False)
        with pytest.raises(RuntimeError, match="workload"):
            manager.stop_sync("job_stop_retry", kill_after_seconds=0)
        assert manager.status("job_stop_retry")["status"] == "running"
        assert manager.events("job_stop_retry")["events"] == []

        monkeypatch.setattr(manager, "_terminate_pid", original)
        assert manager.stop_sync("job_stop_retry", kill_after_seconds=0)["status"] == "cancelled"
        assert [event["type"] for event in manager.events("job_stop_retry")["events"]] == ["cancelled"]
    finally:
        manager.close()


def test_stop_intent_and_pid_publication_interleavings(tmp_path):
    manager = JobManager(tmp_path / "state")
    # Stop the maintenance loop: it would otherwise reconcile the synthetic
    # 'running' row (which has no live pid) to 'orphaned' between the
    # interleavings, so the guarded publish/stop UPDATEs would see a non-running
    # row and the test would flake.
    manager.dispatcher_stop.set()
    if manager.dispatcher_thread is not None:
        manager.dispatcher_thread.join(timeout=5)
    stamp = "2026-01-01T00:00:00Z"
    with manager.db_lock:
        manager.db.execute(
            "INSERT INTO jobs(job_id, command, status, created_at, updated_at, stdout_path, stderr_path, events_path) VALUES (?, ?, 'running', ?, ?, ?, ?, ?)",
            ("job_pid_race", "sleep 30", stamp, stamp, "out", "err", "events"),
        )
        manager.db.commit()

    def guarded(fn):
        # Surface worker-thread failures (e.g. a direct DB write racing the
        # manager's background threads and raising "database is locked") instead
        # of letting the thread die silently and the assertion see a stale row.
        def wrapper():
            try:
                fn()
            except BaseException as exc:  # noqa: BLE001 - re-raised below
                errors.append(exc)

        return wrapper

    def join_all(threads):
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
        assert not any(thread.is_alive() for thread in threads), "worker threads did not finish"
        if errors:
            raise errors[0]

    try:
        errors: list[BaseException] = []
        barrier = threading.Barrier(2)
        intent_done = threading.Event()

        def intent_wins():
            barrier.wait()
            with manager.db_lock:
                manager.db.execute("UPDATE jobs SET stop_requested_at=? WHERE job_id=? AND status='running'", (stamp, "job_pid_race"))
                manager.db.commit()
            intent_done.set()

        def publish_after_intent():
            barrier.wait()
            intent_done.wait(timeout=2)
            assert not _publish_workload(manager, "job_pid_race", 123)

        join_all([threading.Thread(target=guarded(intent_wins)), threading.Thread(target=guarded(publish_after_intent))])
        assert manager._row("SELECT pid FROM jobs WHERE job_id=?", ("job_pid_race",))["pid"] is None

        with manager.db_lock:
            manager.db.execute("UPDATE jobs SET stop_requested_at=NULL WHERE job_id=?", ("job_pid_race",))
            manager.db.commit()

        barrier = threading.Barrier(2)
        published = threading.Event()

        def publish_before_intent():
            barrier.wait()
            assert _publish_workload(manager, "job_pid_race", 456)
            published.set()

        def intent_after_publish():
            barrier.wait()
            published.wait(timeout=2)
            with manager.db_lock:
                manager.db.execute("UPDATE jobs SET stop_requested_at=? WHERE job_id=? AND status='running'", (stamp, "job_pid_race"))
                manager.db.commit()

        join_all([threading.Thread(target=guarded(publish_before_intent)), threading.Thread(target=guarded(intent_after_publish))])
        row = manager._row("SELECT pid, stop_requested_at FROM jobs WHERE job_id=?", ("job_pid_race",))
        assert row["pid"] == 456 and row["stop_requested_at"] == stamp
    finally:
        manager.close()


def test_recovery_and_watcher_keep_running_when_workload_cleanup_fails(tmp_path, monkeypatch):
    manager = JobManager(tmp_path / "state")
    stamp = "2026-01-01T00:00:00Z"
    manager.db.execute(
        "INSERT INTO jobs(job_id, command, status, pid, worker_pid, created_at, updated_at, stdout_path, stderr_path, events_path) VALUES (?, ?, 'running', ?, ?, ?, ?, ?, ?, ?)",
        ("job_cleanup_failure", "sleep 30", 999991, 999992, stamp, stamp, "out", "err", "events"),
    )
    manager.db.commit()
    monkeypatch.setattr(manager, "_pid_alive", lambda pid: False)
    monkeypatch.setattr(manager, "_terminate_pid", lambda *args, **kwargs: False)

    class DeadRunner:
        def wait(self):
            return 1

    try:
        manager._recover_jobs()
        manager._watch_runner("job_cleanup_failure", DeadRunner())
        assert manager.status("job_cleanup_failure")["status"] == "running"
        assert manager.events("job_cleanup_failure")["events"] == []
    finally:
        manager.close()


@pytest.mark.skipif(sys.platform == "win32", reason="Linux process-group regression")
def test_stop_after_restart_kills_unix_child_and_grandchild(tmp_path):
    manager = JobManager(tmp_path / "state")
    pids = tmp_path / "pids"
    code = (
        "import os,pathlib,subprocess,sys,time; "
        "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)']); "
        f"pathlib.Path({str(pids)!r}).write_text(str(os.getpid())+' '+str(child.pid)); "
        "time.sleep(30)"
    )
    command = shlex.join([sys.executable, "-c", code])
    try:
        started = manager.start(command)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not pids.exists():
            time.sleep(0.05)
        assert pids.exists()
        manager.close()
        restarted = JobManager(tmp_path / "state")
        try:
            stopped = restarted.stop_sync(started["job_id"], kill_after_seconds=3)
            assert stopped["status"] == "cancelled"
            child_pid, grandchild_pid = map(int, pids.read_text().split())
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline and (restarted._pid_alive(child_pid) or restarted._pid_alive(grandchild_pid)):
                time.sleep(0.05)
            assert not restarted._pid_alive(child_pid)
            assert not restarted._pid_alive(grandchild_pid)
        finally:
            restarted.close()
    finally:
        if not manager._closed:
            manager.close()
