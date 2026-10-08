"""Pools, priority, and pause/resume for queued jobs (#5)."""

from __future__ import annotations

import sys
import time

import pytest

from vanth.server import JobManager


import shellcmd


def cmd(code: str) -> str:
    return shellcmd.join([sys.executable, "-c", code])


SLEEP = "import time; time.sleep(30)"


def _launched(manager: JobManager, job_id: str) -> bool:
    return manager.status(job_id)["status"] in {"launching", "running"}


def test_pool_job_queues_and_dispatches_by_priority(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    try:
        manager.pool_configure("p", max_parallel=1)
        low = manager.start(cmd(SLEEP), pool="p", priority=0)
        high = manager.start(cmd(SLEEP), pool="p", priority=5)
        assert low["status"] == "queued" and high["status"] == "queued"

        manager._dispatch_queued_jobs()
        assert _launched(manager, high["job_id"]), "higher priority must launch first"
        assert manager.status(low["job_id"])["status"] == "queued", "pool cap 1 must hold the rest"

        manager.stop_sync(high["job_id"])
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and not _launched(manager, low["job_id"]):
            manager._dispatch_queued_jobs()
            time.sleep(0.05)
        assert _launched(manager, low["job_id"]), "freeing capacity must launch the waiter"
        manager.stop_sync(low["job_id"])
    finally:
        manager.close()


def test_pool_pause_holds_then_resume_drains(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    try:
        manager.pool_configure("p", max_parallel=2, paused=True)
        job = manager.start(cmd(SLEEP), pool="p")
        manager._dispatch_queued_jobs()
        assert manager.status(job["job_id"])["status"] == "queued", "paused pool must not launch"

        manager.pool_configure("p", max_parallel=2, paused=False)
        manager._dispatch_queued_jobs()
        assert _launched(manager, job["job_id"])
        manager.stop_sync(job["job_id"])

        listed = manager.pool_list()["pools"]
        assert listed and listed[0]["pool"] == "p"
    finally:
        manager.close()


def test_job_pause_and_resume(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    try:
        manager.pool_configure("p", max_parallel=1)
        job = manager.start(cmd(SLEEP), pool="p")
        manager.job_pause(job["job_id"])
        manager._dispatch_queued_jobs()
        assert manager.status(job["job_id"])["status"] == "queued"

        manager.job_resume(job["job_id"])
        manager._dispatch_queued_jobs()
        assert _launched(manager, job["job_id"])
        manager.stop_sync(job["job_id"])
    finally:
        manager.close()


def test_pause_running_job_rejected(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    try:
        started = manager.start(cmd(SLEEP))
        for _ in range(200):
            if manager.status(started["job_id"])["status"] == "running":
                break
            time.sleep(0.05)
        with pytest.raises(ValueError, match="only a queued job"):
            manager.job_pause(started["job_id"])
        manager.stop_sync(started["job_id"])
    finally:
        manager.close()


def test_global_quota_limits_queued_dispatch(tmp_path, monkeypatch):
    monkeypatch.setenv("VANTH_MAX_RUNNING_JOBS", "1")
    manager = JobManager(tmp_path, recover=False)
    try:
        direct = manager.start(cmd(SLEEP))
        for _ in range(200):
            if manager.status(direct["job_id"])["status"] == "running":
                break
            time.sleep(0.05)
        queued = manager.start(cmd(SLEEP), pool="p")
        assert queued["status"] == "queued"
        manager._dispatch_queued_jobs()
        assert manager.status(queued["job_id"])["status"] == "queued", "global quota must still gate"

        manager.stop_sync(direct["job_id"])
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and not _launched(manager, queued["job_id"]):
            manager._dispatch_queued_jobs()
            time.sleep(0.05)
        assert _launched(manager, queued["job_id"])
        manager.stop_sync(queued["job_id"])
    finally:
        manager.close()


def test_queued_stop_persists_attribution(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    try:
        parent = manager.start(cmd(SLEEP))
        child = manager.start(
            cmd("print('x')"),
            trigger={"job_id": parent["job_id"], "status": "completed"},
        )
        assert child["status"] == "queued"
        manager.stop_sync(child["job_id"], actor="user", reason="changed plan")
        status = manager.status(child["job_id"])
        assert status["status"] == "cancelled"
        assert status["stop_actor"] == "user"
        assert status["stop_reason"] == "changed plan"
        manager.stop_sync(parent["job_id"])
    finally:
        manager.close()


def test_paused_trigger_job_is_still_cancelled(tmp_path):
    """A held job whose trigger parent ends incompatibly must not linger."""
    manager = JobManager(tmp_path, recover=False)
    try:
        parent = manager.start(cmd(SLEEP))
        child = manager.start(
            cmd("print('never')"),
            trigger={"job_id": parent["job_id"], "status": "completed"},
        )
        manager.job_pause(child["job_id"])
        manager.stop_sync(parent["job_id"])  # parent -> cancelled, not completed
        manager._dispatch_queued_jobs()
        assert manager.status(child["job_id"])["status"] == "cancelled"
    finally:
        manager.close()


def test_pool_capacity_holds_under_concurrent_dispatch(tmp_path):
    """Two managers dispatching at once must not both claim the last pool slot."""
    import threading

    home = tmp_path / "state"
    m1 = JobManager(home, recover=False)
    m2 = JobManager(home, recover=False)
    try:
        m1.pool_configure("p", max_parallel=1)
        for _ in range(5):
            m1.start(cmd(SLEEP), pool="p")

        barrier = threading.Barrier(2)

        def dispatch(manager):
            barrier.wait()
            manager._dispatch_queued_jobs()

        threads = [threading.Thread(target=dispatch, args=(m,)) for m in (m1, m2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        active = m1.db.execute(
            "SELECT COUNT(*) FROM jobs WHERE pool='p' AND status IN ('running','launching')"
        ).fetchone()[0]
        assert active == 1, f"pool cap violated: {active} active"
        for job in m1.list()["jobs"]:
            if m1.status(job["job_id"])["status"] in {"running", "launching"}:
                m1.stop_sync(job["job_id"])
    finally:
        m1.close()
        m2.close()


def test_trigger_and_pool_gate_together(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    try:
        manager.pool_configure("p", max_parallel=1)
        parent = manager.start(cmd(SLEEP))
        child = manager.start(
            cmd(SLEEP),
            trigger={"job_id": parent["job_id"], "status": "completed"},
            pool="p",
        )
        assert child["status"] == "queued"
        manager._dispatch_queued_jobs()
        assert manager.status(child["job_id"])["status"] == "queued", "trigger not satisfied yet"
        manager.stop_sync(parent["job_id"])
    finally:
        manager.close()
