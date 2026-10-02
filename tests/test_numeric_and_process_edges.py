"""Reject unrepresentable chart scalars without dropping raw events or killing reused PIDs."""
import json
from types import SimpleNamespace

import pytest

from vanth import server
from vanth.server import JobManager, now_iso


@pytest.fixture
def manager(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    timestamp = now_iso()
    manager.db.execute(
        "INSERT INTO jobs(job_id,command,status,created_at,updated_at,stdout_path,stderr_path,events_path) "
        "VALUES ('job_numeric','unused','running',?,?,?,?,?)",
        (timestamp, timestamp, str(manager.logs / "numeric.stdout"),
         str(manager.logs / "numeric.stderr"), str(manager.events_dir / "numeric.jsonl")))
    manager.db.commit()
    try:
        yield manager
    finally:
        manager.close()


def test_huge_integer_is_retained_in_event_but_not_numeric_series(manager):
    huge = 10 ** 400
    event = manager._emit("job_numeric", "metric", data={"safe": 3, "huge": huge, "_step": huge})
    persisted = manager.db.execute("SELECT data_json FROM events WHERE event_id=?", (event["event_id"],)).fetchone()
    assert json.loads(persisted["data_json"])["huge"] == huge
    rows = manager.db.execute("SELECT metric,x,y FROM metric_series WHERE job_id='job_numeric'").fetchall()
    assert [tuple(row) for row in rows] == [("safe", float(event["seq"]), 3.0)]
    progress = manager._emit("job_numeric", "progress", data={"current": huge, "total": 2, "_step": huge})
    persisted = manager.db.execute("SELECT data_json FROM events WHERE event_id=?", (progress["event_id"],)).fetchone()
    progress_data = json.loads(persisted["data_json"])
    assert progress_data["current"] == huge and "percent" not in progress_data
    rows = manager.db.execute("SELECT metric,x,y FROM metric_series WHERE event_id=?", (progress["event_id"],)).fetchall()
    assert [tuple(row) for row in rows] == [("progress.total", float(progress["seq"]), 2.0)]


def test_direct_metric_ingest_rejects_huge_integer_before_persistence(manager):
    with pytest.raises(ValueError, match="finite number"):
        manager.metric_ingest("job_numeric", [{"name": "safe", "value": 1}, {"name": "huge", "value": 10 ** 400}])
    assert manager.db.execute("SELECT COUNT(*) FROM metric_series").fetchone()[0] == 0
    assert manager.db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0


@pytest.mark.parametrize("force,signal_number", [(False, 15), (True, 9)])
@pytest.mark.parametrize("failure", [None, ProcessLookupError, PermissionError])
def test_reaped_leader_cleanup_never_signals_its_numeric_pid(monkeypatch, force, signal_number, failure):
    manager = object.__new__(JobManager)
    proc = SimpleNamespace(pid=123456, poll=lambda: 0)
    signals = []
    monkeypatch.setattr(server.sys, "platform", "linux")
    # The workload still leads its own group (pgid == pid), so killpg is safe.
    monkeypatch.setattr(manager, "_workload_group", lambda pid: pid)
    def killpg(pid, signal):
        signals.append((pid, signal))
        if failure:
            raise failure()
    monkeypatch.setattr(server.os, "killpg", killpg, raising=False)
    monkeypatch.setattr(server.os, "kill", lambda *a: pytest.fail("reaped PID could have been reused"))
    monkeypatch.setattr(manager, "_kill_pid", lambda *a: pytest.fail("unsafe PID fallback"))
    manager._kill_process(proc, force=force)
    assert signals == [(123456, signal_number)]


def test_reused_pid_leading_a_different_group_is_not_signalled(monkeypatch):
    """The macOS burst-orphan: a reaped pid reused as a *new* session leader
    must not have its unrelated group signalled (that killed a sibling runner)."""
    manager = object.__new__(JobManager)
    proc = SimpleNamespace(pid=123456, poll=lambda: 0)
    monkeypatch.setattr(server.sys, "platform", "linux")
    # ps says the reused pid leads group 999 (not its own) -> not ours.
    monkeypatch.setattr(manager, "_workload_group", lambda pid: None)
    monkeypatch.setattr(server.os, "killpg", lambda *a: pytest.fail("signalled a reused leader's group"), raising=False)
    monkeypatch.setattr(server.os, "kill", lambda *a: pytest.fail("signalled a reused pid"))
    manager._kill_process(proc, force=True)


def test_kill_pid_only_signals_when_the_pid_leads_its_own_group(monkeypatch):
    manager = object.__new__(JobManager)
    monkeypatch.setattr(server.sys, "platform", "linux")
    monkeypatch.setattr(manager, "_workload_group", lambda pid: None)
    calls = []
    monkeypatch.setattr(server.os, "killpg", lambda *a: calls.append(("pg", a)), raising=False)
    monkeypatch.setattr(server.os, "kill", lambda pid, sig: calls.append(("pid", pid, sig)))
    manager._kill_pid(123456, force=True)
    assert calls == [("pid", 123456, 9)]


def test_live_leader_retains_existing_process_tree_cleanup(monkeypatch):
    manager = object.__new__(JobManager)
    proc = SimpleNamespace(pid=123456, poll=lambda: None)
    calls = []
    monkeypatch.setattr(server.sys, "platform", "linux")
    monkeypatch.setattr(manager, "_kill_pid", lambda pid, force: calls.append((pid, force)))
    manager._kill_process(proc, force=True)
    assert calls == [(123456, True)]
