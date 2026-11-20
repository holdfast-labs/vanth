"""Deterministic performance-regression guards for the daemon hot paths.

These tests pin a *cost* (SQL statements, mirror opens, process count, query
plans) instead of wall-clock time, so a loaded CI runner cannot make them
flaky. The one wall-clock test is opt-out with ``VANTH_SKIP_PERF_WALLCLOCK=1``.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path

import pytest

import shellcmd
from vanth.client import VanthClient
from vanth.server import JobManager, normalize_event_payload, now_iso


@contextmanager
def sql_statements(db):
    """Collect every statement SQLite executes on ``db`` in the block."""
    statements: list[str] = []
    db.set_trace_callback(statements.append)
    try:
        yield statements
    finally:
        db.set_trace_callback(None)


def _insert_idle_jobs(manager: JobManager, count: int) -> None:
    stamp = now_iso()
    manager.db.executemany(
        "INSERT INTO jobs(job_id,command,status,created_at,updated_at,stdout_path,stderr_path,events_path) "
        "VALUES (?,?,?,?,?,?,?,?)",
        [
            (f"job_idle_{index:05d}", "true", "completed", stamp, stamp, "o", "e", "ev")
            for index in range(count)
        ],
    )
    manager.db.commit()


def _maintenance_tick(manager: JobManager) -> None:
    """Run one pass of the daemon's 5 Hz maintenance loop body."""
    manager._dispatch_due_deliveries()
    manager._reconcile_running_jobs()
    manager._dispatch_queued_jobs()
    manager._recover_stale_launch_claims()
    manager._fire_due_schedules()
    manager._watch_policies()
    manager._maybe_auto_cleanup()
    manager._expire_decisions()
    manager.relay_expire_stale(stale_after_seconds=300)


def _capture_payload(index: int) -> dict:
    return normalize_event_payload(
        {"type": "metric", "message": "m", "data": {"_step": index, "loss": 1.0}}
    )


def test_idle_maintenance_statements_do_not_grow_with_job_count(tmp_path):
    """Guards the 5 Hz loop against per-job SQL on every tick.

    The perf pass indexed the running/queued/policy polls; a regression that
    loops over every job (or runs a COUNT per job) makes an idle daemon's work
    grow with history, which this statement count catches without timing.
    """
    counts: dict[int, int] = {}
    for jobs in (10, 1000):
        manager = JobManager(tmp_path / f"home_{jobs}", recover=False)
        try:
            _insert_idle_jobs(manager, jobs)
            with sql_statements(manager.db) as statements:
                _maintenance_tick(manager)
            counts[jobs] = len(statements)
        finally:
            manager.close()

    assert counts[1000] <= counts[10] + 2, counts
    assert counts[10] <= 40, counts


def test_capture_batch_cost_is_per_batch_not_per_event(tmp_path, monkeypatch):
    """Guards the one-COUNT/one-MAX/one-mirror-open-per-batch capture fix.

    Before the perf pass a chatty job paid a COUNT(*) and MAX(seq) per event
    and an open/append/close of the JSONL mirror per event. The batch now
    holds the write lock and tracks both counters, so a single 64-event batch
    must cost exactly one of each regardless of how many events it carries.
    """
    manager = JobManager(tmp_path, recover=False)
    try:
        stamp = now_iso()
        events_path = manager.events_dir / "cap.jsonl"
        manager.db.execute(
            "INSERT INTO jobs(job_id,command,status,created_at,updated_at,stdout_path,stderr_path,events_path) "
            "VALUES ('cap','x','running',?,?,?,?,?)",
            (stamp, stamp, "o", "e", str(events_path)),
        )
        manager.db.commit()

        def run(batch_size: int) -> tuple[int, int, int]:
            statements: list[str] = []
            opens: list[int] = []
            original = Path.open

            def counting_open(path, *args, **kwargs):
                if Path(path) == events_path:
                    opens.append(1)
                return original(path, *args, **kwargs)

            manager.db.set_trace_callback(statements.append)
            monkeypatch.setattr(Path, "open", counting_open)
            try:
                manager._emit_capture_batch(
                    "cap", [_capture_payload(i) for i in range(batch_size)], "stdout"
                )
            finally:
                monkeypatch.setattr(Path, "open", original)
                manager.db.set_trace_callback(None)
            counts = sum("COUNT(" in statement.upper() for statement in statements)
            maxes = sum("MAX(SEQ)" in statement.upper().replace(" ", "") for statement in statements)
            return counts, maxes, len(opens)

        # 8 and 64 events are both one batch: the cost is identical.
        assert run(8) == run(64) == (1, 1, 1)
        # 200 events split into four 64-event batches: four of each, not 200.
        assert run(200) == (4, 4, 4)
    finally:
        manager.close()


def test_job_status_statement_count_is_bounded(tmp_path):
    """Guards `job status` against re-reading the whole event history.

    status() must use the (job_id, seq) index for the last/progress rows and
    keep a constant statement count even when the job has thousands of events.
    """
    manager = JobManager(tmp_path, recover=False)
    try:
        stamp = now_iso()
        manager.db.execute(
            "INSERT INTO jobs(job_id,command,status,created_at,updated_at,stdout_path,stderr_path,events_path) "
            "VALUES ('job_status','x','completed',?,?,?,?,?)",
            (stamp, stamp, "o", "e", "ev"),
        )
        manager.db.executemany(
            "INSERT INTO events(event_id,job_id,seq,type,level,data_json,source,created_at) "
            "VALUES (?,?,?,?,'info','{}','agent',?)",
            [(f"evt_{i}", "job_status", i, "log", stamp) for i in range(1, 2001)],
        )
        manager.db.commit()
        with sql_statements(manager.db) as statements:
            payload = manager.status("job_status")
        assert payload["job_id"] == "job_status"
        assert len(statements) <= 5, statements
    finally:
        manager.close()


def test_job_start_statement_count_is_bounded(tmp_path, monkeypatch):
    """Guards job start against scanning the existing job table.

    start() writes one row (plus wake targets and the idempotency table) and
    must not grow its SQL work with the number of jobs already in the home.
    """
    counts: dict[int, int] = {}
    for jobs in (10, 1000):
        manager = JobManager(tmp_path / f"start_{jobs}", recover=False)
        try:
            _insert_idle_jobs(manager, jobs)

            def fake_launch(job_id, stdout_path, stderr_path, events_path, spec_path, *, claim_token):
                return {
                    "job_id": job_id,
                    "status": "launching",
                    "worker_pid": None,
                    "stdout_path": str(stdout_path),
                    "stderr_path": str(stderr_path),
                    "events_path": str(events_path),
                }

            monkeypatch.setattr(manager, "_launch", fake_launch)
            with sql_statements(manager.db) as statements:
                manager.start("echo hello")
            counts[jobs] = len(statements)
        finally:
            manager.close()

    assert counts[1000] <= counts[10] + 2, counts
    assert counts[10] <= 20, counts


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_running(client: VanthClient, job_id: str, timeout: float = 30.0) -> dict:
    deadline = time.monotonic() + timeout
    status: dict = {}
    while time.monotonic() < deadline:
        try:
            status = client.get(f"/jobs/{job_id}/status")
        except Exception:
            status = {}
        if status.get("status") == "running" and status.get("pid") and status.get("worker_pid"):
            return status
        time.sleep(0.1)
    raise AssertionError(f"job {job_id} never reached running with pids: {status}")


def _windows_process_table() -> dict[int, dict]:
    script = (
        "$ErrorActionPreference='Stop'; "
        "$rows = @(Get-CimInstance Win32_Process | Select-Object ProcessId,ParentProcessId,Name); "
        "ConvertTo-Json -InputObject $rows -Compress"
    )
    result = subprocess.run(
        ["powershell", "-NoProfile", "-NonInteractive", "-Command", script],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    rows = json.loads(result.stdout or "[]")
    if isinstance(rows, dict):
        rows = [rows]
    return {
        int(row["ProcessId"]): {
            "ppid": int(row["ParentProcessId"]),
            "name": str(row["Name"]),
        }
        for row in rows
    }


def _descendants(table: dict[int, dict], root: int) -> set[int]:
    children: dict[int, list[int]] = {}
    for pid, info in table.items():
        children.setdefault(info["ppid"], []).append(pid)
    seen: set[int] = set()
    stack = [root]
    while stack:
        pid = stack.pop()
        if pid in seen:
            continue
        seen.add(pid)
        stack.extend(children.get(pid, []))
    return seen


@pytest.mark.skipif(sys.platform != "win32", reason="Windows process-tree check")
def test_plain_command_tree_has_no_shell_or_launcher_layer(tmp_path):
    """Guards the Windows process trim: one runner + workload, no cmd.exe.

    The perf pass starts the base interpreter (``__PYVENV_LAUNCHER__``) so the
    venv launcher does not double every runner, and execs plain commands
    directly so cmd.exe does not wrap them. A shell-feature command must still
    go through cmd.exe. This counts processes, not milliseconds.
    """
    port = _free_port()
    home = tmp_path / "state"
    env = {**os.environ, "VANTH_HOME": str(home), "VANTH_DAEMON_PORT": str(port)}
    proc = subprocess.Popen(
        [sys.executable, "-m", "vanth.daemon"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    client = VanthClient(f"http://127.0.0.1:{port}", home)
    job_ids: list[str] = []
    try:
        deadline = time.monotonic() + 15
        while True:
            try:
                if client.get("/health") == {"ok": True}:
                    break
            except Exception:
                pass
            if time.monotonic() > deadline:
                raise AssertionError("daemon did not become healthy")
            time.sleep(0.05)

        base = getattr(sys, "_base_executable", None) or sys.executable
        code = "import os, time; print(os.getpid(), flush=True); time.sleep(30)"
        plain_command = shellcmd.join([base, "-c", code])
        shell_command = plain_command + " && exit 0"
        plain = client.post("/jobs", {"command": plain_command, "name": "plain"})
        job_ids.append(plain["job_id"])
        shelled = client.post("/jobs", {"command": shell_command, "name": "shelled"})
        job_ids.append(shelled["job_id"])

        plain_status = _wait_running(client, plain["job_id"])
        shell_status = _wait_running(client, shelled["job_id"])
        time.sleep(0.5)
        table = _windows_process_table()

        plain_tree = _descendants(table, int(plain_status["worker_pid"]))
        plain_names = [table[pid]["name"].lower() for pid in plain_tree if pid in table]
        assert "cmd.exe" not in plain_names, plain_names
        # Exactly the runner and the workload. A venv launcher would be a third
        # python process; a cmd.exe wrapper would also add one.
        assert len(plain_tree) == 2, (plain_tree, plain_names)

        shell_tree = _descendants(table, int(shell_status["worker_pid"]))
        shell_names = [table[pid]["name"].lower() for pid in shell_tree if pid in table]
        assert "cmd.exe" in shell_names, shell_names
    finally:
        for job_id in job_ids:
            try:
                client.post(f"/jobs/{job_id}/stop", {})
            except Exception:
                pass
        try:
            client.post("/shutdown", {})
        except Exception:
            pass
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


@pytest.mark.skipif(
    os.environ.get("VANTH_SKIP_PERF_WALLCLOCK") == "1",
    reason="VANTH_SKIP_PERF_WALLCLOCK=1",
)
def test_capture_throughput_20k_events(tmp_path):
    """Wall-clock backstop for capture: 20k events must stay far under 20s.

    The deterministic checks catch per-event COUNT/MAX/mirror regressions; this
    guards the overall throughput (buffering, commits, metric ingest) that a
    statement count cannot see. Min of 3 runs; measured ~5.2-6.0s per 20k on
    this machine (2026-11-19), so the 20s limit is ~3.3x. Skip with
    ``VANTH_SKIP_PERF_WALLCLOCK=1`` on a heavily loaded runner.
    """
    manager = JobManager(tmp_path, recover=False)
    try:
        stamp = now_iso()
        manager.db.execute(
            "INSERT INTO jobs(job_id,command,status,created_at,updated_at,stdout_path,stderr_path,events_path) "
            "VALUES ('throughput','x','running',?,?,?,?,?)",
            (stamp, stamp, "o", "e", str(manager.events_dir / "throughput.jsonl")),
        )
        manager.db.commit()
        payloads = [_capture_payload(i) for i in range(20000)]

        def run() -> float:
            started = time.perf_counter()
            manager._emit_capture_batch("throughput", payloads, "stdout")
            return time.perf_counter() - started

        run()  # warm-up: connection, statement cache, first mirror write
        best = min(run() for _ in range(3))
        assert best < 20.0, f"20k capture took {best:.2f}s (limit 20s)"
    finally:
        manager.close()
