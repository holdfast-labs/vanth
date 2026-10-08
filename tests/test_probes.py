"""Readiness probes for trigger-gated jobs (#10)."""

from __future__ import annotations

import socket
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from vanth.probes import evaluate_probe, validate_probe
from vanth.server import JobManager


import shellcmd


def cmd(code: str) -> str:
    return shellcmd.join([sys.executable, "-c", code])


SLEEP = "import time; time.sleep(30)"


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_status(manager: JobManager, job_id: str, want: set[str], timeout: float = 15.0) -> str:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = manager.status(job_id)["status"]
        if status in want:
            return status
        time.sleep(0.05)
    raise AssertionError(f"job {job_id} did not reach {want}")


def test_validate_probe_accepts_and_normalizes():
    assert validate_probe({"type": "port", "port": 80})["host"] == "127.0.0.1"
    assert validate_probe({"type": "http", "url": "http://x"})["expect_status"] == 200
    assert validate_probe({"type": "log_line", "job_id": "job_x", "pattern": "ready"})["stream"] == "all"
    assert validate_probe({"type": "file", "path": "/tmp/ready"})["path"] == "/tmp/ready"
    assert validate_probe({"type": "port", "port": 80, "timeout_seconds": 5, "interval_seconds": 2})[
        "timeout_seconds"
    ] == 5


def test_validate_probe_rejects_bad_shapes():
    bad = [
        None,
        {},
        {"type": "nope"},
        {"type": "port"},
        {"type": "port", "port": 0},
        {"type": "port", "port": 70000},
        {"type": "port", "port": 80, "bogus": 1},
        {"type": "http", "url": "ftp://x"},
        {"type": "http", "url": "http://x", "expect_status": 99},
        {"type": "log_line", "job_id": "job_x"},
        {"type": "log_line", "pattern": "ready"},
        {"type": "log_line", "job_id": "job_x", "pattern": "r", "stream": "both"},
        {"type": "file"},
        {"type": "file", "path": "/x", "interval_seconds": 0},
    ]
    for probe in bad:
        with pytest.raises(ValueError):
            validate_probe(probe)


def test_evaluate_probe_port_open_and_closed():
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    port = server.getsockname()[1]
    try:
        assert evaluate_probe({"type": "port", "host": "127.0.0.1", "port": port}) is True
    finally:
        server.close()
    assert evaluate_probe({"type": "port", "host": "127.0.0.1", "port": port}) is False


def test_evaluate_probe_file_and_log_line(tmp_path):
    ready = tmp_path / "ready"
    assert evaluate_probe({"type": "file", "path": str(ready)}) is False
    ready.write_text("x", encoding="utf-8")
    assert evaluate_probe({"type": "file", "path": str(ready)}) is True
    assert evaluate_probe({"type": "log_line", "pattern": "ready"}, log_text="all ready now") is True
    assert evaluate_probe({"type": "log_line", "pattern": "ready"}, log_text="nope") is False


def test_evaluate_probe_http():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - stdlib handler API
            self.send_response(200 if self.path == "/ok" else 404)
            self.end_headers()

        def log_message(self, *args):  # silence the test server
            return

    server = HTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        assert evaluate_probe({"type": "http", "url": f"http://127.0.0.1:{port}/ok"}) is True
        assert evaluate_probe({"type": "http", "url": f"http://127.0.0.1:{port}/missing"}) is False
        assert evaluate_probe({"type": "http", "url": "http://127.0.0.1:1/"}) is False
    finally:
        server.shutdown()
        server.server_close()


def test_port_probe_gates_launch_until_open(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    try:
        port = _free_port()
        job = manager.start(
            cmd(SLEEP),
            trigger={"probe": {"type": "port", "host": "127.0.0.1", "port": port}},
        )
        assert job["status"] == "queued"
        manager._dispatch_queued_jobs()
        assert manager.status(job["job_id"])["status"] == "queued", "closed port must not launch"

        server = socket.socket()
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("127.0.0.1", port))
        server.listen(1)
        try:
            manager._probe_last_attempt.clear()  # bypass the probe cadence for the test
            manager._dispatch_queued_jobs()
            assert _wait_status(manager, job["job_id"], {"launching", "running"})
        finally:
            server.close()
        manager.stop_sync(job["job_id"])
    finally:
        manager.close()


def test_probe_timeout_cancels_with_attribution(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    try:
        port = _free_port()
        job = manager.start(
            cmd(SLEEP),
            trigger={"probe": {"type": "port", "host": "127.0.0.1", "port": port, "timeout_seconds": 1}},
        )
        with manager.db_lock:
            manager.db.execute(
                "UPDATE jobs SET created_at=? WHERE job_id=?",
                ("2000-01-01T00:00:00Z", job["job_id"]),
            )
            manager.db.commit()
        manager._dispatch_queued_jobs()
        status = manager.status(job["job_id"])
        assert status["status"] == "cancelled"
        assert status["stop_actor"] == "daemon"
        assert "did not become ready" in (status["stop_reason"] or "")
    finally:
        manager.close()


def test_log_line_probe_requires_known_job(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    try:
        with pytest.raises(ValueError, match="Unknown log_line probe job_id"):
            manager.start(
                cmd(SLEEP),
                trigger={"probe": {"type": "log_line", "job_id": "..\\escape", "pattern": "x"}},
            )
    finally:
        manager.close()


def test_probe_timeout_clock_starts_after_dag_gate(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    try:
        # The parent must actually reach 'completed': the child's trigger targets
        # that status, so a transient parent launch failure on a loaded runner
        # would make the gate unsatisfiable and cancel the child. Retry the
        # parent a few times to keep the probe-clock assertion the subject.
        parent = None
        for _ in range(3):
            candidate = manager.start(cmd("print('done')"))
            if _wait_status(manager, candidate["job_id"], {"completed", "failed"}) == "completed":
                parent = candidate
                break
        assert parent is not None, "parent job did not complete"
        port = _free_port()
        child = manager.start(
            cmd(SLEEP),
            trigger={
                "job_id": parent["job_id"],
                "status": "completed",
                "probe": {"type": "port", "host": "127.0.0.1", "port": port, "timeout_seconds": 1},
            },
        )
        # Backdate the child's creation far past the probe timeout; because the
        # DAG parent only just completed, the readiness clock starts there and
        # the job must NOT be cancelled.
        with manager.db_lock:
            manager.db.execute(
                "UPDATE jobs SET created_at=? WHERE job_id=?",
                ("2000-01-01T00:00:00Z", child["job_id"]),
            )
            manager.db.commit()
        manager._dispatch_queued_jobs()
        assert manager.status(child["job_id"])["status"] == "queued"
    finally:
        manager.close()


def test_probe_budget_limits_io_per_pass(tmp_path, monkeypatch):
    import vanth.server as server_module

    manager = JobManager(tmp_path, recover=False)
    try:
        calls = {"n": 0}

        def counting(probe, **kwargs):
            calls["n"] += 1
            return False

        monkeypatch.setattr(server_module, "evaluate_probe", counting)
        manager.probe_budget = 1
        for index in range(3):
            manager.start(
                cmd(SLEEP),
                trigger={"probe": {"type": "file", "path": f"/nonexistent/{index}"}},
            )
        manager._dispatch_queued_jobs()
        assert calls["n"] == 1, f"only the budgeted probe should run: {calls}"
        # The first job is now throttled (no I/O), so the next pass probes the next.
        manager._dispatch_queued_jobs()
        assert calls["n"] == 2, calls
    finally:
        manager.close()


def test_log_line_probe_releases_when_pattern_appears(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    try:
        producer = manager.start(cmd("import time; print('READY', flush=True); time.sleep(30)"))
        gate = manager.start(
            cmd(SLEEP),
            trigger={"probe": {"type": "log_line", "job_id": producer["job_id"], "pattern": "READY"}},
        )
        assert gate["status"] == "queued"
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and manager.status(gate["job_id"])["status"] == "queued":
            manager._probe_last_attempt.clear()
            manager._dispatch_queued_jobs()
            time.sleep(0.1)
        assert manager.status(gate["job_id"])["status"] in {"launching", "running"}
        manager.stop_sync(gate["job_id"])
        manager.stop_sync(producer["job_id"])
    finally:
        manager.close()
