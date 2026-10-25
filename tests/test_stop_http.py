"""Exercise MCP stop through the HTTP boundary that dispatches local/remote jobs."""

import asyncio
import threading

import pytest

import vanth.daemon as daemon
import vanth.server as server
from vanth.client import VanthClient
from vanth.server import JobManager


@pytest.fixture
def stop_http(monkeypatch, tmp_path):
    monkeypatch.setenv("VANTH_HOME", str(tmp_path / "state"))
    manager = JobManager(tmp_path / "state")
    monkeypatch.setattr(daemon, "get_manager", lambda: manager)
    monkeypatch.setattr(daemon, "shutdown_event", threading.Event())
    httpd = daemon.TrackingHTTPServer(("127.0.0.1", 0), daemon.Handler)
    client = VanthClient(f"http://127.0.0.1:{httpd.server_port}", manager.home)
    monkeypatch.setattr(server, "get_client", lambda: client)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield manager, client
    finally:
        httpd.shutdown()
        thread.join(timeout=5)
        httpd.server_close()
        manager.close()


@pytest.mark.parametrize("key", [None, "local-stop-test-123"])
def test_mcp_stop_local_job_accepts_idempotency_key(stop_http, tmp_path, key):
    manager, _ = stop_http
    # A gated job stays queued, so this covers real cancellation without
    # starting a workload that might be left behind if the request fails.
    job = manager.start("echo gated", trigger={"probe": {"type": "file", "path": str(tmp_path / "gate")}})
    job_id = job["job_id"]
    result = asyncio.run(server.job_stop(job_id, reason="test cancellation", idempotency_key=key))
    assert result["status"] == "cancelled"
    event = manager.status(job_id)["last_event"]
    assert event["type"] == "cancelled"
    assert event["data"]["actor"] == "tool"
    assert event["data"]["reason"] == "test cancellation"
    # Repeated local stops retain their existing idempotent no-op behavior.
    repeated = asyncio.run(server.job_stop(job_id, idempotency_key=key))
    assert repeated["status"] == "cancelled"
    assert "no-op" in repeated["message"]


def test_mcp_stop_remote_preserves_idempotency_key(stop_http, monkeypatch):
    submitted = []

    def submit(remote_id, method, payload):
        submitted.append((remote_id, method, payload))
        return {"status": "completed"}

    monkeypatch.setattr(daemon, "_remote_submit", submit)
    asyncio.run(server.job_stop(
        "job_remote", remote_id="host-a", idempotency_key="remote-stop-test-123",
        signal="kill", kill_after_seconds=0, reason="local attribution",
    ))
    assert submitted == [("host-a", "job.stop", {
        "job_id": "job_remote", "signal": "kill", "kill_after_seconds": 0,
        "idempotency_key": "remote-stop-test-123",
    })]
