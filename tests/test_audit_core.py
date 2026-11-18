"""Regression tests for the MCP/HTTP/manager wire contract."""

import os
import threading

import pytest

import vanth.daemon as daemon
import vanth.server as server_mod
from vanth.client import VanthClient, ensure_auth_token
from vanth.server import JobManager


class _InProcessDaemon:
    """Drive the real daemon handler against an isolated JobManager."""

    def __init__(self, manager: JobManager) -> None:
        self.manager = manager
        self.server = None
        self.thread = None
        self.url = None

    def __enter__(self) -> "_InProcessDaemon":
        self.server = daemon.TrackingHTTPServer(("127.0.0.1", 0), daemon.Handler)
        port = self.server.server_address[1]
        self.url = f"http://127.0.0.1:{port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *exc_info) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


@pytest.fixture
def daemon_home(tmp_path, monkeypatch):
    home = tmp_path / "state"
    monkeypatch.setenv("VANTH_HOME", str(home))
    manager = JobManager(home, recover=False)
    monkeypatch.setattr(daemon, "get_manager", lambda: manager)
    ensure_auth_token(home)
    try:
        with _InProcessDaemon(manager) as running:
            yield running, manager, home
    finally:
        manager.close()


def test_mcp_job_stop_payload_with_idempotency_key_succeeds(daemon_home):
    """The MCP job_stop tool posts idempotency_key (often null). The local
    stop route must not forward it to stop_sync, which does not accept it."""
    running, manager, home = daemon_home
    client = VanthClient(running.url, home)
    started = client.post("/jobs", {"command": "echo stop-me", "pool": "held"})
    assert started["status"] == "queued"
    result = client.post(
        f"/jobs/{started['job_id']}/stop",
        {
            "signal": "terminate",
            "kill_after_seconds": 10,
            "actor": "tool",
            "reason": None,
            "remote_id": None,
            "idempotency_key": None,
        },
    )
    assert result.get("result") != "error", result
    assert manager.status(started["job_id"])["status"] == "cancelled"


def test_mcp_job_rerun_payload_with_idempotency_key_succeeds(daemon_home):
    """A local rerun must ignore the remote-only idempotency_key instead of
    passing it to rerun_sync (which would raise TypeError and become a 500)."""
    running, manager, home = daemon_home
    client = VanthClient(running.url, home)
    started = client.post("/jobs", {"command": "echo rerun-me", "pool": "held"})
    result = client.post(
        f"/jobs/{started['job_id']}/rerun",
        {"idempotency_key": "rerun-key-12345"},
    )
    assert result.get("result") != "error", result
    assert result["job_id"] != started["job_id"]
    assert manager.status(result["job_id"])["status"] == "queued"


def test_wake_me_uses_the_caller_directory_not_the_job_cwd(tmp_path, monkeypatch):
    """wake_me=True must resolve the plugin relay registered for the CALLER's
    directory even when the job runs in another worktree."""
    home = tmp_path / "state"
    monkeypatch.setenv("VANTH_HOME", str(home))
    job_dir = tmp_path / "worktree"
    job_dir.mkdir()
    manager = JobManager(home, recover=False)
    monkeypatch.setattr(daemon, "get_manager", lambda: manager)
    ensure_auth_token(home)
    caller_directory = os.getcwd()
    manager.relay_register(
        "opencode-audit",
        "opencode_thread",
        [{"session_id": "ses_calleraudit", "directory": caller_directory}],
    )
    try:
        with _InProcessDaemon(manager) as running:
            monkeypatch.setattr(
                server_mod, "get_client", lambda: VanthClient(running.url, home)
            )
            started = server_mod.job_start(
                "echo wake-me",
                cwd=str(job_dir),
                pool="held",
                wake_me=True,
            )
            assert started.get("result") != "error", started
            targets = manager._wake_targets_for_job(started["job_id"])
            assert len(targets) == 1
            assert targets[0]["type"] == "opencode_thread"
            assert targets[0]["session_id"] == "ses_calleraudit"
            assert targets[0]["cwd"] == caller_directory
    finally:
        manager.close()


def test_unresolved_opencode_wake_names_the_directory(tmp_path):
    """An omitted session_id with no matching live relay must say so plainly
    instead of telling the caller to omit a field they already omitted."""
    manager = JobManager(tmp_path / "state", recover=False)
    try:
        missing = tmp_path / "no-relay-here"
        missing.mkdir()
        with pytest.raises(ValueError, match="requires session_id") as excinfo:
            manager.start(
                "echo nowhere",
                cwd=str(tmp_path),
                wake_targets=[
                    {
                        "type": "opencode_thread",
                        "events": ["completed"],
                        "cwd": str(missing),
                    }
                ],
            )
        message = str(excinfo.value)
        assert "none was resolved from a live OpenCode plugin relay" in message
        assert f"for directory {missing}" in message
        assert "omit it" not in message
    finally:
        manager.close()


def test_attach_opencode_wake_without_session_names_attach(tmp_path):
    """Relay lookup never runs for an attach target, so the error must not blame the relay."""
    manager = JobManager(tmp_path / "state", recover=False)
    try:
        with pytest.raises(ValueError, match="requires session_id") as excinfo:
            manager.start(
                "echo nowhere",
                cwd=str(tmp_path),
                wake_targets=[{"type": "opencode_thread", "events": ["completed"],
                               "attach": "http://127.0.0.1:4096"}],
            )
        message = str(excinfo.value)
        assert "--attach" in message
        assert "relay" not in message.split("Pass")[0]
    finally:
        manager.close()


def test_mcp_relative_cwd_resolves_against_caller(monkeypatch):
    """The daemon resolves cwd against its own directory, so the MCP tools must
    send an absolute local cwd; a remote cwd belongs to the remote host."""
    captured: list = []

    class FakeClient:
        def post(self, path, payload):
            captured.append(payload)
            return {"job_id": "job_x", "status": "running"}

        def confirm_local_start(self, result):
            return result

    monkeypatch.setattr(server_mod, "get_client", lambda: FakeClient())
    server_mod.job_start(command="echo hi", cwd="sub")
    assert captured[-1]["cwd"] == os.path.abspath("sub")
    server_mod.job_rerun("job_x", cwd="sub")
    assert captured[-1]["cwd"] == os.path.abspath("sub")
    server_mod.job_start(command="echo hi", cwd="proj", remote_id="r1", idempotency_key="k")
    assert captured[-1]["cwd"] == "proj"
    server_mod.job_rerun("job_x", cwd="proj", remote_id="r1", idempotency_key="k")
    assert captured[-1]["cwd"] == "proj"
