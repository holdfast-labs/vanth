import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from vanth.client import VanthClient


def test_ready_prefers_ready_fast_over_doctor(tmp_path):
    """`ensure()` readiness must use cheap `/ready-fast`, not full `/doctor`."""
    hits = []
    home = str(tmp_path)

    class CountingDaemon(BaseHTTPRequestHandler):
        def do_GET(self):
            hits.append(self.path.split("?")[0])
            payload = {"result": "ok", "home": home, "schema_version": 20}
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(payload).encode())

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), CountingDaemon)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        client = VanthClient(url=f"http://127.0.0.1:{server.server_port}", home=tmp_path)
        assert client._ready() is True
        assert hits == ["/ready-fast"], hits
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


def test_ready_falls_back_to_doctor_for_old_daemons(tmp_path):
    """Pre-`/ready-fast` daemons 404 the fast probe; `_ready()` must use `/doctor`."""
    home = str(tmp_path)

    class OldDaemon(BaseHTTPRequestHandler):
        def do_GET(self):
            path = self.path.split("?")[0]
            if path == "/ready-fast":
                payload = {"result": "error", "error": "not found"}
                self.send_response(404)
            else:
                payload = {"home": home, "schema_version": 20}
                self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(payload).encode())

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), OldDaemon)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        client = VanthClient(url=f"http://127.0.0.1:{server.server_port}", home=tmp_path)
        assert client._ready() is True
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


def test_ensure_explains_a_daemon_using_another_home(tmp_path, monkeypatch):
    class OtherDaemon(BaseHTTPRequestHandler):
        def do_GET(self):
            payload = {"ok": True} if self.path == "/health" else {"result": "error", "error": "unauthorized"}
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(payload).encode())

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), OtherDaemon)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        client = VanthClient(url=f"http://127.0.0.1:{server.server_port}", home=tmp_path)
        monkeypatch.setattr("vanth.client.subprocess.Popen", lambda *args, **kwargs: pytest.fail("started a second daemon"))
        with pytest.raises(RuntimeError, match="VANTH_DAEMON_PORT"):
            client.ensure()
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


def test_orphan_scan_cached_within_ttl(tmp_path, monkeypatch):
    """`doctor()` must reuse the process-table scan within the TTL."""
    import vanth.server as server
    from vanth.server import JobManager

    calls = []

    def _fake_scan():
        calls.append(1)
        return []

    monkeypatch.setattr(server, "_orphaned_mcp_servers", _fake_scan)
    manager = JobManager(tmp_path, recover=False)
    try:
        manager.doctor()
        manager.doctor()
        assert len(calls) == 1
        monkeypatch.setenv("VANTH_ORPHAN_CACHE_SECONDS", "0")
        manager.doctor()
        manager.doctor()
        assert len(calls) == 3
    finally:
        manager.close()


def test_reap_orphans_invalidates_cache(tmp_path, monkeypatch):
    """A reap must not leave just-killed PIDs in `doctor` for up to 60s."""
    import vanth.server as server
    from vanth.server import JobManager

    calls = []

    def _fake_scan():
        calls.append(1)
        return []

    monkeypatch.setattr(server, "_orphaned_mcp_servers", _fake_scan)
    manager = JobManager(tmp_path, recover=False)
    try:
        manager.doctor()
        assert len(calls) == 1
        # reap_orphans() performs its own fresh scan (call 2); the point is the
        # next doctor() rescans (call 3) instead of serving the pre-reap cache.
        manager.reap_orphans()
        manager.doctor()
        assert len(calls) == 3
    finally:
        manager.close()


def test_ensure_waits_for_shutting_daemon_instead_of_duplicating(tmp_path, monkeypatch):
    """A 503 (daemon mid-shutdown) must wait for quiet, then spawn — never a duplicate."""
    monkeypatch.setenv("VANTH_ENSURE_QUIET_TIMEOUT_SECONDS", "5")
    spawned = []

    class ShuttingDaemon(BaseHTTPRequestHandler):
        def do_GET(self):
            payload = {"result": "error", "error": "Daemon is shutting down"}
            self.send_response(503)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(payload).encode())

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), ShuttingDaemon)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        client = VanthClient(url=f"http://127.0.0.1:{server.server_port}", home=tmp_path)
        assert client._probe_port() == "shutting"
        monkeypatch.setattr(
            "vanth.client.subprocess.Popen", lambda *args, **kwargs: spawned.append(args) or FakePopen()
        )
        with pytest.raises(RuntimeError, match="did not start"):
            # Waits out the quiet timeout, spawns once, then the readiness
            # poll legitimately fails with no real daemon behind the port.
            client.ensure()
        assert spawned, "ensure() must spawn after waiting (not raise 'already responding')"
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


def test_ensure_503_then_quiet_spawns(tmp_path, monkeypatch):
    """Once the shutting daemon goes quiet, ensure() proceeds to spawn."""
    monkeypatch.setenv("VANTH_ENSURE_QUIET_TIMEOUT_SECONDS", "5")
    spawned = []

    class FlappingDaemon(BaseHTTPRequestHandler):
        calls = 0

        def do_GET(self):
            type(self).calls += 1
            if type(self).calls < 3:
                payload = {"result": "error", "error": "Daemon is shutting down"}
                self.send_response(503)
            else:
                # Port goes quiet mid-shutdown (listener closed): hang up.
                self.connection.close()
                return
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(payload).encode())

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), FlappingDaemon)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        client = VanthClient(url=f"http://127.0.0.1:{server.server_port}", home=tmp_path)
        monkeypatch.setattr(
            "vanth.client.subprocess.Popen", lambda *args, **kwargs: spawned.append(args) or FakePopen()
        )
        with pytest.raises(RuntimeError, match="did not start"):
            # Spawn happens (no duplicate while shutting down), then the
            # 5s readiness poll legitimately fails with no real daemon.
            client.ensure()
        assert spawned, "ensure() must spawn once the port goes quiet"
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


class FakePopen:
    """Stand-in for the spawned daemon process (does nothing)."""

    pid = 0


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX process-group cleanup")
def test_autostarted_daemon_survives_mcp_process_group_cleanup(tmp_path):
    # MCP stdio clients may terminate the server's whole process group when
    # disconnecting. A shared daemon must live in a separate session/group.
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    home = tmp_path / "state"
    ready = tmp_path / "ready"
    env = {**os.environ, "VANTH_HOME": str(home), "VANTH_DAEMON_PORT": str(port)}
    env.pop("VANTH_DAEMON_URL", None)
    launcher = subprocess.Popen(
        [sys.executable, "-c",
         "from pathlib import Path; import sys; from vanth.client import VanthClient; "
         "VanthClient().ensure(); Path(sys.argv[1]).write_text('ready'); sys.stdin.read()", str(ready)],
        env=env, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
        start_new_session=True,
    )
    client = VanthClient(f"http://127.0.0.1:{port}", home)
    try:
        deadline = time.monotonic() + 10
        while not ready.exists() and time.monotonic() < deadline:
            assert launcher.poll() is None, "launcher exited before starting its daemon"
            time.sleep(0.05)
        assert ready.exists(), "daemon did not become ready"
        daemon_pid = json.loads((home / "daemon.json").read_text())["pid"]
        assert os.getsid(daemon_pid) != os.getsid(launcher.pid)
        os.killpg(launcher.pid, signal.SIGTERM)
        launcher.wait(timeout=5)
        assert client._ready(), "MCP cleanup terminated the shared daemon"
    finally:
        if launcher.poll() is None:
            os.killpg(launcher.pid, signal.SIGTERM)
            launcher.wait(timeout=5)
        try:
            if client._ready():
                client.post("/shutdown")
        finally:
            launcher.stdin.close()
            launcher.stderr.close()
