import json
import os
import socket
import subprocess
import sys
import time

import pytest

from vanth.client import VanthClient
from vanth.migrations import LATEST_SCHEMA_VERSION
from vanth import cli


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture()
def daemon(tmp_path):
    """Start a real daemon on a temp home and yield the client."""
    port = free_port()
    env = {**os.environ, "VANTH_HOME": str(tmp_path / "state"), "VANTH_DAEMON_PORT": str(port)}
    proc = subprocess.Popen(
        [sys.executable, "-m", "vanth.daemon"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    client = VanthClient(f"http://127.0.0.1:{port}", tmp_path / "state")
    deadline = time.monotonic() + 5
    while True:
        try:
            assert client.get("/health") == {"ok": True}
            break
        except Exception:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.05)
    yield tmp_path, client, port
    try:
        client.post("/shutdown", {})
    except Exception:
        pass
    proc.wait(timeout=10)


def run_cli(home, *args, port=None):
    """Run `vanth <args>` as a subprocess against a specific home."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env = {**os.environ, "VANTH_HOME": str(home)}
    if port:
        env["VANTH_DAEMON_PORT"] = str(port)
    return subprocess.run(
        [sys.executable, "-c", f"import sys; sys.path.insert(0,{root!r}); from vanth.cli import main; sys.exit(main())", *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_shutdown_route_gracefully_stops_daemon(daemon):
    tmp_path, client, port = daemon
    resp = client.post("/shutdown", {})
    assert resp["result"] == "shutting_down"
    # daemon.json should be removed on graceful shutdown
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and (tmp_path / "state" / "daemon.json").exists():
        time.sleep(0.1)
    assert not (tmp_path / "state" / "daemon.json").exists()


def test_status_reports_up(daemon):
    tmp_path, _, port = daemon
    result = run_cli(tmp_path / "state", "status", port=port)
    assert result.returncode == 0
    assert "UP" in result.stdout
    assert f"schema:   {LATEST_SCHEMA_VERSION}" in result.stdout


def test_status_json(daemon):
    tmp_path, _, port = daemon
    result = run_cli(tmp_path / "state", "status", "--json", port=port)
    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload["up"] is True
    assert payload["daemon_schema_version"] == LATEST_SCHEMA_VERSION


def test_status_down_when_no_daemon(tmp_path):
    result = run_cli(tmp_path, "status")
    assert result.returncode == 1
    assert "DOWN" in result.stdout


def test_rerun_cli_posts_only_requested_overrides(monkeypatch, tmp_path, capsys):
    source_id = "job_abc123456789"
    response = {"job_id": "job_new987654321", "status": "queued"}

    class FakeClient:
        def __init__(self, *args, **kwargs):
            self.posted = None

        def ensure(self):
            pass

        def get(self, path, params=None):
            assert path == "/jobs"
            return {"jobs": [{"job_id": source_id}]}

        def confirm_local_start(self, result):
            return result

        def post(self, path, payload):
            self.posted = (path, payload)
            return response

    client = FakeClient()
    monkeypatch.setattr(cli, "VanthClient", lambda **kwargs: client)
    assert cli.cmd_rerun(
        ["job_abc", "--name", "retry", "--env", "MODE=fast", "--env", "EMPTY=", "--tag", "again", "--timeout", "45"],
        tmp_path,
        json_out=True,
    ) == 0
    assert client.posted == (
        f"/jobs/{source_id}/rerun",
        {"name": "retry", "timeout_seconds": 45, "env": {"MODE": "fast", "EMPTY": ""}, "tags": ["again"]},
    )
    assert json.loads(capsys.readouterr().out) == response


def test_rerun_cli_validates_override_arguments(tmp_path, capsys):
    assert cli.cmd_rerun([], tmp_path) == 2
    assert "missing job id" in capsys.readouterr().err
    assert cli.cmd_rerun(["job_abc", "--timeout", "0"], tmp_path) == 2
    assert "--timeout must be >= 1" in capsys.readouterr().err


def test_doctor_ok(daemon):
    tmp_path, _, port = daemon
    result = run_cli(tmp_path / "state", "doctor", port=port)
    assert result.returncode == 0
    assert "OK" in result.stdout


def test_doctor_summarizes_relays_and_failed_delivery_history(tmp_path, monkeypatch, capsys):
    report = {
        "ok": True,
        "home": str(tmp_path),
        "schema_version": 1,
        "tables": [],
        "delivery_counts": {"failed": 4, "delivered": 8},
        "codex": {"available": True},
        "opencode": {"available": True},
        "quick_check": "ok",
        "maintenance_alive": True,
        "disk_free_bytes": 1024,
        "relays": [
            {
                "client_type": "opencode_thread",
                "client_id": "relay-private-id",
                "live": True,
                "destinations": [{"session_id": f"session-{i}"} for i in range(5)],
            },
            {"client_type": "codex_desktop", "live": False, "destinations": []},
        ],
    }

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        def ensure(self):
            pass

        def get(self, path):
            assert path == "/doctor"
            return report

    monkeypatch.setattr(cli, "VanthClient", FakeClient)
    monkeypatch.setattr(cli, "_print_setup_status", lambda: None)
    assert cli.cmd_doctor([], tmp_path) == 0
    output = capsys.readouterr().out
    assert "2 total, 1 live, 1 stale" in output
    assert "session-0, session-1, session-2, +2 more" in output
    assert "relay-private-id" not in output
    assert "4 failed delivery record(s)" in output
    assert "full destination list: vanth doctor --json" in output
    assert "vanth deliveries --status failed" in output

    assert cli.cmd_doctor([], tmp_path, json_out=True) == 0
    assert json.loads(capsys.readouterr().out) == report


def test_restart_starts_fresh_daemon(tmp_path):
    """Restart a running daemon and confirm a fresh process serves."""
    state = tmp_path / "state"
    port = free_port()
    env = {**os.environ, "VANTH_HOME": str(state), "VANTH_DAEMON_PORT": str(port)}
    proc = subprocess.Popen(
        [sys.executable, "-m", "vanth.daemon"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    client = VanthClient(f"http://127.0.0.1:{port}", state)
    deadline = time.monotonic() + 5
    while True:
        try:
            if client.get("/health") == {"ok": True}:
                break
        except Exception:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.05)
    try:
        result = run_cli(state, "restart", port=port)
        assert result.returncode == 0, result.stdout + result.stderr
        assert "daemon" in result.stdout
        # A new daemon must be serving on the same port.
        deadline = time.monotonic() + 10
        while True:
            try:
                if client.get("/health") == {"ok": True}:
                    break
            except Exception:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.2)
        disc = json.loads((state / "daemon.json").read_text(encoding="utf-8"))
        assert disc["schema_version"] == LATEST_SCHEMA_VERSION
        assert disc["pid"] is not None
    finally:
        try:
            client.post("/shutdown", {})
        except Exception:
            pass
        proc.wait(timeout=10)


def test_cmd_deliveries_clear_requires_a_filter_and_prunes(monkeypatch, capsys):
    calls = {}

    class FakeClient:
        def __init__(self, **kwargs):
            pass

        def ensure(self):
            pass

        def get(self, path, params=None):
            return {"result": "ok", "deliveries": []}

        def post(self, path, payload=None, **kwargs):
            calls["path"] = path
            calls["payload"] = payload
            return {"result": "ok", "matched": 3, "drained": 0 if payload["dry_run"] else 3, "dry_run": payload["dry_run"]}

    monkeypatch.setattr(cli, "VanthClient", lambda **kwargs: FakeClient())

    # No filter: refuse (avoids an accidental full drain).
    assert cli.cmd_deliveries(["--clear"], home=".", json_out=False) == 2

    # Dry run by default, with a useful page size (not the 20-row listing default).
    assert cli.cmd_deliveries(["--clear", "--status", "failed"], home=".", json_out=False) == 0
    assert calls["path"] == "/deliveries/clear"
    assert calls["payload"]["status"] == "failed"
    assert calls["payload"]["dry_run"] is True
    assert calls["payload"]["limit"] == 1000

    # --limit overrides.
    assert cli.cmd_deliveries(["--clear", "--status", "failed", "--limit", "5"], home=".", json_out=False) == 0
    assert calls["payload"]["limit"] == 5

    # --yes applies.
    assert cli.cmd_deliveries(["--clear", "--status", "failed", "--yes"], home=".", json_out=False) == 0
    assert calls["payload"]["dry_run"] is False
