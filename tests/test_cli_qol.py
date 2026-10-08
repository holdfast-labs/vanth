import json
import os
import socket
import subprocess
import sys
import time

import pytest

from vanth.client import VanthClient


import shellcmd


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def cmd(code: str) -> str:
    return shellcmd.join([sys.executable, "-c", code])


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


def wait_status(client, job_id, statuses, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        payload = client.get(f"/jobs/{job_id}/status")
        if payload.get("status") in statuses:
            return payload.get("status")
        time.sleep(0.1)
    raise AssertionError(f"job {job_id} never reached {statuses}: {payload}")


def test_version_flag(daemon):
    result = run_cli(daemon[0] / "state", "--version")
    assert result.returncode == 0
    assert result.stdout.strip()


def test_version_subcommand(daemon):
    result = run_cli(daemon[0] / "state", "version")
    assert result.returncode == 0
    assert result.stdout.strip()


def test_list_shows_running_job(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("import time; time.sleep(30)")})
    job_id = started["job_id"]
    wait_status(client, job_id, ["running"])
    result = run_cli(tmp_path / "state", "list", port=port)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "STATUS" in result.stdout
    assert job_id in result.stdout
    assert "running" in result.stdout
    try:
        client.post(f"/jobs/{job_id}/stop", {})
    except Exception:
        pass


def test_list_ps_alias(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("import time; time.sleep(30)")})
    wait_status(client, started["job_id"], ["running"])
    result = run_cli(tmp_path / "state", "ps", port=port)
    assert result.returncode == 0
    assert started["job_id"] in result.stdout
    try:
        client.post(f"/jobs/{started['job_id']}/stop", {})
    except Exception:
        pass


def test_resolve_endpoint_round_trip(daemon):
    """`/jobs/resolve` settles exact, prefix, ambiguous, and unknown inputs."""
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("print('x')")})
    job_id = started["job_id"]
    assert client.get("/jobs/resolve", {"prefix": job_id}) == {"job_id": job_id, "problem": ""}
    assert client.get("/jobs/resolve", {"prefix": job_id[:12]})["job_id"] == job_id
    second = client.post("/jobs", {"command": cmd("print('y')")})["job_id"]
    ambiguous = client.get("/jobs/resolve", {"prefix": "job_"})
    assert ambiguous["job_id"] is None
    assert "ambiguous" in ambiguous["problem"]
    unknown = client.get("/jobs/resolve", {"prefix": "job_zzz_nope"})
    assert unknown == {"job_id": None, "problem": "unknown job job_zzz_nope"}
    try:
        client.post(f"/jobs/{job_id}/stop", {})
        client.post(f"/jobs/{second}/stop", {})
    except Exception:
        pass


def test_list_json(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("import time; time.sleep(30)")})
    wait_status(client, started["job_id"], ["running"])
    result = run_cli(tmp_path / "state", "list", "--json", port=port)
    assert result.returncode == 0, result.stdout + result.stderr
    jobs = json.loads(result.stdout)
    assert isinstance(jobs, list)
    assert any(job["job_id"] == started["job_id"] for job in jobs)
    try:
        client.post(f"/jobs/{started['job_id']}/stop", {})
    except Exception:
        pass


def test_list_status_filter(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("import time; time.sleep(30)")})
    result = run_cli(tmp_path / "state", "list", "--status", "queued", port=port)
    assert result.returncode == 0
    assert started["job_id"] not in result.stdout
    try:
        client.post(f"/jobs/{started['job_id']}/stop", {})
    except Exception:
        pass


def test_logs_shows_output(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("print('hello from job', flush=True); import time; time.sleep(0.2)")})
    job_id = started["job_id"]
    wait_status(client, job_id, ["completed"])
    result = run_cli(tmp_path / "state", "logs", job_id, port=port)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "hello from job" in result.stdout


def test_logs_tail_alias(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("print('aliased tail', flush=True)")})
    job_id = started["job_id"]
    wait_status(client, job_id, ["completed"])
    result = run_cli(tmp_path / "state", "tail", job_id, port=port)
    assert result.returncode == 0
    assert "aliased tail" in result.stdout


def test_logs_unknown_job(daemon):
    tmp_path, _, port = daemon
    result = run_cli(tmp_path / "state", "logs", "job_nope", port=port)
    assert result.returncode == 1
    assert "unknown job" in result.stderr.lower()


def test_logs_stream_all(daemon):
    tmp_path, client, port = daemon
    code = "import sys; print('OUT-LINE', flush=True); print('ERR-LINE', file=sys.stderr, flush=True)"
    started = client.post("/jobs", {"command": cmd(code)})
    job_id = started["job_id"]
    wait_status(client, job_id, ["completed"])
    result = run_cli(tmp_path / "state", "logs", job_id, "--stream", "all", port=port)
    assert result.returncode == 0
    assert "OUT-LINE" in result.stdout
    assert "ERR-LINE" in result.stdout


def test_logs_grep_filters(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("print('foo-bar', flush=True); print('baz-qux', flush=True)")})
    job_id = started["job_id"]
    wait_status(client, job_id, ["completed"])
    full = run_cli(tmp_path / "state", "logs", job_id, port=port)
    assert "foo-bar" in full.stdout and "baz-qux" in full.stdout
    filtered = run_cli(tmp_path / "state", "logs", job_id, "--grep", "foo", port=port)
    assert filtered.returncode == 0, filtered.stdout + filtered.stderr
    assert "foo-bar" in filtered.stdout
    assert "baz-qux" not in filtered.stdout


def test_diff_cli(daemon):
    tmp_path, client, port = daemon
    base = client.post("/jobs", {"command": cmd("print('a')"), "name": "same", "env": {"X": "1"}})["job_id"]
    other = client.post("/jobs", {"command": cmd("print('b')"), "name": "same", "env": {"X": "2"}})["job_id"]
    wait_status(client, base, ["completed"])
    wait_status(client, other, ["completed"])
    result = run_cli(tmp_path / "state", "diff", base, other, port=port)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "command" in result.stdout
    assert "env" in result.stdout


def test_stop_job(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("import time; time.sleep(30)")})
    job_id = started["job_id"]
    result = run_cli(tmp_path / "state", "stop", job_id, port=port)
    assert result.returncode == 0, result.stdout + result.stderr
    assert f"requested stop for {job_id}" in result.stdout
    status = wait_status(client, job_id, ["cancelled", "stopped", "completed"])
    assert status == "cancelled"


def test_stop_json(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("import time; time.sleep(30)")})
    result = run_cli(tmp_path / "state", "stop", started["job_id"], "--json", port=port)
    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload.get("status") in {"cancelled", "running", "stopped"}
    try:
        client.post(f"/jobs/{started['job_id']}/stop", {})
    except Exception:
        pass


def test_artifacts(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("import time; time.sleep(0.2)")})
    job_id = started["job_id"]
    wait_status(client, job_id, ["completed"])
    client.post(f"/jobs/{job_id}/artifacts", {"name": "a", "uri": "file:///tmp/x"})
    result = run_cli(tmp_path / "state", "artifacts", job_id, port=port)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "a" in result.stdout
    assert "file:///tmp/x" in result.stdout


def test_artifacts_json(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("import time; time.sleep(0.2)")})
    job_id = started["job_id"]
    wait_status(client, job_id, ["completed"])
    client.post(f"/jobs/{job_id}/artifacts", {"name": "b", "uri": "file:///tmp/y"})
    result = run_cli(tmp_path / "state", "artifacts", job_id, "--json", port=port)
    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert any(artifact["name"] == "b" for artifact in payload["artifacts"])


def test_doctor_reap_orphans(daemon):
    tmp_path, client, port = daemon
    result = run_cli(tmp_path / "state", "doctor", "--reap-orphans", port=port)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "orphaned" in result.stdout.lower() or "reaped" in result.stdout.lower()


def test_doctor_reports_orphans_field(daemon):
    tmp_path, client, port = daemon
    result = run_cli(tmp_path / "state", "doctor", "--json", port=port)
    assert result.returncode == 0, result.stdout + result.stderr
    payload = json.loads(result.stdout)
    assert "orphaned_mcp_servers" in payload
    assert isinstance(payload["orphaned_mcp_servers"], list)


@pytest.mark.parametrize(
    "command, expected",
    [
        # Real Vanth MCP stdio servers:
        ("/usr/bin/python3 /opt/venv/bin/vanth", True),
        (r"C:\venv\Scripts\python.exe C:\venv\Scripts\vanth.exe", True),
        ("/usr/bin/python3 -m vanth.server", True),
        # No vanth.mcp module exists: nothing can launch `-m vanth.mcp`
        # (ModuleNotFoundError), so it must not match — matching a shape that
        # can never be a genuine server only risks the reaper.
        ("/usr/bin/python3 -O -m vanth.mcp", False),
        ('"C:\\Program Files\\Python\\python.exe" -m vanth.server', True),
        ("/usr/bin/python3 -X dev -m vanth.server", True),
        # PyInstaller standalone artifact launched bare as the MCP server:
        ("C:\\tools\\vanth-standalone-windows-x86_64.exe", True),
        ("/opt/vanth-standalone-linux-x86_64", True),
        # Not MCP servers — must never be matched (and thus never reaped):
        ("/home/user/vanth-ci/.venv/bin/python /home/user/vanth-ci/.venv/bin/pytest -q", False),
        ("/bin/bash -lc cd /home/user/vanth-ci && uv run pytest", False),
        ("/bin/bash -lc vanth doctor --json", False),
        ("/usr/bin/python3 -m vanth.runner /home/user/state job_x claim.json", False),
        ("/usr/bin/python3 -m vanth.daemon", False),
        # Lookalikes the reaper must refuse to kill:
        ('vanth "logs" --follow', False),
        ("/opt/vanth-standalone-linux-x86_64 logs --follow", False),
        ("/opt/vanth-standalone-notes.txt", False),
        ("/opt/vanth-standalone", False),
        ("/usr/bin/python -c__import__('time').sleep(600) vanth", False),
        ("/usr/bin/python3 unrelated.py -m vanth.server", False),
        ("/usr/bin/python3 -O vanth status", False),
    ],
)
def test_vanth_mcp_command_detection(command, expected):
    from vanth.server import _is_vanth_mcp_command

    assert _is_vanth_mcp_command(command) is expected


def test_remote_list_empty(daemon):
    tmp_path, client, port = daemon
    result = run_cli(tmp_path / "state", "remote", "list", port=port)
    assert result.returncode == 0, result.stdout + result.stderr


def test_remote_doctor_reports_binaries(daemon):
    tmp_path, client, port = daemon
    result = run_cli(tmp_path / "state", "remote", "doctor", "--json", port=port)
    assert result.returncode == 0, result.stdout + result.stderr
    payload = json.loads(result.stdout)
    assert "binaries" in payload
    assert payload["binaries"]["ssh"]


def test_remote_unknown_subcommand(daemon):
    tmp_path, _, port = daemon
    result = run_cli(tmp_path / "state", "remote", "frobnicate", port=port)
    assert result.returncode == 2


def test_artifacts_unknown_job(daemon):
    tmp_path, _, port = daemon
    result = run_cli(tmp_path / "state", "artifacts", "job_nope", port=port)
    assert result.returncode == 1
    assert "unknown job" in result.stderr.lower()


def test_prune_dry_run(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("import time; time.sleep(0.2)")})
    wait_status(client, started["job_id"], ["completed"])
    result = run_cli(tmp_path / "state", "prune", "--dry-run", port=port)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "would remove" in result.stdout
    assert started["job_id"] in result.stdout


def test_prune_dry_run_json(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("import time; time.sleep(0.2)")})
    wait_status(client, started["job_id"], ["completed"])
    result = run_cli(tmp_path / "state", "prune", "--dry-run", "--json", port=port)
    assert result.returncode == 0
    payload = json.loads(result.stdout)
    assert payload.get("count", 0) >= 1
    assert payload.get("dry_run") is True


def test_prune_yes(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("import time; time.sleep(0.2)")})
    wait_status(client, started["job_id"], ["completed"])
    result = run_cli(tmp_path / "state", "prune", "--yes", port=port)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "removed" in result.stdout
    after = client.get(f"/jobs/{started['job_id']}/status")
    assert after.get("result") == "error"


def test_prune_defaults_to_dry_run(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("import time; time.sleep(0.2)")})
    wait_status(client, started["job_id"], ["completed"])
    result = run_cli(tmp_path / "state", "prune", "--yes", port=port)
    assert result.returncode == 0
    client.post("/jobs", {"command": cmd("import time; time.sleep(0.2)")})
    second = client.get("/jobs", {"limit": 50})["jobs"][-1]
    wait_status(client, second["job_id"], ["completed"])
    result = run_cli(tmp_path / "state", "prune", port=port)
    assert result.returncode == 0
    assert "would remove" in result.stdout
