"""Exercise agent conveniences through actual daemon HTTP and MCP stdio."""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path

import pytest
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

from vanth.client import VanthClient

import shellcmd


@pytest.fixture
def live_daemon(tmp_path):
    home = tmp_path / "state"
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    env = {**os.environ, "VANTH_HOME": str(home), "VANTH_DAEMON_PORT": str(port),
           "VANTH_REMOTE_WAKE_SYNC_SECONDS": "0"}
    for key in ("CODEX_THREAD_ID", "VANTH_DAEMON_URL"):
        env.pop(key, None)
    url = f"http://127.0.0.1:{port}"
    processes = []

    def launch():
        proc = subprocess.Popen([sys.executable, "-m", "vanth.daemon"], env=env,
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        processes.append(proc)
        client = VanthClient(url, home)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                if client.get("/health") == {"ok": True}:
                    return client
            except (OSError, ValueError):
                pass
            assert proc.poll() is None, "isolated daemon exited during startup"
            time.sleep(.05)
        raise AssertionError("isolated daemon startup timed out")

    client = launch()
    try:
        yield home, client, env, launch, processes
    finally:
        try:
            for job in client.get("/jobs", {"limit": 100})["jobs"]:
                if job["status"] in {"running", "launching", "queued", "retrying", "paused", "stopping"}:
                    client.post(f"/jobs/{job['job_id']}/stop", {"kill_after_seconds": 1})
            client.post("/shutdown", {})
        except (OSError, ValueError):
            pass
        for proc in processes:
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.terminate()
                proc.wait(timeout=10)


def terminal(client, job_id):
    waited = client.post(f"/jobs/{job_id}/wait", {
        "filters": ["completed", "failed", "timeout", "cancelled", "orphaned"],
        "timeout_seconds": 10}, timeout=15)
    assert waited["status"] == "completed", waited


def test_http_preview_has_no_launch_or_durable_acceptance(live_daemon, tmp_path):
    home, client, *_ = live_daemon
    marker = tmp_path / "should-not-exist"
    command = shellcmd.cmd(f"from pathlib import Path; Path({str(marker)!r}).write_text('ran')")
    preview = client.post("/jobs/preview", {"command": command, "cwd": str(tmp_path),
        "idempotency_key": "preview-key-123", "env": {"TOKEN": "preview-secret"}, "secret_env": ["TOKEN"]})
    assert preview["result"] == "preview"
    assert preview["cwd"] == str(tmp_path.resolve())
    assert preview["shell"] and preview["env_names"] == ["TOKEN"]
    assert "preview-secret" not in json.dumps(preview)
    assert client.get("/jobs")["jobs"] == []
    with sqlite3.connect(home / "jobs.sqlite") as db:
        assert db.execute("SELECT COUNT(*) FROM local_start_requests").fetchone()[0] == 0
    assert not marker.exists()
    assert not list((home / "specs").glob("*.json"))


def test_http_concurrent_retries_restart_and_conflict(live_daemon, tmp_path):
    home, client, _, launch, processes = live_daemon
    marker = tmp_path / "executions.txt"
    payload = {"command": shellcmd.cmd(f"from pathlib import Path; Path({str(marker)!r}).open('a').write('once\\n')"),
               "cwd": str(tmp_path), "idempotency_key": "concurrent-retry-key", "timeout_seconds": 10}
    def start(_):
        return VanthClient(client.url, home).post("/jobs", payload)
    with ThreadPoolExecutor(max_workers=8) as pool:
        replies = list(pool.map(start, range(8)))
    assert all("job_id" in reply for reply in replies), replies
    job_ids = {reply["job_id"] for reply in replies}
    assert len(job_ids) == 1, replies
    job_id = next(iter(job_ids))
    terminal(client, job_id)
    assert marker.read_text() == "once\n"
    assert len(client.get("/jobs")["jobs"]) == 1
    client.post("/shutdown", {})
    processes[-1].wait(timeout=15)
    restarted = launch()
    replay = restarted.post("/jobs", payload)
    assert replay["job_id"] == job_id and replay["idempotent_replay"] is True
    assert marker.read_text() == "once\n"
    altered = {**payload, "command": shellcmd.cmd("print('different')")}
    request = urllib.request.Request(client.url + "/jobs", data=json.dumps(altered).encode(),
        headers={"Authorization": f"Bearer {restarted.token}", "Content-Type": "application/json"})
    with pytest.raises(urllib.error.HTTPError) as failure:
        urllib.request.urlopen(request, timeout=10)
    assert failure.value.code == 400
    assert "different job request" in json.loads(failure.value.read())["error"]
    assert len(restarted.get("/jobs")["jobs"]) == 1


def test_http_doctor_detects_corrupt_artifact_when_requested(live_daemon):
    home, client, *_ = live_daemon
    data = b"isolated artifact integrity regression"
    published = client.post("/artifacts/put", {"name": "integrity.bin", "data_b64": base64.b64encode(data).decode()})
    assert "version_id" in published, published
    sha = hashlib.sha256(data).hexdigest()
    healthy = client.get("/doctor", {"verify_artifacts": True})
    assert healthy["artifact_integrity"]["checked"] == 1, healthy
    path = home / "artifacts-store" / "blobs" / sha[:2] / sha[2:4] / sha
    path.write_bytes(b"corrupt")
    ordinary = client.get("/doctor")
    assert ordinary["artifact_integrity"]["requested"] is False
    verified = client.get("/doctor", {"verify_artifacts": True})
    assert verified["ok"] is False
    assert verified["artifact_integrity"]["complete"] is True
    assert verified["artifact_integrity"]["issues"] == [{"type": "corrupt_artifact_blob", "sha256": sha}]
    # A valid JSON value with the wrong shape is catalog corruption too.
    with sqlite3.connect(home / "artifacts.sqlite") as db:
        db.execute("UPDATE versions SET manifest_json='[]'")
    malformed = client.get("/doctor", {"verify_artifacts": True})
    assert malformed["ok"] is False
    assert malformed["artifact_integrity"]["issues"][0]["type"] == "artifact_catalog_check_failed"


def test_http_tilde_preview_matches_execution_and_retry_fingerprint(live_daemon):
    _, client, *_ = live_daemon
    payload = {"command": shellcmd.cmd("import os; print(os.getcwd())"), "cwd": "~",
               "idempotency_key": "tilde-normalized-key", "timeout_seconds": 10}
    expected = str(Path.home().resolve())
    preview = client.post("/jobs/preview", payload)
    assert preview["cwd"] == expected
    started = client.post("/jobs", payload)
    assert "job_id" in started, started
    terminal(client, started["job_id"])
    tail = client.get(f"/jobs/{started['job_id']}/tail")
    assert tail["content"].strip() == expected
    replay = client.post("/jobs", {**payload, "cwd": expected})
    assert replay["job_id"] == started["job_id"] and replay["idempotent_replay"] is True


def test_http_failure_guidance_distinguishes_launch_from_workload(live_daemon, tmp_path):
    _, client, *_ = live_daemon
    cases = [
        ({"command": shellcmd.cmd("print('cannot launch')"), "cwd": str(tmp_path / "missing-directory")}, "startup_failed"),
        ({"command": shellcmd.cmd("raise SystemExit(7)"), "cwd": str(tmp_path)}, "workload_failed"),
    ]
    for payload, expected in cases:
        started = client.post("/jobs", {**payload, "timeout_seconds": 10})
        assert "job_id" in started, started
        waited = client.post(f"/jobs/{started['job_id']}/wait", {"filters": ["failed"], "timeout_seconds": 10}, timeout=15)
        assert waited["status"] == "failed", waited
        status = client.get(f"/jobs/{started['job_id']}/status")
        assert status["failure_reason"] == expected, status
        assert status["recommended_next_action"]
        summary = client.get(f"/jobs/{started['job_id']}/summary")
        assert summary["failure_reason"] == expected, summary


def test_mcp_preview_retry_masked_and_bounded_stdout(live_daemon, tmp_path):
    home, client, env, *_ = live_daemon
    marker = tmp_path / "mcp-executions"
    command = shellcmd.cmd(f"import os; from pathlib import Path; Path({str(marker)!r}).open('a').write('once\\n'); print('A'*9000); print(os.environ['SECRET']); print('final marker')")
    arguments = {"command": command, "cwd": str(tmp_path), "env": {"SECRET": "masked-first\nmasked-second"},
                 "secret_env": ["SECRET"], "idempotency_key": "mcp-retry-same-request", "timeout_seconds": 10}

    def content(result):
        assert not result.isError, result
        return result.structuredContent if result.structuredContent is not None else json.loads(result.content[0].text)

    async def run():
        server = StdioServerParameters(command=sys.executable, args=["-m", "vanth"],
            cwd=str(Path(__file__).parents[1]), env={**env, "VANTH_DAEMON_URL": client.url})
        async with stdio_client(server) as (read, write):
            async with ClientSession(read, write, read_timeout_seconds=timedelta(seconds=25)) as session:
                await session.initialize()
                preview = content(await session.call_tool("job_start", {**arguments, "dry_run": True}))
                assert preview["result"] == "preview"
                assert not marker.exists()
                assert client.get("/jobs")["jobs"] == []
                first = content(await session.call_tool("job_start_and_wait", {**arguments, "wait_timeout_seconds": 10}))
                assert first["status"] == "completed", first
                excerpt = first["summary"]["stdout_excerpt"]
                assert excerpt.rstrip().endswith("final marker")
                assert "masked-first" not in excerpt and "masked-second" not in excerpt
                assert "***" in excerpt
                assert len(excerpt.encode()) <= 8192
                second = content(await session.call_tool("job_start_and_wait", {**arguments, "wait_timeout_seconds": 10}))
                assert second["job_id"] == first["job_id"]
                assert marker.read_text() == "once\n"
                doctor = content(await session.call_tool("job_doctor", {"verify_artifacts": True}))
                assert doctor["artifact_integrity"]["requested"] is True
    asyncio.run(run())
