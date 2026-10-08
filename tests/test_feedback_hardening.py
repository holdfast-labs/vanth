"""Regression tests for the field-feedback hardening pass (see
planning/feedback-1.9-hardening.md).

Covers the fixes that came out of an agent's field report against vanth 1.9.1:

- ``vanth list`` duration/age + server-side status filtering
- the ``vanth start`` CLI (non-MCP front door) and its argv/``--`` handling
- ``POST /jobs`` field-level validation and explicit local idempotency rejection
- atomic job+wake-target acceptance
- readiness failing when the maintenance dispatcher is dead
- ``vanth setup`` opencode config precedence (JSON vs JSONC)
"""

from __future__ import annotations

import asyncio
import http.client
import json
import os
import re
import socket
import subprocess
import sys
import threading
import time

import pytest

from vanth import setup
from vanth.client import VanthClient
from vanth.server import JobManager

import shellcmd


cmd = shellcmd.cmd


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(autouse=True)
def isolate_opencode_plugin_dir(tmp_path, monkeypatch):
    """`run_setup` installs/removes the OpenCode wake plugin: a test must never
    touch the developer's real ~/.config/opencode/plugins."""
    monkeypatch.setenv("VANTH_OPENCODE_PLUGIN_DIR", str(tmp_path / "opencode-plugins"))


@pytest.fixture()
def daemon(tmp_path):
    """Start a real daemon on a temp home and yield (tmp_path, client, port)."""
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
    payload = {}
    while time.monotonic() < deadline:
        payload = client.get(f"/jobs/{job_id}/status")
        if payload.get("status") in statuses:
            return payload.get("status")
        time.sleep(0.1)
    raise AssertionError(f"job {job_id} never reached {statuses}: {payload}")


def http_get(port, path, token=None):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    connection.request("GET", path, headers=headers)
    response = connection.getresponse()
    body = json.loads(response.read() or b"{}")
    connection.close()
    return response.status, body


def http_post(port, path, payload, token=None):
    """POST raw JSON and return (status, decoded body). Used where the status
    code or a field-level error message matters."""
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    connection.request("POST", path, body=json.dumps(payload), headers=headers)
    response = connection.getresponse()
    body = json.loads(response.read() or b"{}")
    connection.close()
    return response.status, body


# --------------------------------------------------------------------------
# JobManager.list(): derived runtime/age fields
# --------------------------------------------------------------------------


def test_list_returns_runtime_and_timestamps(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = manager.start(cmd("import time; time.sleep(30)"))["job_id"]
        deadline = time.monotonic() + 10
        item = None
        while time.monotonic() < deadline:
            items = [job for job in manager.list()["jobs"] if job["job_id"] == job_id]
            if items and items[0].get("started_at"):
                item = items[0]
                break
            time.sleep(0.1)
        assert item is not None, "job never started"
        # The CLI derives DURATION/AGE from these; if they are absent the running
        # job renders blank duration and an age of 0s (the reported bug).
        for key in ("created_at", "started_at", "ended_at", "exit_code", "runtime_seconds"):
            assert key in item
        assert item["created_at"]
        assert item["runtime_seconds"] is not None and item["runtime_seconds"] >= 0
        assert item["exit_code"] is None

        asyncio.run(manager.stop(job_id))
    finally:
        manager.close()


# --------------------------------------------------------------------------
# start(): field-shape validation and atomic acceptance
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "kwargs, fragment",
    [
        ({"env": {"A": 1}}, "env"),
        ({"tags": "not-a-list"}, "tags"),
        ({"notify_on": "completed"}, "notify_on"),
        ({"name": 5}, "name"),
        ({"cwd": ""}, "cwd"),
        ({"interactive": "yes"}, "interactive"),
    ],
)
def test_start_rejects_bad_field_shapes(tmp_path, kwargs, fragment):
    manager = JobManager(tmp_path / "state")
    try:
        with pytest.raises(ValueError) as excinfo:
            manager.start(cmd("print(1)"), **kwargs)
        assert fragment in str(excinfo.value)
        # Nothing was persisted: a rejected request must not create a job row.
        assert manager.list()["jobs"] == []
    finally:
        manager.close()


def test_start_is_atomic_when_wake_target_insert_fails(tmp_path, monkeypatch):
    manager = JobManager(tmp_path / "state")
    try:

        def boom(*args, **kwargs):
            raise RuntimeError("wake target insert failed")

        monkeypatch.setattr(manager, "_insert_wake_targets", boom)
        target = {"type": "webhook", "url": "http://127.0.0.1:9/none", "events": ["completed"]}
        with pytest.raises(RuntimeError):
            manager.start(cmd("print(1)"), wake_targets=[target])
        # The job row and its targets are one transaction: a failed acceptance
        # must leave NO job behind (not a job that never notifies).
        assert manager.list()["jobs"] == []
    finally:
        manager.close()


# --------------------------------------------------------------------------
# doctor(): maintenance loop is a hard readiness signal
# --------------------------------------------------------------------------


def test_doctor_not_ok_when_maintenance_dead(tmp_path, monkeypatch):
    manager = JobManager(tmp_path / "state")
    try:
        # A real thread that has already finished: is_alive() is False, and
        # close() can still join it (join-before-start would raise).
        dead = threading.Thread(target=lambda: None)
        dead.start()
        dead.join()
        monkeypatch.setattr(manager, "dispatcher_thread", dead)
        report = manager.doctor()
        assert report["maintenance_alive"] is False
        # A dead dispatcher means queues and deliveries stop draining, so
        # readiness must not stay green.
        assert report["ok"] is False
    finally:
        manager.close()


# --------------------------------------------------------------------------
# HTTP boundary: field-level errors and local idempotency
# --------------------------------------------------------------------------


def test_post_jobs_unknown_field_names_it(daemon):
    tmp_path, client, port = daemon
    status, body = http_post(port, "/jobs", {"command": "echo hi", "bogus": 1}, token=client.token)
    assert status == 400
    assert "bogus" in body["error"]


def test_post_jobs_missing_command_is_named(daemon):
    tmp_path, client, port = daemon
    status, body = http_post(port, "/jobs", {}, token=client.token)
    assert status == 400
    assert "command" in body["error"]


def test_post_jobs_rejects_local_idempotency_key(daemon):
    tmp_path, client, port = daemon
    status, body = http_post(
        port, "/jobs", {"command": "echo hi", "idempotency_key": "abc"}, token=client.token
    )
    assert status == 400
    assert "idempotency_key" in body["error"]
    # Nothing was started.
    assert client.get("/jobs").get("jobs") == []


def test_post_jobs_malformed_wake_targets_is_400_not_500(daemon):
    tmp_path, client, port = daemon
    status, body = http_post(
        port, "/jobs", {"command": "echo hi", "wake_targets": [None]}, token=client.token
    )
    assert status == 400, body
    assert "wake_targets" in body["error"]


@pytest.mark.parametrize(
    "payload",
    [
        {"command": "echo hi", "wake_targets": [{"type": []}]},
        {"command": "echo hi", "trigger": {"job_id": "job_x", "status": []}},
        {"command": "echo hi", "policy": {"on_failure": {"after_n": 1, "action": []}}},
        {"command": "echo hi", "origin_thread_id": []},
    ],
)
def test_post_jobs_rejects_nested_shape_errors(daemon, payload):
    """Nested unhashable values must be a field-level 400, not an internal 500."""
    tmp_path, client, port = daemon
    status, body = http_post(port, "/jobs", payload, token=client.token)
    assert status == 400, body
    assert body["result"] == "error"
    assert client.get("/jobs").get("jobs") == []


def test_jobs_collection_exposes_runtime_fields(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("import time; time.sleep(30)")})
    wait_status(client, started["job_id"], ["running"])
    jobs = [job for job in client.get("/jobs")["jobs"] if job["job_id"] == started["job_id"]]
    assert len(jobs) == 1
    assert jobs[0]["runtime_seconds"] is not None
    assert jobs[0]["created_at"]
    client.post(f"/jobs/{started['job_id']}/stop", {})


def test_tail_rejects_invalid_stream_before_sql(daemon):
    """`stream` is interpolated into the SELECT, so it must be validated at the
    method boundary (a guard nested after a raise is dead code)."""
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("print('hi')")})
    job_id = started["job_id"]
    wait_status(client, job_id, ["completed"])
    status, body = http_get(port, f"/jobs/{job_id}/tail?stream=bogus", token=client.token)
    assert status == 400, body
    assert "stream" in body["error"]


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        (0, "0s"),
        (42, "42s"),
        (183, "3m 3s"),
        (3600, "1h"),
        (3700, "1h 1m 40s"),
        # >= 1 day used to print the leftover as raw seconds ("1d 1h 61s").
        (90061, "1d 1h 1m 1s"),
        (494927, "5d 17h 28m 47s"),
        (756742, "8d 18h 12m 22s"),
    ],
)
def test_humanize_normalizes_the_remainder(seconds, expected):
    from vanth.cli import _humanize

    assert _humanize(seconds) == expected


def test_list_without_matches_says_inflight_only(daemon):
    """`vanth list` defaults to in-flight only, so "no jobs" made a finished job
    look like it had vanished."""
    tmp_path, client, port = daemon
    result = run_cli(tmp_path / "state", "list", port=port)
    assert result.returncode == 0, result.stderr
    assert "no in-flight jobs" in result.stdout
    assert "--all" in result.stdout


def test_list_default_includes_launching_jobs(daemon):
    """A job is inserted as `launching` before the runner publishes `running`.
    With a running-only default it appeared in NEITHER `list` nor `list --all`
    for that window: started but invisible (caught by an onboarding test)."""
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("import time; time.sleep(30)"), "name": "fresh"})
    job_id = started["job_id"]
    result = run_cli(tmp_path / "state", "--json", "list", port=port)
    assert result.returncode == 0, result.stderr
    listed = {job["job_id"] for job in json.loads(result.stdout)}
    assert job_id in listed, f"just-started job missing from default list: {result.stdout}"
    client.post(f"/jobs/{job_id}/stop", {"signal": "kill"})


def test_cli_list_supports_name_and_tag_filters(daemon):
    """`vanth list` exposed only status/limit/--all, so the MCP `job_view`
    (thread-scoped) and name/tag filtering had no CLI equivalent."""
    tmp_path, client, port = daemon
    client.post("/jobs", {"command": cmd("import time; time.sleep(30)"), "name": "filt-alpha", "tags": ["red"]})
    client.post("/jobs", {"command": cmd("import time; time.sleep(30)"), "name": "filt-beta", "tags": ["blue"]})

    result = run_cli(tmp_path / "state", "--json", "list", "--name", "filt-alpha", port=port)
    assert result.returncode == 0, result.stderr
    assert {job["name"] for job in json.loads(result.stdout)} == {"filt-alpha"}

    result = run_cli(tmp_path / "state", "--json", "list", "--tag", "blue", port=port)
    assert result.returncode == 0, result.stderr
    assert {job["name"] for job in json.loads(result.stdout)} == {"filt-beta"}

    result = run_cli(tmp_path / "state", "--json", "list", "--thread-id", "no-such-thread", port=port)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == []


def test_cli_status_inspects_one_job_without_losing_daemon_status(daemon):
    """`vanth status <job-id>` silently ignored the argument before, and there
    was no single-job read on the CLI at all (a blind test looked for one)."""
    tmp_path, client, port = daemon
    job_id = client.post("/jobs", {"command": cmd("import time; time.sleep(30)"), "name": "inspect-me"})["job_id"]

    result = run_cli(tmp_path / "state", "status", job_id, port=port)
    assert result.returncode == 0, result.stderr
    assert job_id in result.stdout
    assert "inspect-me" in result.stdout

    # A prefix works, and an unknown id is an error rather than silence.
    result = run_cli(tmp_path / "state", "status", job_id[:-1], port=port)
    assert result.returncode == 0, result.stderr
    result = run_cli(tmp_path / "state", "status", "job_zzzz", port=port)
    assert result.returncode == 1
    assert "unknown job" in result.stderr

    # Bare `vanth status` is still daemon health.
    result = run_cli(tmp_path / "state", "status", port=port)
    assert "vanth daemon:" in result.stdout
    client.post(f"/jobs/{job_id}/stop", {"signal": "kill"})


def test_cli_wait_returns_the_event_and_times_out_with_3(daemon):
    """The CLI half of the documented "use job_wait, don't poll" workflow: a
    shell-only agent had no way to wait (a blind test fell back to sleep+list)."""
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("print('waited')"), "name": "wait-cli"})
    job_id = started["job_id"]
    result = run_cli(tmp_path / "state", "wait", job_id, "--events", "completed,failed", "--timeout", "30", port=port)
    assert result.returncode == 0, result.stderr
    assert "completed" in result.stdout

    long_job = client.post("/jobs", {"command": cmd("import time; time.sleep(30)"), "name": "wait-timeout"})["job_id"]
    result = run_cli(tmp_path / "state", "wait", long_job, "--timeout", "1", port=port)
    assert result.returncode == 3, result.stdout + result.stderr
    assert "timed out" in result.stdout
    client.post(f"/jobs/{long_job}/stop", {"signal": "kill"})


def test_cli_wait_defaults_to_every_terminal_outcome(daemon):
    tmp_path, client, port = daemon
    job_id = client.post(
        "/jobs", {"command": cmd("import time; time.sleep(30)"), "name": "wait-cancelled"}
    )["job_id"]
    client.post(f"/jobs/{job_id}/stop", {"signal": "kill"})
    wait_status(client, job_id, ["cancelled"])

    result = run_cli(tmp_path / "state", "wait", job_id, "--timeout", "5", port=port)
    assert result.returncode == 0, result.stderr
    assert "cancelled" in result.stdout


def test_cli_logs_accepts_a_job_id_prefix(daemon):
    """Observed onboarding failure: one dropped character from a hand-copied id
    produced "unknown job" with no hint."""
    tmp_path, client, port = daemon
    job_id = client.post("/jobs", {"command": cmd("print('prefix-ok')"), "name": "prefix"})["job_id"]
    wait_status(client, job_id, ["completed"])
    result = run_cli(tmp_path / "state", "logs", job_id[:-1], port=port)
    assert result.returncode == 0, result.stderr
    assert "prefix-ok" in result.stdout


def test_cli_per_command_help_does_not_report_unknown_option(daemon):
    tmp_path, client, port = daemon
    for command in ("start", "list", "logs", "stop", "wait", "diff", "setup", "remote", "api"):
        result = run_cli(tmp_path / "state", command, "--help", port=port)
        assert result.returncode == 0, f"{command}: {result.stderr}"
        assert f"usage: vanth {command}" in result.stdout

    root_help = run_cli(tmp_path / "state", "--help", port=port)
    assert "api" in root_help.stdout and "loopback HTTP" in root_help.stdout


def test_remote_tail_route_validates_and_reaches_the_remote(daemon):
    """GET /remotes/{id}/jobs/{job}/tail is the only path to a remote job's log:
    the controller-side log_range existed but nothing called it."""
    tmp_path, client, port = daemon
    status, body = http_get(port, "/remotes/nope/jobs/job_x/tail?stream=bogus", token=client.token)
    assert status == 400 and "stream" in body["error"], body
    status, body = http_get(port, "/remotes/nope/jobs/job_x/tail?size=0", token=client.token)
    assert status == 400 and "size" in body["error"], body
    # A malformed path is a 404, not an index error.
    status, body = http_get(port, "/remotes/nope/tail", token=client.token)
    assert status == 404, body
    status, body = http_get(port, "/remotes/nope/jobs/job_x/tail/extra", token=client.token)
    assert status == 404, body
    # A real (but unpaired) remote is a field-level 400, not a 500.
    status, body = http_get(port, "/remotes/nope/jobs/job_x/tail", token=client.token)
    assert status == 400 and "remote_id" in body["error"], body
    # follow/grep are unsupported for a remote byte range: reject, don't ignore.
    status, body = http_get(port, "/remotes/nope/jobs/job_x/tail?follow=true", token=client.token)
    assert status == 400 and "follow" in body["error"], body
    status, body = http_get(port, "/remotes/nope/jobs/job_x/tail?grep=error", token=client.token)
    assert status == 400 and "grep" in body["error"], body
    # parse_qs drops blank values: `?grep=` must still be rejected, not treated
    # as absent while the caller believes the log was filtered.
    status, body = http_get(port, "/remotes/nope/jobs/job_x/tail?grep=", token=client.token)
    assert status == 400 and "grep" in body["error"], body


def test_remote_mutation_errors_are_not_500s(daemon):
    """Remote mutations REQUIRE a caller-supplied idempotency_key; the rejection
    is actionable, so it must not be flattened into "Internal server error"."""
    tmp_path, client, port = daemon
    status, body = http_post(port, "/jobs", {"command": "echo x", "remote_id": "nope"}, token=client.token)
    assert status == 400, body
    assert "idempotency_key" in body["error"], body


def test_artifact_accessors_do_not_deadlock(daemon):
    """Nested lazy-init (get_artifact_collections -> get_artifacts) must not
    self-deadlock on the daemon's manager lock.

    With a plain Lock the first collections/lifecycle/profiles/broker request
    held the lock forever: the request hung, and every later artifact request
    (including plain materialize/verify) hung too, while /jobs still answered
    because get_manager() short-circuits on its cached global. Each call here
    hangs the test rather than failing loudly if that regresses.
    """
    tmp_path, client, port = daemon
    status, body = http_post(port, "/artifacts/collections", {"name": "deadlock-probe"}, token=client.token)
    assert status == 200, body
    status, body = http_post(port, "/artifacts/materialize", {}, token=client.token)
    assert status == 400 and "version_id" in body["error"], body
    status, body = http_post(port, "/artifacts/push-remote", {}, token=client.token)
    assert status == 400 and "remote_id" in body["error"], body


@pytest.mark.parametrize(
    ("route", "payload", "expected"),
    [
        ("/artifacts/materialize", {}, "version_id"),
        ("/artifacts/materialize", {"version_id": "v1"}, "dest_path"),
        ("/artifacts/verify", {}, "version_id"),
        ("/artifacts/push-remote", {}, "remote_id"),
        ("/artifacts/push-remote", {"remote_id": "r1"}, "version_id"),
        ("/artifacts/pull-remote", {"remote_id": "r1", "version_id": "v1"}, "dest_path"),
        ("/remote/helper", {"frame": 5}, "frame"),
        ("/remote/helper", {"frame": []}, "frame"),
    ],
)
def test_missing_fields_are_field_level_400s_not_500s(daemon, route, payload, expected):
    """A caller that forgets a required field must be told which one. Direct
    `payload[key]` indexing raised KeyError, which the handler maps to
    "Internal server error" - a server fault for a client mistake."""
    tmp_path, client, port = daemon
    status, body = http_post(port, route, payload, token=client.token)
    assert status == 400, body
    assert expected in (body.get("error") or "")


def test_relay_http_roundtrip_delivers_opencode_wake(daemon):
    """End-to-end over HTTP: exactly the register -> poll -> ack path the
    OpenCode plugin uses, with the session resolved from the registered
    directory rather than an attach URL."""
    tmp_path, client, port = daemon
    status, body = http_post(
        port,
        "/relay/register",
        {
            "client_id": "opencode-http",
            "client_type": "opencode_thread",
            "destinations": [
                {"client_type": "opencode_thread", "session_id": "ses_http", "directory": str(tmp_path)}
            ],
        },
        token=client.token,
    )
    assert status == 200, body
    started = client.post(
        "/jobs",
        {
            "command": cmd("print('wake me')"),
            "cwd": str(tmp_path),
            "wake_targets": [{"type": "opencode_thread", "events": ["completed"]}],
        },
    )
    job_id = started["job_id"]
    wait_status(client, job_id, ["completed"])
    status, body = http_get(port, "/relay/poll?client_id=opencode-http&timeout_seconds=5", token=client.token)
    assert status == 200, body
    deliveries = body["deliveries"]
    assert len(deliveries) == 1
    delivery = deliveries[0]
    assert delivery["payload"]["target"]["session_id"] == "ses_http"
    assert "vanth event" in delivery["payload"]["prompt"]
    status, body = http_post(
        port,
        "/relay/ack",
        {
            "client_id": "opencode-http",
            "delivery_id": delivery["delivery_id"],
            "status": "delivered",
            "lease_token": delivery["lease_token"],
        },
        token=client.token,
    )
    assert status == 200, body
    status, body = http_get(port, "/relay/poll?client_id=opencode-http&timeout_seconds=1", token=client.token)
    assert body["deliveries"] == []


def test_quote_for_cmd_rules():
    from vanth.cli import _quote_for_cmd

    assert _quote_for_cmd("plain") == "plain"
    assert _quote_for_cmd("") == '""'  # an empty argument must not collapse
    assert _quote_for_cmd("a b") == '"a b"'
    assert _quote_for_cmd("a&b") == '"a&b"'
    with pytest.raises(ValueError):
        _quote_for_cmd("%PATH%")


def test_daemon_json_documents_auth(daemon):
    tmp_path, _, _ = daemon
    metadata = json.loads((tmp_path / "state" / "daemon.json").read_text(encoding="utf-8"))
    assert metadata["auth"] == "bearer"
    assert metadata["token_path"].endswith("token")


# --------------------------------------------------------------------------
# CLI: list correctness
# --------------------------------------------------------------------------


def test_list_reports_duration_and_age_for_running_job(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("import time; time.sleep(30)"), "name": "feedbackjob"})
    job_id = started["job_id"]
    wait_status(client, job_id, ["running"])
    time.sleep(2)
    result = run_cli(tmp_path / "state", "list", port=port)
    assert result.returncode == 0, result.stdout + result.stderr
    lines = [line for line in result.stdout.splitlines() if job_id in line]
    assert lines, result.stdout
    parts = lines[0].split()
    # status job_id name duration age  (exit is blank while running)
    assert parts[0] == "running"
    assert parts[2] == "feedbackjob"
    assert parts[3] != "", f"blank DURATION: {lines[0]!r}"
    assert parts[4] != "0s", f"stale AGE: {lines[0]!r}"
    client.post(f"/jobs/{job_id}/stop", {})


def test_list_status_filter_is_honored(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("import sys; sys.exit(3)")})
    job_id = started["job_id"]
    wait_status(client, job_id, ["failed"])
    result = run_cli(tmp_path / "state", "list", "--status", "failed", port=port)
    assert result.returncode == 0, result.stdout + result.stderr
    assert job_id in result.stdout


def test_list_all_shows_terminal_and_conflicts_with_status(daemon):
    tmp_path, client, port = daemon
    started = client.post("/jobs", {"command": cmd("print('done')")})
    job_id = started["job_id"]
    wait_status(client, job_id, ["completed"])
    listed = run_cli(tmp_path / "state", "list", "--all", port=port)
    assert listed.returncode == 0, listed.stdout + listed.stderr
    assert job_id in listed.stdout

    conflict = run_cli(tmp_path / "state", "list", "--all", "--status", "completed", port=port)
    assert conflict.returncode == 2


# --------------------------------------------------------------------------
# CLI: start / deliveries / api
# --------------------------------------------------------------------------


def test_start_cli_starts_a_job(daemon):
    tmp_path, client, port = daemon
    result = run_cli(
        tmp_path / "state", "start", "--name", "cli-start", "--env", "FOO=bar", "--", cmd("print('started')"), port=port
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "started job_" in result.stdout
    jobs = [job for job in client.get("/jobs", {"name": "cli-start"})["jobs"]]
    assert len(jobs) == 1
    wait_status(client, jobs[0]["job_id"], ["completed"])


def test_start_cli_quotes_shell_metacharacters_safely(daemon):
    """A `&` in an argument must stay data, not become a second command.
    (The shell only sees it if the reassembly fails to quote it.)"""
    tmp_path, client, port = daemon
    result = run_cli(
        tmp_path / "state", "start", "--name", "amp",
        "--", sys.executable, "-c", "print('a&echo.INJECTED')",
        port=port,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    match = re.search(r"job_[0-9a-f]+", result.stdout)
    assert match
    job_id = match.group(0)
    wait_status(client, job_id, ["completed", "failed"])
    tail = client.get(f"/jobs/{job_id}/tail", {"stream": "stdout"})
    assert "a&echo.INJECTED" in (tail.get("content") or "")


@pytest.mark.skipif(os.name != "nt", reason="cmd.exe expands % even inside quotes")
def test_start_cli_rejects_unencodable_windows_argument(daemon):
    tmp_path, _, port = daemon
    result = run_cli(tmp_path / "state", "start", "--", "echo", "%PATH%", port=port)
    assert result.returncode == 2
    assert "metacharacters" in result.stderr


def test_start_cli_requires_a_command(daemon):
    tmp_path, _, port = daemon
    result = run_cli(tmp_path / "state", "start", port=port)
    assert result.returncode == 2
    assert "missing command" in result.stderr


def test_start_cli_rejects_unknown_option(daemon):
    tmp_path, _, port = daemon
    result = run_cli(tmp_path / "state", "start", "--nope", "echo", port=port)
    assert result.returncode == 2
    assert "unknown option" in result.stderr


def test_start_cli_json_before_separator(daemon):
    tmp_path, client, port = daemon
    result = run_cli(
        tmp_path / "state", "start", "--json", "--name", "cli-json", "--", cmd("print('j')"), port=port
    )
    assert result.returncode == 0, result.stdout + result.stderr
    payload = json.loads(result.stdout)
    assert payload["job_id"].startswith("job_")


def test_start_cli_preserves_quoting_without_a_prebuilt_string(daemon):
    """argv is re-quoted for the host shell, so a program argument containing a
    space stays one argument (a naive space join would break it)."""
    tmp_path, client, port = daemon
    result = run_cli(
        tmp_path / "state", "start", "--name", "quoting",
        "--", sys.executable, "-c", "print('a b')",
        port=port,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    match = re.search(r"job_[0-9a-f]+", result.stdout)
    assert match, result.stdout
    job_id = match.group(0)
    wait_status(client, job_id, ["completed", "failed"])
    tail = client.get(f"/jobs/{job_id}/tail", {"stream": "stdout"})
    assert "a b" in (tail.get("content") or "")


def test_deliveries_cli_runs(daemon):
    tmp_path, _, port = daemon
    result = run_cli(tmp_path / "state", "deliveries", "--status", "failed", port=port)
    assert result.returncode == 0, result.stdout + result.stderr


def test_api_cli_lists_routes_and_auth(daemon):
    tmp_path, _, port = daemon
    result = run_cli(tmp_path / "state", "api", port=port)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Authorization: Bearer" in result.stdout
    assert "POST" in result.stdout


# --------------------------------------------------------------------------
# setup: opencode config precedence (JSON vs JSONC)
# --------------------------------------------------------------------------


def test_config_state_classifies_entries(tmp_path):
    configured = tmp_path / "opencode.json"
    configured.write_text(json.dumps({"mcp": {"vanth": {"type": "local", "command": ["vanth"]}}}), encoding="utf-8")
    assert setup.config_state("opencode", configured) == "configured"

    disabled = tmp_path / "disabled.json"
    disabled.write_text(json.dumps({"mcp": {"vanth": {"enabled": False}}}), encoding="utf-8")
    assert setup.config_state("opencode", disabled) == "disabled"

    absent = tmp_path / "absent.json"
    absent.write_text(json.dumps({"mcp": {}}), encoding="utf-8")
    assert setup.config_state("opencode", absent) == "not-configured"

    # A commented/trailing-comma JSONC file is parsed READ-ONLY (a scanner
    # strips comments in memory; the file itself is never rewritten), including
    # a comment between the key and its colon.
    jsonc = tmp_path / "opencode.jsonc"
    jsonc.write_text(
        '{\n  // comment\n  "mcp": {"vanth" /* why */ : {"enabled": false}},\n}\n',
        encoding="utf-8",
    )
    assert setup.config_state("opencode", jsonc) == "disabled"

    broken = tmp_path / "broken.json"
    broken.write_text("{not json", encoding="utf-8")
    assert setup.config_state("opencode", broken) == "unreadable"


def test_select_write_target_prefers_json_and_refuses_commented_jsonc(tmp_path):
    # A .jsonc with no comments/trailing commas round-trips as plain JSON, so it
    # is editable; only a COMMENTED file (which we would have to rewrite) is
    # refused.
    commented = tmp_path / "opencode.jsonc"
    commented.write_text('{\n  // settings\n  "mcp": {}\n}\n', encoding="utf-8")
    path, note = setup._select_write_target("opencode", [commented])
    assert path is None
    assert "commented JSONC" in note

    plain_jsonc = tmp_path / "plain.jsonc"
    plain_jsonc.write_text("{}", encoding="utf-8")
    path, note = setup._select_write_target("opencode", [plain_jsonc])
    assert path == plain_jsonc
    assert note == ""

    plain = tmp_path / "opencode.json"
    plain.write_text("{}", encoding="utf-8")
    path, note = setup._select_write_target("opencode", [commented, plain])
    assert path == plain
    assert note == ""


def test_select_write_target_refuses_shadowed_write(tmp_path):
    plain = tmp_path / "opencode.json"
    plain.write_text("{}", encoding="utf-8")
    # A higher-precedence JSONC that already defines mcp.vanth would override
    # anything written to opencode.json, so setup must refuse rather than write
    # a registration that silently does nothing.
    jsonc = tmp_path / "opencode.jsonc"
    jsonc.write_text(json.dumps({"mcp": {"vanth": {"enabled": False}}}), encoding="utf-8")
    path, note = setup._select_write_target("opencode", [jsonc, plain])
    assert path is None
    assert "opencode.jsonc" in note


def test_effective_state_respects_precedence(tmp_path):
    plain = tmp_path / "opencode.json"
    plain.write_text(json.dumps({"mcp": {"vanth": {"command": ["vanth"]}}}), encoding="utf-8")
    jsonc = tmp_path / "opencode.jsonc"
    jsonc.write_text(json.dumps({"mcp": {"vanth": {"enabled": False}}}), encoding="utf-8")
    # The jsonc wins in opencode's merge, so the effective state is disabled.
    assert setup.effective_state("opencode", [plain, jsonc]) == "disabled"
    assert setup.effective_state("opencode", [plain]) == "configured"


def test_effective_state_deep_merges_enabled(tmp_path):
    # opencode deep-merges config files, so an `enabled: false` in a lower file
    # survives a higher file that only adds another key.
    plain = tmp_path / "opencode.json"
    plain.write_text(json.dumps({"mcp": {"vanth": {"command": ["vanth"], "enabled": False}}}), encoding="utf-8")
    jsonc = tmp_path / "opencode.jsonc"
    jsonc.write_text(json.dumps({"mcp": {"vanth": {"command": ["vanth"], "timeout": 1000}}}), encoding="utf-8")
    assert setup.effective_state("opencode", [plain, jsonc]) == "disabled"


def test_setup_installs_and_removes_opencode_wake_plugin(tmp_path, monkeypatch):
    """TUI sessions expose no attach URL, so onboarding OpenCode must ship the
    in-process wake plugin or opencode_thread wakes can never be delivered."""
    plain = tmp_path / "opencode.json"
    plain.write_text(json.dumps({"mcp": {}}), encoding="utf-8")
    monkeypatch.setattr(setup, "client_config_paths", lambda home=None: {"opencode": [plain]})
    assert setup.run_setup(["opencode"], home=tmp_path, assume_yes=True) == 0
    target = setup.plugin_target()
    assert target.is_file()
    assert "opencode_thread" in target.read_text(encoding="utf-8")
    assert setup.install_opencode_plugin() == (False, "already installed")
    assert setup.run_setup(["opencode"], home=tmp_path, remove=True, assume_yes=True) == 0
    assert not target.is_file()


def test_remove_with_nothing_registered_succeeds(tmp_path, monkeypatch):
    plain = tmp_path / "opencode.json"
    plain.write_text(json.dumps({"mcp": {}}), encoding="utf-8")
    monkeypatch.setattr(setup, "client_config_paths", lambda home=None: {"opencode": [plain]})
    assert setup.run_setup(["opencode"], home=tmp_path, remove=True, assume_yes=True) == 0


def test_remove_with_no_config_at_all_succeeds(tmp_path, monkeypatch):
    monkeypatch.setattr(setup, "client_config_paths", lambda home=None: {})
    assert setup.run_setup(["opencode"], home=tmp_path, remove=True, assume_yes=True) == 0


def test_run_setup_creates_missing_requested_client(tmp_path, monkeypatch, capsys):
    codex = tmp_path / "config.toml"
    codex.write_text('model = "x"\n', encoding="utf-8")
    monkeypatch.setattr(setup, "client_config_paths", lambda home=None: {"codex": [codex]})
    monkeypatch.setattr(setup, "_default_config_path", lambda client: tmp_path / f"{client}.json")
    result = setup.run_setup(["opencode", "codex"], home=tmp_path, assume_yes=True, json_out=True)
    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert json.loads((tmp_path / "opencode.json").read_text(encoding="utf-8"))["mcp"]["vanth"]


def test_remove_clears_every_registration(tmp_path, monkeypatch):
    first = tmp_path / "opencode.json"
    second = tmp_path / "config.json"
    for path in (first, second):
        path.write_text(json.dumps({"mcp": {"vanth": {"command": ["vanth"]}}}), encoding="utf-8")
    monkeypatch.setattr(setup, "client_config_paths", lambda home=None: {"opencode": [first, second]})
    assert setup.run_setup(["opencode"], home=tmp_path, remove=True, assume_yes=True) == 0
    for path in (first, second):
        assert "vanth" not in (json.loads(path.read_text(encoding="utf-8")).get("mcp") or {})


def test_run_setup_reports_skipped_jsonc_client(tmp_path, monkeypatch, capsys):
    jsonc = tmp_path / "opencode.jsonc"
    jsonc.write_text('{\n  // commented\n  "mcp": {}\n}\n', encoding="utf-8")
    codex = tmp_path / "config.toml"
    codex.write_text('model = "x"\n', encoding="utf-8")
    monkeypatch.setattr(
        setup, "client_config_paths", lambda home=None: {"opencode": [jsonc], "codex": [codex]}
    )
    result = setup.run_setup(["opencode", "codex"], home=tmp_path, assume_yes=True, json_out=True)
    assert result == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert any(item["client"] == "opencode" for item in payload["skipped"])
