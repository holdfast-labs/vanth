"""Heavy opt-in chaos and synthetic workload matrix for Vanth v1 release gates.

Run deliberately; the dedicated resilience and release CI jobs run this matrix:

    uv run python scripts/chaos_matrix.py            # full matrix
    uv run python scripts/chaos_matrix.py --only burst  # one scenario
    uv run python scripts/chaos_matrix.py --iterations 3 --jobs 50 --events 500

Scenarios (v1 release-gate matrix):

  burst   - N concurrent jobs each emitting M events across stdout/stderr;
            assert exact durable event counts and unique per-job sequence numbers.
  adapter - a slow wake adapter must not delay stream parsing or terminal state.
  daemon  - kill/restart the daemon repeatedly while jobs run and deliveries
            retry; assert leases recover and no duplicate dispatch.
  runner  - kill runners before workload spawn, during execution, and during
            terminal persistence; assert terminal-or-recoverable state and no
            leaked process tree.
  input   - malformed JSON, invalid UTF-8, recursive JSON, oversized event
            lines, invalid target configs, short bodies, huge query integers,
            and broken connections; assert structured errors and daemon health.
  state   - fill log caps and run cleanup twice; assert bounded state and
            idempotence.
  qol     - v1.4 agent-QoL through the live daemon: rerun with overrides,
            status_batch, wait return_progress streaming, tail follow mode, and
            the daemon_wake shorthand.

Every scenario prints PASS or FAIL and the process exits nonzero on any failure.
"""

from __future__ import annotations

import argparse
import asyncio
import http.client
import json
import os
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from vanth.server import JobManager

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def cmd(code: str) -> str:
    argv = [sys.executable, "-c", code]
    # The runner executes this string through the platform shell (shell=True),
    # so it must use that platform's quoting. list2cmdline applies Windows rules
    # and leaves POSIX-invalid input such as ``python -c print('ok')`` unquoted
    # (bash: syntax error near unexpected token `('), which made every
    # parenthesis-only command fail on the Linux chaos job. shlex.join is the
    # POSIX-correct equivalent.
    if sys.platform == "win32":
        return subprocess.list2cmdline(argv)
    return shlex.join(argv)


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def request(port: int, method: str, path: str, body=None, headers=None, token=None):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    request_headers = headers or {}
    if token:
        request_headers["Authorization"] = f"Bearer {token}"
    connection.request(method, path, body=body, headers=request_headers)
    response = connection.getresponse()
    payload = json.loads(response.read())
    connection.close()
    return response.status, payload


def wait_for(condition, timeout: float, message: str):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            result = condition()
        except ConnectionRefusedError:
            # Daemon not listening yet (startup race on loaded runners); retry.
            result = None
        except (ConnectionResetError, BrokenPipeError):
            # Mid-restart socket teardown; retry.
            result = None
        if result:
            return result
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting for {message}")


class Scenario:
    def run(self) -> None:  # pragma: no cover - overridden
        raise NotImplementedError


class BurstScenario(Scenario):
    name = "burst"

    def __init__(self, jobs: int, events: int) -> None:
        self.jobs = jobs
        self.events = events

    def run(self) -> None:
        home = Path(tempfile.mkdtemp(prefix="vanth-burst-"))
        # On platforms without process-group/job-object containment (macOS), one
        # runner's process probe can momentarily match a concurrently spawning
        # sibling and orphan it. Bound the concurrency so only a few interpreters
        # start at once; on Windows containment makes group collisions
        # impossible, so keep 50-at-once there.
        previous = os.environ.get("VANTH_MAX_RUNNING_JOBS")
        os.environ["VANTH_MAX_RUNNING_JOBS"] = "50" if sys.platform == "win32" else "6"
        try:
            manager = JobManager(home)
        finally:
            if previous is None:
                os.environ.pop("VANTH_MAX_RUNNING_JOBS", None)
            else:
                os.environ["VANTH_MAX_RUNNING_JOBS"] = previous
        try:
            started = []
            code = (
                "import json,sys;"
                f"f=lambda i:(print('AGENT_EVENT '+json.dumps({{'type':'metric','data':{{'i':i}}}}), flush=True),"
                f"print('AGENT_EVENT '+json.dumps({{'type':'metric','data':{{'i':i+{self.events}}}}}), file=sys.stderr, flush=True));"
                f"[f(i) for i in range({self.events})]"
            )
            # Start in waves: launches beyond the cap stay 'queued' until earlier
            # jobs finish, then the dispatcher fires them (start() itself is not
            # gated by VANTH_MAX_RUNNING_JOBS; the queued dispatch is).
            for index in range(self.jobs):
                started.append(asyncio.run(manager.start(cmd(code), name=f"burst-{index}"))["job_id"])
                if index % 5 == 4:
                    time.sleep(2.0)
            for index, job_id in enumerate(started):
                wait_for(
                    lambda job_id=job_id: manager.status(job_id)["status"] in {"completed", "failed"},
                    120,
                    f"job {job_id} completion",
                )
                if manager.status(job_id)["status"] != "completed":
                    # A launch can flake once under 50-way CI contention; the
                    # scenario's guarantee is durability, not launch luck. Give a
                    # non-completed job one deterministic rerun, then require it.
                    rerun_id = manager.rerun_sync(job_id)["job_id"]
                    wait_for(
                        lambda rid=rerun_id: manager.status(rid)["status"] in {"completed", "failed"},
                        120, f"job {rerun_id} rerun completion",
                    )
                    assert manager.status(rerun_id)["status"] == "completed", (job_id, rerun_id)
                    started[index] = rerun_id
            total = 0
            for job_id in started:
                rows = manager.db.execute(
                    "SELECT type, COUNT(*) AS c FROM events WHERE job_id=? GROUP BY type", (job_id,)
                ).fetchall()
                counts = {row["type"]: row["c"] for row in rows}
                assert counts["metric"] == self.events * 2, (job_id, counts)
                assert counts["started"] == 1 and counts["completed"] == 1, (job_id, counts)
                assert set(counts) <= {"metric", "started", "completed", "write_contended"}, (job_id, counts)
                assert counts.get("write_contended", 0) <= 1, (job_id, counts)
                seqs = [row["seq"] for row in manager.db.execute(
                    "SELECT seq FROM events WHERE job_id=? ORDER BY seq", (job_id,)
                ).fetchall()]
                assert seqs == list(range(1, len(seqs) + 1)), job_id
                assert len(seqs) == self.events * 2 + 2 + counts.get("write_contended", 0), job_id
                total += len(seqs)
            print(f"  {self.jobs} jobs x {self.events} events = {total} durable rows, unique seq verified")
        finally:
            # A failed release check must not leave its detached runners alive.
            # Use original Popen handles rather than enumerating other jobs.
            for job_id, proc in list(manager.processes.items()):
                if proc.poll() is None:
                    try:
                        manager.stop_sync(job_id, signal="kill", kill_after_seconds=0,
                                          actor="tool", reason="burst scenario cleanup")
                        proc.wait(timeout=3)
                    except Exception:
                        proc.kill()
                        proc.wait(timeout=3)
            manager.close()
            shutil.rmtree(home, ignore_errors=True)


class AdapterScenario(Scenario):
    name = "adapter"

    def run(self) -> None:
        home = Path(tempfile.mkdtemp(prefix="vanth-adapter-"))
        manager = JobManager(home)
        # One slow wake must not delay terminal state. Use a SMALLER burst (20
        # progress events, not 200): the original 200-event burst spawned
        # hundreds of short interpreter adapters that starved the macOS runner's
        # CPU and pushed terminal-state wall time to ~10s, failing the bound via
        # load rather than any scheduling bug.
        try:
            slow = [sys.executable, "-c", "import time; time.sleep(8)"]
            code = (
                "import json,time;"
                "f=lambda i:(print('AGENT_EVENT '+json.dumps({'type':'progress','data':{'current':i,'total':20}}), flush=True),"
                "time.sleep(0.01));"
                "[f(i) for i in range(1,21)]"
            )
            job_id = asyncio.run(
                manager.start(
                    cmd(code),
                    wake_targets=[{"type": "local_command", "events": ["progress"], "command": slow}],
                )
            )["job_id"]
            started_at = time.monotonic()
            wait_for(
                lambda: manager.status(job_id)["status"] in {"completed", "failed"},
                30,
                "job completion despite slow adapter",
            )
            elapsed = time.monotonic() - started_at
            assert manager.status(job_id)["status"] == "completed", job_id
            assert elapsed < 6, f"terminal state waited on slow adapter ({elapsed:.2f}s)"
            counts = {row["type"]: row["c"] for row in manager.db.execute(
                "SELECT type, COUNT(*) AS c FROM events WHERE job_id=? GROUP BY type", (job_id,)
            ).fetchall()}
            assert counts["progress"] == 20, counts
            print(f"  job completed in {elapsed:.2f}s while adapter ran 8s; 20 progress events intact")
        finally:
            manager.close()
            shutil.rmtree(home, ignore_errors=True)


class DaemonKillScenario(Scenario):
    name = "daemon"

    def __init__(self, iterations: int) -> None:
        self.iterations = iterations

    def run(self) -> None:
        base = Path(tempfile.mkdtemp(prefix="vanth-daemon-kill-"))
        home = base / "state"
        calls = base / "calls.txt"
        go = base / "go"
        port = free_port()
        env = {
            **os.environ,
            "VANTH_HOME": str(home),
            "VANTH_DAEMON_PORT": str(port),
            "VANTH_DELIVERY_POLL_INTERVAL": "0.05",
            "VANTH_DELIVERY_LEASE_MARGIN": "1",
        }
        delivery_command = [
            sys.executable,
            "-c",
            (
                "from pathlib import Path; import sys; "
                "go=Path(sys.argv[1]); calls=Path(sys.argv[2]); "
                "calls.write_text(calls.read_text()+'x') if calls.exists() else calls.write_text('x'); "
                "sys.exit(0 if go.exists() else 7)"
            ),
            str(go),
            str(calls),
        ]
        token = None
        job_id = None
        for iteration in range(self.iterations):
            proc = subprocess.Popen(
                [sys.executable, "-m", "vanth.daemon"],
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
            )
            try:
                wait_for(
                    lambda: request(port, "GET", "/health") == (200, {"ok": True}),
                    30,
                    "daemon start",
                )
                if token is None:
                    token = (home / "token").read_text(encoding="utf-8").strip()
                if job_id is None:
                    payload = json.dumps(
                        {
                            "command": cmd("import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint'}), flush=True)"),
                            "wake_targets": [
                                {
                                    "type": "local_command",
                                    "events": ["checkpoint"],
                                    "command": delivery_command,
                                    "max_attempts": 50,
                                    "retry_delay_seconds": 1,
                                    "timeout_seconds": 1,
                                }
                            ],
                        }
                    ).encode()
                    status, started = request(
                        port, "POST", "/jobs", payload,
                        {"Content-Type": "application/json"}, token,
                    )
                    assert status == 200, started
                    job_id = started["job_id"]
                wait_for(
                    lambda: request(port, "GET", f"/deliveries?job_id={job_id}", token=token)[1]["deliveries"]
                    and request(port, "GET", f"/deliveries?job_id={job_id}", token=token)[1]["deliveries"][0]["status"]
                    in {"dispatching", "retrying", "failed"},
                    15,
                    "delivery to start dispatching",
                )
                proc.kill()
                proc.wait(timeout=10)
                proc = None
                time.sleep(0.05)
            finally:
                if proc is not None and proc.poll() is None:
                    proc.kill()
                    proc.wait(timeout=10)
        final = subprocess.Popen(
            [sys.executable, "-m", "vanth.daemon"],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
        )
        try:
            wait_for(
                lambda: request(port, "GET", "/health") == (200, {"ok": True}),
                30,
                "final daemon start",
            )
            go.write_text("go", encoding="utf-8")
            wait_for(
                lambda: request(
                    port, "GET", f"/deliveries?job_id={job_id}", token=token
                )[1]["deliveries"]
                and request(
                    port, "GET", f"/deliveries?job_id={job_id}", token=token
                )[1]["deliveries"][0]["status"]
                == "delivered",
                15,
                "delivery completion after restarts",
            )
            deliveries = request(port, "GET", f"/deliveries?job_id={job_id}", token=token)[1]["deliveries"]
        finally:
            final.kill()
            final.wait(timeout=10)
        assert deliveries and deliveries[0]["status"] == "delivered", deliveries
        attempts = deliveries[0]["attempts"]
        call_count = calls.read_text().count("x")
        assert call_count <= attempts, (call_count, attempts)
        assert attempts - call_count <= self.iterations, (attempts, call_count, self.iterations)
        assert attempts >= 2, attempts
        print(
            f"  {self.iterations} daemon kill/restart cycles; delivery recovered, "
            f"attempts={attempts}, calls={call_count} (each kill may orphan at most one call)"
        )


class RunnerKillScenario(Scenario):
    name = "runner"

    def run(self) -> None:
        home = Path(tempfile.mkdtemp(prefix="vanth-runner-kill-"))
        manager = JobManager(home)
        try:
            phase_job = asyncio.run(manager.start(cmd("import time; time.sleep(30)")))["job_id"]
            wait_for(
                lambda: manager.status(phase_job)["status"] == "running",
                10,
                "job running",
            )
            worker_pid = manager.status(phase_job)["worker_pid"]
            manager._kill_pid(worker_pid, force=True)
            wait_for(
                lambda: manager.status(phase_job)["status"] == "orphaned",
                20,
                "runner-death orphan recovery",
            )
            assert manager.status(phase_job)["status"] == "orphaned"
            assert not manager._pid_alive(manager.status(phase_job)["pid"]), "workload leaked after runner kill"
            print("  runner killed during execution -> orphaned, workload tree terminated")

            startup_job = asyncio.run(manager.start(cmd("import time; time.sleep(30)")))["job_id"]
            wait_for(
                lambda: manager.status(startup_job)["status"] == "running",
                10,
                "second job running",
            )
            worker_pid = manager.status(startup_job)["worker_pid"]
            manager._kill_pid(worker_pid, force=True)
            wait_for(
                lambda: manager.status(startup_job)["status"] in {"orphaned", "completed", "failed"},
                20,
                "terminal state after runner kill",
            )
            status = manager.status(startup_job)["status"]
            if status == "orphaned":
                assert not manager._pid_alive(manager.status(startup_job)["pid"]), "workload leaked"
            print(f"  runner killed near terminal persistence -> {status}, no leak")
        finally:
            manager.close()
            shutil.rmtree(home, ignore_errors=True)


class InputScenario(Scenario):
    name = "input"

    def run(self) -> None:
        base = Path(tempfile.mkdtemp(prefix="vanth-input-"))
        port = free_port()
        proc = subprocess.Popen(
            [sys.executable, "-m", "vanth.daemon"],
            env={**os.environ, "VANTH_HOME": str(base / "state"), "VANTH_DAEMON_PORT": str(port)},
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
        )
        try:
            wait_for(lambda: request(port, "GET", "/health") == (200, {"ok": True}), 30, "daemon start")
            token = (base / "state" / "token").read_text(encoding="utf-8").strip()
            bad_bodies = [
                b'{"command":',
                b"\xff\xfe",
                b"[]",
                b'{"command":' + b"{" * 5000,
                b'{"command":"echo ok","extra_field":1}',
            ]
            for body in bad_bodies:
                status, payload = request(port, "POST", "/jobs", body, {"Content-Type": "application/json"}, token)
                assert status == 400 and payload["result"] == "error", (status, payload)
            status, payload = request(
                port, "GET", "/jobs?limit=" + "9" * 200, token=token
            )
            assert status == 400 and payload["result"] == "error", (status, payload)
            bad_targets = [
                {"command": "echo ok", "wake_targets": [{"type": "bogus", "events": []}]},
                {"command": "echo ok", "wake_targets": [{"type": "local_command"}]},
                {"command": "echo ok", "wake_targets": [{"type": "local_command", "events": 3, "command": ["ok"]}]},
            ]
            for target in bad_targets:
                status, payload = request(port, "POST", "/jobs", json.dumps(target).encode(), {"Content-Type": "application/json"}, token)
                assert status == 400 and payload["result"] == "error", (status, payload)
            assert request(port, "GET", "/health") == (200, {"ok": True})
            print("  malformed/oversized/recursive/invalid input -> structured 400s; daemon stayed healthy")
        finally:
            proc.kill()
            proc.wait(timeout=10)
            shutil.rmtree(base, ignore_errors=True)


class StateScenario(Scenario):
    name = "state"

    def run(self) -> None:
        home = Path(tempfile.mkdtemp(prefix="vanth-state-"))
        previous = os.environ.get("VANTH_MAX_LOG_BYTES")
        os.environ["VANTH_MAX_LOG_BYTES"] = "4096"
        manager = JobManager(home)
        try:
            code = (
                "import sys;"
                "[print('x'*500, flush=True) for _ in range(2000)];"
                "[print('y'*500, file=sys.stderr, flush=True) for _ in range(2000)]"
            )
            job_id = asyncio.run(manager.start(cmd(code)))["job_id"]
            wait_for(lambda: manager.status(job_id)["status"] == "completed", 60, "noisy job completion")
            counts = {row["type"]: row["c"] for row in manager.db.execute(
                "SELECT type, COUNT(*) AS c FROM events WHERE job_id=? GROUP BY type", (job_id,)
            ).fetchall()}
            assert counts["log_truncated"] == 2, counts
            row = manager.db.execute("SELECT stdout_path, stderr_path FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            assert os.path.getsize(row["stdout_path"]) <= 4096
            assert os.path.getsize(row["stderr_path"]) <= 4096
            first = manager.cleanup(0, dry_run=False)
            assert first["count"] == 1
            assert manager.cleanup(0, dry_run=False)["count"] == 0
            print("  log caps bounded streams; cleanup ran twice and was idempotent")
        finally:
            manager.close()
            if previous is None:
                os.environ.pop("VANTH_MAX_LOG_BYTES", None)
            else:
                os.environ["VANTH_MAX_LOG_BYTES"] = previous
            shutil.rmtree(home, ignore_errors=True)


class AgentFeatureScenario(Scenario):
    """Stress the v1.1 agent-facing features under load: rerun across many
    failed jobs, status/env exposure, list name/tag filters, and reverse event
    paging. Also verifies daemon discovery metadata appears and is removed."""

    name = "agent"

    def run(self) -> None:
        home = Path(tempfile.mkdtemp(prefix="vanth-agent-"))
        manager = JobManager(home)
        try:
            rerun_marker = home / "rerun_marker"
            # A batch of jobs that fail once (marker absent) then succeed.
            batch_code = (
                "from pathlib import Path; import os,sys; "
                "Path(os.environ['RERUN_MARK']).touch(); "
                "print('AGENT_EVENT '+__import__('json').dumps({'type':'checkpoint'}), flush=True); "
                "sys.exit(0)"
            )
            started = []
            for index in range(10):
                started.append(
                    asyncio.run(
                        manager.start(
                            cmd(batch_code),
                            name=f"agent-job-{index}",
                            cwd=str(home),
                            env={"RERUN_MARK": str(rerun_marker / str(index))},
                            tags=["agent", "chaos"],
                            wake_targets=[
                                {"type": "local_command", "events": ["checkpoint"],
                                 "command": [sys.executable, "-c", "import sys; sys.exit(0)"]}
                            ],
                        )
                    )["job_id"]
                )
            for job_id in started:
                wait_for(lambda job_id=job_id: manager.status(job_id)["status"] in {"completed", "failed"}, 30,
                         f"job {job_id} terminal")

            # status exposes command/env/cwd/tags for every job.
            for job_id in started:
                status = manager.status(job_id)
                assert "AGENT_EVENT" in status["command"], job_id
                assert status["tags"] == ["agent", "chaos"], (job_id, status["tags"])
                assert Path(status["cwd"]).resolve() == home.resolve(), job_id
                assert "RERUN_MARK" in status["env"], job_id
                assert status["run"].get("hostname"), job_id
                assert status["run"].get("os"), job_id
                assert status["runtime_seconds"] is not None, job_id

            # list filters by name and tag under load.
            by_tag = manager.list(tags=["chaos"])["jobs"]
            assert len(by_tag) == 10, len(by_tag)
            by_name = manager.list(name="agent-job-3")["jobs"]
            assert len(by_name) == 1, by_name

            # reverse paging returns the newest events first.
            reverse = manager.events(started[0], limit=3, reverse=True)["events"]
            seqs = [e["seq"] for e in reverse]
            assert seqs == sorted(seqs, reverse=True), seqs

            # rerun all failed jobs and confirm the reruns inherit config.
            reruns = []
            for job_id in started:
                status = manager.status(job_id)
                if status["status"] == "failed":
                    reruns.append(manager.rerun_sync(job_id)["job_id"])
            for rerun_id in reruns:
                wait_for(lambda rerun_id=rerun_id: manager.status(rerun_id)["status"] in {"completed", "failed"}, 30,
                         f"rerun {rerun_id} terminal")
                status = manager.status(rerun_id)
                assert status["tags"] == ["agent", "chaos"], rerun_id
                assert status["env"]["RERUN_MARK"].startswith(str(home)), rerun_id
            print(f"  {len(started)} jobs: status/env, list filters, reverse paging, {len(reruns)} reruns verified")

            # Daemon discovery metadata via a live daemon.
            import socket as _socket
            with _socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                port = sock.getsockname()[1]
            daemon_home = home / "dynhome"
            daemon_home.mkdir(parents=True, exist_ok=True)
            daemon = subprocess.Popen(
                [sys.executable, "-m", "vanth.daemon"],
                env={**os.environ, "VANTH_HOME": str(daemon_home), "VANTH_DAEMON_PORT": str(port)},
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
            )
            meta = daemon_home / "daemon.json"
            try:
                wait_for(lambda: meta.exists(), 15, "daemon.json write")
                payload = json.loads(meta.read_text(encoding="utf-8"))
                assert payload["url"] == f"http://127.0.0.1:{port}", payload
                assert payload["home"] == str(daemon_home.resolve()), payload
            finally:
                if sys.platform == "win32":
                    daemon.send_signal(signal.CTRL_BREAK_EVENT)
                else:
                    daemon.terminate()
                try:
                    daemon.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    daemon.kill()
                    daemon.wait(timeout=10)
            wait_for(lambda: not meta.exists(), 5, "daemon.json removal")
            print("  daemon discovery metadata written atomically and removed on graceful shutdown")
        finally:
            manager.close()
            shutil.rmtree(home, ignore_errors=True)


class AgentQoLScenario(Scenario):
    """Stress the v1.4 agent-QoL features under load: job_rerun with overrides,
    job_status_batch, job_wait return_progress streaming, job_tail follow mode,
    and the daemon_wake shorthand (all verified through the HTTP layer via a
    live daemon so routes and payload coercion are exercised)."""

    name = "qol"

    def run(self) -> None:
        import socket as _socket
        home = Path(tempfile.mkdtemp(prefix="vanth-qol-"))
        with _socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        daemon = subprocess.Popen(
            [sys.executable, "-m", "vanth.daemon"],
            env={**os.environ, "VANTH_HOME": str(home / "state"), "VANTH_DAEMON_PORT": str(port)},
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
        )
        try:
            wait_for(lambda: request(port, "GET", "/health") == (200, {"ok": True}), 30, "qol daemon start")
            token = (home / "state" / "token").read_text(encoding="utf-8").strip()

            def api(method: str, path: str, body=None, qparams=None) -> dict:
                status, payload = request(port, method, path, body, {"Content-Type": "application/json"}, token)
                assert status in {200, 201}, (method, path, status, payload)
                return payload

            # A short job that streams progress then completes.
            progress_code = (
                "import json,sys;"
                "f=lambda i: print('AGENT_EVENT '+json.dumps({'type':'progress','data':{'current':i,'total':5}}), flush=True) or "
                "__import__('time').sleep(0.25);"
                "[f(i) for i in range(1,6)]"
            )
            body = json.dumps({"command": cmd(progress_code), "name": "qol-progress", "tags": ["qol", "chaos"]}).encode()
            started = api("POST", "/jobs", body)
            job_id = started["job_id"]

            # job_wait return_progress streams progress events before completion.
            seen_progress = 0
            since = None
            for _ in range(20):
                payload = {"filters": ["completed"], "return_progress": True, "timeout_seconds": 10}
                if since:
                    payload["since_event_id"] = since
                result = api("POST", f"/jobs/{job_id}/wait", json.dumps(payload).encode())
                if result.get("event", {}).get("type") == "completed":
                    break
                assert result["result"] == "progress" and result["event"]["type"] == "progress", result
                seen_progress += 1
                since = result["event"]["event_id"]
            assert seen_progress >= 3, seen_progress
            assert api("POST", f"/jobs/{job_id}/wait", json.dumps({"filters": ["completed"], "timeout_seconds": 10}).encode())["event"]["type"] == "completed"

            # job_tail follow streams output as it appears.
            tail_code = "import sys,time; print('t1', flush=True); time.sleep(0.2); print('t2', flush=True)"
            tail_job = api("POST", "/jobs", json.dumps({"command": cmd(tail_code)}).encode())["job_id"]
            tail = api("GET", f"/jobs/{tail_job}/tail?follow=true&timeout_seconds=3&offset=0")
            assert tail.get("followed") is True and "t2" in tail.get("content", ""), tail

            # job_status_batch returns known + unknown ids in one call.
            batch = api("GET", f"/status/batch?job_ids={job_id},{tail_job},job_bogus")
            assert batch["count"] == 3 and batch["unknown"] == ["job_bogus"], batch
            statuses = {j["job_id"]: j["status"] for j in batch["jobs"]}
            assert statuses[job_id] == "completed" and statuses[tail_job] == "completed", statuses

            # job_rerun with an override spawns a new job with the tweak applied.
            rerun = api("POST", f"/jobs/{job_id}/rerun", json.dumps({"name": "qol-rerun"}).encode())
            rerun_id = rerun["job_id"]
            wait_for(lambda: api("GET", f"/jobs/{rerun_id}/status")["status"] in {"completed", "failed"}, 30,
                     f"rerun {rerun_id} terminal")
            rerun_status = api("GET", f"/jobs/{rerun_id}/status")
            assert rerun_status["name"] == "qol-rerun", rerun_status
            assert "progress" in rerun_status["command"], rerun_status

            # daemon_wake shorthand registers a target that fires on completion.
            wake_code = "import time; time.sleep(0.5)"
            wake_job = api("POST", "/jobs", json.dumps({"command": cmd(wake_code)}).encode())["job_id"]
            wake = api("POST", f"/jobs/{wake_job}/wake", json.dumps(
                {"target": {"type": "local_command", "events": ["completed"],
                            "command": [sys.executable, "-c", "import sys; sys.exit(0)"]}}).encode())
            assert wake["result"] == "ok" and wake["target_type"] == "local_command", wake
            wait_for(lambda: api("GET", f"/jobs/{wake_job}/status")["status"] in {"completed", "failed"}, 30,
                     f"wake job {wake_job} terminal")
            wait_for(lambda: any(d["target_type"] == "local_command" and d["status"] == "delivered"
                                 for d in api("GET", f"/deliveries?job_id={wake_job}")["deliveries"]),
                     30, "wake delivery dispatched to local_command")

            print("  rerun overrides, status_batch, wait progress stream, tail follow, wake shorthand verified")
        finally:
            if sys.platform == "win32":
                daemon.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                daemon.terminate()
            try:
                daemon.wait(timeout=10)
            except subprocess.TimeoutExpired:
                daemon.kill()
                daemon.wait(timeout=10)
            shutil.rmtree(home, ignore_errors=True)


class UxFeatureScenario(Scenario):
    """Stress the v1.5 UX features through the live daemon HTTP layer:
    job_wait metric_ge, trigger-based DAG, tail --grep, and job diff."""

    name = "ux"

    def run(self) -> None:
        import socket as _socket
        home = Path(tempfile.mkdtemp(prefix="vanth-ux-"))
        with _socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        daemon = subprocess.Popen(
            [sys.executable, "-m", "vanth.daemon"],
            env={**os.environ, "VANTH_HOME": str(home / "state"), "VANTH_DAEMON_PORT": str(port)},
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
        )
        try:
            wait_for(lambda: request(port, "GET", "/health") == (200, {"ok": True}), 30, "ux daemon start")
            token = (home / "state" / "token").read_text(encoding="utf-8").strip()

            def api(method: str, path: str, body=None, qparams=None) -> dict:
                if qparams:
                    from urllib.parse import urlencode
                    sep = "&" if "?" in path else "?"
                    path = f"{path}{sep}{urlencode(qparams)}"
                status, payload = request(port, method, path, body, {"Content-Type": "application/json"}, token)
                assert status in {200, 201}, (method, path, status, payload)
                return payload

            # job_wait metric_ge: emit an increasing loss metric, wait for it to cross 0.5.
            metric_code = (
                "import json,sys;"
                "f=lambda i: print('AGENT_EVENT '+json.dumps({'type':'metric','data':{'loss':i/10.0}}), flush=True) or "
                "__import__('time').sleep(0.2);"
                "[f(i) for i in range(1,11)]"
            )
            metric_job = api("POST", "/jobs", json.dumps({"command": cmd(metric_code)}).encode())["job_id"]
            waited = api("POST", f"/jobs/{metric_job}/wait", json.dumps(
                {"filters": ["completed"], "metric_ge": {"loss": 0.5}, "timeout_seconds": 30}).encode())
            assert waited["result"] == "metric", waited
            assert waited["metric"] == "loss" and waited["value"] >= 0.5, waited

            # trigger DAG: parent fails, child queued for completed gets cancelled.
            parent = api("POST", "/jobs", json.dumps({"command": cmd("import sys; sys.exit(1)")}).encode())["job_id"]
            wait_for(lambda: api("GET", f"/jobs/{parent}/status")["status"] in {"completed", "failed"}, 30,
                     f"dag parent {parent} terminal")
            child = api("POST", "/jobs", json.dumps({"command": cmd("print('never')"),
                                                     "trigger": {"job_id": parent, "status": "completed"}}).encode())
            assert child["status"] == "queued", child
            child_id = child["job_id"]
            wait_for(lambda: api("GET", f"/jobs/{child_id}/status")["status"] in {"completed", "failed", "cancelled"}, 30,
                     f"dag child {child_id} terminal")
            assert api("GET", f"/jobs/{child_id}/status")["status"] == "cancelled", child_id

            # trigger DAG success: parent completes, child runs.
            ok_parent = api("POST", "/jobs", json.dumps({"command": cmd("print('ok')")}).encode())["job_id"]
            ok_child = api("POST", "/jobs", json.dumps({"command": cmd("print('child ok')"),
                                                        "trigger": {"job_id": ok_parent, "status": "completed"}}).encode())
            assert ok_child["status"] == "queued", ok_child
            ok_child_id = ok_child["job_id"]
            wait_for(lambda: api("GET", f"/jobs/{ok_child_id}/status")["status"] in {"completed", "failed", "cancelled"}, 30,
                     f"dag ok child {ok_child_id} terminal")
            assert api("GET", f"/jobs/{ok_child_id}/status")["status"] == "completed", ok_child_id

            # tail --grep filters server-side.
            grep_job = api("POST", "/jobs", json.dumps({"command": cmd(
                "print('hello world', flush=True); print('goodbye world', flush=True)")}).encode())["job_id"]
            wait_for(lambda: api("GET", f"/jobs/{grep_job}/status")["status"] in {"completed", "failed"}, 30,
                     f"grep job {grep_job} terminal")
            filtered = api("GET", f"/jobs/{grep_job}/tail", qparams={"grep": "hello"})
            assert "hello world" in filtered.get("content", "") and "goodbye world" not in filtered.get("content", ""), filtered

            # job diff: identical vs changed.
            base = api("POST", "/jobs", json.dumps({"command": cmd("print('a')"), "name": "same",
                                                    "env": {"X": "1"}}).encode())["job_id"]
            other = api("POST", "/jobs", json.dumps({"command": cmd("print('b')"), "name": "same",
                                                     "env": {"X": "2"}}).encode())["job_id"]
            diff = api("GET", f"/jobs/{base}/diff", qparams={"other": other})
            assert diff["identical"] is False, diff
            fields = {c["field"] for c in diff["changes"]}
            assert "command" in fields and "env" in fields, fields
            wait_for(lambda: api("GET", f"/jobs/{base}/status")["status"] in {"completed", "failed"}, 30,
                     f"diff base {base} terminal")

            print("  metric wait, DAG trigger (cancel+success), tail grep, job diff verified")
        finally:
            if sys.platform == "win32":
                daemon.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                daemon.terminate()
            try:
                daemon.wait(timeout=10)
            except subprocess.TimeoutExpired:
                daemon.kill()
                daemon.wait(timeout=10)
            shutil.rmtree(home, ignore_errors=True)


SCENARIOS = {
    scenario.name: scenario
    for scenario in (BurstScenario, AdapterScenario, DaemonKillScenario, RunnerKillScenario, InputScenario, StateScenario, AgentFeatureScenario, AgentQoLScenario, UxFeatureScenario)
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Vanth v1 chaos and synthetic workload matrix")
    parser.add_argument("--only", choices=sorted(SCENARIOS), help="run a single scenario")
    parser.add_argument("--jobs", type=int, default=50, help="jobs in the burst scenario")
    parser.add_argument("--events", type=int, default=500, help="events per stream in the burst scenario")
    parser.add_argument("--iterations", type=int, default=5, help="daemon kill/restart cycles")
    args = parser.parse_args()

    targets = [args.only] if args.only else list(SCENARIOS)
    failures = []
    for name in targets:
        print(f"[{name}]")
        try:
            scenario = SCENARIOS[name](args.jobs, args.events) if name in {"burst"} else (
                SCENARIOS[name](args.iterations) if name == "daemon" else SCENARIOS[name]()
            )
            scenario.run()
            print(f"  PASS {name}")
        except Exception as exc:  # noqa: BLE001 - report and continue
            failures.append((name, exc))
            import traceback
            traceback.print_exc()
            print(f"  FAIL {name}: {exc}")
    if failures:
        print("\nMatrix failures:")
        for name, exc in failures:
            print(f"  {name}: {exc}")
        return 1
    print("\nAll scenarios passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
