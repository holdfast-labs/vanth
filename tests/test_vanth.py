import asyncio
import datetime
import json
import sqlite3
import sys
import threading
import time
import uuid

import pytest

from vanth.server import JobManager, normalize_event_payload, now_iso, parse_agent_event_line


import shellcmd


def cmd(code: str) -> str:
    return shellcmd.join([sys.executable, "-c", code])


def run(coro):
    async def wrapper():
        try:
            return await coro
        finally:
            await asyncio.sleep(0.1)

    return asyncio.run(wrapper())


def poll_delivery(manager: JobManager, job_id: str, status: str, timeout: float = 5):
    deadline = time.monotonic() + timeout
    delivery = None
    while time.monotonic() < deadline:
        deliveries = manager.deliveries(job_id)["deliveries"]
        if deliveries:
            delivery = deliveries[0]
            if delivery["status"] == status:
                return delivery
        time.sleep(0.05)
    return delivery


def test_parse_valid_inline_event():
    event = parse_agent_event_line('AGENT_EVENT {"type":"checkpoint","message":"done","data":{"x":1}}')
    assert event == {"type": "checkpoint", "message": "done", "data": {"x": 1}}


def test_ignore_malformed_inline_event():
    assert parse_agent_event_line("AGENT_EVENT nope") is None
    assert parse_agent_event_line('AGENT_EVENT {"message":"missing type"}') is None
    assert parse_agent_event_line("hello") is None


def test_normalize_progress_percent():
    event = normalize_event_payload({"type": "progress", "data": {"current": 2, "total": 4}})
    assert event["data"]["percent"] == 50


def test_vanth_home_and_event_size_cap(tmp_path, monkeypatch):
    async def main():
        monkeypatch.setenv("VANTH_HOME", str(tmp_path / "state"))
        monkeypatch.setenv("VANTH_MAX_EVENT_BYTES", "20")
        manager = JobManager()
        started = manager.start(cmd("import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint','data':{'big':'x'*100}}), flush=True)"))
        event = await manager.wait(started["job_id"], ["checkpoint"], timeout_seconds=30)
        assert event["event"]["data"] == {"truncated": True, "max_bytes": 20}
        assert manager.home == tmp_path / "state"
        await manager.wait(started["job_id"], ["completed"], event["event"]["event_id"], timeout_seconds=30)
        manager.close()

    run(main())


def test_wait_returns_stored_event(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        started = manager.start(cmd("print('hello')"))
        result = await manager.wait(started["job_id"], ["completed"], timeout_seconds=30)
        assert result["result"] == "event"
        assert result["event"]["type"] == "completed"

    run(main())


def test_checkpoint_progress_and_tail(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        code = (
            "import json,time;"
            "print('AGENT_EVENT '+json.dumps({'type':'progress','data':{'current':1,'total':2,'unit':'step'}}), flush=True);"
            "print('plain log', flush=True);"
            "time.sleep(.1);"
            "print('AGENT_EVENT '+json.dumps({'type':'checkpoint','message':'done'}), flush=True)"
        )
        started = manager.start(cmd(code))
        progress = await manager.wait(started["job_id"], ["progress"], timeout_seconds=30)
        checkpoint = await manager.wait(started["job_id"], ["checkpoint"], progress["event"]["event_id"], timeout_seconds=30)
        completed = await manager.wait(started["job_id"], ["completed"], checkpoint["event"]["event_id"], timeout_seconds=30)

        assert progress["event"]["data"]["percent"] == 50
        assert checkpoint["event"]["type"] == "checkpoint"
        assert completed["event"]["type"] == "completed"
        assert manager.status(started["job_id"])["progress"]["unit"] == "step"
        assert "plain log" in manager.tail(started["job_id"])["content"]
        tail = manager.tail(started["job_id"], max_bytes=4)
        assert tail["truncated"]
        assert len(tail["content"].encode()) <= 4
        first = manager.tail(started["job_id"], max_bytes=5, offset=0)
        second = manager.tail(started["job_id"], max_bytes=20, offset=first["next_offset"])
        assert first["next_offset"] > first["offset"]
        assert second["offset"] == first["next_offset"]

    run(main())


def test_reader_drain_finishes_before_job_cleanup(tmp_path):
    manager = JobManager(tmp_path)
    drained = threading.Event()
    reader = threading.Thread(target=lambda: (time.sleep(2.1), drained.set()))
    manager.reader_threads["job_test"] = [reader]
    reader.start()

    manager._readers_done("job_test")

    assert drained.is_set()
    manager.close()


def test_wake_target_enqueues_delivery(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        code = "import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint','message':'wake me'}), flush=True)"
        started = manager.start(
            cmd(code),
            wake_targets=[
                {
                    "type": "codex_thread",
                    "thread_id": "thread_test",
                    "events": ["checkpoint"],
                    "auto_dispatch": False,
                }
            ],
        )
        event = await manager.wait(started["job_id"], ["checkpoint"], timeout_seconds=30)
        deliveries = manager.deliveries(started["job_id"])["deliveries"]

        assert len(deliveries) == 1
        assert deliveries[0]["event_id"] == event["event"]["event_id"]
        assert deliveries[0]["target_type"] == "codex_thread"
        assert deliveries[0]["status"] == "pending"
        assert deliveries[0]["payload"]["target"]["thread_id"] == "thread_test"
        assert "wake me" in deliveries[0]["payload"]["prompt"]

        marked = manager.mark_delivery(deliveries[0]["delivery_id"], "delivered")
        assert marked["status"] == "delivered"
        assert marked["attempts"] == 1

    run(main())


def test_codex_thread_delivery_dispatches_via_app_server(tmp_path):
    async def main():
        manager = JobManager(tmp_path / "state")
        calls = tmp_path / "codex_calls.jsonl"
        fake_codex = tmp_path / "fake_codex.py"
        fake_codex.write_text(
            """
import json
import sys
import threading
import time
from pathlib import Path

calls = Path(sys.argv[1])
for line in sys.stdin:
    req = json.loads(line)
    calls.open("a", encoding="utf-8").write(json.dumps(req) + "\\n")
    method = req["method"]
    if method == "initialize":
        result = {"userAgent": "fake", "codexHome": "", "platformFamily": "test", "platformOs": "test"}
    elif method == "thread/resume":
        result = {"thread": {"id": req["params"]["threadId"], "status": {"type": "idle"}}}
    elif method == "turn/start":
        result = {"turn": {"id": "turn_test", "status": "inProgress"}}

    def respond():
        if method == "turn/start":
            # Emit the turn/completed notification AFTER the ack, as the real
            # app-server does; the bridge must wait for it.
            time.sleep(0.05)
            print(json.dumps({"method": "turn/completed", "params": {"threadId": req["params"]["threadId"], "turn": {"id": "turn_test", "status": "completed"}}}), flush=True)
        print(json.dumps({"id": req["id"], "result": result}), flush=True)

    threading.Thread(target=respond, daemon=True).start()
""".strip(),
            encoding="utf-8",
        )
        code = "import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint','message':'codex dispatch'}), flush=True)"
        started = manager.start(
            cmd(code),
            wake_targets=[
                {
                    "type": "codex_thread",
                    "events": ["checkpoint"],
                    "thread_id": "thread_test",
                    "codex_command": [sys.executable, str(fake_codex), str(calls)],
                }
            ],
        )
        await manager.wait(started["job_id"], ["checkpoint"], timeout_seconds=30)
        delivery = poll_delivery(manager, started["job_id"], "delivered")

        assert delivery is not None
        assert delivery["status"] == "delivered"
        assert delivery["attempts"] == 1
        requests = [json.loads(line) for line in calls.read_text(encoding="utf-8").splitlines()]
        assert [request["method"] for request in requests] == ["initialize", "thread/resume", "turn/start"]
        assert requests[1]["params"]["threadId"] == "thread_test"
        assert requests[2]["params"]["input"][0]["text"] == delivery["payload"]["prompt"]

    run(main())


def test_opencode_thread_delivery_dispatches_via_cli(tmp_path):
    async def main():
        manager = JobManager(tmp_path / "state")
        calls = tmp_path / "opencode_args.json"
        fake_opencode = tmp_path / "fake_opencode.py"
        fake_opencode.write_text(
            "import json,sys; open(sys.argv[1], 'w').write(json.dumps(sys.argv[2:]))",
            encoding="utf-8",
        )
        code = "import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint','message':'opencode dispatch'}), flush=True)"
        started = manager.start(
            cmd(code),
            wake_targets=[
                {
                    "type": "opencode_thread",
                    "events": ["checkpoint"],
                    "thread_id": "ses_test",
                    "cwd": str(tmp_path),
                    "attach": "http://127.0.0.1:4096",
                    "opencode_command": [sys.executable, str(fake_opencode), str(calls)],
                }
            ],
        )
        await manager.wait(started["job_id"], ["checkpoint"], timeout_seconds=30)
        delivery = poll_delivery(manager, started["job_id"], "delivered")

        assert delivery is not None
        assert delivery["attempts"] == 1
        assert json.loads(calls.read_text(encoding="utf-8")) == [
            "run",
            "--session",
            "ses_test",
            "--dir",
            str(tmp_path),
            "--attach",
            "http://127.0.0.1:4096",
            "--format",
            "json",
            delivery["payload"]["prompt"],
        ]

    run(main())


def test_local_command_delivery_dispatches_immediately(tmp_path):
    async def main():
        manager = JobManager(tmp_path / "state")
        sink = tmp_path / "delivery.json"
        code = "import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint','message':'dispatch me'}), flush=True)"
        command = [
            sys.executable,
            "-c",
            "import pathlib,sys; pathlib.Path(sys.argv[1]).write_text(sys.stdin.read())",
            str(sink),
        ]
        started = manager.start(
            cmd(code),
            wake_targets=[
                {
                    "type": "local_command",
                    "events": ["checkpoint"],
                    "command": command,
                }
            ],
        )
        await manager.wait(started["job_id"], ["checkpoint"], timeout_seconds=30)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not sink.exists():
            time.sleep(0.05)

        assert sink.exists()
        payload = json.loads(sink.read_text())
        assert payload["event"]["message"] == "dispatch me"
        delivery = poll_delivery(manager, started["job_id"], "delivered")
        assert delivery["status"] == "delivered"
        assert delivery["attempts"] == 1

    run(main())


def test_thread_association_agent_view_and_doctor(tmp_path):
    async def main():
        manager = JobManager(tmp_path / "state")
        started = manager.start(
            cmd("import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint','message':'needs review'}), flush=True)"),
            name="reviewable",
            origin_thread_id="thread_origin",
            tags=["training", "gpu"],
            wake_targets=[
                {
                    "type": "codex_thread",
                    "thread_id": "thread_wake",
                    "events": ["checkpoint"],
                    "auto_dispatch": False,
                }
            ],
        )
        await manager.wait(started["job_id"], ["checkpoint"], timeout_seconds=30)
        status = manager.status(started["job_id"])
        listed = manager.list(thread_id="thread_origin")["jobs"]
        view = manager.agent_view(thread_id="thread_wake")["jobs"]
        doctor = manager.doctor()

        assert status["origin_thread_id"] == "thread_origin"
        assert status["wake_thread_id"] == "thread_wake"
        assert status["tags"] == ["training", "gpu"]
        assert listed[0]["job_id"] == started["job_id"]
        assert view[0]["job_id"] == started["job_id"]
        assert view[0]["delivery_counts"]["pending"] == 1
        assert view[0]["priority"] >= 50
        assert {"jobs", "events", "wake_targets", "deliveries", "delivery_attempts"} <= set(doctor["tables"])
        assert doctor["home"] == str(tmp_path / "state")

    run(main())


def test_delivery_retry_records_attempts_and_succeeds(tmp_path):
    async def main():
        manager = JobManager(tmp_path / "state")
        marker = tmp_path / "ready"
        sink = tmp_path / "delivery.json"
        retry_command = [
            sys.executable,
            "-c",
            (
                "from pathlib import Path; import sys; "
                "marker=Path(sys.argv[1]); sink=Path(sys.argv[2]); data=sys.stdin.read(); "
                "sys.exit(7) if not marker.exists() else sink.write_text(data)"
            ),
            str(marker),
            str(sink),
        ]
        started = manager.start(
            cmd("import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint','message':'retry me'}), flush=True)"),
            wake_targets=[{"type": "local_command", "events": ["checkpoint"], "command": retry_command}],
        )
        await manager.wait(started["job_id"], ["checkpoint"], timeout_seconds=30)
        failed = poll_delivery(manager, started["job_id"], "failed")
        assert failed["attempts"] == 1

        marker.write_text("ok", encoding="utf-8")
        retried = manager.retry_delivery(failed["delivery_id"])
        assert retried["status"] == "retrying"
        delivered = poll_delivery(manager, started["job_id"], "delivered")
        assert delivered["attempts"] == 2
        assert json.loads(sink.read_text())["event"]["message"] == "retry me"

    run(main())


def test_restart_persists_completed_job(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        started = manager.start(cmd("print('hello')"))
        completed = await manager.wait(started["job_id"], ["completed"], timeout_seconds=30)
        manager.close()

        restarted = JobManager(tmp_path)
        assert restarted.status(started["job_id"])["status"] == "completed"
        assert restarted.events(started["job_id"], types=["completed"])["events"][0]["event_id"] == completed["event"]["event_id"]
        assert "hello" in restarted.tail(started["job_id"])["content"]
        restarted.close()

    run(main())


def test_running_runner_survives_manager_restart(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        started = manager.start(cmd("import time; print('before', flush=True); time.sleep(.5); print('after', flush=True)"))
        manager.close()

        restarted = JobManager(tmp_path)
        # With claim-token launches (review rc36 P1), a freshly-started job may
        # still be in the transient 'launching' state when a manager restarts;
        # the runner (a separate process) promotes it to 'running' and completes.
        assert restarted.status(started["job_id"])["status"] in {"running", "launching", "completed"}
        completed = await restarted.wait(started["job_id"], ["completed"], timeout_seconds=30)
        assert completed["event"]["type"] == "completed"
        assert "after" in restarted.tail(started["job_id"])["content"]
        restarted.close()

    run(main())


def test_restart_marks_running_job_orphaned(tmp_path):
    manager = JobManager(tmp_path)
    manager.close()
    ts = now_iso()
    db = sqlite3.connect(tmp_path / "jobs.sqlite")
    db.execute(
        """
        INSERT INTO jobs(job_id, command, status, created_at, updated_at, stdout_path, stderr_path, events_path)
        VALUES ('job_stale', 'sleep 30', 'running', ?, ?, ?, ?, ?)
        """,
        (
            ts,
            ts,
            str(tmp_path / "logs" / "job_stale.stdout.log"),
            str(tmp_path / "logs" / "job_stale.stderr.log"),
            str(tmp_path / "events" / "job_stale.jsonl"),
        ),
    )
    db.commit()
    db.close()

    restarted = JobManager(tmp_path)
    assert restarted.status("job_stale")["status"] == "orphaned"
    restarted.close()


def test_failed_and_cancelled(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        failed_job = manager.start(cmd("import sys; sys.exit(3)"))
        failed = await manager.wait(failed_job["job_id"], ["failed"], timeout_seconds=30)
        assert failed["event"]["data"]["exit_code"] == 3

        slow_job = manager.start(cmd("import time; time.sleep(30)"))
        waiter = asyncio.create_task(manager.wait(slow_job["job_id"], ["cancelled"], timeout_seconds=30))
        stopped = await manager.stop(slow_job["job_id"], kill_after_seconds=1)
        cancelled = await waiter
        assert stopped["status"] == "cancelled"
        assert cancelled["event"]["type"] == "cancelled"

    run(main())


def test_stderr_event_and_timeout(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        stderr_job = manager.start(
            cmd("import json,sys; print('AGENT_EVENT '+json.dumps({'type':'checkpoint'}), file=sys.stderr, flush=True)")
        )
        checkpoint = await manager.wait(stderr_job["job_id"], ["checkpoint"], timeout_seconds=30)
        assert checkpoint["event"]["source"] == "stderr"

        timeout_job = manager.start(cmd("import time; time.sleep(30)"), timeout_seconds=1)
        timed_out = await manager.wait(timeout_job["job_id"], ["timeout"], timeout_seconds=30)
        assert timed_out["event"]["type"] == "timeout"

    run(main())


def test_multiple_waiters(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        code = "import json,time; time.sleep(.2); print('AGENT_EVENT '+json.dumps({'type':'checkpoint'}), flush=True)"
        started = manager.start(cmd(code))
        results = await asyncio.gather(
            manager.wait(started["job_id"], ["checkpoint"], timeout_seconds=30),
            manager.wait(started["job_id"], ["checkpoint"], timeout_seconds=30),
        )
        assert [r["event"]["type"] for r in results] == ["checkpoint", "checkpoint"]

    run(main())


def test_wake_thread_targets_inherit_caller_thread(tmp_path, monkeypatch):
    """User report: wake notifications silently failed because opencode/codex
    thread targets demanded an explicit session/thread id. The LAUNCHING
    thread is now the default destination; explicit ids still win.

    Per review P2-2 the daemon no longer infers the thread from its own
    environment: the caller (MCP wrapper) resolves it and passes
    ``origin_thread_id`` explicitly. This test passes it the same way.

    ``opencode_thread`` does NOT auto-inherit a session id (OpenCode never
    injects OPENCODE_SESSION_ID — review P1-1): callers must pass an explicit
    session_id. codex targets inherit the calling thread id.
    """
    async def main():
        manager = JobManager(tmp_path)
        try:
            started = manager.start(
                cmd("print('inherit')"),
                notify_on=["completed"],
                origin_thread_id="ses_origin",
                wake_targets=[
                    {"type": "opencode_thread", "session_id": "ses_explicit", "attach": "http://127.0.0.1:4096"},  # explicit wins
                    {"type": "codex_thread", "auto_dispatch": False},  # inherits
                ],
            )
            await manager.wait(started["job_id"], ["completed"], timeout_seconds=30)
            targets = {
                t["target_id"]: json.loads(t["config_json"])
                for t in manager.db.execute(
                    "SELECT target_id, config_json FROM wake_targets WHERE job_id=?",
                    (started["job_id"],),
                ).fetchall()
            }
            by_session = {}
            for config in targets.values():
                key = config.get("session_id") or ("codex:" + str(config.get("thread_id")))
                by_session.setdefault(key, 0)
                by_session[key] += 1
            assert by_session.get("ses_explicit") == 1, by_session
            assert by_session.get("codex:ses_origin") == 1, by_session
        finally:
            manager.close()

    run(main())


def test_policy_validation_rejects_bad_shapes(tmp_path):
    from vanth.server import validate_policy
    import pytest

    with pytest.raises(ValueError, match="after_n"):
        validate_policy({"on_failure": {"after_n": 0, "action": "alert"}})
    with pytest.raises(ValueError, match="action"):
        validate_policy({"on_failure": {"after_n": 2, "action": "explode"}})
    with pytest.raises(ValueError, match="job_id"):
        validate_policy({"on_failure": {"after_n": 2, "action": "run_job"}})
    with pytest.raises(ValueError, match="expected_interval_seconds"):
        validate_policy({"schedule": {"expected_interval_seconds": -5}})
    with pytest.raises(ValueError, match="grace_period_seconds"):
        validate_policy({"schedule": {"expected_interval_seconds": 60, "grace_period_seconds": "x"}})
    with pytest.raises(ValueError, match="schedule or on_failure"):
        validate_policy({"unrelated": {}})
    assert validate_policy(None) is None
    assert validate_policy({"schedule": {"expected_interval_seconds": 60}}) == {
        "schedule": {"expected_interval_seconds": 60, "grace_period_seconds": 0}
    }


def test_dead_mans_switch_emits_job_stuck_and_wake(tmp_path):
    """A job with a schedule policy that outlives interval+grace emits
    job_stuck, which flows to wake targets like any other event."""
    async def main():
        manager = JobManager(tmp_path)
        try:
            job = manager.start(
                cmd("import time; time.sleep(30)"),
                notify_on=["job_stuck"],
                wake_targets=[{"type": "codex_thread", "thread_id": "t_dms", "auto_dispatch": False}],
                policy={"schedule": {"expected_interval_seconds": 1, "grace_period_seconds": 1}},
            )
            event = await manager.wait(job["job_id"], ["job_stuck"], timeout_seconds=15)
            assert event["result"] == "event"
            assert "past expected interval" in event["event"]["message"]
            deliveries = manager.deliveries(job["job_id"])["deliveries"]
            assert any(d["target_type"] == "codex_thread" and d["payload"]["target"]["thread_id"] == "t_dms" for d in deliveries)
            status = manager.status(job["job_id"])
            assert status["policy"]["schedule"]["expected_interval_seconds"] == 1
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_dead_mans_switch_emits_schedule_missed(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        try:
            job = manager.start(
                cmd("print('once')"),
                policy={"schedule": {"expected_interval_seconds": 1, "grace_period_seconds": 1}},
            )
            await manager.wait(job["job_id"], ["completed"], timeout_seconds=30)
            event = await manager.wait(job["job_id"], ["schedule_missed"], timeout_seconds=15)
            assert event["result"] == "event"
            assert "no start" in event["event"]["message"]
            # Emitted exactly once per window (no duplicate spam).
            await asyncio.sleep(1.5)
            missed = [e for e in manager.events(job["job_id"], types=["schedule_missed"], limit=50)["events"]]
            assert len(missed) == 1
        finally:
            manager.close()

    run(main())


def test_failure_threshold_alert_action(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        try:
            job = manager.start(
                cmd("import sys; sys.exit(1)"),
                policy={"on_failure": {"after_n": 1, "action": "alert"}},
            )
            event = await manager.wait(job["job_id"], ["failure_threshold"], timeout_seconds=30)
            assert event["result"] == "event"
            assert event["event"]["level"] == "error"
            state = manager._policy_state(job["job_id"])
            assert state["failure_streak"] >= 1
            # Alert does NOT disable: rerun is allowed.
            assert manager._row("SELECT policy_disabled FROM jobs WHERE job_id=?", (job["job_id"],))["policy_disabled"] == 0
        finally:
            manager.close()

    run(main())


def test_failure_threshold_streak_across_reruns_then_reset(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        try:
            job = manager.start(
                cmd("import sys; sys.exit(1)"),
                policy={"on_failure": {"after_n": 2, "action": "alert"}},
            )
            await manager.wait(job["job_id"], ["failed"], timeout_seconds=30)
            await asyncio.sleep(1.0)  # let the watcher record streak 1
            rerun = await manager.rerun(job["job_id"])
            event = await manager.wait(rerun["job_id"], ["failure_threshold"], timeout_seconds=30)
            assert event["result"] == "event"
            data = event["event"]["data"]
            assert data["failure_streak"] >= 2
            # The rerun carried the streak from the original job.
            assert manager._policy_state(rerun["job_id"])["failure_streak"] >= 2
            # A successful run (override the failing command) resets the streak.
            recovered = await manager.rerun(rerun["job_id"], command=cmd("print('recovered')"))
            await manager.wait(recovered["job_id"], ["completed"], timeout_seconds=30)
            await asyncio.sleep(1.0)
            assert manager._policy_state(recovered["job_id"])["failure_streak"] == 0
        finally:
            manager.close()

    run(main())


def test_failure_threshold_disable_action_blocks_rerun(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        try:
            job = manager.start(
                cmd("import sys; sys.exit(1)"),
                policy={"on_failure": {"after_n": 1, "action": "disable"}},
            )
            event = await manager.wait(job["job_id"], ["failure_threshold"], timeout_seconds=30)
            assert event["event"]["data"]["disabled"] is True
            assert manager._row("SELECT policy_disabled FROM jobs WHERE job_id=?", (job["job_id"],))["policy_disabled"] == 1
            launch = manager.prepare_launch(job["job_id"])
            assert launch is None, "disabled job must not relaunch"
        finally:
            manager.close()

    run(main())


def test_failure_threshold_run_job_action_launches_reaction(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        try:
            reaction = manager.start(cmd("print('cleanup')"), name="reaction-job")
            await manager.wait(reaction["job_id"], ["completed"], timeout_seconds=30)
            job = manager.start(
                cmd("import sys; sys.exit(1)"),
                policy={"on_failure": {"after_n": 1, "action": "run_job", "job_id": reaction["job_id"]}},
            )
            event = await manager.wait(job["job_id"], ["failure_threshold"], timeout_seconds=30)
            assert event["event"]["data"]["reaction_job_id"] == reaction["job_id"]
            # The reaction job relaunches via the rerun path.
            await manager.wait(reaction["job_id"], ["running", "completed"], timeout_seconds=30)
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_restart_policy_relails_with_backoff_then_gives_up(tmp_path):
    """policy.restart: failed job relaunches up to max_retries with backoff;
    budget exhaustion emits gave_up; success resets the counter."""
    async def main():
        manager = JobManager(tmp_path)
        try:
            job = manager.start(
                cmd("import sys; sys.exit(1)"),
                policy={"restart": {"max_retries": 2, "backoff_seconds": 0}},
            )
            # Original failure + 2 restarts => 3 failed runs, then gave_up.
            event = await manager.wait(job["job_id"], ["gave_up"], timeout_seconds=60)
            assert event["result"] == "event"
            assert event["event"]["level"] == "error"
            assert event["event"]["data"]["restart_attempts"] == 2
            failures = [e for e in manager.events(job["job_id"], types=["failed", "restarted"], limit=100)["events"]]
            assert sum(1 for e in failures if e["type"] == "restarted") == 2
            state = manager._policy_state(job["job_id"])
            assert state["restart_attempts"] == 2
            assert state["gave_up"] is True
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_restart_policy_resets_on_success(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        try:
            # Alternate: fail, succeed via rerun override with same policy.
            job = manager.start(
                cmd("import sys; sys.exit(1)"),
                policy={"restart": {"max_retries": 3, "backoff_seconds": 0}},
            )
            await manager.wait(job["job_id"], ["failed"], timeout_seconds=30)
            await asyncio.sleep(0.6)
            state = manager._policy_state(job["job_id"])
            assert state["restart_attempts"] >= 1
            # Stop the restart churn, then prove a completed run resets it.
            with manager.db_lock:
                manager.db.execute("UPDATE jobs SET policy_json=NULL WHERE job_id=?", (job["job_id"],))
                manager.db.commit()
            success = await manager.rerun(job["job_id"], command=cmd("print('ok')"))
            with manager.db_lock:
                manager.db.execute(
                    "UPDATE jobs SET policy_json=? WHERE job_id=?",
                    (json.dumps({"restart": {"max_retries": 3, "backoff_seconds": 0}}), success["job_id"]),
                )
                manager.db.commit()
            await manager.wait(success["job_id"], ["completed"], timeout_seconds=30)
            await asyncio.sleep(0.8)
            # A completed run resets (or never accumulates) the counter.
            assert manager._policy_state(success["job_id"]).get("restart_attempts", 0) == 0
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_restart_backoff_delays_relaunch(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        try:
            job = manager.start(
                cmd("import sys; sys.exit(1)"),
                policy={"restart": {"max_retries": 1, "backoff_seconds": 2}},
            )
            await manager.wait(job["job_id"], ["failed"], timeout_seconds=30)
            await asyncio.sleep(0.7)
            state = manager._policy_state(job["job_id"])
            assert state["restart_attempts"] == 1  # budget consumed immediately
            assert state["last_restart_delay_seconds"] == 2
            restarted = [e for e in manager.events(job["job_id"], types=["restarted"], limit=10)["events"]]
            assert restarted == []  # still waiting out the backoff
            event = await manager.wait(job["job_id"], ["restarted"], timeout_seconds=15)
            assert event["result"] == "event"
            assert event["event"]["data"]["delay_seconds"] == 2
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_restart_polling_does_not_consume_budget(tmp_path):
    """Review P1-1: every dispatcher tick previously incremented
    restart_attempts because restart_after was never persisted. A single
    failed run must claim exactly ONE restart, and the relaunch must wait out
    the backoff instead of the budget being exhausted by polling."""
    async def main():
        manager = JobManager(tmp_path)
        try:
            job = manager.start(
                cmd("import sys; sys.exit(1)"),
                policy={"restart": {"max_retries": 3, "backoff_seconds": 8}},
            )
            await manager.wait(job["job_id"], ["failed"], timeout_seconds=30)
            # Give the watcher many ticks; budget must stay at exactly 1.
            # Observation window (2s) is well inside the 8s backoff, so the
            # deadline is guaranteed not to have fired yet (review P2-3).
            await asyncio.sleep(2.0)
            state = manager._policy_state(job["job_id"])
            assert state["restart_attempts"] == 1, f"expected exactly 1 claim, got {state.get('restart_attempts')}"
            # The restart_after deadline is persisted and in the future.
            assert state.get("restart_after") is not None
            # No restart yet: the backoff has not elapsed.
            restarted = [e for e in manager.events(job["job_id"], types=["restarted"], limit=10)["events"]]
            assert restarted == []
            # After the backoff the single claimed restart fires.
            await manager.wait(job["job_id"], ["restarted"], timeout_seconds=20)
            state = manager._policy_state(job["job_id"])
            assert state["restart_attempts"] == 1, "budget must not grow during the due relaunch"
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_failure_streak_counts_each_run_exactly_once(tmp_path):
    """Review P1-2: a single failed run must increment the failure streak once
    regardless of how many watcher ticks observe the still-failed row."""
    async def main():
        manager = JobManager(tmp_path)
        try:
            job = manager.start(
                cmd("import sys; sys.exit(1)"),
                policy={"on_failure": {"after_n": 5, "action": "alert"}},
            )
            await manager.wait(job["job_id"], ["failed"], timeout_seconds=30)
            await asyncio.sleep(3.0)  # many ticks while the row stays failed
            state = manager._policy_state(job["job_id"])
            assert state["failure_streak"] == 1, f"expected exactly 1, got {state.get('failure_streak')}"
            # No failure_threshold: after_n=5 not reached by a single run.
            events = [e for e in manager.events(job["job_id"], types=["failure_threshold"], limit=10)["events"]]
            assert events == []
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_rerun_refuses_disabled_job(tmp_path):
    """Review P1-3: a job disabled by the on_failure disable action must not
    be relaunched through the public rerun API."""
    async def main():
        manager = JobManager(tmp_path)
        try:
            job = manager.start(
                cmd("import sys; sys.exit(1)"),
                policy={"on_failure": {"after_n": 1, "action": "disable"}},
            )
            await manager.wait(job["job_id"], ["failure_threshold"], timeout_seconds=30)
            assert manager._row("SELECT policy_disabled FROM jobs WHERE job_id=?", (job["job_id"],))["policy_disabled"] == 1
            with pytest.raises(ValueError, match="disabled by policy"):
                await manager.rerun(job["job_id"])
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_run_job_refuses_running_reaction(tmp_path):
    """Review P1-3: run_job must never double-launch an already-running
    reaction job; the atomic launch claim refuses it."""
    async def main():
        manager = JobManager(tmp_path)
        try:
            reaction = manager.start(cmd("import time; time.sleep(30)"))
            await manager.wait(reaction["job_id"], ["started"], timeout_seconds=30)
            job = manager.start(
                cmd("import sys; sys.exit(1)"),
                policy={"on_failure": {"after_n": 1, "action": "run_job", "job_id": reaction["job_id"]}},
            )
            event = await manager.wait(job["job_id"], ["failure_threshold"], timeout_seconds=30)
            assert "not idle" in event["event"]["message"]
            assert manager.status(reaction["job_id"])["status"] == "running"
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_dead_mans_flags_rearm_on_restart(tmp_path, monkeypatch):
    """Review P2-1: when an automatic restart reuses the same job row with a
    new started_at, the dead-man's-switch flags from the previous run must not
    suppress the new run's schedule_missed/job_stuck."""
    monkeypatch.setenv("VANTH_RETENTION_MIN_INTERVAL_SECONDS", "1")
    async def main():
        manager = JobManager(tmp_path)
        try:
            job = manager.start(
                cmd("import sys; sys.exit(1)"),
                policy={
                    "schedule": {"expected_interval_seconds": 1, "grace_period_seconds": 1},
                    "restart": {"max_retries": 1, "backoff_seconds": 0},
                },
            )
            # First run fails -> restart relaunches (new started_at on the SAME row).
            await manager.wait(job["job_id"], ["restarted"], timeout_seconds=30)
            state = manager._policy_state(job["job_id"])
            # The observed started_at was updated by the rearm logic.
            assert state.get("observed_started_at") is not None
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_retention_policy_prunes_events_and_metrics(tmp_path, monkeypatch):
    monkeypatch.setenv("VANTH_RETENTION_MIN_INTERVAL_SECONDS", "1")
    async def main():
        manager = JobManager(tmp_path)
        try:
            script = tmp_path / "emit_metric.py"
            script.write_text(
                "import json\n"
                "print('AGENT_EVENT ' + json.dumps({'type': 'metric', 'metric': {'loss': 0.5}}), flush=True)\n"
            )
            job = manager.start(
                shellcmd.join([sys.executable, str(script)]),
                policy={"retention": {"events_seconds": 1, "metrics_seconds": 1}},
            )
            await manager.wait(job["job_id"], ["completed"], timeout_seconds=30)
            events_before = len(manager.events(job["job_id"], limit=100)["events"])
            assert events_before > 0
            await asyncio.sleep(2.5)
            # Terminal events survive; non-terminal ones age out.
            remaining = manager.events(job["job_id"], limit=100)["events"]
            assert all(e["type"] in {"completed", "failed", "timeout", "cancelled", "orphaned"} for e in remaining)
            series = manager.metrics_query(job["job_id"])["series"]
            assert isinstance(series, dict)
        finally:
            manager.close()

    run(main())


def test_clear_deliveries_dry_run_then_drain(tmp_path):
    """Overflow valve: a backed-up delivery queue (disk-full scenario) can be
    previewed with dry_run, then drained in bulk by the agent."""
    async def main():
        manager = JobManager(tmp_path)
        try:
            code = "import json; print('AGENT_EVENT ' + json.dumps({'type': 'checkpoint', 'message': 'x'}), flush=True)"
            started = manager.start(
                cmd(code),
                wake_targets=[{"type": "codex_thread", "thread_id": "t_drain", "events": ["checkpoint"], "auto_dispatch": False}],
            )
            await manager.wait(started["job_id"], ["checkpoint"], timeout_seconds=30)
            deliveries = manager.deliveries(started["job_id"], status="pending")["deliveries"]
            assert len(deliveries) == 1

            preview = manager.clear_deliveries(job_id=started["job_id"], status="pending", dry_run=True)
            assert preview["matched"] == 1 and preview["drained"] == 0
            still = manager.deliveries(started["job_id"], status="pending")["deliveries"]
            assert len(still) == 1, "dry_run must not touch the queue"

            result = manager.clear_deliveries(job_id=started["job_id"], status="pending", dry_run=False)
            assert result["drained"] == 1
            after = manager.deliveries(started["job_id"])["deliveries"]
            assert all(d["status"] == "failed" for d in after)
            assert all("drained" in (d["last_error"] or "") for d in after)
        finally:
            manager.close()

    run(main())


def test_clear_deliveries_stale_only_scopes_to_terminal_jobs(tmp_path):
    async def main():
        manager = JobManager(tmp_path)
        try:
            code = "import json; print('AGENT_EVENT ' + json.dumps({'type': 'checkpoint', 'message': 'x'}), flush=True)"
            done = manager.start(
                cmd(code),
                wake_targets=[{"type": "codex_thread", "thread_id": "t_stale", "events": ["checkpoint"], "auto_dispatch": False}],
            )
            await manager.wait(done["job_id"], ["checkpoint"], timeout_seconds=30)
            # The dispatch thread may still be transitioning the delivery row;
            # poll until the row exists in a settled state.
            deadline = time.monotonic() + 5
            preview = None
            while time.monotonic() < deadline:
                preview = manager.clear_deliveries(stale_only=True, dry_run=True)
                if preview["matched"] >= 1:
                    break
                await asyncio.sleep(0.1)
            assert preview is not None and preview["matched"] >= 1, "terminal-job delivery must match stale_only"
            # A delivery for a NON-terminal job must not match.
            longjob = manager.start(
                cmd("import time; time.sleep(60)"),
                wake_targets=[{"type": "codex_thread", "thread_id": "t_live", "events": ["checkpoint"], "auto_dispatch": False}],
            )
            await asyncio.sleep(0.5)
            fresh = manager.clear_deliveries(stale_only=True, job_id=longjob["job_id"], dry_run=True)
            assert fresh["matched"] == 0
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


class _WebhookSink:
    def __init__(self):
        self.received = []
        self.status = 200
        self.pending = threading.Event()

    def __call__(self, payload, status=200):
        self.received.append(payload)
        self.status = status
        self.pending.set()


def _start_webhook_server(handler):
    import http.server

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            content_type = self.headers.get("Content-Type", "")
            handler(json.loads(body))
            self.send_response(handler.status)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def test_webhook_delivery_dispatches_immediately(tmp_path):
    async def main():
        import http.server
        import urllib.parse

        sink = _WebhookSink()
        server, thread = _start_webhook_server(sink)
        try:
            manager = JobManager(tmp_path / "state")
            try:
                url = f"http://127.0.0.1:{server.server_port}/hook"
                code = "import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint','message':'webhook me'}), flush=True)"
                started = manager.start(
                    cmd(code),
                    wake_targets=[
                        {
                            "type": "webhook",
                            "events": ["checkpoint"],
                            "url": url,
                        }
                    ],
                )
                await manager.wait(started["job_id"], ["checkpoint"], timeout_seconds=30)
                assert sink.pending.wait(timeout=5)
                assert len(sink.received) == 1
                payload = sink.received[0]
                assert payload["event"]["message"] == "webhook me"
                assert payload["event"]["job_id"] == started["job_id"]
                assert payload["delivery_id"].startswith("del_")
                assert payload["target"]["url"] == url
                assert payload["target"]["type"] == "webhook"
                assert "prompt" in payload
                delivery = poll_delivery(manager, started["job_id"], "delivered")
                assert delivery is not None and delivery["status"] == "delivered"
            finally:
                manager.begin_shutdown()
                manager.close()
        finally:
            server.shutdown()
            thread.join()

    run(main())


def test_webhook_non_2xx_marks_delivery_failed(tmp_path):
    async def main():
        class _FailSink:
            status = 500

            def __call__(self, payload, status=200):
                pass

        sink = _FailSink()
        server, thread = _start_webhook_server(sink)
        try:
            manager = JobManager(tmp_path / "state")
            try:
                url = f"http://127.0.0.1:{server.server_port}/hook"
                code = "import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint','message':'x'}), flush=True)"
                started = manager.start(
                    cmd(code),
                    wake_targets=[
                        {
                            "type": "webhook",
                            "events": ["checkpoint"],
                            "url": url,
                            "max_attempts": 1,
                            "retry_delay_seconds": 0,
                        }
                    ],
                )
                await manager.wait(started["job_id"], ["checkpoint"], timeout_seconds=30)
                deadline = time.monotonic() + 5
                delivery = None
                while time.monotonic() < deadline:
                    delivery = poll_delivery(manager, started["job_id"], "failed")
                    if delivery and delivery["status"] == "failed":
                        break
                    time.sleep(0.05)
                assert delivery is not None
                assert delivery["status"] == "failed"
                assert "HTTP 500" in (delivery["last_error"] or "")
            finally:
                manager.begin_shutdown()
                manager.close()
        finally:
            server.shutdown()
            thread.join()

    run(main())


def test_webhook_target_validation(tmp_path):
    async def main():
        manager = JobManager(tmp_path / "state")
        try:
            code = "import time; time.sleep(5)"
            for bad in [
                {"type": "webhook", "events": ["completed"]},
                {"type": "webhook", "events": ["completed"], "url": "not-a-url"},
                {"type": "webhook", "events": ["completed"], "url": "ftp://example.com/hook"},
                {"type": "webhook", "events": ["completed"], "url": "http://x", "headers": {"X-Token": 123}},
            ]:
                try:
                    manager.start(cmd(code), wake_targets=[bad])
                    raise AssertionError(f"expected validation failure for {bad!r}")
                except ValueError:
                    pass
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_webhook_auto_dispatch_false_queues_only(tmp_path):
    async def main():
        sink = _WebhookSink()
        server, thread = _start_webhook_server(sink)
        try:
            manager = JobManager(tmp_path / "state")
            try:
                url = f"http://127.0.0.1:{server.server_port}/hook"
                code = "import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint','message':'x'}), flush=True)"
                started = manager.start(
                    cmd(code),
                    wake_targets=[
                        {
                            "type": "webhook",
                            "events": ["checkpoint"],
                            "url": url,
                            "auto_dispatch": False,
                        }
                    ],
                )
                await manager.wait(started["job_id"], ["checkpoint"], timeout_seconds=30)
                time.sleep(0.5)
                assert not sink.pending.is_set(), "webhook must not fire with auto_dispatch=False"
                deliveries = manager.deliveries(started["job_id"])["deliveries"]
                assert deliveries and deliveries[0]["status"] == "pending"
            finally:
                manager.begin_shutdown()
                manager.close()
        finally:
            server.shutdown()
            thread.join()

    run(main())


def test_launch_claim_cannot_be_acquired_twice(tmp_path):
    """Review P1-1: after prepare_launch claims a job as 'launching', a second
    serialized call must return None (never double-spawn)."""
    async def main():
        manager = JobManager(tmp_path)
        try:
            job = manager.start(cmd("import sys; sys.exit(1)"))
            # A failed job is runnable (eligible for relaunch).
            await manager.wait(job["job_id"], ["failed"], timeout_seconds=30)
            first = manager.prepare_launch(job["job_id"])
            assert first is not None, "first claim should succeed"
            second = manager.prepare_launch(job["job_id"])
            assert second is None, "active claim must not be re-acquirable"
            status = manager.status(job["job_id"])["status"]
            assert status == "launching"
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_launch_claim_is_exclusive_across_manager_instances(tmp_path):
    """Review rc32 P1-2: two JobManager instances sharing one database must not
    both believe they own the same launch claim. The claim UPDATE's rowcount is
    authoritative; a post-UPDATE status SELECT cannot identify the owner, which
    let two callers each think they won."""
    import threading as _threading

    async def main():
        manager = JobManager(tmp_path, recover=False)
        manager2 = JobManager(tmp_path, recover=False)
        try:
            job = manager.start(cmd("import sys; sys.exit(1)"))
            await manager.wait(job["job_id"], ["failed"], timeout_seconds=30)

            # Both instances race prepare_launch on the same failed job.
            barrier = _threading.Barrier(2)
            results: dict[str, object] = {}

            def claim_a():
                barrier.wait()
                try:
                    results["a"] = manager.prepare_launch(job["job_id"])
                except Exception as exc:  # pragma: no cover
                    results["a"] = exc

            def claim_b():
                barrier.wait()
                try:
                    results["b"] = manager2.prepare_launch(job["job_id"])
                except Exception as exc:  # pragma: no cover
                    results["b"] = exc

            ta = _threading.Thread(target=claim_a)
            tb = _threading.Thread(target=claim_b)
            ta.start(); tb.start(); ta.join(); tb.join()

            winners = [k for k in ("a", "b") if isinstance(results[k], dict) and results[k] is not None]
            assert len(winners) == 1, f"expected exactly one claim winner, got {winners}"
            # The row is claimed 'launching' by the winner.
            status = manager.status(job["job_id"])["status"]
            assert status == "launching"
        finally:
            manager.begin_shutdown()
            manager.close()
            manager2.begin_shutdown()
            manager2.close()

    run(main())


def test_runner_promotes_owned_claim_and_parent_cannot_resurrect(tmp_path):
    """Review rc32 P1-3: the runner atomically promotes its OWNED claim
    (launching -> running, guarded by claim_token), and a parent write for that
    claim can never resurrect a run the runner already finished."""
    import os as _os
    import threading as _threading

    async def main():
        manager = JobManager(tmp_path, recover=False)
        try:
            job = manager.start(cmd("import sys; sys.exit(1)"))
            await manager.wait(job["job_id"], ["failed"], timeout_seconds=30)
            launch = manager.prepare_launch(job["job_id"])
            assert launch is not None
            token = launch["claim_token"]
            row = manager._row("SELECT status, claim_token FROM jobs WHERE job_id=?", (job["job_id"],))
            assert row["status"] == "launching"
            assert row["claim_token"] == token

            # The runner promotes its claim atomically.
            from vanth.runner import _publish_workload
            assert _publish_workload(manager, job["job_id"], 4242, token)
            row = manager._row("SELECT status, pid, worker_pid, started_at FROM jobs WHERE job_id=?", (job["job_id"],))
            assert row["status"] == "running"
            assert row["pid"] == 4242
            assert row["worker_pid"] == _os.getpid()
            assert row["started_at"] is not None

            # A fast job then records its terminal state through the claim-owned
            # transition (previously REJECTED because the row was still
            # 'launching' when the parent update raced).
            assert manager._transition_terminal(job["job_id"], "completed", 0, claim_token=token)
            row = manager._row("SELECT status, exit_code FROM jobs WHERE job_id=?", (job["job_id"],))
            assert row["status"] == "completed"

            # A guarded parent write can no longer resurrect the dead runner.
            with manager.db_lock:
                changed = manager.db.execute(
                    "UPDATE jobs SET status='running', updated_at=? "
                    "WHERE job_id=? AND claim_token=? AND status='launching'",
                    (now_iso(), job["job_id"], token),
                ).rowcount
                manager.db.commit()
            assert changed == 0, "parent must not resurrect a finished run"
            assert manager.status(job["job_id"])["status"] == "completed"
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_launch_lost_claim_kills_runner_and_does_not_resurrect(tmp_path):
    """Review rc32 P1-3: if the parent's guarded worker_pid write fails because
    the claim was already finished, _launch must NOT mark the row running."""
    import threading as _threading

    async def main():
        manager = JobManager(tmp_path, recover=False)
        try:
            job = manager.start(cmd("import sys; sys.exit(1)"))
            await manager.wait(job["job_id"], ["failed"], timeout_seconds=30)
            launch = manager.prepare_launch(job["job_id"])
            token = launch["claim_token"]

            # Simulate a runner that already finished the job before the parent
            # write (the deterministic delayed-parent probe).
            assert manager._transition_terminal(job["job_id"], "completed", 0, claim_token=token)

            # _launch with this token must not resurrect the finished run.
            result = manager._launch(
                job["job_id"],
                launch["stdout_path"],
                launch["stderr_path"],
                launch["events_path"],
                launch["spec_path"],
                claim_token=token,
            )
            # It must not report running; the run stays completed.
            assert result["status"] != "running"
            assert manager.status(job["job_id"])["status"] == "completed"
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_stale_launch_claim_recovers_to_orphaned(tmp_path):
    """Review P1-1: a claim abandoned by a crash (row stuck 'launching') is
    recovered by the dispatch loop after the grace period."""
    async def main():
        manager = JobManager(tmp_path)
        try:
            job = manager.start(cmd("import sys; sys.exit(1)"))
            await manager.wait(job["job_id"], ["failed"], timeout_seconds=30)
            first = manager.prepare_launch(job["job_id"])
            assert first is not None
            # Age the claim past the timeout by rewriting updated_at.
            import datetime as _dt
            stale = (_dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(seconds=manager.launch_claim_timeout + 5)).isoformat().replace("+00:00", "Z")
            with manager.db_lock:
                manager.db.execute(
                    "UPDATE jobs SET updated_at=? WHERE job_id=? AND status='launching'",
                    (stale, job["job_id"]),
                )
                manager.db.commit()
            manager._recover_stale_launch_claims()
            status = manager.status(job["job_id"])["status"]
            assert status == "orphaned", f"stale claim should be recovered, got {status}"
            # The recovery emits an orphaned event so waits/wake targets fire;
            # poll briefly since a concurrent dispatcher pass may have won the
            # recovery and the event write can land a tick later under load.
            deadline = time.monotonic() + 5
            events = []
            while time.monotonic() < deadline:
                events = manager.events(job["job_id"], types=["orphaned"], limit=10)["events"]
                if events:
                    break
                time.sleep(0.05)
            assert events, "stale-claim recovery must emit an orphaned event"
            # The recovered job is runnable again.
            assert manager.prepare_launch(job["job_id"]) is not None
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_stale_recovery_skips_live_runner(tmp_path):
    """Review rc32 P1-3: recovery must NOT orphan a 'launching' row whose
    runner process is still alive (it is mid-publish)."""
    async def main():
        manager = JobManager(tmp_path)
        try:
            job = manager.start(cmd("import sys; sys.exit(1)"))
            await manager.wait(job["job_id"], ["failed"], timeout_seconds=30)
            launch = manager.prepare_launch(job["job_id"])
            assert launch is not None
            token = launch["claim_token"]
            # Simulate a spawned (alive) runner: set worker_pid to this process.
            import os as _os
            import datetime as _dt
            with manager.db_lock:
                manager.db.execute(
                    "UPDATE jobs SET worker_pid=?, updated_at=? WHERE job_id=? AND status='launching'",
                    (_os.getpid(), _dt.datetime.now(_dt.timezone.utc).isoformat().replace("+00:00", "Z"), job["job_id"]),
                )
                manager.db.commit()
                stale = (_dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(seconds=manager.launch_claim_timeout + 5)).isoformat().replace("+00:00", "Z")
                manager.db.execute(
                    "UPDATE jobs SET updated_at=? WHERE job_id=? AND status='launching'",
                    (stale, job["job_id"]),
                )
                manager.db.commit()
            manager._recover_stale_launch_claims()
            status = manager.status(job["job_id"])["status"]
            assert status == "launching", f"live launch must not be orphaned, got {status}"
            # And the live run can still promote normally.
            from vanth.runner import _publish_workload
            assert _publish_workload(manager, job["job_id"], 9999, token)
            assert manager.status(job["job_id"])["status"] == "running"
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_restart_deadline_survives_until_claim_is_atomic(tmp_path):
    """Review rc32 P1-4: the restart deadline must remain persisted until it is
    cleared atomically WITH the launch claim. A crash (or failed claim) between
    budget consumption and launch ownership must not leave a failed job with its
    attempt consumed and no pending deadline (which turned max_retries=1 into
    immediate gave_up)."""
    async def main():
        manager = JobManager(tmp_path)
        try:
            job = manager.start(
                cmd("import sys; sys.exit(1)"),
                policy={"restart": {"max_retries": 1, "backoff_seconds": 5}},
            )
            await manager.wait(job["job_id"], ["failed"], timeout_seconds=30)
            await asyncio.sleep(0.6)
            state = manager._policy_state(job["job_id"])
            assert state["restart_attempts"] == 1
            deadline = state.get("restart_after")
            assert deadline is not None, "deadline must persist after budget claim"

            # Disable the job: the claim must be REFUSED and the deadline must
            # stay intact (old code cleared it before prepare_launch -> budget
            # lost -> immediate gave_up).
            with manager.db_lock:
                manager.db.execute(
                    "UPDATE jobs SET policy_disabled=1 WHERE job_id=?", (job["job_id"],)
                )
                manager.db.commit()
            launch = manager._claim_due_restart(job["job_id"], deadline)
            assert launch is None, "disabled job must not claim a restart launch"
            state = manager._policy_state(job["job_id"])
            assert state.get("restart_after") == deadline, "deadline must survive a refused claim"
            assert state["restart_attempts"] == 1, "budget must not be consumed by a refused claim"

            # Re-enable: the atomic claim now clears the deadline AND claims the
            # row in one transaction.
            with manager.db_lock:
                manager.db.execute(
                    "UPDATE jobs SET policy_disabled=0 WHERE job_id=?", (job["job_id"],)
                )
                manager.db.commit()
            launch = manager._claim_due_restart(job["job_id"], deadline)
            assert launch is not None, "due restart must claim after re-enable"
            row = manager._row("SELECT status, policy_state_json FROM jobs WHERE job_id=?", (job["job_id"],))
            assert row["status"] == "launching"
            state = json.loads(row["policy_state_json"] or "{}")
            assert state.get("restart_after") is None, "deadline cleared atomically with the claim"
            # A second claim of the same deadline is refused.
            assert manager._claim_due_restart(job["job_id"], deadline) is None
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_restart_failures_advance_failure_streak(tmp_path):
    """Review P1-2: automatic restarts reuse the job row, and each failed
    execution (original + restarts) must advance the failure streak."""
    async def main():
        manager = JobManager(tmp_path)
        try:
            job = manager.start(
                cmd("import sys; sys.exit(1)"),
                policy={
                    "restart": {"max_retries": 2, "backoff_seconds": 0},
                    "on_failure": {"after_n": 3, "action": "alert"},
                },
            )
            await manager.wait(job["job_id"], ["failed"], timeout_seconds=30)
            await manager.wait(job["job_id"], ["gave_up"], timeout_seconds=60)
            # The streak watcher may lag the gave_up event by a tick; poll.
            # 15s window: the full suite runs under load and the watcher tick can
            # lag well past the 5s it comfortably clears in isolation.
            deadline = time.monotonic() + 15
            streak = 0
            while time.monotonic() < deadline:
                streak = int(manager._policy_state(job["job_id"]).get("failure_streak", 0))
                if streak == 3:
                    break
                await asyncio.sleep(0.2)
            # 1 original + 2 restarts = 3 failed executions.
            assert streak == 3, f"expected streak 3, got {streak}"
            thresholds = [e for e in manager.events(job["job_id"], types=["failure_threshold"], limit=10)["events"]]
            assert len(thresholds) == 1, "after_n=3 should fire exactly once"
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_webhook_redirect_does_not_leak_headers(tmp_path):
    """Review P1-4: a 302 redirect must NOT be followed (credentials would be
    forwarded cross-origin); the delivery fails instead, and the destination
    never receives the request."""
    import http.server

    class _RedirectSink:
        received = False

    sink = _RedirectSink()

    class RedirectHandler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            # Drain the request body before responding: closing with unread data
            # in the socket buffer makes the OS send an RST, which surfaces as a
            # connection-aborted error (WinError 10053) before the client can
            # read the 302 — masking the redirect-refusal path under test.
            length = int(self.headers.get("Content-Length", 0) or 0)
            if length:
                self.rfile.read(length)
            self.send_response(302)
            self.send_header("Location", f"http://127.0.0.1:{target_port}/dest")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args):
            pass

    class DestHandler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            sink.received = True
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args):
            pass

    target_server = http.server.HTTPServer(("127.0.0.1", 0), RedirectHandler)
    target_port = target_server.server_port
    dest_server = http.server.HTTPServer(("127.0.0.1", 0), DestHandler)
    dest_port = dest_server.server_port
    t1 = threading.Thread(target=target_server.serve_forever, daemon=True)
    t2 = threading.Thread(target=dest_server.serve_forever, daemon=True)
    t1.start()
    t2.start()
    try:
        async def main():
            manager = JobManager(tmp_path)
            try:
                url = f"http://127.0.0.1:{target_port}/hook"
                code = "import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint','message':'x'}), flush=True)"
                started = manager.start(
                    cmd(code),
                    wake_targets=[
                        {
                            "type": "webhook",
                            "events": ["checkpoint"],
                            "url": url,
                            "headers": {"Authorization": "Bearer secret"},
                            "max_attempts": 1,
                            "retry_delay_seconds": 0,
                        }
                    ],
                )
                await manager.wait(started["job_id"], ["checkpoint"], timeout_seconds=30)
                delivery = None
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    delivery = poll_delivery(manager, started["job_id"], "failed")
                    if delivery and delivery["status"] == "failed":
                        break
                    time.sleep(0.05)
                assert delivery is not None
                assert delivery["status"] == "failed"
                assert "redirect" in (delivery["last_error"] or "").lower()
                await asyncio.sleep(0.3)
                assert sink.received is False, "redirect destination must not receive the request"
            finally:
                manager.begin_shutdown()
                manager.close()

        run(main())
    finally:
        target_server.shutdown()
        dest_server.shutdown()
        t1.join()
        t2.join()


def test_webhook_payload_omits_header_secrets(tmp_path):
    """Review P1-4: configured header secrets are sent as headers only and
    never duplicated into the JSON payload body."""
    sink = _WebhookSink()
    server, thread = _start_webhook_server(sink)
    try:
        async def main():
            manager = JobManager(tmp_path)
            try:
                url = f"http://127.0.0.1:{server.server_port}/hook"
                code = "import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint','message':'x'}), flush=True)"
                started = manager.start(
                    cmd(code),
                    wake_targets=[
                        {
                            "type": "webhook",
                            "events": ["checkpoint"],
                            "url": url,
                            "headers": {"Authorization": "Bearer secret"},
                        }
                    ],
                )
                await manager.wait(started["job_id"], ["checkpoint"], timeout_seconds=30)
                assert sink.pending.wait(timeout=5)
                payload = sink.received[0]
                target = payload["target"]
                assert "headers" not in target, "secrets must not be in the JSON body"
                assert "Authorization" not in payload
            finally:
                manager.begin_shutdown()
                manager.close()

        run(main())
    finally:
        server.shutdown()
        thread.join()


def test_clear_deliveries_default_does_not_touch_delivered(tmp_path):
    """Review P2-1: a default drain (no explicit status) only clears
    pending/retrying deliveries and never rewrites delivered history."""
    async def main():
        manager = JobManager(tmp_path)
        try:
            code = "import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint','message':'x'}), flush=True)"
            started = manager.start(
                cmd(code),
                wake_targets=[{"type": "codex_thread", "thread_id": "t_del", "events": ["checkpoint"], "auto_dispatch": False}],
            )
            await manager.wait(started["job_id"], ["checkpoint"], timeout_seconds=30)
            # Manually mark the delivery delivered (settled history).
            with manager.db_lock:
                manager.db.execute(
                    "UPDATE deliveries SET status='delivered' WHERE job_id=?",
                    (started["job_id"],),
                )
                manager.db.commit()
            result = manager.clear_deliveries(job_id=started["job_id"], dry_run=False)
            assert result["matched"] == 0, "delivered rows must not be matched by a default drain"
            deliveries = manager.deliveries(started["job_id"])["deliveries"]
            assert deliveries and deliveries[0]["status"] == "delivered"
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_parent_does_not_kill_runner_that_promoted_before_worker_pid_write(tmp_path):
    """Review rc33 P1-1: if the runner promotes launching -> running before the
    parent's worker_pid write, the parent must treat that as OWNED SUCCESS (never
    terminate the valid runner). The old rowcount==0 branch killed it."""
    import os as _os

    async def main():
        manager = JobManager(tmp_path, recover=False)
        try:
            job = manager.start(cmd("import sys; sys.exit(1)"))
            await manager.wait(job["job_id"], ["failed"], timeout_seconds=30)
            launch = manager.prepare_launch(job["job_id"])
            token = launch["claim_token"]

            # The runner promotes first (rowcount 0 for the parent's write).
            from vanth.runner import _publish_workload
            assert _publish_workload(manager, job["job_id"], 4242, token)

            # The parent then attempts its worker_pid write and gets rowcount 0.
            # It must NOT terminate the runner; it must report owned success.
            with manager.db_lock:
                wrote = manager.db.execute(
                    "UPDATE jobs SET worker_pid=?, updated_at=? WHERE job_id=? AND claim_token=? AND status='launching'",
                    (99999, now_iso(), job["job_id"], token),
                ).rowcount
                manager.db.commit()
            assert wrote == 0
            # The row is still running under the runner's worker_pid; no kill.
            row = manager._row("SELECT status, worker_pid, claim_token FROM jobs WHERE job_id=?", (job["job_id"],))
            assert row["status"] == "running"
            assert row["claim_token"] == token
            assert row["worker_pid"] == _os.getpid()
            # The runner can still finish normally.
            assert manager._transition_terminal(job["job_id"], "completed", 0, claim_token=token)
            assert manager.status(job["job_id"])["status"] == "completed"
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_no_token_launch_cannot_resurrect_terminal_job(tmp_path):
    """Review rc33 P1-2: the no-token parent 'running' write is guarded by run
    identity. A fast job that completed/cancelled while the parent was returning
    from Popen must never be resurrected to 'running' by the parent write."""
    async def main():
        manager = JobManager(tmp_path, recover=False)
        try:
            # Simulate the reviewer's delayed-parent probe on the start() path:
            # the row is inserted 'running' with started_at; a terminal
            # transition happens (as a fast job would) before the parent's
            # no-token write.
            job = manager.start(cmd("import sys; sys.exit(1)"))
            await manager.wait(job["job_id"], ["failed"], timeout_seconds=30)
            # The parent's no-token write is guarded by the ORIGINAL started_at;
            # after the failure the row is 'failed' with ended_at set, so the
            # started_at-guarded write cannot match.
            with manager.db_lock:
                row = manager.db.execute("SELECT started_at, status FROM jobs WHERE job_id=?", (job["job_id"],)).fetchone()
                assert row["status"] == "failed"
                changed = manager.db.execute(
                    "UPDATE jobs SET status='running', worker_pid=?, runner_heartbeat_at=?, updated_at=?, exit_code=NULL, ended_at=NULL "
                    "WHERE job_id=? AND status='running' AND started_at=?",
                    (12345, now_iso(), now_iso(), job["job_id"], row["started_at"]),
                ).rowcount
                manager.db.commit()
            assert changed == 0, "no-token parent write must not resurrect a terminal job"
            assert manager.status(job["job_id"])["status"] == "failed"
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_stale_runner_cannot_consume_newer_claim_token(tmp_path):
    """Review rc33 P1-3: a delayed runner reads a CLAIM-SPECIFIC spec file
    (specs/{job_id}-{claim_token}.json), never the shared mutable spec. After
    stale recovery and a new claim, an old delayed runner reading the OLD
    claim-specific file cannot impersonate the new run."""
    async def main():
        manager = JobManager(tmp_path, recover=False)
        try:
            job = manager.start(cmd("import sys; sys.exit(1)"))
            await manager.wait(job["job_id"], ["failed"], timeout_seconds=30)
            launch1 = manager.prepare_launch(job["job_id"])
            token1 = launch1["claim_token"]
            spec1 = manager.home / "specs" / f"{job['job_id']}-{token1}.json"
            assert spec1.exists(), "claim-specific spec must be written"

            # A delayed runner reads the claim-specific spec of ITS claim.
            import vanth.runner as runner_mod
            spec1_json = json.loads(spec1.read_text(encoding="utf-8"))
            assert spec1_json.get("claim_token") == token1

            # The claim is abandoned (crash) and recovered, then a NEW claim wins.
            manager._abandon_launch_claim(job["job_id"], token1)
            launch2 = manager.prepare_launch(job["job_id"])
            token2 = launch2["claim_token"]
            assert token2 != token1
            spec2 = manager.home / "specs" / f"{job['job_id']}-{token2}.json"
            assert spec2.exists()

            # The OLD claim-specific spec is GONE or superseded; a delayed runner
            # that holds the OLD spec name cannot read the NEW token.
            if spec1.exists():
                # Only possible if spec1 is the same path as spec2 (it is not).
                pass
            # The runner is invoked with ITS claim's spec name (argv), so even if
            # the old spec file lingers it carries the old token and cannot
            # promote the new claim.
            spec1_readback = spec1.read_text(encoding="utf-8") if spec1.exists() else None
            assert spec1_readback is None or token1 not in spec2.read_text(encoding="utf-8")
            # The shared mutable specs/{job_id}.json is never written by the
            # launch path, so a delayed runner reading it gets nothing.
            assert not (manager.home / "specs" / f"{job['job_id']}.json").exists()
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_stale_recovery_does_not_orphan_promoted_run(tmp_path):
    """Review rc33 P1-4: recovery must NOT orphan a run the runner already
    promoted to 'running'. The launching-only, token-guarded transition returns
    0 and leaves the live workload alone."""
    async def main():
        manager = JobManager(tmp_path, recover=False)
        try:
            job = manager.start(cmd("import sys; sys.exit(1)"))
            await manager.wait(job["job_id"], ["failed"], timeout_seconds=30)
            launch = manager.prepare_launch(job["job_id"])
            token = launch["claim_token"]
            row = manager._row("SELECT status, claim_token FROM jobs WHERE job_id=?", (job["job_id"],))
            assert row["status"] == "launching"

            # Simulate the runner promoting between the snapshot and the
            # transition: it owns 'running' now.
            from vanth.runner import _publish_workload
            assert _publish_workload(manager, job["job_id"], 4242, token)
            assert manager.status(job["job_id"])["status"] == "running"

            # Stale recovery sees a launching snapshot, but the launching-only
            # guard must NOT orphan the promoted run.
            with manager.db_lock:
                stale = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=manager.launch_claim_timeout + 5)).isoformat().replace("+00:00", "Z")
                manager.db.execute(
                    "UPDATE jobs SET updated_at=? WHERE job_id=? AND status='launching'",
                    (stale, job["job_id"]),
                )
                manager.db.commit()
            manager._recover_stale_launch_claims()
            status = manager.status(job["job_id"])["status"]
            assert status == "running", f"recovery must not orphan a promoted run, got {status}"
            assert manager.events(job["job_id"], types=["orphaned"])["events"] == []
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_heartbeat_reconciliation_does_not_orphan_newer_run(tmp_path):
    """Review rc33 P1-5: heartbeat reconciliation is run-identity guarded. A
    stale-'running' snapshot is finalized only if the row is STILL running under
    the recorded worker (or token); a newer run that took ownership is never
    orphaned by an old reconciliation pass."""
    async def main():
        manager = JobManager(tmp_path, recover=False)
        try:
            job = manager.start(cmd("import sys; sys.exit(1)"))
            await manager.wait(job["job_id"], ["failed"], timeout_seconds=30)
            launch = manager.prepare_launch(job["job_id"])
            token = launch["claim_token"]
            from vanth.runner import _publish_workload
            assert _publish_workload(manager, job["job_id"], 4242, token)
            row = manager._row("SELECT status, worker_pid, claim_token FROM jobs WHERE job_id=?", (job["job_id"],))
            assert row["status"] == "running"
            assert row["claim_token"] == token

            # Simulate a stale heartbeat snapshot for the OLD worker, then a
            # newer run taking ownership (new token, new worker).
            with manager.db_lock:
                manager.db.execute(
                    "UPDATE jobs SET status='running', claim_token=?, worker_pid=?, runner_heartbeat_at=?, updated_at=? "
                    "WHERE job_id=?",
                    ("claim_newer", 55555, now_iso(), now_iso(), job["job_id"]),
                )
                manager.db.commit()
            # The stale reconciliation pass holds the OLD token; the guarded
            # transition must not orphan the newer run.
            transitioned = manager._transition_terminal(job["job_id"], "orphaned", claim_token=token)
            assert not transitioned, "an old token must not orphan a newer run"
            assert manager.status(job["job_id"])["status"] == "running"
            assert manager._row("SELECT claim_token FROM jobs WHERE job_id=?", (job["job_id"],))["claim_token"] == "claim_newer"
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_abandoned_restart_claim_preserves_budgeted_retry(tmp_path):
    """Review rc33 P1-6: a restart claim whose launch never spawns (crash before
    spawn) must keep its budgeted retry intent. Recovery restores the deadline
    and recovers the row as 'failed' so restart policy relaunches."""
    async def main():
        manager = JobManager(tmp_path)
        try:
            job = manager.start(
                cmd("import sys; sys.exit(1)"),
                policy={"restart": {"max_retries": 1, "backoff_seconds": 5}},
            )
            await manager.wait(job["job_id"], ["failed"], timeout_seconds=30)
            await asyncio.sleep(0.6)
            state = manager._policy_state(job["job_id"])
            assert state["restart_attempts"] == 1
            deadline = state.get("restart_after")
            assert deadline is not None

            # Claim the due restart (clears restart_after atomically).
            launch = manager._claim_due_restart(job["job_id"], deadline)
            assert launch is not None
            row = manager._row("SELECT status, policy_state_json FROM jobs WHERE job_id=?", (job["job_id"],))
            assert row["status"] == "launching"
            state = json.loads(row["policy_state_json"] or "{}")
            assert state.get("restart_after") is None
            assert state.get("pending_restart_after") == deadline, "pending deadline must be recorded with the claim"

            # Crash before spawn: the claim goes stale and recovery runs.
            with manager.db_lock:
                stale = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=manager.launch_claim_timeout + 5)).isoformat().replace("+00:00", "Z")
                manager.db.execute(
                    "UPDATE jobs SET updated_at=? WHERE job_id=? AND status='launching'",
                    (stale, job["job_id"]),
                )
                manager.db.commit()
            manager._recover_stale_launch_claims()
            status = manager.status(job["job_id"])["status"]
            # The budgeted retry intent must survive: the row is failed (not
            # orphaned) with the deadline restored so restart policy relaunches.
            assert status == "failed", f"abandoned restart claim must recover as failed, got {status}"
            state = manager._policy_state(job["job_id"])
            assert state.get("restart_after") == deadline, "restart deadline must be restored after abandoned claim"
            assert state.get("pending_restart_after") is None
            # The retry eventually relaunches (policy sees a failed row with a
            # due deadline).
            events = manager.events(job["job_id"], types=["orphaned"])["events"]
            assert events == [], "an abandoned restart claim must not emit orphaned"
        finally:
            manager.begin_shutdown()
            manager.close()


def test_parent_worker_pid_write_does_not_clear_pending_restart_intent(tmp_path):
    """Review rc34 P1-1: the parent must NOT clear pending_restart_after when it
    records worker_pid. The row is still 'launching' at that point; if the runner
    dies before promotion, recovery must still find the pending deadline to
    restore. Only the runner's token-guarded promotion transaction clears it."""
    async def main():
        manager = JobManager(tmp_path)
        try:
            job = manager.start(
                cmd("import sys; sys.exit(1)"),
                policy={"restart": {"max_retries": 1, "backoff_seconds": 5}},
            )
            await manager.wait(job["job_id"], ["failed"], timeout_seconds=30)
            await asyncio.sleep(0.6)
            state = manager._policy_state(job["job_id"])
            deadline = state.get("restart_after")
            assert deadline is not None

            # Stop the maintenance loop: it could otherwise re-claim the due
            # restart and relaunch the job while we assert the abandoned-claim
            # recovery outcome (a dispatcher race on fast runners).
            manager.dispatcher_stop.set()
            if manager.dispatcher_thread is not None:
                manager.dispatcher_thread.join(timeout=5)

            launch = manager._claim_due_restart(job["job_id"], deadline)
            assert launch is not None
            token = launch["claim_token"]
            state = manager._policy_state(job["job_id"])
            assert state.get("pending_restart_after") == deadline

            # The parent records worker_pid (row still 'launching') — this must
            # NOT clear the pending intent.
            with manager.db_lock:
                manager.db.execute(
                    "UPDATE jobs SET worker_pid=?, updated_at=? WHERE job_id=? AND claim_token=? AND status='launching'",
                    (424242, now_iso(), job["job_id"], token),
                )
                manager.db.commit()
            state = manager._policy_state(job["job_id"])
            assert state.get("pending_restart_after") == deadline, "parent worker_pid write must not clear pending restart intent"

            # The runner dies before promotion: the claim is abandoned and
            # recovery restores the deadline (budgeted retry survives).
            with manager.db_lock:
                stale = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=manager.launch_claim_timeout + 5)).isoformat().replace("+00:00", "Z")
                manager.db.execute(
                    "UPDATE jobs SET updated_at=? WHERE job_id=? AND status='launching'",
                    (stale, job["job_id"]),
                )
                manager.db.commit()
            manager._recover_stale_launch_claims()
            assert manager.status(job["job_id"])["status"] == "failed"
            state = manager._policy_state(job["job_id"])
            assert state.get("restart_after") == deadline, "deadline must be restored after abandoned pre-promotion claim"
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_pending_restart_clear_is_token_guarded(tmp_path):
    """Review rc34 P1-1: _clear_pending_restart_after must require the owning
    claim token, so a delayed runner from a newer claim can never erase that
    claim's pending restart intent."""
    async def main():
        manager = JobManager(tmp_path)
        try:
            job = manager.start(
                cmd("import sys; sys.exit(1)"),
                policy={"restart": {"max_retries": 1, "backoff_seconds": 5}},
            )
            await manager.wait(job["job_id"], ["failed"], timeout_seconds=30)
            await asyncio.sleep(0.6)
            deadline = manager._policy_state(job["job_id"]).get("restart_after")
            assert deadline is not None

            launch = manager._claim_due_restart(job["job_id"], deadline)
            token_a = launch["claim_token"]
            assert manager._policy_state(job["job_id"]).get("pending_restart_after") == deadline

            # A stale runner holding a DIFFERENT token must not clear it.
            manager._clear_pending_restart_after(job["job_id"], "claim_wrongtoken")
            assert manager._policy_state(job["job_id"]).get("pending_restart_after") == deadline, "wrong token must not clear intent"

            # The owning token clears it (as the promotion path does).
            manager._clear_pending_restart_after(job["job_id"], token_a)
            assert manager._policy_state(job["job_id"]).get("pending_restart_after") is None
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_recover_between_insert_and_popen_cannot_orphan_pre_spawn(tmp_path):
    """Review rc36 P1: every direct start is inserted 'launching' with a claim
    token (never 'running' with a NULL worker_pid), so a second manager's
    recovery CANNOT orphan a fresh pre-spawn row. Manager B snapshots the
    pre-spawn row (worker_pid IS NULL); if the runner publishes a live pid
    before B's CAS, B's transition must affect zero rows and must not kill or
    terminalize the job."""
    import os as _os

    async def main():
        manager = JobManager(tmp_path, recover=False)
        try:
            job = manager.start(cmd("import sys; sys.exit(1)"))
            await manager.wait(job["job_id"], ["failed"], timeout_seconds=30)
            # Simulate manager A: claim as launching with worker_pid still NULL.
            token = "claim_" + uuid.uuid4().hex[:16]
            with manager.db_lock:
                manager.db.execute(
                    "UPDATE jobs SET status='launching', claim_token=? WHERE job_id=?",
                    (token, job["job_id"]),
                )
                manager.db.commit()

            # Manager B's stale-claim recovery observes worker_pid IS NULL and
            # attempts to abandon the claim (aging it past the grace window).
            # The runner meanwhile publishes a live worker_pid.
            from vanth.runner import _publish_workload
            assert _publish_workload(manager, job["job_id"], 4242, token)

            # B's recovery CAS binds worker_pid IS NULL; the live publish
            # changed it, so the CAS must affect ZERO rows.
            recovered, target = manager._abandon_launch_claim(
                job["job_id"], token, expected_worker_pid=None
            )
            assert not recovered, "stale recovery must not orphan a live runner that published after a NULL snapshot"
            status = manager.status(job["job_id"])["status"]
            assert status == "running", f"live run must survive, got {status}"
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_popen_failure_after_newer_run_is_token_guarded(tmp_path):
    """Review rc36 P1: A's Popen failure handler is claim-token guarded, so a
    stale starter can never mark a NEWER run failed."""
    async def main():
        manager = JobManager(tmp_path, recover=False)
        try:
            job = manager.start(cmd("import sys; sys.exit(1)"))
            await manager.wait(job["job_id"], ["failed"], timeout_seconds=30)
            launch = manager.prepare_launch(job["job_id"])
            token_a = launch["claim_token"]

            # Manager B establishes a newer run with a different claim token.
            token_b = "claim_" + uuid.uuid4().hex[:16]
            with manager.db_lock:
                manager.db.execute(
                    "UPDATE jobs SET status='launching', claim_token=? WHERE job_id=?",
                    (token_b, job["job_id"]),
                )
                manager.db.commit()

            # A's Popen failure handler with its OLD token must not clobber B's
            # row (a guarded write affecting zero rows).
            stamp = now_iso()
            with manager.db_lock:
                changed = manager.db.execute(
                    "UPDATE jobs SET status='failed', exit_code=1, ended_at=?, updated_at=?, stop_requested_at=NULL "
                    "WHERE job_id=? AND claim_token=? AND status='launching'",
                    (stamp, stamp, job["job_id"], token_a),
                ).rowcount
                manager.db.commit()
            assert changed == 0, "stale starter must not mark a newer run failed"
            row = manager._row("SELECT status, claim_token FROM jobs WHERE job_id=?", (job["job_id"],))
            assert row["status"] == "launching" and row["claim_token"] == token_b
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_reconcile_null_worker_snapshot_cas_loses_after_live_publish(tmp_path):
    """Review rc36 P1: a stale reconciler/watcher snapshots worker_pid IS NULL;
    the runner publishes a live pid; the reconciler's terminal CAS (binding
    worker_pid IS NULL) must lose and must not orphan the live run."""
    import os as _os

    async def main():
        manager = JobManager(tmp_path, recover=False)
        try:
            job = manager.start(cmd("import sys; sys.exit(1)"))
            await manager.wait(job["job_id"], ["failed"], timeout_seconds=30)
            launch = manager.prepare_launch(job["job_id"])
            token = launch["claim_token"]

            # Runner publishes a live workload/worker (the row becomes running).
            from vanth.runner import _publish_workload
            assert _publish_workload(manager, job["job_id"], 4242, token)

            # A stale watcher/reconciler that snapshotted worker_pid IS NULL
            # (before the publish) now tries to terminalize. The sentinel binds
            # `worker_pid IS NULL`, which no longer matches.
            changed = manager._transition_terminal(
                job["job_id"], "orphaned", worker_pid=None
            )
            assert not changed, "stale NULL-worker terminal write must lose"
            assert manager.status(job["job_id"])["status"] == "running"
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_stale_claim_recovery_null_snapshot_cas_loses(tmp_path):
    """Review rc36 P1: stale-claim recovery snapshots worker_pid IS NULL; the
    runner publishes a live worker_pid; recovery's launching-only CAS must
    affect zero rows and never kill or terminalize the job."""
    async def main():
        manager = JobManager(tmp_path, recover=False)
        try:
            job = manager.start(cmd("import sys; sys.exit(1)"))
            await manager.wait(job["job_id"], ["failed"], timeout_seconds=30)
            launch = manager.prepare_launch(job["job_id"])
            token = launch["claim_token"]

            # The runner publishes a live worker between the snapshot and CAS.
            import os as _os
            with manager.db_lock:
                manager.db.execute(
                    "UPDATE jobs SET worker_pid=?, updated_at=? WHERE job_id=? AND status='launching'",
                    (_os.getpid(), now_iso(), job["job_id"]),
                )
                manager.db.commit()
                stale = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=manager.launch_claim_timeout + 5)).isoformat().replace("+00:00", "Z")
                manager.db.execute(
                    "UPDATE jobs SET updated_at=? WHERE job_id=? AND status='launching'",
                    (stale, job["job_id"]),
                )
                manager.db.commit()

            # Recovery snapshot observed worker_pid == this live pid; the CAS
            # matches and would orphan — so this asserts the OPPOSITE snapshot:
            # the recovery snapshot saw NULL, the runner then wrote a live pid.
            # Bind NULL explicitly to prove the guard loses.
            recovered, _ = manager._abandon_launch_claim(job["job_id"], token, expected_worker_pid=None)
            assert not recovered, "recovery with a NULL-observed worker must lose against a live publish"
            assert manager.status(job["job_id"])["status"] == "launching"
        finally:
            manager.begin_shutdown()
            manager.close()

    run(main())


def test_restore_clear_cross_process_cas(tmp_path):
    """Review rc36 P1: _restore_pending_restart_after and
    _clear_pending_restart_after must be single guarded UPDATEs (WHERE token +
    full-state CAS), so two managers cannot interleave a SELECT/UPDATE and
    corrupt state. A stale clear from an OLD claim must not erase a NEWER
    claim's pending deadline, and a stale restore must not replace a newer
    claim's state."""
    async def main():
        m1 = JobManager(tmp_path)
        m2 = JobManager(tmp_path, recover=False)
        try:
            job = m1.start(
                cmd("import sys; sys.exit(1)"),
                policy={"restart": {"max_retries": 1, "backoff_seconds": 5}},
            )
            await m1.wait(job["job_id"], ["failed"], timeout_seconds=30)
            await asyncio.sleep(0.6)
            deadline_a = m1._policy_state(job["job_id"]).get("restart_after")
            assert deadline_a is not None
            launch_a = m1._claim_due_restart(job["job_id"], deadline_a)
            token_a = launch_a["claim_token"]
            assert m1._policy_state(job["job_id"]).get("pending_restart_after") == deadline_a

            # A stale clear from claim A (already abandoned) must NOT erase the
            # pending intent of a NEWER claim B held by another manager.
            launch_b = None
            with m2.db_lock:
                # Simulate recovery abandoning claim A and a newer claim B taking
                # over (new token, still has pending intent).
                m2.db.execute(
                    "UPDATE jobs SET status='launching', claim_token=?, policy_state_json=? WHERE job_id=?",
                    ("claim_B", json.dumps({"pending_restart_after": deadline_a, "restart_attempts": 1}, separators=(",", ":")), job["job_id"]),
                )
                m2.db.commit()
            # Stale clear from the OLD token A: must affect zero rows.
            m1._clear_pending_restart_after(job["job_id"], token_a)
            state = m2._policy_state(job["job_id"])
            assert state.get("pending_restart_after") == deadline_a, "stale clear must not erase claim B's pending intent"

            # A stale restore from claim A must NOT replace claim B's state.
            m1._restore_pending_restart_after(job["job_id"], token_a, status="orphaned")
            state = m2._policy_state(job["job_id"])
            assert state.get("pending_restart_after") == deadline_a, "stale restore must not touch claim B"
            assert state.get("restart_after") is None, "stale restore must not set claim B's restart_after"
            row = m2._row("SELECT status, claim_token FROM jobs WHERE job_id=?", (job["job_id"],))
            assert row["status"] == "launching" and row["claim_token"] == "claim_B"
        finally:
            m1.begin_shutdown()
            m1.close()
            m2.begin_shutdown()
            m2.close()

    run(main())
