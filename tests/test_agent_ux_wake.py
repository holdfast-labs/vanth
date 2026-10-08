import asyncio
import json
import sys
import threading

from vanth.server import JobManager, now_iso

import shellcmd


def cmd(code: str) -> str:
    return shellcmd.join([sys.executable, "-c", code])


def start_job(manager, **kwargs):
    return manager.start(cmd("import time; time.sleep(0.2)"), **kwargs)


def test_notify_on_without_targets_is_stored_but_warns(tmp_path):
    """notify_on alone notifies nobody (it only defaults a wake target's events),
    so it is kept for compatibility but the start response warns."""
    manager = JobManager(tmp_path / "state")
    try:
        result = start_job(manager, notify_on=["completed"])
        assert json.loads(
            manager._row("SELECT notify_on FROM jobs WHERE job_id=?", (result["job_id"],))["notify_on"]
        ) == ["completed"]
        assert any("notify_on has no effect without wake_targets" in w for w in result["warnings"])
        # With a wake target it is NOT warned about and still defaults events.
        with_target = start_job(
            manager,
            notify_on=["completed"],
            wake_targets=[{"type": "local_command", "command": [sys.executable, "-c", "pass"]}],
        )
        assert with_target["wake_targets"][0]["events"] == ["completed"]
        assert not with_target.get("warnings")
    finally:
        manager.close()


def test_wake_start_info_reports_identity_and_non_relay(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        result = start_job(
            manager,
            wake_targets=[{"type": "opencode_thread", "session_id": "ses_123", "events": ["completed"]}],
        )
        assert result["wake_targets"] == [{"type": "opencode_thread", "events": ["completed"], "session_id": "ses_123"}]
        assert result["wake_addressable"] is True
        local = start_job(manager, wake_targets=[{"type": "local_command", "events": ["completed"], "command": [sys.executable, "-c", "pass"]}])
        assert local["wake_addressable"] is None
    finally:
        manager.close()


def test_reconcile_invalid_wake_targets(tmp_path):
    home = tmp_path / "state"
    manager = JobManager(home)
    result = start_job(manager, wake_targets=[{"type": "opencode_thread", "session_id": "ses_valid", "events": ["completed"]}])
    job_id = result["job_id"]
    manager.db.execute(
        "INSERT INTO wake_targets(target_id, job_id, type, events_json, config_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        ("target_bad", job_id, "opencode_thread", '["completed"]', json.dumps({"session_id": "opencode-1234-abcdef"}), "now"),
    )
    manager.db.execute(
        "INSERT INTO deliveries(delivery_id, event_id, target_id, job_id, target_type, status, payload_json, created_at) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)",
        ("del_bad", "evt_bad", "target_bad", job_id, "opencode_thread", "{}", "now"),
    )
    manager.db.commit()
    manager.close()

    recovered = JobManager(home)
    try:
        assert recovered.db.execute("SELECT 1 FROM wake_targets WHERE target_id='target_bad'").fetchone() is None
        delivery = recovered.db.execute("SELECT status, last_error FROM deliveries WHERE delivery_id='del_bad'").fetchone()
        assert delivery["status"] == "failed"
        assert "relay client id" in delivery["last_error"]
        assert recovered.db.execute("SELECT 1 FROM wake_targets WHERE job_id=? AND config_json LIKE '%ses_valid%'", (job_id,)).fetchone()
    finally:
        recovered.close()


def test_doctor_flags_undeliverable_wakes_as_hard(tmp_path):
    manager = JobManager(tmp_path / "state", recover=False)
    try:
        # recover=False leaves dispatcher_thread None, which alone makes ok
        # False; give it a live thread (that exits on close) so the
        # undeliverable-wake warning is the only thing that can flip ok.
        manager.dispatcher_thread = threading.Thread(
            target=manager.dispatcher_stop.wait, daemon=True
        )
        manager.dispatcher_thread.start()
        assert manager.doctor()["ok"] is True
        job_id = start_job(manager)["job_id"]
        manager.db.execute(
            "INSERT INTO wake_targets(target_id, job_id, type, events_json, config_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            ("target_doctor", job_id, "opencode_thread", '["completed"]', json.dumps({"session_id": "opencode-1234-abcdef"}), "now"),
        )
        manager.db.commit()
        doctor = manager.doctor()
        assert doctor["pending_deliveries"] == 0
        assert doctor["undeliverable_wakes"] == 1
        assert any(w["type"] == "undeliverable_wake_targets" for w in doctor["warnings"])
        assert doctor["ok"] is False
    finally:
        manager.close()


def test_mcp_job_start_wake_me_payload(monkeypatch):
    """The MCP shorthand must post a VALID target: a wake target with neither
    events nor notify_on is rejected as empty, so the shorthand has to supply
    the default events itself (mirroring `vanth start --wake-me`)."""
    import vanth.server as server_mod

    captured: dict = {}

    class FakeClient:
        def post(self, path, payload):
            captured["path"] = path
            captured["payload"] = payload
            return {"job_id": "job_x", "status": "running"}

        def confirm_local_start(self, result):
            return result

    monkeypatch.setattr(server_mod, "get_client", lambda: FakeClient())
    server_mod.job_start(command="echo hi", wake_me=True)
    assert captured["path"] == "/jobs"
    target = captured["payload"]["wake_targets"][0]
    assert target["type"] == "opencode_thread"
    assert target["events"] == ["completed", "failed", "timeout", "cancelled", "orphaned"]
    assert target["cwd"]  # pins relay resolution to the caller's directory
    server_mod.job_start(command="echo hi", wake_me=True, cwd="D:\\proj")
    assert captured["payload"]["wake_targets"][0]["cwd"] == "D:\\proj"
    # Explicit wake_targets win over the shorthand.
    explicit = [{"type": "local_command", "events": ["checkpoint"], "command": ["echo", "hi"]}]
    server_mod.job_start(command="echo hi", wake_me=True, wake_targets=explicit)
    assert captured["payload"]["wake_targets"] == explicit


def test_start_extras_recommends_a_wake_when_none(tmp_path):
    manager = JobManager(tmp_path / "state", recover=False)
    try:
        extras = manager._start_extras(None, None)
        assert "wake_recommended" in extras
        with_target = manager._start_extras(
            [{"type": "opencode_thread", "session_id": "ses_x", "events": ["completed"]}], None
        )
        assert "wake_recommended" not in with_target
    finally:
        manager.close()


def test_recent_jobs_without_wake_counts_polling_only_jobs(tmp_path):
    manager = JobManager(tmp_path / "state", recover=False)
    try:
        stamp = now_iso()
        for job_id in ("job_a", "job_b"):
            manager.db.execute(
                "INSERT INTO jobs(job_id,command,status,created_at,updated_at,stdout_path,stderr_path,events_path) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (job_id, "c", "completed", stamp, stamp, "o", "e", "ev"),
            )
        manager.db.execute(
            "INSERT INTO wake_targets(target_id,job_id,type,events_json,config_json,created_at) "
            "VALUES ('t1','job_a','opencode_thread','[\"completed\"]','{}',?)",
            (stamp,),
        )
        manager.db.commit()
        assert manager._recent_jobs_without_wake() == 1
    finally:
        manager.close()


def test_default_wake_me_sets_wake_default_flag(monkeypatch):
    import vanth.server as server_mod

    class FakeClient:
        def __init__(self):
            self.payload = None

        def post(self, path, payload):
            self.payload = payload
            return {"job_id": "job_x", "status": "running"}

        def confirm_local_start(self, result):
            return result

    client = FakeClient()
    monkeypatch.setattr(server_mod, "get_client", lambda: client)
    monkeypatch.delenv("VANTH_DEFAULT_WAKE_ME", raising=False)

    # Default + long local job in the caller's directory: daemon asked for a wake.
    server_mod.job_start(command="sleep 1", timeout_seconds=3600)
    assert client.payload["wake_default"] is True
    assert not client.payload["wake_targets"]

    # Short job: not worth a wake.
    client.payload = None
    server_mod.job_start(command="echo hi", timeout_seconds=10)
    assert client.payload["wake_default"] is False

    # Different working directory: relay resolution could hit an unrelated session.
    client.payload = None
    server_mod.job_start(command="sleep 1", timeout_seconds=3600, cwd=__import__("tempfile").mkdtemp())
    assert client.payload["wake_default"] is False

    # Explicit wake_me builds a concrete target (not the default flag).
    client.payload = None
    server_mod.job_start(command="sleep 1", timeout_seconds=3600, wake_me=True)
    assert client.payload["wake_default"] is False
    assert len(client.payload["wake_targets"]) == 1

    # Opt out.
    monkeypatch.setenv("VANTH_DEFAULT_WAKE_ME", "0")
    client.payload = None
    server_mod.job_start(command="sleep 1", timeout_seconds=3600)
    assert client.payload["wake_default"] is False


def test_wake_default_is_best_effort_daemon_side(tmp_path):
    import os

    manager = JobManager(tmp_path / "state", recover=False)
    try:
        # No live relay: the default wake is dropped, but the start still succeeds.
        result = manager.start(cmd("echo hi"), wake_default=True, timeout_seconds=3600)
        assert result["job_id"]
        assert manager._wake_targets_for_job(result["job_id"]) == []

        # With a live relay for the directory, the default wake attaches.
        manager.relay_register(
            client_id="c1",
            client_type="opencode_thread",
            destinations=[{"client_type": "opencode_thread", "session_id": "ses_x", "directory": os.getcwd()}],
        )
        result2 = manager.start(cmd("echo hi"), wake_default=True, timeout_seconds=3600)
        targets = manager._wake_targets_for_job(result2["job_id"])
        assert targets and targets[0].get("session_id") == "ses_x"

        # A second session in the same directory is ambiguous: best-effort skips
        # rather than waking the wrong sibling session.
        manager.relay_register(
            client_id="c2",
            client_type="opencode_thread",
            destinations=[{"client_type": "opencode_thread", "session_id": "ses_y", "directory": os.getcwd()}],
        )
        result3 = manager.start(cmd("echo hi"), wake_default=True, timeout_seconds=3600)
        assert manager._wake_targets_for_job(result3["job_id"]) == []
    finally:
        manager.close()


def test_wake_default_excluded_from_idempotency_hash(tmp_path):
    manager = JobManager(tmp_path / "state", recover=False)
    try:
        first = manager.start(cmd("echo hi"), idempotency_key="key-abcdefgh", timeout_seconds=3600)
        replay = manager.start(cmd("echo hi"), idempotency_key="key-abcdefgh", timeout_seconds=3600, wake_default=True
        )
        assert replay["job_id"] == first["job_id"]
        assert replay.get("idempotent_replay") is True
    finally:
        manager.close()


def test_job_start_and_wait_opts_out_of_default_wake(monkeypatch):
    import vanth.server as server_mod

    captured = {}

    def fake_job_start(**kwargs):
        captured.update(kwargs)
        return {"job_id": "job_x", "status": "running"}

    monkeypatch.setattr(server_mod, "job_start", fake_job_start)
    monkeypatch.setattr(server_mod, "job_run_summary", lambda *a, **k: {"status": "running"})
    monkeypatch.setattr(
        server_mod, "get_client", lambda: type("C", (), {"post": lambda *a, **k: {"result": "timeout", "status": "running"}})()
    )

    asyncio.run(server_mod.job_start_and_wait("sleep", wait_timeout_seconds=5))

    assert captured["wake_targets"] == []

