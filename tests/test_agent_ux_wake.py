import asyncio
import json
import sys
import threading

from vanth.server import JobManager

import shellcmd


def cmd(code: str) -> str:
    return shellcmd.join([sys.executable, "-c", code])


def start_job(manager, **kwargs):
    return asyncio.run(manager.start(cmd("import time; time.sleep(0.2)"), **kwargs))


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
