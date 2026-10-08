import sys

import pytest

import shellcmd

from vanth.server import JobManager


def cmd(code: str) -> str:
    return shellcmd.join([sys.executable, "-c", code])


def test_remote_binding_registers_composite_id_and_is_idempotent(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        targets = [{"type": "local_command", "events": ["completed"], "command": cmd("pass")}]
        first = manager.register_remote_wake_targets("host-a", "job_same", targets)
        second = manager.register_remote_wake_targets("host-a", "job_same", targets)
        assert second == first
        assert manager.db.execute("SELECT COUNT(*) FROM wake_targets").fetchone()[0] == 1
        row = manager.db.execute("SELECT job_id, remote_id FROM wake_targets").fetchone()
        assert row["job_id"] == "remote:host-a:job_same"
        assert row["job_id"] != "job_same"
        assert row["remote_id"] == "host-a"
        assert manager.remote_wake_bindings() == [
            {
                "remote_id": "host-a",
                "remote_job_id": "job_same",
                "binding_id": "remote:host-a:job_same",
                "target_id": first[0],
                "events": ["completed"],
            }
        ]
    finally:
        manager.close()


def test_remote_terminal_enqueues_once_and_removes_binding(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        target_id = manager.register_remote_wake_targets(
            "host-a", "job_remote", [{"type": "local_command", "events": ["completed"], "command": cmd("pass")}]
        )[0]
        event = manager.emit_remote_terminal("host-a", "job_remote", "completed", exit_code=0)
        assert event and event["job_id"] == "remote:host-a:job_remote"
        deliveries = manager.db.execute("SELECT * FROM deliveries WHERE target_id=?", (target_id,)).fetchall()
        assert len(deliveries) == 1
        assert manager.emit_remote_terminal("host-a", "job_remote", "completed", exit_code=0) is None
        assert manager.db.execute("SELECT COUNT(*) FROM deliveries WHERE target_id=?", (target_id,)).fetchone()[0] == 1
        assert manager.db.execute("SELECT COUNT(*) FROM wake_targets WHERE target_id=?", (target_id,)).fetchone()[0] == 0
    finally:
        manager.close()


def test_non_terminal_remote_status_does_nothing(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        manager.register_remote_wake_targets(
            "host-a", "job_remote", [{"type": "local_command", "events": ["completed"], "command": cmd("pass")}]
        )
        assert manager.emit_remote_terminal("host-a", "job_remote", "running") is None
        assert manager.db.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0] == 0
    finally:
        manager.close()


def test_composite_binding_is_safe_from_local_job_collision(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        local_job_id = manager.start(cmd("pass"))['job_id']
        target_id = manager.register_remote_wake_targets(
            "host-a", local_job_id, [{"type": "local_command", "events": ["completed"], "command": cmd("pass")}]
        )[0]
        manager._emit(local_job_id, "completed", source="server")
        assert manager.db.execute("SELECT COUNT(*) FROM deliveries WHERE target_id=?", (target_id,)).fetchone()[0] == 0
    finally:
        manager.close()


def test_wake_targets_has_remote_id_column(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        columns = {row["name"] for row in manager.db.execute("PRAGMA table_info(wake_targets)")}
        assert "remote_id" in columns
    finally:
        manager.close()


def test_cleanup_prunes_settled_remote_rows_but_not_pending(tmp_path):
    """Remote-wake rows have no `jobs` row, so the per-job cleanup never reaches
    them; the dedicated prune must drop the settled ones and keep a pending
    delivery (a wake still owed)."""
    manager = JobManager(tmp_path / "state")
    try:
        manager.register_remote_wake_targets(
            "host-a", "job_remote", [{"type": "local_command", "events": ["completed"], "command": cmd("pass")}]
        )
        manager.emit_remote_terminal("host-a", "job_remote", "completed", exit_code=0)
        binding_id = "remote:host-a:job_remote"
        assert manager.db.execute("SELECT COUNT(*) FROM events WHERE job_id=?", (binding_id,)).fetchone()[0] == 1

        manager.db.execute(
            "INSERT INTO deliveries(delivery_id, event_id, target_id, job_id, target_type, status, payload_json, created_at) "
            "VALUES ('del_pending','evt_x','t_pending','remote:host-a:job_pending','local_command','pending','{}','2000-01-01T00:00:00Z')"
        )
        manager.db.commit()

        manager.cleanup(older_than_seconds=0, dry_run=False)
        assert manager.db.execute("SELECT COUNT(*) FROM events WHERE job_id=?", (binding_id,)).fetchone()[0] == 0
        assert manager.db.execute("SELECT COUNT(*) FROM deliveries WHERE delivery_id='del_pending'").fetchone()[0] == 1
    finally:
        manager.close()


def test_remote_binding_rejects_colon_in_ids(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        with pytest.raises(ValueError, match="must not contain"):
            manager.register_remote_wake_targets("a:b", "job_x", [{"type": "local_command", "events": ["completed"], "command": cmd("pass")}])
        with pytest.raises(ValueError, match="must not contain"):
            manager.register_remote_wake_targets("host-a", "a:b", [{"type": "local_command", "events": ["completed"], "command": cmd("pass")}])
    finally:
        manager.close()


def test_remote_binding_accepts_local_and_explicit_opencode_targets(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        ids = manager.register_remote_wake_targets(
            "host-a",
            "job_remote",
            [
                {"type": "local_command", "events": ["completed"], "command": cmd("pass")},
                {"type": "opencode_thread", "events": ["completed"], "session_id": "ses_test"},
            ],
        )
        assert len(ids) == 2
    finally:
        manager.close()
