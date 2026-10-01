import asyncio
import json
import os
import sqlite3
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

import pytest

from vanth.opencode_bridge import OpenCodeSessionNotFound
from vanth.server import JobManager, now_iso


import shellcmd


def cmd(code: str) -> str:
    return shellcmd.join([sys.executable, "-c", code])


def wait_for_delivery(manager: JobManager, job_id: str, status: str, timeout: float = 20):
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


def test_quick_job_automatic_delivery_retry_succeeds(tmp_path, request):
    manager = JobManager(tmp_path / "state")
    request.addfinalizer(manager.close)
    calls = tmp_path / "calls.txt"
    delivery_command = [
        sys.executable,
        "-c",
        (
            "from pathlib import Path; import sys; "
            "p=Path(sys.argv[1]); calls=p.read_text() if p.exists() else ''; "
            "p.write_text(calls+'x'); sys.exit(7 if not calls else 0)"
        ),
        str(calls),
    ]
    started = asyncio.run(
        manager.start(
            cmd("import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint'}), flush=True)"),
            wake_targets=[
                {
                    "type": "local_command",
                    "events": ["checkpoint"],
                    "command": delivery_command,
                    "max_attempts": 2,
                    "retry_delay_seconds": 1,
                }
            ],
        )
    )

    delivery = wait_for_delivery(manager, started["job_id"], "delivered")

    assert delivery is not None and delivery["status"] == "delivered"
    assert delivery["attempts"] == 2
    assert calls.read_text() == "xx"


def test_concurrent_delivery_retries_dispatch_once(tmp_path, request):
    manager = JobManager(tmp_path / "state")
    request.addfinalizer(manager.close)
    ready = tmp_path / "ready"
    release = tmp_path / "release"
    calls = tmp_path / "calls.txt"
    delivery_command = [
        sys.executable,
        "-c",
        (
            "from pathlib import Path; import sys,time; "
            "ready=Path(sys.argv[1]); calls=Path(sys.argv[2]); release=Path(sys.argv[3]); "
            "calls.open('a').write('success\\n' if ready.exists() else 'failed\\n'); "
            "end=time.monotonic()+2; "
            "ready.exists() and next((None for _ in iter(int,1) if time.sleep(.01) or release.exists() or time.monotonic()>=end),None); "
            "sys.exit(0 if ready.exists() else 7)"
        ),
        str(ready),
        str(calls),
        str(release),
    ]
    started = asyncio.run(
        manager.start(
            cmd("import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint'}), flush=True)"),
            wake_targets=[{"type": "local_command", "events": ["checkpoint"], "command": delivery_command}],
        )
    )
    failed = wait_for_delivery(manager, started["job_id"], "failed")
    assert failed is not None
    ready.write_text("ok")

    barrier = threading.Barrier(3)

    def retry() -> None:
        barrier.wait()
        try:
            manager.retry_delivery(failed["delivery_id"])
        except ValueError:
            pass

    threads = [threading.Thread(target=retry) for _ in range(2)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join()

    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and "success" not in calls.read_text():
        time.sleep(0.01)
    time.sleep(0.3)
    release.write_text("go")
    delivery = wait_for_delivery(manager, started["job_id"], "delivered")
    time.sleep(0.3)

    assert delivery is not None and delivery["status"] == "delivered"
    assert delivery["attempts"] == 2
    assert calls.read_text().splitlines() == ["failed", "success"]


def test_retry_due_after_manager_restart_is_dispatched(tmp_path):
    home = tmp_path / "state"
    calls = tmp_path / "restart-calls.txt"
    delivery_command = [
        sys.executable,
        "-c",
        (
            "from pathlib import Path; import sys; "
            "p=Path(sys.argv[1]); calls=p.read_text() if p.exists() else ''; "
            "p.write_text(calls+'x'); sys.exit(7 if not calls else 0)"
        ),
        str(calls),
    ]
    manager = JobManager(home)
    started = asyncio.run(
        manager.start(
            cmd("import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint'}), flush=True)"),
            wake_targets=[
                {
                    "type": "local_command",
                    "events": ["checkpoint"],
                    "command": delivery_command,
                    "max_attempts": 2,
                    "retry_delay_seconds": 1,
                }
            ],
        )
    )
    retrying = wait_for_delivery(manager, started["job_id"], "retrying")
    # Assert the transient retry state was actually observed BEFORE the manager
    # is closed: otherwise the helper can return an already-delivered row and the
    # test would never exercise recovery across a restart.
    assert retrying is not None and retrying["status"] == "retrying"
    manager.close()

    restarted = JobManager(home)
    try:
        delivered = wait_for_delivery(restarted, started["job_id"], "delivered")
        assert delivered is not None and delivered["attempts"] == 2
        assert calls.read_text() == "xx"
    finally:
        restarted.close()


def test_notify_on_defaults_events_for_targets_without_events(tmp_path, request):
    manager = JobManager(tmp_path / "state")
    request.addfinalizer(manager.close)
    calls = tmp_path / "notify_calls.txt"
    delivery_command = [
        sys.executable,
        "-c",
        (
            "from pathlib import Path; import sys; "
            "p=Path(sys.argv[1]); p.write_text(p.read_text()+'x') if p.exists() else p.write_text('x')"
        ),
        str(calls),
    ]
    started = asyncio.run(
        manager.start(
            cmd("import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint'}), flush=True)"),
            notify_on=["checkpoint"],
            wake_targets=[{"type": "local_command", "command": delivery_command}],
        )
    )
    delivery = wait_for_delivery(manager, started["job_id"], "delivered")
    assert delivery is not None and delivery["status"] == "delivered"
    assert calls.read_text() == "x"


def test_explicit_target_events_override_notify_on(tmp_path, request):
    manager = JobManager(tmp_path / "state")
    request.addfinalizer(manager.close)
    calls = tmp_path / "override_calls.txt"
    delivery_command = [
        sys.executable,
        "-c",
        (
            "from pathlib import Path; import sys; "
            "p=Path(sys.argv[1]); p.write_text(p.read_text()+'x') if p.exists() else p.write_text('x')"
        ),
        str(calls),
    ]
    started = asyncio.run(
        manager.start(
            cmd("import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint'}), flush=True)"),
            notify_on=["completed"],
            wake_targets=[
                {
                    "type": "local_command",
                    "command": delivery_command,
                    "events": ["checkpoint"],
                }
            ],
        )
    )
    delivery = wait_for_delivery(manager, started["job_id"], "delivered")
    assert delivery is not None and delivery["status"] == "delivered"
    assert calls.read_text() == "x"


def test_notify_on_alone_without_targets_still_stores_value(tmp_path, request):
    manager = JobManager(tmp_path / "state")
    request.addfinalizer(manager.close)
    started = asyncio.run(
        manager.start(cmd("import time; time.sleep(1)"), notify_on=["completed"])
    )
    assert json.loads(
        manager._row("SELECT notify_on FROM jobs WHERE job_id=?", (started["job_id"],))["notify_on"]
    ) == ["completed"]


def test_retry_delivery_force_advances_retrying_delivery(tmp_path, request):
    manager = JobManager(tmp_path / "state")
    request.addfinalizer(manager.close)
    calls = tmp_path / "retry_calls.txt"
    delivery_command = [
        sys.executable,
        "-c",
        (
            "from pathlib import Path; import sys; "
            "p=Path(sys.argv[1]); calls=p.read_text() if p.exists() else ''; "
            "p.write_text(calls+'x'); sys.exit(7 if not calls else 0)"
        ),
        str(calls),
    ]
    started = asyncio.run(
        manager.start(
            cmd("import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint'}), flush=True)"),
            wake_targets=[
                {
                    "type": "local_command",
                    "events": ["checkpoint"],
                    "command": delivery_command,
                    "max_attempts": 2,
                    "retry_delay_seconds": 60,
                }
            ],
        )
    )
    retrying = wait_for_delivery(manager, started["job_id"], "retrying")
    assert retrying is not None
    assert retrying["next_attempt_at"] is not None
    manager.retry_delivery(retrying["delivery_id"])
    delivered = wait_for_delivery(manager, started["job_id"], "delivered")
    assert delivered is not None and delivered["status"] == "delivered"
    assert delivered["attempts"] == 2
    assert calls.read_text() == "xx"


def test_delivery_dispatch_respects_concurrency_cap(tmp_path, monkeypatch):
    monkeypatch.setenv("VANTH_DELIVERY_MAX_CONCURRENT", "2")

    def active_threads() -> int:
        with manager._delivery_threads_lock:
            return len(manager._delivery_threads)

    def drain() -> None:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and active_threads() > 0:
            time.sleep(0.05)

    manager = JobManager(tmp_path / "state", recover=False)
    started = asyncio.run(manager.start(cmd("import time; time.sleep(30)")))
    try:
        for _ in range(8):
            manager._insert_wake_targets(
                started["job_id"],
                [{"type": "local_command", "events": ["checkpoint"],
                  "command": [sys.executable, "-c", "import time; time.sleep(0.5)"], "timeout_seconds": 30}],
                now_iso(),
            )
        with manager.db_lock:
            manager.db.commit()
        manager._emit(started["job_id"], "checkpoint", message="burst")
        pending = manager.deliveries(started["job_id"], status="pending")["deliveries"]
        assert len(pending) == 8

        manager._dispatch_due_deliveries()
        assert active_threads() == 2
        drain()
        manager._dispatch_due_deliveries()
        assert active_threads() == 2
        drain()
        manager._dispatch_due_deliveries()
        assert active_threads() == 2
        drain()
        manager._dispatch_due_deliveries()
        assert active_threads() == 2
        drain()

        all_deliveries = manager.deliveries(started["job_id"])["deliveries"]
        assert len(all_deliveries) == 8
        assert all(delivery["status"] == "delivered" for delivery in all_deliveries)
    finally:
        manager.close()


def test_doctor_reports_dead_lettered_deliveries(tmp_path, request):
    manager = JobManager(tmp_path / "state")
    request.addfinalizer(manager.close)
    started = asyncio.run(
        manager.start(
            cmd("import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint'}), flush=True)"),
            wake_targets=[
                {
                    "type": "local_command",
                    "events": ["checkpoint"],
                    "command": [sys.executable, "-c", "import sys; sys.exit(7)"],
                    "max_attempts": 2,
                    "retry_delay_seconds": 1,
                }
            ],
        )
    )
    failed = wait_for_delivery(manager, started["job_id"], "failed")
    assert failed is not None
    assert failed["attempts"] == 2
    doctor = manager.doctor()
    assert doctor["dead_letter_count"] >= 1
    entry = next(item for item in doctor["dead_lettered"] if item["delivery_id"] == failed["delivery_id"])
    assert entry["attempts"] == 2


def test_stale_opencode_session_skips_retries(tmp_path, request, monkeypatch):
    import vanth.server as server_module

    def raise_not_found(payload):
        raise OpenCodeSessionNotFound("opencode session not found: ses_x")

    monkeypatch.setattr(server_module, "send_delivery_to_opencode", raise_not_found)
    manager = JobManager(tmp_path / "state")
    request.addfinalizer(manager.close)
    started = asyncio.run(
        manager.start(
            cmd("import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint'}), flush=True)"),
            wake_targets=[
                {
                    "type": "opencode_thread",
                    "session_id": "ses_x",
                    "attach": "http://127.0.0.1:4096",
                    "events": ["checkpoint"],
                    "max_attempts": 3,
                    "retry_delay_seconds": 1,
                }
            ],
        )
    )
    failed = wait_for_delivery(manager, started["job_id"], "failed")
    assert failed is not None
    assert failed["status"] == "failed"
    assert failed["attempts"] == 1
    assert "session not found" in failed["last_error"]


def _insert_delivery(manager, delivery_id, status, attempts, created_at):
    manager.db.execute(
        "INSERT INTO deliveries(delivery_id, event_id, target_id, job_id, target_type, status, attempts, "
        "payload_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (delivery_id, f"evt_{delivery_id}", f"tgt_{delivery_id}", "job_x", "opencode_thread", status, attempts,
         json.dumps({"target": {"session_id": "ses_x"}}), created_at),
    )
    manager.db.commit()


def _iso_ago(seconds):
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


def test_expire_stale_deliveries_fails_only_never_dispatched_old(tmp_path):
    manager = JobManager(tmp_path / "state", recover=False)
    try:
        _insert_delivery(manager, "del_old_never", "pending", 0, _iso_ago(7200))
        _insert_delivery(manager, "del_old_attempted", "pending", 1, _iso_ago(7200))
        _insert_delivery(manager, "del_fresh_never", "pending", 0, now_iso())
        manager.db.execute(
            "UPDATE deliveries SET claim_token='tok', claimed_at=?, lease_expires_at=?, claim_client_id='c1' "
            "WHERE delivery_id='del_old_never'",
            (now_iso(), now_iso()),
        )
        manager.db.commit()

        assert manager.expire_stale_deliveries(3600) == 1

        statuses = {
            row["delivery_id"]: row["status"]
            for row in manager.db.execute("SELECT delivery_id, status FROM deliveries").fetchall()
        }
        assert statuses["del_old_never"] == "failed"
        assert statuses["del_old_attempted"] == "pending"
        assert statuses["del_fresh_never"] == "pending"
        expired = manager.db.execute(
            "SELECT last_error, claim_token, claimed_at, lease_expires_at, claim_client_id "
            "FROM deliveries WHERE delivery_id='del_old_never'"
        ).fetchone()
        assert "expired" in expired["last_error"]
        assert expired["claim_token"] is None and expired["claimed_at"] is None
        assert expired["lease_expires_at"] is None and expired["claim_client_id"] is None
    finally:
        manager.close()


def test_expire_stale_deliveries_handles_retrying_and_noop_ttl(tmp_path):
    manager = JobManager(tmp_path / "state", recover=False)
    try:
        _insert_delivery(manager, "del_retrying", "retrying", 0, _iso_ago(86400))
        _insert_delivery(manager, "del_recent", "pending", 0, now_iso())

        assert manager.expire_stale_deliveries(0) == 0  # ttl<=0 is a no-op

        assert manager.expire_stale_deliveries(3600) == 1
        statuses = {
            row["delivery_id"]: row["status"]
            for row in manager.db.execute("SELECT delivery_id, status FROM deliveries").fetchall()
        }
        assert statuses["del_retrying"] == "failed"
        assert statuses["del_recent"] == "pending"
    finally:
        manager.close()


def test_expired_delivery_is_a_dead_letter_and_retryable(tmp_path):
    manager = JobManager(tmp_path / "state", recover=False)
    try:
        _insert_delivery(manager, "del_expired", "pending", 0, _iso_ago(86400))
        assert manager._dead_letter_count() == 0

        assert manager.expire_stale_deliveries(3600) == 1

        # An expired wake is surfaced as a dead letter (attempts stays 0).
        assert manager._dead_letter_count() == 1
        doctor = manager.doctor()
        assert any(item["delivery_id"] == "del_expired" for item in doctor["dead_lettered"])

        # Manual retry must re-arm it and survive the next expiry sweep, while
        # staying covered by the TTL (age refreshed, attempts still 0).
        retried = manager.retry_delivery("del_expired")
        assert retried["status"] == "retrying"
        assert retried["attempts"] == 0
        assert manager.expire_stale_deliveries(3600) == 0
        row = manager.db.execute(
            "SELECT status, created_at FROM deliveries WHERE delivery_id='del_expired'"
        ).fetchone()
        assert row["status"] == "retrying"
        assert row["created_at"] > _iso_ago(60)
    finally:
        manager.close()


def test_doctor_flags_stale_pending_delivery(tmp_path):
    manager = JobManager(tmp_path / "state", recover=False)
    try:
        # recover=False leaves dispatcher_thread None; give it a live thread so
        # the stale-delivery warning is the only thing that can flip ok.
        manager.dispatcher_thread = threading.Thread(target=manager.dispatcher_stop.wait, daemon=True)
        manager.dispatcher_thread.start()
        assert manager.doctor()["ok"] is True

        _insert_delivery(manager, "del_stale", "pending", 0, _iso_ago(86400))

        doctor = manager.doctor()
        assert doctor["stale_pending_deliveries"] == 1
        assert any(w["type"] == "stale_pending_deliveries" for w in doctor["warnings"])
        assert doctor["ok"] is False
    finally:
        manager.close()


def test_relay_poll_liveness_write_retries_on_lock(tmp_path, monkeypatch):
    manager = JobManager(tmp_path / "state", recover=False)
    try:
        manager.relay_register(
            client_id="c1",
            client_type="opencode_thread",
            destinations=[{"client_type": "opencode_thread", "session_id": "ses_1"}],
        )
        # Make the stored last_poll_at stale so the heartbeat-gated write runs.
        manager.db.execute(
            "UPDATE relay_subscriptions SET last_poll_at=? WHERE client_id='c1'", (_iso_ago(60),)
        )
        manager.db.commit()
        original = manager._touch_relay_subscription
        calls = [0]

        def flaky(client_id):
            if calls[0] == 0:
                calls[0] += 1
                raise sqlite3.OperationalError("database is locked")
            return original(client_id)

        monkeypatch.setattr(manager, "_touch_relay_subscription", flaky)

        assert manager.relay_poll("c1", timeout_seconds=1) == []
        assert calls[0] == 1  # retried once past the injected lock
        assert manager.sqlite_contentions == 1
    finally:
        manager.close()


def test_relay_poll_skips_liveness_write_when_fresh(tmp_path, monkeypatch):
    manager = JobManager(tmp_path / "state", recover=False)
    try:
        manager.relay_register(
            client_id="c1",
            client_type="opencode_thread",
            destinations=[{"client_type": "opencode_thread", "session_id": "ses_1"}],
        )
        calls = [0]
        monkeypatch.setattr(manager, "_touch_relay_subscription", lambda client_id: calls.__setitem__(0, calls[0] + 1))

        assert manager.relay_poll("c1", timeout_seconds=1) == []
        assert calls[0] == 0  # fresh subscription: no write at all
    finally:
        manager.close()


def test_relay_heartbeat_seconds_is_clamped_and_validated(tmp_path, monkeypatch):
    from vanth.server import _MAX_RELAY_POLL_HEARTBEAT

    manager = JobManager(tmp_path / "state", recover=False)
    try:
        monkeypatch.setenv("VANTH_RELAY_POLL_HEARTBEAT_SECONDS", "10")
        assert manager._relay_heartbeat_seconds() == 10.0
        monkeypatch.setenv("VANTH_RELAY_POLL_HEARTBEAT_SECONDS", "100")
        assert manager._relay_heartbeat_seconds() == _MAX_RELAY_POLL_HEARTBEAT
        monkeypatch.setenv("VANTH_RELAY_POLL_HEARTBEAT_SECONDS", "nan")
        assert manager._relay_heartbeat_seconds() == 5.0
        monkeypatch.setenv("VANTH_RELAY_POLL_HEARTBEAT_SECONDS", "0")
        assert manager._relay_heartbeat_seconds() == 5.0
    finally:
        manager.close()


class _FlakyConn:
    """Proxy that injects one 'database is locked' inside the real transaction."""

    def __init__(self, real, trigger):
        self._real = real
        self._trigger = trigger
        self.raised = False

    def execute(self, sql, *args, **kwargs):
        if isinstance(sql, str) and sql.strip().upper().startswith(self._trigger) and not self.raised:
            self.raised = True
            raise sqlite3.OperationalError("database is locked")
        return self._real.execute(sql, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._real, name)


def test_claim_delivery_rolls_back_on_real_lock(tmp_path):
    manager = JobManager(tmp_path / "state", recover=False)
    real = manager.db
    try:
        _insert_delivery(manager, "del_c", "pending", 0, now_iso())
        manager.db = _FlakyConn(real, "UPDATE DELIVERIES")

        claimed = manager._claim_delivery("del_c")
        assert claimed is not None and claimed["status"] == "dispatching"
        assert manager.db.raised is True
        assert manager.sqlite_contentions == 1

        manager.db = real
        # The failed transaction rolled back, so no duplicate attempt rows.
        assert real.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0] == 1
    finally:
        manager.db = real
        manager.close()


def test_claim_delivery_retries_on_lock(tmp_path, monkeypatch):
    manager = JobManager(tmp_path / "state", recover=False)
    try:
        _insert_delivery(manager, "del_c", "pending", 0, now_iso())
        original = manager._claim_delivery_locked
        calls = [0]

        def flaky(delivery_id, *, claim_client_id=None):
            if calls[0] == 0:
                calls[0] += 1
                raise sqlite3.OperationalError("database is locked")
            return original(delivery_id, claim_client_id=claim_client_id)

        monkeypatch.setattr(manager, "_claim_delivery_locked", flaky)

        claimed = manager._claim_delivery("del_c")
        assert claimed is not None
        assert claimed["status"] == "dispatching"
        assert calls[0] == 1
        assert manager.sqlite_contentions == 1
    finally:
        manager.close()


def test_retry_locked_reraises_non_lock_errors(tmp_path):
    manager = JobManager(tmp_path / "state", recover=False)
    try:
        def boom():
            raise sqlite3.OperationalError("no such table: missing")

        with pytest.raises(sqlite3.OperationalError, match="no such table"):
            manager._retry_locked(boom)
        assert manager.sqlite_contentions == 0
    finally:
        manager.close()
