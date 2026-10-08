"""OpenCode plugin relay: a plain TUI session can be woken without an attach URL.

A TUI OpenCode binds no TCP port and injects no session id into MCP children, so
the historical ``opencode run --attach`` transport (review P0-3) could never
reach the session the user is actually looking at. The wake is instead delivered
by an in-process OpenCode plugin that long-polls the same client relay protocol
as Codex Desktop (``/relay/register|poll|ack``) and injects the prompt through
its own in-process client.

These tests exercise the daemon side of that contract: relay registration for
``opencode_thread``, destination matching, dispatch routing (never spawn the
isolated CLI), session resolution from a registered directory, and liveness
reporting.
"""

import asyncio
import json
import sys
import time

import pytest

from vanth import server as server_mod
from vanth.server import JobManager, validate_wake_targets

import shellcmd


SESSION = "ses_plugin_relay_test"
OTHER_SESSION = "ses_other"
CLIENT = "client_plugin"


def cmd(code: str) -> str:
    return shellcmd.join([sys.executable, "-c", code])


def register(
    manager: JobManager,
    *,
    client_id: str = CLIENT,
    client_type: str = "opencode_thread",
    session_id: str = SESSION,
    directory=None,
) -> dict:
    return manager.relay_register(
        client_id=client_id,
        client_type=client_type,
        destinations=[{"client_type": client_type, "session_id": session_id, "directory": directory}],
    )


def start_job(manager: JobManager, code: str, **kwargs) -> str:
    return asyncio.run(manager.start(cmd(code), **kwargs))["job_id"]


def wait_completed(manager: JobManager, job_id: str) -> None:
    asyncio.run(manager.wait(job_id, ["completed"], timeout_seconds=15))


def relay_deliveries(manager: JobManager, job_id: str) -> list[dict]:
    return [d for d in manager.deliveries(job_id)["deliveries"] if d["target_type"] == "opencode_thread"]


def wait_relay_delivery(manager: JobManager, job_id: str, timeout: float = 10.0) -> list[dict]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        deliveries = relay_deliveries(manager, job_id)
        if deliveries:
            return deliveries
        time.sleep(0.05)
    raise AssertionError(f"no opencode_thread delivery enqueued for job {job_id}")


def stored_session(manager: JobManager, job_id: str) -> str | None:
    row = manager.db.execute(
        "SELECT config_json FROM wake_targets WHERE job_id=? AND type='opencode_thread'",
        (job_id,),
    ).fetchone()
    return json.loads(row["config_json"] or "{}").get("session_id") if row else None


def test_relay_register_accepts_opencode_thread(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        result = register(manager, directory=str(tmp_path))
        assert result["result"] == "ok"
        assert result["client_type"] == "opencode_thread"
    finally:
        manager.close()


def test_relay_register_rejects_unknown_client_type(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        with pytest.raises(ValueError, match="unsupported relay client type"):
            manager.relay_register(client_id="c", client_type="local_command", destinations=[{}])
        with pytest.raises(ValueError, match="list of objects"):
            manager.relay_register(client_id="c", client_type="opencode_thread", destinations=["nope"])
    finally:
        manager.close()


def test_validate_wake_targets_no_longer_requires_attach():
    """A TUI session has no server URL, so attach must be optional while a bad
    attach is still rejected."""
    validate_wake_targets([{"type": "opencode_thread", "events": ["completed"], "session_id": "s"}])
    with pytest.raises(ValueError, match="attach must be a non-empty string"):
        validate_wake_targets(
            [{"type": "opencode_thread", "events": ["completed"], "session_id": "s", "attach": ""}]
        )


def test_validate_wake_targets_requires_identity_for_codex_desktop_with_command():
    """codex_desktop is relay-delivered, so its command is ignored: the identity
    is still required or the delivery would never be claimed."""
    validate_wake_targets(
        [{"type": "codex_desktop", "events": ["completed"], "thread_id": "t", "command": ["echo"]}]
    )
    with pytest.raises(ValueError, match="codex_desktop target requires thread_id"):
        validate_wake_targets([{"type": "codex_desktop", "events": ["completed"], "command": ["echo"]}])


def test_wake_without_attach_is_never_spawned_locally(monkeypatch, tmp_path):
    """The daemon must not fall back to `opencode run --session`, which writes to
    a backend the live TUI never sees; the delivery waits for the plugin relay."""
    spawned = []

    def boom(payload):
        spawned.append(payload)
        raise AssertionError("daemon spawned opencode for a relay-delivered target")

    monkeypatch.setattr(server_mod, "send_delivery_to_opencode", boom)
    manager = JobManager(tmp_path / "state")
    try:
        register(manager, directory=str(tmp_path))
        job_id = start_job(
            manager,
            "print('relay')",
            cwd=str(tmp_path),
            wake_targets=[{"type": "opencode_thread", "events": ["completed"]}],
        )
        wait_completed(manager, job_id)
        deliveries = wait_relay_delivery(manager, job_id)
        time.sleep(0.5)  # give the dispatcher time to (wrongly) claim it
        assert spawned == []
        assert all(d["status"] in {"pending", "retrying", "dispatching"} for d in deliveries)
    finally:
        manager.close()


def test_relay_poll_delivers_opencode_wake_and_acks(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        register(manager, directory=str(tmp_path))
        job_id = start_job(
            manager,
            "print('relay ok')",
            cwd=str(tmp_path),
            wake_targets=[{"type": "opencode_thread", "events": ["completed"]}],
        )
        wait_completed(manager, job_id)
        wait_relay_delivery(manager, job_id)
        claimed = manager.relay_poll(CLIENT, timeout_seconds=2)
        assert len(claimed) == 1
        delivery = claimed[0]
        assert delivery["target_type"] == "opencode_thread"
        assert delivery["payload"]["target"]["session_id"] == SESSION
        assert "vanth event" in delivery["payload"]["prompt"]
        assert delivery["lease_token"]
        manager.relay_ack(CLIENT, delivery["delivery_id"], "delivered", lease_token=delivery["lease_token"])
        assert manager.relay_poll(CLIENT, timeout_seconds=1) == []
    finally:
        manager.close()


def test_relay_poll_ignores_other_sessions(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        register(manager, session_id=SESSION, directory=str(tmp_path))
        job_id = start_job(
            manager,
            "print('other')",
            cwd=str(tmp_path),
            wake_targets=[{"type": "opencode_thread", "events": ["completed"], "session_id": OTHER_SESSION}],
        )
        wait_completed(manager, job_id)
        wait_relay_delivery(manager, job_id)
        assert manager.relay_poll(CLIENT, timeout_seconds=1) == []
        # ... and the other session's own relay still gets it.
        register(manager, client_id="client_other", session_id=OTHER_SESSION, directory=str(tmp_path))
        claimed = manager.relay_poll("client_other", timeout_seconds=2)
        assert [d["payload"]["target"]["session_id"] for d in claimed] == [OTHER_SESSION]
    finally:
        manager.close()


def test_codex_relay_does_not_receive_opencode_deliveries(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        manager.relay_register(
            client_id="client_desktop",
            client_type="codex_desktop",
            destinations=[{"client_type": "codex_desktop", "thread_id": "thread_x"}],
        )
        register(manager, directory=str(tmp_path))  # opencode relay
        job_id = start_job(
            manager,
            "print('mixed')",
            cwd=str(tmp_path),
            wake_targets=[{"type": "opencode_thread", "events": ["completed"]}],
        )
        wait_completed(manager, job_id)
        wait_relay_delivery(manager, job_id)
        assert manager.relay_poll("client_desktop", timeout_seconds=1) == []
        assert len(manager.relay_poll(CLIENT, timeout_seconds=2)) == 1
    finally:
        manager.close()


def test_session_id_resolved_from_registered_directory(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        project = tmp_path / "proj_a"
        project.mkdir()
        elsewhere = tmp_path / "proj_b"
        elsewhere.mkdir()
        register(manager, directory=str(project))
        job_id = start_job(
            manager,
            "print('resolved')",
            cwd=str(project),
            wake_targets=[{"type": "opencode_thread", "events": ["completed"]}],
        )
        assert stored_session(manager, job_id) == SESSION
        # A different project cannot borrow that relay: fail early and actionably.
        with pytest.raises(ValueError, match="requires session_id"):
            start_job(
                manager,
                "print('unresolved')",
                cwd=str(elsewhere),
                wake_targets=[{"type": "opencode_thread", "events": ["completed"]}],
            )
    finally:
        manager.close()


def test_explicit_session_id_wins_over_registered_directory(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        register(manager, directory=str(tmp_path))
        job_id = start_job(
            manager,
            "print('explicit')",
            cwd=str(tmp_path),
            wake_targets=[
                {"type": "opencode_thread", "events": ["completed"], "session_id": OTHER_SESSION}
            ],
        )
        assert stored_session(manager, job_id) == OTHER_SESSION
    finally:
        manager.close()


def test_doctor_reports_relay_liveness(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        register(manager, directory=str(tmp_path))
        relays = [r for r in manager.doctor()["relays"] if r["client_type"] == "opencode_thread"]
        assert len(relays) == 1
        assert relays[0]["live"] is True
        assert relays[0]["destinations"][0]["session_id"] == SESSION
    finally:
        manager.close()


def test_relay_client_id_in_session_id_is_rejected(tmp_path):
    """`vanth doctor` prints the relay client id; copying it into session_id
    yields a delivery the relay never matches (pending forever, no error), so it
    must fail at target creation instead."""
    manager = JobManager(tmp_path / "state")
    try:
        register(manager, client_id="opencode-1234-abcd", session_id=SESSION, directory=str(tmp_path))
        with pytest.raises(ValueError, match="relay client id"):
            start_job(
                manager,
                "print('bad wake')",
                cwd=str(tmp_path),
                wake_targets=[
                    {"type": "opencode_thread", "events": ["completed"], "session_id": "opencode-1234-abcd"}
                ],
            )
        # A stale client id (its relay no longer registered) is caught by shape.
        with pytest.raises(ValueError, match="relay client id"):
            start_job(
                manager,
                "print('stale wake')",
                cwd=str(tmp_path),
                wake_targets=[
                    {"type": "opencode_thread", "events": ["completed"], "session_id": "opencode-9999-zzzz"}
                ],
            )
    finally:
        manager.close()


def test_thread_id_alias_is_canonicalized_and_delivered(tmp_path):
    """An `opencode_thread` target that names its id via the `thread_id` alias
    must be canonicalized to `session_id` (the key the relay SQL matches), not
    left as an alias that is silently never claimed."""
    manager = JobManager(tmp_path / "state")
    try:
        register(manager, directory=str(tmp_path))
        job_id = start_job(
            manager,
            "print('alias')",
            cwd=str(tmp_path),
            wake_targets=[{"type": "opencode_thread", "events": ["completed"], "thread_id": SESSION}],
        )
        assert stored_session(manager, job_id) == SESSION
        wait_completed(manager, job_id)
        wait_relay_delivery(manager, job_id)
        claimed = manager.relay_poll(CLIENT, timeout_seconds=2)
        assert [d["payload"]["target"]["session_id"] for d in claimed] == [SESSION]
    finally:
        manager.close()


def test_camelcase_destination_alias_is_pollable(tmp_path):
    """A relay that registers its destination under the legacy `sessionId` alias
    must still be offered its deliveries (identities are collected under every
    alias, not just the canonical one)."""
    manager = JobManager(tmp_path / "state")
    try:
        manager.relay_register(
            client_id=CLIENT,
            client_type="opencode_thread",
            destinations=[{"client_type": "opencode_thread", "sessionId": SESSION, "directory": str(tmp_path)}],
        )
        job_id = start_job(
            manager,
            "print('camel')",
            cwd=str(tmp_path),
            wake_targets=[{"type": "opencode_thread", "events": ["completed"], "session_id": SESSION}],
        )
        wait_completed(manager, job_id)
        claimed = manager.relay_poll(CLIENT, timeout_seconds=2)
        assert [d["payload"]["target"]["session_id"] for d in claimed] == [SESSION]
    finally:
        manager.close()


def test_relay_poll_releases_on_shutdown(tmp_path):
    """A shutdown must release a long relay poll at once (no 30s pin).

    Relay polls run on non-daemon HTTP handler threads; a poll that ignores
    shutdown keeps the old daemon process alive after `vanth restart`.
    """
    import threading

    manager = JobManager(tmp_path / "state")
    try:
        register(manager, directory=str(tmp_path))
        manager.shutdown_requested.set()
        finished: list[float] = []

        def poll() -> None:
            started = time.monotonic()
            assert manager.relay_poll(CLIENT, timeout_seconds=30) == []
            finished.append(time.monotonic() - started)

        thread = threading.Thread(target=poll, daemon=True)
        thread.start()
        thread.join(timeout=10)
        assert not thread.is_alive(), "relay poll ignored shutdown"
        assert finished and finished[0] < 5, finished
    finally:
        manager.close()
