"""Tests for the MCP parent-liveness/idle watchdog and orphaned-server detection."""

import os
import subprocess
import sys
import time

import pytest

from vanth.process_watch import (
    _InFlight,
    _watch_loop,
    parent_pid,
    process_alive,
    start_watchdog,
    windows_pid_alive,
)


def test_process_alive_false_for_dead_pid():
    assert process_alive(1) is False
    assert process_alive(None) is False
    assert process_alive(0) is False


def test_windows_pid_alive_agrees_with_process_alive():
    """Handle-query fast path must match the tasklist result (and cost ~nothing)."""
    if sys.platform != "win32":
        assert windows_pid_alive(os.getpid()) is None
        return
    assert windows_pid_alive(os.getpid()) is True
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        assert windows_pid_alive(proc.pid) is True
    finally:
        proc.terminate()
        proc.wait(timeout=5)
    # Reaped child: a fixed high PID could exist on a busy host, so use the
    # real dead PID (also covers exit-code-259 ambiguity: any exit means dead).
    assert windows_pid_alive(proc.pid) is False
    assert process_alive(os.getpid()) is True
    assert process_alive(proc.pid) is False
    start = time.perf_counter()
    for _ in range(20):
        windows_pid_alive(os.getpid())
    assert (time.perf_counter() - start) < 1.0


def test_windows_pid_alive_exit_code_259_is_dead():
    """A process that exits with code 259 (STILL_ACTIVE) must read as dead."""
    if sys.platform != "win32":
        pytest.skip("Windows handle semantics")
    proc = subprocess.Popen([sys.executable, "-c", "import sys; sys.exit(259)"])
    proc.wait(timeout=10)
    assert windows_pid_alive(proc.pid) is False
    assert process_alive(proc.pid) is False


def test_process_alive_true_for_running_child():
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        assert process_alive(proc.pid) is True
    finally:
        proc.terminate()
        proc.wait(timeout=5)
    assert process_alive(proc.pid) is False


def test_parent_pid_returns_own_parent():
    pid = parent_pid()
    assert isinstance(pid, int) and pid > 1


def test_parent_pid_env_override(monkeypatch):
    monkeypatch.setenv("VANTH_WATCH_PARENT_PID", "999")
    assert parent_pid() == 999
    monkeypatch.setenv("VANTH_WATCH_PARENT_PID", "not-a-number")
    assert parent_pid() != "not-a-number"


def test_in_flight_tracker():
    tracker = _InFlight()
    assert tracker.active is False
    with tracker:
        assert tracker.active is True
        with tracker:
            assert tracker.active is True
        assert tracker.active is True
    assert tracker.active is False


def test_watch_loop_exits_when_parent_dies():
    exited = []

    def fake_exit():
        exited.append(True)

    # Parent PID 1 is never alive, so the loop exits on the first poll.
    _watch_loop(parent=1, interval=0.01, grace=0.0, idle=0.0, on_exit=fake_exit, tracker=_InFlight())
    assert exited


def test_watch_loop_idle_exit_without_traffic():
    exited = []

    def fake_exit():
        exited.append(True)

    # No stdin traffic (inject a probe that reports none), parent is self
    # (alive), idle threshold is tiny. The loop must exit.
    _watch_loop(
        parent=os.getpid(),
        interval=0.01,
        grace=60,
        idle=0.02,
        on_exit=fake_exit,
        tracker=_InFlight(),
        traffic=lambda: 0,
    )
    assert exited


def test_watch_loop_never_idle_exits_while_in_flight():
    exited = []

    def fake_exit():
        exited.append(True)

    import threading

    tracker = _InFlight()
    with tracker:
        thread = threading.Thread(
            target=_watch_loop,
            args=(os.getpid(), 0.005, 60, 0.01, fake_exit, tracker),
            kwargs={"traffic": lambda: 0, "alive": lambda: True},
            daemon=True,
        )
        thread.start()
        time.sleep(0.05)
        assert not exited, "an in-flight tool call must prevent idle exit"
    # After the `with` block the tracker is inactive: the idle path must now
    # run and terminate the thread (fake_exit fires). This also prevents a
    # leaked daemon thread from polluting later monkeypatched tests.
    thread.join(timeout=1.0)
    assert exited, "inactivity after the in-flight call must idle-exit"
    assert not thread.is_alive(), "idle path must terminate the watchdog thread"


def test_watch_loop_activity_keeps_relay_alive():
    """Review rc38 P1: a Desktop wake relay that keeps marking activity must
    never be idle-reaped. The bump-decrement `notify_activity()` alone is
    invisible to the watchdog (it samples `active` once per interval); the
    last-activity timestamp must reset the idle timer."""
    exited = []
    import threading

    def fake_exit():
        exited.append(True)

    tracker = _InFlight()
    stop = threading.Event()
    parent_alive = {"value": True}

    def relay_poll():
        while not stop.is_set():
            tracker.notify_activity()
            time.sleep(0.01)

    relay = threading.Thread(target=relay_poll, daemon=True)
    relay.start()
    try:
        # Parent self (alive), idle threshold 0.5s, interval 0.005s. Without
        # activity the process would exit in ~0.5s; the relay's continuous
        # activity must hold it far past that. (The threshold is kept well above
        # runner scheduling jitter, which flaked a 50ms value on macOS.)
        thread = threading.Thread(
            target=_watch_loop,
            args=(os.getpid(), 0.005, 0.0, 0.5, fake_exit, tracker),
            kwargs={"traffic": lambda: 0, "alive": lambda: parent_alive["value"]},
            daemon=True,
        )
        thread.start()
        time.sleep(1.2)
        assert not exited, "relay activity must prevent idle exit"
        # Stop the relay AND make the parent "die" so the daemon watchdog thread
        # terminates (a never-exiting daemon thread would keep calling
        # subprocess/tasklist in later tests and break monkeypatched tests).
        stop.set()
        relay.join(timeout=0.5)
        parent_alive["value"] = False
        thread.join(timeout=0.5)
        assert not thread.is_alive(), "watchdog thread must exit once the parent dies"
    finally:
        stop.set()
        relay.join(timeout=0.5)


def test_watch_loop_blocking_relay_work_keeps_alive():
    """Review rc39 P1: a relay long-poll that BLOCKS for longer than the
    configured idle timeout must NOT be reaped. The relay holds the tracker as a
    context manager around the blocking poll, so the watchdog sees it in-flight
    for the whole duration."""
    exited = []
    import threading

    def fake_exit():
        exited.append(True)

    tracker = _InFlight()
    stop = threading.Event()
    parent_alive = {"value": True}

    def relay_poll():
        # A long-poll that blocks well past the 0.1s idle threshold: if the
        # watchdog ignored the in-flight tracker the process would be reaped
        # during the poll.
        while not stop.is_set():
            with tracker:
                time.sleep(0.5)
            time.sleep(0.01)

    relay = threading.Thread(target=relay_poll, daemon=True)
    relay.start()
    try:
        thread = threading.Thread(
            target=_watch_loop,
            args=(os.getpid(), 0.005, 0.0, 0.1, fake_exit, tracker),
            kwargs={"traffic": lambda: 0, "alive": lambda: parent_alive["value"]},
            daemon=True,
        )
        thread.start()
        time.sleep(1.2)
        assert not exited, "a blocking relay poll must keep the process alive past the idle timeout"
        stop.set()
        relay.join(timeout=0.5)
        # Flip parent death so the watchdog thread terminates.
        parent_alive["value"] = False
        thread.join(timeout=0.5)
        assert not thread.is_alive(), "watchdog must terminate once the parent dies"
    finally:
        stop.set()
        relay.join(timeout=0.5)


def test_watch_loop_idle_exits_after_activity_stops():
    """Review rc38 P1: once the relay goes quiet, the idle timer runs and the
    process exits. Activity is a per-sample reset, not a keep-alive grant."""
    exited = []
    import threading

    def fake_exit():
        exited.append(True)

    tracker = _InFlight()
    tracker.notify_activity()
    time.sleep(0.02)
    thread = threading.Thread(
        target=_watch_loop,
        args=(os.getpid(), 0.005, 60, 0.03, fake_exit, tracker),
        kwargs={"traffic": lambda: 0, "alive": lambda: True},
        daemon=True,
    )
    thread.start()
    thread.join(timeout=1.0)
    assert exited, "inactivity after the last activity must idle-exit"


def test_start_watchdog_none_when_no_parent(monkeypatch):
    monkeypatch.delenv("VANTH_WATCH_PARENT_PID", raising=False)

    def fake_getppid():
        return None

    # Patch os.getppid to None → no parent → no watchdog.
    import vanth.process_watch as pw

    orig = pw.os.getppid
    pw.os.getppid = fake_getppid
    try:
        thread, tracker = start_watchdog()
        assert thread is None
    finally:
        pw.os.getppid = orig


def test_start_watchdog_runs_when_parent_exists(monkeypatch):
    import vanth.process_watch as pw

    orig = pw.os.getppid
    pw.os.getppid = lambda: os.getpid()  # self as parent → alive
    orig_alive = pw.process_alive
    # Start with the parent alive, then flip process_alive to False so the
    # grace-0 parent-death path terminates the thread. Without this the
    # self-parent watchdog would run forever and its per-poll `tasklist` probe
    # would pollute later monkeypatched subprocess.run tests.
    state = {"alive": True}
    pw.process_alive = lambda pid: state["alive"]
    exited = []
    try:
        # A no-op on_exit: the DEFAULT _exiting calls os._exit(0), which would
        # kill the whole pytest process when we simulate parent death.
        thread, tracker = start_watchdog(interval=0.05, grace=0.0, idle=0.0, on_exit=lambda: exited.append(True))
        assert thread is not None and thread.is_alive()
        time.sleep(0.1)
        assert thread.is_alive()
        state["alive"] = False
        thread.join(timeout=1.0)
        assert not thread.is_alive(), "watchdog must terminate once the parent dies"
        assert exited, "parent death must trigger the exit callback"
    finally:
        pw.process_alive = orig_alive
        pw.os.getppid = orig


def test_orphaned_mcp_detection_empty_when_no_orphans():
    from vanth.server import _orphaned_mcp_servers

    # On a healthy box this should be an empty list (no exception).
    try:
        result = _orphaned_mcp_servers()
        assert isinstance(result, list)
    except Exception as exc:  # wmic/ps unavailable in some sandboxes
        pytest.skip(f"process enumeration unavailable: {exc}")
