"""Regression tests for bugs found in audit (2026-11)."""

import os
import time
from pathlib import Path

import pytest

from vanth import cli


class RecordingClient:
    payload = None

    def __init__(self, *, home):
        self.home = home

    def ensure(self):
        pass

    def post(self, path, payload):
        RecordingClient.payload = payload
        return {"job_id": "j1", "status": "queued"}

    def confirm_local_start(self, result):
        return result


def test_wake_me_uses_caller_cwd_not_job_cwd(monkeypatch, tmp_path):
    """The OpenCode plugin registers the AGENT's directory, so --wake-me must
    resolve the relay from the caller's cwd. Using the job's --cwd woke an
    unrelated project's session (or none) whenever a job targeted another dir."""
    caller = tmp_path / "caller"
    job = tmp_path / "job"
    caller.mkdir()
    job.mkdir()
    monkeypatch.chdir(caller)
    monkeypatch.setattr(cli, "VanthClient", RecordingClient)

    assert cli.cmd_start(["--cwd", str(job), "--wake-me", "--", "echo", "ok"], tmp_path) == 0

    payload = RecordingClient.payload
    assert payload["cwd"] == str(job)
    target = payload["wake_targets"][0]
    assert target["type"] == "opencode_thread"
    assert target["cwd"] == os.getcwd() == str(caller)


def test_relative_cwd_resolves_against_caller(monkeypatch, tmp_path):
    """The daemon would resolve a relative --cwd against ITS OWN working
    directory (wherever it first started), so `--cwd sub` ran the job in the
    wrong place. The CLI must make the caller-relative path absolute."""
    caller = tmp_path / "caller"
    (caller / "sub").mkdir(parents=True)
    monkeypatch.chdir(caller)
    monkeypatch.setattr(cli, "VanthClient", RecordingClient)

    assert cli.cmd_start(["--cwd", "sub", "--", "echo", "ok"], tmp_path) == 0
    assert Path(RecordingClient.payload["cwd"]) == (caller / "sub").resolve()


def test_autostart_prompt_eof_is_a_clear_error(monkeypatch, capsys):
    """`input()` raises EOFError with a non-interactive stdin (piped/agent).
    That surfaced a traceback instead of the documented --yes hint."""
    from vanth import autostart

    monkeypatch.setattr(autostart, "platform", lambda: "linux")

    def _eof(prompt):
        raise EOFError

    monkeypatch.setattr("builtins.input", _eof)
    code = cli.main(["autostart", "enable"])
    captured = capsys.readouterr()
    assert code == 1
    assert "pass --yes" in captured.err


def test_status_survives_unreadable_token_file(monkeypatch, tmp_path, capsys):
    """A `token` directory/symlink loop must read as DOWN, not traceback."""
    (tmp_path / "token").mkdir()
    monkeypatch.setattr(cli, "_print_setup_status", lambda: None)
    assert cli.cmd_status(tmp_path) == 1
    assert "DOWN" in capsys.readouterr().out


def test_relay_poll_treats_malformed_response_as_empty(monkeypatch, tmp_path):
    """A daemon error response has no `deliveries`; returning None crashed the
    relay thread on enumerate(), silently stopping all future wakes."""
    from vanth.relay import DesktopRelay

    monkeypatch.setenv("VANTH_HOME", str(tmp_path))

    class ErrorClient:
        def get(self, path, params):
            return {"error": "unauthorized"}

    relay = DesktopRelay(
        ErrorClient(), "client_x", destinations=[], caller_thread_id="t", pipe_path=None
    )
    assert relay._poll() == []


def test_outbound_invalid_port_is_a_policy_denial():
    """A malformed port made urlsplit().port raise a raw ValueError, escaping
    the OutboundDenied contract that callers catch."""
    from vanth.outbound import OutboundDenied, check_outbound_url

    with pytest.raises(OutboundDenied):
        check_outbound_url("http://127.0.0.1:notaport/")
    with pytest.raises(OutboundDenied):
        check_outbound_url("http://[::1/")


def test_watchdog_survives_nonfinite_interval(monkeypatch):
    """VANTH_WATCH_INTERVAL=nan made time.sleep raise inside the watchdog
    thread, which died silently and leaked the MCP server forever."""
    import vanth.process_watch as pw

    state = {"alive": True}
    monkeypatch.setattr(pw, "parent_pid", lambda: os.getpid())
    monkeypatch.setattr(pw, "process_alive", lambda pid: state["alive"])
    exited: list[bool] = []
    thread, _ = pw.start_watchdog(
        interval=float("nan"), grace=0.0, idle=0.0, on_exit=lambda: exited.append(True)
    )
    assert thread is not None
    try:
        time.sleep(0.2)
        assert thread.is_alive(), "a non-finite interval must not kill the watchdog"
    finally:
        state["alive"] = False
        thread.join(timeout=3)
    assert exited, "the watchdog must still reap after the parent dies"

