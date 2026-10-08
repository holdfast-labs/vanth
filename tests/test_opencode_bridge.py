from __future__ import annotations

import shutil
import subprocess
import sys

import pytest

from vanth.opencode_bridge import (
    OpenCodeBridgeError,
    OpenCodeSessionNotFound,
    send_delivery_to_opencode,
    _command_argv,
    _session_exists,
    _session_probe_cache,
)


def test_default_windows_npm_command_resolves_native_exe(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    fake = tmp_path / "npm" / "opencode.CMD"
    native = fake.parent / "node_modules" / "opencode-ai" / "bin" / "opencode.exe"
    native.parent.mkdir(parents=True)
    native.touch()
    monkeypatch.setattr(shutil, "which", lambda name: str(fake) if name == "opencode" else None)
    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.delenv("VANTH_OPENCODE_BIN", raising=False)
    assert _command_argv(None) == [str(native)]


def test_windows_batch_fallback_flattens_multiline_prompt(monkeypatch: pytest.MonkeyPatch) -> None:
    seen = {}

    def fake_run(argv, **kwargs):
        seen["argv"] = argv
        return subprocess.CompletedProcess(argv, 0, '{"type":"session.idle"}\n', "")

    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setattr(subprocess, "run", fake_run)
    send_delivery_to_opencode(
        {
            "prompt": "vanth event\ndelivery_id: del_live\nevent: completed",
            "target": {
                "session_id": "ses_live",
                "attach": "http://127.0.0.1:4096",
                "opencode_command": [r"C:\custom\opencode.cmd"],
            },
        }
    )

    assert seen["argv"][-1] == "vanth event delivery_id: del_live event: completed"


def test_explicit_command_takes_precedence(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(shutil, "which", lambda name: None)
    monkeypatch.setenv("VANTH_OPENCODE_BIN", r"C:\custom\opencode.exe")
    assert _command_argv(None) == [r"C:\custom\opencode.exe"]


def test_delivery_invokes_opencode_run(monkeypatch: pytest.MonkeyPatch) -> None:
    seen = {}

    def fake_run(argv, **kwargs):
        seen["argv"] = argv
        seen["kwargs"] = kwargs
        return subprocess.CompletedProcess(argv, 0, '{"type":"session.idle"}\n', "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    result = send_delivery_to_opencode(
        {
            "prompt": "job finished",
            "target": {
                "type": "opencode_thread",
                "sessionId": "ses_123",
                "opencode_command": ["custom-opencode", "--pure"],
                "timeout_seconds": 12,
                "cwd": "F:/work/project",
                "attach": "http://127.0.0.1:4096",
            },
        }
    )

    assert seen == {
        "argv": [
            "custom-opencode",
            "--pure",
            "run",
            "--session",
            "ses_123",
            "--dir",
            "F:/work/project",
            "--attach",
            "http://127.0.0.1:4096",
            "--format",
            "json",
            "job finished",
        ],
        "kwargs": {"capture_output": True, "text": True, "timeout": 12},
    }
    assert result == {"session_id": "ses_123", "stdout": '{"type":"session.idle"}\n', "stderr": ""}


@pytest.mark.parametrize("key", ["session_id", "sessionId", "thread_id", "threadId"])
def test_session_id_aliases(monkeypatch: pytest.MonkeyPatch, key: str) -> None:
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda argv, **kwargs: subprocess.CompletedProcess(argv, 0, "", ""),
    )
    result = send_delivery_to_opencode({"prompt": "wake", "target": {key: "ses_alias"}})
    assert result["session_id"] == "ses_alias"


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({"prompt": "wake", "target": {}}, "requires session_id"),
        ({"prompt": "", "target": {"session_id": "ses_1"}}, "requires prompt"),
    ],
)
def test_required_fields(payload: dict, message: str) -> None:
    with pytest.raises(OpenCodeBridgeError, match=message):
        send_delivery_to_opencode(payload)


def test_nonzero_exit_is_clear(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda argv, **kwargs: subprocess.CompletedProcess(argv, 7, "", "bad session"),
    )
    with pytest.raises(OpenCodeBridgeError, match="opencode exited with 7: bad session"):
        send_delivery_to_opencode({"prompt": "wake", "target": {"session_id": "ses_1"}})


def test_timeout_is_clear(monkeypatch: pytest.MonkeyPatch) -> None:
    def timeout(argv, **kwargs):
        raise subprocess.TimeoutExpired(argv, kwargs["timeout"])

    monkeypatch.setattr(subprocess, "run", timeout)
    with pytest.raises(OpenCodeBridgeError, match="ses_1 timed out after 4 seconds"):
        send_delivery_to_opencode(
            {"prompt": "wake", "target": {"session_id": "ses_1", "timeout_seconds": 4}}
        )


def test_launch_failure_is_clear(monkeypatch: pytest.MonkeyPatch) -> None:
    def raise_file_not_found(argv, **kwargs):
        raise FileNotFoundError(2, "No such file or directory", argv[0])

    monkeypatch.setattr(subprocess, "run", raise_file_not_found)
    with pytest.raises(OpenCodeBridgeError, match="failed to start opencode"):
        send_delivery_to_opencode({"prompt": "wake", "target": {"session_id": "ses_1"}})


def test_missing_session_raises_classifiable_error_without_model_turn(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []

    def fake_run(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, '[{"id": "ses_other"}]\n', "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    with pytest.raises(OpenCodeSessionNotFound, match="session not found"):
        send_delivery_to_opencode({"prompt": "x", "target": {"session_id": "ses_missing"}})
    assert len(calls) == 1
    assert "session" in calls[0] and "list" in calls[0] and "run" not in calls[0]


def test_existing_session_proceeds_to_model_turn(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []

    def fake_run(argv, **kwargs):
        calls.append(argv)
        if "list" in argv:
            return subprocess.CompletedProcess(argv, 0, '[{"id": "ses_live"}]\n', "")
        return subprocess.CompletedProcess(argv, 0, '{"type":"session.idle"}\n', "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    result = send_delivery_to_opencode({"prompt": "wake", "target": {"session_id": "ses_live"}})
    assert len(calls) == 2
    assert "run" in calls[1] and "--session" in calls[1]
    assert result["session_id"] == "ses_live"


@pytest.mark.parametrize("probe_exit", [7])
def test_probe_ambiguity_does_not_block(monkeypatch: pytest.MonkeyPatch, probe_exit: int) -> None:
    calls: list[list[str]] = []

    def fake_run(argv, **kwargs):
        calls.append(argv)
        if "list" in argv:
            return subprocess.CompletedProcess(argv, probe_exit, "", "probe failed")
        return subprocess.CompletedProcess(argv, 0, '{"type":"session.idle"}\n', "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    result = send_delivery_to_opencode({"prompt": "wake", "target": {"session_id": "ses_ambig"}})
    assert len(calls) == 2
    assert "run" in calls[1] and "--session" in calls[1]
    assert result["session_id"] == "ses_ambig"


def test_probe_skipped_with_env(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []
    monkeypatch.setenv("VANTH_OPENCODE_SKIP_PROBE", "1")

    def fake_run(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, '{"type":"session.idle"}\n', "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    send_delivery_to_opencode({"prompt": "wake", "target": {"session_id": "ses_1"}})
    assert len(calls) == 1
    assert "run" in calls[0] and "list" not in calls[0]


def test_probe_skipped_with_target_optout(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []

    def fake_run(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, '{"type":"session.idle"}\n', "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    send_delivery_to_opencode({"prompt": "wake", "target": {"session_id": "ses_1", "skip_probe": True}})
    assert len(calls) == 1
    assert "run" in calls[0] and "list" not in calls[0]


def test_probe_skipped_when_attach_set(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []

    def fake_run(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, '{"type":"session.idle"}\n', "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    send_delivery_to_opencode(
        {"prompt": "wake", "target": {"session_id": "ses_1", "attach": "http://127.0.0.1:4096"}}
    )
    assert len(calls) == 1
    assert "list" not in calls[0]


def test_probe_uses_target_cwd(monkeypatch: pytest.MonkeyPatch) -> None:
    """Review P0-3: the session-existence probe must run against the target
    cwd so a valid cross-project session is not classified as missing. OpenCode
    `session list` does not support --dir, so cwd is passed via the subprocess
    cwd= kwarg (not an unsupported flag)."""
    calls: list[list[str]] = []
    kwargs_seen: list[dict] = []

    def fake_run(argv, **kwargs):
        calls.append(argv)
        kwargs_seen.append(kwargs)
        if "list" in argv:
            return subprocess.CompletedProcess(argv, 0, '[{"id": "ses_proj"}]\n', "")
        return subprocess.CompletedProcess(argv, 0, '{"type":"session.idle"}\n', "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    send_delivery_to_opencode(
        {"prompt": "wake", "target": {"session_id": "ses_proj", "cwd": "F:/work/project", "skip_probe": False}}
    )
    assert len(calls) == 2
    assert "list" in calls[0] and "--dir" not in calls[0]
    assert kwargs_seen[0].get("cwd") == "F:/work/project"
    assert "run" in calls[1] and "--dir" in calls[1]


def test_auth_forwards_credential_references(monkeypatch: pytest.MonkeyPatch) -> None:
    """Review P0-3: authenticated opencode servers are supported through
    non-persisted credential references — only ENV VAR NAMES are accepted (never
    literal values), forwarded as the documented OPENCODE_SERVER_USERNAME /
    OPENCODE_SERVER_PASSWORD variables. Review rc36 P2: only the unambiguous
    username_env / password_env keys are honored (the legacy username/password
    aliases are ambiguous)."""
    monkeypatch.setenv("MY_USER", "bot")
    monkeypatch.setenv("MY_PASS", "s3cret")
    seen = {}

    def fake_run(argv, **kwargs):
        seen.update(kwargs)
        return subprocess.CompletedProcess(argv, 0, '{"type":"session.idle"}\n', "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    send_delivery_to_opencode(
        {
            "prompt": "wake",
            "target": {
                "session_id": "ses_1",
                "skip_probe": True,
                "auth": {"username_env": "MY_USER", "password_env": "MY_PASS"},
            },
        }
    )
    assert "env" in seen
    assert seen["env"].get("OPENCODE_SERVER_USERNAME") == "bot"
    assert seen["env"].get("OPENCODE_SERVER_PASSWORD") == "s3cret"


def test_auth_rejects_literal_values(monkeypatch: pytest.MonkeyPatch) -> None:
    """Review P0-3: literal secret values must NOT be accepted in auth (they
    would be serialized into wake-target config / delivery payloads). Review
    rc36 P2: the legacy username/password aliases are rejected outright (too
    ambiguous), and a referenced-but-absent variable is an explicit error."""
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda argv, **kwargs: subprocess.CompletedProcess(argv, 0, '{"type":"session.idle"}\n', ""),
    )
    # Legacy alias: ambiguous and now rejected outright.
    with pytest.raises(OpenCodeBridgeError, match="username_env"):
        send_delivery_to_opencode(
            {
                "prompt": "wake",
                "target": {
                    "session_id": "ses_1",
                    "skip_probe": True,
                    "auth": {"username": "literal-bot", "password": "literal-pass"},
                },
            }
        )
    # A referenced variable that is not set must fail explicitly (no silent "").
    with pytest.raises(OpenCodeBridgeError, match="not set"):
        send_delivery_to_opencode(
            {
                "prompt": "wake",
                "target": {
                    "session_id": "ses_1",
                    "skip_probe": True,
                    "auth": {"username_env": "UNSET_USER_VAR", "password_env": "UNSET_PASS_VAR"},
                },
            }
        )
    # A literal value in username_env (non-identifier) is still rejected.
    with pytest.raises(OpenCodeBridgeError, match="environment variable NAME"):
        send_delivery_to_opencode(
            {
                "prompt": "wake",
                "target": {
                    "session_id": "ses_1",
                    "skip_probe": True,
                    "auth": {"username_env": "literal-bot"},
                },
            }
        )



def test_session_probe_caches_conclusive_answers(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list = []

    def fake_run(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, '[{"id": "ses_cached"}]', "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    _session_probe_cache.clear()
    try:
        assert _session_exists("ses_cached", ["opencode"]) is True
        assert _session_exists("ses_cached", ["opencode"]) is True
        assert len(calls) == 1
        assert _session_exists("ses_missing", ["opencode"]) is False
        assert len(calls) == 2
    finally:
        _session_probe_cache.clear()


def test_session_probe_ttl_zero_disables_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list = []

    def fake_run(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, '[{"id": "ses_x"}]', "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setenv("VANTH_SESSION_PROBE_TTL_SECONDS", "0")
    _session_probe_cache.clear()
    try:
        assert _session_exists("ses_x", ["opencode"]) is True
        assert _session_exists("ses_x", ["opencode"]) is True
        assert len(calls) == 2
    finally:
        _session_probe_cache.clear()
