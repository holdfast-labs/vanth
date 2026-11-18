import json
import os
import stat
import subprocess
import sys
import time

import pytest

from vanth import setup
from vanth.paths import canonical_home


@pytest.fixture(autouse=True)
def _isolate_opencode_plugin_dir(tmp_path, monkeypatch):
    """`run_setup` installs/removes the OpenCode wake plugin: a test must never
    touch the developer's real ~/.config/opencode/plugins."""
    monkeypatch.setenv("VANTH_OPENCODE_PLUGIN_DIR", str(tmp_path / "opencode-plugins"))
    monkeypatch.setattr(setup.shutil, "which", lambda name: None)


def _scratch_configs(tmp_path, monkeypatch):
    """Create fake opencode/codex/claude configs in tmp_path and point setup
    discovery at them. Returns the three paths."""
    opencode_path = tmp_path / "opencode.json"
    codex_path = tmp_path / "codex.toml"
    claude_path = tmp_path / "claude.json"

    opencode_path.write_text(
        json.dumps({"mcp": {"other": {"type": "local", "command": ["x"]}}}, indent=2), encoding="utf-8"
    )
    codex_path.write_text('[model]\nname = "gpt"\n\n[mcp_servers.other]\ncommand = "other"\n', encoding="utf-8")
    claude_path.write_text(json.dumps({"mcpServers": {"other": {"command": "x"}}}, indent=2), encoding="utf-8")

    monkeypatch.setattr(setup, "client_config_paths", lambda home=None: {
        "opencode": [opencode_path],
        "codex": [codex_path],
        "claude": [claude_path],
    })
    return opencode_path, codex_path, claude_path


def test_register_all_clients_and_idempotence(tmp_path, monkeypatch):
    monkeypatch.setattr("sys.stdin", None)
    opencode_path, codex_path, claude_path = _scratch_configs(tmp_path, monkeypatch)
    home = canonical_home()

    assert setup.run_setup(None, home=home, assume_yes=True) == 0

    # opencode
    oc = json.loads(opencode_path.read_text(encoding="utf-8"))
    assert oc["mcp"]["vanth"] == {
        "type": "local",
        "command": ["vanth"],
        "enabled": True,
        "timeout": 60000,
        "environment": {"VANTH_HOME": str(home)},
    }
    assert oc["mcp"]["other"] is not None  # untouched

    # codex
    import tomllib

    cd = tomllib.loads(codex_path.read_text(encoding="utf-8"))
    assert cd["mcp_servers"]["vanth"]["command"] == "vanth"
    assert cd["mcp_servers"]["vanth"]["env"]["VANTH_HOME"] == str(home)
    assert cd["model"]["name"] == "gpt"  # untouched

    # claude
    cl = json.loads(claude_path.read_text(encoding="utf-8"))
    assert cl["mcpServers"]["vanth"]["command"] == "vanth"
    assert cl["mcpServers"]["vanth"]["env"]["VANTH_HOME"] == str(home)
    assert cl["mcpServers"]["other"] is not None

    # second run is a no-op
    assert setup.run_setup(None, home=home, assume_yes=True) == 0
    assert json.loads(opencode_path.read_text(encoding="utf-8"))["mcp"]["vanth"] is not None


def test_setup_upgrades_stale_opencode_timeout(tmp_path, monkeypatch):
    """A config written by an older setup has `timeout: 15000`, which is shorter
    than a blocking MCP call (25 s wait slice + margin). Re-running setup owns
    the entry, so it must raise the timeout to 60000."""
    opencode_path = tmp_path / "opencode.json"
    opencode_path.write_text(
        json.dumps({"mcp": {"vanth": {
            "type": "local",
            "command": ["vanth"],
            "enabled": True,
            "timeout": 15000,
            "environment": {"VANTH_HOME": str(canonical_home())},
        }}}),
        encoding="utf-8",
    )
    monkeypatch.setattr(setup, "client_config_paths", lambda home=None: {"opencode": [opencode_path]})

    assert setup.run_setup(["opencode"], home=canonical_home(), assume_yes=True) == 0

    entry = json.loads(opencode_path.read_text(encoding="utf-8"))["mcp"]["vanth"]
    assert entry["timeout"] == 60000


def test_explicit_setup_creates_missing_client_configs_without_backups(tmp_path, monkeypatch):
    paths = {
        "opencode": tmp_path / ".config" / "opencode" / "opencode.json",
        "codex": tmp_path / ".codex" / "config.toml",
        "claude": tmp_path / ".claude.json",
    }
    monkeypatch.setattr(setup, "client_config_paths", lambda home=None: {})
    monkeypatch.setattr(setup, "_default_config_path", paths.__getitem__)
    home = tmp_path / "vanth-home"

    assert setup.run_setup(["opencode", "codex", "claude"], home=home, assume_yes=True) == 0

    opencode = tmp_path / ".config" / "opencode" / "opencode.json"
    codex = tmp_path / ".codex" / "config.toml"
    claude = tmp_path / ".claude.json"
    assert json.loads(opencode.read_text(encoding="utf-8"))["mcp"]["vanth"]["enabled"] is True
    assert "[mcp_servers.vanth]" in codex.read_text(encoding="utf-8")
    assert json.loads(claude.read_text(encoding="utf-8"))["mcpServers"]["vanth"]["command"] == "vanth"
    assert list(tmp_path.rglob("*.vanth-setup-*.bak")) == []


def test_bare_setup_creates_configs_for_detected_installed_clients(tmp_path, monkeypatch):
    config = tmp_path / ".codex" / "config.toml"
    monkeypatch.setattr(setup, "client_config_paths", lambda home=None: {})
    monkeypatch.setattr(setup, "_default_config_path", lambda client: config)
    monkeypatch.setattr(setup.shutil, "which", lambda name: f"/bin/{name}" if name == "codex" else None)

    assert setup.run_setup(home=tmp_path / "vanth-home", assume_yes=True) == 0

    assert "[mcp_servers.vanth]" in config.read_text(encoding="utf-8")
    assert not (tmp_path / ".claude.json").exists()


def test_write_new_never_replaces_a_config_that_appeared(tmp_path):
    config = tmp_path / "config.json"
    config.write_text('{"other": true}\n', encoding="utf-8")

    with pytest.raises(FileExistsError):
        setup._write_new(config, '{"mcp": {"vanth": {}}}\n')

    assert config.read_text(encoding="utf-8") == '{"other": true}\n'
    assert list(tmp_path.glob("config.json.*.tmp")) == []


def test_remove_all_clients(tmp_path, monkeypatch):
    opencode_path, codex_path, claude_path = _scratch_configs(tmp_path, monkeypatch)
    home = canonical_home()

    setup.run_setup(None, home=home, assume_yes=True)
    assert setup.run_setup(None, home=home, remove=True, assume_yes=True) == 0

    oc = json.loads(opencode_path.read_text(encoding="utf-8"))
    assert "vanth" not in oc["mcp"]
    assert oc["mcp"]["other"] is not None

    cd = codex_path.read_text(encoding="utf-8")
    assert "mcp_servers.vanth" not in cd
    assert "mcp_servers.other" in cd

    cl = json.loads(claude_path.read_text(encoding="utf-8"))
    assert "vanth" not in cl["mcpServers"]
    assert cl["mcpServers"]["other"] is not None


def test_backups_are_written_before_change(tmp_path, monkeypatch):
    opencode_path, codex_path, claude_path = _scratch_configs(tmp_path, monkeypatch)
    home = canonical_home()
    setup.run_setup(None, home=home, assume_yes=True)
    backups = list(tmp_path.glob("*.bak"))
    assert len(backups) == 3


def test_detect_status(tmp_path, monkeypatch):
    opencode_path, codex_path, claude_path = _scratch_configs(tmp_path, monkeypatch)
    home = canonical_home()
    status = setup.detect_status(home)
    assert status["opencode"][0]["configured"] is False
    setup.run_setup(None, home=home, assume_yes=True)
    status = setup.detect_status(home)
    assert all(entry["configured"] for entries in status.values() for entry in entries)


def test_unknown_client_is_usage_error(tmp_path, monkeypatch):
    _scratch_configs(tmp_path, monkeypatch)
    home = canonical_home()
    assert setup.run_setup(["bogus"], home=home, assume_yes=True) == 2


def test_no_configs_found_returns_one(tmp_path, monkeypatch):
    home = tmp_path / "empty"
    home.mkdir()
    monkeypatch.setattr(setup, "client_config_paths", lambda home=None: {})
    assert setup.run_setup(None, home=home, assume_yes=True) == 1


def test_mcp_startup_hint_mentions_unconfigured_clients(tmp_path, capsys, monkeypatch):
    import vanth.server as server

    opencode_path, codex_path, claude_path = _scratch_configs(tmp_path, monkeypatch)
    monkeypatch.setattr(setup, "client_config_paths", lambda home=None: {"claude": [claude_path]})
    server._hint_setup()
    captured = capsys.readouterr()
    assert "vanth setup" in captured.err


def test_mcp_startup_hint_silent_when_all_configured(tmp_path, capsys, monkeypatch):
    import vanth.server as server

    opencode_path, codex_path, claude_path = _scratch_configs(tmp_path, monkeypatch)
    setup.run_setup(None, home=canonical_home(), assume_yes=True)
    monkeypatch.setattr(setup, "client_config_paths", lambda home=None: {"opencode": [opencode_path]})
    server._hint_setup()
    captured = capsys.readouterr()
    assert "vanth setup" not in captured.err


def test_mcp_startup_hint_respects_env_opt_out(tmp_path, capsys, monkeypatch):
    import vanth.server as server

    opencode_path, codex_path, claude_path = _scratch_configs(tmp_path, monkeypatch)
    monkeypatch.setattr(setup, "client_config_paths", lambda home=None: {"opencode": [opencode_path]})
    monkeypatch.setenv("VANTH_NO_SETUP_HINT", "1")
    server._hint_setup()
    captured = capsys.readouterr()
    assert "vanth setup" not in captured.err


def test_status_line_prints_client_states(tmp_path, capsys, monkeypatch):
    from vanth.cli import _print_setup_status

    opencode_path, codex_path, claude_path = _scratch_configs(tmp_path, monkeypatch)
    _print_setup_status()
    captured = capsys.readouterr()
    assert "opencode=not configured" in captured.out
    setup.run_setup(None, home=canonical_home(), assume_yes=True)
    _print_setup_status()
    captured = capsys.readouterr()
    assert "opencode=configured" in captured.out


def test_cli_help_and_no_args_list_commands(capsys):
    from vanth import cli

    for argv in ([], ["--help"], ["-h"], ["help"]):
        assert cli.main(argv) == 0
        out = capsys.readouterr().out
        assert "usage: vanth" in out
        assert "status" in out
        assert "setup" in out


def test_server_entry_routes_help_to_cli(capsys):
    from vanth import server

    for argv in (["--help"], ["-h"], ["help"]):
        try:
            server.main(argv)
        except SystemExit as exc:
            assert exc.code == 0
        out = capsys.readouterr().out
        assert "usage: vanth" in out
        assert "setup" in out


def test_server_entry_keeps_bare_mcp(capsys, monkeypatch):
    """Bare `vanth` (no args) must keep running the MCP stdio server, not the CLI."""
    from vanth import server

    called: list = []
    monkeypatch.setattr(server.mcp, "run", lambda: called.append("mcp"))
    server.main([])
    assert called == ["mcp"]


def test_bare_vanth_tty_guard_prints_usage(monkeypatch, capsys):
    """User report rc23: bare `vanth` in an interactive terminal started the
    MCP stdio server and appeared to hang. With a TTY stdin+stdout and no
    args we must print guidance and exit 2 instead."""
    import vanth.server as server

    monkeypatch.setattr(server.sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(server.sys.stdout, "isatty", lambda: True)
    called = {"mcp": False}

    def _boom():
        called["mcp"] = True

    monkeypatch.setattr(server, "_run_mcp_server", _boom)
    with pytest.raises(SystemExit) as exc:
        server.main([])
    assert exc.value.code == 2
    assert "vanth-monitor" in capsys.readouterr().err
    assert called["mcp"] is False


def test_bare_vanth_piped_still_runs_mcp(monkeypatch):
    """MCP clients always run bare `vanth` with pipes; the TTY guard must
    not interfere with them."""
    import vanth.server as server

    monkeypatch.setattr(server.sys.stdin, "isatty", lambda: False)
    monkeypatch.setattr(server.sys.stdout, "isatty", lambda: False)
    monkeypatch.setenv("VANTH_NO_SETUP_HINT", "1")
    ran = {"mcp": False}
    monkeypatch.setattr(server, "_run_mcp_server", lambda: ran.__setitem__("mcp", True))
    server.main([])
    assert ran["mcp"] is True


def test_unknown_interactive_command_does_not_hang(monkeypatch, capsys):
    """Review P2-2: `vanth statsu` (a typo) in a terminal must not enter the
    MCP stdio read loop and look hung; it routes to a usage error and exits 2."""
    import vanth.server as server

    monkeypatch.setattr(server.sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(server.sys.stdout, "isatty", lambda: True)
    called = {"mcp": False}
    monkeypatch.setattr(server, "_run_mcp_server", lambda: called.__setitem__("mcp", True))
    with pytest.raises(SystemExit) as exc:
        server.main(["statsu"])
    assert exc.value.code == 2
    assert "unknown command" in capsys.readouterr().err


def test_register_codex_desktop_writes_capability_file(tmp_path, monkeypatch):
    """Review rc37 P0: `vanth setup desktop` provisions Desktop wake by writing
    a per-home capability file (pipe + caller thread) that the relay reads, so
    the relay does NOT rely on ambient env inheritance into Vanth MCP children."""
    monkeypatch.setenv("VANTH_CODEX_DESKTOP_PIPE", "\\\\\\\\.\\\\pipe\\\\codex_app_tools")
    monkeypatch.setenv("VANTH_CODEX_DESKTOP_THREAD", "thread_executor")
    home = tmp_path / "home"
    home.mkdir(parents=True, exist_ok=True)
    changed, summary = setup.register_codex_desktop(home)
    assert changed is True
    assert "thread_executor" in summary
    path = home / "codex_desktop.json"
    assert path.is_file()
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["pipe_path"] == "\\\\\\\\.\\\\pipe\\\\codex_app_tools"
    assert data["thread_id"] == "thread_executor"
    # Remove works.
    changed2, _ = setup.remove_codex_desktop(home)
    assert changed2 is True
    assert not path.exists()


def test_register_codex_desktop_fails_closed_without_capability(tmp_path, monkeypatch):
    """Review rc37 P0: provisioning must fail explicitly (not silently no-op)
    when neither a pipe nor a caller thread identity is available."""
    monkeypatch.delenv("VANTH_CODEX_DESKTOP_PIPE", raising=False)
    monkeypatch.delenv("CODEX_APP_TOOLS_PIPE_PATH", raising=False)
    monkeypatch.delenv("VANTH_CODEX_DESKTOP_THREAD", raising=False)
    monkeypatch.delenv("CODEX_THREAD_ID", raising=False)
    home = tmp_path / "home"
    home.mkdir(parents=True, exist_ok=True)
    changed, summary = setup.register_codex_desktop(home)
    assert changed is False
    assert "unavailable" in summary
    assert not (home / "codex_desktop.json").exists()
