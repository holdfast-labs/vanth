"""Language-neutral surfaces: ``vanth emit`` and job toolchain detection."""

import io
import json
import os
import sys

from vanth import launcher
from vanth.cli import main
from vanth.launcher import resolve_target
from vanth.runtime_info import infer_toolchain

PREFIX = "AGENT_EVENT "


def _payload(capsys):
    line = capsys.readouterr().out.strip()
    assert line.startswith(PREFIX)
    return json.loads(line[len(PREFIX):])


def test_emit_metric_coerces_values(capsys):
    assert main(["emit", "metric", "--data", "_step=10", "--data", "loss=0.42", "--data", "name=abc"]) == 0
    assert _payload(capsys) == {"type": "metric", "data": {"_step": 10, "loss": 0.42, "name": "abc"}}


def test_emit_progress_computes_percent(capsys):
    assert main(["emit", "progress", "--data", "current=10", "--data", "total=100", "--data", "unit=epoch"]) == 0
    assert _payload(capsys)["data"]["percent"] == 10.0


def test_emit_message_level_and_double_dash(capsys):
    assert main(["emit", "log", "--message", "hi", "--level", "warning"]) == 0
    payload = _payload(capsys)
    assert payload["message"] == "hi" and payload["level"] == "warning"
    assert main(["emit", "log", "--", "--dashy"]) == 0
    assert _payload(capsys)["message"] == "--dashy"


def test_emit_json_data_from_file_and_stdin(tmp_path, monkeypatch, capsys):
    path = tmp_path / "ev.json"
    path.write_text('{"current":1,"total":4}')
    assert main(["emit", "progress", "--json-data", "@" + str(path)]) == 0
    assert _payload(capsys)["data"]["percent"] == 25.0
    monkeypatch.setattr(sys, "stdin", io.StringIO('{"loss":0.25}'))
    assert main(["emit", "metric", "--json-data", "-"]) == 0
    assert _payload(capsys)["data"]["loss"] == 0.25


def test_emit_rejects_bad_input(capsys):
    assert main(["emit", "metric", "--nope"]) == 2
    assert main(["emit", "metric", "--data"]) == 2
    assert main(["emit", "metric", "--data", "noequals"]) == 2


def test_launcher_main_dispatches_default_cli(capsys):
    # sys.argv[0] under pytest is not a known role, so this is the CLI path.
    # server.main exits via SystemExit, which the frozen entry re-raises.
    import pytest

    with pytest.raises(SystemExit) as excinfo:
        launcher.main(["emit", "metric", "--data", "x=1"])
    assert excinfo.value.code == 0
    assert _payload(capsys) == {"type": "metric", "data": {"x": 1}}


def test_launcher_main_runs_inline_c(capsys):
    # `sys.executable -c <code>` is how `vanth sleep` launches its worker; the
    # frozen binary must run it instead of entering the MCP loop.
    assert launcher.main(["-c", "print('inline-ok')"]) == 0
    assert capsys.readouterr().out.strip() == "inline-ok"


def test_infer_toolchain_from_command():
    assert infer_toolchain("go test ./...", None)["language"] == "go"
    assert infer_toolchain("cargo build --release", None)["language"] == "rust"
    assert infer_toolchain("node index.js", None)["language"] == "node"
    assert infer_toolchain("uv run pytest", None)["language"] == "python"
    assert infer_toolchain("C:\\Python\\python.exe -c x", None)["language"] == "python"
    # shells are unwrapped to the program they run
    assert infer_toolchain("bash -lc 'go test ./...'", None)["language"] == "go"
    assert infer_toolchain("cmd /c cargo build", None)["language"] == "rust"
    assert infer_toolchain('powershell -Command "go build"', None)["language"] == "go"
    # wrapper/assignment prefixes are skipped
    assert infer_toolchain("FOO=1 python x", None)["language"] == "python"
    assert infer_toolchain("env FOO=1 python x", None)["language"] == "python"
    assert infer_toolchain("sudo go test", None)["language"] == "go"
    # only the program counts: arguments must not mislabel the job
    assert infer_toolchain("rm -rf node", None) is None
    assert infer_toolchain("echo go", None) is None
    assert infer_toolchain("bash --norc python x", None) is None
    assert infer_toolchain("just do-it", None) is None


def test_infer_toolchain_from_marker_up_the_tree(tmp_path):
    (tmp_path / "go.mod").write_text("module x")
    sub = tmp_path / "cmd" / "tool"
    sub.mkdir(parents=True)
    found = infer_toolchain("echo hi", str(sub))
    assert found["language"] == "go"
    assert found["detected_from"] == "file"


def test_infer_toolchain_marker_suffix_and_precedence(tmp_path):
    (tmp_path / "App.csproj").write_text("<Project/>")
    assert infer_toolchain("echo hi", str(tmp_path))["language"] == "dotnet"
    (tmp_path / "go.mod").write_text("module x")
    # the command wins over any marker
    assert infer_toolchain("cargo build", str(tmp_path))["language"] == "rust"


def test_infer_toolchain_stops_at_repo_root(tmp_path):
    outer = tmp_path / "outer"
    (outer / "pyproject.toml").parent.mkdir(parents=True)
    (outer / "pyproject.toml").write_text("[project]")
    repo = outer / "repo"
    (repo / ".git").mkdir(parents=True)
    sub = repo / "pkg"
    sub.mkdir()
    # must not escape the repo to the unrelated outer/pyproject.toml
    assert infer_toolchain("echo hi", str(sub)) is None


def test_infer_toolchain_stops_at_git_file(tmp_path):
    # git worktrees/submodules have `.git` as a file, not a directory
    outer = tmp_path / "outer"
    outer.mkdir()
    (outer / "requirements.txt").write_text("x")
    repo = outer / "repo"
    (repo / "pkg").mkdir(parents=True)
    (repo / ".git").write_text("gitdir: ../.git/modules/repo")
    assert infer_toolchain("echo hi", str(repo / "pkg")) is None


def test_infer_toolchain_ignores_home_dir(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    (home / "requirements.txt").write_text("x")
    monkeypatch.setenv("USERPROFILE", str(home))
    monkeypatch.setenv("HOME", str(home))
    assert infer_toolchain("echo hi", str(home)) is None
    (home / "proj").mkdir()
    assert infer_toolchain("echo hi", str(home / "proj")) is None


def test_frozen_launcher_routes_module_and_prog_name():
    assert resolve_target("C:\\x\\vanth.exe", ["-m", "vanth.runner", "h", "j", "s"]) == (
        "vanth.runner",
        ["h", "j", "s"],
    )
    assert resolve_target("/usr/bin/vanthd", []) == ("vanth.daemon", [])
    assert resolve_target("/usr/bin/vanth-monitor.exe", ["--json"]) == ("vanth.monitor", ["--json"])
    assert resolve_target("/usr/bin/vanth", ["list"]) == (None, ["list"])
    # an unknown -m is not swallowed; it falls through to the CLI
    assert resolve_target("/usr/bin/vanth", ["-m", "other"]) == (None, ["-m", "other"])


def test_clear_frozen_env_forces_fresh_extraction(monkeypatch):
    monkeypatch.setattr(launcher.sys, "frozen", True, raising=False)
    monkeypatch.setenv("_MEIPASS2", "C:/tmp/_MEI123")
    monkeypatch.delenv("PYINSTALLER_RESET_ENVIRONMENT", raising=False)
    launcher.clear_frozen_env()
    assert "_MEIPASS2" not in os.environ
    assert os.environ["PYINSTALLER_RESET_ENVIRONMENT"] == "1"


def test_clear_frozen_env_noop_when_not_frozen(monkeypatch):
    monkeypatch.delattr(launcher.sys, "frozen", raising=False)
    monkeypatch.delenv("PYINSTALLER_RESET_ENVIRONMENT", raising=False)
    launcher.clear_frozen_env()
    assert "PYINSTALLER_RESET_ENVIRONMENT" not in os.environ

