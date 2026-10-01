import os

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


def test_wake_me_forms(monkeypatch, tmp_path):
    monkeypatch.setattr(cli, "VanthClient", RecordingClient)
    assert cli.cmd_start(["--wake-me", "--", "echo", "ok"], tmp_path) == 0
    target = RecordingClient.payload["wake_targets"][0]
    assert target["type"] == "opencode_thread"
    assert target["events"] == ["completed", "failed", "timeout", "cancelled", "orphaned"]
    assert target["cwd"]  # pins relay resolution to the caller's directory

    assert cli.cmd_start(["--wake-me=checkpoint", "--", "echo", "ok"], tmp_path) == 0
    target = RecordingClient.payload["wake_targets"][0]
    assert target["type"] == "opencode_thread"
    assert target["events"] == ["checkpoint"]

    assert cli.cmd_start(["--cwd", "D:\\proj", "--wake-me", "--", "echo", "ok"], tmp_path) == 0
    assert RecordingClient.payload["wake_targets"][0]["cwd"] == "D:\\proj"


def test_wake_me_empty_is_usage_error(capsys, tmp_path):
    assert cli.cmd_start(["--wake-me=", "--", "echo", "ok"], tmp_path) == 2
    assert "non-empty comma-separated" in capsys.readouterr().err


def test_reassembled_shell_command_is_refused(monkeypatch, capsys, tmp_path):
    monkeypatch.setattr(cli, "VanthClient", RecordingClient)
    assert cli.cmd_start(["--", "echo", "one", "&&", "echo", "two"], tmp_path) == 2
    err = capsys.readouterr().err
    assert "refusing reassembled command" in err
    assert "ONE quoted string" in err
    assert ('echo one "&&" echo two' if os.name == "nt" else "echo one '&&' echo two") in err


def test_single_token_shell_command_is_accepted(monkeypatch, tmp_path):
    monkeypatch.setattr(cli, "VanthClient", RecordingClient)
    assert cli.cmd_start(["echo one && echo two"], tmp_path) == 0
    assert RecordingClient.payload["command"] == "echo one && echo two"


def test_operator_inside_a_token_is_not_refused(monkeypatch, tmp_path):
    """A `|`/`>` inside a larger argument (`rg "a|b"`, `-DFOO>bar`) is quoted as
    a whole by `_quote_for_cmd` and is safe; only a BARE operator token is the
    mangling signature."""
    monkeypatch.setattr(cli, "VanthClient", RecordingClient)
    assert cli.cmd_start(["--", "rg", "a|b", "file.txt"], tmp_path) == 0
    expected = 'rg "a|b" file.txt' if os.name == "nt" else "rg 'a|b' file.txt"
    assert RecordingClient.payload["command"] == expected


def test_bare_redirect_token_is_refused(monkeypatch, capsys, tmp_path):
    monkeypatch.setattr(cli, "VanthClient", RecordingClient)
    assert cli.cmd_start(["--", "cmd", "/c", "ping", "-n", "3", "host", ">nul", "&&", "echo", "done"], tmp_path) == 2
    assert "refusing reassembled command" in capsys.readouterr().err


def test_literal_angle_bracket_argument_is_not_refused(monkeypatch, tmp_path):
    """`<html>` is literal data, not an input redirect; only a token that IS the
    operator (or a `>` output redirect) is refused."""
    monkeypatch.setattr(cli, "VanthClient", RecordingClient)
    assert cli.cmd_start(["--", "curl", "-d", "<html>", "http://x"], tmp_path) == 0
    expected = 'curl -d "<html>" http://x' if os.name == "nt" else "curl -d '<html>' http://x"
    assert RecordingClient.payload["command"] == expected


def test_sleep_delegates_to_start(monkeypatch, tmp_path):
    seen = {}

    def fake_start(argv, home, *, json_out=False):
        seen.update(argv=argv, home=home, json_out=json_out)
        return 7

    monkeypatch.setattr(cli, "cmd_start", fake_start)
    assert cli.main(["sleep", "3"]) == 7
    assert seen["argv"][:6] == ["--name", "sleep-3s", "--timeout", "63", "--", cli.sys.executable]
    assert seen["argv"][-2:] == ["-c", "import time; time.sleep(3)"]


def test_sleep_requires_positive_integer(capsys, tmp_path):
    assert cli.cmd_sleep(["abc"], tmp_path) == 2
    assert cli.cmd_sleep([], tmp_path) == 2
    assert "positive number" in capsys.readouterr().err


def test_server_routes_every_cli_subcommand():
    """`vanth` is `server.main`, which only routes to the CLI for names in
    `_VANTH_CLI_SUBCOMMANDS`. A command the CLI dispatches but that set omits is
    unreachable (this is how `vanth sleep` shipped broken)."""
    import inspect
    import re

    from vanth import server

    src = inspect.getsource(cli.main)
    names = set(re.findall(r'command == "([A-Za-z0-9_-]+)"', src))
    for block in re.findall(r"command in \{([^}]*)\}", src, re.DOTALL):
        names |= set(re.findall(r'"([A-Za-z0-9_-]+)"', block))
    missing = names - server._VANTH_CLI_SUBCOMMANDS
    assert not missing, f"dispatched by cli.main but unreachable via `vanth`: {sorted(missing)}"
