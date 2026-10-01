"""CLI onboarding surfaces a newcomer needs.

A blind usability test (an agent told only "use job_start/job_wait" but with no
MCP tools available) scored the CLI 2/5 and hit, in order: no per-command
`--help`, no `wait` command at all, and "unknown job" after it dropped one
character from a job id. These tests pin the fixes.
"""

import pytest

from vanth import cli
from vanth.server import _VANTH_CLI_SUBCOMMANDS

# Aliases and meta-commands that legitimately have no dedicated help entry.
_ALIASES = {"ps": "list", "tail": "logs", "help": None, "-h": None, "--help": None, "--version": None}


def test_every_dispatched_command_has_help():
    """Adding a command without help text is the bug this prevents: `--help` on a
    documented command used to be reported as "unknown option"."""
    missing = [
        name
        for name in _VANTH_CLI_SUBCOMMANDS
        if name not in _COMMAND_HELP_ALLOWED and name not in cli._COMMAND_HELP and _ALIASES.get(name, name) not in cli._COMMAND_HELP
    ]
    assert not missing, f"commands with no help entry: {sorted(missing)}"


_COMMAND_HELP_ALLOWED = {"help", "-h", "--help", "--version", "ps", "tail"}


@pytest.mark.parametrize("command", sorted(cli._COMMAND_HELP))
def test_per_command_help_exits_zero(command, capsys):
    assert cli.main([command, "--help"]) == 0
    out = capsys.readouterr().out
    assert f"usage: vanth {command}" in out


@pytest.mark.parametrize("command", sorted(cli._COMMAND_HELP))
def test_help_topic_matches_per_command_help(command, capsys):
    assert cli.main(["help", command]) == 0
    assert capsys.readouterr().out == cli._COMMAND_HELP[command]


def test_help_for_unknown_command_is_an_error(capsys):
    assert cli.main(["nonsense", "--help"]) == 2
    assert "no help available" in capsys.readouterr().err


def test_help_after_separator_is_not_consumed(monkeypatch, capsys):
    """`vanth start -- prog --help` must reach the child, so the flag only counts
    as help before a `--`."""
    seen: dict[str, list[str]] = {}
    monkeypatch.setattr(cli, "cmd_start", lambda argv, home, json_out=False: seen.setdefault("argv", argv) and 0 or 0)
    assert cli.main(["start", "--", "echo", "--help"]) == 0
    assert seen["argv"] == ["--", "echo", "--help"]
    assert "usage: vanth start" not in capsys.readouterr().out


def test_clean_text_strips_ansi_and_carriage_returns():
    """Delivery errors are stored verbatim from the client (`\\x1b[91m...Error:`)
    and job output is CRLF on Windows; both printed raw until this helper."""
    assert cli._clean_text("\x1b[91m\x1b[1mError:\x1b[0m Session not found") == "Error: Session not found"
    assert cli._clean_text("line one\r\nline two\r") == "line one\nline two"
    assert cli._clean_text(None) == ""


def test_global_flag_before_subcommand_still_routes_to_the_cli(monkeypatch):
    """`vanth --json list` is the documented global form. The entry-point gate
    only checked args[0], so it fell through to the MCP stdio server: a hang for
    an agent (non-tty stdin) and "unknown command '--json'" in a terminal."""
    import vanth.server as server_mod

    seen: dict[str, list[str]] = {}
    monkeypatch.setattr(cli, "main", lambda argv: seen.setdefault("argv", argv) and 0 or 0)
    with pytest.raises(SystemExit):
        server_mod.main(["--json", "list"])
    assert seen["argv"] == ["--json", "list"]


class _RecordingVanth:
    """Stands in for VanthClient so `vanth start` can be tested without a daemon."""

    captured: dict = {}

    def __init__(self, *_args, **_kwargs):
        pass

    def ensure(self):
        pass

    def post(self, path, payload, **_kwargs):
        _RecordingVanth.captured = {"path": path, **payload}
        return {"result": "ok", "job_id": "job_1", "status": "running"}

    def confirm_local_start(self, result):
        return result


def test_start_passes_the_extended_flags(monkeypatch, capsys):
    """`job_start` accepts priority/pool/tags/notes/secret_env/trigger/policy;
    the CLI fallback silently supported none of them."""
    monkeypatch.setattr(cli, "VanthClient", _RecordingVanth)
    rc = cli.main([
        "start", "--name", "n", "--priority", "7", "--pool", "p",
        "--tag", "a", "--tag", "b", "--notes", "x", "--secret-env", "T",
        "--trigger", '{"job_id": "j", "status": "completed"}',
        "--policy", '{"restart": {"max_retries": 2}}',
        "--env", "K=V", "--", "echo", "hi",
    ])
    assert rc == 0, capsys.readouterr().err
    captured = _RecordingVanth.captured
    assert captured["priority"] == 7
    assert captured["pool"] == "p"
    assert captured["tags"] == ["a", "b"]
    assert captured["notes"] == "x"
    assert captured["secret_env"] == ["T"]
    assert captured["trigger"] == {"job_id": "j", "status": "completed"}
    assert captured["policy"] == {"restart": {"max_retries": 2}}
    assert captured["env"] == {"K=V".split("=")[0]: "V"}
    assert captured["command"] == "echo hi"


def test_start_rejects_malformed_trigger_json(monkeypatch, capsys):
    monkeypatch.setattr(cli, "VanthClient", _RecordingVanth)
    assert cli.main(["start", "--trigger", "[1,2]", "--", "echo", "hi"]) == 2
    assert "expects a JSON object" in capsys.readouterr().err


def test_start_refuses_when_shell_operators_were_reassembled(monkeypatch, capsys):
    """The reported onboarding failure: a command with > and && arrived as
    separate argv elements, was reassembled, and died in cmd.exe."""
    monkeypatch.setattr(cli, "VanthClient", _RecordingVanth)
    assert cli.main(["start", "--", "cmd", "/c", "ping", "host", ">nul", "&&", "echo", "hi"]) == 2
    err = capsys.readouterr().err
    assert "refusing reassembled command" in err
    assert "run.cmd" in err  # points at the reliable workaround


def test_start_help_documents_every_real_flag():
    """Help that lists a flag the parser rejects is worse than no help."""
    help_text = cli._COMMAND_HELP["start"]
    for flag in (
        "--name", "--cwd", "--timeout", "--env", "--wake", "--interactive",
        "--priority", "--pool", "--tag", "--notes", "--secret-env", "--trigger", "--policy",
    ):
        assert flag in help_text, f"start help omits {flag}"


def test_top_level_help_mentions_help_topic_and_quoting():
    usage = cli._usage()
    assert "help <command>" in usage
    assert "windows quoting" in usage.lower()
    assert "job_wait" in usage  # the MCP -> CLI mapping


class _FakeClient:
    def __init__(self, job_ids):
        self.job_ids = job_ids

    def get(self, path, params=None, **_kwargs):
        return {"jobs": [{"job_id": job_id} for job_id in self.job_ids]}


def test_job_id_prefix_resolves_when_unambiguous():
    client = _FakeClient(["job_aaaabbbbcccc", "job_ddddeeeeffff"])
    assert cli._resolve_job_id(client, "job_aaaabbbbcccc") == ("job_aaaabbbbcccc", "")
    assert cli._resolve_job_id(client, "job_aaaa") == ("job_aaaabbbbcccc", "")


def test_job_id_ambiguity_and_near_miss_suggestions():
    client = _FakeClient(["job_aaaabbbbcccc", "job_aaaaccccdddd"])
    resolved, problem = cli._resolve_job_id(client, "job_aaaa")
    assert resolved is None
    assert "ambiguous" in problem

    # The observed failure mode: a truncated id, one character short.
    resolved, problem = cli._resolve_job_id(client, "job_aaaabbbbccc")
    assert resolved == "job_aaaabbbbcccc"

    resolved, problem = cli._resolve_job_id(client, "job_zzzz")
    assert resolved is None
    assert "unknown job" in problem


def test_job_id_resolution_degrades_when_listing_fails():
    class _Broken:
        def get(self, *_args, **_kwargs):
            raise RuntimeError("daemon down")

    assert cli._resolve_job_id(_Broken(), "job_x") == ("job_x", "")


def test_wake_registers_a_target_after_the_job_started(monkeypatch, capsys):
    """Adding a wake to an in-flight job is the documented MCP capability; the
    CLI must reach the same /wake route (and /wake-now with --now)."""
    monkeypatch.setattr(cli, "VanthClient", _RecordingVanth)
    rc = cli.main([
        "wake", "job_1", "--type", "opencode_thread", "--events", "completed",
        "--cwd", "F:\\git\\persistent-gdn", "--config", '{"session_id": "ses_x"}',
    ])
    assert rc == 0, capsys.readouterr().err
    captured = _RecordingVanth.captured
    assert captured["path"] == "/jobs/job_1/wake"
    assert captured["target"] == {
        "type": "opencode_thread",
        "events": ["completed"],
        "cwd": "F:\\git\\persistent-gdn",
        "session_id": "ses_x",
    }


def test_wake_now_posts_to_the_wake_now_route(monkeypatch, capsys):
    monkeypatch.setattr(cli, "VanthClient", _RecordingVanth)
    rc = cli.main(["wake", "job_1", "--now", "--type", "local_command",
                   "--config", '{"command": ["echo", "hi"]}'])
    assert rc == 0, capsys.readouterr().err
    assert _RecordingVanth.captured["path"] == "/jobs/job_1/wake-now"
    assert _RecordingVanth.captured["target"]["events"] == ["completed", "failed"]


def test_wake_requires_a_type_or_full_target(monkeypatch, capsys):
    monkeypatch.setattr(cli, "VanthClient", _RecordingVanth)
    assert cli.main(["wake", "job_1"]) == 2
    assert "--type is required" in capsys.readouterr().err


def test_json_options_accept_a_file_and_stdin(tmp_path):
    """PowerShell 5.1 strips the quotes from a JSON literal handed to a native
    executable, so `@path` and `-` (stdin) must work for the JSON options."""
    wake_file = tmp_path / "wake.json"
    wake_file.write_text('{"type": "opencode_thread", "events": ["completed"]}', encoding="utf-8")
    assert cli._load_json_object(f"@{wake_file}", "--wake") == {
        "type": "opencode_thread",
        "events": ["completed"],
    }
    assert cli._load_json_object('{"a": 1}', "--wake") == {"a": 1}
    with pytest.raises(ValueError, match="expects a JSON object"):
        cli._load_json_object("[1, 2]", "--wake")
    with pytest.raises(ValueError, match="cannot read"):
        cli._load_json_object("@/no/such/file.json", "--wake")


def test_start_reads_wake_json_from_a_file(monkeypatch, capsys, tmp_path):
    monkeypatch.setattr(cli, "VanthClient", _RecordingVanth)
    wake_file = tmp_path / "wake.json"
    wake_file.write_text('{"type": "opencode_thread", "events": ["completed"]}', encoding="utf-8")
    rc = cli.main(["start", "--name", "j", "--wake", f"@{wake_file}", "--", "echo", "hi"])
    assert rc == 0, capsys.readouterr().err
    assert _RecordingVanth.captured["wake_targets"] == [
        {"type": "opencode_thread", "events": ["completed"]}
    ]


def test_start_reads_trigger_json_from_stdin(monkeypatch, capsys):
    import io

    monkeypatch.setattr(cli, "VanthClient", _RecordingVanth)
    monkeypatch.setattr("sys.stdin", io.StringIO('{"job_id": "job_x", "status": "completed"}'))
    rc = cli.main(["start", "--trigger", "-", "--", "echo", "hi"])
    assert rc == 0, capsys.readouterr().err
    assert _RecordingVanth.captured["trigger"] == {"job_id": "job_x", "status": "completed"}


def test_exact_old_job_does_not_depend_on_recent_list():
    class OldClient:
        def get(self, path, params=None):
            assert path == "/jobs/job_old/status"
            return {"job_id": "job_old", "status": "completed"}
    assert cli._resolve_job_id(OldClient(), "job_old") == ("job_old", "")
