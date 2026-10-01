"""CLI input should preserve MCP send semantics and support line input."""

import io

from vanth import cli


class RecordingClient:
    payload = None

    def __init__(self, *, home):
        pass

    def ensure(self):
        pass

    def get(self, path, params):
        return {"jobs": [{"job_id": "job_123456"}]}

    def post(self, path, payload):
        assert path == "/jobs/job_123456/send"
        RecordingClient.payload = payload
        return {"sent": len(payload["input"].encode()), "eof": payload["eof"]}


def test_send_raw_line_stdin_and_eof(monkeypatch, tmp_path):
    monkeypatch.setattr(cli, "VanthClient", RecordingClient)
    assert cli.cmd_send(["job_123", "hello"], tmp_path) == 0
    assert RecordingClient.payload == {"input": "hello", "eof": False}

    assert cli.cmd_send(["job_123", "--line", "hello"], tmp_path) == 0
    assert RecordingClient.payload == {"input": "hello\n", "eof": False}

    monkeypatch.setattr(cli.sys, "stdin", io.StringIO("from stdin\n"))
    assert cli.cmd_send(["job_123", "--eof", "-"], tmp_path) == 0
    assert RecordingClient.payload == {"input": "from stdin\n", "eof": True}

    assert cli.cmd_send(["job_123", "--eof"], tmp_path) == 0
    assert RecordingClient.payload == {"input": "", "eof": True}


def test_send_requires_id_and_input(capsys, tmp_path):
    assert cli.cmd_send([], tmp_path) == 2
    assert cli.cmd_send(["job_123"], tmp_path) == 2
    assert "missing input" in capsys.readouterr().err


def test_send_daemon_error(monkeypatch, capsys, tmp_path):
    class ErrorClient(RecordingClient):
        def post(self, path, payload):
            return {"result": "error", "error": "not interactive"}

    monkeypatch.setattr(cli, "VanthClient", ErrorClient)
    assert cli.cmd_send(["job_123", "hello"], tmp_path) == 1
    assert "not interactive" in capsys.readouterr().err


def test_send_empty_line_and_literal_option(monkeypatch, tmp_path):
    monkeypatch.setattr(cli, "VanthClient", RecordingClient)
    assert cli.cmd_send(["job_123", "--line", ""], tmp_path) == 0
    assert RecordingClient.payload == {"input": "\n", "eof": False}
    assert cli.main(["send", "job_123", "--", "--json"]) == 0
    assert RecordingClient.payload == {"input": "--json", "eof": False}
    assert cli.cmd_send(["job_123", "--", "-"], tmp_path) == 0
    assert RecordingClient.payload == {"input": "-", "eof": False}
