import importlib.util
import json
from pathlib import Path

import pytest

from vanth import cli
from vanth.client import VanthClient


def test_preview_and_start_failure(monkeypatch, tmp_path, capsys):
    class Client:
        def __init__(self, **kwargs):
            pass
        def ensure(self):
            pass
        def post(self, path, payload):
            assert payload["idempotency_key"] == "retry-1"
            if path == "/jobs/preview":
                return {"result": "preview", "cwd": str(tmp_path), "command": "echo hi"}
            return {"job_id": "job_failed", "status": "failed"}
        def confirm_local_start(self, result):
            result["startup_confirmed"] = False
            return result
    monkeypatch.setattr(cli, "VanthClient", Client)
    assert cli.cmd_start(["--dry-run", "--idempotency-key", "retry-1", "echo hi"], tmp_path) == 0
    assert json.loads(capsys.readouterr().out)["result"] == "preview"
    assert cli.cmd_start(["--idempotency-key", "retry-1", "echo hi"], tmp_path, json_out=True) == 1
    assert json.loads(capsys.readouterr().out)["job_id"] == "job_failed"


def test_confirmation_exposes_failure_reason(monkeypatch, tmp_path):
    client = VanthClient(home=tmp_path)
    monkeypatch.setattr(client, "post", lambda *a, **kw: {"result": "event", "status": "failed", "event": {"type": "failed"}})
    result = client.confirm_local_start({"job_id": "job_failure", "status": "launching"})
    assert result["startup_confirmed"] is False
    assert result["failure_reason"] == "startup_failed"
    assert "job_failure" in result["recommended_next_action"]


def test_confirmation_does_not_mislabel_launched_workload_failure(monkeypatch, tmp_path):
    client = VanthClient(home=tmp_path)
    monkeypatch.setattr(client, "post", lambda *a, **kw: {
        "result": "event", "status": "failed", "event": {"type": "started"}})
    result = client.confirm_local_start({"job_id": "job_failure", "status": "launching"})
    assert result["startup_confirmed"] is True
    assert result["failure_reason"] == "workload_failed"
    replay = client.confirm_local_start({"job_id": "job_failure", "status": "failed", "idempotent_replay": True})
    assert replay["failure_reason"] == "job_failed"


def test_confirmation_preserves_specific_daemon_failure_guidance(monkeypatch, tmp_path):
    client = VanthClient(home=tmp_path)
    monkeypatch.setattr(client, "get", lambda *a, **kw: {
        "failure_reason": "log_capture_failed", "recommended_next_action": "Check disk space"})
    result = client.confirm_local_start({"job_id": "job_failure", "status": "failed"})
    assert result["failure_reason"] == "log_capture_failed"
    assert result["recommended_next_action"] == "Check disk space"


def test_adapter_smoke_requires_assistant_roundtrip(monkeypatch):
    spec = importlib.util.spec_from_file_location("adapter_smoke", Path(__file__).parents[1] / "scripts" / "real_adapter_smoke.py")
    smoke = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(smoke)
    delivery = "smoke123"
    smoke.assert_reply({"turn": {"items": [{"type": "agentMessage", "text": delivery}]}}, delivery, "codex")
    smoke.assert_reply({"stdout": json.dumps({"type": "text", "part": {"text": delivery}})}, delivery, "opencode")
    for adapter, result in [("codex", {"turn": {"items": [{"type": "userMessage", "text": delivery}]}}), ("opencode", {"stdout": delivery}), ("codex", {"turn": {"items": [{"type": "agentMessage", "text": "wrong"}]}})]:
        with pytest.raises(RuntimeError, match="delivery_id"):
            smoke.assert_reply(result, delivery, adapter)
    monkeypatch.delenv("VANTH_SMOKE_CODEX_THREAD", raising=False)
    monkeypatch.delenv("VANTH_SMOKE_OPENCODE_SESSION", raising=False)
    monkeypatch.setattr(smoke.sys, "argv", ["smoke", "--json"])
    monkeypatch.setattr(smoke, "probe_version", lambda command: "test")
    monkeypatch.setattr(smoke, "smoke_codex", lambda *a: pytest.fail("unexpected live send"))
    monkeypatch.setattr(smoke, "smoke_opencode", lambda *a: pytest.fail("unexpected live send"))
    assert smoke.main() == 0


def test_rerun_confirms_and_returns_failure(monkeypatch, tmp_path, capsys):
    class Client:
        def __init__(self, **kwargs):
            pass
        def ensure(self):
            pass
        def get(self, path, params=None):
            return {"job_id": "job_old"}
        def post(self, path, payload):
            assert path == "/jobs/job_old/rerun"
            return {"job_id": "job_retry", "status": "launching"}
        def confirm_local_start(self, result):
            return {**result, "status": "failed", "startup_confirmed": False}
    monkeypatch.setattr(cli, "VanthClient", Client)
    assert cli.cmd_rerun(["job_old"], tmp_path, json_out=True) == 1
    assert json.loads(capsys.readouterr().out)["job_id"] == "job_retry"
