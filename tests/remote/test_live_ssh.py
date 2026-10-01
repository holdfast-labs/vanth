"""Real SSH is explicitly opt-in; fake injection checks always run."""
import importlib.util
import json
import os
from pathlib import Path

import pytest


def harness():
    spec = importlib.util.spec_from_file_location("real_ssh_smoke", Path(__file__).parents[2] / "scripts" / "real_ssh_smoke.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_unconfigured_smoke_never_contacts_ssh(monkeypatch):
    module = harness()
    for key in module.ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(module.ssh, "fetch_host_keys", lambda *a: pytest.fail("unexpected SSH access"))
    assert module.run_live()["result"] == "skipped"


def test_partial_configuration_fails_before_ssh(monkeypatch):
    module = harness()
    for key in module.ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv(module.ENV_KEYS[0], "user@disposable")
    monkeypatch.setattr(module.ssh, "fetch_host_keys", lambda *a: pytest.fail("unexpected SSH access"))
    with pytest.raises(ValueError, match="Configured target requires"):
        module.run_live()


def test_loss_injection_discards_only_first_real_acceptance(monkeypatch, tmp_path):
    module = harness()
    reply = json.dumps({"version": "1", "kind": "response", "request_id": "req_smoke",
                        "method": "job.start", "result": {"job_id": "job_once"}, "sent_at": "2026-09-30T00:00:00Z"})
    class Session:
        def exchange(self, frame):
            return reply
    monkeypatch.setattr(module.DefaultSessionTransport, "open_session", lambda *a, **kw: Session())
    transport = module.DropAcceptedResponse()
    session = transport.open_session({}, home=tmp_path)
    request = json.dumps({"version": "1", "kind": "request", "request_id": "req_smoke", "method": "job.start", "payload": {"command": "echo smoke"}, "idempotency_key": "smoke-key", "sent_at": "2026-09-30T00:00:00Z"}).encode()
    assert session.exchange(request) is None
    assert transport.accepted_job_id == "job_once"
    assert session.exchange(request) == reply


@pytest.mark.skipif(not os.environ.get("VANTH_SMOKE_SSH_TARGET"), reason="No explicit disposable SSH target configured")
def test_real_ssh_start_reconnect_replay_and_artifacts():
    assert harness().run_live()["result"] == "passed"
