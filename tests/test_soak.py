"""Cheap harness regressions: no background jobs or production state."""
import contextlib
import json
from types import SimpleNamespace

import pytest
import importlib.util
from pathlib import Path

_spec = importlib.util.spec_from_file_location("vanth_soak", Path(__file__).resolve().parents[1] / "scripts" / "soak.py")
soak = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(soak)


def rows(count=3):
    result = [{"seq": 1, "type": "started", "data_json": "{}"}]
    for i in range(count):
        result.append({"seq": i + 2, "type": "metric", "data_json": json.dumps({"i": i, "sent_ns": soak.time.time_ns()})})
    result.append({"seq": count + 2, "type": "completed", "data_json": "{}"})
    return result


def test_validate_rejects_missing_duplicate_payload_and_sequences():
    soak.validate_events(rows(), 3)
    missing = rows()
    del missing[2]
    with pytest.raises(AssertionError, match="sequences"):
        soak.validate_events(missing, 3)
    duplicate = rows()
    duplicate[2]["data_json"] = duplicate[1]["data_json"]
    with pytest.raises(AssertionError, match="completeness"):
        soak.validate_events(duplicate, 3)
    duplicate_seq = rows()
    duplicate_seq[-1]["seq"] -= 1
    with pytest.raises(AssertionError, match="sequences"):
        soak.validate_events(duplicate_seq, 3)
    missing_completed = rows()[:-1]
    with pytest.raises(AssertionError, match="completed"):
        soak.validate_events(missing_completed, 3)


def test_distribution_small_samples_and_empty():
    assert soak.distribution([])["max"] is None
    assert soak.distribution([3, 1, 2]) == {"samples": 3, "p50": 2, "p95": 3, "max": 3}


class FakeProcess:
    def __init__(self):
        self.done = False
        self.pid = 123

    def poll(self):
        return 0 if self.done else None

    def wait(self, timeout):
        assert self.done
        return 0


class FakeManager:
    def __init__(self, home, recover=False):
        self.processes = {}
        self.db_lock = contextlib.nullcontext()
        self.db = self
        self.closed = False

    async def start(self, command, **kwargs):
        job_id = "own-job"
        self.processes[job_id] = FakeProcess()
        return {"job_id": job_id}

    def execute(self, query, params):
        return SimpleNamespace(fetchall=lambda: rows())

    def status(self, job_id):
        self.processes[job_id].done = True
        return {"status": "completed"}

    def close(self):
        self.closed = True


def fake_clock(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(soak.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(soak.time, "sleep", lambda duration: clock.__setitem__(0, clock[0] + duration))


def test_success_report_and_automatic_isolated_home_removal(monkeypatch, tmp_path, capsys):
    fake_clock(monkeypatch)
    monkeypatch.setattr(soak, "JobManager", FakeManager)
    monkeypatch.setattr(soak, "rss_bytes", lambda pid: 100)
    report_path = tmp_path / "report.json"
    assert soak.main(["--duration", ".01", "--jobs", "1", "--events", "3", "--report", str(report_path)]) == 0
    report = json.loads(report_path.read_text())
    assert report["result"] == "pass"
    assert report["jobs_verified"] == 1
    assert report["events_verified"] == 3
    assert report["event_observation_lag_seconds"]["samples"] == 3
    assert report["rss_supported"] is True
    assert report["home_removed"] is True
    assert json.loads(capsys.readouterr().out)["result"] == "pass"


def test_rss_samples_published_worker_instead_of_windows_launcher(monkeypatch, tmp_path):
    fake_clock(monkeypatch)

    class RunningManager(FakeManager):
        def status(self, job_id):
            if soak.time.monotonic() < .02:
                return {"status": "running", "worker_pid": 456}
            return super().status(job_id)

    sampled = []
    monkeypatch.setattr(soak, "JobManager", RunningManager)
    monkeypatch.setattr(soak, "rss_bytes", lambda pid: sampled.append(pid) or 100)
    assert soak.main(["--duration", ".01", "--jobs", "1", "--events", "3",
                      "--report", str(tmp_path / "rss.json")]) == 0
    assert sampled == [soak.os.getpid(), 456]


def test_fail_report_nonzero_and_state_preservation(monkeypatch, tmp_path, capsys):
    from pathlib import Path
    import shutil

    class FailedManager:
        def __init__(self, *args, **kwargs):
            raise OSError("injected disk full")

    monkeypatch.setattr(soak, "JobManager", FailedManager)
    report_path = tmp_path / "failure.json"
    try:
        assert soak.main(["--duration", ".01", "--report", str(report_path)]) == 1
        report = json.loads(report_path.read_text())
        assert "disk full" in report["errors"][0]
        assert report["home_removed"] is False
        assert Path(report["home"]).is_dir()
    finally:
        if report_path.exists():
            shutil.rmtree(json.loads(report_path.read_text())["home"], ignore_errors=True)


def test_cleanup_only_live_owned_process_handles():
    finished, live = FakeProcess(), FakeProcess()
    finished.done = True
    calls = []

    def stop(job_id, **kwargs):
        calls.append(job_id)
        live.done = True
        assert kwargs["actor"] == "tool"

    manager = SimpleNamespace(stop_sync=stop)
    assert soak.cleanup_owned(manager, {"finished": finished, "live": live}) == []
    assert calls == ["live"]


@pytest.mark.parametrize("args", [["--duration", "nan"], ["--jobs", "0"], ["--interval", "-1"]])
def test_invalid_configuration_never_starts_manager(args):
    with pytest.raises(SystemExit) as result:
        soak.main(args)
    assert result.value.code == 2


@pytest.mark.parametrize("flags,expected", [
    (["--max-runtime", "0"], "latency"),
    (["--max-lag", "0"], "lag"),
    (["--max-memory-growth", "0"], "unavailable"),
])
def test_requested_limits_fail_with_report(monkeypatch, tmp_path, flags, expected):
    import shutil

    fake_clock(monkeypatch)
    monkeypatch.setattr(soak, "JobManager", FakeManager)
    monkeypatch.setattr(soak, "rss_bytes", lambda pid: None)
    if expected == "latency":
        original = FakeManager.status
        def slow_status(self, job_id):
            soak.time.sleep(.001)
            return original(self, job_id)
        monkeypatch.setattr(FakeManager, "status", slow_status)
    if expected == "lag":
        def delayed_rows(self, query, params):
            values = rows()
            for row in values:
                if row["type"] == "metric":
                    data = json.loads(row["data_json"])
                    data["sent_ns"] -= 1_000_000_000
                    row["data_json"] = json.dumps(data)
            return SimpleNamespace(fetchall=lambda: values)
        monkeypatch.setattr(FakeManager, "execute", delayed_rows)
    report_path = tmp_path / "limited.json"
    try:
        assert soak.main(["--duration", ".01", "--jobs", "1", "--events", "3",
                          "--report", str(report_path)] + flags) == 1
        report = json.loads(report_path.read_text())
        assert any(expected in error for error in report["errors"])
        assert report["home_removed"] is False
    finally:
        if report_path.exists():
            shutil.rmtree(json.loads(report_path.read_text())["home"], ignore_errors=True)


def test_cleanup_failure_changes_report_to_fail(monkeypatch, tmp_path):
    import shutil

    fake_clock(monkeypatch)
    monkeypatch.setattr(soak, "JobManager", FakeManager)
    monkeypatch.setattr(soak, "rss_bytes", lambda pid: None)
    monkeypatch.setattr(soak, "cleanup_owned", lambda *args: ["injected cleanup failure"])
    report_path = tmp_path / "cleanup.json"
    try:
        assert soak.main(["--duration", ".01", "--jobs", "1", "--events", "3",
                          "--report", str(report_path)]) == 1
        report = json.loads(report_path.read_text())
        assert report["errors"] == ["injected cleanup failure"]
    finally:
        if report_path.exists():
            shutil.rmtree(json.loads(report_path.read_text())["home"], ignore_errors=True)


def test_real_short_soak_emits_all_events_on_platform_shell(tmp_path, capsys):
    report_path = tmp_path / "real-soak.json"
    assert soak.main(["--duration", "2", "--jobs", "1", "--events", "3", "--interval", "0",
                      "--report", str(report_path)]) == 0
    report = json.loads(report_path.read_text())
    assert report["result"] == "pass"
    assert report["jobs_started"] == report["jobs_verified"] >= 1
    assert report["events_verified"] == report["jobs_verified"] * 3
    assert report["event_observation_lag_seconds"]["samples"] == report["events_verified"]
    assert not report["own_runner_leaks"]
    assert report["home_removed"] is True
    assert json.loads(capsys.readouterr().out)["result"] == "pass"
