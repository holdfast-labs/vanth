import asyncio
import os
import socket
import subprocess
import sys
import time

import pytest

from vanth.client import VanthClient
from vanth.server import JobManager


import shellcmd


def cmd(code: str) -> str:
    return shellcmd.join([sys.executable, "-c", code])


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_event(manager: JobManager, job_id: str, event_type: str, timeout=15):
    return asyncio.run(manager.wait(job_id, [event_type], timeout_seconds=timeout))


@pytest.fixture()
def manager(tmp_path):
    m = JobManager(tmp_path / "state")
    yield m
    m.close()


def start_metric_job(manager: JobManager) -> str:
    """Start a job that emits metric + progress events."""
    code = (
        "import sys, time; sys.path.insert(0, 'src'); "
        "from vanth.agent_events import agent_event, progress; "
        "[(agent_event('metric', _step=i, loss=1.0/i+1, acc=i*0.1), "
        "  progress(i, 10, unit='epoch', stage='train'), time.sleep(0.1)) for i in range(1, 5)]"
    )
    started = manager.start(cmd(code), name="metric demo")
    job_id = started["job_id"]
    wait_event(manager, job_id, "completed", timeout=20)
    return job_id


def test_metric_series_persisted(manager):
    job_id = start_metric_job(manager)
    q = manager.metrics_query(job_id)
    assert "loss" in q["metrics"]
    assert "progress.percent" in q["metrics"]
    loss = q["series"]["loss"]
    assert len(loss) == 4
    # x follows _step: 1..4
    assert [p["x"] for p in loss] == [1.0, 2.0, 3.0, 4.0]
    assert loss[-1]["y"] == pytest.approx(1.25)


def test_metrics_query_metric_filter(manager):
    job_id = start_metric_job(manager)
    q = manager.metrics_query(job_id, metric="acc")
    assert q["metrics"] == ["acc"]
    assert len(q["series"]["acc"]) == 4


def test_metric_compare_aggregations(manager):
    job_id = start_metric_job(manager)
    for agg, expected in (("latest", 1.25), ("mean", (2.0 + 1.5 + 1.3333 + 1.25) / 4), ("count", 4)):
        res = manager.metric_compare([job_id], "loss", agg)
        assert res["jobs"][job_id]["value"] == pytest.approx(expected, abs=1e-3)
    res = manager.metric_compare([job_id], "loss", "max")
    assert res["jobs"][job_id]["value"] == pytest.approx(2.0)


def test_metric_compare_unknown_job_raises(manager):
    with pytest.raises(ValueError, match="Unknown job_id"):
        manager.metric_compare(["job_nope"], "loss", "latest")


def test_run_summary(manager):
    job_id = start_metric_job(manager)
    summary = manager.run_summary(job_id)
    assert summary["job_id"] == job_id
    assert summary["status"] == "completed"
    assert summary["progress"]["percent"] == pytest.approx(40.0)
    metric_names = {m["metric"] for m in summary["metrics"]}
    assert {"loss", "acc", "progress.percent"} <= metric_names


def test_artifact_add_and_list(manager):
    job_id = start_metric_job(manager)
    art = manager.artifact_add(job_id, "best.pt", "file:///tmp/best.pt", kind="checkpoint",
                               size_bytes=123, sha256="abc", meta={"epoch": 4})
    assert art["artifact_id"].startswith("art_")
    arts = manager.artifacts(job_id)
    assert len(arts["artifacts"]) == 1
    assert arts["artifacts"][0]["name"] == "best.pt"
    assert arts["artifacts"][0]["meta"] == {"epoch": 4}
    # run_summary includes artifacts
    summary = manager.run_summary(job_id)
    assert [a["name"] for a in summary["artifacts"]] == ["best.pt"]


def test_dashboard_returns_series(manager):
    job_id = start_metric_job(manager)
    dash = manager.dashboard([job_id])
    assert dash["series_count"] == 5
    assert "loss" in dash["series"][job_id]
    # downsample: series length preserved under limit
    assert len(dash["series"][job_id]["loss"]) == 4


def test_schema_version_is_current(manager):
    from vanth.migrations import LATEST_SCHEMA_VERSION

    assert manager.doctor()["schema_version"] == LATEST_SCHEMA_VERSION


def test_progress_derived_series(manager):
    job_id = start_metric_job(manager)
    q = manager.metrics_query(job_id, metric="progress.current")
    assert len(q["series"]["progress.current"]) == 4
    assert [p["y"] for p in q["series"]["progress.current"]] == [1.0, 2.0, 3.0, 4.0]


def test_daemon_telemetry_http_routes(tmp_path):
    port = free_port()
    proc = subprocess.Popen(
        [sys.executable, "-m", "vanth.daemon"],
        env={**os.environ, "VANTH_HOME": str(tmp_path / "state"), "VANTH_DAEMON_PORT": str(port)},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    client = VanthClient(f"http://127.0.0.1:{port}", tmp_path / "state")
    try:
        deadline = time.monotonic() + 5
        while True:
            try:
                assert client.get("/health") == {"ok": True}
                break
            except Exception:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.05)

        # Start a metric-emitting job via HTTP.
        code = (
            "import sys; sys.path.insert(0, 'src'); "
            "from vanth.agent_events import agent_event; "
            "[agent_event('metric', _step=i, loss=1.0/i+1) for i in range(1, 4)]"
        )
        job = client.post("/jobs", {"command": shellcmd.join([sys.executable, "-c", code])})
        job_id = job["job_id"]
        client.post(f"/jobs/{job_id}/wait", {"filters": ["completed"], "timeout_seconds": 20})

        # GET /jobs/{id}/metrics
        metrics = client.get(f"/jobs/{job_id}/metrics", {"metric": "loss"})
        assert metrics["metrics"] == ["loss"]
        assert len(metrics["series"]["loss"]) == 3

        # GET /jobs/{id}/summary
        summary = client.get(f"/jobs/{job_id}/summary")
        assert summary["job_id"] == job_id
        assert summary["status"] == "completed"

        # GET /metrics/compare
        cmp = client.get("/metrics/compare", {"job_ids": [job_id], "metric": "loss", "aggregation": "max"})
        assert cmp["jobs"][job_id]["value"] == pytest.approx(2.0)

        # POST /jobs/{id}/artifacts + GET
        art = client.post(f"/jobs/{job_id}/artifacts", {"name": "model.pt", "uri": "file:///t/model.pt"})
        assert art["artifact_id"].startswith("art_")
        arts = client.get(f"/jobs/{job_id}/artifacts")
        assert [a["name"] for a in arts["artifacts"]] == ["model.pt"]

        # GET /dashboard
        dash = client.get("/dashboard", {"job_ids": [job_id]})
        assert "loss" in dash["series"][job_id]
    finally:
        proc.terminate()
        proc.wait(timeout=5)
