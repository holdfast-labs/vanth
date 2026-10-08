"""Tests for the 4 agent-QoL features: metric_ingest, artifact_read, daemon_wake, cleanup_preview.

Uses the same patterns as test_agent_features.py: a direct JobManager instance
with a tmp_path home and real jobs started via manager.start(...).
"""

import asyncio
import base64
import json
import sys
import time

import pytest

from vanth.server import JobManager


import shellcmd


def cmd(code: str) -> str:
    return shellcmd.join([sys.executable, "-c", code])


def wait_event(manager: JobManager, job_id: str, event_type: str) -> dict:
    return asyncio.run(manager.wait(job_id, [event_type], timeout_seconds=10))


def start_job(manager, code, **kwargs):
    return manager.start(cmd(code), **kwargs)["job_id"]


def test_metric_ingest_records_series(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(manager, "import time; time.sleep(0.3)")
        wait_event(manager, job_id, "started")

        result = manager.metric_ingest(
            job_id,
            [
                {"name": "loss", "value": 0.5},
                {"name": "acc", "value": 0.9, "labels": {"split": "train"}},
                {"name": "step_loss", "value": 1.25, "ts_ms": 1_000_000_000_000},
            ],
        )
        assert result["result"] == "ok"
        assert result["job_id"] == job_id
        assert result["ingested"] == 3
        assert result["event_id"].startswith("evt_")

        series = manager.metrics_query(job_id)["series"]
        assert series["loss"][-1]["y"] == 0.5
        assert series["acc"][-1]["y"] == 0.9
        assert series["step_loss"][-1]["y"] == 1.25

        with_steps = manager.metrics_query(job_id, "step_loss")["series"]["step_loss"]
        assert with_steps[-1]["x"] == 1_000_000_000_000

        wait_event(manager, job_id, "completed")
    finally:
        manager.close()


def test_metric_ingest_idempotency_key_dedupes(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(manager, "import time; time.sleep(0.3)")
        wait_event(manager, job_id, "started")

        points = [{"name": "loss", "value": 0.5}]
        first = manager.metric_ingest(job_id, points, idempotency_key="run-1")
        second = manager.metric_ingest(job_id, points, idempotency_key="run-1")
        assert first["ingested"] == 1
        assert second["ingested"] == 0
        assert second["deduplicated"] is True

        third = manager.metric_ingest(job_id, points, idempotency_key="run-2")
        assert third["ingested"] == 1

        loss = manager.metrics_query(job_id, "loss")["series"]["loss"]
        assert len(loss) == 2

        wait_event(manager, job_id, "completed")
    finally:
        manager.close()


def test_metric_ingest_validation_errors(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(manager, "import time; time.sleep(0.3)")
        wait_event(manager, job_id, "started")

        with pytest.raises(ValueError, match="non-empty"):
            manager.metric_ingest(job_id, [{"name": "", "value": 1.0}])
        with pytest.raises(ValueError, match="finite"):
            manager.metric_ingest(job_id, [{"name": "loss", "value": float("nan")}])
        with pytest.raises(ValueError, match="at most 1000"):
            manager.metric_ingest(job_id, [{"name": f"m{i}", "value": 1.0} for i in range(1001)])
        with pytest.raises(ValueError, match="Unknown job_id"):
            manager.metric_ingest("job_missing", [{"name": "loss", "value": 1.0}])

        wait_event(manager, job_id, "completed")
    finally:
        manager.close()


def test_artifact_read_local_file_and_missing(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(manager, "import time; time.sleep(0.3)")
        wait_event(manager, job_id, "started")

        payload = {"epoch": 3, "weights": "0xdeadbeef"}
        blob = json.dumps(payload).encode()
        artifact_path = tmp_path / "checkpoint.json"
        artifact_path.write_bytes(blob)

        added = manager.artifact_add(job_id, name="chk", uri=str(artifact_path), kind="checkpoint", size_bytes=len(blob))
        read = manager.artifact_read(added["artifact_id"])
        assert read["name"] == "chk"
        assert read["kind"] == "checkpoint"
        assert read["uri"] == str(artifact_path)
        assert base64.b64decode(read["content_base64"]) == blob
        assert read["bytes_read"] == len(blob)
        assert read["truncated"] is False

        file_uri = manager.artifact_add(job_id, name="chk2", uri=artifact_path.as_uri())
        read_file = manager.artifact_read(file_uri["artifact_id"])
        assert base64.b64decode(read_file["content_base64"]) == blob

        with pytest.raises(ValueError, match="artifact content unavailable"):
            manager.artifact_read("art_missing")

        missing_path = tmp_path / "nope.bin"
        missing = manager.artifact_add(job_id, name="nope", uri=str(missing_path))
        with pytest.raises(ValueError, match="artifact content unavailable"):
            manager.artifact_read(missing["artifact_id"])

        wait_event(manager, job_id, "completed")
    finally:
        manager.close()


def test_artifact_read_truncation(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(manager, "import time; time.sleep(0.3)")
        wait_event(manager, job_id, "started")

        artifact_path = tmp_path / "big.bin"
        artifact_path.write_bytes(b"x" * 1000)
        added = manager.artifact_add(job_id, name="big", uri=str(artifact_path))
        read = manager.artifact_read(added["artifact_id"], max_bytes=256)
        assert read["bytes_read"] == 256
        assert read["truncated"] is True
        assert base64.b64decode(read["content_base64"]) == b"x" * 256

        with pytest.raises(ValueError, match="between 256 and"):
            manager.artifact_read(added["artifact_id"], max_bytes=50)

        wait_event(manager, job_id, "completed")
    finally:
        manager.close()


def test_artifact_read_http_gated_by_env(tmp_path, monkeypatch):
    import http.server
    import threading

    body = b"remote payload"
    server = http.server.HTTPServer(("127.0.0.1", 0), http.server.BaseHTTPRequestHandler)

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        uri = f"http://127.0.0.1:{server.server_port}/payload"
        manager = JobManager(tmp_path / "state")
        try:
            job_id = start_job(manager, "import time; time.sleep(0.3)")
            wait_event(manager, job_id, "started")
            added = manager.artifact_add(job_id, name="remote", uri=uri, kind="checkpoint")
            with pytest.raises(ValueError, match="http.*disabled"):
                manager.artifact_read(added["artifact_id"])
            monkeypatch.setenv("VANTH_ALLOW_HTTP_ARTIFACT_READ", "1")
            read = manager.artifact_read(added["artifact_id"])
            assert base64.b64decode(read["content_base64"]) == body
            wait_event(manager, job_id, "completed")
        finally:
            manager.close()
    finally:
        server.shutdown()
        thread.join()


def test_daemon_wake_enqueues_delivery_on_completion(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(manager, "print('wake me')")
        result = manager.add_wake_target(
            job_id,
            {"type": "local_command", "events": ["completed"], "command": [sys.executable, "-c", "import sys; sys.exit(0)"]},
        )
        assert result["result"] == "ok"
        assert result["job_id"] == job_id
        assert result["target_id"].startswith("target_")
        assert result["target_type"] == "local_command"
        assert result["events"] == ["completed"]

        wait_event(manager, job_id, "completed")
        deadline = time.monotonic() + 10
        deliveries = []
        while time.monotonic() < deadline:
            deliveries = manager.deliveries(job_id)["deliveries"]
            if any(d["target_type"] == "local_command" for d in deliveries):
                break
            time.sleep(0.05)
        assert any(d["target_type"] == "local_command" for d in deliveries)
    finally:
        manager.close()


def test_daemon_wake_default_events_and_errors(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(manager, "import time; time.sleep(0.3)")
        wait_event(manager, job_id, "started")

        result = manager.add_wake_target(job_id, {"type": "local_command", "command": [sys.executable, "-c", "import sys; sys.exit(0)"]})
        assert result["events"] == ["completed", "failed"]

        with pytest.raises(ValueError, match="unsupported wake target type"):
            manager.add_wake_target(job_id, {"type": "", "url": "http://x/"})
        with pytest.raises(ValueError, match="list of strings"):
            manager.add_wake_target(job_id, {"type": "local_command", "events": "completed"})
        with pytest.raises(ValueError, match="unsupported wake target type"):
            manager.add_wake_target(job_id, {"type": "http", "events": ["completed"]})
        with pytest.raises(ValueError, match="Unknown job_id"):
            manager.add_wake_target("job_missing", {"type": "local_command", "command": [sys.executable, "-c", "import sys; sys.exit(0)"]})

        wait_event(manager, job_id, "completed")
    finally:
        manager.close()


def test_wake_now_enqueues_immediate_delivery_after_completion(tmp_path):
    """Review P0-1: daemon_wake historically only registered a target for a
    FUTURE event; calling it after a job completed did nothing. wake_now is the
    genuine immediate-wake operation: it enqueues a delivery right away even
    though the triggering event already fired."""
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(manager, "print('done already')")
        wait_event(manager, job_id, "completed")

        result = manager.wake_now(
            job_id,
            {"type": "local_command", "events": ["completed"], "command": [sys.executable, "-c", "import sys; sys.exit(0)"]},
        )
        assert result["result"] == "ok"
        assert result["woken"] is True
        assert result["synthetic_event_type"] == "wake_now"

        deadline = time.monotonic() + 10
        deliveries = []
        while time.monotonic() < deadline:
            deliveries = manager.deliveries(job_id)["deliveries"]
            if any(d["target_type"] == "local_command" and d["status"] in ("pending", "delivered", "retrying", "failed") for d in deliveries):
                break
            time.sleep(0.05)
        assert any(
            d["target_type"] == "local_command"
            for d in deliveries
        ), "wake_now must create a delivery even after the event already fired"
    finally:
        manager.close()


def test_wake_now_opencode_requires_session_or_registered_relay(tmp_path):
    """Review P1-1: OpenCode cannot auto-inherit a session id. A target without an
    explicit id is resolved from a live plugin relay for the job's directory
    (tests/test_opencode_relay.py); with no relay registered it must be rejected."""
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(manager, "print('x')")
        with pytest.raises(ValueError, match="requires session_id"):
            manager.wake_now(job_id, {"type": "opencode_thread", "events": ["completed"]})
        # ``attach`` is OPTIONAL now (a plain TUI exposes no server URL): without
        # it the delivery goes to the in-process plugin relay, and a session id
        # alone is a complete address.
        result = manager.wake_now(
            job_id, {"type": "opencode_thread", "events": ["completed"], "session_id": "ses_1"}
        )
        assert result["woken"] is True
    finally:
        manager.close()


def test_cleanup_preview_lists_without_deleting(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(manager, "print('clean me')", name="cleanme")
        wait_event(manager, job_id, "completed")

        with manager.db_lock:
            manager.db.execute("UPDATE jobs SET updated_at=? WHERE job_id=?", ("2000-01-01T00:00:00Z", job_id))
            manager.db.commit()

        preview = manager.cleanup_preview(0)
        assert preview["dry_run"] is True
        assert preview["older_than_seconds"] == 0
        assert preview["count"] >= 1
        entry = next(j for j in preview["jobs"] if j["job_id"] == job_id)
        assert entry["name"] == "cleanme"
        assert entry["status"] == "completed"
        assert entry["updated_at"] == "2000-01-01T00:00:00Z"
        assert entry["stdout_path"].endswith(".stdout.log")
        assert entry["stderr_path"].endswith(".stderr.log")
        assert entry["events_path"].endswith(".jsonl")

        assert manager.status(job_id)["status"] == "completed"

        assert manager.cleanup_preview(0)["count"] >= 1
    finally:
        manager.close()


def test_cleanup_preview_validation(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        with pytest.raises(ValueError, match="non-negative"):
            manager.cleanup_preview(-1)
        with pytest.raises(ValueError, match="non-negative"):
            manager.cleanup_preview(True)
    finally:
        manager.close()
