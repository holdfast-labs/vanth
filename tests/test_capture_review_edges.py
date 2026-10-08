"""Independent capture review edges; isolated state, no load workload."""
import io
import json
import sqlite3
from pathlib import Path

import pytest

from vanth.server import JobManager, normalize_event_payload, now_iso


@pytest.fixture
def manager(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    timestamp = now_iso()
    manager.db.execute(
        "INSERT INTO jobs(job_id,command,status,run_json,created_at,updated_at,stdout_path,stderr_path,events_path) "
        "VALUES ('job_review','unused','running',?, ?, ?, ?, ?, ?)",
        (json.dumps({"interactive": True}), timestamp, timestamp,
         str(manager.logs / "review.stdout"), str(manager.logs / "review.stderr"),
         str(manager.events_dir / "review.jsonl")))
    manager.db.commit()
    try:
        yield manager
    finally:
        manager.close()


def test_public_event_is_available_before_next_blocking_read_with_secret_mask(manager, monkeypatch):
    captured = []
    observed = []
    monkeypatch.setattr(manager, "_emit_capture_batch", lambda job_id, values, source: captured.extend(values))
    chunks = [b'AGENT_EVENT {"type":"progress","data":{"current":1}}\n', b'']

    class Stream:
        def read1(self, count):
            if len(chunks) == 1:
                observed.append(bool(captured))
            return chunks.pop(0)
        read = read1

    manager._read_stream("job_review", Stream(), manager.logs / "review.stdout", "stdout", ["s" * 100])
    assert observed == [True], "a public complete event must not wait for more bytes or stream EOF"


def test_oversized_event_without_final_newline_records_rejection(manager):
    manager.max_event_line_bytes = 48
    manager._read_stream("job_review", io.BytesIO(b"AGENT_EVENT " + b"x" * 100),
                         manager.logs / "review.stdout", "stdout")
    types = [row[0] for row in manager.db.execute("SELECT type FROM events WHERE job_id='job_review'")]
    assert "event_rejected" in types


def test_secret_with_existing_crlf_is_masked_after_windows_text_translation(manager):
    stream = io.BytesIO(b"before first\r\r\nsecond after\n")
    path = manager.logs / "review.stdout"
    manager._read_stream("job_review", stream, path, "stdout", ["first\r\nsecond"])
    assert path.read_bytes() == b"before *** after\n"


def test_large_numeric_metric_cannot_discard_other_captured_events(manager):
    payloads = [
        {"type": "metric", "data": {"safe": 1}},
        {"type": "metric", "data": {"huge": 10 ** 400}},
    ]
    stream = io.BytesIO(b"".join(b"AGENT_EVENT " + json.dumps(value).encode() + b"\n" for value in payloads))
    manager._read_stream("job_review", stream, manager.logs / "review.stdout", "stdout")
    assert manager.db.execute("SELECT COUNT(*) FROM events WHERE job_id='job_review'").fetchone()[0] == 2
    assert manager.db.execute("SELECT COUNT(*) FROM metric_series WHERE job_id='job_review' AND metric='safe'").fetchone()[0] == 1


def test_capture_batch_retry_rolls_back_prior_event_and_metric_rows(manager, monkeypatch):
    original = manager._persist_metric_series_uncommitted
    calls = [0]

    def fail_second_once(event):
        calls[0] += 1
        original(event)
        if calls[0] == 2:
            raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(manager, "_persist_metric_series_uncommitted", fail_second_once)
    payloads = [normalize_event_payload({"type": "metric", "data": {"i": i}}) for i in range(2)]
    manager._emit_capture_batch("job_review", payloads, "stdout")
    metrics = manager.db.execute("SELECT seq FROM events WHERE job_id='job_review' AND type='metric' ORDER BY seq").fetchall()
    assert [row[0] for row in metrics] == [1, 2]
    assert manager.db.execute("SELECT COUNT(*) FROM metric_series WHERE job_id='job_review'").fetchone()[0] == 2
    mirrored = [json.loads(line) for line in (manager.events_dir / "review.jsonl").read_text().splitlines()]
    assert sum(event["type"] == "metric" for event in mirrored) == 2


def test_failed_eof_publication_does_not_close_the_stdin_channel(manager, monkeypatch):
    original = Path.open

    def fail_channel_open(path, *args, **kwargs):
        if path.name == "job_review.in":
            raise OSError("injected channel disk full")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", fail_channel_open)
    with pytest.raises(OSError, match="injected"):
        manager.send_sync("job_review", "", eof=True)
    assert not (manager.home / "stdin" / "job_review.closed").exists(), "EOF was not durable; retry must remain possible"


@pytest.mark.skipif(__import__("sys").platform != "win32", reason="uses Windows runner containment for safe own-tree teardown")
def test_blocked_stdin_feeder_with_exited_parent_obeys_workload_deadline(tmp_path):
    import subprocess
    import sys
    import time

    manager = JobManager(tmp_path, recover=False)
    proc = None
    code = ("import subprocess,sys,time; "
            "subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)']); "
            "print('ready',flush=True);time.sleep(.5)")
    try:
        job = manager.start(subprocess.list2cmdline([sys.executable, "-c", code]),
                                        interactive=True, timeout_seconds=10, notify_on=[], wake_targets=[])
        job_id = job["job_id"]
        proc = manager.processes[job_id]
        deadline = time.monotonic() + 20
        while manager.status(job_id)["status"] == "launching" and time.monotonic() < deadline:
            time.sleep(.02)
        manager.send_sync(job_id, "x" * 262144)
        while manager.status(job_id)["status"] == "running" and time.monotonic() < deadline:
            time.sleep(.05)
        assert manager.status(job_id)["status"] == "timeout", "stdin close must not bypass the job deadline"
    finally:
        # The captured Popen is this test's runner, whose Windows Job owns all
        # workload descendants. No numeric descendant PID cleanup.
        if proc is not None and proc.poll() is None:
            manager._kill_process(proc, force=True)
            proc.wait(timeout=5)
        manager.close()
