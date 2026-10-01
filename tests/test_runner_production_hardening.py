import asyncio
import io
import json
import struct
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
import shellcmd

from vanth.server import JobManager


class ChunkStream:
    def __init__(self, chunks):
        self.chunks = iter(chunks)

    def read(self, size):
        return next(self.chunks, b"")


def test_capture_masks_multiline_and_chunk_boundary_secrets(tmp_path, monkeypatch):
    manager = JobManager(tmp_path)
    emitted = []
    monkeypatch.setattr(manager, "_emit_capture_batch", lambda job, events, source: emitted.extend(events))
    monkeypatch.setattr(manager, "_emit_safely", lambda *args, **kwargs: None)
    path = tmp_path / "captured"
    stream = ChunkStream([b"before top", b"secret\nvalue after\nAGENT_EVENT ",
                          b'{"type":"checkpoint","message":"topsecret\\nvalue"}\n'])
    manager._read_stream("fake", stream, path, "stdout", ["topsecret\nvalue"])
    assert path.read_bytes() == (b"before *** after\nAGENT_EVENT "
                                 b'{"type":"checkpoint","message":"***"}\n')
    assert emitted[0]["message"] == "***"
    manager.close()


def test_capture_preserves_long_plain_logs_and_flushes_unterminated_event(tmp_path, monkeypatch):
    manager = JobManager(tmp_path)
    manager.max_event_line_bytes = 128
    events, rejected = [], []
    monkeypatch.setattr(manager, "_emit_capture_batch", lambda job, batch, source: events.extend(batch))
    monkeypatch.setattr(manager, "_emit_safely", lambda *args, **kwargs: rejected.append(args[1]))
    raw = b"x" * 5000 + b'\nAGENT_EVENT {"type":"checkpoint","message":"last"}'
    path = tmp_path / "captured"
    manager._read_stream("fake", ChunkStream([raw[:200], raw[200:]]), path, "stdout")
    assert path.read_bytes() == raw
    assert [event["message"] for event in events] == ["last"]
    assert "event_rejected" not in rejected
    manager.close()


def test_log_storage_failure_still_drains_and_parses(tmp_path, monkeypatch):
    manager = JobManager(tmp_path)
    events, diagnostics = [], []
    monkeypatch.setattr(manager, "_emit_capture_batch", lambda job, batch, source: events.extend(batch))
    monkeypatch.setattr(manager, "_emit_safely", lambda *args, **kwargs: diagnostics.append(args[1]))
    path = tmp_path / "directory"
    path.mkdir()
    manager._read_stream("fake", io.BytesIO(b'AGENT_EVENT {"type":"checkpoint"}\n'), path, "stdout")
    assert events[0]["type"] == "checkpoint"
    assert diagnostics == ["log_capture_failed"]
    assert "fake" in manager._capture_failed
    manager.close()


def test_pipe_drain_has_one_deadline_for_all_readers(tmp_path, monkeypatch):
    manager = JobManager(tmp_path)
    stop = threading.Event()
    readers = [threading.Thread(target=stop.wait, daemon=True) for _ in range(2)]
    for reader in readers:
        reader.start()
    manager.reader_threads["fake"] = readers
    events = []
    monkeypatch.setattr(manager, "_emit_safely", lambda *args, **kwargs: events.append(args[1]))
    start = time.monotonic()
    assert not manager._readers_done("fake", timeout=.1)
    assert time.monotonic() - start < .3
    assert events == ["pipe_drain_timeout"]
    stop.set()
    for reader in readers:
        reader.join()
    manager.close()


def test_concurrent_cross_process_batches_preserve_sequences(tmp_path):
    manager = JobManager(tmp_path)
    job = asyncio.run(manager.start(shellcmd.join([sys.executable, "-c", "pass"])))
    job_id = job["job_id"]
    manager.wait_sync(job_id, ["completed"], timeout_seconds=20)
    code = ("from vanth.server import JobManager,normalize_event_payload; import sys; "
            "m=JobManager(sys.argv[1],recover=False); "
            "m._emit_capture_batch(sys.argv[2],"
            "[normalize_event_payload({'type':'checkpoint','data':{'i':i}}) for i in range(150)],'stdout'); m.close()")
    processes = [subprocess.Popen([sys.executable, "-c", code, str(tmp_path), job_id]) for _ in range(3)]
    assert [proc.wait(timeout=30) for proc in processes] == [0, 0, 0]
    events = manager.events(job_id, types=["checkpoint"], limit=1000)["events"]
    assert len(events) == 450
    assert len({event["seq"] for event in events}) == 450
    manager.close()


def test_concurrent_stdin_records_and_eof_are_serialized(tmp_path):
    manager = JobManager(tmp_path)
    command = shellcmd.join([sys.executable, "-c", "import sys; data=sys.stdin.buffer.read(); print(len(data))"])
    job = asyncio.run(manager.start(command, interactive=True))
    job_id = job["job_id"]
    manager.wait_sync(job_id, ["started"], timeout_seconds=20)
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(lambda i: manager.send_sync(job_id, str(i) * 70000), range(6)))
    assert all(result["sent"] == 70000 for result in results)
    manager.send_sync(job_id, "", eof=True)
    with pytest.raises(ValueError, match="closed|not running"):
        manager.send_sync(job_id, "late")
    manager.wait_sync(job_id, ["completed"], timeout_seconds=20)
    raw = (tmp_path / "stdin" / f"{job_id}.in").read_bytes()
    offset, records = 0, []
    while offset < len(raw):
        length = struct.unpack_from("<Q", raw, offset)[0]
        offset += 8
        records.append(raw[offset:offset + length])
        offset += length
    assert sorted(records[:-1]) == [str(i).encode() * 70000 for i in range(6)]
    assert records[-1] == b""
    manager.close()


def test_interactive_restart_resets_eof_and_old_input(tmp_path):
    manager = JobManager(tmp_path)
    command = shellcmd.join([sys.executable, "-c", "import sys; data=sys.stdin.buffer.read(); print(len(data),flush=True); sys.exit(1)"])
    job = asyncio.run(manager.start(command, interactive=True))
    job_id = job["job_id"]
    manager.wait_sync(job_id, ["started"], timeout_seconds=20)
    manager.send_sync(job_id, "old", eof=True)
    ended = manager.wait_sync(job_id, ["failed"], timeout_seconds=20)["event"]["event_id"]
    launch = manager.prepare_launch(job_id)
    assert launch is not None
    manager._launch_prepared(launch)
    manager.wait_sync(job_id, ["started"], since_event_id=ended, timeout_seconds=20)
    manager.send_sync(job_id, "new-input", eof=True)
    manager.wait_sync(job_id, ["failed"], since_event_id=ended, timeout_seconds=20)
    output = manager.tail(job_id)["content"].splitlines()
    assert output == ["3", "9"]
    rerun = manager.rerun_sync(job_id)
    new_id = rerun["job_id"]
    manager.wait_sync(new_id, ["started"], timeout_seconds=20)
    manager.send_sync(new_id, "rerun", eof=True)
    manager.wait_sync(new_id, ["failed"], timeout_seconds=20)
    assert manager.tail(new_id)["content"].strip() == "5"
    manager.close()


def test_exited_parent_with_inherited_pipes_obeys_workload_deadline(tmp_path):
    manager = JobManager(tmp_path)
    pid_file = tmp_path / "descendant.pid"
    child = "import time; time.sleep(8)"
    code = ("import subprocess,sys; from pathlib import Path; "
            f"p=subprocess.Popen([sys.executable,'-c',{child!r}]); "
            f"Path({str(pid_file)!r}).write_text(str(p.pid))")
    started_at = time.monotonic()
    job = asyncio.run(manager.start(shellcmd.join([sys.executable, "-c", code]), timeout_seconds=1))
    try:
        result = manager.wait_sync(job["job_id"], ["timeout"], timeout_seconds=5)
        assert result["result"] == "event"
        assert time.monotonic() - started_at < 5
        diagnostics = manager.events(job["job_id"], types=["pipe_drain_timeout"])["events"]
        assert len(diagnostics) == 1
        if sys.platform == "win32":
            child_pid = int(pid_file.read_text())
            deadline = time.monotonic() + 3
            while manager._pid_alive(child_pid) and time.monotonic() < deadline:
                time.sleep(.05)
            assert not manager._pid_alive(child_pid)

    finally:
        if pid_file.exists():
            manager._kill_pid(int(pid_file.read_text()), force=True)
        manager.close()


def test_stdin_eof_racing_sends_never_leaves_records_after_eof(tmp_path, monkeypatch):
    managers = [JobManager(tmp_path, recover=False) for _ in range(3)]
    for manager in managers:
        monkeypatch.setattr(manager, "_row", lambda *args: {"status": "running", "run_json": '{"interactive":true}'})
    barrier = threading.Barrier(3)
    def send(index):
        barrier.wait()
        try:
            return managers[index].send_sync("fake", str(index) * 100000 if index else "", eof=index == 0)
        except ValueError as exc:
            assert "closed" in str(exc)
            return None
    try:
        with ThreadPoolExecutor(max_workers=3) as pool:
            results = list(pool.map(send, range(3)))
        raw = (tmp_path / "stdin" / "fake.in").read_bytes()
        offset, count = 0, 0
        while True:
            length = struct.unpack_from("<Q", raw, offset)[0]
            offset += 8
            if length == 0:
                break
            assert raw[offset:offset + length] in (b"1" * 100000, b"2" * 100000)
            count += 1
            offset += length
        assert offset == len(raw)
        assert count == sum(result is not None for result in results[1:])
    finally:
        for manager in managers:
            manager.close()


@pytest.mark.skipif(sys.platform != "win32", reason="Windows Job containment")
def test_windows_runner_exit_reaps_descendants_without_inherited_pipes(tmp_path):
    manager = JobManager(tmp_path)
    pid_file = tmp_path / "detached.pid"
    code = ("import subprocess,sys; from pathlib import Path; "
            "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(120)'], "
            "stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL); "
            f"Path({str(pid_file)!r}).write_text(str(p.pid))")
    job = asyncio.run(manager.start(shellcmd.join([sys.executable, "-c", code])))
    try:
        assert manager.wait_sync(job["job_id"], ["completed"], timeout_seconds=10)["result"] == "event"
        child_pid = int(pid_file.read_text())
        deadline = time.monotonic() + 3
        while manager._pid_alive(child_pid) and time.monotonic() < deadline:
            time.sleep(.05)
        assert not manager._pid_alive(child_pid)
    finally:
        if pid_file.exists():
            manager._kill_pid(int(pid_file.read_text()), force=True)
        manager.close()


def test_event_persistence_failure_keeps_draining(tmp_path, monkeypatch):
    import sqlite3
    manager = JobManager(tmp_path)
    consumed = []
    class Stream:
        def read(self, size):
            chunk = b'AGENT_EVENT {"type":"checkpoint"}\n' if len(consumed) < 4 else b""
            consumed.append(chunk)
            return chunk
    def fail(*args):
        raise sqlite3.OperationalError("database or disk is full")
    monkeypatch.setattr(manager, "_emit_capture_batch", fail)
    diagnostics = []
    monkeypatch.setattr(manager, "_emit_safely", lambda *args, **kwargs: diagnostics.append(args[1]))
    manager._read_stream("fake", Stream(), tmp_path / "captured", "stdout")
    assert len(consumed) == 5
    assert len((tmp_path / "captured").read_bytes().splitlines()) == 4
    assert diagnostics == ["log_capture_failed"]
    assert "fake" in manager._capture_failed
    manager.close()



def test_capture_batch_failure_rolls_back_all_events_and_hooks(tmp_path, monkeypatch):
    from vanth.server import normalize_event_payload
    manager = JobManager(tmp_path)
    job = asyncio.run(manager.start(shellcmd.join([sys.executable, "-c", "pass"])))
    job_id = job["job_id"]
    manager.wait_sync(job_id, ["completed"], timeout_seconds=20)
    calls = []
    def fail_second(event):
        calls.append(event)
        if len(calls) == 2:
            raise RuntimeError("simulated persistence crash")
    monkeypatch.setattr(manager, "_persist_metric_series_uncommitted", fail_second)
    with pytest.raises(RuntimeError, match="simulated persistence crash"):
        manager._emit_capture_batch(job_id, [normalize_event_payload({"type": "checkpoint"}) for _ in range(3)], "stdout")
    assert manager.events(job_id, types=["checkpoint"])["events"] == []
    assert '"type":"checkpoint"' not in (tmp_path / "events" / f"{job_id}.jsonl").read_text()
    manager.close()


@pytest.mark.skipif(sys.platform != "win32", reason="nested Windows Job breakaway")
def test_nested_vanth_runner_survives_outer_runner_exit(tmp_path):
    manager = JobManager(tmp_path / "outer")
    nested_home = tmp_path / "nested"
    marker = tmp_path / "nested-completed"
    inner_code = f"import time; from pathlib import Path; time.sleep(3); Path({str(marker)!r}).write_text('done')"
    inner_command = shellcmd.join([sys.executable, "-c", inner_code])
    outer_code = ("import asyncio; from vanth.server import JobManager; "
                  f"m=JobManager({str(nested_home)!r}); j=asyncio.run(m.start({inner_command!r})); "
                  "m.wait_sync(j['job_id'],['started'],timeout_seconds=10); print(j['job_id'],flush=True); m.close()")
    nested = None
    try:
        outer = asyncio.run(manager.start(shellcmd.join([sys.executable, "-c", outer_code])))
        assert manager.wait_sync(outer["job_id"], ["completed"], timeout_seconds=15)["result"] == "event"
        nested = JobManager(nested_home)
        job_id = manager.tail(outer["job_id"])["content"].strip()
        assert nested.status(job_id)["status"] == "running"
        assert nested.wait_sync(job_id, ["completed"], timeout_seconds=10)["result"] == "event"
        assert marker.read_text() == "done"
    finally:
        if nested is not None:
            for job in nested.list()["jobs"]:
                if job["status"] == "running":
                    nested.stop_sync(job["job_id"], signal="kill")
            nested.close()
        manager.close()


def test_eof_marker_crash_recovers_canonical_record_without_duplicate(tmp_path, monkeypatch):
    from pathlib import Path
    manager = JobManager(tmp_path, recover=False)
    monkeypatch.setattr(manager, "_row", lambda *args: {"status": "running", "run_json": '{"interactive":true}'})
    original = Path.touch
    def fail_marker(path, *args, **kwargs):
        if path.name == "fake.closed":
            raise OSError("marker publication failed")
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, "touch", fail_marker)
    try:
        with pytest.raises(OSError, match="marker publication failed"):
            manager.send_sync("fake", "", eof=True)
        assert manager.send_sync("fake", "", eof=True) == {"job_id": "fake", "sent": 0, "eof": True}
        assert (tmp_path / "stdin" / "fake.in").read_bytes() == b"\0" * 8
        with pytest.raises(ValueError, match="closed"):
            manager.send_sync("fake", "late")
    finally:
        manager.close()
