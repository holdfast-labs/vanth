"""Tests for interactive stdin support (job_send + interactive start).

Covers: an interactive job receiving length-prefixed stdin records plus an EOF
sentinel, rejection of sends to non-interactive/unknown/not-running jobs, the
non-interactive DEVNULL behavior, EOF-driven shutdown, cleanup removing the
channel file, and rerun preserving interactivity.
"""

import asyncio
import sys

import pytest

from vanth.server import TERMINAL_STATUSES, JobManager


import shellcmd


def cmd(code: str) -> str:
    return shellcmd.join([sys.executable, "-c", code])


def wait_event(manager: JobManager, job_id: str, event_type: str) -> dict:
    return asyncio.run(manager.wait(job_id, [event_type], timeout_seconds=10))


def tail(manager: JobManager, job_id: str, stream: str) -> str:
    return manager.tail(job_id, stream)["content"]


def test_interactive_job_receives_stdin_and_eof(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        started = manager.start(
                cmd("import sys; data=sys.stdin.read(); print('GOT:'+data, flush=True)"),
                interactive=True,
            
        )
        job_id = started["job_id"]
        wait_event(manager, job_id, "started")
        assert manager.send_sync(job_id, "hello")["sent"] == 5
        assert manager.send_sync(job_id, " world", eof=True)["eof"] is True
        wait_event(manager, job_id, "completed")
        assert manager.status(job_id)["status"] == "completed"
        assert "GOT:hello world" in tail(manager, job_id, "stdout")
    finally:
        manager.close()


def test_job_send_rejects_non_interactive_job(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        started = manager.start(cmd("print('plain')"))
        wait_event(manager, started["job_id"], "completed")
        with pytest.raises(ValueError, match="not interactive"):
            manager.send_sync(started["job_id"], "nope")
    finally:
        manager.close()


def test_job_send_rejects_unknown_or_not_running(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        with pytest.raises(ValueError, match="Unknown job_id"):
            manager.send_sync("job_missing", "nope")
        started = manager.start(cmd("import sys; print(sys.stdin.read())"), interactive=True
        )
        wait_event(manager, started["job_id"], "started")
        manager.send_sync(started["job_id"], "", eof=True)
        wait_event(manager, started["job_id"], "completed")
        with pytest.raises(ValueError, match="not running"):
            manager.send_sync(started["job_id"], "nope")
    finally:
        manager.close()


def test_non_interactive_job_stdin_closed(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        started = manager.start(cmd("import sys; print('E'+sys.stdin.read() or 'X', flush=True)")
        )
        wait_event(manager, started["job_id"], "completed")
        assert manager.status(started["job_id"])["status"] == "completed"
        assert "E" in tail(manager, started["job_id"], "stdout")
    finally:
        manager.close()


def test_eof_allows_program_that_exits_on_stdin_close(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        started = manager.start(
                cmd("import sys; sys.stdin.readline(); print('READY', flush=True)"),
                interactive=True,
            
        )
        job_id = started["job_id"]
        wait_event(manager, job_id, "started")
        manager.send_sync(job_id, "go\n")
        manager.send_sync(job_id, "", eof=True)
        wait_event(manager, job_id, "completed")
        assert "READY" in tail(manager, job_id, "stdout")
    finally:
        manager.close()


def test_cleanup_removes_stdin_file(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        started = manager.start(
                cmd("import sys; sys.stdin.read(); print('done', flush=True)"),
                interactive=True,
            
        )
        job_id = started["job_id"]
        wait_event(manager, job_id, "started")
        channel = tmp_path / "state" / "stdin" / f"{job_id}.in"
        assert manager.send_sync(job_id, "bye")["sent"] == 3
        assert channel.exists()
        manager.send_sync(job_id, "", eof=True)
        wait_event(manager, job_id, "completed")
        manager.cleanup(older_than_seconds=0, dry_run=False)
        assert not channel.exists()
    finally:
        manager.close()


def test_rerun_preserves_interactive(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        started = manager.start(
                cmd("import sys; data=sys.stdin.read(); print('GOT:'+data, flush=True)"),
                interactive=True,
            
        )
        job_id = started["job_id"]
        # An interactive job reads until EOF, so it must be closed before it can
        # complete. Waiting for "completed" without this silently timed out AND
        # left the job's runner alive after pytest exited (one leaked process per
        # run): the job was still `running`, and only the runner's own cleanup on
        # a terminal state stops it.
        wait_event(manager, job_id, "started")
        manager.send_sync(job_id, "", eof=True)
        wait_event(manager, job_id, "completed")
        assert manager.status(job_id)["status"] == "completed"

        reran = manager.rerun_sync(job_id)
        rerun_id = reran["job_id"]
        wait_event(manager, rerun_id, "started")
        assert manager.send_sync(rerun_id, "again")["sent"] == 5
        manager.send_sync(rerun_id, "", eof=True)
        wait_event(manager, rerun_id, "completed")
        assert manager.status(rerun_id)["status"] == "completed"
        assert "GOT:again" in tail(manager, rerun_id, "stdout")
        # Nothing may be left running: an in-flight job keeps its runner process
        # alive after pytest exits (the leak this test used to cause).
        in_flight = [
            job["job_id"] for job in manager.list()["jobs"] if job["status"] not in TERMINAL_STATUSES
        ]
        assert not in_flight, f"jobs left running: {in_flight}"
    finally:
        manager.close()
