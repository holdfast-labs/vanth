"""Local retry safety, side-effect-free previews and bounded result output."""

import asyncio
import concurrent.futures
import multiprocessing
import sys

import pytest
import shellcmd

from vanth.server import JobManager


def _submit(home, barrier, results):
    manager = JobManager(home, recover=False)
    try:
        barrier.wait(timeout=20)
        results.put(asyncio.run(manager.start("echo once", pool="held", idempotency_key="retry-safe-123")))
    finally:
        manager.close()


def test_retry_survives_restart_and_conflicting_requests_are_rejected(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    first = asyncio.run(manager.start("echo once", pool="held", idempotency_key="retry-safe-123"))
    manager.close()
    manager = JobManager(tmp_path, recover=False)
    try:
        replay = asyncio.run(manager.start("echo once", pool="held", idempotency_key="retry-safe-123"))
        assert replay["job_id"] == first["job_id"]
        assert replay["idempotent_replay"]
        assert len(manager.list()["jobs"]) == 1
        with pytest.raises(ValueError, match="different job request"):
            asyncio.run(manager.start("echo twice", pool="held", idempotency_key="retry-safe-123"))
    finally:
        manager.close()


def test_concurrent_process_retry_creates_one_job(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    manager.close()
    context = multiprocessing.get_context("spawn")
    results = context.Queue()
    barrier = context.Barrier(2)
    processes = [context.Process(target=_submit, args=(str(tmp_path), barrier, results)) for _ in range(2)]
    try:
        for process in processes:
            process.start()
        responses = [results.get(timeout=30) for _ in processes]
        for process in processes:
            process.join(timeout=10)
            assert process.exitcode == 0
        assert len({response["job_id"] for response in responses}) == 1
        assert sum(bool(response.get("idempotent_replay")) for response in responses) == 1
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=10)
        results.close()


def test_cleaned_job_key_cannot_launch_a_duplicate(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    try:
        started = asyncio.run(manager.start("echo once", pool="held", idempotency_key="retry-safe-123"))
        manager.db.execute("DELETE FROM jobs WHERE job_id=?", (started["job_id"],))
        manager.db.commit()
        with pytest.raises(ValueError, match="cleaned job"):
            asyncio.run(manager.start("echo once", pool="held", idempotency_key="retry-safe-123"))
        assert not manager.list()["jobs"]
    finally:
        manager.close()


def test_preview_validates_resolves_and_never_launches(tmp_path, monkeypatch):
    manager = JobManager(tmp_path / "state", recover=False)
    try:
        monkeypatch.setattr(manager, "_launch", lambda *args, **kwargs: pytest.fail("preview launched a job"))
        result = asyncio.run(manager.start(
            "echo preview", cwd=str(tmp_path), env={"SECRET": "private"}, secret_env=["SECRET"],
            wake_targets=[{"type": "local_command", "events": ["completed"], "command": ["echo", "wake"]}],
            idempotency_key="preview-123", dry_run=True,
        ))
        assert result["result"] == "preview"
        assert result["cwd"] == str(tmp_path.resolve())
        assert result["env_names"] == ["SECRET"]
        assert "private" not in str(result)
        assert result["wake_targets"][0]["events"] == ["completed"]
        assert not manager.list()["jobs"]
        assert manager.db.execute("SELECT COUNT(*) FROM local_start_requests").fetchone()[0] == 0
        with pytest.raises(ValueError, match="cwd"):
            asyncio.run(manager.start("echo preview", cwd=str(tmp_path / "missing"), dry_run=True))
    finally:
        manager.close()


def test_idempotent_start_runs_workload_only_once(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        path = tmp_path / "side-effect.txt"
        command = shellcmd.join([sys.executable, "-c", f"from pathlib import Path; p=Path({str(path)!r}); p.open('a').write('once\\n'); print('success')"])
        first = asyncio.run(manager.start(command, idempotency_key="effect-once-123"))
        outcome = asyncio.run(manager.wait(first["job_id"], ["completed", "failed"], timeout_seconds=20))
        assert outcome["event"]["type"] == "completed"
        replay = asyncio.run(manager.start(command, idempotency_key="effect-once-123"))
        assert replay["job_id"] == first["job_id"]
        assert path.read_text() == "once\n"
        summary = manager.run_summary(first["job_id"], include_stdout_excerpt=True)
        assert "success" in summary["stdout_excerpt"]
    finally:
        manager.close()


def test_stdout_excerpt_is_bounded_and_optional(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    try:
        started = asyncio.run(manager.start("echo once", pool="held"))
        manager.logs.joinpath(started["job_id"] + ".stdout.log").write_bytes(b"x" * 20000)
        assert "stdout_excerpt" not in manager.run_summary(started["job_id"])
        assert len(manager.run_summary(started["job_id"], include_stdout_excerpt=True)["stdout_excerpt"]) == 8192
    finally:
        manager.close()


def test_migration_from_v18_preserves_jobs_and_adds_retry_ledger(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    started = asyncio.run(manager.start("echo once", pool="held"))
    manager.db.execute("DROP TABLE local_start_requests")
    manager.db.execute("PRAGMA user_version=18")
    manager.db.commit()
    manager.close()
    manager = JobManager(tmp_path, recover=False)
    try:
        assert manager.status(started["job_id"])["status"] == "queued"
        assert manager.db.execute("PRAGMA user_version").fetchone()[0] == 19
        assert manager.db.execute("SELECT COUNT(*) FROM local_start_requests").fetchone()[0] == 0
    finally:
        manager.close()
