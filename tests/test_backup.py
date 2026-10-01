"""Whole-state backup/restore (review B1)."""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import sys
import zipfile
from pathlib import Path

import pytest

from vanth.backup import create_backup, restore_backup
from vanth.cli import cmd_backup, cmd_restore
from vanth.migrations import LATEST_SCHEMA_VERSION
from vanth.server import JobManager


import shellcmd


def cmd(code: str) -> str:
    return shellcmd.join([sys.executable, "-c", code])


def _seed(home: Path) -> None:
    manager = JobManager(home, recover=False)
    try:
        job = asyncio.run(manager.start(cmd("print('hi')")))
        manager.wait_sync(job["job_id"], ["completed"], timeout_seconds=20)
        blob = home / "artifacts-store" / "blobs" / "aa" / "bb" / "deadbeef"
        blob.parent.mkdir(parents=True, exist_ok=True)
        blob.write_bytes(b"artifact-bytes")
    finally:
        manager.close()


def _job_count(home: Path) -> int:
    connection = sqlite3.connect(home / "jobs.sqlite")
    try:
        return connection.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    finally:
        connection.close()


def test_backup_restore_round_trip(tmp_path):
    home = tmp_path / "state"
    _seed(home)
    archive = create_backup(home)
    assert archive.is_file()

    # Destroy live state, then restore it.
    connection = sqlite3.connect(home / "jobs.sqlite")
    connection.execute("DELETE FROM jobs")
    connection.commit()
    connection.close()
    (home / "artifacts-store" / "blobs" / "aa" / "bb" / "deadbeef").unlink()
    assert _job_count(home) == 0

    result = restore_backup(home, archive)
    assert result["result"] == "ok"
    assert _job_count(home) == 1
    assert (home / "artifacts-store" / "blobs" / "aa" / "bb" / "deadbeef").exists()


def test_restore_refuses_tampered_archive(tmp_path):
    archive = tmp_path / "tampered.zip"
    with zipfile.ZipFile(archive, "w") as bundle:
        bundle.writestr("jobs.sqlite", b"not a real database")
        bundle.writestr(
            "manifest.json",
            json.dumps(
                {
                    "format": "vanth-backup/1",
                    "schema_version": LATEST_SCHEMA_VERSION,
                    "files": [{"path": "jobs.sqlite", "sha256": "0" * 64, "size": 21}],
                }
            ),
        )
    with pytest.raises(ValueError, match="integrity"):
        restore_backup(tmp_path / "home", archive)


def test_restore_refuses_newer_schema(tmp_path):
    archive = tmp_path / "future.zip"
    with zipfile.ZipFile(archive, "w") as bundle:
        bundle.writestr("jobs.sqlite", b"x")
        import hashlib

        bundle.writestr(
            "manifest.json",
            json.dumps(
                {
                    "format": "vanth-backup/1",
                    "schema_version": LATEST_SCHEMA_VERSION + 1,
                    "files": [{"path": "jobs.sqlite", "sha256": hashlib.sha256(b"x").hexdigest(), "size": 1}],
                }
            ),
        )
    with pytest.raises(ValueError, match="newer than"):
        restore_backup(tmp_path / "home", archive)


def test_restore_rejects_zipslip_before_mutation(tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    (home / "sentinel.txt").write_text("keep", encoding="utf-8")
    archive = tmp_path / "slip.zip"
    good, evil = b"good", b"evil"
    with zipfile.ZipFile(archive, "w") as bundle:
        bundle.writestr("good.sqlite", good)
        bundle.writestr("../escaped.txt", evil)
        bundle.writestr(
            "manifest.json",
            json.dumps(
                {
                    "format": "vanth-backup/1",
                    "schema_version": LATEST_SCHEMA_VERSION,
                    "files": [
                        {"path": "good.sqlite", "sha256": hashlib.sha256(good).hexdigest(), "size": 4},
                        {"path": "../escaped.txt", "sha256": hashlib.sha256(evil).hexdigest(), "size": 4},
                    ],
                }
            ),
        )
    with pytest.raises(ValueError, match="unsafe path|escapes"):
        restore_backup(home, archive)
    assert not (tmp_path / "escaped.txt").exists()
    assert not (home / "good.sqlite").exists(), "no member may be written before full validation"
    assert (home / "sentinel.txt").read_text(encoding="utf-8") == "keep"


def test_restore_removes_stale_managed_files(tmp_path):
    home = tmp_path / "state"
    _seed(home)
    archive = create_backup(home)
    stale = home / "artifacts-store" / "blobs" / "cc" / "dd" / "stale"
    stale.parent.mkdir(parents=True, exist_ok=True)
    stale.write_bytes(b"stale")
    restore_backup(home, archive)
    assert not stale.exists(), "stale managed files must not survive a restore"


def test_cli_backup_and_restore_guard(tmp_path, capsys):
    home = tmp_path / "state"
    _seed(home)
    assert cmd_backup([], home) == 0
    out = capsys.readouterr().out
    assert "backup written:" in out
    archive = out.strip().split("backup written: ", 1)[1]
    assert Path(archive).is_file()
    # Restore without --yes is refused.
    assert cmd_restore([archive], home) == 2
    assert "refusing without --yes" in capsys.readouterr().err


@pytest.mark.parametrize("force", [False, True])
def test_restore_holds_home_lock_through_mutation(tmp_path, monkeypatch, force):
    from vanth import backup
    from vanth.daemon import DaemonLock

    home = tmp_path / "home"
    (home / "events").mkdir(parents=True)
    (home / "events" / "sample").write_bytes(b"snapshot")
    archive = create_backup(home)
    original = backup.os.replace
    checked = []

    def replace(source, target):
        if str(source).endswith(".restore-tmp"):
            competing = DaemonLock(home / "daemon.lock")
            assert not competing.acquire()
            checked.append(target)
        return original(source, target)

    monkeypatch.setattr(backup.os, "replace", replace)
    restore_backup(home, archive, force=force)
    assert checked
    competing = DaemonLock(home / "daemon.lock")
    assert competing.acquire()
    competing.release()


@pytest.mark.parametrize("force", [False, True])
def test_restore_refuses_live_daemon_even_forced(tmp_path, force):
    from vanth.daemon import DaemonLock

    home = tmp_path / "home"
    archive = create_backup(home)
    lock = DaemonLock(home / "daemon.lock")
    assert lock.acquire()
    try:
        with pytest.raises(ValueError, match="home lock held"):
            restore_backup(home, archive, force=force)
    finally:
        lock.release()


def test_restore_discards_stale_sqlite_sidecars(tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    db = home / "jobs.sqlite"
    connection = sqlite3.connect(db)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("CREATE TABLE sample (value TEXT)")
    connection.execute("INSERT INTO sample VALUES ('old')")
    connection.commit()
    archive = create_backup(home)
    connection.execute("UPDATE sample SET value='new'")
    connection.commit()
    # Preserve real stale WAL/SHM bytes after closing the writer, as a crashed
    # process would. Keeping the writer open would violate restore's contract.
    saved = {}
    for suffix in ("-wal", "-shm"):
        sidecar = db.with_name(db.name + suffix)
        saved[suffix] = sidecar.read_bytes()
    connection.close()
    for suffix, content in saved.items():
        db.with_name(db.name + suffix).write_bytes(content)
    restore_backup(home, archive)
    with sqlite3.connect(db) as connection:
        assert connection.execute("SELECT value FROM sample").fetchone()[0] == "old"


@pytest.mark.parametrize("tree,include_logs", [("events", False), ("logs", True)])
def test_backup_manifest_hashes_archived_bytes_during_live_append(tmp_path, monkeypatch, tree, include_logs):
    from vanth import backup

    home = tmp_path / "home"
    path = home / tree / "stream"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"before\n")
    original = backup._archive_file

    def archive_then_append(bundle, source, name):
        entry = original(bundle, source, name)
        if source == path:
            with path.open("ab") as handle:
                handle.write(b"after\n")
        return entry

    monkeypatch.setattr(backup, "_archive_file", archive_then_append)
    archive = create_backup(home, include_logs=include_logs)
    restore_backup(tmp_path / "restored", archive)
    assert (tmp_path / "restored" / tree / "stream").read_bytes() == b"before\n"


def test_restore_releases_home_lock_after_failed_publication(tmp_path, monkeypatch):
    from vanth import backup
    from vanth.daemon import DaemonLock

    home = tmp_path / "home"
    (home / "events").mkdir(parents=True)
    (home / "events" / "sample").write_bytes(b"snapshot")
    archive = create_backup(home)
    original = backup.os.replace

    def fail_restore(source, target):
        if str(source).endswith(".restore-tmp"):
            raise OSError("injected restore failure")
        return original(source, target)

    monkeypatch.setattr(backup.os, "replace", fail_restore)
    with pytest.raises(OSError, match="injected"):
        restore_backup(home, archive)
    lock = DaemonLock(home / "daemon.lock")
    assert lock.acquire()
    lock.release()
    assert list((home / "backups").glob("pre-restore-*.zip"))


@pytest.mark.parametrize("force", [False, True])
@pytest.mark.parametrize("column", ["worker_pid", "pid"])
def test_restore_refuses_detached_live_recorded_job(tmp_path, force, column):
    import subprocess

    home = tmp_path / "home"
    home.mkdir()
    archive = create_backup(home)
    # A detached worker/workload can outlive the daemon home lock. Use a
    # captured own subprocess and minimal catalog rather than starting a daemon.
    worker = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(30)"])
    try:
        with sqlite3.connect(home / "jobs.sqlite") as connection:
            connection.execute(f"CREATE TABLE jobs(job_id TEXT,status TEXT,{column} INTEGER)")
            connection.execute(f"INSERT INTO jobs VALUES ('detached','running',?)", (worker.pid,))
        sentinel = home / "events" / "keep"
        sentinel.parent.mkdir()
        sentinel.write_bytes(b"keep live state")
        with pytest.raises(ValueError, match="live runner/workload"):
            restore_backup(home, archive, force=force)
        assert sentinel.read_bytes() == b"keep live state"
        from vanth.daemon import DaemonLock
        lock = DaemonLock(home / "daemon.lock")
        assert lock.acquire()
        lock.release()
    finally:
        worker.kill()
        worker.wait(timeout=5)


def test_restore_safety_backup_failure_aborts_before_mutation(tmp_path, monkeypatch):
    from vanth import backup

    home = tmp_path / "home"
    path = home / "events" / "sample"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"old archived bytes")
    archive = create_backup(home)
    path.write_bytes(b"new live bytes")
    db = home / "jobs.sqlite"
    with sqlite3.connect(db) as connection:
        connection.execute("CREATE TABLE marker(value TEXT)")
        connection.execute("INSERT INTO marker VALUES ('keep')")
    prior_db = db.read_bytes()

    def fail_snapshot(*args, **kwargs):
        raise OSError("injected safety backup disk full")

    monkeypatch.setattr(backup, "create_backup", fail_snapshot)
    with pytest.raises(OSError, match="safety backup"):
        restore_backup(home, archive)
    assert path.read_bytes() == b"new live bytes"
    assert db.read_bytes() == prior_db


def test_backup_does_not_initialize_unused_artifact_store(tmp_path):
    home = tmp_path / "home"
    create_backup(home)
    assert not (home / "artifacts-store").exists()
    assert not (home / "artifacts.sqlite").exists()


def test_live_artifact_backup_excludes_gc_between_catalog_snapshot_and_blob_copy(tmp_path, monkeypatch):
    import threading
    from vanth import backup
    from vanth.artifacts.catalog import open_catalog
    from vanth.artifacts.lifecycle import Lifecycle
    from vanth.artifacts.local_store import LocalBlobStore
    from vanth.artifacts.operations import ArtifactOperations

    home = tmp_path / "home"
    catalog = open_catalog(home)
    blobs = LocalBlobStore(home / "artifacts-store", catalog)
    ops = ArtifactOperations(catalog, blobs)
    old = ops.put_file("model", data=b"old archived version", idempotency_key="backup-old")
    latest = ops.put_file("model", data=b"latest archived version", idempotency_key="backup-new")
    begin_gc, gc_fence_attempted, gc_done = threading.Event(), threading.Event(), threading.Event()
    errors = []
    original_enter = LocalBlobStore._Fence.__enter__

    def record_gc_fence_attempt(fence):
        if threading.current_thread().name == "backup-concurrent-gc":
            gc_fence_attempted.set()
        return original_enter(fence)

    monkeypatch.setattr(LocalBlobStore._Fence, "__enter__", record_gc_fence_attempt)

    def collect():
        other_catalog = None
        try:
            assert begin_gc.wait(5)
            other_catalog = open_catalog(home)
            other_blobs = LocalBlobStore(home / "artifacts-store", other_catalog)
            other_ops = ArtifactOperations(other_catalog, other_blobs)
            Lifecycle(other_catalog, other_ops).gc(dry_run=False, idempotency_key="backup-racing-gc")
        except BaseException as exc:
            errors.append(exc)
        finally:
            if other_catalog:
                other_catalog.db.close()
            gc_done.set()

    collector = threading.Thread(target=collect, name="backup-concurrent-gc", daemon=True)
    collector.start()
    original_snapshot = backup._snapshot_sqlite

    def snapshot_then_release_gc(source, destination):
        original_snapshot(source, destination)
        if source.name == "artifacts.sqlite":
            begin_gc.set()
            assert gc_fence_attempted.wait(5)
            assert not gc_done.is_set(), "GC physical deletion must wait for blob archive"
            assert blobs.verify_blob(old["sha256"])
            with pytest.raises(TimeoutError):
                with LocalBlobStore._Fence(blobs.root / ".vanth-gc-fence.lock", timeout=.01):
                    pytest.fail("backup must own the physical GC/publication fence")

    monkeypatch.setattr(backup, "_snapshot_sqlite", snapshot_then_release_gc)
    try:
        archive = create_backup(home)
        assert gc_done.wait(5)
        collector.join(timeout=1)
        assert not errors
        assert not blobs.has_blob(old["sha256"]), "GC must resume after archive releases its fence"
        # Snapshot contains both versions; every reference must have archived
        # bytes even though the concurrent live GC removed the older version.
        restored = tmp_path / "restored"
        restore_backup(restored, archive)
        restored_catalog = open_catalog(restored)
        try:
            restored_blobs = LocalBlobStore(restored / "artifacts-store", restored_catalog)
            rows = restored_catalog.db.execute("SELECT manifest_json FROM versions").fetchall()
            assert len(rows) == 2
            assert all(restored_blobs.verify_blob(json.loads(row[0])["sha256"]) for row in rows)
            assert restored_blobs.read_blob(latest["sha256"]) == b"latest archived version"
        finally:
            restored_catalog.db.close()
    finally:
        begin_gc.set()
        collector.join(timeout=5)
        catalog.db.close()
