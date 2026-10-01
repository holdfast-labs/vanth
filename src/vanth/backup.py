"""Whole-state backup and restore for a Vanth home (review B1).

One archive holds every durable store that must move together: ``jobs.sqlite``,
``artifacts.sqlite``, and ``remote.sqlite`` (via SQLite's online backup API, so
WAL is safe), the per-job event mirrors, and the managed-artifact blob store. A
``manifest.json`` records the schema version and a SHA-256 per file so a restore
can verify integrity before touching the live state.

The token, daemon lock/discovery files, and (by default) logs are excluded.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
import zipfile
from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path

from .migrations import LATEST_SCHEMA_VERSION
from .process_watch import process_alive

BACKUP_FORMAT = "vanth-backup/1"
_SQLITE_FILES = ("jobs.sqlite", "artifacts.sqlite", "remote.sqlite")
_TREE_DIRS = ("events",)
_ARTIFACT_TREE = "artifacts-store"
_MANAGED_TREE_NAMES = {*_TREE_DIRS, _ARTIFACT_TREE, "logs"}
_DEFAULT_MAX_MEMBER_BYTES = 8 * 1024**3


def _max_member_bytes() -> int:
    try:
        return max(1, int(os.environ.get("VANTH_BACKUP_MAX_MEMBER_BYTES", str(_DEFAULT_MAX_MEMBER_BYTES))))
    except ValueError:
        return _DEFAULT_MAX_MEMBER_BYTES


def _archive_file(archive: zipfile.ZipFile, source: Path, name: str) -> dict[str, object]:
    digest = hashlib.sha256()
    size = 0
    with source.open("rb") as handle, archive.open(name, "w", force_zip64=True) as member:
        # Bound a live append-only stream to its size at open; producers must
        # not keep the backup chasing a moving EOF forever.
        remaining = os.fstat(handle.fileno()).st_size
        while remaining:
            chunk = handle.read(min(1024 * 1024, remaining))
            if not chunk:
                break
            member.write(chunk)
            digest.update(chunk)
            size += len(chunk)
            remaining -= len(chunk)
    return {"path": name, "sha256": digest.hexdigest(), "size": size}


def _snapshot_sqlite(source: Path, destination: Path) -> None:
    source_connection = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    try:
        destination_connection = sqlite3.connect(destination)
        try:
            source_connection.backup(destination_connection)
        finally:
            destination_connection.close()
    finally:
        source_connection.close()


def _backup_name() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"vanth-backup-{stamp}.zip"


def create_backup(home: str | Path, *, out: str | Path | None = None, include_logs: bool = False) -> Path:
    """Write one archive of everything needed to restore ``home``. Returns its path."""
    home = Path(home)
    home.mkdir(parents=True, exist_ok=True)
    destination = Path(out) if out else home / "backups" / _backup_name()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".tmp")
    # Never archive the archive itself (e.g. ``--out`` placed inside events/ or
    # artifacts-store/), and keep the temp staging dir out of the walk.
    skip = {temporary.resolve(), destination.resolve(), (home / "backups").resolve()}
    files: list[dict[str, object]] = []

    # Freeze artifact publication/physical GC across catalog snapshot and
    # blob copying. Reuse the existing store fence without claiming ownership
    # or creating an unused artifact store.
    artifact_root = home / _ARTIFACT_TREE
    fence = nullcontext()
    if artifact_root.is_dir():
        from .artifacts.local_store import LocalBlobStore

        fence = LocalBlobStore._Fence(artifact_root / ".vanth-gc-fence.lock")
    with fence, tempfile.TemporaryDirectory(prefix="vanth-backup-") as staging, zipfile.ZipFile(
        temporary, "w", zipfile.ZIP_DEFLATED
    ) as archive:
        staging_path = Path(staging)
        for name in _SQLITE_FILES:
            source = home / name
            if not source.is_file():
                continue
            snapshot = staging_path / name
            _snapshot_sqlite(source, snapshot)
            files.append(_archive_file(archive, snapshot, name))
            snapshot.unlink()

        for tree in (*_TREE_DIRS, _ARTIFACT_TREE):
            root = home / tree
            if not root.is_dir():
                continue
            for path in sorted(root.rglob("*")):
                if not path.is_file():
                    continue
                relative = path.relative_to(home)
                if "staging" in relative.parts or path.name.endswith(".lock"):
                    continue
                if path.resolve() in skip:
                    continue
                files.append(_archive_file(archive, path, relative.as_posix()))

        if include_logs:
            root = home / "logs"
            if root.is_dir():
                for path in sorted(root.rglob("*")):
                    if path.is_file():
                        relative = path.relative_to(home)
                        if path.resolve() not in skip:
                            files.append(_archive_file(archive, path, relative.as_posix()))

        manifest = {
            "format": BACKUP_FORMAT,
            "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "schema_version": LATEST_SCHEMA_VERSION,
            "include_logs": include_logs,
            "files": files,
        }
        archive.writestr("manifest.json", json.dumps(manifest, indent=2))
    os.replace(temporary, destination)  # atomic publish
    return destination


def _safe_member(name: str, home: Path) -> Path:
    candidate = Path(name)
    if candidate.is_absolute() or ".." in candidate.parts or "\\" in name:
        raise ValueError(f"unsafe path in backup archive: {name!r}")
    resolved = (home / candidate).resolve()
    if not resolved.is_relative_to(home.resolve()):
        raise ValueError(f"path escapes the home directory: {name!r}")
    return resolved


def _verify_archive(bundle: zipfile.ZipFile, entries: list[tuple[dict, str]]) -> None:
    limit = _max_member_bytes()
    for entry, name in entries:
        info = bundle.getinfo(name)
        if info.file_size > limit:
            raise ValueError(f"backup member too large: {name!r}")
        digest = hashlib.sha256()
        with bundle.open(name) as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        if digest.hexdigest() != entry["sha256"]:
            raise ValueError(f"backup integrity check failed for {name!r}")


def restore_backup(home: str | Path, archive: str | Path, *, force: bool = False) -> dict[str, object]:
    """Verify and restore an archive into ``home`` (snapshotting current state first)."""
    home = Path(home)
    archive = Path(archive)
    if not archive.is_file():
        raise ValueError(f"backup archive not found: {archive}")

    from .daemon import DaemonLock

    lock = DaemonLock(home / "daemon.lock")
    if not lock.acquire():
        raise ValueError("daemon appears to be running (home lock held); stop it first (force cannot bypass a live daemon)")
    try:
        _refuse_live_jobs(home)
        return _restore_locked(home, archive, force=force)
    finally:
        lock.release()


def _refuse_live_jobs(home: Path) -> None:
    """Detached runners survive daemon shutdown; inspect without opening a manager."""
    database = home / "jobs.sqlite"
    if not database.is_file():
        return
    connection = sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True)
    try:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(jobs)")}
        pids = [name for name in ("worker_pid", "pid") if name in columns]
        if not pids:
            return  # Legacy/marker databases may have no jobs table or PID fields.
        identity = "job_id" if "job_id" in columns else "rowid"
        where = " WHERE status IN ('running','launching','orphaned')" if "status" in columns else ""
        for row in connection.execute(f"SELECT {identity}, {','.join(pids)} FROM jobs{where}"):
            for pid in row[1:]:
                if pid and process_alive(int(pid)):
                    raise ValueError(f"job {row[0]} still has a live runner/workload (PID {pid}); stop jobs before restore")
    finally:
        connection.close()


def _restore_locked(home: Path, archive: Path, *, force: bool) -> dict[str, object]:
    with zipfile.ZipFile(archive) as bundle:
        try:
            manifest = json.loads(bundle.read("manifest.json"))
        except KeyError as exc:
            raise ValueError("archive is not a Vanth backup (no manifest.json)") from exc
        if manifest.get("format") != BACKUP_FORMAT:
            raise ValueError(f"unsupported backup format: {manifest.get('format')!r}")
        schema = int(manifest.get("schema_version", 0))
        if schema > LATEST_SCHEMA_VERSION and not force:
            raise ValueError(
                f"backup schema v{schema} is newer than this binary supports (v{LATEST_SCHEMA_VERSION}); pass force=True"
            )
        files = manifest.get("files") or []
        # Validate EVERY member path AND verify content BEFORE any mutation, so a
        # malformed or tampered archive cannot partially overwrite live state.
        validated: list[tuple[dict, str, Path]] = []
        for entry in files:
            name = str(entry["path"])
            target = _safe_member(name, home)
            validated.append((entry, name, target))
        _verify_archive(bundle, [(entry, name) for entry, name, _ in validated])

    # Snapshot current state so a restore is itself reversible.
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    create_backup(home, out=home / "backups" / f"pre-restore-{timestamp}.zip")

    home.mkdir(parents=True, exist_ok=True)
    # Drop the managed trees the archive repopulates so stale files cannot
    # survive a restore (SQLite catalogs + filesystem stay consistent).
    tree_roots = {Path(name).parts[0] for _, name, _ in validated if len(Path(name).parts) > 1}
    for tree in tree_roots & _MANAGED_TREE_NAMES:
        shutil.rmtree(home / tree, ignore_errors=True)

    restored = 0
    with zipfile.ZipFile(archive) as bundle:
        for entry, name, target in validated:
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_name(target.name + ".restore-tmp")
            with bundle.open(name) as source, temporary.open("wb") as destination_handle:
                shutil.copyfileobj(source, destination_handle)
            # No daemon may start until restore releases the home lock. Old
            # WAL pages must never replay over the restored database.
            if name in _SQLITE_FILES:
                for suffix in ("-wal", "-shm"):
                    target.with_name(target.name + suffix).unlink(missing_ok=True)
            os.replace(temporary, target)  # atomic per file
            restored += 1
    return {"result": "ok", "archive": str(archive), "files_restored": restored, "schema_version": schema}
