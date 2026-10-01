"""Durable artifact operations: idempotency, crash cases, fencing, cleanup (Phase 5)."""

from __future__ import annotations

import asyncio
import hashlib
import json
import sys
from pathlib import Path

import pytest

from vanth.artifacts.catalog import open_catalog
from vanth.artifacts.local_store import LocalBlobStore, default_store_root
from vanth.artifacts.operations import ArtifactOperations
from vanth.server import JobManager


import shellcmd


@pytest.fixture()
def home(tmp_path):
    return tmp_path / "state"


def make_ops(home) -> ArtifactOperations:
    catalog = open_catalog(home)
    blobs = LocalBlobStore(default_store_root(home), catalog)
    return ArtifactOperations(catalog, blobs)


def version_count(ops: ArtifactOperations) -> int:
    return int(ops.catalog.db.execute("SELECT COUNT(*) FROM versions").fetchone()[0])


# ---------------------------------------------------------------------------
# put_file happy path, replay, dedup
# ---------------------------------------------------------------------------


def test_put_file_happy_path(home):
    ops = make_ops(home)
    data = b"artifact payload v1"
    result = ops.put_file("model.bin", data=data, idempotency_key="put-key-0001")
    assert result["deduplicated"] is False
    assert result["version_id"].startswith("ver_")
    assert result["root_id"].startswith("rot_")
    assert result["sha256"] == hashlib.sha256(data).hexdigest()

    root = ops.catalog.db.execute("SELECT * FROM roots WHERE name='model.bin'").fetchone()
    assert root["root_id"] == result["root_id"]
    assert root["latest_version_id"] == result["version_id"]
    version = ops.catalog.db.execute("SELECT * FROM versions WHERE version_id=?", (result["version_id"],)).fetchone()
    assert version["manifest_digest"] == result["manifest_digest"]
    op = ops.catalog.db.execute(
        "SELECT status FROM operations WHERE idempotency_key='put-key-0001'"
    ).fetchone()
    assert op["status"] == "completed"
    assert ops.blobs.has_blob(result["sha256"])


def test_put_file_replay_same_key_same_version(home):
    ops = make_ops(home)
    data = b"replay me"
    first = ops.put_file("r.bin", data=data, idempotency_key="replay-key-01")
    second = ops.put_file("r.bin", data=data, idempotency_key="replay-key-01")
    assert second["version_id"] == first["version_id"]
    assert second["replayed"] is True
    assert version_count(ops) == 1


def test_put_file_new_key_same_content_deduplicates_version_and_blob(home):
    ops = make_ops(home)
    data = b"duplicate content"
    first = ops.put_file("d.bin", data=data, idempotency_key="dedup-key-001")
    second = ops.put_file("d.bin", data=data, idempotency_key="dedup-key-002")
    assert second["version_id"] == first["version_id"]
    assert second["deduplicated"] is True
    assert version_count(ops) == 1
    blobs_dir = ops.blobs.blobs_dir
    assert len([p for p in blobs_dir.rglob("*") if p.is_file()]) == 1


def test_put_dir_dedup_repairs_when_one_referenced_blob_is_missing(home, tmp_path):
    ops = make_ops(home)
    source = tmp_path / "tree"
    source.mkdir()
    (source / "a.txt").write_bytes(b"a")
    first = ops.put_dir(source, "tree", idempotency_key="dir-dedup-001")
    manifest = json.loads(ops.catalog.db.execute(
        "SELECT manifest_json FROM versions WHERE version_id=?", (first["version_id"],)
    ).fetchone()[0])
    blob = ops.blobs.blob_path(next(e["sha256"] for e in manifest["entries"] if e["kind"] == "file"))
    blob.unlink()
    second = ops.put_dir(source, "tree", idempotency_key="dir-dedup-002")
    assert second["version_id"] == first["version_id"]
    assert second["deduplicated"] is True
    assert blob.is_file()


def test_put_file_different_content_new_version_advances_latest(home):
    ops = make_ops(home)
    v1 = ops.put_file("c.bin", data=b"one", idempotency_key="multi-key-0001")
    v2 = ops.put_file("c.bin", data=b"two", idempotency_key="multi-key-0002")
    assert v1["version_id"] != v2["version_id"]
    assert version_count(ops) == 2
    root = ops.catalog.db.execute("SELECT latest_version_id FROM roots WHERE name='c.bin'").fetchone()
    assert root["latest_version_id"] == v2["version_id"]


def test_replay_mismatch_different_payload_same_key(home):
    ops = make_ops(home)
    ops.put_file("m.bin", data=b"original", idempotency_key="mismatch-key1")
    with pytest.raises(ValueError, match="PROTOCOL_REPLAY_MISMATCH"):
        ops.put_file("m.bin", data=b"Different", idempotency_key="mismatch-key1")


def test_replay_mismatch_different_name_same_key(home):
    ops = make_ops(home)
    ops.put_file("a.bin", data=b"same", idempotency_key="name-key-0001")
    with pytest.raises(ValueError, match="PROTOCOL_REPLAY_MISMATCH"):
        ops.put_file("b.bin", data=b"same", idempotency_key="name-key-0001")


# ---------------------------------------------------------------------------
# Crash semantics
# ---------------------------------------------------------------------------


def test_crash_before_catalog_commit_leaves_staging_only(home):
    ops = make_ops(home)

    def explode(op_id):
        raise RuntimeError("simulated crash before publication")

    ops.crash_hook = explode
    with pytest.raises(RuntimeError, match="simulated crash"):
        ops.put_file("crash.bin", data=b"crashy bytes", idempotency_key="crash-key-001")

    # Staging remains discoverable; no version row exists.
    staging = ops.blobs.list_staging()
    assert len(staging) == 1
    orphan = staging[0]
    assert orphan.read_bytes() == b"crashy bytes"
    assert version_count(ops) == 0

    # Retry with the same key completes the operation; the crash orphan stays
    # discoverable in staging (staging GC is not a Phase 5 concern).
    ops.crash_hook = None
    result = ops.put_file("crash.bin", data=b"crashy bytes", idempotency_key="crash-key-001")
    assert version_count(ops) == 1
    assert ops.blobs.has_blob(result["sha256"])
    assert ops.blobs.list_staging() == [orphan]


def test_crash_after_version_commit_replays_to_same_version(home):
    ops = make_ops(home)
    first = ops.put_file("durable.bin", data=b"committed", idempotency_key="after-key-0001")

    # Simulate a process restart: close and reopen the sqlite database.
    ops.catalog.db.close()
    reopened = make_ops(home)
    replay = reopened.put_file("durable.bin", data=b"committed", idempotency_key="after-key-0001")
    assert replay["version_id"] == first["version_id"]
    assert version_count(reopened) == 1


# ---------------------------------------------------------------------------
# Lease fencing
# ---------------------------------------------------------------------------


def _running_op_with_expired_lease(ops: ArtifactOperations, key: str) -> tuple[str, str]:
    op, _ = ops._begin("artifact.verify", {"version_id": "ver_fenced"}, key)
    claimed = ops._claim(op["op_id"])
    old_token = claimed["claim_token"]
    ops.catalog.db.execute(
        "UPDATE operations SET lease_expires_at='2000-01-01T00:00:00Z' WHERE op_id=?",
        (op["op_id"],),
    )
    ops.catalog.db.commit()
    return op["op_id"], old_token


def test_stale_worker_cannot_commit_after_reclaim(home):
    ops = make_ops(home)
    op_id, old_token = _running_op_with_expired_lease(ops, "fence-key-0001")

    reclaimed = ops.reclaim_expired()
    assert [row["op_id"] for row in reclaimed] == [op_id]
    new_token = reclaimed[0]["claim_token"]
    assert new_token != old_token
    row = ops.catalog.db.execute("SELECT * FROM operations WHERE op_id=?", (op_id,)).fetchone()
    assert int(row["lease_generation"]) == 2

    with pytest.raises(ValueError, match="stale worker"):
        ops.complete(op_id, old_token, {"ok": True})
    completed = ops.complete(op_id, new_token, {"ok": True})
    assert completed["status"] == "completed"


def test_complete_rejects_wrong_token_and_expired_lease(home):
    ops = make_ops(home)
    op_id, token = _running_op_with_expired_lease(ops, "fence-key-0002")
    with pytest.raises(ValueError, match="stale worker"):
        ops.complete(op_id, "not-the-token", {"ok": True})
    # Even without reclaim, an expired lease cannot be committed.
    with pytest.raises(ValueError, match="stale worker"):
        ops.complete(op_id, token, {"ok": True})


# ---------------------------------------------------------------------------
# resolve / info / materialize / verify
# ---------------------------------------------------------------------------


def test_resolve_latest_alias_and_explicit(home):
    ops = make_ops(home)
    v1 = ops.put_file("res.bin", data=b"first", idempotency_key="res-key-00001")
    v2 = ops.put_file("res.bin", data=b"second", idempotency_key="res-key-00002")

    latest = ops.resolve("res.bin", idempotency_key="resolve-key-1")
    assert latest["version_id"] == v2["version_id"]
    assert latest["resolved_via"] == "latest"

    explicit = ops.resolve("res.bin", version_id=v1["version_id"], idempotency_key="resolve-key-2")
    assert explicit["version_id"] == v1["version_id"]

    ops.catalog.db.execute(
        "INSERT INTO aliases(alias_name, root_id, version_id, updated_at) VALUES ('stable', ?, ?, ?)",
        (v1["root_id"], v1["version_id"], "2026-01-01T00:00:00Z"),
    )
    ops.catalog.db.commit()
    pinned = ops.resolve("res.bin", alias="stable", idempotency_key="resolve-key-3")
    assert pinned["version_id"] == v1["version_id"]
    assert pinned["resolved_via"] == "alias"

    with pytest.raises(ValueError, match="Unknown root"):
        ops.resolve("nope.bin", idempotency_key="resolve-key-4")


def test_info_reports_blob_state(home):
    ops = make_ops(home)
    result = ops.put_file("i.bin", data=b"info bytes", idempotency_key="info-key-0001")
    info = ops.info(result["version_id"], idempotency_key="info-get-0001")
    assert info["blob_exists"] is True
    assert info["verified"] is True
    assert info["manifest"]["size_bytes"] == len(b"info bytes")


def test_materialize_atomic_and_refuses_existing_dest(home, tmp_path):
    ops = make_ops(home)
    data = b"materialize these exact bytes"
    result = ops.put_file("mat.bin", data=data, idempotency_key="mat-key-00001")
    dest = tmp_path / "out" / "copy.bin"

    materialized = ops.materialize(result["version_id"], dest, idempotency_key="mat-write-001")
    assert dest.read_bytes() == data
    assert materialized["overwritten"] is False

    with pytest.raises(ValueError, match="already exists"):
        ops.materialize(result["version_id"], dest, idempotency_key="mat-write-002")

    again = ops.materialize(result["version_id"], dest, overwrite=True, idempotency_key="mat-write-003")
    assert again["overwritten"] is True
    assert dest.read_bytes() == data


def test_materialize_refuses_symlink_destination(home, tmp_path):
    if not hasattr(Path, "symlink_to"):
        pytest.skip("symlinks unavailable")
    ops = make_ops(home)
    result = ops.put_file("link.bin", data=b"safe", idempotency_key="mat-link-001")
    real = tmp_path / "real.bin"
    real.write_bytes(b"keep")
    link = tmp_path / "link.bin"
    try:
        link.symlink_to(real)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks unavailable")
    with pytest.raises(ValueError, match="symlink"):
        ops.materialize(result["version_id"], link, overwrite=True, idempotency_key="mat-link-002")
    assert real.read_bytes() == b"keep"


def test_verify_detects_tampered_blob_as_result(home):
    ops = make_ops(home)
    data = b"tamper target"
    result = ops.put_file("t.bin", data=data, idempotency_key="ver-key-00001")
    blob = ops.blobs.blob_path(result["sha256"])
    blob.write_bytes(b"TAMPERED!")

    report = ops.verify(result["version_id"], idempotency_key="ver-run-00001")
    assert report["ok"] is False
    assert report["expected_sha256"] == result["sha256"]
    assert report["actual_sha256"] == hashlib.sha256(b"TAMPERED!").hexdigest()

    op = ops.catalog.db.execute(
        "SELECT status FROM operations WHERE idempotency_key='ver-run-00001'"
    ).fetchone()
    assert op["status"] == "completed"

    blob.write_bytes(data)
    ok_report = ops.verify(result["version_id"], idempotency_key="ver-run-00002")
    assert ok_report["ok"] is True


# ---------------------------------------------------------------------------
# Job cleanup never touches managed content
# ---------------------------------------------------------------------------


def cmd(code: str) -> str:
    return shellcmd.join([sys.executable, "-c", code])


def test_job_cleanup_preserves_managed_content(home):
    ops = make_ops(home)
    data = b"managed content survives cleanup"
    published = ops.put_file("keep.bin", data=data, idempotency_key="cleanup-key-1")
    versions_before = version_count(ops)

    manager = JobManager(home)
    try:
        job_id = asyncio.run(manager.start(cmd("import time; time.sleep(0.05)")))["job_id"]
        asyncio.run(manager.wait(job_id, ["completed"], timeout_seconds=30))
        report = manager.cleanup(0, dry_run=False)
        assert job_id in report["jobs"]
    finally:
        manager.close()

    assert version_count(ops) == versions_before
    version = ops.catalog.db.execute(
        "SELECT manifest_json FROM versions WHERE version_id=?", (published["version_id"],)
    ).fetchone()
    assert json.loads(version["manifest_json"])["sha256"] == published["sha256"]
    assert ops.blobs.blob_path(published["sha256"]).read_bytes() == data


def test_file_materialize_stages_on_destination_filesystem(home, tmp_path, monkeypatch):
    ops = make_ops(home)
    source = tmp_path / "source.bin"
    source.write_bytes(b"cross filesystem bytes")
    published = ops.put_file("cross.bin", data=source.read_bytes(), idempotency_key="cross-put")
    dest = tmp_path / "other-volume" / "result.bin"
    original = ops._publish_staged_file

    def require_same_parent(staged, destination, **kwargs):
        assert staged.parent == destination.parent
        return original(staged, destination, **kwargs)

    monkeypatch.setattr(ops, "_publish_staged_file", require_same_parent)
    ops.materialize(published["version_id"], dest, idempotency_key="cross-mat")
    assert dest.read_bytes() == source.read_bytes()
    assert list(dest.parent.glob(".vanth-materialize-*")) == []


def test_file_materialize_failure_preserves_destination_and_cleans_staging(home, tmp_path, monkeypatch):
    ops = make_ops(home)
    source = tmp_path / "source.bin"
    source.write_bytes(b"new bytes")
    published = ops.put_file("failed.bin", data=source.read_bytes(), idempotency_key="failed-mat-put")
    dest = tmp_path / "result.bin"
    dest.write_bytes(b"existing bytes")

    def fail_publish(*args, **kwargs):
        raise OSError("injected rename failure")

    monkeypatch.setattr(ops, "_publish_staged_file", fail_publish)
    with pytest.raises(OSError, match="injected"):
        ops.materialize(published["version_id"], dest, overwrite=True, idempotency_key="failed-mat")
    assert dest.read_bytes() == b"existing bytes"
    assert list(dest.parent.glob(".vanth-materialize-*")) == []
