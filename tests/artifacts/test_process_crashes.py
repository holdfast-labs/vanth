"""Abrupt process exit at durable blob/catalog/GC boundaries, isolated homes."""
import json
import subprocess
import sys
from pathlib import Path

import pytest

from vanth.artifacts.catalog import open_catalog
from vanth.artifacts.lifecycle import Lifecycle
from vanth.artifacts.local_store import LocalBlobStore
from vanth.artifacts.operations import ArtifactOperations


def open_ops(home):
    catalog = open_catalog(home)
    return ArtifactOperations(catalog, LocalBlobStore(home / "artifacts-store", catalog))


def assert_all_referenced_bytes_valid(ops):
    assert ops.catalog.db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    versions = list(ops.catalog.db.execute("SELECT version_id,manifest_json FROM versions"))
    for row in versions:
        manifest = json.loads(row["manifest_json"])
        assert ops.blobs.verify_blob(manifest["sha256"]), row["version_id"]
    for row in ops.catalog.db.execute("SELECT latest_version_id FROM roots WHERE latest_version_id IS NOT NULL"):
        assert row[0] in {version["version_id"] for version in versions}
    return versions


_SETUP = """
import os,sys
from pathlib import Path
from vanth.artifacts.catalog import open_catalog
from vanth.artifacts.lifecycle import Lifecycle
from vanth.artifacts.local_store import LocalBlobStore
from vanth.artifacts.operations import ArtifactOperations
home=Path(sys.argv[1])
catalog=open_catalog(home)
ops=ArtifactOperations(catalog,LocalBlobStore(home/'artifacts-store',catalog))
"""


@pytest.mark.parametrize("boundary", ["published", "committed"])
def test_abrupt_publication_crash_preserves_committed_references(tmp_path, boundary):
    home = tmp_path / "home"
    ops = open_ops(home)
    seed = ops.put_file("baseline", data=b"baseline survives", idempotency_key="baseline-key")
    ops.catalog.db.close()
    if boundary == "published":
        injection = """
original=ops.blobs.publish_staged
def publish_then_die(*args,**kwargs):
    original(*args,**kwargs)
    os._exit(17)
ops.blobs.publish_staged=publish_then_die
"""
    else:
        # The intent is retired strictly after the version/root/result commit.
        injection = """
original=Path.unlink
def die_before_intent_retirement(path,*args,**kwargs):
    if path.name.endswith('.intent.json'):
        os._exit(17)
    return original(path,*args,**kwargs)
Path.unlink=die_before_intent_retirement
"""
    child = subprocess.run([sys.executable, "-c", _SETUP + injection +
                            "ops.put_file('crashy',data=b'crash payload',idempotency_key='crash-put-key')",
                            str(home)], capture_output=True, timeout=15)
    assert child.returncode == 17, child.stderr.decode(errors="replace")
    reopened = open_ops(home)
    try:
        versions = assert_all_referenced_bytes_valid(reopened)
        assert seed["version_id"] in {row["version_id"] for row in versions}
        assert len(versions) == (1 if boundary == "published" else 2)
        state = reopened.catalog.db.execute(
            "SELECT status FROM operations WHERE idempotency_key='crash-put-key'").fetchone()[0]
        assert state == ("running" if boundary == "published" else "completed")
        assert list(reopened.blobs.staging_dir.glob("*.intent.json"))
        if boundary == "committed":
            replay = reopened.put_file("crashy", data=b"crash payload", idempotency_key="crash-put-key")
            assert replay["replayed"] is True
            assert reopened.blobs.read_blob(replay["sha256"]) == b"crash payload"
        # GC after a publication crash may remove only unreferenced bytes.
        Lifecycle(reopened.catalog, reopened).gc(dry_run=False, idempotency_key="post-crash-gc")
        assert_all_referenced_bytes_valid(reopened)
    finally:
        reopened.catalog.db.close()


def test_abrupt_gc_crash_after_catalog_commit_preserves_latest_bytes(tmp_path):
    home = tmp_path / "home"
    ops = open_ops(home)
    old = ops.put_file("model", data=b"old collectible", idempotency_key="old-version-key")
    latest = ops.put_file("model", data=b"latest durable", idempotency_key="latest-version-key")
    ops.catalog.db.close()
    injection = """
original=os.unlink
def die_before_blob_unlink(path,*args,**kwargs):
    if 'blobs' in Path(path).parts:
        os._exit(17)
    return original(path,*args,**kwargs)
os.unlink=die_before_blob_unlink
Lifecycle(catalog,ops).gc(dry_run=False,idempotency_key='crashy-gc-key')
"""
    child = subprocess.run([sys.executable, "-c", _SETUP + injection, str(home)],
                           capture_output=True, timeout=15)
    assert child.returncode == 17, child.stderr.decode(errors="replace")
    reopened = open_ops(home)
    try:
        versions = assert_all_referenced_bytes_valid(reopened)
        assert [row["version_id"] for row in versions] == [latest["version_id"]]
        assert reopened.blobs.verify_blob(latest["sha256"])
        assert reopened.blobs.has_blob(old["sha256"]), "crash must occur before physical removal"
        assert reopened.catalog.db.execute(
            "SELECT status FROM operations WHERE idempotency_key='crashy-gc-key'").fetchone()[0] == "completed"
        Lifecycle(reopened.catalog, reopened).gc(dry_run=False, idempotency_key="restarted-gc-key")
        assert not reopened.blobs.has_blob(old["sha256"])
        assert_all_referenced_bytes_valid(reopened)
    finally:
        reopened.catalog.db.close()
