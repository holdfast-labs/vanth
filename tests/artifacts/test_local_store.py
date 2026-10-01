"""LocalBlobStore: dedup, atomic publication, ownership marker, verification (Phase 5)."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from vanth.artifacts.catalog import open_catalog
from vanth.artifacts.local_store import (
    OWNER_MARKER_NAME,
    LocalBlobStore,
    OwnershipError,
    default_store_root,
)


@pytest.fixture()
def home(tmp_path):
    return tmp_path / "state"


@pytest.fixture()
def store(home):
    catalog = open_catalog(home)
    return LocalBlobStore(default_store_root(home), catalog)


def blob_files(store: LocalBlobStore) -> list[Path]:
    return [p for p in store.blobs_dir.rglob("*") if p.is_file()]


def test_stage_uses_staging_dir_and_final_path_absent(store):
    data = b"atomic publication"
    staged = store.stage(data)
    assert staged.parent == store.staging_dir
    assert staged.read_bytes() == data
    target = store.blob_path(hashlib.sha256(data).hexdigest())
    assert not target.exists()
    published = store.publish_staged(staged, hashlib.sha256(data).hexdigest())
    assert published["sha256"] == hashlib.sha256(data).hexdigest()
    assert published["size_bytes"] == len(data)
    assert published["blob_path"] == target
    assert target.read_bytes() == data
    assert store.list_staging() == []


def test_publish_dedups_identical_bytes(store):
    data = b"same bytes both times"
    sha = hashlib.sha256(data).hexdigest()
    first = store.publish_staged(store.stage(data), sha)
    second = store.publish_staged(store.stage(data), sha)
    assert first["blob_path"] == second["blob_path"]
    assert len(blob_files(store)) == 1


def test_stage_from_path(store, tmp_path):
    source = tmp_path / "input.bin"
    source.write_bytes(b"from a file")
    staged = store.stage(source)
    assert staged.read_bytes() == b"from a file"


def test_publish_rejects_hash_mismatch(store):
    staged = store.stage(b"actual content")
    wrong = hashlib.sha256(b"different content").hexdigest()
    with pytest.raises(ValueError, match="mismatch"):
        store.publish_staged(staged, wrong)
    # The bad staged copy is discarded; nothing was published.
    assert store.list_staging() == []
    assert not store.has_blob(wrong)


def test_verify_blob_detects_truncation_and_modification(store):
    data = b"0123456789" * 10
    sha = hashlib.sha256(data).hexdigest()
    store.publish_staged(store.stage(data), sha)
    assert store.verify_blob(sha)

    blob = store.blob_path(sha)
    truncated = data[:-5]
    blob.write_bytes(truncated)
    assert not store.verify_blob(sha)

    blob.write_bytes(b"X" + data[1:])
    assert not store.verify_blob(sha)

    blob.write_bytes(data)
    assert store.verify_blob(sha)


def test_has_and_read_blob(store):
    data = b"read me"
    sha = hashlib.sha256(data).hexdigest()
    assert not store.has_blob(sha)
    with pytest.raises(FileNotFoundError):
        store.read_blob(sha)
    store.publish_staged(store.stage(data), sha)
    assert store.has_blob(sha)
    assert store.read_blob(sha) == data


def test_ownership_marker_written_and_mismatch_refused(home, tmp_path):
    catalog_a = open_catalog(home)
    root = default_store_root(home)
    store_a = LocalBlobStore(root, catalog_a)
    marker = json.loads((root / OWNER_MARKER_NAME).read_text(encoding="utf-8"))
    identity = catalog_a.identity()
    assert marker["catalog_id"] == identity["catalog_id"]
    assert marker["instance_id"] == identity["instance_id"]

    # A different catalog instance (fresh home) may not use this store root.
    other_home = tmp_path / "other-state"
    catalog_b = open_catalog(other_home)
    with pytest.raises(OwnershipError):
        LocalBlobStore(root, catalog_b)

    # Same catalog reopened is fine.
    catalog_a2 = open_catalog(home)
    LocalBlobStore(root, catalog_a2)


def test_default_store_root_under_home(home):
    assert default_store_root(home) == home / "artifacts-store"


def test_republish_repairs_corrupt_blob_atomically(store, monkeypatch):
    import os

    data = b"valid reusable content"
    sha = hashlib.sha256(data).hexdigest()
    store.publish_staged(store.stage(data), sha)
    target = store.blob_path(sha)
    target.write_bytes(b"corrupt")
    staged = store.stage(data)
    original = os.replace

    def check_atomic(source, destination):
        assert target.read_bytes() == b"corrupt"
        assert Path(source).read_bytes() == data
        return original(source, destination)

    monkeypatch.setattr(os, "replace", check_atomic)
    store.publish_staged(staged, sha)
    assert store.verify_blob(sha)
    assert not staged.exists()


def test_failed_blob_repair_retains_staged_bytes(store, monkeypatch):
    import os

    data = b"valid retry content"
    sha = hashlib.sha256(data).hexdigest()
    store.publish_staged(store.stage(data), sha)
    target = store.blob_path(sha)
    target.write_bytes(b"corrupt")
    staged = store.stage(data)

    def fail_replace(*args, **kwargs):
        raise OSError("injected publication failure")

    monkeypatch.setattr(os, "replace", fail_replace)
    with pytest.raises(OSError, match="injected"):
        store.publish_staged(staged, sha)
    assert staged.read_bytes() == data
    assert target.read_bytes() == b"corrupt"


def test_gc_fence_never_steals_aged_live_holder(store):
    import os
    import time

    path = store.root / ".vanth-gc-fence.lock"
    with LocalBlobStore._Fence(path):
        os.utime(path, (time.time() - 10000,) * 2)
        with pytest.raises(TimeoutError):
            with LocalBlobStore._Fence(path, timeout=0.01, stale_seconds=0):
                pytest.fail("live holder must retain exclusion regardless of age")
    # The lock inode remains stable, and a successor owns it after release.
    assert path.exists()
    with LocalBlobStore._Fence(path, timeout=0.01):
        with pytest.raises(TimeoutError):
            with LocalBlobStore._Fence(path, timeout=0.01):
                pytest.fail("second holder must not bypass successor")


def test_gc_fence_is_released_when_holder_process_crashes(store):
    import subprocess
    import sys

    path = store.root / ".vanth-gc-fence.lock"
    code = (
        "import os,sys; from pathlib import Path; "
        "from vanth.artifacts.local_store import LocalBlobStore; "
        "fence=LocalBlobStore._Fence(Path(sys.argv[1])); "
        "fence.__enter__(); os._exit(17)"
    )
    child = subprocess.run([sys.executable, "-c", code, str(path)], capture_output=True, timeout=15)
    assert child.returncode == 17, child.stderr.decode(errors="replace")
    assert path.exists()
    with LocalBlobStore._Fence(path, timeout=0.1):
        pass
