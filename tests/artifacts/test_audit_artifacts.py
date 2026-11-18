"""Audit regressions: restore input validation."""

from __future__ import annotations

import sqlite3

import pytest

from vanth.artifacts.catalog import open_catalog
from vanth.artifacts.lifecycle import Lifecycle
from vanth.artifacts.local_store import LocalBlobStore, default_store_root
from vanth.artifacts.operations import ArtifactOperations


def make_ops(tmp_path):
    home = tmp_path / "state"
    catalog = open_catalog(home)
    blobs = LocalBlobStore(default_store_root(home), catalog)
    return home, catalog, ArtifactOperations(catalog, blobs)


def test_restore_rejects_non_catalog_sqlite_without_touching_live(tmp_path):
    home, catalog, ops = make_ops(tmp_path)
    keep = ops.put_file("keep.bin", data=b"must survive", idempotency_key="audit-keep-01")
    lifecycle = Lifecycle(catalog, ops)

    bogus = tmp_path / "bogus.sqlite"
    conn = sqlite3.connect(bogus)
    conn.execute("CREATE TABLE unrelated(x INTEGER)")
    conn.commit()
    conn.close()

    with pytest.raises(ValueError, match="not a Vanth artifacts catalog"):
        lifecycle.begin_restore(bogus)

    # The live catalog is untouched: no wiped rows and no recovery lockout.
    assert catalog.db.execute(
        "SELECT COUNT(*) FROM versions WHERE version_id=?", (keep["version_id"],)
    ).fetchone()[0] == 1
    assert catalog.db.execute("SELECT COUNT(*) FROM catalog").fetchone()[0] == 1
    marker = catalog.db.execute(
        "SELECT value FROM catalog_state WHERE key='recovery_required'"
    ).fetchone()
    assert marker is None


def test_restore_rejects_catalog_without_core_tables(tmp_path):
    home, catalog, ops = make_ops(tmp_path)
    keep = ops.put_file("keep.bin", data=b"must survive", idempotency_key="audit-keep-02")
    lifecycle = Lifecycle(catalog, ops)

    # A file that claims the latest schema version but only has a catalog row;
    # migration is a no-op at this version, so validation must catch the gap.
    crafted = tmp_path / "crafted.sqlite"
    conn = sqlite3.connect(crafted)
    conn.execute(
        "CREATE TABLE catalog (id TEXT PRIMARY KEY, instance_id TEXT NOT NULL,"
        " state_epoch INTEGER NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)"
    )
    conn.execute(
        "INSERT INTO catalog VALUES ('cat_x', 'cit_x', 1, 't', 't')"
    )
    conn.execute("PRAGMA user_version=3")
    conn.commit()
    conn.close()

    with pytest.raises(ValueError, match="missing tables"):
        lifecycle.begin_restore(crafted)

    assert catalog.db.execute(
        "SELECT COUNT(*) FROM versions WHERE version_id=?", (keep["version_id"],)
    ).fetchone()[0] == 1
