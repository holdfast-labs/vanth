"""Artifact lifecycle: logical delete/restore, pin/hold, fenced GC, and
backup/restore with epoch rotation (Phase 7).

GC reachability rule (implemented exactly):

    A version is a RECLAIM candidate iff it has ``deleted_at`` set OR it is
    fully unreachable: the root's latest pointer does not reference it AND no
    collection membership AND no alias points at it AND it is not pinned.
    Additionally, anything referenced by an ACTIVE operation (status='running'
    in the operations table) is protected inside the fence.

A blob is freed only when NO remaining version references its sha256 after
candidate deletion. Because publication always stages full bytes and
``publish_staged`` re-creates a missing blob path, removing an unreferenced
blob is safe even against a concurrent publisher of identical content. The
whole decision runs inside ONE transaction under ``BEGIN IMMEDIATE`` that
re-checks pins/aliases/membership/latest and running operations, so GC is
fenced against concurrent publication.

Tombstones: blobs are content-addressed files with no catalog rows, so there
is nothing to reuse from a hard-deleted replica — ``put_file``/``put_dir``
treat any surviving ``(root_id, manifest_digest)`` row as a NORMAL dedup hit,
and such a row exists only if the version was not hard-deleted (GC removes
version rows outright).

Backup/restore: ``backup()`` writes a manual snapshot to
``<home>/backups/artifacts-<stamp>-manual.sqlite`` (distinct from the automatic
pre-migration backups). ``begin_restore()`` replaces the live catalog content
from a backup copy via the SQLite backup API, then rotates identity:
regenerates ``instance_id`` and bumps ``state_epoch``. It also sets the
``recovery_required`` marker; while set, EVERY mutating artifact operation
raises ``ValueError("recovery_required: complete restore first")`` — reads stay
available. ``complete_restore()`` clears the marker and rewrites the blob-store
ownership marker so future opens accept the new instance.
"""

from __future__ import annotations

import json
import os
import uuid
import re
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .catalog import new_id, now_iso
from .local_store import OWNER_MARKER_NAME

__all__ = ["Lifecycle"]


def _manual_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


class Lifecycle:
    def __init__(self, catalog, ops) -> None:
        self.catalog = catalog
        self.ops = ops

    # ------------------------------------------------------------------
    # Logical delete / restore / pin / hold (durable-operation pattern)
    # ------------------------------------------------------------------

    def _lifecycle_op(
        self,
        method: str,
        payload: dict[str, Any],
        idempotency_key: str | None,
        apply,  # callable(db) -> dict result; runs inside BEGIN IMMEDIATE
    ) -> dict[str, Any]:
        op, replayed = self.ops._begin(method, payload, idempotency_key or new_id("akey"))
        if replayed and op["status"] == "completed":
            return {**op["result"], "replayed": True}
        claimed = self.ops._claim(op["op_id"])
        token = claimed["claim_token"]
        db = self.catalog.db
        try:
            with self.catalog.lock:
                db.execute("BEGIN IMMEDIATE")
                try:
                    result = apply(db)
                    changed = db.execute(
                        """
                        UPDATE operations SET status='completed', result_json=?, updated_at=?,
                          claim_token=NULL, claimed_at=NULL, lease_expires_at=NULL
                        WHERE op_id=? AND claim_token=? AND status='running'
                          AND (lease_expires_at IS NULL OR lease_expires_at > ?)
                        """,
                        (
                            json.dumps(result, separators=(",", ":")),
                            now_iso(),
                            op["op_id"],
                            token,
                            now_iso(),
                        ),
                    ).rowcount
                    if not changed:
                        raise ValueError(
                            f"stale worker: operation {op['op_id']} cannot be completed with this claim "
                            "(expired lease or reclaimed generation)"
                        )
                    db.commit()
                except BaseException:
                    db.rollback()
                    raise
            return result
        except BaseException as exc:
            try:
                self.ops.fail(op["op_id"], token, str(exc))
            except ValueError:
                pass
            raise

    def _require_version(self, db, version_id: str):
        if not isinstance(version_id, str) or not version_id.strip():
            raise ValueError("version_id must be a non-empty string")
        row = db.execute("SELECT * FROM versions WHERE version_id=?", (version_id,)).fetchone()
        if not row:
            raise ValueError(f"Unknown version_id: {version_id}")
        return row

    def request_delete(self, version_id: str, *, idempotency_key: str | None = None) -> dict[str, Any]:
        """Logically delete a version (content stays until GC reclaims it).

        Destructive APIs reject aliases: a version an alias points at must
        have the alias moved away first.
        """

        def apply(db):
            self._require_version(db, version_id)
            alias = db.execute(
                "SELECT alias_name FROM aliases WHERE version_id=?", (version_id,)
            ).fetchone()
            if alias:
                raise ValueError(
                    f"version {version_id} is referenced by alias '{alias['alias_name']}'; "
                    "remove or move the alias first"
                )
            db.execute(
                "UPDATE versions SET delete_requested_at=? WHERE version_id=?",
                (now_iso(), version_id),
            )
            return {"version_id": version_id, "delete_requested": True}

        payload = {"version_id": version_id}
        return self._lifecycle_op("lifecycle.request_delete", payload, idempotency_key, apply)

    def restore(self, version_id: str, *, idempotency_key: str | None = None) -> dict[str, Any]:
        """Clear a pending logical delete request."""

        def apply(db):
            self._require_version(db, version_id)
            db.execute(
                "UPDATE versions SET delete_requested_at=NULL WHERE version_id=?", (version_id,)
            )
            return {"version_id": version_id, "delete_requested": False}

        payload = {"version_id": version_id}
        return self._lifecycle_op("lifecycle.restore", payload, idempotency_key, apply)

    def pin(self, version_id: str, hold_reason: str, *, idempotency_key: str | None = None) -> dict[str, Any]:
        """Pin/hold a version: pinned content is never reclaimed by GC."""
        if isinstance(hold_reason, bool) or not isinstance(hold_reason, str) or not hold_reason.strip():
            raise ValueError("hold_reason must be a non-empty string")

        def apply(db):
            self._require_version(db, version_id)
            db.execute(
                "UPDATE versions SET pin_hold=? WHERE version_id=?", (hold_reason, version_id)
            )
            return {"version_id": version_id, "pin_hold": hold_reason}

        payload = {"version_id": version_id, "hold_reason": hold_reason}
        return self._lifecycle_op("lifecycle.pin", payload, idempotency_key, apply)

    def unpin(self, version_id: str, *, idempotency_key: str | None = None) -> dict[str, Any]:
        """Remove a pin/hold."""

        def apply(db):
            self._require_version(db, version_id)
            db.execute("UPDATE versions SET pin_hold=NULL WHERE version_id=?", (version_id,))
            return {"version_id": version_id, "pin_hold": None}

        payload = {"version_id": version_id}
        return self._lifecycle_op("lifecycle.unpin", payload, idempotency_key, apply)

    # ------------------------------------------------------------------
    # Fenced garbage collection
    # ------------------------------------------------------------------

    @staticmethod
    def _manifest_shas(manifest_json: str) -> set[str]:
        manifest = json.loads(manifest_json)
        shas: set[str] = set()
        if manifest.get("kind") == "dir":
            for entry in manifest.get("entries", []):
                if entry.get("kind") == "file":
                    shas.add(str(entry["sha256"]))
        else:
            sha = manifest.get("sha256")
            if sha:
                shas.add(str(sha))
        return shas

    def gc(self, dry_run: bool = True, *, idempotency_key: str | None = None) -> dict[str, Any]:
        """Fenced garbage collection of reclaimable versions and their blobs.

        Returns ``{"dry_run", "candidates", "blobs_freed", "would_free_bytes"}``.
        With ``dry_run=True`` (the default) nothing is modified. The actual
        run performs the whole reachability decision inside one
        ``BEGIN IMMEDIATE`` transaction that re-checks pins, aliases,
        collection membership, root-latest pointers, and active operations.
        """

        def apply(db):
            latest = {
                row[0]
                for row in db.execute(
                    "SELECT latest_version_id FROM roots WHERE latest_version_id IS NOT NULL"
                )
            }
            aliased = {row[0] for row in db.execute("SELECT version_id FROM aliases")}
            members = {row[0] for row in db.execute("SELECT DISTINCT version_id FROM collection_versions")}
            pinned = {row[0] for row in db.execute("SELECT version_id FROM versions WHERE pin_hold IS NOT NULL")}
            # Active-operation fence: protect anything mentioned by an op that
            # is currently running (its claim may still commit a reference).
            running_refs: set[str] = set()
            running_shas: set[str] = set()
            for row in db.execute(
                "SELECT payload_json, result_json FROM operations WHERE status='running'"
            ):
                for field in (row["payload_json"], row["result_json"]):
                    if not field:
                        continue
                    running_refs |= set(re.findall(r"ver_[0-9a-f]{32}", field))
                    running_shas |= set(re.findall(r"[0-9a-f]{64}", field))
            candidates: list[str] = []
            for row in db.execute("SELECT version_id, deleted_at FROM versions"):
                vid = row["version_id"]
                if vid in aliased or vid in members or vid in pinned or vid in running_refs:
                    continue
                if row["deleted_at"] is not None or vid not in latest:
                    candidates.append(vid)

            # Referenced shas are computed EXCLUDING candidate rows so a dry
            # run reports exactly what a real run would free.
            referenced: set[str] = set()
            if candidates:
                marks = ",".join("?" * len(candidates))
                survivor_rows = db.execute(
                    f"SELECT manifest_json FROM versions WHERE version_id NOT IN ({marks})",
                    tuple(candidates),
                ).fetchall()
            else:
                survivor_rows = db.execute("SELECT manifest_json FROM versions").fetchall()
            for row in survivor_rows:
                if row[0]:
                    referenced |= self._manifest_shas(row[0])
            disk_shas = sorted(p.name for p in self.ops.blobs.blobs_dir.rglob("*") if p.is_file())
            unreferenced = [
                sha
                for sha in disk_shas
                if sha not in referenced and sha not in running_shas
            ]
            would_free_bytes = sum(
                (self.ops.blobs.blob_path(sha).stat().st_size for sha in unreferenced), 0
            )

            if dry_run:
                return {
                    "dry_run": True,
                    "candidates": sorted(candidates),
                    "blobs_freed": unreferenced,
                    "would_free_bytes": would_free_bytes,
                }

            if candidates:
                marks = ",".join("?" * len(candidates))
                db.execute(f"DELETE FROM lineage WHERE version_id IN ({marks})", tuple(candidates))
                db.execute(f"DELETE FROM versions WHERE version_id IN ({marks})", tuple(candidates))
            # Re-check after deletion: free only blobs NO remaining version
            # references. Publication re-creates missing blobs on dedup, so a
            # racing identical publish cannot lose content.
            still_referenced: set[str] = set()
            for row in db.execute("SELECT manifest_json FROM versions"):
                still_referenced |= self._manifest_shas(row["manifest_json"])
            freed = [sha for sha in unreferenced if sha not in still_referenced]
            return {
                "dry_run": False,
                "candidates": sorted(candidates),
                "blobs_freed": freed,
                "would_free_bytes": sum(self.ops.blobs.blob_path(sha).stat().st_size for sha in freed),
            }

        payload = {"dry_run": bool(dry_run)}
        op, replayed = self.ops._begin("lifecycle.gc", payload, idempotency_key or new_id("akey"))
        if replayed and op["status"] == "completed":
            return {**op["result"], "replayed": True}
        claimed = self.ops._claim(op["op_id"])
        token = claimed["claim_token"]
        db = self.catalog.db
        try:
            with self.catalog.lock:
                db.execute("BEGIN IMMEDIATE")
                try:
                    result = apply(db)
                    changed = db.execute(
                        """
                        UPDATE operations SET status='completed', result_json=?, updated_at=?,
                          claim_token=NULL, claimed_at=NULL, lease_expires_at=NULL
                        WHERE op_id=? AND claim_token=? AND status='running'
                          AND (lease_expires_at IS NULL OR lease_expires_at > ?)
                        """,
                        (
                            json.dumps(result, separators=(",", ":")),
                            now_iso(),
                            op["op_id"],
                            token,
                            now_iso(),
                        ),
                    ).rowcount
                    if not changed:
                        raise ValueError(
                            f"stale worker: operation {op['op_id']} cannot be completed with this claim "
                            "(expired lease or reclaimed generation)"
                        )
                    db.commit()
                except BaseException:
                    db.rollback()
                    raise
            # Physical blob removal happens while holding the SAME fence as
            # publication (blob replace -> catalog commit), so a concurrent
            # publisher can neither lose its blob nor interleave between our
            # reachability decision and the unlink (review P1-9). Reachability
            # is re-verified inside the fence.
            if not result["dry_run"]:
                with self.ops.blobs.gc_fence():
                    still_referenced: set[str] = set()
                    for row in db.execute("SELECT manifest_json FROM versions"):
                        still_referenced |= self._manifest_shas(row["manifest_json"])
                    freed = [sha for sha in result["blobs_freed"] if sha not in still_referenced]
                    for sha in freed:
                        try:
                            os.unlink(self.ops.blobs.blob_path(sha))
                        except OSError:
                            pass
                    result["blobs_freed"] = freed
            return result
        except BaseException as exc:
            try:
                self.ops.fail(op["op_id"], token, str(exc))
            except ValueError:
                pass
            raise

    # ------------------------------------------------------------------
    # Backup / restore with epoch rotation
    # ------------------------------------------------------------------

    def backup(self, home: str | Path | None = None) -> Path:
        """Manual sqlite backup into ``<home>/backups/artifacts-<stamp>-manual.sqlite``."""
        home = Path(home) if home is not None else self.catalog.home
        backups = home / "backups"
        backups.mkdir(parents=True, exist_ok=True)
        path = backups / f"artifacts-{_manual_stamp()}-manual.sqlite"
        destination = sqlite3.connect(path)
        try:
            self.catalog.db.backup(destination)
        finally:
            destination.close()
        return path

    def begin_restore(self, backup_path: str | Path, *, home: str | Path | None = None) -> dict[str, Any]:
        """Replace catalog content from a backup copy and lock mutations.

        Crash-safe ordering (review P1-10):
        1. The backup is VALIDATED into a temporary database first
           (integrity_check + schema version) — an invalid/newer backup never
           touches the live catalog.
        2. The live catalog is locked (``recovery_required``) BEFORE the
           backup content is copied in, so a crash at ANY later point leaves
           the catalog locked rather than writable with a stale identity.
        3. Rotation of ``instance_id``/``state_epoch`` happens with the marker
           already set; :meth:`complete_restore` re-binds the blob owner
           marker FIRST and only then clears the flag.
        """
        source_path = Path(backup_path)
        if not source_path.is_file():
            raise ValueError(f"backup not found: {source_path}")
        home = Path(home) if home is not None else self.catalog.home
        from .migrations import ARTIFACTS_LATEST_SCHEMA_VERSION, migrate_artifacts

        # Prepare the complete replacement before touching the live catalog.
        # The recovery marker and rotated identity live in this copy too.  If
        # SQLite backup is interrupted after any page is copied, the target
        # can therefore never briefly contain an unlocked restored catalog.
        # PID alone collides when two restores run concurrently in ONE
        # process (threads) — add a random suffix (Sol review P2).
        prepared = home / "backups" / (
            f"restore-prepared-{os.getpid()}-{uuid.uuid4().hex[:12]}.sqlite"
        )
        prepared.parent.mkdir(parents=True, exist_ok=True)
        prepared.unlink(missing_ok=True)
        shutil.copyfile(source_path, prepared)
        try:
            probe = sqlite3.connect(prepared)
            try:
                check = probe.execute("PRAGMA integrity_check").fetchone()[0]
                if check != "ok":
                    raise ValueError(f"backup failed integrity_check: {check}")
                version = int(probe.execute("PRAGMA user_version").fetchone()[0])
                if version > ARTIFACTS_LATEST_SCHEMA_VERSION:
                    raise ValueError(
                        f"backup schema v{version} is newer than this binary supports "
                        f"(v{ARTIFACTS_LATEST_SCHEMA_VERSION})"
                    )
            finally:
                probe.close()
            prepared_db = sqlite3.connect(prepared)
            try:
                prepared_db.row_factory = sqlite3.Row
                migrate_artifacts(prepared_db, home)
                # A syntactically valid SQLite file is not a Vanth catalog.
                # Require the restored database to carry the catalog row and
                # core tables before ANY live content is replaced; otherwise a
                # bogus file would wipe the live catalog (leaving an empty,
                # locked database and no usable error).
                if prepared_db.execute("SELECT id FROM catalog LIMIT 1").fetchone() is None:
                    raise ValueError(
                        "backup is not a Vanth artifacts catalog: missing the catalog row"
                    )
                required = {
                    "catalog", "versions", "roots", "aliases", "operations", "catalog_state",
                }
                present = {
                    row[0]
                    for row in prepared_db.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                missing_tables = sorted(required - present)
                if missing_tables:
                    raise ValueError(
                        "backup is not a Vanth artifacts catalog: missing tables "
                        + ", ".join(missing_tables)
                    )
                from .catalog import set_recovery_required

                prepared_db.execute("BEGIN IMMEDIATE")
                prepared_db.execute(
                    "UPDATE catalog SET instance_id=?, state_epoch=state_epoch+1, updated_at=? "
                    "WHERE id=(SELECT id FROM catalog LIMIT 1)",
                    (new_id("cit"), now_iso()),
                )
                set_recovery_required(prepared_db, True)
                prepared_db.commit()
            finally:
                prepared_db.close()

            # --- pre-lockout BEFORE any content moves ----------------------
            db = self.catalog.db
            with self.catalog.lock:
                from .catalog import set_recovery_required

                set_recovery_required(db, True)
                db.commit()
                prepared_conn = sqlite3.connect(prepared)
                try:
                    prepared_conn.backup(db)
                finally:
                    prepared_conn.close()
                db.commit()
                row = db.execute("SELECT * FROM catalog LIMIT 1").fetchone()
                return {
                    "restored_from": str(source_path),
                    "catalog_id": row["id"],
                    "instance_id": row["instance_id"],
                    "state_epoch": int(row["state_epoch"]),
                    "recovery_required": True,
                }
        finally:
            prepared.unlink(missing_ok=True)

    def complete_restore(self) -> dict[str, Any]:
        """Clear the recovery marker and re-bind the blob store ownership.

        Ordering (review P1-10): the blob-store owner marker is rewritten and
        durable FIRST; only then is the recovery flag cleared, so a crash in
        between leaves the catalog locked rather than writable against
        mismatched ownership."""
        from .catalog import set_recovery_required

        db = self.catalog.db
        with self.catalog.lock:
            row = db.execute("SELECT * FROM catalog LIMIT 1").fetchone()
        # 1. Ownership metadata first — durably.
        marker = self.ops.blobs.root / OWNER_MARKER_NAME
        payload = {
            "catalog_id": row["id"],
            "instance_id": row["instance_id"],
            "created_at": now_iso(),
        }
        tmp = self.ops.blobs.root / (OWNER_MARKER_NAME + ".tmp")
        tmp.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
        os.replace(tmp, marker)
        # 2. Only then unlock mutations.
        with self.catalog.lock:
            db.execute("BEGIN IMMEDIATE")
            try:
                set_recovery_required(db, False)
                db.commit()
            except BaseException:
                db.rollback()
                raise
        return {
            "catalog_id": row["id"],
            "instance_id": row["instance_id"],
            "state_epoch": int(row["state_epoch"]),
            "recovery_required": False,
            "was_locked": True,
            "_marker_written": marker.exists(),
        }
