"""Durable stores for the remote execution protocol (Phase 0).

Two stores share one idempotency contract:

- :class:`RemoteStore` lives on the **controller** (local) side and records
  remote instances, durable requests keyed by ``(remote_id,
  idempotency_key)``, replay tombstones, and minimal remote job shadows.
- :class:`RemoteOperationStore` lives on the **remote** side and records
  accepted operations keyed by ``idempotency_key`` plus replay tombstones.

Both reuse ``configure_connection`` from ``vanth.migrations`` (WAL, 30s busy
timeout, foreign keys) and the ``now_iso()`` timestamp convention from
``vanth.server``. All request mutations run in an explicit transaction so a
crash before commit leaves no trace and a crash after commit is replay-safe.
"""

from __future__ import annotations

import functools
import json
import secrets
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ..migrations import configure_connection
from ..server import now_iso
from .protocol import VanthRemoteProtocolError


def serialize_public_methods(instance) -> None:
    """Wrap every public method of ``instance`` in the instance's ``db_lock``.

    ThreadingHTTPServer dispatches HTTP handler requests on different threads
    while background dispatcher threads share the same sqlite connection;
    sqlite3 connections are not thread-safe by default and even with
    ``check_same_thread=False`` interleaved statements/transactions on one
    connection corrupt transactions. Mirrors JobManager's db_lock pattern.
    """
    lock = instance.db_lock
    for name, attr in list(vars(type(instance)).items()):
        if name.startswith("_") or not callable(attr) or isinstance(attr, (staticmethod, classmethod)):
            continue

        @functools.wraps(attr)
        def wrapper(*args, _attr=attr, _instance=instance, _lock=lock, **kwargs):
            # Instance-attribute functions are not bound, so the receiver is
            # closed over explicitly rather than injected as ``self``.
            with _lock:
                return _attr(_instance, *args, **kwargs)

        setattr(instance, name, wrapper)

CONTROLLER_DDL = """
CREATE TABLE IF NOT EXISTS remotes (
  remote_id TEXT PRIMARY KEY,
  name TEXT,
  target TEXT NOT NULL,
  state TEXT NOT NULL,
  state_epoch INTEGER NOT NULL DEFAULT 1,
  instance_id TEXT,
  controller_id TEXT,
  credential_state TEXT,
  pairing_state TEXT,
  key_path TEXT,
  known_hosts_path TEXT,
  installed_at TEXT,
  installed_authorization TEXT,
  snapshot_cursor_json TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS remote_requests (
  request_id TEXT PRIMARY KEY,
  remote_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  method TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  digest TEXT NOT NULL,
  status TEXT NOT NULL,
  expected_state_epoch INTEGER,
  expected_instance_id TEXT,
  response_json TEXT,
  error_json TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(remote_id, idempotency_key)
);
CREATE TABLE IF NOT EXISTS remote_replay_tombstones (
  tombstone_id TEXT PRIMARY KEY,
  remote_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  digest TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(remote_id, idempotency_key)
);
CREATE TABLE IF NOT EXISTS remote_shadows (
  shadow_id TEXT PRIMARY KEY,
  remote_id TEXT NOT NULL,
  remote_job_id TEXT NOT NULL,
  status TEXT NOT NULL,
  payload_json TEXT,
  state_epoch INTEGER NOT NULL DEFAULT 1,
  suppressed_at TEXT,
  superseded_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_remote_requests_remote ON remote_requests(remote_id, created_at);
CREATE INDEX IF NOT EXISTS idx_remote_requests_status_updated ON remote_requests(status, updated_at);
CREATE INDEX IF NOT EXISTS idx_remote_replay_tombstones_created ON remote_replay_tombstones(created_at);
CREATE INDEX IF NOT EXISTS idx_remote_shadows_remote ON remote_shadows(remote_id, created_at);
"""

REMOTE_OPERATION_DDL = """
CREATE TABLE IF NOT EXISTS remote_operations (
  op_id TEXT PRIMARY KEY,
  idempotency_key TEXT NOT NULL,
  method TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  digest TEXT NOT NULL,
  status TEXT NOT NULL,
  result_json TEXT,
  error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(idempotency_key)
);
CREATE TABLE IF NOT EXISTS remote_replay_tombstones (
  tombstone_id TEXT PRIMARY KEY,
  idempotency_key TEXT NOT NULL,
  digest TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(idempotency_key)
);
CREATE TABLE IF NOT EXISTS remote_state (
  id INTEGER PRIMARY KEY CHECK (id=1),
  state_epoch INTEGER NOT NULL DEFAULT 1,
  instance_id TEXT NOT NULL
);
INSERT OR IGNORE INTO remote_state(id, state_epoch, instance_id) VALUES (1, 1, '');
"""


class RemoteStore:
    """Controller-side durable store for remotes and requests."""

    def __init__(self, db) -> None:
        self.db_lock = threading.RLock()
        self.db = db
        self.db.executescript(CONTROLLER_DDL)
        self._ensure_columns()
        self.db.commit()
        serialize_public_methods(self)

    def _ensure_columns(self) -> None:
        """Add columns introduced after the initial DDL to existing databases."""
        for table, column, definition in (
            ("remotes", "snapshot_cursor_json", "TEXT"),
            ("remotes", "instance_id", "TEXT"),
            ("remotes", "installed_authorization", "TEXT"),
            ("remotes", "wrapper_path", "TEXT"),
            ("remotes", "cleanup_pending", "INTEGER NOT NULL DEFAULT 0"),
            ("remotes", "feed_cursor_json", "TEXT"),
            ("remote_requests", "expected_state_epoch", "INTEGER"),
            ("remote_requests", "expected_instance_id", "TEXT"),
            ("remote_shadows", "state_epoch", "INTEGER NOT NULL DEFAULT 1"),
            ("remote_shadows", "suppressed_at", "TEXT"),
            ("remote_shadows", "superseded_at", "TEXT"),
        ):
            existing = {row[1] for row in self.db.execute(f"PRAGMA table_info({table})").fetchall()}
            if column not in existing:
                self.db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
    # ------------------------------------------------------------------
    # Remotes
    # ------------------------------------------------------------------

    def create_remote(
        self,
        *,
        name: str | None = None,
        target: str,
        state: str = "unpaired",
        state_epoch: int = 1,
        instance_id: str | None = None,
        controller_id: str | None = None,
        credential_state: str | None = None,
        pairing_state: str = "idle",
        key_path: str | None = None,
        known_hosts_path: str | None = None,
        installed_at: str | None = None,
    ) -> dict[str, Any]:
        if not isinstance(target, str) or not target.strip():
            raise ValueError("target must be a non-empty string")
        remote_id = "rmt_" + secrets.token_hex(16)
        stamp = now_iso()
        self.db.execute(
            """
            INSERT INTO remotes(remote_id, name, target, state, state_epoch, instance_id, controller_id,
              credential_state, pairing_state, key_path, known_hosts_path, installed_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                remote_id, name, target, state, state_epoch, instance_id, controller_id,
                credential_state, pairing_state, key_path, known_hosts_path, installed_at, stamp, stamp,
            ),
        )
        self.db.commit()
        return self._remote_dict(self.db.execute(
            "SELECT * FROM remotes WHERE remote_id=?", (remote_id,)
        ).fetchone())

    def get_remote(self, remote_id: str) -> dict[str, Any]:
        row = self.db.execute("SELECT * FROM remotes WHERE remote_id=?", (remote_id,)).fetchone()
        if not row:
            raise ValueError(f"Unknown remote_id: {remote_id}")
        return self._remote_dict(row)

    def list_remotes(self) -> list[dict[str, Any]]:
        return [
            self._remote_dict(row)
            for row in self.db.execute("SELECT * FROM remotes ORDER BY created_at ASC").fetchall()
        ]

    def update_remote_state(self, remote_id: str, state: str) -> dict[str, Any]:
        transition(self._current_remote_state(remote_id), state, machine="pairing")
        self.db.execute(
            "UPDATE remotes SET state=?, updated_at=? WHERE remote_id=?",
            (state, now_iso(), remote_id),
        )
        self.db.commit()
        return self.get_remote(remote_id)

    def _current_remote_state(self, remote_id: str) -> str:
        row = self.db.execute("SELECT state FROM remotes WHERE remote_id=?", (remote_id,)).fetchone()
        if not row:
            raise ValueError(f"Unknown remote_id: {remote_id}")
        return row[0]

    def check_state_epoch(self, remote_id: str, expected_epoch: int | None) -> None:
        """Refuse to mutate through a stale state epoch.

        ``expected_epoch`` is the remote's current ``state_epoch`` as reported
        by its latest ``hello`` frame. When it is ``None`` (no handshake yet)
        nothing is enforced; when it disagrees with the controller's stored
        expectation the call raises ``VanthRemoteError`` so a restored or
        rotated timeline can never be mutated through a stale snapshot.
        """
        if expected_epoch is None:
            return
        row = self.db.execute("SELECT state_epoch FROM remotes WHERE remote_id=?", (remote_id,)).fetchone()
        if not row:
            raise ValueError(f"Unknown remote_id: {remote_id}")
        if int(row["state_epoch"]) != expected_epoch:
            from .ssh import VanthRemoteError

            raise VanthRemoteError(
                f"state_epoch mismatch: controller expects {row['state_epoch']}, remote reported {expected_epoch}"
            )

    def set_state_epoch(self, remote_id: str, state_epoch: int) -> dict[str, Any]:
        if isinstance(state_epoch, bool) or not isinstance(state_epoch, int) or state_epoch < 1:
            raise ValueError("state_epoch must be an integer >= 1")
        self.db.execute(
            "UPDATE remotes SET state_epoch=?, updated_at=? WHERE remote_id=?",
            (state_epoch, now_iso(), remote_id),
        )
        self.db.commit()
        return self.get_remote(remote_id)

    def set_instance_id(self, remote_id: str, instance_id: str) -> dict[str, Any]:
        if not isinstance(instance_id, str) or not instance_id.strip():
            raise ValueError("instance_id must be a non-empty string")
        changed = self.db.execute(
            "UPDATE remotes SET instance_id=?, updated_at=? WHERE remote_id=?",
            (instance_id, now_iso(), remote_id),
        ).rowcount
        if not changed:
            raise ValueError(f"Unknown remote_id: {remote_id}")
        self.db.commit()
        return self.get_remote(remote_id)

    @staticmethod
    def _remote_dict(row) -> dict[str, Any]:
        return dict(row)

    # ------------------------------------------------------------------
    # Durable requests
    # ------------------------------------------------------------------

    def record_request(
        self,
        *,
        remote_id: str,
        idempotency_key: str,
        method: str,
        payload: dict[str, Any],
        digest: str,
        expected_state_epoch: int | None = None,
        expected_instance_id: str | None = None,
        commit: bool = True,
    ) -> dict[str, Any]:
        if not self.db.execute("SELECT remote_id FROM remotes WHERE remote_id=?", (remote_id,)).fetchone():
            raise ValueError(f"Unknown remote_id: {remote_id}")
        if commit:
            self.db.execute("BEGIN IMMEDIATE")
        try:
            existing = self.db.execute(
                "SELECT * FROM remote_requests WHERE remote_id=? AND idempotency_key=?",
                (remote_id, idempotency_key),
            ).fetchone()
            if existing:
                if existing["digest"] != digest:
                    raise VanthRemoteProtocolError("PROTOCOL_REPLAY_MISMATCH")
                result = self._request_dict(existing)
                # Replay re-binds the caller's epoch expectation so a retry
                # carries the SAME binding the original had durably stored.
                stored_epoch = existing["expected_state_epoch"]
                if stored_epoch is not None:
                    result["expected_state_epoch"] = int(stored_epoch)
                if commit:
                    self.db.commit()
                return result
            request_id = "req_" + secrets.token_hex(16)
            stamp = now_iso()
            self.db.execute(
                """
                INSERT INTO remote_requests(request_id, remote_id, idempotency_key, method, payload_json,
                  digest, status, expected_state_epoch, expected_instance_id, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, 'creating', ?, ?, ?, ?)
                """,
                (
                    request_id, remote_id, idempotency_key, method,
                    json.dumps(payload, separators=(",", ":")), digest,
                    int(expected_state_epoch) if expected_state_epoch is not None else None,
                    expected_instance_id, stamp, stamp,
                ),
            )
            result = self._request_dict(self.db.execute(
                "SELECT * FROM remote_requests WHERE request_id=?", (request_id,)
            ).fetchone())
            if commit:
                self.db.commit()
            return result
        except BaseException:
            if commit:
                self.db.rollback()
            raise

    def get_request_by_key(self, remote_id: str, idempotency_key: str) -> dict[str, Any]:
        row = self.db.execute(
            "SELECT * FROM remote_requests WHERE remote_id=? AND idempotency_key=?",
            (remote_id, idempotency_key),
        ).fetchone()
        if not row:
            raise ValueError(f"no request found for key {idempotency_key!r} on remote {remote_id!r}")
        return self._request_dict(row)

    def record_submitting_shadow(self, remote_id: str, request: dict[str, Any]) -> dict[str, Any]:
        """Create the ``submitting`` shadow in the same transaction as the request.

        Called inside the caller's ``BEGIN IMMEDIATE``; never commits. The
        shadow's ``remote_job_id`` is not yet known, so it is keyed by the
        request id. ``update_request_status`` (which commits) also persists the
        final shadow once the remote returns a job id.
        """
        remote_job_id = request["request_id"]
        payload = {
            "request_id": request["request_id"],
            "idempotency_key": request["idempotency_key"],
            "method": request["method"],
            "status": "submitting",
        }
        if not self.db.execute(
            "SELECT shadow_id FROM remote_shadows WHERE remote_id=? AND remote_job_id=?",
            (remote_id, remote_job_id),
        ).fetchone():
            stamp = now_iso()
            self.db.execute(
                """
                INSERT INTO remote_shadows(shadow_id, remote_id, remote_job_id, status, payload_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("shd_" + secrets.token_hex(16), remote_id, remote_job_id, "submitting",
                 json.dumps(payload, separators=(",", ":")), stamp, stamp),
            )
        return self._shadow_dict(self.db.execute(
            "SELECT * FROM remote_shadows WHERE remote_id=? AND remote_job_id=?",
            (remote_id, remote_job_id),
        ).fetchone())

    def update_request_status(
        self, request_id: str, status: str, *, response: dict[str, Any] | None = None, error: str | None = None,
        commit: bool = True
    ) -> dict[str, Any]:
        if commit:
            self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute(
                "SELECT status FROM remote_requests WHERE request_id=?", (request_id,)
            ).fetchone()
            if not row:
                raise ValueError(f"Unknown request_id: {request_id}")
            transition(row[0], status, machine="request")
            response_json = json.dumps(response, separators=(",", ":")) if response is not None else None
            error_json = json.dumps(error, separators=(",", ":")) if error is not None else None
            self.db.execute(
                """
                UPDATE remote_requests SET status=?, response_json=COALESCE(?, response_json),
                  error_json=COALESCE(?, error_json), updated_at=?
                WHERE request_id=?
                """,
                (status, response_json, error_json, now_iso(), request_id),
            )
            if commit:
                self.db.commit()
            return self._request_dict(self.db.execute(
                "SELECT * FROM remote_requests WHERE request_id=?", (request_id,)
            ).fetchone())
        except BaseException:
            if commit:
                self.db.rollback()
            raise

    @staticmethod
    def _request_dict(row) -> dict[str, Any]:
        result = dict(row)
        payload = result.pop("payload_json", None)
        result["payload"] = json.loads(payload) if payload is not None else None
        result["response"] = json.loads(result["response_json"]) if result.get("response_json") else None
        result["error"] = json.loads(result["error_json"]) if result.get("error_json") else None
        return result

    # ------------------------------------------------------------------
    # Replay tombstones
    # ------------------------------------------------------------------

    def record_replay_tombstone(self, remote_id: str, idempotency_key: str, digest: str,
                                commit: bool = True) -> dict[str, Any]:
        if commit:
            self.db.execute("BEGIN IMMEDIATE")
        try:
            existing = self.db.execute(
                "SELECT * FROM remote_replay_tombstones WHERE remote_id=? AND idempotency_key=?",
                (remote_id, idempotency_key),
            ).fetchone()
            if existing:
                result = self._tombstone_dict(existing)
                if commit:
                    self.db.commit()
                return result
            tombstone_id = "tomb_" + secrets.token_hex(16)
            self.db.execute(
                """
                INSERT INTO remote_replay_tombstones(tombstone_id, remote_id, idempotency_key, digest, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (tombstone_id, remote_id, idempotency_key, digest, now_iso()),
            )
            result = self._tombstone_dict(self.db.execute(
                "SELECT * FROM remote_replay_tombstones WHERE tombstone_id=?", (tombstone_id,)
            ).fetchone())
            if commit:
                self.db.commit()
            return result
        except BaseException:
            if commit:
                self.db.rollback()
            raise

    def get_replay_tombstone(self, remote_id: str, idempotency_key: str) -> dict[str, Any]:
        row = self.db.execute(
            "SELECT * FROM remote_replay_tombstones WHERE remote_id=? AND idempotency_key=?",
            (remote_id, idempotency_key),
        ).fetchone()
        if not row:
            raise ValueError(f"no replay tombstone for key {idempotency_key!r} on remote {remote_id!r}")
        return self._tombstone_dict(row)

    def prune_requests(self, older_than_seconds: int, *, commit: bool = True) -> dict[str, int]:
        """Delete settled remote requests and replay tombstones past a TTL.

        The controller's poll loop (`feed_sync`/`events`) submits a fresh request
        per tick, so this table grows without bound while a remote wake binding
        is live. Only SETTLED requests are pruned — an in-flight
        (creating/submitting/accepted) row is the durable replay/retry
        handle and is never removed.
        """
        if older_than_seconds <= 0:
            return {"requests": 0, "tombstones": 0}
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=older_than_seconds)).isoformat().replace("+00:00", "Z")
        settled = ("completed", "failed", "lost")
        placeholders = ",".join("?" for _ in settled)
        with self.db_lock:
            if commit:
                self.db.execute("BEGIN IMMEDIATE")
            try:
                requests = self.db.execute(
                    f"DELETE FROM remote_requests WHERE status IN ({placeholders}) AND updated_at <= ?",
                    (*settled, cutoff),
                ).rowcount
                tombstones = self.db.execute(
                    "DELETE FROM remote_replay_tombstones WHERE created_at <= ?", (cutoff,)
                ).rowcount
                if commit:
                    self.db.commit()
            except BaseException:
                if commit:
                    self.db.rollback()
                raise
        return {"requests": requests, "tombstones": tombstones}

    @staticmethod
    def _tombstone_dict(row) -> dict[str, Any]:
        return dict(row)

    # ------------------------------------------------------------------
    # Remote job shadows
    # ------------------------------------------------------------------

    def upsert_shadow(
        self,
        *,
        remote_id: str,
        remote_job_id: str,
        status: str,
        payload: dict[str, Any] | None = None,
        state_epoch: int = 1,
        commit: bool = True,
    ) -> dict[str, Any] | None:
        """Upsert one shadow. Returns None when the shadow is suppressed — a
        forgotten shadow is never resurrected by a later snapshot."""
        if commit:
            self.db.execute("BEGIN IMMEDIATE")
        try:
            existing = self.db.execute(
                "SELECT shadow_id, suppressed_at, state_epoch FROM remote_shadows WHERE remote_id=? AND remote_job_id=?",
                (remote_id, remote_job_id),
            ).fetchone()
            stamp = now_iso()
            payload_json = json.dumps(payload, separators=(",", ":")) if payload is not None else None
            if existing and existing["suppressed_at"]:
                result = None
            elif existing:
                # Epoch guard (rc19 review N1): a write bound to an OLDER
                # timeline than the shadow already carries must never regress
                # it — defense in depth behind the feed-path rejection.
                existing_epoch = int(existing["state_epoch"] or 0) if "state_epoch" in existing.keys() else 0
                if existing_epoch > int(state_epoch):
                    result = None
                else:
                    self.db.execute(
                        "UPDATE remote_shadows SET status=?, payload_json=?, state_epoch=?, updated_at=? WHERE shadow_id=?",
                        (status, payload_json, state_epoch, stamp, existing[0]),
                    )
                    result = self._shadow_dict(self.db.execute(
                        "SELECT * FROM remote_shadows WHERE shadow_id=?", (existing[0],)
                    ).fetchone())
            else:
                self.db.execute(
                    """
                    INSERT INTO remote_shadows(shadow_id, remote_id, remote_job_id, status, payload_json,
                      state_epoch, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("shd_" + secrets.token_hex(16), remote_id, remote_job_id, status, payload_json, state_epoch, stamp, stamp),
                )
                result = self._shadow_dict(self.db.execute(
                    "SELECT * FROM remote_shadows WHERE remote_id=? AND remote_job_id=?",
                    (remote_id, remote_job_id),
                ).fetchone())
            if commit:
                self.db.commit()
            return result
        except BaseException:
            if commit:
                self.db.rollback()
            raise

    def current_shadows(self, remote_id: str) -> list[dict[str, Any]]:
        """Live read-path shadows for a remote: not suppressed, not superseded,
        and pinned to the remote's current snapshot epoch (old epochs are
        retained only for audit)."""
        row = self.db.execute("SELECT state_epoch FROM remotes WHERE remote_id=?", (remote_id,)).fetchone()
        if not row:
            raise ValueError(f"Unknown remote_id: {remote_id}")
        epoch = int(row["state_epoch"])
        return [
            self._shadow_dict(row_)
            for row_ in self.db.execute(
                "SELECT * FROM remote_shadows WHERE remote_id=? AND suppressed_at IS NULL "
                "AND superseded_at IS NULL AND state_epoch=? ORDER BY created_at ASC",
                (remote_id, epoch),
            ).fetchall()
        ]

    def get_shadow(self, remote_id: str, remote_job_id: str) -> dict[str, Any]:
        row = self.db.execute(
            "SELECT * FROM remote_shadows WHERE remote_id=? AND remote_job_id=? AND suppressed_at IS NULL",
            (remote_id, remote_job_id),
        ).fetchone()
        if not row:
            raise ValueError(f"no shadow for remote job {remote_job_id!r} on remote {remote_id!r}")
        return self._shadow_dict(row)

    def suppress_shadow(self, remote_id: str, remote_job_id: str, *, commit: bool = True) -> dict[str, Any]:
        """Durably suppress (forget) a shadow so no later snapshot resurrects it."""
        if commit:
            self.db.execute("BEGIN IMMEDIATE")
        try:
            stamp = now_iso()
            self.db.execute(
                "UPDATE remote_shadows SET suppressed_at=?, updated_at=? WHERE remote_id=? AND remote_job_id=?",
                (stamp, stamp, remote_id, remote_job_id),
            )
            if commit:
                self.db.commit()
        except BaseException:
            if commit:
                self.db.rollback()
            raise
        return {"remote_id": remote_id, "remote_job_id": remote_job_id, "suppressed": True}

    def supersede_old_epochs(self, remote_id: str, current_epoch: int, *, commit: bool = True) -> int:
        """Mark shadows from older snapshot epochs as superseded (audit-only)."""
        if commit:
            self.db.execute("BEGIN IMMEDIATE")
        try:
            stamp = now_iso()
            cursor = self.db.execute(
                "UPDATE remote_shadows SET superseded_at=?, updated_at=? "
                "WHERE remote_id=? AND state_epoch<? AND superseded_at IS NULL",
                (stamp, stamp, remote_id, current_epoch),
            )
            count = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
            if commit:
                self.db.commit()
            return count
        except BaseException:
            if commit:
                self.db.rollback()
            raise

    def set_snapshot_cursor(self, remote_id: str, cursor: dict[str, Any], *, commit: bool = True) -> None:
        if commit:
            self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute(
                "UPDATE remotes SET snapshot_cursor_json=?, updated_at=? WHERE remote_id=?",
                (json.dumps(cursor, separators=(",", ":")), now_iso(), remote_id),
            )
            if commit:
                self.db.commit()
        except BaseException:
            if commit:
                self.db.rollback()
            raise

    def get_snapshot_cursor(self, remote_id: str) -> dict[str, Any] | None:
        row = self.db.execute(
            "SELECT snapshot_cursor_json FROM remotes WHERE remote_id=?", (remote_id,)
        ).fetchone()
        if not row or not row["snapshot_cursor_json"]:
            return None
        return json.loads(row["snapshot_cursor_json"])

    def set_feed_cursor(self, remote_id: str, cursor: dict[str, Any] | None, *, commit: bool = True) -> None:
        """Persist the controller's change-feed cursor for a remote (Phase 4)."""
        if commit:
            self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute(
                "UPDATE remotes SET feed_cursor_json=?, updated_at=? WHERE remote_id=?",
                (json.dumps(cursor, separators=(",", ":")) if cursor is not None else None,
                 now_iso(), remote_id),
            )
            if commit:
                self.db.commit()
        except BaseException:
            if commit:
                self.db.rollback()
            raise

    def get_feed_cursor(self, remote_id: str) -> dict[str, Any] | None:
        row = self.db.execute(
            "SELECT feed_cursor_json FROM remotes WHERE remote_id=?", (remote_id,)
        ).fetchone()
        if not row or not row["feed_cursor_json"]:
            return None
        return json.loads(row["feed_cursor_json"])

    @staticmethod
    def _shadow_dict(row) -> dict[str, Any]:
        result = dict(row)
        result["payload"] = json.loads(result["payload_json"]) if result.get("payload_json") else None
        return result


class RemoteOperationStore:
    """Remote-side durable store for accepted operations and tombstones."""

    def __init__(self, db) -> None:
        # db_lock is THE serialization point: state-epoch rotation AND
        # transfer publication both hold it (rc18 review R1). Two separate
        # locks invited a db_lock->epoch_lock vs epoch_lock->db_lock
        # deadlock; a single RLock makes the publication fence atomic and
        # ordering deadlock-free (handle_request already runs under it).
        self.db_lock = threading.RLock()
        self.db = db
        self.db.executescript(REMOTE_OPERATION_DDL)
        self._ensure_columns()
        self.db.commit()
        serialize_public_methods(self)

    def _ensure_columns(self) -> None:
        """Add columns introduced after the initial DDL to existing databases."""
        for table, column, definition in (
            ("remote_state", "feed_epoch", "INTEGER NOT NULL DEFAULT 1"),
            ("remote_state", "instance_id", "TEXT"),
            ("remote_operations", "result_json", "TEXT"),
            ("remote_operations", "error", "TEXT"),
        ):
            existing = {row[1] for row in self.db.execute(f"PRAGMA table_info({table})").fetchall()}
            if column not in existing:
                self.db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        row = self.db.execute("SELECT instance_id FROM remote_state WHERE id=1").fetchone()
        if row is not None and not row[0]:
            self.db.execute(
                "UPDATE remote_state SET instance_id=? WHERE id=1",
                ("vri_" + secrets.token_hex(16),),
            )

    def record_operation(
        self, *, idempotency_key: str, method: str, payload: dict[str, Any], digest: str,
        commit: bool = True,
    ) -> dict[str, Any]:
        if commit:
            self.db.execute("BEGIN IMMEDIATE")
        try:
            existing = self.db.execute(
                "SELECT * FROM remote_operations WHERE idempotency_key=?", (idempotency_key,)
            ).fetchone()
            if existing:
                if existing["digest"] != digest:
                    raise VanthRemoteProtocolError("PROTOCOL_REPLAY_MISMATCH")
                result = self._operation_dict(existing)
                if commit:
                    self.db.commit()
                return result
            op_id = "op_" + secrets.token_hex(16)
            stamp = now_iso()
            self.db.execute(
                """
                INSERT INTO remote_operations(op_id, idempotency_key, method, payload_json, digest, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 'accepted', ?, ?)
                """,
                (
                    op_id, idempotency_key, method,
                    json.dumps(payload, separators=(",", ":")), digest, stamp, stamp,
                ),
            )
            result = self._operation_dict(self.db.execute(
                "SELECT * FROM remote_operations WHERE op_id=?", (op_id,)
            ).fetchone())
            if commit:
                self.db.commit()
            return result
        except BaseException:
            if commit:
                self.db.rollback()
            raise

    def get_operation(self, idempotency_key: str) -> dict[str, Any]:
        row = self.db.execute(
            "SELECT * FROM remote_operations WHERE idempotency_key=?", (idempotency_key,)
        ).fetchone()
        if not row:
            raise ValueError(f"no operation found for key {idempotency_key!r}")
        return self._operation_dict(row)

    def update_operation_status(
        self, op_id: str, status: str, *, payload: dict[str, Any] | None = None,
        commit: bool = True,
    ) -> dict[str, Any]:
        if commit:
            self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute(
                "SELECT status FROM remote_operations WHERE op_id=?", (op_id,)
            ).fetchone()
            if not row:
                raise ValueError(f"Unknown op_id: {op_id}")
            transition(row[0], status, machine="operation")
            payload_json = json.dumps(payload, separators=(",", ":")) if payload is not None else None
            self.db.execute(
                """
                UPDATE remote_operations SET status=?, payload_json=COALESCE(?, payload_json), updated_at=?
                WHERE op_id=?
                """,
                (status, payload_json, now_iso(), op_id),
            )
            if commit:
                self.db.commit()
            return self._operation_dict(self.db.execute(
                "SELECT * FROM remote_operations WHERE op_id=?", (op_id,)
            ).fetchone())
        except BaseException:
            if commit:
                self.db.rollback()
            raise

    def record_queued_job(self, *, op_id: str, remote_job_id: str, origin: str) -> None:
        """Persist the job-origin mapping that keeps a remote job recoverable.

        Runs inside the caller's ``BEGIN IMMEDIATE`` (the acceptance
        transaction); never commits. The mapping maps an operation's ``op_id``
        to the ``job_`` row the remote daemon inserted plus a durable launch
        intent (status ``queued``). It is a separate table from the local
        ``jobs`` table so remote machinery never feeds local PID/heartbeat/
        cleanup code.
        """
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS remote_job_origins (
              op_id TEXT PRIMARY KEY,
              remote_job_id TEXT NOT NULL,
              origin TEXT NOT NULL,
              launch_intent TEXT NOT NULL,
              created_at TEXT NOT NULL,
              UNIQUE(remote_job_id)
            )
            """,
        )
        self.db.execute(
            """
            INSERT OR REPLACE INTO remote_job_origins(op_id, remote_job_id, origin, launch_intent, created_at)
            VALUES (?, ?, ?, 'queued', ?)
            """,
            (op_id, remote_job_id, origin, now_iso()),
        )

    def record_replay_tombstone(self, idempotency_key: str, digest: str) -> dict[str, Any]:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            existing = self.db.execute(
                "SELECT * FROM remote_replay_tombstones WHERE idempotency_key=?", (idempotency_key,)
            ).fetchone()
            if existing:
                result = self._tombstone_dict(existing)
                self.db.commit()
                return result
            tombstone_id = "tomb_" + secrets.token_hex(16)
            self.db.execute(
                """
                INSERT INTO remote_replay_tombstones(tombstone_id, idempotency_key, digest, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (tombstone_id, idempotency_key, digest, now_iso()),
            )
            result = self._tombstone_dict(self.db.execute(
                "SELECT * FROM remote_replay_tombstones WHERE tombstone_id=?", (tombstone_id,)
            ).fetchone())
            self.db.commit()
            return result
        except BaseException:
            self.db.rollback()
            raise

    def get_replay_tombstone(self, idempotency_key: str) -> dict[str, Any]:
        row = self.db.execute(
            "SELECT * FROM remote_replay_tombstones WHERE idempotency_key=?", (idempotency_key,)
        ).fetchone()
        if not row:
            raise ValueError(f"no replay tombstone for key {idempotency_key!r}")
        return self._tombstone_dict(row)

    def get_state_epoch(self) -> int:
        """The remote's own state epoch (bumped when its database is restored)."""
        row = self.db.execute("SELECT state_epoch FROM remote_state WHERE id=1").fetchone()
        return int(row["state_epoch"]) if row else 1

    def get_instance_id(self) -> str:
        row = self.db.execute("SELECT instance_id FROM remote_state WHERE id=1").fetchone()
        if not row or not row["instance_id"]:
            raise ValueError("remote instance identity is not initialized")
        return str(row["instance_id"])

    def set_state_epoch(self, epoch: int) -> None:
        """Set the remote's state epoch (a database restore).

        A restore bumps BOTH epochs: ``state_epoch`` becomes the new timeline
        version and ``feed_epoch`` increments so controllers holding a feed
        cursor from the previous timeline can never resume it against the new
        one — they detect the mismatch and fall back to a full snapshot.
        """
        if isinstance(epoch, bool) or not isinstance(epoch, int) or epoch < 1:
            raise ValueError("epoch must be an integer >= 1")
        with self.db_lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                row = self.db.execute(
                    "SELECT state_epoch, feed_epoch FROM remote_state WHERE id=1"
                ).fetchone()
                current = int(row["state_epoch"]) if row else 1
                feed_epoch = int(row["feed_epoch"]) if row and row["feed_epoch"] else 1
                # Monotonic guard (review P1-1): rewinding to an OLDER timeline
                # would let a stale cursor bind again, so it is refused. Setting
                # the SAME value is not a restore — it is a no-op that keeps the
                # feed epoch stable. Only a strictly greater value rotates.
                if int(epoch) < current:
                    raise ValueError(
                        f"state epoch must be strictly monotonic: current={current}, requested={epoch}"
                    )
                if int(epoch) == current:
                    self.db.commit()
                    return
                feed_epoch += 1
                updated = self.db.execute(
                    "UPDATE remote_state SET state_epoch=?, feed_epoch=? WHERE id=1 AND state_epoch=?",
                    (int(epoch), feed_epoch, current),
                ).rowcount
                if not updated:
                    raise ValueError("state epoch moved concurrently; retry with the new value")
                self.db.commit()
            except BaseException:
                self.db.rollback()
                raise

    def get_feed_epoch(self) -> int:
        """The remote's feed epoch (bumped on every restore alongside state_epoch)."""
        try:
            row = self.db.execute("SELECT feed_epoch FROM remote_state WHERE id=1").fetchone()
            return int(row["feed_epoch"]) if row and row["feed_epoch"] else 1
        except Exception:
            return 1

    def get_remote_job_origin(self, remote_job_id: str) -> dict[str, Any] | None:
        row = self.db.execute(
            "SELECT * FROM remote_job_origins WHERE remote_job_id=?", (remote_job_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_remote_job_origin_op(self, op_id: str) -> str | None:
        row = self.db.execute(
            "SELECT remote_job_id FROM remote_job_origins WHERE op_id=?", (op_id,)
        ).fetchone()
        return row[0] if row else None

    def get_remote_operation_by_job(self, remote_job_id: str) -> str | None:
        row = self.db.execute(
            "SELECT op_id FROM remote_job_origins WHERE remote_job_id=?", (remote_job_id,)
        ).fetchone()
        return row[0] if row else None

    @staticmethod
    def _operation_dict(row) -> dict[str, Any]:
        result = dict(row)
        payload = result.pop("payload_json", None)
        result["payload"] = json.loads(payload) if payload is not None else None
        result_json = result.pop("result_json", None)
        result["result"] = json.loads(result_json) if result_json else None
        return result

    @staticmethod
    def _tombstone_dict(row) -> dict[str, Any]:
        return dict(row)


# ---------------------------------------------------------------------------
# State transitions
# ---------------------------------------------------------------------------

TRANSITION_TABLES: dict[str, dict[str, dict[str, str]]] = {
    "pairing": {
        "unpaired": {"pairing": "pairing"},
        "pairing": {"paired": "paired", "error": "error"},
        "paired": {},
        "error": {},
    },
    "request": {
        "creating": {"submitting": "submitting"},
        "submitting": {"accepted": "accepted"},
        # accepted -> submitting allows RE-DRIVING a request whose outcome
        # was retryable-pending (rc20 P1a); the remote's operation
        # idempotency guarantees at most one acceptance per key.
        "accepted": {"completed": "completed", "failed": "failed", "lost": "lost",
                     "submitting": "submitting"},
        "completed": {},
        "failed": {},
        "lost": {},
    },
    "operation": {
        "accepted": {"queued": "queued"},
        # A queued/launched operation can fail before the runner ever reports
        # running (e.g. prepare_launch refuses the job); failing it is terminal.
        "queued": {"launched": "launched", "failed": "failed"},
        "launched": {"running": "running", "failed": "failed"},
        "running": {"completed": "completed", "failed": "failed"},
        "completed": {},
        "failed": {},
    },
}


def transition(current: str, event: str, *, machine: str) -> str:
    """Return the next state for ``event`` from ``current``, or raise ValueError."""
    table = TRANSITION_TABLES.get(machine)
    if table is None:
        raise ValueError(f"unknown state machine: {machine}")
    if current not in table:
        raise ValueError(f"invalid state {current!r} for {machine} state machine")
    if event not in table[current]:
        raise ValueError(
            f"invalid transition {current!r} --({event})--> ? for {machine} state machine"
        )
    return table[current][event]
