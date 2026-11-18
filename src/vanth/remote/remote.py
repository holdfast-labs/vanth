"""Remote-side accepted operations and dispatch (Phase 2).

:class:`RemoteJobManager` runs inside the remote daemon and handles protocol
frames forwarded by the forced-command helper over its authenticated loopback
API (``POST /remote/helper``).

- ``handle_request`` validates the frame, checks the state epoch, and commits
  the operation (``accepted``), the queued job row, the origin mapping, and the
  launch intent in **one** ``BEGIN IMMEDIATE`` transaction, then transitions the
  operation ``accepted -> queued`` (plan §Phase 2 "One transaction on the
  remote daemon that commits the operation, queued job, origin mapping, and
  launch intent before starting a runner").
- Replaying the same ``idempotency_key`` with the same digest returns the
  accepted operation (no second job); a different digest is rejected with
  ``PROTOCOL_REPLAY_MISMATCH``.
- ``_dispatch_loop`` is a daemon thread that polls ``remote_operations`` for
  ``queued`` rows and launches each underlying job through the refactored
  ``JobManager.prepare_launch`` / ``_launch_prepared`` path (never
  ``JobManager.start``), transitioning ``queued -> launched -> running`` and, on
  runner exit, ``completed`` / ``failed``. Because the operation, queued job,
  origin mapping, and launch intent all commit before the runner starts, the
  remote daemon can restart between acceptance and launch and the dispatcher
  resumes the ``queued`` row from the shared sqlite.

Remote rows live only in the remote-side tables (``remote_operations``,
``remote_replay_tombstones``, ``remote_job_origins``). The local ``JobManager``
never reads them: local PID, heartbeat, stdin, timeout, stop, recovery,
trigger, quota, and cleanup code stay entirely on the local ``jobs`` table.
"""

from __future__ import annotations

import json
import os
import re
import secrets
import stat as _stat
import threading
import time
import uuid
from contextlib import nullcontext
from pathlib import Path
from typing import Any

from .feed import FeedStore
from .protocol import (
    VanthRemoteProtocolError,
    decode_frame,
    encode_frame,
    request_digest,
    validate_request,
)
from .store import RemoteOperationStore
from .transfer import TransferRegistry, _TRANSFER_METHODS
from ..server import TERMINAL_STATUSES, now_iso, validate_policy


class _FakeSession:
    """A session-like object for the default in-process transport.

    The real helper forwards frames to the remote daemon over loopback HTTP;
    the daemon route uses the in-process transport below. A future concrete
    transport may talk over a real socket; the injectable seam is the same.
    """

    def __init__(self, handler) -> None:
        self._handler = handler

    def exchange(self, frame_bytes: bytes) -> str:
        frame = decode_frame(frame_bytes.decode("utf-8").rstrip("\n"))
        response = self._handler(frame)
        return encode_frame(response).decode("utf-8").rstrip("\n")


class RemoteJobManager:
    """Remote daemon side: accept operations, dispatch queued jobs."""

    def __init__(self, store: RemoteOperationStore, manager: Any, *, home: Any = None) -> None:
        self.store = store
        self.manager = manager
        self.home = home
        self.feed = FeedStore(store.db)
        self.transfers = TransferRegistry(
            store.db,
            epoch_fn=self._remote_state_epoch,
            ops_factory=self._remote_artifact_ops,
            staging_dir=(Path(home) / "remote-transfers") if home else None,
            fence_lock=self.store.db_lock,
        )
        self.dispatcher_stop = threading.Event()
        self.dispatcher_thread: threading.Thread | None = None
        self.poll_interval = float(__import__("os").environ.get("VANTH_REMOTE_POLL_INTERVAL", "0.2"))
        self._shutdown = False
        self._snapshot_cache: dict[str, dict[str, Any]] = {}

    def _remote_artifact_ops(self) -> Any:
        """The remote host's own ArtifactOperations (Phase 9 publication)."""
        from ..artifacts.catalog import open_catalog
        from ..artifacts.local_store import LocalBlobStore, default_store_root
        from ..artifacts.operations import ArtifactOperations

        if not self.home:
            from ..paths import canonical_home

            home = canonical_home()
        else:
            home = Path(self.home)
        catalog = open_catalog(home)
        blobs = LocalBlobStore(default_store_root(home), catalog)
        return ArtifactOperations(catalog, blobs)

    def start(self) -> "RemoteJobManager":
        self.dispatcher_thread = threading.Thread(target=self._dispatch_loop, daemon=True)
        self.dispatcher_thread.start()
        return self

    def stop(self) -> None:
        self.dispatcher_stop.set()
        if self.dispatcher_thread and self.dispatcher_thread is not threading.current_thread():
            self.dispatcher_thread.join(timeout=2)

    def close(self) -> None:
        self.stop()
        self._shutdown = True

    # ------------------------------------------------------------------
    # Frame handling (invoked by the daemon's POST /remote/helper route)
    # ------------------------------------------------------------------

    def handle_request(self, frame: dict[str, Any]) -> dict[str, Any]:
        """Handle one request frame; returns a response or error frame.

        The ENTIRE handler runs under the store lock (review rc14 P1-1):
        multi-statement BEGIN/commit sequences on the shared connection were
        interleaved by concurrent HTTP handlers and the dispatcher, producing
        nested-transaction corruption under load."""
        with self.store.db_lock:
            return self._handle_request_locked(frame)

    def _handle_request_locked(self, frame: dict[str, Any]) -> dict[str, Any]:
        try:
            method = frame["method"]
            payload = frame["payload"]
            idempotency_key = frame["idempotency_key"]
        except KeyError:
            return self._error_frame(frame, code="INVALID_REQUEST", message="missing request fields")
        digest = frame.get("digest")
        if digest is None:
            digest = request_digest(method, payload, idempotency_key)
        try:
            return self.handle_request_checked(frame, method, payload, idempotency_key, digest)
        except VanthRemoteProtocolError as exc:
            return self._error_frame(frame, code=exc.code, message=str(exc))
        except Exception as exc:
            return self._error_frame(frame, code="INVALID_REQUEST", message=str(exc))

    _MUTATION_REQUEST_METHODS = frozenset({"job.start", "job.stop", "job.rerun"})

    def handle_request_checked(
        self,
        frame: dict[str, Any],
        method: str,
        payload: dict[str, Any],
        idempotency_key: str,
        digest: str,
    ) -> dict[str, Any]:
        validate_request(method, payload)
        # State-epoch fencing: a mutation bound to a stale remote timeline is
        # refused transactionally before replay/acceptance (review P1-1).
        if method in self._MUTATION_REQUEST_METHODS:
            expected = frame.get("expected_state_epoch")
            expected_instance = frame.get("expected_instance_id")
            if expected is None or not isinstance(expected_instance, str) or not expected_instance:
                return self._error_frame(
                    frame, code="INVALID_REQUEST",
                    message="remote mutation requires expected_state_epoch and expected_instance_id",
                )
            current = self.store.get_state_epoch()
            if int(expected) != current:
                return self._error_frame(
                    frame, code="STATE_EPOCH_MISMATCH",
                    message=f"request bound to epoch {int(expected)}, remote is at {current}",
                )
            current_instance = self.store.get_instance_id()
            if expected_instance != current_instance:
                return self._error_frame(
                    frame, code="INSTANCE_ID_MISMATCH",
                    message="request bound to a different remote instance",
                )
        if method == "job.status":
            return self._handle_status(frame, payload, idempotency_key)
        if method == "job.snapshot":
            return self.handle_snapshot_request(frame, payload)
        if method == "job.log_range":
            return self.handle_log_range_request(frame, payload)
        if method == "job.feed":
            return self.handle_feed_request(frame, payload)
        if method == "job.events":
            return self.handle_events_request(frame, payload)
        if method in _TRANSFER_METHODS:
            return self._handle_transfer_frame(frame, method, payload)
        return self._handle_mutation(frame, method, payload, idempotency_key, digest)

    def _handle_transfer_frame(self, frame: dict[str, Any], method: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Route artifact transfer frames to the Phase 9 TransferRegistry."""
        try:
            if method == "artifact.transfer_init":
                result = self.transfers.init(payload)
            elif method == "artifact.blob_chunk":
                result = self.transfers.chunk(payload)
            else:
                result = self.transfers.complete(payload)
        except VanthRemoteProtocolError as exc:
            return self._error_frame(frame, code=exc.code, message=str(exc))
        except Exception as exc:
            return self._error_frame(frame, code="INVALID_REQUEST", message=str(exc))
        return self._response_frame(frame, result)

    def _handle_status(self, frame: dict[str, Any], payload: dict[str, Any], idempotency_key: str) -> dict[str, Any]:
        job_id = payload["job_id"]
        origin = self.store.get_remote_job_origin(job_id)
        status = None
        if origin:
            row = self.store.db.execute("SELECT status FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row:
                status = row["status"]
        result = {
            "job_id": job_id,
            "found": origin is not None,
            "origin": origin,
        }
        if status is not None:
            result["status"] = status
        return self._response_frame(frame, result)

    def _handle_mutation(
        self, frame: dict[str, Any], method: str, payload: dict[str, Any], idempotency_key: str, digest: str
    ) -> dict[str, Any]:
        # Dispatch by method: job.start creates a queued job; job.rerun resolves
        # the original run spec and queues exactly ONE rerun under its own
        # idempotency record; job.stop stops the TARGET job. Routing everything
        # through one "create a queued job" path made stop/rerun spawn
        # unrelated workloads (review P0-3).
        if method == "job.stop":
            return self._handle_stop(frame, payload, idempotency_key, digest)
        return self._handle_start_or_rerun(frame, method, payload, idempotency_key, digest)

    def _record_op_uncommitted(self, idempotency_key: str, method: str, payload: dict[str, Any], digest: str) -> tuple[dict[str, Any], bool]:
        op = self.store.record_operation(
            idempotency_key=idempotency_key,
            method=method,
            payload=payload,
            digest=digest,
            commit=False,
        )
        return op, op["status"] != "accepted"

    def _handle_stop(self, frame: dict[str, Any], payload: dict[str, Any], idempotency_key: str, digest: str) -> dict[str, Any]:
        """Stop the target job on this remote. Idempotent per caller key."""
        self.store.db.execute("BEGIN IMMEDIATE")
        try:
            op, replayed = self._record_op_uncommitted(idempotency_key, "job.stop", payload, digest)
            self.store.db.commit()
        except BaseException:
            self.store.db.rollback()
            raise
        if replayed and op.get("result") is not None:
            # Durable result replay: never re-execute or spawn anything.
            return self._response_frame(frame, {**op["result"], "replayed": True})
        job_id = payload["job_id"]
        stopped_status = None
        permanent_error = None
        transient_error = None
        try:
            row = self.store.db.execute("SELECT status FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if row is None:
                permanent_error = f"unknown job on remote: {job_id}"
            elif row["status"] in TERMINAL_STATUSES:
                stopped_status = row["status"]
            else:
                stop_result = self.manager.stop_sync(job_id)
                stopped_status = stop_result.get("status")
        except Exception as exc:
            # Transient execution failure: the op STAYS nonterminal so the
            # dispatcher retries it on the next tick (rc17 review F1). Only
            # validation failures are terminal here.
            transient_error = str(exc)
        error = permanent_error
        result_status = "error" if error else ("stopping" if transient_error else (stopped_status or "stopping"))
        result = {
            "op_id": op["op_id"],
            "job_id": job_id,
            "status": result_status,
            "retrying": bool(transient_error),
        }
        if error:
            result["error"] = error
        elif transient_error:
            result["transient_error"] = transient_error
        self.store.db.execute("BEGIN IMMEDIATE")
        try:
            if error is None and transient_error is not None:
                # Leave the intent for _reconcile_stop_intent; no durable
                # terminal result, so a retry with this key re-drives the stop.
                self.store.db.execute(
                    "UPDATE remote_operations SET updated_at=? WHERE op_id=?",
                    (now_iso(), op["op_id"]),
                )
            else:
                self.store.db.execute(
                    "UPDATE remote_operations SET status=?, result_json=?, updated_at=? WHERE op_id=?",
                    (
                        "failed" if error else "completed",
                        json.dumps(result, separators=(",", ":")), now_iso(), op["op_id"],
                    ),
                )
            if not error and not transient_error and stopped_status in TERMINAL_STATUSES:
                payload_row = self.store.db.execute(
                    "SELECT job_id, name, command, status, created_at, updated_at, exit_code "
                    "FROM jobs WHERE job_id=?",
                    (job_id,),
                ).fetchone()
                upsert_payload = dict(payload_row) if payload_row else {
                    "job_id": job_id, "status": stopped_status,
                }
                self.feed.append_in_tx(
                    "job.upsert", job_id=job_id,
                    payload=upsert_payload,
                )
            self.store.db.commit()
        except BaseException:
            self.store.db.rollback()
            raise
        if error is None and transient_error is not None:
            # A RETRYABLE error (rc19 review / rc20 P1a): the intent stays
            # queued here, and the dedicated code tells the controller to
            # keep its request pending instead of failing it terminally.
            return self._error_frame(
                frame, code="OPERATION_RETRY_PENDING",
                message=f"stop temporarily failed; intent remains queued for retry: {transient_error}",
            )
        return self._response_frame(frame, result)

    def _handle_start_or_rerun(self, frame, method, payload, idempotency_key, digest):
        if method == "job.rerun":
            source = self._resolve_rerun_spec(payload["job_id"])
            if isinstance(source, str):
                return self._response_frame(frame, {
                    "op_id": None, "job_id": payload["job_id"],
                    "status": "error", "error": source,
                })
            run_payload = {**source, **{k: v for k, v in payload.items() if v is not None and k != "job_id" and k != "idempotency_key"}}
            run_payload.setdefault("command", source.get("command", ""))
        else:
            run_payload = payload
            if "policy" in run_payload:
                try:
                    validate_policy(run_payload.get("policy"))
                except ValueError as exc:
                    raise VanthRemoteProtocolError("INVALID_REQUEST", f"invalid policy: {exc}")
        self.store.db.execute("BEGIN IMMEDIATE")
        try:
            op, replayed = self._record_op_uncommitted(idempotency_key, method, run_payload, digest)
            job_id = None
            if not replayed:
                job_id = "job_" + secrets.token_hex(16)[:12]
                self._insert_queued_job_uncommitted(job_id, run_payload)
                origin = "remote:" + idempotency_key
                self.store.record_queued_job(
                    op_id=op["op_id"],
                    remote_job_id=job_id,
                    origin=origin,
                )
                self.store.update_operation_status(op["op_id"], "queued", commit=False)
                # Outbox row commits ATOMICALLY with the acceptance — a crash
                # after commit can no longer lose the change (review P1-6).
                self.feed.append_in_tx(
                    "job.upsert", job_id=job_id,
                    payload={"job_id": job_id, "status": "queued"},
                )
            else:
                # Replay returns the SAME job — never a second incarnation.
                job_id = self.store.get_remote_job_origin_op(op["op_id"])
            self.store.db.commit()
        except BaseException:
            self.store.db.rollback()
            raise
        if replayed:
            return self._response_frame(frame, {
                "op_id": op["op_id"],
                "job_id": job_id,
                "status": op["status"],
                "replayed": True,
            })
        return self._response_frame(frame, {
            "op_id": op["op_id"],
            "job_id": job_id,
            "status": "queued",
        })

    def _resolve_rerun_spec(self, job_id: str) -> dict[str, Any] | str:
        """Resolve the immutable run spec of an existing remote job for rerun.

        Returns the spec dict, or an error string when the job is unknown.
        """
        row = self.store.db.execute(
            "SELECT command, cwd, env_json, timeout_seconds, name, notes, tags_json, policy_json FROM jobs WHERE job_id=?",
            (job_id,),
        ).fetchone()
        if row is None:
            return f"unknown job on remote: {job_id}"
        try:
            env = json.loads(row["env_json"] or "{}")
            tags = json.loads(row["tags_json"] or "[]")
            policy = json.loads(row["policy_json"] or "null")
        except ValueError:
            env, tags, policy = {}, [], None
        spec = {
            "command": row["command"],
            "cwd": row["cwd"],
            "name": row["name"],
            "notes": row["notes"],
            "timeout_seconds": row["timeout_seconds"],
            "env": env,
            "tags": tags,
        }
        if policy:
            spec["policy"] = policy
        return {k: v for k, v in spec.items() if v is not None}

    def _insert_queued_job_uncommitted(self, job_id: str, payload: dict[str, Any]) -> None:
        """Insert the queued job row + origin mapping inside the caller's transaction."""
        command = payload.get("command") or "true"
        cwd = payload.get("cwd")
        name = payload.get("name")
        timeout_seconds = payload.get("timeout_seconds")
        env = payload.get("env") or {}
        tags = payload.get("tags") or []
        notes = payload.get("notes")
        interactive = payload.get("interactive")
        notify_on = payload.get("notify_on") or []
        trigger = payload.get("trigger")
        policy = payload.get("policy")
        created_at = now_iso()
        self.store.db.execute(
            """
            INSERT INTO jobs(
              job_id, name, command, cwd, status, created_at, updated_at,
              timeout_seconds, notify_on, tags_json, env_json, notes, run_json,
              stdout_path, stderr_path, events_path, trigger_json, policy_json
            )
            VALUES (?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id, name, command, cwd, created_at, created_at,
                timeout_seconds,
                json.dumps(notify_on, separators=(",", ":")),
                json.dumps(tags, separators=(",", ":")),
                json.dumps(env, separators=(",", ":")),
                notes,
                json.dumps({"interactive": bool(interactive)}, separators=(",", ":")),
                str(self.manager.logs / f"{job_id}.stdout.log"),
                str(self.manager.logs / f"{job_id}.stderr.log"),
                str(self.manager.events_dir / f"{job_id}.jsonl"),
                json.dumps(trigger, separators=(",", ":")) if trigger else None,
                json.dumps(policy, separators=(",", ":")) if policy else None,
            ),
        )

    # ------------------------------------------------------------------
    # Dispatcher
    # ------------------------------------------------------------------

    def _dispatch_loop(self) -> None:
        while not self.dispatcher_stop.wait(self.poll_interval):
            try:
                # Dispatcher transactions share the store lock with HTTP
                # handlers so BEGIN/commit sequences can never interleave
                # (review rc14 P1-1).
                with self.store.db_lock:
                    self._dispatch_queued_ops()
                    self._sync_terminal_ops()
            except Exception:
                import logging

                logging.getLogger("vanth.remote").exception("remote dispatch iteration failed")

    def _dispatch_queued_ops(self) -> None:
        """Launch accepted queued jobs after commit.

        Polls operations in ``queued`` (and ``launched`` that never actually
        spawned, e.g. a crash between the ``launched`` commit and the process
        spawn). For each it launches the underlying job through the refactored
        ``JobManager.prepare_launch`` / ``_launch_prepared`` path and drives the
        operation ``queued -> launched -> running``. Because the operation, the
        queued job row, the origin mapping, and the launch intent all commit
        atomically before any spawn, a daemon restart between acceptance and
        launch resumes from the shared sqlite without losing the job.
        """
        rows = self.store.db.execute(
            """
            SELECT op_id, idempotency_key, method, payload_json, digest, status
            FROM remote_operations WHERE status IN ('accepted', 'queued', 'launched', 'running')
            """
        ).fetchall()
        for row in rows:
            try:
                op_id = row["op_id"]
                # Recoverable STOP intents (review rc14 P1-4): a crash between
                # recording an accepted stop and executing it used to strand
                # the operation forever. Reconcile it here.
                if row["method"] == "job.stop":
                    self._reconcile_stop_intent(row)
                    continue
                job_id = self.store.get_remote_job_origin_op(op_id)
                if not job_id:
                    self.store.update_operation_status(op_id, "failed")
                    continue
                job_status = self.store.db.execute(
                    "SELECT status FROM jobs WHERE job_id=?", (job_id,)
                ).fetchone()
                status_text = job_status["status"] if job_status else None
                if status_text in TERMINAL_STATUSES:
                    self._converge_terminal(op_id, job_id, status_text)
                    continue
                if status_text == "running":
                    # Spawn already confirmed (by us before a crash, or the
                    # runner published its PID); never spawn a second process.
                    if row["status"] != "running":
                        self.store.update_operation_status(op_id, "running")
                    continue
                # Job exists but has not started: recover a crash that happened
                # after the op reached launched/running but BEFORE the spawn was
                # confirmed (review P1-2). The durable states stay put until the
                # launch below actually returns.
                payload = json.loads(row["payload_json"] or "{}")
                gate = self._trigger_gate(payload)
                if gate == "wait":
                    continue
                if gate == "cancel":
                    # Cancellation is a terminal outcome the operation state
                    # machine never reaches through normal transitions, so set
                    # both rows in ONE transaction (never a raw UPDATE followed
                    # by update_operation_status's own BEGIN IMMEDIATE, which
                    # leaves a nested transaction open on this shared
                    # connection). The outbox row tells the controller.
                    self.store.db.execute("BEGIN IMMEDIATE")
                    try:
                        self.store.db.execute(
                            "UPDATE jobs SET status='cancelled', ended_at=?, updated_at=? "
                            "WHERE job_id=? AND status='queued'",
                            (now_iso(), now_iso(), job_id),
                        )
                        self.store.db.execute(
                            "UPDATE remote_operations SET status='failed', error=?, updated_at=? "
                            "WHERE op_id=?",
                            (
                                "trigger parent did not reach the requested status",
                                now_iso(),
                                op_id,
                            ),
                        )
                        cancelled_row = self.store.db.execute(
                            "SELECT job_id, name, command, status, created_at, updated_at, exit_code "
                            "FROM jobs WHERE job_id=?",
                            (job_id,),
                        ).fetchone()
                        self.feed.append_in_tx(
                            "job.upsert",
                            job_id=job_id,
                            payload=dict(cancelled_row) if cancelled_row else {
                                "job_id": job_id, "status": "cancelled",
                            },
                        )
                        self.store.db.commit()
                    except BaseException:
                        self.store.db.rollback()
                        raise
                    continue
                launch = self.manager.prepare_launch(job_id)
                if launch is None:
                    self.store.update_operation_status(op_id, "failed")
                    continue
                result = self.manager._launch_prepared(launch)
                if row["status"] == "queued":
                    self.store.update_operation_status(op_id, "launched")
                if result.get("status") in TERMINAL_STATUSES:
                    self._converge_terminal(op_id, job_id, result.get("status"))
                elif result.get("status") == "running":
                    self.store.update_operation_status(op_id, "running")
                # Any other outcome keeps the durable state at launched so the
                # next tick re-drives without double-spawning (the job row now
                # reports running and hits the branch above).
            except Exception:
                import logging

                logging.getLogger("vanth.remote").exception("remote dispatch of op %s failed", row["op_id"])

    def _reconcile_stop_intent(self, row) -> None:
        """Execute a durably-recorded stop intent that has not completed
        (review rc14 P1-4)."""
        payload = json.loads(row["payload_json"] or "{}")
        job_id = str(payload.get("job_id") or "")
        if not job_id:
            self.store.db.execute(
                "UPDATE remote_operations SET status='failed', error='stop intent missing job_id' WHERE op_id=?",
                (row["op_id"],),
            )
            self.store.db.commit()
            return
        # Permanent validation failure: unknown target can never succeed.
        job_row = self.store.db.execute(
            "SELECT status FROM jobs WHERE job_id=?", (job_id,)
        ).fetchone()
        if job_row is None:
            self.store.db.execute(
                "UPDATE remote_operations SET status='failed', error=?, updated_at=? WHERE op_id=?",
                (f"unknown job on remote: {job_id}", now_iso(), row["op_id"]),
            )
            self.store.db.commit()
            return
        if job_row["status"] in TERMINAL_STATUSES:
            status = job_row["status"]
            self.store.db.execute(
                "UPDATE remote_operations SET status='completed', result_json=?, updated_at=? WHERE op_id=?",
                (
                    json.dumps({"op_id": row["op_id"], "job_id": job_id, "status": status},
                               separators=(",", ":")),
                    now_iso(), row["op_id"],
                ),
            )
            payload_full = self.store.db.execute(
                "SELECT job_id, name, command, status, created_at, updated_at, exit_code "
                "FROM jobs WHERE job_id=?",
                (job_id,),
            ).fetchone()
            upsert_payload = dict(payload_full) if payload_full else {
                "job_id": job_id, "status": status,
            }
            self.feed.append_in_tx(
                "job.upsert", job_id=job_id,
                payload=upsert_payload,
            )
            self.store.db.commit()
            return
        try:
            stop_result = self.manager.stop_sync(job_id)
            status = stop_result.get("status") or "stopping"
        except Exception as exc:
            self.store.db.execute(
                "UPDATE remote_operations SET error=?, updated_at=? WHERE op_id=?",
                (str(exc)[:500], now_iso(), row["op_id"]),
            )
            self.store.db.commit()
            # Keep the intent non-terminal. The dispatcher will retry it on
            # the next tick; claiming completion here loses a stop after a
            # transient manager/SQLite failure.
            return
        self.store.db.execute(
            "UPDATE remote_operations SET status='completed', result_json=?, updated_at=? WHERE op_id=?",
            (
                json.dumps({"op_id": row["op_id"], "job_id": job_id, "status": status},
                           separators=(",", ":")),
                now_iso(), row["op_id"],
            ),
        )
        if status in TERMINAL_STATUSES:
            # Terminal UPSERT (not a tombstone): controller shadows must learn
            # the final status (Sol review).
            payload_row = self.store.db.execute(
                "SELECT job_id, name, command, status, created_at, updated_at, exit_code "
                "FROM jobs WHERE job_id=?",
                (job_id,),
            ).fetchone()
            upsert_payload = dict(payload_row) if payload_row else {
                "job_id": job_id, "status": status,
            }
            self.feed.append_in_tx(
                "job.upsert", job_id=job_id,
                payload=upsert_payload,
            )
        self.store.db.commit()

    def _trigger_gate(self, payload: dict[str, Any]) -> str:
        """Evaluate a queued job's trigger metadata (review P1-2).

        Returns ``"go"`` when there is no trigger or it is satisfied,
        ``"wait"`` while the parent has not reached the requested status, and
        ``"cancel"`` when the parent ended in a different terminal state.
        """
        trigger = payload.get("trigger") if isinstance(payload, dict) else None
        if trigger is None:
            return "go"
        if not isinstance(trigger, dict) or not trigger.get("job_id") or not trigger.get("status"):
            return "cancel"
        parent = str(trigger["job_id"])
        wanted = str(trigger["status"])
        # Validate the trigger contract (review rc14 P1-4): malformed ids,
        # invalid statuses, and UNKNOWN parents cancel instead of falling
        # through to an unconditional launch.
        if not re.fullmatch(r"[A-Za-z0-9_\-]{4,80}", parent):
            return "cancel"
        if wanted not in (set(TERMINAL_STATUSES) | {"running", "queued"}):
            return "cancel"
        row = self.manager.db.execute("SELECT status FROM jobs WHERE job_id=?", (parent,)).fetchone()
        if row is None:
            return "cancel"
        status = row["status"]
        if status == wanted:
            return "go"
        if status in TERMINAL_STATUSES:
            return "cancel"
        return "wait"

    def _converge_terminal(self, op_id: str, job_id: str, status: str) -> None:
        """Converge an operation to a terminal status and append the outbox
        row ATOMICALLY with the transition (review P1-6).

        The transition machine is linear (accepted→queued→launched→running→
        terminal), so reaching a terminal state may require stepping through
        intermediate states within the same transaction.
        """
        self.store.db.execute("BEGIN IMMEDIATE")
        try:
            current = self.store.db.execute(
                "SELECT status FROM remote_operations WHERE op_id=?", (op_id,)
            ).fetchone()
            current_status = current["status"] if current else None
            if current_status != status:
                sequence = [s for s in ("queued", "launched", "running") if current_status == s or (
                    current_status in ("accepted", "queued", "launched", "running")
                    and ["accepted", "queued", "launched", "running"].index(current_status)
                    < ["accepted", "queued", "launched", "running"].index(s)
                )]
                for step in (*sequence, status):
                    try:
                        self.store.update_operation_status(op_id, step, commit=False)
                    except ValueError:
                        pass
                # Final target must stick; anything else is a real error.
                row = self.store.db.execute(
                    "SELECT status FROM remote_operations WHERE op_id=?", (op_id,)
                ).fetchone()
                if not row or row["status"] != status:
                    raise ValueError(f"cannot converge operation {op_id} to {status}")
            row = self.manager.db.execute(
                "SELECT job_id, name, command, status, created_at, updated_at, exit_code "
                "FROM jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            payload = dict(row) if row else {"job_id": job_id, "status": status}
            # The outbox row is part of the same transaction as the terminal
            # operation. If it cannot be recorded, roll back and retry rather
            # than exposing a terminal state the controller can never observe.
            self.feed.append_in_tx("job.upsert", job_id=job_id, payload=payload)
            self.store.db.commit()
        except BaseException:
            self.store.db.rollback()
            raise

    def _sync_terminal_ops(self) -> None:
        """Converge ``running`` operations to the terminal status the runner
        recorded on the remote daemon's local ``jobs`` row."""
        rows = self.store.db.execute(
            "SELECT op_id FROM remote_operations WHERE status='running'"
        ).fetchall()
        for row in rows:
            job_id = self.store.get_remote_job_origin_op(row["op_id"])
            if not job_id:
                continue
            job_status = self.store.db.execute(
                "SELECT status FROM jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            if job_status and job_status["status"] in TERMINAL_STATUSES:
                try:
                    self._converge_terminal(row["op_id"], job_id, job_status["status"])
                except Exception:
                    import logging

                    logging.getLogger("vanth.remote").exception(
                        "operation terminal sync failed op=%s job=%s", row["op_id"], job_id
                    )

    def mark_terminal_from_job(self, job_id: str, status: str) -> None:
        """Called by the remote daemon when a local runner records a terminal
        status, so the accepted operation converges to the same terminal state."""
        if status not in {"completed", "failed", "cancelled", "timeout", "orphaned"}:
            return
        op_id = self.store.get_remote_operation_by_job(job_id)
        if not op_id:
            return
        try:
            self._converge_terminal(op_id, job_id, status)
        except Exception:
            import logging

            logging.getLogger("vanth.remote").exception("operation terminal sync failed op=%s job=%s", op_id, job_id)

    # ------------------------------------------------------------------
    # Unsupported (Phase 3/4) frames
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Snapshot + log-range reads (Phase 3)
    # ------------------------------------------------------------------

    SNAPSHOT_PAGE_SIZE = 50
    SNAPSHOT_EVENT_LIMIT = 500

    def _remote_state_epoch(self) -> int:
        """The remote's own state epoch, from its remote_state singleton."""
        try:
            return self.store.get_state_epoch()
        except Exception:
            return 1

    def handle_snapshot_request(self, frame: dict[str, Any], payload: dict[str, Any] | None = None) -> dict[str, Any]:
        with self.store.db_lock:
            return self._handle_snapshot_request_locked(frame, payload)

    def _handle_snapshot_request_locked(
        self, frame: dict[str, Any], payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build one paginated snapshot frame fixed to the remote state epoch.

        Pagination uses a KEYSET cursor (``after_job_id``, ordered by job_id)
        rather than mutable LIMIT/OFFSET, so concurrent inserts/deletes cannot
        shift rows between pages and skip or duplicate entries (review P0-4).
        Event cursors are PER JOB (``event_water: {job_id: seq}``) because
        event sequence numbers reset per job — one shared high-water skipped
        events on later jobs whose sequences were below an earlier job's max
        (review P1-4).
        """
        payload = payload if payload is not None else (frame.get("payload") or {})
        cursor = payload.get("cursor") or {}
        if not isinstance(cursor, dict):
            cursor = {}
        after_job_id = str(cursor.get("after_job_id") or "")
        manager = self.manager
        snapshot_id = cursor.get("snapshot_id")
        if snapshot_id is None:
            state_epoch = self._remote_state_epoch()
            # Capture jobs and their events under one SQLite read transaction.
            # Subsequent pages are slices of this immutable in-memory value,
            # never fresh queries against mutable tables.
            with getattr(manager, "db_lock", nullcontext()):
                manager.db.execute("BEGIN")
                try:
                    captured = manager.db.execute(
                        "SELECT rowid AS _rowid, job_id, name, command, status, "
                        "created_at, updated_at, exit_code FROM jobs ORDER BY job_id"
                    ).fetchall()
                    jobs = []
                    events_by_job: dict[str, list[dict[str, Any]]] = {}
                    high_water = 0
                    for captured_row in captured:
                        item = dict(captured_row)
                        high_water = max(high_water, int(item.pop("_rowid")))
                        jobs.append(item)
                        event_rows = manager.db.execute(
                            "SELECT event_id, job_id, seq, type, level, message, data_json, source, created_at "
                            "FROM events WHERE job_id=? ORDER BY seq ASC LIMIT ?",
                            (item["job_id"], self.SNAPSHOT_EVENT_LIMIT),
                        ).fetchall()
                        events_by_job[item["job_id"]] = [dict(row) for row in event_rows]
                finally:
                    manager.db.rollback()
            # Feed boundary (Sol review): MAX(remote_feed.seq) captured with
            # the snapshot. Feed events at or below it are already reflected
            # by these pages; the controller advances its feed cursor to this
            # boundary so stale events cannot regress snapshot state. Read
            # under the store lock we already hold.
            boundary_row = self.store.db.execute(
                "SELECT COALESCE(MAX(feed_seq), 0) AS seq FROM remote_feed"
            ).fetchone()
            feed_boundary_seq = int(boundary_row["seq"]) if boundary_row else 0
            state_row = self.store.db.execute(
                "SELECT state_epoch, feed_epoch FROM remote_state WHERE id=1"
            ).fetchone()
            feed_epoch = int(state_row["feed_epoch"]) if state_row and state_row["feed_epoch"] else 1
            snapshot_id = f"snap-{uuid.uuid4().hex}"
            while len(self._snapshot_cache) >= 8:
                self._snapshot_cache.pop(next(iter(self._snapshot_cache)))
            self._snapshot_cache[snapshot_id] = {
                "state_epoch": state_epoch,
                "high_water": high_water,
                "jobs": jobs,
                "events": events_by_job,
                "feed_boundary_seq": feed_boundary_seq,
                "feed_epoch": feed_epoch,
            }
        if not isinstance(snapshot_id, str) or not snapshot_id:
            return self._error_frame(frame, code="INVALID_REQUEST", message="invalid snapshot_id")
        cached = self._snapshot_cache.get(snapshot_id)
        if cached is None:
            return self._error_frame(frame, code="SNAPSHOT_EXPIRED", message="snapshot expired; restart sync")
        state_epoch = int(cached["state_epoch"])
        if self._remote_state_epoch() != state_epoch:
            self._snapshot_cache.pop(snapshot_id, None)
            return self._error_frame(frame, code="STATE_EPOCH_MISMATCH", message="snapshot timeline changed")
        high_water = int(cached["high_water"])
        remaining = [job for job in cached["jobs"] if job["job_id"] > after_job_id]
        has_more = len(remaining) > self.SNAPSHOT_PAGE_SIZE
        jobs = remaining[: self.SNAPSHOT_PAGE_SIZE]
        event_water = dict(cursor.get("event_water") or {}) if isinstance(cursor.get("event_water"), dict) else {}
        events = []
        for job in jobs:
            job_high = int(event_water.get(job["job_id"]) or 0)
            for event_row in cached["events"].get(job["job_id"], []):
                if int(event_row["seq"]) <= job_high:
                    continue
                events.append(dict(event_row))
                job_high = max(job_high, int(event_row["seq"]))
            if job_high:
                event_water[job["job_id"]] = job_high
        next_cursor = {
            "after_job_id": jobs[-1]["job_id"] if jobs else after_job_id,
            "event_water": event_water,
            "high_water": high_water,
            "snapshot_id": snapshot_id,
        }
        # Reads travel in a standard response frame's result so the controller
        # run_request path handles them uniformly; the inner kind identifies
        # the payload shape.
        response = self._response_frame(frame, {
            "kind": "snapshot",
            "state_epoch": state_epoch,
            "feed_epoch": int(cached["feed_epoch"]),
            "feed_boundary_seq": int(cached["feed_boundary_seq"]),
            "snapshot_id": snapshot_id,
            "cursor": next_cursor,
            "jobs": jobs,
            "events": events,
            "has_more": has_more,
        })
        if not has_more:
            self._snapshot_cache.pop(snapshot_id, None)
        return response

    # ------------------------------------------------------------------
    # Change feed (Phase 4)
    # ------------------------------------------------------------------

    FEED_DEFAULT_LIMIT = 100
    FEED_MAX_LIMIT = 500
    FEED_MAX_WAIT_MS = 10000
    FEED_POLL_INTERVAL = 0.025

    def _record_job_upsert(self, job_id: str, status: str | None = None) -> None:
        """Append a ``job.upsert`` outbox row with the minimal job row.

        Best-effort: feed recording must never break dispatch or acceptance.
        """
        try:
            payload: dict[str, Any] = {"job_id": job_id}
            row = self.manager.db.execute(
                "SELECT job_id, name, command, status, created_at, updated_at, exit_code "
                "FROM jobs WHERE job_id=?",
                (job_id,),
            ).fetchone()
            if row:
                payload = dict(row)
            elif status:
                payload["status"] = status
            self.feed.append("job.upsert", job_id=job_id, payload=payload)
        except Exception:
            import logging

            logging.getLogger("vanth.remote").exception("feed append failed job=%s", job_id)

    def handle_feed_request(self, frame: dict[str, Any], payload: dict[str, Any] | None = None) -> dict[str, Any]:
        """Serve one bounded batch of outbox changes after the caller's cursor.

        Reads rows strictly after ``cursor.seq`` within the CURRENT
        ``(state_epoch, feed_epoch)``; when no rows exist and ``wait_ms`` is
        requested, long-polls up to that duration before returning an empty
        batch. The result carries ``oldest_seq``/``high_water_seq`` so the
        controller can detect cursor gaps and reset its cursor to the
        high-water mark.
        """
        from .protocol import (
            FEED_DEFAULT_LIMIT,
            FEED_MAX_LIMIT,
            FEED_MAX_WAIT_MS,
        )

        payload = payload if payload is not None else (frame.get("payload") or {})
        limit = int(payload.get("limit") or FEED_DEFAULT_LIMIT)
        if limit < 1:
            return self._error_frame(frame, code="INVALID_REQUEST", message="limit must be >= 1")
        limit = min(limit, FEED_MAX_LIMIT)
        wait_ms = int(payload.get("wait_ms") or 0)
        if wait_ms < 0:
            return self._error_frame(frame, code="INVALID_REQUEST", message="wait_ms must be >= 0")
        wait_ms = min(wait_ms, FEED_MAX_WAIT_MS)
        cursor = payload.get("cursor")
        if cursor is not None and not isinstance(cursor, dict):
            return self._error_frame(frame, code="INVALID_REQUEST", message="cursor must be an object")

        deadline = time.monotonic() + (wait_ms / 1000.0)
        while True:
            batch = self.feed.read(cursor, limit=limit)
            if batch["changes"] or wait_ms <= 0 or time.monotonic() >= deadline:
                break
            time.sleep(self.FEED_POLL_INTERVAL)

        last_seq = int(cursor.get("seq") or 0) if isinstance(cursor, dict) else 0
        for change in batch["changes"]:
            last_seq = max(last_seq, change["seq"])
        next_cursor = {
            "state_epoch": batch["state_epoch"],
            "feed_epoch": batch["feed_epoch"],
            "seq": last_seq,
        }
        return self._response_frame(frame, {
            "kind": "feed",
            "state_epoch": batch["state_epoch"],
            "feed_epoch": batch["feed_epoch"],
            "cursor": next_cursor,
            "changes": batch["changes"],
            "has_more": batch["has_more"],
            "oldest_seq": batch["oldest_seq"],
            "high_water_seq": batch["high_water_seq"],
        })

    def handle_events_request(self, frame: dict[str, Any], payload: dict[str, Any] | None = None) -> dict[str, Any]:
        """Read bounded per-job event batches after independent sequence cursors."""
        from .protocol import EVENTS_DEFAULT_LIMIT, EVENTS_MAX_JOBS, EVENTS_MAX_LIMIT

        payload = payload if payload is not None else (frame.get("payload") or {})
        cursors = payload.get("cursors") or {}
        limit = min(int(payload.get("limit") or EVENTS_DEFAULT_LIMIT), EVENTS_MAX_LIMIT)
        jobs: dict[str, dict[str, Any]] = {}
        with getattr(self.manager, "db_lock", nullcontext()):
            for job_id, since in list(cursors.items())[:EVENTS_MAX_JOBS]:
                high_row = self.manager.db.execute(
                    "SELECT COALESCE(MAX(seq), 0) AS seq FROM events WHERE job_id=?", (job_id,)
                ).fetchone()
                high_water_seq = int(high_row["seq"] if high_row else 0)
                if since is None:
                    jobs[job_id] = {
                        "events": [], "next_seq": high_water_seq,
                        "high_water_seq": high_water_seq, "has_more": False,
                    }
                    continue
                rows = self.manager.db.execute(
                    "SELECT event_id, job_id, seq, type, level, message, data_json, source, created_at "
                    "FROM events WHERE job_id=? AND seq>? ORDER BY seq ASC LIMIT ?",
                    (job_id, since, limit),
                ).fetchall()
                events = [dict(row) for row in rows]
                next_seq = int(events[-1]["seq"]) if events else since
                more = self.manager.db.execute(
                    "SELECT 1 FROM events WHERE job_id=? AND seq>? LIMIT 1", (job_id, next_seq)
                ).fetchone() is not None
                jobs[job_id] = {
                    "events": events, "next_seq": next_seq,
                    "high_water_seq": high_water_seq, "has_more": more,
                }
        state_row = self.store.db.execute(
            "SELECT feed_epoch FROM remote_state WHERE id=1"
        ).fetchone()
        feed_epoch = int(state_row["feed_epoch"] if state_row and state_row["feed_epoch"] else 1)
        return self._response_frame(frame, {
            "kind": "events", "state_epoch": self._remote_state_epoch(),
            "feed_epoch": feed_epoch, "jobs": jobs,
        })

    def handle_log_range_request(self, frame: dict[str, Any], payload: dict[str, Any] | None = None) -> dict[str, Any]:
        """Read a byte range of a remote job's stdout/stderr log.

        Returns a ``log_range`` frame with base64 content; ``truncated`` is set
        when the requested window was clipped to the file size. Arbitrary bytes
        round-trip exactly through base64.
        """
        import base64
        payload = payload if payload is not None else (frame.get("payload") or {})
        remote_job_id = payload["remote_job_id"]
        # Opaque-ID grammar + containment: job ids are untrusted input and are
        # concatenated into log paths (review P1-3).
        import re as _re

        if not isinstance(remote_job_id, str) or not _re.fullmatch(r"[A-Za-z0-9_\-]{4,80}", remote_job_id):
            return self._error_frame(frame, code="INVALID_REQUEST", message="invalid remote_job_id")
        stream = payload.get("stream") or "stdout"
        if stream not in ("stdout", "stderr"):
            return self._error_frame(frame, code="INVALID_REQUEST", message="stream must be 'stdout' or 'stderr'")
        offset = int(payload.get("offset", 0))
        size = int(payload.get("size", 65536))
        if offset < 0 or size <= 0:
            return self._error_frame(frame, code="INVALID_REQUEST", message="offset must be >= 0 and size must be > 0")
        logs_root = Path(self.manager.logs)
        path = logs_root / f"{remote_job_id}.{stream}.log"
        resolved_root = os.path.realpath(str(logs_root))
        resolved_path = os.path.realpath(str(path))
        if not resolved_path.startswith(resolved_root + os.sep):
            return self._error_frame(frame, code="INVALID_REQUEST", message="log path escapes the logs directory")
        try:
            info = os.stat(str(path), follow_symlinks=False)
        except OSError:
            return self._error_frame(frame, code="INVALID_REQUEST", message=f"unknown remote job log: {remote_job_id}")
        if not _stat.S_ISREG(info.st_mode):
            return self._error_frame(frame, code="INVALID_REQUEST", message="log is not a regular file")
        file_size = int(info.st_size)
        # Open the already-validated inode, refusing a final symlink where the
        # platform exposes O_NOFOLLOW. On platforms without it, compare the
        # opened descriptor's identity to the pre-open stat and fail closed on
        # a swap; reads then remain descriptor-bound.
        try:
            flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
            fd = os.open(str(path), flags)
            opened = os.fstat(fd)
            if not _stat.S_ISREG(opened.st_mode) or (
                opened.st_dev != info.st_dev or opened.st_ino != info.st_ino
            ):
                os.close(fd)
                return self._error_frame(frame, code="INVALID_REQUEST", message="log changed during validation")
            with os.fdopen(fd, "rb") as handle:
                handle.seek(offset)
                content = handle.read(size)
        except OSError:
            return self._error_frame(frame, code="INVALID_REQUEST", message="log is not safely readable")
        truncated = (offset + len(content)) < file_size
        return self._response_frame(frame, {
            "kind": "log_range",
            "remote_job_id": remote_job_id,
            "stream": stream,
            "offset": offset,
            "size": file_size,
            "content": base64.b64encode(content).decode("ascii"),
            "truncated": truncated,
        })

    # ------------------------------------------------------------------
    # Frame builders
    # ------------------------------------------------------------------

    def _response_frame(self, request: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
        return {
            "version": "1",
            "kind": "response",
            "request_id": request.get("request_id"),
            "method": request.get("method"),
            "result": result,
            "sent_at": now_iso(),
        }

    def _error_frame(self, request: dict[str, Any], *, code: str, message: str) -> dict[str, Any]:
        from .helper import error_frame

        return error_frame(request, code=code, message=message)


def default_transport() -> Any:
    """A transport whose ``open_session`` talks to an in-process handler.

    Real deployments use the helper over loopback HTTP; this seam lets the
    daemon route and tests exercise the exact same handler.
    """
    return _InProcessTransport()


class _InProcessTransport:
    def __init__(self, handler: Any = None) -> None:
        self._handler = handler

    def open_session(self, remote_row: dict[str, Any], *, home: Any = None) -> Any:
        return _FakeSession(self._handler)
