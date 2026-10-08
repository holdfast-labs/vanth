from __future__ import annotations

import asyncio
import base64
import hmac
import http
import ipaddress
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import secrets
import signal
import socketserver
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from .client import auth_token_path, ensure_auth_token
from .paths import canonical_home, secure_home_permissions
from .remote.protocol import VanthRemoteProtocolError
from .server import TERMINAL_STATUSES, JobManager


manager: JobManager | None = None
# RLock, not Lock: the lazy accessors below nest (get_artifact_broker ->
# get_artifacts, get_artifact_collections -> get_artifacts, ...). With a plain
# Lock the first such call self-deadlocked while holding the lock forever, which
# also blocked get_artifacts() for every later request and bricked the artifact
# subsystem for the daemon's lifetime (get_manager() hides it by short-circuiting
# on the cached global before taking the lock).
manager_lock = threading.RLock()
shutdown_event = threading.Event()
_httpd: "TrackingHTTPServer | None" = None
_remote_store = None
_remote_job_mgr = None
_remote_job_mgr_lock = threading.Lock()
DEFAULT_MAX_REQUEST_BYTES = 1024 * 1024
DEFAULT_MAX_RESPONSE_BYTES = 4 * 1024 * 1024


def get_remote_store():
    """Open (once) the controller-side RemoteStore on the shared home dir.

    ThreadingHTTPServer dispatches pairing and later job requests on different
    handler threads, so the shared connection is opened with
    ``check_same_thread=False`` and every store operation is serialized by the
    store's own RLock (mirroring JobManager's db_lock pattern)."""
    global _remote_store
    with manager_lock:
        if _remote_store is None:
            import sqlite3

            db = sqlite3.connect(canonical_home() / "remote.sqlite", check_same_thread=False)
            db.row_factory = sqlite3.Row
            from .migrations import configure_connection
            from .remote.store import RemoteStore

            configure_connection(db)
            _remote_store = RemoteStore(db)
    return _remote_store


def get_remote_control():
    """Controller-side RemoteControl bound to the shared store (job routing).

    Requests are journaled to ``client-requests.sqlite`` so `vanth remote
    pending` / `vanth remote retry` reflect daemon-initiated requests and a
    lost response can be retried with the ORIGINAL key (review P1-6)."""
    from .remote.control import RemoteControl

    return RemoteControl(get_remote_store(), journal=get_request_journal())


_request_journal = None
_request_journal_lock = threading.Lock()


def get_request_journal():
    global _request_journal
    with _request_journal_lock:
        if _request_journal is None:
            from pathlib import Path as _Path

            from .remote.journal import RequestJournal

            _request_journal = RequestJournal(_Path(canonical_home()) / "client-requests.sqlite")
    return _request_journal


_artifacts_ops = None


def get_artifacts():
    """Open (once) the artifact catalog, blob store, and operations engine."""
    global _artifacts_ops
    with manager_lock:
        if _artifacts_ops is None:
            from .artifacts.catalog import open_catalog
            from .artifacts.local_store import LocalBlobStore, default_store_root
            from .artifacts.operations import ArtifactOperations

            home = canonical_home()
            catalog = open_catalog(home)
            blobs = LocalBlobStore(default_store_root(home), catalog)
            _artifacts_ops = ArtifactOperations(catalog, blobs)
    return _artifacts_ops


_artifact_collections = None


def get_artifact_collections():
    """Open (once) the Phase 7 collections/aliases/lineage engine."""
    global _artifact_collections
    with manager_lock:
        if _artifact_collections is None:
            from .artifacts.collections import Collections

            _artifact_collections = Collections(get_artifacts().catalog, get_artifacts())
    return _artifact_collections


_artifact_lifecycle = None


def get_artifact_lifecycle():
    """Open (once) the Phase 7 lifecycle engine (delete/pin/GC/backup-restore)."""
    global _artifact_lifecycle
    with manager_lock:
        if _artifact_lifecycle is None:
            from .artifacts.lifecycle import Lifecycle

            ops = get_artifacts()
            _artifact_lifecycle = Lifecycle(ops.catalog, ops)
    return _artifact_lifecycle


_artifact_storage_profiles = None


def get_artifact_storage_profiles():
    """Open (once) the Phase 8 storage-profile registry."""
    global _artifact_storage_profiles
    with manager_lock:
        if _artifact_storage_profiles is None:
            from .artifacts.s3 import StorageProfiles

            _artifact_storage_profiles = StorageProfiles(get_artifacts().catalog)
    return _artifact_storage_profiles


_artifact_broker = None


def get_artifact_broker():
    """Open (once) the Phase 9 remote artifact transfer broker.

    Combines the controller-side RemoteControl (bulk session seam) with the
    local ArtifactOperations (source versions + resume ledger). The transfer
    path never touches storage-profile credentials: only content identifiers
    and base64 bytes cross the wire.
    """
    global _artifact_broker
    with manager_lock:
        if _artifact_broker is None:
            from .remote.transfer import RemoteArtifactBroker

            _artifact_broker = RemoteArtifactBroker(get_remote_control(), get_artifacts())
    return _artifact_broker


def _remote_epoch(remote_id: str):
    """Best-effort expected state epoch for a remote (None when not yet seen)."""
    from .remote.ssh import VanthRemoteError

    try:
        return get_remote_store().get_remote(remote_id).get("state_epoch")
    except (ValueError, VanthRemoteError):
        return None


def _remote_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Translate a local job payload to a remote protocol payload.

    ``idempotency_key`` and ``remote_id`` are preserved for the caller:
    dropping the key forced a random mint per HTTP attempt, so a lost
    response + retry created a second remote operation (review rc14 P1-3).
    """
    return {
        key: value
        for key, value in payload.items()
        if key in {
            "command", "cwd", "name", "env", "timeout_seconds", "notify_on",
            "wake_targets", "origin_thread_id", "tags", "notes", "interactive",
            "trigger", "policy", "signal", "kill_after_seconds", "overrides",
            "idempotency_key",
        }
    }


def _remote_submit(remote_id: str, method: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Route a remote mutation through RemoteControl with a caller-supplied key."""
    # Validate the caller's fields BEFORE anything that initializes lockdowning
    # state (get_remote_control takes manager_lock): a malformed body must yield a
    # field error, not a wait on initialization or an init failure masquerading
    # as the caller's fault.
    key = payload.pop("idempotency_key", None)
    if key is None:
        raise VanthRemoteProtocolError(
            "INVALID_REQUEST", "remote mutations require caller-supplied idempotency_key"
        )
    job_id = required_field(payload, "job_id") if method in {"job.stop", "job.rerun"} else None
    control = get_remote_control()
    expected = _remote_epoch(remote_id)
    remote_row = get_remote_store().get_remote(remote_id)
    expected_instance = remote_row.get("instance_id")
    if not expected_instance:
        raise VanthRemoteProtocolError(
            "INVALID_REQUEST", "remote is not paired with a verified instance identity"
        )
    if method == "job.start":
        wake_targets = payload.pop("wake_targets", None)
        notify_on = payload.pop("notify_on", None)
        if notify_on and wake_targets:
            wake_targets = [
                {**target, "events": notify_on}
                if "events" not in target and "notify_on" not in target else target
                for target in wake_targets
            ]
        request = control.submit(remote_id, method, payload, idempotency_key=key, expected_state_epoch=expected, expected_instance_id=expected_instance)
        # A request that is still creating/submitting/accepted must be DRIVEN so
        # its response (and the remote job id) is observed. Only a terminal
        # request short-circuits with a stored response. Treating
        # submitting/accepted as "done" (as an earlier version did) silently
        # skipped wake registration after a lost response or a crash mid-request.
        if request["status"] in {"creating", "submitting", "accepted"}:
            result = control.run_request(remote_id, request, expected_state_epoch=expected)
        else:
            result = request
        warnings = list(result.get("warnings") or [])
        if notify_on and not wake_targets:
            warnings.append(
                "notify_on has no effect without wake_targets: it only sets the default events of a wake target. "
                "Pass wake_me=True (or a wake_targets entry) to actually be woken."
            )
        response = result.get("response") or {}
        remote_job_id = response.get("job_id") if isinstance(response, dict) else None
        if wake_targets and remote_job_id:
            non_terminal = [
                str(target.get("type"))
                for target in wake_targets
                if not (set(target.get("events") or target.get("notify_on") or []) & TERMINAL_STATUSES)
            ]
            if non_terminal:
                warnings.append(
                    "non-terminal remote wakes are best-effort and will never fire if the matching event "
                    "has expired from the remote retention window: " + ", ".join(non_terminal)
                )
            try:
                get_manager().register_remote_wake_targets(remote_id, str(remote_job_id), wake_targets)
            except Exception as exc:
                logging.getLogger("vanth.daemon").warning(
                    "remote wake targets were not registered remote=%s job=%s: %s",
                    remote_id, remote_job_id, exc,
                )
                warnings.append(f"wake targets were NOT registered locally: {exc}")
        if warnings:
            result = {**result, "warnings": warnings}
        return result
    if method == "job.stop":
        return control.stop(
            remote_id, job_id,
            signal=payload.get("signal", "terminate"),
            kill_after_seconds=payload.get("kill_after_seconds", 10),
            idempotency_key=key, expected_state_epoch=expected, expected_instance_id=expected_instance,
        )
    if method == "job.rerun":
        overrides = {k: v for k, v in payload.items() if k != "job_id"}
        return control.rerun(remote_id, job_id, overrides, idempotency_key=key, expected_state_epoch=expected, expected_instance_id=expected_instance)
    return control.submit(remote_id, method, payload, idempotency_key=key, expected_state_epoch=expected, expected_instance_id=expected_instance)


def _remote_wake_sync_once(manager: JobManager, control: Any, store: Any) -> None:
    """Sync remote event feeds, then emit newly observed terminal wake events."""
    bindings = manager.remote_wake_bindings()
    if not bindings:
        return
    remote_ids = {binding["remote_id"] for binding in bindings}
    # Only a binding that can fire on a NON-terminal event needs the event drain.
    # A terminal-only binding (the common `wake_me=True`) is never gated by it, so
    # a failing or unsupported event read cannot starve its terminal wake.
    event_jobs = {
        (binding["remote_id"], binding["remote_job_id"])
        for binding in bindings
        if any(event not in TERMINAL_STATUSES for event in (binding.get("events") or []))
    }
    # Jobs whose event backlog did not fully drain this tick. Their terminal wake
    # MUST wait: the terminal deletes the binding, so an undrained checkpoint
    # would be lost and could otherwise arrive after the terminal wake. The check
    # FAILS SAFE — a job whose read failed or that the host omitted is undrained.
    undrained: set[tuple[str, str]] = set()
    for remote_id in remote_ids:
        try:
            control.feed_sync(remote_id)
        except Exception:
            logging.getLogger("vanth.daemon").warning(
                "remote wake sync failed remote=%s", remote_id, exc_info=True
            )
        jobs_needing_events = sorted({job_id for (rid, job_id) in event_jobs if rid == remote_id})
        if not jobs_needing_events:
            continue
        cursors: dict[str, int | None] = {}
        for job_id in jobs_needing_events:
            try:
                cursors[job_id] = manager.remote_event_cursor(remote_id, job_id)
            except Exception:
                logging.getLogger("vanth.daemon").warning(
                    "remote wake cursor read failed remote=%s job=%s", remote_id, job_id, exc_info=True
                )
        if not cursors:
            undrained.update((remote_id, job_id) for job_id in jobs_needing_events)
            continue
        jobs: dict[str, Any] = {}
        unsupported = False
        for _ in range(5):
            try:
                info = control.events(remote_id, cursors)
            except VanthRemoteProtocolError as exc:
                # An older host without `job.events` cannot serve these events at
                # all; degrade to terminal-only rather than blocking the terminal
                # wake forever.
                if exc.code == "UNSUPPORTED_FEATURE":
                    unsupported = True
                else:
                    logging.getLogger("vanth.daemon").warning(
                        "remote wake event sync failed remote=%s: %s", remote_id, exc
                    )
                break
            except Exception:
                logging.getLogger("vanth.daemon").warning(
                    "remote wake event sync failed remote=%s", remote_id, exc_info=True
                )
                break
            jobs = info.get("jobs") or {}
            for job_id, job_info in jobs.items():
                try:
                    if cursors.get(job_id) is None:
                        manager.set_remote_event_cursor(remote_id, job_id, job_info["next_seq"])
                        cursors[job_id] = job_info["next_seq"]
                    else:
                        for event in job_info.get("events") or []:
                            manager.emit_remote_event(remote_id, job_id, event, event["seq"])
                            cursors[job_id] = event["seq"]
                except Exception:
                    logging.getLogger("vanth.daemon").warning(
                        "remote wake event handling failed remote=%s job=%s", remote_id, job_id, exc_info=True
                    )
            if not any(job_info.get("has_more") for job_info in jobs.values()):
                break
        if unsupported:
            continue
        for job_id in jobs_needing_events:
            info = jobs.get(job_id)
            if info is None or info.get("has_more"):
                undrained.add((remote_id, job_id))
    for binding in bindings:
        if (binding["remote_id"], binding["remote_job_id"]) in undrained:
            continue
        try:
            shadow = store.get_shadow(binding["remote_id"], binding["remote_job_id"])
        except ValueError:
            # The shadow is absent or suppressed (the job was deleted/forgotten on
            # the host), so the wake can never fire. Settle the binding instead of
            # logging a traceback every tick forever.
            manager.drop_remote_wake_binding(binding["remote_id"], binding["remote_job_id"])
            continue
        except Exception:
            logging.getLogger("vanth.daemon").warning(
                "remote wake inspection failed remote=%s job=%s",
                binding.get("remote_id"), binding.get("remote_job_id"), exc_info=True,
            )
            continue
        try:
            if shadow.get("status") in TERMINAL_STATUSES:
                manager.emit_remote_terminal(
                    binding["remote_id"], binding["remote_job_id"], shadow["status"],
                    exit_code=(shadow.get("payload") or {}).get("exit_code"),
                )
        except Exception:
            logging.getLogger("vanth.daemon").warning(
                "remote wake terminal emit failed remote=%s job=%s",
                binding.get("remote_id"), binding.get("remote_job_id"), exc_info=True,
            )


def _prune_remote_request_rows(ttl_seconds: int) -> dict[str, Any]:
    """Prune settled controller request/journal rows (poll-loop bookkeeping)."""
    counts = get_remote_store().prune_requests(ttl_seconds)
    journal = get_request_journal()
    if journal is not None:
        counts["journal"] = journal.prune_resolved(ttl_seconds)
    return counts


def _remote_wake_sync_loop() -> None:
    def _number(name: str, default: float) -> float:
        try:
            value = float(os.environ.get(name, str(default)))
        except ValueError:
            return default
        # inf/nan would raise in int() below and kill the sync thread.
        if value != value or value in (float("inf"), float("-inf")):
            return default
        return value

    interval = max(1.0, _number("VANTH_REMOTE_WAKE_SYNC_SECONDS", 5))
    ttl = int(_number("VANTH_REMOTE_REQUEST_TTL_SECONDS", 604800))
    prune_interval = max(60.0, _number("VANTH_REMOTE_PRUNE_INTERVAL_SECONDS", 3600))
    next_prune = 0.0
    while not shutdown_event.is_set():
        if shutdown_event.wait(interval):
            break
        try:
            _remote_wake_sync_once(get_manager(), get_remote_control(), get_remote_store())
        except Exception:
            logging.getLogger("vanth.daemon").exception("remote wake sync iteration failed")
        if ttl > 0 and time.monotonic() >= next_prune:
            next_prune = time.monotonic() + prune_interval
            try:
                counts = _prune_remote_request_rows(ttl)
                if any(counts.values()):
                    logging.getLogger("vanth.daemon").info("pruned remote request rows: %s", counts)
            except Exception:
                logging.getLogger("vanth.daemon").exception("remote request prune failed")


def _remote_wait(remote_id: str, remote_job_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Wait on a remote job by polling RemoteControl.status every 0.2s.

    A real cross-machine event push is Phase 4; until then wait is a bounded
    poll of the remote's ``job.status`` method. Returns when the remote reports
    a terminal status or the timeout elapses.
    """
    timeout_seconds = float(payload.get("timeout_seconds", 3600))
    filters = payload.get("filters") or ["completed", "failed", "timeout", "cancelled", "orphaned"]
    deadline = time.monotonic() + timeout_seconds
    control = get_remote_control()
    while True:
        if shutdown_event.is_set():
            # Daemon is going away (restart/shutdown): answer now so this
            # non-daemon handler thread does not pin the old process (and its
            # database connection) until the full wait deadline. The caller
            # retries against the new daemon like any other wait shutdown.
            return {"result": "shutdown", "job_id": remote_job_id, "message": "Vanth is shutting down"}
        try:
            result = control.status(
                remote_id, remote_job_id,
                idempotency_key="wait-" + secrets.token_hex(16)[:12],
                expected_state_epoch=_remote_epoch(remote_id),
                expected_instance_id=get_remote_store().get_remote(remote_id).get("instance_id"),
            )
        except Exception as exc:
            text = str(exc)
            return {
                "result": "error", "job_id": remote_job_id,
                "error": {"code": getattr(exc, "code", "REMOTE_WAIT_ERROR"), "message": text},
            }
        if isinstance(result, dict) and (result.get("status") in {"failed", "lost"} or result.get("error")):
            detail = result.get("error") or "remote status request failed"
            return {
                "result": "error", "job_id": remote_job_id,
                "error": {"code": "REMOTE_STATUS_ERROR", "message": str(detail)},
            }
        status = None
        if isinstance(result, dict):
            response = result.get("response") or {}
            if isinstance(response, dict):
                status = response.get("status")
        if status and status in TERMINAL_STATUSES:
            return {"result": "status", "job_id": remote_job_id, "status": status, "response": result}
        if time.monotonic() >= deadline:
            return {"result": "timeout", "job_id": remote_job_id, "status": status, "message": "No terminal status before timeout"}
        time.sleep(min(0.2, deadline - time.monotonic()))


def _remote_job_manager():
    """Remote daemon-side RemoteJobManager singleton (POST /remote/helper).

    On a real remote host the daemon's ``jobs.sqlite`` is its local job store;
    the remote operation tables are created alongside it on the same connection
    so the acceptance transaction (operation + queued job + origin mapping +
    launch intent) commits atomically with the job row the dispatcher launches.
    """
    global _remote_job_mgr
    with _remote_job_mgr_lock:
        if _remote_job_mgr is None:
            import sqlite3
            from pathlib import Path

            from .migrations import configure_connection
            from .remote.remote import RemoteJobManager
            from .remote.store import RemoteOperationStore

            home = canonical_home()
            db = sqlite3.connect(home / "jobs.sqlite", check_same_thread=False)
            db.row_factory = sqlite3.Row
            configure_connection(db)
            _remote_job_mgr = RemoteJobManager(RemoteOperationStore(db), get_manager(), home=home)
            _remote_job_mgr.start()
    return _remote_job_mgr


class RequestTooLarge(ValueError):
    pass


class TrackingHTTPServer(ThreadingHTTPServer):
    daemon_threads = False
    block_on_close = False
    # Windows SO_REUSEADDR lets a second process bind the same loopback port
    # silently, producing a phantom listener that never receives traffic and
    # never exits cleanly. Disable it so a second daemon's bind fails loudly
    # instead (the OS home lock is the real guard anyway).
    allow_reuse_address = os.name != "nt"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._active_condition = threading.Condition()
        self._active_requests = 0

    def server_bind(self) -> None:
        # Skip HTTPServer.server_bind's ``socket.getfqdn(host)``: that reverse
        # DNS lookup can block for seconds (or hang) on locked-down networks,
        # delaying daemon startup past every client timeout. ``server_name`` is
        # only cosmetic metadata we do not depend on.
        socketserver.TCPServer.server_bind(self)
        host, port = self.server_address[:2]
        self.server_name = host
        self.server_port = port

    def _request_finished(self) -> None:
        with self._active_condition:
            self._active_requests -= 1
            self._active_condition.notify_all()

    def process_request(self, request: Any, client_address: Any) -> None:
        with self._active_condition:
            self._active_requests += 1
        thread = threading.Thread(target=self.process_request_thread, args=(request, client_address))
        thread.daemon = self.daemon_threads
        try:
            thread.start()
        except BaseException:
            self._request_finished()
            self.close_request(request)
            raise

    def process_request_thread(self, request: Any, client_address: Any) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._request_finished()

    def wait_for_requests(self, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        with self._active_condition:
            while self._active_requests:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._active_condition.wait(remaining)
        return True


class DaemonLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.handle = None

    def acquire(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+", encoding="utf-8")
        try:
            if os.name == "nt":
                import msvcrt

                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.handle.seek(0)
            self.handle.truncate()
            self.handle.write(str(os.getpid()))
            self.handle.flush()
            return True
        except (BlockingIOError, OSError):
            self.handle.close()
            self.handle = None
            return False

    def release(self) -> None:
        if not self.handle:
            return
        try:
            if os.name == "nt":
                import msvcrt

                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        finally:
            self.handle.close()
            self.handle = None


def get_manager() -> JobManager:
    global manager
    if manager is None:
        with manager_lock:
            if manager is None:
                manager = JobManager()
    return manager


def _set_httpd(server: "TrackingHTTPServer") -> None:
    global _httpd
    _httpd = server


def _stop_httpd(*args: Any) -> None:
    """Gracefully stop the HTTP server from a background thread.

    Used by the signal handler (which passes ``(signum, frame)`` on Unix) and
    the authenticated ``/shutdown`` route (no args). The server thread unwinds
    through ``main``'s finally block, which closes the manager, releases the
    daemon lock, and removes discovery metadata.
    """
    if shutdown_event.is_set():
        return
    shutdown_event.set()
    if manager is not None:
        manager.begin_shutdown()
    global _remote_job_mgr
    if _remote_job_mgr is not None:
        _remote_job_mgr.stop()
    server = _httpd
    if server is not None:
        threading.Thread(target=server.shutdown, daemon=True).start()


def _max_response_bytes() -> int:
    try:
        return max(1024, int(os.environ.get("VANTH_MAX_RESPONSE_BYTES", DEFAULT_MAX_RESPONSE_BYTES)))
    except ValueError:
        return DEFAULT_MAX_RESPONSE_BYTES


def text(handler: BaseHTTPRequestHandler, body: str, status: int = 200, content_type: str = "text/plain; version=0.0.4; charset=utf-8") -> None:
    encoded = body.encode("utf-8")
    try:
        handler.send_response(status)
        handler.send_header("Content-Type", content_type)
        handler.send_header("Content-Length", str(len(encoded)))
        handler.end_headers()
        handler.wfile.write(encoded)
    except (BrokenPipeError, ConnectionError, OSError):
        pass


def ok(handler: BaseHTTPRequestHandler, payload: dict[str, Any], status: int = 200) -> None:
    body = json.dumps(payload).encode()
    if len(body) > _max_response_bytes():
        payload = {"result": "error", "error": "Response exceeds configured size limit"}
        body = json.dumps(payload).encode()
        status = 500
    try:
        handler.send_response(status)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.write(body)
    except (BrokenPipeError, ConnectionError):
        pass


def error(handler: BaseHTTPRequestHandler, message: str, status: int = 400) -> None:
    ok(handler, {"result": "error", "error": message[:4096]}, status)


def required_field(payload: dict[str, Any], key: str) -> Any:
    """Read a required request field, raising a 400-class error when absent.

    Indexing ``payload[key]`` directly raises ``KeyError``, which the POST
    handler maps to a 500 "Internal server error": a client that forgot a field
    would see a server fault instead of the field name. ``ValueError`` maps to a
    field-level 400 (the same contract ``JobManager`` uses).
    """
    value = payload.get(key)
    if value is None or (isinstance(value, str) and not value.strip()):
        raise ValueError(f"{key} is required")
    return value


# Payload surface accepted by POST /jobs. An explicit allow-list means a
# misspelled or unknown field is a clear, field-level 400 at the HTTP boundary
# instead of being forwarded into ``JobManager.start(**payload)`` and surfacing
# as an opaque Python ``TypeError``.
_JOB_START_FIELDS = {
    "command", "cwd", "name", "env", "timeout_seconds", "notify_on",
    "wake_targets", "origin_thread_id", "tags", "notes", "interactive",
    "trigger", "policy", "secret_env", "pool", "priority",
    "remote_id", "idempotency_key", "wake_default",
}


def _validate_job_start_payload(payload: dict[str, Any]) -> None:
    """Reject unknown/missing fields with a field-level message before the
    request reaches the job manager (which validates values, not the shape)."""
    unknown = sorted(set(payload) - _JOB_START_FIELDS)
    if unknown:
        raise ValueError(
            "unknown field(s): " + ", ".join(unknown)
            + " (allowed: " + ", ".join(sorted(_JOB_START_FIELDS)) + ")"
        )
    command = payload.get("command")
    if not isinstance(command, str) or not command.strip():
        raise ValueError("missing required field: command (non-empty string)")


def _decision_route(path: str) -> tuple[str, str, str] | None:
    """Match the exact decision routes, or None.

    Exact segment matching (rather than a loose endswith suffix) so malformed
    paths like ``/jobs/x/resolve`` or ``/jobs/x/not-a-decision/t/resolve`` are
    a clean 404 instead of an IndexError 500 or an unintended resolve.
    """
    parts = path.split("/")
    if len(parts) == 4 and parts[1] == "jobs" and parts[3] == "decision" and parts[2]:
        return ("request", parts[2], "")
    if len(parts) == 6 and parts[1] == "jobs" and parts[3] == "decision" and parts[2] and parts[4]:
        if parts[5] == "resolve":
            return ("resolve", parts[2], parts[4])
        if parts[5] == "withdraw":
            return ("withdraw", parts[2], parts[4])
    return None


class Handler(BaseHTTPRequestHandler):
    server_version = "vanthd/1"

    # Bound every blocking socket read/write so a partial request body or a
    # stalled client cannot pin a request thread indefinitely. This is a
    # per-IO timeout, not a whole-request deadline: the long-poll routes
    # (/jobs/:id/wait, /tail?follow=true) perform no socket I/O while waiting,
    # so they are unaffected and keep their own timeouts.
    try:
        timeout = float(os.environ.get("VANTH_REQUEST_TIMEOUT", "30"))
    except ValueError:
        timeout = 30.0

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _authorized(self) -> bool:
        # Keep the cheap liveness probe usable by process supervisors.
        if urllib.parse.urlparse(self.path).path == "/health":
            return True
        supplied = self.headers.get("Authorization", "")
        actual = supplied[7:] if supplied.startswith("Bearer ") else ""
        try:
            expected = Path(auth_token_path()).read_text(encoding="utf-8").strip()
        except OSError:
            expected = ""
        return bool(expected) and hmac.compare_digest(actual, expected)

    def do_GET(self) -> None:
        if shutdown_event.is_set():
            error(self, "Daemon is shutting down", 503)
            return
        parsed = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed.query)
        if not self._authorized():
            if parsed.path in {"/jobs", "/view", "/deliveries"} and "limit" in query:
                try:
                    value = int(query["limit"][0])
                    if value < 1 or value > 1000:
                        raise ValueError("limit must be an integer between 1 and 1000")
                except (ValueError, TypeError, OverflowError):
                    error(self, "limit must be an integer between 1 and 1000", 400)
                    return
            error(self, "Unauthorized", 401)
            return
        try:
            if parsed.path == "/health":
                ok(self, {"ok": True})
            elif parsed.path == "/remote/identity":
                # Loopback-only identity probe for the remote helper sentinel
                # (review rc14 P0-1.4): proves daemon reachability and returns
                # the live state_epoch for hello binding.
                remote_store = _remote_job_manager().store
                ok(self, {"state_epoch": remote_store.get_state_epoch(), "instance_id": remote_store.get_instance_id()})
            elif parsed.path == "/ready":
                report = get_manager().doctor()
                ok(self, report, 200 if report["ok"] else 503)
            elif parsed.path == "/ready-fast":
                ok(self, get_manager().ready_fast())
            elif parsed.path == "/doctor":
                ok(self, get_manager().doctor(
                    query.get("verify_artifacts", ["false"])[0].lower() in {"1", "true", "yes"}
                ))
            elif parsed.path == "/metrics":
                text(self, get_manager().metrics_text())
            elif parsed.path == "/remotes":
                from .remote.pairing import list_remotes

                ok(self, {"remotes": list_remotes(store=get_remote_store())})
            elif parsed.path == "/remotes/doctor":
                from .remote.pairing import doctor_remote

                ok(self, doctor_remote(remote_id=query.get("remote_id", [None])[0], store=get_remote_store()))
            elif parsed.path.startswith("/remotes/") and parsed.path.endswith("/jobs"):
                from .remote.readapi import projected_jobs

                ok(self, projected_jobs(get_manager(), get_remote_store(), parsed.path.split("/")[2],
                                        limit=int(query.get("limit", ["50"])[0])))
            elif parsed.path.startswith("/remotes/") and "/status/" in parsed.path:
                from .remote.readapi import projected_status

                parts = parsed.path.split("/")
                ok(self, projected_status(get_manager(), get_remote_store(), parts[2], parts[4]))
            elif parsed.path.startswith("/remotes/") and parsed.path.endswith("/dashboard"):
                from .remote.readapi import projected_dashboard

                ok(self, projected_dashboard(get_manager(), get_remote_store(), parsed.path.split("/")[2],
                                             limit=int(query.get("limit", ["5000"])[0])))
            elif parsed.path.startswith("/remotes/") and parsed.path.endswith("/tail"):
                # GET /remotes/{remote_id}/jobs/{job_id}/tail — a byte range of a
                # log living ON THE REMOTE, via the job.log_range protocol. The
                # controller-side log_range existed but nothing called it, so
                # remote logs were unreadable through every surface (CLI, MCP,
                # HTTP) even though the helper implemented them.
                parts = parsed.path.split("/")
                if len(parts) != 6 or parts[3] != "jobs":
                    error(self, "Not found", 404)
                    return
                remote_id, remote_job_id = parts[2], parts[4]
                # A remote read is one byte range. Reject follow/grep here too (the
                # MCP wrapper rejects them) so a direct HTTP caller cannot get a
                # silent, unfiltered snapshot while believing it is following.
                # parse_qs drops blank values, so re-parse with them kept: `?grep=`
                # must be rejected too, not treated as absent.
                supplied = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
                if supplied.get("follow", ["false"])[0].lower() in {"1", "true", "yes"}:
                    raise ValueError("follow is not supported for a remote job log; poll this route instead")
                if "grep" in supplied:
                    raise ValueError("grep is not supported for a remote job log; filter the returned content")
                stream = query.get("stream", ["stdout"])[0]
                if stream not in {"stdout", "stderr"}:
                    raise ValueError("stream must be stdout or stderr")
                size = int(query.get("size", ["65536"])[0])
                if size < 1 or size > 1048576:
                    raise ValueError("size must be an integer between 1 and 1048576")
                frame = get_remote_control().log_range(
                    remote_id, remote_job_id,
                    stream=stream,
                    offset=int(query.get("offset", ["0"])[0]),
                    size=size,
                )
                try:
                    content = base64.b64decode(frame.get("content") or "").decode("utf-8", "replace")
                except (ValueError, TypeError):
                    content = ""
                ok(self, {
                    "job_id": remote_job_id,
                    "remote_id": remote_id,
                    "shadow": False,
                    "stream": frame.get("stream", stream),
                    "offset": frame.get("offset", 0),
                    "size": frame.get("size"),
                    "truncated": frame.get("truncated", False),
                    "content": content,
                })
            elif parsed.path == "/view":
                ok(self, get_manager().agent_view(query.get("thread_id", [None])[0], int(query.get("limit", ["50"])[0])))
            elif parsed.path == "/jobs":
                ok(self, get_manager().list(query.get("status") or None, int(query.get("limit", ["50"])[0]),
                                            query.get("thread_id", [None])[0],
                                            query.get("name", [None])[0],
                                            query.get("tags") or None))
            elif parsed.path == "/status/batch":
                ids = query.get("job_ids", [""])[0]
                job_ids = [jid for jid in ids.split(",") if jid] if ids else []
                ok(self, get_manager().status_batch(job_ids, int(query.get("limit", ["500"])[0])))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/status"):
                remote_id = query.get("remote_id", [None])[0]
                if remote_id:
                    from .remote.ssh import VanthRemoteError

                    try:
                        ok(self, get_remote_control().status(
                            remote_id, parsed.path.split("/")[2],
                            idempotency_key="st-" + secrets.token_hex(16)[:12],
                            expected_state_epoch=_remote_epoch(remote_id),
                        ))
                    except VanthRemoteError as exc:
                        error(self, str(exc), 409)
                else:
                    ok(self, get_manager().status(parsed.path.split("/")[2]))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/events"):
                ok(self, get_manager().events(parsed.path.split("/")[2], query.get("since_event_id", [None])[0],
                                              query.get("types") or None, int(query.get("limit", ["20"])[0]),
                                              query.get("reverse", ["false"])[0].lower() in {"1", "true", "yes"}))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/tail"):
                ok(self, get_manager().tail(parsed.path.split("/")[2], query.get("stream", ["stdout"])[0], int(query.get("max_bytes", ["8192"])[0]), int(query["offset"][0]) if "offset" in query else None, query.get("follow", ["false"])[0] == "true", float(query.get("timeout_seconds", ["5"])[0]), query.get("grep", [None])[0]))
            elif parsed.path == "/deliveries":
                ok(self, get_manager().deliveries(query.get("job_id", [None])[0], query.get("status", [None])[0], int(query.get("limit", ["20"])[0])))
            elif parsed.path == "/decisions":
                ok(self, get_manager().list_decisions(
                    query.get("job_id", [None])[0],
                    query.get("status", [None])[0],
                    int(query.get("limit", ["50"])[0]),
                ))
            elif parsed.path.startswith("/deliveries/") and parsed.path.endswith("/attempts"):
                ok(self, get_manager().delivery_attempts(parsed.path.split("/")[2], int(query.get("limit", ["20"])[0])))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/metrics"):
                ok(self, get_manager().metrics_query(
                    parsed.path.split("/")[2],
                    query.get("metric", [None])[0],
                    int(query["from_ms"][0]) if "from_ms" in query else None,
                    int(query["to_ms"][0]) if "to_ms" in query else None,
                    int(query.get("limit", ["1000"])[0]),
                ))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/summary"):
                ok(self, get_manager().run_summary(
                    parsed.path.split("/")[2],
                    query.get("include_stderr_excerpt", ["false"])[0].lower() in {"1", "true", "yes"},
                    query.get("include_stdout_excerpt", ["false"])[0].lower() in {"1", "true", "yes"},
                ))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/diff"):
                ok(self, get_manager().diff_spec(
                    parsed.path.split("/")[2],
                    query.get("other", [None])[0],
                ))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/artifacts"):
                ok(self, get_manager().artifacts(parsed.path.split("/")[2], int(query.get("limit", ["50"])[0])))
            elif parsed.path.startswith("/artifacts/") and parsed.path.endswith("/content"):
                ok(self, get_manager().artifact_read(parsed.path.split("/")[2], int(query.get("max_bytes", ["262144"])[0])))
            elif parsed.path == "/artifacts/resolve":
                ok(self, get_artifacts().resolve(
                    query.get("name", [""])[0],
                    alias=query.get("alias", [None])[0],
                    version_id=query.get("version_id", [None])[0],
                    idempotency_key=query.get("idempotency_key", [None])[0],
                ))
            elif parsed.path.startswith("/artifacts/info/"):
                ok(self, get_artifacts().info(parsed.path.split("/")[2]))
            elif parsed.path.startswith("/artifacts/collections/"):
                ok(self, get_artifact_collections().get_collection(parsed.path.split("/")[2]))
            elif parsed.path.startswith("/artifacts/lineage/"):
                ok(self, {"version_id": parsed.path.split("/")[2], "lineage": get_artifact_collections().lineage_for(parsed.path.split("/")[2])})
            elif parsed.path.startswith("/artifacts/storage-profiles/"):
                ok(self, get_artifact_storage_profiles().get(parsed.path.split("/")[2]))
            elif parsed.path == "/cleanup/preview":
                ok(self, get_manager().cleanup_preview(int(query.get("older_than_seconds", ["0"])[0])))
            elif parsed.path == "/metrics/compare":
                ok(self, get_manager().metric_compare(
                    query.get("job_ids") or [],
                    query.get("metric", [None])[0],
                    query.get("aggregation", ["latest"])[0],
                    int(query["from_ms"][0]) if "from_ms" in query else None,
                    int(query["to_ms"][0]) if "to_ms" in query else None,
                ))
            elif parsed.path == "/dashboard":
                ok(self, get_manager().dashboard(
                    query.get("job_ids") or None,
                    int(query.get("limit", ["5000"])[0]),
                ))
            elif parsed.path == "/analytics/durations":
                ok(self, get_manager().duration_stats(
                    name=query.get("name", [None])[0],
                    tags=query.get("tags") or None,
                    limit=int(query.get("limit", ["20"])[0]),
                    runs_per_group=int(query.get("runs_per_group", ["200"])[0]),
                    since_ms=int(query["since_ms"][0]) if "since_ms" in query else None,
                    slowest=int(query.get("slowest", ["10"])[0]),
                ))
            elif parsed.path == "/schedules":
                ok(self, get_manager().list_schedules())
            elif parsed.path.startswith("/schedules/") and parsed.path.endswith("/next"):
                ok(self, get_manager().schedule_next_fires(
                    parsed.path.split("/")[2],
                    int(query.get("count", ["5"])[0]),
                ))
            elif parsed.path == "/pools":
                ok(self, get_manager().pool_list())
            elif parsed.path == "/relay/poll":
                ok(self, {"deliveries": get_manager().relay_poll(
                    client_id=query.get("client_id", [""])[0],
                    timeout_seconds=float(query.get("timeout_seconds", ["30"])[0]),
                )})
            else:
                error(self, "Not found", 404)
        except (ValueError, TypeError, OverflowError) as exc:
            error(self, str(exc))
        except Exception:
            logging.getLogger("vanth.daemon").exception("GET request failed")
            error(self, "Internal server error", 500)

    def do_POST(self) -> None:
        if shutdown_event.is_set():
            error(self, "Daemon is shutting down", 503)
            return
        try:
            raw_length = self.headers.get("Content-Length")
            if raw_length is None:
                raise ValueError("Content-Length header is required")
            try:
                length = int(raw_length)
            except ValueError as exc:
                raise ValueError("Content-Length must be an integer") from exc
            if length < 0:
                raise ValueError("Content-Length must not be negative")
            try:
                max_request_bytes = int(os.environ.get("VANTH_MAX_REQUEST_BYTES", DEFAULT_MAX_REQUEST_BYTES))
            except ValueError:
                max_request_bytes = DEFAULT_MAX_REQUEST_BYTES
            if length > max_request_bytes:
                raise RequestTooLarge(f"Request body exceeds {max_request_bytes} bytes")
            body = self.rfile.read(length)
            if len(body) != length:
                raise ValueError("Request body is shorter than Content-Length")
            payload = json.loads(body.decode("utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("JSON body must be an object")
            if not self._authorized():
                if self.path == "/jobs" and ("command" not in payload or set(payload) - _JOB_START_FIELDS):
                    raise ValueError("invalid job request")
                error(self, "Unauthorized", 401)
                return
            parsed = urllib.parse.urlparse(self.path)
            decision_route = _decision_route(parsed.path)
            if parsed.path in {"/jobs", "/jobs/preview"}:
                _validate_job_start_payload(payload)
                remote_id = payload.pop("remote_id", None)
                if parsed.path == "/jobs/preview":
                    if remote_id:
                        raise ValueError("start preview is supported for local jobs only")
                    ok(self, asyncio.run(get_manager().start(**payload, dry_run=True)))
                    return
                if remote_id:
                    ok(self, _remote_submit(remote_id, "job.start", _remote_payload(payload)))
                else:
                    # Field shapes are validated at the manager boundary, so a
                    # TypeError here is a genuine bug, not user error: report it
                    # as a 500 and log a traceback rather than echoing an
                    # internal Python message as if the caller got it wrong.
                    try:
                        result = asyncio.run(get_manager().start(**payload))
                    except TypeError:
                        logging.getLogger("vanth.daemon").exception("job start failed")
                        error(self, "Internal server error", 500)
                        return
                    ok(self, result)
            elif decision_route is not None:
                action, decision_job, token = decision_route
                if action == "request":
                    ok(self, get_manager().request_decision(decision_job, **payload))
                elif action == "resolve":
                    ok(self, get_manager().resolve_decision(decision_job, token, payload.get("choice", "")))
                else:
                    ok(self, get_manager().withdraw_decision(decision_job, token))
            elif "decision" in parsed.path.split("/"):
                # A decision-looking path that did not match one of the exact
                # shapes must not fall through to another job operation (e.g.
                # .../decision/<token>/pause must not pause the job).
                error(self, "Not found", 404)
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/rerun"):
                remote_id = payload.pop("remote_id", None)
                if remote_id:
                    job_id = parsed.path.split("/")[2]
                    # secret_env masking is local-only; drop it before the strict
                    # remote protocol (which rejects unknown fields).
                    remote_payload = {
                        key: value
                        for key, value in payload.items()
                        if key in {"command", "env", "timeout_seconds", "name", "tags", "notes", "cwd", "interactive", "idempotency_key"}
                    }
                    ok(self, _remote_submit(remote_id, "job.rerun", {"job_id": job_id, **remote_payload}))
                else:
                    ok(self, asyncio.run(get_manager().rerun(parsed.path.split("/")[2], **payload)))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/wait"):
                remote_id = payload.pop("remote_id", None)
                if remote_id:
                    ok(self, _remote_wait(remote_id, parsed.path.split("/")[2], payload))
                else:
                    ok(self, get_manager().wait_sync(parsed.path.split("/")[2], **payload))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/stop"):
                remote_id = payload.pop("remote_id", None)
                if remote_id:
                    # actor/reason are local stop attribution; the remote
                    # protocol accepts only signal/kill_after_seconds.
                    remote_payload = {
                        key: value
                        for key, value in payload.items()
                        if key in {"signal", "kill_after_seconds", "idempotency_key"}
                    }
                    ok(self, _remote_submit(remote_id, "job.stop", {"job_id": parsed.path.split("/")[2], **remote_payload}))
                else:
                    ok(self, get_manager().stop_sync(parsed.path.split("/")[2], **payload))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/send"):
                ok(self, get_manager().send_sync(parsed.path.split("/")[2], **payload))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/pause"):
                ok(self, get_manager().job_pause(parsed.path.split("/")[2]))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/resume"):
                ok(self, get_manager().job_resume(parsed.path.split("/")[2]))
            elif parsed.path == "/schedules":
                ok(self, get_manager().create_schedule(**payload))
            elif parsed.path.startswith("/schedules/") and parsed.path.endswith("/update"):
                ok(self, get_manager().update_schedule(parsed.path.split("/")[2], **payload))
            elif parsed.path.startswith("/schedules/") and parsed.path.endswith("/delete"):
                ok(self, get_manager().delete_schedule(parsed.path.split("/")[2]))
            elif parsed.path == "/pools":
                ok(self, get_manager().pool_configure(**payload))
            elif parsed.path.startswith("/deliveries/") and parsed.path.endswith("/mark"):
                ok(self, get_manager().mark_delivery(parsed.path.split("/")[2], **payload))
            elif parsed.path.startswith("/deliveries/") and parsed.path.endswith("/retry"):
                ok(self, get_manager().retry_delivery(parsed.path.split("/")[2]))
            elif parsed.path == "/deliveries/clear":
                ok(self, get_manager().clear_deliveries(**payload))
            elif parsed.path == "/cleanup":
                ok(self, get_manager().cleanup(**payload))
            elif parsed.path == "/reap-orphans":
                ok(self, get_manager().reap_orphans())
            elif parsed.path == "/relay/register":
                ok(self, get_manager().relay_register(
                    client_id=payload.get("client_id", ""),
                    client_type=payload.get("client_type", ""),
                    destinations=payload.get("destinations", []),
                ))
            elif parsed.path == "/relay/unregister":
                ok(self, get_manager().relay_unregister(payload.get("client_id", "")))
            elif parsed.path == "/relay/ack":
                ok(self, get_manager().relay_ack(
                    client_id=payload.get("client_id", ""),
                    delivery_id=payload.get("delivery_id", ""),
                    status=payload.get("status", ""),
                    error=payload.get("error"),
                    lease_token=payload.get("lease_token"),
                ))
            elif parsed.path == "/relay/release":
                ok(self, get_manager().relay_release(
                    client_id=payload.get("client_id", ""),
                    delivery_id=payload.get("delivery_id", ""),
                    lease_token=payload.get("lease_token"),
                ))
            elif parsed.path == "/remotes/pair":
                from .remote.pairing import pair_remote

                ok(self, pair_remote(
                    target=payload.get("target", ""),
                    name=payload.get("name"),
                    allow_root=bool(payload.get("allow_root", False)),
                    accept_host_key=bool(payload.get("accept_host_key", False)),
                    host_fingerprint=payload.get("host_fingerprint"),
                    helper_command=payload.get("helper_command"),
                    remote_home=payload.get("remote_home"),
                    store=get_remote_store(),
                ))
            elif parsed.path == "/remotes/remove":
                from .remote.pairing import remove_remote

                ok(self, remove_remote(remote_id=payload.get("remote_id"), store=get_remote_store()))
            elif parsed.path == "/remote/identity":
                # Loopback-only identity probe used by the remote helper's
                # sentinel hello (review rc14 P0-1.4).
                remote_store = _remote_job_manager().store
                ok(self, {"state_epoch": remote_store.get_state_epoch(), "instance_id": remote_store.get_instance_id()})
            elif parsed.path == "/remote/helper":
                frame = payload.get("frame", payload)
                # ``frame`` is caller-supplied: a non-object (e.g. {"frame": 5})
                # would raise AttributeError on .get and surface as a 500.
                if not isinstance(frame, dict):
                    raise ValueError("frame must be an object")
                remote = _remote_job_manager()
                kind = frame.get("kind")
                if kind == "request":
                    response = remote.handle_request(frame)
                elif kind == "snapshot":
                    response = remote.handle_snapshot_request(frame)
                elif kind == "log_range":
                    response = remote.handle_log_range_request(frame)
                else:
                    raise VanthRemoteProtocolError("PROTOCOL_UNKNOWN_KIND", f"unknown frame kind: {frame.get('kind')!r}")
                ok(self, response)
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/metrics"):
                ok(self, get_manager().metric_ingest(parsed.path.split("/")[2], **payload))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/wake"):
                ok(self, get_manager().add_wake_target(parsed.path.split("/")[2], **payload))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/wake-now"):
                ok(self, get_manager().wake_now(parsed.path.split("/")[2], **payload))
            elif parsed.path.startswith("/jobs/") and parsed.path.endswith("/artifacts"):
                ok(self, get_manager().artifact_add(parsed.path.split("/")[2], **payload))
            elif parsed.path == "/artifacts/put":
                data = None
                if payload.get("data_b64") is not None:
                    import base64

                    data = base64.b64decode(payload["data_b64"])
                ok(self, get_artifacts().put_file(
                    payload.get("name"),
                    data=data,
                    source_path=payload.get("path"),
                    idempotency_key=payload.get("idempotency_key"),
                ))
            elif parsed.path == "/artifacts/put-dir":
                ok(self, get_artifacts().put_dir(
                    payload.get("source_path"),
                    payload.get("name"),
                    idempotency_key=payload.get("idempotency_key"),
                ))
            elif parsed.path == "/artifacts/materialize":
                # Hoisted deliberately: Python evaluates the receiver
                # (get_artifacts()) BEFORE the arguments, so inlining these
                # required_field calls would open the catalog for a request that
                # is missing a field — and could surface an init error instead of
                # the field error.
                materialize_version = required_field(payload, "version_id")
                materialize_dest = required_field(payload, "dest_path")
                ok(self, get_artifacts().materialize(
                    materialize_version,
                    materialize_dest,
                    overwrite=bool(payload.get("overwrite", False)),
                    idempotency_key=payload.get("idempotency_key"),
                ))
            elif parsed.path == "/artifacts/verify":
                verify_version = required_field(payload, "version_id")
                ok(self, get_artifacts().verify(verify_version, idempotency_key=payload.get("idempotency_key")))
            elif parsed.path == "/artifacts/collections":
                ok(self, get_artifact_collections().create_collection(
                    payload.get("name"), idempotency_key=payload.get("idempotency_key")))
            elif parsed.path == "/artifacts/collections/append":
                ok(self, get_artifact_collections().append_version(
                    payload.get("collection") or payload.get("name"),
                    payload.get("version_id"),
                    idempotency_key=payload.get("idempotency_key"),
                ))
            elif parsed.path == "/artifacts/alias-set":
                ok(self, get_artifact_collections().alias_set(
                    payload.get("alias_name"),
                    payload.get("root_id"),
                    payload.get("expected_version_id"),
                    payload.get("new_version_id"),
                    idempotency_key=payload.get("idempotency_key"),
                    updated_by=payload.get("updated_by"),
                ))
            elif parsed.path == "/artifacts/lineage":
                ok(self, get_artifact_collections().link_lineage(
                    payload.get("producer_kind"),
                    payload.get("producer_id"),
                    payload.get("consumer_kind"),
                    payload.get("consumer_id"),
                    payload.get("version_id"),
                    idempotency_key=payload.get("idempotency_key"),
                ))
            elif parsed.path == "/artifacts/delete-request":
                ok(self, get_artifact_lifecycle().request_delete(
                    payload.get("version_id"), idempotency_key=payload.get("idempotency_key")))
            elif parsed.path == "/artifacts/restore-version":
                ok(self, get_artifact_lifecycle().restore(
                    payload.get("version_id"), idempotency_key=payload.get("idempotency_key")))
            elif parsed.path == "/artifacts/pin":
                ok(self, get_artifact_lifecycle().pin(
                    payload.get("version_id"), payload.get("hold_reason"),
                    idempotency_key=payload.get("idempotency_key")))
            elif parsed.path == "/artifacts/unpin":
                ok(self, get_artifact_lifecycle().unpin(
                    payload.get("version_id"), idempotency_key=payload.get("idempotency_key")))
            elif parsed.path == "/artifacts/gc":
                ok(self, get_artifact_lifecycle().gc(
                    dry_run=bool(payload.get("dry_run", True)),
                    idempotency_key=payload.get("idempotency_key")))
            elif parsed.path == "/artifacts/backup":
                ok(self, {"backup_path": str(get_artifact_lifecycle().backup())})
            elif parsed.path == "/artifacts/begin-restore":
                ok(self, get_artifact_lifecycle().begin_restore(payload.get("backup_path")))
            elif parsed.path == "/artifacts/complete-restore":
                ok(self, get_artifact_lifecycle().complete_restore())
            elif parsed.path == "/artifacts/storage-profiles":
                ok(self, get_artifact_storage_profiles().create(
                    payload.get("kind", "s3"), payload.get("config")))
            elif parsed.path.startswith("/artifacts/storage-profiles/") and parsed.path.endswith("/probe"):
                ok(self, get_artifact_storage_profiles().probe(parsed.path.split("/")[2]))
            elif parsed.path.startswith("/artifacts/storage-profiles/") and parsed.path.endswith("/update"):
                ok(self, get_artifact_storage_profiles().update(
                    parsed.path.split("/")[2], payload.get("config") or {},
                    idempotency_key=payload.get("idempotency_key")))
            elif parsed.path == "/artifacts/push-remote":
                # Validate BEFORE building the broker: arguments are evaluated
                # after the receiver, so inlining these calls would open the
                # broker (an expensive, lock-taking init) for a bad request.
                push_remote_id = required_field(payload, "remote_id")
                push_version_id = required_field(payload, "version_id")
                ok(self, get_artifact_broker().push_blob(
                    push_remote_id, push_version_id,
                    idempotency_key=payload.get("idempotency_key"),
                ))
            elif parsed.path == "/artifacts/pull-remote":
                pull_remote_id = required_field(payload, "remote_id")
                pull_version_id = required_field(payload, "version_id")
                pull_dest_path = required_field(payload, "dest_path")
                ok(self, get_artifact_broker().pull_blob(
                    pull_remote_id, pull_version_id, pull_dest_path,
                    idempotency_key=payload.get("idempotency_key"),
                ))
            elif parsed.path == "/shutdown":
                _stop_httpd()
                ok(self, {"result": "shutting_down"})
            else:
                error(self, "Not found", 404)
        except RequestTooLarge as exc:
            error(self, str(exc), 413)
        except VanthRemoteProtocolError as exc:
            # A remote-protocol rejection is a caller/state error, not a daemon
            # fault. Mapping it to 500 hid actionable messages (e.g. "remote
            # mutations require caller-supplied idempotency_key") behind
            # "Internal server error". A malformed request is a 400; state
            # conflicts (unpaired identity, unknown frame kind) are 409, matching
            # the GET remote path.
            error(self, str(exc), 400 if getattr(exc, "code", "") == "INVALID_REQUEST" else 409)
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError, TypeError, OverflowError) as exc:
            error(self, str(exc))
        except Exception:
            logging.getLogger("vanth.daemon").exception("POST request failed")
            error(self, "Internal server error", 500)


def _loopback(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _configure_logging(home: Path) -> logging.Logger:
    (home / "logs").mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("vanth.daemon")
    logger.setLevel(os.environ.get("VANTH_LOG_LEVEL", "INFO").upper())
    logger.propagate = False
    handler = RotatingFileHandler(
        home / "logs" / "daemon.log",
        maxBytes=int(os.environ.get("VANTH_LOG_MAX_BYTES", str(5 * 1024 * 1024))),
        backupCount=int(os.environ.get("VANTH_LOG_BACKUP_COUNT", "3")),
        encoding="utf-8",
    )
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s pid=%(process)d component=daemon %(message)s"))
    logger.addHandler(handler)
    return logger


def write_daemon_metadata(home: Path, url: str) -> None:
    """Atomically write daemon discovery metadata for clients and the monitor."""
    from .migrations import LATEST_SCHEMA_VERSION

    payload = {
        "url": url,
        "home": str(home),
        "pid": os.getpid(),
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "schema_version": LATEST_SCHEMA_VERSION,
        # Auth bootstrap so a client (or a human) can reach the API without
        # reading source. The scheme/path only — never the token itself.
        "auth": "bearer",
        "token_path": str(home / "token"),
    }
    path = home / "daemon.json"
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    os.replace(tmp, path)


def remove_daemon_metadata(home: Path) -> None:
    try:
        (home / "daemon.json").unlink()
    except FileNotFoundError:
        pass


def main() -> None:
    host = os.environ.get("VANTH_DAEMON_HOST", "127.0.0.1")
    if not _loopback(host):
        raise SystemExit("VANTH_DAEMON_HOST must be localhost or a loopback address")
    try:
        port = int(os.environ.get("VANTH_DAEMON_PORT", "8765"))
    except ValueError as exc:
        raise SystemExit("VANTH_DAEMON_PORT must be an integer") from exc
    home = canonical_home()
    ensure_auth_token(home)
    secure_home_permissions(home)
    logger = _configure_logging(home)
    # Startup progress records: a daemon that never logs "started" is stuck
    # before serving (lock, bind, or migration) — without these, a wedged
    # startup is indistinguishable from a slow one in the log.
    lock = DaemonLock(home / "daemon.lock")
    if not lock.acquire():
        raise SystemExit("another vanthd already owns this VANTH_HOME")
    logger.info("vanthd starting home=%s pid=%s (home lock acquired)", home, os.getpid())
    try:
        httpd = None
        for attempt in range(6):
            try:
                httpd = TrackingHTTPServer((host, port), Handler)
                break
            except OSError:
                if attempt == 5:
                    raise
                time.sleep(0.1 * (attempt + 1))
    except OSError as exc:
        lock.release()
        raise SystemExit(f"cannot bind {host}:{port}: {exc}") from exc
    logger.info("vanthd starting home=%s pid=%s (bound %s:%s)", home, os.getpid(), host, port)
    _set_httpd(httpd)
    shutdown_event.clear()
    try:
        remote_wake_interval = float(os.environ.get("VANTH_REMOTE_WAKE_SYNC_SECONDS", "5"))
    except ValueError:
        remote_wake_interval = 5.0
    if remote_wake_interval != 0:
        threading.Thread(target=_remote_wake_sync_loop, name="remote-wake-sync", daemon=True).start()
    daemon_url = f"http://{host}:{port}"
    write_daemon_metadata(home, daemon_url)
    # Always leave a startup record: otherwise a healthy daemon that logs no
    # warnings is indistinguishable from a dead one by its (stale) log file.
    logger.info("vanthd started url=%s home=%s pid=%s", daemon_url, home, os.getpid())

    previous = {}
    signal_names = [signal.SIGINT, signal.SIGTERM]
    if hasattr(signal, "SIGBREAK"):
        signal_names.append(signal.SIGBREAK)
    for name in signal_names:
        try:
            previous[name] = signal.signal(name, _stop_httpd)
        except ValueError:
            pass
    try:
        httpd.serve_forever()
    finally:
        logger.info("vanthd stopping")
        if manager is not None:
            manager.begin_shutdown()
        httpd.wait_for_requests(float(os.environ.get("VANTH_SHUTDOWN_TIMEOUT", "10")))
        httpd.server_close()
        if manager is not None:
            manager.close()
        lock.release()
        remove_daemon_metadata(home)
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)
        for name, handler in previous.items():
            try:
                signal.signal(name, handler)
            except ValueError:
                pass


if __name__ == "__main__":
    main()
