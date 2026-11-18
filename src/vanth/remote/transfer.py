"""Brokered artifact transfer over the bulk SSH session seam (Phase 9).

Two sides, one module:

- :class:`TransferRegistry` runs on the REMOTE side (wired into
  ``RemoteJobManager.handle_request``) and implements the
  ``artifact.transfer_init`` / ``artifact.blob_chunk`` /
  ``artifact.transfer_complete`` methods. Transfers live in the
  ``remote_transfers`` table on the remote database; chunks are verified for
  strict offset continuity and per-chunk sha256 before they are appended to a
  staging file and acknowledged in ONE transaction. Publication happens only
  at ``transfer_complete``, riding the EXISTING fenced durable-operation
  pattern (:meth:`ArtifactOperations.put_file`) keyed by an idempotency key
  derived from the transfer id — no parallel durability mechanism.

- :class:`RemoteArtifactBroker` runs on the CONTROLLER side and brokers
  publication/materialization over ONE bulk SSH session per transfer
  (``control.transport.open_session``). Controller-side resume state lives in
  the ``controller_transfers`` table on the ARTIFACTS CATALOG database
  (chosen because the broker already holds ``ops.catalog.db`` there and the
  transfer is an artifact-lifecycle concern, not a request-row concern).
  Every acknowledged offset is persisted durably before the next chunk is
  sent; a retry with the same idempotency key resumes from the acknowledged
  offset and never re-sends acked bytes.

Security invariants: nothing but artifact bytes (base64) and content
identifiers crosses the wire — no storage-profile credentials, tokens, or
config ever enter a transfer frame. Byte counts are exact (every chunk is
verified; total == manifest size before completion), and BOTH sides hash the
content: the controller knows the expected whole-blob sha256 up front and the
remote hashes what it actually received before publishing.

State-epoch invariant: every init/chunk/complete response carries the
remote's CURRENT state epoch. If it changes mid-transfer the transfer is
aborted permanently ("epoch changed; transfer stopped rather than rebound") —
never rebound onto the new timeline.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import secrets
from pathlib import Path
from typing import Any, Callable

from .protocol import (
    IDEMPOTENCY_KEY_RE,
    VanthRemoteProtocolError,
    decode_frame,
    encode_frame,
    request_digest,
)
from ..server import now_iso

__all__ = [
    "TransferRegistry",
    "RemoteArtifactBroker",
    "TransferInterrupted",
    "TransferAborted",
    "EPOCH_STOP_MESSAGE",
    "DEFAULT_CHUNK_BYTES",
    "_TRANSFER_METHODS",
]

_TRANSFER_METHODS = frozenset(
    {"artifact.transfer_init", "artifact.blob_chunk", "artifact.transfer_complete"}
)

REMOTE_TRANSFER_DDL = """
CREATE TABLE IF NOT EXISTS remote_transfers (
  transfer_id TEXT PRIMARY KEY,
  direction TEXT NOT NULL,
  root_name TEXT NOT NULL,
  manifest_digest TEXT NOT NULL,
  total_bytes INTEGER NOT NULL,
  sha256 TEXT NOT NULL,
  acked_offset INTEGER NOT NULL DEFAULT 0,
  staging_path TEXT NOT NULL,
  state_epoch INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
"""

# Mirrors remote_transfers plus op_claim_token (the fence token that gates
# every controller-side offset/completion update). Lives on the artifacts
# CATALOG database (ops.catalog.db): the transfer is an artifact-lifecycle
# concern and the broker already owns that connection.
CONTROLLER_TRANSFER_DDL = """
CREATE TABLE IF NOT EXISTS controller_transfers (
  transfer_id TEXT PRIMARY KEY,
  remote_id TEXT NOT NULL,
  direction TEXT NOT NULL,
  root_name TEXT NOT NULL,
  manifest_digest TEXT NOT NULL,
  total_bytes INTEGER NOT NULL,
  sha256 TEXT NOT NULL,
  acked_offset INTEGER NOT NULL DEFAULT 0,
  idempotency_key TEXT NOT NULL,
  op_claim_token TEXT,
  state_epoch INTEGER,
  status TEXT NOT NULL DEFAULT 'active',
  error TEXT,
  result_json TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
"""


def _ensure_controller_schema(db: Any) -> None:
    """Migrate the controller ledger off the legacy global-unique key.

    Transfer ids bind remote/destination context, so the SAME caller key in a
    DIFFERENT context must be allowed its own row (review rc14 P1-10b): with
    ``UNIQUE(idempotency_key)`` the INSERT was silently ignored and the
    takeover UPDATE latched onto the other context's row."""
    row = db.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='controller_transfers'"
    ).fetchone()
    if not row or "idempotency_key" not in str(row["sql"]):
        return
    if "UNIQUE" not in str(row["sql"]).upper():
        return
    db.executescript(
        """
        BEGIN;
        CREATE TABLE controller_transfers_new (
          transfer_id TEXT PRIMARY KEY,
          remote_id TEXT NOT NULL,
          direction TEXT NOT NULL,
          root_name TEXT NOT NULL,
          manifest_digest TEXT NOT NULL,
          total_bytes INTEGER NOT NULL,
          sha256 TEXT NOT NULL,
          acked_offset INTEGER NOT NULL DEFAULT 0,
          idempotency_key TEXT NOT NULL,
          op_claim_token TEXT,
          state_epoch INTEGER,
          status TEXT NOT NULL DEFAULT 'active',
          error TEXT,
          result_json TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        INSERT INTO controller_transfers_new SELECT * FROM controller_transfers;
        DROP TABLE controller_transfers;
        ALTER TABLE controller_transfers_new RENAME TO controller_transfers;
        COMMIT;
        """
    )

EPOCH_STOP_MESSAGE = "epoch changed; transfer stopped rather than rebound"

DEFAULT_CHUNK_BYTES = 256 * 1024


def _walk_parent_dirfd(parent: Path) -> int:
    """POSIX: open the staging parent DESCRIPTOR-RELATIVELY (rc19 review N3).

    Every component is opened O_NOFOLLOW|O_DIRECTORY from the filesystem
    root, so a component swapped for a symlink after inspection cannot
    redirect the leaf open. The caller owns the returned fd."""
    import os as _os

    flags = _os.O_RDONLY | getattr(_os, "O_DIRECTORY", 0)
    if hasattr(_os, "O_NOFOLLOW"):
        flags |= _os.O_NOFOLLOW
    absolute = Path(_os.path.abspath(str(parent)))
    fd = _os.open(absolute.anchor, flags)
    try:
        for component in absolute.parts[1:]:
            next_fd = _os.open(component, flags, dir_fd=fd)
            _os.close(fd)
            fd = next_fd
        return fd
    except BaseException:
        _os.close(fd)
        raise


def _stage_leaf_fd(path: Path, *, write: bool, create: bool) -> int:
    """Resolve the staging leaf against the walked parent descriptor and
    return a raw fd (rc19 review N3). POSIX path."""
    import os as _os
    import stat as _stat

    parent_fd = _walk_parent_dirfd(path.parent)
    try:
        flags = _os.O_RDWR if write else _os.O_RDONLY
        if create:
            flags |= _os.O_CREAT
        if hasattr(_os, "O_BINARY"):
            flags |= getattr(_os, "O_BINARY")
        if hasattr(_os, "O_NOFOLLOW"):
            flags |= _os.O_NOFOLLOW
        fd = _os.open(path.name, flags, 0o600, dir_fd=parent_fd)
    finally:
        _os.close(parent_fd)
    info = _os.fstat(fd)
    if not _stat.S_ISREG(info.st_mode):
        _os.close(fd)
        raise TransferAborted(f"staging leaf is not a regular file; refused: {path}")
    return fd


def _windows_stage_file(path: Path, *, write: bool, create: bool):
    """Windows staging open with reparse-safe validation (rc19 review N3).

    A first probe handle opened WITH FILE_FLAG_OPEN_REPARSE_POINT inspects
    the LEAF itself (never its target); anything carrying
    FILE_ATTRIBUTE_REPARSE_POINT aborts. The real I/O handle is then opened
    and compared BY FILE IDENTITY against the probe — a swap between the two
    opens aborts."""
    import ctypes as _ctypes
    import msvcrt as _msvcrt

    import os as _os

    kernel32 = _ctypes.windll.kernel32  # type: ignore[attr-defined]
    GENERIC_READ = 0x80000000
    GENERIC_WRITE = 0x40000000
    OPEN_EXISTING = 3
    CREATE_NEW = 1
    OPEN_ALWAYS = 4
    FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000
    FILE_ATTRIBUTE_NORMAL = 0x80
    FILE_ATTRIBUTE_REPARSE_POINT = 0x400
    INVALID = _ctypes.c_void_p(-1).value

    class _FILETIME(_ctypes.Structure):
        _fields_ = [("dwLowDateTime", _ctypes.c_uint32),
                    ("dwHighDateTime", _ctypes.c_uint32)]

    class _BY_HANDLE_FILE_INFORMATION(_ctypes.Structure):
        # Exact Windows ABI (rc20 review P1c): FILETIME is two DWORDs — a
        # c_uint64 would misalign every field after it.
        _fields_ = [
            ("dwFileAttributes", _ctypes.c_uint32),
            ("ftCreationTime", _FILETIME),
            ("ftLastAccessTime", _FILETIME),
            ("ftLastWriteTime", _FILETIME),
            ("dwVolumeSerialNumber", _ctypes.c_uint32),
            ("nFileSizeHigh", _ctypes.c_uint32),
            ("nFileSizeLow", _ctypes.c_uint32),
            ("nNumberOfLinks", _ctypes.c_uint32),
            ("nFileIndexHigh", _ctypes.c_uint32),
            ("nFileIndexLow", _ctypes.c_uint32),
        ]

    kernel32.CreateFileW.restype = _ctypes.c_void_p
    kernel32.CreateFileW.argtypes = [
        _ctypes.c_wchar_p, _ctypes.c_uint32, _ctypes.c_uint32, _ctypes.c_void_p,
        _ctypes.c_uint32, _ctypes.c_uint32, _ctypes.c_void_p,
    ]
    kernel32.GetFileInformationByHandle.argtypes = [
        _ctypes.c_void_p, _ctypes.POINTER(_BY_HANDLE_FILE_INFORMATION),
    ]
    kernel32.GetFinalPathNameByHandleW.restype = _ctypes.c_uint32
    kernel32.GetFinalPathNameByHandleW.argtypes = [
        _ctypes.c_void_p, _ctypes.c_wchar_p, _ctypes.c_uint32, _ctypes.c_uint32,
    ]

    def _identity(handle):
        info = _BY_HANDLE_FILE_INFORMATION()
        if not kernel32.GetFileInformationByHandle(_ctypes.c_void_p(handle),
                                                   _ctypes.byref(info)):
            return None
        return (info.dwVolumeSerialNumber, info.nFileIndexHigh, info.nFileIndexLow,
                info.dwFileAttributes)

    def _final_path(handle):
        """Resolved path of an open handle. FAILS CLOSED (rc21 review): API
        failure or buffer exhaustion raises instead of returning None."""
        size = 1024
        while True:
            buf = _ctypes.create_unicode_buffer(size)
            # FILE_NAME_NORMALIZED = 0
            written = kernel32.GetFinalPathNameByHandleW(
                _ctypes.c_void_p(handle), buf, size, 0)
            if written == 0:
                raise TransferAborted(
                    "staging containment check failed: cannot resolve the "
                    f"final path of the staging handle (error {_ctypes.GetLastError()})"
                )
            if written <= size:
                final = buf.value
                break
            size = written  # required size; retry with a bigger buffer
        # Normalize device forms (rc22 review): \\?\UNC\server\share must
        # compare equal to the intended \\server\share path.
        if final.startswith("\\\\?\\UNC\\"):
            final = "\\\\" + final[len("\\\\?\\UNC\\"):]
        elif final.startswith("\\\\?\\"):
            final = final[len("\\\\?\\"):]
        return _os.path.normcase(final)

    access = (GENERIC_READ | GENERIC_WRITE) if write else GENERIC_READ
    creation = OPEN_ALWAYS if create else OPEN_EXISTING

    # Probe the leaf WITHOUT following it.
    probe = kernel32.CreateFileW(str(path), GENERIC_READ, 0, None, OPEN_EXISTING,
                                 FILE_FLAG_OPEN_REPARSE_POINT, None)
    exists = bool(probe) and probe != INVALID
    identity = None
    if exists:
        try:
            identity = _identity(probe)
            if identity is None:
                # Fail closed (rc21 review): containment cannot be proven.
                raise TransferAborted(
                    f"staging containment check failed: cannot query handle information for {path}"
                )
            if identity[3] & FILE_ATTRIBUTE_REPARSE_POINT:
                raise TransferAborted(f"staging leaf is a reparse point; refused: {path}")
        finally:
            kernel32.CloseHandle(_ctypes.c_void_p(probe))
    elif create:
        creation = CREATE_NEW  # exclusive create cannot follow any link

    handle = kernel32.CreateFileW(str(path), access, 0, None, creation,
                                  FILE_ATTRIBUTE_NORMAL, None)
    if not handle or handle == INVALID:
        err = _ctypes.GetLastError()
        raise OSError(err, f"staging open failed: {path}")
    # Parent-bind validation (rc20 review P1c): both handles could have been
    # consistently redirected by a swapped ancestor junction — resolve the
    # FINAL path of the I/O handle and require it to be the intended file.
    intended = _os.path.normcase(_os.path.abspath(str(path)))
    try:
        resolved = _final_path(handle)
    except BaseException:
        # Fail closed AND close the raw handle (rc22 review P2): a leaked
        # handle keeps the staging file locked after the abort.
        kernel32.CloseHandle(_ctypes.c_void_p(handle))
        raise
    if resolved != intended:
        kernel32.CloseHandle(_ctypes.c_void_p(handle))
        raise TransferAborted(
            f"staging leaf resolved outside the intended path; refused: {path} -> {resolved}"
        )
    real_identity = _identity(handle)
    if real_identity is None:
        kernel32.CloseHandle(_ctypes.c_void_p(handle))
        raise TransferAborted(
            f"staging containment check failed: cannot query handle information for {path}"
        )
    if identity is not None and real_identity[:3] != identity[:3]:
        kernel32.CloseHandle(_ctypes.c_void_p(handle))
        raise TransferAborted(f"staging leaf was swapped during open; refused: {path}")
    flags = getattr(_os, "O_NOINHERIT", 0)
    fd = _msvcrt.open_osfhandle(handle, flags)
    try:
        mode = "r+b" if write else "rb"
        return _os.fdopen(fd, mode, closefd=True)
    except BaseException:
        _os.close(fd)
        raise


def _stage_open_file(path: Path, *, write: bool, create: bool):
    """Platform-dispatching validated staging open (rc19 review N3)."""
    import os as _os

    if os.name == "nt":
        return _windows_stage_file(path, write=write, create=create)
    fd = _stage_leaf_fd(path, write=write, create=create)
    try:
        return _os.fdopen(fd, "r+b" if write else "rb", closefd=True)
    except BaseException:
        _os.close(fd)
        raise


def _open_local_nofollow(path: Path):
    """Controller-side validated read of a pull staging file (rc18 R3/R4,
    rc19 N3): descriptor-relative on POSIX, validated-handle on Windows."""
    import os as _os

    return _stage_open_file(path, write=False, create=False)


def _verify_staging_chain(base: Path) -> None:
    """Refuse symlink/junction ANCESTORS of a staging directory (rc18 R4).

    Staging parents are created and traversed path-wise; without this sweep
    a swapped ancestor redirects the leaf write outside the trusted tree.
    Mirrors ArtifactOperations._verify_dest_parents: any metadata error is
    fatal, reparse points and symlinks abort."""
    import os as _os
    import stat as _stat

    for ancestor in [base, *base.parents]:
        try:
            info = _os.lstat(str(ancestor))
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise TransferAborted(f"staging parent unusable ({ancestor}): {exc}") from None
        if hasattr(info, "st_reparse_tag") and getattr(info, "st_reparse_tag", 0):
            raise TransferAborted(f"staging parent is a reparse point; refused: {ancestor}")
        if _stat.S_ISLNK(info.st_mode):
            raise TransferAborted(f"staging parent is a symlink; refused: {ancestor}")


_EXPECTED_OFFSET_RE = re.compile(r"expected_offset=(\d+)")


def _mint_key(prefix: str) -> str:
    return f"{prefix}-{secrets.token_hex(12)}"


def _valid_key(key: str | None) -> bool:
    return isinstance(key, str) and bool(IDEMPOTENCY_KEY_RE.match(key))


class TransferInterrupted(RuntimeError):
    """Transport-level interruption; the transfer resumes from its recorded
    acknowledged offset when retried with the SAME idempotency key."""

    def __init__(self, transfer_id: str, acked_offset: int, message: str = "transfer interrupted") -> None:
        super().__init__(message)
        self.transfer_id = transfer_id
        self.acked_offset = int(acked_offset)


class TransferAborted(RuntimeError):
    """Permanent failure (epoch change, tamper); retrying must not resume."""


class TransferRegistry:
    """Remote-side registry of bulk artifact transfers (Phase 9)."""

    def __init__(self, db, *, epoch_fn: Callable[[], int], ops_factory: Callable[[], Any] | None = None,
                 staging_dir: str | Path | None = None,
                 fence_lock: Any = None) -> None:
        self.db = db
        self.db.executescript(REMOTE_TRANSFER_DDL)
        self.db.commit()
        self._epoch_fn = epoch_fn
        self._ops_factory = ops_factory
        self.staging_dir = Path(staging_dir) if staging_dir else None
        self._fence_lock = fence_lock
        self._ops: Any = None

    def _publication_fence(self):
        """Hold the store's serialization lock across the whole publication
        (rc17 F6, rc18 R1). The put_file guard re-checks the epoch INSIDE the
        catalog transaction; with this single shared lock a state-epoch bump
        cannot land between check and commit, and there is no second lock to
        order-invert against db_lock."""
        import contextlib

        return self._fence_lock if self._fence_lock is not None else contextlib.nullcontext()

    # ------------------------------------------------------------------
    # Remote-side ArtifactOperations (lazy: only publication needs it)
    # ------------------------------------------------------------------

    def ops(self) -> Any:
        if self._ops is None:
            if self._ops_factory is None:
                raise RuntimeError("remote artifact operations are unavailable")
            self._ops = self._ops_factory()
        return self._ops

    def _staging_path(self, transfer_id: str) -> str:
        base = self.staging_dir
        if base is None:
            from ..paths import canonical_home

            base = canonical_home() / "remote-transfers"
        base.mkdir(parents=True, exist_ok=True)
        return str(base / f"{transfer_id}.part")

    @staticmethod
    def _open_staging(path: Path, *, write: bool, create: bool = False):
        """Validated staging open (rc17 F4 / rc18 R4 / rc19 N3): the parent
        chain is traversed descriptor-relative on POSIX and the Windows leaf
        is reparse-checked with identity comparison — a swap during open
        aborts instead of redirecting I/O."""
        return _stage_open_file(path, write=write, create=create)

    # ------------------------------------------------------------------
    # Handlers (called from RemoteJobManager routing)
    # ------------------------------------------------------------------

    def init(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Register or resume a durable transfer. Returns the resume point."""
        transfer_id = payload["transfer_id"]
        direction = payload["direction"]
        now = now_iso()
        epoch = int(self._epoch_fn())
        staging_path = self._staging_path(transfer_id)
        root_name = str(payload.get("root_name") or "")
        manifest_digest = str(payload.get("manifest_digest") or "")
        total_bytes = int(payload.get("total_bytes") or 0)
        sha256 = str(payload.get("sha256") or "")
        version_id = payload.get("version_id")
        if direction == "pull":
            # The remote's own catalog is authoritative for pull transfers.
            row_v = self.ops().catalog.db.execute(
                "SELECT * FROM versions WHERE version_id=?", (version_id,)
            ).fetchone()
            if not row_v:
                raise VanthRemoteProtocolError("INVALID_REQUEST", f"unknown remote version: {version_id!r}")
            import json as _json

            manifest = _json.loads(row_v["manifest_json"])
            root_row = self.ops().catalog.db.execute(
                "SELECT name FROM roots WHERE root_id=?", (row_v["root_id"],)
            ).fetchone()
            root_name = root_row["name"] if root_row else ""
            manifest_digest = row_v["manifest_digest"]
            total_bytes = int(row_v["size_bytes"])
            sha256 = manifest["sha256"]
            blob_path = self.ops().blobs.blob_path(sha256)
            if not blob_path.is_file():
                raise VanthRemoteProtocolError(
                    "INVALID_REQUEST", f"remote blob missing for version {version_id!r}: {sha256}"
                )
            staging_path = str(blob_path)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            existing = self.db.execute(
                "SELECT * FROM remote_transfers WHERE transfer_id=?", (transfer_id,)
            ).fetchone()
            if existing:
                if (
                    existing["direction"] != direction
                    or existing["root_name"] != root_name
                    or existing["manifest_digest"] != manifest_digest
                    or int(existing["total_bytes"]) != total_bytes
                    or existing["sha256"] != sha256
                ):
                    raise VanthRemoteProtocolError(
                        "PROTOCOL_REPLAY_MISMATCH",
                        "transfer_id was reused with different content identity",
                    )
                # A registered transfer from an OLD timeline can never resume:
                # its staging content belonged to a rolled-back world (review
                # rc14 P1-10).
                if int(existing["state_epoch"]) != epoch:
                    raise VanthRemoteProtocolError("INVALID_REQUEST", EPOCH_STOP_MESSAGE)
                # A crash can leave the ledger ahead of a lost staging file.
                # Reset the acknowledged prefix so the controller safely
                # retransmits from zero instead of skipping missing bytes.
                if direction == "push":
                    staged_existing = Path(existing["staging_path"])
                    size_ok = False
                    try:
                        info = staged_existing.lstat()
                        import stat as _stat

                        size_ok = _stat.S_ISREG(info.st_mode) and info.st_size >= int(existing["acked_offset"])
                    except OSError:
                        size_ok = False
                    if not size_ok:
                        staged_existing.parent.mkdir(parents=True, exist_ok=True)
                        with self._open_staging(staged_existing, write=True, create=True) as zero:
                            zero.truncate(0)
                        self.db.execute(
                            "UPDATE remote_transfers SET acked_offset=0, updated_at=? WHERE transfer_id=?",
                            (now, transfer_id),
                        )
                        existing = self.db.execute(
                            "SELECT * FROM remote_transfers WHERE transfer_id=?", (transfer_id,)
                        ).fetchone()
                result = {
                    "transfer_id": transfer_id,
                    "direction": direction,
                    "root_name": root_name,
                    "manifest_digest": manifest_digest,
                    "total_bytes": total_bytes,
                    "sha256": sha256,
                    "acked_offset": int(existing["acked_offset"]),
                    "resumed": True,
                    "state_epoch": int(self._epoch_fn()),
                }
                self.db.execute(
                    "UPDATE remote_transfers SET updated_at=? WHERE transfer_id=?", (now, transfer_id)
                )
                self.db.commit()
                return result
            self.db.execute(
                """
                INSERT INTO remote_transfers(transfer_id, direction, root_name, manifest_digest,
                  total_bytes, sha256, acked_offset, staging_path, state_epoch, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)
                """,
                (transfer_id, direction, root_name, manifest_digest, total_bytes, sha256,
                 staging_path, epoch, now, now),
            )
            if direction == "push":
                # A zero-byte push sends no chunks, so nothing would otherwise
                # create the staging file and completion would fail trying to
                # read it. Create (or reset) it up front for every push.
                with self._open_staging(Path(staging_path), write=True, create=True) as handle:
                    handle.truncate(0)
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return {
            "transfer_id": transfer_id,
            "direction": direction,
            "root_name": root_name,
            "manifest_digest": manifest_digest,
            "total_bytes": total_bytes,
            "sha256": sha256,
            "acked_offset": 0,
            "resumed": False,
            "state_epoch": epoch,
        }

    def chunk(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Handle one chunk frame in the transfer's registered direction.

        Push: verify strict offset continuity + chunk sha256, append to the
        staging file at exactly that offset, advance acked_offset in one tx.
        Pull: serve the requested byte window back to the controller.
        """
        transfer_id = payload["transfer_id"]
        row = self._get(transfer_id)
        if not row:
            raise VanthRemoteProtocolError("INVALID_REQUEST", f"unknown transfer: {transfer_id!r}")
        current_epoch = int(self._epoch_fn())
        if current_epoch != int(row["state_epoch"]):
            raise VanthRemoteProtocolError("INVALID_REQUEST", EPOCH_STOP_MESSAGE)
        if row["direction"] == "pull":
            return self._serve_chunk(row, payload)
        return self._receive_chunk(row, payload)

    def _receive_chunk(self, row, payload: dict[str, Any]) -> dict[str, Any]:
        data_b64 = payload.get("data_b64")
        chunk_sha = payload.get("sha256")
        if not isinstance(data_b64, str) or chunk_sha is None:
            raise VanthRemoteProtocolError(
                "INVALID_REQUEST", "push chunk requires data_b64 and sha256"
            )
        try:
            data = base64.b64decode(data_b64, validate=True)
        except Exception:
            raise VanthRemoteProtocolError("INVALID_REQUEST", "data_b64 is not valid base64") from None
        offset = int(payload["offset"])
        acked_offset = int(row["acked_offset"])
        if offset != acked_offset:
            raise VanthRemoteProtocolError(
                "INVALID_REQUEST",
                f"chunk offset discontinuity: expected_offset={acked_offset}",
            )
        if hashlib.sha256(data).hexdigest() != chunk_sha:
            raise VanthRemoteProtocolError(
                "INVALID_REQUEST", f"chunk sha256 mismatch at offset {offset}"
            )
        total = int(row["total_bytes"])
        if offset + len(data) > total:
            raise VanthRemoteProtocolError(
                "INVALID_REQUEST", "chunk exceeds declared total_bytes"
            )
        # Seek+write at exactly the acknowledged offset (never O_APPEND: the
        # write MUST land at `offset`). A crash between the write and the DB
        # update makes a resume rewrite the same bytes — never a duplicate
        # append. The open is no-follow (rc17 review F4).
        staged = Path(row["staging_path"])
        with self._open_staging(staged, write=True, create=True) as handle:
            handle.seek(offset)
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        self.db.execute("BEGIN IMMEDIATE")
        try:
            changed = self.db.execute(
                "UPDATE remote_transfers SET acked_offset=?, updated_at=? "
                "WHERE transfer_id=? AND acked_offset=?",
                (offset + len(data), now_iso(), row["transfer_id"], acked_offset),
            ).rowcount
            if not changed:
                raise ValueError("concurrent chunk writer lost the ack race")
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return {
            "transfer_id": row["transfer_id"],
            "acked_offset": offset + len(data),
            "state_epoch": int(self._epoch_fn()),
        }

    def _serve_chunk(self, row, payload: dict[str, Any]) -> dict[str, Any]:
        offset = int(payload["offset"])
        size = int(payload.get("size") or DEFAULT_CHUNK_BYTES)
        if size <= 0:
            raise VanthRemoteProtocolError("INVALID_REQUEST", "size must be > 0 for pull chunks")
        total = int(row["total_bytes"])
        # Pull chunks are replayable (review P1-14): the controller may request
        # any in-range offset — including one it already holds — because its
        # ledger is authoritative. Enforce range, not continuity.
        if offset < 0 or offset >= total:
            raise VanthRemoteProtocolError(
                "INVALID_REQUEST", f"chunk offset out of range: {offset} (total={total})"
            )
        with self._open_staging(Path(row["staging_path"]), write=False) as handle:
            handle.seek(offset)
            data = handle.read(min(size, max(0, total - offset)))
        # Pull chunks are REPLAYABLE: the durable resume point is NOT advanced
        # here. Advancing on serve meant a lost response left the remote
        # believing bytes were delivered that the controller never received,
        # and retrying the prior offset failed (review P1-14). The
        # controller-side ledger is authoritative for pull resume.
        return {
            "transfer_id": row["transfer_id"],
            "offset": offset,
            "size": len(data),
            "data_b64": base64.b64encode(data).decode("ascii"),
            "sha256": hashlib.sha256(data).hexdigest(),
            # Replayable chunks: the served window end is informational only;
            # the DURABLE remote offset advances solely via push-style chunk
            # acceptance, never on serve (review P1-14).
            "served_to": offset + len(data),
            "acked_offset": int(row["acked_offset"]),
            "state_epoch": int(self._epoch_fn()),
        }

    def complete(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Finalize a transfer after both sides agree on the whole-content sha.

        Push: the remote hashes ITS staged file, refuses unless size and hash
        match the registered identity, then publishes through the standard
        fenced durable operation (:meth:`ArtifactOperations.put_file`) under
        an idempotency key derived from the transfer id — replays collapse to
        the same version, so a duplicate completion can never publish twice.
        Pull: verifies source stability and returns the existing version id.
        """
        transfer_id = payload["transfer_id"]
        row = self._get(transfer_id)
        if not row:
            raise VanthRemoteProtocolError("INVALID_REQUEST", f"unknown transfer: {transfer_id!r}")
        current_epoch = int(self._epoch_fn())
        if current_epoch != int(row["state_epoch"]):
            raise VanthRemoteProtocolError("INVALID_REQUEST", EPOCH_STOP_MESSAGE)
        for field in ("root_name", "manifest_digest", "sha256"):
            if str(payload.get(field)) != str(row[field]):
                raise VanthRemoteProtocolError("INVALID_REQUEST", f"completion {field} does not match transfer")
        if int(payload.get("total_bytes")) != int(row["total_bytes"]):
            raise VanthRemoteProtocolError("INVALID_REQUEST", "completion total_bytes does not match transfer")
        # Read the staged content ONCE through the no-follow handle and hash
        # THE SAME buffer that gets published (rc18 review R3): a same-size
        # swap between a verify-open and a publish-reopen could otherwise
        # publish unverified bytes.
        staged = Path(row["staging_path"])
        with self._open_staging(staged, write=False) as handle:
            data = handle.read()
        actual_size = len(data)
        if actual_size != int(row["total_bytes"]):
            raise VanthRemoteProtocolError(
                "INVALID_REQUEST",
                f"incomplete transfer: expected {int(row['total_bytes'])} bytes, staged {actual_size}",
            )
        actual_sha = hashlib.sha256(data).hexdigest()
        expected_sha = str(payload.get("sha256") or row["sha256"])
        if actual_sha != expected_sha or actual_sha != row["sha256"]:
            # Same-length corruption wedged resume forever otherwise (rc19
            # N6): reset the remote ledger + staging so the controller
            # retransmits from zero; the expected_offset hint makes its
            # classify() treat this as a retry-offset instead of an abort.
            self.db.execute("BEGIN IMMEDIATE")
            try:
                self.db.execute(
                    "UPDATE remote_transfers SET acked_offset=0, updated_at=? WHERE transfer_id=?",
                    (now_iso(), transfer_id),
                )
                self.db.commit()
            except BaseException:
                self.db.rollback()
                raise
            try:
                with self._open_staging(staged, write=True, create=False) as zero:
                    zero.truncate(0)
            except OSError:
                pass
            raise VanthRemoteProtocolError(
                "INVALID_REQUEST",
                f"whole-content sha256 mismatch: expected {row['sha256']}, remote hashed {actual_sha}; "
                f"transfer reset for retransmission (expected_offset=0)",
            )
        if row["direction"] == "pull":
            version_id = payload.get("version_id")
            if not isinstance(version_id, str) or not version_id:
                raise VanthRemoteProtocolError(
                    "INVALID_REQUEST", "pull completion requires version_id"
                )
            version_id = str(version_id)
            vrow = self.ops().catalog.db.execute(
                "SELECT v.version_id FROM versions v JOIN roots r ON r.root_id=v.root_id "
                "WHERE v.version_id=? AND v.manifest_digest=? AND r.name=?",
                (version_id, row["manifest_digest"], row["root_name"]),
            ).fetchone()
            if not vrow:
                raise VanthRemoteProtocolError(
                    "INVALID_REQUEST", "requested remote version/root/manifest binding changed"
                )
            return {
                "transfer_id": transfer_id,
                "version_id": vrow["version_id"],
                "root_name": row["root_name"],
                "manifest_digest": row["manifest_digest"],
                "sha256": actual_sha,
                "total_bytes": int(row["total_bytes"]),
                "verified": True,
                "state_epoch": current_epoch,
            }
        # Publication rides the EXISTING fenced durable-op pattern: put_file
        # begins/replays by this key, claims with lease+generation, and only
        # the live claim can commit the version row — so a duplicated
        # completion (crash between publish and ack) collapses onto the same
        # immutable version.
        # Epoch fence BEFORE publication (review rc14 P1-10), held ACROSS the
        # publication transaction via the shared store lock (rc17 F6, rc18 R1):
        # put_file's guard re-checks inside its commit, and a concurrent bump
        # is blocked until we release. `data` is the SAME verified buffer.
        with self._publication_fence():
            now_epoch = int(self._epoch_fn())
            if now_epoch != current_epoch or now_epoch != int(row["state_epoch"]):
                raise VanthRemoteProtocolError("INVALID_REQUEST", EPOCH_STOP_MESSAGE)
            try:
                result = self.ops().put_file(
                    row["root_name"], data=data, idempotency_key="xfer-" + transfer_id,
                    publish_guard=lambda: int(self._epoch_fn()) == current_epoch,
                )
            except ValueError as exc:
                if "publication fence changed" in str(exc):
                    raise VanthRemoteProtocolError("INVALID_REQUEST", EPOCH_STOP_MESSAGE) from None
                raise
        # Full binding at the fenced commit (review P2-5): the published
        # version must match the REGISTERED identity on every immutable
        # field — sha, size, manifest digest, root name — not just the sha.
        vrow = self.ops().catalog.db.execute(
            "SELECT manifest_json, size_bytes FROM versions WHERE version_id=?",
            (result["version_id"],),
        ).fetchone()
        if vrow is None:
            raise VanthRemoteProtocolError("INVALID_REQUEST", "published version row missing")
        manifest = json.loads(vrow["manifest_json"])
        mismatches = []
        if result.get("sha256") != row["sha256"]:
            mismatches.append("sha256")
        if int(vrow["size_bytes"]) != int(row["total_bytes"]):
            mismatches.append(f"total_bytes ({vrow['size_bytes']} != {row['total_bytes']})")
        if row["manifest_digest"]:
            from ..artifacts.manifest import manifest_digest as _md

            if _md(manifest) != row["manifest_digest"]:
                mismatches.append("manifest_digest")
        rrow = self.ops().catalog.db.execute(
            "SELECT 1 FROM roots WHERE root_id=? AND name=?", (result.get("root_id"), row["root_name"])
        ).fetchone()
        if not rrow:
            mismatches.append(f"root_name ({row['root_name']!r})")
        if mismatches:
            raise VanthRemoteProtocolError(
                "INVALID_REQUEST", "published version binding mismatch: " + ", ".join(mismatches)
            )
        if int(self._epoch_fn()) != current_epoch:
            raise VanthRemoteProtocolError("INVALID_REQUEST", EPOCH_STOP_MESSAGE)
        # The staged file is intentionally retained: a replayed completion
        # (crash after publish, before the controller's ack) must be able to
        # re-verify and collapse onto the same version. Reclaiming staging
        # files is GC territory (deferred).
        return {
            "transfer_id": transfer_id,
            "version_id": result["version_id"],
            "root_name": row["root_name"],
            "manifest_digest": row["manifest_digest"],
            "deduplicated": bool(result.get("deduplicated")),
            "sha256": row["sha256"],
            "total_bytes": int(row["total_bytes"]),
            "state_epoch": current_epoch,
        }

    # ------------------------------------------------------------------

    def _get(self, transfer_id: str):
        return self.db.execute(
            "SELECT * FROM remote_transfers WHERE transfer_id=?", (transfer_id,)
        ).fetchone()


class RemoteArtifactBroker:
    """Controller-side broker for push/pull artifact transfers (Phase 9).

    Opens ONE bulk session per attempt via ``control.transport.open_session``,
    streams sequentially, persists every acknowledged offset durably before
    sending the next chunk, and stops permanently when the remote's state
    epoch changes mid-transfer.
    """

    def __init__(self, control: Any, ops: Any) -> None:
        self.control = control
        self.ops = ops
        db = ops.catalog.db
        db.executescript(CONTROLLER_TRANSFER_DDL)
        _ensure_controller_schema(db)
        db.commit()
        self.db = db

    @property
    def chunk_bytes(self) -> int:
        try:
            value = int(os.environ.get("VANTH_TRANSFER_CHUNK_BYTES", str(DEFAULT_CHUNK_BYTES)))
        except ValueError:
            value = DEFAULT_CHUNK_BYTES
        return max(1024, value)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def _pull_staging_path(self, transfer_id: str) -> Path:
        """Pull staging lives ONLY inside Vanth-owned storage (rc17 F4)."""
        base = Path(self.control.home) / "remote-pull-staging" if getattr(self.control, "home", None) \
            else Path("remote-pull-staging")
        return base / f"{transfer_id}.part"

    def _reconcile_pull_resume(self, transfer_id: str, record, total: int) -> int:
        """Reconcile ledger progress with staging-file reality (rc17 F5).

        The controller's durable ``acked_offset`` is the resume point. When
        the local staging file is missing or truncated BELOW it the two are
        inconsistent: reset BOTH to zero instead of extending a zero-filled
        prefix that could never pass verification."""
        ledger_offset = int(record["acked_offset"] or 0)
        if ledger_offset <= 0:
            return 0
        offset = min(ledger_offset, total)
        staging = self._pull_staging_path(transfer_id)
        consistent = False
        try:
            if staging.is_file() and staging.stat().st_size >= offset:
                consistent = True
        except OSError:
            consistent = False
        if not consistent:
            try:
                staging.unlink(missing_ok=True)
            except OSError:
                pass
            self._persist_progress(transfer_id, record["op_claim_token"], acked_offset=0)
            return 0
        return offset

    def push_blob(self, remote_id: str, local_version_id: str, *, idempotency_key: str | None = None) -> dict[str, Any]:
        """Brokered PUBLICATION of a local managed version to a paired remote."""
        key = idempotency_key or _mint_key("push")
        if not _valid_key(key):
            raise VanthRemoteProtocolError(
                "INVALID_REQUEST", "idempotency_key must be 8..128 chars in [A-Za-z0-9_-]"
            )
        verify = self.ops.verify(local_version_id)
        if not verify.get("ok"):
            raise TransferAborted(
                f"source version failed verification before transfer: {local_version_id}"
            )
        vrow = self.ops.catalog.db.execute(
            "SELECT * FROM versions WHERE version_id=?", (local_version_id,)
        ).fetchone()
        if not vrow:
            raise ValueError(f"Unknown version_id: {local_version_id}")
        import json as _json

        manifest = _json.loads(vrow["manifest_json"])
        if manifest.get("kind") != "file":
            raise ValueError("push_blob supports kind='file' versions only")
        sha256 = manifest["sha256"]
        total = int(vrow["size_bytes"])
        mdigest = vrow["manifest_digest"]
        root_row = self.ops.catalog.db.execute(
            "SELECT name FROM roots WHERE root_id=?", (vrow["root_id"],)
        ).fetchone()
        root_name = root_row["name"] if root_row else local_version_id
        if not (self.ops.blobs.has_blob(sha256) and self.ops.blobs.verify_blob(sha256)):
            raise TransferAborted(f"source blob missing or corrupt: {sha256}")

        # Identity binds REMOTE + direction + caller key + immutable version:
        # the same key/version against a DIFFERENT remote (or destination for
        # pull) must never replay the first ledger entry (review P1-15).
        transfer_id = "xfr_" + hashlib.sha256(
            f"push:{remote_id}:{key}:{local_version_id}".encode("utf-8")
        ).hexdigest()[:32]
        record = self._begin_transfer(
            transfer_id, remote_id, "push", root_name, mdigest, total, sha256, key
        )
        if record["status"] == "completed":
            stored = json.loads(record["result_json"]) if record["result_json"] else {}
            return {**stored, "replayed": True}
        if record["status"] == "failed":
            raise TransferAborted(record["error"] or "transfer previously aborted")

        session, response = self._open_and_init(remote_id, transfer_id, {
            "transfer_id": transfer_id,
            "direction": "push",
            "root_name": root_name,
            "manifest_digest": mdigest,
            "total_bytes": total,
            "sha256": sha256,
        }, record)
        init_result = response["result"]
        epoch = int(init_result["state_epoch"])
        offset = min(int(init_result["acked_offset"]), total)
        # The remote's acknowledged offset is authoritative: adopt it so we
        # never resend bytes it already holds.
        self._persist_progress(transfer_id, record["op_claim_token"],
                               acked_offset=offset, state_epoch=epoch)

        sent_this_attempt = 0
        with self.ops.blobs.blob_path(sha256).open("rb") as blob:
            while offset < total:
                self._check_epoch_record(transfer_id, epoch)
                blob.seek(offset)
                chunk = blob.read(min(self.chunk_bytes, total - offset))
                if not chunk:
                    break
                frame = self._frame("artifact.blob_chunk", {
                    "transfer_id": transfer_id,
                    "offset": offset,
                    "data_b64": base64.b64encode(chunk).decode("ascii"),
                    "sha256": hashlib.sha256(chunk).hexdigest(),
                })
                resp = self._exchange(session, frame)
                outcome = self._classify(resp, epoch, transfer_id)
                if outcome == "retry-offset":
                    match = _EXPECTED_OFFSET_RE.search(resp.get("message") or "")
                    expected = int(match.group(1)) if match else offset
                    offset = min(expected, total)
                    self._persist_progress(transfer_id, record["op_claim_token"], acked_offset=offset)
                    continue
                if outcome == "epoch-stop":
                    self._mark_aborted(transfer_id, record["op_claim_token"], EPOCH_STOP_MESSAGE)
                    raise TransferAborted(EPOCH_STOP_MESSAGE)
                if outcome == "error":
                    message = str(resp.get("message") or "chunk rejected")
                    self._mark_failed(transfer_id, record["op_claim_token"], message)
                    raise TransferAborted(message)
                result = resp["result"]
                # Strict binding (Sol review/rc18 R8): the ack must name THIS
                # transfer, carry its own epoch, and acknowledge EXACTLY the
                # bytes just sent — never a defaulted or partial ack.
                if str(result.get("transfer_id") or "") != transfer_id:
                    self._mark_failed(transfer_id, record["op_claim_token"],
                                      "chunk ack does not bind to this transfer")
                    raise TransferAborted("chunk ack does not bind to this transfer")
                for required_field in ("state_epoch", "acked_offset"):
                    if required_field not in result:
                        self._mark_failed(transfer_id, record["op_claim_token"],
                                          f"chunk ack missing {required_field}")
                        raise TransferAborted(f"chunk ack missing {required_field}")
                new_epoch = int(result["state_epoch"])
                if new_epoch != epoch:
                    self._mark_aborted(transfer_id, record["op_claim_token"], EPOCH_STOP_MESSAGE)
                    raise TransferAborted(EPOCH_STOP_MESSAGE)
                acked = int(result["acked_offset"])
                if acked != offset + len(chunk):
                    raise TransferInterrupted(
                        transfer_id, offset,
                        f"remote ack {acked} does not match the sent window end {offset + len(chunk)}",
                    )
                offset = acked
                sent_this_attempt += 1
                self._persist_progress(transfer_id, record["op_claim_token"], acked_offset=offset)

        if offset != total:
            self._persist_progress(transfer_id, record["op_claim_token"], acked_offset=offset)
            raise TransferInterrupted(transfer_id, offset)

        complete_frame = self._frame("artifact.transfer_complete", {
            "transfer_id": transfer_id, "root_name": root_name,
            "manifest_digest": mdigest, "total_bytes": total, "sha256": sha256,
        })
        resp = self._exchange(session, complete_frame)
        outcome = self._classify(resp, epoch, transfer_id)
        if outcome == "epoch-stop":
            self._mark_aborted(transfer_id, record["op_claim_token"], EPOCH_STOP_MESSAGE)
            raise TransferAborted(EPOCH_STOP_MESSAGE)
        if resp.get("kind") != "response":
            raise TransferInterrupted(transfer_id, offset, str(resp.get("message") or "completion failed"))
        cresp = resp["result"]
        # Strict binding (Sol review): completion must answer THIS transfer
        # with the SAME content identity and epoch we registered. Missing
        # fields are a mismatch — never silently defaulted (rc17 P2).
        if str(cresp.get("transfer_id") or "") != transfer_id:
            raise TransferInterrupted(transfer_id, offset, "completion response does not bind to this transfer")
        for required_field in ("state_epoch", "sha256", "total_bytes", "version_id"):
            if required_field not in cresp:
                raise TransferInterrupted(
                    transfer_id, offset, f"completion response missing {required_field}"
                )
        if int(cresp["state_epoch"]) != epoch:
            self._mark_aborted(transfer_id, record["op_claim_token"], EPOCH_STOP_MESSAGE)
            raise TransferAborted(EPOCH_STOP_MESSAGE)
        if str(cresp["sha256"]) != sha256 or int(cresp["total_bytes"]) != total:
            raise TransferInterrupted(transfer_id, offset, "completion response content identity mismatch")
        if str(cresp.get("root_name") or "") != root_name or str(cresp.get("manifest_digest") or "") != mdigest:
            raise TransferInterrupted(transfer_id, offset, "completion response version identity mismatch")
        remote_version_id = cresp["version_id"]

        # Source stability at the END: the content-addressed source blob must
        # be byte-for-byte unchanged after the transfer too.
        if not self.ops.blobs.verify_blob(sha256):
            self._mark_failed(transfer_id, record["op_claim_token"], "source blob changed during transfer")
            raise TransferAborted("source blob changed during transfer")

        final = {
            "transfer_id": transfer_id,
            "direction": "push",
            "remote_id": remote_id,
            "version_id": remote_version_id,
            "sha256": sha256,
            "manifest_digest": mdigest,
            "total_bytes": total,
            "bytes_sent_total": total,
            "chunks_sent_this_attempt": sent_this_attempt,
            "completed": True,
        }
        self._mark_completed(transfer_id, record["op_claim_token"], final)
        return final

    def pull_blob(self, remote_id: str, remote_version_id: str, dest_path: str | os.PathLike[str],
                  *, idempotency_key: str | None = None,
                  _allow_reset_retry: bool = True) -> dict[str, Any]:
        """Brokered MATERIALIZATION of a remote version onto this machine."""
        dest = Path(dest_path)
        key = idempotency_key or _mint_key("pull")
        if not _valid_key(key):
            raise VanthRemoteProtocolError(
                "INVALID_REQUEST", "idempotency_key must be 8..128 chars in [A-Za-z0-9_-]"
            )
        # Destination identity is part of the transfer identity (review P1-15).
        import hashlib as _h

        dest_token = _h.sha256(str(dest.resolve()).encode("utf-8")).hexdigest()[:16]
        transfer_id = "xfr_" + hashlib.sha256(
            f"pull:{remote_id}:{key}:{remote_version_id}:{dest_token}".encode("utf-8")
        ).hexdigest()[:32]
        record = self._begin_transfer(
            transfer_id, remote_id, "pull", "", "", 0, "0" * 64, key, content_known=False
        )
        if record["status"] == "completed":
            stored = json.loads(record["result_json"]) if record["result_json"] else {}
            return {**stored, "replayed": True}
        if record["status"] == "failed":
            raise TransferAborted(record["error"] or "transfer previously aborted")

        session, response = self._open_and_init(remote_id, transfer_id, {
            "transfer_id": transfer_id,
            "direction": "pull",
            "version_id": remote_version_id,
        }, record)
        init_result = response["result"]
        total = int(init_result["total_bytes"])
        sha256 = init_result["sha256"]
        root_name = init_result["root_name"]
        mdigest = init_result["manifest_digest"]
        epoch = int(init_result["state_epoch"])
        # Pull resume uses the CONTROLLER's durable progress (rc17 review F5):
        # the remote's pull offset intentionally stays 0 (serve never advances
        # it), and adopting it here used to discard verified local bytes.
        offset = self._reconcile_pull_resume(transfer_id, record, total)
        self._persist_progress(transfer_id, record["op_claim_token"], acked_offset=offset,
                               state_epoch=epoch, root_name=root_name,
                               manifest_digest=mdigest, total_bytes=total, sha256=sha256)

        # Stage EXCLUSIVELY inside a Vanth-owned directory (rc17 review F4):
        # writing `.pulling-*` next to an arbitrary destination let a swapped
        # parent redirect the write through a symlink. Ancestors are swept at
        # creation and the leaf open is fully validated (rc18 R4/rc19 N3).
        staging = self._pull_staging_path(transfer_id)
        _verify_staging_chain(staging.parent)
        staging.parent.mkdir(parents=True, exist_ok=True)
        _verify_staging_chain(staging.parent)
        out = _stage_open_file(staging, write=True, create=True)
        try:
            out.truncate(offset)
            # On resume the prefix was already verified chunk-by-chunk;
            # re-hash it so the final whole-content check stays exact.
            received_hash = hashlib.sha256()
            out.seek(0)
            for block in iter(lambda: out.read(1024 * 1024), b""):
                received_hash.update(block)
            while offset < total:
                self._check_epoch_record(transfer_id, epoch)
                frame = self._frame("artifact.blob_chunk", {
                    "transfer_id": transfer_id,
                    "offset": offset,
                    "data_b64": "",
                    "sha256": hashlib.sha256(b"").hexdigest(),
                    "size": self.chunk_bytes,
                })
                resp = self._exchange(session, frame)
                outcome = self._classify(resp, epoch, transfer_id)
                if outcome == "epoch-stop":
                    self._mark_aborted(transfer_id, record["op_claim_token"], EPOCH_STOP_MESSAGE)
                    raise TransferAborted(EPOCH_STOP_MESSAGE)
                if resp.get("kind") != "response":
                    raise TransferInterrupted(transfer_id, offset, str(resp.get("message")))
                result = resp["result"]
                # Strict binding (Sol review/rc18 R8): the served window must
                # name THIS transfer, start at the offset we asked for, and
                # stay in range. Missing fields are protocol violations.
                if str(result.get("transfer_id") or "") != transfer_id:
                    self._mark_failed(transfer_id, record["op_claim_token"],
                                      "pull chunk does not bind to this transfer")
                    raise TransferAborted("pull chunk does not bind to this transfer")
                for required_field in ("offset", "state_epoch", "data_b64", "sha256", "served_to"):
                    if required_field not in result:
                        self._mark_failed(transfer_id, record["op_claim_token"],
                                          f"pull chunk missing {required_field}")
                        raise TransferAborted(f"pull chunk missing {required_field}")
                if int(result["offset"]) != offset:
                    self._mark_failed(transfer_id, record["op_claim_token"],
                                      "pull chunk served a different offset than requested")
                    raise TransferAborted("pull chunk served a different offset than requested")
                new_epoch = int(result["state_epoch"])
                if new_epoch != epoch:
                    self._mark_aborted(transfer_id, record["op_claim_token"], EPOCH_STOP_MESSAGE)
                    raise TransferAborted(EPOCH_STOP_MESSAGE)
                data = base64.b64decode(result["data_b64"], validate=True)
                if hashlib.sha256(data).hexdigest() != result["sha256"]:
                    self._mark_failed(transfer_id, record["op_claim_token"],
                                      f"pull chunk sha mismatch at offset {offset}")
                    raise TransferAborted(f"pull chunk sha mismatch at offset {offset}")
                out.seek(offset)
                out.write(data)
                received_hash.update(data)
                # Pull chunks are replayable: advance locally by the
                # served window; the remote's durable offset does not
                # move on serve (review P1-14).
                served_to = int(result["served_to"])
                if len(data) == 0 or served_to <= offset:
                    # Zero-length windows loop forever otherwise (rc19 N2).
                    self._mark_failed(transfer_id, record["op_claim_token"],
                                      f"pull chunk made no progress at offset {offset}")
                    raise TransferAborted(f"pull chunk made no progress at offset {offset}")
                if served_to > total or served_to != offset + len(data):
                    raise TransferInterrupted(transfer_id, offset, "served window out of range")
                offset = served_to
                self._persist_progress(transfer_id, record["op_claim_token"], acked_offset=offset)
            if offset != total:
                raise TransferInterrupted(transfer_id, offset, "pull did not assemble the full content")
        finally:
            out.close()
            # Keep the staging file on INCOMPLETE pulls (review rc14 P1-10):
            # the ledger's acknowledged offset refers to it; deleting forced
            # the retry to zero-fill a "completed" prefix and fail verification.
            # Only an interrupted-before-first-byte transfer has nothing to keep;
            # a zero-byte transfer (offset == total == 0) is COMPLETE and its
            # (empty) staging file is the content that must still be verified.
            if offset == 0 and total > 0:
                try:
                    staging.unlink(missing_ok=True)
                except OSError:
                    pass

        complete_frame = self._frame("artifact.transfer_complete", {
            "transfer_id": transfer_id, "root_name": root_name,
            "manifest_digest": mdigest, "total_bytes": total, "sha256": sha256,
            "version_id": remote_version_id,
        })
        resp = self._exchange(session, complete_frame)
        if resp.get("kind") != "response":
            raise TransferInterrupted(transfer_id, offset, str(resp.get("message")))
        # Pull completion binding (review rc14 P1-10): the remote's final
        # answer must agree on epoch, SHA, and byte count before local
        # publication. Missing fields are a mismatch — never defaulted
        # (rc17 P2) — and the version/root/manifest identity must match what
        # we pulled.
        cresult = resp.get("result") or {}
        if str(cresult.get("transfer_id") or "") != transfer_id:
            raise TransferInterrupted(transfer_id, offset, "completion response does not bind to this transfer")
        for required_field in ("state_epoch", "sha256", "total_bytes", "version_id",
                               "root_name", "manifest_digest"):
            if required_field not in cresult:
                raise TransferInterrupted(
                    transfer_id, offset, f"completion response missing {required_field}"
                )
        if int(cresult["state_epoch"]) != epoch:
            self._mark_aborted(transfer_id, record["op_claim_token"], EPOCH_STOP_MESSAGE)
            raise TransferAborted(EPOCH_STOP_MESSAGE)
        if str(cresult["sha256"]) != sha256:
            raise TransferInterrupted(transfer_id, offset, "pull completion sha mismatch")
        if int(cresult["total_bytes"]) != total:
            raise TransferInterrupted(transfer_id, offset, "pull completion byte-count mismatch")
        if str(cresult["version_id"]) != remote_version_id \
                or str(cresult["root_name"]) != root_name \
                or str(cresult["manifest_digest"]) != mdigest:
            raise TransferInterrupted(
                transfer_id, offset, "pull completion version/root/manifest identity mismatch"
            )

        # Materialize locally through the standard durable-op pattern, then
        # write dest_path atomically via materialize(). Derived op keys are
        # scoped by the destination token (review rc14 P1-10b): two pulls
        # sharing one caller key but landing on different destinations must
        # not collide in the controller's op ledger.
        # Read the assembled staging ONCE through a fresh no-follow handle
        # and verify THE buffer that gets published (rc18 review R3).
        with _open_local_nofollow(staging) as handle:
            local_bytes = handle.read()
        if len(local_bytes) != total or hashlib.sha256(local_bytes).hexdigest() != sha256:
            # Same-length corruption must not wedge resume forever (rc18 R7):
            # reset ledger + staging and restart ONCE from zero.
            try:
                staging.unlink(missing_ok=True)
            except OSError:
                pass
            self._persist_progress(transfer_id, record["op_claim_token"], acked_offset=0)
            if _allow_reset_retry:
                return self.pull_blob(remote_id, remote_version_id, dest_path,
                                      idempotency_key=key, _allow_reset_retry=False)
            raise TransferInterrupted(
                transfer_id, 0, "assembled staging failed verification; transfer state reset to zero"
            )
        put = self.ops.put_file(root_name, data=local_bytes,
                                idempotency_key=f"{key}-pub-{dest_token}")
        mat = self.ops.materialize(put["version_id"], dest, overwrite=True,
                                   idempotency_key=f"{key}-mat-{dest_token}")
        try:
            staging.unlink(missing_ok=True)
        except OSError:
            pass
        final = {
            "transfer_id": transfer_id,
            "direction": "pull",
            "remote_id": remote_id,
            "remote_version_id": remote_version_id,
            "version_id": put["version_id"],
            "dest_path": str(mat["dest_path"]),
            "sha256": sha256,
            "total_bytes": total,
            "completed": True,
        }
        self._mark_completed(transfer_id, record["op_claim_token"], final)
        return final

    # ------------------------------------------------------------------
    # Session/frame plumbing
    # ------------------------------------------------------------------

    def _session(self, remote_row: dict[str, Any]) -> Any:
        return self.control.transport.open_session(remote_row, home=self.control.home)

    def _open_and_init(self, remote_id: str, transfer_id: str, init_payload: dict[str, Any],
                       record) -> tuple[Any, dict[str, Any]]:
        remote_row = self.control.store.get_remote(remote_id)
        session = self._session(remote_row)
        if session is None:
            raise TransferInterrupted(transfer_id, int(record["acked_offset"]), "cannot open transport session")
        resp = self._exchange(session, self._frame("artifact.transfer_init", init_payload))
        if resp.get("kind") != "response":
            message = str(resp.get("message") or "transfer_init failed")
            if "epoch changed" in message:
                self._mark_aborted(transfer_id, record["op_claim_token"], EPOCH_STOP_MESSAGE)
                raise TransferAborted(EPOCH_STOP_MESSAGE)
            raise TransferInterrupted(transfer_id, int(record["acked_offset"]), message)
        # Strict result binding (Sol review): the init answer must name THIS
        # transfer and carry an epoch.
        rresult = resp["result"]
        if str(rresult.get("transfer_id") or "") != transfer_id:
            self._mark_aborted(transfer_id, record["op_claim_token"],
                               "transfer_init response does not bind to this transfer")
            raise TransferAborted("transfer_init response does not bind to this transfer")
        if record["state_epoch"] is not None and int(resp["result"]["state_epoch"]) != int(record["state_epoch"]):
            self._mark_aborted(transfer_id, record["op_claim_token"], EPOCH_STOP_MESSAGE)
            raise TransferAborted(EPOCH_STOP_MESSAGE)
        return session, resp

    @staticmethod
    def _frame(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        key = _mint_key("trn")
        return {
            "version": "1",
            "kind": "request",
            "request_id": "req_" + secrets.token_hex(16),
            "idempotency_key": key,
            "method": method,
            "payload": payload,
            "digest": request_digest(method, payload, key),
            "sent_at": now_iso(),
        }

    @staticmethod
    def _exchange(session: Any, frame: dict[str, Any]) -> dict[str, Any]:
        line = session.exchange(encode_frame(frame))
        if not line:
            raise ConnectionError("transport returned no response frame")
        resp = decode_frame(line)
        # Strict response binding (Sol review): response AND error frames
        # must echo the request_id and method of the frame that produced
        # them — an unbound frame is a corrupted/foreign exchange (rc17 P2).
        if resp.get("kind") in ("response", "error"):
            if resp.get("request_id") != frame.get("request_id") or resp.get("method") != frame.get("method"):
                raise ConnectionError(
                    "response does not bind to its request (request_id/method mismatch)"
                )
        return resp

    @staticmethod
    def _classify(resp: dict[str, Any], known_epoch: int, transfer_id: str) -> str:
        """Bucket a response frame: ok / retry-offset / epoch-stop."""
        if resp.get("kind") == "error":
            message = str(resp.get("message") or "")
            if "epoch changed" in message:
                return "epoch-stop"
            if _EXPECTED_OFFSET_RE.search(message):
                return "retry-offset"
            return "error"
        return "ok"

    # ------------------------------------------------------------------
    # Durable controller-side ledger (fenced by op_claim_token)
    # ------------------------------------------------------------------

    def _begin_transfer(self, transfer_id: str, remote_id: str, direction: str, root_name: str,
                        manifest_digest: str, total: int, sha256: str, key: str,
                        *, content_known: bool = True) -> dict[str, Any]:
        now = now_iso()
        token = secrets.token_hex(16)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            existing = self.db.execute(
                "SELECT * FROM controller_transfers WHERE transfer_id=?", (transfer_id,)
            ).fetchone()
            if existing is None:
                self.db.execute(
                    """
                    INSERT INTO controller_transfers(transfer_id, remote_id, direction, root_name,
                      manifest_digest, total_bytes, sha256, acked_offset, idempotency_key, op_claim_token,
                      status, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, 'active', ?, ?)
                    """,
                    (transfer_id, remote_id, direction, root_name, manifest_digest, total, sha256,
                     key, token, now, now),
                )
            # Takeover: this process now owns the fence token (single-writer
            # assumption per transfer; the token gate below fences stale
            # writers from committing progress after a takeover).
            self.db.execute(
                "UPDATE controller_transfers SET op_claim_token=?, status='active', error=NULL, updated_at=? "
                "WHERE transfer_id=? AND status IN ('active','interrupted')",
                (token, now, transfer_id),
            )
            row = self.db.execute(
                "SELECT * FROM controller_transfers WHERE transfer_id=?", (transfer_id,)
            ).fetchone()
            # Binding verification: catches legacy-schema takeovers of a
            # foreign row. Pull begins with placeholders (content unknown
            # until transfer_init), so its retries skip content checks —
            # direction/remote always verified.
            if (
                row is None
                or row["remote_id"] != remote_id
                or row["direction"] != direction
            ):
                raise ValueError(
                    f"transfer {transfer_id} ledger row does not match the requested "
                    "remote/destination/content binding"
                )
            if content_known:
                for column, value in (("root_name", root_name), ("manifest_digest", manifest_digest),
                                      ("sha256", sha256)):
                    if value and row[column] not in ("", None) and row[column] != value:
                        raise ValueError(
                            f"transfer {transfer_id} ledger row does not match the requested "
                            f"{column} binding"
                        )
                if total and int(row["total_bytes"]) not in (0, int(total)):
                    raise ValueError(
                        f"transfer {transfer_id} ledger row does not match the requested total_bytes"
                    )
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return dict(row)

    def _persist_progress(self, transfer_id: str, token: str | None, *, acked_offset: int,
                          state_epoch: int | None = None, **identity: Any) -> None:
        sets = ["acked_offset=?", "updated_at=?"]
        params: list[Any] = [int(acked_offset), now_iso()]
        if state_epoch is not None:
            sets.append("state_epoch=?")
            params.append(int(state_epoch))
        for column in ("root_name", "manifest_digest", "total_bytes", "sha256"):
            if column in identity:
                sets.append(f"{column}=?")
                params.append(identity[column])
        params.extend([transfer_id, token])
        self.db.execute("BEGIN IMMEDIATE")
        try:
            changed = self.db.execute(
                f"UPDATE controller_transfers SET {', '.join(sets)} "
                "WHERE transfer_id=? AND op_claim_token=?",
                params,
            ).rowcount
            if not changed:
                raise ValueError(f"stale worker: transfer {transfer_id} claim lost")
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def _check_epoch_record(self, transfer_id: str, known_epoch: int) -> None:
        """Stop BEFORE sending when the recorded epoch was already superseded."""
        row = self.db.execute(
            "SELECT status, error FROM controller_transfers WHERE transfer_id=?", (transfer_id,)
        ).fetchone()
        if row and row["status"] == "failed":
            raise TransferAborted(row["error"] or "transfer aborted")

    def _mark_completed(self, transfer_id: str, token: str | None, result: dict[str, Any]) -> None:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            changed = self.db.execute(
                "UPDATE controller_transfers SET status='completed', result_json=?, acked_offset=?, "
                "updated_at=?, op_claim_token=NULL WHERE transfer_id=? AND op_claim_token=? AND status='active'",
                (json.dumps(result, separators=(",", ":")), int(result.get("total_bytes", 0)),
                 now_iso(), transfer_id, token),
            ).rowcount
            if not changed:
                raise ValueError(f"stale worker: transfer {transfer_id} cannot be completed with this claim")
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def _mark_aborted(self, transfer_id: str, token: str | None, error: str) -> None:
        self._mark_failed(transfer_id, token, error)

    def _mark_failed(self, transfer_id: str, token: str | None, error: str) -> None:
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute(
                "UPDATE controller_transfers SET status='failed', error=?, updated_at=? "
                "WHERE transfer_id=? AND (op_claim_token=? OR ? IS NULL)",
                (error, now_iso(), transfer_id, token, token),
            )
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
