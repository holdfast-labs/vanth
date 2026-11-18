"""Audit regressions: error registry, trigger cancellation, empty transfers,
canonical key ordering, and transaction rollback on write failure."""

from __future__ import annotations

import json
import sqlite3
import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_remote_ops import make_remote, request_frame, seed_job  # noqa: E402

from vanth.artifacts.catalog import open_catalog
from vanth.artifacts.local_store import LocalBlobStore, default_store_root
from vanth.artifacts.operations import ArtifactOperations
from vanth.remote.control import RemoteControl
from vanth.remote.journal import RequestJournal
from vanth.remote.protocol import canonical_json, decode_frame
from vanth.remote.remote import RemoteJobManager
from vanth.remote.store import RemoteOperationStore, RemoteStore
from vanth.remote.transfer import RemoteArtifactBroker


def connect(path):
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=30000")
    return db


class _FailingDb:
    """Connection proxy that injects one disk-full style failure."""

    def __init__(self, real, *, sql_prefix: str | None = None, commit: bool = False) -> None:
        self.real = real
        self.sql_prefix = sql_prefix.upper() if sql_prefix else None
        self.fail_commit = commit
        self.failed = False
        self.rolled_back = False

    def execute(self, sql, params=()):
        if (
            self.sql_prefix is not None
            and not self.failed
            and sql.lstrip().upper().startswith(self.sql_prefix)
        ):
            self.failed = True
            raise sqlite3.OperationalError("database or disk is full")
        return self.real.execute(sql, params)

    def commit(self):
        if self.fail_commit and not self.failed:
            self.failed = True
            raise sqlite3.OperationalError("database or disk is full")
        return self.real.commit()

    def rollback(self):
        self.rolled_back = True
        return self.real.rollback()


# ---------------------------------------------------------------------------
# Error registry: codes the remote actually emits must decode
# ---------------------------------------------------------------------------


def test_instance_mismatch_error_frame_is_registry_valid(tmp_path):
    remote, store, _fake = make_remote(tmp_path)
    store.db.execute("UPDATE remote_state SET instance_id='other-instance' WHERE id=1")
    store.db.commit()

    response = remote.handle_request(request_frame(key="key-audit-inst-01"))
    assert response["kind"] == "error"
    assert response["code"] == "INSTANCE_ID_MISMATCH"
    # The controller validates every decoded error code; an unregistered code
    # turned a clear auth/identity failure into PROTOCOL_MALFORMED (lost).
    decoded = decode_frame(json.dumps(response, separators=(",", ":")))
    assert decoded["code"] == "INSTANCE_ID_MISMATCH"


def test_snapshot_expired_error_frame_is_registry_valid(tmp_path):
    remote, _store, _fake = make_remote(tmp_path)
    frame = request_frame(method="job.snapshot", payload={"cursor": {"snapshot_id": "snap-missing"}},
                          key="key-audit-snap-01")
    response = remote.handle_snapshot_request(frame, frame["payload"])
    assert response["kind"] == "error"
    assert response["code"] == "SNAPSHOT_EXPIRED"
    decoded = decode_frame(json.dumps(response, separators=(",", ":")))
    assert decoded["code"] == "SNAPSHOT_EXPIRED"


# ---------------------------------------------------------------------------
# Trigger cancellation must not leave a nested transaction / queued op
# ---------------------------------------------------------------------------


def test_trigger_cancel_marks_operation_and_leaves_connection_usable(tmp_path):
    remote, store, _fake = make_remote(tmp_path)
    seed_job(store.db, "job_parent01", status="completed")

    payload = {
        "command": "echo child",
        "trigger": {"job_id": "job_parent01", "status": "cancelled"},
    }
    response = remote.handle_request(request_frame(payload=payload, key="key-audit-trigger-01"))
    assert response["kind"] == "response"

    remote._dispatch_queued_ops()

    op = store.db.execute("SELECT status, error FROM remote_operations").fetchone()
    assert op["status"] == "failed"
    assert op["error"]
    cancelled = store.db.execute("SELECT job_id FROM jobs WHERE status='cancelled'").fetchall()
    assert len(cancelled) == 1
    # The raw UPDATE and the operation transition commit as ONE transaction;
    # before the fix the nested BEGIN left an open transaction behind.
    assert store.db.in_transaction is False

    # The shared connection still works for the next request.
    follow_up = remote.handle_request(request_frame(key="key-audit-trigger-02"))
    assert follow_up["kind"] == "response"


def test_prepare_launch_failure_marks_queued_operation_failed(tmp_path):
    remote, store, _fake = make_remote(tmp_path, prepare_fails=True)

    response = remote.handle_request(request_frame(key="key-audit-prepare-01"))
    assert response["kind"] == "response"

    remote._dispatch_queued_ops()

    op = store.db.execute("SELECT status FROM remote_operations").fetchone()
    assert op["status"] == "failed"
    assert store.db.in_transaction is False


# ---------------------------------------------------------------------------
# Empty artifacts must transfer (zero chunks)
# ---------------------------------------------------------------------------


def _transfer_world(tmp_path, monkeypatch):
    monkeypatch.setenv("VANTH_TRANSFER_CHUNK_BYTES", "1024")
    controller_home = tmp_path / "controller-home"
    remote_home = tmp_path / "remote-home"
    cstore = RemoteStore(connect(tmp_path / "controller.sqlite"))
    remote_row = cstore.create_remote(target="user@host", state="paired")
    rstore = RemoteOperationStore(connect(tmp_path / "remote.sqlite"))
    manager = types.SimpleNamespace(
        db=rstore.db, logs=remote_home / "logs", events_dir=remote_home / "events"
    )
    remote = RemoteJobManager(rstore, manager, home=remote_home)
    catalog = open_catalog(controller_home)
    cops = ArtifactOperations(
        catalog, LocalBlobStore(default_store_root(controller_home), catalog)
    )

    class Transport:
        def open_session(self, remote_row, *, home=None):
            class Session:
                def exchange(self, frame_bytes):
                    frame = json.loads(frame_bytes.decode("utf-8").rstrip("\n"))
                    return json.dumps(remote.handle_request(frame), separators=(",", ":")) + "\n"

            return Session()

    control = RemoteControl(cstore, transport=Transport(), home=controller_home)
    return remote_row, remote, cops, RemoteArtifactBroker(control, cops)


def test_empty_file_push_and_pull_roundtrip(tmp_path, monkeypatch):
    remote_row, remote, cops, broker = _transfer_world(tmp_path, monkeypatch)
    put = cops.put_file("empty.bin", data=b"", idempotency_key="audit-empty-put")
    assert put["size_bytes"] == 0

    pushed = broker.push_blob(
        remote_row["remote_id"], put["version_id"], idempotency_key="audit-empty-push"
    )
    assert pushed["completed"] is True
    assert pushed["total_bytes"] == 0
    remote_ops = remote.transfers.ops()
    assert remote_ops.blobs.verify_blob(put["sha256"])

    dest = tmp_path / "pulled-empty.bin"
    pulled = broker.pull_blob(
        remote_row["remote_id"], pushed["version_id"], dest,
        idempotency_key="audit-empty-pull",
    )
    assert pulled["completed"] is True
    assert dest.exists()
    assert dest.read_bytes() == b""


# ---------------------------------------------------------------------------
# Canonical JSON key ordering is UTF-16 code-unit order
# ---------------------------------------------------------------------------


def test_canonical_json_sorts_astral_keys_before_bmp_e000():
    # U+10000 encodes as surrogate units D800 DC00, which sort BEFORE U+E000
    # in UTF-16 code-unit order (RFC 8785), even though its code point is
    # larger. Code-point sorting emitted the opposite order.
    assert canonical_json({"\ue000": 1, "\U00010000": 2}) == (
        '{"\U00010000":2,"\ue000":1}'
    )


# ---------------------------------------------------------------------------
# SQLite write failures must roll back, not leave a half-open transaction
# ---------------------------------------------------------------------------


def test_prune_requests_rolls_back_when_a_delete_fails(tmp_path):
    store = RemoteStore(connect(tmp_path / "controller.sqlite"))
    proxy = _FailingDb(store.db, sql_prefix="DELETE FROM REMOTE_REPLAY_TOMBSTONES")
    store.db = proxy

    with pytest.raises(sqlite3.OperationalError, match="disk is full"):
        store.prune_requests(60)

    assert proxy.rolled_back is True
    assert proxy.real.in_transaction is False


def test_journal_record_rolls_back_when_commit_fails(tmp_path):
    journal = RequestJournal(tmp_path / "client-requests.sqlite")
    real = journal.db
    proxy = _FailingDb(real, commit=True)
    journal.db = proxy
    entry = {
        "request_id": "req_" + "a" * 32,
        "remote_id": "rmt_" + "b" * 32,
        "idempotency_key": "key-audit-jr-01",
        "method": "job.start",
        "payload": {"command": "echo hi"},
        "digest": "d" * 64,
    }

    with pytest.raises(sqlite3.OperationalError, match="disk is full"):
        journal.record(entry)

    assert proxy.rolled_back is True
    assert real.in_transaction is False

    journal.db = real
    journal.record({**entry, "request_id": "req_" + "c" * 32,
                    "idempotency_key": "key-audit-jr-02"})
    assert real.execute("SELECT COUNT(*) FROM client_requests").fetchone()[0] == 1
