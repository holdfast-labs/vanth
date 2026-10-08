"""Opt-in integrity checks and actionable capture diagnostics."""

import json

from vanth.artifacts.catalog import open_catalog
from vanth.artifacts.local_store import LocalBlobStore, default_store_root
from vanth.artifacts.operations import ArtifactOperations
from vanth.server import JobManager


def test_doctor_distinguishes_corruption_and_missing_content(tmp_path):
    catalog = open_catalog(tmp_path)
    blobs = LocalBlobStore(default_store_root(tmp_path), catalog)
    operations = ArtifactOperations(catalog, blobs)
    operations.put_file("artifact", data=b"good content", idempotency_key="health-artifact-123")
    digest = json.loads(catalog.db.execute("SELECT manifest_json FROM versions").fetchone()[0])["sha256"]
    manager = JobManager(tmp_path, recover=False)
    try:
        assert manager.doctor()["artifact_integrity"]["requested"] is False
        report = manager.doctor(verify_artifacts=True)
        assert report["artifact_integrity"]["checked"] == 1
        assert report["artifact_integrity"]["issues"] == []
        blobs.blob_path(digest).write_bytes(b"bad content")
        report = manager.doctor(verify_artifacts=True)
        assert report["artifact_integrity"]["issues"][0]["type"] == "corrupt_artifact_blob"
        assert any(w["type"] == "artifact_integrity" for w in report["warnings"])
        blobs.blob_path(digest).unlink()
        report = manager.doctor(verify_artifacts=True)
        assert report["artifact_integrity"]["issues"][0]["type"] == "missing_artifact_blob"
    finally:
        manager.close()
        catalog.db.close()


def test_doctor_reports_persisted_runner_diagnostics(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    try:
        started = manager.start("echo queued", pool="held")
        manager._emit(started["job_id"], "pipe_drain_timeout", message="child held pipes")
        manager._emit(started["job_id"], "write_contended", data={"write_seconds": 2})
        report = manager.doctor()
        assert {row["type"] for row in report["capture_diagnostics"]} == {"pipe_drain_timeout", "write_contended"}
        assert any(w["type"] == "capture_diagnostics" for w in report["warnings"])
    finally:
        manager.close()


def test_failed_capture_is_actionable_in_status_and_summary(tmp_path):
    manager = JobManager(tmp_path, recover=False)
    try:
        started = manager.start("echo queued", pool="held")
        job_id = started["job_id"]
        manager._emit(job_id, "log_capture_failed", message="disk full")
        manager.db.execute("UPDATE jobs SET status='failed',exit_code=0 WHERE job_id=?", (job_id,))
        manager.db.commit()
        for report in (manager.status(job_id), manager.run_summary(job_id)):
            assert report["failure_reason"] == "log_capture_failed"
            assert "disk space" in report["recommended_next_action"]
    finally:
        manager.close()


def test_doctor_reports_partial_scan_for_oversized_catalog_manifest(tmp_path):
    catalog = open_catalog(tmp_path)
    blobs = LocalBlobStore(default_store_root(tmp_path), catalog)
    operations = ArtifactOperations(catalog, blobs)
    operations.put_file("artifact", data=b"good", idempotency_key="large-manifest-123")
    catalog.db.execute("UPDATE versions SET manifest_json=?", (' ' * 1048577 + '{}',))
    catalog.db.commit()
    manager = JobManager(tmp_path, recover=False)
    try:
        report = manager.doctor(verify_artifacts=True)["artifact_integrity"]
        assert report["complete"] is False
        assert report["checked"] == 0
        assert report["issues"] == []
    finally:
        manager.close()
        catalog.db.close()
