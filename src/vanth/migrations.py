"""Small, ordered SQLite migrations for Vanth state."""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

LATEST_SCHEMA_VERSION = 19
DEFAULT_BUSY_TIMEOUT_MS = 30000


def busy_timeout_ms() -> int:
    try:
        return max(1000, int(os.environ.get("VANTH_BUSY_TIMEOUT_MS", DEFAULT_BUSY_TIMEOUT_MS)))
    except ValueError:
        return DEFAULT_BUSY_TIMEOUT_MS


def configure_connection(db: sqlite3.Connection) -> None:
    db.execute(f"PRAGMA busy_timeout={busy_timeout_ms()}")
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA foreign_keys=ON")


def _column_names(db: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in db.execute(f"PRAGMA table_info({table})").fetchall()}


def _add_missing(db: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
    existing = _column_names(db, table)
    for name, definition in columns.items():
        if name not in existing:
            db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")


def _backup_before_migration(db: sqlite3.Connection, home: Path) -> Path:
    backups = home / "backups"
    backups.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    path = backups / f"jobs-{stamp}.sqlite"
    destination = sqlite3.connect(path)
    try:
        db.backup(destination)
    finally:
        destination.close()
    return path


def _create_latest_schema(db: sqlite3.Connection) -> None:
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS jobs (
          job_id TEXT PRIMARY KEY, name TEXT, command TEXT NOT NULL, cwd TEXT,
          status TEXT NOT NULL, pid INTEGER, worker_pid INTEGER,
          runner_heartbeat_at TEXT, stop_requested_at TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
          started_at TEXT, ended_at TEXT, exit_code INTEGER, timeout_seconds INTEGER,
          notify_on TEXT, origin_thread_id TEXT, wake_thread_id TEXT, tags_json TEXT,
          env_json TEXT, notes TEXT, run_json TEXT, stdout_path TEXT NOT NULL, stderr_path TEXT NOT NULL, events_path TEXT NOT NULL,
          trigger_json TEXT, policy_json TEXT, policy_state_json TEXT, policy_disabled INTEGER NOT NULL DEFAULT 0,
          claim_token TEXT, stop_actor TEXT, stop_reason TEXT, secret_env_json TEXT,
          pool TEXT, priority INTEGER NOT NULL DEFAULT 0, paused INTEGER NOT NULL DEFAULT 0, schedule_id TEXT
        );
        CREATE TABLE IF NOT EXISTS events (
          event_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, seq INTEGER NOT NULL,
          type TEXT NOT NULL, level TEXT, message TEXT, data_json TEXT,
          source TEXT, created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_events_job_seq ON events(job_id, seq);
        CREATE INDEX IF NOT EXISTS idx_events_job_type_seq ON events(job_id, type, seq);
        CREATE INDEX IF NOT EXISTS idx_events_type_created ON events(type, created_at);
        CREATE TABLE IF NOT EXISTS wake_targets (
          target_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, remote_id TEXT, type TEXT NOT NULL,
          events_json TEXT NOT NULL, config_json TEXT NOT NULL, created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_wake_targets_job ON wake_targets(job_id);
        CREATE TABLE IF NOT EXISTS remote_event_cursors (
          binding_id TEXT PRIMARY KEY, next_seq INTEGER NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS deliveries (
          delivery_id TEXT PRIMARY KEY, event_id TEXT NOT NULL, target_id TEXT NOT NULL,
          job_id TEXT NOT NULL, target_type TEXT NOT NULL, status TEXT NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0, payload_json TEXT NOT NULL,
          created_at TEXT NOT NULL, next_attempt_at TEXT, delivered_at TEXT,
          last_error TEXT, claim_token TEXT, claimed_at TEXT, lease_expires_at TEXT,
          claim_client_id TEXT,
          UNIQUE(event_id, target_id)
        );
        CREATE INDEX IF NOT EXISTS idx_deliveries_status ON deliveries(status, next_attempt_at, created_at);
        CREATE TABLE IF NOT EXISTS relay_subscriptions (
          client_id TEXT PRIMARY KEY, client_type TEXT NOT NULL,
          destinations_json TEXT NOT NULL, updated_at TEXT NOT NULL, last_poll_at TEXT
        );
        CREATE TABLE IF NOT EXISTS delivery_attempts (
          attempt_id TEXT PRIMARY KEY, delivery_id TEXT NOT NULL, attempt INTEGER NOT NULL,
          claim_token TEXT, target_type TEXT, started_at TEXT, ended_at TEXT,
          status TEXT NOT NULL, error TEXT, reclaimed INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS cleanup_tombstones (
          tombstone_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, artifacts_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS metric_series (
          series_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, metric TEXT NOT NULL,
          x REAL NOT NULL, y REAL NOT NULL, stage TEXT, event_id TEXT NOT NULL,
          seq INTEGER NOT NULL, created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_metric_series_job_metric ON metric_series(job_id, metric, seq);
        CREATE TABLE IF NOT EXISTS artifacts (
          artifact_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, name TEXT NOT NULL,
          uri TEXT NOT NULL, kind TEXT, size_bytes INTEGER, sha256 TEXT, meta_json TEXT,
          created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_artifacts_job ON artifacts(job_id, created_at);
        CREATE TABLE IF NOT EXISTS pools (
          pool TEXT PRIMARY KEY, max_parallel INTEGER NOT NULL DEFAULT 0,
          paused INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS schedules (
          schedule_id TEXT PRIMARY KEY, name TEXT, cron TEXT, interval_seconds INTEGER,
          timezone TEXT NOT NULL DEFAULT 'UTC', command TEXT NOT NULL, cwd TEXT, env_json TEXT,
          timeout_seconds INTEGER, tags_json TEXT, notes TEXT, secret_env_json TEXT,
          overlap TEXT NOT NULL DEFAULT 'skip', enabled INTEGER NOT NULL DEFAULT 1,
          next_fire_at TEXT, last_fired_at TEXT, fire_count INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_schedules_due ON schedules(enabled, next_fire_at);
        CREATE INDEX IF NOT EXISTS idx_jobs_schedule ON jobs(schedule_id);
        CREATE INDEX IF NOT EXISTS idx_jobs_pool_status ON jobs(pool, status);
        CREATE TABLE IF NOT EXISTS decisions (
          decision_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, prompt TEXT NOT NULL,
          options_json TEXT NOT NULL, choice TEXT, status TEXT NOT NULL,
          resolved_by TEXT, created_at TEXT NOT NULL, expires_at TEXT, resolved_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_decisions_job ON decisions(job_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_decisions_pending ON decisions(status, expires_at);
        CREATE TABLE IF NOT EXISTS local_start_requests (
          idempotency_key TEXT PRIMARY KEY, request_hash TEXT NOT NULL,
          job_id TEXT NOT NULL, created_at TEXT NOT NULL
        );
        PRAGMA user_version=19;
        """
    )


def migrate(db: sqlite3.Connection, home: str | Path) -> Path | None:
    """Create the latest schema or migrate an existing v0 database transactionally."""
    home = Path(home)
    version = int(db.execute("PRAGMA user_version").fetchone()[0])
    if version > LATEST_SCHEMA_VERSION:
        raise RuntimeError(f"database schema version {version} is newer than this Vanth binary supports ({LATEST_SCHEMA_VERSION})")
    has_jobs = bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='jobs'").fetchone())
    if not has_jobs:
        _create_latest_schema(db)
        db.commit()
        return None

    backup = None
    if version < LATEST_SCHEMA_VERSION:
        backup = _backup_before_migration(db, home)
        try:
            db.execute("BEGIN IMMEDIATE")
            if version < 1:
                _add_missing(
                    db,
                    "jobs",
                    {
                        "worker_pid": "INTEGER",
                        "origin_thread_id": "TEXT",
                        "wake_thread_id": "TEXT",
                        "tags_json": "TEXT",
                    },
                )
                _add_missing(db, "deliveries", {"next_attempt_at": "TEXT"})
                db.execute("PRAGMA user_version=1")
                version = 1
            if version < 2:
                _add_missing(db, "jobs", {"runner_heartbeat_at": "TEXT"})
                _add_missing(
                    db,
                    "deliveries",
                    {"claim_token": "TEXT", "claimed_at": "TEXT", "lease_expires_at": "TEXT"},
                )
                db.execute("PRAGMA user_version=2")
                version = 2
            if version < 3:
                _add_missing(
                    db,
                    "delivery_attempts",
                    {
                        "claim_token": "TEXT",
                        "target_type": "TEXT",
                        "started_at": "TEXT",
                        "ended_at": "TEXT",
                        "reclaimed": "INTEGER NOT NULL DEFAULT 0",
                    },
                )
                db.execute("PRAGMA user_version=3")
                version = 3
            if version < 4:
                db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS cleanup_tombstones (
                      tombstone_id TEXT PRIMARY KEY, job_id TEXT NOT NULL,
                      artifacts_json TEXT NOT NULL, created_at TEXT NOT NULL
                    )
                    """
                )
                db.execute("PRAGMA user_version=4")
                version = 4
            if version < 5:
                _add_missing(db, "jobs", {"stop_requested_at": "TEXT"})
                db.execute("PRAGMA user_version=5")
                version = 5
            if version < 6:
                _add_missing(db, "jobs", {"env_json": "TEXT"})
                db.execute("PRAGMA user_version=6")
                version = 6
            if version < 7:
                _add_missing(db, "jobs", {"notes": "TEXT", "run_json": "TEXT"})
                db.execute("PRAGMA user_version=7")
                version = 7
            if version < 8:
                db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS metric_series (
                      series_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, metric TEXT NOT NULL,
                      x REAL NOT NULL, y REAL NOT NULL, stage TEXT, event_id TEXT NOT NULL,
                      seq INTEGER NOT NULL, created_at TEXT NOT NULL
                    )
                    """
                )
                db.execute("CREATE INDEX IF NOT EXISTS idx_metric_series_job_metric ON metric_series(job_id, metric, seq)")
                db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS artifacts (
                      artifact_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, name TEXT NOT NULL,
                      uri TEXT NOT NULL, kind TEXT, size_bytes INTEGER, sha256 TEXT, meta_json TEXT,
                      created_at TEXT NOT NULL
                    )
                    """
                )
                db.execute("CREATE INDEX IF NOT EXISTS idx_artifacts_job ON artifacts(job_id, created_at)")
                db.execute("PRAGMA user_version=8")
                version = 8
            if version < 9:
                _add_missing(db, "jobs", {"trigger_json": "TEXT"})
                db.execute("PRAGMA user_version=9")
                version = 9
            if version < 10:
                _add_missing(
                    db,
                    "jobs",
                    {
                        "policy_json": "TEXT",
                        "policy_state_json": "TEXT",
                        "policy_disabled": "INTEGER NOT NULL DEFAULT 0",
                    },
                )
                db.execute("PRAGMA user_version=10")
                version = 10
            if version < 11:
                _add_missing(db, "jobs", {"claim_token": "TEXT"})
                db.execute("PRAGMA user_version=11")
                version = 11
            if version < 12:
                db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS relay_subscriptions (
                      client_id TEXT PRIMARY KEY, client_type TEXT NOT NULL,
                      destinations_json TEXT NOT NULL, updated_at TEXT NOT NULL, last_poll_at TEXT
                    )
                    """
                )
                db.execute("PRAGMA user_version=12")
                version = 12
            if version < 13:
                _add_missing(db, "deliveries", {"claim_client_id": "TEXT"})
                db.execute("PRAGMA user_version=13")
                version = 13
            if version < 14:
                # Stop attribution (who/why a run was killed) and declared-secret
                # env names (whose values are masked in captured logs/events).
                _add_missing(
                    db,
                    "jobs",
                    {"stop_actor": "TEXT", "stop_reason": "TEXT", "secret_env_json": "TEXT"},
                )
                db.execute("PRAGMA user_version=14")
                version = 14
            if version < 15:
                # Queues (pool/priority/pause), schedule linkage, and the
                # schedule/pool tables.
                _add_missing(
                    db,
                    "jobs",
                    {
                        "pool": "TEXT",
                        "priority": "INTEGER NOT NULL DEFAULT 0",
                        "paused": "INTEGER NOT NULL DEFAULT 0",
                        "schedule_id": "TEXT",
                    },
                )
                db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pools (
                      pool TEXT PRIMARY KEY, max_parallel INTEGER NOT NULL DEFAULT 0,
                      paused INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
                    )
                    """
                )
                db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS schedules (
                      schedule_id TEXT PRIMARY KEY, name TEXT, cron TEXT, interval_seconds INTEGER,
                      timezone TEXT NOT NULL DEFAULT 'UTC', command TEXT NOT NULL, cwd TEXT, env_json TEXT,
                      timeout_seconds INTEGER, tags_json TEXT, notes TEXT, secret_env_json TEXT,
                      overlap TEXT NOT NULL DEFAULT 'skip', enabled INTEGER NOT NULL DEFAULT 1,
                      next_fire_at TEXT, last_fired_at TEXT, fire_count INTEGER NOT NULL DEFAULT 0,
                      created_at TEXT NOT NULL, updated_at TEXT NOT NULL
                    )
                    """
                )
                db.execute("CREATE INDEX IF NOT EXISTS idx_schedules_due ON schedules(enabled, next_fire_at)")
                db.execute("CREATE INDEX IF NOT EXISTS idx_jobs_schedule ON jobs(schedule_id)")
                db.execute("CREATE INDEX IF NOT EXISTS idx_jobs_pool_status ON jobs(pool, status)")
                db.execute("PRAGMA user_version=15")
                version = 15
            if version < 16:
                # Durable approval/decision requests: a job's "needs a human"
                # state machine, kept in its own table so it never overloads
                # jobs.status. Lifecycle events (decision_requested/resolved/
                # withdrawn/expired) live in `events` for the audit trail.
                db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS decisions (
                      decision_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, prompt TEXT NOT NULL,
                      options_json TEXT NOT NULL, choice TEXT, status TEXT NOT NULL,
                      resolved_by TEXT, created_at TEXT NOT NULL, expires_at TEXT, resolved_at TEXT
                    )
                    """
                )
                db.execute("CREATE INDEX IF NOT EXISTS idx_decisions_job ON decisions(job_id, created_at)")
                db.execute("CREATE INDEX IF NOT EXISTS idx_decisions_pending ON decisions(status, expires_at)")
                db.execute("PRAGMA user_version=16")
                version = 16
            if version < 17:
                _add_missing(db, "wake_targets", {"remote_id": "TEXT"})
                db.execute("PRAGMA user_version=17")
                version = 17
            if version < 18:
                db.execute(
                    """
                    CREATE TABLE IF NOT EXISTS remote_event_cursors (
                      binding_id TEXT PRIMARY KEY, next_seq INTEGER NOT NULL, updated_at TEXT NOT NULL
                    )
                    """
                )
                db.execute("PRAGMA user_version=18")
                version = 18
            if version < 19:
                db.execute("CREATE INDEX IF NOT EXISTS idx_events_type_created ON events(type, created_at)")
                db.execute(
                    "CREATE TABLE IF NOT EXISTS local_start_requests ("
                    "idempotency_key TEXT PRIMARY KEY, request_hash TEXT NOT NULL, "
                    "job_id TEXT NOT NULL, created_at TEXT NOT NULL)"
                )
                db.execute("PRAGMA user_version=19")
            db.commit()
        except Exception:
            db.rollback()
            raise
    return backup
