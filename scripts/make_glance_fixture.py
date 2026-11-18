"""Create a Vanth home that exercises the monitor's glance view.

Usage: uv run python scripts/make_glance_fixture.py [HOME]

Writes jobs and events straight into jobs.sqlite with timestamps relative to
now: several running jobs (progress, metrics, declared panels, one stalled,
one without progress), a queued job, and a mix of finished runs. Payloads go
through the daemon's own normalize_event_payload, so panel specs are validated
exactly as live AGENT_EVENT lines are. Render it right away: liveness is judged
against the wall clock.
"""

from __future__ import annotations

import json
import math
import sys
import tempfile
import uuid
from datetime import datetime, timedelta, timezone

from vanth.server import JobManager, normalize_event_payload

NOW = datetime.now(timezone.utc)


def iso(seconds_ago: float) -> str:
    return (NOW - timedelta(seconds=seconds_ago)).isoformat().replace("+00:00", "Z")


class Fixture:
    def __init__(self, home: str) -> None:
        self.manager = JobManager(home, recover=False)
        self.db = self.manager.db

    def job(self, job_id, name, status, started_ago, ended_ago=None, exit_code=None, created_ago=None):
        created = iso(created_ago if created_ago is not None else started_ago + 2)
        self.db.execute(
            "INSERT INTO jobs(job_id, name, command, status, created_at, updated_at, started_at, ended_at, exit_code, "
            "tags_json, stdout_path, stderr_path, events_path) "
            "VALUES (?, ?, 'python job.py', ?, ?, ?, ?, ?, ?, '[]', 'o', 'e', 'ev')",
            (job_id, name, status, created, iso(ended_ago if ended_ago is not None else 0),
             iso(started_ago) if started_ago is not None else None,
             iso(ended_ago) if ended_ago is not None else None, exit_code),
        )
        self.seq = getattr(self, "seq", {})
        self.seq[job_id] = 0
        if started_ago is not None:
            self.event(job_id, started_ago, "started", "job started")

    def event(self, job_id, ago, event_type, message=None, level="info", **data):
        ev = normalize_event_payload({"type": event_type, "message": message, "level": level, "data": data})
        self.seq[job_id] += 1
        self.db.execute(
            "INSERT INTO events(event_id, job_id, seq, type, level, message, data_json, source, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, 'stdout', ?)",
            ("ev_" + uuid.uuid4().hex, job_id, self.seq[job_id], ev["type"], ev["level"], ev["message"],
             json.dumps(ev["data"]), iso(ago)),
        )

    def progress_run(self, job_id, started_ago, last_ago, every, total, unit, stage, done_frac, metrics=None):
        """Evenly spaced progress (and optional metric) events ending last_ago seconds ago."""
        steps = int((started_ago - last_ago) / every)
        for k in range(1, steps + 1):
            ago = started_ago - k * every
            cur = round(total * done_frac * k / steps)
            self.event(job_id, ago, "progress", f"{stage} {cur}/{total} {unit}", current=cur, total=total,
                       unit=unit, stage=stage)
            if metrics:
                self.event(job_id, ago, "metric", None, _step=k, **metrics(k, steps))


def main() -> None:
    home = sys.argv[1] if len(sys.argv) > 1 else tempfile.mkdtemp(prefix="vanth-glance-")
    f = Fixture(home)
    with f.manager.db_lock:
        # 1. Training run with declared panels: gauge, line chart, big stat, text.
        f.job("job_train", "train-resnet50", "running", started_ago=1100)
        f.event("job_train", 1095, "panels", panels=[
            {"kind": "gauge", "metric": "gpu_util", "label": "GPU", "unit": "%", "max": 100},
            {"kind": "line", "metric": "loss"},
            {"kind": "stat", "metric": "tokens_s", "label": "tokens/s"},
            {"kind": "text", "key": "phase", "label": "phase"},
        ])
        f.progress_run("job_train", 1090, 4, 30, 5000, "step", "train", 0.62,
                       metrics=lambda k, n: {"loss": round(2.4 * math.exp(-3 * k / n) + 0.1, 4),
                                             "gpu_util": 70 + (k * 7) % 25, "tokens_s": 1800 + (k * 37) % 400,
                                             "phase": "epoch 3/5"})
        # 2. Ingest job: no valid line for its second panel (noted as ignored).
        f.job("job_ingest", "ingest-s3-logs", "running", started_ago=380)
        f.event("job_ingest", 378, "panels", panels=[
            {"kind": "line", "metric": "mbps", "label": "throughput", "unit": "MB/s"},
            {"kind": "pie", "metric": "mbps"},
        ])
        f.progress_run("job_ingest", 375, 2, 8, 4000, "files", "download", 0.31,
                       metrics=lambda k, n: {"mbps": round(40 + 15 * math.sin(k / 4), 1)})
        # 3. Stalled evaluation: reported every 20s, then went quiet 9 minutes ago.
        f.job("job_eval", "eval-suite", "running", started_ago=1500)
        f.progress_run("job_eval", 1490, 540, 20, 1200, "cases", "eval", 0.4)
        # 4. Backup with no progress events: indeterminate bar.
        f.job("job_backup", "nightly-backup", "running", started_ago=200)
        f.event("job_backup", 15, "log", "copying /var/lib/postgres")
        # 5. Waiting for a pool slot.
        f.job("job_thumbs", "render-thumbnails", "queued", started_ago=None, created_ago=190)
        # Finished runs.
        f.job("job_fail1", "deploy-staging", "failed", started_ago=1900, ended_ago=1700, exit_code=1)
        f.event("job_fail1", 1700, "failed", "exit 1", level="error")
        f.job("job_timeout", "flaky-integration", "timeout", started_ago=7300, ended_ago=3700)
        for k, (name, dur, ago) in enumerate([("lint", 40, 300), ("unit-tests", 380, 900), ("build-wheels", 610, 2600),
                                               ("docs", 95, 4000), ("bench", 1400, 9000), ("lint", 38, 12000)]):
            f.job(f"job_ok{k}", name, "completed", started_ago=ago + dur, ended_ago=ago, exit_code=0)
        f.job("job_cancel", "scratch-run", "cancelled", started_ago=5200, ended_ago=5000)
        f.db.commit()
    f.manager.close()
    print(home)


if __name__ == "__main__":
    main()
