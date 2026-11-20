"""Print current performance numbers as one JSON object.

    uv run python scripts/perf_report.py [--events N] [--repeat N]
        [--skip-stop] [--skip-go]

This is a human comparison tool, not a CI test. It measures:

  import_ms            fresh-interpreter import time per hot module (min of N)
  capture             20k-event batch capture: seconds and events/second (min of N)
  stop_seconds        wall time to stop a sleeping job through JobManager
  go_refresh           BenchmarkRefreshLargeHome ns/op, B/op, allocs/op
  go_render            BenchmarkView160x44 ns/op, B/op, allocs/op

All work uses a throwaway temporary VANTH_HOME; the real home is never touched.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
HOT_MODULES = ("vanth.runner", "vanth.client", "vanth.cli", "vanth.server")
BENCH_RE = re.compile(
    r"^(Benchmark\w+)-\d+\s+\d+\s+([\d.]+) ns/op\s+(\d+) B/op\s+(\d+) allocs/op"
)


def _join(argv: list[str]) -> str:
    return subprocess.list2cmdline(argv) if os.name == "nt" else shlex.join(argv)


@contextmanager
def _temp_home():
    path = tempfile.mkdtemp(prefix="vanth-perf-")
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def import_times(repeat: int) -> dict[str, float]:
    out: dict[str, float] = {}
    for module in HOT_MODULES:
        code = (
            "import time; "
            f"start = time.perf_counter(); import {module}; "
            "print((time.perf_counter() - start) * 1000)"
        )
        samples = []
        for _ in range(repeat):
            result = subprocess.run(
                [sys.executable, "-c", code],
                capture_output=True,
                text=True,
                timeout=120,
            )
            if result.returncode != 0:
                out[module] = -1.0
                break
            samples.append(float(result.stdout.strip()))
        else:
            out[module] = round(min(samples), 3)
    return out


def capture_throughput(events: int, repeat: int) -> dict[str, float]:
    from vanth.server import JobManager, normalize_event_payload, now_iso

    with _temp_home() as home:
        manager = JobManager(home, recover=False)
        try:
            stamp = now_iso()
            manager.db.execute(
                "INSERT INTO jobs(job_id,command,status,created_at,updated_at,stdout_path,stderr_path,events_path) "
                "VALUES ('throughput','x','running',?,?,?,?,?)",
                (stamp, stamp, "o", "e", str(manager.events_dir / "throughput.jsonl")),
            )
            manager.db.commit()
            payloads = [
                normalize_event_payload(
                    {"type": "metric", "message": "m", "data": {"_step": i, "loss": 1.0}}
                )
                for i in range(events)
            ]
            manager._emit_capture_batch("throughput", payloads, "stdout")
            samples = []
            for _ in range(repeat):
                started = time.perf_counter()
                manager._emit_capture_batch("throughput", payloads, "stdout")
                samples.append(time.perf_counter() - started)
            best = min(samples)
            return {
                "events": events,
                "seconds": round(best, 4),
                "events_per_second": round(events / best, 1) if best else 0.0,
            }
        finally:
            manager.close()


def stop_latency(repeat: int) -> dict[str, float] | None:
    from vanth.server import JobManager

    with _temp_home() as home:
        manager = JobManager(home)
        try:
            code = "import time; time.sleep(60)"
            base = getattr(sys, "_base_executable", None) or sys.executable
            command = _join([base, "-c", code])
            samples = []
            for _ in range(repeat):
                job = manager.start(command)
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    status = manager.status(job["job_id"])
                    if status.get("status") == "running":
                        break
                    time.sleep(0.05)
                else:
                    return None
                started = time.perf_counter()
                manager.stop_sync(job["job_id"], actor="user", reason="perf report")
                samples.append(time.perf_counter() - started)
            return {"seconds": round(min(samples), 4), "runs": repeat}
        finally:
            manager.close()


def go_benchmarks() -> dict[str, dict[str, float]]:
    try:
        result = subprocess.run(
            [
                "go",
                "test",
                "./internal/monitor",
                "-run",
                "^$",
                "-bench",
                "BenchmarkRefreshLargeHome|BenchmarkView160x44",
                "-benchmem",
                "-count",
                "1",
            ],
            cwd=REPO,
            capture_output=True,
            text=True,
            timeout=600,
        )
    except (OSError, subprocess.TimeoutExpired):
        return {}
    if result.returncode != 0:
        return {}
    out: dict[str, dict[str, float]] = {}
    for line in result.stdout.splitlines():
        match = BENCH_RE.match(line.strip())
        if not match:
            continue
        name, ns, bytes_per_op, allocs = match.groups()
        out[name] = {
            "ms_per_op": round(float(ns) / 1_000_000, 3),
            "bytes_per_op": int(bytes_per_op),
            "allocs_per_op": int(allocs),
        }
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=20000)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--skip-stop", action="store_true")
    parser.add_argument("--skip-go", action="store_true")
    args = parser.parse_args()

    report = {
        "python": sys.version.split()[0],
        "platform": sys.platform,
        "import_ms": import_times(args.repeat),
        "capture": capture_throughput(args.events, args.repeat),
    }
    if not args.skip_stop:
        report["stop"] = stop_latency(args.repeat)
    if not args.skip_go:
        report["go"] = go_benchmarks()
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
