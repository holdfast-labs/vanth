"""Opt-in sustained runner/event soak; isolated home, no production daemon access.

Run with uv: python scripts/soak.py --duration 60 --jobs 2 --events 50.
The JSON report records exact event/sequence checks, observed event lag, job
latency, own runner leaks, and RSS samples. Timing/memory limits are optional;
unsupported RSS sampling is explicit. Failure preserves the isolated home.
"""
from __future__ import annotations

import argparse
import asyncio
import ctypes
import json
import math
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from vanth.server import JobManager


def distribution(values):
    ordered = sorted(values)
    if not ordered:
        return {"samples": 0, "p50": None, "p95": None, "max": None}
    return {"samples": len(ordered), "p50": ordered[(len(ordered) - 1) // 2],
            "p95": ordered[max(0, math.ceil(len(ordered) * .95) - 1)], "max": ordered[-1]}


def validate_events(rows, expected):
    """Check durable rows, including payload completeness and unique sequences."""
    seqs = [row["seq"] for row in rows]
    if seqs != list(range(1, len(rows) + 1)):
        raise AssertionError(f"non-contiguous or duplicate event sequences: {seqs[:20]}")
    metrics = [json.loads(row["data_json"]) for row in rows if row["type"] == "metric"]
    indices = sorted(data["i"] for data in metrics)
    if indices != list(range(expected)):
        raise AssertionError(f"event completeness failed: expected {expected}, observed {len(indices)}")
    for kind in ("started", "completed"):
        if sum(row["type"] == kind for row in rows) != 1:
            raise AssertionError(f"expected exactly one {kind} event")


def rss_bytes(pid):
    """Current working set / RSS using platform facilities; None if unavailable."""
    try:
        if sys.platform.startswith("linux"):
            resident_pages = int(Path(f"/proc/{pid}/statm").read_text().split()[1])
            return resident_pages * os.sysconf("SC_PAGE_SIZE")
        if sys.platform == "win32":
            from ctypes import wintypes

            class Counters(ctypes.Structure):
                _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD)] + [
                    (name, ctypes.c_size_t) for name in ("PeakWorkingSetSize", "WorkingSetSize",
                    "QuotaPeakPagedPoolUsage", "QuotaPagedPoolUsage", "QuotaPeakNonPagedPoolUsage",
                    "QuotaNonPagedPoolUsage", "PagefileUsage", "PeakPagefileUsage")]
            kernel = ctypes.WinDLL("kernel32", use_last_error=True)
            psapi = ctypes.WinDLL("psapi", use_last_error=True)
            kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
            kernel.OpenProcess.restype = wintypes.HANDLE
            kernel.CloseHandle.argtypes = [wintypes.HANDLE]
            psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD]
            handle = kernel.OpenProcess(0x0400 | 0x0010, False, pid)
            if not handle:
                return None
            try:
                counters = Counters()
                counters.cb = ctypes.sizeof(counters)
                if psapi.GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb):
                    return counters.WorkingSetSize
                return None
            finally:
                kernel.CloseHandle(handle)
        if sys.platform == "darwin":
            value = subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)], timeout=2)
            return int(value.strip()) * 1024
    except (OSError, ValueError, IndexError, subprocess.SubprocessError):
        return None
    return None


def cleanup_owned(manager, owned):
    """Only stop jobs and process handles launched by this isolated manager.

    Check the original Popen handle before cleanup. No machine-wide process
    name matching or runner enumeration.
    """
    errors = []
    for job_id, proc in owned.items():
        try:
            if proc.poll() is None:
                manager.stop_sync(job_id, signal="kill", kill_after_seconds=0, actor="tool", reason="soak cleanup")
                if proc.poll() is None:
                    manager._kill_process(proc, force=True)
                proc.wait(timeout=5)
        except Exception as exc:
            errors.append(f"cleanup {job_id}: {exc}")
    return errors


def run(args):
    home = Path(tempfile.mkdtemp(prefix="vanth-soak-"))
    report = {"result": "fail", "home": str(home), "jobs_started": 0, "jobs_verified": 0,
              "events_verified": 0, "errors": [], "own_runner_leaks": [], "rss_supported": False}
    manager = None
    owned = {}
    active = {}
    lags, runtimes, main_rss, runner_rss = [], [], [], []
    started = time.monotonic()
    next_sample = started
    code = ("import json,time,sys\n"
            f"for i in range({args.events}):\n"
            " event={'type':'metric','data':{'i':i,'sent_ns':time.time_ns()}}\n"
            " print('AGENT_EVENT '+json.dumps(event),file=(sys.stdout if i%2==0 else sys.stderr),flush=True)\n"
            f" time.sleep({args.interval!r})\n")
    try:
        # Keep multiline Python out of the shell command: cmd.exe treats
        # literal newlines in a -c argument as command boundaries.
        workload = home / "soak-workload.py"
        workload.write_text(code, encoding="utf-8")
        argv = [sys.executable, str(workload)]
        command = subprocess.list2cmdline(argv) if os.name == "nt" else shlex.join(argv)
        manager = JobManager(home, recover=False)
        while time.monotonic() < started + args.duration or active:
            now = time.monotonic()
            if now >= started + args.duration + args.grace:
                raise TimeoutError("jobs failed to drain before soak grace deadline")
            while now < started + args.duration and len(active) < args.jobs:
                result = asyncio.run(manager.start(command, name="soak", timeout_seconds=math.ceil(args.grace),
                                                  notify_on=[], wake_targets=[]))
                job_id = result["job_id"]
                proc = manager.processes.get(job_id)
                if proc is None:
                    raise AssertionError(f"runner was not captured for {job_id}: {result}")
                owned[job_id] = proc
                active[job_id] = {"started": time.monotonic(), "seen_seq": 0}
                report["jobs_started"] += 1
                now = time.monotonic()
            for job_id, state in list(active.items()):
                with manager.db_lock:
                    new_rows = manager.db.execute(
                        "SELECT seq,type,data_json FROM events WHERE job_id=? AND seq>? ORDER BY seq",
                        (job_id, state["seen_seq"])).fetchall()
                observed_ns = time.time_ns()
                for row in new_rows:
                    state["seen_seq"] = row["seq"]
                    if row["type"] == "metric":
                        lags.append(max(0, (observed_ns - json.loads(row["data_json"])["sent_ns"]) / 1e9))
                status = manager.status(job_id)
                state["worker_pid"] = status.get("worker_pid") or owned[job_id].pid
                if status["status"] in {"failed", "cancelled", "timeout", "orphaned"}:
                    raise AssertionError(f"job {job_id} ended as {status['status']}: {status}")
                # Terminal state may precede terminal event persistence. Require
                # runner exit before validating all rows.
                if status["status"] == "completed" and owned[job_id].poll() is not None:
                    with manager.db_lock:
                        rows = manager.db.execute(
                            "SELECT seq,type,data_json FROM events WHERE job_id=? ORDER BY seq", (job_id,)).fetchall()
                    validate_events(rows, args.events)
                    final_observed_ns = time.time_ns()
                    for row in rows:
                        if row["seq"] > state["seen_seq"] and row["type"] == "metric":
                            lags.append(max(0, (final_observed_ns - json.loads(row["data_json"])["sent_ns"]) / 1e9))
                    runtimes.append(time.monotonic() - state["started"])
                    report["jobs_verified"] += 1
                    report["events_verified"] += args.events
                    del active[job_id]
                    # Do not retain exited Popen handles for a long soak.
                    owned.pop(job_id).wait(timeout=0)
            if now >= next_sample:
                rss = rss_bytes(os.getpid())
                if rss is not None:
                    main_rss.append(rss)
                # Windows venv executables can be small launcher processes;
                # sample the published Python worker rather than its launcher.
                samples = [rss_bytes(state.get("worker_pid", owned[job_id].pid))
                           for job_id, state in active.items() if owned[job_id].poll() is None]
                samples = [value for value in samples if value is not None]
                if samples:
                    runner_rss.append(sum(samples))
                next_sample = now + 1
            time.sleep(.05)
        # Allow runner shutdown a bounded interval after terminal persistence.
        for job_id, proc in owned.items():
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                report["own_runner_leaks"].append(job_id)
        if report["own_runner_leaks"]:
            raise AssertionError("own runners survived completed jobs")
        if not report["jobs_verified"]:
            raise AssertionError("soak completed without verifying any jobs")
        if args.max_lag is not None and max(lags, default=0) > args.max_lag:
            raise AssertionError(f"event lag exceeded {args.max_lag}s")
        if args.max_runtime is not None and max(runtimes, default=0) > args.max_runtime:
            raise AssertionError(f"job latency exceeded {args.max_runtime}s")
        if args.max_memory_growth is not None:
            if not main_rss:
                raise AssertionError("RSS sampling unavailable for requested memory limit")
            if main_rss[-1] - main_rss[0] > args.max_memory_growth * 1024**2:
                raise AssertionError("manager RSS growth exceeded limit")
        report["result"] = "pass"
    except (Exception, KeyboardInterrupt) as exc:
        report["errors"].append(f"{type(exc).__name__}: {exc}")
    finally:
        if manager is not None:
            for job_id, proc in owned.items():
                try:
                    terminal = manager.status(job_id)["status"] == "completed"
                except Exception:
                    terminal = False
                if proc.poll() is None and terminal and job_id not in report["own_runner_leaks"]:
                    report["own_runner_leaks"].append(job_id)
            cleanup_errors = cleanup_owned(manager, owned)
            report["errors"].extend(cleanup_errors)
            if cleanup_errors:
                report["result"] = "fail"
            try:
                manager.close()
            except Exception as exc:
                report["errors"].append(f"manager close: {exc}")
                report["result"] = "fail"
        report.update({"elapsed_seconds": time.monotonic() - started,
                       "event_observation_lag_seconds": distribution(lags),
                       "job_latency_seconds": distribution(runtimes),
                       "rss_supported": bool(main_rss), "manager_rss_bytes": distribution(main_rss),
                       "runner_total_rss_bytes": distribution(runner_rss),
                       "manager_rss_growth_bytes": main_rss[-1] - main_rss[0] if main_rss else None})
        report["home_removed"] = False
        if report["result"] == "pass" and not args.keep_state:
            try:
                shutil.rmtree(home)
                report["home_removed"] = True
            except OSError as exc:
                report["errors"].append(f"state cleanup: {exc}")
                report["result"] = "fail"
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--jobs", type=int, default=2, help="concurrent jobs")
    parser.add_argument("--events", type=int, default=50, help="events per job, split across stdout/stderr")
    parser.add_argument("--interval", type=float, default=.01, help="seconds between emitted events")
    parser.add_argument("--grace", type=float, default=30, help="final drain deadline and workload timeout")
    parser.add_argument("--max-lag", type=float)
    parser.add_argument("--max-runtime", type=float)
    parser.add_argument("--max-memory-growth", type=float, help="manager RSS growth limit in MiB")
    parser.add_argument("--keep-state", action="store_true")
    parser.add_argument("--report", type=Path, help="optional JSON report file")
    args = parser.parse_args(argv)
    for name in ("duration", "jobs", "events", "grace"):
        if not math.isfinite(getattr(args, name)) or getattr(args, name) <= 0:
            parser.error(f"--{name} must be positive")
    for name in ("interval", "max_lag", "max_runtime", "max_memory_growth"):
        value = getattr(args, name)
        if value is not None and (not math.isfinite(value) or value < 0):
            parser.error(f"--{name.replace('_', '-')} must be finite and nonnegative")
    report = run(args)
    output = json.dumps(report, indent=2)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(output + "\n", encoding="utf-8")
    print(output)
    return 0 if report["result"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
