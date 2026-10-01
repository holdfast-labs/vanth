from __future__ import annotations

import json
import os
import struct
import subprocess
import sys
import threading
import time
from pathlib import Path

from .server import JobManager, now_iso


def _contain_windows_runner() -> int:
    """Keep this runner and its descendants in a kill-on-close Windows Job.

    Join before spawning, so even immediately exiting children inherit membership.
    The non-inheritable handle intentionally lives until OS process teardown, after
    terminal persistence; closing it earlier would terminate this runner too.
    Nested jobs work on supported Windows versions (Windows 8 / Server 2012+).
    """
    import ctypes
    from ctypes import wintypes

    class BasicLimits(ctypes.Structure):
        _fields_ = [("process_time", ctypes.c_longlong), ("job_time", ctypes.c_longlong),
                    ("flags", wintypes.DWORD), ("minimum_working_set", ctypes.c_size_t),
                    ("maximum_working_set", ctypes.c_size_t), ("active_processes", wintypes.DWORD),
                    ("affinity", ctypes.c_size_t), ("priority", wintypes.DWORD),
                    ("scheduling", wintypes.DWORD)]

    class Limits(ctypes.Structure):
        _fields_ = [("basic", BasicLimits), ("io", ctypes.c_ulonglong * 6),
                    ("process_memory", ctypes.c_size_t), ("job_memory", ctypes.c_size_t),
                    ("peak_process_memory", ctypes.c_size_t), ("peak_job_memory", ctypes.c_size_t)]

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
    kernel.CreateJobObjectW.restype = wintypes.HANDLE
    kernel.SetInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
    kernel.SetInformationJobObject.restype = wintypes.BOOL
    kernel.GetCurrentProcess.argtypes = []
    kernel.GetCurrentProcess.restype = wintypes.HANDLE
    kernel.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    kernel.AssignProcessToJobObject.restype = wintypes.BOOL
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.CloseHandle.restype = wintypes.BOOL
    handle = kernel.CreateJobObjectW(None, None)
    if not handle:
        raise ctypes.WinError(ctypes.get_last_error())
    limits = Limits()
    # Ordinary workloads inherit containment; explicit detached Vanth runners use
    # CREATE_BREAKAWAY_FROM_JOB so their durable jobs survive this runner's exit.
    limits.basic.flags = 0x2000 | 0x0800  # KILL_ON_JOB_CLOSE | BREAKAWAY_OK
    if not kernel.SetInformationJobObject(handle, 9, ctypes.byref(limits), ctypes.sizeof(limits)):
        error = ctypes.get_last_error()
        kernel.CloseHandle(handle)
        raise ctypes.WinError(error)
    if not kernel.AssignProcessToJobObject(handle, kernel.GetCurrentProcess()):
        error = ctypes.get_last_error()
        kernel.CloseHandle(handle)
        raise ctypes.WinError(error)
    return handle


def _fail_start(manager: JobManager, job_id: str, exc: Exception, claim_token: str | None = None) -> int:
    message = f"Job runner failed to start: {exc}"
    try:
        # The row may still be 'launching' (the parent claimed it and the runner
        # is mid-start). A claim-owned transition records the failure from
        # 'launching' OR 'running' so a run that fails before publishing is not
        # left stranded as 'launching' until stale-claim recovery. A restart
        # claim that fails to start preserves its budgeted retry intent (review
        # rc33 P1-6): the pending deadline is restored so restart policy still
        # relaunches after the backoff.
        if manager._terminal_event(job_id, "failed", 1, claim_token=claim_token,
                                   message=message, data={"error": str(exc)}, level="error", source="runner"):
            manager._restore_pending_restart_after(job_id, claim_token)
    finally:
        manager.close()
    return 1


def _publish_workload(
    manager: JobManager, job_id: str, pid: int, claim_token: str | None = None
) -> bool:
    def publish() -> int:
        with manager.db_lock:
            if claim_token:
                # Claim-owned promotion (review rc32 P1-3): the runner atomically
                # promotes the row it owns from 'launching' to 'running' ONLY if
                # it still holds claim_token. This is what makes the launching->running
                # transition race-free: a fast job can complete and record its
                # terminal state, and a parent can never resurrect a dead runner
                # with an unguarded 'running' write. If the claim was lost
                # (recovery orphaned it, or a newer launch owns the row), the
                # rowcount is 0 and the runner aborts its workload instead of
                # running an untracked process.
                #
                # Review rc36 P1: the pending restart intent is cleared IN THE
                # SAME UPDATE that promotes the claim (json_remove on
                # policy_state_json), so a crash cannot strand pending_restart_after
                # with restart_after=null — promotion and clear are one atomic
                # transaction.
                changed = manager.db.execute(
                    "UPDATE jobs SET status='running', pid=?, worker_pid=?, started_at=?, runner_heartbeat_at=?, "
                    "updated_at=?, exit_code=NULL, ended_at=NULL, "
                    "policy_state_json=json_remove(policy_state_json, '$.pending_restart_after') "
                    "WHERE job_id=? AND claim_token=? AND status='launching' AND stop_requested_at IS NULL",
                    (pid, os.getpid(), now_iso(), now_iso(), now_iso(), job_id, claim_token),
                ).rowcount
            else:
                # start()-path jobs are inserted 'running' with worker_pid set
                # by the parent (the Popen pid). Record only the workload PID;
                # worker_pid stays the parent-observed process so _watch_runner's
                # pid guard keeps matching on platforms where the runner's
                # os.getpid() differs from the Popen pid (Windows launcher shim).
                changed = manager.db.execute(
                    "UPDATE jobs SET pid=?, runner_heartbeat_at=?, updated_at=? "
                    "WHERE job_id=? AND status='running' AND stop_requested_at IS NULL",
                    (pid, now_iso(), now_iso(), job_id),
                ).rowcount
            manager.db.commit()
        return changed

    return bool(manager._retry_locked(publish))


def _abort_workload(
    manager: JobManager, job_id: str, proc: subprocess.Popen[bytes], claim_token: str | None = None, spec_name: str | None = None
) -> int:
    manager._kill_process(proc, force=True)
    try:
        proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        manager._kill_process(proc, force=True)
        proc.wait()
    if claim_token:
        # Record the failed publish if we still own the claim. If recovery
        # already moved the row to a terminal state, this is a guarded no-op.
        # Preserve any budgeted restart intent (review rc33 P1-6).
        if manager._terminal_event(job_id, "failed", 1, claim_token=claim_token,
                                   message="Workload launch could not be published", level="error", source="runner"):
            manager._restore_pending_restart_after(job_id, claim_token)
    if spec_name:
        try:
            (Path(manager.home) / "specs" / spec_name).unlink(missing_ok=True)
        except OSError:
            pass
    manager.close()
    return 1


def _feed_stdin(job_id: str, channel: Path, stdin, feeder_stop: threading.Event) -> None:
    """Forward length-prefixed records from the job's stdin channel to the child."""
    if stdin is None:
        return
    offset = 0
    buffer = bytearray()
    eof_seen = False
    while not eof_seen and not feeder_stop.is_set():
        try:
            if not channel.exists():
                channel.parent.mkdir(parents=True, exist_ok=True)
                time.sleep(0.02)
                continue
            try:
                with channel.open("rb") as f:
                    f.seek(offset)
                    chunk = f.read()
                    offset = f.tell()
            except OSError:
                time.sleep(0.02)
                continue
            if chunk:
                buffer.extend(chunk)
                while len(buffer) >= 8:
                    (length,) = struct.unpack_from("<Q", buffer)
                    if length == 0:
                        eof_seen = True
                        del buffer[:8]
                        break
                    if len(buffer) < 8 + length:
                        break
                    record = bytes(buffer[8:8 + length])
                    del buffer[:8 + length]
                    try:
                        stdin.write(record)
                        stdin.flush()
                    except (BrokenPipeError, OSError):
                        return
            time.sleep(0.02)
        except Exception:
            time.sleep(0.02)
    if not feeder_stop.is_set():
        try:
            stdin.flush()
        except (BrokenPipeError, OSError):
            pass
    try:
        stdin.close()
    except OSError:
        pass


def run(home: str, job_id: str, spec_file: str | None = None) -> int:
    manager = JobManager(home, recover=False)
    claim_token: str | None = None
    try:
        # Review rc33 P1-3: the runner reads a CLAIM-SPECIFIC spec file
        # (specs/{job_id}-{claim_token}.json) passed in argv, never the shared
        # mutable specs/{job_id}.json. A delayed runner from an old claim cannot
        # read the replacement token of a newer run and impersonate it. The
        # fixed basename path (specs/{job_id}.json) is used by the legacy
        # start() path and by tests that seed the spec directly.
        spec_name = spec_file or f"{job_id}.json"
        spec = json.loads((Path(home) / "specs" / spec_name).read_text(encoding="utf-8"))
        claim_token = spec.get("claim_token")
        env = os.environ.copy()
        env.update(spec.get("env") or {})
        # Declared-secret env values are scrubbed from captured stdout/stderr and
        # structured events (review #9). Resolve them from the job's merged
        # environment; only names are persisted, never the values.
        mask_values = [
            env[name]
            for name in (spec.get("secret_env") or [])
            if isinstance(name, str) and isinstance(env.get(name), str) and env.get(name)
        ]
        creationflags = 0
        if sys.platform == "win32":
            creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
        proc = subprocess.Popen(
            spec["command"],
            cwd=spec.get("cwd"),
            env=env,
            stdin=subprocess.PIPE if spec.get("interactive") else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            creationflags=creationflags,
            start_new_session=sys.platform != "win32",
        )
    except Exception as exc:
        return _fail_start(manager, job_id, exc, claim_token)
    try:
        published = _publish_workload(manager, job_id, proc.pid, claim_token)
    except Exception as exc:
        manager.logger.exception("workload PID publication failed job_id=%s", job_id)
        return _abort_workload(manager, job_id, proc, claim_token, spec_name)
    if not published:
        return _abort_workload(manager, job_id, proc, claim_token, spec_name)
    try:
        (Path(home) / "specs" / spec_name).unlink()
    except FileNotFoundError:
        pass
    manager._emit(job_id, "started")
    manager.processes[job_id] = proc
    manager.reader_threads[job_id] = [
        threading.Thread(
            target=manager._read_stream,
            args=(job_id, proc.stdout, Path(spec["stdout_path"]), "stdout", mask_values),
            daemon=True,
        ),
        threading.Thread(
            target=manager._read_stream,
            args=(job_id, proc.stderr, Path(spec["stderr_path"]), "stderr", mask_values),
            daemon=True,
        ),
    ]
    for thread in manager.reader_threads[job_id]:
        thread.start()
    heartbeat_stop = threading.Event()

    def heartbeat() -> None:
        # Review rc34 P1-4: the heartbeat write is run-IDENTITY guarded so a
        # stale runner from run A can never keep run B's row fresh (masking a
        # dead B runner). Claim-token runs are guarded by claim_token; legacy
        # runs are guarded by the workload pid THIS runner published (the row's
        # worker_pid is the parent-observed Popen pid, which differs from the
        # runner's os.getpid() on Windows). When the guarded update affects zero
        # rows, the row is no longer ours (a newer run owns it, or it is
        # terminal) — stop the heartbeat loop.
        def beat() -> int:
            with manager.db_lock:
                if claim_token:
                    changed = manager.db.execute(
                        "UPDATE jobs SET runner_heartbeat_at=?, updated_at=? "
                        "WHERE job_id=? AND status='running' AND claim_token=?",
                        (now_iso(), now_iso(), job_id, claim_token),
                    ).rowcount
                else:
                    changed = manager.db.execute(
                        "UPDATE jobs SET runner_heartbeat_at=?, updated_at=? "
                        "WHERE job_id=? AND status='running' AND pid=?",
                        (now_iso(), now_iso(), job_id, proc.pid),
                    ).rowcount
                manager.db.commit()
            return changed

        while not heartbeat_stop.wait(manager.heartbeat_interval):
            try:
                changed = manager._retry_locked(beat)
            except Exception:
                manager.logger.exception("runner heartbeat update failed job_id=%s", job_id)
                continue
            if changed == 0:
                # The row is no longer ours (terminal, or a newer run owns the
                # token/pid). Stop heartbeating to avoid masking recovery.
                heartbeat_stop.set()
                break

    heartbeat_thread = threading.Thread(target=heartbeat, name=f"vanth-heartbeat-{job_id}", daemon=True)
    heartbeat_thread.start()
    feeder_stop = threading.Event()
    feeder_thread: threading.Thread | None = None
    if proc.stdin is not None:
        feeder_thread = threading.Thread(
            target=_feed_stdin,
            args=(job_id, Path(home) / "stdin" / f"{job_id}.in", proc.stdin, feeder_stop),
            name=f"vanth-stdin-{job_id}",
            daemon=True,
        )
        feeder_thread.start()
    timeout_seconds = spec.get("timeout_seconds")
    deadline = time.monotonic() + timeout_seconds if timeout_seconds is not None else None
    try:
        exit_code = proc.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        feeder_stop.set()
        if feeder_thread:
            feeder_thread.join(timeout=1)
        manager._kill_process(proc, force=True)
        exit_code = proc.wait()
        manager._readers_done(job_id, timeout=1)
        manager._finish(job_id, "timeout", exit_code, claim_token=claim_token)
        heartbeat_stop.set()
        heartbeat_thread.join(timeout=1)
        manager.close()
        return exit_code
    feeder_stop.set()
    if feeder_thread:
        feeder_thread.join(timeout=1)
    status = manager._row("SELECT status FROM jobs WHERE job_id=?", (job_id,))["status"]
    if status != "cancelled":
        drained = manager._readers_done(job_id, timeout=max(0, deadline - time.monotonic()) if deadline else 30)
        status = "timeout" if not drained and deadline else (
            "completed" if exit_code == 0 and job_id not in manager._capture_failed else "failed"
        )
        manager._finish(job_id, status, exit_code, claim_token=claim_token)
    heartbeat_stop.set()
    heartbeat_thread.join(timeout=1)
    manager.close()
    return exit_code


def main() -> None:
    if sys.platform == "win32":
        try:
            _contain_windows_runner()
        except OSError as exc:
            manager = JobManager(sys.argv[1], recover=False)
            claim = None
            try:
                name = sys.argv[3] if len(sys.argv) > 3 else f"{sys.argv[2]}.json"
                claim = json.loads((manager.specs_dir / name).read_text(encoding="utf-8")).get("claim_token")
            except (OSError, ValueError):
                pass
            raise SystemExit(_fail_start(manager, sys.argv[2], exc, claim))
    raise SystemExit(run(sys.argv[1], sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else None))


if __name__ == "__main__":
    main()
