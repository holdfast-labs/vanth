"""Measure what SQLite's synchronous level buys and costs for jobs.sqlite.

    uv run python scripts/sqlite_durability.py bench    [--events 20000] [--repeat 3]
    uv run python scripts/sqlite_durability.py crash    [--iterations 100]
    uv run python scripts/sqlite_durability.py exposure [--events 20000]
    uv run python scripts/sqlite_durability.py torn     [--events 5000] [--iterations 200]

Each subcommand runs once per level in --levels (default FULL,NORMAL), using
the real JobManager write paths on a throwaway VANTH_HOME. The level is forced
with VANTH_SQLITE_SYNCHRONOUS (the knob vanth.migrations reads), so the paths
measured are exactly the ones a runner and the daemon use.

bench     chatty-job capture throughput (a child printing AGENT_EVENT metric
          lines into a real pipe, read by _read_stream), the full _emit path
          (event + mirror file), the SQLite commit alone (no mirror), and runner
          heartbeat commits. Every latency reports p50/p95/p99.
crash     a runner-style writer (capture batches) and a daemon-style writer
          (one event + job-row update per transaction) share one database and
          are hard-killed (TerminateProcess / SIGKILL) at a random moment.
          Each writer prints the seq of every commit AFTER commit() returns;
          the parent keeps the last line it read. Afterwards: integrity_check,
          every acked seq present, seqs 1..N with no gap or duplicate, and the
          job row agrees with its newest event.
exposure  how often the WAL is actually fsynced under NORMAL: counts WAL
          resets (header checkpoint-sequence changes, which happen after a
          full auto-checkpoint) during a chatty job, and measures WAL frames
          per event / per idle heartbeat with auto-checkpoint off, which bound
          what a power loss can take.
torn      a power loss under NORMAL: the WAL tail never reached disk. A chatty
          job runs with auto-checkpoint off after an initial TRUNCATE, so the
          database file is a complete checkpointed state and the WAL only holds
          event frames. Every variant is the database plus some prefix of the
          WAL, possibly with one earlier frame zeroed (out-of-order
          persistence). Each variant must pass integrity_check and hold a
          gap-free prefix. Not covered: a power loss during the checkpoint
          itself, torn writes to the database file, or hardware that
          acknowledges a flush it did not perform.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import sqlite3
import statistics
import struct
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

LEVELS = ("OFF", "NORMAL", "FULL", "EXTRA")


def _manager(home: Path):
    from vanth.server import JobManager

    return JobManager(home, recover=False)


def _insert_job(manager, job_id: str) -> None:
    from vanth.server import now_iso

    stamp = now_iso()
    manager.db.execute(
        "INSERT INTO jobs(job_id,command,status,created_at,updated_at,stdout_path,stderr_path,events_path) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (job_id, "x", "running", stamp, stamp, str(manager.logs / f"{job_id}.out"),
         str(manager.logs / f"{job_id}.err"), str(manager.events_dir / f"{job_id}.jsonl")))
    manager.db.commit()


def _set_level(level: str) -> None:
    os.environ["VANTH_SQLITE_SYNCHRONOUS"] = level


def _temp_home(prefix: str) -> Path:
    home = Path(tempfile.mkdtemp(prefix=prefix)).resolve()
    os.environ["VANTH_HOME"] = str(home)
    return home


def _metrics(events: int) -> bytes:
    return (
        "import sys\n"
        f"for i in range({events}):\n"
        "    sys.stdout.write('AGENT_EVENT {\"type\":\"metric\",\"data\":{\"_step\":%d,\"loss\":0.5}}\\n' % i)\n"
    ).encode()


# --------------------------------------------------------------------- bench

def bench_capture(level: str, events: int) -> float:
    _set_level(level)
    home = _temp_home(f"vanth-sync-{level}-")
    manager = _manager(home)
    try:
        _insert_job(manager, "chatty")
        child = subprocess.Popen([sys.executable, "-c", _metrics(events).decode()],
                                 stdout=subprocess.PIPE)
        started = time.perf_counter()
        manager._read_stream("chatty", child.stdout, manager.logs / "chatty.out", "stdout")
        elapsed = time.perf_counter() - started
        child.wait()
        count = manager.db.execute("SELECT COUNT(*) FROM events WHERE job_id='chatty'").fetchone()[0]
        assert count == events, (count, events)
        return events / elapsed
    finally:
        manager.close()
        shutil.rmtree(home, ignore_errors=True)


def bench_transitions(level: str, n: int) -> dict[str, float]:
    _set_level(level)
    home = _temp_home(f"vanth-sync-{level}-")
    manager = _manager(home)
    try:
        _insert_job(manager, "steady")
        from vanth.server import normalize_event_payload

        payload = normalize_event_payload({"type": "progress", "data": {"i": 0}, "level": "info"})
        data_json = json.dumps(payload["data"], separators=(",", ":"))
        emit, commit, beat = [], [], []
        for i in range(n):
            t0 = time.perf_counter()
            manager._emit("steady", "progress", data={"i": i})
            emit.append(time.perf_counter() - t0)
            t0 = time.perf_counter()
            with manager.db_lock:
                manager._emit_transactional(
                    "steady", payload, data_json, "progress", "info", "server", None)
            commit.append(time.perf_counter() - t0)
            t0 = time.perf_counter()
            with manager.db_lock:
                manager.db.execute("UPDATE jobs SET runner_heartbeat_at=?, updated_at=? WHERE job_id='steady'",
                                   (str(i), str(i)))
                manager.db.commit()
            beat.append(time.perf_counter() - t0)

        def p(values, q):
            return sorted(values)[min(len(values) - 1, int(q * (len(values) - 1)))] * 1000

        return {"emit_p50_ms": p(emit, 0.5), "emit_p95_ms": p(emit, 0.95), "emit_p99_ms": p(emit, 0.99),
                "commit_p50_ms": p(commit, 0.5), "commit_p95_ms": p(commit, 0.95),
                "commit_p99_ms": p(commit, 0.99),
                "heartbeat_p50_ms": p(beat, 0.5), "heartbeat_p95_ms": p(beat, 0.95),
                "heartbeat_p99_ms": p(beat, 0.99)}
    finally:
        manager.close()
        shutil.rmtree(home, ignore_errors=True)


def cmd_bench(args) -> None:
    for level in args.levels:
        rates = [bench_capture(level, args.events) for _ in range(args.repeat)]
        latency = bench_transitions(level, args.transitions)
        print(json.dumps({"level": level, "events": args.events,
                          "capture_events_per_s": [round(r) for r in rates],
                          "capture_median": round(statistics.median(rates)),
                          **{k: round(v, 3) for k, v in latency.items()}}))


# --------------------------------------------------------------------- crash

def writer(home: str, role: str, seed: int) -> None:
    """Write until killed; print each seq only after its commit returned."""
    manager = _manager(Path(home))
    rnd = random.Random(seed)
    out = sys.stdout
    if role == "runner":
        k = 0
        while True:
            n = rnd.randint(1, 200)
            payloads = [{"type": "metric", "level": "info", "message": None,
                         "data": {"_step": k + i, "loss": 0.5}} for i in range(n)]
            manager._emit_capture_batch("cap", payloads, "stdout")
            k += n
            out.write(f"{k}\n")
            out.flush()
    else:
        k = 0
        while True:
            k += 1
            value = k

            def mutate(db, value=value):
                db.execute("UPDATE jobs SET exit_code=?, updated_at=? WHERE job_id='life'", (value, str(value)))

            manager._emit("life", "progress", data={"k": k}, mutate=mutate)
            out.write(f"{k}\n")
            out.flush()


def _reader(stream, box: list[int]) -> None:
    for line in stream:
        try:
            box[0] = int(line)
        except ValueError:
            pass


def _check(home: Path, acked: dict[str, int]) -> dict[str, object]:
    """Reopen the killed DB; retry a transient Windows open/read failure.

    A hard kill releases the killed processes' handles a moment before the
    kernel and any file scanner are done with them. "disk I/O error" here was
    observed once in 100 iterations and never reproduced in 60; a real
    corruption shows up as an integrity_check row, not an open error.
    """
    last: sqlite3.OperationalError | None = None
    for attempt in range(10):
        try:
            return _check_once(home, acked)
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            if "disk i/o error" not in message and "unable to open" not in message:
                raise
            last = exc
            time.sleep(0.2 * (attempt + 1))
    assert last is not None
    raise last


def _check_once(home: Path, acked: dict[str, int]) -> dict[str, object]:
    db = sqlite3.connect(home / "jobs.sqlite", timeout=5)
    try:
        integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
        result: dict[str, object] = {"integrity": integrity}
        for job, expect_type in (("cap", "metric"), ("life", "progress")):
            seqs = [row[0] for row in db.execute(
                "SELECT seq FROM events WHERE job_id=? AND type=? ORDER BY seq", (job, expect_type))]
            first = db.execute("SELECT MIN(seq) FROM events WHERE job_id=?", (job,)).fetchone()[0] or 0
            # seq 1 is the 'started' setup event; captured/progress seqs follow.
            body = [s - first for s in seqs]
            result[f"{job}_rows"] = len(seqs)
            result[f"{job}_contiguous"] = body == list(range(1, len(body) + 1))
            result[f"{job}_acked"] = acked[job]
            result[f"{job}_lost_acked"] = max(0, acked[job] - len(seqs))
        exit_code = db.execute("SELECT exit_code FROM jobs WHERE job_id='life'").fetchone()[0]
        result["life_row_matches_events"] = (exit_code or 0) == result["life_rows"]
        return result
    finally:
        db.close()


def crash_once(level: str, seed: int) -> dict[str, object]:
    _set_level(level)
    home = _temp_home(f"vanth-crash-{level}-")
    try:
        manager = _manager(home)
        _insert_job(manager, "cap")
        _insert_job(manager, "life")
        manager._emit("cap", "started")
        manager._emit("life", "started")
        manager.close()
        env = dict(os.environ, PYTHONPATH=str(ROOT / "src"), VANTH_HOME=str(home),
                   VANTH_SQLITE_SYNCHRONOUS=level)
        procs, boxes, threads = {}, {}, []
        for role, job in (("runner", "cap"), ("daemon", "life")):
            proc = subprocess.Popen([sys.executable, __file__, "_writer", str(home), role, str(seed)],
                                    stdout=subprocess.PIPE, text=True, env=env)
            box = [0]
            thread = threading.Thread(target=_reader, args=(proc.stdout, box), daemon=True)
            thread.start()
            procs[job], boxes[job] = proc, box
            threads.append(thread)
        rnd = random.Random(seed)
        time.sleep(rnd.uniform(0.6, 2.5))  # startup ~0.4 s, then a random point mid-write
        for proc in procs.values():
            proc.kill()  # TerminateProcess on Windows, SIGKILL on POSIX
        for proc in procs.values():
            proc.wait()
        for thread in threads:
            thread.join(timeout=5)
        time.sleep(0.05)  # let the kernel finish closing the killed handles
        acked = {job: box[0] for job, box in boxes.items()}
        result = _check(home, acked)
        # Recovery: the real code path reopens and reads the jobs cleanly.
        manager = _manager(home)
        try:
            manager.status("cap")
            manager.status("life")
            result["reopen_ok"] = True
        finally:
            manager.close()
        return result
    finally:
        shutil.rmtree(home, ignore_errors=True)


def cmd_crash(args) -> None:
    for level in args.levels:
        bad, lost, rows = [], 0, {"cap": 0, "life": 0}
        for i in range(args.iterations):
            try:
                r = crash_once(level, seed=i)
            except Exception as exc:  # one bad iteration reports, never aborts the level
                r = {"error": f"{type(exc).__name__}: {exc}", "integrity": "error",
                     "cap_contiguous": False, "life_contiguous": False,
                     "life_row_matches_events": False, "reopen_ok": False,
                     "cap_lost_acked": 0, "life_lost_acked": 0, "cap_rows": 0, "life_rows": 0}
            ok = (r["integrity"] == "ok" and r["cap_contiguous"] and r["life_contiguous"]
                  and r["life_row_matches_events"] and r["reopen_ok"]
                  and r["cap_lost_acked"] == 0 and r["life_lost_acked"] == 0)
            lost += int(r["cap_lost_acked"]) + int(r["life_lost_acked"])
            rows["cap"] += int(r["cap_rows"])
            rows["life"] += int(r["life_rows"])
            if not ok:
                bad.append(r)
        print(json.dumps({"level": level, "iterations": args.iterations, "failed_iterations": len(bad),
                          "acked_commits_lost": lost, "capture_rows_total": rows["cap"],
                          "transition_rows_total": rows["life"], "first_failures": bad[:3]}))


# ------------------------------------------------------------------ exposure

def _wal_checkpoint_seq(path: Path) -> int | None:
    try:
        with open(path, "rb") as f:
            header = f.read(16)
    except OSError:
        return None
    if len(header) < 16:
        return None
    # WAL header: magic(0), version(4), page size(8), checkpoint seq(12).
    return struct.unpack(">I", header[12:16])[0]


def cmd_exposure(args) -> None:
    from vanth.server import JobManager

    for level in args.levels:
        _set_level(level)
        home = _temp_home(f"vanth-exp-{level}-")
        manager = JobManager(home, recover=False)
        try:
            _insert_job(manager, "chatty")
            wal = home / "jobs.sqlite-wal"
            resets, stop = [0], threading.Event()

            def poll() -> None:
                last = _wal_checkpoint_seq(wal)
                while not stop.is_set():
                    now = _wal_checkpoint_seq(wal)
                    if now is not None and now != last:
                        resets[0] += 1
                        last = now
                    time.sleep(0.002)

            poller = threading.Thread(target=poll, daemon=True)
            poller.start()
            child = subprocess.Popen([sys.executable, "-c", _metrics(args.events).decode()],
                                     stdout=subprocess.PIPE)
            started = time.perf_counter()
            manager._read_stream("chatty", child.stdout, manager.logs / "chatty.out", "stdout")
            elapsed = time.perf_counter() - started
            child.wait()
            stop.set()
            poller.join()
            # Frames per event and per heartbeat, with auto-checkpoint off so the
            # WAL only grows: frame = 24-byte header + page.
            page = manager.db.execute("PRAGMA page_size").fetchone()[0]
            manager.db.execute("PRAGMA wal_autocheckpoint=0")
            manager.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            size0 = wal.stat().st_size if wal.exists() else 0
            manager._emit_capture_batch("chatty", [{"type": "metric", "level": "info", "message": None,
                                                    "data": {"_step": i, "loss": 0.5}} for i in range(640)], "stdout")
            size1 = wal.stat().st_size
            for i in range(100):
                with manager.db_lock:
                    manager.db.execute("UPDATE jobs SET runner_heartbeat_at=?, updated_at=? WHERE job_id='chatty'",
                                       (str(i), str(i)))
                    manager.db.commit()
            size2 = wal.stat().st_size
            frame = 24 + page
            per_event = (size1 - size0) / frame / 640
            per_beat = (size2 - size1) / frame / 100
            rate = args.events / elapsed
            events_per_checkpoint = round(1000 / per_event) if per_event else None
            beats_per_checkpoint = round(1000 / per_beat) if per_beat else None
            print(json.dumps({
                "level": level, "events": args.events, "capture_seconds": round(elapsed, 2),
                "events_per_s": round(rate),
                # Directly observed during the live capture (auto-checkpoint on).
                "wal_resets_during_capture": resets[0],
                "capture_seconds_per_wal_reset": round(elapsed / resets[0], 3) if resets[0] else None,
                # Measured on the populated DB with auto-checkpoint off: the
                # steady-state cost of the writes that fill one 1000-page WAL.
                "steady_state_wal_frames_per_event": round(per_event, 3),
                "steady_state_wal_frames_per_heartbeat": round(per_beat, 2),
                "steady_state_events_per_checkpoint": events_per_checkpoint,
                "steady_state_capture_seconds_between_fsyncs": round(events_per_checkpoint / rate, 3)
                if events_per_checkpoint else None,
                "idle_heartbeats_per_checkpoint": beats_per_checkpoint,
                "idle_seconds_between_fsyncs": round(beats_per_checkpoint *
                                                     float(os.environ.get("VANTH_RUNNER_HEARTBEAT_INTERVAL", "1")), 1)
                if beats_per_checkpoint else None,
            }))
        finally:
            manager.close()
            shutil.rmtree(home, ignore_errors=True)


# ---------------------------------------------------------------------- torn

def cmd_torn(args) -> None:
    import shutil as _shutil

    _set_level("NORMAL")
    home = _temp_home("vanth-torn-")
    manager = _manager(home)
    try:
        _insert_job(manager, "chatty")
        manager.db.execute("PRAGMA wal_autocheckpoint=0")
        # Make the main database a complete checkpointed state, so the copied
        # WAL contains only the event frames this test cuts.
        manager.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        rnd = random.Random(1)
        step = 0
        while step < args.events:
            n = rnd.randint(1, 200)
            manager._emit_capture_batch("chatty", [{"type": "metric", "level": "info", "message": None,
                                                    "data": {"_step": step + i, "loss": 0.5}} for i in range(n)], "stdout")
            step += n
        page = manager.db.execute("PRAGMA page_size").fetchone()[0]
        db_bytes = (home / "jobs.sqlite").read_bytes()
        wal_bytes = (home / "jobs.sqlite-wal").read_bytes()
        manager.db.execute("PRAGMA wal_autocheckpoint=1000")
        # Snapshot WITHOUT closing: close() would checkpoint into the db file.
        frame = 24 + page
        frames = (len(wal_bytes) - 32) // frame
        results = {"variants": 0, "integrity_ok": 0, "prefix_ok": 0, "opened_ok": 0,
                   "rows_min": None, "rows_max": 0, "committed_rows": step, "wal_frames": frames,
                   "full_wal_rows": None}
        variants = []
        for i in range(args.iterations):
            # Variant 0 keeps the whole WAL: the complete data must survive.
            cut = len(wal_bytes) if i == 0 else rnd.randint(32, len(wal_bytes))
            wal = bytearray(wal_bytes[:cut])
            if i % 2 and cut > 32 + 4 * frame:
                # Out-of-order persistence: a middle frame never landed (zeroed).
                k = rnd.randint(0, (cut - 32) // frame - 2)
                start = 32 + k * frame
                wal[start:start + frame] = bytes(frame)
            target = Path(tempfile.mkdtemp(prefix="vanth-torn-v-")).resolve()
            variants.append(target)
            (target / "jobs.sqlite").write_bytes(db_bytes)
            (target / "jobs.sqlite-wal").write_bytes(bytes(wal))
            db = sqlite3.connect(target / "jobs.sqlite")
            try:
                ok = db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
                seqs = [r[0] for r in db.execute("SELECT seq FROM events WHERE job_id='chatty' ORDER BY seq")]
                opened = True
            except sqlite3.Error:
                ok, seqs, opened = False, [], False
            finally:
                db.close()
            results["variants"] += 1
            results["opened_ok"] += opened
            results["integrity_ok"] += ok
            results["prefix_ok"] += seqs == list(range(1, len(seqs) + 1))
            results["rows_min"] = len(seqs) if results["rows_min"] is None else min(results["rows_min"], len(seqs))
            results["rows_max"] = max(results["rows_max"], len(seqs))
            if cut == len(wal_bytes) and i % 2 == 0:
                results["full_wal_rows"] = len(seqs)
        print(json.dumps(results))
    finally:
        for target in locals().get("variants", []):
            _shutil.rmtree(target, ignore_errors=True)
        manager.close()
        shutil.rmtree(home, ignore_errors=True)


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "_writer":
        writer(sys.argv[2], sys.argv[3], int(sys.argv[4]))
        return
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["bench", "crash", "exposure", "torn"])
    parser.add_argument("--levels", default="FULL,NORMAL", type=lambda s: s.upper().split(","))
    parser.add_argument("--events", type=int, default=20000)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--transitions", type=int, default=500)
    parser.add_argument("--iterations", type=int, default=100)
    args = parser.parse_args()
    for level in args.levels:
        if level not in LEVELS:
            parser.error(f"unknown level {level!r}; choose from {', '.join(LEVELS)}")
    {"bench": cmd_bench, "crash": cmd_crash, "exposure": cmd_exposure, "torn": cmd_torn}[args.command](args)


if __name__ == "__main__":
    main()
