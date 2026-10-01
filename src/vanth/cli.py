"""Human-facing CLI for the Vanth daemon.

Unlike the MCP tools (which are JSON request/response over stdio), these
commands are meant for a person at a terminal: ``vanth status``, ``vanth
doctor``, ``vanth restart``, ``vanth setup``. They read the same daemon
discovery metadata and speak the same authenticated loopback HTTP, but print
readable output and exit with a meaningful status code (0 = healthy, 1 =
problem, 2 = usage).
"""

from __future__ import annotations

import difflib
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import autostart
from .client import VanthClient
from .paths import canonical_home

# Terminal job states (see README "Job lifecycle"). Everything else is
# in-flight and therefore excluded from `vanth list --all`.
_TERMINAL_STATUSES = {"completed", "failed", "timeout", "cancelled", "orphaned"}
# `vanth list` defaults to these rather than just "running": a job is inserted as
# "launching" and only becomes "running" once the runner publishes, so a
# freshly-started job used to appear in NEITHER `list` nor `list --all` - the
# user started a job and saw nothing (caught by an onboarding test).
_INFLIGHT_STATUSES = ("launching", "paused", "queued", "retrying", "running", "stopping")


def _resolve_job_id(client: VanthClient, raw: str) -> tuple[str | None, str]:
    """Resolve ``raw`` to a full job id, tolerating an unambiguous prefix.

    Agents and humans transcribe ids by hand from `vanth start` output, and a
    single dropped character produced "unknown job" with no hint (observed in an
    onboarding test). Returns ``(job_id, problem)``: exactly one is set. A
    best-effort lookup - if the job list is unavailable the raw value is used
    unchanged and the daemon reports the error.
    """
    if "/" in raw or len(raw) >= 40:  # not a plausible id; let the daemon decide
        return raw, ""
    try:
        exact = client.get(f"/jobs/{raw}/status")
        if exact.get("job_id") == raw:
            return raw, ""
    except Exception:
        pass
    try:
        jobs = client.get("/jobs", {"limit": 1000}).get("jobs") or []
    except Exception:
        return raw, ""
    ids = [job["job_id"] for job in jobs if isinstance(job.get("job_id"), str)]
    if raw in ids:
        return raw, ""
    matches = [job_id for job_id in ids if job_id.startswith(raw)]
    if len(matches) == 1:
        return matches[0], ""
    if len(matches) > 1:
        return None, f"{raw!r} is ambiguous: {', '.join(sorted(matches)[:5])}"
    near = difflib.get_close_matches(raw, ids, n=3, cutoff=0.5)
    hint = f"; did you mean {', '.join(near)}?" if near else ""
    return None, f"unknown job {raw}{hint}"


def _requiring_job_id(client: VanthClient, raw: str, command: str) -> tuple[str | None, int]:
    """Resolve a job id or print the problem. Returns ``(job_id, exit_code)``."""
    resolved, problem = _resolve_job_id(client, raw)
    if problem:
        print(f"vanth {command}: {problem}", file=sys.stderr)
        return None, 1
    return resolved, 0


def _discovery(home: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads((home / "daemon.json").read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else None
    except (OSError, ValueError):
        return None


def _daemon_version() -> str:
    """Best-effort version of the installed package (not the daemon process)."""
    try:
        from . import __version__

        return __version__
    except Exception:
        return "unknown"


def _health(url: str, token: str) -> dict[str, Any] | None:
    """Return the /health payload, or None if the daemon is unreachable."""
    try:
        request = urllib.request.Request(
            url + "/health", headers={"Authorization": f"Bearer {token}"}
        )
        with urllib.request.urlopen(request, timeout=2) as response:
            return json.loads(response.read().decode())
    except Exception:
        return None


def _pid_alive(pid: int) -> bool:
    """Return whether a process with the given PID is running."""
    if not pid:
        return False
    if sys.platform == "win32":
        try:
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                timeout=2,
            )
            return str(pid) in result.stdout
        except Exception:
            return True  # assume alive on probe failure
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def cmd_job_status(job_id: str, home: Path, *, json_out: bool = False) -> int:
    """Inspect ONE job - the CLI counterpart of the MCP ``job_status``.

    Without this, `vanth status <job-id>` silently ignored the argument and
    printed daemon health, and there was no single-job read at all (a blind
    onboarding test looked for exactly this).
    """
    client = VanthClient(home=home)
    try:
        client.ensure()
    except Exception as exc:
        print(f"vanth status: failed to reach daemon: {exc}", file=sys.stderr)
        return 1
    job_id, problem = _requiring_job_id(client, job_id, "status")
    if problem:
        return problem
    try:
        result = client.get(f"/jobs/{job_id}/status")
    except Exception as exc:
        print(f"vanth status: failed to reach daemon: {exc}", file=sys.stderr)
        return 1
    if not _expect_ok(result):
        print(f"vanth status: daemon error: {result.get('error') or result}", file=sys.stderr)
        return 1
    if json_out:
        print(json.dumps(result, indent=2, default=str))
        return 0
    print(f"job:       {result.get('job_id')}")
    print(f"  name:    {result.get('name')}")
    print(f"  status:  {result.get('status')}" + (f" (exit {result['exit_code']})" if result.get("exit_code") is not None else ""))
    runtime = result.get("runtime_seconds")
    if runtime is not None:
        print(f"  runtime: {_humanize(runtime)}")
    print(f"  command: {result.get('command')}")
    if result.get("cwd"):
        print(f"  cwd:     {result.get('cwd')}")
    if result.get("pid"):
        print(f"  pid:     {result.get('pid')}")
    if result.get("stop_reason"):
        print(f"  stopped: {result.get('stop_actor')}: {result.get('stop_reason')}")
    event = result.get("last_event") or {}
    if event.get("type"):
        print(f"  last:    {event.get('type')} seq {event.get('seq')} ({event.get('created_at')})")
    progress = result.get("progress") or {}
    if progress:
        print(f"  progress: {json.dumps({k: v for k, v in progress.items() if k != 'updated_at'}, default=str)}")
    return 0


def cmd_status(home: Path, argv: list[str] | None = None, *, json_out: bool = False) -> int:
    # `vanth status <job-id>` inspects one job; bare `vanth status` is daemon health.
    args = [arg for arg in (argv or []) if not arg.startswith("-")]
    if args:
        if len(args) > 1:
            print(f"vanth status: expected at most one job id, got {len(args)}", file=sys.stderr)
            return 2
        return cmd_job_status(args[0], home, json_out=json_out)
    disc = _discovery(home)
    url = disc["url"] if disc else None
    token_path = home / "token"
    token = token_path.read_text(encoding="utf-8").strip() if token_path.exists() else ""

    up = False
    health = None
    doctor = None
    if url and token:
        health = _health(url, token)
        if health is not None:
            up = True
            try:
                client = VanthClient(url, home)
                doctor = client.get("/doctor")
            except Exception:
                doctor = None

    if json_out:
        payload = {
            "up": up,
            "url": url,
            "pid": disc.get("pid") if disc else None,
            "daemon_schema_version": disc.get("schema_version") if disc else None,
            "started_at": disc.get("started_at") if disc else None,
            "package_version": _daemon_version(),
            "health": health,
            "doctor": doctor,
        }
        print(json.dumps(payload, indent=2, default=str))
        return 0 if up else 1

    running = []
    if up:
        try:
            client = VanthClient(url, home)
            running = client.get("/jobs", {"status": ["running"]}).get("jobs", [])
        except Exception:
            running = []

    print(f"vanth daemon: {'UP' if up else 'DOWN'}")
    print(f"  home:     {home}")
    print(f"  url:      {url or '(no daemon.json - never started)'}")
    if disc:
        print(f"  pid:      {disc.get('pid')}")
        print(f"  schema:   {disc.get('schema_version')}")
        print(f"  started:  {disc.get('started_at')}")
    print(f"  package:  {_daemon_version()}")
    if doctor:
        print(f"  running jobs: {len(running)}")
        for job in running[:10]:
            print(f"    - {job.get('job_id')}  {job.get('name') or ''}  ({job.get('status')})")
        print(f"  schema (db):  {doctor.get('schema_version')}")
        counts = doctor.get("delivery_counts") or {}
        if counts:
            print(f"  deliveries:   {counts}")
        if doctor.get("warnings"):
            print("  warnings:")
            for warning in doctor["warnings"]:
                print(f"    - {warning.get('type')}: {warning}")
    elif up:
        print("  doctor:   unreachable (auth/schema problem)")

    _print_setup_status()
    return 0 if up else 1


def _print_setup_status() -> None:
    """Show which MCP clients have the Vanth MCP server configured, so a user
    can see onboarding state at a glance and is pointed at `vanth setup`."""
    try:
        from .setup import client_config_paths, effective_state

        found = client_config_paths()
        if not found:
            print("  mcp:      no known client configs found - run `vanth setup`")
            return
        parts = []
        for client in ("opencode", "codex", "claude"):
            paths = found.get(client) or []
            if not paths:
                continue
            # Precedence-aware: an `enabled` entry in a lower-precedence file
            # must not read as "configured" when a higher one disables it.
            state = effective_state(client, paths)
            parts.append(f"{client}={'not configured' if state == 'not-configured' else state}")
        if not parts:
            print("  mcp:      no known client configs found - run `vanth setup`")
            return
        state = "  mcp:      " + ", ".join(parts)
        print(state)
        if any("not configured" in part for part in parts):
            print("            run `vanth setup` to register")
    except Exception:
        pass


def cmd_doctor(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    client = VanthClient(home=home)
    reap = "--reap-orphans" in argv
    try:
        client.ensure()
        if reap:
            result = client.post("/reap-orphans", {})
            if json_out:
                print(json.dumps(result, indent=2, default=str))
            else:
                print(f"vanth doctor: reaped {len(result.get('reaped', []))} orphaned MCP server(s)")
                for entry in result.get("failed", []):
                    print(f"  failed:      pid {entry['pid']}: {entry['error']}")
            return 0
        report = client.get("/doctor", {"verify_artifacts": True}) if "--verify-artifacts" in argv else client.get("/doctor")
    except Exception as exc:
        print(f"vanth doctor: failed to reach daemon: {exc}")
        return 1
    if json_out:
        print(json.dumps(report, indent=2, default=str))
    else:
        ok = report.get("ok")
        print(f"vanth doctor: {'OK' if ok else 'PROBLEM'}")
        print(f"  home:          {report.get('home')}")
        print(f"  schema:        {report.get('schema_version')}")
        print(f"  tables:        {len(report.get('tables', []))}")
        delivery_counts = report.get("delivery_counts") or {}
        print(f"  deliveries:    {delivery_counts}")
        failed_deliveries = delivery_counts.get("failed", 0)
        if failed_deliveries:
            print(
                f"  failed_wakes:  {failed_deliveries} failed delivery record(s) "
                "(inspect with `vanth deliveries --status failed`)"
            )
        print(f"  codex:         {'available' if report.get('codex', {}).get('available') else 'MISSING'}")
        print(f"  opencode:      {'available' if report.get('opencode', {}).get('available') else 'MISSING'}")
        print(f"  quick_check:   {report.get('quick_check')}")
        maintenance = report.get("maintenance_alive")
        print(f"  maintenance:   {'alive' if maintenance else 'DEAD - queues and deliveries are not draining'}")
        print(f"  disk_free:     {_fmt_bytes(report.get('disk_free_bytes', 0))}")
        relays = report.get("relays") or []
        relay_types = {"codex_desktop", "opencode_thread"}
        wake_relays = [r for r in relays if r.get("client_type") in relay_types]
        if wake_relays:
            live_relays = sum(bool(relay.get("live")) for relay in wake_relays)
            destination_ids = sorted({
                str(destination_id)
                for relay in wake_relays
                for destination in relay.get("destinations") or []
                if isinstance(destination, dict)
                if (destination_id := (destination.get("session_id") or destination.get("thread_id")))
            })
            shown_ids = ", ".join(destination_ids[:3]) or "none"
            if len(destination_ids) > 3:
                shown_ids += f", +{len(destination_ids) - 3} more"
            print(
                f"  relays:        {len(wake_relays)} total, {live_relays} live, "
                f"{len(wake_relays) - live_relays} stale; destinations: {shown_ids}"
            )
            if len(destination_ids) > 3:
                print("                 full destination list: vanth doctor --json")
        else:
            print("  relays:        none - codex_desktop/opencode_thread wakes cannot be delivered")
        undeliverable_wakes = report.get("undeliverable_wakes", 0)
        if undeliverable_wakes > 0:
            print(
                f"  wake_targets:  {undeliverable_wakes} undeliverable "
                "(these wakes can never fire; inspect with `vanth deliveries --status pending`)"
            )
        dead = report.get("dead_lettered") or []
        if dead:
            print(f"  dead_letters:  {report.get('dead_letter_count')} (inspect with `vanth deliveries --status failed`)")
            for entry in dead[:5]:
                print(f"    {entry.get('delivery_id')} job {entry.get('job_id')} "
                      f"attempts {entry.get('attempts')}: {_clean_text(entry.get('last_error'))}")
        orphaned = report.get("orphaned_mcp_servers") or []
        if orphaned:
            print(f"  orphaned_mcp: {len(orphaned)} (reap with `vanth doctor --reap-orphans`)")
            for entry in orphaned[:5]:
                print(f"    pid {entry['pid']} parent {entry['parent_pid']} started {entry['started']}")
        for warning in report.get("warnings", []):
            print(f"  WARNING:       {warning}")
        _print_setup_status()
    return 0 if report.get("ok") else 1


def _fmt_bytes(n: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if n < 1024 or unit == "TiB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n:.1f} TiB"


def _humanize(seconds: float) -> str:
    """Humanize a seconds count: "42s", "2m 3s", "1h", "3d 4h"."""
    if seconds is None:
        return ""
    seconds = int(max(0.0, float(seconds)))
    if seconds < 60:
        return f"{seconds}s"
    parts = []
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60)):
        if seconds >= size:
            count, seconds = divmod(seconds, size)
            parts.append(f"{count}{unit}")
        if len(parts) >= 2:
            break
    # The loop above stops after two units, so `seconds` can still be >= 60
    # (e.g. 5d 17h leaves minutes+seconds). Normalize it instead of printing a
    # raw seconds count: "5d 17h 1727s" was wrong, "5d 17h 28m 47s" is not.
    if parts and seconds:
        if seconds >= 60:
            minutes, seconds = divmod(seconds, 60)
            parts.append(f"{minutes}m")
        if seconds:
            parts.append(f"{seconds}s")
    return " ".join(parts) if parts else f"{seconds}s"


def _parse_iso_ts(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _age_from(value: str | None) -> str:
    """Humanized age since an ISO timestamp (blank when unparsable)."""
    if not value:
        return ""
    parsed = _parse_iso_ts(value)
    if parsed is None:
        return ""
    delta = (datetime.now(timezone.utc) - parsed).total_seconds()
    return _humanize(max(0.0, delta))


def cmd_restart(home: Path, *, json_out: bool = False) -> int:
    """Gracefully stop the daemon (if running) and start it again fresh.

    In-flight jobs are owned by detached runner processes, so they survive the
    daemon restart; the new daemon reconciles them on startup. This is the
    reliable way to pick up a code/version update.
    """
    disc = _discovery(home)
    url = disc["url"] if disc else None
    token_path = home / "token"
    token = token_path.read_text(encoding="utf-8").strip() if token_path.exists() else ""
    was_up = bool(url and token and _health(url, token) is not None)
    old_pid = disc.get("pid") if disc else None

    if was_up:
        # Ask the daemon to shut down gracefully.
        try:
            request = urllib.request.Request(
                url + "/shutdown",
                data=b"{}",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=5) as response:
                response.read()
        except urllib.error.HTTPError as exc:
            print(f"vanth restart: warning: shutdown returned HTTP {exc.code}: {exc.read()!r}", file=sys.stderr)
        except Exception as exc:
            print(f"vanth restart: warning: shutdown request failed: {exc}", file=sys.stderr)
        # Wait until the old daemon is truly gone: port closed, discovery
        # metadata removed, and the old process (if known) has exited so the
        # home lock is released before the new daemon starts.
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            health_down = _health(url, token) is None
            metadata_gone = not (home / "daemon.json").exists()
            pid_gone = True
            if old_pid and sys.platform == "win32":
                pid_gone = not _pid_alive(old_pid)
            elif old_pid:
                pid_gone = not _pid_alive(old_pid)
            if health_down and metadata_gone and pid_gone:
                break
            time.sleep(0.2)

    # Start fresh and verify a new process is actually serving.
    client = VanthClient(home=home)
    try:
        # The old process may still be releasing its home lock; retry briefly.
        last_error: Exception | None = None
        doctor = None
        for _ in range(20):
            try:
                client.ensure()
                doctor = client.get("/doctor")
                break
            except Exception as exc:  # noqa: BLE001 - retry transient lock races
                last_error = exc
                time.sleep(0.25)
        if doctor is None:
            raise last_error or RuntimeError("vanthd did not start")
    except Exception as exc:
        if json_out:
            print(json.dumps({"ok": False, "error": str(exc)}))
        else:
            print(f"vanth restart: failed to start daemon: {exc}")
        return 1
    if json_out:
        print(json.dumps({"ok": True, "restarted_from_running": was_up, "schema_version": doctor.get("schema_version")}))
    else:
        print(f"vanth restart: daemon {'restarted' if was_up else 'started'} (schema v{doctor.get('schema_version')})")
    return 0


def cmd_setup(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    from .setup import remove_codex_desktop, register_codex_desktop, run_setup

    if "-h" in argv or "--help" in argv:
        print(
            "usage: vanth setup [opencode] [codex] [claude] [desktop] [--remove] [--yes]\n"
            "\n"
            "Register (or remove) the Vanth MCP server in the given clients' configs.\n"
            "With no client names, detects and configures every known client found.\n"
            "`vanth setup desktop` provisions Codex Desktop wake by writing the\n"
            "capability file from the current Desktop session's app-tools pipe.\n"
            "\n"
            "options:\n"
            "  --remove, -r   remove the Vanth MCP entry instead of adding it\n"
            "  --yes, -y      do not prompt; apply immediately\n"
            "  --json         machine-readable output\n"
        )
        return 0
    remove = "--remove" in argv or "-r" in argv
    assume_yes = "--yes" in argv or "-y" in argv
    if "desktop" in argv:
        if json_out:
            changed, summary = (remove_codex_desktop if remove else register_codex_desktop)(home)
            print(json.dumps({"ok": changed or summary.startswith("Desktop wake provisioned"), "changed": changed, "summary": summary}))
            return 0 if (changed or summary.startswith("Desktop wake provisioned")) else 1
        changed, summary = (remove_codex_desktop if remove else register_codex_desktop)(home)
        print(f"vanth setup desktop: {'removed' if remove else 'provisioned'}: {summary}")
        return 0 if (changed or summary.startswith("Desktop wake provisioned")) else 1
    clients = [arg for arg in argv if arg in {"opencode", "codex", "claude"}]
    return run_setup(clients or None, home=home, remove=remove, assume_yes=assume_yes, json_out=json_out)


def cmd_autostart(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    """`vanth autostart enable|disable|status [--dry-run] [--yes] [--json]`."""
    if not argv or argv[0] in {"--help", "-h"}:
        print(
            "usage: vanth autostart enable|disable|status [--dry-run] [--yes] [--json]\n"
            "\n"
            "Register the Vanth daemon as a background service so it survives\n"
            "reboots (Windows Task Scheduler / macOS launchd / Linux systemd).\n"
            "\n"
            "actions:\n"
            "  status         show whether autostart is registered\n"
            "  enable         register the daemon to start automatically\n"
            "  disable        remove the autostart registration\n"
            "\n"
            "options:\n"
            "  --dry-run      show what would happen without changing anything\n"
            "  --yes, -y      do not prompt; apply immediately\n"
            "  --json         machine-readable output\n"
        )
        return 0
    action = argv[0]
    rest = argv[1:]
    dry_run = "--dry-run" in rest
    assume_yes = "--yes" in rest or "-y" in rest
    if action == "status":
        state = autostart.detect(home)
        if json_out:
            print(json.dumps(state, indent=2, default=str))
        else:
            enabled = state.get("enabled", False)
            print(f"vanth autostart: {'enabled' if enabled else 'disabled'}")
            print(f"  platform: {state.get('platform')}")
            print(f"  target:   {state.get('target')}")
            if state.get("error"):
                print(f"  error:    {state['error']}")
        return 0 if state.get("enabled") else 1
    if action == "enable":
        if not assume_yes and not dry_run:
            answer = input("Install Vanth autostart? [y/N] ").strip().lower()
            if answer not in {"y", "yes"}:
                print("vanth autostart: declined")
                return 0
        try:
            result = autostart.enable(home, dry_run=dry_run)
        except Exception as exc:
            print(f"vanth autostart: enable failed: {exc}", file=sys.stderr)
            return 1
        if json_out:
            print(json.dumps(result, indent=2, default=str))
        else:
            if result.get("dry_run"):
                print(f"vanth autostart: would install: {result.get('would_install')}")
            else:
                print(f"vanth autostart: enabled ({result.get('target')})")
        return 0
    if action == "disable":
        if not assume_yes and not dry_run:
            answer = input("Remove Vanth autostart? [y/N] ").strip().lower()
            if answer not in {"y", "yes"}:
                print("vanth autostart: declined")
                return 0
        try:
            result = autostart.disable(home, dry_run=dry_run)
        except Exception as exc:
            print(f"vanth autostart: disable failed: {exc}", file=sys.stderr)
            return 1
        if json_out:
            print(json.dumps(result, indent=2, default=str))
        else:
            if result.get("dry_run"):
                print(f"vanth autostart: would uninstall: {result.get('would_uninstall')}")
            else:
                print(f"vanth autostart: disabled ({result.get('target')})")
        return 0
    print(f"vanth: unknown autostart action {action!r}", file=sys.stderr)
    return 2


def cmd_version() -> int:
    print(_daemon_version())
    return 0


_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def _clean_text(value: Any) -> str:
    """Make stored text safe to print: drop ANSI escapes and carriage returns.

    Delivery errors are stored verbatim from the client they came from (an
    OpenCode failure arrives as ``\x1b[91m\x1b[1mError:\x1b[0m Session not
    found``), which renders as raw escape codes in `vanth doctor`/`deliveries`.
    Job output keeps its CRLF line endings, which show up as stray ``\\r``.
    """
    if value is None:
        return ""
    text = _ANSI_RE.sub("", str(value))
    return text.replace("\r\n", "\n").replace("\r", "")


def _job_error(message: str) -> dict[str, Any]:
    return {"result": "error", "error": message}


def _expect_ok(payload: dict[str, Any]) -> bool:
    return bool(payload and payload.get("result") != "error")


def cmd_list(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    statuses: list[str] = []
    limit = 50
    include_all = False
    thread_id: str | None = None
    name: str | None = None
    tags: list[str] = []
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg == "--status":
            i += 1
            if i >= len(argv):
                print("vanth list: --status requires a value", file=sys.stderr)
                return 2
            statuses.extend(item.strip() for item in argv[i].split(",") if item.strip())
        elif arg == "--limit":
            i += 1
            if i >= len(argv):
                print("vanth list: --limit requires a value", file=sys.stderr)
                return 2
            try:
                limit = int(argv[i])
            except ValueError:
                print(f"vanth list: invalid --limit value {argv[i]!r}", file=sys.stderr)
                return 2
        elif arg in {"--thread-id", "--name", "--tag"}:
            i += 1
            if i >= len(argv):
                print(f"vanth list: {arg} requires a value", file=sys.stderr)
                return 2
            if arg == "--thread-id":
                thread_id = argv[i]
            elif arg == "--name":
                name = argv[i]
            else:
                tags.append(argv[i])
        elif arg == "--all":
            include_all = True
        else:
            print(f"vanth list: unknown option {arg!r}", file=sys.stderr)
            return 2
        i += 1
    if include_all and statuses:
        print("vanth list: --status and --all are mutually exclusive", file=sys.stderr)
        return 2
    # Resolve the status set BEFORE the request so the server's LIMIT applies to
    # the set being shown. Filtering a truncated page locally (the old behavior)
    # silently dropped matching jobs once newer non-matching ones filled the
    # window, and made an explicit `--status X` return nothing.
    if include_all:
        statuses = sorted(_TERMINAL_STATUSES)
    elif not statuses:
        statuses = list(_INFLIGHT_STATUSES)
    client = VanthClient(home=home)
    try:
        client.ensure()
        payload = client.get(
            "/jobs",
            {"status": statuses, "limit": limit, "thread_id": thread_id, "name": name, "tags": tags or None},
        )
    except Exception as exc:
        print(f"vanth list: failed to reach daemon: {exc}", file=sys.stderr)
        return 1
    if not _expect_ok(payload):
        print(f"vanth list: daemon error: {payload.get('error') or payload}", file=sys.stderr)
        return 1
    jobs = payload.get("jobs") or []
    if json_out:
        print(json.dumps(jobs, indent=2, default=str))
        return 0
    if not jobs:
        # The default filter is in-flight-only, so an empty result is NOT "there
        # are no jobs": saying so made a finished job look like it vanished.
        if set(statuses) == set(_INFLIGHT_STATUSES):
            print("vanth list: no in-flight jobs (use --all to include finished jobs)")
        else:
            print(f"vanth list: no jobs with status {', '.join(statuses)}")
        return 0
    jobs = sorted(jobs, key=lambda job: job.get("updated_at") or "", reverse=True)
    print(f"{'STATUS':<10} {'JOB ID':<24} {'NAME':<28} {'DURATION':<10} {'EXIT':<6} AGE")
    for job in jobs:
        # runtime_seconds is derived server-side from started_at/ended_at, so a
        # running job shows a live figure and a finished one stops counting.
        runtime = job.get("runtime_seconds")
        duration = _humanize(runtime) if runtime is not None else ""
        exit_code = job.get("exit_code")
        exit_text = "" if exit_code is None else str(exit_code)
        age = _age_from(job.get("created_at") or job.get("updated_at"))
        print(
            f"{(job.get('status') or ''):<10} {(job.get('job_id') or ''):<24} "
            f"{(job.get('name') or ''):<28} {duration:<10} {exit_text:<6} {age}"
        )
    return 0


#: cmd.exe metacharacters that are literal INSIDE double quotes, so wrapping a
#: token in quotes neutralises them. ``list2cmdline`` quotes only for the CRT
#: argv parser, not for cmd.exe, so it leaves a bare ``&``/``|`` active.
_CMD_SAFE_IN_QUOTES = set("&|<>^()")
#: cmd.exe expands these even inside double quotes, and a double quote cannot be
#: encoded unambiguously; we refuse to guess rather than corrupt or inject.
_CMD_UNENCODABLE = set('%!"\r\n')


def _quote_for_cmd(token: str) -> str:
    if any(char in _CMD_UNENCODABLE for char in token):
        raise ValueError(token)
    if token == "":
        # An empty argv element is meaningful; it must be quoted or the shell
        # collapses it and every later argument shifts position.
        return '""'
    needs_quote = any(char in " \t" for char in token) or any(
        char in _CMD_SAFE_IN_QUOTES for char in token
    )
    if not needs_quote:
        return token
    if token.endswith("\\"):
        # The CRT argv parser would treat the doubled backslash as escaping the
        # closing quote; refuse instead of mis-encoding.
        raise ValueError(token)
    return f'"{token}"'


#: A shell operator/redirect that arrived as its OWN argv token is the mangling
#: signature: ``_quote_for_cmd`` then wraps it in quotes, so cmd.exe sees a
#: literal ``">nul"``/``"&&"`` argument instead of a redirect/chain and the job
#: silently does the wrong thing. An operator INSIDE a larger token (``rg
#: "a|b"``) is quoted as a whole and is therefore safe, so it must NOT be
#: refused.
_SHELL_OPERATOR_TOKENS = {"&", "&&", "|", "||", ">", ">>", "<", "<<"}


def _is_shell_operator_token(token: str) -> bool:
    stripped = token.strip()
    if not stripped:
        return False
    # A bare operator, or an output redirect (`>nul`, `>>log`) — a redirect is
    # unambiguously shell syntax, whereas a leading `<` (`<html>`, `<foo>`) is
    # commonly literal data, so it is refused only when the token IS the
    # operator.
    if stripped in _SHELL_OPERATOR_TOKENS or stripped.startswith(">"):
        return True
    # A file-descriptor redirect (`2>`, `2>&1`, `1>>log`) starts with a digit
    # before the operator.
    return bool(re.match(r"^\d+(>>?|<<?)", stripped))


def _join_command(tokens: list[str]) -> str:
    """Reassemble argv into a shell command string using the HOST shell's quoting.

    The runner executes the command through the platform shell
    (``Popen(command, shell=True)``), so re-quote for that same shell. Raises
    ``ValueError(token)`` for a Windows argument that cannot be encoded safely
    (the caller reports it and tells the user to pass one quoted string).
    """
    if os.name == "nt":
        return " ".join(_quote_for_cmd(token) for token in tokens)
    import shlex

    return shlex.join(tokens)


def _load_json_object(value: str, arg: str) -> dict[str, Any]:
    """Parse a JSON object from a literal, a file path (``@path``), or stdin (``-``).

    The file/stdin forms exist because PowerShell 5.1 strips the quotes from a
    JSON literal handed to a native executable, so ``--wake '{...}'`` is
    unusable there. ``--wake @wake.json`` (or ``--wake -``) is the reliable form
    on any shell. Raises ``ValueError`` with an actionable message.
    """
    if value == "-":
        text = sys.stdin.read()
    elif value.startswith("@"):
        path = value[1:]
        try:
            text = Path(path).read_text(encoding="utf-8")
        except OSError as exc:
            raise ValueError(f"{arg}: cannot read {path!r}: {exc}") from exc
    else:
        text = value
    try:
        parsed = json.loads(text)
    except ValueError:
        raise ValueError(
            f"{arg} expects a JSON object, got {value!r} "
            "(pass a literal, `@path` to read a file, or `-` for stdin)"
        ) from None
    if not isinstance(parsed, dict):
        raise ValueError(f"{arg} expects a JSON object")
    return parsed


def cmd_start(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    """Start a background job without the MCP tools.

    Usage:
      vanth start [--name N] [--cwd DIR] [--env KEY=VAL]... [--timeout SECONDS]
                  [--wake JSON]... [--wake-me[=EVENTS]] [--interactive] [--] <command...>

    The command is the first non-option argument onward. A single argument is
    used verbatim; multiple arguments are re-quoted for the host shell so the
    program and its arguments survive. Use ``--`` before a command that starts
    with a dash (or to keep a literal ``--json``).
    """
    payload: dict[str, Any] = {}
    dry_run = False
    env: dict[str, str] = {}
    wake: list[dict[str, Any]] = []
    wake_me_events: list[str] | None = None
    tags: list[str] = []
    secret_env: list[str] = []
    command_tokens: list[str] = []
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg == "--":
            command_tokens = list(argv[i + 1:])
            break
        if arg == "--wake-me" or arg.startswith("--wake-me="):
            value = arg.partition("=")[2] if "=" in arg else "completed,failed,timeout,cancelled,orphaned"
            parts = value.split(",")
            events = [part.strip() for part in parts]
            if not value or any(not event or not re.fullmatch(r"[A-Za-z0-9_.:-]+", event) for event in events):
                print(
                    "vanth start: --wake-me expects a non-empty comma-separated event list "
                    "(e.g. completed,failed)",
                    file=sys.stderr,
                )
                return 2
            wake_me_events = events
        elif arg in {
            "--name", "--cwd", "--timeout", "--env", "--wake", "--priority",
            "--pool", "--tag", "--notes", "--secret-env", "--trigger", "--policy", "--idempotency-key",
        }:
            i += 1
            if i >= len(argv):
                print(f"vanth start: {arg} requires a value", file=sys.stderr)
                return 2
            value = argv[i]
            if arg == "--idempotency-key":
                payload["idempotency_key"] = value
            elif arg == "--name":
                payload["name"] = value
            elif arg == "--cwd":
                payload["cwd"] = value
            elif arg == "--notes":
                payload["notes"] = value
            elif arg == "--pool":
                payload["pool"] = value
            elif arg == "--tag":
                tags.append(value)
            elif arg == "--secret-env":
                secret_env.append(value)
            elif arg == "--priority":
                try:
                    payload["priority"] = int(value)
                except ValueError:
                    print(f"vanth start: invalid --priority value {value!r}", file=sys.stderr)
                    return 2
            elif arg in {"--trigger", "--policy"}:
                try:
                    parsed = _load_json_object(value, arg)
                except ValueError as exc:
                    print(f"vanth start: {exc}", file=sys.stderr)
                    return 2
                payload["trigger" if arg == "--trigger" else "policy"] = parsed
            elif arg == "--timeout":
                try:
                    seconds = int(value)
                except ValueError:
                    print(f"vanth start: invalid --timeout value {value!r}", file=sys.stderr)
                    return 2
                if seconds < 1:
                    print("vanth start: --timeout must be >= 1", file=sys.stderr)
                    return 2
                payload["timeout_seconds"] = seconds
            elif arg == "--env":
                key, sep, val = value.partition("=")
                if not sep or not key:
                    print(f"vanth start: --env expects KEY=VALUE, got {value!r}", file=sys.stderr)
                    return 2
                env[key] = val
            elif arg == "--wake":
                try:
                    target = _load_json_object(value, "--wake")
                except ValueError as exc:
                    print(f"vanth start: {exc}", file=sys.stderr)
                    return 2
                wake.append(target)
        elif arg == "--dry-run":
            dry_run = True
        elif arg == "--interactive":
            payload["interactive"] = True
        elif arg.startswith("--"):
            print(f"vanth start: unknown option {arg!r}", file=sys.stderr)
            return 2
        else:
            command_tokens = list(argv[i:])
            break
        i += 1
    if not command_tokens or not any(token.strip() for token in command_tokens):
        print("vanth start: missing command (e.g. `vanth start -- make -j8`)", file=sys.stderr)
        return 2
    # A single argument is the command as written (`vanth start "python -c
    # 'print(1)'"` is one argv element and needs no reassembly). Multiple
    # arguments are reassembled with the host shell's quoting so the program and
    # its arguments survive - a naive space join would drop the quotes.
    try:
        command = command_tokens[0] if len(command_tokens) == 1 else _join_command(command_tokens)
    except ValueError as exc:
        print(
            f"vanth start: argument {exc.args[0]!r} contains shell metacharacters that cannot be "
            "safely re-quoted on Windows; pass the whole command as ONE quoted string instead "
            '(e.g. `vanth start "..."`)',
            file=sys.stderr,
        )
        return 2
    # Shell operators only mean something to the shell, and reassembling them
    # from separate argv elements is where quoting goes wrong (the reported
    # failure: PowerShell 5.1 split a single-quoted command containing inner
    # quotes into garbage argv, and the joined command then died in cmd.exe).
    # Refuse instead of silently running something the caller did not mean.
    # Keyed on TOKENS, not the joined string: a bare `>nul`/`&&` argv element
    # was meant as shell syntax but `_quote_for_cmd` neutralises it into a
    # literal argument, whereas an operator inside a larger token (`rg "a|b"`)
    # is safe and must be allowed.
    if len(command_tokens) > 1 and any(_is_shell_operator_token(token) for token in command_tokens):
        print(
            "vanth start: refusing reassembled command (a bare shell operator was passed as its own argument):\n"
            f"  {command}\n"
            "  Pass the whole command as ONE quoted string - "
            '`vanth start "cmd /c ping -n 3 host >nul && echo done"` -\n'
            "  or a script file: write the steps to `run.cmd` and `vanth start -- run.cmd`.",
            file=sys.stderr,
        )
        return 2
    payload["command"] = command
    payload.setdefault("cwd", os.getcwd())
    if env:
        payload["env"] = env
    if wake_me_events is not None:
        # Resolve the live plugin relay for the CALLER's directory (the job's
        # --cwd, else this process's cwd) — an omitted cwd would otherwise match
        # the newest relay of ANY project and wake an unrelated session.
        wake.append({
            "type": "opencode_thread",
            "events": wake_me_events,
            "cwd": payload.get("cwd") or os.getcwd(),
        })
    if wake:
        payload["wake_targets"] = wake
    if tags:
        payload["tags"] = tags
    if secret_env:
        payload["secret_env"] = secret_env
    client = VanthClient(home=home)
    try:
        client.ensure()
        result = client.post("/jobs/preview" if dry_run else "/jobs", payload)
        if not dry_run:
            result = client.confirm_local_start(result)
    except Exception as exc:
        print(f"vanth start: failed to reach daemon: {exc}", file=sys.stderr)
        return 1
    if not _expect_ok(result):
        print(f"vanth start: {result.get('error') or result}", file=sys.stderr)
        return 1
    for warning in result.get("warnings") or []:
        print(f"vanth start: warning: {warning}", file=sys.stderr)
    if json_out or dry_run:
        print(json.dumps(result, indent=2, default=str))
    else:
        status = result.get("status")
        verb = "queued" if status == "queued" else "failed to start" if status in {"failed", "lost"} and result.get("startup_confirmed") is False else "started"
        print(f"{verb} {result.get('job_id')} ({status})")
        print(f"  watch: vanth logs {result.get('job_id')}")
    return 1 if result.get("status") in {"failed", "lost", "timeout", "cancelled", "orphaned"} else 0


def cmd_sleep(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    """Start a background job that sleeps for the requested seconds."""
    if len(argv) != 1:
        print("vanth sleep: expected exactly one positive number of seconds", file=sys.stderr)
        return 2
    if not re.fullmatch(r"[0-9]+", argv[0].strip()) or int(argv[0]) <= 0:
        print("vanth sleep: expected exactly one positive number of seconds", file=sys.stderr)
        return 2
    seconds = int(argv[0])
    return cmd_start(
        [
            "--name", f"sleep-{seconds}s",
            "--timeout", str(seconds + 60),
            "--", sys.executable, "-c", f"import time; time.sleep({seconds})",
        ],
        home,
        json_out=json_out,
    )


def cmd_logs(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    if not argv:
        print("vanth logs: missing job id", file=sys.stderr)
        return 2
    job_id = argv[0]
    stream = "stdout"
    max_bytes = 8192
    offset: int | None = None
    grep: str | None = None
    i = 1
    while i < len(argv):
        arg = argv[i]
        if arg == "--stream":
            i += 1
            if i >= len(argv):
                print("vanth logs: --stream requires a value", file=sys.stderr)
                return 2
            value = argv[i]
            if value not in {"stdout", "stderr", "all"}:
                print("vanth logs: --stream must be stdout, stderr, or all", file=sys.stderr)
                return 2
            stream = value
        elif arg == "--max-bytes":
            i += 1
            if i >= len(argv):
                print("vanth logs: --max-bytes requires a value", file=sys.stderr)
                return 2
            try:
                max_bytes = int(argv[i])
            except ValueError:
                print(f"vanth logs: invalid --max-bytes value {argv[i]!r}", file=sys.stderr)
                return 2
        elif arg == "--offset":
            i += 1
            if i >= len(argv):
                print("vanth logs: --offset requires a value", file=sys.stderr)
                return 2
            try:
                offset = int(argv[i])
            except ValueError:
                print(f"vanth logs: invalid --offset value {argv[i]!r}", file=sys.stderr)
                return 2
        elif arg == "--grep":
            i += 1
            if i >= len(argv):
                print("vanth logs: --grep requires a value", file=sys.stderr)
                return 2
            grep = argv[i]
        else:
            print(f"vanth logs: unknown option {arg!r}", file=sys.stderr)
            return 2
        i += 1
    client = VanthClient(home=home)
    try:
        client.ensure()
    except Exception as exc:
        print(f"vanth logs: failed to reach daemon: {exc}", file=sys.stderr)
        return 1
    job_id, problem = _requiring_job_id(client, job_id, "logs")
    if problem:
        return problem
    streams = ["stdout", "stderr"] if stream == "all" else [stream]
    results: list[dict[str, Any]] = []
    for name in streams:
        params: dict[str, Any] = {"stream": name, "max_bytes": max_bytes}
        if offset is not None:
            params["offset"] = offset
        if grep is not None:
            params["grep"] = grep
        try:
            result = client.get(f"/jobs/{job_id}/tail", params)
        except Exception as exc:
            print(f"vanth logs: failed to reach daemon: {exc}", file=sys.stderr)
            return 1
        if not _expect_ok(result):
            error = result.get("error") or ""
            if "unknown" in error.lower() or "job_id" in error.lower() or "not found" in error.lower():
                print(f"vanth logs: unknown job {job_id}", file=sys.stderr)
                return 1
            print(f"vanth logs: daemon error: {error}", file=sys.stderr)
            return 1
        results.append(result)
    if json_out:
        print(json.dumps(results[0] if len(results) == 1 else results, indent=2, default=str))
        return 0
    for result in results:
        # Normalize CRLF: job output captured on Windows otherwise shows stray
        # carriage returns when piped or captured by a tool.
        print(_clean_text(result.get("content")), end="")
    return 0


def cmd_wait(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    """Block until a job emits one of the requested events (or the timeout).

    The CLI counterpart of the MCP ``job_wait``: without an MCP client there was
    no way to wait, so the documented "don't poll" workflow was unusable from a
    shell (an onboarding test fell back to `sleep` + `list`).
    """
    if not argv:
        print("vanth wait: missing job id", file=sys.stderr)
        return 2
    job_id = argv[0]
    events = ["completed", "failed", "timeout", "cancelled", "orphaned"]
    timeout_seconds = 3600
    since_event_id: str | None = None
    i = 1
    while i < len(argv):
        arg = argv[i]
        if arg == "--events":
            i += 1
            if i >= len(argv):
                print("vanth wait: --events requires a value", file=sys.stderr)
                return 2
            events = [item.strip() for item in argv[i].split(",") if item.strip()]
            if not events:
                print("vanth wait: --events must name at least one event type", file=sys.stderr)
                return 2
        elif arg == "--timeout":
            i += 1
            if i >= len(argv):
                print("vanth wait: --timeout requires a value", file=sys.stderr)
                return 2
            try:
                timeout_seconds = int(argv[i])
            except ValueError:
                print(f"vanth wait: invalid --timeout value {argv[i]!r}", file=sys.stderr)
                return 2
            if timeout_seconds < 0 or timeout_seconds > 86400:
                print("vanth wait: --timeout must be between 0 and 86400 seconds", file=sys.stderr)
                return 2
        elif arg == "--since-event-id":
            i += 1
            if i >= len(argv):
                print("vanth wait: --since-event-id requires a value", file=sys.stderr)
                return 2
            since_event_id = argv[i]
        else:
            print(f"vanth wait: unknown option {arg!r}", file=sys.stderr)
            return 2
        i += 1
    client = VanthClient(home=home)
    try:
        client.ensure()
    except Exception as exc:
        print(f"vanth wait: failed to reach daemon: {exc}", file=sys.stderr)
        return 1
    job_id, problem = _requiring_job_id(client, job_id, "wait")
    if problem:
        return problem
    try:
        result = client.post(
            f"/jobs/{job_id}/wait",
            {"filters": events, "since_event_id": since_event_id, "timeout_seconds": timeout_seconds},
            timeout=float(timeout_seconds) + 30,
        )
    except Exception as exc:
        print(f"vanth wait: failed to reach daemon: {exc}", file=sys.stderr)
        return 1
    if not _expect_ok(result):
        print(f"vanth wait: daemon error: {result.get('error') or result}", file=sys.stderr)
        return 1
    if json_out:
        print(json.dumps(result, indent=2, default=str))
        return 0
    outcome = result.get("result")
    if outcome == "timeout":
        print(f"vanth wait: timed out after {timeout_seconds}s waiting for {', '.join(events)}")
        return 3
    event = result.get("event") or {}
    print(
        f"vanth wait: {event.get('type')} "
        f"(job {event.get('job_id') or job_id}, seq {event.get('seq')})"
        + (f": {event.get('message')}" if event.get("message") else "")
    )
    return 0


def cmd_diff(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    if len(argv) < 2:
        print("vanth diff: requires two job ids: <job> <other>", file=sys.stderr)
        return 2
    base_job_id, other_job_id = argv[0], argv[1]
    client = VanthClient(home=home)
    try:
        client.ensure()
    except Exception as exc:
        print(f"vanth diff: failed to reach daemon: {exc}", file=sys.stderr)
        return 1
    base_job_id, problem = _requiring_job_id(client, base_job_id, "diff")
    if problem:
        return problem
    other_job_id, problem = _requiring_job_id(client, other_job_id, "diff")
    if problem:
        return problem
    try:
        result = client.get(f"/jobs/{base_job_id}/diff", {"other": other_job_id})
    except Exception as exc:
        print(f"vanth diff: failed to reach daemon: {exc}", file=sys.stderr)
        return 1
    if not _expect_ok(result):
        error = result.get("error") or ""
        if "unknown" in error.lower() or "job_id" in error.lower():
            print(f"vanth diff: unknown job (base={base_job_id} other={other_job_id})", file=sys.stderr)
            return 1
        print(f"vanth diff: daemon error: {error}", file=sys.stderr)
        return 1
    if json_out:
        print(json.dumps(result, indent=2, default=str))
        return 0
    if result.get("identical"):
        print(f"vanth diff: {base_job_id} and {other_job_id} are identical")
        return 0
    print(f"vanth diff: {base_job_id} vs {other_job_id}")
    for change in result.get("changes") or []:
        field = change["field"]
        if field == "env":
            print(f"\n  env ({len(change.get('changes') or [])} keys changed):")
            for entry in change.get("changes") or []:
                print(f"    {entry['key']}: {entry.get('base')!r} -> {entry.get('other')!r}")
        else:
            print(f"  {field}: {change.get('base')!r} -> {change.get('other')!r}")
    return 0


def cmd_remote(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    """`vanth remote <pair|list|doctor|remove>` - remote execution pairing."""
    if not argv or argv[0] in {"--help", "-h", "help"}:
        print("usage: vanth remote <pair|list|doctor|remove|pending|retry> [options]\n"
              "  pair <target>   pair a remote host (user@host[:port]) [--name N] [--allow-root]\n"
              "  list            list paired remotes\n"
              "  doctor          report SSH binaries and remote state [--remote <id>]\n"
              "  remove <id>     remove a remote [--yes]\n"
              "  pending         list unresolved client requests [--remote <id>]\n"
              "  retry <req_id>  re-run an unresolved request with its original key", file=sys.stderr)
        return 2 if not argv else 0
    sub = argv[0]
    client = VanthClient(home=home)
    if sub == "pending":
        from .remote.journal import RequestJournal

        remote_filter = None
        if "--remote" in argv:
            i = argv.index("--remote")
            if i + 1 < len(argv):
                remote_filter = argv[i + 1]
        journal = RequestJournal(home / "client-requests.sqlite")
        try:
            rows = journal.pending(remote_filter)
        finally:
            journal.close()
        if json_out:
            print(json.dumps({"pending": rows}, indent=2, default=str))
            return 0
        if not rows:
            print("vanth remote pending: no unresolved requests")
            return 0
        for row in rows:
            print(f"  {row['request_id']}  {row['remote_id']}  {row['method']}  key={row['idempotency_key']}  since={row['created_at']}")
        return 0
    if sub == "retry":
        from .remote.journal import RequestJournal

        targets = [a for a in argv[1:] if not a.startswith("--")]
        if not targets:
            print("vanth remote retry: missing request_id", file=sys.stderr)
            return 2
        request_id = targets[0]
        journal = RequestJournal(home / "client-requests.sqlite")
        try:
            entry = journal.get(request_id)
        finally:
            journal.close()
        if not entry:
            print(f"vanth remote retry: unknown request_id {request_id}", file=sys.stderr)
            return 1
        import sqlite3 as _sqlite3

        from .remote.control import RemoteControl
        from .remote.store import RemoteStore

        db = _sqlite3.connect(home / "remote.sqlite")
        db.row_factory = _sqlite3.Row
        db.execute("PRAGMA busy_timeout=30000")
        store = RemoteStore(db)
        control = RemoteControl(store)
        # Re-submit with the ORIGINAL method/payload/key: replay returns the
        # same durable request (never a second job), then run it.
        request = control.submit(
            entry["remote_id"], entry["method"], entry["payload"] or {},
            idempotency_key=entry["idempotency_key"],
            expected_state_epoch=entry.get("expected_state_epoch"),
            expected_instance_id=entry.get("expected_instance_id"),
        )
        result = control.run_request(entry["remote_id"], request)
        if json_out:
            print(json.dumps(result, indent=2, default=str))
        else:
            print(f"vanth remote retry: {request_id} status={result.get('status')}")
        return 0 if result.get("status") in ("completed", "failed") else 1
    if sub == "pair":
        args = argv[1:]
        allow_root = "--allow-root" in args
        accept_host_key = "--accept-host-key" in args
        host_fingerprint = None
        if "--host-fingerprint" in args:
            i = args.index("--host-fingerprint")
            if i + 1 < len(args):
                host_fingerprint = args[i + 1]
        name = None
        if "--name" in args:
            i = args.index("--name")
            if i + 1 < len(args):
                name = args[i + 1]
        helper_command = None
        if "--helper-command" in args:
            i = args.index("--helper-command")
            if i + 1 < len(args):
                helper_command = args[i + 1]
        remote_home = None
        if "--remote-home" in args:
            i = args.index("--remote-home")
            if i + 1 < len(args):
                remote_home = args[i + 1]
        targets = [a for a in args if not a.startswith("--")]
        if not targets:
            print("vanth remote pair: missing target (user@host[:port])", file=sys.stderr)
            return 2
        target = targets[0]
        try:
            client.ensure()
            result = client.post("/remotes/pair", {
                "target": target, "name": name, "allow_root": allow_root,
                "accept_host_key": accept_host_key,
                "host_fingerprint": host_fingerprint,
                "helper_command": helper_command,
                "remote_home": remote_home,
            })
        except Exception as exc:
            print(f"vanth remote pair: failed to reach daemon: {exc}", file=sys.stderr)
            return 1
        if result.get("result") == "error" or "error" in result:
            print(f"vanth remote pair: {result.get('error', 'pairing failed')}", file=sys.stderr)
            return 1
        if json_out:
            print(json.dumps(result, indent=2, default=str))
            return 0
        print(f"vanth remote pair: paired {target} ({result.get('remote_id')})")
        return 0
    if sub == "list":
        try:
            client.ensure()
            result = client.get("/remotes")
        except Exception as exc:
            print(f"vanth remote list: failed to reach daemon: {exc}", file=sys.stderr)
            return 1
        remotes = result.get("remotes", result if isinstance(result, list) else [])
        if json_out:
            print(json.dumps(result, indent=2, default=str))
            return 0
        if not remotes:
            print("vanth remote list: no remotes paired")
            return 0
        for row in remotes:
            print(f"  {row.get('remote_id')}  {row.get('target')}  state={row.get('state')}")
        return 0
    if sub == "doctor":
        remote_id = None
        if "--remote" in argv:
            i = argv.index("--remote")
            if i + 1 < len(argv):
                remote_id = argv[i + 1]
        try:
            client.ensure()
            result = client.get("/remotes/doctor", {"remote_id": remote_id} if remote_id else None)
        except Exception as exc:
            print(f"vanth remote doctor: failed to reach daemon: {exc}", file=sys.stderr)
            return 1
        if json_out:
            print(json.dumps(result, indent=2, default=str))
            return 0
        bins = result.get("binaries") or {}
        missing = [name for name, path in bins.items() if not path]
        print("vanth remote doctor:")
        print(f"  ssh:        {bins.get('ssh') or 'MISSING'}")
        print(f"  ssh-keygen: {bins.get('ssh-keygen') or 'MISSING'}")
        print(f"  scp:        {bins.get('scp') or 'MISSING'}")
        if missing:
            print("  WARNING: OpenSSH binaries missing - remote execution unavailable")
        for row in result.get("remotes") or []:
            print(f"  remote:     {row.get('remote_id')} {row.get('target')} state={row.get('state')}")
        return 0 if not missing else 1
    if sub == "remove":
        args = argv[1:]
        targets = [a for a in args if not a.startswith("--")]
        if not targets:
            print("vanth remote remove: missing remote_id", file=sys.stderr)
            return 2
        remote_id = targets[0]
        assume_yes = "--yes" in args
        if not assume_yes:
            print(f"vanth remote remove: this removes local keys/config for {remote_id}; pass --yes to confirm", file=sys.stderr)
            return 1
        try:
            client.ensure()
            result = client.post("/remotes/remove", {"remote_id": remote_id})
        except Exception as exc:
            print(f"vanth remote remove: failed to reach daemon: {exc}", file=sys.stderr)
            return 1
        if result.get("result") == "error":
            print(f"vanth remote remove: {result.get('error')}", file=sys.stderr)
            return 1
        print(f"vanth remote remove: removed {remote_id}")
        return 0
    print(f"vanth remote: unknown subcommand {sub!r}", file=sys.stderr)
    return 2


def cmd_stop(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    if not argv:
        print("vanth stop: missing job id", file=sys.stderr)
        return 2
    job_id = argv[0]
    signal = "terminate"
    kill_after = 10
    reason = None
    i = 1
    while i < len(argv):
        arg = argv[i]
        if arg == "--reason":
            i += 1
            if i >= len(argv):
                print("vanth stop: --reason requires a value", file=sys.stderr)
                return 2
            reason = argv[i]
        elif arg == "--signal":
            i += 1
            if i >= len(argv):
                print("vanth stop: --signal requires a value", file=sys.stderr)
                return 2
            value = argv[i]
            if value not in {"terminate", "kill"}:
                print("vanth stop: --signal must be terminate or kill", file=sys.stderr)
                return 2
            signal = value
        elif arg == "--kill-after":
            i += 1
            if i >= len(argv):
                print("vanth stop: --kill-after requires a value", file=sys.stderr)
                return 2
            try:
                kill_after = int(argv[i])
            except ValueError:
                print(f"vanth stop: invalid --kill-after value {argv[i]!r}", file=sys.stderr)
                return 2
        else:
            print(f"vanth stop: unknown option {arg!r}", file=sys.stderr)
            return 2
        i += 1
    client = VanthClient(home=home)
    try:
        client.ensure()
    except Exception as exc:
        print(f"vanth stop: failed to reach daemon: {exc}", file=sys.stderr)
        return 1
    job_id, problem = _requiring_job_id(client, job_id, "stop")
    if problem:
        return problem
    try:
        # The daemon may wait out the full grace period before responding.
        result = client.post(
            f"/jobs/{job_id}/stop",
            {"signal": signal, "kill_after_seconds": kill_after, "actor": "user", "reason": reason},
            timeout=float(kill_after) + 30,
        )
    except Exception as exc:
        print(f"vanth stop: failed to reach daemon: {exc}", file=sys.stderr)
        return 1
    if not _expect_ok(result):
        error = result.get("error") or ""
        if "not running" in error or "is not running" in error or "unknown" in error.lower():
            print(f"vanth stop: unknown or not running job {job_id}", file=sys.stderr)
            return 1
        print(f"vanth stop: daemon error: {error}", file=sys.stderr)
        return 1
    if json_out:
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"vanth stop: requested stop for {job_id}")
    return 0


def cmd_rerun(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    if not argv:
        print("vanth rerun: missing job id", file=sys.stderr)
        return 2
    raw_job_id = argv[0]
    payload: dict[str, Any] = {}
    env: dict[str, str] = {}
    tags: list[str] = []
    secret_env: list[str] = []
    i = 1
    while i < len(argv):
        arg = argv[i]
        if arg in {"--command", "--cwd", "--timeout", "--env", "--name", "--tag", "--notes", "--secret-env"}:
            i += 1
            if i >= len(argv):
                print(f"vanth rerun: {arg} requires a value", file=sys.stderr)
                return 2
            value = argv[i]
            if arg == "--command":
                payload["command"] = value
            elif arg == "--cwd":
                payload["cwd"] = value
            elif arg == "--name":
                payload["name"] = value
            elif arg == "--notes":
                payload["notes"] = value
            elif arg == "--tag":
                tags.append(value)
            elif arg == "--secret-env":
                secret_env.append(value)
            elif arg == "--timeout":
                try:
                    timeout = int(value)
                except ValueError:
                    print(f"vanth rerun: invalid --timeout value {value!r}", file=sys.stderr)
                    return 2
                if timeout < 1:
                    print("vanth rerun: --timeout must be >= 1", file=sys.stderr)
                    return 2
                payload["timeout_seconds"] = timeout
            else:
                key, sep, val = value.partition("=")
                if not sep or not key:
                    print(f"vanth rerun: --env expects KEY=VALUE, got {value!r}", file=sys.stderr)
                    return 2
                env[key] = val
        elif arg == "--interactive":
            payload["interactive"] = True
        elif arg == "--no-interactive":
            payload["interactive"] = False
        else:
            print(f"vanth rerun: unknown option {arg!r}", file=sys.stderr)
            return 2
        i += 1
    if env:
        payload["env"] = env
    if tags:
        payload["tags"] = tags
    if secret_env:
        payload["secret_env"] = secret_env

    client = VanthClient(home=home)
    try:
        client.ensure()
    except Exception as exc:
        print(f"vanth rerun: failed to reach daemon: {exc}", file=sys.stderr)
        return 1
    job_id, problem = _requiring_job_id(client, raw_job_id, "rerun")
    if problem:
        return problem
    try:
        result = client.post(f"/jobs/{job_id}/rerun", payload)
        result = client.confirm_local_start(result)
    except Exception as exc:
        print(f"vanth rerun: failed to reach daemon: {exc}", file=sys.stderr)
        return 1
    if not _expect_ok(result):
        print(f"vanth rerun: daemon error: {result.get('error') or result}", file=sys.stderr)
        return 1
    if json_out:
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"reran {job_id} as {result.get('job_id')} ({result.get('status')})")
    return 1 if result.get("status") in {"failed", "lost", "timeout", "cancelled", "orphaned"} else 0


def cmd_send(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    """Send raw input or EOF to an interactive job."""
    if not argv:
        print("vanth send: missing job id", file=sys.stderr)
        return 2
    raw_job_id, *args = argv
    line = eof = False
    while args and args[0] in {"--line", "--eof"}:
        option = args.pop(0)
        line |= option == "--line"
        eof |= option == "--eof"
    literal = bool(args and args[0] == "--")
    if literal:
        args.pop(0)
    if args == ["-"] and not literal:
        input_text = sys.stdin.read()
    elif args:
        input_text = " ".join(args)
    elif eof:
        input_text = ""
    else:
        print("vanth send: missing input (use - to read stdin)", file=sys.stderr)
        return 2
    if line and not input_text.endswith("\n"):
        input_text += "\n"

    client = VanthClient(home=home)
    try:
        client.ensure()
    except Exception as exc:
        print(f"vanth send: failed to reach daemon: {exc}", file=sys.stderr)
        return 1
    job_id, problem = _requiring_job_id(client, raw_job_id, "send")
    if problem:
        return problem
    try:
        result = client.post(f"/jobs/{job_id}/send", {"input": input_text, "eof": eof})
    except Exception as exc:
        print(f"vanth send: failed to reach daemon: {exc}", file=sys.stderr)
        return 1
    if not _expect_ok(result):
        print(f"vanth send: {result.get('error') or result}", file=sys.stderr)
        return 1
    if json_out:
        print(json.dumps(result, indent=2, default=str))
    else:
        suffix = " (stdin closed)" if eof else ""
        print(f"vanth send: sent {result.get('sent', 0)} bytes to {job_id}{suffix}")
    return 0


def cmd_deliveries(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    """List wake deliveries so the `failed` counter in `vanth status` is
    actionable (which job, how many attempts, why it failed)."""
    status: str | None = None
    job_id: str | None = None
    limit = 20
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg in {"--status", "--job", "--limit"}:
            i += 1
            if i >= len(argv):
                print(f"vanth deliveries: {arg} requires a value", file=sys.stderr)
                return 2
            value = argv[i]
            if arg == "--status":
                status = value
            elif arg == "--job":
                job_id = value
            else:
                try:
                    limit = int(value)
                except ValueError:
                    print(f"vanth deliveries: invalid --limit value {value!r}", file=sys.stderr)
                    return 2
        else:
            print(f"vanth deliveries: unknown option {arg!r}", file=sys.stderr)
            return 2
        i += 1
    client = VanthClient(home=home)
    try:
        client.ensure()
        payload = client.get("/deliveries", {"status": status, "job_id": job_id, "limit": limit})
    except Exception as exc:
        print(f"vanth deliveries: failed to reach daemon: {exc}", file=sys.stderr)
        return 1
    if not _expect_ok(payload):
        print(f"vanth deliveries: daemon error: {payload.get('error') or payload}", file=sys.stderr)
        return 1
    deliveries = payload.get("deliveries") or []
    if json_out:
        print(json.dumps(deliveries, indent=2, default=str))
        return 0
    if not deliveries:
        print("vanth deliveries: none")
        return 0
    print(f"{'STATUS':<11} {'DELIVERY':<20} {'TRIES':<6} {'JOB':<22} LAST ERROR")
    for item in deliveries:
        attempts = item.get("attempts")
        print(
            f"{(item.get('status') or ''):<11} {(item.get('delivery_id') or ''):<20} "
            f"{(str(attempts) if attempts is not None else ''):<6} "
            f"{(item.get('job_id') or ''):<22} {_clean_text(item.get('last_error'))}"
        )
    return 0


def cmd_wake(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    """Register a wake target on a job after the fact (the CLI counterpart of
    the MCP `job_add_wake_target` / `job_wake_now`).

    Works on an in-flight or finished job: ``--now`` surfaces a synthetic wake
    immediately, otherwise the target fires on the job's FUTURE events.
    """
    if not argv:
        print("vanth wake: missing job id", file=sys.stderr)
        return 2
    job_id = argv[0]
    now = False
    target_type: str | None = None
    events: list[str] | None = None
    config: dict[str, Any] = {}
    target: dict[str, Any] | None = None
    i = 1
    while i < len(argv):
        arg = argv[i]
        if arg == "--now":
            now = True
        elif arg in {"--type", "--events", "--config", "--cwd", "--target"}:
            i += 1
            if i >= len(argv):
                print(f"vanth wake: {arg} requires a value", file=sys.stderr)
                return 2
            value = argv[i]
            if arg == "--type":
                target_type = value
            elif arg == "--cwd":
                config["cwd"] = value
            elif arg == "--events":
                events = [part.strip() for part in value.split(",") if part.strip()]
                if not events:
                    print("vanth wake: --events requires a non-empty comma-separated list", file=sys.stderr)
                    return 2
            elif arg == "--target":
                try:
                    target = _load_json_object(value, "--target")
                except ValueError as exc:
                    print(f"vanth wake: {exc}", file=sys.stderr)
                    return 2
            else:
                try:
                    parsed = _load_json_object(value, "--config")
                except ValueError as exc:
                    print(f"vanth wake: {exc}", file=sys.stderr)
                    return 2
                config.update(parsed)
        else:
            print(f"vanth wake: unknown option {arg!r}", file=sys.stderr)
            return 2
        i += 1
    if target is None:
        if not target_type:
            print("vanth wake: --type is required (or pass --target JSON)", file=sys.stderr)
            return 2
        target = {"type": target_type, "events": events or ["completed", "failed"], **config}
    elif target_type or events or config:
        # A supplied --target is posted verbatim, so --type/--events/--cwd/--config
        # would be silently ignored. Reject the mixture instead of dropping input.
        print(
            "vanth wake: --target cannot be combined with --type/--events/--cwd/--config",
            file=sys.stderr,
        )
        return 2
    client = VanthClient(home=home)
    try:
        client.ensure()
    except Exception as exc:
        print(f"vanth wake: failed to reach daemon: {exc}", file=sys.stderr)
        return 1
    job_id, problem = _requiring_job_id(client, job_id, "wake")
    if problem:
        return problem
    route = "wake-now" if now else "wake"
    try:
        result = client.post(f"/jobs/{job_id}/{route}", {"target": target})
    except Exception as exc:
        print(f"vanth wake: failed to reach daemon: {exc}", file=sys.stderr)
        return 1
    if not _expect_ok(result):
        print(f"vanth wake: daemon error: {result.get('error') or result}", file=sys.stderr)
        return 1
    if json_out:
        print(json.dumps(result, indent=2, default=str))
    else:
        verb = "surfaced" if now else "registered"
        print(
            f"vanth wake: {verb} {result.get('target_type') or target.get('type')} wake on "
            f"{job_id} {result.get('events') or target.get('events')}"
        )
    return 0


_HTTP_ROUTES: tuple[tuple[str, str], ...] = (
    ("POST", "/jobs/preview"),
    ("GET", "/health (no auth)"),
    ("GET", "/ready | /doctor | /metrics"),
    ("GET", "/jobs?status=&limit=&name=&tags="),
    ("GET", "/jobs/{id}/status | /events | /tail | /metrics | /summary | /diff | /artifacts"),
    ("GET", "/view | /deliveries | /decisions | /schedules | /pools"),
    ("POST", "/jobs  (add `remote_id` to run on a paired host)"),
    ("POST", "/jobs/{id}/stop | /send | /pause | /resume | /rerun | /wait"),
    ("POST", "/jobs/{id}/wake | /wake-now  (add a wake target after the fact)"),
    ("POST", "/deliveries/{id}/mark | /retry | /deliveries/clear"),
    ("POST", "/cleanup | /reap-orphans | /shutdown"),
    ("POST", "/artifacts/put | /put-dir | /materialize | /verify | /gc | ..."),
    ("GET", "/remotes | /remotes/doctor | /remotes/{id}/jobs"),
    ("GET", "/remotes/{id}/status/{job_id} | /remotes/{id}/jobs/{job_id}/tail"),
    ("POST", "/remotes/pair | /remove | /remote/helper"),
)


def cmd_api(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    """Summarize the loopback HTTP surface and how to authenticate to it."""
    if argv:
        print(f"vanth api: unknown option {argv[0]!r}", file=sys.stderr)
        return 2
    if json_out:
        print(json.dumps({"routes": [{"method": m, "path": p} for m, p in _HTTP_ROUTES]}, indent=2))
        return 0
    discovery = _discovery(home) or {}
    print("vanth HTTP api (loopback only)")
    print(f"  base url: {discovery.get('url') or 'see <VANTH_HOME>/daemon.json'}")
    print(f"  token:    {home / 'token'}  (send as `Authorization: Bearer <token>`)")
    print("  note:     /health is unauthenticated; every other route requires the token")
    print("")
    for method, path in _HTTP_ROUTES:
        print(f"  {method:<5} {path}")
    return 0


def cmd_artifacts(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    if not argv:
        print("vanth artifacts: missing job id", file=sys.stderr)
        return 2
    job_id = argv[0]
    limit = 50
    i = 1
    while i < len(argv):
        arg = argv[i]
        if arg == "--limit":
            i += 1
            if i >= len(argv):
                print("vanth artifacts: --limit requires a value", file=sys.stderr)
                return 2
            try:
                limit = int(argv[i])
            except ValueError:
                print(f"vanth artifacts: invalid --limit value {argv[i]!r}", file=sys.stderr)
                return 2
        else:
            print(f"vanth artifacts: unknown option {arg!r}", file=sys.stderr)
            return 2
        i += 1
    client = VanthClient(home=home)
    try:
        client.ensure()
    except Exception as exc:
        print(f"vanth artifacts: failed to reach daemon: {exc}", file=sys.stderr)
        return 1
    job_id, problem = _requiring_job_id(client, job_id, "artifacts")
    if problem:
        return problem
    try:
        result = client.get(f"/jobs/{job_id}/artifacts", {"limit": limit})
    except Exception as exc:
        print(f"vanth artifacts: failed to reach daemon: {exc}", file=sys.stderr)
        return 1
    if not _expect_ok(result):
        error = result.get("error") or ""
        if "unknown" in error.lower() or "job_id" in error.lower():
            print(f"vanth artifacts: unknown job {job_id}", file=sys.stderr)
            return 1
        print(f"vanth artifacts: daemon error: {error}", file=sys.stderr)
        return 1
    artifacts = result.get("artifacts") or []
    if json_out:
        print(json.dumps(result, indent=2, default=str))
        return 0
    if not artifacts:
        print(f"vanth artifacts: no artifacts for {job_id}")
        return 0
    for artifact in artifacts:
        size = artifact.get("size_bytes")
        size_text = _fmt_bytes(size) if size is not None else ""
        print(
            f"{(artifact.get('name') or ''):<24} {(artifact.get('kind') or ''):<12} "
            f"{size_text:<10} {(artifact.get('uri') or ''):<40} {artifact.get('created_at') or ''}"
        )
    return 0


def cmd_prune(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    older_than = 0
    dry_run: bool | None = None
    assume_yes = False
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg == "--older-than":
            i += 1
            if i >= len(argv):
                print("vanth prune: --older-than requires a value", file=sys.stderr)
                return 2
            try:
                older_than = int(argv[i])
            except ValueError:
                print(f"vanth prune: invalid --older-than value {argv[i]!r}", file=sys.stderr)
                return 2
        elif arg == "--dry-run":
            dry_run = True
        elif arg == "--yes":
            assume_yes = True
            dry_run = False
        else:
            print(f"vanth prune: unknown option {arg!r}", file=sys.stderr)
            return 2
        i += 1
    if dry_run is None:
        dry_run = not assume_yes
    client = VanthClient(home=home)
    try:
        client.ensure()
        result = client.post("/cleanup", {"older_than_seconds": older_than, "dry_run": dry_run})
    except Exception as exc:
        print(f"vanth prune: failed to reach daemon: {exc}", file=sys.stderr)
        return 1
    if not _expect_ok(result):
        print(f"vanth prune: daemon error: {result.get('error') or result}", file=sys.stderr)
        return 1
    count = int(result.get("count") or 0)
    job_ids = result.get("jobs") or []
    if json_out:
        print(json.dumps(result, indent=2, default=str))
        return 0
    if dry_run:
        if count:
            print(f"vanth prune: would remove {count} job(s): {', '.join(job_ids)}")
        else:
            print("vanth prune: no jobs to remove")
        return 0
    if not assume_yes:
        if count == 0:
            print("vanth prune: no jobs to remove")
            return 0
        try:
            answer = input(f"Remove {count} job(s)? [y/N] ")
        except EOFError:
            answer = "n"
        if answer.strip().lower() not in {"y", "yes"}:
            print("vanth prune: aborted")
            return 0
    if count:
        print(f"vanth prune: removed {count} job(s)")
    else:
        print("vanth prune: no jobs to remove")
    return 0


def cmd_backup(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    out = None
    include_logs = False
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg == "--out":
            i += 1
            if i >= len(argv):
                print("vanth backup: --out requires a path", file=sys.stderr)
                return 2
            out = argv[i]
        elif arg == "--include-logs":
            include_logs = True
        else:
            print(f"vanth backup: unknown option {arg!r}", file=sys.stderr)
            return 2
        i += 1
    from .backup import create_backup

    try:
        path = create_backup(home, out=out, include_logs=include_logs)
    except Exception as exc:
        print(f"vanth backup: {exc}", file=sys.stderr)
        return 1
    if json_out:
        print(json.dumps({"result": "ok", "archive": str(path)}))
    else:
        print(f"backup written: {path}")
    return 0


def cmd_restore(argv: list[str], home: Path, *, json_out: bool = False) -> int:
    if not argv:
        print("vanth restore: missing archive path", file=sys.stderr)
        return 2
    archive = argv[0]
    yes = False
    force = False
    for arg in argv[1:]:
        if arg == "--yes":
            yes = True
        elif arg == "--force":
            force = True
        else:
            print(f"vanth restore: unknown option {arg!r}", file=sys.stderr)
            return 2
    if not yes:
        print("vanth restore: refusing without --yes (this overwrites live state)", file=sys.stderr)
        return 2
    from .backup import restore_backup

    try:
        result = restore_backup(home, archive, force=force)
    except Exception as exc:
        print(f"vanth restore: {exc}", file=sys.stderr)
        return 1
    if json_out:
        print(json.dumps(result))
    else:
        print(f"restored {result['files_restored']} files from {result['archive']}")
    return 0


def _usage() -> str:
    return (
        "usage: vanth <command> [options]\n"
        "\n"
        "Vanth is a local background-job daemon for AI agents (and humans).\n"
        "The CLI covers core jobs and administration; MCP exposes the full agent surface.\n"
        "\n"
        "commands:\n"
        "  status         show daemon health, running jobs, and MCP client state\n"
        "  doctor         full health report (schema, deliveries, tools);\n"
        "                 --reap-orphans terminates orphaned MCP servers\n"
        "  start          start a background job without the MCP tools\n"
        "  list, ps       list jobs (in-flight by default; --all for terminal)\n"
        "  logs, tail     show a job's stdout/stderr output (--grep filters lines)\n"
        "  wait           block until a job emits an event (CLI job_wait)\n"
        "  diff           diff the run specs of two jobs\n"
        "  rerun          relaunch a job with its original settings (optional overrides)\n"
        "  send           send input or EOF to an interactive job\n"
        "  stop           stop a running job\n"
        "  sleep          start a background sleep job\n"
        "  wake           add a wake target to a job after it started (--now to fire once)\n"
        "  deliveries     list wake deliveries (--status failed shows why)\n"
        "  artifacts      list a job's artifacts\n"
        "  backup         write one archive of jobs + artifacts + events\n"
        "  restore        restore a backup archive (requires --yes)\n"
        "  prune          manually clean up terminal jobs (default dry-run)\n"
        "  restart        gracefully restart the daemon (jobs survive)\n"
        "  remote         pair/list/doctor/remove/pending/retry remote execution hosts\n"
        "  setup          register the MCP server in your clients' configs (one-shot)\n"
        "  autostart      enable/disable/status (daemon survives reboots)\n"
        "  version        print the installed package version\n"
        "  api            summarize loopback HTTP routes and authentication details\n"
        "\n"
        "  help <command> show help for one command (same as `vanth <command> --help`)\n"
        "\n"
        "options:\n"
        "  --help, -h     show this help\n"
        "  --json         machine-readable output (where supported); may precede or\n"
        "                 follow the command: `vanth --json list` == `vanth list --json`\n"
        "\n"
        "run `vanth setup` after installing to connect your MCP clients; run\n"
        "`vanth <command> --help` for command-specific options.\n"
        "\n"
        "core MCP counterparts in the CLI (MCP has more agent tools):\n"
        "  job_start   -> vanth start -- <command>      job_tail  -> vanth logs <id>\n"
        "  job_wait    -> vanth wait <id>               job_stop  -> vanth stop <id>\n"
        "  job_status  -> vanth status <id>             job_list  -> vanth list [--all]\n"
        "  job_rerun   -> vanth rerun <id>             job_send  -> vanth send <id> <text>\n"
        "  job_add_wake_target / job_wake_now -> vanth wake <id> [--now]\n"
        "  full workflow: vanth sleep 30\n"
        "                 vanth wait <id>\n"
        "                 vanth status <id> && vanth logs <id>\n"
        "\n"
        "windows quoting (vanth start): a single quoted command string is used\n"
        "verbatim; separate arguments are re-quoted for cmd.exe. Use ONE quoted\n"
        "string for anything with && | > or % ! \", or write the steps to a script:\n"
        "  vanth start \"cmd /c ping -n 30 host >nul && echo done\"\n"
        "  vanth start -- run.cmd\n"
    )


_COMMAND_HELP: dict[str, str] = {
    "status": "usage: vanth status [<job-id>] [--json]\n"
              "  Bare: daemon health, running jobs, and MCP client state.\n"
              "  With a job id (or an unambiguous prefix): that one job's status,\n"
              "  exit code, runtime, pid, last event, and progress - the CLI\n"
              "  counterpart of the MCP `job_status`.\n"
              "  examples: vanth status            (is the daemon up?)\n"
              "            vanth status job_abc123 (how did that job go?)\n",
    "doctor": "usage: vanth doctor [--verify-artifacts] [--reap-orphans] [--json]\n"
              "  --verify-artifacts checks recent blob hashes with bounded time/bytes;\n"
              "  see artifact_integrity.complete for checks that hit the scan limit.\n"
              "  Full health report: schema, deliveries, relay liveness, dead letters,\n"
              "  orphaned MCP servers. OK means the daemon is healthy: a non-zero\n"
              "  failed-delivery count is reported but does not by itself fail it\n"
              "  (see `vanth deliveries --status failed`).\n",
    "list": "usage: vanth list [--status a,b] [--all] [--limit N] [--thread-id ID]\n"
            "                  [--name TEXT] [--tag TAG]... [--json]\n"
            "  Jobs, in-flight by default (launching/queued/running/paused/stopping/\n"
            "  retrying, so a just-started job shows immediately); --all lists\n"
            "  finished ones instead. --thread-id is the closest equivalent of the\n"
            "  MCP `job_view`.\n"
            "  example: vanth list --all --name train --limit 10\n",
    "start": "usage: vanth start [--dry-run] [--idempotency-key KEY] [--name N] [--cwd DIR] [--timeout S] [--env K=V]...\n"
             "                  [--wake JSON|@FILE|-]... [--wake-me[=EVENTS]]\n"
             "                  [--trigger JSON|@FILE|-]\n"
             "                  [--interactive] [--] <command...>\n"
             "  Start a background job (the non-MCP front door).\n"
             "\n"
             "  options: --name --cwd --timeout --env --wake --wake-me --interactive --priority\n"
             "           --pool --tag --notes --secret-env --trigger --policy --json\n"
             "\n"
             "  quoting (Windows/PowerShell): pass simple commands as separate\n"
             "  arguments. For anything with && | > or % ! \", pass the WHOLE command\n"
             "  as one double-quoted string with NO quotes inside it, or use a script\n"
             "  file. A single-quoted string containing inner \" is split into garbage\n"
             "  arguments by PowerShell 5.1; % ! and \" cannot be encoded either.\n"
             "  JSON options (--wake/--trigger/--policy) hit the same quoting wall:\n"
             "  write the object to a file and pass @path (or @- for stdin).\n"
             "\n"
             "  wakes: use --wake-me for the current OpenCode session, or --wake @wake.json\n"
             "  shorthand: vanth start --wake-me -- <command> (or --wake-me=completed,failed,checkpoint)\n"
             "  --wake-me defaults to completed,failed,timeout,cancelled,orphaned.\n"
             "  needs NO session_id; the daemon resolves the live OpenCode plugin\n"
             "  relay for --cwd. Add \"session_id\":\"ses_...\" only to target a specific\n"
             "  session; never use the relay client id (opencode-<pid>-<rand>).\n"
             "\n"
             "  examples:\n"
             "    vanth start -- python train.py --epochs 10\n"
             "    vanth start --name train --cwd C:\\\\work -- python train.py\n"
             "    vanth sleep 30\n"
             "    vanth start \"cmd /c ping -n 15 host >nul && echo ONBOARD_MARKER\"\n"
             "    vanth start -- run.cmd\n"
             "    vanth start --name j --wake @wake.json -- make -j8\n",
     "logs": "usage: vanth logs <job-id> [--stream stdout|stderr|all] [--max-bytes N]\n"
             "                   [--offset N] [--grep TEXT] [--json]\n"
             "  Captured output. Defaults: --stream stdout, --max-bytes 8192 (a tail;\n"
             "  raise it or page with --offset for more). `-h/--help` is help, not a job\n"
             "  id, and an unambiguous job-id prefix is accepted.\n"
             "  example: vanth logs job_abc123 --stream all --max-bytes 65536\n",
    "wait": "usage: vanth wait <job-id> [--events completed,failed,timeout,cancelled,orphaned] [--timeout SECONDS]\n"
            "                  [--since-event-id ID] [--json]\n"
            "  Block until a matching event fires (the CLI counterpart of job_wait).\n"
            "  Exit 0 on the event, 3 on timeout (default --timeout 3600). It blocks,\n"
            "  so give it a timeout shorter than your own patience.\n"
            "  By default, waits for any terminal outcome: completed, failed, timeout,\n"
            "  cancelled, or orphaned. Pass --events to narrow the event types.\n"
            "  example: vanth wait job_abc123 --timeout 120\n",
    "stop": "usage: vanth stop <job-id> [--signal terminate|kill] [--kill-after SECONDS]\n"
             "                  [--reason TEXT]\n"
             "  Stop a running job. terminate asks first and escalates to kill after\n"
             "  --kill-after seconds (default 10); the job ends `cancelled` (non-zero\n"
             "  exit code), not `failed`.\n"
             "  It returns as soon as the stop is REQUESTED, so confirm with\n"
             "  `vanth status <job-id>` (or `vanth list --all`).\n"
             "  example: vanth stop job_abc123 --signal kill && vanth status job_abc123\n",
    "rerun": "usage: vanth rerun <job-id> [--command CMD] [--name N] [--cwd DIR]\n"
              "                   [--timeout SECONDS] [--env K=V]... [--tag TAG]...\n"
              "                   [--notes TEXT] [--secret-env KEY]... [--interactive|--no-interactive] [--json]\n"
              "  Start a new job from the source job's settings, overriding only fields\n"
              "  you pass. An unambiguous job-id prefix is accepted.\n"
              "  example: vanth rerun job_abc123 --env MODE=retry --name second-attempt\n",
    "send": "usage: vanth send <job-id> [--line] [--eof] [--] <text|-> [--json]\n"
            "  Send input to a job started with --interactive. Text is sent exactly\n"
            "  as given; --line appends a newline if missing. Use - to read stdin,\n"
            "  or omit text with --eof to close the job's stdin.\n"
            "  example: vanth send job_abc123 --line hello\n",
    "sleep": "usage: vanth sleep <seconds>\n"
             "  Start a background job that sleeps for a positive number of seconds.\n",
    "deliveries": "usage: vanth deliveries [--status delivered|failed|pending] [--job-id ID] [--json]\n"
                  "  Wake deliveries, so a nonzero `failed` count is actionable.\n",
    "wake": "usage: vanth wake <job-id> [--now] [--type TYPE] [--events a,b]\n"
            "                  [--cwd DIR] [--config JSON|@FILE|-] [--target JSON|@FILE|-]\n"
            "                  [--json]\n"
            "  Register a wake target on a job AFTER it started (the CLI counterpart\n"
            "  of the MCP `job_add_wake_target`). Fires on the job's FUTURE events;\n"
            "  --now surfaces a synthetic wake immediately instead (job_wake_now).\n"
            "  --type is required unless --target gives a full dict: local_command /\n"
            "  codex_cli_thread / codex_thread / codex_desktop / opencode_thread /\n"
            "  webhook. JSON options accept @path (a file) or - (stdin) to dodge\n"
            "  PowerShell 5.1 quote stripping.\n"
            "  opencode_thread resolves the session from a live plugin relay for the\n"
            "  job's cwd; otherwise pass session_id (the OpenCode ses_... id from\n"
            "  `opencode session list` or the destination in `vanth doctor`) inside\n"
            "  --config. NOT the relay client id (opencode-<pid>-<rand>) — that is the\n"
            "  long-poll identity and never wakes. attach is optional (headless serve).\n"
            "  Events default to completed,failed.\n"
            "  examples: vanth wake job_abc123 --type opencode_thread --events completed\n"
            "            vanth wake job_abc123 --now --type local_command \\\n"
            "              --config '{\"command\": [\"echo\", \"done\"]}'\n",
    "artifacts": "usage: vanth artifacts <job-id> [--limit N] [--json]\n"
                 "  Artifacts attached to a job.\n",
    "diff": "usage: vanth diff <job-id> <other-job-id> [--json]\n"
            "  Compare two jobs' run specs (command/env/cwd/tags/wake targets).\n",
    "api": "usage: vanth api [--json]\n  A route summary and how to authenticate to the loopback HTTP API.\n",
    "remote": "usage: vanth remote <pair|list|doctor|remove|pending|retry> [options]\n"
              "  pair <user@host> [--name N] [--allow-root]   create an SSH pairing\n"
              "  list   doctor [--remote ID]   remove <id> [--yes]\n"
              "  pending [--remote ID]   retry <request-id>\n"
              "  Pairing prepares SSH hosts for remote execution; use the returned\n"
              "  remote_id with Vanth's remote execution tools.\n",
    "backup": "usage: vanth backup [--out PATH]\n  One archive of jobs + artifacts + events.\n",
    "restore": "usage: vanth restore <archive> --yes\n  Restore a backup archive.\n",
    "prune": "usage: vanth prune [--older-than SECONDS] [--yes]\n  Remove terminal jobs (dry run by default).\n",
    "restart": "usage: vanth restart\n  Gracefully restart the daemon; running jobs survive.\n",
    "setup": "usage: vanth setup [opencode] [codex] [claude] [desktop] [--remove] [--yes] [--json]\n"
             "  Register Vanth MCP in selected clients; with no clients, configure\n"
             "  detected clients. OpenCode setup also installs its wake plugin.\n"
             "  --remove removes the registration; --yes skips confirmation.\n",
    "autostart": "usage: vanth autostart <enable|disable|status>\n  Whether the daemon survives reboots.\n",
    "version": "usage: vanth version\n  Print the installed package version.\n",
}


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    home = canonical_home()
    # A global `--json` is recognized only BEFORE a `--` separator, so a command
    # passed to `vanth start` can carry its own literal `--json` untouched.
    boundary = argv.index("--") if "--" in argv else len(argv)
    json_out = "--json" in argv[:boundary]
    before_separator = [arg for arg in argv[:boundary] if arg != "--json"]
    argv = before_separator + argv[boundary:]
    if not argv or argv[0] in {"--help", "-h", "help"}:
        # `vanth help <command>` shows that command's help, not the global list.
        topic = argv[1] if len(argv) > 1 else None
        if topic in _COMMAND_HELP:
            print(_COMMAND_HELP[topic], end="")
            return 0
        print(_usage(), end="")
        return 0
    command = argv[0]
    # `vanth <command> --help` is help, not an unknown option (only `-h/--help`
    # before any `--` counts, so `vanth start -- prog --help` still passes it on).
    if any(arg in {"-h", "--help"} for arg in before_separator[1:]):
        text = _COMMAND_HELP.get(command)
        if text is None:
            print(f"vanth {command}: no help available for that command", file=sys.stderr)
            return 2
        print(text, end="")
        return 0
    if command in {"--version", "version"}:
        return cmd_version()
    if command == "status":
        return cmd_status(home, argv[1:], json_out=json_out)
    if command == "doctor":
        return cmd_doctor(argv[1:], home, json_out=json_out)
    if command == "restart":
        return cmd_restart(home, json_out=json_out)
    if command == "setup":
        return cmd_setup(argv[1:], home, json_out=json_out)
    if command == "autostart":
        return cmd_autostart(argv[1:], home, json_out=json_out)
    if command in {"list", "ps"}:
        return cmd_list(argv[1:], home, json_out=json_out)
    if command == "start":
        return cmd_start(argv[1:], home, json_out=json_out)
    if command in {"logs", "tail"}:
        return cmd_logs(argv[1:], home, json_out=json_out)
    if command == "wait":
        return cmd_wait(argv[1:], home, json_out=json_out)
    if command == "diff":
        return cmd_diff(argv[1:], home, json_out=json_out)
    if command == "stop":
        return cmd_stop(argv[1:], home, json_out=json_out)
    if command == "rerun":
        return cmd_rerun(argv[1:], home, json_out=json_out)
    if command == "send":
        return cmd_send(argv[1:], home, json_out=json_out)
    if command == "sleep":
        return cmd_sleep(argv[1:], home, json_out=json_out)
    if command == "deliveries":
        return cmd_deliveries(argv[1:], home, json_out=json_out)
    if command == "wake":
        return cmd_wake(argv[1:], home, json_out=json_out)
    if command == "api":
        return cmd_api(argv[1:], home, json_out=json_out)
    if command == "artifacts":
        return cmd_artifacts(argv[1:], home, json_out=json_out)
    if command == "prune":
        return cmd_prune(argv[1:], home, json_out=json_out)
    if command == "backup":
        return cmd_backup(argv[1:], home, json_out=json_out)
    if command == "restore":
        return cmd_restore(argv[1:], home, json_out=json_out)
    if command == "remote":
        return cmd_remote(argv[1:], home, json_out=json_out)
    print(f"vanth: unknown command {command!r}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
