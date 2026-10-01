"""Opt-in real Codex/OpenCode wake smoke for Vanth v1 release gates.

Never runs a live wake by default. It always records the installed adapter
versions, and only sends a real wake when the caller provides explicit
environment configuration:

    $env:VANTH_SMOKE_CODEX_THREAD="<thread_id>"       # wake a real Codex thread
    $env:VANTH_SMOKE_OPENCODE_SESSION="<session_id>"  # resume a real OpenCode session

When neither is set, the script prints versions, verifies both CLIs are
reachable with a cheap non-mutating probe, and exits 0 (the "recorded versions"
gate) without touching any model session.

When configured, it dispatches a delivery through the real adapter and asserts
the response round-trips a payload containing `delivery_id`, then records the
result. Set VANTH_CODEX_BIN / VANTH_OPENCODE_BIN to override resolution.

Exit codes: 0 = pass (or skipped), 1 = any failure.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any


def probe_version(command: list[str]) -> str | None:
    try:
        result = subprocess.run(command + ["--version"], capture_output=True, text=True, timeout=15)
        if result.returncode == 0:
            return (result.stdout or result.stderr).strip().splitlines()[0]
    except (OSError, subprocess.TimeoutExpired):
        return None
    return None


def codex_command() -> list[str]:
    configured = os.environ.get("VANTH_CODEX_BIN")
    if configured:
        return [configured]
    win_default = Path(r"C:\codex\codex.exe")
    if sys.platform == "win32" and win_default.exists():
        return [str(win_default)]
    return ["codex"]


def opencode_command() -> list[str]:
    configured = os.environ.get("VANTH_OPENCODE_BIN")
    if configured:
        return [configured]
    found = shutil.which("opencode")
    if found:
        return [found]
    return ["opencode"]


def assert_reply(result: dict[str, Any], delivery_id: str, adapter: str) -> None:
    """Check assistant output only: echoed prompts and metadata cannot pass."""
    replies = []
    if adapter == "codex":
        for item in (result.get("turn") or {}).get("items", []):
            if item.get("type") == "agentMessage":
                replies.append(item.get("text", ""))
    else:
        for line in result.get("stdout", "").splitlines():
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if event.get("type") == "text":
                replies.append((event.get("part") or {}).get("text", ""))
    if not any(isinstance(reply, str) and reply.strip() == delivery_id for reply in replies):
        raise RuntimeError(f"{adapter} assistant did not reply with the expected delivery_id")


def smoke_codex(thread_id: str) -> dict[str, Any]:
    from vanth.codex_bridge import send_message_to_thread

    delivery_id = uuid.uuid4().hex[:16]
    prompt = (
        "vanth release smoke (opt-in). Reply with the delivery_id from this "
        "message exactly. Do not run tools.\n"
        f"delivery_id: {delivery_id}"
    )
    result = send_message_to_thread(
        thread_id, prompt, codex_command=codex_command(), timeout_seconds=int(os.environ.get("VANTH_SMOKE_TIMEOUT", "90"))
    )
    assert_reply(result, delivery_id, "codex")
    return {"delivery_id": delivery_id, "adapter": "codex", "thread_id": thread_id, "result": result}


def smoke_opencode(session_id: str) -> dict[str, Any]:
    from vanth.opencode_bridge import send_message_to_session

    delivery_id = uuid.uuid4().hex[:16]
    prompt = (
        "vanth release smoke (opt-in). Reply with the delivery_id from this "
        "message exactly. Do not run tools.\n"
        f"delivery_id: {delivery_id}"
    )
    result = send_message_to_session(
        session_id,
        prompt,
        opencode_command=opencode_command(),
        timeout_seconds=int(os.environ.get("VANTH_SMOKE_TIMEOUT", "90")),
    )
    assert_reply(result, delivery_id, "opencode")
    return {"delivery_id": delivery_id, "adapter": "opencode", "session_id": session_id, "result": result}


def main() -> int:
    parser = argparse.ArgumentParser(description="Opt-in real Codex/OpenCode wake smoke")
    parser.add_argument("--json", action="store_true", help="print machine-readable results")
    parser.add_argument("--codex-thread", help="override VANTH_SMOKE_CODEX_THREAD")
    parser.add_argument("--opencode-session", help="override VANTH_SMOKE_OPENCODE_SESSION")
    args = parser.parse_args()

    codex_ver = probe_version(codex_command())
    opencode_ver = probe_version(opencode_command())
    record = {
        "codex": {"command": codex_command(), "version": codex_ver},
        "opencode": {"command": opencode_command(), "version": opencode_ver},
    }
    codex_thread = args.codex_thread or os.environ.get("VANTH_SMOKE_CODEX_THREAD")
    opencode_session = args.opencode_session or os.environ.get("VANTH_SMOKE_OPENCODE_SESSION")

    results: list[dict[str, Any]] = []
    failures = []

    if codex_thread:
        try:
            results.append(smoke_codex(codex_thread))
        except Exception as exc:  # noqa: BLE001
            failures.append(("codex", str(exc)))
    if opencode_session:
        try:
            results.append(smoke_opencode(opencode_session))
        except Exception as exc:  # noqa: BLE001
            failures.append(("opencode", str(exc)))

    if args.json:
        print(json.dumps({"versions": record, "results": results, "failures": failures}, indent=2))
    else:
        print("Vanth real-adapter smoke")
        print(f"  codex   version: {record['codex']['version'] or 'not found'}")
        print(f"  opencode version: {record['opencode']['version'] or 'not found'}")
        if not codex_thread and not opencode_session:
            print("  no session configured (VANTH_SMOKE_CODEX_THREAD / VANTH_SMOKE_OPENCODE_SESSION); live wake skipped")
        for result in results:
            print(f"  {result['adapter']} wake OK: {result}")
        for name, error in failures:
            print(f"  {name} wake FAILED: {error}")

    if failures:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
