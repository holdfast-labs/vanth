from __future__ import annotations

import json
import secrets
import os
import stat
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from .paths import canonical_home, secure_home_permissions


def _default_daemon_url() -> str:
    host = os.environ.get("VANTH_DAEMON_HOST", "127.0.0.1")
    try:
        port = int(os.environ.get("VANTH_DAEMON_PORT", "8765"))
    except ValueError:
        port = 8765
    return f"http://{host}:{port}"


def _default_timeout() -> float | None:
    """Per-request socket timeout for the HTTP client.

    Without this a hung/blocked daemon blocks the CLI (and an MCP tool call)
    forever — ``urlopen(timeout=None)`` has no upper bound. Override with
    ``VANTH_CLIENT_TIMEOUT`` seconds; values <= 0 mean "no timeout" for
    callers that deliberately want to block.
    """
    try:
        value = float(os.environ.get("VANTH_CLIENT_TIMEOUT", "30"))
    except ValueError:
        return 30.0
    return None if value <= 0 else value


def auth_token_path(home: str | os.PathLike[str] | None = None) -> str:
    return os.fspath(canonical_home(home) / "token")


def ensure_auth_token(home: str | os.PathLike[str] | None = None) -> str:
    path = auth_token_path(home)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    created = False
    try:
        with open(path, "x", encoding="utf-8") as handle:
            handle.write(secrets.token_urlsafe(32))
        created = True
    except FileExistsError:
        pass
    try:
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
    except OSError:
        pass
    if created:
        # Newly-created token: tighten the home ACL so a broad profile-level
        # grant (e.g. a sandbox group) cannot read it.
        secure_home_permissions(os.path.dirname(path))
    with open(path, encoding="utf-8") as handle:
        token = handle.read().strip()
    if not token:
        raise RuntimeError("Vanth authentication token is empty")
    return token


class VanthClient:
    def __init__(self, url: str | None = None, home: str | os.PathLike[str] | None = None) -> None:
        self.home = canonical_home(home)
        self.url = self._resolve_url(url).rstrip("/")
        self.token = ensure_auth_token(self.home)

    def _resolve_url(self, url: str | None) -> str:
        if url:
            return url
        env_url = os.environ.get("VANTH_DAEMON_URL")
        if env_url:
            return env_url
        discovered = self._discover_url()
        if discovered:
            return discovered
        host = os.environ.get("VANTH_DAEMON_HOST", "127.0.0.1")
        try:
            port = int(os.environ.get("VANTH_DAEMON_PORT", "8765"))
        except ValueError:
            port = 8765
        return f"http://{host}:{port}"

    def _discover_url(self) -> str | None:
        try:
            payload = json.loads((self.home / "daemon.json").read_text(encoding="utf-8"))
            return payload.get("url")
        except (OSError, ValueError):
            return None

    def _ready(self) -> bool:
        # Prefer the cheap `/ready-fast` probe (home + schema only, ~10ms);
        # fall back to full `/doctor` for pre-1.14.1 daemons without the route.
        try:
            payload: dict = self.get("/ready-fast")
        except Exception:
            payload = {}
        if not isinstance(payload, dict) or payload.get("result") == "error" or payload.get("schema_version") is None:
            try:
                payload = self.get("/doctor")
            except Exception:
                return False
        return (
            isinstance(payload, dict)
            and payload.get("result") != "error"
            and payload.get("schema_version") is not None
            and Path(str(payload.get("home", ""))).expanduser().resolve() == self.home
        )

    def ensure(self) -> None:
        try:
            if self._ready():
                return
        except Exception:
            pass
        try:
            occupied = self.get("/health", timeout=1) == {"ok": True}
        except (OSError, ValueError):
            occupied = False
        if occupied:
            raise RuntimeError(
                f"a service is already responding at {self.url}, but Vanth cannot use it with "
                f"VANTH_HOME={self.home}; use a free VANTH_DAEMON_PORT for a separate home "
                "or check its token and schema"
            )
        subprocess.Popen(
            [sys.executable, "-m", "vanth.daemon"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=os.environ.copy(),
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) if sys.platform == "win32" else 0,
        )
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                if self._ready():
                    return
            except Exception:
                pass
            time.sleep(0.1)
        raise RuntimeError(f"vanthd did not start; inspect {self.home / 'logs' / 'daemon.log'}")

    def get(self, path: str, params: dict[str, Any] | None = None, *, timeout: float | None = None) -> dict[str, Any]:
        url = self.url + path
        if params:
            clean = {key: value for key, value in params.items() if value is not None}
            if clean:
                url += "?" + urllib.parse.urlencode(clean, doseq=True)
        try:
            request = urllib.request.Request(url, headers={"Authorization": f"Bearer {self.token}"})
            with urllib.request.urlopen(request, timeout=_default_timeout() if timeout is None else timeout) as response:
                return json.loads(response.read().decode())
        except urllib.error.HTTPError as exc:
            return json.loads(exc.read().decode())

    def post(self, path: str, payload: dict[str, Any] | None = None, *, timeout: float | None = None) -> dict[str, Any]:
        data = json.dumps(payload or {}).encode()
        request = urllib.request.Request(
            self.url + path,
            data=data,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {self.token}"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=_default_timeout() if timeout is None else timeout) as response:
                return json.loads(response.read().decode())
        except urllib.error.HTTPError as exc:
            return json.loads(exc.read().decode())

    def confirm_local_start(self, started: dict[str, Any]) -> dict[str, Any]:
        """Briefly confirm a directly launched workload while preserving its job ID."""
        job_id = started.get("job_id")
        status = started.get("status")
        if not job_id or status == "queued":
            return started

        def preserve_failure_details() -> None:
            try:
                snapshot = self.get(f"/jobs/{job_id}/status", timeout=1)
                for field in ("failure_reason", "recommended_next_action"):
                    if snapshot.get(field):
                        started.setdefault(field, snapshot[field])
            except Exception:
                pass

        if status in {"failed", "lost", "timeout", "cancelled", "orphaned"}:
            started["startup_confirmed"] = False
            preserve_failure_details()
            reason = "job_failed" if started.get("idempotent_replay") else "startup_failed"
            started.setdefault("failure_reason", reason if status == "failed" else status)
            started.setdefault("recommended_next_action", f"Inspect job_status and job_tail for {job_id} before rerunning")
            return started
        if status not in {"running", "launching", "completed"}:
            return started
        try:
            acknowledged = self.post(
                f"/jobs/{job_id}/wait",
                {"filters": ["started", "failed", "timeout", "cancelled", "orphaned", "completed"],
                 "timeout_seconds": 3},
                timeout=33,
            )
            started["startup_confirmed"] = (
                acknowledged.get("result") == "event"
                and acknowledged.get("event", {}).get("type") == "started"
            )
            if acknowledged.get("result") == "event":
                started["status"] = acknowledged.get("status", status)
                if started["status"] in {"failed", "lost", "timeout", "cancelled", "orphaned"}:
                    preserve_failure_details()
                    reason = "workload_failed" if started["startup_confirmed"] else "startup_failed"
                    if started["status"] != "failed":
                        reason = started["status"]
                    started.setdefault("failure_reason", acknowledged.get("failure_reason") or reason)
                    started.setdefault("recommended_next_action", f"Inspect job_status and job_tail for {job_id} before rerunning")
            elif acknowledged.get("result") != "timeout":
                started.setdefault("warnings", []).append("Startup confirmation was unavailable; use job_status")
        except Exception:
            started["startup_confirmed"] = False
            started.setdefault("warnings", []).append("Startup confirmation was unavailable; use job_status")
        return started
