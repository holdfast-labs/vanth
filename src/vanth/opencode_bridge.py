from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


class OpenCodeBridgeError(RuntimeError):
    pass


class OpenCodeSessionNotFound(OpenCodeBridgeError):
    """The targeted opencode session is stale or gone.

    Raised before a model turn when a cheap `session list` probe confirms the
    session id no longer exists. Callers may treat this as permanently
    non-retryable (dead-letter) rather than burning backoff on a doomed turn.
    """

    pass


def _command_argv(command: Any) -> list[str]:
    if command is not None:
        if isinstance(command, list) and command:
            return [str(part) for part in command]
        if isinstance(command, str) and command:
            return [command]
        raise OpenCodeBridgeError("opencode_command must be a string path or argv list")
    configured = os.environ.get("VANTH_OPENCODE_BIN")
    if configured:
        return [configured]
    found = shutil.which("opencode")
    if found:
        if sys.platform == "win32" and Path(found).suffix.lower() in {".cmd", ".bat"}:
            # The npm shim forwards arguments through `%*`. Embedded newlines in
            # a wake prompt are then interpreted by cmd.exe, and live testing
            # showed OpenCode receiving only the first line (`vanth event`).
            # Prefer the native binary shipped by the standard npm package.
            native = Path(found).parent / "node_modules" / "opencode-ai" / "bin" / "opencode.exe"
            if native.is_file():
                return [str(native)]
        return [found]
    return ["opencode"]


# Probe results keyed (session_id, directory, command): `session list` costs a
# full subprocess (~100ms+) per wake delivery and stalls one of 4 delivery
# threads. Sessions die rarely; a brief cache is safe (a miss only wastes one
# turn, exactly like skip_probe; ids are never reused).
_session_probe_cache: dict[tuple, tuple[float, bool | None]] = {}


def _session_probe_ttl() -> float:
    try:
        return max(0.0, float(os.environ.get("VANTH_SESSION_PROBE_TTL_SECONDS", "60")))
    except ValueError:
        return 60.0


def _session_exists(session_id: str, opencode_command: Any, timeout_seconds: float = 5, directory: str | None = None) -> bool | None:
    """Probe whether an opencode session id is still live.

    Cheap non-model probe: runs `opencode session list --format json` and checks
    the parsed array for the session id. Returns True when present, False when
    the probe succeeded but the id is absent, and None on ANY ambiguity
    (timeout, spawn failure, non-zero exit, invalid JSON, unexpected error) —
    None means "can't tell" and must never block a valid dispatch.

    ``directory`` (the target session's cwd) scopes the probe to the SAME
    project context as the target session. OpenCode's ``session list`` supports
    only ``--max-count``/``--format`` (not ``--dir``), so the working directory
    is passed to the subprocess via ``cwd=`` rather than an unsupported flag
    (review P0-3).

    Conclusive answers are cached briefly (see ``_session_probe_ttl``); only
    ambiguity (None) is never cached, so a transient failure always retries.
    """
    key = (session_id, directory or "", str(opencode_command))
    ttl = _session_probe_ttl()
    if ttl > 0 and key in _session_probe_cache:
        at, cached = _session_probe_cache[key]
        if time.monotonic() - at < ttl:
            return cached
    argv = _command_argv(opencode_command) + ["session", "list", "--format", "json"]
    try:
        result = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            **({"cwd": directory} if directory else {}),
        )
    except (subprocess.TimeoutExpired, OSError):
        return None
    if result.returncode:
        return None
    try:
        sessions = json.loads(result.stdout)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(sessions, list):
        return None
    for item in sessions:
        if isinstance(item, dict) and item.get("id") == session_id:
            found: bool | None = True
            break
    else:
        found = False
    if ttl > 0:
        # Bound the cache: distinct session ids are otherwise unbounded over a
        # months-long daemon lifetime (each entry is tiny, but never evicted).
        if len(_session_probe_cache) >= 1000:
            _session_probe_cache.pop(next(iter(_session_probe_cache)))
        _session_probe_cache[key] = (time.monotonic(), found)
    return found


def send_message_to_session(
    session_id: str,
    prompt: str,
    *,
    opencode_command: Any = None,
    timeout_seconds: int = 30,
    directory: str | None = None,
    attach: str | None = None,
    skip_probe: bool = False,
    auth: dict[str, Any] | None = None,
) -> dict[str, Any]:
    # Probe-before-dispatch: when the target is a plain (non-attached) session
    # and the caller did not opt out, check the session is still live before
    # burning a model turn. The probe NEVER blocks — only a confirmed-missing
    # session (a permanent, retry-free failure) raises; ambiguity proceeds.
    # The probe runs against the target's cwd so a cross-project session is not
    # misclassified as missing (review P0-3).
    skip_probe = skip_probe or os.environ.get("VANTH_OPENCODE_SKIP_PROBE") == "1"
    if not skip_probe and not attach:
        found = _session_exists(session_id, opencode_command, directory=directory)
        if found is False:
            raise OpenCodeSessionNotFound(
                f"opencode session not found: {session_id} "
                "(stale or removed; refresh the wake target's session_id or start a new session)"
            )

    command_argv = _command_argv(opencode_command)
    # A user may explicitly configure a batch shim, or use a nonstandard install
    # where its native binary cannot be resolved. Keep every wake field intact
    # by flattening line breaks before cmd.exe expands `%*`.
    prompt_arg = prompt
    if sys.platform == "win32" and Path(command_argv[0]).suffix.lower() in {".cmd", ".bat"}:
        prompt_arg = " ".join(prompt.splitlines())
    argv = command_argv + ["run", "--session", session_id]
    if directory:
        argv += ["--dir", directory]
    if attach:
        argv += ["--attach", attach]
    argv += ["--format", "json", prompt_arg]

    env = None
    if auth:
        # Non-persisted credential references (review P0-3): only environment
        # variable NAMES are accepted — never literal secret values. The values
        # are read from the daemon's environment and forwarded to the opencode
        # subprocess as the documented OPENCODE_SERVER_USERNAME /
        # OPENCODE_SERVER_PASSWORD variables, without writing secrets to disk or
        # into wake-target config/delivery payloads.
        #
        # Review rc36 P2: ONLY the unambiguous ``username_env`` / ``password_env``
        # keys are accepted. The legacy ``auth.username`` / ``auth.password``
        # aliases were ambiguous (an identifier-looking literal such as "bot" is
        # indistinguishable from a variable reference) and a missing referenced
        # variable silently became an empty string. A referenced-but-absent
        # variable is now an explicit error instead of a confusing auth failure.
        if not isinstance(auth, dict):
            raise OpenCodeBridgeError("auth must be an object with username_env/password_env keys")
        legacy = set(auth).intersection({"username", "password"})
        if legacy:
            raise OpenCodeBridgeError(
                "auth.username/auth.password are ambiguous legacy aliases (a literal "
                "value like 'bot' is indistinguishable from a variable reference); "
                f"use username_env/password_env instead (got: {sorted(legacy)})"
            )
        env = os.environ.copy()
        for key, var_name, target_key in (
            ("username_env", "username", "OPENCODE_SERVER_USERNAME"),
            ("password_env", "password", "OPENCODE_SERVER_PASSWORD"),
        ):
            if key not in auth:
                continue
            value = auth[key]
            if not isinstance(value, str) or not value.isidentifier():
                raise OpenCodeBridgeError(
                    f"auth.{key} must be an environment variable NAME (e.g. {target_key}), not a literal value"
                )
            if value not in os.environ:
                raise OpenCodeBridgeError(
                    f"auth.{key} references environment variable {value!r} which is not set"
                )
            env[target_key] = os.environ[value]

    try:
        result = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            **({"env": env} if env is not None else {}),
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise OpenCodeBridgeError(f"opencode session {session_id} timed out after {timeout_seconds} seconds") from exc
    except OSError as exc:
        raise OpenCodeBridgeError(f"failed to start opencode: {exc}") from exc

    if result.returncode:
        detail = (result.stderr or result.stdout).strip()
        suffix = f": {detail}" if detail else ""
        raise OpenCodeBridgeError(f"opencode exited with {result.returncode}{suffix}")
    return {"session_id": session_id, "stdout": result.stdout, "stderr": result.stderr}


def _timeout_seconds(target: dict[str, Any], default: int = 300) -> int:
    """Read an integer timeout from a wake target with a clear error.

    Daemon-stored targets are validated at creation, but direct callers can
    pass anything: a bare int() would surface `invalid literal...` instead
    of naming the field.
    """
    value = target.get("timeout_seconds", default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise OpenCodeBridgeError("timeout_seconds must be an integer number of seconds")
    return value


def send_delivery_to_opencode(payload: dict[str, Any]) -> dict[str, Any]:
    target = payload.get("target") or {}
    config = target.get("config")
    if not isinstance(config, dict):
        config = {}
    session_id = (
        target.get("session_id")
        or target.get("sessionId")
        or target.get("thread_id")
        or target.get("threadId")
    )
    prompt = payload.get("prompt")
    if not isinstance(session_id, str) or not session_id:
        raise OpenCodeBridgeError("opencode_thread target requires session_id")
    if not isinstance(prompt, str) or not prompt:
        raise OpenCodeBridgeError("delivery payload requires prompt")

    directory = target.get("cwd") or target.get("dir")
    attach = target.get("attach")
    skip_probe = target.get("skip_probe") or config.get("skip_probe")
    if directory is not None and (not isinstance(directory, str) or not directory):
        raise OpenCodeBridgeError("cwd or dir must be a non-empty string")
    if attach is not None and (not isinstance(attach, str) or not attach):
        raise OpenCodeBridgeError("attach must be a non-empty string")

    return send_message_to_session(
        session_id,
        prompt,
        opencode_command=target.get("opencode_command"),
        # A busy session cannot take a new turn until the active one finishes;
        # the delivery worker is a background thread, so default to a generous
        # wait (5 min) instead of 30s. Per-target override still wins.
        timeout_seconds=_timeout_seconds(target),
        directory=directory,
        attach=attach,
        skip_probe=skip_probe,
        auth=target.get("auth"),
    )


def main() -> None:
    payload = json.load(sys.stdin)
    print(json.dumps(send_delivery_to_opencode(payload), separators=(",", ":")))
