from __future__ import annotations

import json
import os
import queue
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any


class CodexBridgeError(RuntimeError):
    pass


class CodexActiveWriterError(CodexBridgeError):
    """Codex Desktop's own app-server owns the task.

    A second app-server cannot take a turn on a thread that already has an
    active writer (``thread ... already has an active writer``). This is a
    PERMANENT condition for a Desktop task while it is live — retrying against
    the same wall is pointless. Callers should treat it as non-retryable
    (dead-letter) rather than burning backoff.
    """

    pass


_INITIALIZE_RETRY_DELAYS: tuple[float, ...] = (0.5, 1.0)


def _default_codex_command() -> list[str]:
    configured = os.environ.get("VANTH_CODEX_BIN")
    if configured:
        return [configured]
    # Prefer the Desktop-managed binary (which matches the running Desktop
    # app's protocol) over the standalone CLI build. Desktop's binary lives
    # under %LOCALAPPDATA%\\Programs\\codex on Windows; the legacy hard-coded
    # C:\\codex\\codex.exe may be a different (older) build (review P0-2).
    if sys.platform == "win32":
        local_appdata = os.environ.get("LOCALAPPDATA")
        if local_appdata:
            desktop_bin = Path(local_appdata) / "Programs" / "codex" / "codex.exe"
            if desktop_bin.exists():
                return [str(desktop_bin)]
        legacy = Path(r"C:\codex\codex.exe")
        if legacy.exists():
            return [str(legacy)]
    return ["codex"]


def _command_argv(command: Any) -> list[str]:
    if command is None:
        return _default_codex_command()
    if isinstance(command, list):
        return [str(part) for part in command]
    if isinstance(command, str):
        return [command]
    raise CodexBridgeError("codex_command must be a string path or argv list")


def _reader(stream, out: queue.Queue[str]) -> None:
    while True:
        line = stream.readline()
        if not line:
            return
        out.put(line.rstrip("\r\n"))


class _CodexAppServer:
    def __init__(self, command: Any, timeout_seconds: int) -> None:
        argv = _command_argv(command) + ["app-server", "--listen", "stdio://", "--analytics-default-enabled"]
        self.deadline = time.monotonic() + timeout_seconds
        self.stdout: queue.Queue[str] = queue.Queue()
        self.stderr: queue.Queue[str] = queue.Queue()
        self.stderr_tail: list[str] = []
        self.completed_turns: list[dict[str, Any]] = []
        # Match runner.py/server.py: detach the codex child from the daemon's console group on Windows.
        creationflags = 0
        if sys.platform == "win32":
            creationflags = subprocess.CREATE_NEW_PROCESS_GROUP | getattr(subprocess, "CREATE_BREAKAWAY_FROM_JOB", 0)
        try:
            self.proc = subprocess.Popen(
                argv,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                creationflags=creationflags,
            )
        except OSError as exc:
            raise CodexBridgeError(f"failed to launch codex app-server: {exc}") from exc
        assert self.proc.stdout is not None
        assert self.proc.stderr is not None
        threading.Thread(target=_reader, args=(self.proc.stdout, self.stdout), daemon=True).start()
        threading.Thread(target=_reader, args=(self.proc.stderr, self.stderr), daemon=True).start()

    def close(self) -> None:
        if self.proc.poll() is not None:
            return
        try:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=2)
        except Exception:
            pass

    def send(self, request_id: int, method: str, params: dict[str, Any]) -> None:
        if self.proc.poll() is not None:
            raise CodexBridgeError(self._error(f"codex app-server exited with {self.proc.returncode}"))
        if self.proc.stdin is None:
            raise CodexBridgeError("codex app-server stdin closed")
        try:
            self.proc.stdin.write(json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}) + "\n")
            self.proc.stdin.flush()
        except (BrokenPipeError, OSError) as exc:
            exit_code = self.proc.poll()
            if exit_code is None:
                raise CodexBridgeError(self._error("codex app-server exited (broken pipe)")) from exc
            raise CodexBridgeError(self._error(f"codex app-server exited with {exit_code}")) from exc

    def response(self, request_id: int) -> dict[str, Any]:
        while True:
            while not self.stderr.empty():
                self.stderr_tail.append(self.stderr.get())
                self.stderr_tail = self.stderr_tail[-5:]
            remaining = self.deadline - time.monotonic()
            if remaining <= 0:
                raise CodexBridgeError(self._error("timed out waiting for codex app-server"))
            try:
                line = self.stdout.get(timeout=min(0.1, remaining))
            except queue.Empty:
                if self.proc.poll() is not None:
                    raise CodexBridgeError(self._error(f"codex app-server exited with {self.proc.returncode}"))
                continue
            try:
                message = json.loads(line)
            except json.JSONDecodeError:
                continue
            # Stash turn/completed notifications seen while waiting for a
            # response: they can arrive before the ack for the turn/start
            # request (notification ordering is not guaranteed), and the
            # turn-completion waiter must not miss them.
            if message.get("method") == "turn/completed":
                self.completed_turns.append(message.get("params", {}).get("turn") or {})
                continue
            if message.get("id") != request_id:
                continue
            if "error" in message:
                error_text = message["error"].get("message", "codex app-server error")
                if "active writer" in error_text.lower():
                    raise CodexActiveWriterError(self._error(f"codex desktop task already has an active writer: {error_text}"))
                raise CodexBridgeError(self._error(error_text))
            return message.get("result", {})

    def _error(self, message: str) -> str:
        if not self.stderr_tail:
            return message
        return f"{message}: {' | '.join(self.stderr_tail)}"

    def _check_turn_status(self, turn: dict[str, Any]) -> None:
        """Raise when a completed turn did NOT actually run to a delivered
        outcome. ``interrupted`` (review P2) and ``failed`` are failures, not
        successes — a wake must not report delivered when the model was cut off."""
        status = turn.get("status")
        if status in ("failed", "interrupted"):
            error = (turn.get("error") or {}).get("message", f"turn {status}")
            raise CodexBridgeError(self._error(f"codex turn {status}: {error}"))

    def wait_for_turn_completed(self, request_id: int, turn_id: str | None) -> dict[str, Any]:
        """Wait until the turn started by ``request_id`` finishes.

        The app-server emits ``turn/completed`` (with the final Turn) when the
        model finishes, errors, or is interrupted. Waiting here is what makes
        "delivered" mean "the wake actually ran" rather than "the turn was
        accepted". The turn id is matched when known; the request id is kept
        as a fallback for older servers that echo it differently.
        """
        # Check notifications already drained while reading the ack first.
        for turn in list(self.completed_turns):
            if turn_id is None or turn.get("id") in (None, turn_id):
                self.completed_turns.remove(turn)
                self._check_turn_status(turn)
                return turn
        while True:
            while not self.stderr.empty():
                self.stderr_tail.append(self.stderr.get())
                self.stderr_tail = self.stderr_tail[-5:]
            remaining = self.deadline - time.monotonic()
            if remaining <= 0:
                raise CodexBridgeError(self._error("timed out waiting for turn/completed"))
            try:
                line = self.stdout.get(timeout=min(0.1, remaining))
            except queue.Empty:
                if self.proc.poll() is not None:
                    raise CodexBridgeError(self._error(f"codex app-server exited with {self.proc.returncode}"))
                continue
            try:
                message = json.loads(line)
            except json.JSONDecodeError:
                continue
            method = message.get("method")
            if method == "turn/completed":
                turn = message.get("params", {}).get("turn") or {}
                if turn_id is None or turn.get("id") in (None, turn_id):
                    self._check_turn_status(turn)
                    return turn
            # Responses to unrelated requests and other notifications are drained
            # silently; a response error for OUR turn is surfaced.
            if message.get("id") == request_id and "error" in message:
                raise CodexBridgeError(self._error(message["error"].get("message", "codex app-server error")))


def _timeout_seconds(target: dict[str, Any], key: str, default: int) -> int:
    """Read an integer timeout from a wake target with a clear error.

    Daemon-stored targets are validated at creation, but direct callers (the
    `vanth-codex-notify` adapter, tests) can pass anything: a bare int()
    would surface `invalid literal...` instead of naming the field.
    """
    value = target.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise CodexBridgeError(f"{key} must be an integer number of seconds")
    return value


def send_message_to_thread(
    thread_id: str,
    prompt: str,
    *,
    codex_command: Any = None,
    timeout_seconds: int = 30,
    turn_timeout_seconds: int | None = None,
) -> dict[str, Any]:
    server = _CodexAppServer(codex_command, timeout_seconds)
    try:
        last_error: CodexBridgeError | None = None
        for attempt in range(len(_INITIALIZE_RETRY_DELAYS) + 1):
            try:
                server.send(
                    1,
                    "initialize",
                    {"clientInfo": {"name": "vanth", "version": "0"}, "capabilities": {"experimentalApi": True}},
                )
                server.response(1)
                last_error = None
                break
            except CodexBridgeError as exc:
                last_error = exc
                if attempt >= len(_INITIALIZE_RETRY_DELAYS):
                    break
                delay = _INITIALIZE_RETRY_DELAYS[attempt]
                if server.deadline - time.monotonic() <= delay:
                    break
                time.sleep(delay)
        if last_error is not None:
            raise last_error
        server.send(2, "thread/resume", {"threadId": thread_id, "excludeTurns": True})
        server.response(2)
        server.send(3, "turn/start", {"threadId": thread_id, "input": [{"type": "text", "text": prompt}]})
        turn = server.response(3)
        turn_id = (turn.get("turn") or {}).get("id")        # A turn/start acknowledgment only means the turn was ACCEPTED
        # (status inProgress). The bridge closes the app-server process when
        # this function returns, which would kill an in-flight turn before
        # the model acts on it. Wait for turn/completed so "delivered" means
        # the agent actually processed the wake, not merely received it.
        # The model turn gets its own budget on top of setup: initialize /
        # resume / start are quick handshakes, while a real turn can take
        # minutes. Sharing one deadline made slow-but-healthy turns burn the
        # whole budget and then duplicate on retry.
        if turn_timeout_seconds is not None:
            server.deadline = max(server.deadline, time.monotonic() + turn_timeout_seconds)
        completed = server.wait_for_turn_completed(request_id=3, turn_id=turn_id)
        return {"thread_id": thread_id, "turn": completed}
    finally:
        server.close()


def _target_thread_id(target: dict[str, Any]) -> str | None:
    return target.get("thread_id") or target.get("threadId")


def send_delivery_to_codex(payload: dict[str, Any]) -> dict[str, Any]:
    target = payload.get("target") or {}
    target_type = target.get("type")
    thread_id = _target_thread_id(target)
    prompt = payload.get("prompt")
    if not isinstance(thread_id, str) or not thread_id:
        raise CodexBridgeError(f"{target_type} target requires thread_id")
    if not isinstance(prompt, str) or not prompt:
        raise CodexBridgeError("delivery payload requires prompt")
    return send_message_to_thread(
        thread_id,
        prompt,
        codex_command=target.get("codex_command"),
        timeout_seconds=_timeout_seconds(target, "timeout_seconds", 300),
        # Absent (or explicit null) keeps the historic single-budget behavior;
        # a set value extends the turn-completion wait past the setup budget.
        turn_timeout_seconds=(
            None if target.get("turn_timeout_seconds") is None
            else _timeout_seconds(target, "turn_timeout_seconds", 300)
        ),
    )


def send_delivery_to_codex_desktop(
    payload: dict[str, Any],
    *,
    caller_thread_id: str | None = None,
    pipe_path: str | None = None,
) -> dict[str, Any]:
    """Deliver a wake into a running Codex Desktop task via the native
    app-tools host pipe (review rc36 P0).

    This is the Desktop route: it calls ``codex_app/send_message_to_thread`` on
    the app-tools pipe so the follow-up lands in the already-running Desktop
    task. It NEVER spawns a second app-server and never falls back to the CLI
    thread bridge.

    ``pipe_path`` is the relay's RESOLVED app-tools pipe path (the explicit
    ``VANTH_CODEX_DESKTOP_PIPE`` / ``codex_desktop.json`` handoff). The
    delivery path itself must NOT fall back to ``CODEX_APP_TOOLS_PIPE_PATH``:
    real Vanth MCP children do not inherit it (review rc38 P0 — the provisioned
    pipe was discarded at delivery). When ``pipe_path`` is absent the inherited
    environment is read as a last resort (direct callers).

    ``caller_thread_id`` is the relay's authenticated executor/thread identity
    and is REQUIRED as the outer ``params.threadId`` by the native host. The
    relay injects its own identity here; the target's optional
    ``caller_thread_id`` is a legacy alias for direct callers. A delivery
    without either is rejected BEFORE any pipe I/O (review rc37 P0).
    """
    from .codex_pipe import CodexPipeUnavailable, send_desktop_message

    target = payload.get("target") or {}
    target_type = target.get("type")
    thread_id = _target_thread_id(target)
    prompt = payload.get("prompt")
    if not isinstance(thread_id, str) or not thread_id:
        raise CodexBridgeError(f"{target_type} target requires thread_id")
    if not isinstance(prompt, str) or not prompt:
        raise CodexBridgeError("delivery payload requires prompt")
    caller = caller_thread_id or target.get("caller_thread_id")
    if not isinstance(caller, str) or not caller:
        raise CodexBridgeError(
            "codex_desktop delivery requires a caller thread id (the relay's executor identity); "
            "refusing to call the Desktop host without it"
        )
    delivery_id = payload.get("delivery_id") or "vanth-delivery"
    try:
        result = send_desktop_message(
            destination_thread_id=thread_id,
            prompt=prompt,
            caller_thread_id=caller,
            call_id=f"vanth-{delivery_id}",
            pipe_path=pipe_path,
            timeout_seconds=_timeout_seconds(target, "timeout_seconds", 300),
        )
    except CodexPipeUnavailable:
        raise
    except Exception as exc:
        raise CodexBridgeError(f"Codex Desktop wake failed: {exc}") from exc
    return {"thread_id": thread_id, "desktop": True, "result": result}


def main() -> None:
    payload = json.load(sys.stdin)
    result = send_delivery_to_codex(payload)
    print(json.dumps(result, separators=(",", ":")))
