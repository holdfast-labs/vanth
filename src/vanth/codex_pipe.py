"""Native app-tools host-pipe client for Codex Desktop wake (review rc36 P0).

Codex Desktop runs a private app-tools JSON-RPC server on a Windows named pipe
or a Unix domain socket on Linux/macOS. Its path is inherited by the MCP process
as ``CODEX_APP_TOOLS_PIPE_PATH``.
The bundled app-tools MCP server exposes ``codex_app/send_message_to_thread``
on that pipe; calling it submits a follow-up prompt into the already-running
Desktop task — the visible app owns the task writer, so this never hits the
second-writer problem of spawning a second ``codex app-server``.

This module contains ONLY the framing + call logic. It takes the pipe path from
the current process's inherited environment (never from a job target or daemon
message), implements the length-prefixed JSON-RPC framing with strict size
caps and exact/fragmented reads, preflights the capability, and submits a
``tools/call`` envelope. It is deliberately small and replaceable so a future
stable public Desktop endpoint can swap in without touching the wake/relay
machinery.
"""

from __future__ import annotations

import json
import os
import queue
import socket
import struct
import subprocess
import sys
import threading
import time
from typing import Any

MAX_FRAME_BYTES = 8 * 1024 * 1024
DEFAULT_TIMEOUT_SECONDS = 30.0
# Allowance for the helper subprocess's own startup/teardown on top of the wake
# sequence budget. Kept tiny so the helper hard deadline is always strictly
# shorter than the claim lease (review rc39 P1).
_HELPER_PROCESS_BUFFER_SECONDS = 1.0


def wake_sequence_budget(timeout_seconds: float) -> float:
    """The end-to-end wall-clock budget for ONE Desktop wake.

    This single budget covers the WHOLE sequence — ``tools/list`` preflight AND
    ``tools/call`` — so a slow preflight cannot consume a second full timeout
    (review rc39 P1). ``timeout_seconds`` is the operator-configured
    ``timeout_seconds`` on the wake target.
    """
    return max(1.0, float(timeout_seconds))


def helper_hard_deadline(timeout_seconds: float) -> float:
    """The helper subprocess's hard wall-clock lifetime for one wake sequence.

    The helper performs the whole sequence under the shared wake budget plus a
    tiny process buffer. This is STRICTLY shorter than the claim lease
    (``claim_lease_seconds``), which guarantees a stalled helper dies before the
    daemon reclaims the delivery (review rc38/rc39 P1).
    """
    return wake_sequence_budget(timeout_seconds) + _HELPER_PROCESS_BUFFER_SECONDS


def claim_lease_seconds(timeout_seconds: float, margin: int) -> float:
    """The delivery claim lease length for a Desktop wake.

    Derived from the SAME end-to-end deadline as the helper: the lease is the
    helper hard deadline PLUS the operator-configured margin. Because the helper
    deadline already includes the process buffer, ``lease > helper`` holds for
    EVERY ``margin >= 1`` — there is no duplicated/test-only arithmetic with a
    hardcoded default margin (review rc39 P1).
    """
    return helper_hard_deadline(timeout_seconds) + max(1, int(margin))


def _produce(target_queue: "queue.Queue[Any]", fn, *args: Any) -> None:
    try:
        target_queue.put(fn(*args))
    except Exception as exc:  # noqa: BLE001 - surfaced via the queue
        target_queue.put(exc)


class CodexPipeError(RuntimeError):
    """A protocol/host failure talking to the Codex Desktop app-tools pipe.

    The error text is surfaced in delivery errors but never includes pipe
    details (the path may be sensitive/private). Raw exceptions carrying the
    path are logged separately; only sanitized text reaches delivery errors.
    """

    pass


class CodexPipeUnavailable(CodexPipeError):
    """The Desktop app-tools capability is not present or not reachable.

    Raised when the inherited env lacks ``CODEX_APP_TOOLS_PIPE_PATH``, the pipe
    cannot be opened, or the capability is not advertised. Callers treat this as
    fail-closed (never fall back to a second app-server). The message is
    sanitized — the raw OSError (which may embed the named-pipe path) is never
    included in the public text.
    """

    pass


def app_tools_pipe_path() -> str | None:
    """Return the inherited Desktop app-tools pipe path, if any."""
    value = os.environ.get("CODEX_APP_TOOLS_PIPE_PATH")
    if not value:
        return None
    return value


def _read_exact(handle, length: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < length:
        try:
            if hasattr(handle, "recv"):
                chunk = handle.recv(length - len(chunks))
            else:
                chunk = handle.read(length - len(chunks))
        except OSError as exc:
            # A reset/aborted read on Windows surfaces as ConnectionResetError;
            # the peer is gone either way — treat as a closed connection.
            raise CodexPipeError("Codex Desktop app-tools host closed the connection") from exc
        if not chunk:
            raise CodexPipeError("Codex Desktop app-tools host closed the connection")
        chunks.extend(chunk)
    return bytes(chunks)


def _write_all(handle, data: bytes) -> None:
    view = memoryview(data)
    while view:
        try:
            if hasattr(handle, "send"):
                written = handle.send(view)
            else:
                written = handle.write(view)
        except OSError as exc:
            if isinstance(exc, (BrokenPipeError, ConnectionResetError)):
                # A peer that has gone away surfaces as EPIPE/ECONNRESET on
                # write on POSIX and ECONNRESET on Windows; report it the same
                # way as a read-side close so callers see one consistent error.
                raise CodexPipeError("Codex Desktop app-tools host closed the connection") from exc
            raise CodexPipeError("lost connection to Codex Desktop app-tools host (write)") from exc
        if written is None or written <= 0:
            raise CodexPipeError("Codex Desktop app-tools host closed the connection (write)")
        view = view[written:]


class CodexPipeClient:
    """Serialize one JSON-RPC request/response over the Desktop pipe or socket.

    A single client instance is used for ONE call sequence (tools/list preflight
    then tools/call) and then closed. The blocking read loop runs on a worker
    thread so ``call()`` can enforce a wall-clock deadline by closing the handle
    (which unblocks the reader). Production delivery additionally runs the whole
    sequence in a killable helper subprocess with a hard deadline so a stalled
    named-pipe read can never hang the relay thread (review rc37 P1).
    """

    def __init__(self, pipe_path: str, timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS) -> None:
        if not pipe_path:
            raise CodexPipeUnavailable("Codex Desktop app-tools pipe is not configured")
        self.pipe_path = pipe_path
        self.timeout_seconds = timeout_seconds
        # The end-to-end sequence deadline: BOTH calls (tools/list preflight and
        # tools/call) share ONE budget so a slow preflight cannot consume a
        # second full timeout (review rc39 P1).
        self.sequence_deadline = time.monotonic() + wake_sequence_budget(timeout_seconds)
        try:
            if sys.platform == "win32":
                # Windows named pipes expose a file-like byte stream.
                self.handle = open(pipe_path, "r+b", buffering=0)  # noqa: SIM115
            else:
                # Unix socket paths cannot be opened as files (ENXIO on Linux).
                # Connect within the same budget as the subsequent RPC calls.
                handle = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                try:
                    handle.settimeout(max(0.0, self.sequence_deadline - time.monotonic()))
                    handle.connect(pipe_path)
                    handle.settimeout(None)
                except OSError:
                    handle.close()
                    raise
                self.handle = handle
        except OSError as exc:
            # Never embed the raw OSError: Windows errors contain the full
            # named-pipe path, which must not reach delivery errors (P2).
            raise CodexPipeUnavailable(
                "cannot open Codex Desktop app-tools pipe (Desktop integration unavailable; "
                "restart/update Desktop or re-register Vanth)"
            ) from exc
        self._request_id = 0

    def close(self) -> None:
        handle = getattr(self, "handle", None)
        if handle is None:
            return
        # Shutting down a socket before closing it reliably wakes a recv()
        # blocked on another thread on POSIX; closing alone does not. Named
        # pipe/file handles have no shutdown() and are unaffected.
        shutdown = getattr(handle, "shutdown", None)
        if shutdown is not None:
            try:
                shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        try:
            if hasattr(handle, "close"):
                handle.close()
        except Exception:
            pass

    def _next_id(self) -> int:
        self._request_id += 1
        return self._request_id

    def _send_frame(self, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        if len(body) > MAX_FRAME_BYTES:
            raise CodexPipeError("request frame exceeds 8 MiB cap")
        _write_all(self.handle, struct.pack("<I", len(body)) + body)

    def _read_frame(self) -> dict[str, Any]:
        header = _read_exact(self.handle, 4)
        (length,) = struct.unpack("<I", header)
        if length > MAX_FRAME_BYTES:
            raise CodexPipeError("response frame exceeds 8 MiB cap")
        body = _read_exact(self.handle, length)
        try:
            message = json.loads(body.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise CodexPipeError("invalid JSON response from Codex Desktop app-tools host") from exc
        if not isinstance(message, dict):
            raise CodexPipeError("malformed response from Codex Desktop app-tools host")
        return message

    def _read_loop(self, request_id: int, deadline: float) -> dict[str, Any]:
        """Blocking reader used on a worker thread; returns the response."""
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise CodexPipeError(
                    f"timed out waiting for Codex Desktop response after {self.timeout_seconds}s"
                )
            message = self._read_frame()
            if message.get("id") != request_id:
                continue
            if "error" in message:
                error = message["error"]
                detail = error.get("message", "codex app-tools error") if isinstance(error, dict) else str(error)
                raise CodexPipeError(detail)
            result = message.get("result")
            return result if isinstance(result, dict) else {"result": result}

    def call(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        """Send a JSON-RPC request and wait for its response with a hard
        wall-clock deadline.

        The blocking read runs on a worker thread; if the deadline elapses the
        handle is closed (unblocking the reader) and a timeout error is raised.
        This is what lets a stalled host be interrupted in-process (tests). In
        production the whole sequence also runs inside a killable helper
        subprocess, so a named-pipe read that cannot be unblocked by close() is
        still killed at the process boundary (review rc37 P1).
        """
        # Each call shares the ONE sequence deadline: the call's own timeout is
        # capped by the remaining end-to-end budget (review rc39 P1), so a slow
        # preflight cannot consume a second full timeout. Tests that construct
        # the client via __new__ may not set sequence_deadline; fall back to a
        # fresh per-call deadline then.
        sequence_deadline = getattr(self, "sequence_deadline", None)
        now = time.monotonic()
        if sequence_deadline is not None:
            if sequence_deadline - now <= 0:
                # Budget already exhausted (e.g. a slow preflight consumed it):
                # fail WITHOUT sending a doomed frame the host might still act
                # on (self-review rc40).
                raise CodexPipeError(
                    f"timed out waiting for Codex Desktop {method} after {self.timeout_seconds}s (sequence budget exhausted)"
                )
            deadline = min(now + self.timeout_seconds, sequence_deadline)
        else:
            deadline = now + self.timeout_seconds
        request_id = self._next_id()
        self._send_frame(
            {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
        )
        result_queue: queue.Queue[Any] = queue.Queue()
        reader = threading.Thread(
            target=_produce,
            args=(result_queue, self._read_loop, request_id, deadline),
            name="vanth-pipe-reader",
            daemon=True,
        )
        reader.start()
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    # Close the handle to unblock the worker, then reap it.
                    self.close()
                    reader.join(timeout=2)
                    raise CodexPipeError(
                        f"timed out waiting for Codex Desktop {method} after {self.timeout_seconds}s"
                    )
                try:
                    outcome = result_queue.get(timeout=min(0.25, remaining))
                except queue.Empty:
                    continue
                if isinstance(outcome, BaseException):
                    raise outcome
                return outcome
        except CodexPipeError:
            raise
        except Exception:
            self.close()
            reader.join(timeout=2)
            raise

    def tools_list(self) -> list[dict[str, Any]]:
        result = self.call("tools/list", {"threadStartKind": "all"})
        tools = result.get("tools", result.get("toolsList", []))
        if not isinstance(tools, list):
            raise CodexPipeError("malformed tools/list response from Codex Desktop app-tools host")
        return [tool for tool in tools if isinstance(tool, dict)]

    def _tool_namespace_and_name(self, tool: dict[str, Any]) -> tuple[str | None, str | None]:
        """Extract (namespace, name) from a tools/list entry.

        The live Desktop catalog reports them as SEPARATE fields
        (``namespace="codex_app"``, ``name="send_message_to_thread"``); some
        builds also emit a combined ``name`` like ``"codex_app/send_message_to_thread"``.
        Both shapes are accepted (review rc37 P0).
        """
        namespace = tool.get("namespace")
        name = tool.get("name")
        if namespace and name:
            return namespace, name
        if name and isinstance(name, str) and "/" in name:
            parts = name.split("/", 1)
            return parts[0], parts[1]
        return namespace, name

    def has_capability(self, namespace: str, name: str) -> bool:
        return any(
            self._tool_namespace_and_name(tool) == (namespace, name)
            for tool in self.tools_list()
        )

    def send_message_to_thread(
        self,
        *,
        destination_thread_id: str,
        prompt: str,
        caller_thread_id: str,
        call_id: str,
    ) -> dict[str, Any]:
        """Preflight the capability then submit a wake prompt into the task.

        ``caller_thread_id`` (the outer ``params.threadId``) is REQUIRED: the
        native host rejects ``tools/call`` without it (``-32602 Invalid app tool
        request``). The destination is ``arguments.threadId``. Both identities
        are required (review rc37 P0).
        """
        if not isinstance(caller_thread_id, str) or not caller_thread_id:
            raise CodexPipeError(
                "Codex Desktop wake requires a caller thread id (the relay's executor identity); "
                "refusing to call the host without it"
            )
        if not self.has_capability("codex_app", "send_message_to_thread"):
            raise CodexPipeError(
                "Codex Desktop app-tools host does not advertise codex_app/send_message_to_thread "
                "(update Desktop or re-register Vanth)"
            )
        arguments: dict[str, Any] = {"threadId": destination_thread_id, "prompt": prompt}
        params: dict[str, Any] = {
            "arguments": arguments,
            "callerSource": "codex",
            "callId": call_id,
            "namespace": "codex_app",
            "tool": "send_message_to_thread",
            "threadId": caller_thread_id,
            "turnId": call_id,
        }
        result = self.call("tools/call", params)
        if result.get("success") is not True:
            native_text = " ".join(
                item["text"] for item in result.get("contentItems", [])
                if isinstance(item, dict) and item.get("type") == "inputText" and isinstance(item.get("text"), str)
            )
            error_text = result.get("error") or result.get("message") or native_text or "send_message_to_thread did not report success"
            raise CodexPipeError(str(error_text))
        return result


def send_desktop_message(
    *,
    destination_thread_id: str,
    prompt: str,
    caller_thread_id: str,
    call_id: str,
    pipe_path: str | None = None,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    _handle: Any = None,
) -> dict[str, Any]:
    """Open the inherited pipe and submit a Desktop wake prompt.

    ``pipe_path`` supplies the relay's resolved capability, overriding the
    inherited ``CODEX_APP_TOOLS_PIPE_PATH``. ``_handle`` is a test-only socket/file
    handle that bypasses the transport connection.

    Production runs the blocking pipe I/O in a killable helper subprocess with a
    hard deadline (review rc37 P1) so a stalled named-pipe read can never hang
    the relay thread; the resolved path is passed to the helper on stdin.
    The hard deadline is strictly shorter than the delivery
    claim lease (review rc38 P1).
    """
    if not isinstance(caller_thread_id, str) or not caller_thread_id:
        raise CodexPipeError(
            "Codex Desktop wake requires a caller thread id; refusing before any pipe I/O"
        )
    if _handle is not None:
        client = CodexPipeClient.__new__(CodexPipeClient)
        client.handle = _handle
        client.timeout_seconds = timeout_seconds
        client._request_id = 0
        try:
            return client.send_message_to_thread(
                destination_thread_id=destination_thread_id,
                prompt=prompt,
                caller_thread_id=caller_thread_id,
                call_id=call_id,
            )
        finally:
            client.close()
    path = pipe_path or app_tools_pipe_path()
    if not path:
        raise CodexPipeUnavailable(
            "Codex Desktop wake is unavailable: CODEX_APP_TOOLS_PIPE_PATH is not set "
            "(Desktop integration not active; restart/update Desktop or re-register Vanth)"
        )
    request = {
        "pipe_path": path,
        "timeout_seconds": timeout_seconds,
        "destination_thread_id": destination_thread_id,
        "prompt": prompt,
        "caller_thread_id": caller_thread_id,
        "call_id": call_id,
    }
    argv = [sys.executable, "-m", "vanth.codex_pipe", "--helper"]
    # Strictly shorter than the claim lease (derived from the SAME end-to-end
    # deadline) so a stalled helper can never outlive the lease and cause
    # concurrent duplicate delivery (review rc38/rc39 P1).
    hard_deadline = helper_hard_deadline(timeout_seconds)
    try:
        result = subprocess.run(
            argv,
            input=json.dumps(request, separators=(",", ":")),
            capture_output=True,
            text=True,
            timeout=hard_deadline,
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) if sys.platform == "win32" else 0,
            start_new_session=sys.platform != "win32",
        )
    except subprocess.TimeoutExpired as exc:
        # The helper was killed by subprocess.run at the hard deadline.
        raise CodexPipeError(
            f"Codex Desktop wake timed out after {timeout_seconds}s (host did not respond)"
        ) from exc
    except OSError as exc:
        raise CodexPipeUnavailable(
            "Codex Desktop wake unavailable: could not start the Desktop bridge helper"
        ) from exc
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        # Sanitized: never surface the pipe path from stderr.
        if stderr:
            raise CodexPipeError(f"Codex Desktop wake failed: {stderr[:500]}")
        raise CodexPipeError("Codex Desktop wake failed (Desktop integration unavailable)")
    try:
        payload = json.loads(result.stdout)
    except (json.JSONDecodeError, TypeError) as exc:
        raise CodexPipeError("Codex Desktop bridge helper returned an invalid response") from exc
    if payload.get("ok") is not True:
        # Re-raise pipe-unavailable as its own class (see _helper_main): the
        # relay matches on it to reload a re-provisioned capability after a
        # Desktop restart. The message stays sanitized (no pipe path).
        if payload.get("unavailable") is True:
            raise CodexPipeUnavailable(payload.get("error") or "Codex Desktop app-tools pipe is not available")
        raise CodexPipeError(payload.get("error", "Codex Desktop wake failed"))
    return payload.get("result", {})


def _helper_main() -> int:
    """Helper subprocess: perform one pipe call sequence with a hard deadline."""
    import queue as _q

    request = json.load(sys.stdin)
    pipe_path = request.get("pipe_path")
    timeout_seconds = float(request.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS))
    try:
        client = CodexPipeClient(pipe_path, timeout_seconds)
        try:
            result = client.send_message_to_thread(
                destination_thread_id=request["destination_thread_id"],
                prompt=request["prompt"],
                caller_thread_id=request["caller_thread_id"],
                call_id=request["call_id"],
            )
            sys.stdout.write(json.dumps({"ok": True, "result": result}, separators=(",", ":")))
            sys.stdout.flush()
            return 0
        finally:
            client.close()
    except CodexPipeUnavailable as exc:
        # Preserve the UNAVAILABLE class across the process boundary so the
        # relay can distinguish a dead pipe (Desktop restart → reload capability)
        # from other failures. Still sanitized: never the pipe path.
        # (self-review rc40: without this flag every production outage arrived
        # as plain CodexPipeError and the reload path never fired.)
        sys.stdout.write(json.dumps({"ok": False, "unavailable": True, "error": str(exc)}, separators=(",", ":")))
        sys.stdout.flush()
        return 0
    except CodexPipeError as exc:
        sys.stdout.write(json.dumps({"ok": False, "error": str(exc)}, separators=(",", ":")))
        sys.stdout.flush()
        return 0
    except Exception as exc:
        # Do not leak the pipe path from an unexpected raw exception.
        sys.stdout.write(
            json.dumps({"ok": False, "error": "Codex Desktop bridge helper failed unexpectedly"}, separators=(",", ":"))
        )
        sys.stdout.flush()
        return 1


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "--helper":
        raise SystemExit(_helper_main())
    # Interactive misuse guard: reading a JSON request from stdin.
    request = json.load(sys.stdin)
    pipe_path = request.get("pipe_path")
    timeout_seconds = float(request.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS))
    client = CodexPipeClient(pipe_path, timeout_seconds)
    try:
        result = client.send_message_to_thread(
            destination_thread_id=request["destination_thread_id"],
            prompt=request["prompt"],
            caller_thread_id=request["caller_thread_id"],
            call_id=request["call_id"],
        )
        print(json.dumps({"ok": True, "result": result}, separators=(",", ":")))
    finally:
        client.close()


if __name__ == "__main__":
    # Module entry point: production launches the helper as
    # `python -m vanth.codex_pipe --helper` (review rc38 P0). Without this the
    # subprocess exits 0 with empty stdout and every Desktop wake fails as an
    # invalid helper response.
    main()
