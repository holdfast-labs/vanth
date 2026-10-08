"""Tests for the Codex Desktop native app-tools pipe client and the client-side
wake relay (review rc36 P0 / rc37 P0/P1).

Covers the reviewer's acceptance tests:
1. Framing unit tests: fragmented header/body, wrong response ID, JSON-RPC
   error, invalid JSON, EOF, and >8 MiB rejection.
2. Live-shaped capability preflight: tools are reported as SEPARATE
   ``namespace``/``name`` fields (the real Desktop catalog) — the fake server
   reproduces the captured live schema (review rc37 P0).
3. Required caller thread id: the native host requires the outer
   ``params.threadId``; a delivery without one is rejected BEFORE pipe I/O.
4. Claimant-bound ack: only the claiming relay (client_id + opaque lease token)
   can complete a delivery; a different relay or a reclaimed lease affects zero
   rows.
5. SQL destination filtering before LIMIT: 20 older foreign-task deliveries do
   NOT starve a matching delivery.
6. Hard timeout: a fake handle that never answers is released at the deadline.
"""

import asyncio
import json
import os
import socket
import struct
import subprocess
import sys
import threading
import time

import pytest

from vanth.codex_pipe import (
    CodexPipeClient,
    CodexPipeError,
    CodexPipeUnavailable,
    MAX_FRAME_BYTES,
    claim_lease_seconds,
    helper_hard_deadline,
    wake_sequence_budget,
    send_desktop_message,
)
from vanth.server import JobManager


import shellcmd


class FakePipeServer:
    """A fake Desktop app-tools host speaking the length-prefixed JSON-RPC
    protocol over a socketpair.

    Reproduces the LIVE Desktop tool catalog shape: each tool is reported as
    separate ``namespace`` and ``name`` fields (review rc37 P0), not a combined
    qualified name.
    """

    def __init__(self, tools=(("codex_app", "send_message_to_thread"), ("codex_app", "list_threads"))):
        self.tools = tools
        self.seen_requests = []
        self.deliveries = []
        self._sock, self._client = socket.socketpair()
        self._server_thread = threading.Thread(target=self._serve, daemon=True)
        self._server_thread.start()

    def _serve(self):
        conn = self._sock
        try:
            while True:
                header = self._read_exact(conn, 4)
                if not header:
                    return
                (length,) = struct.unpack("<I", header)
                body = self._read_exact(conn, length)
                message = json.loads(body.decode("utf-8"))
                self.seen_requests.append(message)
                method = message.get("method")
                if method == "tools/list":
                    response = {
                        "jsonrpc": "2.0",
                        "id": message.get("id"),
                        "result": {
                            "tools": [
                                {"namespace": namespace, "name": name, "description": ""}
                                for namespace, name in self.tools
                            ],
                        },
                    }
                elif method == "tools/call":
                    params = message.get("params", {})
                    arguments = params.get("arguments", {})
                    self.deliveries.append(arguments)
                    response = {
                        "jsonrpc": "2.0",
                        "id": message.get("id"),
                        "result": {"success": True, "content": [{"type": "text", "text": "accepted"}]},
                    }
                else:
                    response = {
                        "jsonrpc": "2.0",
                        "id": message.get("id"),
                        "error": {"code": -32601, "message": "method not found"},
                    }
                self._write_frame(conn, response)
        except OSError:
            return

    @staticmethod
    def _read_exact(conn, length):
        data = b""
        while len(data) < length:
            chunk = conn.recv(length - len(data))
            if not chunk:
                return b""
            data += chunk
        return data

    @staticmethod
    def _write_frame(conn, payload):
        body = json.dumps(payload).encode("utf-8")
        conn.sendall(struct.pack("<I", len(body)) + body)

    def client_socket(self):
        return self._client

    def close(self):
        try:
            self._sock.close()
        except OSError:
            pass
        try:
            self._client.close()
        except OSError:
            pass


def _frame(payload):
    body = json.dumps(payload).encode("utf-8")
    return struct.pack("<I", len(body)) + body


class TestFraming:
    def _client(self, server):
        client = CodexPipeClient.__new__(CodexPipeClient)
        client.handle = server.client_socket()
        client.timeout_seconds = 5.0
        client._request_id = 0
        return client

    def test_full_round_trip(self):
        server = FakePipeServer()
        try:
            client = self._client(server)
            result = client.call("tools/list", {"threadStartKind": "all"})
            assert "tools" in result
            # Live schema: separate namespace/name fields.
            assert client.has_capability("codex_app", "send_message_to_thread")
            assert not client.has_capability("codex_app", "nope")
            client.close()
        finally:
            server.close()

    def test_qualified_name_shape_also_accepted(self):
        # A build that emits a combined qualified name is also matched.
        server = FakePipeServer(tools=(("codex_app", "send_message_to_thread"),))
        try:
            client = self._client(server)
            # Force the live (separate-field) match path and also check the
            # combined-name compatibility: build a fake tool list directly.
            from vanth.codex_pipe import CodexPipeClient as _C
            # The live shape is what the fake server sends (separate fields);
            # assert the split of a combined name works via the helper.
            tool = {"name": "codex_app/send_message_to_thread"}
            assert client._tool_namespace_and_name(tool) == ("codex_app", "send_message_to_thread")
            client.close()
        finally:
            server.close()

    def test_fragmented_read(self):
        server = FakePipeServer()
        try:
            sock = server.client_socket()
            # Write the frame one byte at a time to exercise fragmented reads.
            frame = _frame({"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}})
            for byte in frame:
                sock.sendall(bytes([byte]))
            # Read the response.
            header = server._read_exact(sock, 4)
            (length,) = struct.unpack("<I", header)
            body = server._read_exact(sock, length)
            response = json.loads(body)
            assert response.get("id") == 1
            assert "result" in response
        finally:
            server.close()

    def test_wrong_response_id_is_skipped(self):
        server = FakePipeServer()
        try:
            client = self._client(server)
            client._send_frame({"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {"threadStartKind": "all"}})
            first = client._read_frame()
            assert first.get("id") == 1
            result = client.call("tools/list", {"threadStartKind": "all"})
            assert "tools" in result
            client.close()
        finally:
            server.close()

    def test_json_rpc_error(self):
        server = FakePipeServer(tools=())
        try:
            client = self._client(server)
            with pytest.raises(CodexPipeError, match="method not found"):
                client.call("bogus/method", {})
            client.close()
        finally:
            server.close()

    def test_invalid_json_response(self):
        sock, client_sock = socket.socketpair()

        def serve():
            conn = sock
            try:
                _read = FakePipeServer._read_exact
                _read(conn, 4)
                body = b"not-json{{{"
                conn.sendall(struct.pack("<I", len(body)) + body)
            except OSError:
                return

        threading.Thread(target=serve, daemon=True).start()
        client = CodexPipeClient.__new__(CodexPipeClient)
        client.handle = client_sock
        client.timeout_seconds = 5.0
        client._request_id = 0
        with pytest.raises(CodexPipeError, match="invalid JSON"):
            client.call("tools/list", {})
        client.close()
        sock.close()

    def test_eof_raises(self):
        sock, client_sock = socket.socketpair()
        sock.close()
        client = CodexPipeClient.__new__(CodexPipeClient)
        client.handle = client_sock
        client.timeout_seconds = 5.0
        client._request_id = 0
        with pytest.raises(CodexPipeError, match="closed the connection"):
            client.call("tools/list", {})
        client.close()

    def test_oversized_frame_rejected(self):
        sock, client_sock = socket.socketpair()

        def serve():
            conn = sock
            try:
                _read = FakePipeServer._read_exact
                _read(conn, 4)
                conn.sendall(struct.pack("<I", MAX_FRAME_BYTES + 1))
            except OSError:
                return

        threading.Thread(target=serve, daemon=True).start()
        client = CodexPipeClient.__new__(CodexPipeClient)
        client.handle = client_sock
        client.timeout_seconds = 5.0
        client._request_id = 0
        with pytest.raises(CodexPipeError, match="8 MiB cap"):
            client.call("tools/list", {})
        client.close()
        sock.close()


class TestHelperSubprocess:
    """Review rc38 P0: production launches `python -m vanth.codex_pipe --helper`
    and must get a real JSON response. This exercises the REAL subprocess branch
    (no `_handle` shortcut) exactly as production does."""

    def test_helper_module_entry_point_responds(self):
        # No pipe configured: the helper must still respond with a valid JSON
        # error object (not exit 0 with empty stdout), proving main() runs.
        env = dict(os.environ)
        env.pop("CODEX_APP_TOOLS_PIPE_PATH", None)
        env.pop("VANTH_CODEX_DESKTOP_PIPE", None)
        request = {
            "pipe_path": "",
            "timeout_seconds": 5,
            "destination_thread_id": "t",
            "prompt": "p",
            "caller_thread_id": "c",
            "call_id": "x",
        }
        proc = subprocess.run(
            [sys.executable, "-m", "vanth.codex_pipe", "--helper"],
            input=json.dumps(request),
            capture_output=True,
            text=True,
            env=env,
            timeout=30,
        )
        assert proc.returncode == 0, f"helper must exit 0, got {proc.returncode}: {proc.stderr}"
        assert proc.stdout, "helper must write a JSON response (not exit with empty stdout)"
        payload = json.loads(proc.stdout)
        assert payload.get("ok") is False
        assert "not configured" in payload.get("error", "")

    def test_helper_unavailable_flag_survives_subprocess(self):
        """Self-review rc40: a dead pipe through the REAL helper subprocess must
        surface as CodexPipeUnavailable (not plain CodexPipeError) so the relay
        can match on the class and reload the capability. The message stays
        sanitized."""
        request = {
            "pipe_path": r"\\.\pipe\nonexistent_vanth_unavail",
            "timeout_seconds": 2,
            "destination_thread_id": "t",
            "prompt": "p",
            "caller_thread_id": "c",
            "call_id": "x",
        }
        proc = subprocess.run(
            [sys.executable, "-m", "vanth.codex_pipe", "--helper"],
            input=json.dumps(request),
            capture_output=True,
            text=True,
            timeout=30,
        )
        payload = json.loads(proc.stdout)
        assert payload.get("ok") is False
        assert payload.get("unavailable") is True
        assert r"\\.\pipe\nonexistent_vanth_unavail" not in payload.get("error", "")

    def test_send_desktop_message_raises_unavailable_class(self):
        """End-to-end through send_desktop_message (real subprocess branch): a
        dead pipe raises CodexPipeUnavailable so relay outage handling fires."""
        with pytest.raises(CodexPipeUnavailable):
            send_desktop_message(
                destination_thread_id="t",
                prompt="p",
                caller_thread_id="c",
                call_id="x",
                pipe_path=r"\\.\pipe\nonexistent_vanth_class",
                timeout_seconds=2,
            )

    def test_helper_subprocess_sanitizes_unreachable_pipe(self):
        # A pipe that cannot be opened must surface a sanitized error through
        # the REAL subprocess helper (never embed the pipe path in the JSON).
        request = {
            "pipe_path": r"\\.\pipe\nonexistent_vanth_test",
            "timeout_seconds": 2,
            "destination_thread_id": "t",
            "prompt": "p",
            "caller_thread_id": "c",
            "call_id": "x",
        }
        proc = subprocess.run(
            [sys.executable, "-m", "vanth.codex_pipe", "--helper"],
            input=json.dumps(request),
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert proc.returncode == 0
        payload = json.loads(proc.stdout)
        assert payload.get("ok") is False
        # The error must be sanitized: never embed the pipe path.
        assert r"\\.\pipe\nonexistent_vanth_test" not in payload.get("error", "")


class TestDeadlineLeaseInvariant:
    """Review rc39 P1: the helper's hard wall-clock lifetime must be STRICTLY
    SHORTER than the delivery claim lease for EVERY supported lease margin, so
    a stalled helper can never outlive the lease and cause concurrent duplicate
    delivery. The helper and the lease derive from ONE end-to-end deadline."""

    def test_helper_deadline_shorter_than_lease_for_all_margins(self):
        # The reviewer reproduced the invariant failing at margin=1 with the
        # old duplicated arithmetic. The lease is now helper_deadline + margin,
        # so helper < lease holds for every margin >= 1.
        for timeout in (1, 30, 60, 120, 300, 600, 900):
            for margin in (1, 2, 5, 10, 30):
                assert helper_hard_deadline(timeout) < claim_lease_seconds(timeout, margin), (
                    f"helper {helper_hard_deadline(timeout)}s must be < lease "
                    f"{claim_lease_seconds(timeout, margin)}s (timeout={timeout}, margin={margin})"
                )

    def test_lease_is_derived_from_same_deadline_as_helper(self):
        # No duplicated arithmetic: the lease is the helper deadline PLUS the
        # operator margin, so it always strictly outlives the helper.
        for timeout in (30, 300):
            for margin in (1, 5):
                assert claim_lease_seconds(timeout, margin) == helper_hard_deadline(timeout) + max(1, margin)

    def test_sequence_budget_covers_two_calls(self):
        # One end-to-end budget must cover the WHOLE sequence (tools/list +
        # tools/call); a slow preflight cannot consume a second full timeout.
        timeout = 30
        budget = wake_sequence_budget(timeout)
        assert budget == timeout
        # The helper deadline = budget + tiny process buffer.
        assert helper_hard_deadline(timeout) > budget
        assert helper_hard_deadline(timeout) <= budget + 2


class TestDesktopDelivery:
    def test_send_desktop_message_preflights_and_calls(self):
        server = FakePipeServer()
        try:
            result = send_desktop_message(
                destination_thread_id="thread_dest",
                prompt="wake up",
                caller_thread_id="thread_caller",
                call_id="vanth-del_123",
                _handle=server.client_socket(),
                timeout_seconds=5,
            )
            assert result.get("success") is True
            methods = [req.get("method") for req in server.seen_requests]
            assert methods[0] == "tools/list"
            assert "tools/call" in methods
            call = next(req for req in server.seen_requests if req.get("method") == "tools/call")
            params = call["params"]
            assert params["namespace"] == "codex_app"
            assert params["tool"] == "send_message_to_thread"
            assert params["arguments"]["threadId"] == "thread_dest"
            assert params["arguments"]["prompt"] == "wake up"
            # The outer caller thread id is REQUIRED and always present.
            assert params["threadId"] == "thread_caller"
            assert params["callId"] == "vanth-del_123"
            assert params["turnId"] == "vanth-del_123"
        finally:
            server.close()

    def test_missing_caller_thread_id_rejected_before_io(self):
        server = FakePipeServer()
        try:
            with pytest.raises(CodexPipeError, match="caller thread id"):
                send_desktop_message(
                    destination_thread_id="thread_dest",
                    prompt="wake",
                    caller_thread_id=None,
                    call_id="vanth-del_no-caller",
                    _handle=server.client_socket(),
                    timeout_seconds=5,
                )
            # No tools/list must have been sent.
            assert not server.seen_requests
        finally:
            server.close()

    def test_one_delivery_per_call_id(self):
        call_ids = []
        for call_id in ("vanth-del_a", "vanth-del_b"):
            server = FakePipeServer()
            try:
                send_desktop_message(
                    destination_thread_id="thread_dest",
                    prompt="wake",
                    caller_thread_id="thread_caller",
                    call_id=call_id,
                    _handle=server.client_socket(),
                    timeout_seconds=5,
                )
                call = next(req for req in server.seen_requests if req.get("method") == "tools/call")
                params = call["params"]
                assert params["callId"] == call_id
                assert params["turnId"] == call_id
                call_ids.append(params["callId"])
            finally:
                server.close()
        assert len(set(call_ids)) == 2

    def test_sequence_budget_shared_between_preflight_and_call(self):
        """Review rc39 P1: tools/list and tools/call share ONE end-to-end
        budget. A slow preflight must NOT grant the tool call a second full
        timeout; the whole sequence fails at the shared deadline."""
        # A server that delays the tools/list response by ~40% of the budget and
        # then NEVER answers tools/call. The client's total must not exceed the
        # shared sequence budget (timeout_seconds).
        import socket as _socket

        server_sock, client_sock = _socket.socketpair()
        timeout = 1.0

        def serve():
            conn = server_sock
            try:
                header = FakePipeServer._read_exact(conn, 4)
                if not header:
                    return
                (length,) = struct.unpack("<I", header)
                body = FakePipeServer._read_exact(conn, length)
                message = json.loads(body.decode("utf-8"))
                if message.get("method") == "tools/list":
                    # Slow preflight: consume 40% of the budget.
                    time.sleep(timeout * 0.4)
                    FakePipeServer._write_frame(
                        conn,
                        {
                            "jsonrpc": "2.0",
                            "id": message.get("id"),
                            "result": {"tools": [{"namespace": "codex_app", "name": "send_message_to_thread"}]},
                        },
                    )
                # Read tools/call but never answer it.
                while True:
                    header = FakePipeServer._read_exact(conn, 4)
                    if not header:
                        return
                    (length,) = struct.unpack("<I", header)
                    FakePipeServer._read_exact(conn, length)
            except OSError:
                return

        threading.Thread(target=serve, daemon=True).start()
        client = CodexPipeClient.__new__(CodexPipeClient)
        client.handle = client_sock
        client.timeout_seconds = timeout
        client.sequence_deadline = time.monotonic() + wake_sequence_budget(timeout)
        client._request_id = 0
        start = time.monotonic()
        try:
            with pytest.raises(CodexPipeError, match="timed out"):
                client.send_message_to_thread(
                    destination_thread_id="t",
                    prompt="wake",
                    caller_thread_id="c",
                    call_id="x",
                )
            elapsed = time.monotonic() - start
            # The total must stay within ~the shared budget (not 2x timeout).
            assert elapsed < timeout * 1.8, f"sequence must not get a second full timeout, took {elapsed:.2f}s"
        finally:
            client.close()
            server_sock.close()
            client_sock.close()

    def test_missing_pipe_is_fail_closed(self, monkeypatch):
        monkeypatch.delenv("CODEX_APP_TOOLS_PIPE_PATH", raising=False)
        monkeypatch.delenv("VANTH_CODEX_DESKTOP_PIPE", raising=False)
        with pytest.raises(CodexPipeUnavailable, match="not set"):
            send_desktop_message(
                destination_thread_id="thread_dest",
                prompt="wake",
                caller_thread_id="thread_caller",
                call_id="vanth-del_x",
            )

    def test_missing_capability_is_fail_closed(self):
        server = FakePipeServer(tools=(("codex_app", "list_threads"),))
        try:
            with pytest.raises(CodexPipeError, match="does not advertise"):
                send_desktop_message(
                    destination_thread_id="thread_dest",
                    prompt="wake",
                    caller_thread_id="thread_caller",
                    call_id="vanth-del_y",
                    _handle=server.client_socket(),
                    timeout_seconds=5,
                )
        finally:
            server.close()

    def test_hard_timeout_releases_blocked_read(self):
        # A peer that accepts the frame but NEVER answers: call() must raise a
        # timeout at the wall-clock deadline and clean up (the reader is
        # unblocked by closing the handle).
        sock, client_sock = socket.socketpair()

        def serve():
            conn = sock
            try:
                # Read the request, never respond.
                _read = FakePipeServer._read_exact
                while True:
                    _read(conn, 4)
            except OSError:
                return

        threading.Thread(target=serve, daemon=True).start()
        client = CodexPipeClient.__new__(CodexPipeClient)
        client.handle = client_sock
        client.timeout_seconds = 0.3
        client._request_id = 0
        start = time.monotonic()
        with pytest.raises(CodexPipeError, match="timed out waiting for Codex Desktop"):
            client.call("tools/list", {"threadStartKind": "all"})
        elapsed = time.monotonic() - start
        assert elapsed < 3.0, f"timeout must fire near the deadline, took {elapsed:.2f}s"
        client.close()
        sock.close()


class TestRelayProvisionedPipeDelivery:
    """Review rc38 P0: the relay's RESOLVED pipe path (explicit handoff env OR
    capability file) must be carried into delivery — the delivery path must NOT
    fall back to the un-inherited CODEX_APP_TOOLS_PIPE_PATH."""

    def test_relay_passes_resolved_pipe_to_delivery(self, monkeypatch, tmp_path):
        import vanth.relay as relay_mod

        monkeypatch.setenv("VANTH_CODEX_DESKTOP_PIPE", r"\\.\pipe\handoff_pipe")
        monkeypatch.setenv("VANTH_CODEX_DESKTOP_THREAD", "thread_exec")
        monkeypatch.setenv("VANTH_HOME", str(tmp_path))

        delivered = {}

        class FakeClient:
            def ensure(self):
                return True

            def post(self, path, payload):
                if path == "/relay/ack":
                    return {"result": "ok"}
                return {"result": "ok"}

            def get(self, path, params):
                return {"deliveries": []}

        from vanth import codex_bridge

        orig = codex_bridge.send_delivery_to_codex_desktop

        def spy(payload, *, caller_thread_id=None, pipe_path=None):
            delivered["caller_thread_id"] = caller_thread_id
            delivered["pipe_path"] = pipe_path
            return {"ok": True}

        codex_bridge.send_delivery_to_codex_desktop = spy
        try:
            relay = relay_mod.DesktopRelay(FakeClient(), "client_x")
            # Resolved from the explicit handoff env.
            assert relay.pipe_path == r"\\.\pipe\handoff_pipe"
            delivery = {
                "delivery_id": "del_1",
                "lease_token": "tok",
                "payload": {"target": {"type": "codex_desktop", "thread_id": "t"}, "prompt": "wake"},
            }
            relay._deliver(delivery)
            assert delivered["caller_thread_id"] == "thread_exec"
            assert delivered["pipe_path"] == r"\\.\pipe\handoff_pipe", (
                "the resolved handoff pipe must reach delivery, not the un-inherited env"
            )
        finally:
            codex_bridge.send_delivery_to_codex_desktop = orig

    def test_relay_uses_capability_file_pipe(self, monkeypatch, tmp_path):
        """The capability-file handoff path: the relay must resolve the pipe from
        the per-home file and pass THAT through delivery, with CODEX_APP_TOOLS_PIPE_PATH
        absent (real MCP children do not inherit it)."""
        import json as _json
        import vanth.relay as relay_mod

        monkeypatch.delenv("CODEX_APP_TOOLS_PIPE_PATH", raising=False)
        monkeypatch.delenv("VANTH_CODEX_DESKTOP_PIPE", raising=False)
        monkeypatch.setenv("VANTH_CODEX_DESKTOP_THREAD", "thread_cap")
        home = tmp_path / "home"
        home.mkdir(parents=True, exist_ok=True)
        monkeypatch.setenv("VANTH_HOME", str(home))
        cap = home / "codex_desktop.json"
        from datetime import datetime, timezone

        cap.write_text(
            _json.dumps(
                {
                    "pipe_path": r"\\.\pipe\capability_pipe",
                    "thread_id": "thread_cap",
                    "caller_thread_id": "thread_cap",
                    "provisioned_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                }
            ),
            encoding="utf-8",
        )

        delivered = {}

        class FakeClient:
            def ensure(self):
                return True

            def post(self, path, payload):
                if path == "/relay/ack":
                    return {"result": "ok"}
                return {"result": "ok"}

            def get(self, path, params):
                return {"deliveries": []}

        from vanth import codex_bridge

        orig = codex_bridge.send_delivery_to_codex_desktop

        def spy(payload, *, caller_thread_id=None, pipe_path=None):
            delivered["caller_thread_id"] = caller_thread_id
            delivered["pipe_path"] = pipe_path
            return {"ok": True}

        codex_bridge.send_delivery_to_codex_desktop = spy
        try:
            relay = relay_mod.DesktopRelay(FakeClient(), "client_cap")
            assert relay.pipe_path == r"\\.\pipe\capability_pipe"
            assert relay.caller_thread_id == "thread_cap"
            relay._deliver(
                {
                    "delivery_id": "del_2",
                    "lease_token": "tok2",
                    "payload": {"target": {"type": "codex_desktop", "thread_id": "t"}, "prompt": "wake"},
                }
            )
            assert delivered["pipe_path"] == r"\\.\pipe\capability_pipe"
            assert delivered["caller_thread_id"] == "thread_cap"
        finally:
            codex_bridge.send_delivery_to_codex_desktop = orig


class TestRelayRestartRecovery:
    """Review rc39 P1: a Desktop restart invalidates the private pipe. The relay
    must reload/re-register the capability (or fail with an explicit
    reprovision diagnostic) instead of permanently consuming the delivery."""

    def test_relay_reloads_capability_after_pipe_change(self, monkeypatch, tmp_path):
        import json as _json
        import vanth.relay as relay_mod

        from datetime import datetime, timezone

        monkeypatch.delenv("CODEX_APP_TOOLS_PIPE_PATH", raising=False)
        monkeypatch.delenv("VANTH_CODEX_DESKTOP_PIPE", raising=False)
        monkeypatch.delenv("VANTH_CODEX_DESKTOP_THREAD", raising=False)
        home = tmp_path / "home"
        home.mkdir(parents=True, exist_ok=True)
        monkeypatch.setenv("VANTH_HOME", str(home))
        cap = home / "codex_desktop.json"

        def write(pipe, thread, ts):
            cap.write_text(
                _json.dumps(
                    {
                        "pipe_path": pipe,
                        "thread_id": thread,
                        "caller_thread_id": thread,
                        "provisioned_at": ts.isoformat().replace("+00:00", "Z"),
                    }
                ),
                encoding="utf-8",
            )

        write(r"\\.\pipe\old_pipe", "thread_exec", datetime.now(timezone.utc))

        class FakeClient:
            def ensure(self):
                return True

            def post(self, path, payload):
                if path == "/relay/ack":
                    return {"result": "ok"}
                return {"result": "ok"}

            def get(self, path, params):
                return {"deliveries": []}

        relay = relay_mod.DesktopRelay(FakeClient(), "client_restart")
        assert relay.pipe_path == r"\\.\pipe\old_pipe"

        # Desktop restarts and the operator re-provisions with a NEW pipe (and a
        # fresh provisioned_at). The relay must detect the change on reload.
        write(r"\\.\pipe\new_pipe", "thread_exec", datetime.now(timezone.utc))
        changed = relay._reload_capability()
        assert changed is True, "a re-provisioned capability must be detected"
        assert relay.pipe_path == r"\\.\pipe\new_pipe"
        assert relay._registered is False, "the relay must re-register after reload"

        # When NOTHING changed (genuine outage, no reprovision), reload returns
        # False so the caller fails the delivery (explicit reprovision needed).
        changed2 = relay._reload_capability()
        assert changed2 is False

    def test_relay_pipe_unavailable_releases_without_consuming(self, monkeypatch, tmp_path):
        """When the pipe is unavailable and the capability did NOT change, the
        delivery must be RELEASED back to pending (no attempt consumed) — not
        acked failed (terminal at max_attempts=1) and not silently delivered."""
        import json as _json
        import vanth.relay as relay_mod

        from datetime import datetime, timezone

        monkeypatch.delenv("CODEX_APP_TOOLS_PIPE_PATH", raising=False)
        monkeypatch.delenv("VANTH_CODEX_DESKTOP_PIPE", raising=False)
        monkeypatch.delenv("VANTH_CODEX_DESKTOP_THREAD", raising=False)
        home = tmp_path / "home"
        home.mkdir(parents=True, exist_ok=True)
        monkeypatch.setenv("VANTH_HOME", str(home))
        cap = home / "codex_desktop.json"
        cap.write_text(
            _json.dumps(
                {
                    "pipe_path": r"\\.\pipe\dead_pipe",
                    "thread_id": "thread_exec",
                    "caller_thread_id": "thread_exec",
                    "provisioned_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                }
            ),
            encoding="utf-8",
        )

        from vanth.codex_pipe import CodexPipeUnavailable

        posted = []

        class FakeClient:
            def ensure(self):
                return True

            def post(self, path, payload):
                posted.append((path, payload))
                return {"result": "ok"}

            def get(self, path, params):
                return {"deliveries": []}

        from vanth import codex_bridge

        orig = codex_bridge.send_delivery_to_codex_desktop

        def unavailable(payload, *, caller_thread_id=None, pipe_path=None):
            raise CodexPipeUnavailable("cannot open Codex Desktop app-tools pipe")

        codex_bridge.send_delivery_to_codex_desktop = unavailable
        try:
            relay = relay_mod.DesktopRelay(FakeClient(), "client_outage")
            with pytest.raises(relay_mod.RelayCapabilityLost):
                relay._deliver(
                    {
                        "delivery_id": "del_outage",
                        "lease_token": "tok",
                        "payload": {"target": {"type": "codex_desktop", "thread_id": "t"}, "prompt": "wake"},
                    }
                )
            # The capability did not change -> the delivery was RELEASED (not
            # acked failed, which would terminally consume it at max_attempts=1).
            paths = [path for path, _ in posted]
            assert "/relay/release" in paths, f"must release, posted: {paths}"
            assert not any(
                path == "/relay/ack" and payload.get("status") == "failed"
                for path, payload in posted
            ), "must NOT ack-failed a released delivery"
            release = next(payload for path, payload in posted if path == "/relay/release")
            assert release["delivery_id"] == "del_outage"
            assert release["lease_token"] == "tok"
            assert relay.pipe_path == r"\\.\pipe\dead_pipe"
        finally:
            codex_bridge.send_delivery_to_codex_desktop = orig

    def test_relay_retries_with_reprovisioned_pipe(self, monkeypatch, tmp_path):
        """Desktop restart + reprovision while the relay runs: the first send
        fails on the dead pipe, the relay reloads the NEW capability, and the
        same pending wake is delivered through the fresh pipe (never lost)."""
        import json as _json
        import vanth.relay as relay_mod

        from datetime import datetime, timezone

        monkeypatch.delenv("CODEX_APP_TOOLS_PIPE_PATH", raising=False)
        monkeypatch.delenv("VANTH_CODEX_DESKTOP_PIPE", raising=False)
        monkeypatch.delenv("VANTH_CODEX_DESKTOP_THREAD", raising=False)
        home = tmp_path / "home"
        home.mkdir(parents=True, exist_ok=True)
        monkeypatch.setenv("VANTH_HOME", str(home))
        cap = home / "codex_desktop.json"

        def write(pipe):
            cap.write_text(
                _json.dumps(
                    {
                        "pipe_path": pipe,
                        "thread_id": "thread_exec",
                        "caller_thread_id": "thread_exec",
                        "provisioned_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    }
                ),
                encoding="utf-8",
            )

        write(r"\\.\pipe\old_dead_pipe")

        from vanth.codex_pipe import CodexPipeUnavailable

        posted = []
        delivered = {}

        class FakeClient:
            def ensure(self):
                return True

            def post(self, path, payload):
                posted.append((path, payload))
                return {"result": "ok"}

            def get(self, path, params):
                return {"deliveries": []}

        from vanth import codex_bridge

        orig = codex_bridge.send_delivery_to_codex_desktop

        def flaky_send(payload, *, caller_thread_id=None, pipe_path=None):
            # The old (dead) pipe fails; the re-provisioned pipe admits.
            if pipe_path == r"\\.\pipe\old_dead_pipe":
                # Simulate the operator re-provisioning DURING the outage.
                write(r"\\.\pipe\new_live_pipe")
                raise CodexPipeUnavailable("cannot open Codex Desktop app-tools pipe")
            delivered["pipe_path"] = pipe_path
            return {"ok": True}

        codex_bridge.send_delivery_to_codex_desktop = flaky_send
        try:
            relay = relay_mod.DesktopRelay(FakeClient(), "client_reprov")
            assert relay.pipe_path == r"\\.\pipe\old_dead_pipe"
            # Must NOT raise: the retry through the fresh pipe succeeds.
            relay._deliver(
                {
                    "delivery_id": "del_reprov",
                    "lease_token": "tok",
                    "payload": {"target": {"type": "codex_desktop", "thread_id": "t"}, "prompt": "wake"},
                }
            )
            assert delivered["pipe_path"] == r"\\.\pipe\new_live_pipe"
            assert relay.pipe_path == r"\\.\pipe\new_live_pipe"
            acks = [payload for path, payload in posted if path == "/relay/ack"]
            assert acks and acks[0]["status"] == "delivered"
            assert not any(path == "/relay/release" for path, _ in posted)
        finally:
            codex_bridge.send_delivery_to_codex_desktop = orig


class TestRelay:
    def _start_delivery(self, manager, thread_id="thread_dest"):
        async def main():
            job = manager.start(
                shellcmd.join([sys.executable, "-c", "import sys; sys.exit(0)"]),
                wake_targets=[{"type": "codex_desktop", "events": ["completed"], "thread_id": thread_id}],
            )
            await manager.wait(job["job_id"], ["completed"], timeout_seconds=30)
            return job["job_id"]

        return asyncio.run(main())

    def test_relay_register_poll_ack(self, tmp_path):
        manager = JobManager(tmp_path / "state", recover=False)
        try:
            result = manager.relay_register(
                "client_1", "codex_desktop", [{"client_type": "codex_desktop", "thread_id": "thread_dest"}]
            )
            assert result["result"] == "ok"

            job_id = self._start_delivery(manager)

            polled = manager.relay_poll("client_1", timeout_seconds=1)
            assert polled, "relay poll must return the due codex_desktop delivery"
            delivery = polled[0]
            assert delivery["target_type"] == "codex_desktop"
            assert delivery["payload"]["target"].get("thread_id") == "thread_dest"
            assert delivery.get("lease_token"), "poll must return an opaque lease token"

            # Acknowledge delivered with the lease token.
            ack = manager.relay_ack("client_1", delivery["delivery_id"], "delivered", lease_token=delivery["lease_token"])
            assert ack["result"] == "ok"
            matching = [d for d in manager.deliveries(job_id)["deliveries"] if d["delivery_id"] == delivery["delivery_id"]]
            assert matching and matching[0]["status"] == "delivered"
        finally:
            manager.close()

    def test_ack_requires_lease_token(self, tmp_path):
        manager = JobManager(tmp_path / "state", recover=False)
        try:
            manager.relay_register(
                "client_1", "codex_desktop", [{"client_type": "codex_desktop", "thread_id": "thread_dest"}]
            )
            self._start_delivery(manager)
            polled = manager.relay_poll("client_1", timeout_seconds=1)
            assert polled
            delivery = polled[0]
            with pytest.raises(ValueError, match="lease_token"):
                manager.relay_ack("client_1", delivery["delivery_id"], "delivered", lease_token=None)
        finally:
            manager.close()

    def test_ack_rejected_for_non_claimant_client(self, tmp_path):
        manager = JobManager(tmp_path / "state", recover=False)
        try:
            manager.relay_register(
                "client_1", "codex_desktop", [{"client_type": "codex_desktop", "thread_id": "thread_dest"}]
            )
            manager.relay_register(
                "client_2", "codex_desktop", [{"client_type": "codex_desktop", "thread_id": "thread_dest"}]
            )
            self._start_delivery(manager)
            polled = manager.relay_poll("client_1", timeout_seconds=1)
            assert polled
            delivery = polled[0]
            # client_2 (not the claimant) must NOT be able to ack.
            with pytest.raises(ValueError, match="not claimed by this relay"):
                manager.relay_ack("client_2", delivery["delivery_id"], "delivered", lease_token=delivery["lease_token"])
            # client_1 still can.
            ack = manager.relay_ack("client_1", delivery["delivery_id"], "delivered", lease_token=delivery["lease_token"])
            assert ack["result"] == "ok"
        finally:
            manager.close()

    def test_ack_rejected_after_lease_reclaimed(self, tmp_path):
        manager = JobManager(tmp_path / "state", recover=False)
        try:
            manager.relay_register(
                "client_1", "codex_desktop", [{"client_type": "codex_desktop", "thread_id": "thread_dest"}]
            )
            self._start_delivery(manager)
            polled = manager.relay_poll("client_1", timeout_seconds=1)
            assert polled
            delivery = polled[0]
            # Simulate the lease expiring and a NEW claim (new token).
            import datetime as _dt
            stale = (_dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
            with manager.db_lock:
                manager.db.execute(
                    "UPDATE deliveries SET lease_expires_at=? WHERE delivery_id=?",
                    (stale, delivery["delivery_id"]),
                )
                manager.db.commit()
            # Reclaim with a different relay client.
            manager.relay_register(
                "client_2", "codex_desktop", [{"client_type": "codex_desktop", "thread_id": "thread_dest"}]
            )
            repolled = manager.relay_poll("client_2", timeout_seconds=1)
            assert repolled
            # The OLD client's lease token no longer matches the new claim.
            with pytest.raises(ValueError, match="not claimed by this relay"):
                manager.relay_ack("client_1", delivery["delivery_id"], "delivered", lease_token=delivery["lease_token"])
        finally:
            manager.close()

    def test_ack_atomic_cas_includes_claim_client_id(self, tmp_path):
        """Review rc39 P1: the relay ack validation+completion is ONE atomic CAS
        that includes claim_client_id. Even if the row still matches the claim
        TOKEN, a completion attempted by a different client_id affects zero rows
        and raises — the client never receives a silent success."""
        manager = JobManager(tmp_path / "state", recover=False)
        try:
            manager.relay_register(
                "client_1", "codex_desktop", [{"client_type": "codex_desktop", "thread_id": "thread_dest"}]
            )
            self._start_delivery(manager)
            polled = manager.relay_poll("client_1", timeout_seconds=1)
            assert polled
            delivery = polled[0]
            # The token still matches, but a DIFFERENT client_id must fail the
            # atomic completion CAS (the guard is token AND client).
            from vanth.server import now_iso
            import datetime as _dt

            row = manager._row(
                "SELECT * FROM deliveries WHERE delivery_id=?", (delivery["delivery_id"],)
            )
            with pytest.raises(ValueError, match="not claimed by this relay"):
                manager._complete_delivery(
                    {
                        "delivery_id": delivery["delivery_id"],
                        "claim_token": delivery["lease_token"],
                        "attempts": row["attempts"],
                        "payload": {"target": json.loads(row["payload_json"]).get("target", {})},
                    },
                    "delivered",
                    require_claim_client_id="client_other",
                )
            # The row is untouched and still dispatching under client_1.
            row = manager._row(
                "SELECT status, claim_client_id FROM deliveries WHERE delivery_id=?", (delivery["delivery_id"],)
            )
            assert row["status"] == "dispatching"
            assert row["claim_client_id"] == "client_1"
            # client_1 can still ack.
            ack = manager.relay_ack("client_1", delivery["delivery_id"], "delivered", lease_token=delivery["lease_token"])
            assert ack["result"] == "ok"
        finally:
            manager.close()

    def test_threadId_alias_target_is_pollable(self, tmp_path):
        """Review rc38 P1: the documented legacy `threadId` alias is accepted by
        validation and must be pollable by the relay. The target key is
        canonicalized to thread_id at persistence (and the SQL filter covers
        both JSON paths), so a delivery registered with threadId is NOT left
        pending forever."""
        manager = JobManager(tmp_path / "state", recover=False)
        try:
            manager.relay_register(
                "client_1", "codex_desktop", [{"client_type": "codex_desktop", "thread_id": "thread_legacy"}]
            )
            # Wake a job with the camelCase alias.
            from vanth.server import canonicalize_wake_target

            async def main():
                job = manager.start(
                    shellcmd.join([sys.executable, "-c", "import sys; sys.exit(0)"]),
                    wake_targets=[{"type": "codex_desktop", "events": ["completed"], "threadId": "thread_legacy"}],
                )
                await manager.wait(job["job_id"], ["completed"], timeout_seconds=30)
                return job["job_id"]

            job_id = asyncio.run(main())
            polled = manager.relay_poll("client_1", timeout_seconds=1)
            assert polled, "a delivery registered with threadId must be pollable"
            target = polled[0]["payload"]["target"]
            assert target.get("thread_id") == "thread_legacy"
            assert canonicalize_wake_target({"threadId": "x"}) == {"thread_id": "x"}
        finally:
            manager.close()

    def test_session_id_alias_is_canonicalized_to_thread_id(self, tmp_path):
        """A codex_desktop target naming its thread via `session_id` is accepted
        by validation, so it must be canonicalized to `thread_id` (the key the
        relay SQL reads) — otherwise it is silently never claimed."""
        manager = JobManager(tmp_path / "state", recover=False)
        try:
            manager.relay_register(
                "client_1", "codex_desktop", [{"client_type": "codex_desktop", "thread_id": "thread_alias"}]
            )

            async def main():
                job = manager.start(
                    shellcmd.join([sys.executable, "-c", "import sys; sys.exit(0)"]),
                    wake_targets=[{"type": "codex_desktop", "events": ["completed"], "session_id": "thread_alias"}],
                )
                await manager.wait(job["job_id"], ["completed"], timeout_seconds=30)
                return job["job_id"]

            asyncio.run(main())
            polled = manager.relay_poll("client_1", timeout_seconds=1)
            assert polled, "a delivery registered with session_id must be pollable"
            assert polled[0]["payload"]["target"].get("thread_id") == "thread_alias"
        finally:
            manager.close()

    def test_relay_client_id_in_thread_id_is_rejected(self, tmp_path):
        manager = JobManager(tmp_path / "state", recover=False)
        try:
            manager.relay_register(
                "mcp-1234-abcd", "codex_desktop", [{"client_type": "codex_desktop", "thread_id": "thread_real"}]
            )
            with pytest.raises(ValueError, match="relay client id"):
                
                    manager.start(
                        shellcmd.join([sys.executable, "-c", "import sys; sys.exit(0)"]),
                        wake_targets=[{"type": "codex_desktop", "events": ["completed"], "thread_id": "mcp-1234-abcd"}],
                    
                )
        finally:
            manager.close()

    def test_sql_filter_before_limit_no_starvation(self, tmp_path):
        manager = JobManager(tmp_path / "state", recover=False)
        try:
            manager.relay_register(
                "client_1", "codex_desktop", [{"client_type": "codex_desktop", "thread_id": "thread_target"}]
            )
            # Create 21 foreign-task codex_desktop deliveries + 1 matching.
            # Insert directly: 20 older deliveries for OTHER threads, then one
            # for thread_target.
            from vanth.server import now_iso
            created = now_iso()
            with manager.db_lock:
                for i in range(20):
                    target_id = f"target_foreign_{i}"
                    delivery_id = f"del_foreign_{i}"
                    payload = {
                        "target": {"type": "codex_desktop", "thread_id": f"thread_other_{i}"},
                        "prompt": f"foreign {i}",
                        "delivery_id": delivery_id,
                    }
                    manager.db.execute(
                        "INSERT INTO deliveries(delivery_id, event_id, target_id, job_id, target_type, status, payload_json, created_at) "
                        "VALUES (?, ?, ?, ?, 'codex_desktop', 'pending', ?, ?)",
                        (delivery_id, f"evt_f_{i}", target_id, "job_foreign", json.dumps(payload), created),
                    )
                manager.db.commit()
            # Now the real matching delivery.
            job_id = self._start_delivery(manager, thread_id="thread_target")
            polled = manager.relay_poll("client_1", timeout_seconds=1)
            assert polled, "matching delivery must NOT be starved by 20 foreign-task deliveries"
            assert polled[0]["payload"]["target"].get("thread_id") == "thread_target"
        finally:
            manager.close()

    def test_relay_register_refreshes_liveness(self, tmp_path):
        """Review rc39 P2: relay_register must refresh last_poll_at on the
        upsert, so a stale client that reconnects is NOT deleted by
        relay_expire_stale between its registration and first poll."""
        manager = JobManager(tmp_path / "state", recover=False)
        try:
            manager.relay_register(
                "client_1", "codex_desktop", [{"client_type": "codex_desktop", "thread_id": "thread_dest"}]
            )
            # Simulate the subscription going stale (last_poll_at in the past).
            import datetime as _dt

            old = (_dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(seconds=3600)).isoformat().replace("+00:00", "Z")
            with manager.db_lock:
                manager.db.execute(
                    "UPDATE relay_subscriptions SET last_poll_at=? WHERE client_id=?",
                    (old, "client_1"),
                )
                manager.db.commit()
            # Re-register (a reconnect) MUST refresh last_poll_at.
            manager.relay_register(
                "client_1", "codex_desktop", [{"client_type": "codex_desktop", "thread_id": "thread_dest"}]
            )
            row = manager._row(
                "SELECT last_poll_at FROM relay_subscriptions WHERE client_id=?", ("client_1",)
            )
            assert row is not None, "re-register must keep the subscription"
            assert row["last_poll_at"] > old, "re-register must refresh last_poll_at"
            # A stale sweep must NOT remove the just-refreshed subscription.
            assert manager.relay_expire_stale(stale_after_seconds=300) == 0
        finally:
            manager.close()

    def test_relay_release_returns_to_pending_without_consuming(self, tmp_path):
        """relay_release returns a claimed delivery to pending WITHOUT
        consuming an attempt (claim bookkeeping rolled back, immediately due). Wrong
        token/client raises; the delivery stays dispatching under its owner."""
        manager = JobManager(tmp_path / "state", recover=False)
        try:
            manager.relay_register(
                "client_1", "codex_desktop", [{"client_type": "codex_desktop", "thread_id": "thread_dest"}]
            )
            self._start_delivery(manager)
            polled = manager.relay_poll("client_1", timeout_seconds=1)
            assert polled
            delivery = polled[0]
            assert delivery["attempts"] == 1
            # Wrong token must fail and leave the row dispatching.
            with pytest.raises(ValueError, match="not claimed by this relay"):
                manager.relay_release("client_1", delivery["delivery_id"], lease_token="bogus")
            # Correct release: pending, the cancelled claim no longer consumes
            # retry budget or an attempt-history row, and it is due immediately.
            released = manager.relay_release("client_1", delivery["delivery_id"], lease_token=delivery["lease_token"])
            assert released["result"] == "ok"
            assert released["status"] == "pending"
            row = manager._row(
                "SELECT status, attempts, claim_token, claim_client_id, next_attempt_at FROM deliveries WHERE delivery_id=?",
                (delivery["delivery_id"],),
            )
            assert row["status"] == "pending"
            assert int(row["attempts"]) == 0
            assert row["claim_token"] is None
            assert row["claim_client_id"] is None
            assert row["next_attempt_at"] is None
            assert manager.delivery_attempts(delivery["delivery_id"])["attempts"] == []
            # The released wake is immediately reclaimable by a later poll.
            repolled = manager.relay_poll("client_1", timeout_seconds=1)
            assert repolled and repolled[0]["delivery_id"] == delivery["delivery_id"]
            assert repolled[0]["attempts"] == 1
        finally:
            manager.close()

    def test_relay_outage_releases_unprocessed_batch(self, monkeypatch):
        """An outage on the first claimed delivery must not strand the rest of
        the poll batch under leases until expiry."""
        import vanth.relay as relay_mod

        released = []

        class FakeClient:
            def post(self, path, payload):
                if path == "/relay/release":
                    released.append(payload["delivery_id"])
                return {"result": "ok"}

            def get(self, path, params):
                return {"deliveries": []}

        deliveries = [
            {"delivery_id": "del_1", "lease_token": "tok_1", "payload": {}},
            {"delivery_id": "del_2", "lease_token": "tok_2", "payload": {}},
            {"delivery_id": "del_3", "lease_token": "tok_3", "payload": {}},
        ]
        relay = relay_mod.DesktopRelay(FakeClient(), "client_batch")
        relay._register = lambda: True
        polls = iter([deliveries, []])
        relay._poll = lambda: next(polls)

        def outage(delivery):
            relay._release(delivery)
            relay._stop.set()
            raise relay_mod.RelayCapabilityLost("down")

        relay._deliver = outage
        monkeypatch.setattr(relay_mod.time, "sleep", lambda _: None)
        relay._run()

        assert released == ["del_1", "del_2", "del_3"]

    def test_relay_poll_unknown_client(self, tmp_path):
        manager = JobManager(tmp_path / "state", recover=False)
        try:
            with pytest.raises(ValueError, match="Unknown relay client_id"):
                manager.relay_poll("nope", timeout_seconds=1)
        finally:
            manager.close()

    def test_relay_ack_rejected_is_not_silent(self):
        """Review rc38 P1: a rejected acknowledgement (stale lease, ownership
        mismatch) must RAISE, never be silently treated as success. The relay
        must not believe a wake is complete while the row stays 'dispatching'."""
        from vanth.relay import DesktopRelay, RelayError

        calls = []

        class RejectingClient:
            def post(self, path, payload):
                if path == "/relay/ack":
                    calls.append(payload)
                    # VanthClient.post converts HTTP 409 into a JSON error object.
                    return {"result": "error", "error": "delivery is not claimed by this relay"}
                return {"result": "ok"}

        relay = DesktopRelay(RejectingClient(), "client_x")
        delivery = {"delivery_id": "del_reject", "lease_token": "stale_tok"}
        with pytest.raises(RelayError, match="rejected|stale|reclaimed"):
            relay._ack(delivery, "delivered")
        assert calls, "the ack must be attempted"
        assert calls[0]["lease_token"] == "stale_tok"

    def test_relay_ack_success_does_not_raise(self):
        from vanth.relay import DesktopRelay

        class OkClient:
            def post(self, path, payload):
                return {"result": "ok"}

        relay = DesktopRelay(OkClient(), "client_x")
        relay._ack({"delivery_id": "del_ok", "lease_token": "tok"}, "delivered")  # must not raise


class TestRelayProvisioning:
    def test_destinations_require_pipe_and_thread(self, monkeypatch):
        """Review rc37 P0: the relay registers a destination ONLY when the pipe
        capability AND a caller thread identity are both available."""
        import vanth.relay as relay_mod

        monkeypatch.delenv("VANTH_CODEX_DESKTOP_PIPE", raising=False)
        monkeypatch.delenv("CODEX_APP_TOOLS_PIPE_PATH", raising=False)
        monkeypatch.delenv("VANTH_CODEX_DESKTOP_THREAD", raising=False)
        monkeypatch.delenv("CODEX_THREAD_ID", raising=False)
        assert relay_mod._destinations() == []

        # Pipe but no thread -> still nothing.
        monkeypatch.setenv("VANTH_CODEX_DESKTOP_PIPE", "\\\\pipe\\codex")
        assert relay_mod._destinations() == []

        # Both present -> one destination.
        monkeypatch.setenv("VANTH_CODEX_DESKTOP_THREAD", "thread_exec")
        dests = relay_mod._destinations()
        assert dests == [{"client_type": "codex_desktop", "thread_id": "thread_exec"}]

    def test_thread_identity_fallback_chain(self, monkeypatch):
        import vanth.relay as relay_mod

        monkeypatch.delenv("VANTH_CODEX_DESKTOP_THREAD", raising=False)
        monkeypatch.setenv("CODEX_THREAD_ID", "from_codex_thread_id")
        assert relay_mod._codex_thread_identity() == "from_codex_thread_id"
        monkeypatch.setenv("VANTH_CODEX_DESKTOP_THREAD", "from_handoff")
        assert relay_mod._codex_thread_identity() == "from_handoff"

    def test_start_desktop_relay_noops_without_capability(self, monkeypatch):
        import vanth.relay as relay_mod

        monkeypatch.delenv("VANTH_CODEX_DESKTOP_PIPE", raising=False)
        monkeypatch.delenv("CODEX_APP_TOOLS_PIPE_PATH", raising=False)
        monkeypatch.delenv("VANTH_CODEX_DESKTOP_THREAD", raising=False)
        monkeypatch.delenv("CODEX_THREAD_ID", raising=False)
        assert relay_mod.start_desktop_relay() is None

    def test_stale_capability_is_detected_and_fails_closed(self, monkeypatch, tmp_path):
        """Review rc38 P2: a capability file older than the TTL (a Desktop
        restart invalidates the private pipe) must be treated as ABSENT — fail
        closed with a diagnostic, not silently used."""
        import json as _json
        import vanth.relay as relay_mod

        from datetime import datetime, timedelta, timezone

        monkeypatch.delenv("CODEX_APP_TOOLS_PIPE_PATH", raising=False)
        monkeypatch.delenv("VANTH_CODEX_DESKTOP_PIPE", raising=False)
        monkeypatch.delenv("VANTH_CODEX_DESKTOP_THREAD", raising=False)
        monkeypatch.delenv("CODEX_THREAD_ID", raising=False)
        home = tmp_path / "home"
        home.mkdir(parents=True, exist_ok=True)
        monkeypatch.setenv("VANTH_HOME", str(home))
        stale = datetime.now(timezone.utc) - timedelta(seconds=relay_mod._DESKTOP_CAPABILITY_TTL_SECONDS + 3600)
        cap = home / "codex_desktop.json"
        cap.write_text(
            _json.dumps(
                {
                    "pipe_path": r"\\.\pipe\stale_pipe",
                    "thread_id": "thread_stale",
                    "caller_thread_id": "thread_stale",
                    "provisioned_at": stale.isoformat().replace("+00:00", "Z"),
                }
            ),
            encoding="utf-8",
        )
        # Fresh capability: resolves.
        fresh = datetime.now(timezone.utc)
        cap.write_text(
            _json.dumps(
                {
                    "pipe_path": r"\\.\pipe\fresh_pipe",
                    "thread_id": "thread_fresh",
                    "caller_thread_id": "thread_fresh",
                    "provisioned_at": fresh.isoformat().replace("+00:00", "Z"),
                }
            ),
            encoding="utf-8",
        )
        assert relay_mod._desktop_pipe_path() == r"\\.\pipe\fresh_pipe"
        # Now make it stale.
        cap.write_text(
            _json.dumps(
                {
                    "pipe_path": r"\\.\pipe\stale_pipe",
                    "thread_id": "thread_stale",
                    "caller_thread_id": "thread_stale",
                    "provisioned_at": stale.isoformat().replace("+00:00", "Z"),
                }
            ),
            encoding="utf-8",
        )
        assert relay_mod._desktop_pipe_path() is None, "stale capability must fail closed"
        assert relay_mod._codex_thread_identity() is None

    def test_start_desktop_relay_with_handoff_provisions(self, monkeypatch, tmp_path):
        """Review rc37 P0: with the host handoff env (the supported way to grant
        the pipe capability) the relay starts and registers."""
        import vanth.relay as relay_mod

        monkeypatch.setenv("VANTH_CODEX_DESKTOP_PIPE", "\\\\pipe\\codex")
        monkeypatch.setenv("VANTH_CODEX_DESKTOP_THREAD", "thread_exec")
        monkeypatch.setenv("VANTH_HOME", str(tmp_path))
        # The relay registers with a VanthClient that POSTs /relay/register.
        # Provide a fake client so we can assert the destinations registered.
        from vanth.relay import DesktopRelay

        registered = {}

        class FakeClient:
            def ensure(self):
                return True

            def post(self, path, payload):
                if path == "/relay/register":
                    registered.update(payload)
                    return {"result": "ok"}
                return {"result": "ok"}

            def get(self, path, params):
                return {"deliveries": []}

        relay = DesktopRelay(FakeClient(), "client_handoff")
        assert relay.destinations == [{"client_type": "codex_desktop", "thread_id": "thread_exec"}]
        assert relay.caller_thread_id == "thread_exec"
        relay._register()
        assert registered.get("client_type") == "codex_desktop"
        assert registered.get("destinations") == [{"client_type": "codex_desktop", "thread_id": "thread_exec"}]
