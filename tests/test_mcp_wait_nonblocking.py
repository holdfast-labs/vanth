import asyncio
import time

import vanth.server as server_mod


class _RecordingClient:
    def __init__(self, post_result=None, get_result=None, post_delay=0.0, get_delay=0.0):
        self.post_result = post_result if post_result is not None else {}
        self.get_result = get_result if get_result is not None else {}
        self.post_delay = post_delay
        self.get_delay = get_delay
        self.calls = []

    def post(self, path, payload=None, *, timeout=None):
        self.calls.append(("POST", path, payload, timeout))
        if self.post_delay:
            time.sleep(self.post_delay)
        return dict(self.post_result)

    def get(self, path, params=None, *, timeout=None):
        self.calls.append(("GET", path, params, timeout))
        if self.get_delay:
            time.sleep(self.get_delay)
        return dict(self.get_result)


def test_job_wait_slices_long_timeout_into_still_running(monkeypatch):
    client = _RecordingClient(
        post_result={"result": "timeout", "job_id": "job_x", "status": "running", "message": "No matching event before timeout"}
    )
    monkeypatch.setattr(server_mod, "get_client", lambda: client)

    result = asyncio.run(server_mod.job_wait("job_x", ["completed"], timeout_seconds=3600))

    assert result["result"] == "still_running"
    assert result["waited_seconds"] == 25
    assert result["requested_timeout_seconds"] == 3600
    _method, path, payload, timeout = client.calls[0]
    assert path == "/jobs/job_x/wait"
    assert payload["timeout_seconds"] == 25
    assert timeout == 30.0


def test_job_wait_keeps_short_timeout_semantics(monkeypatch):
    client = _RecordingClient(post_result={"result": "timeout", "job_id": "job_x", "status": "running"})
    monkeypatch.setattr(server_mod, "get_client", lambda: client)

    result = asyncio.run(server_mod.job_wait("job_x", ["completed"], timeout_seconds=5))

    assert result["result"] == "timeout"
    assert client.calls[0][2]["timeout_seconds"] == 5


def test_job_wait_returns_event_immediately(monkeypatch):
    client = _RecordingClient(
        post_result={"result": "event", "job_id": "job_x", "status": "completed", "event": {"type": "completed"}}
    )
    monkeypatch.setattr(server_mod, "get_client", lambda: client)

    result = asyncio.run(server_mod.job_wait("job_x", ["completed"], timeout_seconds=3600))

    assert result["result"] == "event"


def test_job_wait_does_not_invite_a_loop_on_a_terminal_job(monkeypatch):
    """A terminal job whose narrow filter never matched must not become
    still_running, or the caller would re-call forever."""
    client = _RecordingClient(post_result={"result": "timeout", "job_id": "job_x", "status": "completed"})
    monkeypatch.setattr(server_mod, "get_client", lambda: client)

    result = asyncio.run(server_mod.job_wait("job_x", ["checkpoint"], timeout_seconds=3600))

    assert result["result"] == "timeout"
    assert result["status"] == "completed"


def test_job_wait_does_not_block_the_event_loop(monkeypatch):
    class SlowClient:
        def post(self, path, payload, *, timeout=None):
            time.sleep(0.4)
            return {"result": "event", "job_id": "job_x", "status": "completed", "event": {"type": "completed"}}

    monkeypatch.setattr(server_mod, "get_client", lambda: SlowClient())

    async def scenario():
        ticks = 0

        async def ticker():
            nonlocal ticks
            while True:
                await asyncio.sleep(0.02)
                ticks += 1

        task = asyncio.create_task(ticker())
        try:
            await server_mod.job_wait("job_x", ["completed"], timeout_seconds=5)
        finally:
            task.cancel()
        return ticks

    assert asyncio.run(scenario()) >= 5


def test_job_tail_follow_is_capped_to_the_slice(monkeypatch):
    client = _RecordingClient(get_result={"content": "", "next_offset": 0})
    monkeypatch.setattr(server_mod, "get_client", lambda: client)

    asyncio.run(server_mod.job_tail("job_x", follow=True, timeout_seconds=3600))

    _method, path, payload, timeout = client.calls[0]
    assert path == "/jobs/job_x/tail"
    assert payload["timeout_seconds"] == 25.0
    assert timeout == 30.0


def test_job_tail_follow_within_slice_is_unchanged(monkeypatch):
    client = _RecordingClient(get_result={"content": "", "next_offset": 0})
    monkeypatch.setattr(server_mod, "get_client", lambda: client)

    asyncio.run(server_mod.job_tail("job_x", follow=True, timeout_seconds=3))

    assert client.calls[0][2]["timeout_seconds"] == 3.0
    assert client.calls[0][3] == 8.0


def test_job_stop_is_bounded_and_returns_acknowledgement(monkeypatch):
    client = _RecordingClient(post_delay=0.3)
    monkeypatch.setattr(server_mod, "get_client", lambda: client)
    monkeypatch.setattr(server_mod, "_mcp_wait_slice_seconds", lambda: 0.05)

    result = asyncio.run(server_mod.job_stop("job_x", kill_after_seconds=600))

    assert result["result"] == "still_running"
    assert result["job_id"] == "job_x"


def test_job_start_and_wait_reports_still_running_for_long_waits(monkeypatch):
    client = _RecordingClient(post_result={"result": "timeout", "job_id": "job_x", "status": "running"})
    monkeypatch.setattr(server_mod, "get_client", lambda: client)
    monkeypatch.setattr(server_mod, "job_start", lambda **kwargs: {"job_id": "job_x", "status": "running"})
    monkeypatch.setattr(server_mod, "job_run_summary", lambda *a, **k: {"status": "running"})

    result = asyncio.run(server_mod.job_start_and_wait("sleep", wait_timeout_seconds=300))

    assert result["wait"]["result"] == "still_running"


def test_wait_slice_is_configurable_and_validated(monkeypatch):
    monkeypatch.setenv("VANTH_MCP_WAIT_SLICE", "10")
    assert server_mod._mcp_wait_slice_seconds() == 10.0
    monkeypatch.setenv("VANTH_MCP_WAIT_SLICE", "0")
    assert server_mod._mcp_wait_slice_seconds() == 25.0
    monkeypatch.setenv("VANTH_MCP_WAIT_SLICE", "not-a-number")
    assert server_mod._mcp_wait_slice_seconds() == 25.0
    monkeypatch.setenv("VANTH_MCP_WAIT_SLICE", "100")
    assert server_mod._mcp_wait_slice_seconds() == server_mod._MAX_MCP_WAIT_SLICE
    monkeypatch.setenv("VANTH_MCP_WAIT_SLICE", "nan")
    assert server_mod._mcp_wait_slice_seconds() == 25.0
