import asyncio
import json
import os
import socket
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client


import shellcmd


def content(result):
    if result.structuredContent is not None:
        return result.structuredContent
    return json.loads(result.content[0].text)


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def start_daemon(tmp_path, extra_env=None):
    port = free_port()
    proc = subprocess.Popen(
        [sys.executable, "-m", "vanth.daemon"],
        env={
            **os.environ,
            "VANTH_HOME": str(tmp_path),
            "VANTH_DAEMON_PORT": str(port),
            **(extra_env or {}),
        },
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return proc, f"http://127.0.0.1:{port}"


def test_mcp_stdio_start_wait_tail(tmp_path):
    async def main():
        daemon, url = start_daemon(tmp_path)
        env = {**os.environ, "VANTH_DAEMON_URL": url, "VANTH_HOME": str(tmp_path)}
        server = StdioServerParameters(
            command=sys.executable,
            args=["-m", "vanth"],
            cwd=str(Path(__file__).parents[1]),
            env=env,
        )
        try:
            async with stdio_client(server) as (read, write):
                async with ClientSession(read, write, read_timeout_seconds=timedelta(seconds=30)) as session:
                    await session.initialize()
                    tools = {tool.name for tool in (await session.list_tools()).tools}
                    assert {"job_start", "job_wait", "job_tail", "job_view", "job_doctor", "job_retry_delivery"} <= tools
                    # Remote execution must be discoverable from MCP alone, not
                    # only by hand-writing HTTP against the daemon.
                    assert {"remote_list", "remote_doctor"} <= tools

                    start = content(
                        await session.call_tool(
                            "job_start",
                            {
                                "command": shellcmd.join(
                                    [sys.executable, str(Path(__file__).parents[1] / "examples" / "long_job.py")]
                                ),
                                "wake_targets": [
                                    {
                                        "type": "codex_thread",
                                        "thread_id": "thread_test",
                                        "events": ["checkpoint"],
                                        "auto_dispatch": False,
                                    }
                                ],
                                "origin_thread_id": "thread_origin",
                                "tags": ["demo"],
                            },
                        )
                    )
                    assert start["startup_confirmed"] is True
                    progress = content(
                        await session.call_tool(
                            "job_wait",
                            {"job_id": start["job_id"], "filters": ["progress"], "timeout_seconds": 5},
                            read_timeout_seconds=timedelta(seconds=30),
                        )
                    )
                    status = content(await session.call_tool("job_status", {"job_id": start["job_id"]}))
                    waited = content(
                        await session.call_tool(
                            "job_wait",
                            {
                                "job_id": start["job_id"],
                                "filters": ["checkpoint"],
                                "since_event_id": progress["event"]["event_id"],
                                "timeout_seconds": 5,
                            },
                            read_timeout_seconds=timedelta(seconds=30),
                        )
                    )
                    tail = content(await session.call_tool("job_tail", {"job_id": start["job_id"]}))
                    deliveries = content(await session.call_tool("job_deliveries", {"job_id": start["job_id"]}))
                    view = content(await session.call_tool("job_view", {"thread_id": "thread_test"}))
                    doctor = content(await session.call_tool("job_doctor", {}))

                    assert progress["event"]["type"] == "progress"
                    assert progress["event"]["data"]["current"] == 1
                    # The status snapshot is taken when the wait returns; a short
                    # job can finish first on a loaded runner, so accept either.
                    assert progress["status"] in {"running", "completed"}
                    assert status["progress"]["current"] >= 1
                    assert status["origin_thread_id"] == "thread_origin"
                    assert status["wake_thread_id"] == "thread_test"
                    assert status["tags"] == ["demo"]
                    assert waited["event"]["message"] == "demo checkpoint"
                    assert deliveries["deliveries"][0]["target_type"] == "codex_thread"
                    assert deliveries["deliveries"][0]["payload"]["target"]["thread_id"] == "thread_test"
                    assert view["jobs"][0]["job_id"] == start["job_id"]
                    assert "jobs" in doctor["tables"]
                    assert "AGENT_EVENT" in tail["content"]

                    # A short job can be launched, awaited, and diagnosed in one call.
                    quick = content(await session.call_tool(
                        "job_start_and_wait",
                        {"command": shellcmd.join([sys.executable, "-c", "print('done')"]),
                         "wait_timeout_seconds": 5},
                        read_timeout_seconds=timedelta(seconds=30),
                    ))
                    assert quick["status"] == "completed"
                    assert quick["wait"]["result"] == "event"
                    assert quick["summary"]["exit_code"] == 0

                    failure = content(await session.call_tool(
                        "job_start_and_wait",
                        {"command": shellcmd.join([sys.executable, "-c",
                                                   "import sys; sys.stderr.write('E'*2100 + ' final marker'); sys.exit(7)"]),
                         "wait_timeout_seconds": 5},
                        read_timeout_seconds=timedelta(seconds=30),
                    ))
                    assert failure["status"] == "failed"
                    assert failure["summary"]["exit_code"] == 7
                    assert failure["summary"]["stderr_excerpt"].endswith("final marker")
                    assert len(failure["summary"]["stderr_excerpt"].encode()) <= 2048
                    plain_summary = content(await session.call_tool(
                        "job_run_summary", {"job_id": failure["job_id"]},
                    ))
                    assert "stderr_excerpt" not in plain_summary
                    verbose_summary = content(await session.call_tool(
                        "job_run_summary", {"job_id": failure["job_id"], "include_stderr_excerpt": True},
                    ))
                    assert verbose_summary["stderr_excerpt"].endswith("final marker")

                    waiting = content(await session.call_tool(
                        "job_start_and_wait",
                        {"command": shellcmd.join([sys.executable, "-c", "import time; time.sleep(5)"]),
                         "wait_timeout_seconds": 1},
                        read_timeout_seconds=timedelta(seconds=30),
                    ))
                    assert waiting["wait"]["result"] == "timeout"
                    assert waiting["status"] in {"running", "launching"}
                    assert waiting["job_id"]
        finally:
            daemon.terminate()
            daemon.wait(timeout=5)

    asyncio.run(main())


def test_mcp_stdio_errors_and_event_cap(tmp_path):
    async def main():
        daemon, url = start_daemon(tmp_path, {"VANTH_MAX_EVENT_BYTES": "20"})
        env = {**os.environ, "VANTH_DAEMON_URL": url, "VANTH_HOME": str(tmp_path)}
        server = StdioServerParameters(
            command=sys.executable,
            args=["-m", "vanth"],
            cwd=str(Path(__file__).parents[1]),
            env=env,
        )
        try:
            async with stdio_client(server) as (read, write):
                async with ClientSession(read, write, read_timeout_seconds=timedelta(seconds=30)) as session:
                    await session.initialize()
                    missing = content(await session.call_tool("job_status", {"job_id": "job_missing"}))
                    assert missing["result"] == "error"

                    command = shellcmd.join(
                        [
                            sys.executable,
                            "-c",
                            "import json; print('AGENT_EVENT '+json.dumps({'type':'checkpoint','data':{'big':'x'*100}}), flush=True)",
                        ]
                    )
                    start = content(await session.call_tool("job_start", {"command": command}))
                    event = content(
                        await session.call_tool(
                            "job_wait",
                            {"job_id": start["job_id"], "filters": ["checkpoint"], "timeout_seconds": 5},
                            read_timeout_seconds=timedelta(seconds=30),
                        )
                    )
                    assert event["event"]["data"] == {"truncated": True, "max_bytes": 20}
        finally:
            daemon.terminate()
            daemon.wait(timeout=5)

    asyncio.run(main())


def test_job_wake_now_inherits_caller_codex_thread(tmp_path):
    """Review P0-2: job_wake_now must resolve CODEX_THREAD_ID in the MCP process
    (which owns the calling task) and include it so a codex_cli_thread target
    without an explicit thread_id inherits the caller's task."""
    async def main():
        daemon, url = start_daemon(tmp_path)
        env = {
            **os.environ,
            "VANTH_DAEMON_URL": url,
            "VANTH_HOME": str(tmp_path),
            "CODEX_THREAD_ID": "thread_from_caller",
        }
        server = StdioServerParameters(
            command=sys.executable,
            args=["-m", "vanth"],
            cwd=str(Path(__file__).parents[1]),
            env=env,
        )
        try:
            async with stdio_client(server) as (read, write):
                async with ClientSession(read, write, read_timeout_seconds=timedelta(seconds=30)) as session:
                    await session.initialize()
                    # Start a quick job so there is a real job row to wake.
                    command = shellcmd.join([sys.executable, "-c", "print('wake me')"])
                    start = content(await session.call_tool("job_start", {"command": command}))
                    # Review rc38 P1: the documented rc37 wake tool names must
                    # remain callable over stdio (agents must not learn
                    # implementation-prefix `mcp_` names).
                    tools = await session.list_tools()
                    names = {tool.name for tool in tools.tools}
                    assert "job_add_wake_target" in names, "rc37 MCP name job_add_wake_target must be registered"
                    assert "job_wake_now" in names, "rc37 MCP name job_wake_now must be registered"
                    assert "daemon_wake" in names, "rc37 MCP name daemon_wake must be registered"
                    # The implementation-prefixed callables are NOT advertised
                    # (clients must not learn prefix names).
                    assert "mcp_job_wake_now" not in names
                    assert "mcp_job_add_wake_target" not in names
                    assert "mcp_daemon_wake" not in names
                    # No explicit thread_id: must inherit CODEX_THREAD_ID.
                    woken = content(
                        await session.call_tool(
                            "job_wake_now",
                            {"job_id": start["job_id"], "type": "codex_cli_thread", "events": ["completed"]},
                        )
                    )
                    assert woken["result"] == "ok"
                    assert woken["woken"] is True
                    # The delivery enqueued by wake_now must carry the inherited
                    # caller thread id (status may have moved off pending if the
                    # dispatch worker already attempted the delivery).
                    deliv = content(
                        await session.call_tool(
                            "job_deliveries", {"job_id": start["job_id"], "limit": 10}
                        )
                    )
                    assert deliv["deliveries"], "wake_now must enqueue a delivery"
                    target = deliv["deliveries"][0]["payload"]["target"]
                    assert target.get("thread_id") == "thread_from_caller", f"must inherit caller thread, got {target}"
                    assert target.get("type") == "codex_cli_thread"
        finally:
            daemon.terminate()
            daemon.wait(timeout=5)

    asyncio.run(main())
