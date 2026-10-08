"""Tests for vanth.agent_logger (loguru -> AGENT_EVENT log lines)."""

import asyncio
import io
import json
import sys

import pytest

from vanth.agent_logger import logger, log_with_context
from vanth.server import JobManager, parse_agent_event_line


import shellcmd


def capture_log(method, *args, **kwargs):
    out = io.StringIO()
    original = sys.stdout

    class _Shim(io.StringIO):
        pass

    # Swap stdout for the duration of the log call.
    import builtins

    real_print = builtins.print
    builtins.print = lambda *a, **k: out.write(a[0] + "\n")
    try:
        method(*args, **kwargs)
    finally:
        builtins.print = real_print
    return out.getvalue().strip()


def test_logger_emits_parseable_agent_event():
    line = capture_log(logger.info, "training started", run_id="abc")
    event = parse_agent_event_line(line)
    assert event is not None
    assert event["type"] == "log"
    assert event["level"] == "info"
    assert event["message"] == "training started"
    assert event["data"] == {"run_id": "abc"}


def test_log_with_context_and_levels():
    line = capture_log(log_with_context, "warning", "low disk", disk_gb=2.5)
    event = parse_agent_event_line(line)
    assert event["type"] == "log"
    assert event["level"] == "warning"
    assert event["data"] == {"disk_gb": 2.5}


def test_logger_events_persist_through_daemon(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        code = (
            "from vanth.agent_logger import logger;"
            "logger.info('loguru line one', phase='train');"
            "logger.warning('loguru warn')"
        )
        command = shellcmd.join([sys.executable, "-c", code])
        job_id = manager.start(command)["job_id"]
        asyncio.run(manager.wait(job_id, ["log"], timeout_seconds=10))
        asyncio.run(manager.wait(job_id, ["completed"], timeout_seconds=10))
        logs = [e for e in manager.events(job_id, types=["log"], limit=10)["events"]]
        assert len(logs) >= 2, logs
        messages = [e["message"] for e in logs]
        assert "loguru line one" in messages
        assert "loguru warn" in messages
        line_one = next(e for e in logs if e["message"] == "loguru line one")
        assert line_one["level"] == "info"
        assert line_one["data"] == {"phase": "train"}
    finally:
        manager.close()


def test_logger_error_attr_raises():
    with pytest.raises(AttributeError):
        logger.nonexistent_level("boom")
