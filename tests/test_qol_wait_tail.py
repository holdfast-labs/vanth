import asyncio
import json
import sys
import time

from vanth.server import JobManager


import shellcmd


def cmd(code: str) -> str:
    return shellcmd.join([sys.executable, "-c", code])


def start_job(manager, code, **kwargs):
    return manager.start(cmd(code), **kwargs)["job_id"]


def test_wait_return_progress_streams_then_completes(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        code = (
            "import json,time;"
            "f=lambda i:(print('AGENT_EVENT '+json.dumps({'type':'progress','data':{'current':i,'total':10}}), flush=True), "
            "time.sleep(0.05));"
            "[f(i) for i in range(1,11)]"
        )
        job_id = start_job(manager, code)
        seen = []
        since = None
        result = None
        for _ in range(20):
            result = asyncio.run(manager.wait(job_id, ["completed"], since_event_id=since,
                                              return_progress=True, timeout_seconds=10))
            if result["result"] == "progress":
                assert result["event"]["type"] == "progress"
                seen.append(result["event"])
                since = result["event"]["event_id"]
                continue
            assert result["result"] == "event"
            assert result["event"]["type"] == "completed"
            break
        assert result is not None
        assert result["result"] == "event"
        assert result["event"]["type"] == "completed"
        assert len(seen) >= 3
    finally:
        manager.close()


def test_wait_return_progress_false_returns_completed_directly(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        code = (
            "import json,time;"
            "f=lambda i:(print('AGENT_EVENT '+json.dumps({'type':'progress','data':{'current':i,'total':10}}), flush=True), "
            "time.sleep(0.05));"
            "[f(i) for i in range(1,11)]"
        )
        job_id = start_job(manager, code)
        result = asyncio.run(manager.wait(job_id, ["completed"], return_progress=False, timeout_seconds=10))
        assert result["result"] == "event"
        assert result["event"]["type"] == "completed"
    finally:
        manager.close()


def test_wait_return_progress_false_is_identical_default(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(manager, "print('plain')")
        default_result = asyncio.run(manager.wait(job_id, ["completed"], timeout_seconds=10))
        assert default_result["result"] == "event"
        assert default_result["event"]["type"] == "completed"
    finally:
        manager.close()


def test_tail_follow_accumulates_new_output(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        code = (
            "import time;"
            "print('line1', flush=True);"
            "time.sleep(0.3);"
            "print('line2', flush=True);"
            "time.sleep(0.3);"
            "print('line3', flush=True)"
        )
        job_id = start_job(manager, code)
        asyncio.run(manager.wait(job_id, ["started"], timeout_seconds=10))
        result = manager.tail(job_id, follow=True, timeout_seconds=2.0, offset=0)
        assert result["followed"] is True
        assert result["new_bytes"] > 0
        assert "line1" in result["content"]
        assert "line2" in result["content"]
        assert result["next_offset"] >= result["offset"]
    finally:
        manager.close()


def test_tail_follow_without_follow_has_no_followed_key(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(manager, "print('snapshot', flush=True)")
        asyncio.run(manager.wait(job_id, ["completed"], timeout_seconds=10))
        result = manager.tail(job_id)
        assert "followed" not in result
        assert "line" not in result
        assert "snapshot" in result["content"]
        plain = manager.tail(job_id, follow=False)
        assert plain == result
    finally:
        manager.close()


def test_tail_follow_returns_after_timeout_with_no_output(tmp_path):
    manager = JobManager(tmp_path / "state")
    try:
        job_id = start_job(manager, "import time; time.sleep(1.0)")
        asyncio.run(manager.wait(job_id, ["started"], timeout_seconds=10))
        started = time.monotonic()
        result = manager.tail(job_id, follow=True, timeout_seconds=1.0, offset=0)
        elapsed = time.monotonic() - started
        assert result["followed"] is True
        assert result["new_bytes"] == 0
        assert result["content"] == ""
        assert elapsed < 3.0
    finally:
        manager.close()
