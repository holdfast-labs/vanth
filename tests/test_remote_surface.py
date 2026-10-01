"""Remote surfaces an agent can actually reach.

Remote execution was half-wired: the MCP tools accepted a ``remote_id`` that
nothing helped you discover, remote jobs could not be listed, and remote logs
were unreachable through every surface even though the controller-side
``log_range`` (and the helper that serves it) existed. These tests pin the
dispatch of the tools that close those gaps.
"""

import asyncio

import pytest

from vanth import server as server_mod


class RecordingClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, object]] = []

    def get(self, path, params=None, **_kwargs):
        self.calls.append(("GET", path, params))
        return {"ok": True}

    def post(self, path, payload=None, **_kwargs):
        self.calls.append(("POST", path, payload))
        return {"ok": True}


@pytest.fixture()
def client(monkeypatch):
    recording = RecordingClient()
    monkeypatch.setattr(server_mod, "get_client", lambda: recording)
    return recording


def test_job_tail_remote_reads_the_remote_log(client):
    asyncio.run(server_mod.job_tail("job_1", remote_id="r1", stream="stderr", max_bytes=4096, offset=128))
    _method, path, params = client.calls[-1]
    assert path == "/remotes/r1/jobs/job_1/tail"
    assert params["stream"] == "stderr"
    assert params["size"] == 4096
    assert params["offset"] == 128


def test_job_tail_remote_refuses_options_it_cannot_honour(client, monkeypatch):
    """`follow`/`grep` cannot work on a remote byte range; silently ignoring them
    would leave the caller thinking a live log was being followed. Validation
    must happen before any daemon I/O."""
    def _boom():
        raise AssertionError("get_client must not be called before remote option validation")

    monkeypatch.setattr(server_mod, "get_client", _boom)
    with pytest.raises(ValueError, match="follow is not supported"):
        asyncio.run(server_mod.job_tail("job_1", remote_id="r1", follow=True))
    with pytest.raises(ValueError, match="grep is not supported"):
        asyncio.run(server_mod.job_tail("job_1", remote_id="r1", grep="error"))
    assert client.calls == []


def test_job_tail_without_remote_id_is_unchanged(client):
    asyncio.run(server_mod.job_tail("job_1", follow=True, timeout_seconds=3))
    _method, path, params = client.calls[-1]
    assert path == "/jobs/job_1/tail"
    assert params["follow"] is True


def test_job_list_remote_uses_the_remote_projection(client):
    server_mod.job_list(limit=5, remote_id="r1")
    _method, path, params = client.calls[-1]
    assert path == "/remotes/r1/jobs"
    assert params == {"limit": 5}


def test_job_list_without_remote_id_is_unchanged(client):
    server_mod.job_list(status=["running"])
    assert client.calls[-1][1] == "/jobs"


def test_job_list_remote_rejects_filters_it_cannot_apply(client):
    """The remote read path projects a shadow and supports only `limit`; silently
    dropping a filter would return jobs the caller did not ask for."""
    with pytest.raises(ValueError, match="filters not supported for remote job lists: status"):
        server_mod.job_list(status=["running"], remote_id="r1")
    with pytest.raises(ValueError, match="name"):
        server_mod.job_list(name="train", remote_id="r1")
    assert client.calls == []


def test_remote_list_and_doctor_are_reachable(client):
    server_mod.remote_list()
    assert client.calls[-1][1] == "/remotes"

    server_mod.remote_doctor()
    assert client.calls[-1][1] == "/remotes/doctor"
    assert client.calls[-1][2] == {"remote_id": None}

    server_mod.remote_doctor("r1")
    assert client.calls[-1][2] == {"remote_id": "r1"}
