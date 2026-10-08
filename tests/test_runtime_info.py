"""Tests for run-overview metadata capture costs and behavior."""

import pytest

from vanth import runtime_info
from vanth.runtime_info import _git_state


def test_git_state_skips_spawns_outside_repo(tmp_path, monkeypatch):
    """Non-repo cwds must not pay 3 git spawns (~200ms on Windows)."""
    monkeypatch.setattr(
        runtime_info.subprocess, "run", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("spawned git"))
    )
    assert _git_state(str(tmp_path)) is None


def test_git_state_still_works_in_repo():
    """The gate must not change results for real checkouts (this repo)."""
    import os
    import shutil

    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if not os.path.lexists(os.path.join(here, ".git")) or shutil.which("git") is None:
        pytest.skip("no git checkout available")
    state = _git_state(here)
    assert state is not None and "commit" in state


def test_git_state_refreshes_stale_entries(tmp_path, monkeypatch):
    """Branch/commit go stale on checkout, so entries expire via TTL."""
    from vanth.runtime_info import _git_cache

    cwd = str(tmp_path)
    _git_cache[cwd] = (0.0, {"branch": "stale-branch"})
    monkeypatch.setattr(
        runtime_info.subprocess, "run", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("spawned git"))
    )
    # tmp_path is not a repo: recompute hits the no-spawn gate and drops the
    # stale positive instead of returning it.
    assert _git_state(cwd) is None
    assert _git_cache[cwd][1] is None


def test_git_state_cached_within_ttl(tmp_path, monkeypatch):
    """A fresh entry must not recompute (no spawns at all)."""
    from vanth.runtime_info import _git_cache
    import time

    cwd = str(tmp_path)
    _git_cache[cwd] = (time.monotonic(), None)
    monkeypatch.setattr(
        runtime_info.subprocess, "run", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("spawned git"))
    )
    assert _git_state(cwd) is None


def test_git_cache_bounded():
    """Distinct cwds must not grow the cache forever."""
    from vanth.runtime_info import _GIT_CACHE_MAX, _remember_git_state, _git_cache

    saved = dict(_git_cache)
    try:
        for index in range(_GIT_CACHE_MAX + 50):
            _remember_git_state(f"C:\\nope\\{index}", None, 0.0)
        assert len(_git_cache) <= _GIT_CACHE_MAX
    finally:
        _git_cache.clear()
        _git_cache.update(saved)
