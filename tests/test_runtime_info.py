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
