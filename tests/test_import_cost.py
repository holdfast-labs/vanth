"""Tests guarding process-startup import costs (CLI + job runner hot paths).

Every `vanth` invocation and every job-runner spawn pays the interpreter +
`vanth.server` import before doing any work, so heavy modules must stay out
of the import graph (subprocess-based: hermetic, independent of whatever the
test session itself already imported).
"""

import subprocess
import sys

import pytest


def _run(code: str) -> str:
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def test_package_import_avoids_metadata_scan():
    """`import vanth` must not pay the importlib.metadata scan (~90ms)."""
    assert _run(
        "import vanth, sys; "
        "print('importlib.metadata' in sys.modules)"
    ) == "False"


def test_version_resolves_without_slow_path():
    """The lazy `__version__` must resolve via the METADATA fast path."""
    out = _run(
        "import vanth, sys; v = vanth.__version__; "
        "print(v); print('importlib.metadata' in sys.modules)"
    )
    version, slow = out.splitlines()
    assert version.strip()
    assert slow == "False"
    # Cached in module globals: the value is stable and re-importable.
    assert _run("from vanth import __version__; print(__version__)") == version.strip()


def test_server_import_avoids_asyncio_and_client():
    """`import vanth.server` must stay free of asyncio (~50ms) and the
    urllib-backed client (~40ms); both are imported function-locally."""
    out = _run(
        "import vanth.server, sys; "
        "print('asyncio' in sys.modules); print('vanth.client' in sys.modules)"
    )
    assert out.splitlines() == ["False", "False"]


def test_version_matches_installed_distribution():
    """The fast path must agree with importlib.metadata (no stale value)."""
    try:
        from importlib.metadata import PackageNotFoundError, version

        version("vanth")
    except PackageNotFoundError:
        pytest.skip("vanth is not installed; only the 0.0.0 fallback applies")
    assert _run(
        "import vanth; from importlib.metadata import version; "
        "print(vanth.__version__ == version('vanth'))"
    ) == "True"
