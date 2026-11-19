"""Python entry point (``python -m vanth.monitor``) for the native Go monitor.

The monitor is a read-only Go binary that renders Vanth's durable SQLite/JSONL
state as a Bubble Tea TUI. Platform wheels install it as the native
``vanth-monitor`` executable in the env's scripts directory; standalone builds
bundle it as ``vanth/monitor-bin/vanth-monitor``. This module locates and
re-executes it.

If the bundled binary is missing (for example, in a source checkout or sdist
install), the wrapper explains how to build and run the monitor from the Go
source or install a platform wheel.
"""

from __future__ import annotations

import os
import subprocess
import sys
import sysconfig
from pathlib import Path

import vanth

_BIN_DIR_NAME = "monitor-bin"
_EXE_SUFFIX = ".exe" if os.name == "nt" else ""


def _binary_name() -> str:
    return "vanth-monitor" + _EXE_SUFFIX


def bundled_binary() -> Path | None:
    """Return the host-platform monitor binary bundled in the wheel, if any."""
    candidates = [Path(vanth.__file__).parent / _BIN_DIR_NAME / _binary_name()]
    if not getattr(sys, "frozen", False):
        # A frozen build is itself named vanth-monitor; never re-exec it.
        candidates.insert(0, Path(sysconfig.get_path("scripts")) / _binary_name())
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return None


def find_monitor_binary() -> Path:
    """Return the bundled monitor binary, or raise with a fix-it message."""
    bundled = bundled_binary()
    if bundled is None:
        if os.name == "nt":
            build_command = (
                r"mkdir dist 2>nul & "
                r"go build -o dist\vanth-monitor.exe .\cmd\vanth && "
                r"dist\vanth-monitor.exe monitor"
            )
        else:
            build_command = (
                "mkdir -p dist && go build -o dist/vanth-monitor ./cmd/vanth && "
                "./dist/vanth-monitor monitor"
            )
        raise RuntimeError(
            "the vanth-monitor native binary is not present in this install.\n"
            "From the repository root, build and run it directly with:\n"
            f"  {build_command}\n"
            "To use the vanth-monitor command without a Go toolchain, install "
            "the platform wheel:\n"
            "  uv tool install --force vanth\n"
            f"(looked in: {sysconfig.get_path('scripts')} and {Path(vanth.__file__).parent / _BIN_DIR_NAME})"
        )
    return bundled


def main() -> int:
    try:
        binary = find_monitor_binary()
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    args = sys.argv[1:]
    # The native binary uses subcommands; default to the monitor when invoked
    # via the dedicated console script (vanth-monitor).
    if not args or args[0] not in {"monitor", "daemon", "mcp", "doctor", "cleanup", "--version"}:
        args = ["monitor", *args]
    try:
        return subprocess.call([str(binary), *args])
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
