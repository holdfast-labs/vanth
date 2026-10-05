"""Build a self-contained Vanth executable with PyInstaller.

PyInstaller builds for the host platform only, so the release workflow runs one
job per OS. The Go terminal monitor is bundled when a binary is available (from
``VANTH_MONITOR_BIN`` or ``build/monitor/``); when it is absent the build still
succeeds and ``vanth monitor`` prints its fix-it message at run time.

Usage:
  uv run python scripts/build_standalone.py [--output-name NAME]
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MONITOR_DIR = ROOT / "build" / "monitor"


def monitor_binary() -> Path | None:
    """Source of the Go monitor for this host, or None when unavailable."""
    name = "vanth-monitor.exe" if os.name == "nt" else "vanth-monitor"
    env = os.environ.get("VANTH_MONITOR_BIN")
    if env:
        source = Path(env)
        if not source.is_file():
            raise SystemExit(f"VANTH_MONITOR_BIN is set but not a file: {source}")
        return source
    candidate = MONITOR_DIR / name
    return candidate if candidate.is_file() else None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-name", default="vanth")
    args = parser.parse_args()

    sep = ";" if os.name == "nt" else ":"
    workpath = ROOT / "build" / "pyinstaller"
    workpath.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        "-m",
        "PyInstaller",
        "--noconfirm",
        "--clean",
        "--onefile",
        "--name",
        args.output_name,
        "--distpath",
        str(ROOT / "dist"),
        "--workpath",
        str(workpath),
        "--specpath",
        str(workpath),
        "--collect-submodules",
        "vanth",
        # `mcp` is mostly statically reachable from vanth.server, but
        # `mcp.cli` imports the optional `typer` CLI dependency, so collecting
        # every submodule (or letting static analysis wander into it) fails.
        "--exclude-module",
        "mcp.cli",
        # jsonschema (pulled in by mcp's validation) loads its meta-schemas as
        # package data files; without this the runner subprocess crashes on
        # import.
        "--collect-data",
        "jsonschema_specifications",
    ]
    if sys.platform == "win32":
        cmd += ["--collect-data", "tzdata"]

    source = monitor_binary()
    if source is not None:
        target_name = "vanth-monitor.exe" if os.name == "nt" else "vanth-monitor"
        MONITOR_DIR.mkdir(parents=True, exist_ok=True)
        staged = MONITOR_DIR / target_name
        if source.resolve() != staged.resolve():
            shutil.copyfile(source, staged)
            if os.name != "nt":
                os.chmod(staged, 0o755)
        cmd += ["--add-data", f"{staged}{sep}vanth/monitor-bin"]
    else:
        print("warning: no monitor binary found; `vanth monitor` will require Go", file=sys.stderr)

    cmd.append(str(ROOT / "scripts" / "vanth_frozen.py"))
    return subprocess.call(cmd, cwd=ROOT)


if __name__ == "__main__":
    raise SystemExit(main())
