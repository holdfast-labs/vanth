"""Render monitor snapshot frames to PNG for visual review.

Usage: uv run --with rich python scripts/monitor_screenshots.py OUT_DIR [VANTH_HOME]

Runs the opt-in Go snapshot test (internal/monitor/snapshot_test.go), converts
each ANSI frame to HTML with rich, and screenshots it with headless Edge/Chrome.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

from rich.console import Console
from rich.terminal_theme import MONOKAI
from rich.text import Text

BROWSERS = [
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    "msedge", "chromium", "google-chrome", "chrome",
]


def browser() -> str:
    for candidate in BROWSERS:
        found = candidate if Path(candidate).is_file() else shutil.which(candidate)
        if found:
            return found
    raise SystemExit("no Edge/Chrome found for headless screenshots")


def main() -> None:
    out = Path(sys.argv[1]).resolve()
    out.mkdir(parents=True, exist_ok=True)
    home = sys.argv[2] if len(sys.argv) > 2 else os.environ.get("VANTH_HOME") or str(Path.home() / ".vanth")
    env = {**os.environ, "VANTH_MONITOR_SNAPSHOT_DIR": str(out), "VANTH_MONITOR_E2E_HOME": home}
    root = Path(__file__).resolve().parent.parent
    subprocess.run(["go", "test", "./internal/monitor", "-run", "TestWriteSnapshots", "-count=1"],
                   cwd=root, env=env, check=True)
    exe = browser()
    for frame in sorted(out.glob("*.ans")):
        lines = frame.read_text(encoding="utf-8").split("\n")
        width = max(len(Text.from_ansi(line).plain) for line in lines)
        console = Console(record=True, width=width, file=open(os.devnull, "w", encoding="utf-8"))
        console.print(Text.from_ansi("\n".join(lines)), soft_wrap=True)
        html = frame.with_suffix(".html")
        page = console.export_html(theme=MONOKAI, inline_styles=True)
        # Fixed line height so box-drawing borders join; the window is sized from it.
        # !important: rich inlines a font-family (Consolas here) that lacks heavy box-drawing glyphs.
        css = ("<style>body{margin:12px;background:#0c0c0c}pre{margin:0;font:14px/17px "
               "'Cascadia Mono',Consolas,monospace!important}</style></head>")
        page = page.replace("</head>", css, 1)
        # Fallback fonts draw symbols (✔ ▲ ◌ …) wider than a cell; pin each to 1ch so columns stay aligned.
        page = re.sub(r"[←-⯿]", lambda m: f'<span style="display:inline-block;width:1ch">{m.group()}</span>', page)
        html.write_text(page, encoding="utf-8")
        png = frame.with_suffix(".png")
        size = f"{int(width * 8.15) + 24},{len(lines) * 17 + 24}"
        subprocess.run([exe, "--headless", "--disable-gpu", "--hide-scrollbars",
                        f"--window-size={size}", f"--screenshot={png}", html.as_uri()],
                       check=True, capture_output=True, timeout=60)
        print(png)


if __name__ == "__main__":
    main()
