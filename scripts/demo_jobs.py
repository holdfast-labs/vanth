"""Start a handful of realistic demo jobs to watch in `vanth monitor`.

Usage: uv run python scripts/demo_jobs.py [--duration SCALE] [--home DIR]

Jobs are started through the normal CLI (`vanth start ...`), which starts the
daemon for the home if it is not running. Each job is a script in
examples/demo/ that prints AGENT_EVENT lines for a few minutes:

  finetune-llama   training: loss, accuracy, gpu util/mem, tokens/s, lr; declares panels
  eval-suite       evaluation that stalls (prints nothing) partway, then finishes
  ingest-s3-logs   file download with MB/s
  nightly-backup   checkpoints but no progress
  deploy-staging   fails with exit 1 after a while
  render-thumbs    queued behind the training job in the one-slot "demo-gpu" pool

--duration scales every job (2 = twice as long). --home defaults to the home
vanth itself uses; with another home the daemon gets a free port of its own so
it never collides with a daemon already running for the default home. Watch
with `VANTH_HOME=DIR vanth-monitor`; stop that daemon when done with
  uv run python -c "from vanth.client import VanthClient; VanthClient(home=r'DIR').post('/shutdown', {})"
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
DEMO = REPO / "examples" / "demo"

# name, script, extra `vanth start` options
JOBS = [
    ("finetune-llama", "train.py", ["--pool", "demo-gpu", "--tag", "train"]),
    ("eval-suite", "eval.py", ["--tag", "eval"]),
    ("ingest-s3-logs", "ingest.py", ["--tag", "etl"]),
    ("nightly-backup", "backup.py", ["--tag", "ops"]),
    ("deploy-staging", "deploy.py", ["--tag", "ops"]),
    ("render-thumbs", "render.py", ["--pool", "demo-gpu", "--tag", "render"]),
]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def live_daemon_url(home: Path) -> str | None:
    """The URL of a daemon already serving this home, if one answers."""
    try:
        url = json.loads((home / "daemon.json").read_text(encoding="utf-8"))["url"]
        with urllib.request.urlopen(url.rstrip("/") + "/health", timeout=2) as resp:
            return url if resp.status == 200 else None
    except (OSError, ValueError, KeyError):
        return None


def use_home(home: Path) -> None:
    """Point this process (and the CLI it runs) at home and its own daemon."""
    os.environ["VANTH_HOME"] = str(home)
    url = live_daemon_url(home)
    if url is None:
        port = free_port()
        os.environ["VANTH_DAEMON_PORT"] = str(port)
        url = f"http://127.0.0.1:{port}"
    os.environ["VANTH_DAEMON_URL"] = url


def start_job(name: str, script: str, options: list[str], duration: float) -> str:
    argv = [sys.executable, "-m", "vanth", "--json", "start", "--name", name, "--tag", "demo", *options,
            "--", sys.executable, str(DEMO / script), "--duration", str(duration)]
    out = subprocess.run(argv, capture_output=True, text=True, cwd=REPO)
    if out.returncode != 0:
        raise SystemExit(f"vanth start {name} failed ({out.returncode}): {out.stderr or out.stdout}")
    try:
        result = json.loads(out.stdout)
    except ValueError:
        return out.stdout.strip()
    return f"{result.get('job_id')} ({result.get('status')})"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--duration", type=float, default=1.0, help="time scale for every job (default 1.0, a few minutes)")
    parser.add_argument("--home", help="vanth home to use (default: vanth's own)")
    args = parser.parse_args()
    if args.duration <= 0:
        parser.error("--duration must be positive")
    if args.home:
        home = Path(args.home).expanduser().resolve()
        home.mkdir(parents=True, exist_ok=True)
        use_home(home)

    from vanth.client import VanthClient

    client = VanthClient()
    client.ensure()
    # One slot: the render job queues until the training job finishes.
    client.post("/pools", {"pool": "demo-gpu", "max_parallel": 1})
    for name, script, options in JOBS:
        print(f"{name:16} {start_job(name, script, options, args.duration)}")
    print(f"VANTH_HOME={client.home}")
    print(f"daemon={client.url}")
    print(f"watch:  VANTH_HOME={client.home} vanth-monitor   (or: go run ./cmd/vanth monitor)")


if __name__ == "__main__":
    main()
