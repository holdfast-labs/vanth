# vanth

Event-driven background jobs for agents.

Vanth is a localhost background-job daemon with a Model Context Protocol (MCP)
interface. It runs detached, non-interactive shell commands; captures their
output durably; parses optional `AGENT_EVENT` structured events into progress
bars, metric series, and checkpoints; and can wake a Codex or OpenCode session
when a job needs attention. It is built for one trusted user on one machine.

- **Any command**: downloads, image/audio processing, ETL, ML training — if it
  runs in a shell, Vanth can run it detached and track it.
- **Durable**: jobs and events live in SQLite (`WAL`, busy-timeout) and survive
  daemon, MCP, and machine restarts.
- **Event-first**: agents `job_wait` for meaningful events instead of polling
  logs.
- **Wake-on-attention**: durable at-least-once deliveries resume a Codex thread
  or OpenCode session when a job needs a human or agent.
- **Terminal dashboard**: the native Go `monitor` renders a live
  W&B-LEET-style dashboard of jobs, metrics, and plots.

Out of scope: TLS, multi-user tenancy/RBAC, a web UI, and distributed workers.
Supported: interactive stdin (`job_send`), concurrent-job quotas, automatic
retention, cron/interval schedules, pools/priority/pause queues, readiness
triggers, kill attribution + secret masking, duration/flaky analytics, managed
artifacts, and remote SSH execution (**beta**).

**For agents:** when MCP is available, start work with `job_start`, then
`job_wait` for `progress`/`checkpoint`/`completed` events instead of polling.
When MCP is unavailable, use the `vanth start` CLI fallback. Make jobs emit
`AGENT_EVENT` lines (below) so progress, metrics, and checkpoints appear live
in the `vanth-monitor` dashboard; and let long jobs resume you via wake targets
instead of you checking in.

For a short local command with a known time bound, `job_start_and_wait` combines
the start, bounded wait, and run summary in one call. Use `job_start` plus a
wake target for long-running work that should continue while you do something
else, or when you need to handle intermediate events such as checkpoints.

Local starts accept an optional durable `idempotency_key`: retry identical
settings with the same key to recover the original job after a lost response or
daemon restart. The CLI form is `vanth start --idempotency-key KEY -- <command>`.
Changed settings with an existing key are rejected. `vanth start --dry-run -- <command>`
(MCP: `job_start(..., dry_run=True)`) previews the resolved shell,
working directory, wake destination, and policy without launching work.
Start-and-wait results include bounded stdout and stderr excerpts.

### Wake me when it finishes

```cmd
vanth start --wake-me -- <command>
```

The MCP form for this core workflow is `job_start(command="...", wake_me=True)`. `--wake-me`
defaults to all terminal outcomes (`completed`, `failed`, `timeout`, `cancelled`,
`orphaned`); use `--wake-me=completed,failed,checkpoint` to override the event
list. Never use the relay client id `opencode-<pid>-<rand>` as `session_id`: use
the `ses_...` destination in `vanth doctor --json`, or omit `session_id` to
resolve the live relay automatically.

---

## Quick start

Install with `uv` (Python 3.11+):

```cmd
uv tool install vanth
```

This installs the `vanth` MCP server, `vanthd` daemon, `vanth-monitor`, and
the ops CLI as standalone tools (the wheel bundles the native Go monitor, so
no Go toolchain is needed). Wheels are published for Windows x86_64, Linux
x86_64/arm64, and macOS x86_64/arm64.

**No Python toolchain?** Download the self-contained binary for your platform
from the [releases page](https://github.com/holdfast-labs/vanth/releases)
(`vanth-standalone-linux-x86_64`, `vanth-standalone-windows-x86_64.exe`, or
`vanth-standalone-macos-arm64`) and run it directly — it needs neither Python
nor a package manager. It ships the same CLI/MCP server and daemon, bundles the
Go monitor, and dispatches the internal spawns that a wheel would run as
`python -m vanth.<module>`. Copies named `vanthd` / `vanth-monitor` act as those
commands. (One caveat: it is a PyInstaller one-file build, so each job runner
re-extracts the bundle on start; the wheel is faster for many short jobs.)

From a source checkout (development), install the project environment with `uv sync` and run commands as `uv run vanth ...`. The rest of this guide shows the installed `vanth` command unless a row says `uv run`.

```cmd
git clone https://github.com/holdfast-labs/vanth.git
cd vanth
uv sync
```

The daemon **autostarts on demand** for MCP calls and operational CLI commands.
`vanth status` is an observational check: it reports `DOWN` if the daemon is
stopped and does not start it. Use `vanth doctor` for health details; a normal
MCP call or command such as `vanth list` will start the daemon on demand.

1. **Register the MCP server** in opencode, Codex, and Claude-style clients:

   ```cmd
   vanth setup
   ```

   This detects client configs and supported client commands on `PATH`, then
   configures them with prompts. When a
   supported client's config file is missing, setup creates it before adding
   Vanth. Use `vanth setup --yes` for scripts, or name clients such as
   `vanth setup opencode codex`.

2. **Refresh and verify the client connection.** MCP clients usually load server
   configuration at startup. Restart or reload the client after setup, then
   confirm its tool picker/list includes `job_start`, `job_start_and_wait`,
   `job_wait`, and `job_doctor`. In OpenCode, `opencode mcp list` checks the
   connection. If the server is missing, run `vanth status` and `vanth doctor`; for a source
   checkout, make sure the client config launches `uv run vanth` from the repo.

3. **Check health**:

   ```cmd
   vanth status
   vanth doctor
   ```

   `status` shows daemon state and jobs without starting the daemon;
   `doctor` prints the full health report and may start it on demand.

4. **Pick up updates** — after upgrading, restart the daemon so it runs the new
   code. In-flight jobs survive (runners are detached):

   ```cmd
   vanth restart
   ```

### End-to-end: run a tracked job

Once the MCP client is connected, this is the whole loop:

| Goal | MCP tools |
|---|---|
| Start a job | `job_start` |
| Start and collect a bounded result | `job_start_and_wait` (short local jobs) |
| Wait for progress or completion | `job_wait` |
| Inspect output or status | `job_status`, `job_run_summary`, `job_tail`, `job_events` |
| Stop or retry work | `job_stop`, `job_rerun` |
| Diagnose a wake | `job_deliveries`, `job_delivery_attempts`, `job_retry_delivery` |

The complete MCP reference is [docs/agent-tools.md](docs/agent-tools.md).

`job_start` itself confirms acceptance and returns a job ID. For a direct local
job it also waits briefly for the workload's `started` event and reports
`startup_confirmed`; this confirms process launch, not successful completion.
Queued jobs return `queued` immediately. Use `job_wait` for later progress or
completion, rather than to find out whether the initial start was accepted.

For a bounded command that should finish before the tool call returns:

```text
job_start_and_wait(
  command="uv run python -m compileall -q src",
  wait_timeout_seconds=60,
)
# -> job ID, status, wait result, and run summary
```

`wait_timeout_seconds` is 1–300 seconds; if the wait expires, the job keeps
running. Set the MCP client's own tool-call timeout longer than this wait
budget. For long jobs, start with `job_start(..., wake_me=True)` (or explicit
`wake_targets`) and let the wake resume the session; use `job_wait` when you
need progress or checkpoint events while the job runs.

```text
job_start(
  command="uv run python examples\\long_job.py",
  name="demo run",
  wake_me=True,
)
# -> job_<id>

job_wait(job_id="job_<id>", filters=["checkpoint"], timeout_seconds=120)
# -> returns the first checkpoint event + current status

job_wait(job_id="job_<id>", filters=["completed", "failed", "timeout", "cancelled", "orphaned"], timeout_seconds=300)
# -> returns the terminal event + exit code

# If it failed, inspect the summary and recent output:
job_run_summary(job_id="job_<id>")
job_tail(job_id="job_<id>", stream="stderr", max_bytes=8192)
```

And in a third terminal, watch it live:

```cmd
vanth-monitor
```

### Command-line entry points

| Installed command | Source checkout command | Purpose |
|---|---|---|
| `vanth` | `uv run vanth` | MCP stdio server and human CLI |
| `vanthd` | `uv run vanthd` | Background HTTP daemon |
| `vanth-monitor` | `uv run vanth-monitor` | Live terminal dashboard (Go binary, bundled in the wheel) |
| `vanth-codex-notify` | `uv run vanth-codex-notify` | Delivery adapter: reads a wake payload on stdin and dispatches it to Codex |

The MCP stdio server exits when its launching client closes stdin or dies, and
reaps itself after `VANTH_WATCH_IDLE` seconds of idle (default 1800; `0`
disables). It does not exit during a blocking tool call. `vanth doctor
--reap-orphans` cleans up orphaned MCP servers left by older versions.

### Human CLI

`vanth` provides core job operations and daemon administration from the command
line. The MCP server exposes the full agent tool surface; many advanced
delivery, coordination, metrics, and artifact operations have no direct CLI
command.
Operational CLI commands autostart the daemon on demand; bare `vanth status` is
read-only and reports `DOWN` when it is stopped. `vanth status <job-id>` inspects
a job and may start the daemon. Every flag-based command supports `--json` where
noted for scripts.

| Command | Purpose |
|---|---|
| `vanth --version` / `vanth version` | Print the installed version |
| `vanth status [<job-id>]` | Bare command: read-only daemon up/down, pid, schema, running jobs, deliveries. With a job id: inspect that job's status/exit/runtime/last event (may start daemon). (`--json`) |
| `vanth doctor` | Print the health report (human-readable; `--json` for JSON output) |
| `vanth restart` | Gracefully stop + start the daemon (jobs survive) |
| `vanth setup [opencode] [codex] [claude] [desktop] [--remove] [--yes]` | Register/unregister the MCP server in your clients' configs |
| `vanth start [options] [--] <command...>` | Start a background job without MCP. Options: `--name`, `--cwd`, `--timeout`, `--env K=V`, `--wake JSON`, `--wake-me[=EVENTS]`, `--interactive`, `--priority`, `--pool`, `--tag`, `--notes`, `--secret-env`, `--trigger JSON`, `--policy JSON`; `--` passes command flags verbatim |
| `vanth rerun <job_id>` | Rerun a job with its saved configuration (core counterpart of `job_rerun`) |
| `vanth send <job_id> [--line] [--eof] [<text|->]` | Send raw input to an interactive job; `--line` appends a newline, `-` reads stdin, and text may be omitted with `--eof` (core counterpart of `job_send`) |
| `vanth sleep <seconds>` | Start a trivial sleep job |
| `vanth emit <type> [message] [--data K=V]... [--level L]` | Print one `AGENT_EVENT` line (language-neutral event SDK) |
| `vanth list` (`ps` alias) | List jobs (`--status`, `--limit`, `--all`, `--json`); defaults to in-flight (launching/queued/running/…), `--all` shows finished jobs; running jobs show DURATION and AGE |
| `vanth deliveries [--status S] [--job JOB_ID] [--limit N] [--json]` | List wake deliveries, attempts, and last errors |
| `vanth wake <job_id> [--now] [--type T] [--events a,b] [--cwd DIR] [--config JSON] [--target JSON]` | Add a wake target to a job **after it started** (in-flight or finished). Fires on future events; `--now` surfaces a synthetic wake immediately. CLI forms of the core `job_add_wake_target` / `job_wake_now` operations |
| `vanth api` | Print a loopback HTTP route summary and authentication details |
| `vanth logs <job_id>` (`tail` alias) | Show a job's output (`--stream stdout\|stderr\|all`, `--offset`, `--max-bytes`, `--grep`, `--json`) |
| `vanth wait <job_id>` | Block until an event fires (core counterpart of `job_wait`): defaults to terminal outcomes (`completed`, `failed`, `timeout`, `cancelled`, `orphaned`); `--events` narrows the set. Also accepts `--timeout SECONDS`, `--since-event-id ID`. Exits 0 on an event, 3 on timeout |
| `vanth diff <job_id> <other>` | Compare two jobs' run specs |
| `vanth stop <job_id>` | Stop a running job (`--signal`, `--kill-after`) |
| `vanth artifacts <job_id>` | List a job's artifacts (`--limit`, `--json`) |
| `vanth prune` | Manual retention cleanup; dry-run by default (`--older-than N`, `--yes`) |
| `vanth backup [--out PATH] [--include-logs]` | Write an archive of jobs, artifacts, and events |
| `vanth restore <archive> --yes [--force]` | Restore an archive |
| `vanth remote <action>` | Pair, list, inspect, remove, or retry remote execution hosts |
| `vanth autostart enable\|disable\|status` | Daemon survives reboots (Windows Task Scheduler / macOS launchd / Linux systemd user unit) |
| `vanth help <command>` | Show command-specific help; `vanth --help` lists commands |

Run `vanth --help` for the command list. Command-specific help is available
with `vanth <command> --help` for `status`, `doctor`, `list`, `start`, `rerun`,
`send`, `logs`, `wait`, `stop`, `sleep`, `emit`, `deliveries`, `wake`, `artifacts`,
`diff`, `api`, `remote`, `backup`, `restore`, `prune`, `restart`, `setup`,
`autostart`, and `version`. Job-id arguments accept an **unambiguous prefix**;
an unknown id suggests near matches.

`vanth start --json` includes `startup_confirmed` for direct local jobs, with
the same brief confirmation as MCP `job_start`. Queued jobs return immediately.

For `vanth start`, `<command...>` begins at the first non-option argument. A
single argument is used verbatim (so a whole quoted command string works);
multiple arguments are re-quoted for the host shell, so the program and its
arguments — including empty ones — survive, and shell metacharacters in an
argument stay data (on Windows `%`, `!`, and `"` cannot be encoded safely, so
pass the whole command as one quoted string for those). Repeated `--env`,
`--wake`, `--tag`, and `--secret-env` flags are allowed;

**If the command contains shell operators (`&&`, `|`, `>`, `<`), pass it as ONE
quoted string or put the steps in a script file and start that.** Reassembling
operators from separate arguments is where shell quoting goes wrong (and the
classic PowerShell-5.1 failure mode is that a single-quoted string with inner
quotes arrives split into garbage argv). `vanth start` refuses this with exit 2:

```cmd
vanth start "cmd /c echo done && timeout /t 30 /nobreak >nul"
vanth start -- run.cmd
```

The first command passes one quoted shell command; the second starts a script.

For a trivial delay, use `vanth sleep <seconds>` rather than a shell sleep
idiom.

`--wake` takes the same wake-target JSON shape documented in
[docs/agent-tools.md](docs/agent-tools.md). The global `--json` flag is
recognized only before `--`, so a wrapped command keeps its own literal
`--json`.

Examples:

```cmd
vanth status --json
vanth list --status running --limit 20 --json
vanth logs job_abc123 --stream stderr --max-bytes 65536
vanth stop job_abc123 --signal terminate --kill-after 10
vanth artifacts job_abc123 --json
vanth prune --older-than 604800 --yes
vanth backup
vanth remote list
vanth autostart enable
```

`vanth prune --yes` performs deletion; without `--yes`, it only previews.
`vanth autostart enable` configures the daemon to start at login.

`vanth restart` is the reliable way to pick up a code/version update: it sends
the daemon a graceful shutdown over loopback, waits for the old process to
fully release the home lock, then starts a fresh daemon. In-flight jobs are
owned by detached runners, so they continue across the restart.

`vanth autostart` installs a start-at-login mechanism per platform — a Windows
Task Scheduler task, a macOS launchd agent, or a Linux systemd user unit —
then `vanth status` reports whether it is active.

---

## How it works

```
MCP client / HTTP client
        |
        v
   vanthd (localhost HTTP daemon, bearer-token auth)
        |                 |                    |
        |                 |                    +---> wake adapters
        |                 |                          (local_command / codex_thread / opencode_thread / webhook)
        |                 |
        |                 +----> jobs.sqlite (durable source of truth)
        |
        +----> vanth.runner (detached worker process)
                    |
                    +----> your command (own process group)
                              |
                              +----> stdout/stderr -> logs/ + AGENT_EVENT parsing
```

Ownership rules:

- the **runner** owns the real command, its timeout, and stream draining;
- the **daemon** owns maintenance, delivery dispatch, API requests, and recovery;
- **SQLite is the source of truth** across process restarts;
- the **MCP and HTTP clients** never need to stay alive for jobs to continue.

A job is not considered terminal until both output streams have reached EOF and
all structured events have been persisted.

### Job lifecycle

A job moves through a small set of states. Terminal states are permanent.

| State | Meaning |
|---|---|
| `running` | Workload launched; runner is streaming output and heartbeating |
| `completed` | Command exited 0, streams drained, events persisted |
| `failed` | Command exited non-zero |
| `timeout` | Command exceeded `timeout_seconds`; runner terminated it |
| `cancelled` | `job_stop` was issued and the process tree actually terminated |
| `orphaned` | Runner died unexpectedly (crash); never silently dropped |

The runner enforces `timeout_seconds` even across daemon restarts. On recovery,
a `running` job whose runner is gone is marked `cancelled` (if a stop was
requested) or `orphaned` (if not) — never left as a zombie `running` row.

---

## Installing the MCP server

`vanth` is the MCP stdio server. It talks to the daemon, starting it
automatically on first use if it is not already running.

### One-shot setup

After installing the tool, connect it to the MCP clients on your machine in a
single step:

```cmd
uv tool install vanth && vanth setup
```

`vanth setup` detects existing client configs and supported client commands on
`PATH` (OpenCode, Codex, and Claude Code), creates a missing config file for a
selected client when needed, and shows what it found. It backs up each existing
config before changing it (`.vanth-setup-<ts>.bak`), and upserts
the Vanth MCP entry — leaving every other setting and comment untouched. OpenCode
deep-merges `config.json`, `opencode.json`, and `opencode.jsonc` in that order
(later files win). Setup reads JSONC read-only (comments are stripped in memory,
never rewritten) and edits a `.jsonc` only when it has no comments to lose; it
writes at most one file (never a duplicate entry) and refuses to write a
lower-precedence file that a higher-precedence one would override. A registration
it cannot safely make is reported as a skipped client with manual instructions
rather than as success, and `--remove` clears every safely-editable
registration. `vanth status` reports the *effective* state of the deep-merged
entry: an entry with `enabled: false` is `disabled`, and a file that cannot be
parsed is `unreadable` rather than `not configured`.

```cmd
vanth setup
vanth setup --yes
vanth setup opencode codex
vanth setup --json
vanth setup --remove
```

By default setup detects clients and prompts before applying changes. `--yes`
skips prompts, client names limit the targets, `--json` prints machine-readable
output, and `--remove` unregisters Vanth.

Configs it manages:

| Client | File | Section |
|---|---|---|
| opencode | `~/.config/opencode/config.json`, `opencode.json`, or `opencode.jsonc` | `mcp.vanth` |
| Codex | `~/.codex/config.toml` | `[mcp_servers.vanth]` |
| Claude Code / Cursor | `~/.claude.json` | `mcpServers.vanth` |

Manually, the same entries are:

### opencode

Add to `~/.config/opencode/opencode.json`:

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "vanth": {
      "type": "local",
      "command": ["vanth"],
      "enabled": true,
      "timeout": 15000
    }
  }
}
```

From a source checkout, use `uv` directly instead of a bare `vanth`:

```json
{
  "mcp": {
    "vanth": {
      "type": "local",
      "command": ["uv", "run", "--directory", "/path/to/vanth", "vanth"],
      "enabled": true,
      "timeout": 15000
    }
  }
}
```

Verify the connection and tools:

```cmd
opencode mcp list
```

### Claude-style MCP clients (`mcpServers`)

Published wheel:

```json
{
  "mcpServers": {
    "vanth": { "command": "vanth", "env": { "VANTH_HOME": "C:/Users/you/.vanth" } }
  }
}
```

From a source checkout:

```json
{
  "mcpServers": {
    "vanth": {
      "command": "uv",
      "args": ["--directory", "/path/to/vanth", "run", "vanth"],
      "env": { "VANTH_HOME": "C:/Users/you/.vanth" }
    }
  }
}
```

### Configuring the daemon home

Both the MCP server and the daemon resolve the same state root from `VANTH_HOME`
(default `%USERPROFILE%\.vanth` on Windows, `~/.vanth` on Unix; `AGENT_BG_HOME`
is accepted as an alias). If both are set they must resolve to the same
directory.

---

## Instrumenting jobs with events

A job in **any language** emits structured events by printing a single line to
stdout (or stderr) that Vanth parses and the monitor charts:

```
AGENT_EVENT {"type":"metric","data":{"loss":0.42,"_step":10}}
```

This is optional — plain scripts still run and log — but it is what turns a job
into a first-class tracked object. Python jobs can use the `vanth.agent_events`
helper; every other language can shell out to `vanth emit` or print the line
directly.

### Emitting from any language

`vanth emit` is the language-neutral SDK: run it from shell, Go, Node, Rust,
Java, or anything else to print a correctly-formed event. Values are parsed as
JSON when possible, so `loss=0.42` is numeric and `stage=train` is a string.

```sh
# shell (also works from any language via subprocess)
vanth emit checkpoint "epoch done" --data epoch=10 --data val_loss=0.42
vanth emit metric --data _step=10 --data loss=0.42 --data acc=0.88
vanth emit progress --data current=10 --data total=100 --data unit=epoch --data stage=train
vanth emit log --level warning "low disk" --data free_gb=2.5
```

```go
// Go: any language that can run a subprocess is enough
exec.Command("vanth", "emit", "metric", "--data", "loss="+strconv.FormatFloat(v, 'f', 4, 64)).Run()
```

The raw wire format is just as portable: print `AGENT_EVENT ` followed by a JSON
object with a required `type` and optional `message` / `data` / `level` keys to
stdout, flushed on its own line.

### Python helper

Any Python script can emit the same events without the subprocess overhead:

```python
from vanth.agent_events import agent_event, progress

# A checkpoint: something meaningful happened.
agent_event("checkpoint", "epoch complete", epoch=10, val_loss=0.42)

# A progress update: drives the progress bar and progress.* plots.
progress(10, 100, unit="epoch", stage="train", message="10/100 epochs")

# Arbitrary scalar metrics: become their own line plots.
agent_event("metric", _step=10, loss=0.42, acc=0.88, mbps=12.4)
```

Notes:

- the helper prints `AGENT_EVENT {json}` with `flush=True` (flush matters);
- `progress(current, total, unit=..., stage=...)` computes `percent` for you
  (`vanth emit progress` does the same when `current` and `total` are present);
- `metric` payloads: numeric fields become series; `_step` (if present and
  numeric) is the x-axis, otherwise the event sequence number is used; keys
  starting with `_` other than `_step` are ignored; booleans are not metrics;
  NaN/Infinity/null values are skipped and counted in the monitor's warning
  badge;
- any other field (e.g. `file`, `stage`, `phase`) is preserved and visible in
  the exact event table.

### Example: a tracked downloader

```python
# downloader.py
import os
from vanth.agent_events import agent_event, progress

files = ["a.bin", "b.bin", "c.bin"]
total = sum(os.path.getsize(f) for f in files)
done = 0

for f in files:
    agent_event("checkpoint", f"starting {f}", file=f)
    # ... download f ...
    done += os.path.getsize(f)
    progress(done, total, unit="bytes", stage="download",
             message=f"{done}/{total} bytes")
```

### Example: an image-processing batch

```python
from vanth.agent_events import agent_event, progress

images = list(find_images("input/"))
for i, img in enumerate(images, 1):
    out = process(img)                    # resize, denoise, ...
    agent_event("metric", _step=i, sharpness=out.sharpness, size_mb=out.size_mb)
    progress(i, len(images), unit="images", stage="process", message=img.name)
```

### Timestamped, leveled logging with `loguru`

Vanth ships a loguru wrapper that routes every record into a structured
`AGENT_EVENT` log line, so logs appear as timestamped, level-aware events in
the event table (with the level badge and exact timestamps) instead of bare
text:

```python
from vanth.agent_logger import logger, log_with_context

logger.info("training started", lr=8e-5, batch_size=8)     # event type "log", level info
logger.warning("low disk", free_gb=2.5)
log_with_context("error", "failed to load checkpoint", path="best.pt")
```

Each call emits `AGENT_EVENT {"type":"log","level":"info","message":"...","data":{...}}`
which the daemon persists as a durable event. `data` carries extra context. The
monitor shows these in the exact event table alongside `metric`/`progress`
events.

---

## Tool reference (MCP tools)

| Tool | Purpose |
|---|---|
| `job_start` | Launch a command as a detached job |
| `job_start_and_wait` | Start a short local job, wait for a bounded time, and return its summary |
| `job_rerun` | Re-launch a job, optionally overriding `command`/`env`/`cwd`/`timeout_seconds`/`name`/`tags`/`notes`/`interactive` |
| `job_send` | Feed stdin to an interactive job (`interactive=True` first) |
| `job_wait` | Block until a matching event (or timeout) — the preferred way to await jobs (`return_progress` optional) |
| `job_status` | One job's status, command, env, progress, last event, linkage, tags |
| `job_status_batch` | Many jobs' status in one call (`job_ids`, `limit`) |
| `job_list` | Recent jobs, filterable by `status` / `thread_id` / `name` / `tags` |
| `job_view` | Agent-facing summaries sorted by attention priority |
| `job_events` | Structured events for a job (forward via `since_event_id`, or latest-first via `reverse`) |
| `job_tail` | Bounded stdout/stderr log tail with byte offsets (`follow`/`timeout_seconds`/`grep` optional) |
| `job_metrics_query` | Read stored scalar metric series (loss, acc, progress.percent, ...) |
| `job_metric_compare` | Compare one metric across jobs (latest/mean/min/max/sum/count) |
| `job_duration_stats` | Per-job p50/p95 duration + queue time, success rate, flaky score, slowest-N, trend |
| `job_run_summary` | One-call "did it work?" — status, runtime, progress, metrics, artifacts |
| `job_diff` | Diff the run specs of two jobs (command/env/cwd/tags/wake targets) |
| `job_artifact_add` | Attach an artifact (checkpoint, CSV, output) to a job |
| `job_artifacts` | List artifacts attached to a job |
| `job_dashboard` | Downsampled chart-data view for any renderer |
| `job_deliveries` | Wake deliveries for a job, filterable by `status` |
| `job_mark_delivery` | Manually set a delivery's status |
| `job_retry_delivery` | Requeue a failed delivery for dispatch |
| `job_delivery_attempts` | Attempt/lease history for one delivery |
| `job_stop` | Stop a running job (terminate process tree) |
| `job_pause` / `job_resume` | Hold / release a queued (pool or trigger) job |
| `job_request_decision` | Ask a human to decide something about a job; notifies its wake targets and returns a durable `decision_id` |
| `job_resolve` / `job_withdraw_decision` | Answer a pending decision with one of its options, or withdraw it |
| `job_decisions` | List decisions, filterable by `job_id` / `status` |
| `pool_configure` / `pool_list` | Per-pool `max_parallel` + pause state, and live queue depths |
| `schedule_create` | Create a cron or interval schedule that launches a job per fire |
| `schedule_list` / `schedule_update` / `schedule_delete` | Manage schedules in place |
| `schedule_next` | Preview a schedule's next N fire times |
| `job_doctor` | Daemon health, schema, tables, binary availability |
| `job_cleanup` | Dry-run or real removal of old terminal jobs |
| `daemon_wake` | Schedule a self-resume wake target — full target dict or `events`/`type`/`...config` shorthand (Python API) |

For the full parameter contract and response shapes of every tool, see
[docs/agent-tools.md](docs/agent-tools.md).

### AGENT_EVENT protocol

Jobs can emit `AGENT_EVENT <json>` lines on stdout (or stderr) to create typed
events — `progress`, `metric`, `checkpoint`, `completed`, and more — that the
daemon persists durably, the dashboard charts, and wake deliveries carry to
agents. See `vanth/agent_events.py` for the Python helpers and
`vanth/agent_logger.py` for the loguru integration.

### job_start_and_wait — start and collect a bounded result

```text
job_start_and_wait(
  command="python -m compileall -q src",
  wait_timeout_seconds=60,
)
```

This convenience tool combines `job_start`, a bounded `job_wait`, and
`job_run_summary`. It returns the job ID and status plus `wait` and `summary`.
Parameters: `command`, `cwd`, `name`, `env`,
`timeout_seconds`, `wait_timeout_seconds` (default 20; 1–300), `tags`, `notes`,
and `secret_env`. It returns `job_id`, `status`, `wait`, and `summary` (or a
start error). The summary includes a bounded `stderr_excerpt` of up to 2048
bytes. If the wait expires, the job keeps running. Set the MCP client's own
tool-call timeout longer than `wait_timeout_seconds`. Use it for short local
commands whose result is useful immediately. For work that may outlast the
wait bound, use `job_start` with `wake_me=True` or `wake_targets`; for
intermediate progress or checkpoints, use `job_start` followed by `job_wait`.

### job_start

```text
job_start(
  command="uv run python examples\\long_job.py",
  name="training run",
  cwd="F:\\git\\project",            # optional
  env={"CUDA_VISIBLE_DEVICES": "0"}, # optional
  timeout_seconds=3600,              # optional; None = no timeout
  interactive=True,                  # optional; open stdin for job_send
  notify_on=["progress","checkpoint","failed","completed"],
  origin_thread_id="019f...",        # the agent thread that launched it
  tags=["training","gpu"],           # optional
  secret_env=["HF_TOKEN"],           # optional; mask these env values in logs/events
  wake_targets=[...],                # optional, see below
  trigger={"job_id": "job_A", "status": "completed"}  # optional DAG: start after job_A completes
)
```

Returns `job_id`, `status`, `worker_pid`, and the log/event paths. With
`trigger` set, the job is created `queued` and starts automatically when the
parent job reaches that status (or is `cancelled` if the parent ends
differently).

### Readiness triggers — wait for a condition, not just a job

A `trigger` may also carry a **readiness probe**; when the DAG gate and the
probe are both present they are ANDed. The job stays `queued` until the probe
passes (or is `cancelled` once an optional `timeout_seconds` elapses):

```text
job_start(command="migrate.sh", trigger={"probe": {"type": "port",
           "host": "127.0.0.1", "port": 5432, "timeout_seconds": 120}})

# probe types
{"probe": {"type": "port",     "host": "127.0.0.1", "port": 5432}}
{"probe": {"type": "http",     "url": "http://127.0.0.1:8080/health", "expect_status": 200}}
{"probe": {"type": "log_line", "job_id": "job_B", "pattern": "ready", "stream": "stdout"}}
{"probe": {"type": "file",     "path": "/tmp/ready"}}
```

This lets one job orchestrate a stack — "start the DB, wait until the port
accepts, then migrate" — instead of `sleep` hacks. Probes run on the daemon host
(direct connection; no proxy) at `interval_seconds` cadence (default 1s), bounded
per dispatcher pass so blocked probes can't stall other work. `timeout_seconds`
is measured from when the dependency gate is satisfied (or from queue creation
with no dependency gate); a missed deadline is attributed (`actor="daemon"` +
reason) on the `cancelled` event.

### job_send — feed stdin to an interactive job

```text
job_send(job_id="job_...", input="y", eof=False)
```

Appends `input` to a running job's stdin. Start the job with `interactive=True`
first. `eof=True` closes the job's stdin (the child sees EOF). Non-blocking:
returns immediately; the input is queued to the runner. Rejects jobs that are
not interactive, not running, or unknown. `job_rerun` preserves the
`interactive` flag.

### job_request_decision — ask a human and wait durably

```text
job_request_decision(job_id="job_...", prompt="Ship the release?",
                     options=["approve", "deny"], timeout_seconds=3600)
job_wait(job_id="job_...", filters=["decision_resolved"])   # await the answer
job_resolve(job_id="job_...", token="dec_...", choice="approve")
```

Records a durable "needs a decision" request against a non-terminal job and
notifies the job's wake targets, so the owning thread learns a human is needed.
The job keeps running (its `status` is untouched) — the answer arrives as a
`decision_resolved` event. `timeout_seconds` expires the request (emitting
`decision_expired`); `job_withdraw_decision` cancels it; `job_decisions` lists
pending/answered requests.

### job_status — see what a job is running

```text
job_status(job_id="job_...")
```

Returns status, **command**, **cwd**, **env**, **timeout_seconds**, **notes**,
**run** (author, hostname, OS, toolchain, CPU/GPU, git repo/branch/commit),
**runtime_seconds**, progress, last event, thread linkage, tags, and exit code.
This is the fastest way for an agent to answer "what is this job doing?" — and
mirrors the run-overview you'd see for a run in W&B.

Pass `notes="..."` to `job_start` to annotate a run ("what makes this run
special?"), which is preserved on `job_rerun` and shown in the monitor.

### job_rerun — relaunch a failed job

```text
job_rerun(job_id="job_...")
```

Re-launches the job with its **original command, cwd, env, timeout, name, tags,
origin thread, and wake targets** — a new `job_id` is returned. Use it to retry
a failed download, flaky processing batch, or transient failure without
reconstructing the request.

### job_list — filter by name or tag

```text
job_list(status=["running"], name="train", tags=["gpu"], limit=20)
```

Filters: `status` (list), `thread_id`, `name` (substring), `tags` (must contain
all listed tags).

### job_events — forward or latest-first

```text
job_events(job_id="job_...", since_event_id="evt_...", limit=20)      # events after the cursor
job_events(job_id="job_...", reverse=true, limit=20)                   # the 20 newest events, newest first
```

`reverse: true` returns the most recent events (newest first) — ideal for "what
happened recently?" — and can be combined with `since_event_id` to page
backward.

### job_wait — the heart of agent usage

```text
job_wait(job_id="job_...", filters=["checkpoint","completed","failed","timeout","cancelled","orphaned"], timeout_seconds=3600)
```

- waits for the **first event matching any filter**, returning it with the
  current status;
- pass `since_event_id` to wait only for events newer than one you already saw;
- on timeout returns `result: "timeout"`; on daemon shutdown returns
  `result: "shutdown"`.

### job_view — what to show the user

```text
job_view(thread_id="019f...", limit=20)
```

Returns compact summaries sorted by attention priority: running and failed jobs
first, then jobs with pending/failed deliveries, then everything else. Each
entry includes status, progress, the latest event, thread linkage, tags, and
delivery counts.

### job_stop — stop a running job

```text
job_stop(job_id="job_...", signal="terminate", kill_after_seconds=10, reason="superseded by run #2")
```

Terminates the job's process tree. A graceful `signal` (default `terminate`) is
sent first; if the job has not exited within `kill_after_seconds`, it is killed.
The job becomes `cancelled` only after the workload tree actually terminated;
otherwise it stays `running` and the stop is retryable.

**Kill attribution.** The `cancelled` event carries
`data={"actor": ..., "reason": ...}` so a stop is never an unattributable
"killed". Actors are `tool` (an MCP call), `user` (`vanth stop`, or a human
HTTP call), `watchdog` (recovery / heartbeat reconciliation), and `timeout`
(the runner's timeout); `job_status` exposes the persisted `stop_actor` /
`stop_reason`. `vanth stop <id> --reason "..."` sets the user reason.

### secret_env — mask declared secrets in captured output

```text
job_start(command="python train.py", env={"HF_TOKEN": "hf_..."}, secret_env=["HF_TOKEN"])
```

Every value named in `secret_env` is replaced with `***` before it is written to
the job's captured stdout/stderr logs or parsed into structured events (the
GitHub Actions `::add-mask::` pattern), so masked output never leaks through
logs, events, deliveries, or the monitor. Note this protects *emitted output*:
as with any `env` value, a declared secret is still stored in the job's
environment in the owner-only `jobs.sqlite` (the single-user state directory is
protected by owner-only permissions). Masking applies to local jobs (remote jobs
ignore `secret_env`).

### job_mark_delivery / job_retry_delivery — manual delivery control

```text
job_mark_delivery(delivery_id="del_...", status="delivered", error="optional reason")
job_retry_delivery(delivery_id="del_...")   # requeue a failed delivery
```

`job_mark_delivery` sets a delivery's status by hand (e.g. after resolving an
adapter problem); `job_retry_delivery` requeues a failed one for the next
dispatch pass. `job_delivery_attempts` shows the claim/lease history.

### job_cleanup — remove old terminal jobs

```text
job_cleanup(older_than_seconds=86400, dry_run=true)   # preview
job_cleanup(older_than_seconds=86400, dry_run=false)  # delete
```

Removes terminal jobs older than the cutoff: logs, event mirrors, specs,
deliveries, attempts, wake targets, events, stdin channels, then the job row.
Running jobs are never selected. Dry-run is fully read-only. Cleanup is safe
to repeat.

**Automatic retention**: set `VANTH_RETENTION_SECONDS` on the daemon to purge
old terminal jobs in the background (polled every
`VANTH_RETENTION_INTERVAL_SECONDS`, default 3600; safe by default —
`VANTH_RETENTION_DRY_RUN=0` to actually delete).

**Concurrency quota**: set `VANTH_MAX_RUNNING_JOBS` (default `0` = unlimited)
to cap how many jobs may run at once; `job_start` returns a clear error when
the cap is reached.

### job_metrics_query — read stored scalar series

```text
job_metrics_query(job_id="job_...", metric="loss", from_ms=..., to_ms=..., limit=1000)
```

Returns the stored series for one job, grouped by metric name. `metric`
filters to a single series (e.g. `loss`, `acc`, `progress.percent`);
`from_ms`/`to_ms` filter by event timestamp (epoch milliseconds). Points are
ordered by event sequence. This is the read side of the terminal monitor's
data.

### job_metric_compare — compare a metric across runs

```text
job_metric_compare(job_ids=["job_a", "job_b"], metric="val_loss", aggregation="min")
```

Compares one metric across jobs (e.g. val_loss across seeds or configs).
`aggregation` is `latest`, `mean`, `min`, `max`, `sum`, or `count`; the result
includes the per-job value plus the first/last points. This is the W&B-style
"which run won?" primitive.

### job_duration_stats — did it get slower?

```text
job_duration_stats(name="nightly backup", tags=["prod"], slowest=10)
```

Groups terminal runs by logical job (`name`, falling back to the command) and
returns p50/p95 runtime and queue time, success rate, a **flaky score** (a
failed run that has a success both before and after it — real intermittency,
not a first-attempt failure), each group's slowest recent runs, and a
`trend` flag (`regressing` / `stable` / `improving`). The trend compares the
newer half's p50 against the older half's, so it catches "this backup crept
40min → 2h over 6 weeks". The top-level `slowest` list is the slowest-N runs
across all groups.

### job_run_summary — did it work?

```text
job_run_summary(job_id="job_...", include_stderr_excerpt=True)
```

One call returns status, name, runtime, exit code, latest progress, notes,
per-metric overview (latest/first/min/max/count), and attached artifacts — the
fastest way for an agent to report on a finished job. The optional
`include_stderr_excerpt` flag adds `stderr_excerpt` (up to 2048 bytes); it is
omitted by default.

### job_artifact_add / job_artifacts — attach outputs

```text
job_artifact_add(job_id="job_...", name="best.pt", uri="file:///...", kind="checkpoint",
                 size_bytes=..., sha256="...", meta={"epoch": 5})
job_artifacts(job_id="job_...")
```

Attach artifacts (checkpoints, CSVs, rendered outputs) to a job so they are
listed in `job_run_summary` and retrievable later. `meta` is free-form JSON.

### job_dashboard — chart data for any renderer

```text
job_dashboard(job_ids=["job_..."], limit=5000)
```

Returns the job list plus every stored metric series, downsampled to `limit`
points per series — the same data the Go terminal monitor charts, exposed over
HTTP/MCP so any client (a future web/cloud dashboard) can render it.

---

## Schedules and queues

Vanth has no external scheduler process: the daemon's existing 0.2s maintenance
loop fires schedules and launches queued jobs, and everything lives in
`jobs.sqlite`.

### Schedules (cron or interval)

A schedule launches a **fresh job per fire** (optionally masked with
`secret_env`, tagged `scheduled`, and linked by `job_status`'s `schedule_id`):

```text
schedule_create(name="nightly backup", command="backup.sh",
                cron="0 3 * * *", timezone_name="America/New_York",
                overlap="skip")          # cron: 5 fields or @daily/@hourly/...
schedule_create(name="poll", command="poll.sh", interval_seconds=300)
schedule_next(schedule_id="sched_...", count=5)   # preview fire times
schedule_update(schedule_id="sched_...", changes={"cron": "0 4 * * *"})
schedule_update(schedule_id="sched_...", changes={"enabled": False})  # pause
schedule_list()
schedule_delete(schedule_id="sched_...")
```

- **Cron** is 5-field (`minute hour day-of-month month day-of-week`), numeric
  values with `*`, ranges (`1-5`), lists (`1,13`), and steps (`*/15`), plus the
  `@hourly`/`@daily`/`@weekly`/`@monthly`/`@yearly` shorthands.
- **Timezones** are IANA names, matched against the local wall clock. DST is
  handled by construction: a nonexistent local time (spring forward) is skipped;
  an ambiguous one (fall back) matches once per UTC minute that maps to it.
  UTC needs no timezone database; named zones use the OS database on Linux/macOS
  and the bundled `tzdata` package on Windows.
- **`overlap`**: `skip` (default) skips the fire (advancing to the next) while a
  job from the same schedule is still active; `allow` always launches.
- Missed fires while the daemon was down are **not** backfilled — the schedule
  resumes at the next future match (the dead-man's-switch policy already alerts
  on missed runs).

### Queues: pools, priority, pause

Start a job into a named pool instead of launching it immediately:

```text
pool_configure(pool="gpu", max_parallel=1)     # 0 = unlimited
pool_list()                                    # queued/running per pool
job_start(command="train.py", pool="gpu", priority=5)
job_pause(job_id="job_...")                    # hold a queued job
job_resume(job_id="job_...")
pool_configure(pool="gpu", paused=True)        # hold the whole pool
```

Queued jobs (pool, trigger, or both) launch from the one dispatcher ordered by
`priority` (higher first), oldest first, once the trigger is satisfied, the pool
is not paused and is under `max_parallel`, and the global `VANTH_MAX_RUNNING_JOBS`
quota allows. Pausing affects queued jobs only; running jobs are untouched.

---

## Wake targets (wake an agent when a job needs attention)

When a job emits a matching event, the daemon creates a durable delivery and
dispatches it through the adapter. Delivery is **at-least-once**; every payload
carries a `delivery_id` for deduplication.

Targets are not fixed at start time. `POST /jobs/{id}/wake` (MCP
`job_add_wake_target`, CLI `vanth wake`) registers a target on a job that is
already running or finished — it fires on events **after** registration.
`POST /jobs/{id}/wake-now` (MCP `job_wake_now`, CLI `vanth wake --now`)
registers the target AND enqueues a synthetic `"wake_now"` delivery at once, so
a wake reaches the session even if the triggering event already fired; it never
fabricates a `"completed"`/`"failed"` event.

**Thread identity.** `codex_cli_thread` / `codex_desktop` targets omit the id and
inherit the calling task's (`CODEX_THREAD_ID` / `VANTH_CODEX_DESKTOP_THREAD`);
an explicit `thread_id` always wins. `opencode_thread` does **not** inherit the
calling session (OpenCode never injects `OPENCODE_SESSION_ID` into MCP
subprocesses): it resolves the session from the newest registered plugin relay
for the job's `cwd` (see `vanth doctor`). Omitting `session_id` therefore works
whenever a relay is registered for that cwd; if none is, target creation fails
with an actionable error. Use `vanth doctor --json` for the full relay/session
list. A target naming an explicit session can be resumed by
a subprocess (`opencode run --session`); the plugin relay is what lands the
prompt in the TUI you are watching.

### local_command

Runs an arbitrary command, passing the delivery payload as JSON on stdin:

```json
{
  "type": "local_command",
  "events": ["checkpoint", "failed", "completed"],
  "command": ["python", "deliver.py"]
}
```

Exit 0 marks the delivery `delivered`; any other exit marks it `failed`.

### codex_thread / codex_cli_thread

Resumes a Codex thread through the local app-server (for an unloaded CLI task):

```json
{
  "type": "codex_thread",
  "thread_id": "019f...",
  "events": ["checkpoint", "failed", "completed"],
  "codex_command": ["C:\\codex\\codex.exe"]
}
```

Protocol: `initialize -> thread/resume -> turn/start`.

The target thread must already have had at least one turn (a persisted
"rollout"). Resuming a brand-new, zero-turn thread fails with
`no rollout found for thread id <id>` — the intended wake target is an
existing/active conversation, not a never-started one.

### codex_desktop

Wakes a RUNNING Codex Desktop task through the native app-tools host pipe
(`codex_app/send_message_to_thread` on `CODEX_APP_TOOLS_PIPE_PATH`). This never
spawns a second app-server and never falls back to the CLI thread bridge:

```json
{
  "type": "codex_desktop",
  "thread_id": "019f...",
  "events": ["checkpoint", "failed", "completed"]
}
```

Desktop wake is delivered by a client-side relay: the Vanth MCP integration
registers the task id it can wake, long-polls the daemon for due deliveries,
submits the follow-up into the already-running Desktop task through the pipe,
and acknowledges only after admission succeeds. The private pipe stays inside
the Codex MCP process and is provisioned through a supported handoff — run
`vanth setup desktop` inside a Codex Desktop session with the app-tools
capability active (it writes a per-home `codex_desktop.json` capability file),
or launch Vanth with `VANTH_CODEX_DESKTOP_PIPE` /
`VANTH_CODEX_DESKTOP_THREAD` set. Without a pipe capability the delivery fails
closed with an actionable "Desktop integration unavailable" error and is never
routed to the CLI.

> **Experimental.** The private host-pipe contract is not documented by official
> Codex material. This integration is scoped to ONE provisioned task per Desktop
> lifetime: one per-home `codex_desktop.json` stores one pipe/thread tuple,
> provisioning a second Desktop task overwrites the first, and a Desktop restart
> invalidates the private pipe. A stale capability (older than 24h) is detected
> and fails closed with a diagnostic asking you to re-run `vanth setup desktop`
> inside an active Desktop session. If Desktop restarts mid-operation, the relay
> reloads a re-provisioned capability and retries the pending wake; otherwise
> the wake is released back to pending (never terminally consumed) until you
> re-provision. Automatic or durable multi-task Desktop wake is NOT supported
> yet. The task must still be running in the current Desktop host lifetime;
> arbitrary historical/unloaded Desktop threads are not supported. Live tests
> found that the private host can accept sends to some such threads while
> producing no usable turn, so admission alone is not a delivery guarantee.

### opencode_thread

Resumes an OpenCode session:

```json
{
  "type": "opencode_thread",
  "session_id": "ses_...",              # optional if a plugin is registered
  "events": ["checkpoint", "failed", "completed"],
  "cwd": "F:\\git\\project",
  "opencode_command": ["opencode"],     # override the binary (attach path only)
  "attach": "http://127.0.0.1:4096",    # only for an `opencode serve` instance
  "timeout_seconds": 120
}
```

**A plain `opencode` TUI is woken by the plugin relay, not by `attach`.** A TUI
binds no TCP port and injects no session id, so there is no server URL to attach
to and an external `opencode run --session` writes to a backend the visible
session never sees. `vanth setup` therefore installs a small plugin
(`~/.config/opencode/plugins/vanth.ts`); it registers the session it lives in
with the daemon and injects a wake prompt through its own in-process client, so
the prompt lands in the session you are watching. Restart opencode once after
installing it.

With the plugin loaded, `vanth start`/`job_start` can name just
`{"type": "opencode_thread", "events": [...]}`: Vanth resolves the session from
the newest relay registered for the job's `cwd` (an explicit `session_id` always wins).
`vanth doctor` summarizes registered relays and their liveness;
`vanth doctor --json` includes the full relay/session list. If none is `live`,
`opencode_thread` wakes cannot be delivered. `attach` remains supported for
headless `opencode serve` deployments and takes the direct-subprocess path.

The default OpenCode turn timeout is 30 seconds; raise it for long turns.

On Windows, Vanth resolves the standard npm `opencode.cmd` shim to the native
`opencode.exe` shipped in the same package. This avoids `cmd.exe` truncating a
multiline wake prompt to its first line. Explicit/nonstandard batch shims remain
supported; Vanth flattens their prompt line breaks so all wake fields arrive.

Before dispatching to a plain (non-`attach`) session, Vanth runs a cheap
`opencode session list` probe to confirm the session still exists — a
confirmed-missing session fails fast (dead-lettered immediately, no retry
burn) with `opencode session not found: <id>`. The probe never blocks a valid
dispatch; on any ambiguity it proceeds. Opt out per-target with
`"skip_probe": true` or globally with `VANTH_OPENCODE_SKIP_PROBE=1`.

### webhook

POSTs the delivery payload as JSON to any HTTP(S) endpoint — a generic
channel that covers ntfy, Gotify, Telegram bots, Slack/Discord webhooks,
PagerDuty Events, and more:

```json
{
  "type": "webhook",
  "url": "https://hooks.slack.com/services/...",
  "events": ["failed", "completed"],
  "headers": { "Authorization": "Bearer <token>" },
  "timeout_seconds": 10
}
```

The payload is the same delivery payload every adapter receives (`event`,
`prompt`, `delivery_id`, `target`), POSTed with `Content-Type: application/json`.
2xx responses (200/201/202/204) mark the delivery `delivered`; anything else
is a failed delivery (retried per `max_attempts`/`retry_delay_seconds`, then
dead-lettered). `headers` lets you add auth tokens or presets for specific
services.

### Shared delivery options

```json
{
  "type": "codex_thread",
  "thread_id": "019f...",
  "events": ["checkpoint"],
  "auto_dispatch": false,      // leave the delivery pending for manual inspection
  "max_attempts": 3,           // default 1
  "retry_delay_seconds": 5,    // default 5
  "timeout_seconds": 30        // adapter timeout; also sizes the delivery lease
}
```

With `auto_dispatch: false`, deliveries stay `pending` until an agent either
dispatches them manually or changes the target.

A target that omits `events` inherits the job's top-level `notify_on` list.
`notify_on` is not a notification switch: it only supplies default `events` for
an already-supplied `wake_targets` entry. Without `wake_targets` it notifies
nobody — the job still starts, and the start response carries a `warnings`
entry saying so. An explicit target `events` always wins:

```json
job_start(command="...", notify_on=["checkpoint","failed"],
          wake_targets=[{"type": "local_command", "command": ["deliver.py"]}])
```

Concurrent adapter dispatches are capped (default 4) so a burst of events
doesn't spawn unlimited adapter processes; excess deliveries stay queued and
are picked up on the next dispatch pass. Set `VANTH_DELIVERY_MAX_CONCURRENT`
to tune.

### Delivery operations

```text
job_deliveries(job_id="job_...")
job_delivery_attempts(delivery_id="del_...")
job_retry_delivery(delivery_id="del_...")     # requeue a failed OR retrying delivery
job_mark_delivery(delivery_id="del_...", status="delivered")
```

Attempt history records the claim token, start/end times, status, and whether
the attempt was reclaimed after an expired lease. If the daemon crashes after an
adapter accepts a wake but before Vanth records success, the delivery is
reclaimed and retried — surfaced as a `reclaimed` attempt rather than claimed as
exactly-once delivery.

`job_retry_delivery` requeues a delivery immediately for dispatch — including
one that is currently `retrying` on backoff (resets `next_attempt_at`). If a
delivery has exhausted `max_attempts`, it is dead-lettered: `vanth doctor`
reports `dead_letter_count` and the most recent `dead_lettered` deliveries
(each with `delivery_id`, `job_id`, `attempts`, `last_error`) so you can see
which wakes were never delivered and why.

---

## Running the daemon

Foreground (for development or diagnosis):

```cmd
uv run vanthd
```

Start-at-login options:

- **Windows**: the daemon is started from the user Startup folder
  (`startup_commands.bat`) alongside other startup commands; a Task Scheduler
  action template is also in `deploy/vanthd.cmd`.
- **Unix**: `deploy/vanthd.service` is a systemd user service.

Enable only one daemon per `VANTH_HOME`. A second daemon for the same home
exits immediately (OS-level lock). The daemon binds only to loopback
(`127.0.0.1` / `::1` / `localhost`); a non-loopback `VANTH_DAEMON_HOST` is
rejected.

### Security

- Every data route requires `Authorization: Bearer <token>`; the token is
  generated per home and never logged. `GET /health` is the only
  unauthenticated route (a cheap liveness probe for supervisors).
- On daemon start the state directory is re-tightened to the owner: Unix
  `chmod 0700`/`0600`; Windows disables ACL inheritance and grants only the
  owner, SYSTEM, and Administrators via `icacls`. This blocks other accounts
  (e.g. sandbox/CI users that inherit read from the user profile) from reading
  the token or per-job env/spec data.
- On Windows, socket `SO_REUSEADDR` is disabled so a second daemon cannot
  become a phantom listener on the same port; a failed bind releases the home
  lock and exits cleanly.

### The Go terminal monitor

The native Go dashboard reads the same home **read-only** and renders live
plots, progress bars, the exact event table, and log tails:

```cmd
vanth-monitor
```

Published platform wheels include the native Go binary, so `vanth-monitor`
runs without a Go toolchain. In a source checkout on Windows, build it once
from the repo root using `cmd.exe` (requires Go on `PATH`), then run the
generated executable:

```cmd
mkdir dist 2>nul & go build -o dist\vanth-monitor.exe .\cmd\vanth && dist\vanth-monitor.exe monitor
```

The source checkout's `vanth-monitor` wrapper does not build or discover that
output automatically; run the executable produced by `go build` directly.

Keys: `up/down` or `j/k` select jobs · `enter` pins a job's series · `e` event
table · `l` log tail · `s` slowest-runs table · `+`/`-` zoom a chart · `[`/`]`
pan · `t` back to live tail · `?` help · `q` or `Ctrl+C` quit.

---

## Configuration reference

Environment variables (defaults live in `src/vanth/server.py`,
`src/vanth/daemon.py`, `src/vanth/migrations.py`):

| Variable | Default | Purpose |
|---|---|---|
| `VANTH_HOME` | `~/.vanth` | State root (alias: `AGENT_BG_HOME`) |
| `VANTH_DAEMON_URL` | `http://127.0.0.1:8765` | Where clients reach the daemon |
| `VANTH_DAEMON_HOST` | `127.0.0.1` | Bind address (loopback only) |
| `VANTH_DAEMON_PORT` | `8765` | Bind port |
| `VANTH_CLIENT_TIMEOUT` | `30s` | Client socket timeout; `<=0` disables (long polls use their own budget) |
| `VANTH_REQUEST_TIMEOUT` | `30s` | Daemon blocking socket read/write timeout |
| `VANTH_MAX_REQUEST_BYTES` | `1 MiB` | HTTP request body cap |
| `VANTH_MAX_RESPONSE_BYTES` | `4 MiB` | HTTP response cap |
| `VANTH_MAX_EVENT_BYTES` | `64 KiB` | Single event payload cap |
| `VANTH_MAX_EVENT_LINE_BYTES` | `1 MiB` | AGENT_EVENT line cap |
| `VANTH_MAX_LOG_BYTES` | `10 MiB` | Per-stream log cap (drain continues) |
| `VANTH_MAX_EVENTS_PER_JOB` | `100000` | Structured event cap per job |
| `VANTH_DELIVERY_POLL_INTERVAL` | `0.2s` | Maintenance loop cadence |
| `VANTH_DELIVERY_LEASE_MARGIN` | `5s` | Extra lease time beyond adapter timeout |
| `VANTH_RUNNER_HEARTBEAT_INTERVAL` | `1s` | Runner liveness heartbeat |
| `VANTH_RUNNER_HEARTBEAT_STALE_AFTER` | `10s` | Heartbeat staleness threshold |
| `VANTH_CODEX_BIN` | `codex` / `C:\codex\codex.exe` | Codex binary |
| `VANTH_OPENCODE_BIN` | `opencode` (via `shutil.which`) | OpenCode binary |
| `VANTH_LOG_LEVEL` | `INFO` | Daemon log level |
| `VANTH_LOG_MAX_BYTES` | `5 MiB` | Rotating daemon log size |
| `VANTH_LOG_BACKUP_COUNT` | `3` | Daemon log rotation count |
| `VANTH_BUSY_TIMEOUT_MS` | `30000` | SQLite write-lock wait |

Key knobs in one glance:

| Variable | Purpose |
|---|---|
| `VANTH_HOME` | State root (alias: `AGENT_BG_HOME`) |
| `VANTH_DAEMON_HOST` / `VANTH_DAEMON_PORT` | Where the daemon binds (loopback only; default `127.0.0.1:8765`) |
| `VANTH_MAX_RUNNING_JOBS` | Concurrency quota; `0` = unlimited |
| `VANTH_RETENTION_SECONDS` / `VANTH_RETENTION_INTERVAL` / `VANTH_RETENTION_DRY_RUN` | Automatic background retention of old terminal jobs (dry-run by default) |
| `VANTH_NO_SETUP_HINT` | Suppress the stderr "MCP server not configured" hint |
| `VANTH_OPENCODE_SKIP_PROBE` | Skip the `opencode session list` probe before dispatch |
| `VANTH_DELIVERY_MAX_CONCURRENT` | Cap on concurrent adapter dispatches (default 4) |
| `VANTH_MAX_REQUEST_BYTES` | HTTP request body cap (default 1 MiB) |
| `VANTH_PROBE_BUDGET` | Max readiness-probe I/O calls per dispatcher pass (default 8) |
| `VANTH_OUTBOUND_ALLOW` | Strict allowlist of host/ip/cidr for webhooks + http probes (link-local/metadata always denied) |
| `VANTH_OUTBOUND_BLOCK_PRIVATE` | `1` also denies loopback + private destinations |
| `VANTH_ALERT_WEBHOOK` | Edge-triggered operator alert destination |
| `VANTH_ALERT_DISK_FREE_BYTES` | Free-disk threshold that raises a `disk_low` alert |
| `VANTH_ALERT_INTERVAL` | Alert evaluation cadence in seconds (default 30) |

---

## Operations

### State layout

```
~/.vanth/
  jobs.sqlite      durable jobs (incl. env, notes, run-overview) / events / deliveries / targets / attempts / tombstones
  artifacts.sqlite managed-artifact catalog (separate DB)
  artifacts-store/ content-addressed artifact blobs (+ staging)
  remote.sqlite    remote-host pairing + transfer journals (when remote is used)
  token            bearer token (owner-only permissions)
  daemon.lock      single-daemon OS lock
  daemon.json      discovery metadata (url, pid, started_at, schema, auth, token_path) — written atomically, removed on graceful shutdown
  logs/            daemon.log + per-job runner/stdout/stderr logs
  events/          per-job JSONL event mirrors (monitor fallback source)
  specs/           per-job launch specs (removed once the runner starts)
  backups/         pre-migration snapshots AND `vanth backup` archives
```

`vanth backup` archives `jobs.sqlite`, `artifacts.sqlite`, `remote.sqlite`, the `artifacts-store/`
blobs and the `events/` mirrors into one verified zip (`manifest.json` with a
SHA-256 per file); `vanth restore <archive> --yes` verifies, snapshots the
current state, and swaps it back while holding the home lock. Restore refuses a
live daemon or detached job even with `--force`, and aborts if its safety backup
fails. A backup from a newer schema requires `--force`.

Archive entries are hashed from the exact bytes written to the ZIP; live
append-only files are bounded to their size at open. Restore removes stale
SQLite WAL/SHM sidecars before replacing database snapshots.

Production checks include the chaos matrix and `uv run python scripts/soak.py --duration 60`;
the sustained harness verifies events, sequences, runner exits,
latency, and RSS in isolated state. CI and releases run these checks across all
three platforms. Manual CI supports a ten-minute soak; real SSH testing is
opt-in against a disposable host ([setup](docs/real-ssh-validation.md)).
`vanth doctor --verify-artifacts` adds a bounded artifact integrity check;
inspect `artifact_integrity.complete` to distinguish a full scan from a partial one.

### Health, readiness, and diagnosis

```text
job_doctor()
```

Reports the state directory, database tables, delivery counts by status, schema
version, `PRAGMA quick_check`, stale delivery leases, free disk, token path, and
whether the Codex/OpenCode binaries resolve. It never reveals the token.

The HTTP daemon also exposes:

- `GET /health` — cheap, unauthenticated liveness probe for supervisors;
- `GET /ready` — authenticated readiness (doctor report; 503 when not ok);
- `GET /ready-fast` — cheap authenticated readiness (home + schema only;
  what per-command `ensure()` checks instead of full `/doctor`);
- `GET /metrics` — authenticated Prometheus text exposition (jobs by status,
  running/queued, pools, deliveries, dead letters, stale leases, disk/db size,
  schema, maintenance aliveness).

### Alerting

Set `VANTH_ALERT_WEBHOOK` to receive **edge-triggered** operational alerts (one
POST per state change, not per tick): the dead-letter queue becoming non-empty,
and free disk crossing `VANTH_ALERT_DISK_FREE_BYTES`. Destinations go through the
same outbound policy as webhooks. The payload is
`{type, condition, active, severity, message, details, at}`.

### Upgrades and backups

Schema changes are ordered SQLite migrations. Before the first migration of an
existing database, a timestamped backup is written under `backups/` via
SQLite's backup API (never a raw file copy while WAL is active). A future
database schema is rejected without touching the files.

For a full off-host copy, `vanth backup` writes one verified archive of every
durable store (see State layout); `vanth restore <archive> --yes` puts it back
after snapshotting the current state. Run `vanth backup` while the daemon is up
(SQLite online backup), but stop the daemon before `vanth restore`.

---

## HTTP API

The loopback API base URL is the `url` in `<VANTH_HOME>/daemon.json` and the
token is stored at `<VANTH_HOME>/token`. Every route except `GET /health` needs
`Authorization: Bearer <token>`. The routes below are a selected index, not a
complete list of MCP tools or HTTP routes. Run `vanth api` for a route summary;
see [docs/agent-tools.md](docs/agent-tools.md) for the full MCP
agent surface. Some MCP tools have no corresponding CLI command or one-to-one
HTTP route.

| Method | Path | Purpose |
|---|---|---|
| GET | `/metrics` | Prometheus text exposition |
| GET | `/jobs` | List jobs (`status`, `limit`, `thread_id`, `name`, `tags`); includes lifecycle timestamps, exit code, and runtime |
| POST | `/jobs` | Start a job |
| POST | `/jobs/{id}/rerun` | Rerun a job with its original configuration |
| POST | `/jobs/{id}/wake` | Register a wake target for the job's FUTURE events, after the job started (`target`) |
| POST | `/jobs/{id}/wake-now` | Register a wake target and surface a synthetic wake immediately (`target`) |
| GET | `/jobs/{id}/status` | Job status (includes command/env/cwd) |
| GET | `/jobs/{id}/events` | Events (`since_event_id`, `types`, `limit`, `reverse`) |
| GET | `/jobs/{id}/metrics` | Metric series (`metric`, `from_ms`, `to_ms`, `limit`) |
| GET | `/jobs/{id}/summary` | Run summary (status, runtime, metrics, artifacts) |
| GET | `/jobs/{id}/artifacts` | Artifacts (`limit`) |
| POST | `/jobs/{id}/artifacts` | Add an artifact |
| GET | `/metrics/compare` | Compare metric across jobs (`job_ids`, `metric`, `aggregation`) |
| GET | `/analytics/durations` | Duration/flakiness analytics (`name`, `tags`, `limit`, `since_ms`, `slowest`) |
| GET | `/dashboard` | Chart data (`job_ids`, `limit`) |
| GET | `/jobs/{id}/tail` | Log tail (`stream`, `max_bytes`, `offset`) |
| POST | `/jobs/{id}/wait` | Wait for an event |
| POST | `/jobs/{id}/stop` | Stop a job |
| POST | `/jobs/{id}/send` | Send stdin to an interactive job |
| POST | `/jobs/{id}/pause` / `/resume` | Hold / release a queued job |
| GET | `/schedules` | List schedules |
| POST | `/schedules` | Create a schedule |
| POST | `/schedules/{id}/update` / `/delete` | Edit in place / delete |
| GET | `/schedules/{id}/next` | Next fire times (`count`) |
| GET | `/pools` | List pools with queue depths |
| POST | `/pools` | Configure a pool (`pool`, `max_parallel`, `paused`) |
| GET | `/view` | Agent view (`thread_id`, `limit`) |
| GET | `/deliveries` | Deliveries (`job_id`, `status`, `limit`) |
| GET | `/deliveries/{id}/attempts` | Attempt history |
| POST | `/deliveries/{id}/mark` | Mark a delivery |
| POST | `/deliveries/{id}/retry` | Retry a delivery |
| POST | `/deliveries/clear` | Preview or drain matching wake deliveries (`dry_run`, filters, and limit) |
| POST | `/cleanup` | Cleanup (`older_than_seconds`, `dry_run`) |
| GET | `/doctor` | Health report |
| GET | `/ready-fast` | Cheap readiness (home + schema) |
| GET | `/jobs/resolve` | Resolve an unambiguous job-id prefix (`prefix`) |
| GET | `/health` | Unauthenticated liveness |
| GET | `/remotes` | Paired remote hosts |
| GET | `/remotes/doctor` | SSH binaries + remote state (`remote_id`) |
| GET | `/remotes/{id}/jobs` | Remote jobs from the controller's shadow (`limit`) |
| GET | `/remotes/{id}/status/{job}` | One remote job's status |
| GET | `/remotes/{id}/jobs/{job}/tail` | Remote log range (`stream`, `offset`, `size`) |
| POST | `/artifacts/push-remote` | Publish an artifact to a remote (`remote_id`, `version_id`) |
| POST | `/artifacts/pull-remote` | Fetch a remote artifact (`remote_id`, `version_id`, `dest_path`) |

---

## Remote execution

Running jobs on another host is **beta** (POSIX targets). The CLI handles
pairing and host administration (`list`, `doctor`, `remove`, `pending`, and
`retry`). MCP and HTTP expose host discovery and diagnostics, remote job
operations, and remote artifact operations.

```bash
vanth remote pair user@host        # one-time; writes ~/.vanth/remote.sqlite
vanth remote list                  # -> remote ids
vanth remote doctor                # SSH binaries + per-host state
```

Then target that host with the `remote_id`:

| Task | MCP | HTTP |
|---|---|---|
| See hosts + ids | `remote_list` | `GET /remotes` |
| Start a job there | `job_start(remote_id=...)` | `POST /jobs` with `"remote_id"` |
| List its jobs | `job_list(remote_id=...)` | `GET /remotes/{id}/jobs` |
| Status / wait / stop / rerun | `job_status` / `job_wait` / `job_stop` / `job_rerun` with `remote_id` | `/remotes/{id}/status/{job}` etc. |
| Read its logs | `job_tail(job_id, remote_id=...)` | `GET /remotes/{id}/jobs/{job}/tail` |

Notes: remote mutations **require** a caller-supplied `idempotency_key` (8–128
chars of `[A-Za-z0-9_-]`) — that is what makes a lost response safe to retry —
and the daemon rejects a missing one (a local `start` must *not* pass one).
Read costs differ: `job_list(remote_id=...)` reads the controller's shadow with
no SSH round trip (and accepts no filters beyond `limit`), while
`job_status(remote_id=...)` makes a live status request to the host, and
`job_tail(..., remote_id=...)` reads one byte range of the remote log over the
protocol (no `follow`/`grep`). `vanth remote pending` / `retry` reconcile
requests whose response was lost.

**Remote wakes.** `job_start(remote_id=..., wake_me=True)` (or `wake_targets=`)
is supported: the wake targets are registered on the LOCAL daemon (never sent to
the host). The local daemon polls the host's retained structured events, so a
remote wake can match any event type (including `checkpoint`, `progress`, and
`metric`) after the binding is registered. This is best-effort for non-terminal
events, subject to the remote event cap/retention and the poll interval;
terminal wakes remain durable via the change feed. The controller polls every
`VANTH_REMOTE_WAKE_SYNC_SECONDS` (default 5; set `0` to disable). Settled
controller request/journal rows from that polling are pruned after
`VANTH_REMOTE_REQUEST_TTL_SECONDS` (default 604800 = 7 days; set `0` to
disable), checked every `VANTH_REMOTE_PRUNE_INTERVAL_SECONDS` (default 3600).

---

## Agent usage tips

1. **Wait, don't poll.** Use `job_wait(job_id, filters=[...], timeout_seconds=...)`
   instead of looping `job_status`. The daemon wakes the wait immediately when a
   matching event is persisted.
2. **Pass `since_event_id`** to the next `job_wait` after handling an event, so
   you never re-process an old one.
3. **Tag and thread your jobs.** Set `origin_thread_id` (the agent thread that
   launched the job) and `tags`; use `job_view(thread_id=...)` to summarize.
4. **Prefer `job_view` over `job_status`** when presenting a situation to a
   user — it is already sorted by attention priority.
5. **Make jobs self-describing.** Emit `AGENT_EVENT progress` / `checkpoint` /
   `metric` lines (see [above](#instrumenting-jobs-with-agent_event)). Jobs that
   are silent still work, but tracked jobs are far easier to reason about.
6. **Use wake targets for long jobs.** If a training run or long download needs
   a decision at a checkpoint, add a `codex_thread` or `opencode_thread` target
   with `events: ["checkpoint", "failed", "completed"]` so the agent is resumed
   instead of polling.
7. **Inspect delivery failures.** `job_delivery_attempts` shows the lease/claim
   history; `job_retry_delivery` requeues a failed one after fixing the cause.
8. **Set a sane `timeout_seconds`** on `job_start` so a hung command becomes a
   `timeout` (terminal) state instead of running forever; the runner enforces it
   even across daemon restarts. Pass the whole shell command as ONE quoted
   string: a shell operator passed as its own argument (`&&`, `|`, `>nul`, ...)
   is refused. Prefer `--wake-me` over hand-written wake JSON.
9. **Clean up old state** with `job_cleanup(older_than_seconds=..., dry_run=false)`
   so the SQLite store and log files stay bounded.
10. **Rerun failed jobs, don't rebuild them.** `job_rerun(job_id=...)` relaunches
    with the original command, env, cwd, and wake targets — ideal for retrying a
    transiently failed download or batch.
11. **Ask "what is this job?" with `job_status`.** It now returns the command,
    cwd, env, and timeout, so you can explain a job to a user without reading
    logs.
12. **Filter lists by name/tag.** `job_list(name="train", tags=["gpu"])` narrows a
    growing job list without paging through everything.
13. **Use `reverse=true` for "what happened recently."** `job_events(job_id, reverse=true, limit=20)`
    returns the newest events first, and you can page further back with
    `since_event_id` set to the oldest id you've seen.
14. **A job survives the daemon.** The runner is detached; jobs continue across
    daemon/MCP restarts. If a runner is gone at recovery, the job is marked
    `orphaned` (never silently dropped).

---

## Examples

```text
uv run python examples\long_job.py    # emits progress + checkpoints
```

`examples/long_job.py` is a small reference job that uses `vanth.agent_events`.
Start it through `job_start` and watch it in `vanth monitor`.

---

## Troubleshooting

- **`Unauthorized` (401)**: the bearer token in `~/.vanth/token` is what the
  daemon expects. Confirm `VANTH_HOME` is the same for the daemon and client.
- **Second daemon won't start**: `another vanthd already owns this VANTH_HOME`.
  One daemon per home by design.
- **Daemon startup reports a bind/port error**: another process is using the
  configured loopback port (default `8765`). Stop the conflicting process or
  choose another port by setting `VANTH_DAEMON_PORT` consistently for the
  daemon and its clients, then retry. A different `VANTH_HOME` prevents state
  collisions between isolated instances but does not free a port shared by
  them; assign each concurrent home a distinct port.
- **Job stuck `running` then `orphaned`**: the runner process died. Check
  `logs/<job_id>.runner.log` and the heartbeat thresholds.
- **No charts in the monitor**: the job isn't emitting `AGENT_EVENT` `metric` or
  `progress` lines — add them (optional).
- **OpenCode wake never arrives (TUI session)**: check `vanth doctor` for the
  relay summary, or `vanth doctor --json` for all relay/session details. If no
  `opencode_thread` relay is `live`, the plugin is not loaded. Re-run
  `vanth setup opencode` and restart opencode. Without a live relay an
  `opencode_thread` target is rejected at creation rather than silently lost.
- **Wake will never fire**: inspect `vanth deliveries --status pending` (or
  `vanth deliveries --status failed`); `vanth doctor` reports
  `pending_deliveries` and `undeliverable_wakes`.
- **OpenCode wake timing out**: increase `timeout_seconds` on the wake target
  beyond the expected turn length.
- **OpenCode wake failed with `Session not found`**: the wake target's
  `session_id` is stale or was removed. Vanth now probes the session before
  dispatching (`opencode session list`) and fails fast with
  `opencode session not found: <id>` instead of retrying a dead session.
  Refresh the wake target's `session_id` (or start a new session) and
  `job_retry_delivery` to re-dispatch. Per-target opt-out: `skip_probe: true`;
  global opt-out: `VANTH_OPENCODE_SKIP_PROBE=1`.
- **Codex wake failed with `no rollout found for thread id`**: the target
  thread has never had a turn. Start a first turn in that thread (or target an
  existing, active conversation) before waking it.
- **Monitor shows nothing / empty state**: confirm `VANTH_HOME` points at the
  daemon's home, and that `jobs.sqlite` exists there.

---

## Development

```cmd
uv run pytest -q
uv run python -m compileall -q src tests examples
uv build
go vet ./...
go test ./...
```

`uv build` produces the sdist and wheel; the wheel bundles the Go monitor.

The wheel build runs a hatchling build hook (`build-hooks/bundle_monitor.py`)
that compiles the Go monitor for the host platform and bundles it under
`vanth/monitor-bin/` so `vanth-monitor` needs no Go toolchain at runtime. `go`
must be on PATH when building the wheel; it is not needed to install or run it.
Wheels are platform-tagged (`py3-none-<platform>`) because they contain the
native binary.

Release-gate automation lives in `scripts/`:

- `scripts/chaos_matrix.py` — heavy synthetic workloads and kill/restart matrix;
- `scripts/real_adapter_smoke.py` — opt-in live Codex/OpenCode wake smokes
  (set `VANTH_SMOKE_CODEX_THREAD` / `VANTH_SMOKE_OPENCODE_SESSION`);
- `scripts/generate_go_fixture.py` — regenerates the deterministic schema-v5
  conformance fixture in `testdata/`;
- `scripts/demo_jobs.py` — starts demo jobs (training run, quick task, failing
  task) for the monitor.

## Limitations (v1)

- Delivery is at-least-once; a crash after an adapter accepts a wake but before
  Vanth records success is a documented, surfaced ambiguity.
- **Remote SSH execution and managed artifacts are beta** (POSIX targets;
  single-node controller) and are not continuously live-tested against real SSH
  hosts. Codex Desktop wake is experimental (see Wake targets).
- Outbound webhook/probe destinations are governed by the policy in the
  configuration table; link-local/cloud-metadata addresses are always refused.
- TLS, multi-user policy/RBAC, distributed workers, and a web UI are out of scope.
