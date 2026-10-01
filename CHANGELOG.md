# Changelog

All notable changes to Vanth are documented here.

## 1.13.0 - 2026-10-01

### Agent usability

- MCP calls no longer time out or starve the server. `job_wait`, `job_tail`,
  `job_stop`, and `job_start_and_wait` are asynchronous and run their blocking
  HTTP call off the event loop; `job_wait` and `job_tail --follow` are bounded
  to `VANTH_MCP_WAIT_SLICE` (25s) and return a resumable `still_running` result
  instead of the client cancelling the call with `-32001`.
- Wakes are the default for non-interactive local starts: the daemon attaches
  the calling-session wake when a single relay matches the job's `cwd`, and
  skips it otherwise. Opt out with `VANTH_DEFAULT_WAKE_ME=0`. `job_start`
  documents `wake_me` first, a no-wake start returns a `wake_recommended`
  advisory, and `doctor`/metrics report recent jobs started without a wake.
- `vanth deliveries --clear` prunes old or undeliverable delivery records
  (dry-run by default, `--yes` to apply), and `doctor` reports
  `stale_pending_deliveries`.
- The daemon writes a startup/shutdown record so a healthy daemon is not
  mistaken for a dead one.

### Reliability

- Wakes pending forever: deliveries that are never claimed now expire to
  durable dead letters after `VANTH_DELIVERY_TTL_SECONDS` (6h); manual retry
  re-arms a delivery without reopening the leak.
- Relay poll: `database is locked` under concurrent jobs is retried with
  backoff and rollback-under-lock; the liveness write is throttled to a
  heartbeat and clamped.
- Event-capture contention is tracked per job, so an unrelated job's retry can
  no longer emit a spurious `write_contended` for this one.
- Periodic PASSIVE WAL checkpoint plus TRUNCATE on close; `stop` retries a
  lingering process tree before raising.
- Schema 20: add an index on `jobs(created_at)`.

### Production hardening (carried from the unreleased 1.12.3 cycle)

- Correctness and durability fixes across snapshots, backups, artifact
  GC/catalog fencing, pipe draining and descendant reaping, secret masking
  (including Windows text translation), long/oversized log lines, log
  write/disk-full failures, concurrent interactive sends, EOF-marker crash
  recovery, atomic terminal-state persistence, Windows process cleanup, blob
  repair, cross-volume materialization, and a non-stealable GC fence.
- Durable local start idempotency with request-conflict detection, bounded
  stdout in start-and-wait, structured failure reasons and next actions, a
  start preview, and health diagnostics for artifact corruption, pipe draining,
  and event-ingestion contention.

## 1.12.3 - 2026-09-23

### Agent usability

- CLI `wait` and the CLI/MCP `wake_me` shorthand now cover every terminal
  outcome by default: completed, failed, timeout, cancelled, and orphaned.
- MCP tools have descriptions for all registered actions, and the agent tool
  reference groups the available tools by task.
- `doctor` summarizes relays and highlights failed delivery records while
  retaining the full relay list in JSON output.
- The quick start now includes MCP client verification, and CLI help and
  examples are aligned with the available commands.
- `vanth status` is documented as a read-only daemon check; it reports `DOWN`
  on a fresh home instead of triggering autostart. Operational CLI commands and
  MCP calls still start the daemon on demand.
- `vanth setup` now documents creating a missing supported-client config before
  registering Vanth, which lets first-run setup work on clean profiles.
- Monitor instructions now distinguish published wheels, which bundle the
  native Go executable, from source checkouts, where Go must be built once.
- Client startup now identifies an occupied daemon port and points to the
  daemon log when startup fails. Added recovery guidance and parameter-level
  workflows for all 26 versioned-artifact MCP tools, including safe GC and
  alias update usage.

## 1.12.2 - 2026-09-23

### Fixes

- `--wake-me` / `job_start(wake_me=True)` with no `cwd` resolved the newest
  plugin relay of **any** project, so the wake could land in an unrelated
  session. The shorthand now pins the target's `cwd` to the job's `--cwd` (or
  the caller's working directory), so relay resolution targets the caller's
  project.

## 1.12.1 - 2026-09-23

### Fixes

- `vanth sleep <seconds>` was unreachable through the real entrypoint: `vanth`
  is `server.main`, which routes to the CLI only for names in
  `_VANTH_CLI_SUBCOMMANDS`, and `sleep` was missing. Added it, plus a parity
  test asserting every command `cli.main` dispatches is routable via `vanth`.
- The Go monitor's `LatestSchemaVersion` was not bumped for schemas 17/18, so
  the monitor rejected a current database (cross-language CI was red). Bumped
  to 18 and regenerated the committed cross-language fixture.

## 1.12.0 - 2026-09-23

### Zero-JSON wakes and agent UX

- `vanth start --wake-me[=EVENTS]` (CLI) and `job_start(..., wake_me=True)`
  (MCP): be woken when a job finishes without hand-writing wake JSON. The
  shorthand defaults to `completed,failed` and resolves the live plugin relay
  for the job's directory.
- `job_start` / `vanth start` responses now echo the resolved `wake_targets`
  (with their `session_id`/`thread_id`) and `wake_addressable`, so a caller can
  confirm exactly which session will be woken.
- `notify_on` is no longer a silent no-op: it is stored (it only supplies
  default `events` for a `wake_targets` entry) but a start with `notify_on` and
  no `wake_targets` returns a `warnings` entry saying it notifies nobody.
- `vanth sleep <seconds>` starts a trivial sleep job; the `ping -n` idiom is no
  longer recommended.
- `vanth start` now **refuses** (exit 2) a command reassembled from separate
  arguments when a bare shell-operator token (`&&`, `|`, `>nul`) is present,
  instead of warning and running a broken command. Pass the whole command as
  ONE quoted string, or use a script file.
- `vanth doctor` reports `pending_deliveries` and `undeliverable_wakes`, and
  startup reconciles pre-1.11 relay-client-id wake targets (their pending
  deliveries are failed and the dead targets removed).

### Remote wakes

- Wake targets on a remote job (`job_start(remote_id=..., wake_me=True)`) are
  registered on the **local** daemon and fire when the remote job emits a
  matching event. Terminal events are durable via the change feed; non-terminal
  events (`checkpoint` / `progress` / `metric`) are delivered on a best-effort
  basis from the host's retained events.
- New remote `job.events` method: bounded, per-job sequence cursors (chunked,
  `null` = initialize to the current high-water so history is not replayed).
  Remote `job.start` accepts but ignores `wake_targets` / `notify_on` — the
  controller registers them, so an un-upgraded controller can still start jobs.
- The daemon polls the host while a wake binding is outstanding
  (`VANTH_REMOTE_WAKE_SYNC_SECONDS`, default 5) and prunes settled controller
  request/journal rows (`VANTH_REMOTE_REQUEST_TTL_SECONDS`, default 7 days).
- Wake bindings whose remote job is deleted or forgotten are settled rather
  than re-polled forever.

### Schema

- Schema 17 adds `wake_targets.remote_id`; schema 18 adds `remote_event_cursors`.

## 1.11.0 - 2026-09-22

### CLI and HTTP API

- Added `vanth wake <job-id>` (the CLI counterpart of the MCP
  `job_add_wake_target` / `job_wake_now`) to register a wake target on a job
  that is already running or finished: `--type` / `--events` / `--cwd` /
  `--config JSON` (or a full `--target JSON`), and `--now` to enqueue a
  synthetic wake immediately. `vanth api` now lists
  `POST /jobs/{id}/wake` and `POST /jobs/{id}/wake-now`. README documents that
  targets are not fixed at start time and spells out per-type thread-identity
  resolution.
- `--wake` / `--trigger` / `--policy` accept `@path` (a JSON file) or `-`
  (stdin) in addition to a literal, so PowerShell 5.1 quote-stripping no longer
  corrupts nested JSON.

### Wake target fixes

- **A relay client id is rejected as a wake destination.** `opencode_thread`
  (`opencode-<pid>-<rand>`) and `codex_desktop` (`mcp-<pid>-<thread>`) targets
  naming the long-poll `client_id` that `vanth doctor` prints were accepted and
  then never claimed — the delivery sat `pending` forever with no error. They are
  now refused at creation with an actionable message pointing at the destination
  session/thread id.
- **Identity aliases are canonicalized once at persistence.** A target
  registered with `threadId`/`sessionId` (or the other type's key) is rewritten
  to the key the relay eligibility SQL and bridges actually read, so an
  alias-only `opencode_thread` target is matchable instead of silently
  unclaimable. `wake_thread_id` now records the session id for
  `opencode_thread`, so `list --thread-id` finds it.
- **The relay poll collects destinations under every accepted alias**, so a
  relay registered with a legacy key is offered its deliveries rather than
  polling with an empty identity set.
- **`codex_desktop` with a `command` but no thread id is rejected** — the relay
  path ignores `command` and still requires the identity.
- `daemon_wake` / `job_add_wake_target` now resolve caller-task identity
  (`CODEX_THREAD_ID` / `VANTH_CODEX_DESKTOP_THREAD`) like `job_wake_now` did.

### Reliability fixes

- A queued trigger job whose parent row was pruned/removed is cancelled
  (`trigger parent ... no longer exists`) instead of staying `queued` forever
  with no event.
- `stop` on a queued job re-reads and falls through to the live-stop path when
  the dispatcher claimed the row to `launching` between the read and the CAS,
  so a zero-row CAS is never reported as a successful cancellation.
- `cleanup` re-checks terminal status inside the delete transaction, so a
  restart recovery that reclaims a terminal row to `launching` cannot have the
  job (and its wake targets/events) deleted out from under a live launch.
- A launch re-claim clears the previous run's `pid` / `worker_pid` /
  `runner_heartbeat_at`, so an abandoned re-claim cannot force-kill whatever
  process now holds the recycled pid.
- A delivery dispatch thread that fails to start is dropped from the in-flight
  set instead of permanently consuming a `max_delivery_concurrency` slot.

## 1.10.0 - 2026-09-17

### OpenCode wake (TUI)

- `opencode_thread` wakes now work in a plain `opencode` TUI session. Such a
  session binds no TCP port and injects no session id, so there was never an
  `attach` URL to use and an external `opencode run --session` wrote to a
  backend the visible session never saw — meaning this target type could not be
  delivered at all. `vanth setup` now installs an in-process OpenCode plugin
  (`~/.config/opencode/plugins/vanth.ts`) that registers the session it lives in
  and injects wake prompts through its own client, over the same client relay
  protocol (`/relay/register|poll|ack`) used for Codex Desktop. `attach` is now
  optional (still required to be a valid URL when set) and remains the path for
  headless `opencode serve`. A target with no `session_id` resolves to the relay
  registered for the job's `cwd`; with no live relay it is rejected at creation
  with an actionable message instead of pending forever. `vanth doctor` reports
  registered relays and their liveness.

### CLI and HTTP API

- Added `vanth start` as a non-MCP fallback for launching background jobs,
  including repeated `--env`/`--wake` options, interactive mode, and `--`
  command-argument passthrough. Arguments are re-quoted for the host shell, so
  command quoting, empty arguments, and shell metacharacters in an argument
  survive (an argument that cannot be encoded safely on Windows — `%`, `!`, `"`
  — is refused with an instruction to pass one quoted string). Added
  `vanth deliveries` for
  actionable wake delivery failures and `vanth api` for the loopback HTTP
  surface.
- `vanth --help` now documents API discovery and bearer authentication;
  `daemon.json` also records `auth: "bearer"` and `token_path`, never the token.
- `vanth list` now renders running-job duration and age, honors explicit status
  filters, treats `--all` as terminal-only, and rejects `--status` with `--all`.
  `GET /jobs` includes lifecycle timestamps, exit code, and runtime.

### CLI onboarding

- Found by a blind usability test (an agent given only "use `job_start`/`job_wait`
  with no MCP tools available", which scored the CLI 2/5):
  - **`vanth diff` never ran** — it was implemented in the CLI but missing from
    the entry point's dispatch set, so it fell through to the MCP stdio server
    and reported "unknown command".
  - **Every subcommand now supports `-h`/`--help`** (and `vanth help <command>`).
    Previously `vanth start --help` returned "unknown option '--help'" and
    `vanth stop --help` was read as a *job id*.
  - **`vanth wait <job_id>`** — the CLI counterpart of `job_wait`, so the
    documented "wait, don't poll" workflow works without an MCP client. Exits 0
    on the event, 3 on timeout.
  - Job-id arguments accept an **unambiguous prefix**, and an unknown id suggests
    near matches: the test dropped one character from a copied id and got a bare
    "unknown job".
  - **`vanth status <job-id>`** inspects a single job (status, exit code,
    runtime, pid, last event, progress) — the CLI counterpart of `job_status`
    that a blind test went looking for. It previously ignored the argument and
    reprinted daemon health.
  - `vanth --help` now prints an **MCP-tool → CLI-command mapping** plus a full
    start/wait/status/logs example: the onboarding tests were told to use
    `job_start`/`job_wait` and had to discover `vanth start`/`vanth wait`
    themselves (one tried `vanth job_start --help`).
  - **`vanth --json <command>` now works.** The entry point only inspected the
    first argument, so the documented global flag before the subcommand fell
    through to the MCP stdio server (a hang for an agent, "unknown command
    '--json'" in a terminal). `vanth list --json` was unaffected.
  - **`vanth list` now defaults to in-flight jobs, not just `running`.** A job is
    inserted as `launching` and only becomes `running` once the runner publishes,
     so for that window it appeared in neither `list` nor `list --all`: the agent
     started a job, listed, and saw nothing.
  - `vanth start` gained the `job_start` options it was missing — `--priority`,
    `--pool`, `--tag`, `--notes`, `--secret-env`, `--trigger JSON`,
    `--policy JSON` — so the CLI fallback is a real substitute for the MCP tool.
  - `vanth list` gained `--thread-id`, `--name`, and `--tag`: the closest CLI
    equivalent of `job_view` and filtered `job_list`.
  - `vanth start` now **warns** when a command using shell operators (`&&`, `|`,
    `>`) was reassembled from separate arguments, and points at the two reliable
    forms: one quoted string, or a script file. (A fourth blind run hit exactly
    this: PowerShell split a single-quoted command into garbage argv and the job
    died in cmd.exe.)
  - Help: `vanth --help` documents `help <command>`, both `--json` forms
    (`vanth --json list` == `vanth list --json`), an MCP→CLI mapping, a full
    workflow example, and Windows quoting rules; `start`/`logs`/`stop`/`doctor`/
    `status`/`list` now carry examples and defaults where the audit asked.

### Remote execution

- `remote_list` and `remote_doctor` MCP tools, `job_list(remote_id=...)`, and
  `job_tail(job_id, remote_id=...)`, so a remote host's jobs and logs are
  reachable without hand-written HTTP. `vanth api` and the README now document
  the `/remotes/{id}/...` routes and that `POST /jobs` accepts `remote_id`.
- Wired the previously orphaned remote log read: `RemoteControl.log_range` and
  the helper that serves `job.log_range` both existed, but no route, tool, or
  command called the controller side, so a remote job's output was unreadable
  through every surface. `GET /remotes/{id}/jobs/{job}/tail` now reads a byte
  range (`stream`, `offset`, `size`).

### Fixes

- **Daemon deadlock (artifact subsystem)**: `manager_lock` was a non-reentrant
  `Lock` while the lazy artifact accessors nest (`get_artifact_broker` /
  `get_artifact_collections` / `get_artifact_lifecycle` /
  `get_artifact_storage_profiles` -> `get_artifacts`). The first such request
  self-deadlocked while holding the lock forever, which then blocked
  `get_artifacts()` for every later request — collections, lifecycle, storage
  profiles, remote artifact transfers, and even plain `materialize`/`verify`
  all hung for the daemon's lifetime, while `/jobs` kept answering because
  `get_manager()` short-circuits on its cached global. The lock is now an
  `RLock`.
- `vanth list` no longer reports "no jobs" when its default running-only filter
  is empty and finished jobs exist; it says so and points at `--all`.
- `vanth list` DURATION/AGE no longer renders a raw seconds remainder for
  durations of a day or more (`5d 17h 1727s` -> `5d 17h 28m 47s`).
- Malformed request bodies on `/artifacts/push-remote`, `/artifacts/pull-remote`,
  remote `job.stop`/`job.rerun`, and `/remote/helper` are now field-level 400s
  naming the missing field instead of internal 500s.
- Hardened `POST /jobs` validation and error classification, rejected
  idempotency keys for local starts, and committed a job with its wake targets
  atomically; remote idempotency keys remain supported. Malformed
  `wake_targets` containers are a field-level 400, not an internal 500.
- Dead maintenance or dispatch loops now make `vanth doctor` and `GET /ready`
  unhealthy; doctor reports maintenance state and dead-lettered deliveries.
  OpenCode wake validation errors now explain the missing TUI server URL and
  point to `opencode serve` or `local_command`/`webhook` targets.
- OpenCode setup/status now handles the merged `config.json`, `opencode.json`,
  and `opencode.jsonc` files. JSONC is parsed read-only (comments stripped in
  memory) so a commented file is detected accurately, and is edited only when it
  has no comments to lose. Status and the startup hint report the deep-merged
  *effective* state (a lower file's `enabled: false` still wins), setup refuses
  to write a file a higher-precedence one would shadow, `--remove` clears every
  safely-editable registration, and any skipped client (including a requested
  client with no config file) is reported as an incomplete result rather than
  success.
- Malformed nested job fields (`wake_targets`/`trigger`/`policy`/`origin_thread_id`
  and the delivery/tail/schedule status filters) are now field-level 400s instead
  of internal 500s from unhashable set membership or SQLite binding.
- Included oversized stop grace periods in the client deadline so
  `vanth stop --kill-after` and the MCP `job_stop` no longer time out while the
  daemon is still stopping the job.
- HTTP client requests now default to a 30-second socket timeout via
  `VANTH_CLIENT_TIMEOUT`, and daemon socket reads/writes are bounded by
  `VANTH_REQUEST_TIMEOUT`; long polls retain their own budgets.

### Durable approval / decision requests

Jobs can now ask a human a question and wait for the answer durably, without
holding a thread or killing the job.

- `job_request_decision(job_id, prompt, options=["approve","deny"],
  timeout_seconds=None)` records a decision (status `pending`) and emits
  `decision_requested`, which reuses the wake-target delivery path so the
  job's owning thread is notified. The job keeps running — its status is
  untouched.
- `job_resolve(job_id, token, choice)` records one of the offered options
  (idempotent for the same choice, an error for a different one);
  `job_withdraw_decision(job_id, token)` cancels a pending request;
  `job_decisions(...)` lists them. `job_wait(job_id, ["decision_resolved"])`
  waits for the answer like any other event.
- An optional `timeout_seconds` expires the request; the maintenance loop
  marks it `expired` and emits `decision_expired`, after which it can no
  longer be resolved. Deadline, status and choice are all validated **inside**
  the write transaction that performs the resolution, so the deadline is
  enforced at the authoritative transition rather than on a stale read.
- Each decision state change commits together with its lifecycle event and
  wake deliveries in one transaction: a crash can never leave a resolved
  decision with no `decision_resolved` event for a `job_wait` caller (a retry
  could not repair that). Authoritative decision transitions are also exempt
  from the per-job structured-event cap, so a busy job at the cap still gets
  its wake. The exemption is per-call rather than per event type, because job
  stdout can emit any event type via `AGENT_EVENT` — keying on the type would
  let a job forge `decision_requested` lines and bypass the cap.
- `prompt`/`options` are bounded (10000 chars, 50 options, 200 chars each) and
  the *serialized* lifecycle payload is checked against `max_event_bytes`
  before commit, so the payload can never be truncated (truncation replaces
  the whole data object, dropping the `decision_id` and breaking the wake and
  wait paths).
- Decision routes match exact segment shapes, and an unmatched
  decision-looking path is a 404 rather than falling through to another job
  operation (e.g. `.../decision/<token>/pause` cannot pause the job).
- `job_cleanup` deletes a job's decisions with the rest of its state, so a
  removed job leaves no actionable pending request behind.
- Lifecycle events (`decision_requested` / `decision_resolved` /
  `decision_withdrawn` / `decision_expired`) are the audit trail;
  `decision_requested` also ranks as an attention event in `job_view`.
- Schema v16 adds the `decisions` table (additive; existing databases
  migrate in place with the usual pre-migration backup); the Go mirrored
  `LatestSchemaVersion` and cross-language fixture move to 16 with it.

## 1.9.1 - 2026-09-11

### POSIX Python CI + portability fixes

Enables the Python test matrix on `ubuntu-latest` and `macos-latest` alongside
Windows (`.github/workflows/ci.yml`). Getting the suite green there surfaced
the fixes below.

- **Test command builders are POSIX-correct.** A shared `tests/shellcmd.py`
  quotes workload command strings with `shlex.join` on POSIX and
  `subprocess.list2cmdline` on Windows, so `python -c "print('x')"`-style jobs
  run under `sh`/`bash` instead of failing with a shell syntax error. The dev
  scripts follow the same rule.
- **`job_wait` returns the earliest matching signal.** When several signals
  match (terminal event, a `metric_ge` threshold crossed, or
  `return_progress`), the earliest by event sequence wins instead of always
  preferring the terminal event. A metric/progress signal that precedes
  completion is returned first; a terminal event still wins once nothing
  earlier is pending. Metric candidates honour `since_event_id` (a threshold
  already returned at or before the cursor is not returned again, so a caller
  that advances the cursor still reaches the terminal event) and every
  threshold is considered, not just the first in dictionary order.
- **Codex Desktop pipe hardening.** A peer that closes the connection is now
  reported consistently ("closed the connection") whether detected on read or
  write, and `close()` shuts a socket down before closing it so a reader
  blocked in `recv()` wakes promptly on POSIX (previously it could add ~2s to a
  timed-out call).
- **Failure-streak ordering and counting.** The `on_failure` policy persists
  the updated `failure_streak` before emitting the `failure_threshold` event,
  and counts every failed execution in the interval between two event-sequence
  watermarks (bounded at the latest failure read, so a concurrent failure is
  neither skipped nor double-counted; a fast restart with backoff 0 no longer
  undercounts; state written by 1.9.0 is migrated, so an upgrade does not
  recount old failures). The reaction is marked complete only after it
  succeeds, so a daemon crash or action error retries it instead of dropping
  it — at-least-once delivery: a crash between the side effect and the marker
  save can repeat it, which is harmless for the built-in actions (disable
  excludes the job from the scan, run_job refuses a busy target, alerts are
  advisory).
- **macOS artifact materialization.** Directory materialization uses the
  dev/inode-checked plain-path fallback on macOS instead of `/dev/fd`, which is
  unreliable for creating nested entries under a directory fd; the destination
  parent and staging descriptors are re-verified immediately before publication
  and the operation fails closed if the parent was persistently swapped
  mid-write. A transient swap restored before the check, or a staging-directory
  replacement under an unchanged parent, is still not caught — closing that
  requires descriptor-relative tree construction on macOS.
- **Daemon startup.** The HTTP server no longer calls `socket.getfqdn` at
  bind time — a reverse-DNS lookup that can block for seconds (or hang) on
  locked-down networks and stall startup past client timeouts.
- **Launch claims.** A new launch claim clears the previous run's
  `worker_pid`, so stale-claim recovery can no longer skip an abandoned claim
  whose old runner pid is still momentarily visible.
- **Idle reaper.** A healthy Desktop wake relay whose activity cadence is
  coarser than the watchdog's sampling interval is no longer idle-reaped (the
  freshness window is the idle threshold, not the sampling interval), and the
  effective idle timeout is measured from the last activity rather than
  restarting a second window when the freshness window expires.
- **Event write resilience.** Structured-event writes retry further under lock
  contention instead of being dropped (Windows CI lost reader events under a
  concurrent job burst).
- **Orphaned-MCP reaping is safer.** Detection matches the actual Vanth MCP
  entrypoint only — the `vanth` console script with no CLI subcommand, or
  `<python> -m vanth.server` (with interpreter options tolerated) — and rejects
  lookalikes such as `python unrelated.py -m vanth.server`, `bash -lc 'python -m
  vanth.server'`, and CLI invocations like `vanth logs --follow`, so
  `vanth doctor --reap-orphans` can no longer terminate unrelated processes.
  Windows enumeration now uses `Get-CimInstance` with the command line (the old
  WMIC CSV parse misread the alphabetically-ordered columns and could not
  establish identity). Orphan findings are an advisory warning and no longer
  flip `vanth doctor`'s exit code.

Full suite: 783 passed, 6 skipped on Windows; 787 passed, 2 skipped on Linux
(Python 3.12); `go test ./...` green.

## 1.9.0 - 2026-09-10

### Operational hardening (from the 2026-09 roadmap research)

- **Backup/restore as one unit.** `vanth backup [--out PATH] [--include-logs]`
  writes a verified archive (SHA-256 `manifest.json`) of `jobs.sqlite`,
  `artifacts.sqlite`, the artifact blob store, and the event mirrors, using
  SQLite's online backup API so it is safe while the daemon runs.
  `vanth restore <archive> --yes` verifies integrity, snapshots the current
  state, then swaps files in; it refuses while the daemon looks running and
  refuses a backup from a newer schema without `--force`.
- **Prometheus metrics.** `GET /metrics` (authenticated) exposes jobs by
  status, running/queued, pool depth + caps, deliveries by status, dead letters,
  stale leases, disk/db size, schema version, and maintenance aliveness.
- **Operator alerts.** `VANTH_ALERT_WEBHOOK` receives edge-triggered alerts
  (dead-letter queue non-empty; free disk below `VANTH_ALERT_DISK_FREE_BYTES`),
  one POST per state change, via the outbound policy.
- **Outbound destination policy (SSRF).** Webhooks and HTTP readiness probes now
  share one policy that always refuses link-local / cloud-metadata / unspecified
  destinations (every resolved IP is checked, so DNS rebinding is caught), with
  optional `VANTH_OUTBOUND_ALLOW` (strict allowlist) and
  `VANTH_OUTBOUND_BLOCK_PRIVATE=1`.
- **Readiness-probe I/O budget** (`VANTH_PROBE_BUDGET`, default 8) so a batch of
  blocked probes cannot stall delivery dispatch/recovery/schedules.
- **Docs.** Remote execution + managed artifacts labelled **beta**; state layout,
  backup, alert, and config docs updated; the remote-artifacts plan marked
  historical.

Deferred (tracked): re-enabling the Linux/macOS Python test matrix, which needs
the test command builders made POSIX-safe (the runner is already POSIX-capable;
the suite's `list2cmdline` helpers are not).

Full suite: 774 passed, 6 skipped; `go test ./...` green.

## 1.8.0 - 2026-09-10

### Readiness-based triggers (#10)

- A queued job's `trigger` may now carry a **readiness probe** (ANDed with the
  existing DAG gate when both are present). The job stays `queued` until the
  probe passes, then launches through the same dispatcher. Probe types: `port`
  (TCP connect), `http` (GET status), `log_line` (pattern in a job's captured
  log), and `file` (path exists). Each accepts optional `timeout_seconds`
  (cancel the queued job, attributed `actor="daemon"`) and `interval_seconds`
  (probe cadence, default 1s). Probes run on the daemon host over a direct
  connection (no proxy), are throttled per job, and are bounded per dispatcher
  pass (`VANTH_PROBE_BUDGET`, default 8) so blocked probes cannot stall other
  maintenance. `timeout_seconds` is measured from when the DAG gate is satisfied
  (or from queue creation with no gate). A `log_line` probe must target an
  existing job, and its log path is confined under the logs directory. No schema
  change — the probe lives in the existing `trigger_json`.

Full suite: 753 passed, 6 skipped; `go test ./...` green.

## 1.7.0 - 2026-09-10

### Kill attribution + declared-secret masking (schema v14)

- **Stops are attributable.** `job_stop` (and `vanth stop --reason`) persist
  `stop_actor` / `stop_reason` on the job, and every resulting `cancelled` event
  carries `data: {"actor": ..., "reason": ...}`. Actors: `tool` (MCP call),
  `user` (CLI/human), `watchdog` (recovery / heartbeat reconciliation), and
  `timeout` (runner timeout). `job_status` exposes both fields.
- **Declared secrets are masked in captured output.** `job_start(secret_env=[...])`
  names env vars whose values are replaced with `***` in captured stdout/stderr
  and structured events before they are written — the GitHub `::add-mask::`
  pattern — so they never appear in logs, events, deliveries, or the monitor.
  The job's `env` (like any env var) is still stored in the owner-only
  `jobs.sqlite`; masking protects emitted output, not the environment
  definition. `job_rerun` preserves the declaration; masking is local-only for
  remote jobs.
- **Schema v14** adds `jobs.stop_actor`, `jobs.stop_reason`, and
  `jobs.secret_env_json` (the Go conformance fixture and
  `internal/state.LatestSchemaVersion` moved to 14 here, then to 15 below).

### Duration + flakiness analytics

- **`job_duration_stats`** groups terminal runs by logical job (`name`, falling
  back to the command) and reports p50/p95 runtime and queue time, success
  rate, a flaky score (a failed run with a success both before and after it),
  each group's slowest recent runs, and a `trend` flag (`regressing` / `stable`
  / `improving`) computed from the newer vs older half's p50 — catching "this
  backup crept 40min → 2h over 6 weeks". The top-level `slowest` list is the
  slowest-N runs across all groups. Exposed at `GET /analytics/durations`.
- **Monitor slowest-runs pane.** Press `s` in `vanth-monitor` for a top-20
  table of the longest terminal runs (runtime + status + name), read from the
  same `jobs.sqlite`.

### Schedules and queues (schema v15)

- **Cron/interval schedules.** `schedule_create` (and list/update/delete/next)
  launches a fresh job per fire with a cron expression (5-field or `@daily`-style)
  or a fixed `interval_seconds`. Timezones are IANA names matched against the
  local wall clock; DST is handled by construction (nonexistent local times are
  skipped, ambiguous ones fire once per UTC minute). `overlap=skip` (default)
  holds a fire while the previous run is active. No scheduler process: the
  daemon's existing maintenance loop fires due rows; missed fires are not
  backfilled. UTC needs no tz database; Windows named zones use the bundled
  `tzdata` package.
- **Queues: pools, priority, pause.** `job_start(pool=, priority=)` queues a job
  behind a named pool (`pool_configure` sets `max_parallel`, `0` = unlimited,
  and `paused`). The single dispatcher launches queued (pool and/or trigger)
  jobs by `priority` descending, oldest first, once every gate passes; the
  global `VANTH_MAX_RUNNING_JOBS` quota still applies. `job_pause`/`job_resume`
  hold/release a queued job; `pool_configure(paused=True)` holds a whole pool.
- **Schema v15** adds `jobs.pool` / `priority` / `paused` / `schedule_id`, the
  `pools` and `schedules` tables, and their indexes; the Go conformance fixture
  and `internal/state.LatestSchemaVersion` move to 15. A Windows-only `tzdata`
  dependency backs named schedule timezones.

Full suite: 742 passed, 6 skipped; `go test ./...` green.

## 1.6.0 - 2026-09-10

### Windows OpenCode live-delivery fix (rc43)

- **Multiline OpenCode wakes survive the Windows npm launcher.** The standard
  `opencode.cmd` shim forwards arguments through `%*`, which caused `cmd.exe`
  to treat prompt line breaks as command separators. Live delivery therefore
  reached OpenCode as only `vanth event`, without its delivery/job ids, event,
  message, or continuation instructions. Vanth now resolves the npm shim to
  its package's native `opencode.exe`; explicitly configured/nonstandard batch
  shims fall back to a single-line prompt that preserves every wake field.
- **Live wake transports validated.** Attached OpenCode and a persisted Codex
  CLI thread both received complete wake prompts and produced exact requested
  responses. Codex Desktop wake was validated for a current/running task. The
  experimental Desktop docs now state the observed host boundary explicitly:
  arbitrary historical/unloaded Desktop threads may accept a native send
  without producing a usable turn and are not supported.

### rc41 release-readiness review fixes (rc42)

- **Released claims no longer consume retry budget.** `relay_release` previously
  left the claim-time `attempts` increment and a completed attempt-history row
  behind. Persistent Desktop outages therefore inflated both retry accounting
  and history despite release being documented as non-consuming. Release now
  rolls back the claim counter and removes the cancelled attempt record before
  returning the delivery to pending.
- **Outages release the whole claimed batch.** Relay polling claims up to 20
  deliveries at once. When the first delivery found a dead Desktop pipe, only
  that delivery was released and the remaining claims were held until lease
  expiry. The outage path now releases every unprocessed sibling immediately.
- **Legacy stop fallback is PID-safe.** A no-token row now uses its observed
  workload PID as the ownership CAS when no worker PID exists. If the original
  legacy row had no identity at all, any claim or PID that appears after the
  stop request is treated conservatively as a replacement and is never killed.

Full suite: 706 passed, 6 skipped across two clean full runs.

### rc40 self-review fixes (rc41)

- **Stop never touches a replacement's processes.** The ownership-guarded
  stop-request could still win on A and then terminate B's workload/runner PIDs
  read after a takeover. `_stop` now re-verifies ownership after the re-read
  and returns early on mismatch — no termination, no transition — and clears
  only its own flag value so B's runner is not poisoned. Regression test uses a
  live sleeper as B's process and asserts survival.
- **Helper preserves the Unavailable class.** Production pipe failures arrived
  as plain `CodexPipeError` (the helper JSON had no class marker), so relay
  outage handling never fired. The helper now sets `"unavailable": true` and
  the parent re-raises `CodexPipeUnavailable` (message still sanitized).
- **Outage releases instead of consuming.** The old path acked failed on pipe
  outage — terminal at the default `max_attempts=1`. Now a re-provisioned
  capability is reloaded and retried once with the fresh pipe; otherwise the
  delivery is released back to pending via the new `/relay/release` endpoint
  (ownership-CAS'd, attempts untouched, immediately due) and the relay backs
  off and keeps polling. A dedicated `RelayCapabilityLost` skips the
  ack-failed path in `_run`.
- **No doomed frames.** `CodexPipeClient.call()` checks the shared sequence
  budget BEFORE sending; an exhausted budget raises without transmitting a
  prompt the host might still act on.

Full suite: 704 passed, 6 skipped across two clean full runs.

### rc39-review races: one ownership CAS, one deadline, one in-flight scope (rc40)

#### One ownership CAS (P1)

- **Stop can no longer rebind itself to a replacement launch.** `_stop`'s FIRST
  read now captures the full ownership identity (claim token + worker pid), and
  BOTH the stop-request update and every terminal transition CAS against THAT
  same identity. A finish+restart/recovery that installs claim B between the
  observation and the stop-request write means the ownership-guarded UPDATE
  affects zero rows and `_stop` returns WITHOUT setting the stop flag or
  cancelling B. The regression test drives the real `_stop` interleaving (swap
  A→B between the first read and the write) rather than calling
  `_transition_terminal` in isolation.
- **Relay acknowledgement is one atomic CAS.** `relay_ack` previously did a
  SELECT-then-UPDATE with the lock released between them; a lease reclaimed in
  that window made `_complete_delivery` update zero rows whose rowcount was
  discarded, so `relay_ack` returned success for a delivery it no longer owned.
  Validation + completion are now a SINGLE guarded UPDATE (including
  `claim_client_id`) inside one lock; a zero-row guard raises instead of
  silently reporting success. A test asserts the completion CAS is
  client-identity bound even when the token still matches.

#### One deadline (P1)

- **Helper deadline and claim lease derive from ONE end-to-end deadline.** The
  helper previously used `timeout + 2` while the server independently used
  `timeout + margin` — with `VANTH_DELIVERY_LEASE_MARGIN=1` a 300s helper got a
  302s deadline against a 301s lease, letting a stalled helper outlive its
  claim. Now the Desktop lease is the helper hard deadline PLUS the margin
  (both derived from `wake_sequence_budget`), so `helper < lease` holds for
  every margin ≥ 1. No duplicated/test-only lease arithmetic.
- **One budget covers the whole sequence.** `CodexPipeClient` now carries a
  single `sequence_deadline` shared by BOTH calls (`tools/list` preflight and
  `tools/call`); a slow preflight can no longer consume a second full timeout.
  Test covers a slow preflight followed by a never-answering tool call.

#### One in-flight scope (P1)

- **Watchdog covers blocking relay work.** The relay previously recorded one
  instantaneous activity timestamp before `/relay/poll` and did not stay active
  around a long Desktop delivery — with a small `VANTH_WATCH_IDLE` the watchdog
  reaped a healthy relay mid-operation. `_run` now holds the tracker as a
  context manager around the blocking poll AND the delivery+ack, so the
  watchdog sees them in-flight for their whole duration. A test uses a poll
  that blocks longer than the idle timeout.

#### Desktop restart recovery (P1)

- On pipe-unavailable failure the relay re-resolves the capability
  (`codex_desktop.json` / env). If a re-provisioned pipe/thread changed, it
  re-registers and continues; if nothing changed, the failure propagates so the
  delivery is marked failed (retried per `max_attempts`) rather than silently
  acknowledged. The 24h age-only check is complemented by an immediate-restart
  simulation test.

#### Relay liveness (P2)

- `relay_register` now refreshes `last_poll_at` on the upsert, so a stale
  client that reconnects is not deleted by `relay_expire_stale` between its
  registration and first poll. Regression test added.

Full suite: 699 passed, 6 skipped across two clean full runs.

### rc38-review release blockers: Desktop wake production path, relay ownership, watchdog (rc39)

#### Codex Desktop wake production path actually delivers (P0)

- **Helper module entry point.** `python -m vanth.codex_pipe --helper` never
  called `main()`, so production Desktop wake exited 0 with empty stdout and
  every delivery failed as an invalid helper response. The module now has a
  real `if __name__ == "__main__": main()` entry point, with a subprocess-level
  regression test (no parent `_handle` shortcut).
- **Provisioned pipe is carried into delivery.** The relay resolves the rc38
  handoff pipe (`VANTH_CODEX_DESKTOP_PIPE` / `codex_desktop.json`) for
  registration but DISCARDED it at delivery — `send_delivery_to_codex_desktop`
  only consulted `CODEX_APP_TOOLS_PIPE_PATH`, which real MCP children do not
  inherit. The relay now passes its resolved pipe path through `_deliver` →
  `send_delivery_to_codex_desktop(pipe_path=...)` → `send_desktop_message`.
  End-to-end tests cover both the explicit handoff env and the capability-file
  path with `CODEX_APP_TOOLS_PIPE_PATH` absent.

#### Relay ownership, watchdog, and contract (P1)

- **MCP wake tool names restored.** rc38 registered the wake tools under
  `mcp_`-prefixed names, breaking the rc37/documented contract
  (`job_add_wake_target` / `job_wake_now` / `daemon_wake`). The explicit-config
  adapters are now registered under the EXISTING external MCP names via
  FastMCP's `name=` argument; agents never see `mcp_`-prefixed names. The stdio
  contract test asserts the complete documented tool-name set and that no
  `mcp_`-prefixed name is exposed.
- **Relay activity actually resets the idle watchdog.** `notify_activity()`'s
  bump-decrement was invisible to `_watch_loop` (which samples `active` once
  per interval). `_InFlight` now records a last-activity monotonic timestamp on
  every bump, and the watchdog resets its idle timer whenever
  `now - last_activity < interval`. Deterministic watchdog tests prove repeated
  relay activity prevents idle exit and that inactivity still exits.
- **Helper deadline is strictly shorter than the claim lease.** The helper hard
  deadline was `timeout * 2` (600s for the default 300s timeout) while the
  delivery lease is ~305s — a stalled helper could outlive the lease and permit
  concurrent duplicate delivery. The helper deadline is now `timeout + 2`,
  strictly shorter than the lease. Invariant tests cover default/minimum/
  configured timeouts.
- **`threadId` alias deliveries are pollable.** Validation and
  `_delivery_thread_target` accepted the documented legacy `threadId`, but the
  SQL eligibility filter only read `$.target.thread_id`, leaving such deliveries
  pending forever. Targets are canonicalized to `thread_id` at persistence and
  the SQL covers both JSON paths. A relay-poll regression uses `threadId`.
- **Rejected acknowledgements are not silent.** `VanthClient.post()` converts
  HTTP errors into JSON error objects; `_ack` ignored the returned object, so a
  stale lease / ownership mismatch was treated as success while the row stayed
  `dispatching`. `_ack` now requires `result == "ok"` and raises otherwise.
- **Stale stop cannot cancel a replacement owner.** The launching-branch stop
  fallback called the unguarded `_transition_terminal(job_id, "cancelled")`
  after a failed CAS, allowing a stale stop to cancel a recovery/restart's new
  claim. The fallback now preserves the observed claim token and CAS's on it
  throughout; a changed owner returns without mutating the new launch. A
  deterministic stop/recovery interleaving test covers it.

#### Experimental scoping & stale-capability detection (P2)

- The private Desktop host-pipe contract is explicitly experimental and scoped
  to ONE provisioned task per Desktop lifetime. The capability file now records
  `provisioned_at`; a capability older than 24h is detected and fails closed
  with a diagnostic (re-run `vanth setup desktop`) instead of silently using a
  dead pipe. Docs (README / agent-tools) state the single-task, restart-fragile
  limitation and that automatic/durable multi-task Desktop wake is not
  supported yet.

Full suite: 694 passed, 6 skipped across two clean full runs.

### rc37-review release blockers: real Desktop wake path, relay ownership (rc38)

#### Codex Desktop wake actually works (P0)

- **Capability preflight matches the LIVE Desktop catalog.** Desktop reports the
  tool as separate fields (`namespace="codex_app"`, `name="send_message_to_thread"`);
  the preflight now matches `(namespace, name)` and also accepts a combined
  qualified name for compatibility. The fake pipe server reproduces the captured
  live schema.
- **Required caller thread id always supplied.** The native host rejects
  `tools/call` without the outer `params.threadId` (`-32602 Invalid app tool
  request`). The relay retains its authenticated executor/thread identity and
  injects it; a delivery without one is rejected BEFORE any pipe I/O.
- **Provisioned through a supported handoff, not ambient inheritance.** Real
  Vanth MCP children do NOT receive `CODEX_APP_TOOLS_PIPE_PATH` /
  `CODEX_THREAD_ID`. `vanth setup desktop` writes a per-home `codex_desktop.json`
  capability file (pipe + caller thread identity), and the relay also accepts
  `VANTH_CODEX_DESKTOP_PIPE` / `VANTH_CODEX_DESKTOP_THREAD`. Without a
  capability the relay records an explicit diagnostic instead of silently
  no-oping.
- **Hard timeout on blocking pipe I/O.** `codex_pipe` runs the call on a worker
  thread with a wall-clock deadline (closing the handle unblocks the reader), and
  production delivery additionally runs the whole sequence in a killable helper
  subprocess with a hard deadline, so a stalled named-pipe host can never hang
  the relay thread.
- **Pipe path never reaches delivery errors.** `CodexPipeUnavailable` messages
  are sanitized (Windows OSErrors embed the full named-pipe path); the raw
  exception is logged only in protected debug logging.

#### Relay ownership & lifecycle (P1)

- **Claimant-bound acknowledgements.** `relay_poll` returns an opaque
  `lease_token` per delivery (the claim token); `relay_ack` CAS's on
  `delivery_id`, `status='dispatching'`, `claim_token`, AND `claim_client_id`
  matching the caller. A different relay — or the same relay after its lease was
  reclaimed — affects zero rows. The current claim token is never reloaded on
  behalf of the acknowledger (schema v13, `claim_client_id` column).
- **No SQL starvation.** Destinations are filtered in SQL (`json_extract` on
  `payload_json`) BEFORE `ORDER BY ... LIMIT`, so 20+ older deliveries for other
  tasks can never starve a matching delivery.
- **Relay lifecycle is exception-safe.** Delivery/ack failures are caught at the
  `_run` boundary and always re-enter bounded reconnect; `stop()` unregisters in
  `finally`; stale subscriptions are expired server-side from the dispatch loop;
  at-least-once semantics use the stable delivery id as the call/turn key.

#### Launch-identity (P1)

- **Concurrent-job quota is now atomic.** `start()` counts and inserts in ONE
  `BEGIN IMMEDIATE` transaction, so two manager processes can no longer both pass
  `VANTH_MAX_RUNNING_JOBS=1` and create two `launching` rows (reproduced
  deterministically; regression test added).

#### Compatibility (P1)

- **Original Python wake names restored.** `daemon_wake` / `job_wake_now` /
  `job_add_wake_target` keep the original signature
  `(job_id, target=None, events=None, type=None, **config)` — old calls such as
  `daemon_wake(job_id, type="local_command", command=...)` work again. The MCP
  surface is registered under separately named explicit-config adapters
  (`mcp_daemon_wake` / `mcp_job_wake_now` / `mcp_job_add_wake_target`).
- **Docs/schema aligned:** README, agent-tools, and the remote protocol schema
  now list `codex_cli_thread`/`codex_desktop`; the stale "planned v1.4" and
  "shared relay protocol" claims are corrected.

Full suite: 679 passed, 6 skipped across two clean full runs.

### rc36-review release blockers: Desktop wake relay, launch-identity hardening (rc37)

#### Codex Desktop wake via client relay (P0)

- **`codex_desktop` wake restored as a distinct target** (removed in rc36 because
  the HTTP relay it referenced does not exist). It now wakes a RUNNING Desktop
  task through the native app-tools host pipe (`CODEX_APP_TOOLS_PIPE_PATH`),
  calling `codex_app/send_message_to_thread` so the follow-up lands in the
  already-running Desktop app — never a second app-server.
- **Client-side outbound relay architecture.** The persistent daemon never
  discovers client processes or stores rotating pipe names. The MCP/client
  integration owns a relay that (1) opens a durable localhost subscription to
  the daemon registering the task ids it can wake, (2) long-polls for due
  `codex_desktop` deliveries, (3) delivers through the pipe, and (4)
  acknowledges only after admission succeeds. A disconnect leaves the delivery
  pending; a reconnect re-registers and resumes from the last acknowledged
  delivery id. (OpenCode wake currently uses daemon-side attached CLI dispatch;
  the relay subscription/ack protocol is Codex-Desktop-specific for now.)
- **Provisioned through a supported handoff, not ambient inheritance.** The real
  Codex Desktop MCP children do NOT receive `CODEX_APP_TOOLS_PIPE_PATH` or
  `CODEX_THREAD_ID`. `vanth setup desktop` writes a per-home
  `codex_desktop.json` capability file (pipe + caller thread identity), or a
  host wrapper can set `VANTH_CODEX_DESKTOP_PIPE` /
  `VANTH_CODEX_DESKTOP_THREAD`. Without a capability the relay records an
  explicit diagnostic instead of silently no-oping.
- **Private pipe is contained inside the Codex MCP process** and is taken only
  from the provisioned capability — never from a job target or daemon message.
- **Fail-closed and opt-in:** with no inherited `CODEX_APP_TOOLS_PIPE_PATH` or
  capability, `codex_desktop` delivery fails with an actionable "Desktop
  integration unavailable" error and NEVER routes to the CLI thread bridge.
- The pipe client (`vanth.codex_pipe`) is a small replaceable adapter: 4-byte
  LE length-prefixed JSON-RPC frames, 8 MiB cap, exact/fragmented reads,
  per-connection serialization, response-id verification, `tools/list`
  capability preflight before `tools/call`.
- New daemon relay endpoints: `/relay/register`, `/relay/unregister`,
  `/relay/poll`, `/relay/ack` (schema v12, `relay_subscriptions` table).

#### Launch-identity hardening (P1)

- **Every direct start now carries a claim token.** `start()` inserts the job
  `launching` with a durable `claim_token` (like the `prepare_launch` path) and
  the runner promotes it atomically; the no-token `running`-with-NULL-worker
  state machine is removed. A second manager can no longer orphan a fresh
  pre-spawn row (its recovery CAS asserts `worker_pid IS NULL`), and a stale
  starter can no longer mark a newer run failed (every failure/recovery/watcher/
  terminal write is claim-token guarded).
- **Observed NULL worker is CAS'd, not skipped.** `_transition_terminal` and
  `_abandon_launch_claim` use a sentinel to distinguish "no worker guard" from
  "the snapshot observed `worker_pid IS NULL`"; an explicit NULL binds
  `worker_pid IS ?` so a runner that published a live pid after a NULL snapshot
  is never orphaned (legacy no-token recovery/reconcile included).
- `stop()` handles a `launching` row (sets `stop_requested_at` so the runner
  cannot promote; transitions to `cancelled` deterministically) instead of
  returning "already terminal" during the brief pre-promotion window.
- Adversarial tests: recover-between-insert-and-Popen, Popen-failure-after-
  newer-run, reconcile-NULL-snapshot-vs-live-publish, stale-claim-NULL-snapshot
  CAS, plus the two existing cross-process CAS tests.

#### Compatibility & contract (P1/P2)

- **Python wake wrappers restored.** `daemon_wake`/`job_wake_now`/
  `job_add_wake_target` MCP tools keep the explicit `config` dict (FastMCP
  cannot bind `**config`); the module-level `daemon_wake_python` /
  `job_wake_now_python` / `job_add_wake_target_python` wrappers accept the old
  `**config` kwargs style for direct Python callers.
- **OpenCode auth uses only `username_env`/`password_env`.** The ambiguous
  legacy `auth.username`/`auth.password` aliases are rejected outright, and a
  referenced-but-unset variable is an explicit error (no silent empty string).
- **Wake docs updated:** the stale `...` kwargs parameter row is gone (replaced
  by the `config` object), `synthetic_event_type: "wake_now"` is documented, and
  a response-contract assertion test was added.

Full suite: 665 passed, 6 skipped across a clean full run.

### rc35-review release blockers: Desktop wake, OpenCode TUI wake, restart/ownership races (rc36)

#### Codex transport (P0)

- **`codex_desktop` removed from the release surface.** It depended on an HTTP
  relay (`/send_message_to_thread`) Vanth neither ships nor discovers, and
  official Codex app-server transports are JSON-RPC over stdio/WebSocket/
  sockets — not that HTTP route. A normal Desktop install has nothing that can
  receive it. `codex_cli_thread` (alias `codex_thread`) is the only codex wake
  transport, for unloaded CLI tasks; Desktop-task wakes are not advertised
  until a supported Desktop/native app-tools channel is integrated.
- **`interrupted` turns are failures, not delivered.** The codex app-server
  bridge now raises for both `failed` and `interrupted` turn outcomes so a
  wake never reports delivered when the model was cut off.

#### OpenCode transport (P0/P1)

- **`opencode_thread` targets now REQUIRE `attach`** — the opencode server URL
  that the visible TUI/client is attached to. Without it, `opencode run
  --session` runs against an isolated backend and never wakes the visible
  client. The wake is dispatched with `run --attach <url>` so it reaches the
  running client's server.
- **The session probe no longer uses the unsupported `session list --dir`.**
  OpenCode 1.18.x rejects `--dir`; the target cwd is now passed to the probe
  subprocess via `cwd=` so a cross-project session is not misclassified missing.
- **Auth uses the documented `OPENCODE_SERVER_USERNAME` /
  `OPENCODE_SERVER_PASSWORD` variables**, and accepts only environment-variable
  NAMES (never literal secret values) so credentials are never serialized into
  wake-target config or delivery payloads.

#### Wake contract (P0/P1)

- **`job_wake_now` now inherits the calling Codex task.** The MCP wrapper
  resolves `CODEX_THREAD_ID` (in the process that owns the calling task) and
  injects it into `codex_cli_thread`/`codex_thread` targets before posting to
  `/wake-now`, so the wake resumes the caller's task. Verified at the wrapper
  level via an MCP stdio test.
- **`wake_now` rejects `auto_dispatch:false`.** It must actually dispatch, so a
  target that would leave a permanently-pending delivery is rejected instead of
  returning `woken:true`.
- **`wake_now` uses a distinct `wake_now` synthetic event type** carrying the
  real job status — it no longer fabricates a `completed` event for running or
  failed jobs.
- **`validate_wake_targets` rejects `events=[]`** (which was a silent wildcard),
  and the events/`notify_on` default is applied before validation so
  `job_start`/`wake_now` still default to `["completed", "failed"]`.
- `wake_thread_id` extraction now includes `codex_cli_thread`.

#### Launch/restart ownership (P1)

- **Promotion and pending-restart clear are ONE transaction.** The runner's
  `launching -> running` UPDATE now also `json_remove`s `pending_restart_after`
  in the same statement, so a crash cannot strand `pending_restart_after` with
  `restart_after=null`.
- **Clear/restore are single guarded UPDATEs (cross-process CAS).**
  `_clear_pending_restart_after` and `_restore_pending_restart_after` now do
  the token/status/state check in the SAME write statement (`WHERE job_id=?
  AND claim_token=? AND <json predicate>`, rowcount authoritative), closing the
  SELECT-then-UPDATE TOCTOU where a stale daemon could overwrite a newer
  claim's state.
- **Stale-claim recovery CAS includes the observed worker identity.** A runner
  that became live after the snapshot (worker_pid changed) is never orphaned.
- **Failed-kill no longer leaves a terminal row with a live workload.**
  Recovery/watcher revert the row to `launching` (and un-restore the deadline)
  when the workload kill fails, so a later pass retries.

Full suite: 646 passed, 6 skipped across two clean runs.

### Wake-contract, Codex Desktop/OpenCode transport, and rc34-review crash fixes (rc35)

#### Wake contract (P0)

- **`daemon_wake` was only registering a target for a future event.** Calling it
  after a job completed returned `result: ok` without waking anything. Added a
  genuine **`job_wake_now`** MCP tool + `JobManager.wake_now` that registers the
  target AND enqueues an immediate synthetic delivery, so a wake surfaces even
  when the triggering event already fired. `daemon_wake` is kept as a
  deprecated alias of `job_add_wake_target` (register-for-future only), and a
  new `job_add_wake_target` tool names the original semantics honestly.
- **Target-ID resolution is shared.** `start` and `wake_now` now use one
  `resolve_wake_target_identity` helper (caller thread id injected into
  codex targets; explicit ids always win).

#### Codex transport (P0)

- **`codex_desktop` removed from the release surface.** The HTTP relay it
  expected (`/send_message_to_thread`) is not shipped or discovered by Vanth,
  and official Codex app-server transports are JSON-RPC over stdio/WebSocket/
  sockets — not that HTTP route. A normal Desktop installation has nothing that
  can receive it. `codex_cli_thread` (alias `codex_thread`) remains the only
  codex wake transport for unloaded CLI tasks; Desktop-task wakes are not
  advertised until a supported Desktop/native app-tools channel is integrated.
- **`active writer` is permanent/non-retryable.** A new `CodexActiveWriterError`
  is raised when the app-server reports an active writer; the delivery
  dispatcher dead-letters it (max_attempts=1) instead of retrying into the same
  wall.
- The default codex binary prefers the Desktop-managed build under
  `%LOCALAPPDATA%\Programs\codex` over the legacy `C:\codex\codex.exe` to
  avoid protocol drift between the CLI and Desktop builds.

#### OpenCode transport (P0/P1)

- **The session probe now runs against the target cwd** so a valid
  cross-project session is not misclassified as missing and dead-lettered.
- **Authenticated servers** are supported through non-persisted credential
  references (env forwarding: `OPENCODE_USERNAME` / `OPENCODE_PASSWORD` /
  `OPENCODE_TOKEN` from a referenced env var) — never written to disk.
- **OpenCode cannot auto-inherit a session id** (`OPENCODE_SESSION_ID` is not
  injected into MCP subprocesses), so `opencode_thread` targets now REQUIRE an
  explicit `session_id` instead of silently defaulting to a wrong/absent value.
- `register_opencode` now pins `VANTH_HOME` so a custom-state installation
  cannot reach a different daemon.

#### rc34-review crash/ownership fixes

- **Parent no longer clears `pending_restart_after` before the runner is live
  (P1).** Only the runner's token-guarded `launching -> running` promotion
  clears the pending restart deadline; the parent's `worker_pid` write leaves it
  intact so a crash between the two still restores the budgeted retry.
  `_clear_pending_restart_after` now requires the owning claim token — a
  delayed runner can never erase a newer claim's intent.
- **No-token run identity is the ORIGINAL `started_at` captured before Popen
  (P1).** The parent write no longer re-reads `started_at` after Popen (which
  could capture a newer restart's timestamp and clobber its worker_pid).
- **Shutdown no longer orphans another process's claim (P1).** `prepare_launch`
  no longer mutates a `launching` row when it did not acquire the claim; a
  lost-to-shutdown claim is left for the dispatch loop's stale-claim recovery.
- **Heartbeat is run-identity guarded (P1).** A stale runner from run A can no
  longer keep run B's row fresh (masking a dead B runner): heartbeat writes are
  guarded by the claim token (or the runner's published workload pid), and the
  heartbeat loop stops when the guarded update affects zero rows.
- **Process termination is coupled to current ownership (P2).** Recovery,
  reconciliation, and the runner watcher now re-validate run identity before
  killing a workload PID, so a newer run that took ownership (or a reused PID)
  is never terminated; a failed kill keeps the job running (no orphaned,
  untracked workload).
- **Test-infra portability.** `tests/remote/test_rc14_regressions.py` now
  derives its import path from `__file__` instead of a hardcoded
  `F:/git/vanth/tests/remote`, so the suite is reproducible from a clean
  checkout at any path.

Full suite: 645 passed, 6 skipped across two clean runs.

### Launch-claim concurrency and crash-consistency fixes (rc34)

Six P1 concurrency/crash defects found in a review of the rc33 launch-token
implementation. Full suite: 636 passed, 6 skipped across two clean runs.

- **A delayed parent no longer kills a runner that already promoted (P1).**
  If the runner changes `launching -> running` before the parent's
  `worker_pid` write, the write returns rowcount 0. The parent previously
  treated every 0 as claim loss and terminated a VALID long-running workload.
  It now distinguishes owned success (same claim_token with status running or
  terminal — leave the runner alone) from genuine claim loss (mismatched
  token — terminate).
- **Ordinary starts can no longer resurrect terminal jobs (P1).** The no-token
  parent write that unconditionally set `status='running'` is now guarded by
  the run's original `started_at`. A fast job that emitted `completed`/`failed`
  (or was cancelled/orphaned) while the parent was returning from `Popen` is
  never written back to `running`.
- **A stale runner can no longer consume a newer claim token (P1).** Runners
  previously read the shared mutable `specs/{job_id}.json`, so a delayed runner
  from an old claim could read the replacement token after stale recovery. Each
  claim now writes a CLAIM-SPECIFIC spec (`specs/{job_id}-{claim_token}.json`)
  and the runner is given its own spec filename in argv; an old process can
  never acquire a newer run's identity. Claim-specific specs are cleaned up by
  the runner on success/abort and by job cleanup.
- **Stale recovery can no longer orphan a live run (P1).** Recovery used to
  snapshot a stale `launching` row, release the lock, and then transition with
  a helper that also accepted `running` — a runner promoting between the two
  got orphaned. Recovery now performs an ATOMIC launching-only, token-guarded
  transition (`status='launching' AND claim_token=?`); it reconciles/kills
  workload processes only AFTER winning that transition. If the runner promoted
  first, the guard returns 0 and the live workload is untouched.
- **Heartbeat reconciliation is run-identity guarded (P1).** Reconciliation
  finalized a stale `running` snapshot with an unguarded terminal update, so a
  newer restart could take ownership between PID reconciliation and the
  transition and then be orphaned by the old pass. The final update now
  requires the same claim_token (or the stale run's worker_pid for legacy rows).
- **Restart intent survives a claimed-but-unspawned launch (P1).** The restart
  deadline was cleared atomically with the claim, but a crash/disk error during
  spec construction left the row `launching`, recovery changed it to
  `orphaned`, and restart policy (which only watches failed/completed rows)
  dropped the already-budgeted retry. The claim now records the pre-clear
  deadline under `pending_restart_after`; an abandoned claim is recovered as
  `failed` with the deadline restored so the budgeted relaunch still fires. The
  pending intent is cleared once the launch is confirmed live (runner promoted
  or parent recorded worker_pid).

### Launch claims, runner ownership, and wheel-executable fixes (rc33)

- **POSIX wheels now bundle an executable monitor (P1).** Artifact download and
  `shutil.copyfile` do not preserve executable bits, so every published RC32
  Linux/macOS wheel stored `vanth/monitor-bin/vanth-monitor` as mode 0644 and
  failed with `PermissionError`. The build hook now `chmod 0755`s the injected
  binary for every non-Windows target.
- **Launch claims are exclusive across processes (P1).** `prepare_launch` now
  claims a job with a single guarded `UPDATE ... WHERE status IN (...)` whose
  rowcount==1 is authoritative, instead of SELECT-then-verify. Two
  `JobManager` instances can no longer both believe they own the same claim,
  because a post-UPDATE status SELECT cannot identify the writer.
- **The runner atomically promotes its owned claim (P1).** The runner promotes
  `launching -> running` guarded by a durable `claim_token` recorded in the run
  spec, and every runner terminal transition is claim-token guarded. A fast job
  can no longer finish while the row is still `launching` and then have the
  parent's unguarded update resurrect it as `running`.
- **Stale-claim recovery reconciles process ownership (P1).** Recovery now
  skips `launching` rows whose runner is still alive, terminates any workload
  PID before freeing the row, and emits an `orphaned` event through the normal
  terminal path so waits, wake targets, and feeds are notified.
- **Restart deadline clears atomically with the launch claim (P1).** The
  `restart_after` deadline and the `launching` claim are one guarded UPDATE, so
  a crash between the old deadline-clear and `prepare_launch` can no longer
  leave a failed job with its attempt consumed and no pending deadline (which
  turned `max_retries=1` into immediate `gave_up`).
- **Prebuilt target tagging falls back to the target tag (P2).** With a
  prebuilt binary + target GOOS/GOARCH and no explicit `VANTH_MONITOR_TAG`, the
  wheel is tagged for the target (`platform_tag_for(goos, goarch)`) instead of
  the build host.
- **RC tags publish as GitHub prereleases (P2).** The release workflow passes
  `--prerelease` for `*-rc*` tags so RC builds are not presented as stable
  releases.
- Adds `jobs.claim_token` (schema v11). Full suite: 630 passed, 6 skipped.

### Wheel bundles the Windows monitor with the correct `.exe` name

- **Windows wheels were missing the TUI binary** — every wheel is assembled on
  a Linux CI host, and the build hook named the bundled monitor binary from the
  BUILD host (no `.exe`) for every target. On Windows the runtime looks for
  `vanth/monitor-bin/vanth-monitor.exe`, so a Windows install reported the
  "native binary is not present" error (exit 2). The hook now names the bundled
  binary from the TARGET GOOS (`VANTH_MONITOR_GOOS`): Windows wheels bundle
  `vanth-monitor.exe`, POSIX wheels bundle `vanth-monitor`. The release
  workflow passes `VANTH_MONITOR_GOOS`/`VANTH_MONITOR_GOARCH` through to the
  wheel builds.
- Added build-hook regression tests for target-OS naming (Windows `.exe` vs
  POSIX no-suffix).

### Review fixes (RC30 policy + wake delivery reliability)

- **Launch claims are exclusive and stale claims recover (P1)** — `launching`
  is no longer in the runnable-status set, so two serialized `prepare_launch`
  calls cannot both succeed and double-spawn the same job. A claim abandoned
  by a crash (row stuck `launching`) is recovered to `orphaned` by the
  dispatch loop after `VANTH_LAUNCH_CLAIM_TIMEOUT` (default 30s), so the job
  becomes relaunchable.
- **Restart failures advance the failure streak (P1)** — automatic restarts
  reuse the job row, and `_watch_on_failure` previously suppressed every later
  failure once `last_failure_event_id` was set (a probe produced three `failed`
  events but a streak of one). The watcher now compares the newest failed event
  (ordered by `seq`, the per-job monotonic sequence — event ids are random
  UUIDs and not time-ordered) with the stored id, so each execution — original
  plus every restart — increments the streak exactly once.
- **Delivery leases cover the adapter's effective timeout (P1)** — thread
  bridges (codex/opencode) wait up to 300s by default, but the delivery lease
  defaulted to 30s + 5s margin, letting the dispatcher reclaim and re-send a
  wake while its turn was still running. The lease is now computed from each
  adapter's effective timeout (300s for thread targets, 30s otherwise), so a
  codex wake is never reclaimed mid-turn.
- **Webhook redirects cannot exfiltrate credentials (P1)** — urllib's default
  redirect handler forwarded configured headers (e.g. `Authorization`) to a
  cross-origin destination. A custom no-redirect handler fails 3xx deliveries
  instead of leaking; configured header secrets are also stripped from the
  JSON payload body (headers are sent as headers only).
- **Queue drain preserves settled history (P2)** — `clear_deliveries` without
  an explicit `status` now only touches `pending`/`retrying` rows; delivered
  records are audit history and require an explicit `status` to drain. Draining
  an in-flight row finalizes its `delivery_attempts` entry instead of leaving
  it `dispatching`.
- **Daemon no longer infers thread identity (P2)** — `JobManager.start` no
  longer falls back to the persistent daemon's `CODEX_THREAD_ID`/
  `OPENCODE_SESSION_ID` (which would inherit the thread that spawned the
  daemon); callers pass `origin_thread_id` explicitly (the MCP wrapper
  resolves it). Caller-owned wake-target dicts are copied before inherited ids
  or events are injected — never mutated.
- **Restart regression test de-flaked (P2)** — the polling-budget test observed
  for exactly the configured backoff window (2s) and could race the relaunch;
  it now uses an 8s backoff with a 2s observation window.

### Review fixes (RC27 policy + delivery reliability)

- **Restart budget consumed by launch, not polling (P1)** — the restart policy
  previously incremented `restart_attempts` and rescheduled an in-memory timer
  on every dispatcher tick because `restart_after` was never persisted. A probe
  exhausted three retries before the first timer fired. Restart bookkeeping now
  lives entirely in `policy_state`: one `restart_after` deadline is claimed
  atomically by a single dispatcher tick, no timers are involved (a daemon
  restart can't lose or double-schedule a pending relaunch), and polling never
  consumes budget.
- **Failure streaks count executions, not ticks (P1)** — a single failed run
  previously advanced `failure_streak` once per watcher poll (a `sys.exit(1)`
  with `after_n=3` tripped `failure_threshold` after three watcher calls). The
  watcher now records the terminal event id and folds each failed run into the
  streak exactly once.
- **Atomic launch gating (P1)** — `prepare_launch` now atomically verifies
  status + `policy_disabled` under one transaction and claims the row
  (`launching`) so concurrent callers cannot double-spawn; `run_job` can no
  longer launch an already-running reaction job (the probe's two-live-PIDs
  case), and the public `job_rerun` refuses a policy-disabled job.
- **Thread identity resolved in the MCP process (P1)** — `job_start` resolves
  `origin_thread_id` from the calling MCP task's environment (CODEX_THREAD_ID /
  OPENCODE_SESSION_ID) before POSTing to the daemon, and copies caller-owned
  wake-target dicts before injecting the inherited id (no caller mutation).
- **Remote jobs support policy end-to-end (P1)** — `policy` is accepted by the
  remote protocol (`START_OPTIONAL_FIELDS` + JSON schema), validated remotely,
  persisted on the remote queued job row, and carried across remote rerun.
- **Retention throttled + transactional (P1)** — per-job pruning runs at most
  once per `VANTH_RETENTION_MIN_INTERVAL_SECONDS` (default 60s) instead of a
  DELETE+commit on every 0.2s tick; deletion is rollback-on-error
  (no partial commits) and deleting deliveries cascades to their
  `delivery_attempts` (no orphans).
- **Dead-man's flags rearm on restart (P2)** — `_watch_schedule` tracks the
  observed `started_at` and clears `stuck_emitted`/`missed_emitted_at_elapsed`
  when an automatic restart reuses the same job row, so subsequent runs emit
  their own `job_stuck`/`schedule_missed`.
- **Interactive typo no longer hangs (P2)** — `vanth statsu` (unknown arg) in a
  terminal now prints `unknown command` and exits 2 instead of entering the MCP
  stdio loop; the TTY guard keys on interactive stdin alone (redirected stdout
  no longer masks it).

### Webhook wake target (notification channel beyond agent threads)

- **New `webhook` wake target type** — POSTs the delivery payload (same shape
  every adapter receives: `event`, `prompt`, `delivery_id`, `target`) as JSON
  to any HTTP(S) URL. One generic channel covers ntfy, Gotify, Telegram bots,
  Slack/Discord webhooks, PagerDuty Events, etc. — the roadmap's most-requested
  integration set (Discord, Telegram, Gotify, MQTT, IFTTT) without per-service
  code.
- Target config: `url` (required, http/https), `headers` (string key/value map
  for auth tokens / service presets), `timeout_seconds`. 2xx
  (200/201/202/204) marks the delivery `delivered`; other statuses or
  transport errors mark it failed (retried per `max_attempts` /
  `retry_delay_seconds`, then dead-lettered).
- Works everywhere wake targets work: `job_start` wake_targets, `daemon_wake`
  shorthand (`type="webhook", url=...`), and the remote protocol (schema enum
  updated).

### Delivery queue management + wake delivery reliability

- **Bulk delivery-queue clearing** — `vanth deliveries clear` (daemon route
  `POST /deliveries/clear`, MCP tool `job_clear_deliveries`). Filter by
  `job_id`, `status`, `older_than_seconds`, or `stale_only` (only deliveries
  whose source job is terminal). `dry_run` defaults to true so agents can
  preview what a drain would remove before committing; `limit` caps batch
  size.
- **Wake "delivered" now means the model actually ran** — the codex bridge
  previously returned `delivered` the instant `turn/start` acknowledged the
  turn (`inProgress`), then tore down the app-server process, killing the
  in-flight turn before the model acted on the wake. The bridge now waits
  for the `turn/completed` notification (matching the started turn id) and
  only then reports success; a failed turn is surfaced as a delivery error.
- **Turn-completion notification ordering** — the bridge buffers
  `turn/completed` notifications that arrive before the `turn/start`
  response (notification ordering is not guaranteed) so the completion
  waiter never misses them.
- **Wake delivery timeouts raised** — bridge default delivery timeout
  raised 30s -> 300s (opencode + codex), and `_complete_delivery` now
  retries with exponential backoff (5s x 3^(n-1), capped at 300s) instead
  of giving up after the first busy-session failure.

### Restart policies + retention pruning (job policies, continued)

- **Restart policies** — `policy.restart: {max_retries, backoff_seconds,
  backoff_max_seconds}` relaunches a failed job automatically with linear
  backoff capped at the max. Each relaunch emits `restarted` with attempt
  counts; the attempt budget is persisted before launch (crash-safe) and a
  successful completion resets it. When the budget is exhausted emits
  `gave_up` (level=error, flows to wake targets) once.
- **Retention pruning** — `policy.retention: {events_seconds,
  metrics_seconds, deliveries_seconds}` prunes a job's non-terminal events,
  metric points, and settled deliveries older than the TTL every dispatch
  iteration. Terminal events are always kept (status history survives).
  Log-retention without the per-entry paywall.
- **Stale-watcher race fix**: a previous run's `_watch_runner` could mark a
  relaunched job `orphaned` mid-boot (restart vs. watcher race). The watcher
  now verifies the recorded worker_pid still belongs to its own runner
  process before declaring the job dead.
- **Retention transaction hygiene**: a zero-match DELETE leaves an implicit
  transaction open which blocked runner processes for the full busy_timeout
  (30s) — retention now always settles the transaction, even when nothing
  matched.
- Fixed `DeadRunner` test-fake compatibility in `_watch_runner`.

### Dead-man's switch + failure reactions (job policies)

New per-job `policy` block on `job_start` (persisted, carried across
`rerun`, exposed in `job_status`), watched by the daemon dispatch loop:

- **Dead-man's switch** — `policy.schedule: {expected_interval_seconds,
  grace_period_seconds}` emits `schedule_missed` when no new run starts
  within interval+grace, and `job_stuck` when a run outlasts interval+grace.
  The daemon is the monitor: no external pinging service needed. Emitted
  once per window, reset on a fresh start.
- **Failure reactions** — `policy.on_failure: {after_n, action}` fires once
  the consecutive-failure streak reaches N (streak persists across reruns
  of the logical job; a completed run resets it):
  - `alert` emits `failure_threshold`
  - `disable` additionally sets a flag that blocks future launches
    (`prepare_launch` refuses disabled jobs)
  - `run_job` launches a named reaction job (e.g. cleanup/failover)

All policy events are `warning`/`error` level, flow to wake targets
(codex/opencode threads) and the delivery queue like any other event, and
carry structured data (`failure_streak`, `action`, `disabled`,
`reaction_job_id`, elapsed/interval/grace seconds).

Also: jobs launched via the dispatcher (`prepare_launch`/`_launch_prepared`)
now clear `exit_code`/`ended_at` so re-runs of a previously terminal job
start clean.

### Field-report fixes (from 1.6.0-rc23 pre-release use)

- **Wake targets inherit the launching thread by default**: a
  `codex_thread`/`opencode_thread` wake target without an explicit
  `thread_id`/`session_id` now resolves to the caller's thread
  (`origin_thread_id`, falling back to `CODEX_THREAD_ID` then
  `OPENCODE_SESSION_ID` from the MCP client environment) instead of failing
  permanently at delivery time with "requires thread_id". Explicit ids in
  the target always win, so agents can still fan out to other threads.
- **Bare `vanth` in a terminal no longer "hangs"**: with a TTY on stdin and
  stdout and no subcommand, the MCP stdio server refuses to start, prints
  where to find the dashboard (`vanth-monitor`) and human subcommands, and
  exits 2. Piped invocations (real MCP clients) are unaffected.
- **`vanth-monitor` fails fast without the bundled binary**: source/sdist
  installs raise a clear reinstall-from-platform-wheel error instead of the
  misleading "`go` not on PATH; run `uv build`" — local Go builds and
  standalone-binary overrides were removed so shipped wheels are the single
  supported path.

### RC22 review fixes

- **P1 lost-transport stop re-drive**: the public `stop()` convergence path
  now also re-drives response-less `submitting` requests (the durable state
  after transport loss), not just retry-pending `accepted` rows — a second
  same-key public call reopens transport and completes instead of returning
  the stale row.
- **P2 UNC staging paths**: Windows final-path normalization converts
  `\\?\UNC\server\share\...` to the intended `\\server\share\...` form, so
  valid UNC staging locations pass containment instead of being falsely
  rejected; `_final_path()` failures now close the opened handle before
  propagating (no leaked lock on the staging file).

### RC21 review fixes

- **P1 public stop convergence**: `control.stop()` (and any same-key public
  call) now RE-DRIVES a retry-pending request — an `accepted` row without a
  response goes back through `run_request` instead of returning the stale
  row. Regression test exercises two PUBLIC `stop()` calls through fail →
  pending → completed.
- **P1 snapshot/feed race**: the publish phase of a snapshot sync captures
  durable feed progress before fetching and ABORTS (`raced concurrent feed
  progress`) when cursor or timeline moved during fetch — a stale snapshot
  can no longer revert a newer shadow whose event would then be skipped.
- **P1 schema reconciliation**: `OPERATION_RETRY_PENDING` added to the JSON
  Schema error enum and `remote-errors-v1.json`; `STATE_EPOCH_MISMATCH`
  reconciled into both as well.
- **P2 Windows containment fails closed**: final-path resolution resizes its
  buffer as required and raises on API failure; handle-identity queries that
  cannot be completed abort the open instead of proceeding unvalidated.
- **P2 portable publication**: source-type inspection failures propagate
  instead of silently taking the non-atomic rename fallback.
- Snapshot page fetches run outside the global controller DB lock (publish
  phase still validates and holds it).

### RC20 adversarial review fixes

- **P1 retryable stops end-to-end**: transient stop failures return the new
  `OPERATION_RETRY_PENDING` error code; the controller keeps its request
  PENDING (never failed, no replay tombstone), and `accepted -> submitting`
  is now a legal request transition so same-key retries re-drive the stop
  and observe the eventual remote success. Verified by a full
  fail→pending→recover→complete controller cycle test.
- **P1 portable directories**: the fallback inspects the SOURCE
  descriptor-relatively (`lstat(dir_fd=...)`) before deciding, so directory
  publication fails closed on every platform lacking atomic no-replace —
  including the dir_fd-supplied call path used in production.
- **P1 Windows staging**: `_BY_HANDLE_FILE_INFORMATION` uses the exact ABI
  (FILETIME as two DWORDs — a c_uint64 misaligned every later field), and
  the I/O handle is validated via `GetFinalPathNameByHandleW` against the
  intended staging path, catching ancestor-junction redirects that leaf
  identity comparison alone could not.
- **P2 cleanup metadata ordering**: `wrapper_path` + `cleanup_pending=1`
  are committed BEFORE the first remote mutation of a pairing, so a lost ACK
  after a successful wrapper write always leaves a recorded obligation.
- **P2 stale-feed result fields**: the stale-batch branch derives ALL
  top-level epoch fields from the ACCEPTED durable cursor.
- Snapshot sync fetches paginated pages OUTSIDE the global controller DB
  lock; only the apply/publish transaction holds it.

### RC19 adversarial review fixes

- **P1 cross-timeline feed batches**: the apply-path guard now rejects ANY
  divergence between the durable cursor and the request/response timeline
  before touching shadows — foreign-epoch responses are additionally caught
  upstream by gap-recovery (snapshot resync), and `upsert_shadow` itself
  refuses writes bound to an older epoch than the shadow already carries.
  `feed_sync` reports the cursor DURABLY ACCEPTED, not the response's.
- **P1 zero-progress pull chunks**: an empty served window aborts the
  transfer (`no progress`) instead of spinning on the same offset forever.
- **P1 staging TOCTOU**: staging opens are now descriptor-relative on POSIX
  (`openat` walk with O_NOFOLLOW per component + leaf regular-file fstat);
  Windows uses a reparse-safe probe handle plus a second I/O handle compared
  BY FILE IDENTITY, so a synchronized parent/leaf swap aborts instead of
  redirecting access. The check-then-open pattern is gone from both
  controller pull and remote push staging.
- **P1 pairing orphan risk**: cleanup metadata is persisted BEFORE any
  remote mutation (`remotes.cleanup_pending=1` + wrapper path), cleared only
  after provable installation; removal attempts revocation for pending rows
  even without a stored authorization line, and refuses to delete records
  with live remote state unless forced.
- **P2 schema**: transfer_init requires `version_id` only under the pull
  conditional — canonical push frames validate against the JSON Schema too.
- **P2 remote push corruption**: a whole-content hash mismatch at completion
  resets the remote ledger offset AND truncates its staging file, returning
  `expected_offset=0` so the controller's classifier retransmits from zero
  instead of wedging at EOF forever.
- **P2 portable publication**: directory trees no longer fall back to
  lstat+rename when atomic primitives are missing — they fail closed with
  ENOSYS instead of racing a clobber.
- Transient stop failures now surface as ERROR frames so controllers never
  mark a still-queued stop as completed.

### RC18 adversarial review fixes

- **P1 deadlock**: the separate epoch lock is gone — state-epoch rotation
  and transfer publication both serialize on the store's `db_lock`, so the
  restore `db_lock→epoch_lock` vs publication `epoch_lock→db_lock` inversion
  can no longer deadlock (single-lock ordering).
- **P1 stale feed batches**: `feed_sync` now validates durable progress
  BEFORE applying anything — a batch whose end seq is at or below the stored
  cursor on the same timeline is skipped wholesale (`stale_batch_skipped`),
  so a stale `running` can never overwrite fresher shadows again.
- **P1 verified-bytes binding**: push completion and pull assembly each read
  ONE buffer through the no-follow handle, hash THAT buffer, and publish it;
  a same-size swap between verify-open and publish-reopen can no longer
  publish unverified data.
- **P1 staging containment**: every staging access sweeps its full ancestor
  chain (symlink/junction/reparse ancestors abort), and Windows leaves are
  checked for `FILE_ATTRIBUTE_REPARSE_POINT` via a
  `FILE_FLAG_OPEN_REPARSE_POINT` handle before any open.
- **P1 pairing cleanup handles**: `_compensate` only deletes local
  credentials when BOTH remote cleanup steps succeeded; `remove_remote`
  refuses to delete a record whose local revocation material is missing
  unless `force=True`.
- **P1 darwin AT_FDCWD**: `renameatx_np` gets Darwin's `-2`, not Linux's
  `-100`; relative non-overwrite publication works on macOS.
- **P2 resume wedge**: same-length staging corruption now fails whole-buffer
  verification, resets ledger+staging to zero, and restarts once from zero
  within the same call.
- **P2 transfer bindings**: push acks must carry epoch/acked_offset and
  acknowledge EXACTLY the sent window; pull serve responses require all
  binding fields; pull init requires `version_id` (runtime + spec); pull
  completion without `version_id` is an INVALID_REQUEST instead of a
  KeyError.

### RC17 adversarial review fixes

- **P1 stop retry semantics**: a transient `stop_sync` failure leaves the
  stop intent NONTERMINAL (`retrying: true` in the response) so the
  dispatcher re-drives it after recovery; only permanent validation failures
  (unknown target) are terminal. The dispatcher's reconciliation gained the
  same unknown-job fast-fail and already-terminal shortcut.
- **P1 pairing cleanup**: the exact wrapper path is persisted on the remote
  row at pair time; compensation and removal always target THAT per-remote
  file (legacy shared-name cleanup is best-effort). When remote revocation
  fails, local credentials and the DB row are RETAINED — `force=True`
  deletes anyway.
- **P1 cursor regression**: feed-cursor updates on one timeline are now
  compare-and-set forward-only (`_advance_feed_cursor`); gap recovery adopts
  the boundary the snapshot actually wrote instead of overwriting it with
  values from the stale feed response.
- **P1 staging containment**: pull staging lives exclusively under
  `<home>/remote-pull-staging/<transfer_id>.part` (never beside an arbitrary
  destination), opened no-follow via fd; remote push staging opens are
  leaf-symlink-proof (`O_NOFOLLOW`) across chunk receive, serve, hashing,
  and init zeroing.
- **P1 pull resume**: resume uses the controller's durable ledger offset;
  a staging file missing or truncated below it resets BOTH to zero rather
  than extending a zero-filled prefix that could never verify.
- **P1 atomic epoch fence**: publication holds the store's new epoch-rotation
  lock across put_file, whose `publish_guard` re-checks the epoch INSIDE the
  catalog transaction right before commit — a concurrent timeline rotation
  cannot land between check and publish.
- **P1 POSIX publication**: macOS uses `renameatx_np(RENAME_EXCL)` for
  atomic no-replace renames; Linux degrades to the portable hardlink/checked
  path when renameat2 is unavailable; directory tree construction stays
  descriptor-relative via `/dev/fd` on macOS.
- **P2**: completion responses must carry state_epoch/sha256/total_bytes/
  version_id (+root/manifest identity echoes added to both directions);
  unbound error frames are rejected like unbound responses; the protocol
  spec's transfer_init/transfer_complete payload definitions now match the
  runtime validators.
### Sol review fixes (second re-review)

- **P0**: replayed mutations keep the epoch binding SQLite stored — the
  controller no longer overwrites `expected_state_epoch` in memory, so a
  retry can never silently rebind while the durable row and journal keep the
  original.
- **P1 materialization ordering**: the fail-closed parent sweep now runs
  BEFORE any `mkdir` — file materialization, directory materialization, and
  pull staging never create directories through a symlink/reparse ancestor
  that the sweep is about to reject.
- **P1 stop semantics**: a failed stop records the op as FAILED (replays its
  failure durably) instead of completed; successful terminal stops emit
  full terminal UPSERTS (name/command/status/exit_code) instead of
  tombstones, so controller shadows learn the final status.
- **P1 wrapper isolation**: every pairing installs its OWN
  `remote-wrapper-<remote_id>.sh`; multiple remotes no longer overwrite or
  delete each other's forced-command target; literal remote-home paths with
  spaces are shell-quoted inside the forced command.
- **P1 snapshot feed boundary**: snapshot pages carry the feed boundary
  (`MAX(remote_feed.seq)` + feed_epoch) captured at page 1; the controller
  fail-fasts on missing/drifting boundaries and advances its stored feed
  cursor to that boundary at finalize — stale feed events can no longer
  regress fresher snapshot state.
- **P1 macOS support restored**: atomic publication falls back to
  hardlink-based no-replace publish for files (checked rename otherwise)
  outside Linux's renameat2; directory staging uses plain paths with a
  dev/inode cross-check of the opened parent where `/proc/self/fd` does not
  exist.
- **P2 transfer binding**: response frames must echo request_id+method;
  init/chunk/completion results must name the transfer, stay in range, and
  agree on epoch/content identity before bytes are adopted.
- **P2 restore temp names**: prepared restore databases include a random
  suffix so concurrent restores in one process cannot collide.

### Re-review fixes (remote-artifacts-rc14-rereview.md)

- **P0-1 pairing**: host-key fallback writes and uses a real OpenSSH config
  and cannot authenticate unless the caller explicitly selected TOFU;
  fingerprinting preserves the real key type. The forced-command wrapper is
  a syntax-checked literal POSIX script, reads daemon URL/token only at exec
  time, honors an explicitly configured Vanth home and helper path, and is
  removed during compensation/removal. Sentinel hello is bound to the paired
  remote ID plus authenticated daemon instance ID and state epoch.
- **P0-2 snapshots**: the remote materializes one immutable job/event view and
  serves every page from it. The controller verifies snapshot ID, epoch and
  high-water, stages every page, then publishes/reconciles in one transaction;
  failed or expired syncs leave shadows, epoch and cursor unchanged.
- **P1-1 concurrency**: remote and controller multi-statement transactions run
  under their store locks; 30-thread remote-start and controller-submit stress
  tests pass deterministically.
- **P1-2 durable fencing**: mutations require both expected epoch and stable
  daemon instance ID. Both are persisted with requests and journal retries;
  replay requires the original binding and never rebinds it. Response
  request-ID/method matching is mandatory.
- **P1-3 caller keys preserved** through HTTP → payload → submit.
- **P1-4 stop intents recoverable + trigger validation**: accepted stops are
  reconciled by the dispatcher after crashes; malformed/unknown triggers
  cancel instead of launching; already-terminal stop is an idempotent no-op
  (fixes the reproducible full-suite red).
- **P1-5/P1-6**: journal connection is thread-safe; queued, terminal, stop and
  tombstone feed records commit with their state transitions; production DefaultConfig wires
  RemoteDBPath so monitor shadow merging works without manual config.
- **P1-7 put_dir fence encloses catalog commit** (GC can no longer delete
  blobs between publish and version commit).
- **P1-10 transfers**: pull staging is retained and re-hashed for resume;
  missing/truncated staging resets to zero. Completion requires and validates
  epoch, bytes, whole-content SHA, root, manifest and exact version ID. Push
  publication checks the epoch inside the catalog commit; same-digest versions
  cannot substitute across roots.
- **P1-10b controller ledger**: `controller_transfers` no longer
  global-unique-keys idempotency (transfer ids bind context); existing
  databases are migrated automatically; takeover verifies the ledger row
  against the requested remote/direction/content binding; pull-derived
  publication/materialization op keys are scoped by destination token so
  shared caller keys across destinations cannot collide.
- **P2-5**: transfer protocol tests use direct `pytest.raises` again (the
  NameError-swallowing helper is gone).
- P2: descriptor-bound/no-follow log reads; recovery-marked catalog restore;
  atomic POSIX no-replace artifact publication; structured remote-wait errors;
  IPv6 bracket targets; dir-version dedup verifies/repairs all blobs; leases
  renew during long artifact loops; collection append returns its persisted
  timestamp; StorageProfiles.update has durable idempotency and is exposed as
  a guarded route/tool.

### Earlier rc14 items (monitor wiring, sweeps, transfer binding)

- **P1-7 monitor wiring**: the Go monitor now consumes remote shadows —
  `Config.RemoteDBPath` makes `Refresh` merge current-timeline shadow
  projections into the job list (failures are non-fatal warnings), with an
  end-to-end refresh test proving `job_live` arrives flagged as a remote.
- **P1-11**: materialization rejects symlink/reparse ancestors on every
  platform. POSIX final publication additionally traverses parents with
  descriptor-relative `O_NOFOLLOW` opens and uses atomic no-replace rename;
  directory publication can no longer replace a raced-in empty directory.
- **P2-5 transfer completion binding**: push completion validates the
  published version against the registered identity on sha256, total_bytes,
  manifest digest, AND root name, and re-checks the epoch immediately before
  acknowledging — any drift stops the transfer instead of committing.
- **P2-7 publication intent ledger**: put_file/put_dir write an explicit
  `<op_id>.intent.json` (content shas + manifest digest) before the first
  blob replace, removed only after the catalog commit — a crash in that
  window leaves discoverable evidence of exactly what was being published.
- **P2-9 storage profiles**: config is whitelisted to
  bucket/prefix/region/endpoint_url; secret-shaped keys are rejected outright
  before the whitelist; output configs are redacted on read; custom
  `endpoint_url` requires an explicit `VANTH_S3_ENDPOINT_ALLOWLIST` (SSRF).
- **P2-10 multipart**: InMemoryProvider completion enforces contiguous
  1..N part numbers, rejects duplicates, verifies every supplied ETag
  against stored parts, and assembles by part number (not list order);
  Boto3Provider detects S3's HTTP-200-with-embedded-error completion form.
- **P2-11**: caller-supplied idempotency keys exposed across the alias/
  delete/restore/pin/unpin/gc MCP tool surface (daemon routes already
  accepted them).
- **P2-15 capabilities-as-observations**: probe results are recorded in a
  separate `capability_observations` table with provenance/time; the
  immutable revision row is never rewritten (`get()` attaches the newest
  observation).
- **CLI**: `vanth remote pair` gains `--host-fingerprint <SHA256>` and
  `--accept-host-key`, matching the P0-1 host-key pinning contract.

### Review fixes (remote-artifacts-implementation-review.md)

- **P0-1 Pairing hardened**: strict target validation (rejects control chars /
  config injection), `Host *` dedicated per-remote config always passed via
  `-F` so directives can never be skipped by targeting the raw hostname,
  host-key pinning before any auth (`--host-fingerprint` verification or
  explicit `--accept-host-key` TOFU consent), real authorized-keys install
  script (atomic, idempotent, refuses unrestricted duplicates of our key),
  canonical hello sentinel requiring a validated `vanth.remote` response, and
  compensation that revokes ONLY the marker line on failure/remove.
- **P0-2 Cross-thread SQLite fixed**: shared remote-store connections opened
  with `check_same_thread=False` and every store operation serialized via
  RLock (JobManager db_lock pattern) — pairing + subsequent job requests on
  different handler threads no longer crash.
- **P0-3 job.stop / job.rerun dispatch correctly**: stop targets the existing
  job via the manager (no phantom queued job); rerun resolves the original
  immutable run spec and queues exactly one rerun whose replay returns the
  SAME new job id; both carry durable results for lost-response replay.
- **P0-4 Snapshot pagination repaired**: remote pages use a stable keyset
  cursor (job_id ordered) instead of mutable OFFSET; controller applies pages
  WITHOUT deletion reconciliation and reconciles only after the FINAL page
  over the accumulated identity set; every sync starts from a fresh cursor.
  >50-job snapshots and second syncs no longer suppress valid shadows.
- **P0-5 Helper framing**: daemon protocol frames forwarded UNCHANGED after
  request_id/method binding — no more double-wrapped responses hiding flat
  result fields (state_epoch, acked_offset) from the controller/transfer path.
- **P1 fixes**: responses bound to their request_id/method; artifact ops can
  no longer steal a live claim (only pending/failed/expired-running may be
  claimed); remote log reads enforce opaque-ID grammar + containment +
  no-symlink; `_run_request` re-drive seam preserved for retry.
- **P1-9 GC/publication fence**: blob publication (put_file/put_dir) and GC's
  unlink phase hold the same O_EXCL root fence, and GC re-verifies
  reachability inside the fence — a publisher can no longer commit a version
  whose blob GC just deleted.
- **P1-10 Restore crash windows closed**: backups are validated into a temp
  database (integrity_check + schema ceiling) BEFORE touching the live
  catalog; the recovery lockout is applied to the live catalog BEFORE content
  moves (any crash leaves it locked, never writable with a stale identity);
  `complete_restore` rewrites the blob owner marker first and only then
  unlocks mutations.
- **P1-12/P1-13**: storage-profile create/update/probe are gated behind
  `recovery_required`; S3-backed managed-artifact storage is explicitly
  marked UNSUPPORTED this release (provider/lease machinery only) until a
  full provider-side publication round trip ships.
- **P2 fixes**: dedup verifies the referenced blob before returning an
  existing version (corrupt content is republished instead of returned);
  Windows reserved-name validation covers basenames before extension after
  trailing dot/space trimming (`CON.txt`, `LPT1.log`); alias CAS refuses
  cross-root movement as a separate explicit error (`ALIAS_CROSS_ROOT_MOVE`);
  long artifact operations renew their claim lease between work units.
- **P2/P3**: placeholder `submitting` shadows only created for mutations and
  retired when the real shadow lands; remote wait surfaces hard errors
  immediately instead of burning an hour of timeout; collection append
  returns the persisted timestamp; version bumped to 1.6.0rc12.

### Remote execution (in progress)

- **Phase 0-3 of the remote execution plan are implemented** (see
  `remote-execution-managed-artifacts-plan.md`): protocol contract with RFC
  8785 canonicalization and golden digest vectors; secure SSH pairing
  (`vanth remote pair/list/doctor/remove`, forced-command helper, Ed25519
  identities, ambient-config neutralization); durable remote
  start/status/stop/rerun with idempotency keys, state-epoch fencing, and a
  crash-safe remote dispatcher; paginated snapshot recovery with deletion
  repair and epoch supersession; exact byte-range remote log reads; and read
  API projection across local jobs and current remote shadows (Go monitor
  included). Spec: `docs/spec/remote-protocol-v1.md`.

### Robustness

- **NEW - MCP process watchdog**: the `vanth` MCP stdio server now self-terminates
  when its launching client (codex/opencode) dies or closes stdin, and reaps
  itself when idle for `VANTH_WATCH_IDLE` seconds (default 1800, `0` disables).
  Previously a force-killed session or a client that accumulates cached workers
  left `vanth.exe` processes running forever (observed: a new process every few
  minutes, none ever reaped). A blocking tool call (`job_wait`, `job_tail
  --follow`) is never killed mid-flight. Tuning: `VANTH_WATCH_INTERVAL`,
  `VANTH_WATCH_GRACE`, `VANTH_WATCH_PARENT_PID`.
- **`vanth doctor` reports orphaned MCP servers**: `orphaned_mcp_servers` lists
  `vanth` processes whose launching client is gone, and
  `vanth doctor --reap-orphans` terminates them. Doctor now warns when orphans
  are found.
- **Schema constants reconciled**: Go `internal/state/state.go` and the Go
  conformance fixture generator now report schema v9 (matching
  `migrations.py`), including the `trigger_json` column.
- **Legacy `artifact_read` HTTP retrieval is gated**: http(s) artifact reads are
  disabled by default; opt in with `VANTH_ALLOW_HTTP_ARTIFACT_READ=1`. This path
  is legacy and is never used by managed artifacts.

## 1.5.0 - 2026-08-20

### Agent + user QoL

- **MCP tool `job_wait` gains `metric_ge`**: wait until a named metric (e.g.
  `loss`, `progress.percent`) reaches a numeric threshold, returning
  `{"result": "metric", ...}` instead of blocking for an event. Combined with
  the existing multi-event `filters`, one call can wait on "loss < 0.5 OR job
  completes".
- **NEW - job DAG via `trigger`**: `job_start(..., trigger={"job_id": A,
  "status": "completed"})` creates the job `queued`; its runner starts
  automatically once A reaches that status. If A ends in a different terminal
  status, the queued job is `cancelled`. Lightweight — one column, no graph
  engine. `vanth stop` cancels a queued job before it fires.
- **MCP tool `job_tail` gains `grep`**: server-side line filtering on stdout /
  stderr, so agents can pull only the matching lines without shipping the whole
  log. CLI: `vanth logs <id> --grep <pattern>`.
- **NEW - MCP tool `job_diff` + CLI `vanth diff`**: compare the run specs
  (command, env, cwd, timeout, tags, wake targets) of two jobs — e.g. a job vs
  its rerun — returning per-field base/other changes or `identical: true`.

### Docs + CI

- Schema v9: `jobs.trigger_json` column (migrated automatically from v8).
- Chaos matrix gains a `ux` scenario covering metric waits, DAG trigger
  (cancel + success), tail grep, and job diff through the live daemon HTTP
  layer. All 9 scenarios pass.

## 1.4.1 - 2026-08-20

### Agent + user QoL

- **MCP tool `job_rerun` now accepts override params**: `command`, `env`,
  `timeout_seconds`, `name`, `tags`, `notes`, `cwd`, and `interactive` — omitted
  parameters reuse the original job's values.
- **NEW - MCP tool `job_status_batch`**: fetch many jobs' status in one call
  (`job_ids`, `limit`) instead of N `job_status` round trips.
- **MCP tool `job_wait` gains `return_progress`**: optionally include the job's
  latest progress block in the wait response.
- **MCP tool `job_tail` gains `follow` / `timeout_seconds`**: block for new
  output until the job ends or the timeout elapses.
- **MCP tool `daemon_wake` gains a shorthand**: pass a full target dict as
  `target`, or use `type` (required, one of `local_command` / `codex_thread` /
  `opencode_thread`) plus `events` / extra config kwargs (events default to
  `["completed", "failed"]`). `add_wake_target` now validates wake targets
  against `validate_wake_targets`, rejecting unsupported types.

### Docs + CI

- **Docs**: documented that `codex_thread` wakes require a thread that has
  already had at least one turn (a persisted rollout); resuming a zero-turn
  thread fails with `no rollout found for thread id`.
- **CI**: the flaky Windows python job now runs on `pull_request` only (not
  `push`), so pushes stop triggering the intermittent
  `test_stop_after_restart_kills_runner_and_workload` pid-teardown flake.

## 1.4.0 - 2026-08-19

### Agent + user QoL

- **NEW - richer human CLI**: `vanth list` (alias `ps`), `vanth logs`
  (alias `tail`), `vanth stop`, `vanth artifacts`, `vanth prune`, and
  `vanth --version` join `status` / `doctor` / `restart` / `setup` as
  first-class operations. `list` filters by `--status`/`--limit`/`--all` and
  prints JSON; `logs` selects `--stream stdout|stderr|all` with `--offset` /
  `--max-bytes`; `prune` is a manual retention pass that is dry-run by default
  (`--older-than N`, `--yes` to apply).
- **NEW - `vanth autostart enable|disable|status`**: installs a
  start-at-login mechanism per platform (Windows Task Scheduler / macOS
  launchd / Linux systemd user unit) so the daemon survives reboots, and
  reports activation state via `vanth status`.
- **NEW - MCP tool `job_metric_ingest`**: write scalar metric points
  into a job's series with idempotency-key support, complementing the existing
  read-only `job_metrics_query`.
- **NEW - MCP tool `job_artifact_read`**: read a stored artifact's
  contents/metadata back out of a job (the read side of `job_artifact_add`).
- **NEW - MCP tool `daemon_wake`**: request the daemon's attention from
  inside a job context (e.g. to surface an agent-facing wake without a wake
  target event).
- **NEW - MCP tool `job_cleanup_preview`**: a dedicated dry-run
  retention preview that reports exactly what would be removed, separate from
  the destructive `job_cleanup`.

## 1.3.1 - 2026-08-18

- Fixed a Linux-only daemon crash on shutdown: `_stop_httpd` is registered as a
  Unix signal handler, which passes `(signum, frame)`; it previously took no
  arguments and raised `TypeError`, leaking a traceback into daemon stderr and
  failing signal-driven shutdown. It now accepts and ignores the handler args
  (the authenticated `/shutdown` route still calls it with none).

## 1.3.0 - 2026-08-18

### Cross-platform wheels + release automation

- The wheel build now supports injecting a prebuilt (possibly cross-compiled)
  Go monitor via `VANTH_MONITOR_BIN` + `VANTH_MONITOR_TAG`, and cross-compiling
  in place via `VANTH_MONITOR_GOOS` / `VANTH_MONITOR_GOARCH`. Local `uv build`
  behavior is unchanged.
- New `.github/workflows/release.yml` publishes Linux x86_64/arm64, macOS
  x86_64/arm64, and Windows x86_64 wheels to PyPI and a GitHub Release on any
  `v*` tag push (also runnable via `workflow_dispatch`). `uv tool install vanth`
  now works on Linux and macOS, not just Windows.
- CI gains a `resilience` job that runs the chaos matrix on Linux.

### Interactive stdin + `job_send`

- `job_start(..., interactive=True)` opens the job's stdin. The runner forwards
  length-prefixed records from a per-job channel (`<home>/stdin/<job_id>.in`)
  to the child's stdin; a zero-length record closes stdin (EOF).
- New MCP tool `job_send(job_id, input, eof=False)` (and HTTP
  `POST /jobs/{id}/send`) appends input to a running interactive job's channel.
  Non-blocking; rejects unknown/not-running/non-interactive jobs.
- `job_rerun` preserves the `interactive` flag; `job_cleanup` also removes the
  stdin channel files.

### Quotas + automatic retention

- `VANTH_MAX_RUNNING_JOBS` caps concurrent running jobs (default `0` =
  unlimited). `job_start` returns a clean 400 when the quota is reached.
- `VANTH_RETENTION_SECONDS` (default `0` = off), `VANTH_RETENTION_INTERVAL_SECONDS`
  (default 3600), and `VANTH_RETENTION_DRY_RUN` (default `1`) add automatic
  background retention of old terminal jobs, wired into the existing dispatcher
  loop. Safe-by-default: dry-run unless explicitly enabled.
- `vanth doctor` now reports `running_jobs`, `max_running_jobs`, and a
  `retention` config block.

## 1.2.1 - 2026-08-18

### Stale opencode session recovery

- **Probe-before-dispatch**: `opencode_thread` deliveries now cheaply check
  (`opencode session list --format json`) that the target session still exists
  before burning a model turn. A confirmed-missing session raises a
  classifiable `OpenCodeSessionNotFound` instead of failing with a raw
  "Session not found" after wasting a turn.
- **Skip retries on a dead session**: the delivery layer treats
  `OpenCodeSessionNotFound` as permanently non-retryable — it dead-letters
  immediately (attempts=1) instead of exhausting `max_attempts` on backoff for
  a session that can never succeed.
- The probe never blocks a valid dispatch: on any ambiguity (timeout, probe
  failure, non-zero exit, bad JSON) it proceeds normally. Opt out per-target
  with `skip_probe: true` or globally with `VANTH_OPENCODE_SKIP_PROBE=1`;
  `attach` targets skip the probe automatically.
- Previously-silent dead-lettered wakes from 1.2.0 (`opencode ... Session not
  found`, e.g. "Session not found") now fail fast with actionable errors.

## 1.2.0 - 2026-08-18

Reliability hardening of the delivery/job core and the wake adapters. Goal: a
job is never lost because of Vanth itself.

### Job/delivery core

- **Runner spawn is now crash-safe** (`JobManager.start`): if the detached
  runner process fails to launch (missing venv python, OSError), the job is
  transitioned to `failed` with an error event instead of being left as a
  phantom `running` row with no worker.
- **`notify_on` now actually works**: it becomes the default `events` list for
  any wake target that doesn't specify its own `events`. Previously it was
  stored but never consumed (an agent setting `notify_on` would never get
  woken).
- **`retry_delivery` can force-advance a `retrying` delivery** (reset its
  backoff immediately), not just a `failed` one.
- **Delivery dispatch backpressure**: concurrent adapter dispatches are capped
  at `VANTH_DELIVERY_MAX_CONCURRENT` (default 4); excess stays queued in
  SQLite and is picked up on the next poll.
- **`job_start` MCP tool is now sync** so FastMCP runs it in a threadpool
  instead of blocking the event loop on HTTP.
- **Dead-letter visibility in `doctor`**: `dead_letter_count` and a
  `dead_lettered` list (deliveries that exhausted `max_attempts`).

### Wake adapters (codex_thread / opencode_thread)

- Codex `initialize` handshake retries up to 3 times with backoff (bounded by
  the delivery timeout); `thread/resume` and `turn/start` are never retried
  (side-effect safety).
- Dead/broken-pipe codex processes fail with a clear error including the exit
  code and last stderr tail, instead of a raw `BrokenPipeError`.
- Child cleanup can't mask the original delivery error.
- Codex binary launch failures and opencode binary launch failures raise
  clear `CodexBridgeError`/`OpenCodeBridgeError` messages.
- Codex child processes are detached from the daemon's console group on
  Windows.



- Fixed `vanth --help` through the real entry point (`vanth = vanth.server:main`
  previously only routed status/doctor/restart/setup to the CLI, so `--help`
  fell through into the MCP stdio server and printed nothing). Bare `vanth`
  still runs the MCP server as expected.

## 1.1.2 - 2026-08-18

- `vanth --help` / `vanth -h` / `vanth help` / bare `vanth` now print a proper
  usage summary listing status / doctor / restart / setup (previously bare
  `vanth` dumped the module docstring to stderr and `--help` was an unknown
  command).

## 1.1.1 - 2026-08-18

- `vanth status` and `vanth doctor` now show MCP client registration state
  (`opencode=configured, codex=not configured, ...`) and point at `vanth setup`
  when something is missing.
- The MCP server prints a one-line stderr hint on startup when a known client
  isn't configured yet (suppress with `VANTH_NO_SETUP_HINT=1`).

## 1.1.0 - 2026-08-18

### MCP client setup

- New `vanth setup` command that connects the MCP server to the clients
  installed on the machine in one step. It detects opencode, Codex, and
  generic `mcpServers`-style clients (Claude Code / Cursor), backs up each
  config it touches (`*.vanth-setup-<ts>.bak`), and upserts the Vanth MCP
  entry without disturbing anything else in the file.
  - `vanth setup` — detect and configure everything found (prompts before
    changing).
  - `vanth setup --yes` — apply without prompting (scripts/CI).
  - `vanth setup opencode codex` — only specific clients.
  - `vanth setup --json` — machine-readable result.
  - `vanth setup --remove` — remove the Vanth MCP entries instead.
  - `vanth setup --help` — usage.
- MCP stdio tests now spawn `python -m vanth` instead of `uv run vanth`,
  so they don't trip over a running daemon locking the venv's entry-point
  scripts on Windows.

## 1.0.1 - 2026-08-18

- Packaging: add Apache-2.0 `LICENSE`, production PyPI metadata (classifiers,
  URLs, keywords), and point the quick-start install back at `uv tool install
  vanth` now that the package is published. No runtime changes.

## 1.0.0 - 2026-08-18

First supported v1 release. Vanth is a localhost background-job daemon and MCP
interface for agents: start detached jobs, receive `AGENT_EVENT` structured
events, wait on durable SQLite state, and wake Codex or OpenCode sessions when
a job needs attention.

### Operations CLI and daemon lifecycle

- New `vanth` subcommands for humans (not MCP): `vanth status`, `vanth doctor`,
  `vanth restart`. `status` reports daemon up/down, pid, schema, running jobs,
  delivery counts (supports `--json`); `doctor` prints a human-readable health
  report; `restart` gracefully stops the daemon and starts a fresh one (jobs
  survive — runners are detached).
- The daemon gains an authenticated `POST /shutdown` route so a client can
  request graceful shutdown over loopback HTTP (used by `vanth restart`).
- `VanthClient` now honors `VANTH_DAEMON_HOST`/`VANTH_DAEMON_PORT` when no URL
  or discovery metadata is present, so clients and `vanth restart` work on
  non-default ports.
- The terminal monitor ships as a bundled native Go binary via the
  `vanth-monitor` console script (platform-tagged wheels, no Go toolchain
  needed at runtime).

### Telemetry (schema v8)

- `metric_series` table: scalar fields of `metric`/`progress` AGENT_EVENTs are
  mirrored into queryable series (job, metric, x/y, stage, event id, seq,
  timestamp), matching the Go monitor's transform semantics (`_step` as x,
  `progress.current`/`total`/`percent` derived).
- `artifacts` table: jobs can carry named artifacts (checkpoints, CSVs,
  rendered outputs) with uri, kind, size, sha256, and meta.
- New MCP tools + HTTP routes:
  - `job_metrics_query(job_id, metric?, from_ms?, to_ms?, limit?)` — read scalar series.
  - `job_metric_compare(job_ids, metric, aggregation)` — compare a metric across runs
    (latest/mean/min/max/sum/count).
  - `job_run_summary(job_id)` — one-call "did it work?" (status, runtime, progress,
    latest metrics, artifacts).
  - `job_artifact_add(job_id, name, uri, ...)` and `job_artifacts(job_id)`.
  - `job_dashboard(job_ids?, limit?)` — downsampled chart-data view for any renderer.
- SQLite schema bumped to v8 (adds `metric_series`, `artifacts` tables);
  existing homes migrate with a backup. Go fixture/conformance updated to v8.

### Agent-facing features (schema v7)

- `job_status` / `job_view` now return the job's `command`, `cwd`, `env`,
  `timeout_seconds`, `notes`, a `run` overview (author, hostname, OS, Python
  version, CPU/GPU, git repo/branch/commit), and `runtime_seconds` — so an
  agent can answer "what is this job?" without reading logs.
- `job_list` accepts `name` (substring) and `tags` filters.
- `job_events` accepts `reverse=true` to return the newest events first, with
  backward paging via `since_event_id`.
- `job_rerun(job_id)` relaunches a job with its original command, cwd, env,
  timeout, name, tags, notes, origin thread, and wake targets.
- The daemon writes `daemon.json` discovery metadata atomically on start
  (url, pid, started_at, schema) and removes it on graceful shutdown; the MCP
  client discovers the daemon URL from it.
- `vanth.agent_logger` routes loguru records into structured `AGENT_EVENT`
  log events (timestamped, level-aware, with context) persisted in the event
  table.

### Migration and state

- Ordered SQLite migrations driven by `PRAGMA user_version`.
- A timestamped backup of an existing database is created under
  `VANTH_HOME/backups/` before any migration runs.
- SQLite uses WAL mode with a configurable `busy_timeout`
  (`VANTH_BUSY_TIMEOUT_MS`, default 30000) and foreign keys enabled.
- Event sequence numbers are allocated inside a `BEGIN IMMEDIATE` transaction,
  so concurrent runner/daemon processes can never allocate the same per-job
  `seq`.
- The per-event SQLite write lock is no longer held during the JSONL mirror
  append; transient `database is locked` is retried for events, workload PID
  publication, heartbeats, and terminal transitions; a reader thread survives a
  single failed persist instead of dying and losing the stream.

### Security

- The daemon requires `Authorization: Bearer <token>` on every data route.
  The token is generated per home, stored owner-only, and never logged.
- On daemon start the state directory's permissions are re-tightened to the
  owner only (Unix `0700`/`0600`; Windows `icacls` disables ACL inheritance and
  grants only owner, SYSTEM, and Administrators). This prevents a broad
  profile-level grant (e.g. a sandbox group with read access to the user
  profile) from exposing the bearer token or per-job env/spec data.
- The daemon binds only to loopback addresses; a non-loopback
  `VANTH_DAEMON_HOST` is rejected. On Windows, socket `SO_REUSEADDR` is
  disabled so a second daemon cannot silently become a phantom listener on the
  same port; a failed bind releases the home lock and exits cleanly.
- An upstream `pydantic-settings` warning (an unresolved `lifespan` forward
  reference in mcp's FastMCP) that printed on every console-script invocation
  of a fresh install is suppressed.
- One OS-backed daemon lock per `VANTH_HOME`; a second daemon exits quickly.

### Delivery

- Durable at-least-once wake delivery with leases, claim tokens, and attempt
  history (`job_delivery_attempts`), automatic due retries, and crash ambiguity
  surfaced as reclaimed attempts.
- `delivery_id` is the idempotency key in every adapter payload.
- Codex app-server (`initialize -> thread/resume -> turn/start`) and OpenCode
  CLI (`opencode run --session <id> --format json`) adapters. The OpenCode
  default command is resolved through `shutil.which()` so npm `.CMD` shims work
  on Windows.

### Operations

- Graceful signal shutdown that drains in-flight work and leaves detached jobs
  recoverable.
- Bounded rotating daemon and per-job runner diagnostic logs; bounded per-stream
  log caps with `log_truncated` events; structured event cap per job.
- `job_doctor` readiness and `job_cleanup` with dry-run and tombstones.
- Windows Startup/Task Scheduler action (`deploy/vanthd.cmd`) and Unix systemd
  user service (`deploy/vanthd.service`) templates.

### Compatibility and packaging

- `mcp` is pinned to `>=1.0,<2.0` because mcp 2.0 removed
  `mcp.server.fastmcp`.
- The wheel bundles a native Go monitor binary (platform-tagged,
  `py3-none-<platform>`), built by a hatchling build hook; no Go toolchain is
  needed to install or run it.
- CI workflow (`github/workflows/ci.yml`) runs the suite, compile check, wheel
  build, and an isolated wheel import smoke on Windows and Linux for Python
  3.11 and 3.12.
- Release-gate matrix: `scripts/chaos_matrix.py` (50-job x 500-event burst with
  exact counts, slow-adapter non-blocking, daemon kill/restart recovery, runner
  kills, malformed-input battery, log caps and cleanup idempotence) and
  `scripts/real_adapter_smoke.py` (opt-in real Codex/OpenCode wakes; records
  installed versions).

### Limitations

- `job_send` and interactive stdin are not implemented; jobs run with stdin
  closed.
- A daemon crash after an external wake adapter accepts the side effect but
  before Vanth records success remains inherently at-least-once ambiguity.
- Live Codex/OpenCode wake smokes are opt-in and were not run during release
  validation; fake-adapter contract tests cover the protocol.
