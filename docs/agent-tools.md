# Vanth agent tool surface

This is the reference contract for the MCP tools an agent sees when connected
to Vanth. Agents call these MCP tools directly; Vanth handles the local HTTP
connection to its daemon internally and starts the daemon on demand. Agents do
not need to construct HTTP requests for the workflows below. All responses are
JSON objects.

Conventions:

- `job_id` values look like `job_<hex>`; `event_id` like `evt_<hex>`;
  `delivery_id` like `del_<hex>`; `artifact_id` like `art_<hex>`.
- Errors are returned as `{"result": "error", "error": "<message>"}` (HTTP 4xx
  on the wire) rather than raised.
- Timestamps are ISO-8601 UTC strings with a `Z` suffix (e.g.
  `2026-08-18T12:00:00Z`).
- `limit` is validated to 1–1000 (up to 10000 for metrics, 50000 for
  dashboard, 1000 for artifacts).

## Tool map

This guide describes the MCP names registered by the current server. Use the
core job tools for the usual start, wait, inspect, and recover loop; the rest
are optional surfaces for delivery management, scheduling, metrics, artifacts,
and remote execution.

MCP is Vanth's full agent surface. The CLI covers core job operations and
administration, with counterparts called out below where available; those
commands may have different arguments or output from the MCP tools. Advanced
delivery management, scheduling, metrics, and versioned artifact workflows are
MCP-first and do not all have CLI counterparts.

| Need | Tools | Reference |
|---|---|---|
| Start and control work | `job_start`, `job_start_and_wait`, `job_rerun`, `job_send`, `job_stop`, `job_pause`, `job_resume` | [Start](#job_start), [bounded start and wait](#job_start_and_wait), [rerun](#job_rerun), [interactive stdin](#job_send), [stop](#job_stop) |
| Wait and inspect | `job_wait`, `job_status`, `job_status_batch`, `job_list`, `job_view`, `job_tail`, `job_events`, `job_run_summary`, `job_diff` | [status](#job_status), [list](#job_list), [events](#job_events), [wait](#job_wait), [summary](#job_run_summary) |
| Wake a session | `job_add_wake_target`, `job_wake_now`; `daemon_wake` is a deprecated alias | [Wake targets](#job_add_wake_target) |
| Diagnose deliveries | `job_deliveries`, `job_mark_delivery`, `job_retry_delivery`, `job_delivery_attempts`, `job_clear_deliveries` | [Delivery tools](#job_deliveries) |
| Coordinate jobs | `job_request_decision`, `job_resolve`, `job_withdraw_decision`, `job_decisions`, `pool_configure`, `pool_list`, `schedule_create`, `schedule_list`, `schedule_update`, `schedule_delete`, `schedule_next` | [Decisions](#job_request_decision--job_resolve--job_withdraw_decision--job_decisions), [pools](#pool_configure--pool_list), [schedules](#schedule_create--schedule_list--schedule_update--schedule_delete--schedule_next) |
| Read or compare metrics | `job_metrics_query`, `job_metric_ingest`, `job_metric_compare`, `job_duration_stats`, `job_dashboard` | [Metrics](#job_metrics_query) |
| Attach job files | `job_artifact_add`, `job_artifacts`, `job_artifact_read` | [Job artifacts](#job_artifact_add) |
| Manage versioned artifacts | `artifact_put`, `artifact_put_dir`, `artifact_resolve`, `artifact_info`, `artifact_materialize`, `artifact_verify`, `artifact_collection_create`, `artifact_collection_append`, `artifact_collection_get`, `artifact_alias_set`, `artifact_link_lineage`, `artifact_lineage_for`, `artifact_delete_request`, `artifact_restore`, `artifact_pin`, `artifact_unpin`, `artifact_gc`, `artifact_backup`, `artifact_begin_restore`, `artifact_complete_restore`, `artifact_storage_profile_create`, `artifact_storage_profile_get`, `artifact_storage_profile_probe`, `artifact_storage_profile_update`, `artifact_push_remote`, `artifact_pull_remote` | [Versioned artifacts](#versioned-artifacts) |
| Health and cleanup | `job_doctor`, `job_cleanup_preview`, `job_cleanup` | [Health](#job_doctor), [cleanup](#job_cleanup) |
| Remote execution | `remote_list`, `remote_doctor`; `job_start`, `job_list`, `job_status`, `job_tail`, `job_wait`, `job_stop`, `job_rerun` accept remote execution where documented | [Remote execution](#remote-execution) |

The versioned artifact tools store files and directories by name, resolve
versions or aliases, build collections, and record producer/consumer lineage.
Pinning protects a version from garbage collection; delete requests can be
restored. Storage profiles configure and probe backends, while backup/restore
and remote push/pull move artifact data between stores and paired hosts.

### Versioned artifacts

These tools use immutable version IDs (`version_id`) under a named root. A root
resolves to its latest version; aliases are explicit movable pointers, updated
with compare-and-swap. Paths are local to the machine running the Vanth daemon.
`idempotency_key` is optional for local mutations and useful when retrying a
request after an uncertain response. Remote artifact mutations require it.

| Tool | Parameters | Use |
|---|---|---|
| `artifact_put(path, name, idempotency_key?)` | Local file path, root name | Publish a file; identical content under the same root deduplicates. |
| `artifact_put_dir(source_path, name, idempotency_key?)` | Local directory path, root name | Publish a directory tree as one version. Symlinks, reparse points, special files, and concurrent source changes are rejected. |
| `artifact_resolve(name, alias?, version_id?)` | Root name and optionally one selector | Resolve the latest root version, named alias, or explicit version. |
| `artifact_info(version_id)` | Version ID | Read its manifest and blob/verification state. |
| `artifact_materialize(version_id, dest_path, overwrite=False)` | Version ID, local destination | Atomically write content; an existing destination fails unless `overwrite=True`. |
| `artifact_verify(version_id)` | Version ID | Re-hash content and compare it with the manifest. |
| `artifact_collection_create(name, idempotency_key?)` | Collection name | Create an ordered collection. |
| `artifact_collection_append(collection, version_id, idempotency_key?)` | Collection name, version ID | Append in monotonic order; duplicate append is a no-op. |
| `artifact_collection_get(name)` | Collection name | Read its ordered versions. |
| `artifact_alias_set(alias_name, root_id, new_version_id, expected_version_id?, updated_by?, idempotency_key?)` | Alias, root ID, target version, optional expected current version | Move the alias only if its current target matches `expected_version_id`; omit/null to create a new alias. Mismatch returns `ALIAS_CAS_MISMATCH`. |
| `artifact_link_lineage(producer_kind, producer_id, consumer_kind, consumer_id, version_id, idempotency_key?)` | Producer and consumer identities plus version | Record a link. Kinds are `job`, `remote_job`, `version`, or `alias`. |
| `artifact_lineage_for(version_id)` | Version ID | List recorded lineage links. |
| `artifact_delete_request(version_id, idempotency_key?)` | Version ID | Request logical deletion; aliased versions are rejected and content remains until GC. |
| `artifact_restore(version_id, idempotency_key?)` | Version ID | Clear a pending delete request. |
| `artifact_pin(version_id, hold_reason, idempotency_key?)` | Version ID, reason | Hold a version so GC cannot reclaim it. |
| `artifact_unpin(version_id, idempotency_key?)` | Version ID | Remove the hold. |
| `artifact_gc(dry_run=True, idempotency_key?)` | Dry-run flag | Report eligible unreachable content; inspect this result before setting `dry_run=False`. |
| `artifact_backup()` | None | Create a SQLite catalog backup. |
| `artifact_begin_restore(backup_path)` | Local backup path | Begin catalog recovery. Mutations stay locked until complete-restore. |
| `artifact_complete_restore()` | None | Clear the recovery-required marker after restore. |
| `artifact_storage_profile_create(kind="s3", config?)` | Backend kind and config | Register a profile at revision 1. |
| `artifact_storage_profile_get(profile_id)` | Profile ID | Read its latest revision and capabilities. |
| `artifact_storage_profile_probe(profile_id)` | Profile ID | Probe endpoint capabilities and record them on the latest revision. |
| `artifact_storage_profile_update(profile_id, config, idempotency_key?)` | Profile ID and replacement config | Create the next immutable profile revision; prior revisions remain queryable. |
| `artifact_push_remote(remote_id, version_id, idempotency_key?)` | Paired remote ID, version ID | Push a version to a paired host using resumable transfer. |
| `artifact_pull_remote(remote_id, version_id, dest_path, idempotency_key?)` | Paired remote ID, remote version ID, local destination | Pull and materialize a remote version here using resumable transfer. |

Typical flow: publish, resolve, verify, then materialize. For example, call
`artifact_put(path="F:/models/best.pt", name="experiment-a")`, pass its
returned `version_id` to `artifact_verify`, and use that ID with
`artifact_materialize(version_id="...", dest_path="F:/restore/best.pt")`.
Use `artifact_alias_set` when a consumer needs a stable name such as `stable`,
and supply the expected prior version when moving it. Pin versions that must
survive collection/retention cleanup. Start garbage collection with its default
dry run and review candidates before making a destructive call.

Storage profile `config` is backend-specific. Create or update a profile, read
it back, and probe it before relying on storage capabilities; do not place
credentials in prompts or logs. Remote push/pull use the paired-host broker and
require an `idempotency_key` for safe retries.

`job_wait` can return current progress, and `job_tail` can follow output; these
options are documented in their sections. Where a CLI counterpart is listed,
it provides the corresponding core operation, not necessarily the same
parameter or response contract. Wake tools also have Python counterparts, but
the MCP names are the external names shown above. The implementation adapters
named `mcp_*` are not exposed to agents.

---

## `job_start`

Launch a command as a detached job. The job keeps running even if the MCP
client or daemon restarts.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `command` | `string` | required | Shell command to run detached |
| `cwd` | `string?` | `None` | Working directory |
| `name` | `string?` | `None` | Human-readable label |
| `env` | `map<string,string>?` | `{}` | Extra environment |
| `timeout_seconds` | `int?` | `None` | >= 1; `None` = no timeout (enforced even across daemon restarts) |
| `notify_on` | `string[]?` | `None` | Only defaults `events` on an existing `wake_targets` entry; without `wake_targets` it notifies nobody, and the start response carries a `warnings` entry saying so |
| `wake_targets` | `object[]?` | `None` | See README "Wake targets"; `{type, events, ...config}` |
| `wake_me` | `bool?` | `False` | Zero-JSON `opencode_thread` wake; defaults to all terminal outcomes (`completed`, `failed`, `timeout`, `cancelled`, `orphaned`), omits `session_id`, and resolves the live relay for the job's cwd |
| `origin_thread_id` | `string?` | `None` | The agent thread that launched it (defaults to `CODEX_THREAD_ID`) |
| `tags` | `string[]?` | `None` | Arbitrary labels, filterable in `job_list` |
| `notes` | `string?` | `None` | Free-form annotation shown in the monitor |
| `interactive` | `bool` | `false` | Open stdin for `job_send` |
| `trigger` | `object?` | `None` | DAG gate `{"job_id": A, "status": "completed"}` and/or readiness `probe` (see below) |
| `secret_env` | `string[]?` | `None` | Env var NAMES whose values are masked (`***`) in captured logs/events (local jobs) |
| `idempotency_key` | `string?` | `None` | Optional durable local retry key (8..128 letters/digits/underscore/hyphen); identical retries recover the original job, changed settings are rejected |
| `dry_run` | `bool` | `False` | Validate and return a local start preview without creating a job or executing the command |

CLI equivalents: `vanth start --idempotency-key KEY -- <command>` and
`vanth start --dry-run -- <command>`. CLI and MCP default to the caller's working
directory. Explicit directories expand `~` and resolve to absolute paths.
Retry keys survive daemon restarts and job cleanup; a key belonging to a cleaned
job is rejected instead of launching duplicate work. Use a new key for new work.

`job_start_and_wait` accepts the same local retry key and returns bounded
`stdout_excerpt` (8 KiB) and `stderr_excerpt` (2 KiB) in its summary. Optional
excerpt flags are also available on `job_run_summary`. Failures in status and summaries include
`failure_reason` and `recommended_next_action`; direct local reruns use the same
startup confirmation as starts.

`job_doctor(verify_artifacts=True)` / `vanth doctor --verify-artifacts` distinguish
missing and corrupt blobs. The scan is bounded to 100 recent versions, 1,000
blobs, 16 MiB of manifests (1 MiB each), 64 MiB of blob content, and three seconds;
`artifact_integrity.complete=false` means a partial scan.
Persisted pipe-drain, capture-failure, and contention diagnostics are returned
with job IDs. Historical capture/contention warnings are advisory.

**Response**

```json
{
  "job_id": "job_abc123",
  "status": "running",
  "worker_pid": 4242,
  "startup_confirmed": true,
  "stdout_path": "C:/Users/you/.vanth/logs/job_abc123.stdout.log",
  "stderr_path": "C:/Users/you/.vanth/logs/job_abc123.stderr.log",
  "events_path": "C:/Users/you/.vanth/events/job_abc123.jsonl",
  "message": "Job started"
}
```

For a direct local job, `job_start` waits up to three seconds for the runner's
`started` event before returning. `startup_confirmed=true` means the workload
process was spawned; it does not mean the command succeeded. If confirmation
does not arrive in that bound, the job remains tracked and the response returns
`startup_confirmed=false` with its `job_id`; use `job_status` or `job_wait` to
follow it. A queued job returns `queued` immediately because its gates may take
arbitrarily long. Remote starts return the remote submission response without
local startup confirmation.

CLI core counterpart: `vanth start --wake-me -- <command>` (or
`--wake-me=completed,failed,checkpoint` to override events) and
`vanth sleep <seconds>` for a trivial sleep job.

`vanth start` covers the core local start workflow; MCP also exposes optional
job fields and agent integrations documented above.

When wake targets are supplied (including `wake_me`), the response also carries
`wake_targets` (each resolved target with its `session_id`/`thread_id`) and
`wake_addressable` (`true` when every relay-delivered target has a destination,
`false` when one does not, `null` when there is no relay-delivered target), so
the caller can confirm exactly which session will be woken. A non-empty
`notify_on` with no `wake_targets` adds a `warnings` entry — it notifies nobody.
With no wake target at all, the response adds a `wake_recommended` advisory
pointing at `wake_me`.

By default, a local `job_start` from the MCP server asks the daemon to attach
the calling-session `wake_me` target when the caller supplied no wake, the job is
not interactive/remote/preview, its `timeout_seconds` is unset or at least
`VANTH_DEFAULT_WAKE_MIN_SECONDS` (default 60), and its `cwd` matches the caller's
directory. It is best-effort: the daemon skips it silently when no live plugin
relay resolves, so it never fails a start. Disable with `VANTH_DEFAULT_WAKE_ME=0`
in the MCP server's (agent process) environment; only that process reads it (the
daemon does not).

With `trigger` set, the job is created `queued` (no `worker_pid`) and the
response carries `"trigger"` plus a message like `"Job queued; will start when
job_A reaches completed"`. Its runner launches automatically once the parent
reaches that status; if the parent ends in a different terminal status, the
queued job is `cancelled`. `vanth stop` cancels a queued job before it fires.

On runner-launch failure: `status: "failed"`, `exit_code: 1`, `message` with
the cause. On quota exhaustion (`VANTH_MAX_RUNNING_JOBS`):
`{"result": "error", "error": "concurrent job quota reached (N running jobs)"}`.

### Readiness probes

`trigger.probe` waits for a condition instead of (or in addition to) a job
status. One probe per trigger; when a DAG gate is also present both must pass.

| Probe | Fields | Ready when |
|---|---|---|
| `port` | `host` (default `127.0.0.1`), `port` | a TCP connect succeeds |
| `http` | `url` (http/https), `expect_status` (default 200) | GET returns that status |
| `log_line` | `job_id`, `pattern`, `stream` (`stdout`/`stderr`/`all`) | the pattern is in the job's captured log tail |
| `file` | `path` | the path exists |

Any probe also accepts `timeout_seconds` (cancel the queued job if it never
becomes ready; measured from when the DAG gate is satisfied, or from queue
creation with no DAG gate) and `interval_seconds` (probe cadence, default 1).
Probes run on the daemon host over a direct connection (no proxy), bounded per
dispatcher pass. Timeout cancellation is attributed on the `cancelled` event
(`actor: "daemon"`). A `log_line` probe's `job_id` must be an existing job.

```json
{ "probe": { "type": "http", "url": "http://127.0.0.1:8080/health",
             "expect_status": 200, "timeout_seconds": 120 } }
```

---

## `job_start_and_wait`

Start a short local job, wait for its result for a bounded time, and return a
run summary in the same MCP call. It accepts the applicable core `job_start`
options; it does not support remote execution, wake targets, interactive stdin,
or triggers.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `command` | `string` | required | Shell command to run locally |
| `cwd` | `string?` | `None` | Working directory |
| `name` | `string?` | `None` | Human-readable label |
| `env` | `map<string,string>?` | `{}` | Extra environment |
| `timeout_seconds` | `int?` | `None` | Job runtime limit; `None` = no runtime timeout |
| `wait_timeout_seconds` | `int` | `20` | Bounded wait duration, 1–300 seconds |
| `tags` | `string[]?` | `None` | Arbitrary labels, filterable in `job_list` |
| `notes` | `string?` | `None` | Free-form annotation shown in the monitor |
| `secret_env` | `string[]?` | `None` | Env var names whose values are masked in captured logs/events |

**Response**

Returns `job_id` and the current `status`, plus `wait` and `summary`:

```json
{
  "job_id": "job_abc123",
  "status": "completed",
  "wait": { "result": "event", "event": { "type": "completed", "...": "..." } },
  "summary": { "job_id": "job_abc123", "status": "completed", "stderr_excerpt": "..." }
}
```

The summary includes `stderr_excerpt`, capped at 2048 bytes. If the bounded
wait expires, the response reports the timeout and the job continues running;
use the returned `job_id` with `job_wait` or `job_status` to continue. A single
call never blocks longer than `VANTH_MCP_WAIT_SLICE` (default 25 s): if
`wait_timeout_seconds` exceeds that, the call returns
`{"wait": {"result": "still_running"}, "job_id": ...}` before the MCP client's
own request timeout and you re-call to keep waiting. Do not raise the client's
timeout.

Use this tool for short local commands whose result is useful immediately. For
long-running work, use `job_start` with `wake_me` or `wake_targets` so a wake
can resume the session. Use `job_start` plus `job_wait` when you need to handle
progress or checkpoint events while the job runs.

---

## `job_rerun`

Re-launch a job with its **original** command, cwd, env, timeout, name, tags,
notes, origin thread, wake targets, and interactive flag. Any parameter may be
overridden on the re-run; omitted parameters reuse the original job's values.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string` | required | Job to re-launch |
| `command` | `string?` | `None` | Override the command |
| `env` | `map<string,string>?` | `None` | Override the environment |
| `timeout_seconds` | `int?` | `None` | Override the timeout |
| `name` | `string?` | `None` | Override the label |
| `tags` | `string[]?` | `None` | Override the tags |
| `notes` | `string?` | `None` | Override the notes |
| `cwd` | `string?` | `None` | Override the working directory |
| `interactive` | `bool?` | `None` | Override the interactive flag |

**Response**

Same shape as `job_start` (new `job_id`). Errors if `job_id` is unknown.

CLI counterpart: `vanth rerun <job_id> [options]` reuses the saved configuration
and supports selected overrides; MCP exposes the fields listed above.

---

## `job_status_batch`

Multiple jobs' status at once — fewer round trips than N `job_status` calls.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_ids` | `string[]` | required | Jobs to inspect (comma-joined on the wire) |
| `limit` | `int` | `500` | 1–1000 |

**Response**

```json
{ "jobs": [ { "job_id": "job_abc123", "status": "running", "...status fields..." } ] }
```

Each entry is a `job_status` object.

---

## `job_status`

One job's full status. The fastest way for an agent to answer "what is this
job doing?"

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string` | required | Job to inspect |

**Response**

```json
{
  "job_id": "job_abc123",
  "status": "running",
  "command": "python train.py",
  "cwd": "F:/git/project",
  "timeout_seconds": 3600,
  "pid": 4141,
  "worker_pid": 4242,
  "name": "training run",
  "origin_thread_id": "019f...",
  "wake_thread_id": null,
  "tags": ["training", "gpu"],
  "env": {"CUDA_VISIBLE_DEVICES": "0"},
  "notes": null,
  "run": {"author": "you", "hostname": "..."},
  "runtime_seconds": 12.3,
  "created_at": "2026-08-18T12:00:00Z",
  "updated_at": "2026-08-18T12:00:12Z",
  "exit_code": null,
  "last_event": { "event_id": "evt_...", "job_id": "job_abc123", "seq": 4,
                  "type": "progress", "level": "info", "message": "...",
                  "data": {}, "source": "stdout", "created_at": "..." },
  "progress": {"current": 10, "total": 100, "percent": 10.0,
               "stage": "train", "updated_at": "..."}
}
```

`progress` is the latest `progress` event's data plus `updated_at`, or `null`.

---

## `job_send`

Feed stdin to a running interactive job. Start the job with
`interactive=True` first.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string` | required | Running interactive job |
| `input` | `string` | required | Bytes to append to the job's stdin |
| `eof` | `bool` | `false` | Close the job's stdin (`eof=True` with empty `input` allowed) |

**Response**

```json
{ "job_id": "job_abc123", "sent": 3, "eof": false }
```

Errors for unknown / not-running / non-interactive jobs.

CLI counterpart: `vanth send <job_id> [--line] [--eof] [<text|->]` sends raw
input by default, appends a newline with `--line`, and reads stdin when the text
argument is `-`. `--eof` closes stdin after sending; text may be omitted when
closing stdin.

---

## `job_list`

Recent jobs, ordered by most-recently-updated.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `status` | `string[]?` | `None` | Filter by one or more statuses |
| `limit` | `int` | `50` | 1–1000 |
| `thread_id` | `string?` | `None` | Matches `origin_thread_id` or `wake_thread_id` |
| `name` | `string?` | `None` | Substring match on job name |
| `tags` | `string[]?` | `None` | Must contain all listed tags |

**Response**

```json
{ "jobs": [ { "job_id": "job_abc123", "name": null, "status": "running",
              "updated_at": "...", "origin_thread_id": "...",
              "wake_thread_id": null, "tags": [] } ] }
```

---

## `job_view`

Agent-facing summaries sorted by attention priority (failed/timeout/orphaned
first, then pending/failed deliveries, then jobs with attention events, then
the rest).

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `thread_id` | `string?` | `None` | Filter to one thread's jobs |
| `limit` | `int` | `50` | 1–1000 |

**Response**

```json
{ "jobs": [ { "job_id": "job_abc123", "status": "failed", "...status fields...",
              "delivery_counts": {"failed": 1}, "priority": 175 } ] }
```

Each entry is a `job_status` object plus `delivery_counts` and `priority`.

---

## `job_events`

Structured events for a job.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string` | required | Job whose events to read |
| `since_event_id` | `string?` | `None` | Forward paging cursor: events after this one |
| `types` | `string[]?` | `None` | Filter by event type (`progress`, `checkpoint`, `metric`, ...) |
| `limit` | `int` | `20` | 1–1000 |
| `reverse` | `bool` | `false` | Newest events first; combine with `since_event_id` to page backward |

**Response**

```json
{ "events": [ { "event_id": "evt_...", "job_id": "job_abc123", "seq": 4,
                "type": "progress", "level": "info", "message": "10/100 epochs",
                "data": {"current": 10, "total": 100}, "source": "stdout",
                "created_at": "2026-08-18T12:00:00Z" } ] }
```

---

## `job_deliveries`

Wake deliveries for a job (or across all jobs), filterable by status.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string?` | `None` | Filter to one job |
| `status` | `string?` | `None` | `pending`, `dispatching`, `retrying`, `delivered`, `failed` |
| `limit` | `int` | `20` | 1–1000 |

**Response**

```json
{ "deliveries": [ { "delivery_id": "del_...", "event_id": "evt_...",
                    "target_id": "target_...", "job_id": "job_abc123",
                    "target_type": "codex_thread", "status": "delivered",
                    "attempts": 1, "payload": {"target": {}},
                    "created_at": "...", "next_attempt_at": null,
                    "delivered_at": "...", "last_error": null,
                    "claim_token": null, "claimed_at": null,
                    "lease_expires_at": null } ] }
```

---

## `job_mark_delivery`

Manually set a delivery's status (e.g. after resolving an adapter problem).

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `delivery_id` | `string` | required | Delivery to mark |
| `status` | `string` | required | `pending`, `retrying`, `delivered`, `failed` |
| `error` | `string?` | `None` | Optional reason (recorded on the attempt) |

**Response**

The full `job_deliveries` delivery object for the updated delivery
(`attempts` incremented).

---

## `job_retry_delivery`

Requeue a delivery for dispatch immediately — including one currently
`retrying` on backoff (resets `next_attempt_at`).

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `delivery_id` | `string` | required | Delivery to requeue |

**Response**

The full delivery object with `status: "retrying"`, `next_attempt_at: null`,
`last_error: null`.

---

## `job_delivery_attempts`

Attempt/lease history for one delivery.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `delivery_id` | `string` | required | Delivery whose attempts to read |
| `limit` | `int` | `20` | 1–1000 |

**Response**

```json
{ "attempts": [ { "attempt_id": "att_...", "delivery_id": "del_...",
                  "attempt": 1, "claim_token": "...", "target_type": "codex_thread",
                  "started_at": "...", "ended_at": "...", "status": "delivered",
                  "error": null, "reclaimed": false, "created_at": "..." } ] }
```

`reclaimed: true` means the lease expired and the delivery was re-claimed after
a daemon crash ambiguity.

---

## `job_clear_deliveries`

Preview or drain matching wake deliveries when stale notifications have built
up. This MCP tool has no CLI counterpart; the HTTP endpoint is
`POST /deliveries/clear`.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string?` | `None` | Restrict to one job |
| `status` | `string?` | `None` | `pending`, `retrying`, `dispatching`, `delivered`, or `failed`; omitted means only `pending` and `retrying` |
| `older_than_seconds` | `int?` | `None` | Restrict to deliveries created before this age; must be non-negative |
| `stale_only` | `bool` | `false` | Restrict to deliveries whose source event belongs to a terminal job |
| `limit` | `int` | `1000` | Maximum rows to drain; 1–10000 |
| `dry_run` | `bool` | `true` | Preview the match count without changing deliveries |

All filters combine. Review the queue with `job_deliveries` before draining.
`matched` is the total count that passes the filters; `drained` is capped by
`limit`. With `dry_run=true`, the response reports `matched` and `drained: 0`. With
`dry_run=false`, at most `limit` matching deliveries are marked `failed` so the
queue no longer retries them automatically, with their retry/claim state
cleared. Any active attempt for a drained `dispatching` delivery is finalized as
failed. The response includes the number drained and up to
25 affected delivery IDs. Settled `delivered` or `failed` history is untouched
unless that status is explicitly selected.

**Response**

```json
{ "matched": 8, "drained": 0, "dry_run": true }
```

After a non-dry-run drain, the response also includes `ids` (up to 25 IDs) and
`dry_run: false`.

---

## `job_tail`

Bounded stdout/stderr log tail with byte offsets.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string` | required | Job whose log to read |
| `stream` | `string` | `stdout` | `stdout` or `stderr` |
| `max_bytes` | `int` | `8192` | Max bytes to read |
| `offset` | `int?` | `None` | Byte offset to start from; `None` = last `max_bytes` bytes |
| `follow` | `bool` | `false` | Block for new output until the job ends or `timeout_seconds` elapses |
| `timeout_seconds` | `int` | `5` | Follow mode cap; `None` = until the job is terminal. A follow is capped at `VANTH_MCP_WAIT_SLICE` (default 25 s) |
| `grep` | `string?` | `None` | Server-side substring filter; only lines containing it are returned |

**Response**

```json
{ "job_id": "job_abc123", "stream": "stdout", "offset": 0,
  "next_offset": 1234, "size": 4096, "truncated": false, "content": "..." }
```

`truncated` is true when the requested window was clipped to the log size or
the byte cap. Use `next_offset` to page forward. With `follow: true`, repeated
blocks append as output lands; the call returns when the job is terminal or
`timeout_seconds` is hit. A follow never blocks longer than the MCP-safe slice
(`VANTH_MCP_WAIT_SLICE`, default 25 s), so it returns partial content rather
than being cancelled with `-32001`; resume from the returned `next_offset`.
With `grep`, `content` holds only the matching lines
(and `size` reflects the full log, not the filtered window).

---

## `job_wait`

The heart of agent usage: block until the first event matching any filter is
persisted, then return it with the current status. The daemon wakes the wait
immediately — do not poll.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|---|
| `job_id` | `string` | required | Job to wait on |
| `filters` | `string[]` | required | Event types to wait for (e.g. `["checkpoint","failed","completed"]`) |
| `since_event_id` | `string?` | `None` | Only events newer than this one |
| `timeout_seconds` | `int` | `3600` | 0–86400; a single call is capped at `VANTH_MCP_WAIT_SLICE` (default 25 s) |
| `return_progress` | `bool` | `false` | Include the job's latest progress block in the response |
| `metric_ge` | `object?` | `None` | `{metric: threshold}` — return when the latest stored value reaches the threshold |

**Response**

```json
{ "result": "event", "job_id": "job_abc123", "status": "running",
  "event": { "event_id": "evt_...", "type": "checkpoint", "..." },
  "progress": {"current": 10, "total": 100, "percent": 10.0, "stage": "train",
               "updated_at": "..."} }
```

With `return_progress: true`, `progress` is the job's latest `progress` event's
data plus `updated_at` (or `null`).

With `metric_ge: {"loss": 0.5}`, the wait returns as soon as the latest stored
value of the `loss` metric series is `>= 0.5`:

```json
{ "result": "metric", "job_id": "job_abc123", "status": "running",
  "metric": "loss", "threshold": 0.5, "value": 0.62, "event": {...} }
```

Timeout: `{"result": "timeout", "job_id": ..., "status": ..., "message": "No matching event before timeout"}`.
Daemon shutdown: `{"result": "shutdown", "job_id": ..., "message": "Vanth is shutting down"}`.

When `timeout_seconds` exceeds `VANTH_MCP_WAIT_SLICE` (default 25 s), the call
returns `still_running` before the MCP client's own request timeout instead of
blocking and being cancelled with `-32001`:

```json
{ "result": "still_running", "job_id": "job_abc123", "status": "running",
  "waited_seconds": 25, "requested_timeout_seconds": 3600,
  "message": "No matching event within the MCP wait slice; call job_wait again ..." }
```

Call `job_wait` again to keep waiting (the daemon wakes the wait immediately,
so re-calls are cheap), or use `wake_me`/`wake_targets` on the job so completion
resumes the session without polling.

---

## `job_stop`

Stop a running job by terminating its process tree.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string` | required | Running job to stop |
| `signal` | `string` | `terminate` | `terminate` (graceful) or `kill` |
| `kill_after_seconds` | `int` | `10` | 0–86400; escalate to force-kill after this |
| `reason` | `string?` | `None` | Why the caller stopped it; recorded on the `cancelled` event |

**Response**

```json
{ "job_id": "job_abc123", "status": "cancelled", "message": "Job stopped" }
```

The job becomes `cancelled` only after the workload tree actually terminated;
otherwise the stop is retryable (and a `RuntimeError` "Failed to stop workload
process tree" is returned).

A stop waits at most `VANTH_MCP_WAIT_SLICE` (default 25 s) for the tree to
terminate. If the grace period is longer, it returns
`{"result": "still_running", "job_id": ..., "message": "Stop requested; ..."}`
instead of being cancelled with `-32001`; the daemon keeps stopping, so re-check
with `job_status` or wait for the `cancelled` event.

The resulting `cancelled` event carries `data: {"actor": "tool", "reason": ...}`;
`job_status` also exposes `stop_actor` / `stop_reason`. Actors are `tool` (an
MCP call), `user` (the `vanth stop` CLI / human HTTP call), `watchdog`
(recovery or heartbeat reconciliation), and `timeout` (the runner's timeout).
Use `vanth stop <id> --reason "..."` for the user-facing equivalent.

---

## `job_pause` / `job_resume`

Hold or release a **queued** job (pool- or trigger-gated) so the dispatcher
skips it. Only a job whose status is `queued` can be paused/resumed; a running
or terminal job returns an error.

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string` | required | The queued job |

Response: `{ "result": "ok", "job_id": "...", "paused": true|false }`.

---

## `job_request_decision` / `job_resolve` / `job_withdraw_decision` / `job_decisions`

Ask a human to decide something about a **non-terminal** job and wait durably
for the answer. The request is its own state machine keyed by `decision_id`
(`dec_<hex>`); the job's `status` is not changed, so a running job keeps
running while it waits.

`job_request_decision` parameters:

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string` | required | A queued/running (non-terminal) job |
| `prompt` | `string` | required | The question shown to the human |
| `options` | `string[]` | `["approve", "deny"]` | Allowed choices (deduplicated; max 50, 200 chars each) |
| `timeout_seconds` | `int` | none | Expire the request after N seconds |

`prompt` is limited to 10000 characters. The request, its `decision_requested`
event and any wake deliveries commit in one transaction; the serialized
payload is checked against the event byte limit, and authoritative decision
transitions are exempt from the per-job structured-event cap.

Requesting emits a `decision_requested` event, which reuses the wake-target
delivery path — the job's wake targets are notified, so the owning thread
learns a human is needed. Only targets whose `events` list includes
`decision_requested` are woken. Response: the decision object
(`status: "pending"`).

```json
{ "result": "ok", "decision_id": "dec_1f2e...", "job_id": "job_ab12...",
  "prompt": "Ship the release?", "options": ["approve", "deny"],
  "choice": null, "status": "pending", "resolved_by": null,
  "created_at": "2026-09-15T10:00:00Z", "expires_at": null, "resolved_at": null }
```

Wait for the answer with
`job_wait(job_id, ["decision_resolved"])`, or poll `job_decisions`.

- `job_resolve(job_id, token, choice)` records one of the request's `options`
  and emits `decision_resolved`. Resolving the same choice twice is
  idempotent; a different choice is an error; a resolved/withdrawn/expired
  decision cannot be resolved.
- `job_withdraw_decision(job_id, token)` cancels a pending request (emits
  `decision_withdrawn`).
- When `timeout_seconds` elapses the daemon marks the decision `expired`
  (emits `decision_expired`) and it can no longer be resolved.
- `job_decisions(job_id=None, status=None, limit=50)` lists decisions newest
  first; `status` is one of `pending`, `resolved`, `withdrawn`, `expired`.

```json
{ "decisions": [ { "decision_id": "dec_1f2e...", "status": "resolved",
                   "choice": "approve", "resolved_by": "user" } ], "count": 1 }
```

---

## `pool_configure` / `pool_list`

`pool_configure(pool, max_parallel=0, paused=None)` upserts a concurrency pool.
`max_parallel` `0` means unlimited; `paused` holds every queued job in the pool
(omit to leave it unchanged). `pool_list()` returns each pool with its
`queued` and `running` counts.

```json
{ "pools": [ { "pool": "gpu", "max_parallel": 1, "paused": false, "queued": 3, "running": 1 } ] }
```

---

## `schedule_create` / `schedule_list` / `schedule_update` / `schedule_delete` / `schedule_next`

A schedule launches a fresh job per fire. Pass **exactly one** of `cron` or
`interval_seconds`.

`schedule_create` parameters:

| Param | Type | Default | Notes |
|---|---|---|---|
| `command` | `string` | required | Job command |
| `cron` | `string?` | `None` | 5-field cron or an `@daily`-style macro |
| `interval_seconds` | `int?` | `None` | Fixed interval |
| `name` | `string?` | `None` | Label (also the job name) |
| `timezone_name` | `string` | `UTC` | IANA name for cron matching |
| `cwd` | `string?` | `None` | Job working directory |
| `env` | `map?` | `None` | Job env |
| `timeout_seconds` | `int?` | `None` | Per-run timeout |
| `tags` | `string[]?` | `None` | Job tags (`scheduled` is added) |
| `notes` | `string?` | `None` | Job notes |
| `secret_env` | `string[]?` | `None` | Masked env values (see `job_start`) |
| `overlap` | `string` | `skip` | `skip` (skip the fire while a run is active) or `allow` |
| `enabled` | `bool` | `true` | `false` parks the schedule |

The response is the schedule object: `schedule_id`, `next_fire_at` (UTC),
`last_fired_at`, `fire_count`, `enabled`, and the template fields.

`schedule_update(schedule_id, changes={...})` edits any of the above in place
and recomputes `next_fire_at`. `schedule_delete(schedule_id)` removes the
schedule (already-created jobs are untouched). `schedule_next(schedule_id,
count=5)` previews the next fire times.

---

## `job_doctor`

Daemon health report.

**Parameters**

None.

**Response**

```json
{
  "ok": true,
  "home": "C:/Users/you/.vanth",
  "db_path": "...", "logs_dir": "...", "events_dir": "...",
  "tables": ["jobs", "events", "..."],
  "delivery_counts": {"pending": 0, "delivered": 3},
  "codex": {"command": "codex", "available": true},
  "opencode": {"command": "opencode", "available": true},
  "schema_version": 15,
  "quick_check": "ok",
  "maintenance_alive": true,
  "stale_delivery_leases": 0,
  "dead_letter_count": 0,
  "dead_lettered": [],
  "running_jobs": 1, "max_running_jobs": 0,
  "retention": {"seconds": 0, "interval_seconds": 3600, "dry_run": true},
  "disk_free_bytes": 123456789,
  "token_path": "C:/Users/you/.vanth/token",
  "warnings": []
}
```

`ok` is false when there are warnings (e.g. missing tables, Codex/OpenCode
unavailable) or `quick_check != ok`. `dead_lettered` lists up to 20 deliveries
that exhausted `max_attempts`, each with `delivery_id`, `job_id`, `attempts`,
`last_error`. The token is never revealed.

---

## `job_cleanup`

Dry-run or real removal of old terminal jobs. Running jobs are never selected.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `older_than_seconds` | `int` | required | Non-negative cutoff age |
| `dry_run` | `bool` | `true` | Preview only (fully read-only); `false` deletes |

**Response**

```json
{ "dry_run": true, "older_than_seconds": 86400,
  "jobs": ["job_old1", "job_old2"], "count": 2 }
```

With `dry_run: false`, removes logs, event mirrors, specs, deliveries,
attempts, wake targets, events, stdin channels, and the job row (tombstoned,
idempotent, safe to repeat).

---

## `job_metrics_query`

Read stored scalar metric series for a job (loss, accuracy, `progress.percent`,
...). The read side of the terminal monitor's data.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string` | required | Job whose series to read |
| `metric` | `string?` | `None` | One series name; omit for all metrics |
| `from_ms` | `int?` | `None` | Epoch-ms lower bound |
| `to_ms` | `int?` | `None` | Epoch-ms upper bound |
| `limit` | `int` | `1000` | Up to 10000 |

**Response**

```json
{ "job_id": "job_abc123",
  "series": { "loss": [ { "x": 0, "y": 0.5, "stage": "train",
                          "event_id": "evt_...", "seq": 1, "at": "..." } ] },
  "metrics": ["loss"] }
```

---

## `job_metric_compare`

Compare one metric across jobs — the "which run won?" primitive.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_ids` | `string[]` | required | Non-empty, at most 50 |
| `metric` | `string` | required | Metric name |
| `aggregation` | `string` | `latest` | `latest`, `mean`, `min`, `max`, `sum`, `count` |
| `from_ms` | `int?` | `None` | Epoch-ms lower bound |
| `to_ms` | `int?` | `None` | Epoch-ms upper bound |

**Response**

```json
{ "metric": "val_loss", "aggregation": "min",
  "jobs": { "job_a": { "value": 0.41, "points": 5,
                       "first": { "x": 0, "y": 0.8, "..." },
                       "last": { "x": 4, "y": 0.41, "..." } } } }
```

`value` is `null` when a job has no points for the metric.

---

## `job_duration_stats`

Duration, queue-time, and flakiness analytics grouped by logical job.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `name` | `string?` | `None` | Substring filter on job name |
| `tags` | `string[]?` | `None` | All tags must be present |
| `limit` | `int` | `20` | Max groups (1–200) |
| `runs_per_group` | `int` | `200` | Max recent runs considered per group (1–5000) |
| `since_ms` | `int?` | `None` | Only runs ending after this epoch-ms |
| `slowest` | `int` | `10` | Slowest runs per group and overall (1–100) |

**Response**

```json
{
  "group_count": 3,
  "groups": [
    { "key": "nightly backup", "runs": 42, "completed": 40, "failed": 2,
      "success_rate": 0.9524,
      "duration_seconds": { "p50": 120.5, "p95": 900.2, "mean": 210.0, "min": 60.0, "max": 1800.0 },
      "queue_seconds": { "p50": 0.4, "p95": 12.0 },
      "flaky_score": 0.0476, "flaky_runs": 2,
      "trend": { "direction": "regressing", "factor": 1.8, "recent_p50": 2400.0, "baseline_p50": 1300.0 },
      "slowest_runs": [ { "job_id": "job_...", "status": "completed", "duration_seconds": 1800.0, "..." } ],
      "last_run": { "job_id": "job_...", "status": "completed", "duration_seconds": 122.0, "..." } }
  ],
  "slowest": [ { "key": "nightly backup", "job_id": "job_...", "duration_seconds": 1800.0, "..." } ]
}
```

`trend.direction` is `unknown` with fewer than 6 runs; `regressing`/`improving`
require a recent/older p50 ratio of ≥1.5x / ≤0.67x. `flaky_score` is the
fraction of runs that failed with a success both before and after them.

---

## `job_run_summary`

One-call "did it work?" — status, runtime, progress, metric overview, artifacts.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string` | required | Job to summarize |
| `include_stderr_excerpt` | `bool` | `false` | Add `stderr_excerpt`, capped at 2048 bytes; omitted by default |

**Response**

```json
{
  "job_id": "job_abc123", "status": "completed", "name": "training run",
  "runtime_seconds": 123.4, "exit_code": 0,
  "progress": {"current": 100, "total": 100, "percent": 100.0},
  "notes": null,
  "metrics": [ { "metric": "loss", "latest": 0.1, "first": 0.9,
                 "min": 0.1, "max": 0.9, "count": 10, "stage": "train" } ],
  "latest_metrics": {"loss": 0.1},
  "stderr_excerpt": "...",
  "artifacts": [ { "artifact_id": "art_...", "name": "best.pt", "uri": "file:///...", "..." } ]
}
```

`stderr_excerpt` is present only when `include_stderr_excerpt=true` and contains
at most the last 2048 bytes of captured stderr.

---

## `job_diff`

Compare the run specs of two jobs — e.g. a job vs its rerun, or two pipeline
stages — and see exactly what changed (command, env, cwd, timeout, name, tags,
wake targets).

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `base_job_id` | `string` | required | Reference job |
| `other_job_id` | `string` | required | Job to compare against |

**Response**

```json
{
  "base_job_id": "job_abc", "other_job_id": "job_xyz",
  "identical": false,
  "changes": [
    { "field": "command", "base": "train.py --lr 0.1", "other": "train.py --lr 0.01" },
    { "field": "env", "changes": [ { "key": "SEED", "base": "1", "other": "42" } ] }
  ]
}
```

`identical: true` with `changes: []` when nothing differs. CLI: `vanth diff <job> <other>`.

---

## `job_artifact_add`

Attach an artifact (checkpoint, CSV, rendered output) to a job.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string` | required | Job to attach to |
| `name` | `string` | required | Artifact name (non-empty) |
| `uri` | `string` | required | Where the artifact lives (non-empty) |
| `kind` | `string?` | `None` | e.g. `checkpoint`, `csv`, `output` |
| `size_bytes` | `int?` | `None` | Optional size |
| `sha256` | `string?` | `None` | Optional content hash |
| `meta` | `object?` | `{}` | Free-form JSON |

**Response**

```json
{ "artifact_id": "art_...", "job_id": "job_abc123", "name": "best.pt",
  "uri": "file:///...", "kind": "checkpoint", "size_bytes": 42,
  "sha256": "...", "meta": {"epoch": 5}, "created_at": "..." }
```

---

## `job_artifacts`

List artifacts attached to a job.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string` | required | Job whose artifacts to list |
| `limit` | `int` | `50` | Up to 1000 |

**Response**

```json
{ "artifacts": [ { "artifact_id": "art_...", "job_id": "job_abc123",
                   "name": "best.pt", "uri": "file:///...", "kind": "checkpoint",
                   "size_bytes": 42, "sha256": "...", "meta": {},
                   "created_at": "..." } ] }
```

---

## `job_dashboard`

Chart-data view for any renderer: every stored metric series (downsampled per
job) plus the job list — the same data the Go terminal monitor charts.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_ids` | `string[]?` | `None` | Jobs to chart; omit for all (max 50) |
| `limit` | `int` | `5000` | Max points per series, up to 50000 |

**Response**

```json
{ "jobs": [ {"job_id": "job_abc123", "name": null, "status": "running", "..."} ],
  "series": { "job_abc123": { "loss": [ {"x": 0, "y": 0.5, "..."} ] } },
  "series_count": 3 }
```

---

## Remote execution

Jobs can run on a paired remote host. Discover hosts with `remote_list` (the
CLI also has `vanth remote list`); pairing and host administration are CLI
operations (`vanth remote pair user@host`). Pass the returned `remote_id` to
the MCP tool that acts on that host:

| Tool | Remote behaviour |
|---|---|
| `job_start(remote_id=...)` | Run the job on the host instead of locally |
| `job_list(remote_id=...)` | That host's jobs, from the controller's shadow; only `limit` is supported (other filters are rejected rather than silently dropped) |
| `job_status(job_id, remote_id=...)` | Live status from the host |
| `job_tail(job_id, remote_id=...)` | A single byte range of the host's log; `follow` and `grep` are rejected |
| `job_wait` / `job_stop` / `job_rerun` | Bounded wait / stop / rerun on the host |
| `remote_doctor(remote_id=...)` | SSH availability and per-host state |

**Remote mutations require a caller-supplied `idempotency_key`** (8-128 chars of
`[A-Za-z0-9_-]`) so a lost response is safe to retry; the daemon rejects a
missing one. Local starts are the opposite: they must NOT pass a key.
`artifact_push_remote` / `artifact_pull_remote` move artifact versions the same
way. Over HTTP the equivalents are `POST /jobs` with `remote_id`,
`GET /remotes/{id}/jobs`, `GET /remotes/{id}/status/{job}`, and
`GET /remotes/{id}/jobs/{job}/tail` (`vanth api` lists them).

---

## Additional shipped tools

The tools in this section are registered in the current server release.

### `job_metric_ingest`

Write scalar metric points into a job's metric series programmatically
(complementing the read-only `job_metrics_query`).

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string` | required | Job whose series to extend |
| `metrics` | `object[]` | required | One or more points: `{name, value, ts_ms?, labels?}` |
| `idempotency_key` | `string?` | `None` | Replay protection — a repeated key is a no-op |

**Response**

```json
{ "job_id": "job_abc123", "ingested": 2, "idempotency_key": "..." }
```

### `job_artifact_read`

Read a stored job artifact's metadata and, when it is a local file, contents.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `artifact_id` | `string` | required | Artifact to read |
| `max_bytes` | `int` | `262144` | Maximum local-file content bytes returned |

**Response**

```json
{ "artifact_id": "art_...", "job_id": "job_abc123", "name": "best.pt",
  "uri": "file:///...", "kind": "checkpoint", "size_bytes": 42,
  "sha256": "...", "meta": {}, "content": "...", "truncated": false }
```

### `job_add_wake_target` (MCP + Python)

Register a wake target against a job for **future** events. This only fires
when the triggering event occurs **after** registration — it does NOT surface
a wake for an event that already happened. Use `job_wake_now` for that.

CLI counterpart: `vanth wake <job_id>` adds a target after start; use
`vanth start --wake-me -- <command>` to create the default OpenCode wake at
start time. `vanth deliveries --status pending` or `--status failed` diagnoses
wakes that will not fire.

Pass a full target dict as `target` (`{"type", "events", ...config}`), or use
the shorthand: `type` (required, one of `local_command` / `codex_cli_thread` /
`codex_thread` / `codex_desktop` / `opencode_thread` / `webhook`) plus optional
`events` and `config` (a single object holding the extra target config). Events
default to `["completed", "failed"]`.

`codex_cli_thread` / `codex_thread` / `codex_desktop` targets inherit the calling
Codex task's thread id (resolved by the MCP wrapper from `CODEX_THREAD_ID` /
`VANTH_CODEX_DESKTOP_THREAD`); an explicit `thread_id` always wins.
`codex_desktop` wakes a RUNNING Desktop task through its native app-tools host
pipe — it requires the Desktop integration to be provisioned (run `vanth setup
desktop` inside a Desktop session, or set `VANTH_CODEX_DESKTOP_PIPE` /
`VANTH_CODEX_DESKTOP_THREAD`) and fails closed (never falls back to the CLI
thread bridge) when it is not. It does not support arbitrary historical or
unloaded Desktop threads: the private host may accept those sends without
producing a usable turn. Use `codex_cli_thread` for an unloaded persisted task.

`opencode_thread` targets may omit `session_id`: Vanth then resolves the newest
registered plugin relay for the job's `cwd`, and the in-process plugin injects
the wake into the TUI you are watching. If no relay is registered for that cwd,
creation fails with an actionable error. An explicit `session_id` (`ses_...`,
from `opencode session list`) always wins — use it, **not** the relay client id
`opencode-<pid>-<rand>` that may appear as a relay client id in diagnostics (a
target naming a client id is rejected, since the relay matches on the destination
session and would never claim it). Use `vanth doctor --json` for the full
relay/session details, or let `wake_me` resolve the live destination. `attach` is optional and only needed for a
headless `opencode serve` (an explicit `session_id` plus the server URL);
without a plugin relay and without `attach` there is no visible client to wake.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `job_id` | `string` | required | Job to register the wake target on |
| `target` | `object?` | `None` | Full wake-target dict (`{type, events, ...config}`); given, used as-is |
| `events` | `string[]?` | `["completed","failed"]` | Shorthand; non-empty list of event types (e.g. `["checkpoint"]`) |
| `type` | `string?` | required (shorthand) | One of `local_command` / `codex_cli_thread` / `codex_thread` / `codex_desktop` / `opencode_thread` / `webhook` |
| `config` | `object?` | `{}` | Extra target config (e.g. `{"command": ..., "thread_id": ..., "session_id": ..., "attach": ...}`) merged into the shorthand target |

**Response**

```json
{ "result": "ok", "job_id": "job_abc123", "target_id": "target_...",
  "target_type": "local_command", "events": ["completed", "failed"] }
```

Errors: `ValueError` when `events` is empty or not a list of strings, or
`type` is empty.

### `job_wake_now` (MCP + Python)

Surface a wake **immediately**, even if the triggering event already fired.
This is the genuine "wake now" operation: it registers the target AND enqueues
a synthetic delivery right away, so the wake reaches the target session without
waiting for a matching event. Same target contract as `job_add_wake_target`;
`opencode_thread` targets may omit `session_id` when a plugin relay is
registered for the job's cwd (`vanth doctor` summarizes relays; `vanth doctor
--json` gives the full relay/session list),
**not** the relay client id `opencode-<pid>-<rand>`; otherwise supply the
`ses_...` session id.

**Response**

```json
{ "result": "ok", "job_id": "job_abc123", "target_id": "target_...",
  "target_type": "local_command", "events": ["completed", "failed"],
  "woken": true, "synthetic_event_type": "wake_now", "requested_status": "running" }
```

Note the response contract (review rc36 P2): `synthetic_event_type` is always
the literal `"wake_now"` — wake_now never fabricates a `"completed"`/`"failed"`
event for a job that is still running or already failed.

### `daemon_wake` (deprecated)

Kept for backward compatibility. Alias of `job_add_wake_target` — it registers
a wake target for future events only. Use `job_wake_now` to surface a wake
immediately, or `job_add_wake_target` to register a target.

The Python functions keep the ORIGINAL signature
`(job_id, target=None, events=None, type=None, **config)` — extra target config
is passed as plain keyword arguments (e.g. `daemon_wake(job_id,
type="local_command", command=...)`). The MCP surface is registered under the
external names `daemon_wake` / `job_wake_now` / `job_add_wake_target` (the rc37
contract, restored in rc39) using explicit-signature config adapters — FastMCP
cannot bind `**config`, so the MCP adapter takes an explicit `config` dict while
keeping the documented tool name. Agents never see `mcp_`-prefixed names.

### `job_cleanup_preview`

A dedicated dry-run retention preview: reports exactly what would be removed
without deleting anything. Separate from the destructive `job_cleanup`.

**Parameters**

| Param | Type | Default | Notes |
|---|---|---|---|
| `older_than_seconds` | `int` | required | Cutoff age |

**Response**

```json
{ "dry_run": true, "older_than_seconds": 86400,
  "jobs": [ {"job_id": "job_old1", "status": "completed", "updated_at": "...",
             "size_bytes": 1024} ],
  "total_size_bytes": 2048, "count": 2 }
```
