# Production hardening and agent usability

Status: fixes, sub-agent review, and local validation complete. The two remaining items are blocked on external infrastructure (no disposable SSH target; no Linux/macOS runner) rather than on code. Existing workspace changes preserved.

## Correctness

- [x] Restore SQLite snapshots while holding the daemon lock; remove stale WAL/SHM files safely; refuse detached live jobs and failed safety backups.
- [x] Hash backup entries from the same snapshot that is archived; fence artifact catalog/blob snapshots against GC.
- [x] Bound output-pipe draining and handle descendants after workload exit.
- [x] Mask multiline secrets and secrets spanning read boundaries, including Windows text translation.
- [x] Preserve ordinary long log lines independently of structured-event limits.
- [x] Handle log write/disk-full failures without stranding workloads.
- [x] Serialize concurrent interactive sends and EOF; recover EOF marker-publication crashes.
- [x] Preserve raw events when huge numeric values overflow metric/progress conversion.
- [x] Avoid recycled PID fallback when a POSIX workload leader has exited.
- [x] Atomically persist terminal state, events, and wakes; roll back exhausted writes.
- [x] Bound Windows process cleanup calls, including stale-claim cleanup while holding a write transaction.
- [x] Repair corrupt existing artifact blobs on republish.
- [x] Stage file materialization beside the destination for cross-volume support.
- [x] Use a GC fence that cannot be stolen from a live holder or released by a previous owner.
- [x] Fix high-concurrency event ingestion and pass the original 50-job burst workload.
- [x] Resolve exact CLI job IDs regardless of retained history size.
- [x] Return nonzero on confirmed startup failure.
- [x] Preserve literal send input and support empty line input.
- [x] Update the outdated CLI help assertion.

## Agent usability

- [x] Add durable local start idempotency with request-conflict detection and restart/concurrency tests.
- [x] Include bounded stdout in start-and-wait results.
- [x] Return structured failure reasons and recommended next actions in confirmation, status, and summaries.
- [x] Confirm rerun startup consistently with start.
- [x] Add a start preview for shell, cwd, wake destination, queue gates, and policy without launching work.
- [x] Add health diagnostics for artifact corruption, pipe draining, and event-ingestion contention.

## Production validation and release gates

- [x] Require Python checks on pushes and passing validation before publication.
- [x] Assert actual adapter delivery-ID roundtrips; explicitly report skipped live smoke tests.
- [x] Add Windows/macOS chaos coverage and supported Go race testing.
- [x] Add disk-full, permissions, terminal-persistence contention, and crash-boundary regression tests.
- [x] Add a real SSH disconnect/reconnect/replay/artifact-transfer integration harness.
- [x] Add a sustained soak test checking completeness, latency, memory, and process leaks.
- [ ] (blocked) Run live SSH and adapter delivery checks against configured disposable targets.
- [ ] (blocked) Confirm Linux/macOS matrix outcomes in CI (local validation is Windows).
- [x] Review every sub-agent change and run targeted regression tests.
- [x] Run the full Python suite, Go checks, chaos matrix, soak, and installed-wheel smoke.

## Validation notes

Record commands, results, limitations, and any externally blocked checks here before completion.

- Go tests and vet passed; Windows Go race testing passed with CGO and the resident GCC compiler.
- Backup regressions: 21 passed; real F: to C: artifact materialization and overwrite verified (1,081,344 bytes).
- Fresh installed-wheel job execution, bounded stdout, retry idempotency, and bundled monitor smoke passed. Bypass uv's cached environment when rebuilding the same wheel version locally.
- Live SSH is explicitly skipped without a disposable target, fingerprint, and remote interpreter/helper paths.
- Combined runner and real HTTP/MCP usability regressions: 35 passed. Additional secret text-translation and bounded catalog regressions added.
- Atomic terminal persistence regressions cover actual lock exhaustion and failure after event insertion.
- Initial Python 3.12 full run: 1,148 passed, 7 skipped, one immediate-stop timeout failure. Corrected the bound and passed its focused regression.
- Final Python 3.11 full run: 1,149 passed, 7 skipped (957.18 seconds). Fresh installed-wheel execution also passed on Python 3.11 and 3.12.
- Chaos matrix: all nine scenarios passed, including 50 jobs with 500 events per stream (50,000 metric events, 50,140 durable rows including lifecycle/contention diagnostics; sequences verified).
- Corrected the sustained harness's Windows multiline-command quoting and launcher-versus-worker RSS sampling. Fourteen harness tests passed; real nested smoke verified two jobs and all six events, with no runner leaks.

- Final 600-second soak passed: 752 jobs, 37,600 expected events, zero errors or runner leaks. Event observation lag p95 110 ms, maximum 927 ms; job latency p95 1.641 seconds; manager RSS growth 16.0 MiB. Successful soak home removed.
- Automatic approval review rejected cleanup of the initial failed-soak diagnostic folder with 'blocked by policy'; that folder is retained for inspection.
- Reload the daemon and MCP server to activate the changed API and tool schemas; the running production daemon was not restarted during validation.
- Full Python suite re-run on the finalized tree immediately before commit: 1151 passed, 7 skipped (1078.35 s).
- The two blocked items stay unchecked until a disposable SSH host (see `docs/real-ssh-validation.md`) and a Linux/macOS CI run are available.
