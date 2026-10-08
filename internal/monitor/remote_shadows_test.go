package monitor

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func seedShadowDB(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "remote.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`
		CREATE TABLE remotes (
		  remote_id TEXT PRIMARY KEY, name TEXT, target TEXT NOT NULL, state TEXT NOT NULL,
		  state_epoch INTEGER NOT NULL DEFAULT 1, controller_id TEXT, credential_state TEXT,
		  pairing_state TEXT, key_path TEXT, known_hosts_path TEXT, installed_at TEXT,
		  snapshot_cursor_json TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
		);
		CREATE TABLE remote_shadows (
		  shadow_id TEXT PRIMARY KEY, remote_id TEXT NOT NULL, remote_job_id TEXT NOT NULL,
		  status TEXT NOT NULL, payload_json TEXT, state_epoch INTEGER NOT NULL DEFAULT 1,
		  suppressed_at TEXT, superseded_at TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
		);
		INSERT INTO remotes(remote_id, target, state, state_epoch, created_at, updated_at)
		VALUES ('rmt_a', 'user@host', 'paired', 2, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z');
		INSERT INTO remote_shadows(shadow_id, remote_id, remote_job_id, status, payload_json, state_epoch, created_at, updated_at)
		VALUES ('shd_1', 'rmt_a', 'job_live', 'running', '{"name":"train","command":"python train.py"}', 2, '2026-01-01T00:00:01Z', '2026-01-01T00:00:02Z');
		INSERT INTO remote_shadows(shadow_id, remote_id, remote_job_id, status, payload_json, state_epoch, created_at, updated_at)
		VALUES ('shd_2', 'rmt_a', 'job_old_epoch', 'completed', '{}', 1, '2026-01-01T00:00:03Z', '2026-01-01T00:00:04Z');
		INSERT INTO remote_shadows(shadow_id, remote_id, remote_job_id, status, payload_json, state_epoch, suppressed_at, created_at, updated_at)
		VALUES ('shd_3', 'rmt_a', 'job_forgotten', 'running', '{}', 2, '2026-01-01T00:00:05Z', '2026-01-01T00:00:05Z', '2026-01-01T00:00:05Z');
		INSERT INTO remote_shadows(shadow_id, remote_id, remote_job_id, status, payload_json, state_epoch, superseded_at, created_at, updated_at)
		VALUES ('shd_4', 'rmt_a', 'job_superseded', 'failed', '{}', 1, '2026-01-01T00:00:06Z', '2026-01-01T00:00:06Z', '2026-01-01T00:00:06Z');
	`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	return path
}

func TestLoadRemoteShadowsFiltersCurrentTimeline(t *testing.T) {
	path := seedShadowDB(t)
	rows, err := LoadRemoteShadows(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1 (only the live current-epoch shadow): %+v", len(rows), rows)
	}
	row := rows[0]
	if row.JobID != "job_live" || row.Status != "running" || row.RemoteID != "rmt_a" {
		t.Fatalf("unexpected row: %+v", row)
	}
	if row.Name != "train" || row.Command != "python train.py" {
		t.Fatalf("payload extraction failed: %+v", row)
	}
}

func TestProjectShadowsSummaries(t *testing.T) {
	summaries := ProjectShadows([]ShadowRow{{
		RemoteID: "rmt_a", JobID: "job_live", Status: "queued",
		Name: "train", Command: "python train.py",
		CreatedAt: "2026-01-01T00:00:01Z", UpdatedAt: "2026-01-01T00:00:02Z", StateEpoch: 2,
	}})
	if len(summaries) != 1 {
		t.Fatalf("got %d summaries", len(summaries))
	}
	s := summaries[0]
	if !s.IsRemote() || s.Location() != "rmt_a" {
		t.Fatalf("shadow flags wrong: %+v", s)
	}
	// A queued shadow renders as submitting (no local PID exists).
	if s.Status != "queued" {
		t.Fatalf("status changed unexpectedly: %s", s.Status)
	}
	local := JobSummary{JobID: "job_local", Status: "running"}
	if local.IsRemote() || local.Location() != "local" {
		t.Fatalf("local summary misprojected: %+v", local)
	}
}

func TestExtractPayloadStringDecodesEscapes(t *testing.T) {
	// Payloads with JSON escapes must decode fully: the old substring scan
	// stopped at the first quote even when escaped, and never decoded
	// backslashes, newlines, or unicode escapes.
	payload := `{"name":"a\"b\\c","command":"echo hi","note":"line1\nline2 ☃"}`
	if got := extractPayloadName(payload); got != `a"b\c` {
		t.Fatalf("name = %q, want %q", got, `a"b\c`)
	}
	if got := extractPayloadCommand(payload); got != "echo hi" {
		t.Fatalf("command = %q", got)
	}
	if got := extractPayloadString(payload, "note"); got != "line1\nline2 ☃" {
		t.Fatalf("note = %q", got)
	}
	if got := extractPayloadString("not json", "name"); got != "" {
		t.Fatalf("garbage payload should yield empty, got %q", got)
	}
	if got := extractPayloadString(`{"name":42}`, "name"); got != "" {
		t.Fatalf("non-string field should yield empty, got %q", got)
	}
}
