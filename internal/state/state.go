// Package state owns SQLite access and typed queries for the native Vanth port.
// Compatibility rule: every database this package opens or writes must
// round-trip through Python's sqlite3, and vice versa.
package state

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

// DefaultBusyTimeoutMS mirrors src/vanth/migrations.py DEFAULT_BUSY_TIMEOUT_MS.
const DefaultBusyTimeoutMS = 30000

// LatestSchemaVersion is the current schema version (matches migrations.py).
const LatestSchemaVersion = 20

// Open opens the database read-write and applies the shared connection policy.
func Open(path string) (*sql.DB, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(%d)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)",
		path, DefaultBusyTimeoutMS)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// OpenReadOnly opens the database read-only for the monitor. It must never
// request a journal-mode change.
func OpenReadOnly(path string) (*sql.DB, error) {
	return OpenReadOnlyBusy(path, DefaultBusyTimeoutMS)
}

// OpenReadOnlyBusy opens the database read-only with a caller-selected busy
// timeout in milliseconds. The monitor uses a short timeout so a locked
// database degrades to a transient refresh miss instead of blocking the TUI.
func OpenReadOnlyBusy(path string, busyMS int) (*sql.DB, error) {
	dsn := fmt.Sprintf("file:%s?mode=ro&_pragma=busy_timeout(%d)&_pragma=query_only(1)", path, busyMS)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// SchemaVersion returns PRAGMA user_version.
func SchemaVersion(db *sql.DB) (int, error) {
	var version int
	if err := db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		return 0, err
	}
	return version, nil
}

// JobRow is a typed subset of the jobs table used across the port.
type JobRow struct {
	JobID         string
	Name          sql.NullString
	Command       string
	Status        string
	Pid           sql.NullInt64
	WorkerPid     sql.NullInt64
	StopRequested sql.NullString
	CreatedAt     string
	UpdatedAt     string
	ExitCode      sql.NullInt64
	TagsJSON      string
	EnvJSON       string
	Notes         sql.NullString
	RunJSON       string
	StdoutPath    string
	StderrPath    string
	EventsPath    string
}

// Job returns one job row by ID.
func Job(db *sql.DB, jobID string) (JobRow, error) {
	row := db.QueryRow(`
		SELECT job_id, name, command, status, pid, worker_pid, stop_requested_at,
		       created_at, updated_at, exit_code, tags_json, env_json, notes, run_json,
		       stdout_path, stderr_path, events_path
		FROM jobs WHERE job_id=?`, jobID)
	var j JobRow
	if err := row.Scan(&j.JobID, &j.Name, &j.Command, &j.Status, &j.Pid, &j.WorkerPid,
		&j.StopRequested, &j.CreatedAt, &j.UpdatedAt, &j.ExitCode,
		&j.TagsJSON, &j.EnvJSON, &j.Notes, &j.RunJSON,
		&j.StdoutPath, &j.StderrPath, &j.EventsPath); err != nil {
		return JobRow{}, err
	}
	return j, nil
}
