package monitor

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"vanth/internal/state"
)

// SourceMode labels where the current snapshot came from.
const (
	ModeSQLite   = "sqlite"
	ModeFallback = "fallback"
)

// RefreshRequest is the immutable snapshot a refresh command captures before
// doing I/O.
type RefreshRequest struct {
	Targets   []string // job IDs to load events for (selected + pinned)
	LastSeq   map[string]int64
	KnownJobs []JobSummary // last known job list, used when SQLite is down
}

// RefreshResult is the immutable result of one refresh.
type RefreshResult struct {
	Gen       int
	Jobs      []JobSummary
	JobsOK    bool
	Events    map[string][]Event
	Points    map[SeriesKey][]Point
	LastSeq   map[string]int64
	Warnings  int
	Malformed int
	Mode      string // ModeSQLite or ModeFallback
	Stale     bool   // SQLite busy/locked; caller keeps the last frame
	Fallback  bool   // events served from JSONL mirrors
	Empty     bool   // no database and no event mirrors found
	DBOpen    bool
	Err       error
}

// Querier owns the read-only SQLite connection plus JSONL fallback access. It
// is used only from command goroutines; a mutex keeps the lazy handle safe.
type Querier struct {
	cfg Config

	mu    sync.Mutex
	db    *sql.DB
	dbErr error
}

// NewQuerier returns a Querier for the given configuration.
func NewQuerier(cfg Config) *Querier {
	return &Querier{cfg: cfg}
}

// Close releases the read-only database handle.
func (q *Querier) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.db != nil {
		q.db.Close()
		q.db = nil
	}
}

// open returns the cached read-only handle, retrying a previously failed open
// so transient lock contention or a database that appears later recovers.
func (q *Querier) open(ctx context.Context) (*sql.DB, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.db != nil {
		return q.db, nil
	}
	db, err := state.OpenReadOnlyBusy(q.cfg.DBPath, q.cfg.DBBusyTimeoutMS)
	if err != nil {
		q.dbErr = err
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		q.dbErr = err
		return nil, err
	}
	q.db = db
	q.dbErr = nil
	return q.db, nil
}

// Refresh loads a bounded job list plus incremental events for the requested
// jobs. It never writes.
func (q *Querier) Refresh(ctx context.Context, req RefreshRequest) RefreshResult {
	res := RefreshResult{
		LastSeq: map[string]int64{},
		Mode:    ModeSQLite,
	}
	db, err := q.open(ctx)
	if err != nil {
		if isBusy(err) {
			res.Stale = true
			return res
		}
		res.Fallback = true
		res.Mode = ModeFallback
		if len(req.KnownJobs) == 0 {
			res.Jobs = q.discoverJobs()
			res.JobsOK = true
			if len(res.Jobs) == 0 {
				res.Empty = true
			}
		}
		if len(req.Targets) > 0 {
			q.loadEventsJSONL(req, &res)
		}
		return res
	}
	res.DBOpen = true
	if err := q.loadJobs(ctx, db, &res); err != nil {
		if isBusy(err) {
			res.Stale = true
		} else {
			res.Fallback = true
			res.Mode = ModeFallback
		}
	}
	// Remote shadow projection (review P1-7): merge current-timeline shadows
	// so remote jobs appear alongside local ones instead of the loader being
	// dead code. Failures are non-fatal (monitor still shows local state).
	if !res.Stale && q.cfg.RemoteDBPath != "" {
		if _, statErr := os.Stat(q.cfg.RemoteDBPath); statErr == nil {
			if shadowRows, err := LoadRemoteShadows(q.cfg.RemoteDBPath); err == nil {
				res.Jobs = append(res.Jobs, ProjectShadows(shadowRows)...)
			} else {
				res.Warnings++
			}
		}
	}
	if !res.Stale && len(req.Targets) > 0 {
		if err := q.loadEvents(ctx, db, req, &res); err != nil {
			if isBusy(err) {
				res.Stale = true
			} else {
				res.Fallback = true
				res.Mode = ModeFallback
			}
		}
	}
	if res.Fallback && len(req.Targets) > 0 {
		q.loadEventsJSONL(req, &res)
	}
	return res
}

func (q *Querier) loadJobs(ctx context.Context, db *sql.DB, res *RefreshResult) error {
	const query = `
		SELECT j.job_id, j.name, j.command, j.status, j.pid, j.worker_pid, j.stop_requested_at,
		       j.created_at, j.updated_at, j.exit_code, j.started_at, j.ended_at,
		       j.tags_json, j.notes, j.run_json,
		       j.stdout_path, j.stderr_path, j.events_path,
		       (SELECT COUNT(*) FROM events e WHERE e.job_id = j.job_id) AS event_count
		FROM jobs j
		ORDER BY CASE WHEN j.status IN ('running', 'orphaned') THEN 0 ELSE 1 END, j.updated_at DESC, j.created_at DESC
		LIMIT ?`
	rows, err := db.QueryContext(ctx, query, q.cfg.JobLimit)
	if err != nil {
		return err
	}
	defer rows.Close()

	jobs := make([]JobSummary, 0, q.cfg.JobLimit)
	for rows.Next() {
		var (
			j         JobSummary
			name      sql.NullString
			pid       sql.NullInt64
			workerPid sql.NullInt64
			stopReq   sql.NullString
			exitCode  sql.NullInt64
			startedAt sql.NullString
			endedAt   sql.NullString
			tagsJSON  string
			notes     sql.NullString
			runJSON   sql.NullString
		)
		if err := rows.Scan(&j.JobID, &name, &j.Command, &j.Status, &pid, &workerPid, &stopReq,
			&j.CreatedAt, &j.UpdatedAt, &exitCode, &startedAt, &endedAt, &tagsJSON, &notes, &runJSON,
			&j.StdoutPath, &j.StderrPath, &j.EventsPath, &j.EventCount); err != nil {
			return err
		}
		j.Name = name.String
		j.Notes = notes.String
		j.RunJSON = runJSON.String
		j.StartedAt = startedAt.String
		j.EndedAt = endedAt.String
		if pid.Valid {
			p := pid.Int64
			j.Pid = &p
		}
		if workerPid.Valid {
			p := workerPid.Int64
			j.WorkerPid = &p
		}
		if exitCode.Valid {
			c := exitCode.Int64
			j.ExitCode = &c
		}
		if tagsJSON != "" && tagsJSON != "null" {
			var tags []string
			if json.Unmarshal([]byte(tagsJSON), &tags) == nil {
				j.Tags = tags
			}
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	res.Jobs = jobs
	res.JobsOK = true
	return nil
}

func (q *Querier) loadEvents(ctx context.Context, db *sql.DB, req RefreshRequest, res *RefreshResult) error {
	const query = `
		SELECT event_id, job_id, seq, type, level, message, data_json, source, created_at
		FROM events WHERE job_id = ? AND seq > ? ORDER BY seq ASC LIMIT ?`
	store := NewSeriesStore()
	for _, jobID := range req.Targets {
		rows, err := db.QueryContext(ctx, query, jobID, req.LastSeq[jobID], q.cfg.EventBatchLimit)
		if err != nil {
			return err
		}
		var events []Event
		for rows.Next() {
			var (
				ev       Event
				message  sql.NullString
				dataJSON string
			)
			if err := rows.Scan(&ev.EventID, &ev.JobID, &ev.Seq, &ev.Type, &ev.Level,
				&message, &dataJSON, &ev.Source, &ev.CreatedAt); err != nil {
				rows.Close()
				return err
			}
			ev.Message = message.String
			ev.Data, _ = decodeData(dataJSON)
			events = append(events, ev)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		if len(events) > 0 {
			store.Ingest(jobID, events, q.cfg.SeriesCap, q.cfg.EventLimit)
		}
	}
	mergeStoreIntoResult(store, req, res)
	return nil
}

func mergeStoreIntoResult(store *SeriesStore, req RefreshRequest, res *RefreshResult) {
	for jobID, evs := range store.Events {
		if res.Events == nil {
			res.Events = map[string][]Event{}
		}
		res.Events[jobID] = append(res.Events[jobID], evs...)
		if maxSeq := lastSeqOf(evs); maxSeq > res.LastSeq[jobID] {
			res.LastSeq[jobID] = maxSeq
		}
	}
	for key, pts := range store.Points {
		if res.Points == nil {
			res.Points = map[SeriesKey][]Point{}
		}
		res.Points[key] = append(res.Points[key], pts...)
	}
	res.Warnings += store.Warnings
}

func lastSeqOf(events []Event) int64 {
	var max int64
	for _, ev := range events {
		if ev.Seq > max {
			max = ev.Seq
		}
	}
	return max
}

func isBusy(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "locked") || strings.Contains(msg, "busy") ||
		strings.Contains(msg, "deadline exceeded") || strings.Contains(msg, "context canceled")
}

// discoverJobs lists event mirror files to recover a job list without SQLite.
func (q *Querier) discoverJobs() []JobSummary {
	entries, err := os.ReadDir(q.cfg.EventsDir)
	if err != nil {
		return nil
	}
	var jobs []JobSummary
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".jsonl") {
			continue
		}
		jobID := strings.TrimSuffix(e.Name(), ".jsonl")
		jobs = append(jobs, JobSummary{JobID: jobID, Name: jobID, Status: "unknown"})
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].JobID < jobs[j].JobID })
	return jobs
}

const (
	maxMalformedWarnings = 5
	jsonlTailBytes       = 256 * 1024
)

// jsonlEvent mirrors the event dict written by the Python daemon's
// _append_event_mirror (server.py).
type jsonlEvent struct {
	EventID   string         `json:"event_id"`
	JobID     string         `json:"job_id"`
	Seq       int64          `json:"seq"`
	Type      string         `json:"type"`
	Level     string         `json:"level"`
	Message   *string        `json:"message"`
	Data      map[string]any `json:"data"`
	Source    string         `json:"source"`
	CreatedAt string         `json:"created_at"`
}

func (q *Querier) loadEventsJSONL(req RefreshRequest, res *RefreshResult) {
	store := NewSeriesStore()
	var malformed int
	for _, jobID := range req.Targets {
		path := filepath.Join(q.cfg.EventsDir, jobID+".jsonl")
		events := q.readJSONLTail(path, req.LastSeq[jobID], &malformed)
		if len(events) > 0 {
			store.Ingest(jobID, events, q.cfg.SeriesCap, q.cfg.EventLimit)
		}
	}
	mergeStoreIntoResult(store, req, res)
	res.Malformed = malformed
}

// readJSONLTail reads up to jsonlTailBytes from the end of a mirror file,
// parses complete lines, and returns events with seq above the cursor. An
// incomplete final line is ignored until complete; malformed lines produce a
// bounded warning count rather than a crash.
func (q *Querier) readJSONLTail(path string, lastSeq int64, malformed *int) []Event {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil
	}
	size := st.Size()
	offset := size - jsonlTailBytes
	if offset < 0 {
		offset = 0
	}
	if _, err := f.Seek(offset, 0); err != nil {
		return nil
	}
	// Drop a leading partial line introduced by seeking into the middle.
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	var (
		events []Event
		first  = true
	)
	for scanner.Scan() {
		line := strings.TrimRight(scanner.Text(), "\r")
		if line == "" {
			continue
		}
		if first && offset > 0 {
			first = false
			// The first line from a mid-file seek is likely partial; only
			// trust it if it parses cleanly with all required fields.
			var probe jsonlEvent
			if err := json.Unmarshal([]byte(line), &probe); err != nil || probe.EventID == "" {
				continue
			}
		}
		ev, ok := parseJSONL(line)
		if !ok {
			if *malformed < maxMalformedWarnings {
				*malformed++
			}
			continue
		}
		if ev.Seq <= lastSeq {
			continue
		}
		events = append(events, ev)
	}
	return events
}

func parseJSONL(line string) (Event, bool) {
	var raw jsonlEvent
	if err := json.Unmarshal([]byte(line), &raw); err != nil {
		return Event{}, false
	}
	if raw.Type == "" || raw.Seq <= 0 {
		return Event{}, false
	}
	data := raw.Data
	if data == nil {
		data = map[string]any{}
	}
	ev := Event{
		EventID:   raw.EventID,
		JobID:     raw.JobID,
		Seq:       raw.Seq,
		Type:      raw.Type,
		Level:     raw.Level,
		Data:      data,
		Source:    raw.Source,
		CreatedAt: raw.CreatedAt,
	}
	if raw.Message != nil {
		ev.Message = *raw.Message
	}
	return ev, true
}

// LogTail is the bounded result of reading a log file tail.
type LogTail struct {
	JobID string
	Lines []string
	Err   error
}

// ReadLogTail reads the last byteLimit bytes of a file and returns the last
// lineLimit complete lines (plus a trailing partial line, which is normal for
// a live log stream).
func ReadLogTail(path string, byteLimit int64, lineLimit int) LogTail {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return LogTail{}
		}
		return LogTail{Err: err}
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return LogTail{Err: err}
	}
	size := st.Size()
	offset := size - byteLimit
	if offset < 0 {
		offset = 0
	}
	if _, err := f.Seek(offset, 0); err != nil {
		return LogTail{Err: err}
	}
	data := make([]byte, size-offset)
	if _, err := io.ReadFull(f, data); err != nil {
		return LogTail{Err: err}
	}
	text := string(data)
	if offset > 0 {
		// Drop the leading partial line from the mid-file seek.
		if i := strings.IndexByte(text, '\n'); i >= 0 {
			text = text[i+1:]
		} else {
			text = ""
		}
	}
	lines := strings.Split(text, "\n")
	if n := len(lines); n > 0 && lines[n-1] == "" {
		lines = lines[:n-1]
	}
	if len(lines) > lineLimit {
		lines = lines[len(lines)-lineLimit:]
	}
	return LogTail{Lines: lines}
}

// FormatTimestamp returns the clock portion of an RFC3339 UTC timestamp used
// by the event table, or the raw value when it cannot be parsed structurally.
func FormatTimestamp(ts string) string {
	// Python emits e.g. 2026-01-01T00:00:00.123456Z (or +00:00 in odd cases).
	if len(ts) >= 12 {
		if ts[10] == 'T' {
			end := len(ts)
			if end > 23 && ts[19] == '.' {
				end = 23
			} else if end > 19 {
				end = 19
			}
			return ts[11:end]
		}
	}
	return ts
}

// NowISO is a small helper mirroring server.now_iso for tests.
func NowISO() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

// CompactJSON renders data compactly for the event table.
func CompactJSON(data map[string]any) string {
	if len(data) == 0 {
		return ""
	}
	b, err := json.Marshal(data)
	if err != nil {
		return fmt.Sprintf("%v", data)
	}
	return string(b)
}
