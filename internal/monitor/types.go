// Package monitor implements the read-only terminal monitor for Vanth jobs.
//
// The model owns only presentation state and a bounded in-memory snapshot of
// durable state. SQLite and filesystem access happen exclusively in Bubble Tea
// commands that return immutable messages; Update and View never perform I/O.
package monitor

import (
	"path/filepath"
	"strings"
	"time"
)

// SeriesKey identifies one scalar series within a job.
type SeriesKey struct {
	JobID  string
	Metric string
}

// MinChartHeight is the minimum cell height (rows) for a bordered metric chart
// box to render a visible braille body: top border + title space + plot canvas
// + bottom border. Charts are folded behind a "+N more" indicator when the
// terminal cannot fit this many rows.
const MinChartHeight = 5

// MinChartWidth is the minimum cell width for a bordered metric chart box.
// Narrower cells fall back to a single column.
const MinChartWidth = 16

// Point is one plotted sample. X follows _step when finite and numeric,
// otherwise the event sequence number.
type Point struct {
	EventID string
	Seq     int64
	X       float64
	Y       float64
	At      string // raw RFC3339 created_at, kept exact for the event table
	Stage   string
}

// JobSummary is the bounded view of a jobs row used by the monitor.
type JobSummary struct {
	JobID      string
	Name       string
	Command    string
	Status     string
	Pid        *int64
	WorkerPid  *int64
	ExitCode   *int64
	CreatedAt  string
	UpdatedAt  string
	StartedAt  string
	EndedAt    string
	StdoutPath string
	StderrPath string
	EventsPath string
	Tags       []string
	EventCount int64
	Notes      string
	RunJSON    string
	// Remote shadow projection (Phase 3): set when the row comes from a
	// remote_shadows table rather than the local jobs table. A shadow has no
	// local PID/heartbeat; the view renders its remote location + status.
	RemoteID string
	Shadow   bool
}

// IsRemote reports whether this summary is a projected remote shadow.
func (j JobSummary) IsRemote() bool { return j.Shadow }

// DurationSeconds returns the run's wall-clock runtime for terminal jobs whose
// started_at and ended_at are both present and well-formed. ok is false for
// running, queued, or remote-shadow rows (which carry no local timestamps).
//
// The daemon emits fractional seconds (2026-01-01T00:00:00.123456Z), so this
// parses RFC3339Nano — plain RFC3339 rejects the fraction and would silently
// exclude every real job from duration analytics.
func (j JobSummary) DurationSeconds() (float64, bool) {
	if j.StartedAt == "" || j.EndedAt == "" {
		return 0, false
	}
	start, err := time.Parse(time.RFC3339Nano, j.StartedAt)
	if err != nil {
		return 0, false
	}
	end, err := time.Parse(time.RFC3339Nano, j.EndedAt)
	if err != nil {
		return 0, false
	}
	seconds := end.Sub(start).Seconds()
	if seconds < 0 {
		return 0, false
	}
	return seconds, true
}

// Location returns "local" or the remote id for display.
func (j JobSummary) Location() string {
	if j.RemoteID != "" {
		return j.RemoteID
	}
	return "local"
}

// DisplayName returns the human-friendly job label.
func (j JobSummary) DisplayName() string {
	if strings.TrimSpace(j.Name) != "" {
		return strings.TrimSpace(j.Name)
	}
	return j.JobID
}

// Event is one exact, original event used by the event table.
type Event struct {
	EventID   string
	JobID     string
	Seq       int64
	Type      string
	Level     string
	Message   string
	Data      map[string]any
	Source    string
	CreatedAt string
}

// Config carries all tunable monitor parameters. Refresh intervals are
// configurable so tests can drive deterministic cadence without a TTY.
type Config struct {
	Home      string
	DBPath    string
	EventsDir string
	LogsDir   string
	// RemoteDBPath optionally points at the controller's remote.sqlite
	// (review P1-7). When set and the file exists, Refresh merges
	// current-timeline remote shadows into the job list.
	RemoteDBPath string

	JobLimit        int
	EventLimit      int // events retained per job in memory
	EventBatchLimit int // events fetched per job per refresh
	SeriesCap       int // points retained per series key in memory
	LogTailBytes    int64
	LogTailLines    int
	QueryTimeout    time.Duration
	RefreshRunning  time.Duration // cadence while a visible job is running
	RefreshIdle     time.Duration // cadence when every visible job is terminal
	DBBusyTimeoutMS int

	MinWidth            int
	MinHeight           int
	MaxOverlays         int // max simultaneously visible job overlays per chart
	GapFactor           float64
	DownsampleThreshold float64 // factor applied to chart braille width
}

// DefaultConfig returns the production monitor configuration for a home dir.
func DefaultConfig(home string) Config {
	return Config{
		Home:      home,
		DBPath:    filepath.Join(home, "jobs.sqlite"),
		EventsDir: filepath.Join(home, "events"),
		LogsDir:   filepath.Join(home, "logs"),
		// Production wiring for the remote shadow projection (review
		// rc14 P1-5): the controller's remote.sqlite sits beside jobs.sqlite.
		RemoteDBPath:        filepath.Join(home, "remote.sqlite"),
		JobLimit:            100,
		EventLimit:          2000,
		EventBatchLimit:     5000,
		SeriesCap:           20000,
		LogTailBytes:        64 * 1024,
		LogTailLines:        200,
		QueryTimeout:        800 * time.Millisecond,
		RefreshRunning:      250 * time.Millisecond,
		RefreshIdle:         time.Second,
		DBBusyTimeoutMS:     1000,
		MinWidth:            50,
		MinHeight:           14,
		MaxOverlays:         8,
		GapFactor:           8,
		DownsampleThreshold: 2,
	}
}

// resolvePath resolves a possibly-relative durable path against the home dir.
func (c Config) resolvePath(p string) string {
	if p == "" {
		return ""
	}
	if filepath.IsAbs(p) {
		return p
	}
	return filepath.Join(c.Home, p)
}
