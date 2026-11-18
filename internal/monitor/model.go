package monitor

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/colorprofile"
)

// zoom limits for the time window of a chart.
const (
	minZoomSpan = 30 * time.Second
	maxZoomSpan = 32 * 24 * time.Hour
)

// refreshMsg carries one immutable refresh result.
type refreshMsg struct {
	Gen    int
	Result RefreshResult
}

// tickMsg drives the refresh cadence.
type tickMsg time.Time

// logTailMsg carries a bounded stdout/stderr tail.
type logTailMsg struct {
	Gen   int
	JobID string
	Lines []string
	Err   error
}

// Model is the Bubble Tea monitor. Update and View never touch SQLite or the
// filesystem; all I/O happens in commands producing immutable messages.
type Model struct {
	cfg Config
	q   *Querier

	width  int
	height int
	ready  bool

	dark    bool
	profile colorprofile.Profile

	gen      int
	selected int
	pinned   map[string]bool
	jobByID  map[string]int
	jobs     []JobSummary

	points     map[SeriesKey][]Point
	events     map[string][]Event
	lastSeq    map[string]int64
	warnings   int
	malformed  int
	metrics    []string
	chartFocus int                      // index into the chart list; its page is shown
	zoom       map[string]time.Duration // per chart key: the time window the user zoomed to
	colors     map[string]int           // series colour slot per charted job

	hideSidebar bool // [ / ]: fold the sidebar into the compact run list
	showEvents  bool
	showLogs    bool
	showSlowest bool
	logLoading  bool
	logTail     []string
	logTailJob  string
	logTailErr  string
	lowerScroll int

	loading     bool
	staleSince  *time.Time
	lastRefresh time.Time
	beat        int                // refresh counter; drives the heartbeat border (no timer)
	constants   map[string]float64 // flat metrics, shown as values only
	mode        string
	empty       bool
	emptyMsg    string
	errorMsg    string

	panels map[string]panelDecl // latest declared panels per job

	help bool
	quit bool
}

var quitCmd tea.Cmd = func() tea.Msg { return tea.Quit() }

// New constructs a monitor model.
func New(cfg Config, q *Querier) Model {
	return Model{
		cfg:     cfg,
		q:       q,
		pinned:  map[string]bool{},
		jobByID: map[string]int{},
		points:  map[SeriesKey][]Point{},
		events:  map[string][]Event{},
		lastSeq: map[string]int64{},
		zoom:    map[string]time.Duration{},
		colors:  map[string]int{},
		mode:    ModeSQLite,
	}
}

// Init starts the refresh loop and requests the terminal background color.
func (m Model) Init() tea.Cmd {
	return tea.Batch(
		func() tea.Msg { return tea.RequestBackgroundColor() },
		m.refreshCmd(),
	)
}

// Update is a pure state machine over typed messages.
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		m.ready = true
		return m, nil
	case tea.KeyPressMsg:
		return m.handleKey(msg)
	case tea.MouseClickMsg:
		return m.handleMouseClick(msg.Mouse())
	case tea.MouseWheelMsg:
		return m.handleMouseWheel(msg.Mouse())
	case tea.BackgroundColorMsg:
		m.dark = msg.IsDark()
		return m, nil
	case tea.ColorProfileMsg:
		m.profile = msg.Profile
		return m, nil
	case refreshMsg:
		if msg.Gen != m.gen {
			// A slow result for a previous selection arrived late. Discard it
			// but keep the loop alive with a fresh refresh for the current
			// selection.
			return m, m.refreshCmd()
		}
		return m.applyRefresh(msg.Result)
	case tickMsg:
		return m, m.refreshCmd()
	case logTailMsg:
		if msg.Gen != m.gen || msg.JobID != m.selectedJobID() {
			return m, nil
		}
		m.logLoading = false
		m.logTailJob = msg.JobID
		m.logTail = msg.Lines
		m.logTailErr = ""
		if msg.Err != nil {
			m.logTailErr = msg.Err.Error()
		}
		return m, nil
	}
	return m, nil
}

func (m Model) handleKey(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	k := msg.String()
	if k == "ctrl+c" || k == "q" {
		m.quit = true
		return m, quitCmd
	}
	if m.help {
		if k == "?" || k == "esc" {
			m.help = false
		}
		return m, nil
	}
	n := len(m.chartSpecs())
	switch k {
	case "?":
		m.help = true
	case "tab":
		if n > 0 {
			m.chartFocus = (clamp(m.chartFocus, 0, n-1) + 1) % n
		}
	case "shift+tab":
		if n > 0 {
			m.chartFocus = (clamp(m.chartFocus, 0, n-1) + n - 1) % n
		}
	case "left", "right":
		m.pageCharts(k == "right", n)
	case "[", "]":
		m.hideSidebar = !m.hideSidebar
	case "enter":
		m.togglePinSelected()
	case "t":
		m.zoom = map[string]time.Duration{}
	case "l":
		return m.toggleLogs()
	case "e":
		m.toggleEvents()
	case "s":
		m.toggleSlowest()
	case "esc":
		m.showEvents, m.showLogs, m.showSlowest, m.logLoading = false, false, false, false
	case "up", "k", "down", "j":
		delta := 1
		if k == "up" || k == "k" {
			delta = -1
		}
		gen := m.gen
		m.moveRun(delta)
		if m.showLogs && m.gen != gen {
			m.logLoading = true
			return m, m.logTailCmd()
		}
	case "pgup":
		m.lowerScroll++
	case "pgdown", "pgdn":
		m.lowerScroll = max(m.lowerScroll-1, 0)
	case "+", "=":
		if specs := m.chartSpecs(); n > 0 {
			m.zoomChart(specs[clamp(m.chartFocus, 0, n-1)], 0.5)
		}
	case "-", "_":
		if specs := m.chartSpecs(); n > 0 {
			m.zoomChart(specs[clamp(m.chartFocus, 0, n-1)], 2)
		}
	}
	return m, nil
}

// pageCharts moves the chart focus to the first chart of the next or previous
// page of the grid.
func (m *Model) pageCharts(next bool, n int) {
	if n == 0 {
		return
	}
	cols, rows := gridShape(m.computeLeet().main, n)
	per := cols * rows
	page := clamp(m.chartFocus, 0, n-1)/per - 1
	if next {
		page += 2
	}
	m.chartFocus = clamp(page, 0, (n-1)/per) * per
}

// zoomChart scales a chart's time window (factor < 1 zooms in).
func (m *Model) zoomChart(s chartSpec, factor float64) {
	lo, hi, _ := m.chartWindow(s)
	span := time.Duration(float64(hi.Sub(lo)) * factor)
	m.zoom[s.key] = min(max(span, minZoomSpan), maxZoomSpan)
}

func (m *Model) togglePinSelected() {
	id := m.selectedJobID()
	if id == "" {
		return
	}
	if m.pinned[id] {
		delete(m.pinned, id)
	} else {
		m.pinned[id] = true
	}
	m.bumpGen()
}

func (m Model) toggleLogs() (tea.Model, tea.Cmd) {
	m.showLogs = !m.showLogs
	if m.showLogs {
		m.showEvents = false
		m.showSlowest = false
		m.lowerScroll = 0
		m.logLoading = true
		return m, m.logTailCmd()
	}
	m.lowerScroll = 0
	m.logLoading = false
	return m, nil
}

func (m *Model) toggleEvents() {
	m.showEvents = !m.showEvents
	if m.showEvents {
		m.showLogs = false
		m.showSlowest = false
	}
	m.lowerScroll = 0
}

// toggleSlowest switches the main area to the slowest-runs table. It is
// mutually exclusive with the event table and log tail.
func (m *Model) toggleSlowest() {
	m.showSlowest = !m.showSlowest
	if m.showSlowest {
		m.showEvents = false
		m.showLogs = false
		m.logLoading = false
	}
	m.lowerScroll = 0
}

// slowestJobs returns the n terminal local jobs with the longest runtime,
// longest first. Running/queued rows and remote shadows (no local timestamps)
// are excluded. Ties fall back to most-recently-updated.
func (m Model) slowestJobs(n int) []JobSummary {
	type ranked struct {
		job     JobSummary
		seconds float64
		updated string
	}
	rankedJobs := make([]ranked, 0, len(m.jobs))
	for _, job := range m.jobs {
		if job.Shadow {
			continue
		}
		seconds, ok := job.DurationSeconds()
		if !ok {
			continue
		}
		rankedJobs = append(rankedJobs, ranked{job: job, seconds: seconds, updated: job.UpdatedAt})
	}
	sort.SliceStable(rankedJobs, func(i, j int) bool {
		if rankedJobs[i].seconds != rankedJobs[j].seconds {
			return rankedJobs[i].seconds > rankedJobs[j].seconds
		}
		return rankedJobs[i].updated > rankedJobs[j].updated
	})
	if len(rankedJobs) > n {
		rankedJobs = rankedJobs[:n]
	}
	out := make([]JobSummary, 0, len(rankedJobs))
	for _, r := range rankedJobs {
		out = append(out, r.job)
	}
	return out
}

func (m Model) handleMouseClick(mouse tea.Mouse) (tea.Model, tea.Cmd) {
	if !m.ready || len(m.jobs) == 0 {
		return m, nil
	}
	gen := m.gen
	m = m.leetClick(mouse.X, mouse.Y)
	if m.showLogs && m.gen != gen {
		m.logLoading = true
		return m, m.logTailCmd()
	}
	return m, nil
}

// handleMouseWheel zooms the chart under the pointer, or scrolls an open pane.
func (m Model) handleMouseWheel(mouse tea.Mouse) (tea.Model, tea.Cmd) {
	if !m.ready {
		return m, nil
	}
	up := mouse.Button == tea.MouseWheelUp
	if m.lowerExpanded() {
		if up {
			m.lowerScroll++
		} else {
			m.lowerScroll = max(m.lowerScroll-1, 0)
		}
		return m, nil
	}
	if s, ok := m.chartAt(mouse.X, mouse.Y); ok {
		factor := 2.0
		if up {
			factor = 0.5
		}
		m.zoomChart(s, factor)
	}
	return m, nil
}

func (m Model) metricSet() map[string]bool {
	set := map[string]bool{}
	for key := range m.points {
		set[key.Metric] = true
	}
	return set
}

// applyRefresh merges an immutable refresh result into the model.
func (m Model) applyRefresh(res RefreshResult) (tea.Model, tea.Cmd) {
	if res.JobsOK {
		first := len(m.jobs) == 0
		m.setJobs(res.Jobs)
		if first && len(m.jobs) > 0 {
			m.selected = m.runOrder()[0] // the first run of the sidebar, not of the raw list
		}
	}
	m.notePanels(res.Events)
	for jobID, evs := range res.Events {
		m.events[jobID] = append(m.events[jobID], evs...)
		if n := len(m.events[jobID]); n > m.cfg.EventLimit {
			m.events[jobID] = m.events[jobID][n-m.cfg.EventLimit:]
		}
		if s := lastSeqOf(evs); s > m.lastSeq[jobID] {
			m.lastSeq[jobID] = s
		}
	}
	if len(res.Points) > 0 {
		for key, pts := range res.Points {
			m.points[key] = append(m.points[key], pts...)
			if n := len(m.points[key]); n > m.cfg.SeriesCap {
				m.points[key] = m.points[key][n-m.cfg.SeriesCap:]
			}
		}
		m.rebuildMetrics()
	}
	m.assignColors()
	m.warnings += res.Warnings
	if res.Malformed > m.malformed {
		m.malformed = res.Malformed
	}
	m.mode = res.Mode
	m.loading = false
	m.lastRefresh = time.Now()
	if res.Stale {
		now := time.Now()
		m.staleSince = &now
	} else {
		m.staleSince = nil
	}
	if res.Empty && len(m.jobs) == 0 {
		m.empty = true
		m.emptyMsg = "no jobs found: jobs.sqlite is missing and no event mirrors are present"
	} else {
		m.empty = false
	}
	return m, m.tickCmd()
}

func (m *Model) setJobs(jobs []JobSummary) {
	m.jobs = jobs
	m.jobByID = map[string]int{}
	for i, j := range jobs {
		m.jobByID[j.JobID] = i
	}
	if len(jobs) == 0 {
		m.selected = 0
		m.pinned = map[string]bool{}
		return
	}
	if m.selected >= len(jobs) {
		m.selected = len(jobs) - 1
	}
	for id := range m.pinned {
		if _, ok := m.jobByID[id]; !ok {
			delete(m.pinned, id)
		}
	}
	if _, ok := m.jobByID[m.jobs[m.selected].JobID]; !ok {
		m.selected = 0
	}
}

func (m *Model) rebuildMetrics() {
	set := m.metricSet()
	m.metrics = m.metrics[:0]
	m.constants = map[string]float64{}
	for k := range set {
		if v, ok := m.constantSeries(k); ok {
			// A flat line (e.g. progress.total) is a fact, not a chart: it is
			// shown as a value in the Metrics pane only.
			m.constants[k] = v
			continue
		}
		m.metrics = append(m.metrics, k)
	}
	sort.Strings(m.metrics)
	if m.chartFocus >= len(m.metrics) {
		m.chartFocus = max(0, len(m.metrics)-1)
	}
}

// constantSeries reports whether every series of the metric has at least two
// points and they all share one value.
func (m *Model) constantSeries(metric string) (float64, bool) {
	var val float64
	seen := false
	for key, pts := range m.points {
		if key.Metric != metric {
			continue
		}
		if len(pts) < 2 {
			return 0, false
		}
		for _, p := range pts {
			if !seen {
				val, seen = p.Y, true
			} else if p.Y != val {
				return 0, false
			}
		}
	}
	return val, seen
}

// loadTargets returns the bounded set of jobs whose events are loaded: the
// selected job, pinned jobs, then active jobs (their rows need progress and
// liveness), capped at MaxOverlays.
func (m Model) loadTargets() []string {
	targets := map[string]bool{}
	var out []string
	add := func(id string) {
		if id != "" && !targets[id] && len(out) < m.cfg.MaxOverlays {
			targets[id] = true
			out = append(out, id)
		}
	}
	add(m.selectedJobID())
	for _, j := range m.jobs {
		if m.pinned[j.JobID] {
			add(j.JobID)
		}
	}
	for _, j := range m.jobs {
		if isActive(j.Status) && !j.Shadow {
			add(j.JobID)
		}
	}
	sort.Strings(out)
	return out
}

func (m Model) selectedJobID() string {
	if m.selected < 0 || m.selected >= len(m.jobs) {
		return ""
	}
	return m.jobs[m.selected].JobID
}

func (m Model) selectedJob() (JobSummary, bool) {
	if m.selected < 0 || m.selected >= len(m.jobs) {
		return JobSummary{}, false
	}
	return m.jobs[m.selected], true
}

func (m Model) lastSeqSnapshot() map[string]int64 {
	out := map[string]int64{}
	for k, v := range m.lastSeq {
		out[k] = v
	}
	return out
}

func (m Model) jobsSnapshot() []JobSummary {
	out := make([]JobSummary, len(m.jobs))
	copy(out, m.jobs)
	return out
}

// refreshCmd performs the read-only query in a command goroutine and returns
// an immutable message carrying the generation it was issued for.
func (m Model) refreshCmd() tea.Cmd {
	gen := m.gen
	req := RefreshRequest{
		Targets:   m.loadTargets(),
		LastSeq:   m.lastSeqSnapshot(),
		KnownJobs: m.jobsSnapshot(),
	}
	q := m.q
	timeout := m.cfg.QueryTimeout
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		res := q.Refresh(ctx, req)
		res.Gen = gen
		return refreshMsg{Gen: gen, Result: res}
	}
}

// tickCmd schedules the next refresh after the cadence appropriate to the
// current visible state.
func (m Model) tickCmd() tea.Cmd {
	return tea.Tick(m.nextInterval(), func(t time.Time) tea.Msg { return tickMsg(t) })
}

// nextInterval returns the refresh cadence: fast while any visible job is
// running, slow when all visible jobs are terminal.
func (m Model) nextInterval() time.Duration {
	for _, j := range m.jobs {
		if j.Status == "running" {
			return m.cfg.RefreshRunning
		}
	}
	return m.cfg.RefreshIdle
}

// RefreshInterval is the pure cadence rule used by tests.
func RefreshInterval(cfg Config, jobs []JobSummary) time.Duration {
	for _, j := range jobs {
		if j.Status == "running" {
			return cfg.RefreshRunning
		}
	}
	return cfg.RefreshIdle
}

func (m Model) logTailCmd() tea.Cmd {
	job, ok := m.selectedJob()
	if !ok {
		return nil
	}
	gen := m.gen
	stdout := m.cfg.resolvePath(job.StdoutPath)
	stderr := m.cfg.resolvePath(job.StderrPath)
	byteLimit := m.cfg.LogTailBytes
	lineLimit := m.cfg.LogTailLines
	return func() tea.Msg {
		var errs []string
		out := ReadLogTail(stdout, byteLimit, lineLimit)
		if out.Err != nil {
			errs = append(errs, "stdout: "+out.Err.Error())
		}
		errTail := ReadLogTail(stderr, byteLimit, lineLimit)
		if errTail.Err != nil {
			errs = append(errs, "stderr: "+errTail.Err.Error())
		}
		combined := combineTails(out.Lines, errTail.Lines)
		var err error
		if len(errs) > 0 {
			err = fmt.Errorf("%s", strings.Join(errs, "; "))
		}
		return logTailMsg{Gen: gen, JobID: job.JobID, Lines: combined, Err: err}
	}
}

func combineTails(stdout, stderr []string) []string {
	if len(stderr) == 0 {
		return stdout
	}
	if len(stdout) == 0 {
		return stderr
	}
	out := make([]string, 0, len(stdout)+len(stderr))
	for _, l := range stdout {
		out = append(out, "[stdout] "+l)
	}
	for _, l := range stderr {
		out = append(out, "[stderr] "+l)
	}
	return out
}

// bumpGen marks a change of the selection or pins: in-flight refreshes for
// the old target set are discarded, and the new set gets its colours.
func (m *Model) bumpGen() {
	m.gen++
	m.assignColors()
}
