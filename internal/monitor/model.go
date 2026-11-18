package monitor

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/colorprofile"
)

// Focus regions the keyboard can move between.
type focus int

const (
	focusJobs focus = iota
	focusCharts
	focusLower
)

const (
	zoomInFactor   = 0.6
	zoomOutFactor  = 1.7
	pageZoomIn     = 0.5
	pageZoomOut    = 2.0
	panFraction    = 0.2
	crosshairAXISW = 6 // left columns reserved for the chart y-axis labels
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
	chartFocus int

	liveTail  bool
	views     map[int][2]float64
	crosshair map[int]float64

	focus       focus
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
	beat        int // refresh counter; drives the heartbeat border (no timer)
	flash       map[string]statusFlash
	constants   map[string]float64 // flat metrics, shown as values only
	mode        string
	empty       bool
	emptyMsg    string
	errorMsg    string

	help bool
	quit bool
}

var quitCmd tea.Cmd = func() tea.Msg { return tea.Quit() }

// New constructs a monitor model.
func New(cfg Config, q *Querier) Model {
	return Model{
		cfg:       cfg,
		q:         q,
		pinned:    map[string]bool{},
		jobByID:   map[string]int{},
		points:    map[SeriesKey][]Point{},
		events:    map[string][]Event{},
		lastSeq:   map[string]int64{},
		views:     map[int][2]float64{},
		crosshair: map[int]float64{},
		liveTail:  true,
		// The event table is the default lower pane so the dashboard shows
		// durable event activity immediately; 'e' and 'l' toggle between the
		// event table, the log tail, and the neutral hint.
		mode: ModeSQLite,
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
		if k == "?" {
			m.help = false
		}
		return m, nil
	}
	switch k {
	case "?":
		m.help = true
	case "tab":
		m.cycleFocus(1)
	case "shift+tab":
		m.cycleFocus(-1)
	case "enter":
		m.togglePinSelected()
	case "t":
		m.resetLiveTail()
	case "l":
		return m.toggleLogs()
	case "e":
		m.toggleEvents()
	case "s":
		m.toggleSlowest()
	case "up", "k":
		m.moveUp()
	case "down", "j":
		m.moveDown()
	case "left", "[":
		m = m.panChart(-panFraction)
	case "right", "]":
		m = m.panChart(panFraction)
	case "+", "=", "pgup":
		m = m.zoomChart(zoomInFactor)
	case "-", "_", "pgdn":
		m = m.zoomChart(zoomOutFactor)
	}
	return m, nil
}

func (m *Model) cycleFocus(delta int) {
	m.focus = focus((int(m.focus) + delta + 3) % 3)
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

func (m *Model) resetLiveTail() {
	m.liveTail = true
	m.views = map[int][2]float64{}
	m.crosshair = map[int]float64{}
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

// toggleSlowest switches the lower pane to the slowest-runs table. It is
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

func (m *Model) moveUp() {
	switch m.focus {
	case focusJobs:
		if m.selected > 0 {
			m.selected--
			m.bumpGen()
		}
	case focusCharts:
		if m.chartFocus > 0 {
			m.chartFocus--
		}
	case focusLower:
		m.lowerScroll++
	}
}

func (m *Model) moveDown() {
	switch m.focus {
	case focusJobs:
		if m.selected < len(m.jobs)-1 {
			m.selected++
			m.bumpGen()
		}
	case focusCharts:
		if m.chartFocus < len(m.metrics)-1 {
			m.chartFocus++
		}
	case focusLower:
		if m.lowerScroll > 0 {
			m.lowerScroll--
		}
	}
}

// zoomChart zooms the x window of the focused metric around the crosshair or
// center.
func (m Model) zoomChart(factor float64) Model {
	if len(m.metrics) == 0 {
		return m
	}
	idx := m.chartFocus
	minX, maxX, ok := m.metricWindow(idx)
	if !ok {
		return m
	}
	anchor := minX + (maxX-minX)/2
	if x, has := m.crosshair[idx]; has {
		anchor = x
	}
	return m.zoomChartAt(idx, anchor, factor)
}

func (m Model) zoomChartAt(idx int, anchor, factor float64) Model {
	minX, maxX, ok := m.metricWindow(idx)
	if !ok {
		return m
	}
	r := (maxX - minX) * factor
	if r <= 0 || math.IsNaN(r) || math.IsInf(r, 0) {
		return m
	}
	half := r / 2
	m.views[idx] = [2]float64{anchor - half, anchor + half}
	m.liveTail = false
	return m
}

func (m Model) panChart(frac float64) Model {
	if len(m.metrics) == 0 {
		return m
	}
	idx := m.chartFocus
	minX, maxX, ok := m.metricWindow(idx)
	if !ok {
		return m
	}
	shift := (maxX - minX) * frac
	m.views[idx] = [2]float64{minX + shift, maxX + shift}
	m.liveTail = false
	return m
}

func (m Model) handleMouseClick(mouse tea.Mouse) (tea.Model, tea.Cmd) {
	if !m.ready {
		return m, nil
	}
	lay := m.computeLayout()
	if inRect(mouse.X, mouse.Y, lay.jobs) && lay.jobs.h > 0 {
		// Row 0 of the box is the "Jobs" header; job blocks start at row 1 and
		// each block is jobBlockHeight content rows tall, so the block index is
		// the content row divided by the block height.
		row := mouse.Y - lay.jobs.y - 1
		if row >= 0 {
			if idx := m.jobAtRow(row, lay.jobs.h-2); idx >= 0 {
				m.selected = idx
				m.bumpGen()
			}
		}
		return m, nil
	}
	for _, cr := range lay.charts {
		if inRect(mouse.X, mouse.Y, cr.rect) {
			m.chartFocus = cr.metric
			if x, ok := m.chartXAt(cr, mouse.X); ok {
				m.crosshair[cr.metric] = x
			}
			return m, nil
		}
	}
	if inRect(mouse.X, mouse.Y, lay.metrics) {
		m.focus = focusCharts
		return m, nil
	}
	if inRect(mouse.X, mouse.Y, lay.lower) {
		m.focus = focusLower
	}
	return m, nil
}

func (m Model) handleMouseWheel(mouse tea.Mouse) (tea.Model, tea.Cmd) {
	if !m.ready {
		return m, nil
	}
	lay := m.computeLayout()
	for _, cr := range lay.charts {
		if inRect(mouse.X, mouse.Y, cr.rect) {
			m.chartFocus = cr.metric
			anchor := 0.0
			if x, ok := m.chartXAt(cr, mouse.X); ok {
				anchor = x
			}
			factor := 1.25
			if mouse.Button == tea.MouseWheelUp {
				factor = 0.8
			}
			return m.zoomChartAt(cr.metric, anchor, factor), nil
		}
	}
	return m, nil
}

func inRect(x, y int, r rect) bool {
	return x >= r.x && x < r.x+r.w && y >= r.y && y < r.y+r.h
}

// chartXAt maps a terminal column to a metric x value using the current window.
// cr is the full bordered box: the plot canvas begins one cell inside the left
// border, after the y-axis labels.
func (m Model) chartXAt(cr chartRect, mx int) (float64, bool) {
	minX, maxX, ok := m.metricWindow(cr.metric)
	if !ok {
		return 0, false
	}
	cw := cr.w - 2 - crosshairAXISW
	if cw <= 0 {
		return 0, false
	}
	x := minX + float64(mx-(cr.x+1+crosshairAXISW))*(maxX-minX)/float64(cw)
	if x < minX {
		x = minX
	}
	if x > maxX {
		x = maxX
	}
	return x, true
}

// metricWindow returns the effective x window for a metric: the stored zoom
// window or, in live-tail mode, the padded auto window over visible points.
func (m Model) metricWindow(idx int) (minX, maxX float64, ok bool) {
	if idx < 0 || idx >= len(m.metrics) {
		return 0, 0, false
	}
	if w, exists := m.views[idx]; exists {
		return w[0], w[1], true
	}
	minX, maxX, has := m.autoWindow(idx)
	if !has {
		return 0, 0, false
	}
	px, py := padRange(minX, maxX, 0.05, 1)
	return px, py, true
}

func (m Model) autoWindow(idx int) (float64, float64, bool) {
	if idx < 0 || idx >= len(m.metrics) {
		return 0, 0, false
	}
	metric := m.metrics[idx]
	var minX, maxX float64
	has := false
	for _, j := range m.visibleOverlayJobs(metric) {
		for _, p := range m.points[SeriesKey{JobID: j.JobID, Metric: metric}] {
			if !has {
				minX, maxX = p.X, p.X
				has = true
				continue
			}
			if p.X < minX {
				minX = p.X
			}
			if p.X > maxX {
				maxX = p.X
			}
		}
	}
	return minX, maxX, has
}

func padRange(minX, maxX, frac, minPad float64) (float64, float64) {
	r := maxX - minX
	pad := r * frac
	if r < 1e-12 {
		pad = math.Max(math.Abs(minX)*frac, minPad)
	}
	lo := minX - pad
	if minX >= 0 && lo < 0 {
		lo = 0 // data that starts at zero (steps, progress) never gets a negative axis
	}
	return lo, maxX + pad
}

// visibleOverlayJobs returns the jobs drawn for a metric: pinned first, then
// the selected job, then the rest in list order, capped at MaxOverlays.
func (m Model) visibleOverlayJobs(metric string) []JobSummary {
	var pinned, rest []JobSummary
	for _, j := range m.jobs {
		if len(m.points[SeriesKey{JobID: j.JobID, Metric: metric}]) == 0 {
			continue
		}
		if m.pinned[j.JobID] {
			pinned = append(pinned, j)
		} else {
			rest = append(rest, j)
		}
	}
	sel := m.selectedJobID()
	for i, j := range rest {
		if j.JobID == sel {
			rest = append(rest[:i:i], rest[i+1:]...)
			rest = append([]JobSummary{j}, rest...)
			break
		}
	}
	out := append(pinned, rest...)
	if len(out) > m.cfg.MaxOverlays {
		out = out[:m.cfg.MaxOverlays]
	}
	return out
}

// allChartJobIDs returns the IDs of every job contributing to any chart; slot
// assignment is computed over this stable set so colors do not change with
// list order or with the eight-overlay cap.
func (m Model) allChartJobIDs() []string {
	seen := map[string]bool{}
	var ids []string
	for _, j := range m.jobs {
		for metric := range m.metricSet() {
			if len(m.points[SeriesKey{JobID: j.JobID, Metric: metric}]) > 0 {
				seen[j.JobID] = true
				break
			}
		}
	}
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
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
		m.setJobs(res.Jobs)
	}
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
	m.warnings += res.Warnings
	if res.Malformed > m.malformed {
		m.malformed = res.Malformed
	}
	m.mode = res.Mode
	m.loading = false
	m.lastRefresh = time.Now()
	m.beat++
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
	m.noteTransitions(jobs)
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

// loadTargets returns the bounded set of jobs whose events are loaded.
func (m Model) loadTargets() []string {
	targets := map[string]bool{}
	if id := m.selectedJobID(); id != "" {
		targets[id] = true
	}
	for _, j := range m.jobs {
		if m.pinned[j.JobID] {
			targets[j.JobID] = true
		}
	}
	var out []string
	for id := range targets {
		out = append(out, id)
	}
	sort.Strings(out)
	if len(out) > m.cfg.MaxOverlays {
		out = out[:m.cfg.MaxOverlays]
	}
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

func (m *Model) bumpGen() {
	m.gen++
}

// statusFlash remembers that a job just changed status, so its row can glow for
// a few refreshes. Expiry is counted in refreshes, not wall time: nothing ticks
// just to animate.
type statusFlash struct {
	to   string
	beat int
}

const flashBeats = 3

// noteTransitions records jobs whose status differs from the previous list.
func (m *Model) noteTransitions(next []JobSummary) {
	if m.flash == nil {
		m.flash = map[string]statusFlash{}
	}
	for id, f := range m.flash {
		if m.beat-f.beat >= flashBeats {
			delete(m.flash, id)
		}
	}
	for _, j := range next {
		if i, ok := m.jobByID[j.JobID]; ok && i < len(m.jobs) && m.jobs[i].Status != j.Status {
			// applyRefresh bumps beat right after setJobs.
			m.flash[j.JobID] = statusFlash{to: j.Status, beat: m.beat + 1}
		}
	}
}

// flashPhase returns the effect progress (0,1) for a recently changed job, or 0.
func (m Model) flashPhase(jobID string) (statusFlash, float64) {
	f, ok := m.flash[jobID]
	if !ok || m.beat-f.beat >= flashBeats {
		return statusFlash{}, 0
	}
	return f, float64(m.beat-f.beat+1) / float64(flashBeats+1)
}
