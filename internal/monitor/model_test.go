package monitor

import (
	"path/filepath"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
)

func baseModel(t *testing.T) Model {
	t.Helper()
	cfg := DefaultConfig(filepath.Join(t.TempDir(), "home"))
	m := New(cfg, nil)
	m.detail = true // these tests drive the detail dashboard; glance_test.go covers the default view
	return m
}

func updateModel(t *testing.T, m Model, msg tea.Msg) (Model, tea.Cmd) {
	t.Helper()
	mm, cmd := m.Update(msg)
	out, ok := mm.(Model)
	if !ok {
		t.Fatalf("Update returned %T, want Model", mm)
	}
	return out, cmd
}

func tj(id, status string) JobSummary {
	return JobSummary{JobID: id, Name: id, Status: status, UpdatedAt: "2026-01-01T00:00:00Z"}
}

func resultWithJobs(jobs ...JobSummary) RefreshResult {
	return RefreshResult{Jobs: jobs, JobsOK: true, Mode: ModeSQLite}
}

func windowed(t *testing.T, m Model, w, h int) Model {
	t.Helper()
	m, _ = updateModel(t, m, tea.WindowSizeMsg{Width: w, Height: h})
	if !m.ready {
		t.Fatal("model not ready after WindowSizeMsg")
	}
	return m
}

func TestResizeMarksReady(t *testing.T) {
	m := baseModel(t)
	if m.ready {
		t.Fatal("model should start unready")
	}
	m = windowed(t, m, 120, 40)
	if m.width != 120 || m.height != 40 {
		t.Errorf("size = %dx%d", m.width, m.height)
	}
}

func TestRefreshLoadsJobsAndPoints(t *testing.T) {
	m := baseModel(t)
	res := resultWithJobs(tj("job_a", "running"), tj("job_b", "completed"))
	res.Points = map[SeriesKey][]Point{
		{JobID: "job_a", Metric: "loss"}: {{EventID: "e1", Seq: 1, X: 1, Y: 0.5, At: "2026-01-01T00:00:00Z"}},
	}
	m, _ = updateModel(t, m, refreshMsg{Gen: m.gen, Result: res})
	if len(m.jobs) != 2 {
		t.Fatalf("jobs = %d, want 2", len(m.jobs))
	}
	if m.metrics[0] != "loss" {
		t.Errorf("metrics = %v, want [loss]", m.metrics)
	}
	if len(m.points[SeriesKey{JobID: "job_a", Metric: "loss"}]) != 1 {
		t.Error("point not stored")
	}
}

func TestStaleGenerationIgnored(t *testing.T) {
	m := baseModel(t)
	m, _ = updateModel(t, m, refreshMsg{Gen: m.gen, Result: resultWithJobs(tj("job_a", "running"))})
	if len(m.jobs) != 1 {
		t.Fatalf("setup refresh not applied: %+v", m.jobs)
	}
	m.gen = m.gen + 2 // selection change bumps gen; an in-flight old refresh arrives

	stale := resultWithJobs(tj("job_OLD", "completed"))
	m, _ = updateModel(t, m, refreshMsg{Gen: m.gen - 2, Result: stale})
	if len(m.jobs) != 1 || m.jobs[0].JobID != "job_a" {
		t.Fatalf("stale generation overwrote screen: %+v", m.jobs)
	}
}

func TestStaleKeepsLastFrame(t *testing.T) {
	m := baseModel(t)
	m, _ = updateModel(t, m, refreshMsg{Gen: m.gen, Result: resultWithJobs(tj("job_a", "running"))})
	m, cmd := updateModel(t, m, refreshMsg{Gen: m.gen, Result: RefreshResult{
		Stale: true, Mode: ModeSQLite,
	}})
	if m.staleSince == nil {
		t.Error("stale indicator not set")
	}
	if len(m.jobs) != 1 {
		t.Error("last frame was dropped on stale refresh")
	}
	if cmd == nil {
		t.Error("stale refresh should still schedule a retry tick")
	}
}

func TestEmptyState(t *testing.T) {
	m := baseModel(t)
	m, _ = updateModel(t, m, refreshMsg{Gen: m.gen, Result: RefreshResult{
		Mode: ModeFallback, Fallback: true, Empty: true,
	}})
	if !m.empty {
		t.Error("empty state not set")
	}
	if m.emptyMsg == "" {
		t.Error("empty message missing")
	}
}

func TestFallbackModeLabel(t *testing.T) {
	m := baseModel(t)
	m, _ = updateModel(t, m, refreshMsg{Gen: m.gen, Result: RefreshResult{
		Mode: ModeFallback, Fallback: true,
	}})
	if m.mode != ModeFallback {
		t.Errorf("mode = %s, want fallback", m.mode)
	}
}

func TestSelectionKeysMoveAndPin(t *testing.T) {
	m := baseModel(t)
	m, _ = updateModel(t, m, refreshMsg{Gen: m.gen, Result: resultWithJobs(
		tj("job_a", "running"), tj("job_b", "running"), tj("job_c", "completed"))})

	m, _ = updateModel(t, m, tea.KeyPressMsg(tea.Key{Code: 'j'}))
	if m.selected != 1 {
		t.Fatalf("j should move selection to 1, got %d", m.selected)
	}
	m, _ = updateModel(t, m, tea.KeyPressMsg(tea.Key{Code: tea.KeyDown}))
	if m.selected != 2 {
		t.Fatalf("down should move selection to 2, got %d", m.selected)
	}
	// selection changed the load target set -> gen must bump
	gen := m.gen
	m, _ = updateModel(t, m, tea.KeyPressMsg(tea.Key{Code: 'k'}))
	if m.selected != 1 {
		t.Fatalf("k should move selection to 1, got %d", m.selected)
	}
	if m.gen == gen {
		t.Error("selection move should bump generation")
	}

	m, _ = updateModel(t, m, tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	if !m.pinned["job_b"] {
		t.Error("enter should pin the selected job")
	}
	m, _ = updateModel(t, m, tea.KeyPressMsg(tea.Key{Code: tea.KeyEnter}))
	if m.pinned["job_b"] {
		t.Error("second enter should unpin the selected job")
	}
}

func TestLowerPaneToggles(t *testing.T) {
	m := baseModel(t)
	m, _ = updateModel(t, m, refreshMsg{Gen: m.gen, Result: resultWithJobs(tj("job_a", "running"))})

	// The compact ticker + timeline is the default lower area; the full event
	// table is opt-in.
	if m.showEvents || m.lowerExpanded() {
		t.Error("compact ticker should be the default lower area")
	}

	// 'e' expands the full event table.
	m, _ = updateModel(t, m, tea.KeyPressMsg(tea.Key{Code: 'e'}))
	if !m.showEvents || m.showLogs {
		t.Errorf("e: events=%v logs=%v", m.showEvents, m.showLogs)
	}
	// Press again to return to the ticker.
	m, _ = updateModel(t, m, tea.KeyPressMsg(tea.Key{Code: 'e'}))
	if m.showEvents || m.showLogs {
		t.Errorf("e: events=%v logs=%v", m.showEvents, m.showLogs)
	}
	// 'l' switches to the log tail and hides the events.
	m, cmd := updateModel(t, m, tea.KeyPressMsg(tea.Key{Code: 'l'}))
	if !m.showLogs || m.showEvents {
		t.Errorf("l: logs=%v events=%v", m.showLogs, m.showEvents)
	}
	if cmd == nil {
		t.Error("l should return a log tail command")
	}
}

func TestQuitKeys(t *testing.T) {
	for _, k := range []tea.KeyPressMsg{
		tea.KeyPressMsg(tea.Key{Code: 'q'}),
		tea.KeyPressMsg(tea.Key{Code: 'c', Mod: tea.ModCtrl}),
	} {
		m := baseModel(t)
		_, cmd := updateModel(t, m, k)
		if cmd == nil {
			t.Fatalf("quit key %q returned no command", k)
		}
		if msg := cmd(); msg.(tea.QuitMsg) != (tea.QuitMsg{}) {
			t.Fatalf("quit key %q returned %T", k, msg)
		}
	}
}

func TestHelpToggle(t *testing.T) {
	m := baseModel(t)
	m, _ = updateModel(t, m, tea.KeyPressMsg(tea.Key{Code: '?'}))
	if !m.help {
		t.Error("? should open help")
	}
	m, _ = updateModel(t, m, tea.KeyPressMsg(tea.Key{Code: '?'}))
	if m.help {
		t.Error("? should close help")
	}
}

func TestZoomAndLiveTailReset(t *testing.T) {
	m := baseModel(t)
	res := resultWithJobs(tj("job_a", "running"))
	res.Points = map[SeriesKey][]Point{
		{JobID: "job_a", Metric: "loss"}: {
			{X: 1, Y: 1, Seq: 1}, {X: 2, Y: 2, Seq: 2}, {X: 3, Y: 3, Seq: 3},
		},
	}
	m, _ = updateModel(t, m, refreshMsg{Gen: m.gen, Result: res})
	if len(m.metrics) != 1 || m.metrics[0] != "loss" {
		t.Fatalf("metrics = %v", m.metrics)
	}

	m, _ = updateModel(t, m, tea.KeyPressMsg(tea.Key{Code: '+'}))
	if m.liveTail {
		t.Error("zoom should leave live tail mode")
	}
	if len(m.views) != 1 {
		t.Fatalf("zoom should store a view window, got %v", m.views)
	}
	minX, maxX, ok := m.metricWindow(0)
	if !ok || !(maxX > minX) {
		t.Errorf("zoomed window invalid: %v %v %v", minX, maxX, ok)
	}
	m, _ = updateModel(t, m, tea.KeyPressMsg(tea.Key{Code: 't'}))
	if !m.liveTail || len(m.views) != 0 {
		t.Error("t should restore live tail")
	}
}

func TestPanMovesWindow(t *testing.T) {
	m := baseModel(t)
	res := resultWithJobs(tj("job_a", "running"))
	res.Points = map[SeriesKey][]Point{
		{JobID: "job_a", Metric: "loss"}: {{X: 1, Y: 1, Seq: 1}, {X: 100, Y: 2, Seq: 2}},
	}
	m, _ = updateModel(t, m, refreshMsg{Gen: m.gen, Result: res})
	m, _ = updateModel(t, m, tea.KeyPressMsg(tea.Key{Code: ']'}))
	if len(m.views) != 1 {
		t.Fatalf("pan should create a window, got %v", m.views)
	}
	before := m.views[0]
	m, _ = updateModel(t, m, tea.KeyPressMsg(tea.Key{Code: '['}))
	after := m.views[0]
	if after[0] >= before[0] {
		t.Error("[ should pan left")
	}
}

func TestTabCyclesFocus(t *testing.T) {
	m := baseModel(t)
	m, _ = updateModel(t, m, tea.KeyPressMsg(tea.Key{Code: tea.KeyTab}))
	if m.focus != focusCharts {
		t.Errorf("tab focus = %v, want charts", m.focus)
	}
	m, _ = updateModel(t, m, tea.KeyPressMsg(tea.Key{Code: tea.KeyTab}))
	if m.focus != focusLower {
		t.Errorf("tab focus = %v, want lower", m.focus)
	}
	m, _ = updateModel(t, m, tea.KeyPressMsg(tea.Key{Code: tea.KeyTab}))
	if m.focus != focusJobs {
		t.Errorf("tab focus = %v, want jobs", m.focus)
	}
}

func TestTickSchedulesNextRefresh(t *testing.T) {
	m := baseModel(t)
	_, cmd := updateModel(t, m, tickMsg{})
	if cmd == nil {
		t.Fatal("tick should return a refresh command")
	}
}

func TestRefreshSchedulesTick(t *testing.T) {
	m := baseModel(t)
	_, cmd := updateModel(t, m, refreshMsg{Gen: m.gen, Result: resultWithJobs(tj("job_a", "completed"))})
	if cmd == nil {
		t.Fatal("refresh should schedule a tick")
	}
}

func TestBackgroundColorMsg(t *testing.T) {
	m := baseModel(t)
	m, _ = updateModel(t, m, tea.BackgroundColorMsg{})
	// IsDark on a zero color is deterministic; just assert no crash and flag
	// unchanged semantics.
	_ = m.dark
}

func TestRefreshIntervalRule(t *testing.T) {
	cfg := DefaultConfig("x")
	cfg.RefreshRunning = 250 * time.Millisecond
	cfg.RefreshIdle = time.Second
	if got := RefreshInterval(cfg, []JobSummary{{Status: "running"}}); got != cfg.RefreshRunning {
		t.Errorf("running interval = %v", got)
	}
	if got := RefreshInterval(cfg, []JobSummary{{Status: "completed"}, {Status: "failed"}}); got != cfg.RefreshIdle {
		t.Errorf("idle interval = %v", got)
	}
	if got := RefreshInterval(cfg, nil); got != cfg.RefreshIdle {
		t.Errorf("empty interval = %v", got)
	}
}

func TestMouseClickSelectsJob(t *testing.T) {
	m := windowed(t, baseModel(t), 120, 40)
	m, _ = updateModel(t, m, refreshMsg{Gen: m.gen, Result: resultWithJobs(
		tj("job_a", "running"), tj("job_b", "running"), tj("job_c", "running"))})
	lay := m.computeLayout()
	// Row 0 of the box is the "Jobs" title. Without progress series the selected
	// job takes 2 content rows (header + details) and the others 1 each, so
	// with job 0 selected: rows 0-1 = job 0, row 2 = job 1, row 3 = job 2.
	click := func(row int) {
		m, _ = updateModel(t, m, tea.MouseClickMsg(tea.Mouse{X: lay.jobs.x + 2, Y: lay.jobs.y + 1 + row}))
	}
	click(2)
	if m.selected != 1 {
		t.Errorf("click on row 2 should select index 1, got %d", m.selected)
	}
	// Now job 1 is selected: row 0 = job 0, rows 1-2 = job 1 (details row
	// still maps to it), row 3 = job 2.
	click(2)
	if m.selected != 1 {
		t.Errorf("click on the selected job's details row should keep index 1, got %d", m.selected)
	}
	click(0)
	if m.selected != 0 {
		t.Errorf("click on row 0 should select index 0, got %d", m.selected)
	}
	// A click inside the "Jobs" title row must not change the selection.
	m, _ = updateModel(t, m, tea.MouseClickMsg(tea.Mouse{X: lay.jobs.x + 2, Y: lay.jobs.y}))
	if m.selected != 0 {
		t.Errorf("click on title row should not select, got %d", m.selected)
	}
}

func TestMouseClickSetsCrosshair(t *testing.T) {
	m := windowed(t, baseModel(t), 120, 40)
	res := resultWithJobs(tj("job_a", "running"))
	res.Points = map[SeriesKey][]Point{
		{JobID: "job_a", Metric: "loss"}: {{X: 1, Y: 1, Seq: 1}, {X: 100, Y: 2, Seq: 2}},
	}
	m, _ = updateModel(t, m, refreshMsg{Gen: m.gen, Result: res})
	lay := m.computeLayout()
	if len(lay.charts) == 0 {
		t.Fatal("no chart rects")
	}
	cr := lay.charts[0]
	m, _ = updateModel(t, m, tea.MouseClickMsg(tea.Mouse{X: cr.x + 10, Y: cr.y + 2}))
	if _, ok := m.crosshair[cr.metric]; !ok {
		t.Error("click on chart should set crosshair")
	}
}

func TestMouseWheelZoomsChart(t *testing.T) {
	m := windowed(t, baseModel(t), 120, 40)
	res := resultWithJobs(tj("job_a", "running"))
	res.Points = map[SeriesKey][]Point{
		{JobID: "job_a", Metric: "loss"}: {{X: 1, Y: 1, Seq: 1}, {X: 100, Y: 2, Seq: 2}},
	}
	m, _ = updateModel(t, m, refreshMsg{Gen: m.gen, Result: res})
	lay := m.computeLayout()
	if len(lay.charts) == 0 {
		t.Fatal("no chart rects")
	}
	cr := lay.charts[0]
	m, _ = updateModel(t, m, tea.MouseWheelMsg(tea.Mouse{
		X: cr.x + 10, Y: cr.y + 2, Button: tea.MouseWheelUp}))
	if len(m.views) == 0 {
		t.Error("wheel should create a zoom window")
	}
}

func TestLogTailStaleGenerationIgnored(t *testing.T) {
	m := baseModel(t)
	m, _ = updateModel(t, m, refreshMsg{Gen: m.gen, Result: resultWithJobs(tj("job_a", "running"))})
	gen := m.gen
	m, _ = updateModel(t, m, logTailMsg{Gen: gen - 1, JobID: "job_a", Lines: []string{"stale"}})
	if len(m.logTail) != 0 {
		t.Error("stale log tail result applied")
	}
}

func TestLogTailWrongJobIgnored(t *testing.T) {
	m := baseModel(t)
	m, _ = updateModel(t, m, refreshMsg{Gen: m.gen, Result: resultWithJobs(tj("job_a", "running"))})
	m, _ = updateModel(t, m, logTailMsg{Gen: m.gen, JobID: "other", Lines: []string{"x"}})
	if len(m.logTail) != 0 {
		t.Error("log tail for a non-selected job applied")
	}
}
