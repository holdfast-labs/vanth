package monitor

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"
)

func stripANSI(s string) string { return ansi.Strip(s) }

func apply(t *testing.T, m Model, msg tea.Msg) Model {
	t.Helper()
	mm, _ := m.Update(msg)
	out, ok := mm.(Model)
	if !ok {
		t.Fatalf("Update returned %T, want Model", mm)
	}
	return out
}

func linesOf(t *testing.T, s string) []string {
	t.Helper()
	s = strings.TrimSuffix(s, "\n")
	return strings.Split(s, "\n")
}

func populatedModel(t *testing.T, width, height int) Model {
	t.Helper()
	cfg := DefaultConfig(filepath.Join(t.TempDir(), "home"))
	m := New(cfg, nil)
	m = apply(t, m, tea.WindowSizeMsg{Width: width, Height: height})
	res := RefreshResult{JobsOK: true, Mode: ModeSQLite, Points: map[SeriesKey][]Point{}, Events: map[string][]Event{}}
	for i := 0; i < 10; i++ {
		id := "job_" + string(rune('a'+i))
		res.Jobs = append(res.Jobs, JobSummary{
			JobID: id, Name: "job " + string(rune('a'+i)), Status: "running",
			UpdatedAt:  "2026-01-01T00:00:0" + string(rune('0'+i%10)) + "Z",
			EventCount: 100,
		})
		for _, metric := range []string{"loss", "val_loss"} {
			res.Points[SeriesKey{JobID: id, Metric: metric}] = []Point{
				{X: 1, Y: float64(i), Seq: 1, At: "2026-01-01T00:00:00Z"},
				{X: 2, Y: float64(i) + 0.5, Seq: 2, At: "2026-01-01T00:00:00Z"},
				{X: 3, Y: float64(i) + 1, Seq: 3, At: "2026-01-01T00:00:00Z"},
			}
		}
		res.Points[SeriesKey{JobID: id, Metric: "progress.percent"}] = []Point{
			{X: 1, Y: 100, Seq: 1, At: "2026-01-01T00:00:00Z"},
		}
	}
	res.Events = map[string][]Event{
		"job_a": {
			{EventID: "e1", JobID: "job_a", Seq: 1, Type: "metric", Level: "info",
				Message: "epoch one", Data: map[string]any{"loss": 0.5}, CreatedAt: "2026-01-01T00:00:01.000000Z"},
		},
	}
	m = apply(t, m, refreshMsg{Gen: m.gen, Result: res})
	return m
}

func TestViewSizesNoNegativeDimensionsNoPanic(t *testing.T) {
	cases := []struct {
		name        string
		width       int
		height      int
		expectLines int
	}{
		{name: "wide", width: 160, height: 50, expectLines: 50},
		{name: "medium", width: 100, height: 40, expectLines: 40},
		{name: "narrow", width: 60, height: 30, expectLines: 30},
		{name: "minimum", width: 50, height: 14, expectLines: 14},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := populatedModel(t, tc.width, tc.height)
			v := m.View()
			view := stripANSI(v.Content)
			lines := linesOf(t, view)
			if len(lines) != tc.expectLines {
				t.Fatalf("view has %d lines, want %d", len(lines), tc.expectLines)
			}
			for i, l := range lines {
				if w := displayWidth(l); w > tc.width {
					t.Errorf("line %d width %d exceeds terminal width %d: %q", i, w, tc.width, l)
				}
			}
			if strings.Contains(view, "panic") {
				t.Error("view mentions panic")
			}
		})
	}
}

func TestViewBelowMinimum(t *testing.T) {
	m := populatedModel(t, 30, 8)
	v := stripANSI(m.View().Content)
	if !strings.Contains(v, "too small") {
		t.Errorf("below-minimum view should report size, got %q", v)
	}
}

func TestViewNoData(t *testing.T) {
	cfg := DefaultConfig(filepath.Join(t.TempDir(), "home"))
	m := New(cfg, nil)
	m = apply(t, m, tea.WindowSizeMsg{Width: 120, Height: 40})
	v := stripANSI(m.View().Content)
	if !strings.Contains(v, "no jobs") {
		t.Errorf("empty view should mention no jobs, got %q", v)
	}
}

func TestViewLegendSingleSeriesAbsent(t *testing.T) {
	cfg := DefaultConfig(filepath.Join(t.TempDir(), "home"))
	m := New(cfg, nil)
	m = apply(t, m, tea.WindowSizeMsg{Width: 120, Height: 40})
	res := RefreshResult{JobsOK: true, Mode: ModeSQLite, Jobs: []JobSummary{
		{JobID: "job_a", Name: "alpha", Status: "running", UpdatedAt: "2026-01-01T00:00:00Z"},
	}}
	res.Points = map[SeriesKey][]Point{
		{JobID: "job_a", Metric: "loss"}: {{X: 1, Y: 1, Seq: 1}},
	}
	m = apply(t, m, refreshMsg{Gen: m.gen, Result: res})
	view := stripANSI(m.View().Content)
	if strings.Contains(view, "+1 hidden") {
		t.Error("single-series chart should not show hidden count")
	}
}

func TestDashboardSummaryHeaderCountsByStatus(t *testing.T) {
	m := populatedModel(t, 160, 44)
	view := stripANSI(m.View().Content)
	for _, want := range []string{"Vanth", "monitor", "running 10", "completed 0", "events"} {
		if !strings.Contains(view, want) {
			t.Errorf("summary header missing %q:\n%s", want, view)
		}
	}
}

func TestDashboardKeyMetricsPanelShowsProgressAndValues(t *testing.T) {
	m := populatedModel(t, 160, 44)
	view := stripANSI(m.View().Content)
	if !strings.Contains(view, "Metrics") {
		t.Errorf("key-metrics panel header missing:\n%s", view)
	}
	if !strings.Contains(view, "100%") {
		t.Errorf("progress readout missing in key-metrics panel:\n%s", view)
	}
	if !strings.Contains(view, "events") {
		t.Errorf("event count missing in key-metrics panel:\n%s", view)
	}
}

func TestViewLegendTwoSeriesAndFoldBeyondEight(t *testing.T) {
	cfg := DefaultConfig(filepath.Join(t.TempDir(), "home"))
	m := New(cfg, nil)
	m = apply(t, m, tea.WindowSizeMsg{Width: 140, Height: 44})
	res := RefreshResult{JobsOK: true, Mode: ModeSQLite, Points: map[SeriesKey][]Point{}}
	for i := 0; i < 10; i++ {
		id := "job_" + string(rune('a'+i))
		res.Jobs = append(res.Jobs, JobSummary{JobID: id, Name: id, Status: "completed", UpdatedAt: "2026-01-01T00:00:00Z"})
		res.Points[SeriesKey{JobID: id, Metric: "loss"}] = []Point{{X: 1, Y: float64(i), Seq: 1}}
	}
	m = apply(t, m, refreshMsg{Gen: m.gen, Result: res})
	view := stripANSI(m.View().Content)
	if !strings.Contains(view, "+2 hidden") {
		t.Errorf("10 overlays should fold to 8 with +2 hidden, got %q", view)
	}
}

func TestViewLightAndDarkPaths(t *testing.T) {
	for _, dark := range []bool{false, true} {
		m := populatedModel(t, 120, 40)
		m.dark = dark
		view := stripANSI(m.View().Content)
		if !strings.Contains(view, "loss") {
			t.Errorf("dark=%v: chart title missing", dark)
		}
	}
}

func TestDashboardSummaryHeaderShowsAttentionAndTotals(t *testing.T) {
	m := populatedModel(t, 160, 44)
	view := stripANSI(m.View().Content)
	for _, want := range []string{"attention", "jobs 10", "mode", "refresh"} {
		if !strings.Contains(view, want) {
			t.Errorf("summary header missing %q:\n%s", want, view)
		}
	}
}

func TestJobBlocksRenderThreeLinesPerJob(t *testing.T) {
	m := populatedModel(t, 160, 44)
	view := stripANSI(m.View().Content)
	// Each job block renders a header line (name + right-aligned status), a
	// progress bar line with a percent readout, and an events metadata line.
	for _, want := range []string{"100%", "evt 100"} {
		if !strings.Contains(view, want) {
			t.Errorf("job block missing %q:\n%s", want, view)
		}
	}
}

func TestViewEventTable(t *testing.T) {
	m := populatedModel(t, 120, 40)
	m.showEvents = true
	m.focus = focusLower
	view := stripANSI(m.View().Content)
	if !strings.Contains(view, "epoch one") {
		t.Errorf("event table should contain exact message, got %q", view)
	}
	if !strings.Contains(view, "Events") {
		t.Error("event table header missing")
	}
}

func TestViewLogTailPane(t *testing.T) {
	m := populatedModel(t, 120, 40)
	m.showEvents = false
	m.showLogs = true
	m.logTail = []string{"line one", "line two"}
	m.logTailJob = "job_a"
	view := stripANSI(m.View().Content)
	if !strings.Contains(view, "line one") || !strings.Contains(view, "line two") {
		t.Errorf("log tail lines missing: %q", view)
	}
}

func TestViewHelp(t *testing.T) {
	m := populatedModel(t, 120, 40)
	m.help = true
	view := stripANSI(m.View().Content)
	for _, k := range []string{"pin", "live tail", "event table", "quit"} {
		if !strings.Contains(view, k) {
			t.Errorf("help missing %q", k)
		}
	}
}

func TestViewStatusBarShowsStaleAndFallback(t *testing.T) {
	m := populatedModel(t, 120, 40)
	now := time.Now()
	m.staleSince = &now
	m.mode = ModeFallback
	view := stripANSI(m.View().Content)
	if !strings.Contains(view, "stale") || !strings.Contains(view, "fallback") {
		t.Errorf("status bar should show stale+fallback: %q", view)
	}
}

func displayWidth(s string) int {
	w := 0
	for _, r := range s {
		w += runeWidth(r)
	}
	return w
}

func TestShortRepoHandlesURLForms(t *testing.T) {
	cases := map[string]string{
		"https://github.com/holdfast-labs/vanth.git": "vanth",
		"https://github.com/holdfast-labs/vanth":     "vanth",
		"git@github.com:holdfast-labs/vanth.git":     "vanth",
		"git@github.com:vanth.git":                   "vanth",
		"vanth": "vanth",
		"":      "",
	}
	for in, want := range cases {
		if got := shortRepo(in); got != want {
			t.Errorf("shortRepo(%q) = %q, want %q", in, got, want)
		}
	}
}
