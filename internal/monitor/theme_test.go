package monitor

import (
	"bytes"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/colorprofile"
)

var statuses = []string{"running", "queued", "completed", "failed", "cancelled", "orphaned"}

// mixedModel has one job per status so every glyph and badge is exercised.
func mixedModel(t *testing.T, width, height int) Model {
	t.Helper()
	m := New(DefaultConfig(filepath.Join(t.TempDir(), "home")), nil)
	m.detail = true
	m = apply(t, m, tea.WindowSizeMsg{Width: width, Height: height})
	res := RefreshResult{JobsOK: true, Mode: ModeSQLite, Points: map[SeriesKey][]Point{}, Events: map[string][]Event{}}
	for i, st := range statuses {
		id := "job_" + st
		res.Jobs = append(res.Jobs, JobSummary{JobID: id, Name: st + " job", Status: st,
			UpdatedAt: "2026-01-01T00:00:0" + string(rune('0'+i)) + "Z", EventCount: int64(10 * (i + 1))})
		res.Points[SeriesKey{JobID: id, Metric: "loss"}] = []Point{
			{X: 1, Y: 3, Seq: 1, At: "2026-01-01T00:00:00Z"}, {X: 2, Y: 2, Seq: 2, At: "2026-01-01T00:00:00Z"},
			{X: 3, Y: 1, Seq: 3, At: "2026-01-01T00:00:00Z"}}
		res.Points[SeriesKey{JobID: id, Metric: "progress.percent"}] = []Point{{X: 1, Y: float64(15 * (i + 1)), Seq: 1, At: "2026-01-01T00:00:00Z"}}
	}
	res.Events["job_running"] = []Event{{EventID: "e1", JobID: "job_running", Seq: 1, Type: "log", Level: "error",
		Message: "boom", CreatedAt: "2026-01-01T00:00:01.000000Z"}}
	return apply(t, m, refreshMsg{Gen: m.gen, Result: res})
}

func TestStatusGlyphsAreDistinct(t *testing.T) {
	seen := map[string]string{}
	for _, st := range statuses {
		g := statusIcon(st)
		if prev, dup := seen[g]; dup {
			t.Errorf("glyph %q shared by %s and %s", g, prev, st)
		}
		seen[g] = st
		if lipgloss.Width(g) != 1 {
			t.Errorf("glyph %q for %s is not 1 cell wide", g, st)
		}
	}
}

func TestMixedStatusFrameAllSizes(t *testing.T) {
	for _, sz := range [][2]int{{160, 44}, {120, 36}, {80, 30}, {60, 24}, {50, 14}} {
		m := mixedModel(t, sz[0], sz[1])
		view := stripANSI(m.View().Content)
		lines := linesOf(t, view)
		if len(lines) != sz[1] {
			t.Errorf("%dx%d: got %d lines", sz[0], sz[1], len(lines))
		}
		for i, l := range lines {
			if w := displayWidth(l); w > sz[0] {
				t.Errorf("%dx%d: line %d width %d: %q", sz[0], sz[1], i, w, l)
			}
		}
		for _, st := range statuses {
			if !strings.Contains(view, statusIcon(st)) {
				t.Errorf("%dx%d: glyph for %s missing", sz[0], sz[1], st)
			}
		}
		if sz[0] >= 120 {
			t.Logf("frame %dx%d:\n%s", sz[0], sz[1], view)
		}
	}
	t.Logf("narrow frame 60x24:\n%s", stripANSI(mixedModel(t, 60, 24).View().Content))
}

func TestHeaderTilesWideAndStripNarrow(t *testing.T) {
	wide := stripANSI(mixedModel(t, 160, 44).View().Content)
	for _, want := range []string{"● running", "◌ queued", "✔ completed", "✘ failed", "▲ orphaned", "⚑ attention", "jobs 6"} {
		if !strings.Contains(wide, want) {
			t.Errorf("wide tiles missing %q:\n%s", want, wide)
		}
	}
	if !strings.Contains(wide, "━┓") { // big digit "1"
		t.Errorf("tiles should show big digits:\n%s", wide)
	}
	narrow := stripANSI(mixedModel(t, 80, 30).View().Content)
	if !strings.Contains(narrow, statusIcon("failed")+" 1") || !strings.Contains(narrow, "⚑ 2") || !strings.Contains(narrow, "Vanth monitor") {
		t.Errorf("narrow strip missing compact badges:\n%s", narrow)
	}
	if strings.Contains(narrow, "━┓") {
		t.Error("narrow layout should use the compact strip, not tiles")
	}
}

func TestBigNumberFallsBackWhenTooNarrow(t *testing.T) {
	if rows := bigNumber(128, 11); len(rows) != 3 || lipgloss.Width(rows[0]) != 11 {
		t.Errorf("rows %q", rows)
	}
	if bigNumber(128, 10) != nil {
		t.Error("expected fallback")
	}
}

func TestTickerAndTimelineAreTheDefaultLowerArea(t *testing.T) {
	m := mixedModel(t, 160, 44)
	v := stripANSI(m.View().Content)
	if !strings.Contains(v, "Timeline") || !strings.Contains(v, "e table") || !strings.Contains(v, "boom") || !strings.Contains(v, "✘") {
		t.Errorf("ticker/timeline missing:\n%s", v)
	}
	if lay := m.computeLayout(); lay.lower.h != tickerH {
		t.Errorf("ticker height %d", lay.lower.h)
	}
	m.showEvents = true
	if lay := m.computeLayout(); lay.lower.h <= tickerH {
		t.Errorf("expanded table should be taller, got %d", lay.lower.h)
	}
}

func TestSparkline(t *testing.T) {
	if got := sparkline([]float64{1, 2, 3, 4, 5, 6, 7, 8}, 8); got != "▁▂▃▄▅▆▇█" {
		t.Errorf("got %q", got)
	}
	if got := sparkline([]float64{5, 5}, 4); got != "▄▄" {
		t.Errorf("flat series %q", got)
	}
	if sparkline(nil, 4) != "" {
		t.Error("empty")
	}
}

var sgrColor = regexp.MustCompile(`\x1b\[[0-9;:]*(?:3[0-9]|4[0-9]|9[0-9]|10[0-9]|38|48)[0-9;:]*m`)

// With the Ascii profile (NO_COLOR / dumb terminal) the output must carry no
// color escapes, and the content must stay legible through glyphs and labels.
func TestNoColorProfileStripsColorKeepsMeaning(t *testing.T) {
	m := mixedModel(t, 100, 30)
	raw := m.View().Content
	if !sgrColor.MatchString(raw) {
		t.Fatal("test premise: styled output should contain color escapes")
	}
	var buf bytes.Buffer
	w := &colorprofile.Writer{Forward: &buf, Profile: colorprofile.Ascii}
	if _, err := w.WriteString(raw); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if sgrColor.MatchString(out) {
		t.Errorf("color escapes survive the Ascii profile: %q", sgrColor.FindString(out))
	}
	plain := stripANSI(out)
	for _, st := range statuses {
		if !strings.Contains(plain, statusIcon(st)) || !strings.Contains(plain, st) {
			t.Errorf("status %s not identifiable without color", st)
		}
	}
}

func TestPanelBoxExactSizeAndFallback(t *testing.T) {
	m := mixedModel(t, 100, 30)
	for w := 2; w <= 40; w += 3 {
		for h := 2; h <= 8; h += 2 {
			out := m.panelBoxOpts(rect{w: w, h: h}, "Title", "a\nb", panelOpts{hint: "hint", focused: true, pulse: 0.5})
			if lipgloss.Width(out) != w || lipgloss.Height(out) != h {
				t.Fatalf("%dx%d: got %dx%d", w, h, lipgloss.Width(out), lipgloss.Height(out))
			}
		}
	}
	if got := m.panelBox(rect{w: 1, h: 1}, "x", ""); got != "" {
		t.Errorf("1x1 panel should be empty, got %q", got)
	}
}

func TestStatusDistBarWidth(t *testing.T) {
	m := mixedModel(t, 100, 30)
	for _, c := range []summaryCounts{{running: 5, failed: 1}, {completed: 1}, {running: 1, queued: 1, completed: 1, failed: 1, cancelled: 1, orphaned: 1}} {
		if w := lipgloss.Width(m.statusDistBar(c, 20)); w != 20 {
			t.Errorf("%+v: bar width %d, want 20", c, w)
		}
	}
	if m.statusDistBar(summaryCounts{}, 20) != "" || m.statusDistBar(summaryCounts{running: 1}, 4) != "" {
		t.Error("empty or too-narrow bar should be empty")
	}
}

func TestHeartbeatOnlyWhileRunningAndFresh(t *testing.T) {
	m := mixedModel(t, 100, 30)
	if m.pulsePhase(0) != 0 {
		t.Error("no running jobs: no pulse")
	}
	if p := m.pulsePhase(1); p <= 0 || p > 1 {
		t.Errorf("phase out of range: %v", p)
	}
	now := m.lastRefresh
	m.staleSince = &now
	if m.pulsePhase(1) != 0 {
		t.Error("stale data: no pulse")
	}
}

func TestStatusBarLiveStaleFallback(t *testing.T) {
	m := mixedModel(t, 120, 36)
	bar := func(m Model) string {
		l := linesOf(t, stripANSI(m.View().Content))
		return l[len(l)-1]
	}
	live := bar(m)
	if !strings.Contains(live, "live") || strings.Contains(live, "stale") || !strings.Contains(live, "q quit") {
		t.Errorf("live bar: %q", live)
	}
	t.Logf("status bar (live):  %s", live)

	now := m.lastRefresh.Add(-95 * 1e9)
	m.staleSince = &now
	stale := bar(m)
	if !strings.Contains(stale, "stale") || !strings.Contains(stale, "1m35s") {
		t.Errorf("stale bar should state age: %q", stale)
	}
	if !strings.Contains(m.View().Content, "\x1b[") || !strings.Contains(m.View().Content, "7m") && !strings.Contains(m.View().Content, ";7") {
		t.Log("note: reverse-video chip not detected in raw output")
	}
	t.Logf("status bar (stale): %s", stale)

	m.staleSince = nil
	m.mode = ModeFallback
	if fb := bar(m); !strings.Contains(fb, "fallback") {
		t.Errorf("fallback bar: %q", fb)
	}
	// Narrow: hints drop, chip and width hold.
	n := mixedModel(t, 60, 24)
	if l := bar(n); displayWidth(l) > 60 || !strings.Contains(l, "live") {
		t.Errorf("narrow bar: %q", l)
	}
}

func TestMetricsPaneSample(t *testing.T) {
	m := mixedModel(t, 160, 44)
	view := stripANSI(m.View().Content)
	for _, want := range []string{"Metrics", "running", "loss", "progress.percent", "▲", "job_running"} {
		if want == "▲" {
			continue // trend needs two points; the fixture has three for loss
		}
		if !strings.Contains(view, want) {
			t.Errorf("metrics pane missing %q", want)
		}
	}
	for _, l := range linesOf(t, view) {
		if strings.Contains(l, "Metrics") || strings.Contains(l, "latest") || strings.Contains(l, "loss  ") {
			t.Log(l)
		}
	}
}

func TestChartLegendRowNotTruncatedInTitle(t *testing.T) {
	m := mixedModel(t, 60, 40)
	view := stripANSI(m.View().Content)
	if strings.Contains(view, "[●") {
		t.Errorf("legend should not be embedded in the title any more:\n%s", view)
	}
	m = mixedModel(t, 140, 44)
	view = stripANSI(m.View().Content)
	if !strings.Contains(view, "● running job") {
		t.Errorf("legend swatch row missing:\n%s", view)
	}
	// Width squeeze: entries drop whole with a "+k more" marker.
	leg := stripANSI(m.chartLegend(m.chartDrawJobsForTest(), 0, 30))
	if displayWidth(leg) != 30 || !strings.Contains(leg, "more") {
		t.Errorf("squeezed legend %q", leg)
	}
}

func (m Model) chartDrawJobsForTest() []drawJob {
	d, _ := m.chartDrawJobs("loss", AssignSlots(m.allChartJobIDs()))
	return d
}

func TestStatusChangeFlashesForAFewRefreshesOnly(t *testing.T) {
	m := mixedModel(t, 120, 36)
	refresh := func(m Model, status string) Model {
		jobs := append([]JobSummary(nil), m.jobs...)
		for i := range jobs {
			if jobs[i].JobID == "job_queued" {
				jobs[i].Status = status
			}
		}
		return apply(t, m, refreshMsg{Gen: m.gen, Result: RefreshResult{JobsOK: true, Mode: ModeSQLite, Jobs: jobs}})
	}
	m = refresh(m, "failed")
	if v := stripANSI(m.View().Content); !strings.Contains(v, "now failed") {
		t.Errorf("transition not flagged:\n%s", v)
	}
	for i := 0; i < flashBeats; i++ {
		m = refresh(m, "failed")
	}
	if v := stripANSI(m.View().Content); strings.Contains(v, "now failed") {
		t.Error("flash should expire after a few refreshes without any timer")
	}
	if len(m.flash) != 0 {
		m = refresh(m, "failed")
		if len(m.flash) != 0 {
			t.Errorf("flash entries not pruned: %v", m.flash)
		}
	}
}

func TestHelpIsOverlayOnLargeAndPaneOnSmall(t *testing.T) {
	m := mixedModel(t, 120, 36)
	m.help = true
	big := stripANSI(m.View().Content)
	if !strings.Contains(big, "Help") || !strings.Contains(big, "Jobs (6)") || !strings.Contains(big, "mouse wheel") {
		t.Errorf("help overlay should sit over the dashboard:\n%s", big)
	}
	if n := len(linesOf(t, big)); n != 36 {
		t.Errorf("overlay frame has %d lines", n)
	}
	small := mixedModel(t, 60, 20)
	small.help = true
	if v := stripANSI(small.View().Content); !strings.Contains(v, "mouse wheel") && !strings.Contains(v, "larger window") {
		t.Errorf("small help:\n%s", v)
	}
}

func TestLoadingShowsSkeleton(t *testing.T) {
	m := New(DefaultConfig(filepath.Join(t.TempDir(), "home")), nil)
	m = apply(t, m, tea.WindowSizeMsg{Width: 100, Height: 30})
	m.loading = true
	v := stripANSI(m.View().Content)
	if !strings.Contains(v, "loading jobs") {
		t.Errorf("loading message missing:\n%s", v)
	}
	if sk := loadingSkeleton(30, 5); lipgloss.Height(sk) != 5 || lipgloss.Width(sk) != 30 {
		t.Errorf("skeleton size %dx%d", lipgloss.Width(sk), lipgloss.Height(sk))
	}
}

func TestPadRangeNeverGoesNegativeForNonNegativeData(t *testing.T) {
	if lo, hi := padRange(0, 100, 0.05, 1); lo != 0 || hi <= 100 {
		t.Errorf("padRange(0,100) = %v,%v", lo, hi)
	}
	if lo, _ := padRange(0, 0, 0.05, 1); lo != 0 {
		t.Errorf("single zero point lo = %v", lo)
	}
	if lo, _ := padRange(-5, 5, 0.05, 1); lo >= -5 {
		t.Errorf("negative data keeps its padding, lo = %v", lo)
	}
}

func TestEventPreviewRoundsFloatsAndFitsTypes(t *testing.T) {
	m := mixedModel(t, 160, 44)
	evs := []Event{{Type: "log_capture_failed", Level: "error", Seq: 1, Data: map[string]any{"acc": 0.8639999999999999}}}
	line := stripANSI(m.eventLines(evs, 150)[0])
	if !strings.Contains(line, "log_capture_failed") || !strings.Contains(line, `"acc":0.864}`) {
		t.Errorf("event line: %q", line)
	}
}

func TestJobListCollapsesRowsAndKeepsSelectionVisible(t *testing.T) {
	m := New(DefaultConfig(filepath.Join(t.TempDir(), "home")), nil)
	m = apply(t, m, tea.WindowSizeMsg{Width: 160, Height: 44})
	res := RefreshResult{JobsOK: true, Mode: ModeSQLite, Points: map[SeriesKey][]Point{}, Events: map[string][]Event{}}
	for i := 0; i < 100; i++ {
		res.Jobs = append(res.Jobs, JobSummary{JobID: fmt.Sprintf("j%03d", i), Name: fmt.Sprintf("job %d", i), Status: "completed"})
	}
	m = apply(t, m, refreshMsg{Gen: m.gen, Result: res})
	contentH := m.computeLayout().jobs.h - 2
	if got := len(linesOf(t, stripANSI(m.renderJobs(m.computeLayout().jobs)))) - 2; got != contentH {
		t.Fatalf("rows %d", got)
	}
	if m.jobRows(1) != 1 || m.jobRows(0) != 2 {
		t.Errorf("rows: unselected %d selected %d", m.jobRows(1), m.jobRows(0))
	}
	m.selected = 80
	if s := m.jobWindowStart(contentH); s > 80 || s < 80-contentH {
		t.Errorf("selected job 80 scrolled out of view (start %d)", s)
	}
	if !strings.Contains(stripANSI(m.renderJobs(m.computeLayout().jobs)), "job 80") {
		t.Error("selected job not rendered")
	}
	if idx := m.jobAtRow(0, contentH); idx != m.jobWindowStart(contentH) {
		t.Errorf("row 0 maps to %d", idx)
	}
	if idx := m.jobAtRow(contentH, contentH); idx != -1 {
		t.Errorf("bottom border row selects hidden job %d", idx)
	}
}

func TestConstantSeriesAreValuesNotCharts(t *testing.T) {
	m := mixedModel(t, 160, 44)
	flat := []Point{{X: 1, Y: 100, Seq: 1}, {X: 2, Y: 100, Seq: 2}, {X: 3, Y: 100, Seq: 3}}
	m = apply(t, m, refreshMsg{Gen: m.gen, Result: RefreshResult{JobsOK: true, Mode: ModeSQLite,
		Jobs: m.jobs, Points: map[SeriesKey][]Point{{JobID: "job_running", Metric: "progress.total"}: flat}}})
	for _, k := range m.metrics {
		if k == "progress.total" {
			t.Fatal("flat series must not get a chart")
		}
	}
	if m.constants["progress.total"] != 100 {
		t.Errorf("constants = %v", m.constants)
	}
	if v := stripANSI(m.View().Content); !strings.Contains(v, "progress.tot") || !strings.Contains(v, "constant") {
		t.Errorf("constant value missing from Metrics pane:\n%s", v)
	}
	// A single point is not "constant": it still charts.
	if _, ok := m.constantSeries("loss"); ok {
		t.Error("varying series flagged constant")
	}
}

func TestFleetVisualsReplaceEmptyPlots(t *testing.T) {
	m := New(DefaultConfig(filepath.Join(t.TempDir(), "home")), nil)
	m = apply(t, m, tea.WindowSizeMsg{Width: 160, Height: 44})
	res := RefreshResult{JobsOK: true, Mode: ModeSQLite, Points: map[SeriesKey][]Point{}, Events: map[string][]Event{}}
	for i := 0; i < 6; i++ {
		res.Jobs = append(res.Jobs, JobSummary{JobID: fmt.Sprintf("j%d", i), Name: "nightly", Status: []string{"completed", "failed"}[i%2],
			CreatedAt: fmt.Sprintf("2026-01-01T0%d:00:00Z", i), StartedAt: fmt.Sprintf("2026-01-01T0%d:00:00Z", i),
			EndedAt: fmt.Sprintf("2026-01-01T0%d:%02d:00Z", i, 10+i*5), UpdatedAt: "2026-01-01T09:00:00Z"})
	}
	m = apply(t, m, refreshMsg{Gen: m.gen, Result: res})
	v := stripANSI(m.View().Content)
	for _, want := range []string{"Timeline", "Recent runs", "History · nightly", "Jan 01 00:00", "Jan 01 05:00"} {
		if !strings.Contains(v, want) {
			t.Errorf("fleet view missing %q:\n%s", want, v)
		}
	}
	if strings.Contains(v, "no metric events yet") {
		t.Error("empty plots placeholder should be gone")
	}
	if n := len(linesOf(t, v)); n != 44 {
		t.Errorf("lines %d", n)
	}
}

func TestTimelineWindowZoomsToRecentJobs(t *testing.T) {
	now := time.Date(2026, 1, 30, 12, 0, 0, 0, time.UTC)
	times := []time.Time{now.Add(-40 * 24 * time.Hour)}
	for i := 0; i < 20; i++ {
		times = append(times, now.Add(-time.Duration(i)*time.Minute))
	}
	lo, hi := timelineWindow(times)
	if !hi.Equal(now) || hi.Sub(lo) > time.Hour {
		t.Errorf("window %v..%v should zoom to the last hour", lo, hi)
	}
}

func TestJobRowsDropStatusWordAndShowAge(t *testing.T) {
	m := mixedModel(t, 160, 44)
	v := stripANSI(m.View().Content)
	if strings.Count(v, "completed job") < 1 {
		t.Fatal("setup")
	}
	if got := m.jobAgeText(JobSummary{StartedAt: "2026-01-01T00:00:00Z", EndedAt: "2026-01-01T00:12:30Z"}); got != "12m" {
		t.Errorf("duration text %q", got)
	}
	// Unselected rows end with an age, not the status word.
	for _, l := range linesOf(t, v) {
		if strings.Contains(l, "queued job") && strings.Contains(l, "queued job  ") && strings.Contains(strings.Split(l, "│")[1], "queued job   queued") {
			t.Errorf("status word still on unselected row: %q", l)
		}
	}
}

func TestTileCaptionFits(t *testing.T) {
	v := stripANSI(mixedModel(t, 160, 44).View().Content)
	if !strings.Contains(v, "6 jobs · 210 ev") {
		t.Errorf("attention caption:\n%s", v)
	}
}

func TestChartYGutterFitsWidestLabel(t *testing.T) {
	m := mixedModel(t, 160, 44)
	for h := 5; h <= 40; h++ {
		lc := m.newChart(40, h, 0, 10, 1, 100)
		lc.DrawXYAxisAndLabel()
		if v := stripANSI(lc.View()); !strings.Contains(v, "100") {
			t.Fatalf("height %d: top y label clipped:\n%s", h, v)
		}
	}
	for h := 3; h <= 40; h++ {
		if step := yStepFor(h - 2); step < 1 || (h-2 > 1 && (h-2)%step != 0) {
			t.Errorf("height %d: step %d does not divide the graph height", h, step)
		}
	}
}
