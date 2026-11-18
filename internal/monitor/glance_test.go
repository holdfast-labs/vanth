package monitor

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/colorprofile"
)

func ago(d time.Duration) string { return time.Now().Add(-d).UTC().Format(time.RFC3339Nano) }

// progressEvents makes n progress events every `every`, the last one `last` ago.
func progressEvents(jobID string, n int, every, last time.Duration, total float64) ([]Event, []Point, []Point) {
	var evs []Event
	var pct, cur []Point
	for k := 1; k <= n; k++ {
		at := ago(last + time.Duration(n-k)*every)
		c := total * 0.5 * float64(k) / float64(n)
		evs = append(evs, Event{EventID: fmt.Sprint(jobID, k), JobID: jobID, Seq: int64(k), Type: "progress",
			Message: fmt.Sprintf("step %d", k), Data: map[string]any{"current": c, "total": total, "unit": "files"}, CreatedAt: at})
		pct = append(pct, Point{X: float64(k), Y: c / total * 100, Seq: int64(k), At: at})
		cur = append(cur, Point{X: float64(k), Y: c, Seq: int64(k), At: at})
	}
	return evs, pct, cur
}

// glanceModel: two healthy running jobs (one with declared panels), one
// stalled, one queued, and finished runs including a failure.
func glanceModel(t *testing.T, w, h int) Model {
	t.Helper()
	m := New(DefaultConfig(filepath.Join(t.TempDir(), "home")), nil)
	m = apply(t, m, tea.WindowSizeMsg{Width: w, Height: h})
	res := RefreshResult{JobsOK: true, Mode: ModeSQLite, Points: map[SeriesKey][]Point{}, Events: map[string][]Event{}}
	add := func(j JobSummary) { res.Jobs = append(res.Jobs, j) }
	add(JobSummary{JobID: "job_train", Name: "train-model", Status: "running", StartedAt: ago(10 * time.Minute), CreatedAt: ago(11 * time.Minute)})
	add(JobSummary{JobID: "job_ingest", Name: "ingest-logs", Status: "running", StartedAt: ago(5 * time.Minute)})
	add(JobSummary{JobID: "job_eval", Name: "eval-suite", Status: "running", StartedAt: ago(30 * time.Minute)})
	add(JobSummary{JobID: "job_q", Name: "thumbnails", Status: "queued", CreatedAt: ago(3 * time.Minute)})
	code := int64(2)
	add(JobSummary{JobID: "job_bad", Name: "deploy", Status: "failed", ExitCode: &code, StartedAt: ago(time.Hour), EndedAt: ago(50 * time.Minute)})
	add(JobSummary{JobID: "job_ok", Name: "lint", Status: "completed", StartedAt: ago(2 * time.Hour), EndedAt: ago(119 * time.Minute)})
	for _, spec := range []struct {
		id          string
		every, last time.Duration
	}{{"job_train", 10 * time.Second, 2 * time.Second}, {"job_ingest", 5 * time.Second, time.Second}, {"job_eval", 20 * time.Second, 9 * time.Minute}} {
		evs, pct, cur := progressEvents(spec.id, 20, spec.every, spec.last, 1000)
		res.Events[spec.id] = evs
		res.Points[SeriesKey{JobID: spec.id, Metric: "progress.percent"}] = pct
		res.Points[SeriesKey{JobID: spec.id, Metric: "progress.current"}] = cur
	}
	res.Events["job_train"] = append(res.Events["job_train"], Event{EventID: "p", JobID: "job_train", Seq: 99, Type: "panels",
		CreatedAt: ago(2 * time.Second), Data: map[string]any{"panels": []any{
			map[string]any{"kind": "gauge", "metric": "gpu", "label": "GPU"},
			map[string]any{"kind": "line", "metric": "loss"},
			map[string]any{"kind": "stat", "metric": "tok", "unit": "tok/s"},
			map[string]any{"kind": "text", "key": "phase", "label": "phase"},
			map[string]any{"kind": "pie"},
		}}})
	res.Events["job_train"][0].Data["phase"] = "warmup"
	res.Points[SeriesKey{JobID: "job_train", Metric: "gpu"}] = []Point{{X: 1, Y: 40, Seq: 1}, {X: 2, Y: 71, Seq: 2}}
	res.Points[SeriesKey{JobID: "job_train", Metric: "loss"}] = []Point{{X: 1, Y: 2, Seq: 1}, {X: 2, Y: 1.2, Seq: 2}, {X: 3, Y: 0.4, Seq: 3}}
	res.Points[SeriesKey{JobID: "job_train", Metric: "tok"}] = []Point{{X: 1, Y: 1500, Seq: 1}, {X: 2, Y: 1930, Seq: 2}}
	return apply(t, m, refreshMsg{Gen: m.gen, Result: res})
}

func TestGlanceIsDefaultAndFitsEverySize(t *testing.T) {
	for _, sz := range [][2]int{{80, 24}, {120, 36}, {160, 44}, {50, 14}} {
		m := glanceModel(t, sz[0], sz[1])
		view := stripANSI(m.View().Content)
		lines := linesOf(t, view)
		if len(lines) != sz[1] {
			t.Errorf("%dx%d: %d lines", sz[0], sz[1], len(lines))
		}
		for i, l := range lines {
			if w := lipgloss.Width(l); w > sz[0] {
				t.Errorf("%dx%d: line %d is %d wide: %q", sz[0], sz[1], i, w, l)
			}
		}
		if sz[0] < 80 {
			continue
		}
		for _, want := range []string{"Running 3", "train-model", "ingest-logs", "eval-suite", "STALLED", "ETA", "%", "deploy exit 2"} {
			if !strings.Contains(view, want) {
				t.Errorf("%dx%d: missing %q:\n%s", sz[0], sz[1], want, view)
			}
		}
		if sz[0] == 160 {
			// Room for the declared panels: gauge, line, big stat, text, and the note.
			for _, want := range []string{"GPU", "71%", "loss", "phase", "warmup", "1 panel spec ignored", "files/s"} {
				if !strings.Contains(view, want) {
					t.Errorf("160x44: missing %q:\n%s", want, view)
				}
			}
		}
	}
}

func TestGlanceCardsPutStalledFirstAndSelectFirstCard(t *testing.T) {
	m := glanceModel(t, 120, 36)
	order := m.activeJobs(time.Now())
	var names []string
	for _, i := range order {
		names = append(names, m.jobs[i].JobID)
	}
	if got := strings.Join(names, ","); got != "job_eval,job_train,job_ingest,job_q" {
		t.Errorf("card order = %s", got)
	}
	if m.selectedJobID() != "job_eval" {
		t.Errorf("first load should select the first card, got %s", m.selectedJobID())
	}
	m = apply(t, m, tea.KeyPressMsg(tea.Key{Code: tea.KeyDown}))
	m = apply(t, m, tea.KeyPressMsg(tea.Key{Code: tea.KeyDown}))
	if pos := indexOf(m.glanceOrder(), m.selected); pos < 1 {
		t.Errorf("down should walk the glance order, at %d", pos)
	}
}

func indexOf(xs []int, v int) int {
	for i, x := range xs {
		if x == v {
			return i
		}
	}
	return -1
}

func TestLivenessStalledLateFreshQuiet(t *testing.T) {
	now := time.Now()
	mk := func(n int, every, last time.Duration) Model {
		m := New(DefaultConfig(t.TempDir()), nil)
		evs, _, _ := progressEvents("j", n, every, last, 10)
		m.events["j"] = evs
		return m
	}
	j := JobSummary{JobID: "j", Status: "running"}
	cases := []struct {
		m    Model
		want liveness
	}{
		{mk(20, 20*time.Second, 9*time.Minute), liveStalled},
		{mk(20, 20*time.Second, 80*time.Second), liveLate},
		{mk(20, 20*time.Second, 5*time.Second), liveFresh},
		{mk(20, time.Second, 8*time.Second), liveFresh}, // floors stop fast jobs flickering
		{mk(2, time.Second, 5*time.Minute), liveQuiet},  // too few events to know the interval
		{New(DefaultConfig(t.TempDir()), nil), liveUnknown},
	}
	for i, c := range cases {
		if got, _, _ := c.m.jobLiveness(j, now); got != c.want {
			t.Errorf("case %d: liveness %d, want %d", i, got, c.want)
		}
	}
}

func TestETAAndRate(t *testing.T) {
	m := New(DefaultConfig(t.TempDir()), nil)
	j := JobSummary{JobID: "j", Status: "running", StartedAt: ago(100 * time.Second)}
	// 10% -> 50% over the last 80 seconds: 0.5 %/s, so 50% left is ~100 s.
	at := func(d time.Duration) string { return ago(d) }
	m.points[SeriesKey{JobID: "j", Metric: "progress.percent"}] = []Point{{Y: 10, At: at(80 * time.Second)}, {Y: 50, At: at(0)}}
	m.points[SeriesKey{JobID: "j", Metric: "progress.current"}] = []Point{{Y: 100, At: at(80 * time.Second)}, {Y: 500, At: at(0)}}
	m.events["j"] = []Event{{Type: "progress", Data: map[string]any{"unit": "rows"}}}
	eta, ok := m.jobETA(j, time.Now())
	if !ok || eta < 95*time.Second || eta > 105*time.Second {
		t.Errorf("eta = %v %v", eta, ok)
	}
	if r := m.jobRate(j); r != "5 rows/s" {
		t.Errorf("rate = %q", r)
	}
	// One point: fall back to the average since start (50% in 100 s).
	m.points[SeriesKey{JobID: "j", Metric: "progress.percent"}] = []Point{{Y: 50, At: at(0)}}
	if eta, ok := m.jobETA(j, time.Now()); !ok || eta < 95*time.Second || eta > 105*time.Second {
		t.Errorf("fallback eta = %v %v", eta, ok)
	}
	m.points[SeriesKey{JobID: "j", Metric: "progress.current"}] = []Point{{Y: 0, At: at(time.Hour)}, {Y: 3, At: at(0)}}
	if r := m.jobRate(j); r != "3 rows/h" {
		t.Errorf("slow rate = %q", r)
	}
	if _, ok := m.jobETA(JobSummary{JobID: "none"}, time.Now()); ok {
		t.Error("no progress, no ETA")
	}
}

func TestGlanceNoColorKeepsStatusReadable(t *testing.T) {
	m := glanceModel(t, 120, 36)
	var buf bytes.Buffer
	w := &colorprofile.Writer{Forward: &buf, Profile: colorprofile.Ascii}
	if _, err := w.WriteString(m.View().Content); err != nil {
		t.Fatal(err)
	}
	if sgrColor.MatchString(buf.String()) {
		t.Error("color escapes survive the Ascii profile")
	}
	plain := stripANSI(buf.String())
	for _, want := range []string{"✖ STALLED", "◌ thumbnails", "queued", "✘ deploy exit 2", "✔ lint", "● Running 3", "ago"} {
		if !strings.Contains(plain, want) {
			t.Errorf("no-color glance missing %q:\n%s", want, plain)
		}
	}
}

func TestGlanceIdleSaysNothingRunning(t *testing.T) {
	for _, sz := range [][2]int{{80, 24}, {160, 44}} {
		m := New(DefaultConfig(t.TempDir()), nil)
		m = apply(t, m, tea.WindowSizeMsg{Width: sz[0], Height: sz[1]})
		res := RefreshResult{JobsOK: true, Mode: ModeSQLite}
		res.Jobs = []JobSummary{{JobID: "a", Name: "lint", Status: "completed", StartedAt: ago(time.Hour), EndedAt: ago(50 * time.Minute)}}
		m = apply(t, m, refreshMsg{Gen: m.gen, Result: res})
		view := stripANSI(m.View().Content)
		if !strings.Contains(view, "Nothing running") || !strings.Contains(view, "last finished ✔ lint") || !strings.Contains(view, "Timeline") {
			t.Errorf("%v idle view:\n%s", sz, view)
		}
		if n := len(linesOf(t, view)); n != sz[1] {
			t.Errorf("%v: %d lines", sz, n)
		}
	}
}

func TestViewToggleAndClickSelectsCard(t *testing.T) {
	m := glanceModel(t, 120, 36)
	m = apply(t, m, tea.KeyPressMsg(tea.Key{Code: 'v'}))
	if !m.detail || !strings.Contains(stripANSI(m.View().Content), "Jobs (6)") {
		t.Fatal("v should switch to the detail dashboard")
	}
	m = apply(t, m, tea.KeyPressMsg(tea.Key{Code: 'v'}))
	g := m.computeGlance()
	target := g.cards[len(g.cards)-1]
	m = apply(t, m, tea.MouseClickMsg(tea.Mouse{X: 5, Y: target.y}))
	if m.selected != target.job {
		t.Errorf("click selected %d, want %d", m.selected, target.job)
	}
	if len(g.chips) == 0 {
		t.Fatal("no recent chips")
	}
	c := g.chips[0]
	m = apply(t, m, tea.MouseClickMsg(tea.Mouse{X: c.x, Y: c.y}))
	if m.jobs[m.selected].JobID != "job_bad" {
		t.Errorf("failures come first in the strip; selected %s", m.selectedJobID())
	}
}

func TestLoadTargetsIncludeActiveJobs(t *testing.T) {
	m := glanceModel(t, 120, 36)
	got := strings.Join(m.loadTargets(), ",")
	for _, id := range []string{"job_train", "job_ingest", "job_eval", "job_q"} {
		if !strings.Contains(got, id) {
			t.Errorf("targets %s missing %s", got, id)
		}
	}
}

func TestParsePanelsIgnoresInvalid(t *testing.T) {
	specs, bad := parsePanels(map[string]any{"panels": []any{
		map[string]any{"kind": "gauge", "metric": "gpu", "min": 50.0, "max": 10.0},
		map[string]any{"kind": "CHART", "metric": "loss", "label": strings.Repeat("x", 99)},
		map[string]any{"kind": "pie", "metric": "x"},
		map[string]any{"kind": "line"},
		map[string]any{"kind": "text"},
		"junk",
		map[string]any{"kind": "number", "metric": "n"},
		map[string]any{"kind": "status", "text": "ok"},
		map[string]any{"kind": "line", "metric": "over-the-limit"},
	}})
	if bad != 5 {
		t.Errorf("invalid = %d", bad)
	}
	if len(specs) != 4 || specs[0].Min != nil || specs[1].Kind != "line" || len(specs[1].Label) != 32 ||
		specs[2].Kind != "stat" || specs[3].Kind != "text" {
		t.Errorf("specs = %+v", specs)
	}
	if s, bad := parsePanels(map[string]any{"panels": "nope"}); s != nil || bad != 1 {
		t.Error("non-list panels must be ignored")
	}
}

func TestRenderEachPanelKind(t *testing.T) {
	m := glanceModel(t, 160, 44)
	j := m.jobs[m.jobByID["job_train"]]
	ten := 10.0
	for _, c := range []struct {
		spec PanelSpec
		h    int
		want []string
	}{
		{PanelSpec{Kind: "gauge", Metric: "gpu", Label: "GPU"}, 2, []string{"GPU", "71%", "█"}},
		{PanelSpec{Kind: "gauge", Metric: "gpu", Unit: "W", Max: &ten}, 2, []string{"71W"}},
		{PanelSpec{Kind: "line", Metric: "loss"}, 4, []string{"loss", "0.4", "▼", "█"}},
		{PanelSpec{Kind: "stat", Metric: "gpu", Unit: "%"}, 4, []string{"━┓", "%"}},
		{PanelSpec{Kind: "stat", Metric: "tok", Unit: "tok/s"}, 4, []string{"1930 tok/s"}}, // too wide for big digits
		{PanelSpec{Kind: "stat", Metric: "tok"}, 2, []string{"1930"}},
		{PanelSpec{Kind: "text", Key: "phase"}, 2, []string{"phase", "warmup"}},
		{PanelSpec{Kind: "text", Text: "hello"}, 2, []string{"hello"}},
		{PanelSpec{Kind: "line", Metric: "missing"}, 2, []string{"missing", "–"}},
	} {
		rows := m.renderPanel(j, c.spec, 20, c.h)
		out := stripANSI(strings.Join(rows, "\n"))
		if len(rows) != c.h {
			t.Errorf("%+v: %d rows", c.spec, len(rows))
		}
		for _, r := range rows {
			if lipgloss.Width(r) > 20 {
				t.Errorf("%+v: row too wide %q", c.spec, stripANSI(r))
			}
		}
		for _, w := range c.want {
			if !strings.Contains(out, w) {
				t.Errorf("%+v: missing %q in\n%s", c.spec, w, out)
			}
		}
	}
	grid := m.renderPanelGrid(j, m.panels["job_train"].specs, 40, 2)
	if len(grid) != 4 { // four panels, two per 40-cell row, two rows each
		t.Errorf("grid rows = %d", len(grid))
	}
	if m.panels["job_train"].invalid != 1 {
		t.Errorf("invalid count = %d", m.panels["job_train"].invalid)
	}
}
