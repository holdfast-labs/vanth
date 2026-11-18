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
	return strings.Split(strings.TrimSuffix(s, "\n"), "\n")
}

func key(r rune) tea.KeyPressMsg { return tea.KeyPressMsg(tea.Key{Code: r}) }

func plain(m Model) string { return stripANSI(m.View().Content) }

var sgrColor = regexp.MustCompile(`\x1b\[[0-9;:]*(?:3[0-9]|4[0-9]|9[0-9]|10[0-9]|38|48)[0-9;:]*m`)

// sgrFg is the SGR parameter text of a true-colour foreground, as found in a
// frame after Bloom re-encodes the styles.
func sgrFg(hex string) string {
	c := lipgloss.Color(hex)
	r, g, b, _ := c.RGBA()
	return fmt.Sprintf("38;2;%d;%d;%d", r>>8, g>>8, b>>8)
}

func asciiFrame(t *testing.T, m Model) string {
	t.Helper()
	var buf bytes.Buffer
	w := &colorprofile.Writer{Forward: &buf, Profile: colorprofile.ASCII}
	if _, err := w.WriteString(m.View().Content); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

func TestFrameIsExactlyTheTerminalAtEverySize(t *testing.T) {
	for _, sz := range [][2]int{{160, 44}, {120, 36}, {100, 30}, {80, 24}, {50, 14}} {
		m := leetModel(t, sz[0], sz[1])
		for _, frame := range []string{plain(m), plain(apply(t, m, key('e'))), plain(apply(t, m, key(']')))} {
			lines := linesOf(t, frame)
			if len(lines) != sz[1] {
				t.Fatalf("%dx%d: %d lines", sz[0], sz[1], len(lines))
			}
			for i, l := range lines {
				if w := lipgloss.Width(l); w != sz[0] {
					t.Errorf("%dx%d: line %d is %d wide: %q", sz[0], sz[1], i, w, l)
				}
			}
		}
		if sz[0] < 80 {
			continue // the minimum size keeps the frame whole, not every run visible
		}
		view := plain(m)
		for _, want := range []string{"vanth", "4 ", "LIVE", "q quit", "progress %", "eval-suite", "finetune-llama-8b"} {
			if !strings.Contains(view, want) {
				t.Errorf("%dx%d: missing %q:\n%s", sz[0], sz[1], want, view)
			}
		}
	}
}

func TestSidebarAtWideAndCompactListWhenNarrow(t *testing.T) {
	wide := leetModel(t, 160, 44)
	g := wide.computeLeet()
	if g.sideW == 0 || len(g.compact) != 0 {
		t.Fatalf("160 wide: sidebar %d, compact %d", g.sideW, len(g.compact))
	}
	v := plain(wide)
	for _, want := range []string{"Running 4", "Queued 1", "Finished 6 · 2 failed", "Overview", "deploy-staging", "exit 1",
		"timeout", "render-thumbnails", "host        gpu-02"} {
		if !strings.Contains(v, want) && !strings.Contains(plain(apply(t, wide, key('j'))), want) {
			t.Errorf("sidebar missing %q:\n%s", want, v)
		}
	}
	for _, sz := range [][2]int{{80, 24}, {100, 30}} {
		m := leetModel(t, sz[0], sz[1])
		if sz[0] < sidebarMinTermW {
			if g := m.computeLeet(); g.sideW != 0 {
				t.Fatalf("%d wide still has a sidebar", sz[0])
			}
		} else {
			m = apply(t, m, key(']'))
		}
		g := m.computeLeet()
		if g.sideW != 0 || len(g.compact) < 4 {
			t.Fatalf("%dx%d: compact list %+v", sz[0], sz[1], g.compact)
		}
		v := plain(m)
		for _, want := range []string{"eval-suite", "finetune-llama-8b", "ingest-s3-logs", "nightly-backup", "✖ stalled", "62%"} {
			if !strings.Contains(v, want) {
				t.Errorf("%dx%d compact list missing %q:\n%s", sz[0], sz[1], want, v)
			}
		}
		if g.main.y != 1+len(g.compact)/max(1, sz[0]/40) {
			t.Errorf("charts start at row %d", g.main.y)
		}
	}
	if m := apply(t, apply(t, wide, key('[')), key('[')); m.computeLeet().sideW == 0 {
		t.Error("[ twice should bring the sidebar back")
	}
}

func TestJobKeepsOneColourEverywhere(t *testing.T) {
	m := leetModel(t, 160, 44)
	raw := m.View().Content
	seen := map[int]string{}
	for _, id := range []string{"job_train", "job_eval", "job_ingest", "job_backup"} {
		c := m.colorIndex(id)
		if other, dup := seen[c]; dup {
			t.Errorf("%s and %s share colour %d", id, other, c)
		}
		seen[c] = id
	}
	// Sidebar swatch, chart line and chart readout of a job share its colour.
	ingest := m.jobColor("job_ingest")
	if row := m.jobRow(m.jobByID["job_ingest"], 30); !strings.Contains(row, fg(ingest).Render("■")) {
		t.Errorf("ingest swatch not in its colour: %q", row)
	}
	progress := m.chartSpecs()[0]
	if ro := m.readout(progress); !strings.Contains(ro, fg(ingest).Bold(true).Render("31")) {
		t.Errorf("ingest readout not in its colour: %q", ro)
	}
	lo, hi, _ := m.chartWindow(progress)
	for _, l := range m.chartLines(progress, lo, hi) {
		if l.marker == m.jobMarker("job_ingest") && l.style.GetForeground() != ingest {
			t.Error("ingest progress line not in its colour")
		}
	}
	if !strings.Contains(raw, sgrFg(seriesHex[m.colorIndex("job_ingest")])) {
		t.Error("ingest colour missing from the frame")
	}
	// Colours stick when the set changes: finishing one job keeps the others'.
	before := map[string]int{}
	for id, c := range m.colors {
		before[id] = c
	}
	res := leetFixture()
	for i := range res.Jobs {
		if res.Jobs[i].JobID == "job_ingest" {
			res.Jobs[i].Status, res.Jobs[i].EndedAt = "completed", ago(0)
		}
	}
	m = apply(t, m, refreshMsg{Gen: m.gen, Result: RefreshResult{JobsOK: true, Mode: ModeSQLite, Jobs: res.Jobs}})
	for _, id := range []string{"job_train", "job_eval", "job_backup"} {
		if m.colors[id] != before[id] {
			t.Errorf("%s changed colour %d -> %d", id, before[id], m.colors[id])
		}
	}
	if _, still := m.colors["job_ingest"]; still {
		t.Error("finished, unselected job keeps a series colour")
	}
}

func TestChartPagingAndFocus(t *testing.T) {
	m := leetModel(t, 100, 30)
	g := m.computeLeet()
	if g.pages < 2 || g.page != 0 || len(g.cells) != 4 {
		t.Fatalf("pages %d page %d cells %d", g.pages, g.page, len(g.cells))
	}
	m = apply(t, m, tea.KeyPressMsg(tea.Key{Code: tea.KeyRight}))
	if g := m.computeLeet(); g.page != 1 || m.chartFocus != 4 {
		t.Errorf("→: page %d focus %d", g.page, m.chartFocus)
	}
	if v := plain(m); !strings.Contains(v, fmt.Sprintf("page 2/%d", g.pages)) {
		t.Errorf("page indicator missing:\n%s", v)
	}
	m = apply(t, m, tea.KeyPressMsg(tea.Key{Code: tea.KeyLeft}))
	if m.chartFocus != 0 {
		t.Errorf("←: focus %d", m.chartFocus)
	}
	for k := 0; k < 4; k++ {
		m = apply(t, m, tea.KeyPressMsg(tea.Key{Code: tea.KeyTab}))
	}
	if g := m.computeLeet(); m.chartFocus != 4 || g.page != 1 {
		t.Errorf("tab x4: focus %d page %d", m.chartFocus, g.page)
	}
	if raw := m.View().Content; !strings.Contains(raw, sgrFg(focusHex)) {
		t.Error("focused chart is not drawn in focus teal")
	}
	if got := m.chartTitle(m.computeLeet().specs[4], true); got != hexStyle(focusHex).Bold(true).Render(m.computeLeet().specs[4].title) {
		t.Errorf("focused title %q", got)
	}
	m = apply(t, m, tea.KeyPressMsg(tea.Key{Code: tea.KeyTab, Mod: tea.ModShift}))
	if m.chartFocus != 3 {
		t.Errorf("shift+tab: focus %d", m.chartFocus)
	}
}

func TestStalledJobIsFlaggedEverywhere(t *testing.T) {
	m := leetModel(t, 160, 44)
	v := plain(m)
	for _, want := range []string{"✖ 1 stalled", "✖ stalled 9m", "27.7 ✖ stalled"} {
		if !strings.Contains(v, want) {
			t.Errorf("missing %q:\n%s", want, v)
		}
	}
	if ro := m.readout(m.chartSpecs()[0]); !strings.Contains(ro, hexStyle(crashHex).Bold(true).Render("40")) {
		t.Errorf("stalled job's progress readout is not red: %q", ro)
	}
}

func TestIdleShowsSelectedJobChartsOrFleet(t *testing.T) {
	m := New(DefaultConfig(filepath.Join(t.TempDir(), "home")), nil)
	m = apply(t, m, tea.WindowSizeMsg{Width: 120, Height: 36})
	jobs := []JobSummary{
		{JobID: "old", Name: "old-run", Status: "completed", StartedAt: ago(2 * time.Hour), EndedAt: ago(110 * time.Minute)},
		{JobID: "new", Name: "new-run", Status: "completed", StartedAt: ago(20 * time.Minute), EndedAt: ago(5 * time.Minute)},
	}
	res := RefreshResult{JobsOK: true, Mode: ModeSQLite, Jobs: jobs, Points: map[SeriesKey][]Point{
		{JobID: "new", Metric: "loss"}: fixtureSeries(20*time.Minute, 5*time.Minute, 30*time.Second, func(u float64, _ int) float64 { return 1 - u }),
	}}
	m = apply(t, m, refreshMsg{Gen: m.gen, Result: res})
	if m.selectedJobID() != "new" {
		t.Fatalf("selected %s, want the most recent run", m.selectedJobID())
	}
	v := plain(m)
	for _, want := range []string{"Running 0", "nothing running", "Finished 2", "loss", "Overview", "new-run"} {
		if !strings.Contains(v, want) {
			t.Errorf("idle view missing %q:\n%s", want, v)
		}
	}
	if strings.Contains(v, "now") {
		t.Error("a finished run's chart should end at its last point, not now")
	}
	m = apply(t, m, key('j')) // the old run has no metrics: fleet visuals instead
	if v := plain(m); !strings.Contains(v, "Timeline") || !strings.Contains(v, "Recent runs") {
		t.Errorf("fleet view missing:\n%s", v)
	}
}

func TestNoColourUsesSeriesMarkersAndGlyphs(t *testing.T) {
	m := leetModel(t, 160, 44)
	m.profile = colorprofile.ASCII
	out := asciiFrame(t, m)
	if sgrColor.MatchString(out) {
		t.Errorf("colour escapes survive: %q", sgrColor.FindString(out))
	}
	v := stripANSI(out)
	markers := map[rune]bool{}
	for _, id := range []string{"job_train", "job_eval", "job_ingest", "job_backup"} {
		mk := m.jobMarker(id)
		markers[mk] = true
		name := m.jobs[m.jobByID[id]].Name
		if id != m.selectedJobID() && !strings.Contains(v, string(mk)+" "+name) {
			t.Errorf("sidebar row of %s lacks its marker %q", id, mk)
		}
	}
	if len(markers) != 4 {
		t.Errorf("markers not distinct: %v", markers)
	}
	// Each progress line is drawn with its job's marker, and readouts carry it.
	progress := m.computeLeet().cells[0]
	lines := linesOf(t, v)
	for _, id := range []string{"job_train", "job_ingest"} {
		mk, found := string(m.jobMarker(id)), false
		for _, l := range lines[progress.y+1 : progress.y+progress.h-3] {
			found = found || strings.Contains(ansi.Cut(l, progress.x+8, progress.x+progress.w-1), mk)
		}
		if !found {
			t.Errorf("progress chart has no %q points for %s", mk, id)
		}
	}
	if !strings.Contains(lines[progress.y], string(m.jobMarker("job_ingest"))+"31") {
		t.Errorf("readout lacks markers: %q", lines[progress.y])
	}
	for _, want := range []string{"✘ deploy-staging", "✔ lint", "◌ render-thumbnails", "✖ stalled", "exit 1"} {
		if !strings.Contains(v, want) {
			t.Errorf("no-colour frame missing %q", want)
		}
	}
}

func TestPanelKindsMapToGridCells(t *testing.T) {
	m := leetModel(t, 160, 44)
	byKey := map[string]chartSpec{}
	var order []string
	for _, s := range m.chartSpecs() {
		byKey[s.key] = s
		order = append(order, s.key)
	}
	if order[0] != "progress.percent" || order[1] != "loss" {
		t.Errorf("order %v: progress, then the declared panels", order)
	}
	if s := byKey["gpu"]; s.kind != "gauge" || *s.lo != 0 || *s.hi != 100 || s.title != "gpu util %" || len(s.jobs) != 2 {
		t.Errorf("gauge %+v", s)
	}
	if s := byKey["tokens_s"]; s.kind != "stat" {
		t.Errorf("stat %+v", s)
	}
	if _, ok := byKey["phase"]; ok {
		t.Error("text panel became a chart")
	}
	// Metrics of jobs that declared nothing still chart.
	for _, k := range []string{"cases_min", "mb_s"} {
		if _, ok := byKey[k]; !ok {
			t.Errorf("undeclared metric %s missing", k)
		}
	}
	m.selected = m.jobByID["job_train"]
	v := plain(m)
	for _, want := range []string{"phase       epoch 3/5", bigNumber(1767, 20)[0]} {
		if !strings.Contains(v, want) {
			t.Errorf("missing %q:\n%s", want, v)
		}
	}
	// Gauge bounds are fixed: the axis reads 100 at the top even though data peaks lower.
	for _, c := range m.computeLeet().cells {
		if m.computeLeet().specs[c.spec].key == "gpu" {
			if top := stripANSI(strings.Split(m.renderChartCell(c, byKey["gpu"], false), "\n")[1]); !strings.Contains(top, "100┤") {
				t.Errorf("gauge top label: %q", top)
			}
		}
	}
	// An invalid spec is reported in the Overview.
	m.panels["job_train"] = panelDecl{specs: m.panels["job_train"].specs, invalid: 1}
	if v := plain(m); !strings.Contains(v, "1 spec ignored") {
		t.Errorf("invalid panel note missing:\n%s", v)
	}
}

func TestMouseSelectsRunsAndChartsAndWheelZooms(t *testing.T) {
	m := leetModel(t, 160, 44)
	g := m.computeLeet()
	row := -1
	for r, l := range g.side {
		if l.job == m.jobByID["job_ingest"] {
			row = r
		}
	}
	if row < 0 {
		t.Fatal("ingest row not in the sidebar")
	}
	m = apply(t, m, tea.MouseClickMsg(tea.Mouse{X: 5, Y: 1 + row}))
	if m.selectedJobID() != "job_ingest" {
		t.Errorf("click selected %s", m.selectedJobID())
	}
	m = apply(t, m, tea.MouseClickMsg(tea.Mouse{X: 2, Y: 1})) // the "Running" title row
	if m.selectedJobID() != "job_ingest" {
		t.Error("title row click changed the selection")
	}
	cell := m.computeLeet().cells[4]
	m = apply(t, m, tea.MouseClickMsg(tea.Mouse{X: cell.x + 3, Y: cell.y + 3}))
	if m.chartFocus != cell.spec {
		t.Errorf("click focused chart %d, want %d", m.chartFocus, cell.spec)
	}
	ck := m.computeLeet().specs[cell.spec].key
	lo, hi, _ := m.chartWindow(m.computeLeet().specs[cell.spec])
	m = apply(t, m, tea.MouseWheelMsg(tea.Mouse{X: cell.x + 3, Y: cell.y + 3, Button: tea.MouseWheelUp}))
	if got := m.zoom[ck]; got != hi.Sub(lo)/2 {
		t.Errorf("wheel up: zoom %v, want %v", got, hi.Sub(lo)/2)
	}
	if v := plain(m); !strings.Contains(v, "-15m") || !strings.Contains(v, "-7m30s") {
		t.Errorf("zoomed axis labels missing:\n%s", v)
	}
	m = apply(t, m, key('t'))
	if len(m.zoom) != 0 {
		t.Error("t should reset zoom")
	}

	narrow := leetModel(t, 80, 24)
	e := narrow.computeLeet().compact[2]
	narrow = apply(t, narrow, tea.MouseClickMsg(tea.Mouse{X: e.x + 4, Y: e.y}))
	if narrow.selected != e.job {
		t.Errorf("compact click selected %d, want %d", narrow.selected, e.job)
	}
}

func TestPanesReplaceChartsAndFollowSelection(t *testing.T) {
	m := leetModel(t, 120, 36)
	m.events[m.selectedJobID()] = append(m.events[m.selectedJobID()], Event{Type: "log", Level: "error", Message: "epoch one", Seq: 1})
	m = apply(t, m, key('e'))
	if v := plain(m); !strings.Contains(v, "Events ·") || !strings.Contains(v, "epoch one") || strings.Contains(v, "progress %") {
		t.Errorf("event pane:\n%s", v)
	}
	m = apply(t, m, tea.KeyPressMsg(tea.Key{Code: tea.KeyEscape}))
	if m.lowerExpanded() {
		t.Error("esc should close the pane")
	}
	m = apply(t, m, key('s'))
	if v := plain(m); !strings.Contains(v, "Slowest runs") {
		t.Errorf("slowest pane:\n%s", v)
	}
	m, cmd := updateModel(t, apply(t, m, key('l')), key('j'))
	if !m.showLogs || cmd == nil {
		t.Error("moving the selection with logs open should fetch the new run's logs")
	}
	m.logTail, m.logTailJob = []string{"line one"}, m.selectedJobID()
	if v := plain(m); !strings.Contains(v, "line one") {
		t.Errorf("log pane:\n%s", v)
	}
}

func TestHeaderCountsAndStaleBadge(t *testing.T) {
	m := leetModel(t, 160, 44)
	head := linesOf(t, plain(m))[0]
	for _, want := range []string{"vanth", "4 running", "1 queued", "3 done", "2 failed", "1 cancelled", "1 stalled", "sqlite", "11 jobs"} {
		if !strings.Contains(head, want) {
			t.Errorf("header missing %q: %q", want, head)
		}
	}
	now := time.Now()
	m.staleSince = &now
	if bar := linesOf(t, plain(m))[43]; !strings.Contains(bar, "STALE") {
		t.Errorf("help bar: %q", bar)
	}
	m.staleSince, m.mode = nil, ModeFallback
	if bar := linesOf(t, plain(m))[43]; !strings.Contains(bar, "FALLBACK") {
		t.Errorf("help bar: %q", bar)
	}
}

func TestOnlyRefreshesScheduleWork(t *testing.T) {
	// No animation timers: UI keys and the mouse return no command; the next
	// frame comes from the refresh tick alone.
	m := leetModel(t, 120, 36)
	for _, msg := range []tea.Msg{key('j'), key('k'), tea.KeyPressMsg(tea.Key{Code: tea.KeyTab}), key(']'), key('e'), key('e'),
		tea.KeyPressMsg(tea.Key{Code: tea.KeyRight}), key('+'), key('t'), tea.MouseClickMsg(tea.Mouse{X: 60, Y: 5}),
		tea.MouseWheelMsg(tea.Mouse{X: 60, Y: 5, Button: tea.MouseWheelDown})} {
		var cmd tea.Cmd
		m, cmd = updateModel(t, m, msg)
		if cmd != nil {
			t.Errorf("%T %v scheduled a command", msg, msg)
		}
	}
	if _, cmd := updateModel(t, m, refreshMsg{Gen: m.gen, Result: leetFixture()}); cmd == nil {
		t.Error("a refresh must schedule the next tick")
	}
}

// ---------- kept from the previous views ----------

func TestViewBelowMinimumAndNoData(t *testing.T) {
	m := leetModel(t, 30, 8)
	if v := plain(m); !strings.Contains(v, "too small") {
		t.Errorf("below-minimum view: %q", v)
	}
	empty := apply(t, New(DefaultConfig(filepath.Join(t.TempDir(), "home")), nil), tea.WindowSizeMsg{Width: 120, Height: 40})
	if v := plain(empty); !strings.Contains(v, "no jobs") || len(linesOf(t, v)) != 40 {
		t.Errorf("empty view:\n%s", v)
	}
	empty.loading = true
	if v := plain(empty); !strings.Contains(v, "loading jobs") {
		t.Errorf("loading view:\n%s", v)
	}
	if sk := loadingSkeleton(30, 5); lipgloss.Height(sk) != 5 || lipgloss.Width(sk) != 30 {
		t.Errorf("skeleton size %dx%d", lipgloss.Width(sk), lipgloss.Height(sk))
	}
}

func TestHelpIsOverlayOnLargeAndFullScreenOnSmall(t *testing.T) {
	m := leetModel(t, 120, 36)
	m.help = true
	big := plain(m)
	if !strings.Contains(big, "vanth monitor · keys") || !strings.Contains(big, "Running 4") || len(linesOf(t, big)) != 36 {
		t.Errorf("help overlay should sit over the frame:\n%s", big)
	}
	m = apply(t, m, tea.KeyPressMsg(tea.Key{Code: tea.KeyEscape}))
	if m.help {
		t.Error("esc should close help")
	}
	small := leetModel(t, 60, 16)
	small.help = true
	if v := plain(small); !strings.Contains(v, "select run") || len(linesOf(t, v)) != 16 {
		t.Errorf("small help:\n%s", v)
	}
}

func TestStatusGlyphsAreDistinct(t *testing.T) {
	seen := map[string]string{}
	for _, st := range []string{"running", "queued", "completed", "failed", "cancelled", "orphaned", "timeout"} {
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

func TestPanelBoxExactSizeAndFallback(t *testing.T) {
	m := leetModel(t, 100, 30)
	for w := 2; w <= 40; w += 3 {
		for h := 2; h <= 8; h += 2 {
			out := m.panelBoxOpts(rect{w: w, h: h}, "Title", "a\nb", panelOpts{hint: "hint", focused: true})
			if lipgloss.Width(out) != w || lipgloss.Height(out) != h {
				t.Fatalf("%dx%d: got %dx%d", w, h, lipgloss.Width(out), lipgloss.Height(out))
			}
		}
	}
	if got := m.panelBox(rect{w: 1, h: 1}, "x", ""); got != "" {
		t.Errorf("1x1 panel should be empty, got %q", got)
	}
	if out := m.legacyPanelBox(rect{w: 20, h: 5}, "t", "x"); lipgloss.Width(out) != 20 || lipgloss.Height(out) != 5 {
		t.Error("hand-drawn fallback box has the wrong size")
	}
}

func TestPlotKeepsSpikesAndBreaksLongGaps(t *testing.T) {
	m := leetModel(t, 100, 30)
	lo := time.Now().Add(-10 * time.Minute)
	at := func(d time.Duration) time.Time { return lo.Add(d) }
	pts := []Point{{Y: 0}, {Y: 10}, {Y: 0}, {Y: 0}}
	l := plotLine{pts: pts, at: []time.Time{at(time.Minute), at(time.Minute + time.Second), at(time.Minute + 2*time.Second), at(9 * time.Minute)},
		style: lipgloss.NewStyle(), marker: '*'}
	rows := m.plot(20, 4, []plotLine{l}, lo, lo.Add(10*time.Minute), 0, 10)
	if !strings.ContainsAny(stripANSI(rows[0]), brailleRange[1:]) {
		t.Errorf("spike lost: %q", stripANSI(strings.Join(rows, "\n")))
	}
	// 8 minutes between the 3rd and 4th points is over a quarter of the window: no line between them.
	mid := stripANSI(rows[3])
	if r := []rune(mid); r[10] != '⠤' {
		t.Errorf("gap bridged: %q", mid)
	}
}

func TestEventPreviewRoundsFloatsAndFitsTypes(t *testing.T) {
	m := leetModel(t, 160, 44)
	evs := []Event{{Type: "log_capture_failed", Level: "error", Seq: 1, Data: map[string]any{"acc": 0.8639999999999999}}}
	line := stripANSI(m.eventLines(evs, 150)[0])
	if !strings.Contains(line, "log_capture_failed") || !strings.Contains(line, `"acc":0.864}`) {
		t.Errorf("event line: %q", line)
	}
}

func TestConstantSeriesAreNotCharts(t *testing.T) {
	m := leetModel(t, 160, 44)
	flat := []Point{{X: 1, Y: 100, Seq: 1}, {X: 2, Y: 100, Seq: 2}, {X: 3, Y: 100, Seq: 3}}
	m = apply(t, m, refreshMsg{Gen: m.gen, Result: RefreshResult{JobsOK: true, Mode: ModeSQLite,
		Jobs: m.jobs, Points: map[SeriesKey][]Point{{JobID: "job_eval", Metric: "flat"}: flat}}})
	for _, s := range m.chartSpecs() {
		if s.key == "flat" {
			t.Fatal("flat series must not get a chart")
		}
	}
	if m.constants["flat"] != 100 {
		t.Errorf("constants = %v", m.constants)
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

func TestBigNumberAndSparkline(t *testing.T) {
	if rows := bigNumber(128, 11); len(rows) != 3 || lipgloss.Width(rows[0]) != 11 {
		t.Errorf("rows %q", rows)
	}
	if bigNumber(128, 10) != nil {
		t.Error("expected fallback")
	}
	if got := sparkline([]float64{1, 2, 3, 4, 5, 6, 7, 8}, 8); got != "▁▂▃▄▅▆▇█" {
		t.Errorf("got %q", got)
	}
}

// ---------- liveness, ETA and panels ----------

// progressEvents makes n progress events every `every`, the last one `last` ago.
func progressEvents(jobID string, n int, every, last time.Duration, total float64) []Event {
	var evs []Event
	for k := 1; k <= n; k++ {
		c := total * 0.5 * float64(k) / float64(n)
		evs = append(evs, Event{EventID: fmt.Sprint(jobID, k), JobID: jobID, Seq: int64(k), Type: "progress",
			Data: map[string]any{"current": c, "total": total}, CreatedAt: ago(last + time.Duration(n-k)*every)})
	}
	return evs
}

func TestLivenessStalledLateFreshQuiet(t *testing.T) {
	now := time.Now()
	mk := func(n int, every, last time.Duration) Model {
		m := New(DefaultConfig(t.TempDir()), nil)
		m.events["j"] = progressEvents("j", n, every, last, 10)
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
	m.points[SeriesKey{JobID: "j", Metric: "progress.percent"}] = []Point{{Y: 10, At: ago(80 * time.Second)}, {Y: 50, At: ago(0)}}
	m.points[SeriesKey{JobID: "j", Metric: "progress.current"}] = []Point{{Y: 100, At: ago(80 * time.Second)}, {Y: 500, At: ago(0)}}
	m.events["j"] = []Event{{Type: "progress", Data: map[string]any{"unit": "rows"}}}
	eta, ok := m.jobETA(j, time.Now())
	if !ok || eta < 95*time.Second || eta > 105*time.Second {
		t.Errorf("eta = %v %v", eta, ok)
	}
	if r := m.jobRate(j); r != "5 rows/s" {
		t.Errorf("rate = %q", r)
	}
	// One point: fall back to the average since start (50% in 100 s).
	m.points[SeriesKey{JobID: "j", Metric: "progress.percent"}] = []Point{{Y: 50, At: ago(0)}}
	if eta, ok := m.jobETA(j, time.Now()); !ok || eta < 95*time.Second || eta > 105*time.Second {
		t.Errorf("fallback eta = %v %v", eta, ok)
	}
	m.points[SeriesKey{JobID: "j", Metric: "progress.current"}] = []Point{{Y: 0, At: ago(time.Hour)}, {Y: 3, At: ago(0)}}
	if r := m.jobRate(j); r != "3 rows/h" {
		t.Errorf("slow rate = %q", r)
	}
	if _, ok := m.jobETA(JobSummary{JobID: "none"}, time.Now()); ok {
		t.Error("no progress, no ETA")
	}
}

func TestLoadTargetsIncludeActiveJobs(t *testing.T) {
	m := leetModel(t, 120, 36)
	got := strings.Join(m.loadTargets(), ",")
	for _, id := range []string{"job_train", "job_ingest", "job_eval", "job_backup", "job_q"} {
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
