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

	"bloomfx.dev/bloom/fig"
)

func stripANSI(s string) string { return ansi.Strip(s) }

func apply(t testing.TB, m Model, msg tea.Msg) Model {
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

// cardOf returns the card of a job in the layout.
func cardOf(t *testing.T, m Model, id string) cardCell {
	t.Helper()
	for _, c := range m.computeMission().cards {
		if c.job >= 0 && m.jobs[c.job].JobID == id {
			return c
		}
	}
	t.Fatalf("no card for %s", id)
	return cardCell{}
}

func idleModel(t *testing.T, w, h int) Model {
	t.Helper()
	m := New(DefaultConfig(filepath.Join(t.TempDir(), "home")), nil)
	m = apply(t, m, tea.WindowSizeMsg{Width: w, Height: h})
	m = apply(t, m, tea.BackgroundColorMsg{Color: blackColor})
	code := int64(2)
	jobs := []JobSummary{
		{JobID: "old", Name: "old-run", Status: "completed", StartedAt: ago(2 * time.Hour), EndedAt: ago(110 * time.Minute)},
		{JobID: "bad", Name: "broken", Status: "failed", ExitCode: &code, StartedAt: ago(27 * time.Hour), EndedAt: ago(26 * time.Hour)},
		{JobID: "new", Name: "new-run", Status: "completed", StartedAt: ago(20 * time.Minute), EndedAt: ago(5 * time.Minute)},
	}
	res := RefreshResult{JobsOK: true, Mode: ModeSQLite, Jobs: jobs, Points: map[SeriesKey][]Point{
		{JobID: "new", Metric: "loss"}: fixtureSeries(20*time.Minute, 5*time.Minute, 30*time.Second, func(u float64, _ int) float64 { return 1 - u }),
	}}
	return apply(t, m, refreshMsg{Gen: m.gen, Result: res})
}

func TestFrameIsExactlyTheTerminalAtEverySize(t *testing.T) {
	for _, sz := range [][2]int{{160, 44}, {120, 36}, {100, 30}, {80, 24}, {50, 14}} {
		m := fixtureModel(t, sz[0], sz[1])
		help := m
		help.help = true
		for name, frame := range map[string]string{"main": plain(m), "events": plain(apply(t, m, key('e'))),
			"page": plain(apply(t, m, key(']'))), "help": plain(help), "idle": plain(idleModel(t, sz[0], sz[1]))} {
			lines := linesOf(t, frame)
			if len(lines) != sz[1] {
				t.Fatalf("%dx%d %s: %d lines", sz[0], sz[1], name, len(lines))
			}
			for i, l := range lines {
				if w := lipgloss.Width(l); w != sz[0] {
					t.Errorf("%dx%d %s: line %d is %d wide: %q", sz[0], sz[1], name, i, w, l)
				}
			}
		}
		view := plain(m)
		for _, want := range []string{"vanth", "q quit", "eval-suite", "finetune-ll", "STALLED"} {
			if !strings.Contains(view, want) {
				t.Errorf("%dx%d: missing %q:\n%s", sz[0], sz[1], want, view)
			}
		}
	}
}

func TestCardGridReflows(t *testing.T) {
	cases := []struct {
		w, h, cols, cardH int
	}{{160, 44, 4, 10}, {120, 36, 2, 8}, {100, 30, 2, 6}, {80, 24, 1, 1}, {50, 14, 1, 1}} // 120: 2x2, never 3 + a lone card
	for _, c := range cases {
		g := fixtureModel(t, c.w, c.h).computeMission()
		if len(g.cards) != 4 {
			t.Fatalf("%dx%d: %d cards", c.w, c.h, len(g.cards))
		}
		perRow := 0
		for _, card := range g.cards {
			if card.h != c.cardH || card.job < 0 {
				t.Errorf("%dx%d: card %+v, want height %d", c.w, c.h, card, c.cardH)
			}
			if card.y == g.cards[0].y {
				perRow++
			}
		}
		if perRow != c.cols {
			t.Errorf("%dx%d: %d cards per row, want %d", c.w, c.h, perRow, c.cols)
		}
		if last := g.cards[len(g.cards)-1]; last.y+last.h > g.main.y {
			t.Errorf("%dx%d: cards overlap the charts", c.w, c.h)
		}
	}
	// A fifth running job joins the first row at 160 wide instead of wrapping.
	five := missionFixture()
	five.Jobs = append(five.Jobs, JobSummary{JobID: "job_x5", Name: "fifth", Status: "running", StartedAt: ago(time.Minute)})
	m5 := New(DefaultConfig(filepath.Join(t.TempDir(), "home")), nil)
	m5 = apply(t, m5, tea.WindowSizeMsg{Width: 160, Height: 44})
	m5 = apply(t, m5, refreshMsg{Gen: m5.gen, Result: five})
	if g := m5.computeMission(); len(g.cards) != 5 || g.cards[4].y != g.cards[0].y {
		t.Errorf("5 jobs at 160x44: want one row of 5, got %+v", g.cards)
	}
	// More running jobs than fit: a "+N more" card, and the selection stays visible.
	res := missionFixture()
	for k := 0; k < 8; k++ {
		res.Jobs = append(res.Jobs, JobSummary{JobID: fmt.Sprintf("job_x%d", k), Name: fmt.Sprintf("extra-%d", k),
			Status: "running", StartedAt: ago(time.Duration(k+1) * time.Minute)})
	}
	m := New(DefaultConfig(filepath.Join(t.TempDir(), "home")), nil)
	m = apply(t, m, tea.WindowSizeMsg{Width: 100, Height: 30})
	m = apply(t, m, refreshMsg{Gen: m.gen, Result: res})
	g := m.computeMission()
	last := g.cards[len(g.cards)-1]
	if last.job != -1 || last.more != 12-len(g.cards)+1 {
		t.Fatalf("overflow card %+v of %d", last, len(g.cards))
	}
	if v := plain(m); !strings.Contains(v, fmt.Sprintf("+%d more", last.more)) {
		t.Errorf("overflow card text missing:\n%s", v)
	}
	for k := 0; k < 11; k++ {
		m = apply(t, m, key('j'))
	}
	cardOf(t, m, m.selectedJobID())
}

func TestStalledCardIsImpossibleToMiss(t *testing.T) {
	m := fixtureModel(t, 160, 44)
	eval := m.renderCard(cardOf(t, m, "job_eval"))
	v := stripANSI(eval)
	for _, want := range []string{"┏", "┃", "┛", "STALLED 9m", "no output", "for 9m", "eta –"} {
		if !strings.Contains(v, want) {
			t.Errorf("stalled card missing %q:\n%s", want, v)
		}
	}
	if !strings.Contains(eval, sgrFg(failHex)) {
		t.Error("stalled card is not red")
	}
	if train := stripANSI(m.renderCard(cardOf(t, m, "job_train"))); strings.Contains(train, "┏") || !strings.Contains(train, "╭") {
		t.Errorf("a healthy card should be rounded:\n%s", train)
	}
	if head := linesOf(t, plain(m))[0]; !strings.Contains(head, "⚠ 1 stalled") {
		t.Errorf("header lacks the stall pill: %q", head)
	}
	// The compact layout flags it on its line, and the thick border survives without colour.
	if v := plain(fixtureModel(t, 80, 24)); !strings.Contains(v, "✕ STALLED 9m") {
		t.Errorf("compact view lacks the stall:\n%s", v)
	}
	m.profile = colorprofile.ASCII
	if out := stripANSI(asciiFrame(t, m)); !strings.Contains(out, "┏") || !strings.Contains(out, "STALLED 9m") {
		t.Errorf("no-colour frame loses the stall:\n%s", out)
	}
}

func TestHeaderPillsHideZeroCounts(t *testing.T) {
	m := fixtureModel(t, 160, 44)
	raw := strings.Split(m.View().Content, "\n")[0]
	head := stripANSI(raw)
	for _, want := range []string{"◆ vanth", "mission control", "● 4 running", "○ 1 queued", "3 done", "✕ 2 failed", "⚠ 1 stalled", "11 jobs · sqlite"} {
		if !strings.Contains(head, want) {
			t.Errorf("header missing %q: %q", want, head)
		}
	}
	if !strings.Contains(raw, "48;2;") {
		t.Error("pills have no background colour")
	}
	idle := linesOf(t, plain(idleModel(t, 160, 44)))[0]
	for _, gone := range []string{"running", "queued", "stalled"} {
		if strings.Contains(idle, gone) {
			t.Errorf("zero %s count still shown: %q", gone, idle)
		}
	}
	if !strings.Contains(idle, "2 done") || !strings.Contains(idle, "✕ 1 failed") {
		t.Errorf("idle header: %q", idle)
	}
}

func TestRailPutsFailuresFirst(t *testing.T) {
	m := fixtureModel(t, 160, 44)
	rail := linesOf(t, plain(m))[m.computeMission().railY]
	at := func(s string) int {
		k := strings.Index(rail, s)
		if k < 0 {
			t.Fatalf("rail lacks %q: %q", s, rail)
		}
		return k
	}
	if !(at("✕ deploy-staging exit 1 · 4m") < at("✕ flaky-integration timeout · 12m") &&
		at("flaky-integration") < at("○ render-thumbnails queued 3m") && at("render-thumbnails") < at("✓ lint")) {
		t.Errorf("rail order: %q", rail)
	}
	narrow := fixtureModel(t, 80, 24)
	if r := strings.TrimRight(linesOf(t, plain(narrow))[narrow.computeMission().railY], " "); !strings.HasSuffix(r, "…") ||
		!strings.HasPrefix(strings.TrimSpace(r), "✕ deploy-staging") {
		t.Errorf("narrow rail: %q", r)
	}
}

func TestIdleShowsNothingRunningAndRecentCharts(t *testing.T) {
	m := idleModel(t, 120, 36)
	if m.selectedJobID() != "new" {
		t.Fatalf("selected %s, want the most recent run", m.selectedJobID())
	}
	v := plain(m)
	for _, want := range []string{"○ nothing running", "last run finished 5m ago", "new-run", "✓ done", "loss", "✕ broken"} {
		if !strings.Contains(v, want) {
			t.Errorf("idle view missing %q:\n%s", want, v)
		}
	}
	if len(m.computeMission().cards) != 0 {
		t.Error("idle view should have no cards")
	}
	if strings.Contains(v, "now") {
		t.Error("a finished run's chart should end at its last point, not now")
	}
	m = apply(t, m, key('j')) // the next run has no metrics: fleet visuals instead
	if v := plain(m); !strings.Contains(v, "Timeline") || !strings.Contains(v, "Recent runs") {
		t.Errorf("fleet view missing:\n%s", v)
	}
}

func TestNoColourUsesMarkersGlyphsAndReverse(t *testing.T) {
	m := fixtureModel(t, 160, 44)
	m.profile = colorprofile.ASCII
	out := asciiFrame(t, m)
	if sgrColor.MatchString(out) {
		t.Errorf("colour escapes survive: %q", sgrColor.FindString(out))
	}
	v := stripANSI(out)
	markers := map[rune]bool{}
	for _, id := range []string{"job_train", "job_eval", "job_ingest", "job_backup"} {
		markers[m.jobMarker(id)] = true
	}
	if len(markers) != 4 {
		t.Errorf("markers not distinct: %v", markers)
	}
	for _, id := range []string{"job_train", "job_eval"} {
		if want := string(m.jobMarker(id)) + " " + m.jobs[m.jobByID[id]].Name; !strings.Contains(v, want) {
			t.Errorf("legend lacks %q", want)
		}
	}
	first := m.computeMission().cells[0]
	lines := linesOf(t, v)
	found := false
	for _, l := range lines[first.y+1 : first.y+first.h-1] {
		found = found || strings.Contains(ansi.Cut(l, first.x, first.x+first.w-6), string(m.jobMarker("job_train")))
	}
	if !found {
		t.Errorf("first chart has no %q points", m.jobMarker("job_train"))
	}
	for _, want := range []string{"▸ eval-suite", "✕ deploy-staging", "✓ lint", "○ render-thumbnails", "STALLED 9m", "exit 1", "━"} {
		if !strings.Contains(v, want) {
			t.Errorf("no-colour frame missing %q", want)
		}
	}
	if !strings.Contains(out, "\x1b[7") && !strings.Contains(out, ";7m") && !strings.Contains(out, ";7;") {
		t.Error("pills should fall back to reverse video")
	}
	compact := fixtureModel(t, 80, 24)
	compact.profile = colorprofile.ASCII
	cv := stripANSI(asciiFrame(t, compact))
	if want := string(compact.jobMarker("job_ingest")) + " ingest-s3-logs"; !strings.Contains(cv, want) {
		t.Errorf("compact line lacks %q:\n%s", want, cv)
	}
}

func TestPanelKindsMapToGridCells(t *testing.T) {
	m := fixtureModel(t, 160, 44)
	byKey := map[string]chartSpec{}
	var order []string
	for _, s := range m.chartSpecs() {
		byKey[s.key] = s
		order = append(order, s.key)
	}
	if strings.Join(order[:7], ",") != "loss,acc,gpu,gpu_mem,tokens_s,lr,progress.percent" {
		t.Errorf("order %v: the declared panels, then progress", order)
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
	for _, k := range []string{"cases_min", "mb_s"} { // metrics of jobs that declared nothing still chart
		if _, ok := byKey[k]; !ok {
			t.Errorf("undeclared metric %s missing", k)
		}
	}
	if c := fixtureModel(t, 80, 24).chartSpecs(); c[0].key != "progress.percent" {
		t.Errorf("compact layout leads with %s, want progress", c[0].key)
	}
	// The text panel lands on the card's step line.
	if card := stripANSI(m.renderCard(cardOf(t, m, "job_train"))); !strings.Contains(card, "train 3100/5000 step · epoch 3/5") {
		t.Errorf("train card:\n%s", card)
	}
	// Gauge bounds are fixed: the axis reads 100 at the top even though data peaks lower.
	gpu := stripANSI(m.renderChart(rect{w: 50, h: 13}, byKey["gpu"], false))
	if top := strings.Split(gpu, "\n")[1]; !strings.HasSuffix(strings.TrimRight(top, " "), " 100") {
		t.Errorf("gauge top label: %q", top)
	}
	// A stat panel is big gradient digits.
	if stat := m.renderChart(rect{w: 50, h: 13}, byKey["tokens_s"], false); !strings.Contains(stat, sgrFg(seriesGrad[m.colorIndex("job_train")][0])) ||
		len(linesOf(t, stripANSI(stat))) != 13 {
		t.Errorf("stat tile:\n%s", stripANSI(stat))
	}
	m.panels["job_train"] = panelDecl{specs: m.panels["job_train"].specs, invalid: 1}
	wide := cardOf(t, m, "job_train")
	wide.w = 80
	if card := stripANSI(m.renderCard(wide)); !strings.Contains(card, "1 panel spec ignored") {
		t.Errorf("invalid panel note missing:\n%s", card)
	}
}

func TestMouseSelectsCardsRailAndChartsAndWheelZooms(t *testing.T) {
	m := fixtureModel(t, 160, 44)
	c := cardOf(t, m, "job_ingest")
	m = apply(t, m, tea.MouseClickMsg(tea.Mouse{X: c.x + 3, Y: c.y + 2}))
	if m.selectedJobID() != "job_ingest" {
		t.Errorf("card click selected %s", m.selectedJobID())
	}
	m = apply(t, m, tea.MouseClickMsg(tea.Mouse{X: 2, Y: 0})) // the header
	if m.selectedJobID() != "job_ingest" {
		t.Error("header click changed the selection")
	}
	g := m.computeMission()
	it := g.rail[0]
	m = apply(t, m, tea.MouseClickMsg(tea.Mouse{X: 1 + it.x + 2, Y: g.railY}))
	if m.selectedJobID() != "job_deploy" {
		t.Errorf("rail click selected %s", m.selectedJobID())
	}
	cardOf(t, m, "job_deploy") // a selected finished job gets a card
	cell := m.computeMission().cells[1]
	m = apply(t, m, tea.MouseClickMsg(tea.Mouse{X: cell.x + 3, Y: cell.y + 3}))
	if m.chartFocus != cell.spec {
		t.Errorf("click focused chart %d, want %d", m.chartFocus, cell.spec)
	}
	spec := m.computeMission().specs[cell.spec]
	lo, hi, _ := m.chartWindow(spec)
	m = apply(t, m, tea.MouseWheelMsg(tea.Mouse{X: cell.x + 3, Y: cell.y + 3, Button: tea.MouseWheelUp}))
	if got := m.zoom[spec.key]; got != hi.Sub(lo)/2 {
		t.Errorf("wheel up: zoom %v, want %v", got, hi.Sub(lo)/2)
	}
	if v := plain(m); !strings.Contains(v, "15m ago") {
		t.Errorf("zoomed axis label missing:\n%s", v)
	}
	m = apply(t, m, key('t'))
	if len(m.zoom) != 0 {
		t.Error("t should reset zoom")
	}

	narrow := fixtureModel(t, 80, 24)
	e := narrow.computeMission().cards[2]
	narrow = apply(t, narrow, tea.MouseClickMsg(tea.Mouse{X: e.x + 4, Y: e.y}))
	if narrow.selected != e.job {
		t.Errorf("compact click selected %d, want %d", narrow.selected, e.job)
	}
}

func TestJobKeepsOneColourEverywhere(t *testing.T) {
	m := fixtureModel(t, 160, 44)
	raw := m.View().Content
	seen := map[int]string{}
	for _, id := range []string{"job_train", "job_eval", "job_ingest", "job_backup"} {
		c := m.colorIndex(id)
		if other, dup := seen[c]; dup {
			t.Errorf("%s and %s share colour %d", id, other, c)
		}
		seen[c] = id
	}
	train := m.jobColor("job_train")
	if lg := m.renderLegend(m.computeMission()); !strings.Contains(lg, fg(train).Render("■")) {
		t.Errorf("legend swatch not in the job's colour: %q", lg)
	}
	var progress chartSpec
	for _, s := range m.chartSpecs() {
		if s.key == "progress.percent" {
			progress = s
		}
	}
	ingest := m.jobColor("job_ingest")
	if ro := m.readout(progress); !strings.Contains(ro, fg(ingest).Bold(true).Render("31")) {
		t.Errorf("ingest readout not in its colour: %q", ro)
	}
	lo, hi, _ := m.chartWindow(progress)
	for _, l := range m.chartLines(progress, lo, hi) {
		if l.marker == m.jobMarker("job_ingest") && l.style.GetForeground() != ingest {
			t.Error("ingest progress line not in its colour")
		}
	}
	for _, id := range []string{"job_train", "job_ingest"} { // the card's numbers and bar use the job's gradient
		if !strings.Contains(m.renderCard(cardOf(t, m, id)), sgrFg(seriesGrad[m.colorIndex(id)][0])) {
			t.Errorf("%s card lacks its colour", id)
		}
	}
	if !strings.Contains(raw, sgrFg(seriesGrad[m.colorIndex("job_train")][0])) {
		t.Error("train colour missing from the frame")
	}
	// Colours stick when the set changes: finishing one job keeps the others'.
	before := map[string]int{}
	for id, c := range m.colors {
		before[id] = c
	}
	res := missionFixture()
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

func TestArrowsSelectJobsAndBracketsPageCharts(t *testing.T) {
	m := fixtureModel(t, 100, 30)
	first := m.selectedJobID()
	m = apply(t, m, tea.KeyPressMsg(tea.Key{Code: tea.KeyRight}))
	if m.selectedJobID() == first {
		t.Error("→ should select the next job")
	}
	if m = apply(t, m, tea.KeyPressMsg(tea.Key{Code: tea.KeyLeft})); m.selectedJobID() != first {
		t.Error("← should select the previous job")
	}
	g := m.computeMission()
	if g.pages < 2 || g.page != 0 || len(g.cells) != 3 {
		t.Fatalf("pages %d page %d cells %d", g.pages, g.page, len(g.cells))
	}
	m = apply(t, m, key(']'))
	if g := m.computeMission(); g.page != 1 || m.chartFocus != 3 {
		t.Errorf("]: page %d focus %d", g.page, m.chartFocus)
	}
	if v := plain(m); !strings.Contains(v, fmt.Sprintf("charts 2/%d", g.pages)) {
		t.Errorf("page indicator missing:\n%s", v)
	}
	if m = apply(t, m, key('[')); m.chartFocus != 0 {
		t.Errorf("[: focus %d", m.chartFocus)
	}
	for k := 0; k < 3; k++ {
		m = apply(t, m, tea.KeyPressMsg(tea.Key{Code: tea.KeyTab}))
	}
	if g := m.computeMission(); m.chartFocus != 3 || g.page != 1 {
		t.Errorf("tab x3: focus %d page %d", m.chartFocus, g.page)
	}
	if raw := m.View().Content; !strings.Contains(raw, sgrFg(brandHex)) {
		t.Error("focused chart title is not in the brand colour")
	}
	if m = apply(t, m, tea.KeyPressMsg(tea.Key{Code: tea.KeyTab, Mod: tea.ModShift})); m.chartFocus != 2 {
		t.Errorf("shift+tab: focus %d", m.chartFocus)
	}
}

func TestPanesReplaceChartsAndFollowSelection(t *testing.T) {
	m := fixtureModel(t, 120, 36)
	m.events[m.selectedJobID()] = append(m.events[m.selectedJobID()], Event{Type: "log", Level: "error", Message: "epoch one", Seq: 1})
	m = apply(t, m, key('e'))
	if v := plain(m); !strings.Contains(v, "Events ·") || !strings.Contains(v, "epoch one") || strings.Contains(v, "METRICS") {
		t.Errorf("event pane:\n%s", v)
	}
	if v := plain(m); !strings.Contains(v, "nightly-backup") {
		t.Errorf("the cards stay above an open pane:\n%s", v)
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

func TestKeyLineShowsFreshness(t *testing.T) {
	m := fixtureModel(t, 160, 44)
	if l := linesOf(t, plain(m))[43]; !strings.Contains(l, "updated 0s ago") || !strings.Contains(l, "↑↓←→ job") {
		t.Errorf("key line: %q", l)
	}
	now := time.Now()
	m.staleSince = &now
	if l := linesOf(t, plain(m))[43]; !strings.Contains(l, "STALE") {
		t.Errorf("key line: %q", l)
	}
	m.staleSince, m.mode = nil, ModeFallback
	if l := linesOf(t, plain(m))[43]; !strings.Contains(l, "FALLBACK") {
		t.Errorf("key line: %q", l)
	}
}

func TestMotionStepsWithRefreshesOnly(t *testing.T) {
	m := fixtureModel(t, 160, 44)
	eval, backup, train := cardOf(t, m, "job_eval"), cardOf(t, m, "job_backup"), cardOf(t, m, "job_train")
	a, b := m, m
	a.beat, b.beat = 2, 3
	if a.renderCard(eval) == b.renderCard(eval) {
		t.Error("the stalled card border should pulse between refreshes")
	}
	if a.renderCard(backup) == b.renderCard(backup) {
		t.Error("the no-progress bar should move between refreshes")
	}
	if a.renderCard(train) != b.renderCard(train) {
		t.Error("a healthy card with progress should not animate")
	}
	beat := m.beat
	if m = apply(t, m, refreshMsg{Gen: m.gen, Result: missionFixture()}); m.beat != beat+1 {
		t.Error("a refresh should step the motion")
	}
}

func TestOnlyRefreshesScheduleWork(t *testing.T) {
	// No animation timers: UI keys and the mouse return no command; the next
	// frame comes from the refresh tick alone.
	m := fixtureModel(t, 120, 36)
	for _, msg := range []tea.Msg{key('j'), key('k'), tea.KeyPressMsg(tea.Key{Code: tea.KeyTab}), key(']'), key('['), key('e'), key('e'),
		tea.KeyPressMsg(tea.Key{Code: tea.KeyRight}), tea.KeyPressMsg(tea.Key{Code: tea.KeyLeft}), key('+'), key('t'),
		tea.MouseClickMsg(tea.Mouse{X: 10, Y: 4}), tea.MouseClickMsg(tea.Mouse{X: 60, Y: 25}),
		tea.MouseWheelMsg(tea.Mouse{X: 60, Y: 25, Button: tea.MouseWheelDown})} {
		var cmd tea.Cmd
		m, cmd = updateModel(t, m, msg)
		if cmd != nil {
			t.Errorf("%T %v scheduled a command", msg, msg)
		}
	}
	if _, cmd := updateModel(t, m, refreshMsg{Gen: m.gen, Result: missionFixture()}); cmd == nil {
		t.Error("a refresh must schedule the next tick")
	}
}

// ---------- panes, help and building blocks ----------

func TestViewBelowMinimumAndNoData(t *testing.T) {
	m := fixtureModel(t, 30, 8)
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
	m := fixtureModel(t, 120, 36)
	m.help = true
	big := plain(m)
	if !strings.Contains(big, "vanth monitor · keys") || !strings.Contains(big, "mission control") || len(linesOf(t, big)) != 36 {
		t.Errorf("help overlay should sit over the frame:\n%s", big)
	}
	m = apply(t, m, tea.KeyPressMsg(tea.Key{Code: tea.KeyEscape}))
	if m.help {
		t.Error("esc should close help")
	}
	small := fixtureModel(t, 60, 16)
	small.help = true
	if v := plain(small); !strings.Contains(v, "select job") || len(linesOf(t, v)) != 16 {
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
	m := fixtureModel(t, 100, 30)
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
	m := fixtureModel(t, 100, 30)
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
	m := fixtureModel(t, 160, 44)
	evs := []Event{{Type: "log_capture_failed", Level: "error", Seq: 1, Data: map[string]any{"acc": 0.8639999999999999}}}
	line := stripANSI(m.eventLines(evs, 150)[0])
	if !strings.Contains(line, "log_capture_failed") || !strings.Contains(line, `"acc":0.864}`) {
		t.Errorf("event line: %q", line)
	}
}

func TestConstantSeriesAreNotCharts(t *testing.T) {
	m := fixtureModel(t, 160, 44)
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

func TestBigTextAndSparkline(t *testing.T) {
	rows := bigText("62%", 20, fig.Gradient(lipgloss.Color("#38BDF8"), lipgloss.Color("#818CF8")))
	if len(rows) != 3 || lipgloss.Width(rows[0]) > 20 {
		t.Errorf("rows %q", rows)
	}
	if !strings.Contains(strings.Join(rows, ""), sgrFg("#38BDF8")) {
		t.Error("big text lacks the gradient's first colour")
	}
	if bigText("100%", 6, fig.Solid(nil)) != nil {
		t.Error("expected nil when no font fits")
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
	m := fixtureModel(t, 120, 36)
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

func TestStalledJobKeepsItsPlace(t *testing.T) {
	m := fixtureModel(t, 160, 44)
	// Make the stalled job the newest, so stall-first ordering would move it ahead.
	for i, j := range m.jobs {
		if m.isStalled(j) {
			m.jobs[i].StartedAt = ago(time.Second)
		}
	}
	var prev time.Time
	stalled := false
	for _, i := range m.activeJobs(time.Now()) {
		j := m.jobs[i]
		if !isRunning(j.Status) {
			continue
		}
		stalled = stalled || m.isStalled(j)
		start, _ := jobStart(j)
		if start.Before(prev) {
			t.Fatalf("running jobs out of start order at %s (a stalled job jumped ahead)", j.DisplayName())
		}
		prev = start
	}
	if !stalled {
		t.Fatal("fixture has no stalled job to check")
	}
}
