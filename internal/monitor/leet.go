package monitor

import (
	"encoding/json"
	"fmt"
	"image/color"
	"math"
	"strings"
	"time"

	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/colorprofile"
)

// The monitor view, after W&B LEET: a one-line header with status counts, a
// run sidebar (Running / Queued / Finished / Overview of the selected run), a
// grid of braille line charts (one per metric, every job in its own colour)
// and a key-hint bar. Narrow terminals fold the sidebar into a compact run
// list above the charts so the runs never disappear.

func fg(c color.Color) lipgloss.Style { return lipgloss.NewStyle().Foreground(c) }

func hexStyle(hex string) lipgloss.Style { return fg(lipgloss.Color(hex)) }

func isRunning(status string) bool { return status == "running" || status == "launching" }

// noColor reports a terminal without colour (NO_COLOR, dumb terminal, pipe):
// series then use per-job marker characters instead of colours.
func (m Model) noColor() bool {
	return m.profile == colorprofile.NoTTY || m.profile == colorprofile.ASCII
}

// spread puts l on the left and r on the right of exactly w cells; r is
// dropped if both do not fit.
func spread(l, r string, w int) string {
	gap := w - lipgloss.Width(l) - lipgloss.Width(r)
	if gap < 1 {
		return fillLine(l, w)
	}
	return l + strings.Repeat(" ", gap) + r
}

// frameBlock pads or clips a block to exactly w x h cells.
func frameBlock(block string, w, h int) string {
	ls := strings.Split(block, "\n")
	out := make([]string, h)
	for i := range out {
		l := ""
		if i < len(ls) {
			l = ls[i]
		}
		out[i] = fillLine(l, w)
	}
	return strings.Join(out, "\n")
}

// ---------- which jobs and charts ----------

// chartJobs are the jobs drawn on the charts, in run-list order: every running
// job plus the selected and pinned ones.
func (m Model) chartJobs() []int {
	sel := m.selectedJobID()
	var out []int
	for _, i := range m.runOrder() {
		j := m.jobs[i]
		if isRunning(j.Status) || j.JobID == sel || m.pinned[j.JobID] {
			out = append(out, i)
		}
	}
	return out
}

func (m Model) chartJobIDs() []string {
	var ids []string
	for _, i := range m.chartJobs() {
		ids = append(ids, m.jobs[i].JobID)
	}
	return ids
}

// chartSpec is one cell of the chart grid.
type chartSpec struct {
	key, title string
	kind       string // line | gauge | stat
	lo, hi     *float64
	jobs       []int // charted jobs with data for key
}

// chartSpecs lists the charts: progress of every charted job first, then each
// job's declared panels (line and gauge become charts, stat a big-number
// tile; text goes to the Overview), then the metrics of jobs that declared
// none. A metric shared by several jobs is one chart with a line per job.
func (m Model) chartSpecs() []chartSpec {
	cj := m.chartJobs()
	has := func(i int, key string) bool { return len(m.points[SeriesKey{JobID: m.jobs[i].JobID, Metric: key}]) > 0 }
	var specs []chartSpec
	seen := map[string]bool{}
	add := func(s chartSpec) {
		if s.key != "" && !seen[s.key] {
			seen[s.key] = true
			specs = append(specs, s)
		}
	}
	zero, hundred := 0.0, 100.0
	for _, i := range cj {
		if has(i, "progress.percent") {
			add(chartSpec{key: "progress.percent", title: "progress %", kind: "line", lo: &zero, hi: &hundred})
			break
		}
	}
	// Declared panels first, so a job's own gauge bounds and labels win over a
	// plain chart of the same metric from a job that declared nothing.
	for _, i := range cj {
		for _, p := range m.panels[m.jobs[i].JobID].specs {
			if p.Kind == "text" {
				continue
			}
			title := p.Label
			if title == "" {
				title = p.Metric
			}
			if p.Unit != "" && !strings.Contains(title, p.Unit) {
				title += " " + p.Unit
			}
			s := chartSpec{key: p.Metric, title: title, kind: p.Kind, lo: p.Min, hi: p.Max}
			if p.Kind == "gauge" {
				if s.lo == nil {
					s.lo = &zero
				}
				if s.hi == nil {
					s.hi = &hundred
				}
			}
			add(s)
		}
	}
	for _, i := range cj {
		if len(m.panels[m.jobs[i].JobID].specs) > 0 {
			continue // a job that declared panels chose its view
		}
		for _, k := range m.metrics {
			if !strings.HasPrefix(k, "progress.") && has(i, k) {
				add(chartSpec{key: k, title: k, kind: "line"})
			}
		}
	}
	for s := range specs {
		for _, i := range cj {
			if has(i, specs[s].key) {
				specs[s].jobs = append(specs[s].jobs, i)
			}
		}
	}
	return specs
}

// ---------- layout ----------

type sideLine struct {
	text string
	job  int // -1: not a run row
}

type compactEntry struct {
	rect
	job int
}

type chartCell struct {
	rect
	spec int // index into leetLayout.specs
}

type leetLayout struct {
	sideW   int // 0: sidebar folded into the compact list
	side    []sideLine
	compact []compactEntry
	main    rect
	specs   []chartSpec
	cells   []chartCell
	page    int
	pages   int
}

const (
	sidebarMinTermW = 90 // narrower terminals get the compact run list
	chartMinW       = 34
	chartIdealH     = 13
	chartMinH       = 7
)

// computeLeet lays the frame out; View and the mouse hit-tests share it.
func (m Model) computeLeet() leetLayout {
	w, gh := m.width, m.height-2
	var g leetLayout
	if w >= sidebarMinTermW && !m.hideSidebar {
		g.sideW = clamp(w*236/1000, 30, 44)
		g.side = m.sidebarLines(g.sideW, gh)
		g.main = rect{x: g.sideW + 1, y: 1, w: w - g.sideW - 1, h: gh}
	} else {
		var rows int
		g.compact, rows = m.compactEntries(w, gh)
		g.main = rect{y: 1 + rows, w: w, h: gh - rows}
	}
	if !m.lowerExpanded() {
		g.specs = m.chartSpecs()
		g.cells, g.page, g.pages = m.gridCells(g.main, len(g.specs))
	}
	return g
}

// gridShape picks columns and rows for an area: cells at least chartMinW
// wide and about chartIdealH tall.
func gridShape(area rect, n int) (cols, rows int) {
	cols = clamp(area.w/chartMinW, 1, 4)
	rows = max(1, int(math.Round(float64(area.h)/chartIdealH)))
	if area.h/rows < chartMinH {
		rows = max(1, area.h/chartMinH)
	}
	if n < cols*rows {
		cols = max(1, min(cols, n))
		rows = max(1, (n+cols-1)/cols)
	}
	return cols, rows
}

// gridCells places the page of charts holding the focused chart.
func (m Model) gridCells(area rect, n int) (cells []chartCell, page, pages int) {
	if n == 0 || area.w < 10 || area.h < 4 {
		return nil, 0, 0
	}
	cols, rows := gridShape(area, n)
	per := cols * rows
	pages = (n + per - 1) / per
	page = clamp(m.chartFocus, 0, n-1) / per
	cw, ch := area.w/cols, area.h/rows
	for k := page * per; k < min(n, (page+1)*per); k++ {
		c, r := (k-page*per)%cols, (k-page*per)/cols
		cell := rect{x: area.x + c*cw, y: area.y + r*ch, w: cw, h: ch}
		if c == cols-1 {
			cell.w = area.x + area.w - cell.x
		}
		if r == rows-1 {
			cell.h = area.y + area.h - cell.y
		}
		cells = append(cells, chartCell{rect: cell, spec: k})
	}
	return cells, page, pages
}

// ---------- run rows ----------

func (m Model) isStalled(j JobSummary) bool {
	if !isRunning(j.Status) {
		return false
	}
	st, _, _ := m.jobLiveness(j, time.Now())
	return st == liveStalled
}

// selStyle is the selected run row: LEET's yellow bar (reverse video without
// colour).
func (m Model) selStyle() lipgloss.Style {
	if m.noColor() {
		return lipgloss.NewStyle().Reverse(true).Bold(true)
	}
	return lipgloss.NewStyle().Foreground(lipgloss.Color(brandFgHex)).Background(lipgloss.Color(brandHex)).Bold(true)
}

// jobRow is one run in the sidebar or compact list, exactly w cells: a colour
// swatch (running) or status glyph, the name, and on the right the progress,
// a stall or late flag, the exit reason or the duration.
func (m Model) jobRow(i, w int) string {
	j := m.jobs[i]
	ch := m.chrome()
	now := time.Now()
	mark, markSt := statusIcon(j.Status), fg(ch.axis)
	name, nameSt := j.DisplayName(), fg(ch.text)
	right, rightSt := m.jobAgeText(j), fg(ch.axis)
	switch {
	case isRunning(j.Status):
		mark, markSt = "■", fg(m.jobColor(j.JobID))
		if m.noColor() {
			mark = string(m.jobMarker(j.JobID))
		}
		nameSt = fg(ch.value)
		if pct := m.jobProgressPercent(j.JobID); pct >= 0 {
			right, rightSt = fmt.Sprintf("%.0f%%", pct), fg(ch.text)
		}
		switch st, since, _ := m.jobLiveness(j, now); st {
		case liveStalled:
			nameSt = hexStyle(crashHex).Bold(true)
			right, rightSt = "✖ stalled "+compactDuration(since), hexStyle(crashHex).Bold(true)
		case liveLate:
			right, rightSt = "◔ late "+compactDuration(since), hexStyle(lateHex)
		}
	case isFailure(j.Status):
		markSt, nameSt = hexStyle(crashHex).Bold(true), hexStyle(crashHex).Bold(true)
		right, rightSt = j.Status, hexStyle(crashHex)
		if j.ExitCode != nil && *j.ExitCode != 0 {
			right = fmt.Sprintf("exit %d", *j.ExitCode)
		}
	case j.Status == "completed":
		markSt = hexStyle(runHex)
	case j.Status == "cancelled":
		nameSt = fg(ch.axis)
	default: // queued and other waiting states: how long it has waited
		if d := ageOf(j.CreatedAt); d > 0 {
			right = compactDuration(d)
		}
	}
	if m.pinned[j.JobID] {
		right = "pin " + right
	}
	nameW := max(w-3-lipgloss.Width(right)-2, 4)
	name = truncate(name, nameW)
	if m.selectedJobID() == j.JobID {
		return m.selStyle().Render(spread(" "+mark+" "+name, right+" ", w))
	}
	return spread(" "+markSt.Render(mark)+" "+nameSt.Render(name), rightSt.Render(right)+" ", w)
}

// window returns up to c items of list as a slice that keeps the selected job
// visible; more is how many were left out (shown as a "+N more" row).
func (m Model) window(list []int, c int) (shown []int, more int) {
	if len(list) <= c {
		return list, 0
	}
	k := max(c-1, 0)
	start := 0
	for p, i := range list {
		if i == m.selected && p >= k {
			start = p - k + 1
		}
	}
	return list[start : start+k], len(list) - k
}

// sidebarLines builds the sidebar, exactly h lines of w-1 cells (the last
// column is the separator).
func (m Model) sidebarLines(w, h int) []sideLine {
	ch := m.chrome()
	var running, queued []int
	for _, i := range m.runOrder() {
		switch st := m.jobs[i].Status; {
		case isRunning(st):
			running = append(running, i)
		case !isTerminal(st):
			queued = append(queued, i)
		}
	}
	finished := m.recentJobs()
	failed := 0
	for _, i := range finished {
		if isFailure(m.jobs[i].Status) {
			failed++
		}
	}
	over := m.overviewRows(w - 1)

	// Every section gets its title and at least one row; the rest of the
	// height goes to running jobs, the overview, queued, then finished runs.
	type section struct {
		title, count string
		items        []int
		rows         int
		over         bool
	}
	secs := []*section{{title: "Running", count: fmt.Sprint(len(running)), items: running}}
	if len(queued) > 0 {
		secs = append(secs, &section{title: "Queued", count: fmt.Sprint(len(queued)), items: queued})
	}
	if len(finished) > 0 {
		c := fmt.Sprint(len(finished))
		if failed > 0 {
			c += fmt.Sprintf(" · %d failed", failed)
		}
		secs = append(secs, &section{title: "Finished", count: c, items: finished})
	}
	if len(over) > 0 {
		secs = append(secs, &section{title: "Overview", over: true})
	}
	size := func(s *section) int {
		if s.over {
			return len(over)
		}
		return max(len(s.items), 1) // "nothing running" holds the row
	}
	left := h - 2*len(secs) + 1 // titles and the blank rows between sections
	for _, s := range secs {
		if left > 0 {
			s.rows, left = 1, left-1
		}
	}
	for _, want := range []string{"Running", "Overview", "Queued", "Finished"} {
		for _, s := range secs {
			if s.title == want && left > 0 {
				extra := min(size(s)-s.rows, left)
				s.rows, left = s.rows+extra, left-extra
			}
		}
	}

	var out []sideLine
	emit := func(text string, job int) { out = append(out, sideLine{text: fillLine(text, w-1), job: job}) }
	for k, s := range secs {
		if k > 0 {
			emit("", -1)
		}
		emit(fg(ch.title).Bold(true).Render(s.title)+" "+fg(ch.axis).Render(s.count), -1)
		switch {
		case s.over:
			for _, r := range pickRows(over, s.rows) {
				emit(r, -1)
			}
		case len(s.items) == 0 && s.rows > 0:
			emit(" "+fg(ch.axis).Render("nothing running"), -1)
		default:
			shown, more := m.window(s.items, s.rows)
			for _, i := range shown {
				emit(m.jobRow(i, w-1), i)
			}
			if more > 0 && s.rows > 0 {
				emit(" "+fg(ch.axis).Render(fmt.Sprintf("+%d more", more)), -1)
			}
		}
	}
	if len(out) > h {
		out = out[:h]
	}
	return out
}

// overviewRow is a key/value line of the Overview; rows with a higher prio
// number are dropped first when the sidebar is short.
type overviewRow struct {
	text string
	prio int
}

func pickRows(rows []overviewRow, n int) []string {
	keep := make([]bool, len(rows))
	for p := 0; n > 0 && p <= 10; p++ {
		for k, r := range rows {
			if r.prio == p && n > 0 {
				keep[k], n = true, n-1
			}
		}
	}
	var out []string
	for k, r := range rows {
		if keep[k] {
			out = append(out, r.text)
		}
	}
	return out
}

// overviewRows describes the selected run: state, host, elapsed time, step,
// phase, ETA and rate, liveness, exit code, its text panels and latest note.
func (m Model) overviewRows(w int) []overviewRow {
	j, ok := m.selectedJob()
	if !ok {
		return nil
	}
	ch := m.chrome()
	now := time.Now()
	var rows []overviewRow
	seen := map[string]bool{}
	add := func(k, v string, vst lipgloss.Style, prio int) {
		if v == "" || seen[k] {
			return
		}
		seen[k] = true
		v = truncate(v, max(w-14, 4))
		rows = append(rows, overviewRow{" " + fg(ch.key).Render(fmt.Sprintf("%-12s", truncate(k, 11))) + vst.Render(v), prio})
	}
	val := fg(ch.value)
	add("run", j.DisplayName(), val, 7)

	state, stSt := statusIcon(j.Status)+" "+j.Status, fg(ch.text)
	live, since, _ := liveUnknown, time.Duration(0), time.Duration(0)
	switch {
	case isRunning(j.Status):
		live, since, _ = m.jobLiveness(j, now)
		stSt = hexStyle(runHex)
		switch live {
		case liveStalled:
			state, stSt = "✖ stalled "+compactDuration(since), hexStyle(crashHex).Bold(true)
		case liveLate:
			state, stSt = "◔ late "+compactDuration(since), hexStyle(lateHex)
		}
	case j.Status == "completed":
		stSt = hexStyle(runHex)
	case isFailure(j.Status):
		stSt = hexStyle(crashHex).Bold(true)
	}
	add("state", state, stSt, 0)
	if j.ExitCode != nil {
		add("exit", fmt.Sprint(*j.ExitCode), val, 1)
	}
	host := j.RemoteID
	var run map[string]any
	if json.Unmarshal([]byte(j.RunJSON), &run) == nil {
		if h, _ := run["hostname"].(string); h != "" && host == "" {
			host = h
		}
	}
	add("host", host, val, 6)
	if isRunning(j.Status) {
		if st, ok := jobStart(j); ok {
			add("elapsed", formatAge(now.Sub(st)), val, 1)
		}
	} else if d, ok := j.DurationSeconds(); ok {
		add("elapsed", formatAge(time.Duration(d*float64(time.Second))), val, 1)
	}
	if cur := m.latestField(j.JobID, "current"); cur != "" {
		step := cur
		if tot := m.latestField(j.JobID, "total"); tot != "" {
			step += " / " + tot
		}
		add("step", strings.TrimSpace(step+" "+truncate(m.latestField(j.JobID, "unit"), 10)), val, 2)
	}
	phase := m.latestField(j.JobID, "phase")
	if phase == "" {
		phase = m.latestField(j.JobID, "stage")
	}
	add("phase", phase, val, 5)
	if isRunning(j.Status) {
		var parts []string
		if eta, ok := m.jobETA(j, now); ok {
			parts = append(parts, compactDuration(eta))
		}
		if rate := m.jobRate(j); rate != "" {
			parts = append(parts, rate)
		}
		add("eta", strings.Join(parts, " · "), val, 3)
		if live != liveUnknown {
			add("last output", compactDuration(since)+" ago", val, 4)
		}
	}
	if d, ok := m.panels[j.JobID]; ok {
		for _, p := range d.specs {
			if p.Kind != "text" {
				continue
			}
			v := p.Text
			if p.Key != "" {
				if f := m.latestField(j.JobID, p.Key); f != "" {
					v = f
				}
			}
			label := p.Label
			if label == "" {
				label = p.Key
			}
			add(label, v, val, 5)
		}
		if d.invalid > 0 {
			add("panels", fmt.Sprintf("%d spec ignored", d.invalid), hexStyle(lateHex), 8)
		}
	}
	add("note", m.jobMessage(j.JobID), fg(ch.text), 9)
	return rows
}

// compactEntries is the run list of narrow terminals: running, selected and
// pinned runs, flowed into columns of about 40 cells under the header. rows
// is how many lines it takes.
func (m Model) compactEntries(w, gh int) ([]compactEntry, int) {
	var list []int
	sel := m.selectedJobID()
	for _, i := range m.runOrder() {
		if j := m.jobs[i]; isRunning(j.Status) || j.JobID == sel || m.pinned[j.JobID] {
			list = append(list, i)
		}
	}
	if len(list) == 0 {
		return nil, 0
	}
	ncol := max(1, w/40)
	rows := min((len(list)+ncol-1)/ncol, max(1, gh/6))
	shown, more := m.window(list, rows*ncol)
	colW := w / ncol
	var out []compactEntry
	for k, i := range shown {
		out = append(out, compactEntry{rect: rect{x: (k / rows) * colW, y: 1 + k%rows, w: colW - 1, h: 1}, job: i})
	}
	if more > 0 {
		k := len(shown)
		out = append(out, compactEntry{rect: rect{x: (k / rows) * colW, y: 1 + k%rows, w: colW - 1, h: 1}, job: -1 - more})
	}
	return out, rows
}

// ---------- header and help bar ----------

func (m Model) renderLeetHeader() string {
	w := m.width
	ch := m.chrome()
	var running, queued, done, failed, cancelled, stalled int
	for _, j := range m.jobs {
		switch {
		case isRunning(j.Status):
			running++
			if m.isStalled(j) {
				stalled++
			}
		case isFailure(j.Status):
			failed++
		case j.Status == "completed":
			done++
		case j.Status == "cancelled":
			cancelled++
		case !isTerminal(j.Status):
			queued++
		}
	}
	badge := lipgloss.NewStyle().Foreground(lipgloss.Color(brandFgHex)).Background(lipgloss.Color(brandHex)).Bold(true).Render(" vanth ")
	if m.noColor() {
		badge = lipgloss.NewStyle().Reverse(true).Bold(true).Render(" vanth ")
	}
	count := func(glyph string, n int, label string, st lipgloss.Style, short bool) string {
		if n == 0 {
			st = fg(ch.axis)
		}
		if short && label != "stalled" {
			return st.Render(fmt.Sprintf("%s %d", glyph, n))
		}
		return st.Render(fmt.Sprintf("%s %d %s", glyph, n, label))
	}
	counts := func(short bool) string {
		parts := []string{
			count(statusIcon("running"), running, "running", hexStyle(runHex), short),
			count(statusIcon("queued"), queued, "queued", fg(ch.text), short),
			count(statusIcon("completed"), done, "done", hexStyle(runHex), short),
			count(statusIcon("failed"), failed, "failed", hexStyle(crashHex).Bold(true), short),
		}
		if cancelled > 0 {
			parts = append(parts, count(statusIcon("cancelled"), cancelled, "cancelled", fg(ch.text), short))
		}
		if stalled > 0 {
			parts = append(parts, count("✖", stalled, "stalled", hexStyle(crashHex).Bold(true), short))
		}
		sep := "  "
		if short {
			sep = " "
		}
		return badge + " " + strings.Join(parts, sep)
	}
	var facts []string
	if m.warnings > 0 {
		facts = append(facts, hexStyle(lateHex).Render(fmt.Sprintf("warn %d", m.warnings)))
	}
	if m.malformed > 0 {
		facts = append(facts, hexStyle(crashHex).Render(fmt.Sprintf("malformed %d", m.malformed)))
	}
	clock := time.Now().Format("15:04:05")
	full := strings.Join(append(facts, m.mode, fmt.Sprintf("%d jobs", len(m.jobs)), clock), " · ")
	for _, cand := range [][2]string{{counts(false), full}, {counts(false), clock}, {counts(false), ""}, {counts(true), clock}, {counts(true), ""}} {
		right := fg(ch.axis).Render(cand[1]) + " "
		if lipgloss.Width(cand[0])+lipgloss.Width(right)+3 <= w { // keep a clear gap
			return spread(cand[0], right, w)
		}
	}
	return fillLine(counts(true), w)
}

// renderHelpBar is the bottom line: a LIVE (or STALE / FALLBACK) badge and the
// key hints on teal. Hints drop out, least important first, when narrow.
func (m Model) renderHelpBar(g leetLayout) string {
	w := m.width
	badge, badgeHex := " LIVE ", brandHex
	switch {
	case m.staleSince != nil:
		badge, badgeHex = " STALE "+formatAge(time.Since(*m.staleSince))+" ", lateHex
	case m.mode == ModeFallback:
		badge = " FALLBACK "
	}
	type hint struct {
		text string
		drop int // higher drops first
	}
	page := "←→ page"
	if g.pages > 1 {
		page += fmt.Sprintf(" %d/%d", g.page+1, g.pages)
	}
	hints := []hint{{"↑↓ run", 2}, {"tab chart", 4}, {page, 3}, {"[ ] sidebar", 6}, {"enter pin", 9},
		{"e events", 7}, {"l logs", 5}, {"s slowest", 8}, {"? help", 0}, {"q quit", 0}}
	if m.lowerExpanded() {
		hints = append(hints[:1], hint{"pgup/pgdn scroll", 1}, hint{"esc close", 1}, hint{"? help", 0}, hint{"q quit", 0})
	}
	text := func() string {
		var parts []string
		for _, h := range hints {
			parts = append(parts, h.text)
		}
		return " " + strings.Join(parts, "  ")
	}
	for lipgloss.Width(badge)+lipgloss.Width(text()) > w && len(hints) > 2 {
		worst := 0
		for k, h := range hints {
			if h.drop > hints[worst].drop {
				worst = k
			}
		}
		hints = append(hints[:worst], hints[worst+1:]...)
	}
	rest := fillLine(text(), max(w-lipgloss.Width(badge), 0))
	if m.noColor() {
		return fillLine(lipgloss.NewStyle().Reverse(true).Bold(true).Render(badge)+rest, w)
	}
	dark := lipgloss.Color(brandFgHex)
	return fillLine(lipgloss.NewStyle().Foreground(dark).Background(lipgloss.Color(badgeHex)).Bold(true).Render(badge)+
		lipgloss.NewStyle().Foreground(dark).Background(lipgloss.Color(focusHex)).Render(rest), w)
}

// ---------- frame ----------

func (m Model) renderLeet() string {
	g := m.computeLeet()
	w := m.width
	ch := m.chrome()
	main := strings.Split(m.renderMain(g), "\n")
	body := make([]string, 0, m.height-2)
	if g.sideW > 0 {
		sep := fg(ch.border).Render("│")
		for r := 0; r < g.main.h; r++ {
			l := strings.Repeat(" ", g.sideW)
			if r < len(g.side) {
				l = fillLine(g.side[r].text, g.sideW)
			}
			body = append(body, l+sep+main[r])
		}
	} else {
		rows := g.main.y - 1
		lines := make([]string, rows)
		widths := make([]int, rows)
		for _, e := range g.compact {
			text := ""
			if e.job >= 0 {
				text = m.jobRow(e.job, e.w)
			} else {
				text = fillLine(" "+fg(ch.axis).Render(fmt.Sprintf("+%d more · ↑↓", -1-e.job)), e.w)
			}
			r := e.y - 1
			lines[r] += strings.Repeat(" ", max(e.x-widths[r], 0)) + text
			widths[r] = e.x + e.w
		}
		for _, l := range lines {
			body = append(body, fillLine(l, w))
		}
		body = append(body, main...)
	}
	return m.renderLeetHeader() + "\n" + strings.Join(body, "\n") + "\n" + m.renderHelpBar(g)
}

// renderMain draws the area right of (or under) the run list: a pane (events,
// logs, slowest) when one is open, else the chart grid, else the fleet view.
func (m Model) renderMain(g leetLayout) string {
	r := g.main
	if r.w <= 0 || r.h <= 0 {
		return ""
	}
	switch {
	case m.lowerExpanded():
		return frameBlock(m.renderLower(r), r.w, r.h)
	case len(m.jobs) == 0:
		msg := "no jobs found"
		if m.empty && m.emptyMsg != "" {
			msg = m.emptyMsg
		} else if m.mode == ModeFallback {
			msg = "no jobs found; reading fallback event mirrors"
		}
		if m.loading {
			msg = "loading jobs…"
		}
		return frameBlock(m.centeredPane(msg, r), r.w, r.h)
	case len(g.cells) == 0:
		return frameBlock(m.renderFleet(r), r.w, r.h)
	}
	grid := make([]string, r.h)
	focus := clamp(m.chartFocus, 0, len(g.specs)-1)
	for _, c := range g.cells {
		box := strings.Split(m.renderChartCell(c, g.specs[c.spec], c.spec == focus), "\n")
		for k := 0; k < c.h && k < len(box); k++ {
			row := c.y - r.y + k
			grid[row] += strings.Repeat(" ", max(c.x-r.x-lipgloss.Width(grid[row]), 0)) + box[k]
		}
	}
	return frameBlock(strings.Join(grid, "\n"), r.w, r.h)
}

// ---------- charts ----------

// niceSpans are the auto window lengths; each halves cleanly for the middle
// axis label.
var niceSpans = []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute, 10 * time.Minute, 20 * time.Minute,
	30 * time.Minute, time.Hour, 2 * time.Hour, 4 * time.Hour, 6 * time.Hour, 12 * time.Hour, 24 * time.Hour,
	48 * time.Hour, 96 * time.Hour, 8 * 24 * time.Hour, 16 * 24 * time.Hour, 32 * 24 * time.Hour}

// chartWindow is the time range of a chart: it ends now while any of its jobs
// runs (at the newest point otherwise) and spans the zoom the user picked or
// the shortest nice span holding every point.
func (m Model) chartWindow(s chartSpec) (lo, hi time.Time, live bool) {
	hi = time.Now()
	var first, last time.Time
	for _, i := range s.jobs {
		live = live || isRunning(m.jobs[i].Status)
		pts := m.points[SeriesKey{JobID: m.jobs[i].JobID, Metric: s.key}]
		if len(pts) == 0 {
			continue
		}
		if t, ok := parseTS(pts[0].At); ok && (first.IsZero() || t.Before(first)) {
			first = t
		}
		if t, ok := parseTS(pts[len(pts)-1].At); ok && t.After(last) {
			last = t
		}
	}
	if !live && !last.IsZero() {
		hi = last
	}
	span, zoomed := m.zoom[s.key]
	if !zoomed {
		span = niceSpans[len(niceSpans)-1]
		for _, d := range niceSpans {
			if first.IsZero() || hi.Sub(first) <= d {
				span = d
				break
			}
		}
	}
	return hi.Add(-span), hi, live
}

// seriesIn returns the points of a series inside [lo, hi], newest last. Points
// arrive in time order, so the scan stops at the first one before lo.
func seriesIn(pts []Point, lo, hi time.Time) (out []Point, at []time.Time) {
	for k := len(pts) - 1; k >= 0; k-- {
		t, ok := parseTS(pts[k].At)
		if !ok || t.After(hi) {
			continue
		}
		if t.Before(lo) {
			break
		}
		out, at = append(out, pts[k]), append(at, t)
	}
	for a, b := 0, len(out)-1; a < b; a, b = a+1, b-1 {
		out[a], out[b], at[a], at[b] = out[b], out[a], at[b], at[a]
	}
	return out, at
}

// fmtNum is an axis or readout number in at most about six cells.
func fmtNum(v float64) string {
	switch a := math.Abs(v); {
	case a != 0 && a < 1e-3:
		return fmt.Sprintf("%.0e", v)
	case a >= 1000 && a < 1e4:
		return fmt.Sprintf("%.0f", v) // 1767, not 1770
	}
	return shortNum(v)
}

func fmtSpan(d time.Duration) string {
	if d%time.Hour == 0 && d >= time.Hour {
		return compactDuration(d)
	}
	if d%time.Minute == 0 || d < time.Minute {
		return compactDuration(d)
	}
	return fmt.Sprintf("%dm%02ds", int(d.Minutes()), int(d.Seconds())%60)
}

type plotLine struct {
	pts    []Point
	at     []time.Time
	style  lipgloss.Style
	marker rune
}

var brailleBit = [4][2]rune{{0x01, 0x08}, {0x02, 0x10}, {0x04, 0x20}, {0x40, 0x80}}

// plot draws the lines into a w x h cell braille canvas (2x4 dots per cell).
// Each dot column holds the range of its points, so spikes survive
// downsampling; neighbouring columns are joined unless the gap is over a
// quarter of the window. A cell takes the colour of the last line through it.
// Without colour every line draws its marker character instead.
func (m Model) plot(w, h int, lines []plotLine, lo, hi time.Time, ymin, ymax float64) []string {
	bits := make([][]rune, h)
	owner := make([][]int, h)
	for y := range bits {
		bits[y], owner[y] = make([]rune, w), make([]int, w)
	}
	dw, dh := w*2, h*4
	span := hi.Sub(lo).Seconds()
	set := func(x, y, si int) {
		if x >= 0 && y >= 0 && x < dw && y < dh {
			bits[y/4][x/2] |= brailleBit[y%4][x%2]
			owner[y/4][x/2] = si
		}
	}
	vline := func(x, a, b, si int) {
		for y := min(a, b); y <= max(a, b); y++ {
			set(x, y, si)
		}
	}
	dotY := func(v float64) int {
		f := 0.5
		if ymax > ymin {
			f = (v - ymin) / (ymax - ymin)
		}
		return clamp(int(math.Round((1-f)*float64(dh-1))), 0, dh-1)
	}
	for si, l := range lines {
		first, last := make([]int, dw), make([]int, dw)
		lo2, hi2 := make([]int, dw), make([]int, dw)
		seen := make([]bool, dw)
		for k, p := range l.pts {
			x := 0
			if span > 0 {
				x = clamp(int(math.Round(l.at[k].Sub(lo).Seconds()/span*float64(dw-1))), 0, dw-1)
			}
			y := dotY(p.Y)
			if !seen[x] {
				seen[x], first[x], lo2[x], hi2[x] = true, y, y, y
			}
			last[x], lo2[x], hi2[x] = y, min(lo2[x], y), max(hi2[x], y)
		}
		prev := -1
		for x := 0; x < dw; x++ {
			if !seen[x] {
				continue
			}
			if prev >= 0 && x-prev <= max(dw/4, 2) {
				for xx := prev + 1; xx <= x; xx++ {
					y0 := last[prev] + (first[x]-last[prev])*(xx-1-prev)/(x-prev)
					y1 := last[prev] + (first[x]-last[prev])*(xx-prev)/(x-prev)
					vline(xx, y0, y1, si)
				}
			}
			vline(x, lo2[x], hi2[x], si)
			prev = x
		}
	}
	ch := m.chrome()
	out := make([]string, h)
	for y := range bits {
		var b strings.Builder
		run, runOwner := "", -2
		flush := func() {
			switch {
			case run == "":
			case runOwner == -1:
				b.WriteString(fg(ch.guide).Render(run))
			case runOwner < -1 || m.noColor():
				b.WriteString(run)
			default:
				b.WriteString(lines[runOwner].style.Render(run))
			}
			run = ""
		}
		for x, r := range bits[y] {
			cell, o := " ", -3
			switch {
			case r != 0 && m.noColor():
				cell, o = string(lines[owner[y][x]].marker), -3
			case r != 0:
				cell, o = string(0x2800+r), owner[y][x]
			case y == h-1 && !m.noColor():
				cell, o = "⠤", -1 // faint baseline
			}
			if o != runOwner {
				flush()
				runOwner = o
			}
			run += cell
		}
		flush()
		out[y] = b.String()
	}
	return out
}

// chartLines collects the drawable series of a chart, the selected job last
// so it is drawn on top.
func (m Model) chartLines(s chartSpec, lo, hi time.Time) []plotLine {
	var out []plotLine
	var selLine *plotLine
	for _, i := range s.jobs {
		j := m.jobs[i]
		pts, at := seriesIn(m.points[SeriesKey{JobID: j.JobID, Metric: s.key}], lo, hi)
		if len(pts) == 0 {
			continue
		}
		l := plotLine{pts: pts, at: at, style: fg(m.jobColor(j.JobID)), marker: m.jobMarker(j.JobID)}
		if j.JobID == m.selectedJobID() {
			selLine = &l
			continue
		}
		out = append(out, l)
	}
	if selLine != nil {
		out = append(out, *selLine)
	}
	return out
}

// readout is the latest value of every series, in series colours (red for a
// stalled job), set into the chart's top border; " ✖ stalled" when every job
// on the chart has stalled.
func (m Model) readout(s chartSpec) string {
	var parts []string
	stalled := 0
	for _, i := range s.jobs {
		j := m.jobs[i]
		v, ok := m.latestValue(j.JobID, s.key)
		if !ok {
			continue
		}
		txt, st := fmtNum(v), fg(m.jobColor(j.JobID)).Bold(true)
		if s.key == "progress.percent" {
			txt = fmt.Sprintf("%.0f", v)
		}
		if m.noColor() {
			txt = string(m.jobMarker(j.JobID)) + txt
		}
		if m.isStalled(j) {
			st, stalled = hexStyle(crashHex).Bold(true), stalled+1
		}
		parts = append(parts, st.Render(txt))
	}
	out := strings.Join(parts, " ")
	if stalled > 0 && stalled == len(parts) {
		out += hexStyle(crashHex).Bold(true).Render(" ✖ stalled")
	}
	return out
}

func (m Model) chartTitle(s chartSpec, focused bool) string {
	st := fg(m.chrome().title).Bold(true)
	if focused {
		st = hexStyle(focusHex).Bold(true)
	}
	return st.Render(s.title)
}

// renderChartCell draws one grid cell: a rounded box with the title and the
// latest values on its top border, faint y labels, the braille plot and a
// relative time axis.
func (m Model) renderChartCell(c chartCell, s chartSpec, focused bool) string {
	if s.kind == "stat" {
		return m.renderStatTile(c.rect, s, focused)
	}
	ch := m.chrome()
	iw, ih := c.w-2, c.h-2
	lo, hi, live := m.chartWindow(s)
	lines := m.chartLines(s, lo, hi)
	opts := panelOpts{hint: m.readout(s), focused: focused}
	if len(lines) == 0 || iw < 12 || ih < 2 {
		msg := "waiting for " + s.key
		return m.panelBoxOpts(c.rect, m.chartTitle(s, focused), lipgloss.Place(max(iw, 1), max(ih, 1), lipgloss.Center, lipgloss.Center, fg(ch.axis).Render(truncate(msg, max(iw, 1)))), opts)
	}
	ymin, ymax := math.Inf(1), math.Inf(-1)
	for _, l := range lines {
		for _, p := range l.pts {
			ymin, ymax = math.Min(ymin, p.Y), math.Max(ymax, p.Y)
		}
	}
	if s.lo != nil {
		ymin = *s.lo
	}
	if s.hi != nil {
		ymax = *s.hi
	}
	if ymin == ymax {
		pad := math.Max(math.Abs(ymin)*0.1, 1)
		ymin, ymax = ymin-pad, ymax+pad
	} else if s.lo == nil && s.hi == nil {
		pad := (ymax - ymin) * 0.05
		if ymin >= 0 && ymin-pad <= (ymax-ymin)*0.1 {
			ymin = 0 // non-negative data near zero gets a zero floor, never a negative one
		} else if ymin >= 0 {
			ymin -= pad
		} else {
			ymin -= pad
		}
		ymax += pad
	}
	ph := ih - 2 // rows for the axis line and the time labels
	axis := ph >= 2
	if !axis {
		ph = ih
	}
	labels := map[int]string{0: fmtNum(ymax), ph - 1: fmtNum(ymin)}
	if ph >= 5 {
		labels[ph/2] = fmtNum((ymin + ymax) / 2)
	}
	gut := 3
	for _, l := range labels {
		gut = max(gut, lipgloss.Width(l))
	}
	gut = min(gut, 7)
	pw := iw - gut - 1
	plot := m.plot(pw, ph, lines, lo, hi, ymin, ymax)
	ax := fg(ch.axis)
	body := make([]string, 0, ih)
	for y, row := range plot {
		tick := "│"
		lab, ok := labels[y]
		if ok {
			tick = "┤"
		}
		body = append(body, ax.Render(fmt.Sprintf("%*s", gut, truncate(lab, gut))+tick)+row)
	}
	if axis {
		span := hi.Sub(lo)
		end := "now"
		if !live {
			end = hi.Local().Format("15:04")
		}
		left := fillLine("-"+fmtSpan(span), max(pw/2-2, 0)) + "-" + fmtSpan(span/2)
		body = append(body, ax.Render(strings.Repeat(" ", gut)+"└"+strings.Repeat("─", pw)),
			ax.Render(strings.Repeat(" ", gut+1)+spread(left, end, pw)))
	}
	return m.panelBoxOpts(c.rect, m.chartTitle(s, focused), strings.Join(body, "\n"), opts)
}

// renderStatTile is a stat panel: the value of the first job as big digits
// (bold text when it is not a whole number or does not fit), a sparkline of
// its history and the other jobs' values.
func (m Model) renderStatTile(r rect, s chartSpec, focused bool) string {
	ch := m.chrome()
	iw, ih := r.w-2, r.h-2
	var body []string
	if len(s.jobs) > 0 {
		j := m.jobs[s.jobs[0]]
		st := fg(m.jobColor(j.JobID)).Bold(true)
		if m.isStalled(j) {
			st = hexStyle(crashHex).Bold(true)
		}
		if v, ok := m.latestValue(j.JobID, s.key); ok {
			if big := bigNumber(int(math.Round(v)), iw-2); ih >= 5 && big != nil && v >= 0 && v == math.Trunc(v) {
				for _, l := range big {
					body = append(body, " "+st.Render(l))
				}
			} else {
				body = append(body, " "+st.Render(fmtNum(v)))
			}
			if ih >= len(body)+2 && iw >= 8 {
				sp := sparkline(m.seriesValues(j.JobID, s.key, iw-2), iw-2)
				if m.noColor() {
					body = append(body, "", " "+sp)
				} else {
					body = append(body, "", " "+fg(m.jobColor(j.JobID)).Render(sp))
				}
			}
		}
		var others []string
		for _, i := range s.jobs[1:] {
			if v, ok := m.latestValue(m.jobs[i].JobID, s.key); ok {
				mark := fg(m.jobColor(m.jobs[i].JobID)).Render("■")
				if m.noColor() {
					mark = string(m.jobMarker(m.jobs[i].JobID))
				}
				others = append(others, mark+" "+fg(ch.value).Render(fmtNum(v)))
			}
		}
		if len(others) > 0 && ih > len(body)+1 {
			body = append(body, "", " "+strings.Join(others, "  "))
		}
	}
	if len(body) == 0 {
		body = []string{fg(ch.axis).Render("waiting for " + s.key)}
	}
	top := max((ih-len(body))/2, 0)
	content := strings.Repeat("\n", top) + strings.Join(body, "\n")
	return m.panelBoxOpts(r, m.chartTitle(s, focused), content, panelOpts{focused: focused})
}

// ---------- hit testing ----------

// leetClick selects the run or focuses the chart under the pointer.
func (m Model) leetClick(x, y int) Model {
	g := m.computeLeet()
	selectJob := func(i int) {
		if i >= 0 && i != m.selected {
			m.selected = i
			m.bumpGen()
		}
	}
	if g.sideW > 0 && x < g.sideW && y >= 1 && y-1 < len(g.side) {
		selectJob(g.side[y-1].job)
		return m
	}
	for _, e := range g.compact {
		if inRect(x, y, e.rect) {
			selectJob(e.job)
			return m
		}
	}
	for _, c := range g.cells {
		if inRect(x, y, c.rect) {
			m.chartFocus = c.spec
			return m
		}
	}
	return m
}

// chartAt returns the chart spec under the pointer.
func (m Model) chartAt(x, y int) (chartSpec, bool) {
	g := m.computeLeet()
	for _, c := range g.cells {
		if inRect(x, y, c.rect) {
			return g.specs[c.spec], true
		}
	}
	return chartSpec{}, false
}

func inRect(x, y int, r rect) bool {
	return x >= r.x && x < r.x+r.w && y >= r.y && y < r.y+r.h
}
