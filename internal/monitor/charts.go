package monitor

import (
	"fmt"
	"image/color"
	"math"
	"strings"
	"time"

	"bloomfx.dev/bloom/fig"
	"bloomfx.dev/bloom/fig/fonts"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/colorprofile"
	"github.com/charmbracelet/x/ansi"
)

// The chart grid under the cards: borderless braille line charts, one per
// metric, every job in its own colour, the latest values right-aligned in the
// title row, faint y labels and a relative time axis.

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

func (m Model) isStalled(j JobSummary) bool {
	if !isRunning(j.Status) {
		return false
	}
	st, _, _ := m.jobLiveness(j, time.Now())
	return st == liveStalled
}

// bigFonts are tried in turn for the big gradient numbers of the cards and
// stat tiles; both are three rows of half blocks once blank rows are trimmed
// (and pagga-style ░ backgrounds dropped).
var bigFonts = []func() *fig.Font{fonts.LinguaholicMiniBlock, fonts.Miniwi}

// bigText renders s in the first big font that fits maxW cells, painted with
// p, its blank rows trimmed. nil when none fits.
func bigText(s string, maxW int, p fig.Paint) []string {
	for _, font := range bigFonts {
		f := font().Figure(s, fig.Options{})
		if w, _ := f.Size(); w > maxW {
			continue
		}
		var out []string
		for _, l := range strings.Split(strings.ReplaceAll(f.Render(p), "░", " "), "\n") {
			if strings.TrimSpace(ansi.Strip(l)) != "" {
				out = append(out, l)
			}
		}
		return out
	}
	return nil
}

// ---------- which jobs and charts ----------

// chartJobs are the jobs with a card and on the charts, in run-list order:
// every running job plus the selected and pinned ones.
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

// chartSpecs lists the charts: the jobs' declared panels (line and gauge
// become charts, stat a big-number tile; text goes to the cards), the
// progress of every charted job, then the metrics of jobs that declared none.
// The cards already show progress, so it follows the declared panels, except
// in the compact layout, where the progress chart is the one chart shown.
// A metric shared by several jobs is one chart with a line per job.
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
	progress := func() {
		for _, i := range cj {
			if has(i, "progress.percent") {
				add(chartSpec{key: "progress.percent", title: "progress %", kind: "line", lo: &zero, hi: &hundred})
				return
			}
		}
	}
	if m.compactLayout() {
		progress()
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
	progress()
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

// ---------- grid ----------

type chartCell struct {
	rect
	spec int // index into the layout's specs
}

const (
	chartMinW   = 30
	chartIdealH = 14 // 13 rows of chart and a blank row
	chartMinH   = 6
)

// gridShape picks columns and rows for an area: cells at least chartMinW
// wide and about chartIdealH tall; one column in the compact layout.
func (m Model) gridShape(area rect, n int) (cols, rows int) {
	cols = clamp(area.w/chartMinW, 1, 3)
	if m.compactLayout() {
		cols = 1
	}
	rows = max(1, int(math.Round(float64(area.h)/chartIdealH)))
	if area.h/rows < chartMinH {
		rows = max(1, area.h/chartMinH)
	}
	if n < cols*rows { // few charts: widen them, but keep their height
		cols = max(1, min(cols, (n+rows-1)/rows))
	}
	return cols, rows
}

// gridCells places the page of charts holding the focused chart.
func (m Model) gridCells(area rect, n int) (cells []chartCell, page, pages int) {
	if n == 0 || area.w < 10 || area.h < 3 {
		return nil, 0, 0
	}
	cols, rows := m.gridShape(area, n)
	per := cols * rows
	pages = (n + per - 1) / per
	page = clamp(m.chartFocus, 0, n-1) / per
	cw, ch := area.w/cols, area.h/rows
	for k := page * per; k < min(n, (page+1)*per); k++ {
		c, r := (k-page*per)%cols, (k-page*per)/cols
		cell := rect{x: area.x + c*cw, y: area.y + r*ch, w: cw - 3, h: ch - 1} // 3 cells and a row of air
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

// ---------- chart data ----------

// niceSpans are the auto window lengths.
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
				b.WriteString(fg(ch.faint).Render(run))
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

// trend is ↑ or ↓ when a series moved clearly over its last points.
func trend(pts []Point) string {
	if len(pts) < 10 {
		return ""
	}
	lo, hi := pts[0].Y, pts[0].Y
	for _, p := range pts {
		lo, hi = math.Min(lo, p.Y), math.Max(hi, p.Y)
	}
	d := pts[len(pts)-1].Y - pts[len(pts)-10].Y
	switch {
	case hi == lo || math.Abs(d) < (hi-lo)*0.05:
		return ""
	case d > 0:
		return " ↑"
	}
	return " ↓"
}

// readout is the latest value of every series, in series colours (red for a
// stalled job), right-aligned in the chart's title row; " ✕ stalled" when
// every job on the chart has stalled. A single line also shows its trend.
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
			st, stalled = hexStyle(failHex).Bold(true), stalled+1
		}
		if len(s.jobs) == 1 && s.kind != "stat" {
			txt += trend(m.points[SeriesKey{JobID: j.JobID, Metric: s.key}])
		}
		parts = append(parts, st.Render(txt))
	}
	out := strings.Join(parts, " ")
	if stalled > 0 && stalled == len(parts) {
		out += hexStyle(failHex).Bold(true).Render(" ✕ stalled")
	}
	return out
}

func (m Model) chartTitle(s chartSpec, focused bool) string {
	ch := m.chrome()
	switch {
	case focused && m.noColor():
		return boldStyle.Render("▸ " + s.title)
	case focused:
		return hexStyle(brandHex).Bold(true).Render(s.title)
	}
	return fg(ch.text).Bold(true).Render(s.title)
}

// yRange is the y axis of a chart: fixed bounds when declared, else the data
// range padded, with a zero floor for non-negative data near zero.
func yRange(s chartSpec, lines []plotLine) (ymin, ymax float64) {
	ymin, ymax = math.Inf(1), math.Inf(-1)
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
		return ymin - pad, ymax + pad
	}
	if s.lo == nil && s.hi == nil {
		pad := (ymax - ymin) * 0.05
		if ymin >= 0 && ymin-pad <= (ymax-ymin)*0.1 {
			ymin = 0 // non-negative data near zero gets a zero floor, never a negative one
		} else {
			ymin -= pad
		}
		ymax += pad
	}
	return ymin, ymax
}

// renderChart draws one borderless grid cell, exactly r.w x r.h: the title
// and readout, the braille plot with faint y labels on its right, and the
// time axis ("30m ago … now").
func (m Model) renderChart(r rect, s chartSpec, focused bool) string {
	if s.kind == "stat" {
		return m.renderStatTile(r, s, focused)
	}
	ch := m.chrome()
	dim := fg(ch.dim)
	head := spread(m.chartTitle(s, focused), m.readout(s), r.w)
	lo, hi, live := m.chartWindow(s)
	lines := m.chartLines(s, lo, hi)
	if len(lines) == 0 || r.w < 12 || r.h < 3 {
		msg := dim.Render(truncate("waiting for "+s.key, r.w))
		return frameBlock(head+"\n"+lipgloss.Place(r.w, max(r.h-1, 1), lipgloss.Center, lipgloss.Center, msg), r.w, r.h)
	}
	ymin, ymax := yRange(s, lines)
	axis := r.h >= 5
	ph := r.h - 1
	if axis {
		ph--
	}
	top, bot := fmtNum(ymax), fmtNum(ymin)
	gut := min(max(lipgloss.Width(top), lipgloss.Width(bot))+1, 8)
	pw := r.w - gut
	body := []string{head}
	for y, row := range m.plot(pw, ph, lines, lo, hi, ymin, ymax) {
		lab := ""
		switch y {
		case 0:
			lab = top
		case ph - 1:
			lab = bot
		}
		body = append(body, row+dim.Render(" "+truncate(lab, gut-1)))
	}
	if axis {
		end := "now"
		if !live {
			end = hi.Local().Format("15:04")
		}
		body = append(body, dim.Render(spread(fmtSpan(hi.Sub(lo))+" ago", end, pw)))
	}
	return frameBlock(strings.Join(body, "\n"), r.w, r.h)
}

// renderStatTile is a stat panel: the first job's value as big gradient
// digits (bold text when they do not fit), a sparkline of its history and
// the other jobs' values in the title row.
func (m Model) renderStatTile(r rect, s chartSpec, focused bool) string {
	ch := m.chrome()
	var body []string
	var others []string
	if len(s.jobs) > 0 {
		j := m.jobs[s.jobs[0]]
		a, b := m.jobGrad(j)
		if v, ok := m.latestValue(j.JobID, s.key); ok {
			txt := fmtNum(v)
			if big := bigText(txt, r.w, fig.Gradient(a, b)); r.h >= 5 && big != nil {
				body = append(body, big...)
			} else {
				body = append(body, fg(a).Bold(true).Render(txt))
			}
			if r.h >= len(body)+3 && r.w >= 8 {
				sp := sparkline(m.seriesValues(j.JobID, s.key, r.w), r.w)
				if !m.noColor() {
					sp = fg(m.jobColor(j.JobID)).Render(sp)
				}
				body = append(body, "", sp)
			}
		}
		for _, i := range s.jobs[1:] {
			if v, ok := m.latestValue(m.jobs[i].JobID, s.key); ok {
				txt := fmtNum(v)
				if m.noColor() {
					txt = string(m.jobMarker(m.jobs[i].JobID)) + txt
				}
				others = append(others, fg(m.jobColor(m.jobs[i].JobID)).Bold(true).Render(txt))
			}
		}
	}
	if len(body) == 0 {
		body = []string{fg(ch.dim).Render("waiting for " + s.key)}
	}
	head := spread(m.chartTitle(s, focused), strings.Join(others, " "), r.w)
	top := max((r.h-1-len(body))/2, 1)
	return frameBlock(head+strings.Repeat("\n", top)+strings.Join(body, "\n"), r.w, r.h)
}

func inRect(x, y int, r rect) bool {
	return x >= r.x && x < r.x+r.w && y >= r.y && y < r.y+r.h
}
