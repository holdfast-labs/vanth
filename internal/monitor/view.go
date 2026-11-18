package monitor

import (
	"encoding/json"
	"fmt"
	"image/color"
	"math"
	"strconv"
	"strings"
	"time"

	"bloomfx.dev/bloom"
	"bloomfx.dev/bloom/fx"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/NimbleMarkets/ntcharts/v2/canvas"
	"github.com/NimbleMarkets/ntcharts/v2/linechart"
)

// Chrome colors. Panels share one muted border color (W&B LEET uses a muted
// "layout" gray), titles get an adaptive accent, and the selected list row gets
// the amber highlight LEET uses for the selected run.
const (
	panelBorderHex = "#8b8b8b"
	selectedBgHex  = "#FCBC32"
	selectedFgHex  = "#171717"
)

func titleColor(dark bool) color.Color {
	if dark {
		return lipgloss.Color("#c9c9c9")
	}
	return lipgloss.Color("#3a3a3a")
}

func statusBarColors(dark bool) (fg, bg color.Color) {
	if dark {
		return lipgloss.Color("#e8eaed"), lipgloss.Color("#3a3f45")
	}
	return lipgloss.Color("#20242a"), lipgloss.Color("#dfe3e8")
}

// View returns the terminal view. It never performs I/O; charts are drawn from
// the immutable in-memory snapshot.
func (m Model) View() tea.View {
	v := tea.NewView(m.render())
	v.AltScreen = true
	v.MouseMode = tea.MouseModeCellMotion
	return v
}

func (m Model) render() string {
	if !m.ready {
		return m.centered("loading vanth monitor…")
	}
	if m.width < m.cfg.MinWidth || m.height < m.cfg.MinHeight {
		return fmt.Sprintf("terminal too small: %dx%d (minimum %dx%d)",
			m.width, m.height, m.cfg.MinWidth, m.cfg.MinHeight)
	}
	lay := m.computeLayout()

	var jobsPane, metricsPane, chartsPane, lowerPane, status string
	headerPane := m.renderHeader(lay.header)
	if m.help && m.helpOnly() {
		jobsPane = m.renderJobs(lay.jobs)
		chartsPane = ""
		metricsPane = ""
		lowerPane = m.renderHelp(lay.lower)
	} else if len(m.jobs) == 0 {
		msg := "no jobs found"
		if m.empty && m.emptyMsg != "" {
			msg = m.emptyMsg
		} else if m.mode == ModeFallback {
			msg = "no jobs found; reading fallback event mirrors"
		}
		if m.loading {
			msg = "loading jobs…"
		}
		jobsPane = ""
		chartsPane = ""
		metricsPane = ""
		lowerPane = m.centeredPane(msg, lay.lower)
	} else {
		jobsPane = m.renderJobs(lay.jobs)
		metricsPane = m.renderMetrics(lay.metrics)
		chartsPane = m.renderCharts(lay)
		lowerPane = m.renderLower(lay.lower)
	}
	status = m.renderStatusBar(lay)

	var top string
	if lay.narrow {
		top = lipgloss.JoinVertical(lipgloss.Top, jobsPane, "", chartsPane)
	} else {
		left := jobsPane
		if metricsPane != "" {
			left = lipgloss.JoinVertical(lipgloss.Left, jobsPane, metricsPane)
		}
		top = lipgloss.JoinHorizontal(lipgloss.Top, left, " ", chartsPane)
	}
	top = padToHeight(top, lay.body.h)

	var parts []string
	parts = append(parts, headerPane)
	if gapTop := lay.body.y - (lay.header.y + lay.header.h); gapTop > 0 {
		parts = append(parts, "")
	}
	if top != "" {
		parts = append(parts, top)
	}
	if gapBottom := lay.lower.y - (lay.body.y + lay.body.h); gapBottom > 0 {
		parts = append(parts, "")
	}
	parts = append(parts, lowerPane)
	parts = append(parts, status)
	frame := strings.Join(parts, "\n")
	if m.help && !m.helpOnly() {
		if out, ok := m.helpDialog(frame); ok {
			return out
		}
	}
	return frame
}

// helpOnly reports whether help must use the old lower-pane form because the
// terminal is too small for a dialog over the dashboard.
func (m Model) helpOnly() bool {
	return m.width < 44 || m.height < 22
}

// boxTopLine builds a rounded top border with the title embedded, exactly
// `width` cells wide: "╭─ title ───╮".
func boxTopLine(width int, title string) string {
	if width < 4 {
		return ""
	}
	inner := width - 2
	capW := inner - 4
	if capW < 1 {
		capW = 1
	}
	t := lipgloss.NewStyle().MaxWidth(capW).Render(title)
	tw := lipgloss.Width(t)
	fill := inner - 3 - tw
	if fill < 1 {
		fill = 1
	}
	return lipgloss.NewStyle().Foreground(lipgloss.Color(panelBorderHex)).
		Render("╭─ " + t + " " + strings.Repeat("─", fill) + "╮")
}

// boxBottomLine builds a rounded bottom border, exactly `width` cells wide.
func boxBottomLine(width int) string {
	if width < 2 {
		return ""
	}
	inner := width - 2
	return lipgloss.NewStyle().Foreground(lipgloss.Color(panelBorderHex)).
		Render("╰" + strings.Repeat("─", inner) + "╯")
}

// fillLine truncates a possibly ANSI-styled string to `width` cells and pads it
// with trailing spaces to exactly `width` cells.
func fillLine(line string, width int) string {
	if width <= 0 {
		return ""
	}
	l := lipgloss.NewStyle().MaxWidth(width).Render(line)
	if w := lipgloss.Width(l); w < width {
		l += strings.Repeat(" ", width-w)
	}
	return l
}

// jobRows is the number of content rows job i occupies: unselected jobs take
// one row (two when they have a progress bar), the selected job adds a
// details row. renderJobs, the scroll window and the mouse hit-test all use it.
func (m Model) jobRows(i int) int {
	n := 1
	if m.jobProgressPercent(m.jobs[i].JobID) >= 0 || m.jobSparkMetric(m.jobs[i].JobID) != "" {
		n = 2
	}
	if i == m.selected {
		return max(n, 2) + boolInt(n == 2)
	}
	return n
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// jobWindowStart returns the first visible job so the selected job (with some
// rows of context below it) stays on screen.
func (m Model) jobWindowStart(contentH int) int {
	if m.selected <= 0 || m.selected >= len(m.jobs) {
		return 0
	}
	budget := max(contentH-contentH/3, m.jobRows(m.selected))
	rows, start := m.jobRows(m.selected), m.selected
	for start > 0 && rows+m.jobRows(start-1) <= budget {
		start--
		rows += m.jobRows(start)
	}
	return start
}

// jobAtRow maps a content row of the jobs pane to a job index (or -1).
func (m Model) jobAtRow(row, contentH int) int {
	if row < 0 || row >= contentH {
		return -1 // border rows; a job past the last visible row is not on screen
	}
	y := 0
	for i := m.jobWindowStart(contentH); i < len(m.jobs); i++ {
		y += m.jobRows(i)
		if row < y {
			return i
		}
	}
	return -1
}

// renderJobs renders the job list pane as a titled, bordered box. Row 0 of the
// box is the top border carrying the title, so job rows start at content row 0
// (rect row 1), matching handleMouseClick via jobAtRow.
func (m Model) renderJobs(r rect) string {
	title := "Jobs"
	if len(m.jobs) > 0 {
		title = fmt.Sprintf("Jobs (%d)", len(m.jobs))
	}
	innerW := r.w - 2
	contentH := r.h - 2
	var lines []string
	for i := m.jobWindowStart(contentH); i < len(m.jobs); i++ {
		if len(lines) >= contentH {
			break
		}
		for _, l := range m.jobBlock(i, innerW) {
			if len(lines) >= contentH {
				break
			}
			lines = append(lines, l)
		}
	}
	styledTitle := lipgloss.NewStyle().Bold(true).Foreground(titleColor(m.dark)).Render(title)
	return m.panelBoxOpts(r, styledTitle, strings.Join(lines, "\n"), panelOpts{
		hint:    "↑↓ select · enter pin",
		focused: m.focus == focusJobs,
		pulse:   m.pulsePhase(m.summary().running),
	})
}

// selectedStyle is the accent-tinted, bold highlight of the selected job: loud
// enough to find at a glance, calm enough not to glare.
func (m Model) selectedStyle() lipgloss.Style {
	if m.dark {
		return lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#f4f0ff")).Background(lipgloss.Color("#3b2f78"))
	}
	return lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#1d1540")).Background(lipgloss.Color("#d9d0fb"))
}

// jobBlock renders jobRows(i) lines for one job: a header line (cursor, icon,
// pin, name, right-aligned status); a progress-bar line with events when the job
// has progress; and, for the selected job only, a details line (events, tags,
// exit code). Every line is exactly innerW cells wide.
func (m Model) jobBlock(i int, innerW int) []string {
	j := m.jobs[i]
	sel := i == m.selected
	cursor := " "
	if sel {
		cursor = ">"
	}
	pin := " "
	if m.pinned[j.JobID] {
		pin = "P"
	}
	prefix := cursor + statusIcon(j.Status) + pin + " "

	// The status word repeats what glyph and color already say, so only the
	// selected row spells it out; other rows spend the room on the name and a
	// dim right-aligned age/duration.
	status := j.Status
	if !sel {
		status = m.jobAgeText(j)
	}
	if sw := lipgloss.Width(status); sw > innerW-2 {
		status = truncate(status, innerW-2)
	}
	statusW := lipgloss.Width(status)
	nameArea := innerW - statusW - 1
	if nameArea < 1 {
		nameArea = 1
	}
	right := status
	if !sel {
		right = dimStyle.Render(status)
	}
	out := []string{fillLine(prefix+truncate(j.DisplayName(), nameArea-lipgloss.Width(prefix)), nameArea) + " " + right}

	evt := fmt.Sprintf("evt %d", j.EventCount)
	pct := m.jobProgressPercent(j.JobID)
	spark := ""
	if k := m.jobSparkMetric(j.JobID); k != "" && innerW >= 30 {
		sc := lipgloss.NewStyle().Foreground(statusColor(j.Status, m.dark))
		spark = " " + sc.Render(sparkline(m.seriesValues(j.JobID, k, 10), 10))
	}
	switch {
	case pct >= 0:
		pctText := fmt.Sprintf(" %3.0f%%", pct)
		barW := innerW - 2 - lipgloss.Width(pctText) - lipgloss.Width(spark)
		if barW < 5 {
			spark, barW = "", innerW-2-lipgloss.Width(pctText)
		}
		out = append(out, fillLine("  "+m.styledBar(j.Status, pct, max(barW, 1), sel)+pctText+spark, innerW))
	case spark != "":
		out = append(out, fillLine("  "+strings.TrimLeft(spark, " ")+dimStyle.Render(" "+m.jobSparkMetric(j.JobID)), innerW))
	}
	if sel {
		meta := evt
		if len(j.Tags) > 0 {
			meta += " · tags " + strings.Join(j.Tags, ",")
		}
		if j.ExitCode != nil {
			meta += fmt.Sprintf(" · exit %d", *j.ExitCode)
		}
		out = append(out, fillLine("  "+truncate(meta, max(innerW-2, 0)), innerW))
		st := m.selectedStyle()
		for i := range out {
			out[i] = keepBg(st, out[i])
		}
	} else {
		accent := lipgloss.NewStyle().Foreground(statusColor(j.Status, m.dark))
		out[0] = accent.Bold(j.Status == "failed" || j.Status == "running").Render(out[0])
		if len(out) > 1 {
			out[1] = lipgloss.NewStyle().Faint(true).Render(out[1])
		}
	}
	if f, t := m.flashPhase(j.JobID); t > 0 && !sel {
		// A status change glows for a few refreshes: Bloom sweeps a band of the
		// new status color across the block, one step per refresh.
		note := fillLine("  ↳ now "+lipgloss.NewStyle().Bold(true).Foreground(statusColor(f.to, m.dark)).Render(f.to), innerW)
		if len(out) == 1 {
			out = append(out, note)
		} else {
			out[len(out)-1] = note
		}
		glow := bloom.Render(innerW, len(out), bloom.Transition(t, bloom.Text(strings.Join(out, "\n")),
			fx.Shimmer{Color: statusColor(f.to, m.dark), Width: 14}))
		if gl := strings.Split(glow, "\n"); len(gl) == len(out) && lipgloss.Width(glow) == innerW {
			out = gl
		}
	}
	return out
}

// jobProgressPercent returns the latest normalized progress percent for a job,
// or -1 when no progress series exists.
func (m Model) jobProgressPercent(jobID string) float64 {
	pts := m.points[SeriesKey{JobID: jobID, Metric: "progress.percent"}]
	if len(pts) == 0 {
		return -1
	}
	return pts[len(pts)-1].Y
}

// summaryCounts aggregates jobs by status plus durable event totals.
type summaryCounts struct {
	queued, running, completed, failed, cancelled, orphaned, other int
	events, pendingDeliveries                                      int64
}

func (m Model) summary() summaryCounts {
	var s summaryCounts
	for _, j := range m.jobs {
		s.events += j.EventCount
		switch j.Status {
		case "queued", "pending":
			s.queued++
		case "running":
			s.running++
		case "completed":
			s.completed++
		case "failed":
			s.failed++
		case "cancelled":
			s.cancelled++
		case "orphaned":
			s.orphaned++
		default:
			s.other++
		}
	}
	return s
}

// renderSummaryHeader draws the LEET-style overview box: status counts, a
// "needs attention" triage line, totals, and mode/refresh badges, with the
// title embedded in the top border. It always renders exactly 4 content rows so
// the header box has a stable height (headerH).
func (m Model) renderSummaryHeader(r rect) string {
	s := m.summary()
	title := gradientText("Vanth monitor", true)

	innerW := max(r.w-2, 1)
	type badge struct {
		status string
		count  int
	}
	all := []badge{{"running", s.running}, {"queued", s.queued}, {"completed", s.completed},
		{"failed", s.failed}, {"cancelled", s.cancelled}, {"orphaned", s.orphaned}}
	var full, compact []string
	for _, b := range all {
		full = append(full, m.statusBadge(b.status, b.count, false))
		compact = append(compact, m.statusBadge(b.status, b.count, true))
	}
	if s.other > 0 {
		o := lipgloss.NewStyle().Foreground(lipgloss.Color("#8b8b8b")).Render(fmt.Sprintf("other %d", s.other))
		full, compact = append(full, o), append(compact, o)
	}
	countLine := strings.Join(full, "  ")
	if lipgloss.Width(countLine) > innerW {
		// Narrow terminals: glyph + count only; the glyph keeps the status
		// identifiable without color.
		countLine = strings.Join(compact, " ")
	}

	// Row 2: "needs attention" triage (LEET's health badge): failed and
	// orphaned jobs plus running jobs still below 100% progress.
	attn := m.attentionCount()
	var attnLine string
	if attn > 0 {
		attnLine = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#e34948")).
			Render(fmt.Sprintf("attention %d", attn))
	} else {
		attnLine = lipgloss.NewStyle().Faint(true).Render("attention 0")
	}
	if bar := m.statusDistBar(s, min(innerW-lipgloss.Width(attnLine)-2, 24)); bar != "" {
		attnLine += "  " + bar
	}

	// Volume totals share the triage row; mode, refresh age and stale/malformed
	// flags live in the status bar, so the header stays two content rows.
	totals := []string{fmt.Sprintf("jobs %d", len(m.jobs)), fmt.Sprintf("events %d", s.events)}
	if n := len(m.pinned); n > 0 {
		totals = append(totals, fmt.Sprintf("pinned %d", n))
	}
	if age := m.oldestRunningAge(); age > 0 {
		totals = append(totals, "oldest running "+formatAge(age))
	}
	totalsLine := dimStyle.Render(strings.Join(totals, " · "))

	if r.h-2 <= 1 {
		// Compact strip: one row of badges, attention and the status bar;
		// totals move onto the border.
		short := dimStyle.Render("⚑ 0")
		if attn > 0 {
			short = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#e34948")).Render(fmt.Sprintf("⚑ %d", attn))
		}
		line := countLine + "  " + short
		if room := innerW - lipgloss.Width(line) - 2; room >= 8 {
			line += "  " + m.statusDistBar(s, min(room, 24))
		}
		return m.panelBoxOpts(r, title, line, panelOpts{hint: strings.Join(totals, " · ")})
	}
	content := lipgloss.JoinVertical(lipgloss.Top, countLine, attnLine+"   "+totalsLine)
	return m.panelBox(r, title, content)
}

// attentionCount aggregates jobs that need human attention: failed or orphaned
// jobs plus running jobs whose latest recorded progress is below 100%. Running
// jobs without any progress series are left out (no signal one way or the
// other).
func (m Model) attentionCount() int {
	s := m.summary()
	n := s.failed + s.orphaned
	for _, j := range m.jobs {
		if j.Status != "running" {
			continue
		}
		if pct := m.jobProgressPercent(j.JobID); pct >= 0 && pct < 100 {
			n++
		}
	}
	return n
}

// oldestRunningAge returns the age of the longest-running job still marked
// running, or 0 when there is none (or its CreatedAt cannot be parsed).
func (m Model) oldestRunningAge() time.Duration {
	var oldest time.Duration
	for _, j := range m.jobs {
		if j.Status != "running" {
			continue
		}
		if age := ageOf(j.CreatedAt); age > 0 && (oldest == 0 || age > oldest) {
			oldest = age
		}
	}
	return oldest
}

func ageOf(ts string) time.Duration {
	t, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return 0
	}
	if d := time.Since(t); d > 0 {
		return d
	}
	return 0
}

// formatAge renders a duration compactly, e.g. "47m12s" or "2h05m".
func formatAge(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	if h > 0 {
		return fmt.Sprintf("%dh%02dm", h, m)
	}
	return fmt.Sprintf("%dm%02ds", m, int(d.Seconds())%60)
}

// statusBadge renders "glyph label count" (or "glyph count" when compact) in the
// status color; zero counts are dimmed so non-empty buckets pop.
func (m Model) statusBadge(status string, count int, compact bool) string {
	text := fmt.Sprintf("%s %s %d", statusIcon(status), status, count)
	if compact {
		text = fmt.Sprintf("%s %d", statusIcon(status), count)
	}
	if count == 0 {
		return lipgloss.NewStyle().Faint(true).Render(text)
	}
	return lipgloss.NewStyle().Bold(true).Foreground(statusColor(status, m.dark)).Render(text)
}

// statusDistBar is a stacked bar of job counts per status, width cells wide.
// Every non-empty status gets at least one cell. Returns "" when it cannot fit.
func (m Model) statusDistBar(s summaryCounts, width int) string {
	total := s.running + s.queued + s.completed + s.failed + s.cancelled + s.orphaned
	if total == 0 || width < 8 {
		return ""
	}
	order := []struct {
		status string
		n      int
	}{{"running", s.running}, {"queued", s.queued}, {"completed", s.completed},
		{"failed", s.failed}, {"cancelled", s.cancelled}, {"orphaned", s.orphaned}}
	cells := make([]int, len(order))
	used, nonEmpty := 0, 0
	for i, o := range order {
		if o.n > 0 {
			nonEmpty++
			cells[i] = max(1, o.n*width/total)
			used += cells[i]
		}
	}
	// Trim or pad the largest segment so the bar is exactly width cells.
	big := 0
	for i := range cells {
		if cells[i] > cells[big] {
			big = i
		}
	}
	cells[big] += width - used
	var b strings.Builder
	for i, o := range order {
		if cells[i] > 0 {
			b.WriteString(lipgloss.NewStyle().Foreground(statusColor(o.status, m.dark)).
				Render(strings.Repeat("█", cells[i])))
		}
	}
	return b.String()
}

// styledBar draws a progress bar whose fill is tinted by job status and whose
// track is dimmed. Selected rows keep the plain bar (the amber background owns
// the colors there).
func (m Model) styledBar(status string, pct float64, width int, selected bool) string {
	bar := progressBar(pct, width)
	if width <= 4 {
		return bar
	}
	filled := strings.Count(bar, "█")
	return lipgloss.NewStyle().Foreground(statusColor(status, m.dark)).Render(strings.Repeat("█", filled)) +
		lipgloss.NewStyle().Faint(true).Render(strings.Repeat("░", width-filled))
}

// keepBg renders row in style st even when the row already contains styled
// segments: their SGR resets would otherwise end st's background early.
func keepBg(st lipgloss.Style, row string) string {
	const mark = "\x00"
	pre, _, ok := strings.Cut(st.Render(mark), mark)
	if !ok || pre == "" {
		return st.Render(row)
	}
	return st.Render(strings.ReplaceAll(row, "\x1b[m", "\x1b[m"+pre))
}

// progressBar renders a filled progress bar of the given width for a 0-100
// value. A width <= 4 renders a compact numeric readout instead.
func progressBar(percent float64, width int) string {
	if width <= 4 {
		return fmt.Sprintf("%3.0f%%", percent)
	}
	clamped := percent
	if clamped < 0 {
		clamped = 0
	}
	if clamped > 100 {
		clamped = 100
	}
	filled := int(math.Round(clamped / 100 * float64(width)))
	if filled > width {
		filled = width
	}
	return strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
}

// renderCharts builds every metric chart inside its own bordered box.
func (m Model) renderCharts(lay layout) string {
	if len(m.metrics) == 0 {
		// No per-job metrics: show fleet-level visuals instead of an empty box.
		a := lay.chartsArea
		if a.w < 2 || a.h < 2 {
			return ""
		}
		if out := m.renderFleet(a); out != "" && lipgloss.Height(out) == a.h {
			return out
		}
		return m.panelBox(a, lipgloss.NewStyle().Bold(true).Render("Plots"),
			lipgloss.Place(a.w-2, max(a.h-2, 1), lipgloss.Center, lipgloss.Center, dimStyle.Render("no metric events yet")))
	}
	if len(lay.charts) == 0 || lay.chartsArea.h <= 0 {
		return ""
	}
	slots := AssignSlots(m.allChartJobIDs())
	cols := 1
	if len(lay.charts) >= 2 && lay.charts[1].y == lay.charts[0].y {
		cols = 2
	}
	var rows []string
	var row []string
	for i, cr := range lay.charts {
		row = append(row, m.renderChart(cr, slots))
		lastInRow := cols == 1 || (i+1)%cols == 0 || i == len(lay.charts)-1
		if lastInRow {
			parts := []string{}
			for j, r := range row {
				if j > 0 {
					parts = append(parts, " ")
				}
				parts = append(parts, r)
			}
			rows = append(rows, lipgloss.JoinHorizontal(lipgloss.Top, parts...))
			row = nil
		}
	}
	grid := strings.Join(rows, "\n")
	if len(lay.charts) < len(m.metrics) {
		grid += "\n" + lipgloss.NewStyle().Faint(true).Render(
			fmt.Sprintf("+%d more metrics", len(m.metrics)-len(lay.charts)))
	}
	return padToHeight(grid, lay.chartsArea.h)
}

// renderChart renders one metric plot inside a titled, rounded box. The braille
// canvas occupies the box interior minus the borders.
func (m Model) renderChart(cr chartRect, slots map[string]int) string {
	metric := m.metrics[cr.metric]
	minX, maxX, ok := m.metricWindow(cr.metric)
	titleW := cr.w
	if titleW <= 0 {
		return ""
	}
	if !ok {
		title := m.chartTitle(metric, nil, 0, 0, 0, titleW)
		content := fillLine(lipgloss.NewStyle().Faint(true).Render("no data for "+metric), max(cr.w-2, 1))
		return m.panelBox(cr.rect, title, content)
	}
	draw, hidden := m.chartDrawJobs(metric, slots)
	title := m.chartTitle(metric, draw, hidden, minX, maxX, titleW)

	canvasW := cr.w - 2
	canvasH := cr.h - 2
	if canvasW < 10 || canvasH < 3 {
		return m.panelBox(cr.rect, title, "")
	}
	// Legend swatches get their own row inside the box (instead of crowding
	// the title) whenever the chart is tall enough to spare it.
	legend := ""
	if len(draw) > 1 && canvasH >= 6 {
		legend = m.chartLegend(draw, hidden, canvasW) + "\n"
		canvasH--
	}
	minY, maxY, series := m.chartDomain(draw, metric, minX, maxX)
	if minY > maxY {
		minY, maxY = 0, 1
	}

	lc := m.newChart(canvasW, canvasH, minX, maxX, minY, maxY)
	lc.DrawXYAxisAndLabel()

	// Draw unpinned jobs first, pinned last so pinned stay visible.
	for _, s := range series {
		if s.dj.pinned {
			continue
		}
		m.drawSeriesPoints(&lc, s, minX, maxX)
	}
	for _, s := range series {
		if !s.dj.pinned {
			continue
		}
		m.drawSeriesPoints(&lc, s, minX, maxX)
	}
	// Selected-job marker after all lines.
	for _, s := range series {
		if s.dj.job.JobID != m.selectedJobID() {
			continue
		}
		if last := lastPoint(s.pts); last != nil {
			style := lipgloss.NewStyle().Foreground(lipgloss.Color("#ffffff")).Bold(true)
			lc.DrawRuneWithStyle(canvas.Float64Point{X: last.X, Y: last.Y}, '●', style)
		}
		break
	}
	// Crosshair markers.
	if x, has := m.crosshair[cr.metric]; has {
		style := lipgloss.NewStyle().Foreground(lipgloss.Color("#ffffff")).Bold(true)
		for _, s := range series {
			if p := nearestPoint(s.pts, x); p != nil {
				lc.DrawRuneWithStyle(canvas.Float64Point{X: p.X, Y: p.Y}, '✕', style)
			}
		}
	}
	return m.panelBoxOpts(cr.rect, title, legend+lc.View(), panelOpts{hint: m.chartReadout(metric, draw, minX, maxX), focused: m.focus == focusCharts && cr.metric == m.chartFocus})
}

type drawJob struct {
	job    JobSummary
	slot   int
	pinned bool
}

type seriesPts struct {
	dj  drawJob
	pts []Point
}

// chartDrawJobs returns the jobs drawn for a metric in draw order (unpinned
// first, pinned last) plus the hidden-overlay count.
func (m Model) chartDrawJobs(metric string, slots map[string]int) ([]drawJob, int) {
	var withData []JobSummary
	for _, j := range m.jobs {
		if len(m.points[SeriesKey{JobID: j.JobID, Metric: metric}]) == 0 {
			continue
		}
		withData = append(withData, j)
	}
	hidden := len(withData) - m.cfg.MaxOverlays
	if hidden < 0 {
		hidden = 0
	}
	var pinned, unpinned []JobSummary
	sel := m.selectedJobID()
	for _, j := range withData {
		if m.pinned[j.JobID] {
			pinned = append(pinned, j)
		} else {
			unpinned = append(unpinned, j)
		}
	}
	for i, j := range unpinned {
		if j.JobID == sel {
			unpinned = append(unpinned[:i:i], unpinned[i+1:]...)
			unpinned = append([]JobSummary{j}, unpinned...)
			break
		}
	}
	all := append([]JobSummary{}, unpinned...)
	all = append(all, pinned...)
	if len(all) > m.cfg.MaxOverlays {
		all = all[:m.cfg.MaxOverlays]
	}
	out := make([]drawJob, 0, len(all))
	for _, j := range all {
		out = append(out, drawJob{job: j, slot: slots[j.JobID], pinned: m.pinned[j.JobID]})
	}
	return out, hidden
}

// chartDomain filters points to the window and computes the shared y domain.
func (m Model) chartDomain(draw []drawJob, metric string, minX, maxX float64) (minY, maxY float64, series []seriesPts) {
	minY, maxY = math.Inf(1), math.Inf(-1)
	for _, dj := range draw {
		pts := m.filteredPoints(dj.job.JobID, metric, minX, maxX)
		for _, p := range pts {
			if p.Y < minY {
				minY = p.Y
			}
			if p.Y > maxY {
				maxY = p.Y
			}
		}
		series = append(series, seriesPts{dj: dj, pts: pts})
	}
	if math.IsInf(minY, 1) || math.IsInf(maxY, -1) {
		return 0, 1, series
	}
	if minY == maxY {
		pad := math.Max(math.Abs(minY)*0.1, 1)
		minY -= pad
		maxY += pad
	} else if maxY-minY < 1e-9 {
		mid := (minY + maxY) / 2
		minY, maxY = mid-1, mid+1
	}
	return minY, maxY, series
}

func (m Model) filteredPoints(jobID, metric string, minX, maxX float64) []Point {
	all := m.points[SeriesKey{JobID: jobID, Metric: metric}]
	out := make([]Point, 0, len(all))
	for _, p := range all {
		if p.X >= minX && p.X <= maxX {
			out = append(out, p)
		}
	}
	return out
}

func (m Model) newChart(w, h int, minX, maxX, minY, maxY float64) linechart.Model {
	// Few labels: the shape matters more than tick values.
	xStep := max(1, w/4)
	yStep := yStepFor(h - 2)
	axis := lipgloss.NewStyle().Foreground(lipgloss.Color("#8b8b8b"))
	label := lipgloss.NewStyle().Foreground(lipgloss.Color("#8b8b8b"))
	return linechart.New(w, h, minX, maxX, minY, maxY,
		linechart.WithXYSteps(xStep, yStep),
		linechart.WithXLabelFormatter(func(i int, v float64) string { return compactFmt(v) }),
		linechart.WithYLabelFormatter(func(i int, v float64) string { return compactFmt(v) }),
		linechart.WithStyles(axis, label, axis),
	)
}

// drawSeriesPoints downsamples each connectable segment and draws it.
func (m Model) drawSeriesPoints(lc *linechart.Model, s seriesPts, minX, maxX float64) {
	if len(s.pts) == 0 {
		return
	}
	style := lipgloss.NewStyle().Foreground(slotColor(s.dj.slot, m.dark))
	if s.dj.job.JobID == m.selectedJobID() {
		style = style.Bold(true)
	}
	graphW := lc.GraphWidth()
	for _, seg := range SplitSegments(s.pts, m.cfg.GapFactor) {
		draw := seg
		if shouldDownsample(seg, graphW, m.cfg.DownsampleThreshold) {
			draw = DownsampleSeries(seg, graphW)
		}
		drawSegment(lc, draw, style)
	}
}

func drawSegment(lc *linechart.Model, seg []Point, style lipgloss.Style) {
	for i := 0; i < len(seg); i++ {
		f1 := canvas.Float64Point{X: seg[i].X, Y: seg[i].Y}
		if i+1 < len(seg) {
			f2 := canvas.Float64Point{X: seg[i+1].X, Y: seg[i+1].Y}
			lc.DrawBrailleLineWithStyle(f1, f2, style)
		} else {
			lc.DrawBrailleLineWithStyle(f1, f1, style)
		}
	}
}

func lastPoint(pts []Point) *Point {
	if len(pts) == 0 {
		return nil
	}
	return &pts[len(pts)-1]
}

// nearestPoint returns the point with x closest to target (linear scan; series
// may contain x regressions so binary search is unsafe).
func nearestPoint(pts []Point, target float64) *Point {
	if len(pts) == 0 {
		return nil
	}
	best := 0
	bestD := math.Abs(pts[0].X - target)
	for i := 1; i < len(pts); i++ {
		if d := math.Abs(pts[i].X - target); d < bestD {
			best, bestD = i, d
		}
	}
	return &pts[best]
}

// chartTitle composes the metric name, an overlay legend (with hidden count)
// when multiple jobs are drawn, and the selected job's latest value. The
// legend comes first so the fold indicator survives narrow truncation. The
// caller embeds the result in the box's top border, which truncates it safely.
func (m Model) chartTitle(metric string, draw []drawJob, hidden int, minX, maxX float64, width int) string {
	title := lipgloss.NewStyle().Bold(true).Render(metric)
	if hidden > 0 && len(draw) > 1 {
		title += "  " + lipgloss.NewStyle().Faint(true).Render(fmt.Sprintf("+%d hidden", hidden))
	}
	line := title
	if width > 0 {
		return lipgloss.NewStyle().MaxWidth(width).Render(line)
	}
	return line
}

// legendOrder sorts drawn jobs in stable list order.
func (m Model) legendOrder(draw []drawJob) []drawJob {
	out := make([]drawJob, len(draw))
	copy(out, draw)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && m.jobOrder(out[j].job) < m.jobOrder(out[j-1].job); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

func (m Model) jobOrder(j JobSummary) int {
	if idx, ok := m.jobByID[j.JobID]; ok {
		return idx
	}
	return len(m.jobs)
}

func (m Model) lastVisiblePoint(jobID, metric string, minX, maxX float64) *Point {
	pts := m.filteredPoints(jobID, metric, minX, maxX)
	if len(pts) == 0 {
		return nil
	}
	return &pts[len(pts)-1]
}

// renderLower renders the exact event table (the default), the log tail, or a
// hint when both panes are toggled off.
// renderSlowest renders the top terminal runs by runtime (#6). Durations come
// from started_at/ended_at, so running and remote-shadow rows are excluded.
func (m Model) renderSlowest(r rect) string {
	jobs := m.slowestJobs(20)
	title := "Slowest runs · top 20 by runtime"
	if len(jobs) == 0 {
		return m.panelBox(r, title, lipgloss.NewStyle().Faint(true).Render("no completed runs yet"))
	}
	width := r.w - 2
	if width < 24 {
		width = 24
	}
	jobW, statusW, runtimeW := 20, 9, 9
	nameW := width - jobW - statusW - runtimeW - 3
	if nameW < 8 {
		nameW = 8
	}
	header := lipgloss.NewStyle().Bold(true).Render(fmt.Sprintf(
		"%-*s %-*s %-*s %s", jobW, "JOB", statusW, "STATUS", runtimeW, "RUNTIME", "NAME"))
	lines := []string{header}
	for _, job := range jobs {
		seconds, _ := job.DurationSeconds()
		lines = append(lines, fmt.Sprintf("%-*s %-*s %-*s %s",
			jobW, truncate(job.JobID, jobW),
			statusW, truncate(job.Status, statusW),
			runtimeW, formatAge(time.Duration(seconds*float64(time.Second))),
			truncate(job.DisplayName(), nameW)))
	}
	return m.renderScrollPane(title, lines, r)
}

func (m Model) renderEvents(r rect) string {
	id := m.selectedJobID()
	events := m.events[id]
	title := fmt.Sprintf("Events · %s (%d)", id, len(events))
	lines := m.eventLines(events, r.w-2)
	return m.renderScrollPane(title, lines, r)
}

func (m Model) eventLines(events []Event, width int) []string {
	if width <= 0 {
		return nil
	}
	timeW := 12
	seqW := 7
	typeW := 6
	for _, ev := range events {
		typeW = max(typeW, min(lipgloss.Width(ev.Type), 18))
	}
	levelW := 7
	fixed := timeW + seqW + typeW + levelW
	rest := width - fixed
	if rest < 12 {
		return nil
	}
	msgW := rest * 45 / 100
	dataW := rest - msgW
	var out []string
	for _, ev := range events {
		ts := truncate(FormatTimestamp(ev.CreatedAt), timeW)
		seq := fmt.Sprintf("%6d", ev.Seq)
		typ := truncate(ev.Type, typeW)
		lvl := levelStyle(ev.Level, m.dark).Render(fmt.Sprintf("%-*s", levelW, truncate(ev.Level, levelW)))
		msg := truncate(ev.Message, msgW)
		data := truncate(CompactJSON(roundFloats(ev.Data).(map[string]any)), dataW)
		out = append(out, fmt.Sprintf("%-*s %s %-*s %s %-*s %s",
			timeW, ts, seq, typeW, typ, lvl, msgW, msg, data))
	}
	return out
}

func (m Model) renderLogs(r rect) string {
	id := m.selectedJobID()
	title := fmt.Sprintf("Logs · %s", id)
	lines := m.logTail
	if m.logTailJob != id {
		title += " (loading…)"
		lines = nil
	} else if m.logLoading {
		title += " (loading…)"
	}
	if m.logTailErr != "" && m.logTailJob == id {
		title += " · " + m.logTailErr
	}
	return m.renderScrollPane(title, lines, r)
}

func (m Model) renderScrollPane(title string, lines []string, r rect) string {
	rows := r.h - 2
	if rows < 1 {
		rows = 1
	}
	maxScroll := len(lines) - rows
	if maxScroll < 0 {
		maxScroll = 0
	}
	if m.lowerScroll > maxScroll {
		m.lowerScroll = maxScroll
	}
	if m.lowerScroll < 0 {
		m.lowerScroll = 0
	}
	start := len(lines) - rows - m.lowerScroll
	if start < 0 {
		start = 0
	}
	end := start + rows
	if end > len(lines) {
		end = len(lines)
	}
	var out []string
	shown := lines[start:end]
	if len(shown) == 0 {
		out = append(out, fillLine(lipgloss.NewStyle().Faint(true).Render("(no entries)"), r.w-2))
	} else {
		for _, l := range shown {
			out = append(out, fillLine(l, r.w-2))
		}
	}
	styledTitle := lipgloss.NewStyle().Bold(true).Foreground(titleColor(m.dark)).Render(title)
	return m.panelBoxOpts(r, styledTitle, strings.Join(out, "\n"), panelOpts{
		hint:    "↑↓ scroll · t tail",
		focused: m.focus == focusLower,
	})
}

func (m Model) renderHelp(r rect) string {
	if r.w < 20 || r.h < 8 {
		return "help needs a larger window"
	}
	lines := []string{
		"vanth monitor",
		"",
		"up/down or j/k   move in list/table",
		"tab / shift+tab  move focus",
		"enter            pin/unpin job",
		"left/right [ ]   pan x",
		"+/-/pgup/pgdn    zoom x",
		"t                return to live tail",
		"l                toggle log tail",
		"e                toggle event table",
		"s                toggle slowest-runs table",
		"mouse wheel      zoom chart under pointer",
		"mouse click      select job / set crosshair",
		"?                toggle this help",
		"q or ctrl+c      quit",
	}
	title := lipgloss.NewStyle().Bold(true).Foreground(titleColor(m.dark)).Render("Help")
	return m.panelBox(r, title, lipgloss.JoinVertical(lipgloss.Top, lines...))
}

func refreshAge(t time.Time) string {
	if t.IsZero() {
		return "–"
	}
	d := time.Since(t)
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return fmt.Sprintf("%.1fs", d.Seconds())
}

func (m Model) centeredPane(msg string, r rect) string {
	title := lipgloss.NewStyle().Bold(true).Foreground(titleColor(m.dark)).Render("Status")
	if r.w < 2 || r.h < 2 {
		return ""
	}
	innerW := r.w - 2
	innerH := r.h - 2
	if innerW < 1 || innerH < 1 {
		return m.panelBox(r, title, "")
	}
	content := lipgloss.Place(innerW, innerH, lipgloss.Center, lipgloss.Center,
		lipgloss.NewStyle().Faint(true).Render(msg))
	if m.loading && innerH >= 5 && innerW >= 20 {
		// First load: Bloom skeleton bars above the message.
		skel := loadingSkeleton(innerW-4, innerH-2)
		content = lipgloss.JoinVertical(lipgloss.Left, "  "+strings.ReplaceAll(skel, "\n", "\n  "),
			lipgloss.PlaceHorizontal(innerW, lipgloss.Center, dimStyle.Render(msg)))
	}
	return m.panelBox(r, title, content)
}

func (m Model) centered(msg string) string {
	if m.width <= 0 || m.height <= 0 {
		return msg
	}
	return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, msg)
}

// compactFmt renders a number with compact SI suffixes for quiet axes.
// parseRunOverview renders the run-overview metadata (W&B overview tab) into a
// compact list of display rows: author@host, runtime, git branch, hardware.
func parseRunOverview(runJSON string) []string {
	if runJSON == "" || runJSON == "null" {
		return nil
	}
	var run map[string]any
	if err := json.Unmarshal([]byte(runJSON), &run); err != nil {
		return nil
	}
	var rows []string
	author, _ := run["author"].(string)
	host, _ := run["hostname"].(string)
	if author != "" && host != "" {
		rows = append(rows, fmt.Sprintf("%s@%s", author, host))
	} else if host != "" {
		rows = append(rows, host)
	}
	if git, ok := run["git"].(map[string]any); ok {
		branch, _ := git["branch"].(string)
		commit, _ := git["commit"].(string)
		repo, _ := git["repository"].(string)
		if branch != "" {
			b := branch
			if commit != "" && len(commit) >= 7 {
				b += " @" + commit[:7]
			}
			if repo != "" {
				b += " · " + shortRepo(repo)
			}
			rows = append(rows, b)
		}
	}
	if cpu, ok := run["cpu_count"].(float64); ok && cpu > 0 {
		gpus := ""
		if g, ok := run["gpus"].([]any); ok && len(g) > 0 {
			if first, ok := g[0].(map[string]any); ok {
				if name, ok := first["name"].(string); ok && name != "" {
					gpus = " · " + name
				}
			}
		}
		rows = append(rows, fmt.Sprintf("%.0f cpu%s", cpu, gpus))
	}
	return rows
}

// shortRepo trims git remote URLs to a readable name, handling both
// slash-separated URLs and scp-like `user@host:org/repo` syntax.
func shortRepo(repo string) string {
	repo = strings.TrimSuffix(repo, ".git")
	if i := strings.LastIndex(repo, "/"); i >= 0 {
		return repo[i+1:]
	}
	if i := strings.LastIndex(repo, ":"); i >= 0 {
		return repo[i+1:]
	}
	return repo
}

func compactFmt(v float64) string {
	if v == math.Trunc(v) && math.Abs(v) < 1e15 {
		return strconv.FormatInt(int64(v), 10)
	}
	switch {
	case math.Abs(v) >= 1e9:
		return fmt.Sprintf("%.1fB", v/1e9)
	case math.Abs(v) >= 1e6:
		return fmt.Sprintf("%.1fM", v/1e6)
	case math.Abs(v) >= 1e3:
		return fmt.Sprintf("%.1fk", v/1e3)
	}
	return strconv.FormatFloat(v, 'g', 4, 64)
}

func truncate(s string, width int) string {
	if width <= 0 {
		return ""
	}
	if lipgloss.Width(s) <= width {
		return s
	}
	r := []rune(s)
	w := 0
	for i, ru := range r {
		ruWidth := runeWidth(ru)
		if w+ruWidth > width {
			if w == width {
				return string(r[:i])
			}
			return string(r[:i]) + "…"
		}
		w += ruWidth
	}
	return s
}

func runeWidth(r rune) int {
	switch {
	case r >= 0x1100 && (r <= 0x115f || r == 0x2329 || r == 0x232a ||
		(0x2e80 <= r && r <= 0xa4cf && r != 0x303f) ||
		(0xac00 <= r && r <= 0xd7a3) ||
		(0xf900 <= r && r <= 0xfaff) ||
		(0xfe10 <= r && r <= 0xfe19) ||
		(0xfe30 <= r && r <= 0xfe6f) ||
		(0xff00 <= r && r <= 0xff60) ||
		(0xffe0 <= r && r <= 0xffe6)):
		return 2
	}
	return 1
}

func truncateLines(lines []string, width int) []string {
	out := make([]string, len(lines))
	for i, l := range lines {
		out[i] = truncate(l, width)
	}
	return out
}

// padToHeight pads or trims a string to exactly h lines.
func padToHeight(s string, h int) string {
	if h <= 0 {
		return ""
	}
	cur := lipgloss.Height(s)
	if cur < h {
		return s + strings.Repeat("\n", h-cur)
	}
	if cur > h {
		lines := strings.Split(s, "\n")
		return strings.Join(lines[:h], "\n")
	}
	return s
}

// roundFloats returns v with every float rounded to 6 significant digits, so a
// payload preview reads 0.864 instead of 0.8639999999999999. Values that are
// not floats pass through; maps and slices are copied, never mutated.
func roundFloats(v any) any {
	switch x := v.(type) {
	case float64:
		r, err := strconv.ParseFloat(strconv.FormatFloat(x, 'g', 6, 64), 64)
		if err != nil {
			return x
		}
		return r
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = roundFloats(e)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = roundFloats(e)
		}
		return out
	}
	return v
}

// chartReadout is the big current value of the selected job on a chart: bold,
// in the job's series color, with its trend arrow. It sits on the box border.
func (m Model) chartReadout(metric string, draw []drawJob, minX, maxX float64) string {
	sel := m.selectedJobID()
	p := m.lastVisiblePoint(sel, metric, minX, maxX)
	if p == nil {
		return ""
	}
	for _, dj := range draw {
		if dj.job.JobID == sel {
			arrow := trendArrow(m.points[SeriesKey{JobID: sel, Metric: metric}])
			return lipgloss.NewStyle().Bold(true).Foreground(slotColor(dj.slot, m.dark)).
				Render(compactFmt(p.Y) + " " + arrow)
		}
	}
	return ""
}

// jobAgeText is a compact duration for finished jobs, or the age for running
// ones: "45s", "12m", "5h", "9d".
func (m Model) jobAgeText(j JobSummary) string {
	if sec, ok := j.DurationSeconds(); ok {
		return compactDuration(time.Duration(sec * float64(time.Second)))
	}
	ref := j.UpdatedAt
	if j.Status == "running" || j.Status == "queued" {
		ref = j.CreatedAt
		if j.StartedAt != "" {
			ref = j.StartedAt
		}
	}
	if d := ageOf(ref); d > 0 {
		return compactDuration(d)
	}
	return ""
}

func compactDuration(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
}

// yStepFor picks a y-label step that divides the graph height exactly. ntcharts
// sizes the label gutter by sampling heights 0, step, 2*step, ... but draws the
// top label at the full height too; when the step does not divide the height
// the top (often widest, e.g. "100") label was never measured and got clipped.
func yStepFor(graphH int) int {
	if graphH <= 1 {
		return 1
	}
	best, bestScore := graphH, 1<<30 // fall back to labelling just the two ends
	for d := 1; d <= graphH; d++ {
		if graphH%d != 0 {
			continue
		}
		if labels := graphH / d; labels >= 2 && labels <= 6 {
			if score := abs(labels - 4); score < bestScore {
				best, bestScore = d, score
			}
		}
	}
	return best
}

func abs(v int) int {
	if v < 0 {
		return -v
	}
	return v
}
