package monitor

import (
	"encoding/json"
	"fmt"
	"image/color"
	"math"
	"strconv"
	"strings"
	"time"

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
	headerPane := m.renderSummaryHeader(lay.header)
	if m.help {
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
		top = lipgloss.JoinHorizontal(lipgloss.Top, jobsPane, " ", metricsPane, " ", chartsPane)
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
	return strings.Join(parts, "\n")
}

// panelBox renders a rounded, titled border around content, exactly r.w×r.h
// terminal cells. The title is embedded in the top border (header style);
// content is truncated and padded to the interior width (r.w-2).
func (m Model) panelBox(r rect, title string, content string) string {
	if r.w < 2 || r.h < 2 {
		return ""
	}
	innerW := r.w - 2
	lines := []string{boxTopLine(r.w, title)}
	contentH := r.h - 2
	cl := strings.Split(content, "\n")
	for i := 0; i < contentH; i++ {
		l := ""
		if i < len(cl) {
			l = cl[i]
		}
		lines = append(lines, lipgloss.NewStyle().Foreground(lipgloss.Color(panelBorderHex)).
			Render("│"+fillLine(l, innerW)+"│"))
	}
	lines = append(lines, boxBottomLine(r.w))
	return strings.Join(lines, "\n")
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

// renderJobs renders the job list pane as a titled, bordered box. Row 0 of the
// box is the top border carrying the title, so job blocks start at content row
// 0 (rect row 1) — matching the mouse hit-test in handleMouseClick, which
// divides the content row by jobBlockHeight.
func (m Model) renderJobs(r rect) string {
	title := "Jobs"
	if len(m.jobs) > 0 {
		title = fmt.Sprintf("Jobs (%d)", len(m.jobs))
	}
	innerW := r.w - 2
	contentH := r.h - 2
	var lines []string
	for i := range m.jobs {
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
	return m.panelBox(r, styledTitle, strings.Join(lines, "\n"))
}

// jobBlock renders the jobBlockHeight content lines for one job: a header line
// with cursor/icon/pin/name and right-aligned status, a progress-bar line, and
// a metadata line (events, tags, exit code). Every line is exactly innerW cells
// wide so the selected block's amber highlight spans the full column.
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

	status := j.Status
	if sw := lipgloss.Width(status); sw > innerW-2 {
		status = truncate(status, innerW-2)
	}
	statusW := lipgloss.Width(status)
	nameArea := innerW - statusW - 1
	if nameArea < 1 {
		nameArea = 1
	}
	row1 := fillLine(prefix+truncate(j.DisplayName(), nameArea-lipgloss.Width(prefix)), nameArea) + " " + status

	// Progress bar line: 2-cell indent, bar, gap, then "NN%".
	pct := m.jobProgressPercent(j.JobID)
	row2 := ""
	if pct >= 0 {
		barW := innerW - 2 - 5 // indent (2) + gap (1) + "100%"
		if barW < 1 {
			barW = 1
		}
		row2 = fillLine("  "+progressBar(pct, barW)+" "+fmt.Sprintf("%.0f%%", pct), innerW)
	} else {
		row2 = fillLine("", innerW)
	}

	meta := fmt.Sprintf("evt %d", j.EventCount)
	if len(j.Tags) > 0 {
		meta += " · tags " + strings.Join(j.Tags, ",")
	}
	if j.ExitCode != nil {
		meta += fmt.Sprintf(" · exit %d", *j.ExitCode)
	}
	row3 := fillLine("  "+truncate(meta, max(innerW-2, 0)), innerW)

	out := []string{row1, row2, row3}
	if sel {
		amber := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color(selectedFgHex)).
			Background(lipgloss.Color(selectedBgHex))
		for i := range out {
			out[i] = amber.Render(out[i])
		}
	} else {
		accent := lipgloss.NewStyle().Foreground(statusColor(j.Status, m.dark))
		out[0] = accent.Render(out[0])
		faint := lipgloss.NewStyle().Faint(true)
		out[1] = faint.Render(out[1])
		out[2] = faint.Render(out[2])
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
	running, completed, failed, cancelled, orphaned, other int
	events, pendingDeliveries                              int64
}

func (m Model) summary() summaryCounts {
	var s summaryCounts
	for _, j := range m.jobs {
		s.events += j.EventCount
		switch j.Status {
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
	title := lipgloss.NewStyle().Bold(true).Foreground(titleColor(m.dark)).Render("Vanth monitor")

	var badges []string
	badges = append(badges, statusBadge("running", s.running, "#008300"))
	badges = append(badges, statusBadge("completed", s.completed, "#2a78d6"))
	badges = append(badges, statusBadge("failed", s.failed, "#e34948"))
	badges = append(badges, statusBadge("cancelled", s.cancelled, "#eda100"))
	badges = append(badges, statusBadge("orphaned", s.orphaned, "#eb6834"))
	if s.other > 0 {
		badges = append(badges, lipgloss.NewStyle().Foreground(lipgloss.Color("#8b8b8b")).Render(fmt.Sprintf("other %d", s.other)))
	}
	countLine := strings.Join(badges, "  ")

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

	// Row 3: volume totals + pinned comparisons + longest-running job.
	totals := []string{fmt.Sprintf("jobs %d", len(m.jobs)), fmt.Sprintf("events %d", s.events)}
	if n := len(m.pinned); n > 0 {
		totals = append(totals, fmt.Sprintf("pinned %d", n))
	}
	if age := m.oldestRunningAge(); age > 0 {
		totals = append(totals, "oldest running "+formatAge(age))
	}
	totalsLine := strings.Join(totals, " · ")

	// Row 4: data source, refresh age, and durability/health flags.
	mode := lipgloss.NewStyle().Faint(true).Render("mode " + m.mode)
	refresh := lipgloss.NewStyle().Faint(true).Render("refresh " + refreshAge(m.lastRefresh))
	meta := []string{mode, refresh}
	if m.staleSince != nil {
		meta = append(meta, lipgloss.NewStyle().Foreground(lipgloss.Color("#eda100")).Render("stale"))
	}
	if m.malformed > 0 {
		meta = append(meta, lipgloss.NewStyle().Foreground(lipgloss.Color("#e34948")).Render(fmt.Sprintf("malformed %d", m.malformed)))
	}
	if m.warnings > 0 {
		meta = append(meta, lipgloss.NewStyle().Foreground(lipgloss.Color("#8b8b8b")).Render(fmt.Sprintf("warn %d", m.warnings)))
	}
	metaLine := strings.Join(meta, "  ·  ")

	content := lipgloss.JoinVertical(lipgloss.Top, countLine, attnLine, totalsLine, metaLine)
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

func statusBadge(label string, count int, hex string) string {
	if count == 0 {
		return lipgloss.NewStyle().Faint(true).Render(fmt.Sprintf("%s 0", label))
	}
	return lipgloss.NewStyle().Foreground(lipgloss.Color(hex)).Render(fmt.Sprintf("%s %d", label, count))
}

// renderMetrics renders the key-metrics panel for the selected job: a progress
// bar, durable event/delivery counts, and the latest value of every visible
// metric series (LEET run-overview style).
func (m Model) renderMetrics(r rect) string {
	title := lipgloss.NewStyle().Bold(true).Foreground(titleColor(m.dark)).Render("Metrics")
	if r.w < 4 || r.h < 4 {
		return m.panelBox(r, title, "")
	}
	innerW := r.w - 2
	contentH := r.h - 2
	var lines []string
	job, ok := m.selectedJob()
	if !ok {
		lines = append(lines, fillLine(lipgloss.NewStyle().Faint(true).Render("(no job selected)"), innerW))
		return m.panelBox(r, title, strings.Join(lines, "\n"))
	}
	name := lipgloss.NewStyle().Bold(true).Foreground(statusColor(job.Status, m.dark)).Render(fillLine(job.DisplayName(), innerW))
	lines = append(lines, name)

	pct := m.jobProgressPercent(job.JobID)
	if pct >= 0 {
		lines = append(lines, fillLine(progressBar(pct, innerW), innerW))
	}

	meta := fmt.Sprintf("events %d", job.EventCount)
	if job.ExitCode != nil {
		meta += fmt.Sprintf(" · exit %d", *job.ExitCode)
	}
	lines = append(lines, fillLine(lipgloss.NewStyle().Faint(true).Render(meta), innerW))

	// Run overview (W&B-style): notes, author@host, runtime, git branch.
	if job.Notes != "" {
		lines = append(lines, fillLine(lipgloss.NewStyle().Foreground(lipgloss.Color("#8b8b8b")).Render(truncate("notes: "+job.Notes, innerW)), innerW))
	}
	if overview := parseRunOverview(job.RunJSON); len(overview) > 0 {
		for _, row := range overview {
			if len(lines) >= contentH {
				break
			}
			lines = append(lines, fillLine(lipgloss.NewStyle().Faint(true).Render(truncate(row, innerW)), innerW))
		}
	}

	if len(job.Tags) > 0 {
		lines = append(lines, fillLine(lipgloss.NewStyle().Faint(true).Render(truncate("tags "+strings.Join(job.Tags, ", "), innerW)), innerW))
	}

	for _, metric := range m.metrics {
		if len(lines) >= contentH {
			break
		}
		pts := m.points[SeriesKey{JobID: job.JobID, Metric: metric}]
		if len(pts) == 0 {
			continue
		}
		last := pts[len(pts)-1]
		value := lipgloss.NewStyle().Bold(true).Render(compactFmt(last.Y))
		row := fmt.Sprintf("%-20s %s", truncate(metric, 20), value)
		if metric == "progress.percent" {
			row += "  " + progressBar(last.Y, max(0, innerW-28))
		}
		lines = append(lines, fillLine(row, innerW))
	}

	// Pinned jobs summary (LEET "pin to top" comparison line).
	var pinnedIDs []string
	for id := range m.pinned {
		pinnedIDs = append(pinnedIDs, id)
	}
	if len(pinnedIDs) > 0 {
		if len(lines) < contentH {
			lines = append(lines, "")
		}
		for _, id := range pinnedIDs {
			if len(lines) >= contentH {
				break
			}
			if j, ok := m.jobByID[id]; ok {
				pj := m.jobs[j]
				pt := m.points[SeriesKey{JobID: id, Metric: "progress.percent"}]
				val := ""
				if len(pt) > 0 {
					val = fmt.Sprintf(" %3.0f%%", pt[len(pt)-1].Y)
				}
				line := fmt.Sprintf("P %s%s", truncate(pj.DisplayName(), innerW-8), val)
				lines = append(lines, fillLine(lipgloss.NewStyle().Faint(true).Render(line), innerW))
			}
		}
	}
	return m.panelBox(r, title, strings.Join(lines, "\n"))
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
		title := lipgloss.NewStyle().Bold(true).Foreground(titleColor(m.dark)).Render("Plots")
		if lay.chartsArea.w < 2 || lay.chartsArea.h < 2 {
			return ""
		}
		innerW := lay.chartsArea.w - 2
		innerH := lay.chartsArea.h - 2
		if innerW < 1 || innerH < 1 {
			return m.panelBox(lay.chartsArea, title, "")
		}
		msg := lipgloss.NewStyle().Faint(true).Render("no metric events yet")
		content := lipgloss.Place(innerW, innerH, lipgloss.Center, lipgloss.Center, msg)
		return m.panelBox(lay.chartsArea, title, content)
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
	return m.panelBox(cr.rect, title, lc.View())
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
	xStep := max(1, w/7)
	yStep := max(1, h/4)
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
	var legend []string
	if len(draw) > 1 {
		if hidden > 0 {
			legend = append(legend, lipgloss.NewStyle().Faint(true).Render(fmt.Sprintf("+%d hidden", hidden)))
		}
		for _, j := range m.legendOrder(draw) {
			c := slotColor(j.slot, m.dark)
			legend = append(legend, lipgloss.NewStyle().Foreground(c).Render("●"+j.job.DisplayName()))
		}
	}
	var readout string
	if p := m.lastVisiblePoint(m.selectedJobID(), metric, minX, maxX); p != nil {
		sel := m.selectedJobID()
		for _, dj := range draw {
			if dj.job.JobID == sel {
				c := slotColor(dj.slot, m.dark)
				readout = lipgloss.NewStyle().Foreground(c).Render(
					dj.job.DisplayName() + ": " + compactFmt(p.Y))
				break
			}
		}
	}
	line := title
	if len(legend) > 0 {
		line += "  [" + strings.Join(legend, " ") + "]"
	}
	if readout != "" {
		line += "  " + readout
	}
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
func (m Model) renderLower(r rect) string {
	if r.h <= 0 || r.w <= 0 {
		return ""
	}
	if m.showEvents {
		return m.renderEvents(r)
	}
	if m.showLogs {
		return m.renderLogs(r)
	}
	if m.showSlowest {
		return m.renderSlowest(r)
	}
	title := lipgloss.NewStyle().Bold(true).Foreground(titleColor(m.dark)).Render("Activity")
	innerW := r.w - 2
	innerH := r.h - 2
	if innerW < 1 || innerH < 1 {
		return m.panelBox(r, title, "")
	}
	// The event table is the default pane, so this neutral state only occurs
	// after 'e' toggled events off; the hint advertises how to bring a pane
	// back (and '?' opens the full key map).
	hint := lipgloss.NewStyle().Faint(true).Render("press e: event table · l: log tail · s: slowest runs · ?: help")
	content := lipgloss.Place(innerW, innerH, lipgloss.Center, lipgloss.Center, hint)
	return m.panelBox(r, title, content)
}

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
	typeW := 10
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
		lvl := truncate(ev.Level, levelW)
		msg := truncate(ev.Message, msgW)
		data := truncate(CompactJSON(ev.Data), dataW)
		out = append(out, fmt.Sprintf("%-*s %s %-*s %-*s %-*s %s",
			timeW, ts, seq, typeW, typ, levelW, lvl, msgW, msg, data))
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
	return m.panelBox(r, styledTitle, strings.Join(out, "\n"))
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

// renderStatusBar renders a separator rule over a full-width status strip.
func (m Model) renderStatusBar(lay layout) string {
	parts := []string{
		fmt.Sprintf("jobs: %d", len(m.jobs)),
		fmt.Sprintf("sel: %s", truncate(m.selectedJobID(), 24)),
		fmt.Sprintf("mode: %s", m.mode),
	}
	if m.staleSince != nil {
		parts = append(parts, "stale")
	}
	if m.mode == ModeFallback {
		parts = append(parts, "fallback")
	}
	if m.warnings > 0 {
		parts = append(parts, fmt.Sprintf("warn: %d", m.warnings))
	}
	if m.malformed > 0 {
		parts = append(parts, fmt.Sprintf("malformed: %d", m.malformed))
	}
	parts = append(parts, fmt.Sprintf("refresh %s ago", refreshAge(m.lastRefresh)))
	parts = append(parts, "q quit · ? help")
	line := strings.Join(parts, " · ")
	line = fillLine(line, lay.lower.w)
	rule := lipgloss.NewStyle().Foreground(lipgloss.Color(panelBorderHex)).
		Render(strings.Repeat("─", lay.lower.w))
	fg, bg := statusBarColors(m.dark)
	bar := lipgloss.NewStyle().Foreground(fg).Background(bg).Render(line)
	return rule + "\n" + bar
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

func statusIcon(status string) string {
	switch status {
	case "running":
		return "●"
	case "completed":
		return "✓"
	case "failed":
		return "✗"
	case "cancelled":
		return "■"
	case "orphaned":
		return "!"
	default:
		return "?"
	}
}

func statusColor(status string, dark bool) color.Color {
	var hex string
	switch status {
	case "running":
		hex = "#008300"
	case "completed":
		hex = "#2a78d6"
		if dark {
			hex = "#3987e5"
		}
	case "failed":
		hex = "#e34948"
		if dark {
			hex = "#e66767"
		}
	case "cancelled":
		hex = "#eda100"
		if dark {
			hex = "#c98500"
		}
	case "orphaned":
		hex = "#eb6834"
		if dark {
			hex = "#d95926"
		}
	default:
		hex = "#8b8b8b"
	}
	return lipgloss.Color(hex)
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
