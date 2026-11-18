package monitor

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"bloomfx.dev/bloom"
	"bloomfx.dev/bloom/fx"
	"charm.land/lipgloss/v2"
)

var (
	dimStyle  = lipgloss.NewStyle().Faint(true)
	boldStyle = lipgloss.NewStyle().Bold(true)
)

// divider is a dim labelled rule, "── label ──────", exactly width cells wide.
func divider(label string, width int) string {
	if width < 4 {
		return strings.Repeat("─", max(width, 0))
	}
	head := "── " + label + " "
	if w := lipgloss.Width(head); w < width {
		head += strings.Repeat("─", width-w)
	}
	return dimStyle.Render(truncate(head, width))
}

// statusChip is "glyph status" in the status color, bold.
func (m Model) statusChip(status string) string {
	return lipgloss.NewStyle().Bold(true).Foreground(statusColor(status, m.dark)).
		Render(statusIcon(status) + " " + status)
}

// trendArrow compares the last two points of a series.
func trendArrow(pts []Point) string {
	if len(pts) < 2 {
		return " "
	}
	switch a, b := pts[len(pts)-2].Y, pts[len(pts)-1].Y; {
	case b > a:
		return "▲"
	case b < a:
		return "▼"
	}
	return "="
}

// renderMetrics renders the key-metrics panel for the selected job: status
// chip + name, a status-tinted progress bar, run facts, then a table of the
// latest value of every metric with a trend arrow, then pinned comparisons.
func (m Model) renderMetrics(r rect) string {
	title := boldStyle.Foreground(titleColor(m.dark)).Render("Metrics")
	if r.w < 4 || r.h < 4 {
		return m.panelBox(r, title, "")
	}
	hint := ""
	if j, ok := m.selectedJob(); ok {
		hint = truncate(j.JobID, 16)
	}
	return m.panelBoxOpts(r, title, strings.Join(m.metricsLines(r.w-2, r.h-2), "\n"), panelOpts{hint: hint})
}

// metricsHeight is the pane height (border included) that fits its content, so
// the jobs list can have every spare row.
func (m Model) metricsHeight(innerW int) int {
	return len(m.metricsLines(innerW, 1<<20)) + 2
}

// metricsLines builds up to contentH rows of the key-metrics pane.
func (m Model) metricsLines(innerW, contentH int) []string {
	var lines []string
	add := func(l string) {
		if len(lines) < contentH {
			lines = append(lines, fillLine(l, innerW))
		}
	}
	job, ok := m.selectedJob()
	if !ok {
		add(dimStyle.Render("(no job selected)"))
		return lines
	}
	chip := m.statusChip(job.Status)
	add(chip + "  " + boldStyle.Render(truncate(job.DisplayName(), max(innerW-lipgloss.Width(chip)-2, 1))))

	if pct := m.jobProgressPercent(job.JobID); pct >= 0 {
		add(m.styledBar(job.Status, pct, max(innerW-5, 1), false) + fmt.Sprintf(" %3.0f%%", pct))
	}

	// Key/value cells: bold value, dim label.
	facts := boldStyle.Render(fmt.Sprint(job.EventCount)) + dimStyle.Render(" events")
	if job.ExitCode != nil {
		st := "completed"
		if *job.ExitCode != 0 {
			st = "failed"
		}
		facts += "   " + lipgloss.NewStyle().Bold(true).Foreground(statusColor(st, m.dark)).Render(fmt.Sprint(*job.ExitCode)) + dimStyle.Render(" exit")
	}
	if len(job.Tags) > 0 {
		facts += "   " + dimStyle.Render(truncate(strings.Join(job.Tags, ","), max(innerW/3, 4)))
	}
	add(facts)

	// One row per metric: name, value, trend, and a sparkline of its history.
	nameW := min(16, max(innerW/3, 6))
	sparkW := innerW - nameW - 13
	sc := lipgloss.NewStyle().Foreground(statusColor(job.Status, m.dark))
	var rows []string
	for _, metric := range m.metrics {
		pts := m.points[SeriesKey{JobID: job.JobID, Metric: metric}]
		if len(pts) == 0 {
			continue
		}
		last := pts[len(pts)-1]
		row := fmt.Sprintf("%s %s %s", dimStyle.Render(fmt.Sprintf("%-*s", nameW, truncate(metric, nameW))),
			boldStyle.Render(fmt.Sprintf("%8s", compactFmt(last.Y))), dimStyle.Render(trendArrow(pts)))
		if sparkW >= 4 {
			row += " " + sc.Render(sparkline(m.seriesValues(job.JobID, metric, sparkW), sparkW))
		}
		rows = append(rows, row)
	}
	for _, row := range rows {
		add(row)
	}
	// Flat series (e.g. progress.total) are values, not charts.
	names := make([]string, 0, len(m.constants))
	for k := range m.constants {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		if pts := m.points[SeriesKey{JobID: job.JobID, Metric: k}]; len(pts) > 0 {
			add(fmt.Sprintf("%s %s %s", dimStyle.Render(fmt.Sprintf("%-*s", nameW, truncate(k, nameW))),
				boldStyle.Render(fmt.Sprintf("%8s", compactFmt(pts[len(pts)-1].Y))), dimStyle.Render("= constant")))
		}
	}
	// Run details only where space is left over.
	if job.Notes != "" {
		add(dimStyle.Render(truncate("notes: "+job.Notes, innerW)))
	}
	for _, row := range parseRunOverview(job.RunJSON) {
		add(dimStyle.Render(truncate(row, innerW)))
	}

	if len(m.pinned) > 0 && len(lines) < contentH-1 {
		ids := make([]string, 0, len(m.pinned))
		for id := range m.pinned {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		add(divider("pinned", innerW))
		for _, id := range ids {
			j, ok := m.jobByID[id]
			if !ok {
				continue
			}
			pj := m.jobs[j]
			val := ""
			if pct := m.jobProgressPercent(id); pct >= 0 {
				val = fmt.Sprintf(" %3.0f%%", pct)
			}
			glyph := lipgloss.NewStyle().Foreground(statusColor(pj.Status, m.dark)).Render(statusIcon(pj.Status))
			add("P " + glyph + " " + truncate(pj.DisplayName(), max(innerW-9, 1)) + dimStyle.Render(val))
		}
	}
	return lines
}

// chartLegend is the swatch row shown inside a chart box: one colored dot per
// drawn job, a dim "+N hidden" fold count first, and "+k more" if the row runs
// out of width. Entries are dropped whole, never cut mid-name.
func (m Model) chartLegend(draw []drawJob, hidden, width int) string {
	var parts []string
	if hidden > 0 {
		parts = append(parts, dimStyle.Render(fmt.Sprintf("+%d hidden", hidden)))
	}
	order := m.legendOrder(draw)
	used := 0
	for _, p := range parts {
		used += lipgloss.Width(p) + 1
	}
	for i, j := range order {
		entry := lipgloss.NewStyle().Foreground(slotColor(j.slot, m.dark)).Render("●") + " " + truncate(j.job.DisplayName(), 14)
		moreFn := func(n int) string { return dimStyle.Render(fmt.Sprintf("+%d more", n)) }
		reserve := 0
		if left := len(order) - i - 1; left > 0 {
			reserve = lipgloss.Width(moreFn(left)) + 2
		}
		if used+lipgloss.Width(entry)+reserve > width {
			parts = append(parts, moreFn(len(order)-i))
			break
		}
		used += lipgloss.Width(entry) + 2
		parts = append(parts, entry)
	}
	return fillLine(strings.Join(parts, "  "), width)
}

// renderStatusBar renders a rule over a full-width status strip. The left chip
// states data freshness (LIVE / STALE / FALLBACK); stale turns the whole strip
// amber so it cannot be missed. Key hints sit on the right and drop out when
// the width runs short.
func (m Model) renderStatusBar(lay layout) string {
	w := lay.lower.w
	fg, bg := statusBarColors(m.dark)
	base := lipgloss.NewStyle().Foreground(fg).Background(bg)
	stale := m.staleSince != nil
	if stale {
		bg = lipgloss.Color("#b45309")
		fg = lipgloss.Color("#ffffff")
		base = lipgloss.NewStyle().Foreground(fg).Background(bg)
	}

	var chip string
	switch {
	case stale:
		chip = lipgloss.NewStyle().Bold(true).Reverse(true).Render(" ◌ stale · data " + formatAge(time.Since(*m.staleSince)) + " old ")
	case m.mode == ModeFallback:
		chip = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#1a1a1a")).Background(lipgloss.Color("#e0a526")).Render(" ◐ fallback ")
	default:
		chip = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#0b2a12")).Background(lipgloss.Color("#2fbf4f")).Render(" ● live ")
	}

	facts := []string{
		fmt.Sprintf("jobs %d", len(m.jobs)),
		"sel " + truncate(m.selectedJobID(), 24),
		"mode " + m.mode,
		"refresh " + refreshAge(m.lastRefresh) + " ago",
	}
	if m.warnings > 0 {
		facts = append(facts, lipgloss.NewStyle().Foreground(statusColor("cancelled", true)).Render(fmt.Sprintf("warn %d", m.warnings)))
	}
	if m.malformed > 0 {
		facts = append(facts, lipgloss.NewStyle().Bold(true).Foreground(statusColor("failed", true)).Render(fmt.Sprintf("malformed %d", m.malformed)))
	}
	left := chip + base.Render(" "+strings.Join(facts, " · ")+" ")

	key := func(k, d string) string {
		return boldStyle.Render(k) + " " + d
	}
	hints := []string{key("q", "quit"), key("?", "help"), key("tab", "focus"), key("e/l/s", "panes")}
	brand := gradientText("vanth", true) + " "
	right := ""
	for n := len(hints); n > 0; n-- {
		cand := strings.Join(hints[:n], "  ") + " "
		if n == len(hints) {
			cand += " " + brand
		}
		if lipgloss.Width(left)+lipgloss.Width(cand)+1 <= w {
			right = cand
			break
		}
	}
	gap := max(w-lipgloss.Width(left)-lipgloss.Width(right), 0)
	// The tinted strip ends after the facts; the hints sit on the plain
	// terminal background so no key letter lands on the strip.
	line := lipgloss.NewStyle().MaxWidth(w).Render(left + strings.Repeat(" ", gap) + right)
	ruleHex := panelBorderHex
	if stale {
		ruleHex = "#b45309"
	}
	rule := lipgloss.NewStyle().Foreground(lipgloss.Color(ruleHex)).Render(strings.Repeat("─", w))
	return rule + "\n" + fillLine(line, w)
}

// loadingPane draws Bloom skeleton bars (static: no shimmer timer) while the
// first job list loads.
func loadingSkeleton(w, h int) string {
	if w < 8 || h < 1 {
		return ""
	}
	lines := make([]int, 0, h)
	for i := 0; i < h; i++ {
		switch i % 3 {
		case 0:
			lines = append(lines, max(w*2/3, 1))
		case 1:
			lines = append(lines, max(w/3, 1))
		default:
			lines = append(lines, 0)
		}
	}
	return bloom.Render(w, h, bloom.Skeleton(lines...))
}

// helpDialog overlays the key map on the dashboard. The base frame is dimmed by
// Bloom; if the terminal is too small for a dialog the caller falls back to
// the lower-pane help.
func (m Model) helpDialog(frame string) (string, bool) {
	const lines = 15
	w, h := min(m.width-4, 56), lines+2
	if w < 40 || m.height < h+2 {
		return "", false
	}
	dialog := m.renderHelp(rect{w: w, h: h})
	out := bloom.Render(m.width, m.height, bloom.Overlay(bloom.Text(frame),
		bloom.Layer(bloom.Text(dialog)).Modal().Backdrop(fx.Dim{Amount: 0.6}).Shadow(1, 1)))
	if lipgloss.Height(out) != m.height {
		return "", false
	}
	return out, true
}
