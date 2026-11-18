package monitor

import (
	"fmt"
	"strings"
	"time"

	"bloomfx.dev/bloom"
	"charm.land/lipgloss/v2"
)

// bigDigits is a 3-row heavy box-drawing digit font for the stat tiles.
var bigDigits = [10][3]string{
	{"┏━┓", "┃ ┃", "┗━┛"},
	{"━┓ ", " ┃ ", "━┻━"},
	{"━━┓", "┏━┛", "┗━━"},
	{"━━┓", " ━┫", "━━┛"},
	{"┃ ┃", "┗━┫", "  ┃"},
	{"┏━━", "┗━┓", "━━┛"},
	{"┏━━", "┣━┓", "┗━┛"},
	{"━━┓", "  ┃", "  ┃"},
	{"┏━┓", "┣━┫", "┗━┛"},
	{"┏━┓", "┗━┫", "━━┛"},
}

// bigNumber renders n as three rows of heavy digits, or returns nil when the
// width does not fit (callers then fall back to a plain bold number).
func bigNumber(n, width int) []string {
	s := fmt.Sprint(max(n, 0))
	if len(s)*4-1 > width {
		return nil
	}
	rows := make([]string, 3)
	for i, c := range s {
		d := bigDigits[c-'0']
		for r := range rows {
			if i > 0 {
				rows[r] += " "
			}
			rows[r] += d[r]
		}
	}
	return rows
}

// tile is one stat tile: a Bloom Box in the status color with the label on its
// border and the count as big digits. Zero counts are dimmed (gray border).
func (m Model) tile(glyph, label string, n int, hex string, innerW int, corner string) bloom.Child {
	col := lipgloss.Color(hex)
	border := col
	numStyle := lipgloss.NewStyle().Bold(true).Foreground(col)
	if n == 0 {
		border = lipgloss.Color("#555555")
		numStyle = lipgloss.NewStyle().Faint(true)
	}
	rows := bigNumber(n, innerW)
	if rows == nil {
		rows = []string{"", fmt.Sprint(n), ""}
	}
	for i := range rows {
		rows[i] = numStyle.Render(rows[i])
	}
	box := bloom.Box(
		lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(border).Padding(0, 1),
		bloom.Text(strings.Join(rows, "\n")),
	)
	if short, ok := tileShort[label]; ok && lipgloss.Width(label)+4 > innerW {
		label = short // narrow tile: a shorter word rather than a cut one
	}
	lab := lipgloss.NewStyle().Bold(n > 0).Foreground(border).Render(" " + glyph + " " + label + " ")
	box = box.Edge(bloom.Top, bloom.Start, bloom.Text(lab).Truncate())
	if corner != "" {
		box = box.Edge(bloom.Bottom, bloom.End, bloom.Text(dimStyle.Render(" "+corner+" ")).Truncate())
	}
	return box
}

var tileShort = map[string]string{"completed": "done", "cancelled": "cancel", "attention": "attn", "orphaned": "orphan"}

// renderHeader draws the stat tiles (roomy terminals) or the compact strip.
func (m Model) renderHeader(r rect) string {
	if r.h >= tilesH {
		if out := m.renderTiles(r); out != "" {
			return out
		}
	}
	return m.renderSummaryHeader(r)
}

func (m Model) renderTiles(r rect) string {
	s := m.summary()
	type spec struct {
		status, label string
		n             int
	}
	specs := []spec{{"running", "running", s.running}, {"queued", "queued", s.queued},
		{"completed", "completed", s.completed}, {"failed", "failed", s.failed}}
	if s.cancelled > 0 {
		specs = append(specs, spec{"cancelled", "cancelled", s.cancelled})
	}
	specs = append(specs, spec{"orphaned", "orphaned", s.orphaned})
	count := len(specs) + 1
	const gap = 1
	inner := (r.w-gap*(count-1))/count - 4 // border (2) + padding (2)
	var kids []bloom.Child
	for _, sp := range specs {
		kids = append(kids, bloom.Fill(m.tile(statusIcon(sp.status), sp.label, sp.n, statusHex(sp.status, m.dark), inner, "")))
	}
	attn := m.attentionCount()
	corner := ""
	for _, c := range []string{
		fmt.Sprintf("%d jobs · %s ev", len(m.jobs), compactFmt(float64(s.events))),
		fmt.Sprintf("%d jobs", len(m.jobs)), fmt.Sprintf("%dj", len(m.jobs)),
	} {
		if lipgloss.Width(c)+2 <= inner {
			corner = c
			break
		}
	}
	kids = append(kids, bloom.Fill(m.tile("⚑", "attention", attn, "#e34948", inner, corner)))
	out := bloom.Render(r.w, r.h, bloom.Row(kids...).Gap(gap))
	if lipgloss.Width(out) != r.w || lipgloss.Height(out) != r.h {
		return ""
	}
	return out
}

const sparkRunes = "▁▂▃▄▅▆▇█"

// sparkline maps the last width values onto block characters.
func sparkline(vals []float64, width int) string {
	if len(vals) == 0 || width <= 0 {
		return ""
	}
	if len(vals) > width {
		vals = vals[len(vals)-width:]
	}
	lo, hi := vals[0], vals[0]
	for _, v := range vals {
		lo, hi = min(lo, v), max(hi, v)
	}
	rs := []rune(sparkRunes)
	var b strings.Builder
	for _, v := range vals {
		i := 3
		if hi > lo {
			i = int((v - lo) / (hi - lo) * float64(len(rs)-1))
		}
		b.WriteRune(rs[i])
	}
	return b.String()
}

// seriesValues returns up to the last n Y values of a series.
func (m Model) seriesValues(jobID, metric string, n int) []float64 {
	pts := m.points[SeriesKey{JobID: jobID, Metric: metric}]
	if len(pts) > n {
		pts = pts[len(pts)-n:]
	}
	out := make([]float64, len(pts))
	for i, p := range pts {
		out[i] = p.Y
	}
	return out
}

// jobSparkMetric picks the first non-progress metric the job has data for.
func (m Model) jobSparkMetric(jobID string) string {
	for _, k := range m.metrics {
		if !strings.HasPrefix(k, "progress.") && len(m.points[SeriesKey{JobID: jobID, Metric: k}]) > 1 {
			return k
		}
	}
	return ""
}

// levelGlyph is a colored one-cell marker for an event level.
func (m Model) levelGlyph(level string) string {
	switch strings.ToLower(level) {
	case "error", "fatal", "critical":
		return levelStyle(level, m.dark).Render("✘")
	case "warn", "warning":
		return levelStyle(level, m.dark).Render("▲")
	case "debug":
		return dimStyle.Render("·")
	}
	return lipgloss.NewStyle().Foreground(statusColor("completed", m.dark)).Render("●")
}

// renderLower is the default lower area: an event ticker beside a jobs
// timeline. The full tables stay on e / l / s.
func (m Model) renderLower(r rect) string {
	if r.h <= 0 || r.w <= 0 {
		return ""
	}
	switch {
	case m.showEvents:
		return m.renderEvents(r)
	case m.showLogs:
		return m.renderLogs(r)
	case m.showSlowest:
		return m.renderSlowest(r)
	}
	if r.w < 70 || m.fleetMode() {
		// With no per-job charts the timeline lives in the plots area.
		return m.renderTicker(r)
	}
	tw := r.w * 58 / 100
	return lipgloss.JoinHorizontal(lipgloss.Top,
		m.renderTicker(rect{w: tw, h: r.h}), " ", m.renderTimeline(rect{w: r.w - tw - 1, h: r.h}))
}

// renderTicker shows the newest events of the selected job, one per row.
func (m Model) renderTicker(r rect) string {
	id := m.selectedJobID()
	events := m.events[id]
	title := boldStyle.Foreground(titleColor(m.dark)).Render("Events")
	innerW, rows := r.w-2, r.h-2
	if innerW < 1 || rows < 1 {
		return m.panelBox(r, title, "")
	}
	var lines []string
	if len(events) == 0 {
		lines = append(lines, dimStyle.Render("no events yet"))
	}
	from := max(len(events)-rows, 0)
	typeW := 6
	for _, ev := range events[from:] {
		typeW = max(typeW, min(lipgloss.Width(ev.Type), 24))
	}
	typeW = min(typeW, max(innerW/3, 6))
	for _, ev := range events[from:] {
		ts := FormatTimestamp(ev.CreatedAt)
		if len(ts) > 8 {
			ts = ts[:8] // HH:MM:SS; the table (e) has milliseconds
		}
		msg := ev.Message
		if msg == "" {
			msg = CompactJSON(roundFloats(ev.Data).(map[string]any))
		}
		lines = append(lines, fmt.Sprintf("%s %s %s %s", dimStyle.Render(ts), m.levelGlyph(ev.Level),
			boldStyle.Render(truncate(ev.Type, typeW))+strings.Repeat(" ", max(typeW-lipgloss.Width(truncate(ev.Type, typeW)), 0)), truncate(msg, max(innerW-14-typeW, 1))))
	}
	for i := range lines {
		lines[i] = fillLine(lines[i], innerW)
	}
	return m.panelBoxOpts(r, title, strings.Join(lines, "\n"), panelOpts{
		hint: fmt.Sprintf("%d · e table · l logs · s slowest", len(events)), focused: m.focus == focusLower})
}

// jobStart returns a job's first known timestamp.
func jobStart(j JobSummary) (time.Time, bool) {
	for _, ts := range []string{j.StartedAt, j.CreatedAt} {
		if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// severity ranks statuses for a timeline bucket's color: the worst one wins.
func severity(status string) int {
	switch status {
	case "failed":
		return 5
	case "orphaned":
		return 4
	case "cancelled":
		return 3
	case "running":
		return 2
	case "queued", "pending":
		return 1
	}
	return 0
}

// formatSpan renders a duration coarsely: "45m", "6h", "12d".
func formatSpan(d time.Duration) string {
	switch {
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
}
