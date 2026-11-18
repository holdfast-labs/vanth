package monitor

import (
	"fmt"
	"image/color"
	"math"
	"strconv"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

// panelBorderHex is the muted border of every box (LEET's layout grey).
const panelBorderHex = "#444444"

func titleColor(dark bool) color.Color {
	if dark {
		return lipgloss.Color("#bcbcbc")
	}
	return lipgloss.Color("#3a3a3a")
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
	frame := m.renderLeet()
	if m.help {
		if out, ok := m.helpDialog(frame); ok {
			return out
		}
		// Too small for the dialog: the help replaces the frame.
		return frameBlock(m.renderHelp(rect{w: m.width, h: m.height}), m.width, m.height)
	}
	return frame
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

// jobProgressPercent returns the latest normalized progress percent for a job,
// or -1 when no progress series exists.
func (m Model) jobProgressPercent(jobID string) float64 {
	pts := m.points[SeriesKey{JobID: jobID, Metric: "progress.percent"}]
	if len(pts) == 0 {
		return -1
	}
	return pts[len(pts)-1].Y
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
		hint: "pgup/pgdn scroll · esc close",
	})
}

func (m Model) renderHelp(r rect) string {
	if r.w < 20 || r.h < 8 {
		return "help needs a larger window"
	}
	lines := []string{
		"↑/↓ or k/j        select run",
		"tab / shift+tab   focus next / previous chart",
		"←/→               previous / next page of charts",
		"+/- · wheel       zoom the chart's time window",
		"t                 reset zoom (live window)",
		"[ or ]            sidebar / compact run list",
		"enter             pin run (keep it on the charts)",
		"e / l / s         events / logs / slowest runs",
		"pgup/pgdn         scroll the open pane",
		"esc               close the pane",
		"click             select run · focus chart",
		"?                 toggle this help",
		"q or ctrl+c       quit",
	}
	title := lipgloss.NewStyle().Bold(true).Foreground(titleColor(m.dark)).Render("vanth monitor · keys")
	return m.panelBox(r, title, lipgloss.JoinVertical(lipgloss.Top, lines...))
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
	// Too wide: keep what fits in width-1 cells and mark the cut with "…".
	r := []rune(s)
	w := 0
	for i, ru := range r {
		if w+runeWidth(ru) > width-1 {
			return string(r[:i]) + "…"
		}
		w += runeWidth(ru)
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
