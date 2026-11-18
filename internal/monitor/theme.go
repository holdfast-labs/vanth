package monitor

import (
	"image/color"
	"strings"

	"bloomfx.dev/bloom"
	"bloomfx.dev/bloom/fx"
	"charm.land/lipgloss/v2"
)

// Accent colors for the redesigned chrome. Border colors stay muted until a
// pane has keyboard focus, then switch to the accent so the active region is
// obvious even on a busy dashboard.
const (
	focusBorderHex  = "#7d56f4"
	gradientFromHex = "#7d56f4"
	gradientToHex   = "#2bd9c5"
)

// gradientText colors s rune-by-rune along a two-stop gradient. It only adds
// foreground color, so it degrades to plain text on no-color profiles.
func gradientText(s string, bold bool) string {
	rs := []rune(s)
	if len(rs) == 0 {
		return ""
	}
	cols := lipgloss.Blend1D(len(rs), lipgloss.Color(gradientFromHex), lipgloss.Color(gradientToHex))
	var b strings.Builder
	for i, r := range rs {
		st := lipgloss.NewStyle().Bold(bold)
		if i < len(cols) {
			st = st.Foreground(cols[i])
		}
		b.WriteString(st.Render(string(r)))
	}
	return b.String()
}

// panelOpts carries the optional decorations of a panel.
type panelOpts struct {
	hint    string  // right-aligned text on the top border
	focused bool    // accent border
	alert   bool    // failure-red border (wins over focus)
	pulse   float64 // heartbeat phase in (0,1]; 0 disables the border effect

	// Optional dim captions on the bottom border (e.g. time-axis ends).
	bottomL, bottomR string
}

// panelBox renders a rounded, titled border around content, exactly r.w x r.h
// terminal cells.
func (m Model) panelBox(r rect, title string, content string) string {
	return m.panelBoxOpts(r, title, content, panelOpts{})
}

// panelBoxOpts draws the panel with Bloom's Box/Edge. Bloom is told the exact
// size, and if its output ever disagrees (it must not) the hand-rolled border
// below is used, so a panel can never break the layout grid.
func (m Model) panelBoxOpts(r rect, title, content string, o panelOpts) string {
	if r.w < 2 || r.h < 2 {
		return ""
	}
	hex := panelBorderHex
	if o.focused {
		hex = focusBorderHex
	}
	if o.alert {
		hex = statusHex("failed", m.dark)
	}
	border := lipgloss.Color(hex)
	box := bloom.Box(
		lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(border),
		bloom.Text(content),
	)
	if r.w >= 6 && title != "" {
		box = box.Edge(bloom.Top, bloom.Start, bloom.Text(" "+title+" ").Truncate())
	}
	if r.w >= 24 && o.hint != "" {
		box = box.Edge(bloom.Top, bloom.End, bloom.Text(" "+o.hint+" ").Truncate())
	}
	if r.w >= 20 && o.bottomL != "" {
		box = box.Edge(bloom.Bottom, bloom.Start, bloom.Text(dimStyle.Render(" "+o.bottomL+" ")).Truncate())
	}
	if r.w >= 20 && o.bottomR != "" {
		box = box.Edge(bloom.Bottom, bloom.End, bloom.Text(dimStyle.Render(" "+o.bottomR+" ")).Truncate())
	}
	if o.pulse > 0 {
		box = box.BorderFx(o.pulse, fx.Pulse{Color: lipgloss.Color(gradientToHex)})
	}
	out := bloom.Render(r.w, r.h, box)
	if lipgloss.Width(out) == r.w && lipgloss.Height(out) == r.h {
		return out
	}
	return m.legacyPanelBox(r, title, content)
}

// legacyPanelBox is the pre-Bloom border, kept as the size-safe fallback.
func (m Model) legacyPanelBox(r rect, title string, content string) string {
	innerW := r.w - 2
	lines := []string{boxTopLine(r.w, title)}
	cl := strings.Split(content, "\n")
	for i := 0; i < r.h-2; i++ {
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

// pulsePhase returns the heartbeat phase for the jobs border: it advances once
// per data refresh (the refresh loop already runs while jobs are live), so no
// timer of its own is needed. Zero means "no effect".
func (m Model) pulsePhase(running int) float64 {
	if running == 0 || m.staleSince != nil || m.beat == 0 {
		return 0
	}
	return float64(m.beat%5+1) / 6
}

// glyphs and labels per status. Each status has a distinct shape as well as a
// color so the list stays readable on 16-color and no-color terminals.
func statusIcon(status string) string {
	switch status {
	case "running":
		return "●"
	case "queued", "pending":
		return "◌"
	case "completed":
		return "✔"
	case "failed":
		return "✘"
	case "cancelled":
		return "⊘"
	case "orphaned":
		return "▲"
	case "timeout":
		return "◷"
	default:
		return "?"
	}
}

func statusHex(status string, dark bool) string {
	pick := func(light, d string) string {
		if dark {
			return d
		}
		return light
	}
	switch status {
	case "running":
		return pick("#008300", "#2fbf4f")
	case "queued", "pending":
		return pick("#6b7280", "#9ca3af")
	case "completed":
		return pick("#2a78d6", "#3987e5")
	case "failed":
		return pick("#e34948", "#e66767")
	case "cancelled":
		return pick("#b57f00", "#e0a526")
	case "orphaned":
		return pick("#eb6834", "#f07a47")
	case "timeout":
		return pick("#c2410c", "#f2884b")
	default:
		return "#8b8b8b"
	}
}

func statusColor(status string, dark bool) color.Color {
	return lipgloss.Color(statusHex(status, dark))
}

// levelStyle colors an event level so errors and warnings stand out.
func levelStyle(level string, dark bool) lipgloss.Style {
	switch strings.ToLower(level) {
	case "error", "fatal", "critical":
		return lipgloss.NewStyle().Bold(true).Foreground(statusColor("failed", dark))
	case "warn", "warning":
		return lipgloss.NewStyle().Foreground(statusColor("cancelled", dark))
	case "debug":
		return lipgloss.NewStyle().Faint(true)
	}
	return lipgloss.NewStyle()
}
