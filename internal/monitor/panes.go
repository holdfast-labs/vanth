package monitor

import (
	"bloomfx.dev/bloom"
	"bloomfx.dev/bloom/fx"
	"charm.land/lipgloss/v2"
)

var (
	dimStyle  = lipgloss.NewStyle().Faint(true)
	boldStyle = lipgloss.NewStyle().Bold(true)
)

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
	const lines = 12
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
