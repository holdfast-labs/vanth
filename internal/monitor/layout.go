package monitor

// rect is a screen rectangle in terminal cells.
type rect struct {
	x, y, w, h int
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// lowerExpanded reports whether a pane (events table, logs or slowest runs)
// replaces the charts.
func (m Model) lowerExpanded() bool {
	return m.showEvents || m.showLogs || m.showSlowest
}
