package monitor

import (
	"image/color"

	"charm.land/lipgloss/v2"
)

// The monitor's palette: a slate base, one two-stop gradient per job (the
// same everywhere the job appears: card, bar, chart line, legend), red for
// failure, amber for a stall and green for success.
var seriesGrad = [...][2]string{
	{"#38BDF8", "#818CF8"}, // sky
	{"#FBBF24", "#FB923C"}, // amber
	{"#34D399", "#2DD4BF"}, // emerald
	{"#C084FC", "#F472B6"}, // violet
	{"#A3E635", "#4ADE80"}, // lime
	{"#E879F9", "#C084FC"}, // fuchsia
	{"#22D3EE", "#60A5FA"}, // cyan
	{"#FB923C", "#FBBF24"}, // orange
	{"#818CF8", "#A78BFA"}, // indigo
	{"#2DD4BF", "#A3E635"}, // teal
}

// seriesMarkers stand in for the series colours when there is no colour:
// chart points, legend and run rows use the job's marker instead.
var seriesMarkers = [...]rune{'*', '+', 'o', 'x', '#', '@', '%', '&', '=', '~'}

const (
	brandHex   = "#38BDF8" // ◆ vanth and the focused chart
	failHex    = "#F43F5E" // failed and stalled
	failDimHex = "#9F1239"
	stallHex   = "#FBBF24"
	okHex      = "#22C55E"
	pillInkHex = "#0F172A" // dark text on light pills
	pillRedHex = "#EB4268" // CharmTone Sriracha
	pillAmbHex = "#FF985A" // CharmTone Tang

	runHex   = okHex // status colours shared with the panes and fleet views
	crashHex = failHex
	lateHex  = stallHex
	focusHex = brandHex
)

// chrome is the text hierarchy: value (brightest), text, dim, faint (rules
// and baselines) and track (empty bar cells). Slate on dark, inverted on light.
type chrome struct {
	value, text, dim, faint, track color.Color
}

func (m Model) chrome() chrome {
	c := func(s string) color.Color { return lipgloss.Color(s) }
	if m.dark {
		return chrome{c("#F1F5F9"), c("#CBD5E1"), c("#64748B"), c("#334155"), c("#1E293B")}
	}
	return chrome{c("#0F172A"), c("#334155"), c("#64748B"), c("#CBD5E1"), c("#E2E8F0")}
}

// assignColors gives every charted job (running, selected, pinned) a series
// colour. A job keeps its colour while it stays charted; a new job takes the
// lowest colour no charted job holds, so colours never shuffle on refresh.
func (m *Model) assignColors() {
	keep := map[string]int{}
	used := map[int]bool{}
	ids := m.chartJobIDs()
	for _, id := range ids {
		if c, ok := m.colors[id]; ok {
			keep[id], used[c] = c, true
		}
	}
	for _, id := range ids {
		if _, ok := keep[id]; !ok {
			c := freeColor(used)
			keep[id], used[c] = c, true
		}
	}
	m.colors = keep
}

func freeColor(used map[int]bool) int {
	for c := range seriesGrad {
		if !used[c] {
			return c
		}
	}
	return len(used) % len(seriesGrad) // more jobs than colours: reuse in turn
}

// colorIndex is the job's series colour slot. A job that is not charted yet
// (assignColors runs on refresh and selection) gets the next free slot.
func (m Model) colorIndex(jobID string) int {
	if c, ok := m.colors[jobID]; ok {
		return c
	}
	used := map[int]bool{}
	for _, c := range m.colors {
		used[c] = true
	}
	return freeColor(used)
}

// jobColor is the job's line colour, the first stop of its gradient.
func (m Model) jobColor(jobID string) color.Color {
	return lipgloss.Color(seriesGrad[m.colorIndex(jobID)][0])
}

// jobGrad is the job's gradient; a stalled job is drawn in failure red.
func (m Model) jobGrad(j JobSummary) (color.Color, color.Color) {
	if m.isStalled(j) || isFailure(j.Status) {
		return lipgloss.Color(failHex), lipgloss.Color("#FB7185")
	}
	g := seriesGrad[m.colorIndex(j.JobID)]
	return lipgloss.Color(g[0]), lipgloss.Color(g[1])
}

func (m Model) jobMarker(jobID string) rune {
	return seriesMarkers[m.colorIndex(jobID)]
}
