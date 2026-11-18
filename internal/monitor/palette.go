package monitor

import (
	"image/color"

	"charm.land/lipgloss/v2"
)

// The monitor's palette follows W&B LEET: ten series colours (one per job,
// the same everywhere it appears) and muted grey chrome.
var seriesHex = [...]string{
	"#58D3DB", "#FCA36F", "#7DB1FA", "#E180FF", "#FFCF4D",
	"#B8E986", "#FF9ECF", "#A1A6FF", "#E8C9A0", "#6FE0B5",
}

// seriesMarkers stand in for the series colours when there is no colour:
// chart points and the sidebar swatch use the job's marker instead.
var seriesMarkers = [...]rune{'*', '+', 'o', 'x', '#', '@', '%', '&', '=', '~'}

const (
	brandHex   = "#FCBC32" // vanth/LIVE badges and the selected row
	brandFgHex = "#171A1F"
	focusHex   = "#10BFCC" // focused chart and the help bar
	runHex     = "#3DD68C"
	crashHex   = "#FF5F5F" // failed and stalled
	lateHex    = "#E0A526"
)

// chrome is the grey scale for borders, titles and text; LEET's values on a
// dark background, darker ones on a light background.
type chrome struct {
	border, title, text, key, value, axis, guide color.Color
}

func (m Model) chrome() chrome {
	c := func(s string) color.Color { return lipgloss.Color(s) }
	if m.dark {
		return chrome{c("#444444"), c("#bcbcbc"), c("#8a8a8a"), c("#767676"), c("#d0d0d0"), c("#585858"), c("#383838")}
	}
	return chrome{c("#bcbcbc"), c("#3a3a3a"), c("#5f5f5f"), c("#767676"), c("#262626"), c("#8a8a8a"), c("#d0d0d0")}
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
	for c := range seriesHex {
		if !used[c] {
			return c
		}
	}
	return len(used) % len(seriesHex) // more jobs than colours: reuse in turn
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

func (m Model) jobColor(jobID string) color.Color {
	return lipgloss.Color(seriesHex[m.colorIndex(jobID)])
}

func (m Model) jobMarker(jobID string) rune {
	return seriesMarkers[m.colorIndex(jobID)]
}
