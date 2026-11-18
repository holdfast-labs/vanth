package monitor

// rect is a screen rectangle in terminal cells.
type rect struct {
	x, y, w, h int
}

// chartRect couples a metric to its screen rectangle (the full bordered box).
type chartRect struct {
	rect
	metric int
}

// layout is the stable pane geometry used by both Update (hit testing) and
// View (rendering). The dashboard mirrors a W&B LEET run overview: a summary
// header box on top, a jobs sidebar on the left, a key-metrics panel in the
// center, and the tracking plots on the right, with the event/log pane below.
// Every pane rectangle is the FULL box (border included); renderers reserve the
// border thickness themselves.
type layout struct {
	header     rect
	body       rect // region between the header and the lower pane (incl. gaps)
	jobs       rect
	metrics    rect
	charts     []chartRect
	chartsArea rect // full area available to the charts grid + fold footer
	lower      rect
	statusY    int
	narrow     bool
}

const (
	// Header: a row of stat tiles (border + 3 big-number rows + border) on
	// roomy terminals, a 3-row compact strip otherwise.
	tilesH, stripH       = 5, 3
	tilesMinW, tilesMinH = 100, 32

	// tickerH is the default lower area: border + 3 ticker/timeline rows.
	tickerH = 5

	// jobBlockHeight is the number of content rows one job occupies in the
	// jobs sidebar. renderJobs and the mouse hit-test in handleMouseClick both
	// key off this constant so click rows map onto job blocks without drift.
	jobBlockHeight = 3

	// Minimum column widths for the wide three-column layout. When the body
	// cannot satisfy all three the layout falls back to fewer columns.
	jobsMinW   = 34
	chartsMinW = 44

	panelGap = 1 // blank cells between top-level panels so borders never touch
	statusH  = 2 // status bar: separator rule row + text row
)

// computeLayout derives pane rectangles from the current terminal size. It is
// a pure function of width/height and the metric list.
func (m Model) computeLayout() layout {
	w, h := m.width, m.height
	lay := layout{statusY: h - 1}
	if !m.ready || w < m.cfg.MinWidth || h < m.cfg.MinHeight {
		return lay
	}
	hh := stripH
	if w >= tilesMinW && h >= tilesMinH {
		hh = tilesH
	}
	// The lower area is a compact ticker + timeline unless a full pane (event
	// table, logs, slowest runs) was asked for with e / l / s.
	lowerH := tickerH
	if m.lowerExpanded() {
		lowerH = clamp((h-statusH)*35/100, 6, 18)
	} else if h-statusH-hh < 16 {
		lowerH = 4
	}
	lower := rect{x: 0, y: h - statusH - lowerH, w: w, h: lowerH}
	lay.lower = lower

	lay.header = rect{x: 0, y: 0, w: w, h: hh}
	bodyTop := hh + panelGap
	bodyBottom := lower.y - panelGap
	bodyH := bodyBottom - bodyTop
	if bodyH < 2 {
		// Very short terminal: reclaim the gaps so the body keeps at least a
		// two-row box (top and bottom border).
		bodyTop = hh
		bodyBottom = lower.y
		bodyH = bodyBottom - bodyTop
	}
	lay.body = rect{x: 0, y: bodyTop, w: w, h: bodyH}

	// Dashboard collapses to a jobs+plots stack on narrow terminals.
	if w < 110 {
		return m.collapseToNarrow(lay, bodyTop, bodyBottom, w)
	}

	// Wide: a left column (jobs over key metrics) and the plots, which get the
	// rest of the width: the charts are the point of the dashboard.
	leftW := clamp(w*27/100, jobsMinW, 50)
	chartsX := leftW + panelGap
	if w-chartsX < chartsMinW {
		return m.collapseToNarrow(lay, bodyTop, bodyBottom, w)
	}
	jobsH, metricsH := bodyH, 0
	if bodyH >= 16 {
		// The Metrics pane shrinks to its content; the jobs list gets the rest.
		metricsH = clamp(m.metricsHeight(leftW-2), 4, bodyH*45/100)
		jobsH = bodyH - metricsH
		// A short job list gives its spare rows to the Metrics pane.
		need := 2
		for i := range m.jobs {
			need += m.jobRows(i)
		}
		if need < jobsH {
			jobsH = max(need, 6)
			metricsH = bodyH - jobsH
		}
	}
	lay.jobs = rect{x: 0, y: bodyTop, w: leftW, h: jobsH}
	if metricsH > 0 {
		lay.metrics = rect{x: 0, y: bodyTop + jobsH, w: leftW, h: metricsH}
	}
	chartArea := rect{x: chartsX, y: bodyTop, w: w - chartsX, h: bodyH}
	cols := 1
	if chartArea.w >= 96 {
		cols = 2
	}
	lay.chartsArea = chartArea
	lay.charts = m.chartRects(chartArea, cols)
	return lay
}

// lowerExpanded reports whether a full-size lower pane (events table, logs or
// slowest runs) replaces the compact ticker.
func (m Model) lowerExpanded() bool {
	return m.showEvents || m.showLogs || m.showSlowest
}

// collapseToNarrow turns a layout into the jobs+plots stack used on narrow
// terminals and when the wide three-column minimums cannot fit. It fills in the
// jobs/charts rects and leaves the already-computed header/lower/body intact.
func (m Model) collapseToNarrow(lay layout, bodyTop, bodyBottom, w int) layout {
	lay.narrow = true
	bodyH := bodyBottom - bodyTop
	// Jobs take what their rows need (up to 58% of the body) so a short list
	// does not leave blank rows while the plots are squeezed.
	need := 2
	for i := range m.jobs {
		need += m.jobRows(i)
	}
	share := 58
	if m.fleetMode() {
		share = 70 // the fleet timeline is a luxury; the list is not
	}
	jobsH := clamp(min(need, bodyH*share/100), 3, 22)
	if bodyH-jobsH-panelGap < 1 {
		jobsH = bodyH // no room for charts; the jobs pane takes the body
	}
	lay.jobs = rect{x: 0, y: bodyTop, w: w, h: jobsH}
	chartTop := bodyTop + jobsH + panelGap
	chartArea := rect{x: 0, y: chartTop, w: w, h: bodyBottom - chartTop}
	if chartArea.h > 0 {
		lay.chartsArea = chartArea
		lay.charts = m.chartRects(chartArea, 1)
	}
	return lay
}

// chartRects lays out metric charts in a grid inside an area. Each rectangle
// is the full bordered box: the plot canvas inside it is w-2 by h-2. Cells in
// a row are separated by a 1-cell gap; rows abut (as in LEET's grids, so chart
// bodies keep as much height as possible). When more metrics exist than fit,
// the last row is reserved for a "+N more metrics" footer only if the remaining
// cells still render a visible braille body. Cells never overflow the area.
func (m Model) chartRects(area rect, cols int) []chartRect {
	n := len(m.metrics)
	if n == 0 || area.w <= 0 || area.h <= 0 {
		return nil
	}
	if cols < 1 {
		cols = 1
	}
	if cols > n {
		cols = n
	}
	cellW := (area.w - (cols-1)*panelGap) / cols
	if cellW < MinChartWidth {
		cols = 1
		cellW = area.w
	}
	rows := (n + cols - 1) / cols
	cellH := area.h / rows
	if cellH < MinChartHeight {
		rows = area.h / MinChartHeight
		if rows < 1 {
			rows = 1
		}
		cellH = area.h / rows
	}
	// Reserve the fold footer row only when the visible cells keep a body.
	if rows*cols < n && cellH > MinChartHeight {
		effH := area.h - 1
		if effH > 0 {
			r2 := (n + cols - 1) / cols
			c2 := effH / r2
			if c2 < MinChartHeight {
				r2 = effH / MinChartHeight
				if r2 < 1 {
					r2 = 1
				}
				c2 = effH / r2
			}
			if r2*cols >= rows*cols && c2 >= MinChartHeight {
				rows, cellH = r2, c2
			}
		}
	}
	var out []chartRect
	for i := 0; i < n && i/cols < rows; i++ {
		c := i % cols
		r := i / cols
		x := area.x + c*(cellW+panelGap)
		y := area.y + r*cellH
		ww := cellW
		if x+ww > area.x+area.w {
			ww = area.x + area.w - x
		}
		hh := cellH
		if y+hh > area.y+area.h {
			hh = area.y + area.h - y
		}
		if ww <= 0 || hh <= 0 {
			break
		}
		out = append(out, chartRect{rect: rect{x: x, y: y, w: ww, h: hh}, metric: i})
	}
	return out
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
