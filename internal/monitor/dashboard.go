package monitor

import (
	"strings"
	"time"
)

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

// renderLower draws the pane opened with e / l / s in place of the charts.
func (m Model) renderLower(r rect) string {
	if r.h <= 0 || r.w <= 0 {
		return ""
	}
	switch {
	case m.showEvents:
		return m.renderEvents(r)
	case m.showLogs:
		return m.renderLogs(r)
	}
	return m.renderSlowest(r)
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
