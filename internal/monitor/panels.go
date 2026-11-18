package monitor

import (
	"fmt"
	"math"
	"strings"

	"charm.land/lipgloss/v2"
)

// PanelSpec is one declared visual for a job, from a `panels` event:
// AGENT_EVENT {"type":"panels","data":{"panels":[{"kind":"gauge","metric":"gpu"}]}}.
// The daemon validates these (server.validate_panels); the monitor checks them
// again because the database is a trust boundary of its own.
type PanelSpec struct {
	Kind   string // gauge | line | stat | text
	Metric string
	Key    string // text panels: show the latest value of this event data field
	Text   string // text panels: fixed text
	Label  string
	Unit   string
	Min    *float64
	Max    *float64
}

const maxPanels = 8

var panelKindAliases = map[string]string{"progress": "gauge", "bar": "gauge", "chart": "line",
	"sparkline": "line", "number": "stat", "big": "stat", "status": "text"}

// parsePanels reads a `panels` event payload. Invalid entries are skipped and
// counted; nothing here can fail.
func parsePanels(data map[string]any) (specs []PanelSpec, invalid int) {
	raw, ok := data["panels"].([]any)
	if !ok {
		return nil, 1
	}
	if len(raw) > maxPanels {
		invalid += len(raw) - maxPanels
		raw = raw[:maxPanels]
	}
	str := func(m map[string]any, k string, limit int) string {
		s, _ := m[k].(string)
		s = strings.TrimSpace(s)
		if r := []rune(s); len(r) > limit {
			s = string(r[:limit])
		}
		return s
	}
	num := func(m map[string]any, k string) *float64 {
		if v, ok := numericValue(m[k]); ok {
			return &v
		}
		return nil
	}
	for _, e := range raw {
		m, ok := e.(map[string]any)
		if !ok {
			invalid++
			continue
		}
		kind := strings.ToLower(str(m, "kind", 16))
		if a, ok := panelKindAliases[kind]; ok {
			kind = a
		}
		p := PanelSpec{Kind: kind, Metric: str(m, "metric", 64), Key: str(m, "key", 64),
			Text: str(m, "text", 120), Label: str(m, "label", 32), Unit: str(m, "unit", 12),
			Min: num(m, "min"), Max: num(m, "max")}
		switch {
		case kind == "text" && p.Text == "" && p.Key == "",
			kind != "text" && kind != "gauge" && kind != "line" && kind != "stat",
			kind != "text" && p.Metric == "":
			invalid++
			continue
		}
		if p.Min != nil && p.Max != nil && *p.Min >= *p.Max {
			p.Min, p.Max = nil, nil
		}
		specs = append(specs, p)
	}
	return specs, invalid
}

// panelDecl is the latest declaration seen for a job.
type panelDecl struct {
	specs   []PanelSpec
	invalid int
}

// notePanels keeps the newest `panels` event per job. The newest declaration
// replaces the previous one, so a job can change its view mid-run.
func (m *Model) notePanels(events map[string][]Event) {
	for jobID, evs := range events {
		for _, ev := range evs {
			if ev.Type != "panels" {
				continue
			}
			specs, bad := parsePanels(ev.Data)
			if prev, ok := ev.Data["invalid"].([]any); ok {
				bad = max(bad, len(prev))
			}
			if m.panels == nil {
				m.panels = map[string]panelDecl{}
			}
			m.panels[jobID] = panelDecl{specs: specs, invalid: bad}
		}
	}
}

// jobPanels returns the job's declared panels, or a default set: line charts
// of up to three of its metrics.
func (m Model) jobPanels(jobID string) (specs []PanelSpec, declared bool) {
	if d, ok := m.panels[jobID]; ok && len(d.specs) > 0 {
		return d.specs, true
	}
	for _, k := range m.metrics {
		if strings.HasPrefix(k, "progress.") || len(m.points[SeriesKey{JobID: jobID, Metric: k}]) == 0 {
			continue
		}
		if specs = append(specs, PanelSpec{Kind: "line", Metric: k}); len(specs) == 3 {
			break
		}
	}
	return specs, false
}

const panelMinW = 16

// panelGridRows is the number of rows renderPanelGrid uses.
func panelGridRows(n, width, cellH int) int {
	if n == 0 {
		return 0
	}
	per := max(1, min(n, (width+2)/(panelMinW+2)))
	return (n + per - 1) / per * cellH
}

// renderPanelGrid lays panels out left to right in cells of at least panelMinW
// cells, wrapping as needed. cellH is 2 (label + one body row) or 4 (label +
// three body rows: taller charts and big digits). Every line is width wide.
func (m Model) renderPanelGrid(j JobSummary, specs []PanelSpec, width, cellH int) []string {
	if len(specs) == 0 || width < panelMinW {
		return nil
	}
	per := max(1, min(len(specs), (width+2)/(panelMinW+2)))
	cellW := (width - 2*(per-1)) / per
	var out []string
	for start := 0; start < len(specs); start += per {
		rows := make([]string, cellH)
		for i := start; i < min(start+per, len(specs)); i++ {
			cell := m.renderPanel(j, specs[i], cellW, cellH)
			for r := range rows {
				if i > start {
					rows[r] += "  "
				}
				rows[r] += fillLine(cell[r], cellW)
			}
		}
		for _, r := range rows {
			out = append(out, fillLine(r, width))
		}
	}
	return out
}

// latestValue returns the newest point of a job's metric series.
func (m Model) latestValue(jobID, metric string) (float64, bool) {
	pts := m.points[SeriesKey{JobID: jobID, Metric: metric}]
	if len(pts) == 0 {
		return 0, false
	}
	return pts[len(pts)-1].Y, true
}

// latestField returns the newest value of an event data field, as text.
func (m Model) latestField(jobID, key string) string {
	evs := m.events[jobID]
	for i := len(evs) - 1; i >= 0; i-- {
		if v, ok := evs[i].Data[key]; ok && v != nil {
			switch x := v.(type) {
			case string:
				return x
			case float64:
				return compactFmt(x)
			case bool:
				return fmt.Sprint(x)
			}
		}
	}
	return ""
}

// renderPanel draws one panel as h rows of at most w cells.
func (m Model) renderPanel(j JobSummary, p PanelSpec, w, h int) []string {
	rows := make([]string, h)
	label := p.Label
	if label == "" {
		label = p.Metric
		if label == "" {
			label = p.Key
		}
	}
	col := lipgloss.NewStyle().Foreground(statusColor(j.Status, m.dark))
	val, has := m.latestValue(j.JobID, p.Metric)
	valText := ""
	if has {
		valText = compactFmt(roundSig(val)) + p.Unit
	}
	head := func(right string) string {
		lw := w - lipgloss.Width(right) - 1
		return dimStyle.Render(truncate(label, max(lw, 1))) + strings.Repeat(" ", max(lw-lipgloss.Width(truncate(label, max(lw, 1))), 0)+1) + right
	}
	waiting := dimStyle.Render("–")
	body := h - 1 // rows under the label
	switch p.Kind {
	case "gauge":
		lo, hi := 0.0, 100.0
		if p.Min != nil {
			lo = *p.Min
		}
		if p.Max != nil {
			hi = *p.Max
		}
		rows[0] = head(boldStyle.Render(valText))
		if !has {
			rows[1] = waiting
			break
		}
		pct := (val - lo) / (hi - lo) * 100
		if p.Unit == "" && p.Min == nil && p.Max == nil {
			rows[0] = head(boldStyle.Render(fmt.Sprintf("%.0f%%", clampF(pct, 0, 100))))
		}
		rows[1+(body-1)/2] = m.styledBar(j.Status, clampF(pct, 0, 100), w, false)
	case "line":
		vals := m.seriesValues(j.JobID, p.Metric, w)
		arrow := trendArrow(m.points[SeriesKey{JobID: j.JobID, Metric: p.Metric}])
		rows[0] = head(boldStyle.Render(valText) + dimStyle.Render(strings.TrimSpace(" "+arrow)))
		if len(vals) == 0 {
			rows[1] = waiting
			break
		}
		for i, l := range miniChart(vals, w, body) {
			rows[1+i] = col.Render(l)
		}
	case "stat":
		rows[0] = dimStyle.Render(truncate(label, w))
		if !has {
			rows[1] = waiting
			break
		}
		if big := bigNumber(int(math.Round(val)), w-lipgloss.Width(p.Unit)-1); body >= 3 && big != nil && val >= 0 && val == math.Trunc(val) {
			for i, l := range big {
				rows[1+i] = col.Bold(true).Render(l)
			}
			rows[3] += " " + dimStyle.Render(p.Unit)
			break
		}
		rows[1+(body-1)/2] = col.Bold(true).Render(compactFmt(roundSig(val))) + " " + dimStyle.Render(p.Unit)
	case "text":
		rows[0] = dimStyle.Render(truncate(label, w))
		txt := p.Text
		if p.Key != "" {
			if v := m.latestField(j.JobID, p.Key); v != "" {
				txt = v
			}
		}
		if txt == "" {
			rows[1] = waiting
			break
		}
		rows[1+(body-1)/2] = boldStyle.Render(truncate(txt, w))
	}
	return rows
}

// miniChart draws vals as an h-row block area chart, w cells wide (newest
// values on the right).
func miniChart(vals []float64, w, h int) []string {
	if len(vals) > w {
		vals = vals[len(vals)-w:]
	}
	if h <= 1 {
		return []string{sparkline(vals, w)}
	}
	lo, hi := vals[0], vals[0]
	for _, v := range vals {
		lo, hi = min(lo, v), max(hi, v)
	}
	levels := []rune(" ▁▂▃▄▅▆▇█")
	out := make([]string, h)
	for row := 0; row < h; row++ {
		var b strings.Builder
		for _, v := range vals {
			units := h * 4 // flat series sit mid-height
			if hi > lo {
				units = 1 + int((v-lo)/(hi-lo)*float64(h*8-1))
			}
			b.WriteRune(levels[clamp(units-(h-1-row)*8, 0, 8)])
		}
		out[row] = b.String()
	}
	return out
}

func clampF(v, lo, hi float64) float64 { return math.Max(lo, math.Min(hi, v)) }

// roundSig rounds to three significant digits so readouts stay short.
func roundSig(v float64) float64 {
	if v == 0 || math.IsNaN(v) || math.IsInf(v, 0) {
		return v
	}
	p := math.Pow(10, 2-math.Floor(math.Log10(math.Abs(v))))
	return math.Round(v*p) / p
}
