package monitor

import (
	"fmt"
	"math"
	"strings"
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

// roundSig rounds to three significant digits so readouts stay short.
func roundSig(v float64) float64 {
	if v == 0 || math.IsNaN(v) || math.IsInf(v, 0) {
		return v
	}
	p := math.Pow(10, 2-math.Floor(math.Log10(math.Abs(v))))
	return math.Round(v*p) / p
}
