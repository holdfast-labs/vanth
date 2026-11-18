package monitor

import (
	"encoding/json"
	"math"
	"sort"
	"strings"
)

// SeriesStore accumulates transformed points and raw events during a refresh.
// It is mutated only inside command goroutines and converted to an immutable
// RefreshResult before crossing back into the model.
type SeriesStore struct {
	Points   map[SeriesKey][]Point
	Events   map[string][]Event
	Warnings int // skipped NaN/Inf/null/non-numeric values
}

// NewSeriesStore returns an empty store.
func NewSeriesStore() *SeriesStore {
	return &SeriesStore{
		Points: map[SeriesKey][]Point{},
		Events: map[string][]Event{},
	}
}

// decodeData parses a data_json payload. Python's json.dumps defaults to
// allow_nan=True, so the durable text can legally contain NaN/Infinity
// literals that Go's encoding/json rejects. Those tokens are normalized to
// null (skipped by the transform and counted in the warning counter).
func decodeData(raw string) (map[string]any, bool) {
	if raw == "" {
		return map[string]any{}, true
	}
	sanitized := sanitizeJSONNumbers(raw)
	var m map[string]any
	if err := json.Unmarshal([]byte(sanitized), &m); err != nil {
		return map[string]any{}, false
	}
	if m == nil {
		return map[string]any{}, true
	}
	return m, true
}

// sanitizeJSONNumbers rewrites top-level NaN/Infinity/-Infinity JSON literals
// to null while leaving strings untouched. A token is only treated as a
// literal when it sits at a JSON value position (after a structural delimiter).
func sanitizeJSONNumbers(raw string) string {
	if !strings.ContainsAny(raw, "NInf") {
		return raw
	}
	var b strings.Builder
	b.Grow(len(raw))
	inStr := false
	esc := false
	for i := 0; i < len(raw); {
		c := raw[i]
		if inStr {
			b.WriteByte(c)
			if esc {
				esc = false
			} else if c == '\\' {
				esc = true
			} else if c == '"' {
				inStr = false
			}
			i++
			continue
		}
		switch c {
		case '"':
			inStr = true
			b.WriteByte(c)
			i++
		case 'N':
			if strings.HasPrefix(raw[i:], "NaN") && atValuePosition(raw, i) {
				b.WriteString("null")
				i += 3
				continue
			}
			b.WriteByte(c)
			i++
		case 'I':
			if strings.HasPrefix(raw[i:], "Infinity") && atValuePosition(raw, i) {
				b.WriteString("null")
				i += 8
				continue
			}
			b.WriteByte(c)
			i++
		case '-':
			if i+1 < len(raw) && strings.HasPrefix(raw[i+1:], "Infinity") && atValuePosition(raw, i) {
				b.WriteString("null")
				i += 9
				continue
			}
			b.WriteByte(c)
			i++
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String()
}

func atValuePosition(s string, i int) bool {
	j := i - 1
	for j >= 0 && (s[j] == ' ' || s[j] == '\t' || s[j] == '\n' || s[j] == '\r') {
		j--
	}
	if j < 0 {
		return true
	}
	switch s[j] {
	case '{', '[', ',', ':':
		return true
	}
	return false
}

// numericValue reports whether v is a usable finite number.
func numericValue(v any) (float64, bool) {
	var f float64
	switch n := v.(type) {
	case float64:
		f = n
	case json.Number:
		var err error
		f, err = n.Float64()
		if err != nil {
			return 0, false
		}
	case int:
		f = float64(n)
	case int64:
		f = float64(n)
	case uint64:
		f = float64(n)
	default:
		return 0, false
	}
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, false
	}
	return f, true
}

// eventStage extracts the stage/phase table context from an event's data.
func eventStage(data map[string]any) string {
	if s, ok := data["stage"].(string); ok && s != "" {
		return s
	}
	if s, ok := data["phase"].(string); ok && s != "" {
		return s
	}
	return ""
}

// Ingest transforms a batch of ordered events for one job into points and
// appends them plus the raw events into the store.
func (s *SeriesStore) Ingest(jobID string, events []Event, seriesCap, eventCap int) {
	events = s.ingestEvents(jobID, events, seriesCap)
	if len(events) == 0 {
		return
	}
	s.Events[jobID] = append(s.Events[jobID], events...)
	if n := len(s.Events[jobID]); n > eventCap {
		s.Events[jobID] = s.Events[jobID][n-eventCap:]
	}
}

func (s *SeriesStore) ingestEvents(jobID string, events []Event, seriesCap int) []Event {
	if len(events) == 0 {
		return events
	}
	out := events[:0:0]
	for _, ev := range events {
		switch ev.Type {
		case "metric":
			s.ingestMetric(jobID, ev, seriesCap)
		case "progress":
			s.ingestProgress(jobID, ev, seriesCap)
		}
		out = append(out, ev)
	}
	return out
}

func (s *SeriesStore) ingestMetric(jobID string, ev Event, seriesCap int) {
	stage := eventStage(ev.Data)
	for k, v := range ev.Data {
		if strings.HasPrefix(k, "_") {
			continue
		}
		if k == "stage" || k == "phase" {
			continue
		}
		y, ok := numericValue(v)
		if !ok {
			s.Warnings++
			continue
		}
		s.appendPoint(jobID, k, ev, y, stage, seriesCap)
	}
}

func (s *SeriesStore) ingestProgress(jobID string, ev Event, seriesCap int) {
	stage := eventStage(ev.Data)
	data := ev.Data
	current, curOK := numericValue(data["current"])
	total, totOK := numericValue(data["total"])
	percent, pctOK := numericValue(data["percent"])
	if !pctOK {
		if curOK && totOK {
			if total != 0 {
				percent = math.Round(current/total*100*100) / 100
			} else {
				percent = 0
			}
			pctOK = true
		}
	}
	if _, present := data["current"]; present {
		if curOK {
			s.appendPoint(jobID, "progress.current", ev, current, stage, seriesCap)
		} else {
			s.Warnings++
		}
	}
	if _, present := data["total"]; present {
		if totOK {
			s.appendPoint(jobID, "progress.total", ev, total, stage, seriesCap)
		} else {
			s.Warnings++
		}
	}
	if _, present := data["percent"]; present || pctOK {
		if pctOK {
			s.appendPoint(jobID, "progress.percent", ev, percent, stage, seriesCap)
		} else {
			s.Warnings++
		}
	}
}

func (s *SeriesStore) appendPoint(jobID, metric string, ev Event, y float64, stage string, seriesCap int) {
	x := float64(ev.Seq)
	if v, ok := numericValue(ev.Data["_step"]); ok {
		x = v
	}
	p := Point{
		EventID: ev.EventID,
		Seq:     ev.Seq,
		X:       x,
		Y:       y,
		At:      ev.CreatedAt,
		Stage:   stage,
	}
	key := SeriesKey{JobID: jobID, Metric: metric}
	s.Points[key] = append(s.Points[key], p)
	if n := len(s.Points[key]); n > seriesCap {
		s.Points[key] = s.Points[key][n-seriesCap:]
	}
}

// typicalStep returns the lower median positive x delta of a series, or 0 when
// no positive deltas exist. The lower median resists skew from a single large
// gap so that one outlier delta does not inflate the "typical" step.
func typicalStep(points []Point) float64 {
	var deltas []float64
	for i := 1; i < len(points); i++ {
		if d := points[i].X - points[i-1].X; d > 0 {
			deltas = append(deltas, d)
		}
	}
	if len(deltas) == 0 {
		return 0
	}
	sort.Float64s(deltas)
	return deltas[(len(deltas)-1)/2]
}
