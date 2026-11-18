package monitor

import (
	"math"
	"testing"
)

func tev(seq int64, typ string, data map[string]any) Event {
	return Event{
		EventID:   "evt_" + string(rune('a'+seq)),
		JobID:     "job_test",
		Seq:       seq,
		Type:      typ,
		Level:     "info",
		Data:      data,
		CreatedAt: "2026-01-01T00:00:00Z",
	}
}

func ingestOne(events ...Event) *SeriesStore {
	s := NewSeriesStore()
	s.Ingest("job_test", events, 1000, 100)
	return s
}

func TestDecodeDataSanitizesPythonNaNInfinity(t *testing.T) {
	cases := []struct {
		raw   string
		want  float64
		isNil bool
	}{
		{raw: `{"loss":0.42}`, want: 0.42},
		{raw: `{"loss":NaN}`, isNil: true},
		{raw: `{"loss":Infinity}`, isNil: true},
		{raw: `{"loss":-Infinity}`, isNil: true},
		{raw: `{"loss": "has NaN inside"}`, want: 0},
		{raw: `{"loss":null}`, isNil: true},
	}
	for _, c := range cases {
		m, ok := decodeData(c.raw)
		if !ok {
			t.Fatalf("decodeData(%q) failed", c.raw)
		}
		v, present := m["loss"]
		if c.isNil {
			if present && v != nil {
				t.Errorf("decodeData(%q): expected nil, got %v", c.raw, v)
			}
			continue
		}
		if !present {
			continue
		}
		f, ok := numericValue(v)
		if !ok {
			continue
		}
		if math.Abs(f-c.want) > 1e-9 && c.raw != `{"loss": "has NaN inside"}` {
			t.Errorf("decodeData(%q) loss = %v, want %v", c.raw, f, c.want)
		}
	}
}

func TestDecodeDataStringNaNPreserved(t *testing.T) {
	m, ok := decodeData(`{"msg":"waiting for NaN step","loss":1.5}`)
	if !ok {
		t.Fatal("decodeData failed")
	}
	if got := m["msg"]; got != "waiting for NaN step" {
		t.Errorf("string value corrupted: %v", got)
	}
	if got := m["loss"]; got != 1.5 {
		t.Errorf("loss = %v, want 1.5", got)
	}
}

func TestDecodeDataMalformed(t *testing.T) {
	if _, ok := decodeData(`{broken`); ok {
		t.Error("malformed data accepted")
	}
}

func TestMetricXFromStep(t *testing.T) {
	s := ingestOne(tev(1, "metric", map[string]any{
		"_step": 10.0, "loss": 0.42, "val_loss": 0.51, "accuracy": 0.88,
	}))
	for _, tc := range []struct {
		metric string
		y      float64
	}{
		{"loss", 0.42}, {"val_loss", 0.51}, {"accuracy", 0.88},
	} {
		key := SeriesKey{JobID: "job_test", Metric: tc.metric}
		pts := s.Points[key]
		if len(pts) != 1 {
			t.Fatalf("%s: got %d points, want 1", tc.metric, len(pts))
		}
		if pts[0].X != 10 {
			t.Errorf("%s: x = %v, want 10 (_step)", tc.metric, pts[0].X)
		}
		if pts[0].Y != tc.y {
			t.Errorf("%s: y = %v, want %v", tc.metric, pts[0].Y, tc.y)
		}
		if pts[0].Seq != 1 {
			t.Errorf("%s: seq = %d, want 1", tc.metric, pts[0].Seq)
		}
	}
}

func TestMetricXFallsBackToSeq(t *testing.T) {
	s := ingestOne(tev(7, "metric", map[string]any{"loss": 1.0}))
	pts := s.Points[SeriesKey{JobID: "job_test", Metric: "loss"}]
	if pts[0].X != 7 {
		t.Errorf("x = %v, want seq 7 when _step absent", pts[0].X)
	}
	s = ingestOne(tev(8, "metric", map[string]any{"_step": "nope", "loss": 2.0}))
	pts = s.Points[SeriesKey{JobID: "job_test", Metric: "loss"}]
	if pts[0].X != 8 {
		t.Errorf("x = %v, want seq 8 when _step non-numeric", pts[0].X)
	}
}

func TestMetricSkipsUnderscoreKeys(t *testing.T) {
	s := ingestOne(tev(1, "metric", map[string]any{
		"_step": 1.0, "_ignore": 5.0, "loss": 0.5,
	}))
	if _, ok := s.Points[SeriesKey{JobID: "job_test", Metric: "_ignore"}]; ok {
		t.Error("_ignore must not become a series")
	}
	if len(s.Points) != 1 {
		t.Errorf("expected only loss series, got %d", len(s.Points))
	}
}

func TestMetricSkipsStageAndPhaseAsSeries(t *testing.T) {
	s := ingestOne(tev(1, "metric", map[string]any{"stage": "train", "phase": "epoch1", "loss": 0.5}))
	for _, m := range []string{"stage", "phase"} {
		if _, ok := s.Points[SeriesKey{JobID: "job_test", Metric: m}]; ok {
			t.Errorf("%s must be table context, not a series", m)
		}
	}
}

func TestMetricSkipsNaNInfBoolStringAndCountsWarnings(t *testing.T) {
	s := ingestOne(tev(1, "metric", map[string]any{
		"nan":  math.NaN(),
		"inf":  math.Inf(1),
		"bool": true,
		"text": "nope",
		"null": nil,
		"good": 3.0,
	}))
	if len(s.Points) != 1 {
		t.Errorf("expected only good series, got %d", len(s.Points))
	}
	if s.Warnings != 5 {
		t.Errorf("warnings = %d, want 5", s.Warnings)
	}
}

func TestProgressDerivesPercent(t *testing.T) {
	s := ingestOne(tev(1, "progress", map[string]any{"current": 10.0, "total": 100.0}))
	for _, tc := range []struct {
		metric string
		y      float64
	}{
		{"progress.current", 10},
		{"progress.total", 100},
		{"progress.percent", 10},
	} {
		pts := s.Points[SeriesKey{JobID: "job_test", Metric: tc.metric}]
		if len(pts) != 1 {
			t.Fatalf("%s: got %d points", tc.metric, len(pts))
		}
		if math.Abs(pts[0].Y-tc.y) > 1e-9 {
			t.Errorf("%s: y = %v, want %v", tc.metric, pts[0].Y, tc.y)
		}
		if pts[0].X != 1 {
			t.Errorf("%s: x = %v, want seq 1", tc.metric, pts[0].X)
		}
	}
}

func TestProgressKeepsProvidedPercent(t *testing.T) {
	s := ingestOne(tev(2, "progress", map[string]any{
		"current": 1.0, "total": 3.0, "percent": 33.33,
	}))
	pts := s.Points[SeriesKey{JobID: "job_test", Metric: "progress.percent"}]
	if len(pts) != 1 || math.Abs(pts[0].Y-33.33) > 1e-9 {
		t.Errorf("percent = %v, want 33.33", pts)
	}
}

func TestProgressMissingFields(t *testing.T) {
	s := ingestOne(tev(1, "progress", map[string]any{"percent": 50.0}))
	if _, ok := s.Points[SeriesKey{JobID: "job_test", Metric: "progress.current"}]; ok {
		t.Error("absent current must not create a series")
	}
	if _, ok := s.Points[SeriesKey{JobID: "job_test", Metric: "progress.total"}]; ok {
		t.Error("absent total must not create a series")
	}
	pts := s.Points[SeriesKey{JobID: "job_test", Metric: "progress.percent"}]
	if len(pts) != 1 {
		t.Fatalf("percent points = %d, want 1", len(pts))
	}
	if s.Warnings != 0 {
		t.Errorf("warnings = %d, want 0", s.Warnings)
	}
}

func TestProgressNonNumericWarns(t *testing.T) {
	s := ingestOne(tev(1, "progress", map[string]any{
		"current": "ten", "total": 100.0, "percent": nil,
	}))
	if len(s.Points) != 1 { // only progress.total survives
		t.Errorf("expected only total series, got %d", len(s.Points))
	}
	if s.Warnings != 2 {
		t.Errorf("warnings = %d, want 2 (current + percent)", s.Warnings)
	}
}

func TestProgressStoresPercentClampedLikePython(t *testing.T) {
	// Python derives percent = round(current/total*100, 2) with total==0 -> 0.
	s := ingestOne(tev(1, "progress", map[string]any{"current": 5.0, "total": 0.0}))
	pts := s.Points[SeriesKey{JobID: "job_test", Metric: "progress.percent"}]
	if len(pts) != 1 || pts[0].Y != 0 {
		t.Errorf("percent = %v, want 0", pts)
	}
}

func TestSeriesCapBoundsMemory(t *testing.T) {
	s := NewSeriesStore()
	evs := make([]Event, 0, 12)
	for i := int64(1); i <= 12; i++ {
		evs = append(evs, tev(i, "metric", map[string]any{"loss": float64(i)}))
	}
	s.Ingest("job_test", evs, 5, 100)
	pts := s.Points[SeriesKey{JobID: "job_test", Metric: "loss"}]
	if len(pts) != 5 {
		t.Fatalf("series cap: got %d points, want 5", len(pts))
	}
	if pts[0].Seq != 8 {
		t.Errorf("expected oldest kept seq 8, got %d", pts[0].Seq)
	}
}

func TestEventCapBoundsMemory(t *testing.T) {
	s := NewSeriesStore()
	evs := make([]Event, 0, 10)
	for i := int64(1); i <= 10; i++ {
		evs = append(evs, tev(i, "progress", map[string]any{"current": float64(i)}))
	}
	s.Ingest("job_test", evs, 100, 4)
	got := s.Events["job_test"]
	if len(got) != 4 {
		t.Fatalf("event cap: got %d events, want 4", len(got))
	}
	if got[0].Seq != 7 {
		t.Errorf("expected first kept seq 7, got %d", got[0].Seq)
	}
}

func TestTransformKeepsAllOriginalEvents(t *testing.T) {
	evs := []Event{
		tev(1, "metric", map[string]any{"loss": 1.0}),
		tev(2, "progress", map[string]any{"current": 1.0}),
		tev(3, "info", map[string]any{"note": "x"}),
		tev(4, "metric", map[string]any{"loss": math.NaN()}),
	}
	s := NewSeriesStore()
	s.Ingest("job_test", evs, 100, 100)
	got := s.Events["job_test"]
	if len(got) != 4 {
		t.Fatalf("event table lost events: got %d, want 4", len(got))
	}
	for i, ev := range got {
		if ev.Seq != evs[i].Seq {
			t.Errorf("event order changed at %d", i)
		}
	}
}
