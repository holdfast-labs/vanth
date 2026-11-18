package monitor

import (
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The glance view (the default; `v` toggles the detail dashboard) puts active
// work first: one card per running or queued job with a big progress bar,
// elapsed time, ETA, rate and a liveness readout, then a compact strip of
// recently finished runs. Colour, glyphs and bar length carry the meaning.

// isActive reports whether a job is still in flight.
func isActive(status string) bool {
	switch status {
	case "running", "launching", "queued", "pending":
		return true
	}
	return false
}

func isTerminal(status string) bool {
	switch status {
	case "completed", "failed", "timeout", "cancelled", "orphaned":
		return true
	}
	return false
}

func isFailure(status string) bool {
	return status == "failed" || status == "timeout" || status == "orphaned"
}

// Liveness: how recently a running job reported, judged against its own usual
// event interval.
type liveness int

const (
	liveUnknown liveness = iota // no events loaded yet
	liveFresh
	liveQuiet // too few events to know the usual interval, and none for a while
	liveLate
	liveStalled
)

const (
	lateFactor, stallFactor = 3, 6
	lateFloor, stallFloor   = 10 * time.Second, 30 * time.Second
	quietAfter              = time.Minute
)

func parseTS(ts string) (time.Time, bool) {
	t, err := time.Parse(time.RFC3339Nano, ts)
	return t, err == nil
}

// eventInterval is the median gap between the job's last (up to 20) events.
func eventInterval(evs []Event) time.Duration {
	if len(evs) > 21 {
		evs = evs[len(evs)-21:]
	}
	var gaps []time.Duration
	var prev time.Time
	for i, ev := range evs {
		t, ok := parseTS(ev.CreatedAt)
		if !ok {
			continue
		}
		if i > 0 && !prev.IsZero() && t.After(prev) {
			gaps = append(gaps, t.Sub(prev))
		}
		prev = t
	}
	if len(gaps) < 2 {
		return 0
	}
	sort.Slice(gaps, func(a, b int) bool { return gaps[a] < gaps[b] })
	return gaps[len(gaps)/2]
}

// jobLiveness classifies a running job: since is the time since its newest
// event and interval its usual gap (0 when unknown).
func (m Model) jobLiveness(j JobSummary, now time.Time) (state liveness, since, interval time.Duration) {
	evs := m.events[j.JobID]
	if len(evs) == 0 {
		return liveUnknown, 0, 0
	}
	last, ok := parseTS(evs[len(evs)-1].CreatedAt)
	if !ok {
		return liveUnknown, 0, 0
	}
	since = max(now.Sub(last), 0)
	interval = eventInterval(evs)
	switch {
	case interval == 0 && since >= quietAfter:
		return liveQuiet, since, 0
	case interval == 0:
		return liveFresh, since, 0
	case since > max(stallFactor*interval, stallFloor):
		return liveStalled, since, interval
	case since > max(lateFactor*interval, lateFloor):
		return liveLate, since, interval
	}
	return liveFresh, since, interval
}

// rateWindow is how many recent progress points the rate is measured over.
const rateWindow = 30

// seriesRate is the change per second of a series over its last points, using
// the event timestamps.
func seriesRate(pts []Point) (float64, bool) {
	if len(pts) > rateWindow {
		pts = pts[len(pts)-rateWindow:]
	}
	if len(pts) < 2 {
		return 0, false
	}
	t0, ok0 := parseTS(pts[0].At)
	t1, ok1 := parseTS(pts[len(pts)-1].At)
	if !ok0 || !ok1 || !t1.After(t0) {
		return 0, false
	}
	return (pts[len(pts)-1].Y - pts[0].Y) / t1.Sub(t0).Seconds(), true
}

// jobETA estimates the remaining time from the recent progress rate, falling
// back to the average rate since start. ok is false when there is no estimate.
func (m Model) jobETA(j JobSummary, now time.Time) (time.Duration, bool) {
	pts := m.points[SeriesKey{JobID: j.JobID, Metric: "progress.percent"}]
	if len(pts) == 0 {
		return 0, false
	}
	pct := pts[len(pts)-1].Y
	if pct >= 100 {
		return 0, true
	}
	rate, ok := seriesRate(pts)
	if !ok || rate <= 0 {
		start, has := jobStart(j)
		if !has || pct <= 0 || !now.After(start) {
			return 0, false
		}
		rate = pct / now.Sub(start).Seconds()
	}
	secs := (100 - pct) / rate
	if secs > 1e7 || math.IsNaN(secs) {
		return 0, false
	}
	return time.Duration(secs * float64(time.Second)), true
}

// jobRate is the recent throughput in the job's progress unit, e.g. "41 it/s".
func (m Model) jobRate(j JobSummary) string {
	rate, ok := seriesRate(m.points[SeriesKey{JobID: j.JobID, Metric: "progress.current"}])
	if !ok || rate <= 0 {
		return ""
	}
	unit := m.latestField(j.JobID, "unit")
	if unit == "" {
		unit = "it"
	}
	per := "s"
	switch {
	case rate < 1.0/60:
		rate, per = rate*3600, "h"
	case rate < 1:
		rate, per = rate*60, "min"
	}
	return shortNum(rate) + " " + truncate(unit, 10) + "/" + per
}

// shortNum renders v in at most about five cells: 0.213, 41.3, 4130, 41.3k.
func shortNum(v float64) string {
	a := math.Abs(v)
	f := func(x float64) string { return strconv.FormatFloat(roundSig(x), 'f', -1, 64) }
	switch {
	case a >= 1e9:
		return f(v/1e9) + "B"
	case a >= 1e6:
		return f(v/1e6) + "M"
	case a >= 1e4:
		return f(v/1e3) + "k"
	}
	return f(v)
}

// jobMessage is the newest human-readable step: the latest event message,
// prefixed with the progress stage when there is one.
func (m Model) jobMessage(jobID string) string {
	evs := m.events[jobID]
	for i := len(evs) - 1; i >= 0 && i >= len(evs)-200; i-- {
		ev := evs[i]
		if ev.Message == "" || ev.Type == "panels" {
			continue
		}
		if st := eventStage(ev.Data); st != "" && !strings.Contains(ev.Message, st) {
			return st + " · " + ev.Message
		}
		return ev.Message
	}
	if st := m.latestField(jobID, "stage"); st != "" {
		return st
	}
	return ""
}

// activeJobs orders the cards: stalled jobs first (they need a look), then
// running jobs oldest first, then queued ones, so cards keep their place
// between refreshes.
func (m Model) activeJobs(now time.Time) []int {
	type item struct {
		i, rank int
		t       time.Time
	}
	var items []item
	for i, j := range m.jobs {
		if !isActive(j.Status) {
			continue
		}
		rank := 2
		if j.Status == "running" || j.Status == "launching" {
			rank = 1
			if st, _, _ := m.jobLiveness(j, now); st == liveStalled {
				rank = 0
			}
		}
		t, _ := jobStart(j)
		items = append(items, item{i, rank, t})
	}
	sort.SliceStable(items, func(a, b int) bool {
		if items[a].rank != items[b].rank {
			return items[a].rank < items[b].rank
		}
		return items[a].t.Before(items[b].t)
	})
	out := make([]int, len(items))
	for k, it := range items {
		out[k] = it.i
	}
	return out
}

// glanceOrder is the order up/down walks in the glance view: active cards,
// then recent runs, then anything else.
func (m Model) glanceOrder() []int {
	var out []int
	seen := map[int]bool{}
	for _, i := range m.activeJobs(time.Now()) {
		out = append(out, i)
		seen[i] = true
	}
	for _, i := range m.recentJobs() {
		out = append(out, i)
		seen[i] = true
	}
	for i := range m.jobs {
		if !seen[i] {
			out = append(out, i)
		}
	}
	return out
}

// moveGlance steps the selection along glanceOrder.
func (m *Model) moveGlance(delta int) {
	order := m.glanceOrder()
	for p, i := range order {
		if i == m.selected {
			if n := p + delta; n >= 0 && n < len(order) {
				m.selected = order[n]
				m.bumpGen()
			}
			return
		}
	}
}

// recentJobs lists finished jobs: failures from the last day first, then the
// rest, newest first.
func (m Model) recentJobs() []int {
	type item struct {
		i    int
		end  time.Time
		fail bool
	}
	now := time.Now()
	var items []item
	for i, j := range m.jobs {
		if !isTerminal(j.Status) {
			continue
		}
		end, ok := parseTS(j.EndedAt)
		if !ok {
			end, _ = parseTS(j.UpdatedAt)
		}
		items = append(items, item{i, end, isFailure(j.Status) && now.Sub(end) < 24*time.Hour})
	}
	sort.SliceStable(items, func(a, b int) bool {
		if items[a].fail != items[b].fail {
			return items[a].fail
		}
		return items[a].end.After(items[b].end)
	})
	out := make([]int, len(items))
	for k, it := range items {
		out[k] = it.i
	}
	return out
}
