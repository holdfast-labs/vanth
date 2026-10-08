package monitor

import "testing"

func TestJobDurationSeconds(t *testing.T) {
	job := JobSummary{StartedAt: "2026-01-01T00:00:00Z", EndedAt: "2026-01-01T00:02:00Z"}
	seconds, ok := job.DurationSeconds()
	if !ok || seconds != 120 {
		t.Fatalf("DurationSeconds = %v, %v; want 120, true", seconds, ok)
	}
	// The daemon always emits fractional seconds (now_iso microsecond
	// precision); durations must parse those, not just whole seconds.
	frac := JobSummary{StartedAt: "2026-01-01T00:00:00.123456Z", EndedAt: "2026-01-01T00:02:00.623456Z"}
	seconds, ok = frac.DurationSeconds()
	if !ok || seconds != 120.5 {
		t.Fatalf("DurationSeconds fractional = %v, %v; want 120.5, true", seconds, ok)
	}
	for _, bad := range []JobSummary{
		{},
		{StartedAt: "2026-01-01T00:00:00Z"},
		{StartedAt: "not-a-time", EndedAt: "2026-01-01T00:00:00Z"},
		{StartedAt: "2026-01-01T00:02:00Z", EndedAt: "2026-01-01T00:00:00Z"},
	} {
		if _, ok := bad.DurationSeconds(); ok {
			t.Fatalf("expected no duration for %+v", bad)
		}
	}
}

func TestSlowestJobsOrdersAndExcludes(t *testing.T) {
	m := Model{jobs: []JobSummary{
		{JobID: "job_fast", StartedAt: "2026-01-01T00:00:00Z", EndedAt: "2026-01-01T00:00:10Z"},
		{JobID: "job_slow", StartedAt: "2026-01-01T00:00:00Z", EndedAt: "2026-01-01T01:00:00Z"},
		{JobID: "job_mid", StartedAt: "2026-01-01T00:00:00Z", EndedAt: "2026-01-01T00:05:00Z"},
		{JobID: "job_running", Status: "running"},
		{JobID: "job_shadow", Shadow: true, StartedAt: "2026-01-01T00:00:00Z", EndedAt: "2026-01-01T09:00:00Z"},
	}}
	got := m.slowestJobs(2)
	if len(got) != 2 || got[0].JobID != "job_slow" || got[1].JobID != "job_mid" {
		t.Fatalf("slowestJobs = %+v", got)
	}
	// Asking for more than available returns all eligible rows.
	if all := m.slowestJobs(50); len(all) != 3 {
		t.Fatalf("slowestJobs(50) len = %d, want 3", len(all))
	}
}

func TestToggleSlowestIsExclusive(t *testing.T) {
	m := Model{showEvents: true}
	m.toggleSlowest()
	if !m.showSlowest || m.showEvents || m.showLogs {
		t.Fatalf("toggleSlowest on = %+v", m)
	}
	m.toggleEvents()
	if !m.showEvents || m.showSlowest {
		t.Fatalf("toggleEvents should clear slowest: %+v", m)
	}
}
