package monitor

import (
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
)

func ago(d time.Duration) string { return time.Now().Add(-d).UTC().Format(time.RFC3339Nano) }

// fixtureSeries samples f(u) for u in [0,1] every `every` from `from` ago to
// `to` ago.
func fixtureSeries(from, to, every time.Duration, f func(u float64, k int) float64) []Point {
	var out []Point
	n := int((from - to) / every)
	for k := 0; k <= n; k++ {
		u := float64(k) / float64(max(n, 1))
		out = append(out, Point{X: float64(k), Y: f(u, k), Seq: int64(k + 1), At: ago(from - time.Duration(k)*every)})
	}
	return out
}

func wobble(k, salt int) float64 {
	x := math.Sin(float64(k*7919+salt*104729)) * 43758.5453
	return x - math.Floor(x) - .5
}

// missionFixture mirrors the mission-control mockup: four running jobs (one stalled, one
// without progress), one queued, and finished runs with two failures. The
// training job declares panels.
func missionFixture() RefreshResult {
	res := RefreshResult{JobsOK: true, Mode: ModeSQLite, Points: map[SeriesKey][]Point{}, Events: map[string][]Event{}}
	add := func(j JobSummary) { res.Jobs = append(res.Jobs, j) }
	code1 := int64(1)
	add(JobSummary{JobID: "job_train", Name: "finetune-llama-8b", Status: "running", StartedAt: ago(18 * time.Minute),
		CreatedAt: ago(18 * time.Minute), RunJSON: `{"hostname":"gpu-02"}`})
	add(JobSummary{JobID: "job_eval", Name: "eval-suite", Status: "running", StartedAt: ago(25 * time.Minute), CreatedAt: ago(25 * time.Minute)})
	add(JobSummary{JobID: "job_ingest", Name: "ingest-s3-logs", Status: "running", StartedAt: ago(6 * time.Minute), CreatedAt: ago(6 * time.Minute)})
	add(JobSummary{JobID: "job_backup", Name: "nightly-backup", Status: "running", StartedAt: ago(3 * time.Minute), CreatedAt: ago(3 * time.Minute)})
	add(JobSummary{JobID: "job_q", Name: "render-thumbnails", Status: "queued", CreatedAt: ago(3 * time.Minute)})
	add(JobSummary{JobID: "job_deploy", Name: "deploy-staging", Status: "failed", ExitCode: &code1, StartedAt: ago(6 * time.Minute), EndedAt: ago(4 * time.Minute)})
	add(JobSummary{JobID: "job_flaky", Name: "flaky-integration", Status: "timeout", StartedAt: ago(42 * time.Minute), EndedAt: ago(12 * time.Minute)})
	add(JobSummary{JobID: "job_lint", Name: "lint", Status: "completed", StartedAt: ago(16 * time.Minute), EndedAt: ago(15 * time.Minute)})
	add(JobSummary{JobID: "job_unit", Name: "unit-tests", Status: "completed", StartedAt: ago(27 * time.Minute), EndedAt: ago(21 * time.Minute)})
	add(JobSummary{JobID: "job_docs", Name: "docs", Status: "completed", StartedAt: ago(42 * time.Minute), EndedAt: ago(41 * time.Minute)})
	add(JobSummary{JobID: "job_scratch", Name: "scratch-run", Status: "cancelled", StartedAt: ago(55 * time.Minute), EndedAt: ago(52 * time.Minute)})

	put := func(job, metric string, pts []Point) { res.Points[SeriesKey{JobID: job, Metric: metric}] = pts }
	progress := func(job string, from, to, every time.Duration, total, upto float64, unit, stage string) {
		pct := fixtureSeries(from, to, every, func(u float64, _ int) float64 { return upto * u })
		put(job, "progress.percent", pct)
		var cur []Point
		for _, p := range pct {
			c := math.Round(p.Y / 100 * total)
			cur = append(cur, Point{X: p.X, Y: c, Seq: p.Seq, At: p.At})
			res.Events[job] = append(res.Events[job], Event{EventID: fmt.Sprint(job, p.Seq), JobID: job, Seq: p.Seq, Type: "progress",
				Message: fmt.Sprintf("%s %.0f/%.0f %s", stage, c, total, unit), CreatedAt: p.At,
				Data: map[string]any{"current": c, "total": total, "unit": unit, "stage": stage}})
		}
		put(job, "progress.current", cur)
	}
	progress("job_train", 18*time.Minute, 2*time.Second, 10*time.Second, 5000, 62, "step", "train")
	progress("job_eval", 25*time.Minute, 9*time.Minute, 10*time.Second, 1200, 40, "cases", "eval")
	progress("job_ingest", 6*time.Minute, time.Second, 5*time.Second, 4000, 31, "files", "download")
	for k := 0; k < 36; k++ {
		at := ago(3*time.Minute - time.Duration(k)*5*time.Second)
		res.Events["job_backup"] = append(res.Events["job_backup"], Event{EventID: fmt.Sprint("b", k), JobID: "job_backup",
			Seq: int64(k + 1), Type: "log", Message: "copying /var/lib/postgres", CreatedAt: at})
	}
	res.Events["job_train"][len(res.Events["job_train"])-1].Data["phase"] = "epoch 3/5"
	res.Events["job_train"] = append([]Event{{EventID: "panels", JobID: "job_train", Seq: 0, Type: "panels",
		CreatedAt: ago(18 * time.Minute), Data: map[string]any{"panels": []any{
			map[string]any{"kind": "line", "metric": "loss", "label": "train/loss"},
			map[string]any{"kind": "line", "metric": "acc", "label": "accuracy"},
			map[string]any{"kind": "gauge", "metric": "gpu", "label": "gpu util", "unit": "%"},
			map[string]any{"kind": "line", "metric": "gpu_mem", "label": "gpu mem", "unit": "GB"},
			map[string]any{"kind": "stat", "metric": "tokens_s", "label": "tokens/s"},
			map[string]any{"kind": "line", "metric": "lr", "label": "learning rate"},
			map[string]any{"kind": "text", "key": "phase", "label": "phase"},
		}}}}, res.Events["job_train"]...)

	tr := func(metric string, f func(u float64, k int) float64) {
		put("job_train", metric, fixtureSeries(18*time.Minute, 2*time.Second, 10*time.Second, f))
	}
	tr("loss", func(u float64, k int) float64 { return 2.3*math.Exp(-u*4.2) + .19 + wobble(k, 1)*.12*(1-u*.85) })
	tr("acc", func(u float64, k int) float64 { return .914 - .52*math.Exp(-u*3.5) + wobble(k, 2)*.02 })
	tr("gpu", func(u float64, k int) float64 { return 92 + wobble(k, 4)*8 })
	tr("gpu_mem", func(u float64, k int) float64 { return math.Min(61, 20+u*120) + wobble(k, 12)*.6 })
	tr("tokens_s", func(u float64, k int) float64 { return math.Round(1930 + wobble(k, 7)*90 + 120*math.Sin(u*30)) })
	tr("lr", func(u float64, k int) float64 {
		if u < .05 {
			return 3e-4 * u / .05
		}
		return 3e-4 * (.5 + .5*math.Cos(math.Pi*(u-.05)/.95))
	})
	ev := func(metric string, f func(u float64, k int) float64) {
		put("job_eval", metric, fixtureSeries(25*time.Minute, 9*time.Minute, 10*time.Second, f))
	}
	ev("acc", func(u float64, k int) float64 { return .86 + wobble(k, 3)*.03 })
	ev("gpu", func(u float64, k int) float64 { return 71 + wobble(k, 6)*14 })
	ev("gpu_mem", func(u float64, k int) float64 { return math.Min(38, 10+u*90) + wobble(k, 13)*.4 })
	ev("cases_min", func(u float64, k int) float64 { return 30 + wobble(k, 14)*8 })
	put("job_ingest", "mb_s", fixtureSeries(6*time.Minute, time.Second, 5*time.Second, func(u float64, k int) float64 {
		return 24 + 6*math.Sin(u*20) + wobble(k, 8)*4
	}))
	return res
}

// fixtureModel is a dark-background model at w x h holding missionFixture.
func fixtureModel(t *testing.T, w, h int) Model {
	t.Helper()
	m := New(DefaultConfig(filepath.Join(t.TempDir(), "home")), nil)
	m = apply(t, m, tea.WindowSizeMsg{Width: w, Height: h})
	m = apply(t, m, tea.BackgroundColorMsg{Color: blackColor})
	return apply(t, m, refreshMsg{Gen: m.gen, Result: missionFixture()})
}
