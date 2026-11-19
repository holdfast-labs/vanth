package monitor

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// largeHome copies the schema fixture and fills it with jobs x eventsPerJob
// metric/progress events, the size of a long-lived real home.
func largeHome(b *testing.B, jobs, eventsPerJob int) (string, []string) {
	b.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "state", "jobs.sqlite"))
	if err != nil {
		b.Fatalf("read fixture: %v", err)
	}
	path := filepath.Join(b.TempDir(), "jobs.sqlite")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		b.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}
	base := time.Now().Add(-72 * time.Hour)
	ids := make([]string, 0, jobs)
	for i := 0; i < jobs; i++ {
		id := fmt.Sprintf("job_bench%07d", i)
		ids = append(ids, id)
		at := base.Add(time.Duration(i) * time.Minute).UTC().Format(time.RFC3339Nano)
		if _, err := tx.Exec(`INSERT INTO jobs(job_id,name,command,status,created_at,updated_at,stdout_path,stderr_path,events_path,tags_json)
			VALUES (?,?,?,?,?,?,?,?,?,'[]')`, id, "train", "python train.py", "completed", at, at, "o", "e", "ev"); err != nil {
			b.Fatal(err)
		}
		for s := 1; s <= eventsPerJob; s++ {
			typ, payload := "metric", fmt.Sprintf(`{"_step":%d,"loss":%g}`, s, 1/float64(s))
			if s%2 == 0 {
				typ, payload = "progress", fmt.Sprintf(`{"current":%d,"total":%d}`, s, eventsPerJob)
			}
			if _, err := tx.Exec(`INSERT INTO events(event_id,job_id,seq,type,level,data_json,source,created_at)
				VALUES (?,?,?,?,'info',?,'agent',?)`, fmt.Sprintf("evt_%s_%d", id, s), id, s, typ, payload, at); err != nil {
				b.Fatal(err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	return path, ids
}

// BenchmarkRefreshLargeHome is one steady-state monitor refresh (no new
// events) against 1200 jobs / 120k events with 8 chart targets.
func BenchmarkRefreshLargeHome(b *testing.B) {
	path, ids := largeHome(b, 1200, 100)
	q := NewQuerier(testCfgFor(path))
	defer q.Close()
	req := RefreshRequest{Targets: ids[len(ids)-8:], LastSeq: map[string]int64{}}
	for _, id := range req.Targets {
		req.LastSeq[id] = 100
	}
	b.ReportAllocs()
	for b.Loop() {
		if res := q.Refresh(context.Background(), req); !res.JobsOK {
			b.Fatalf("refresh failed: stale=%v fallback=%v open=%v", res.Stale, res.Fallback, res.DBOpen)
		}
	}
}

// BenchmarkView160x44 renders one mission-control frame.
func BenchmarkView160x44(b *testing.B) {
	m := fixtureModel(b, 160, 44)
	b.ReportAllocs()
	for b.Loop() {
		_ = m.View()
	}
}
