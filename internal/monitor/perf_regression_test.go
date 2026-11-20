package monitor

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"modernc.org/sqlite"

	"vanth/internal/state"
)

// TestView160x44Allocations pins the per-frame allocation count of the
// mission-control renderer. A regression that reintroduces a lipgloss Style
// per line (or another per-cell allocation) raises this even when wall-clock
// timing is noisy on a loaded CI runner.
//
// Measured 2026-11-19 on this machine: 8253 allocs/op (BenchmarkView160x44);
// AllocsPerRun reports 8252. The budget is ~27% headroom.
func TestView160x44Allocations(t *testing.T) {
	m := fixtureModel(t, 160, 44)
	const budget = 10500
	allocs := testing.AllocsPerRun(50, func() { _ = m.View() })
	t.Logf("View(160x44) allocs/op = %.0f (budget %d)", allocs, budget)
	if allocs > budget {
		t.Fatalf("View(160x44) allocs/op = %.0f, budget %d (measured 8253)", allocs, budget)
	}
}

// TestRefreshLargeHomeAllocations pins the allocation count of one monitor
// refresh against a long-lived home. The perf pass removed a correlated
// COUNT(*) over each job's events; reintroducing it (or another per-job
// allocation) raises this count independently of machine load.
//
// Measured 2026-11-19 on this machine: 3772 allocs/op
// (BenchmarkRefreshLargeHome, 1200 jobs / 120k events); AllocsPerRun reports
// 3751. The budget is ~28% headroom.
func TestRefreshLargeHomeAllocations(t *testing.T) {
	path, ids := largeHome(t, 1200, 100)
	q := NewQuerier(testCfgFor(path))
	defer q.Close()
	req := RefreshRequest{Targets: ids[len(ids)-8:], LastSeq: map[string]int64{}}
	for _, id := range req.Targets {
		req.LastSeq[id] = 100
	}
	if res := q.Refresh(context.Background(), req); !res.JobsOK {
		t.Fatalf("warm-up refresh failed: stale=%v fallback=%v open=%v", res.Stale, res.Fallback, res.DBOpen)
	}
	const budget = 4800
	allocs := testing.AllocsPerRun(10, func() {
		_ = q.Refresh(context.Background(), req)
	})
	t.Logf("Refresh allocs/op = %.0f (budget %d)", allocs, budget)
	if allocs > budget {
		t.Fatalf("Refresh allocs/op = %.0f, budget %d (measured 3772)", allocs, budget)
	}
}

// countingConnector wraps modernc's driver so the test can count prepared
// statements without touching production code. The wrapper deliberately does
// not implement driver.QueryerContext/ExecerContext, so database/sql falls
// back to PrepareContext and one count equals one statement execution.
type countingConnector struct {
	dsn   string
	drv   *sqlite.Driver
	count *int64
}

func (c *countingConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.drv.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	return &countingConn{Conn: conn, count: c.count}, nil
}

func (c *countingConnector) Driver() driver.Driver { return c.drv }

type countingConn struct {
	driver.Conn
	count *int64
}

func (c *countingConn) Prepare(query string) (driver.Stmt, error) {
	atomic.AddInt64(c.count, 1)
	return c.Conn.Prepare(query)
}

func (c *countingConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	atomic.AddInt64(c.count, 1)
	if pc, ok := c.Conn.(driver.ConnPrepareContext); ok {
		return pc.PrepareContext(ctx, query)
	}
	return c.Conn.Prepare(query)
}

// TestRefreshQueryCountIsBoundedByJobCount guards the job-list query against
// reintroducing a per-job statement (the removed correlated COUNT was one).
// A refresh must issue one job-list query plus one event query per target,
// whatever the size of the jobs table.
func TestRefreshQueryCountIsBoundedByJobCount(t *testing.T) {
	var counts []int64
	for _, jobs := range []int{50, 500} {
		path, ids := largeHome(t, jobs, 5)
		cfg := testCfgFor(path)
		cfg.JobLimit = 100
		var counter int64
		q := NewQuerier(cfg)
		q.db = sql.OpenDB(&countingConnector{
			dsn:   fmt.Sprintf("file:%s?mode=ro&_pragma=busy_timeout(%d)&_pragma=query_only(1)", path, 50),
			drv:   &sqlite.Driver{},
			count: &counter,
		})
		targets := ids[:4]
		req := RefreshRequest{Targets: targets, LastSeq: map[string]int64{}}
		if res := q.Refresh(context.Background(), req); !res.JobsOK {
			t.Fatalf("%d jobs: warm-up refresh failed: stale=%v fallback=%v open=%v", jobs, res.Stale, res.Fallback, res.DBOpen)
		}
		atomic.StoreInt64(&counter, 0)
		res := q.Refresh(context.Background(), req)
		if !res.JobsOK {
			t.Fatalf("%d jobs: refresh failed: stale=%v fallback=%v open=%v", jobs, res.Stale, res.Fallback, res.DBOpen)
		}
		counts = append(counts, atomic.LoadInt64(&counter))
		q.Close()
	}
	t.Logf("refresh statements: 50 jobs=%d, 500 jobs=%d", counts[0], counts[1])
	if counts[0] != counts[1] {
		t.Fatalf("refresh query count grew with job count: 50 jobs=%d, 500 jobs=%d", counts[0], counts[1])
	}
	if want := int64(1 + 4); counts[0] > want+2 {
		t.Fatalf("refresh issued %d statements, want <= %d (1 job list + 4 event windows)", counts[0], want+2)
	}
}

// TestMonitorQueriesUseIndexes guards the monitor's SQL plans. The job-list
// query must never scan `events` per job (the removed correlated COUNT did),
// and the incremental event query must use idx_events_job_seq.
func TestMonitorQueriesUseIndexes(t *testing.T) {
	path := fixtureCopy(t)
	db, err := state.OpenReadOnly(path)
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	defer db.Close()

	plan := queryPlan(t, db, jobListQuery, 100)
	if strings.Contains(plan, "SCAN events") {
		t.Fatalf("job-list query scans events per job: %s", plan)
	}
	if !strings.Contains(plan, "SEARCH j USING INTEGER PRIMARY KEY") {
		t.Fatalf("job-list query does not fetch the bounded rowids by primary key: %s", plan)
	}

	eventPlan := queryPlan(t, db, eventsQuery, "job_completed", 0, 5000)
	if strings.Contains(eventPlan, "SCAN events") || !strings.Contains(eventPlan, "idx_events_job_seq") {
		t.Fatalf("event query does not use idx_events_job_seq: %s", eventPlan)
	}
}

func queryPlan(t *testing.T, db *sql.DB, query string, args ...any) string {
	t.Helper()
	rows, err := db.Query("EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		t.Fatalf("explain query plan: %v", err)
	}
	defer rows.Close()
	var parts []string
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		if err := rows.Scan(&id, &parent, &notUsed, &detail); err != nil {
			t.Fatalf("scan plan row: %v", err)
		}
		parts = append(parts, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("plan rows: %v", err)
	}
	return strings.Join(parts, " | ")
}
