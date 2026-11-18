package monitor

import (
	"context"
	"image/color"
	"os"
	"path/filepath"
	"testing"

	tea "charm.land/bubbletea/v2"
)

var blackColor = color.Black

var snapshotSizes = []struct {
	name string
	w, h int
}{{"160x44", 160, 44}, {"120x36", 120, 36}, {"100x30", 100, 30}, {"80x24", 80, 24}}

// TestWriteSnapshots renders monitor frames (with colour escapes) to
// VANTH_MONITOR_SNAPSHOT_DIR for visual review; scripts/monitor_screenshots.py
// turns them into PNGs. Opt-in. It always renders the built-in fixture and, when
// VANTH_MONITOR_E2E_HOME (or VANTH_HOME) is set, that home read-only.
func TestWriteSnapshots(t *testing.T) {
	dir := os.Getenv("VANTH_MONITOR_SNAPSHOT_DIR")
	if dir == "" {
		t.Skip("set VANTH_MONITOR_SNAPSHOT_DIR (and optionally VANTH_MONITOR_E2E_HOME) to write snapshots")
	}
	write := func(m Model, name string) {
		if err := os.WriteFile(filepath.Join(dir, name+".ans"), []byte(m.render()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, size := range snapshotSizes {
		m := leetModel(t, size.w, size.h)
		write(m, "fixture-"+size.name)
		if size.w == 160 {
			m.help = true
			write(m, "fixture-help")
		}
	}

	home := os.Getenv("VANTH_MONITOR_E2E_HOME")
	if home == "" {
		home = os.Getenv("VANTH_HOME")
	}
	if home == "" {
		return
	}
	cfg := DefaultConfig(home)
	q := NewQuerier(cfg)
	defer q.Close()
	for _, size := range snapshotSizes {
		m := New(cfg, q)
		m = apply(t, m, tea.WindowSizeMsg{Width: size.w, Height: size.h})
		// The PNGs are drawn on a dark page; tell the model so it picks the dark palette.
		m = apply(t, m, tea.BackgroundColorMsg{Color: blackColor})
		// Two cycles, as the live loop does: jobs first, then the selected job's events.
		for cycle := 0; cycle < 2; cycle++ {
			res := q.Refresh(context.Background(), RefreshRequest{Targets: m.loadTargets(), LastSeq: m.lastSeqSnapshot()})
			if res.Err != nil {
				t.Fatalf("refresh: %v", res.Err)
			}
			m = apply(t, m, refreshMsg{Gen: m.gen, Result: res})
		}
		write(m, "home-"+size.name)
	}
}
