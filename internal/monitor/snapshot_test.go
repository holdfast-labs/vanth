package monitor

import (
	"context"
	"image/color"
	"os"
	"path/filepath"
	"testing"

	tea "charm.land/bubbletea/v2"
)

// TestWriteSnapshots renders real glance and detail frames (with colour escapes) to
// VANTH_MONITOR_SNAPSHOT_DIR for visual review; scripts/monitor_screenshots.py
// turns them into PNGs. Opt-in: reads VANTH_MONITOR_E2E_HOME (or VANTH_HOME).
func TestWriteSnapshots(t *testing.T) {
	dir := os.Getenv("VANTH_MONITOR_SNAPSHOT_DIR")
	home := os.Getenv("VANTH_MONITOR_E2E_HOME")
	if home == "" {
		home = os.Getenv("VANTH_HOME")
	}
	if dir == "" || home == "" {
		t.Skip("set VANTH_MONITOR_SNAPSHOT_DIR and VANTH_MONITOR_E2E_HOME (or VANTH_HOME) to write snapshots")
	}
	cfg := DefaultConfig(home)
	q := NewQuerier(cfg)
	defer q.Close()

	for _, size := range []struct {
		name string
		w, h int
	}{{"large", 160, 44}, {"medium", 120, 36}, {"narrow", 80, 30}, {"small", 80, 24}} {
		m := New(cfg, q)
		updated, _ := m.Update(tea.WindowSizeMsg{Width: size.w, Height: size.h})
		m = updated.(Model)
		// The PNGs are drawn on a dark page; tell the model so it picks the dark palette.
		updated, _ = m.Update(tea.BackgroundColorMsg{Color: color.Black})
		m = updated.(Model)
		// Two cycles, as the live loop does: jobs first, then the selected job's events.
		for cycle := 0; cycle < 2; cycle++ {
			res := q.Refresh(context.Background(), RefreshRequest{Targets: m.loadTargets(), LastSeq: m.lastSeqSnapshot()})
			if res.Err != nil {
				t.Fatalf("refresh: %v", res.Err)
			}
			updated, _ = m.Update(refreshMsg{Gen: m.gen, Result: res})
			m = updated.(Model)
		}
		write := func(name string) {
			if err := os.WriteFile(filepath.Join(dir, name+".ans"), []byte(m.render()), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		write("glance-" + size.name)
		m.detail = true
		write("detail-" + size.name)
		if size.name == "large" {
			m.detail, m.help = false, true
			write("help")
		}
	}
}
