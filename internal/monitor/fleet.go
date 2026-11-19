package monitor

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"charm.land/lipgloss/v2"
)

// timelineWindow picks the time window to draw: the smallest of 1h/6h/24h/7d/30d
// ending at the newest start that still holds 80% of the jobs, or the full span.
func timelineWindow(times []time.Time) (lo, hi time.Time) {
	for i, t := range times {
		if i == 0 || t.Before(lo) {
			lo = t
		}
		if t.After(hi) {
			hi = t
		}
	}
	for _, w := range []time.Duration{time.Hour, 6 * time.Hour, 24 * time.Hour, 7 * 24 * time.Hour, 30 * 24 * time.Hour} {
		n := 0
		for _, t := range times {
			if !t.Before(hi.Add(-w)) {
				n++
			}
		}
		if n*100 >= len(times)*80 {
			if cand := hi.Add(-w); cand.After(lo) {
				lo = cand
			}
			return lo, hi
		}
	}
	return lo, hi
}

func stampLabel(t time.Time, span time.Duration) string {
	if span >= 7*24*time.Hour {
		return t.Format("Jan 02")
	}
	return t.Format("Jan 02 15:04")
}

// renderTimeline is a stacked histogram of when jobs started: column height is
// the job count in that time bucket, color is the worst status in it. It zooms
// to the window where most jobs are, and labels both time-axis ends.
func (m Model) renderTimeline(r rect) string {
	title := boldStyle.Foreground(titleColor(m.dark)).Render("Timeline")
	innerW, rows := r.w-2, r.h-2
	if innerW < 2 || rows < 1 {
		return m.panelBox(r, title, "")
	}
	type item struct {
		t      time.Time
		status string
	}
	var items []item
	var times []time.Time
	for _, j := range m.jobs {
		if t, ok := jobStart(j); ok {
			items = append(items, item{t, j.Status})
			times = append(times, t)
		}
	}
	if len(items) == 0 {
		return m.panelBox(r, title, fillLine(dimStyle.Render("no start times yet"), innerW))
	}
	lo, hi := timelineWindow(times)
	span := hi.Sub(lo)
	counts := make([]int, innerW)
	worst := make([]int, innerW)
	status := make([]string, innerW)
	older, shown := 0, 0
	for _, it := range items {
		if it.t.Before(lo) {
			older++
			continue
		}
		shown++
		b := 0
		if span > 0 {
			b = min(int(float64(it.t.Sub(lo))/float64(span)*float64(innerW)), innerW-1)
		}
		counts[b]++
		if sv := severity(it.status); status[b] == "" || sv > worst[b] {
			worst[b], status[b] = sv, it.status
		}
	}
	peak := 1
	for _, c := range counts {
		peak = max(peak, c)
	}
	levels := []rune(" ▁▂▃▄▅▆▇█")
	lines := make([]string, rows)
	for row := 0; row < rows; row++ {
		var b strings.Builder
		for x := 0; x < innerW; x++ {
			if counts[x] == 0 {
				if row == rows-1 {
					b.WriteString(dimStyle.Render("·"))
				} else {
					b.WriteByte(' ')
				}
				continue
			}
			units := max(1, counts[x]*rows*8/peak) - (rows-1-row)*8
			units = clamp(units, 0, 8)
			b.WriteString(lipgloss.NewStyle().Foreground(statusColor(status[x], m.dark)).Render(string(levels[units])))
		}
		lines[row] = b.String()
	}
	hint := fmt.Sprintf("%d jobs · peak %d", shown, peak)
	if older > 0 {
		hint += fmt.Sprintf(" · +%d older", older)
	}
	return m.panelBoxOpts(r, title, strings.Join(lines, "\n"), panelOpts{
		hint: hint, bottomL: stampLabel(lo, span), bottomR: stampLabel(hi, span)})
}

// renderFleet fills the plots area with fleet-level visuals: the timeline, a
// bar chart of recent run durations colored by status, and an outcome strip for
// jobs sharing the selected job's name.
func (m Model) renderFleet(r rect) string {
	if r.w < 12 || r.h < 3 {
		return ""
	}
	hist := m.sameNameJobs()
	hh := 0
	if len(hist) >= 2 && r.h >= 16 {
		hh = 4
	}
	th := max(r.h*42/100, 5)
	dh := r.h - th - hh
	if dh < 5 {
		th, dh = r.h-hh, 0
	}
	parts := []string{m.renderTimeline(rect{w: r.w, h: th})}
	if dh > 0 {
		parts = append(parts, m.renderDurations(rect{w: r.w, h: dh}))
	}
	if hh > 0 {
		parts = append(parts, m.renderHistory(rect{w: r.w, h: hh}, hist))
	}
	return strings.Join(parts, "\n")
}

// sameNameJobs lists jobs sharing the selected job's display name, oldest first.
func (m Model) sameNameJobs() []JobSummary {
	sel, ok := m.selectedJob()
	if !ok {
		return nil
	}
	var out []JobSummary
	for _, j := range m.jobs {
		if j.DisplayName() == sel.DisplayName() {
			out = append(out, j)
		}
	}
	sort.SliceStable(out, func(a, b int) bool {
		ta, _ := jobStart(out[a])
		tb, _ := jobStart(out[b])
		return ta.Before(tb)
	})
	return out
}

// renderHistory is a run-outcome strip: one colored glyph per run, oldest to
// newest, with a duration sparkline underneath.
func (m Model) renderHistory(r rect, jobs []JobSummary) string {
	name := ""
	if len(jobs) > 0 {
		name = jobs[0].DisplayName()
	}
	title := boldStyle.Foreground(titleColor(m.dark)).Render("History · " + truncate(name, max(r.w-16, 4)))
	innerW := r.w - 2
	if len(jobs) > innerW/2 {
		jobs = jobs[len(jobs)-innerW/2:]
	}
	var glyphs strings.Builder
	var durs []float64
	for _, j := range jobs {
		glyphs.WriteString(lipgloss.NewStyle().Bold(true).Foreground(statusColor(j.Status, m.dark)).Render(statusIcon(j.Status)) + " ")
		d, _ := j.DurationSeconds()
		durs = append(durs, d)
	}
	spark := dimStyle.Render(sparkline(durs, innerW))
	return m.panelBoxOpts(r, title, fillLine(glyphs.String(), innerW)+"\n"+fillLine(spark, innerW),
		panelOpts{hint: fmt.Sprintf("%d runs", len(jobs))})
}

// renderDurations is a horizontal bar chart of the most recent runs: bar length
// is runtime, color is status. Running jobs show their age as a hatched bar.
func (m Model) renderDurations(r rect) string {
	title := boldStyle.Foreground(titleColor(m.dark)).Render("Recent runs")
	innerW, rows := r.w-2, r.h-2
	type run struct {
		j      JobSummary
		d      time.Duration
		active bool
		start  time.Time
	}
	var runs []run
	for _, j := range m.jobs {
		st, ok := jobStart(j)
		if !ok {
			continue
		}
		if sec, ok := j.DurationSeconds(); ok {
			runs = append(runs, run{j, time.Duration(sec * float64(time.Second)), false, st})
		} else if j.Status == "running" {
			runs = append(runs, run{j, time.Since(st), true, st})
		}
	}
	if len(runs) == 0 {
		return m.panelBox(r, title, fillLine(dimStyle.Render("no finished runs yet"), innerW))
	}
	sort.SliceStable(runs, func(a, b int) bool { return runs[a].start.After(runs[b].start) })
	if len(runs) > rows {
		runs = runs[:rows]
	}
	var peak time.Duration
	for _, rn := range runs {
		peak = max(peak, rn.d)
	}
	nameW := clamp(innerW/3, 8, 26)
	barW := innerW - nameW - 10
	if barW < 4 {
		barW = 0
	}
	lines := make([]string, 0, len(runs))
	for _, rn := range runs {
		col := lipgloss.NewStyle().Foreground(statusColor(rn.j.Status, m.dark))
		val := compactDuration(rn.d)
		if rn.active {
			val += "+"
		}
		line := col.Render(statusIcon(rn.j.Status)) + " " + fmt.Sprintf("%-*s", nameW, truncate(rn.j.DisplayName(), nameW))
		if barW > 0 {
			n := 0
			if peak > 0 {
				n = max(1, int(float64(rn.d)/float64(peak)*float64(barW)))
			}
			ch := "█"
			if rn.active {
				ch = "▒"
			}
			line += " " + col.Render(strings.Repeat(ch, n)) + strings.Repeat(" ", barW-n)
		}
		line += " " + dimStyle.Render(fmt.Sprintf("%5s", val))
		lines = append(lines, fillLine(line, innerW))
	}
	return m.panelBoxOpts(r, title, strings.Join(lines, "\n"),
		panelOpts{hint: fmt.Sprintf("last %d · longest %s", len(runs), compactDuration(peak))})
}
