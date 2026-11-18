package monitor

import (
	"fmt"
	"image/color"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"charm.land/lipgloss/v2"
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

// card is one active job's slot in the running panel.
type card struct {
	job   int // index into m.jobs
	level int // 0: one row; 1: header, bar, step; 2/3: plus panels (2 or 4 rows)
	y     int // first screen row
	h     int
}

// chip is one finished job's slot in the recent strip.
type chip struct {
	job     int
	x, y, w int
	text    string
}

type glanceLayout struct {
	header, main, recent, lower rect
	cards                       []card
	hiddenCards                 int
	chips                       []chip
	moreChips                   int
}

// cardHeight is the row count of a card at a level.
func (m Model) cardHeight(i, level, innerW int) int {
	if level == 0 {
		return 1
	}
	base := 3
	if m.jobs[i].Status != "running" && m.jobs[i].Status != "launching" {
		base = 2
	}
	if level == 1 {
		return base
	}
	specs, _ := m.jobPanels(m.jobs[i].JobID)
	cellH := 2
	if level == 3 {
		cellH = 4
	}
	return base + panelGridRows(len(specs), innerW-2, cellH)
}

// fitCards picks a level for every card so they fill avail rows: everything
// starts as one row, then the selected card and the rest are upgraded a level
// at a time while they fit. When even one-row cards overflow, a window of
// cards around the selection is shown.
func (m Model) fitCards(active []int, innerW, avail int) ([]card, int) {
	if len(active) == 0 || avail <= 0 {
		return nil, len(active)
	}
	cards := make([]card, len(active))
	for k, i := range active {
		cards[k] = card{job: i}
	}
	total := func(cs []card) int {
		n := 0
		for k := range cs {
			cs[k].h = m.cardHeight(cs[k].job, cs[k].level, innerW)
			n += cs[k].h
			if k > 0 && (cs[k].level > 0 || cs[k-1].level > 0) {
				n++ // a blank row between multi-row cards
			}
		}
		return n
	}
	if total(cards) > avail {
		start := 0
		for k, c := range cards {
			if c.job == m.selected && k >= avail {
				start = k - avail + 1
			}
		}
		end := min(start+avail, len(cards))
		shown := cards[start:end]
		total(shown)
		return shown, len(cards) - len(shown)
	}
	order := make([]int, 0, len(cards))
	for k, c := range cards {
		if c.job == m.selected {
			order = append([]int{k}, order...)
		} else {
			order = append(order, k)
		}
	}
	for level := 1; level <= 3; level++ {
		for _, k := range order {
			prev := cards[k].level
			if prev >= level {
				continue
			}
			before := m.cardHeight(cards[k].job, level-1, innerW)
			cards[k].level = level
			if level > 1 && m.cardHeight(cards[k].job, level, innerW) == before {
				cards[k].level = prev // no panels: nothing to gain
				continue
			}
			if total(cards) > avail {
				cards[k].level = prev
			}
		}
	}
	total(cards)
	return cards, 0
}

// recentChipText is the label of a finished run in the strip.
func recentChipText(j JobSummary) string {
	t := statusIcon(j.Status) + " " + truncate(j.DisplayName(), 24)
	if isFailure(j.Status) {
		if j.ExitCode != nil && *j.ExitCode != 0 {
			return t + fmt.Sprintf(" exit %d", *j.ExitCode)
		}
		return t + " " + j.Status
	}
	if d, ok := j.DurationSeconds(); ok {
		t += " " + compactDuration(time.Duration(d*float64(time.Second)))
	}
	return t
}

// flowChips places chips left to right over rows lines of width w, starting
// at recent[start]. It returns the placed chips and how many did not fit.
func (m Model) flowChips(recent []int, start int, r rect) ([]chip, int) {
	innerW, rows := r.w-2, r.h-2
	var out []chip
	x, y := 0, 0
	for k := start; k < len(recent); k++ {
		text := recentChipText(m.jobs[recent[k]])
		w := lipgloss.Width(text)
		left := len(recent) - k - 1
		reserve := 0
		if left > 0 && y == rows-1 {
			reserve = len(fmt.Sprintf("  +%d", left))
		}
		if x > 0 && x+3+w+reserve > innerW {
			x, y = 0, y+1
			if y == rows-1 && left > 0 {
				reserve = len(fmt.Sprintf("  +%d", left))
			}
		}
		if y >= rows || w+reserve > innerW {
			return out, len(recent) - k
		}
		if x > 0 {
			x += 3
		}
		if x+w+reserve > innerW {
			return out, len(recent) - k
		}
		out = append(out, chip{job: recent[k], x: r.x + 1 + x, y: r.y + 1 + y, w: w, text: text})
		x += w
	}
	return out, 0
}

// computeGlance lays out the glance view for the current size.
func (m Model) computeGlance() glanceLayout {
	w, h := m.width, m.height
	var g glanceLayout
	hh := stripH
	if w >= tilesMinW && h >= tilesMinH {
		hh = tilesH
	}
	g.header = rect{w: w, h: hh}
	recent := m.recentJobs()
	recentH := 0
	if len(recent) > 0 {
		recentH = 3
		if h >= 36 {
			// A second row of chips once the first one is full.
			if _, more := m.flowChips(recent, 0, rect{w: w, h: 3}); more > 0 {
				recentH = 4
			}
		}
	}
	lowerH := 0
	if m.lowerExpanded() {
		lowerH = clamp((h-statusH)*35/100, 6, 18)
	} else if h >= 40 {
		lowerH = tickerH
	}
	gap := func(n int) int {
		if n > 0 {
			return panelGap
		}
		return 0
	}
	place := func() int {
		return h - statusH - lowerH - gap(lowerH) - recentH - gap(recentH) - (hh + panelGap)
	}
	if place() < 5 && lowerH > 0 && !m.lowerExpanded() {
		lowerH = 0
	}
	if place() < 4 && hh == tilesH {
		hh = stripH
		g.header.h = hh
	}
	mainH := max(place(), 2)
	g.main = rect{y: hh + panelGap, w: w, h: mainH}
	y := g.main.y + mainH
	if recentH > 0 {
		g.recent = rect{y: y + panelGap, w: w, h: recentH}
		y = g.recent.y + recentH
	}
	if lowerH > 0 {
		g.lower = rect{y: y + panelGap, w: w, h: lowerH}
	}

	cards, hidden := m.fitCards(m.activeJobs(time.Now()), w-2, mainH-2)
	cy := g.main.y + 1
	for k := range cards {
		if k > 0 && (cards[k].level > 0 || cards[k-1].level > 0) {
			cy++
		}
		cards[k].y = cy
		cy += cards[k].h
	}
	g.cards, g.hiddenCards = cards, hidden

	if recentH > 0 {
		g.chips, g.moreChips = m.flowChips(recent, 0, g.recent)
		selShown := false
		for _, c := range g.chips {
			selShown = selShown || c.job == m.selected
		}
		if !selShown {
			for k, i := range recent {
				if i == m.selected {
					g.chips, g.moreChips = m.flowChips(recent, k, g.recent)
				}
			}
		}
	}
	return g
}

// renderGlance draws the glance view (everything above the status bar).
func (m Model) renderGlance() string {
	g := m.computeGlance()
	parts := []string{m.renderHeader(g.header), ""}
	if len(g.cards) > 0 || g.hiddenCards > 0 {
		parts = append(parts, m.renderCards(g))
	} else {
		parts = append(parts, m.renderIdle(g.main))
	}
	if g.recent.h > 0 {
		parts = append(parts, "", m.renderRecent(g))
	}
	if g.lower.h > 0 {
		lower := m.renderTicker(g.lower)
		if m.lowerExpanded() {
			lower = m.renderLower(g.lower)
		}
		parts = append(parts, "", lower)
	}
	return padToHeight(strings.Join(parts, "\n"), m.height-statusH)
}

// renderCards draws the running panel.
func (m Model) renderCards(g glanceLayout) string {
	r := g.main
	innerW := r.w - 2
	s := m.summary()
	title := lipgloss.NewStyle().Bold(true).Foreground(statusColor("running", m.dark)).Render(fmt.Sprintf("%s Running %d", statusIcon("running"), s.running))
	if s.queued > 0 {
		title += dimStyle.Render(fmt.Sprintf(" · %s queued %d", statusIcon("queued"), s.queued))
	}
	lines := make([]string, 0, r.h-2)
	now := time.Now()
	for k, c := range g.cards {
		if k > 0 && (c.level > 0 || g.cards[k-1].level > 0) {
			lines = append(lines, "")
		}
		lines = append(lines, m.cardLines(c, innerW, now)...)
	}
	for i := range lines {
		lines[i] = fillLine(lines[i], innerW)
	}
	hint := "v detail"
	if g.hiddenCards > 0 {
		hint = fmt.Sprintf("+%d more · ↑↓ · v detail", g.hiddenCards)
	}
	return m.panelBoxOpts(r, title, strings.Join(lines, "\n"), panelOpts{
		hint: hint, focused: m.focus == focusJobs, pulse: m.pulsePhase(s.running)})
}

var spinner = []string{"◐", "◓", "◑", "◒"}

// livenessText is the right-hand liveness readout and its colour.
func (m Model) livenessText(state liveness, since time.Duration) string {
	switch state {
	case liveStalled:
		return lipgloss.NewStyle().Bold(true).Foreground(statusColor("failed", m.dark)).Render("✖ STALLED " + compactDuration(since))
	case liveLate:
		return lipgloss.NewStyle().Foreground(statusColor("cancelled", m.dark)).Render("◔ late " + compactDuration(since))
	case liveQuiet:
		return dimStyle.Render("○ quiet " + compactDuration(since))
	case liveFresh:
		return lipgloss.NewStyle().Foreground(statusColor("running", m.dark)).Render("● " + compactDuration(since) + " ago")
	}
	return ""
}

// cardLines renders one card: exactly c.h lines of at most innerW cells.
func (m Model) cardLines(c card, innerW int, now time.Time) []string {
	j := m.jobs[c.job]
	sel := c.job == m.selected
	col := lipgloss.NewStyle().Foreground(statusColor(j.Status, m.dark))
	running := j.Status == "running" || j.Status == "launching"
	state, since, _ := liveUnknown, time.Duration(0), time.Duration(0)
	if running {
		state, since, _ = m.jobLiveness(j, now)
	}

	glyph := col.Render(statusIcon(j.Status))
	switch {
	case state == liveStalled:
		glyph = lipgloss.NewStyle().Bold(true).Foreground(statusColor("failed", m.dark)).Render("✖")
	case running && state == liveFresh && m.beat > 0:
		glyph = col.Render(spinner[m.beat%len(spinner)])
	}
	cursor := " "
	if sel {
		cursor = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color(focusBorderHex)).Render("▌")
	}
	pin := ""
	if m.pinned[j.JobID] {
		pin = dimStyle.Render(" P")
	}

	// Right side of the header: elapsed time and liveness (or queue wait).
	var right []string
	if running {
		if st, ok := jobStart(j); ok {
			right = append(right, dimStyle.Render(formatAge(now.Sub(st))))
		}
		if lt := m.livenessText(state, since); lt != "" {
			right = append(right, lt)
		}
	} else {
		right = append(right, col.Render(j.Status))
		if d := ageOf(j.CreatedAt); d > 0 {
			right = append(right, dimStyle.Render("waiting "+compactDuration(d)))
		}
	}

	pct := m.jobProgressPercent(j.JobID)
	progressText := func(room int) string {
		// "62%  ETA 7m  41 it/s", dropping parts that do not fit.
		if pct < 0 {
			return dimStyle.Render(fmt.Sprintf("%d events", j.EventCount))
		}
		parts := []string{boldStyle.Render(fmt.Sprintf("%3.0f%%", pct))}
		if eta, ok := m.jobETA(j, now); ok && pct < 100 {
			parts = append(parts, "ETA "+boldStyle.Render(compactDuration(eta)))
		}
		if rate := m.jobRate(j); rate != "" {
			parts = append(parts, dimStyle.Render(rate))
		}
		for n := len(parts); n > 0; n-- {
			if t := strings.Join(parts[:n], "  "); lipgloss.Width(t) <= room {
				return t
			}
		}
		return ""
	}
	bar := func(width int) string {
		if pct >= 0 {
			return m.styledBar(j.Status, pct, width, false)
		}
		if !running {
			return dimStyle.Render(strings.Repeat("·", width))
		}
		// No progress reported: a block that steps along once per refresh.
		pos := (m.beat * 2) % max(width, 1)
		var b strings.Builder
		for x := 0; x < width; x++ {
			if d := (x - pos + width) % width; d < 4 {
				b.WriteString(col.Render("▓"))
			} else {
				b.WriteString(dimStyle.Render("░"))
			}
		}
		return b.String()
	}

	name := col.Bold(true).Render(j.DisplayName())
	rightText := strings.Join(right, "  ")
	var out []string
	if c.level == 0 {
		// One row: name, bar, percent/ETA, liveness.
		nameW := clamp(innerW/4, 8, 28)
		info := ""
		if running {
			info = progressText(18)
		}
		fixed := 3 + nameW + 1 + 1 + lipgloss.Width(info) + 2 + lipgloss.Width(rightText)
		barW := innerW - fixed
		if barW < 6 {
			rightText, fixed = "", fixed-lipgloss.Width(rightText)-2
			barW = innerW - fixed
		}
		line := cursor + glyph + " " + fillLine(col.Bold(true).Render(truncate(j.DisplayName(), nameW)), nameW) + pin
		if barW >= 4 {
			line += " " + bar(barW) + " " + info
		}
		if rightText != "" {
			line += "  " + rightText
		}
		out = append(out, line)
	} else {
		head := cursor + glyph + " " + name + pin
		nameRoom := innerW - lipgloss.Width(rightText) - 2
		if lipgloss.Width(head) > nameRoom {
			head = cursor + glyph + " " + col.Bold(true).Render(truncate(j.DisplayName(), max(nameRoom-3-lipgloss.Width(pin), 4))) + pin
		}
		out = append(out, head+strings.Repeat(" ", max(innerW-lipgloss.Width(head)-lipgloss.Width(rightText), 1))+rightText)
		if running {
			room := min(34, innerW/2)
			info := progressText(room)
			barW := innerW - 3 - lipgloss.Width(info) - 1
			out = append(out, "   "+bar(barW)+" "+info)
			msg := m.jobMessage(j.JobID)
			note := ""
			if d, ok := m.panels[j.JobID]; ok && d.invalid > 0 {
				note = lipgloss.NewStyle().Foreground(statusColor("cancelled", m.dark)).Render(fmt.Sprintf("%d panel spec ignored", d.invalid))
			}
			line := "   "
			if msg != "" {
				line += dimStyle.Render("↳ " + truncate(msg, max(innerW-5-lipgloss.Width(note)-2, 1)))
			}
			if note != "" {
				line += strings.Repeat(" ", max(innerW-lipgloss.Width(line)-lipgloss.Width(note), 1)) + note
			}
			out = append(out, line)
		} else {
			out = append(out, "   "+dimStyle.Render(fmt.Sprintf("%s waiting for a slot", statusIcon(j.Status))))
		}
		if c.level >= 2 {
			specs, _ := m.jobPanels(j.JobID)
			cellH := 2
			if c.level == 3 {
				cellH = 4
			}
			for _, l := range m.renderPanelGrid(j, specs, innerW-3, cellH) {
				out = append(out, "   "+l)
			}
		}
	}
	for i := range out {
		out[i] = fillLine(out[i], innerW)
	}
	if sel {
		st := lipgloss.NewStyle().Background(m.selectedTint())
		for i := range out {
			out[i] = keepBg(st, out[i])
		}
	}
	return out
}

// selectedTint is a faint background for the selected card: it marks the
// selection without drowning the status colours.
func (m Model) selectedTint() color.Color {
	if m.dark {
		return lipgloss.Color("#22213a")
	}
	return lipgloss.Color("#ecebfa")
}

// renderIdle says plainly that nothing is running, then shows the fleet view.
func (m Model) renderIdle(r rect) string {
	title := boldStyle.Foreground(titleColor(m.dark)).Render("Now")
	line := boldStyle.Render("○ Nothing running")
	var last JobSummary
	var lastEnd time.Time
	for _, j := range m.jobs {
		if t, ok := parseTS(j.EndedAt); ok && t.After(lastEnd) {
			last, lastEnd = j, t
		}
	}
	if !lastEnd.IsZero() {
		line += dimStyle.Render("   last finished ") +
			lipgloss.NewStyle().Foreground(statusColor(last.Status, m.dark)).Render(statusIcon(last.Status)+" "+truncate(last.DisplayName(), 30)) +
			dimStyle.Render(" "+compactDuration(time.Since(lastEnd))+" ago")
	}
	banner := m.panelBoxOpts(rect{w: r.w, h: 3}, title, fillLine(line, r.w-2), panelOpts{hint: "v detail"})
	if r.h < 3+5 {
		return padToHeight(banner, r.h)
	}
	fleet := m.renderFleet(rect{w: r.w, h: r.h - 3})
	return padToHeight(banner+"\n"+fleet, r.h)
}

// renderRecent draws the finished-runs strip. Failures from the last day come
// first in bold red and turn the border red.
func (m Model) renderRecent(g glanceLayout) string {
	r := g.recent
	innerW, rows := r.w-2, r.h-2
	lines := make([]string, rows)
	widths := make([]int, rows)
	fails, done := 0, 0
	now := time.Now()
	for _, j := range m.jobs {
		if end, ok := parseTS(j.EndedAt); ok && now.Sub(end) < 24*time.Hour {
			switch {
			case isFailure(j.Status):
				fails++
			case j.Status == "completed":
				done++
			}
		}
	}
	for _, c := range g.chips {
		j := m.jobs[c.job]
		row := c.y - r.y - 1
		st := lipgloss.NewStyle().Foreground(statusColor(j.Status, m.dark))
		switch {
		case isFailure(j.Status):
			st = st.Bold(true)
		case j.Status == "cancelled":
			st = st.Faint(true)
		case j.Status == "completed":
			st = lipgloss.NewStyle()
		}
		text := st.Render(c.text)
		if j.Status == "completed" {
			text = lipgloss.NewStyle().Foreground(statusColor("completed", m.dark)).Render(statusIcon(j.Status)) + st.Render(strings.TrimPrefix(c.text, statusIcon(j.Status)))
		}
		if c.job == m.selected {
			text = m.selectedStyle().Render(c.text)
		}
		pad := c.x - r.x - 1 - widths[row]
		lines[row] += strings.Repeat(" ", max(pad, 0)) + text
		widths[row] = c.x - r.x - 1 + c.w
	}
	if g.moreChips > 0 {
		lines[rows-1] += dimStyle.Render(fmt.Sprintf("  +%d", g.moreChips))
	}
	for i := range lines {
		lines[i] = fillLine(lines[i], innerW)
	}
	title := boldStyle.Foreground(titleColor(m.dark)).Render("Recent")
	var hint []string
	if done > 0 {
		hint = append(hint, lipgloss.NewStyle().Foreground(statusColor("completed", m.dark)).Render(fmt.Sprintf("%s %d", statusIcon("completed"), done)))
	}
	if fails > 0 {
		title = lipgloss.NewStyle().Bold(true).Foreground(statusColor("failed", m.dark)).Render(fmt.Sprintf("Recent · %s %d failed", statusIcon("failed"), fails))
	}
	if len(hint) > 0 {
		hint = append(hint, dimStyle.Render("last 24h"))
	}
	return m.panelBoxOpts(r, title, strings.Join(lines, "\n"), panelOpts{hint: strings.Join(hint, " "), alert: fails > 0})
}

// handleGlanceClick selects the card or recent run under the pointer.
func (m Model) handleGlanceClick(x, y int) Model {
	g := m.computeGlance()
	for _, c := range g.cards {
		if inRect(x, y, g.main) && y >= c.y && y < c.y+c.h {
			if m.selected != c.job {
				m.selected = c.job
				m.bumpGen()
			}
			m.focus = focusJobs
			return m
		}
	}
	for _, c := range g.chips {
		if y == c.y && x >= c.x && x < c.x+c.w {
			if m.selected != c.job {
				m.selected = c.job
				m.bumpGen()
			}
			return m
		}
	}
	if inRect(x, y, g.lower) {
		m.focus = focusLower
	}
	return m
}
