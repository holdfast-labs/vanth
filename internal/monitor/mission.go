package monitor

import (
	"fmt"
	"image/color"
	"strings"
	"time"

	"bloomfx.dev/bloom/fig"
	"charm.land/lipgloss/v2"
)

// The "mission control" view: a header with status pills, a row of hero
// cards for the running jobs (big gradient percent, ETA, rate, a gradient
// bar and the current step; a stalled job gets a thick red card), a
// borderless grid of braille charts for their metrics, and a one-line rail
// of finished and queued runs above the key hints. Below 90 columns the
// cards become one line per job above a single chart.

const compactBelowW = 90 // narrower terminals get one line per job

func (m Model) compactLayout() bool { return m.width < compactBelowW }

type cardCell struct {
	rect
	job  int // -1: the "+N more" card
	more int
}

type railItem struct {
	x, w int
	job  int
	text string
}

type missionLayout struct {
	cards       []cardCell // card grid, or one-line rows in the compact layout
	hero        rect       // "nothing running" lines when idle
	legendY     int        // METRICS legend row, -1 for none
	main        rect       // charts or an open pane
	specs       []chartSpec
	cells       []chartCell
	page, pages int
	ruleY       int // -1 for none
	railY       int
	keysY       int
	rail        []railItem
}

// computeMission lays the frame out; the view and the mouse hit-tests share it.
func (m Model) computeMission() missionLayout {
	w, h := m.width, m.height
	g := missionLayout{legendY: -1, ruleY: -1, railY: h - 2, keysY: h - 1}
	bodyEnd := g.railY
	if h >= 16 {
		g.ruleY, bodyEnd = h-3, h-3
	}
	g.rail = m.railItems(w - 2)
	top, avail := 1, bodyEnd-1
	air := avail >= 30 // room for blank rows around the cards
	if air {
		top++
	}
	jobs := m.chartJobs()
	running := false
	for _, j := range m.jobs {
		running = running || isRunning(j.Status)
	}
	switch {
	case len(m.jobs) == 0:
		g.main = rect{y: 1, w: w, h: bodyEnd - 1}
		return g
	case !running:
		g.hero = rect{x: 1, y: top, w: w - 2, h: 2}
		top += 3
	case m.compactLayout():
		rows := min(len(jobs), max(1, min(avail-6, avail/2)))
		shown, more := m.window(jobs, rows)
		for k, i := range shown {
			g.cards = append(g.cards, cardCell{rect: rect{x: 1, y: top + k, w: w - 2, h: 1}, job: i})
		}
		if more > 0 {
			g.cards = append(g.cards, cardCell{rect: rect{x: 1, y: top + len(shown), w: w - 2, h: 1}, job: -1, more: more})
		}
		top += len(g.cards) + 1
	default:
		const gap, minW = 2, 30
		aw := w - 2
		// Four cards across by default, a fifth fits at 160 wide instead of wrapping.
		cols := min(clamp((aw+gap)/(minW+gap), 1, 5), max(len(jobs), 4))
		need := (len(jobs) + cols - 1) / cols
		budget := avail / 2
		ch, rows := 6, max(1, min(need, budget/6))
		for _, cand := range []int{10, 8} {
			if need*cand <= budget {
				ch, rows = cand, need
				break
			}
		}
		if rows > 1 && len(jobs) <= rows*cols {
			cols = (len(jobs) + rows - 1) / rows // balance rows: 2x2, not 3 + a lone card
		}
		cw := (aw - gap*(cols-1)) / cols
		shown, more := m.window(jobs, rows*cols)
		for k, i := range shown {
			g.cards = append(g.cards, cardCell{rect: rect{x: 1 + (k%cols)*(cw+gap), y: top + (k/cols)*ch, w: cw, h: ch}, job: i})
		}
		if more > 0 {
			k := len(shown)
			g.cards = append(g.cards, cardCell{rect: rect{x: 1 + (k%cols)*(cw+gap), y: top + (k/cols)*ch, w: cw, h: ch}, job: -1, more: more})
		}
		top += rows * ch
		if air {
			top++
		}
	}
	if m.lowerExpanded() {
		g.main = rect{y: top, w: w, h: bodyEnd - top}
		return g
	}
	if !m.compactLayout() {
		g.legendY, top = top, top+1
	}
	g.main = rect{x: 1, y: top, w: w - 2, h: bodyEnd - top}
	if g.main.h < 3 {
		g.main.h, g.legendY = 0, -1
		return g
	}
	g.specs = m.chartSpecs()
	g.cells, g.page, g.pages = m.gridCells(g.main, len(g.specs))
	return g
}

// window returns up to c items of list as a slice that keeps the selected job
// visible; more is how many were left out (shown as a "+N more" card).
func (m Model) window(list []int, c int) (shown []int, more int) {
	if len(list) <= c {
		return list, 0
	}
	k := max(c-1, 0)
	start := 0
	for p, i := range list {
		if i == m.selected && p >= k {
			start = p - k + 1
		}
	}
	return list[start : start+k], len(list) - k
}

// ---------- small pieces ----------

// pill is bold text on a coloured background (reverse video without colour).
func (m Model) pill(text, bg, ink string) string {
	if m.noColor() {
		return lipgloss.NewStyle().Reverse(true).Bold(true).Render(" " + text + " ")
	}
	return lipgloss.NewStyle().Background(lipgloss.Color(bg)).Foreground(lipgloss.Color(ink)).Bold(true).Render(" " + text + " ")
}

// elapsedText is a duration to the second: 42s, 3m22s, 1h05m.
func elapsedText(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm%02ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	return fmt.Sprintf("%dh%02dm", int(d.Hours()), int(d.Minutes())%60)
}

func (m Model) elapsed(j JobSummary) time.Duration {
	if sec, ok := j.DurationSeconds(); ok {
		return time.Duration(sec * float64(time.Second))
	}
	if st, ok := jobStart(j); ok {
		return max(time.Since(st), 0)
	}
	return 0
}

// gradBar is a w-cell bar filled to frac with the job's gradient on a dark
// track. A job without progress (frac < 0) shows a short gradient segment
// that moves one step per refresh.
func (m Model) gradBar(w int, frac float64, a, b color.Color) string {
	if w <= 0 {
		return ""
	}
	ch := m.chrome()
	cells := make([]string, w)
	for x := range cells {
		cells[x] = fg(ch.track).Render("━")
		if m.noColor() {
			cells[x] = "─"
		}
	}
	lo, n := 0, int(frac*float64(w)+0.5)
	if frac < 0 {
		n = max(w/6, 3)
		lo = (m.beat * 2) % max(w-n, 1)
	}
	n = min(n, w-lo)
	cols := lipgloss.Blend1D(max(n, 2), a, b)
	for k := 0; k < n; k++ {
		cells[lo+k] = fg(cols[k]).Render("━")
		if m.noColor() {
			cells[lo+k] = "━"
		}
	}
	return strings.Join(cells, "")
}

// liveText is the right-hand state of a job: "● 12s" since the last output,
// "◔ 1m" late, a STALLED pill, or the outcome of a finished job.
func (m Model) liveText(j JobSummary) string {
	ch := m.chrome()
	dim := fg(ch.dim)
	switch {
	case isRunning(j.Status):
		st, since, _ := m.jobLiveness(j, time.Now())
		switch st {
		case liveStalled:
			return m.pill("✕ STALLED "+compactDuration(since), failHex, "#FFFFFF")
		case liveLate:
			return hexStyle(stallHex).Render("◔ " + compactDuration(since))
		case liveQuiet:
			return dim.Render("◔ " + compactDuration(since))
		case liveFresh:
			return hexStyle(okHex).Render("●") + dim.Render(" "+compactDuration(since))
		}
		return dim.Render("○")
	case j.Status == "completed":
		return hexStyle(okHex).Render("✓") + dim.Render(" done")
	case isFailure(j.Status):
		return m.pill("✕ "+m.failReason(j), failHex, "#FFFFFF")
	}
	return dim.Render(statusIcon(j.Status) + " " + j.Status)
}

func (m Model) failReason(j JobSummary) string {
	if j.ExitCode != nil && *j.ExitCode != 0 {
		return fmt.Sprintf("exit %d", *j.ExitCode)
	}
	return j.Status
}

// stepLine is the job's current step plus its text panels.
func (m Model) stepLine(j JobSummary) string {
	parts := []string{}
	if msg := m.jobMessage(j.JobID); msg != "" {
		parts = append(parts, msg)
	}
	if d, ok := m.panels[j.JobID]; ok {
		for _, p := range d.specs {
			if p.Kind != "text" {
				continue
			}
			v := p.Text
			if p.Key != "" {
				if f := m.latestField(j.JobID, p.Key); f != "" {
					v = f
				}
			}
			if v != "" && !strings.Contains(strings.Join(parts, " "), v) {
				parts = append(parts, v)
			}
		}
	}
	return strings.Join(parts, " · ")
}

// stepCount is "3100/5000 step" from the newest progress event.
func (m Model) stepCount(j JobSummary) string {
	cur := m.latestField(j.JobID, "current")
	if cur == "" {
		return ""
	}
	if tot := m.latestField(j.JobID, "total"); tot != "" {
		cur += "/" + tot
	}
	return strings.TrimSpace(cur + " " + truncate(m.latestField(j.JobID, "unit"), 10))
}

func jobEnded(j JobSummary) time.Duration {
	if d := ageOf(j.EndedAt); d > 0 {
		return d
	}
	return ageOf(j.UpdatedAt)
}

// ---------- header ----------

func (m Model) statusCounts() (running, queued, done, failed, stalled int) {
	for _, j := range m.jobs {
		switch {
		case isRunning(j.Status):
			running++
			if m.isStalled(j) {
				stalled++
			}
		case isFailure(j.Status):
			failed++
		case j.Status == "completed":
			done++
		case !isTerminal(j.Status):
			queued++
		}
	}
	return
}

// renderHeader: "◆ vanth  mission control" on the left; on the right status
// pills (hidden at zero), the done count and the job count.
func (m Model) renderHeader() string {
	w := m.width
	ch := m.chrome()
	running, queued, done, failed, stalled := m.statusCounts()
	brand := hexStyle(brandHex).Render("◆") + " " + fg(ch.value).Bold(true).Render("vanth")
	sub := brand + "  " + fg(ch.dim).Render("mission control")
	pills := func(short bool) string {
		var out []string
		add := func(n int, glyph, label, bg, ink string) {
			if n == 0 {
				return
			}
			t := fmt.Sprintf("%s %d %s", glyph, n, label)
			if short {
				t = fmt.Sprintf("%s %d", glyph, n)
			}
			out = append(out, m.pill(t, bg, ink))
		}
		add(running, "●", "running", okHex, pillInkHex)
		add(queued, "○", "queued", "#334155", "#E2E8F0")
		if done > 0 {
			t := fmt.Sprintf("%d done", done)
			if short {
				t = fmt.Sprint(done)
			}
			out = append(out, hexStyle(okHex).Render("✓")+" "+fg(ch.text).Render(t))
		}
		add(failed, "✕", "failed", pillRedHex, "#FFFFFF")
		add(stalled, "⚠", "stalled", pillAmbHex, pillInkHex)
		return strings.Join(out, " ")
	}
	var facts []string
	if m.warnings > 0 {
		facts = append(facts, hexStyle(stallHex).Render(fmt.Sprintf("warn %d", m.warnings)))
	}
	if m.malformed > 0 {
		facts = append(facts, hexStyle(failHex).Render(fmt.Sprintf("malformed %d", m.malformed)))
	}
	facts = append(facts, fg(ch.dim).Render(fmt.Sprintf("%d jobs · %s", len(m.jobs), m.mode)))
	fact := strings.Join(facts, fg(ch.dim).Render(" · "))
	for _, c := range [][2]string{{sub, pills(false) + "  " + fact}, {sub, pills(false)}, {brand, pills(false)}, {brand, pills(true)}} {
		if lipgloss.Width(c[0])+lipgloss.Width(c[1])+4 <= w { // keep a clear gap
			return spread(" "+c[0], c[1]+" ", w)
		}
	}
	return fillLine(" "+brand+" "+pills(true), w)
}

// ---------- cards ----------

// cardBorder draws content (inner rows) in a border of exactly r.w x r.h.
func cardBorder(r rect, b lipgloss.Border, st lipgloss.Style, rows []string) string {
	iw := r.w - 2
	out := []string{st.Render(b.TopLeft + strings.Repeat(b.Top, iw) + b.TopRight)}
	for k := 0; k < r.h-2; k++ {
		l := ""
		if k < len(rows) {
			l = rows[k]
		}
		out = append(out, st.Render(b.Left)+fillLine(l, iw)+st.Render(b.Right))
	}
	out = append(out, st.Render(b.BottomLeft+strings.Repeat(b.Bottom, iw)+b.BottomRight))
	return strings.Join(out, "\n")
}

// renderCard draws one hero card. A stalled job gets a thick red border, red
// numbers and a STALLED pill; the selected card's border is in the job's full
// colour, the others in a darkened one.
func (m Model) renderCard(c cardCell) string {
	ch := m.chrome()
	dim := fg(ch.dim)
	if c.job < 0 {
		rows := make([]string, c.h-2)
		mid := (c.h - 2) / 2
		rows[max(mid-1, 0)] = lipgloss.PlaceHorizontal(c.w-2, lipgloss.Center, fg(ch.text).Render(fmt.Sprintf("+%d more", c.more)))
		if mid < len(rows) {
			rows[mid] = lipgloss.PlaceHorizontal(c.w-2, lipgloss.Center, dim.Render("↑↓ to see them"))
		}
		return cardBorder(c.rect, lipgloss.RoundedBorder(), fg(ch.faint), rows)
	}
	j := m.jobs[c.job]
	a, b := m.jobGrad(j)
	stalled := m.isStalled(j)
	sel := j.JobID == m.selectedJobID()
	border, bst := lipgloss.RoundedBorder(), fg(lipgloss.Darken(a, 0.45))
	if sel {
		bst = fg(a)
	}
	if stalled {
		border, bst = lipgloss.ThickBorder(), hexStyle(failHex)
		if m.beat%2 == 1 {
			bst = hexStyle("#FB7185") // a gentle pulse, one step per refresh
		}
	}
	iw := c.w - 4
	pad := func(l string) string { return " " + fillLine(l, iw) }

	name := j.DisplayName()
	if sel && m.noColor() {
		name = "▸ " + name
	}
	live := m.liveText(j)
	if m.pinned[j.JobID] {
		live = dim.Render("pin ") + live
	}
	nameSt := fg(ch.value).Bold(true)
	if stalled {
		nameSt = hexStyle(failHex).Bold(true)
	}
	head := spread(nameSt.Render(truncate(name, max(iw-lipgloss.Width(live)-1, 4))), live, iw)

	pct := m.jobProgressPercent(j.JobID)
	now := time.Now()
	big, right := "", []string{"", "", ""}
	var stats []string
	switch {
	case stalled:
		_, since, _ := m.jobLiveness(j, now)
		big = fmt.Sprintf("%.0f%%", max(pct, 0))
		red := hexStyle(failHex).Bold(true)
		right = []string{red.Render("no output"), red.Render("for " + compactDuration(since)), dim.Render("eta –")}
		stats = []string{red.Render(fmt.Sprintf("%.0f%%", max(pct, 0))), red.Render("no output for " + compactDuration(since))}
	case isRunning(j.Status) && pct >= 0:
		big = fmt.Sprintf("%.0f%%", pct)
		eta := ""
		if d, ok := m.jobETA(j, now); ok {
			eta = compactDuration(d) + " left"
		}
		right = []string{fg(ch.value).Bold(true).Render(eta), dim.Render(m.jobRate(j)), dim.Render(elapsedText(m.elapsed(j)))}
		stats = []string{fg(a).Bold(true).Render(big), fg(ch.value).Bold(true).Render(eta), dim.Render(m.jobRate(j))}
	case isRunning(j.Status):
		big = elapsedText(m.elapsed(j))
		right = []string{"", "", ""} // the moving bar says "no progress"
		stats = []string{fg(a).Bold(true).Render(big), dim.Render("elapsed"), dim.Render("no progress")}
	default:
		big = elapsedText(m.elapsed(j))
		right = []string{dim.Render(j.Status), dim.Render("ended " + compactDuration(jobEnded(j)) + " ago"), dim.Render(m.stepCount(j))}
		stats = []string{fg(a).Bold(true).Render(big), dim.Render(j.Status + " · ended " + compactDuration(jobEnded(j)) + " ago")}
	}
	frac := pct / 100
	switch {
	case !isRunning(j.Status) && pct < 0:
		frac = 1
	case stalled:
		frac = max(pct, 0) / 100
	}
	bar := m.gradBar(iw, min(frac, 1), a, b)
	step := m.stepLine(j)
	if d, ok := m.panels[j.JobID]; ok && d.invalid > 0 {
		step += fmt.Sprintf(" · %d panel spec ignored", d.invalid)
	}
	stepRow := dim.Render(truncate(step, iw))

	var rows []string
	var bigRows []string
	if c.h >= 8 {
		rw := 0
		for _, r := range right {
			rw = max(rw, lipgloss.Width(r))
		}
		bigRows = bigText(big, iw-rw-2, fig.Gradient(a, b))
		for k := range bigRows {
			r := ""
			if k < len(right) {
				r = right[k]
			}
			bigRows[k] = spread(bigRows[k], r, iw)
		}
	}
	switch {
	case bigRows != nil && c.h >= 10:
		rows = append([]string{head, ""}, bigRows...)
		rows = append(rows, "", bar, stepRow)
	case bigRows != nil:
		rows = append([]string{head}, bigRows...)
		rows = append(rows, bar, stepRow)
	default:
		rows = []string{head, strings.Join(stats, "  "), bar, stepRow}
	}
	for k := range rows {
		rows[k] = pad(rows[k])
	}
	return cardBorder(c.rect, border, bst, rows)
}

// renderJobLine is one job in the compact layout: colour stripe, name,
// gradient bar, percent, ETA (or elapsed time) and liveness.
func (m Model) renderJobLine(c cardCell) string {
	ch := m.chrome()
	dim := fg(ch.dim)
	if c.job < 0 {
		return dim.Render(fmt.Sprintf("  +%d more · ↑↓", c.more))
	}
	j := m.jobs[c.job]
	a, b := m.jobGrad(j)
	now := time.Now()
	stripe := fg(a).Render("▌")
	if m.noColor() {
		stripe = string(m.jobMarker(j.JobID))
	}
	nameW := clamp(c.w/4, 10, 22)
	nameSt := fg(ch.value).Bold(true)
	if m.isStalled(j) {
		nameSt = hexStyle(failHex).Bold(true)
	}
	switch sel := j.JobID == m.selectedJobID(); {
	case sel && m.noColor():
		nameSt = lipgloss.NewStyle().Reverse(true).Bold(true)
	case sel:
		nameSt = nameSt.Background(ch.track)
	}
	name := nameSt.Render(fmt.Sprintf("%-*s", nameW, truncate(j.DisplayName(), nameW)))
	pct := m.jobProgressPercent(j.JobID)
	pctTxt, eta := "··", ""
	frac := pct / 100
	switch {
	case pct >= 0:
		pctTxt = fmt.Sprintf("%.0f%%", pct)
		if d, ok := m.jobETA(j, now); ok && isRunning(j.Status) && !m.isStalled(j) {
			eta = compactDuration(d) + " left"
		}
	case !isRunning(j.Status):
		frac, pctTxt = 1, "✓"
		if isFailure(j.Status) {
			pctTxt = "✕"
		}
	}
	if eta == "" && pct < 0 {
		eta = elapsedText(m.elapsed(j))
	}
	live := m.liveText(j)
	etaW := 10
	if c.w < 64 {
		etaW = 0
	}
	barW := max(c.w-2-nameW-1-1-5-etaW-1-14, 4)
	line := stripe + " " + name + " " + m.gradBar(barW, min(frac, 1), a, b) + " " +
		fg(ch.value).Bold(true).Render(fmt.Sprintf("%4s", pctTxt)) + " "
	if etaW > 0 {
		line += fg(ch.text).Render(fmt.Sprintf(" %-*s", etaW-1, truncate(eta, etaW-1)))
	}
	return spread(line, live, c.w)
}

// renderHero is the idle state: nothing runs, so a calm line and the
// selected (most recent) run's outcome.
func (m Model) renderHero(r rect) []string {
	ch := m.chrome()
	dim := fg(ch.dim)
	l1 := fg(ch.text).Render("○ nothing running")
	if rec := m.recentJobs(); len(rec) > 0 {
		l1 += dim.Render(fmt.Sprintf("  ·  last run finished %s ago", compactDuration(jobEnded(m.jobs[rec[0]]))))
	}
	l2 := ""
	if j, ok := m.selectedJob(); ok {
		parts := []string{elapsedText(m.elapsed(j))}
		if s := m.stepCount(j); s != "" {
			parts = append(parts, "step "+s)
		}
		if step := m.stepLine(j); step != "" {
			parts = append(parts, step)
		}
		if isTerminal(j.Status) {
			parts = append(parts, "ended "+compactDuration(jobEnded(j))+" ago")
		}
		l2 = "  " + fg(ch.value).Bold(true).Render(j.DisplayName()) + "  " + m.liveText(j) + dim.Render("  "+strings.Join(parts, " · "))
	}
	return []string{fillLine(l1, r.w), fillLine(l2, r.w)}
}

// ---------- legend, rail and keys ----------

// renderLegend names the jobs drawn on the current page of charts, each in
// its colour (or with its marker), and the page when there are several.
func (m Model) renderLegend(g missionLayout) string {
	ch := m.chrome()
	seen := map[int]bool{}
	for _, c := range g.cells {
		for _, i := range g.specs[c.spec].jobs {
			seen[i] = true
		}
	}
	l := fg(ch.dim).Render("METRICS")
	for _, i := range m.chartJobs() {
		if !seen[i] {
			continue
		}
		id := m.jobs[i].JobID
		mark := fg(m.jobColor(id)).Render("■")
		if m.noColor() {
			mark = string(m.jobMarker(id))
		}
		l += "  " + mark + " " + fg(m.jobColor(id)).Render(m.jobs[i].DisplayName())
	}
	r := ""
	if g.pages > 1 {
		r = fg(ch.dim).Render(fmt.Sprintf("charts %d/%d · [ ]", g.page+1, g.pages))
	}
	return spread(l, r, g.main.w)
}

// railItems are the finished and queued runs of the rail, in the order they
// are drawn: failures first, then queued runs, then the rest (dimmed), cut
// with "…" when the line is full. x is relative to the rail's start.
// railFailWindow is how recent a failure must be to lead the rail in bold with its
// reason; older failures fall back to a plain red name among the rest.
const railFailWindow = 3 * time.Hour

func (m Model) railItems(w int) []railItem {
	ch := m.chrome()
	dim, faint := fg(ch.dim), fg(ch.faint)
	var fails, queued, rest []int
	for _, i := range m.recentJobs() {
		j := m.jobs[i]
		if isFailure(j.Status) && jobEnded(j) < railFailWindow {
			fails = append(fails, i)
		} else {
			rest = append(rest, i)
		}
	}
	for _, i := range m.activeJobs(time.Now()) {
		if !isRunning(m.jobs[i].Status) {
			queued = append(queued, i)
		}
	}
	text := func(i int, recent bool) string {
		j := m.jobs[i]
		name := j.DisplayName()
		switch {
		case isFailure(j.Status) && recent:
			return hexStyle(failHex).Bold(true).Render("✕ "+name) + hexStyle(failDimHex).Render(" "+m.failReason(j)+" · "+compactDuration(jobEnded(j)))
		case isFailure(j.Status):
			return hexStyle(failHex).Render("✕ " + name)
		case j.Status == "completed":
			return hexStyle(okHex).Render("✓") + dim.Render(" "+name)
		case isTerminal(j.Status):
			return faint.Render(statusIcon(j.Status) + " " + name)
		}
		return dim.Render("○ "+name) + faint.Render(" "+j.Status+" "+compactDuration(ageOf(j.CreatedAt)))
	}
	var out []railItem
	x := 0
	for gi, group := range [][]int{fails, queued, rest} {
		if len(group) == 0 {
			continue
		}
		if len(out) > 0 {
			x += 5 // "  │  " between groups
		}
		for k, i := range group {
			if k > 0 {
				x += 3
			}
			t := text(i, gi == 0)
			tw := lipgloss.Width(t)
			if x+tw > w {
				if x+2 <= w {
					out = append(out, railItem{x: x, w: w - x, job: i, text: ansiPlainFit(t, w-x)})
				}
				return out
			}
			out = append(out, railItem{x: x, w: tw, job: i, text: t})
			x += tw
		}
	}
	return out
}

// ansiPlainFit cuts a styled string to n cells, ending it with "…".
func ansiPlainFit(s string, n int) string {
	if n <= 1 {
		return "…"
	}
	return lipgloss.NewStyle().MaxWidth(n-1).Render(s) + "…"
}

func (m Model) renderRail(g missionLayout) string {
	ch := m.chrome()
	if len(g.rail) == 0 {
		return " " + fg(ch.dim).Render("no finished runs yet")
	}
	var b strings.Builder
	at := 0
	prev := -1
	for _, it := range g.rail {
		gap := it.x - at
		if gap >= 5 && prev >= 0 {
			b.WriteString("  " + fg(ch.faint).Render("│") + strings.Repeat(" ", gap-3))
		} else {
			b.WriteString(strings.Repeat(" ", max(gap, 0)))
		}
		b.WriteString(it.text)
		at, prev = it.x+it.w, it.job
	}
	return " " + b.String()
}

// renderKeys is the key-hint line, least important hints dropped first when
// narrow, with the freshness of the data on the right.
func (m Model) renderKeys(g missionLayout) string {
	w := m.width
	ch := m.chrome()
	type hint struct {
		key, label string
		drop       int // higher drops first
	}
	hints := []hint{{"↑↓←→", "job", 2}, {"tab", "chart", 4}, {"[ ]", "page", 3}, {"enter", "pin", 9},
		{"e", "events", 7}, {"l", "logs", 5}, {"s", "slowest", 8}, {"?", "help", 0}, {"q", "quit", 0}}
	if m.lowerExpanded() {
		hints = []hint{hints[0], {"pgup/pgdn", "scroll", 1}, {"esc", "close", 1}, hints[7], hints[8]}
	}
	right := fg(ch.dim).Render("updated " + compactDuration(time.Since(m.lastRefresh)) + " ago")
	switch {
	case m.staleSince != nil:
		right = hexStyle(stallHex).Bold(true).Render("STALE " + compactDuration(time.Since(*m.staleSince)))
	case m.mode == ModeFallback:
		right = hexStyle(stallHex).Render("FALLBACK · event mirrors")
	}
	text := func() string {
		var parts []string
		for _, h := range hints {
			parts = append(parts, fg(ch.text).Bold(true).Render(h.key)+" "+fg(ch.dim).Render(h.label))
		}
		return " " + strings.Join(parts, "   ")
	}
	for lipgloss.Width(text())+lipgloss.Width(right)+3 > w && len(hints) > 2 {
		worst := 0
		for k, h := range hints {
			if h.drop > hints[worst].drop {
				worst = k
			}
		}
		hints = append(hints[:worst], hints[worst+1:]...)
	}
	return spread(text(), right+" ", w)
}

// ---------- frame ----------

// put draws block at (x, y) into rows; blocks on a row are placed left to right.
func put(rows []string, x, y int, block string) {
	for k, l := range strings.Split(block, "\n") {
		if r := y + k; r >= 0 && r < len(rows) {
			rows[r] += strings.Repeat(" ", max(x-lipgloss.Width(rows[r]), 0)) + l
		}
	}
}

func (m Model) renderMission() string {
	g := m.computeMission()
	w, h := m.width, m.height
	ch := m.chrome()
	rows := make([]string, h)
	rows[0] = m.renderHeader()
	if g.hero.h > 0 {
		for k, l := range m.renderHero(g.hero) {
			put(rows, g.hero.x, g.hero.y+k, l)
		}
	}
	for _, c := range g.cards {
		if c.h == 1 {
			put(rows, c.x, c.y, m.renderJobLine(c))
		} else {
			put(rows, c.x, c.y, m.renderCard(c))
		}
	}
	if g.legendY >= 0 && len(g.cells) > 0 {
		put(rows, g.main.x, g.legendY, m.renderLegend(g))
	}
	if r := g.main; r.w > 0 && r.h > 0 {
		switch {
		case m.lowerExpanded():
			put(rows, r.x, r.y, frameBlock(m.renderLower(r), r.w, r.h))
		case len(m.jobs) == 0:
			msg := "no jobs found"
			if m.empty && m.emptyMsg != "" {
				msg = m.emptyMsg
			} else if m.mode == ModeFallback {
				msg = "no jobs found; reading fallback event mirrors"
			}
			if m.loading {
				msg = "loading jobs…"
			}
			put(rows, r.x, r.y, frameBlock(m.centeredPane(msg, r), r.w, r.h))
		case len(g.cells) == 0:
			put(rows, r.x, r.y, frameBlock(m.renderFleet(r), r.w, r.h))
		default:
			focus := clamp(m.chartFocus, 0, len(g.specs)-1)
			used := r.y
			for _, c := range g.cells {
				put(rows, c.x, c.y, m.renderChart(c.rect, g.specs[c.spec], c.spec == focus))
				used = max(used, c.y+c.h)
			}
			if rest := r.y + r.h - used - 1; g.hero.h > 0 && rest >= 6 {
				// Idle with room to spare: the fleet timeline under the recent run's charts.
				put(rows, r.x, used+1, m.renderTimeline(rect{w: r.w, h: min(rest, 12)}))
			}
		}
	}
	if g.ruleY >= 0 {
		rows[g.ruleY] = fg(ch.faint).Render(strings.Repeat("─", w))
	}
	rows[g.railY] = m.renderRail(g)
	rows[g.keysY] = m.renderKeys(g)
	for k := range rows {
		rows[k] = fillLine(rows[k], w)
	}
	return strings.Join(rows, "\n")
}

// ---------- hit testing ----------

// missionClick selects the job under the pointer (card, job line or rail
// item) or focuses the chart under it.
func (m Model) missionClick(x, y int) Model {
	g := m.computeMission()
	selectJob := func(i int) {
		if i >= 0 && i != m.selected {
			m.selected = i
			m.bumpGen()
		}
	}
	for _, c := range g.cards {
		if inRect(x, y, c.rect) {
			selectJob(c.job)
			return m
		}
	}
	if y == g.railY {
		for _, it := range g.rail {
			if x >= 1+it.x && x < 1+it.x+it.w {
				selectJob(it.job)
				return m
			}
		}
	}
	for _, c := range g.cells {
		if inRect(x, y, c.rect) {
			m.chartFocus = c.spec
			return m
		}
	}
	return m
}

// chartAt returns the chart spec under the pointer.
func (m Model) chartAt(x, y int) (chartSpec, bool) {
	g := m.computeMission()
	for _, c := range g.cells {
		if inRect(x, y, c.rect) {
			return g.specs[c.spec], true
		}
	}
	return chartSpec{}, false
}
