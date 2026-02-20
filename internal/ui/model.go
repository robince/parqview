package ui

import (
	"context"
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/robince/parqview/internal/clipboard"
	"github.com/robince/parqview/internal/engine"
	"github.com/robince/parqview/internal/selection"
	"github.com/robince/parqview/internal/types"
	"github.com/robince/parqview/internal/util"
)

// Focus tracks which pane has focus.
type Focus int

const (
	FocusTable Focus = iota
	FocusColumns
)

// Overlay tracks which overlay is visible.
type Overlay int

const (
	OverlayNone Overlay = iota
	OverlayHelp
	OverlayDetail
)

// Messages
type profileBasicDoneMsg struct {
	colName string
	summary *types.ColumnSummary
	err     error
}

type profileDetailDoneMsg struct {
	colName string
	summary *types.ColumnSummary
	err     error
}

type previewDoneMsg struct {
	rows       [][]string
	colNames   []string
	totalRows  int64
	filterRows int64
	err        error
}

type firstNullMsg struct {
	rowID  int64
	offset int64
	err    error
}

type statusMsg string

// Model is the root Bubble Tea model.
type Model struct {
	engine   *engine.Engine
	fileName string

	// Schema
	columns []types.ColumnInfo
	sel     *selection.Set

	// Search
	searchInput   textinput.Model
	searchFocused bool
	searchQuery   string

	// Column list state
	filteredCols []types.ColumnInfo // columns matching search
	colCursor    int                // cursor in filteredCols

	// Table state
	tableData    [][]string
	tableCols    []string // column names in current projection
	tableOffset  int      // row offset for pagination
	tableColOff  int      // horizontal scroll (column offset)
	showSelected bool     // show only selected columns
	rowFilter    string   // active SQL filter
	totalRows    int64
	filterRows   int64 // -1 means no filter active

	// Profiling
	summaries map[string]*types.ColumnSummary

	// UI state
	focus     Focus
	overlay   Overlay
	detailCol string // column shown in detail panel
	detailTab int    // 0=TopValues, 1=Stats, 2=Histogram
	width     int
	height    int
	statusMsg string
	ready     bool

	pageSize int // rows per page
}

// NewModel creates the initial model.
func NewModel(eng *engine.Engine, fileName string) Model {
	cols := eng.Columns()
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}

	ti := textinput.New()
	ti.Prompt = "/ "
	ti.CharLimit = 256

	m := Model{
		engine:      eng,
		fileName:    fileName,
		columns:     cols,
		sel:         selection.New(names),
		searchInput: ti,
		summaries:   make(map[string]*types.ColumnSummary),
		filterRows:  -1,
		totalRows:   eng.TotalRows(),
		pageSize:    50,
		focus:       FocusTable,
	}
	m.updateFilteredCols()
	return m
}

func (m *Model) updateFilteredCols() {
	var filtered []types.ColumnInfo
	for _, c := range m.columns {
		if util.FuzzyMatch(c.Name, m.searchQuery) {
			filtered = append(filtered, c)
		}
	}
	m.filteredCols = filtered
	if m.colCursor >= len(m.filteredCols) {
		m.colCursor = max(0, len(m.filteredCols)-1)
	}
}

func (m Model) Init() tea.Cmd {
	return tea.Batch(
		m.loadPreview(),
		m.profileNext(),
	)
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.ready = true
		return m, nil

	case tea.KeyMsg:
		return m.handleKey(msg)

	case previewDoneMsg:
		if msg.err != nil {
			m.statusMsg = fmt.Sprintf("Error: %v", msg.err)
		} else {
			m.tableData = msg.rows
			m.tableCols = msg.colNames
			m.totalRows = msg.totalRows
			if msg.filterRows >= 0 {
				m.filterRows = msg.filterRows
			}
			prevOffset := m.tableOffset
			m.clampTableOffset()
			if m.tableOffset != prevOffset {
				return m, m.loadPreview()
			}
		}
		return m, nil

	case profileBasicDoneMsg:
		if msg.err == nil && msg.summary != nil {
			existing, exists := m.summaries[msg.colName]
			if !exists || existing == nil || !existing.DetailLoaded {
				m.summaries[msg.colName] = msg.summary
			}
		}
		// Chain: profile the next column
		return m, m.profileNext()

	case profileDetailDoneMsg:
		if msg.err == nil && msg.summary != nil {
			m.summaries[msg.colName] = msg.summary
		}
		return m, nil

	case firstNullMsg:
		switch {
		case msg.err != nil:
			m.statusMsg = fmt.Sprintf("Error: %v", msg.err)
		case msg.rowID == 0:
			m.statusMsg = "No nulls found"
		default:
			// Jump to the row
			m.tableOffset = max(0, int(msg.offset))
			m.clampTableOffset()
			m.overlay = OverlayNone
			m.statusMsg = fmt.Sprintf("Jumped to row %d", msg.rowID)
			return m, m.loadPreview()
		}
		return m, nil

	case statusMsg:
		m.statusMsg = string(msg)
		return m, nil
	}

	return m, nil
}

func (m Model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	key := msg.String()

	// Search input captures keys when focused
	if m.searchFocused {
		switch key {
		case "esc":
			m.searchFocused = false
			return m, nil
		case "ctrl+u":
			m.searchInput.SetValue("")
			m.searchQuery = ""
			m.updateFilteredCols()
			return m, nil
		case "enter":
			m.searchFocused = false
			return m, nil
		default:
			var cmd tea.Cmd
			m.searchInput, cmd = m.searchInput.Update(msg)
			m.searchQuery = m.searchInput.Value()
			m.updateFilteredCols()
			return m, cmd
		}
	}

	// Help overlay
	if m.overlay == OverlayHelp {
		if key == "esc" || key == "?" || key == "q" {
			m.overlay = OverlayNone
		}
		return m, nil
	}

	// Detail overlay
	if m.overlay == OverlayDetail {
		switch key {
		case "esc", "q":
			m.overlay = OverlayNone
		case "t":
			m.detailTab = (m.detailTab + 1) % 3
		case "n":
			return m, m.jumpToFirstNull(m.detailCol, m.rowFilter)
		}
		return m, nil
	}

	// Global keys
	switch key {
	case "q", "ctrl+c":
		return m, tea.Quit
	case "?":
		m.overlay = OverlayHelp
		return m, nil
	case "tab":
		if m.focus == FocusTable {
			m.focus = FocusColumns
		} else {
			m.focus = FocusTable
		}
		return m, nil
	}

	if m.focus == FocusColumns {
		return m.handleColumnsKey(key)
	}
	return m.handleTableKey(key)
}

func (m Model) handleColumnsKey(key string) (tea.Model, tea.Cmd) {
	switch key {
	case "/":
		m.searchFocused = true
		m.searchInput.Focus()
		return m, textinput.Blink
	case "up", "k":
		if m.colCursor > 0 {
			m.colCursor--
		}
	case "down", "j":
		if m.colCursor < len(m.filteredCols)-1 {
			m.colCursor++
		}
	case " ":
		if m.colCursor < len(m.filteredCols) {
			m.sel.Toggle(m.filteredCols[m.colCursor].Name)
		}
	case "s":
		names := make([]string, len(m.filteredCols))
		for i, c := range m.filteredCols {
			names[i] = c.Name
		}
		m.sel.AddAll(names)
	case "d":
		names := make([]string, len(m.filteredCols))
		for i, c := range m.filteredCols {
			names[i] = c.Name
		}
		m.sel.RemoveAll(names)
	case "a":
		m.sel.SelectAll()
	case "x":
		m.sel.Clear()
	case "y":
		selected := m.sel.Selected()
		if len(selected) > 0 {
			text := clipboard.FormatPythonList(selected)
			if err := clipboard.Copy(text); err != nil {
				m.statusMsg = fmt.Sprintf("Clipboard error: %v", err)
			} else {
				m.statusMsg = fmt.Sprintf("Copied %d columns to clipboard", len(selected))
			}
		} else {
			m.statusMsg = "No columns selected"
		}
	case "enter":
		if m.colCursor < len(m.filteredCols) {
			m.detailCol = m.filteredCols[m.colCursor].Name
			m.detailTab = 0
			m.overlay = OverlayDetail
			// Trigger detail profiling if needed
			if s, ok := m.summaries[m.detailCol]; !ok || !s.DetailLoaded {
				var existing *types.ColumnSummary
				if ok && s != nil {
					copySummary := *s
					existing = &copySummary
				}
				return m, m.loadDetail(m.detailCol, existing, m.columnType(m.detailCol))
			}
		}
	}
	return m, nil
}

func (m Model) handleTableKey(key string) (tea.Model, tea.Cmd) {
	switch key {
	case "up", "k":
		if m.tableOffset > 0 {
			m.tableOffset--
			return m, m.loadPreview()
		}
	case "down", "j":
		maxOffset := m.maxTableOffset()
		if m.tableOffset < maxOffset {
			m.tableOffset++
			return m, m.loadPreview()
		}
	case "g":
		m.tableOffset = 0
		return m, m.loadPreview()
	case "G":
		m.tableOffset = m.maxTableOffset()
		return m, m.loadPreview()
	case "left", "h":
		if m.tableColOff > 0 {
			m.tableColOff--
		}
	case "right", "l":
		m.tableColOff++
	case "[":
		m.tableColOff = max(0, m.tableColOff-5)
	case "]":
		m.tableColOff += 5
	case "S":
		m.showSelected = !m.showSelected
		m.tableColOff = 0
		return m, m.loadPreview()
	case "f":
		// Toggle null filter
		if m.rowFilter != "" {
			m.rowFilter = ""
			m.filterRows = -1
		} else {
			selected := m.sel.Selected()
			if len(selected) > 0 {
				m.rowFilter = engine.BuildNullFilter(selected)
			} else {
				// Use all visible columns
				names := make([]string, len(m.filteredCols))
				for i, c := range m.filteredCols {
					names[i] = c.Name
				}
				m.rowFilter = engine.BuildNullFilter(names)
			}
		}
		m.tableOffset = 0
		return m, m.loadPreview()
	}
	return m, nil
}

// Commands

func (m Model) loadPreview() tea.Cmd {
	eng := m.engine
	colNames := m.projectionCols()
	rowFilter := m.rowFilter
	limit := m.pageSize
	offset := m.tableOffset
	maxOffset := m.maxTableOffset()
	if offset > maxOffset {
		offset = maxOffset
	}
	if offset < 0 {
		offset = 0
	}

	return func() tea.Msg {
		ctx := context.Background()
		rows, err := eng.Preview(ctx, colNames, rowFilter, limit, offset)
		if err != nil {
			return previewDoneMsg{err: err}
		}
		totalRows := eng.TotalRows()
		var filterRows int64 = -1
		if rowFilter != "" {
			filterRows, _ = eng.FilteredRowCount(ctx, rowFilter)
		}
		return previewDoneMsg{
			rows:       rows,
			colNames:   colNames,
			totalRows:  totalRows,
			filterRows: filterRows,
		}
	}
}

func (m Model) projectionCols() []string {
	if m.showSelected {
		selected := m.sel.Selected()
		if len(selected) > 0 {
			return selected
		}
	}
	// Default: all columns
	names := make([]string, len(m.columns))
	for i, c := range m.columns {
		names[i] = c.Name
	}
	return names
}

func (m Model) profileNext() tea.Cmd {
	eng := m.engine
	cols := m.columns
	summaries := m.summaries

	// Find the next column that hasn't been profiled yet
	var target string
	for _, c := range cols {
		if _, ok := summaries[c.Name]; !ok {
			target = c.Name
			break
		}
	}
	if target == "" {
		return nil // all done
	}

	colName := target
	return func() tea.Msg {
		summary, err := eng.ProfileBasic(context.Background(), colName)
		return profileBasicDoneMsg{colName: colName, summary: summary, err: err}
	}
}

func (m Model) loadDetail(colName string, existing *types.ColumnSummary, colType string) tea.Cmd {
	eng := m.engine

	return func() tea.Msg {
		ctx := context.Background()
		summary := existing
		if summary == nil {
			var err error
			summary, err = eng.ProfileBasic(ctx, colName)
			if err != nil {
				return profileDetailDoneMsg{colName: colName, err: err}
			}
		}

		if err := eng.ProfileDetail(ctx, colName, summary, colType); err != nil {
			return profileDetailDoneMsg{colName: colName, err: err}
		}
		return profileDetailDoneMsg{colName: colName, summary: summary}
	}
}

func (m Model) jumpToFirstNull(colName, rowFilter string) tea.Cmd {
	eng := m.engine
	return func() tea.Msg {
		ctx := context.Background()
		rowID, err := eng.FirstNullRow(ctx, colName, rowFilter)
		if err != nil || rowID == 0 {
			return firstNullMsg{rowID: rowID, err: err}
		}
		offset, err := eng.OffsetForRowID(ctx, rowID, rowFilter)
		return firstNullMsg{rowID: rowID, offset: offset, err: err}
	}
}

// View

func (m Model) View() string {
	if !m.ready {
		return "Loading..."
	}

	if m.overlay == OverlayHelp {
		return m.viewHelp()
	}

	// Layout: top bar, main area (table + columns), bottom bar
	topBar := m.viewTopBar()
	bottomBar := m.viewBottomBar()

	mainHeight := m.height - 2 // top + bottom bars

	// Split: table gets 65%, columns gets 35%
	tableWidth := m.width * 65 / 100
	colWidth := m.width - tableWidth

	tableView := m.viewTable(tableWidth-2, mainHeight-2)
	colView := m.viewColumns(colWidth-2, mainHeight-2)

	// Apply borders based on focus
	var tablePane, colPane string
	if m.focus == FocusTable {
		tablePane = activeBorderStyle.Width(tableWidth - 2).Height(mainHeight - 2).Render(tableView)
		colPane = inactiveBorderStyle.Width(colWidth - 2).Height(mainHeight - 2).Render(colView)
	} else {
		tablePane = inactiveBorderStyle.Width(tableWidth - 2).Height(mainHeight - 2).Render(tableView)
		colPane = activeBorderStyle.Width(colWidth - 2).Height(mainHeight - 2).Render(colView)
	}

	// Detail overlay rendered on top of columns pane
	if m.overlay == OverlayDetail {
		colPane = activeBorderStyle.Width(colWidth - 2).Height(mainHeight - 2).Render(m.viewDetail(colWidth - 4))
	}

	main := lipgloss.JoinHorizontal(lipgloss.Top, tablePane, colPane)

	return lipgloss.JoinVertical(lipgloss.Left, topBar, main, bottomBar)
}

func (m Model) viewTopBar() string {
	left := fmt.Sprintf(" %s  %d rows × %d cols", m.fileName, m.totalRows, len(m.columns))
	right := ""
	if m.rowFilter != "" {
		filterInfo := "Filter: rows with nulls"
		if m.filterRows >= 0 {
			filterInfo += fmt.Sprintf(" (%d rows)", m.filterRows)
		}
		right = filterStyle.Render(filterInfo)
	}
	gap := m.width - lipgloss.Width(left) - lipgloss.Width(right)
	if gap < 0 {
		gap = 0
	}
	return topBarStyle.Width(m.width).Render(left + strings.Repeat(" ", gap) + right)
}

func (m Model) viewBottomBar() string {
	selCount := m.sel.Count()
	var hints string
	if m.focus == FocusColumns {
		hints = "/:search  Space:toggle  s:add  d:rm  y:copy  Enter:detail"
	} else {
		hints = "hjkl:scroll  S:selected-only  f:null-filter  []:page-scroll"
	}
	status := fmt.Sprintf("  Sel: %d/%d", selCount, len(m.columns))
	if m.statusMsg != "" {
		status += "  " + m.statusMsg
	}
	return bottomBarStyle.Width(m.width).Render(hints + status)
}

func (m Model) viewTable(w, h int) string {
	if len(m.tableCols) == 0 {
		return "No data loaded"
	}

	colWidth := 14 // fixed column width for v1
	rowNumW := 6

	// How many columns fit
	visibleCols := (w - rowNumW) / colWidth
	if visibleCols < 1 {
		visibleCols = 1
	}

	startCol := m.tableColOff
	if startCol >= len(m.tableCols) {
		startCol = max(0, len(m.tableCols)-1)
	}
	endCol := startCol + visibleCols
	if endCol > len(m.tableCols) {
		endCol = len(m.tableCols)
	}

	var lines []string

	// Header
	header := rowNumStyle.Render(fmt.Sprintf("%*s", rowNumW, "#"))
	for i := startCol; i < endCol; i++ {
		name := truncate(m.tableCols[i], colWidth-1)
		header += headerStyle.Render(fmt.Sprintf(" %-*s", colWidth-1, name))
	}
	lines = append(lines, header)

	// Data rows
	maxRows := h - 1 // minus header
	for r := 0; r < maxRows && r < len(m.tableData); r++ {
		rowNum := m.tableOffset + r + 1
		line := rowNumStyle.Render(fmt.Sprintf("%*d", rowNumW, rowNum))
		row := m.tableData[r]
		for i := startCol; i < endCol && i < len(row); i++ {
			val := truncate(row[i], colWidth-1)
			if row[i] == "NULL" {
				line += nullStyle.Render(fmt.Sprintf(" %-*s", colWidth-1, val))
			} else {
				line += cellStyle.Render(fmt.Sprintf(" %-*s", colWidth-1, val))
			}
		}
		lines = append(lines, line)
	}

	return strings.Join(lines, "\n")
}

func (m Model) viewColumns(w, h int) string {
	var lines []string

	// Search bar
	switch {
	case m.searchFocused:
		lines = append(lines, searchPromptStyle.Render("/")+m.searchInput.View())
	case m.searchQuery != "":
		lines = append(lines, searchPromptStyle.Render("/ ")+m.searchQuery)
	default:
		lines = append(lines, searchPromptStyle.Render("/ (type / to search)"))
	}
	lines = append(lines, "")

	// Column list
	listHeight := h - 2
	startIdx := 0
	if m.colCursor >= listHeight {
		startIdx = m.colCursor - listHeight + 1
	}

	for i := startIdx; i < len(m.filteredCols) && i < startIdx+listHeight; i++ {
		col := m.filteredCols[i]
		mark := unselectedMark
		if m.sel.IsSelected(col.Name) {
			mark = selectedMark
		}

		name := truncate(col.Name, w-12)
		typeBadge := typeBadgeStyle.Render(truncate(col.DuckType, 8))

		// Inline stats
		stats := ""
		if s, ok := m.summaries[col.Name]; ok && s.Loaded {
			stats = statStyle.Render(fmt.Sprintf(" M:%.0f%% D:%.0f%%", s.MissingPct, s.DistinctPct))
		}

		line := fmt.Sprintf("%s %s %s%s", mark, name, typeBadge, stats)

		if i == m.colCursor && m.focus == FocusColumns {
			line = highlightStyle.Render(line)
		}

		lines = append(lines, line)
	}

	return strings.Join(lines, "\n")
}

func (m Model) viewDetail(w int) string {
	col := m.detailCol
	var lines []string

	lines = append(lines, detailTitleStyle.Render(col))

	// Find type
	var colType string
	for _, c := range m.columns {
		if c.Name == col {
			colType = c.DuckType
			break
		}
	}
	lines = append(lines, detailLabelStyle.Render("Type: ")+detailValueStyle.Render(colType))
	lines = append(lines, "")

	s, ok := m.summaries[col]
	if !ok || !s.Loaded {
		lines = append(lines, "Loading...")
		return strings.Join(lines, "\n")
	}

	lines = append(lines, detailLabelStyle.Render("Missing: ")+detailValueStyle.Render(fmt.Sprintf("%d (%.1f%%)", s.MissingCount, s.MissingPct)))
	lines = append(lines, detailLabelStyle.Render("Distinct: ")+detailValueStyle.Render(fmt.Sprintf("~%d (%.1f%%)", s.DistinctApprox, s.DistinctPct)))
	lines = append(lines, "")

	tabs := []string{"Top Values", "Stats", "Histogram"}
	tabLine := ""
	for i, t := range tabs {
		if i == m.detailTab {
			tabLine += detailTitleStyle.Render("["+t+"]") + " "
		} else {
			tabLine += detailLabelStyle.Render(" "+t+" ") + " "
		}
	}
	lines = append(lines, tabLine)
	lines = append(lines, "")

	if !s.DetailLoaded {
		lines = append(lines, "Computing...")
		return strings.Join(lines, "\n")
	}

	switch m.detailTab {
	case 0: // Top Values
		if len(s.Top3) > 0 {
			for _, tv := range s.Top3 {
				lines = append(lines, fmt.Sprintf("  %s: %d (%.1f%%)", truncate(tv.Value, w-20), tv.Count, tv.Pct))
			}
			otherPct := 100.0
			for _, tv := range s.Top3 {
				otherPct -= tv.Pct
			}
			if otherPct > 0.1 {
				lines = append(lines, fmt.Sprintf("  Other: %.1f%%", otherPct))
			}
		} else {
			lines = append(lines, "  High cardinality — no top values")
		}

	case 1: // Stats
		if s.Numeric != nil {
			lines = append(lines, fmt.Sprintf("  Min:    %.4g", s.Numeric.Min))
			lines = append(lines, fmt.Sprintf("  Max:    %.4g", s.Numeric.Max))
			lines = append(lines, fmt.Sprintf("  Mean:   %.4g", s.Numeric.Mean))
			lines = append(lines, fmt.Sprintf("  Stddev: %.4g", s.Numeric.Stddev))
		} else {
			lines = append(lines, "  Not a numeric column")
		}

	case 2: // Histogram
		if s.Hist != nil && len(s.Hist.Bins) > 0 {
			maxCount := int64(0)
			for _, b := range s.Hist.Bins {
				if b.Count > maxCount {
					maxCount = b.Count
				}
			}
			barWidth := w - 20
			if barWidth < 10 {
				barWidth = 10
			}
			for _, b := range s.Hist.Bins {
				barLen := 0
				if maxCount > 0 {
					barLen = int(float64(b.Count) / float64(maxCount) * float64(barWidth))
				}
				label := fmt.Sprintf("%8.2g", b.Low)
				bar := strings.Repeat("█", barLen)
				lines = append(lines, fmt.Sprintf("  %s |%s %d", label, bar, b.Count))
			}
		} else {
			lines = append(lines, "  No histogram available")
		}
	}

	lines = append(lines, "")
	lines = append(lines, detailLabelStyle.Render("t:tab  n:jump-to-null  Esc:close"))

	return strings.Join(lines, "\n")
}

func (m Model) viewHelp() string {
	help := []struct{ key, desc string }{
		{"Tab", "Switch focus (Table ↔ Columns)"},
		{"q / Ctrl+C", "Quit"},
		{"?", "Toggle help"},
		{"", ""},
		{"── Columns Pane ──", ""},
		{"/", "Focus search"},
		{"Esc", "Unfocus search"},
		{"Ctrl+U", "Clear search"},
		{"↑/↓ or j/k", "Move cursor"},
		{"Space", "Toggle column selection"},
		{"s", "Add all visible to selection"},
		{"d", "Remove all visible from selection"},
		{"a", "Select all columns"},
		{"x", "Clear selection"},
		{"y", "Copy selected as Python list"},
		{"Enter", "Open column detail"},
		{"", ""},
		{"── Table Pane ──", ""},
		{"h/j/k/l or arrows", "Scroll"},
		{"g / G", "Top / Bottom"},
		{"[ / ]", "Page scroll left / right"},
		{"S", "Toggle show selected columns only"},
		{"f", "Toggle null-row filter"},
		{"", ""},
		{"── Detail Panel ──", ""},
		{"t", "Cycle tabs (Top Values / Stats / Histogram)"},
		{"n", "Jump to first null"},
		{"Esc", "Close"},
	}

	var lines []string
	lines = append(lines, detailTitleStyle.Render("  Keybindings"))
	lines = append(lines, "")
	for _, h := range help {
		switch {
		case h.key == "":
			lines = append(lines, "")
		case h.desc == "":
			lines = append(lines, helpKeyStyle.Render("  "+h.key))
		default:
			lines = append(lines, fmt.Sprintf("  %s  %s",
				helpKeyStyle.Width(20).Render(h.key),
				helpDescStyle.Render(h.desc)))
		}
	}
	lines = append(lines, "")
	lines = append(lines, detailLabelStyle.Render("  Press Esc or ? to close"))

	content := strings.Join(lines, "\n")

	// Center in screen
	style := lipgloss.NewStyle().
		Width(m.width-4).
		Height(m.height-2).
		Padding(1, 2).
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("62"))

	return style.Render(content)
}

func truncate(s string, maxLen int) string {
	if maxLen <= 0 {
		return ""
	}
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 1 {
		return "…"
	}
	return s[:maxLen-1] + "…"
}

func (m Model) columnType(colName string) string {
	for _, c := range m.columns {
		if c.Name == colName {
			return c.DuckType
		}
	}
	return ""
}

func (m Model) activeRowCount() int64 {
	if m.rowFilter != "" && m.filterRows >= 0 {
		return m.filterRows
	}
	return m.totalRows
}

func (m Model) maxTableOffset() int {
	active := m.activeRowCount()
	if active <= 0 {
		return 0
	}
	maxOffset := int(active) - m.pageSize
	if maxOffset < 0 {
		return 0
	}
	return maxOffset
}

func (m *Model) clampTableOffset() {
	if m.tableOffset < 0 {
		m.tableOffset = 0
		return
	}
	maxOffset := m.maxTableOffset()
	if m.tableOffset > maxOffset {
		m.tableOffset = maxOffset
	}
}
