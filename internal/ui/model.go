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

	// Unified column cursor — single source of truth across both panes
	selectedColName string

	// Table state
	tableData       [][]string
	tableCols       []string // column names in current projection
	tableOffset     int      // row offset for pagination
	tableRowCursor  int      // row cursor position within visible page
	tableColOffHint int      // preferred column offset; -1 = auto
	showSelected   bool     // show only selected columns
	rowFilter      string   // active SQL filter
	totalRows      int64
	filterRows     int64 // -1 means no filter active

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

	var firstCol string
	if len(cols) > 0 {
		firstCol = cols[0].Name
	}

	m := Model{
		engine:          eng,
		fileName:        fileName,
		columns:         cols,
		sel:             selection.New(names),
		searchInput:     ti,
		summaries:       make(map[string]*types.ColumnSummary),
		filterRows:      -1,
		tableColOffHint: -1,
		totalRows:       eng.TotalRows(),
		pageSize:        50,
		focus:           FocusTable,
		selectedColName: firstCol,
	}
	m.updateFilteredCols()
	return m
}

// tableColCursor returns the index of selectedColName in tableCols, or -1.
func (m Model) tableColCursor() int {
	for i, name := range m.tableCols {
		if name == m.selectedColName {
			return i
		}
	}
	return -1
}

// computeTableColOff returns the scroll offset to keep the column cursor visible.
// If tableColOffHint is set and the cursor is visible within that viewport, use it.
func (m Model) computeTableColOff(visibleCols int) int {
	cursor := m.tableColCursor()
	if h := m.tableColOffHint; h >= 0 && cursor >= h && cursor < h+visibleCols {
		return h
	}
	if cursor < 0 || cursor < visibleCols {
		return 0
	}
	return cursor - visibleCols + 1
}

// syncSelectedColFromCursor sets selectedColName from the columns pane cursor.
func (m *Model) syncSelectedColFromCursor() {
	if m.colCursor >= 0 && m.colCursor < len(m.filteredCols) {
		m.selectedColName = m.filteredCols[m.colCursor].Name
	}
}

// syncCursorFromSelectedColName finds selectedColName in filteredCols and sets colCursor.
func (m *Model) syncCursorFromSelectedColName() {
	for i, c := range m.filteredCols {
		if c.Name == m.selectedColName {
			m.colCursor = i
			return
		}
	}
	// Not found — keep colCursor as-is
}

func (m Model) columnsCursorColName() string {
	if m.colCursor >= 0 && m.colCursor < len(m.filteredCols) {
		return m.filteredCols[m.colCursor].Name
	}
	return ""
}

func (m Model) columnsHasFilteredCol(name string) bool {
	for _, col := range m.filteredCols {
		if col.Name == name {
			return true
		}
	}
	return false
}

// columnsActiveColName returns the column that actions (x, enter) operate on
// in the columns pane. The crosshair column (selectedColName) takes priority
// over the colCursor position when both are visible in the filtered list.
// After reconcileSelectedColNameWithTableCols (e.g. projection change while a
// search filter is active), selectedColName may differ from colCursor — the
// highlight and actions will follow selectedColName, not the cursor position.
func (m Model) columnsActiveColName() string {
	if m.selectedColName != "" && m.columnsHasFilteredCol(m.selectedColName) {
		return m.selectedColName
	}
	return m.columnsCursorColName()
}

func (m *Model) updateFilteredCols() {
	var filtered []types.ColumnInfo
	for _, c := range m.columns {
		if util.FuzzyMatch(c.Name, m.searchQuery) {
			filtered = append(filtered, c)
		}
	}
	m.filteredCols = filtered

	// Try to preserve selectedColName position
	found := false
	for i, c := range m.filteredCols {
		if c.Name == m.selectedColName {
			m.colCursor = i
			found = true
			break
		}
	}
	if !found {
		if m.colCursor >= len(m.filteredCols) {
			m.colCursor = max(0, len(m.filteredCols)-1)
		}
		if len(m.filteredCols) > 0 {
			m.selectedColName = m.filteredCols[m.colCursor].Name
		} else {
			m.selectedColName = ""
		}
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
		prevOffset := m.tableOffset
		m.clampTableOffset()
		m.clampTableRowCursor()
		if m.tableOffset != prevOffset {
			return m, m.loadPreview()
		}
		return m, nil

	case tea.KeyMsg:
		return m.handleKey(msg)

	case previewDoneMsg:
		if msg.err != nil {
			m.statusMsg = fmt.Sprintf("Error: %v", msg.err)
		} else {
			m.tableData = msg.rows
			m.tableCols = msg.colNames
			m.reconcileSelectedColNameWithTableCols()
			m.totalRows = msg.totalRows
			if msg.filterRows >= 0 {
				m.filterRows = msg.filterRows
			}
			prevOffset := m.tableOffset
			m.clampTableOffset()
			if m.tableOffset != prevOffset {
				return m, m.loadPreview()
			}
			m.clampTableRowCursor()
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
			m.tableRowCursor = 0
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
	case "s", "S":
		m.showSelected = !m.showSelected
		m.tableRowCursor = 0
		return m, m.loadPreview()
	case "enter":
		targetCol := m.selectedColName
		if m.focus == FocusColumns {
			targetCol = m.columnsActiveColName()
		}
		if targetCol != "" {
			m.detailCol = targetCol
			m.detailTab = 0
			m.overlay = OverlayDetail
			if s, ok := m.summaries[m.detailCol]; !ok || !s.DetailLoaded {
				var existing *types.ColumnSummary
				if ok && s != nil {
					copySummary := *s
					existing = &copySummary
				}
				return m, m.loadDetail(m.detailCol, existing, m.columnType(m.detailCol))
			}
		}
		return m, nil
	case " ":
		if m.focus == FocusColumns {
			return m.handleColumnsPaging()
		}
		return m.handleTablePageDown()
	}

	if m.focus == FocusColumns {
		return m.handleColumnsKey(key)
	}
	return m.handleTableKey(key)
}

func (m Model) handleColumnsPaging() (tea.Model, tea.Cmd) {
	// Page down in column list
	_, h := m.columnsPaneDimensions()
	listHeight := m.columnsListHeight(h)
	newCursor := m.colCursor + listHeight
	if newCursor >= len(m.filteredCols) {
		newCursor = len(m.filteredCols) - 1
	}
	if newCursor < 0 {
		newCursor = 0
	}
	m.colCursor = newCursor
	m.syncSelectedColFromCursor()
	return m, nil
}

func (m Model) columnsPaneDimensions() (int, int) {
	mainHeight := m.height - statusBarH
	tableWidth := m.width * tableSplitPct / 100
	colWidth := m.width - tableWidth
	w := colWidth - paneBorderW
	h := mainHeight - paneBorderH
	if w < 0 {
		w = 0
	}
	if h < 0 {
		h = 0
	}
	return w, h
}

func (m Model) columnsListHeight(h int) int {
	listHeight := h - 3
	if listHeight < 1 {
		listHeight = 1
	}
	return listHeight
}

func (m Model) tablePaneDimensions() (int, int) {
	mainHeight := m.height - statusBarH
	tableWidth := m.width * tableSplitPct / 100
	return tableWidth - paneBorderW, mainHeight - paneBorderH
}

func (m Model) tableDataRowsHeight(h int) int {
	maxRows := h - 2
	if maxRows < 0 {
		maxRows = 0
	}
	return maxRows
}

func (m Model) visibleTableRows() int {
	_, h := m.tablePaneDimensions()
	rows := m.tableDataRowsHeight(h)
	if rows > 0 {
		rows-- // reserve one line for the footer
	}
	return rows
}

func (m *Model) clampTableRowCursor() {
	maxRows := m.visibleTableRows()
	if len(m.tableData) < maxRows {
		maxRows = len(m.tableData)
	}
	maxCursor := maxRows - 1
	if maxCursor < 0 {
		maxCursor = 0
	}
	if m.tableRowCursor > maxCursor {
		m.tableRowCursor = maxCursor
	}
	if m.tableRowCursor < 0 {
		m.tableRowCursor = 0
	}
}

func (m *Model) reconcileSelectedColNameWithTableCols() {
	if len(m.tableCols) == 0 {
		m.selectedColName = ""
		return
	}
	for _, name := range m.tableCols {
		if name == m.selectedColName {
			m.syncCursorFromSelectedColName()
			return
		}
	}
	m.selectedColName = m.tableCols[0]
	m.syncCursorFromSelectedColName()
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
			m.syncSelectedColFromCursor()
		}
	case "down", "j":
		if m.colCursor < len(m.filteredCols)-1 {
			m.colCursor++
			m.syncSelectedColFromCursor()
		}
	case "x":
		targetCol := m.columnsActiveColName()
		if targetCol != "" {
			m.sel.Toggle(targetCol)
			if m.showSelected {
				// If we just deselected a column, it will vanish from
				// the projection. Refresh filtered cols and reconcile
				// the cursor so selectedColName stays valid while the
				// preview reloads.
				if !m.sel.IsSelected(targetCol) {
					m.updateFilteredCols()
					if m.colCursor >= len(m.filteredCols) {
						m.colCursor = len(m.filteredCols) - 1
					}
					if m.colCursor < 0 {
						m.colCursor = 0
					}
					// Only re-derive selectedColName when the deselected column
					// was the active one. Otherwise selectedColName is still valid
					// by name even though colCursor may have shifted indices.
					if targetCol == m.selectedColName || !m.columnsHasFilteredCol(m.selectedColName) {
						m.syncSelectedColFromCursor()
					}
				}
				return m, m.loadPreview()
			}
		}
	case "a":
		names := make([]string, len(m.filteredCols))
		for i, c := range m.filteredCols {
			names[i] = c.Name
		}
		m.sel.AddAll(names)
		if m.showSelected {
			return m, m.loadPreview()
		}
	case "d":
		names := make([]string, len(m.filteredCols))
		for i, c := range m.filteredCols {
			names[i] = c.Name
		}
		m.sel.RemoveAll(names)
		if m.showSelected {
			return m, m.loadPreview()
		}
	case "A":
		m.sel.SelectAll()
		if m.showSelected {
			return m, m.loadPreview()
		}
	case "X":
		m.sel.Clear()
		if m.showSelected {
			return m, m.loadPreview()
		}
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
	}
	return m, nil
}

func (m Model) handleTablePageDown() (tea.Model, tea.Cmd) {
	return m.pageTableOffset(m.pageSize)
}

func (m Model) pageTableOffset(delta int) (tea.Model, tea.Cmd) {
	prevOffset := m.tableOffset
	maxOff := m.maxTableOffset()
	m.tableOffset += delta
	if m.tableOffset < 0 {
		m.tableOffset = 0
	}
	if m.tableOffset > maxOff {
		m.tableOffset = maxOff
	}
	if m.tableOffset == prevOffset {
		return m, nil
	}
	m.clampTableRowCursor() // clamp against stale data; re-clamped in previewDoneMsg handler
	return m, m.loadPreview()
}

func (m Model) handleTableKey(key string) (tea.Model, tea.Cmd) {
	m.clampTableRowCursor()
	switch key {
	case "up", "k":
		if m.tableRowCursor > 0 {
			m.tableRowCursor--
		} else if m.tableOffset > 0 {
			m.tableOffset--
			// tableRowCursor stays at 0
			return m, m.loadPreview()
		}
	case "down", "j":
		maxVisibleRows := m.visibleTableRows()
		if len(m.tableData) < maxVisibleRows {
			maxVisibleRows = len(m.tableData)
		}
		maxVisibleCursor := maxVisibleRows - 1
		if maxVisibleCursor < 0 {
			maxVisibleCursor = 0
		}
		if m.tableRowCursor < maxVisibleCursor {
			m.tableRowCursor++
		} else {
			maxOff := m.maxTableOffset()
			if m.tableOffset < maxOff {
				m.tableOffset++
				// tableRowCursor stays at bottom
				return m, m.loadPreview()
			}
		}
	case "left", "h":
		m.tableColOffHint = -1
		idx := m.tableColCursor()
		if idx > 0 {
			m.selectedColName = m.tableCols[idx-1]
			m.syncCursorFromSelectedColName()
		} else if idx < 0 && len(m.tableCols) > 0 {
			m.selectedColName = m.tableCols[0]
			m.syncCursorFromSelectedColName()
		}
	case "right", "l":
		m.tableColOffHint = -1
		idx := m.tableColCursor()
		if idx >= 0 && idx < len(m.tableCols)-1 {
			m.selectedColName = m.tableCols[idx+1]
			m.syncCursorFromSelectedColName()
		} else if idx < 0 && len(m.tableCols) > 0 {
			m.selectedColName = m.tableCols[0]
			m.syncCursorFromSelectedColName()
		}
	case "0":
		m.tableColOffHint = -1
		if len(m.tableCols) > 0 {
			m.selectedColName = m.tableCols[0]
			m.syncCursorFromSelectedColName()
		}
	case "$":
		m.tableColOffHint = -1
		if len(m.tableCols) > 0 {
			m.selectedColName = m.tableCols[len(m.tableCols)-1]
			m.syncCursorFromSelectedColName()
		}
	case "[":
		return m.pageColumnsHorizontal(-1)
	case "]":
		return m.pageColumnsHorizontal(1)
	case "g":
		m.tableOffset = 0
		m.tableRowCursor = 0
		return m, m.loadPreview()
	case "G":
		m.tableOffset = m.maxTableOffset()
		m.tableRowCursor = max(0, m.visibleTableRows()-1)
		return m, m.loadPreview()
	case "ctrl+f":
		return m.pageTableOffset(m.pageSize)
	case "ctrl+b":
		return m.pageTableOffset(-m.pageSize)
	case "ctrl+d":
		return m.pageTableOffset(m.pageSize / 2)
	case "ctrl+u":
		return m.pageTableOffset(-(m.pageSize / 2))
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
		m.tableRowCursor = 0
		return m, m.loadPreview()
	}
	return m, nil
}

// visibleColCount returns how many columns fit in the table pane.
func (m Model) visibleColCount() int {
	w, _ := m.tablePaneDimensions()
	visibleCols := (w - tableRowNumW - tableRowPrefixW) / tableColWidth
	if visibleCols < 1 {
		visibleCols = 1
	}
	return visibleCols
}

// pageColumnsHorizontal scrolls the column viewport by one screenful.
// direction should be -1 (left) or +1 (right).
func (m Model) pageColumnsHorizontal(direction int) (tea.Model, tea.Cmd) {
	if len(m.tableCols) == 0 {
		return m, nil
	}
	visibleCols := m.visibleColCount()
	startCol := m.computeTableColOff(visibleCols)
	newStart := startCol + direction*visibleCols
	if newStart < 0 {
		newStart = 0
	}
	maxStart := len(m.tableCols) - visibleCols
	if maxStart < 0 {
		maxStart = 0
	}
	if newStart > maxStart {
		newStart = maxStart
	}
	if newStart == startCol {
		return m, nil
	}
	var newIdx int
	if direction < 0 {
		newIdx = newStart // land at left edge when paging left
	} else {
		newIdx = newStart + visibleCols - 1 // land at right edge when paging right
	}
	if newIdx >= len(m.tableCols) {
		newIdx = len(m.tableCols) - 1
	}
	if newIdx < 0 {
		newIdx = 0
	}
	m.tableColOffHint = newStart
	m.selectedColName = m.tableCols[newIdx]
	m.syncCursorFromSelectedColName()
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

	mainHeight := m.height - statusBarH // top + bottom bars

	// Split: table gets tableSplitPct%, columns gets the rest
	tableWidth := m.width * tableSplitPct / 100
	colWidth := m.width - tableWidth

	tableView := m.viewTable(tableWidth-paneBorderW, mainHeight-paneBorderH)
	colView := m.viewColumns(colWidth-paneBorderW, mainHeight-paneBorderH)

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
		hints = "/:search  x:toggle  a:add  d:rm  y:copy  Enter:detail"
	} else {
		hints = "hjkl:move  space:pgdn  []:col-page  f:null-filter"
	}
	status := fmt.Sprintf("  Sel: %d/%d", selCount, len(m.columns))
	if m.showSelected {
		status += "  [show-sel]"
	}
	if m.statusMsg != "" {
		status += "  " + m.statusMsg
	}
	return bottomBarStyle.Width(m.width).Render(hints + status)
}

func (m Model) viewTable(w, h int) string {
	if len(m.tableCols) == 0 {
		return "No data loaded"
	}
	if h <= 0 {
		return ""
	}

	// How many columns fit
	visibleCols := m.visibleColCount()

	startCol := m.computeTableColOff(visibleCols)
	if startCol >= len(m.tableCols) {
		startCol = max(0, len(m.tableCols)-1)
	}
	endCol := startCol + visibleCols
	if endCol > len(m.tableCols) {
		endCol = len(m.tableCols)
	}

	cursorColIdx := m.tableColCursor()

	var lines []string

	// Header (space prefix for alignment with row null dots)
	header := " " + rowNumStyle.Render(fmt.Sprintf("%*s", tableRowNumW, "#"))
	for i := startCol; i < endCol; i++ {
		name := truncate(m.tableCols[i], tableColWidth-2)
		nameStr := fmt.Sprintf(" %-*s", tableColWidth-2, name)
		// Check if column has nulls from profiling
		hasNulls := false
		if s, ok := m.summaries[m.tableCols[i]]; ok && s.Loaded && s.MissingCount > 0 {
			hasNulls = true
		}
		if i == cursorColIdx {
			if hasNulls {
				header += activeColHeaderStyle.Render(nameStr) + nullDotActiveHeader
			} else {
				header += activeColHeaderStyle.Render(nameStr + " ")
			}
		} else {
			if hasNulls {
				header += headerStyle.Render(nameStr) + nullDotHeader
			} else {
				header += headerStyle.Render(nameStr + " ")
			}
		}
	}
	lines = append(lines, header)

	// Data rows — footer is only rendered when there's room for it.
	maxRows := m.tableDataRowsHeight(h)
	renderFooter := maxRows > 0
	if renderFooter {
		maxRows-- // reserve one line for the footer
	}
	// Clamp cursor for rendering in case data hasn't loaded yet after navigation
	renderCursor := m.tableRowCursor
	if renderCursor >= maxRows {
		renderCursor = max(0, maxRows-1)
	}
	for r := 0; r < maxRows && r < len(m.tableData); r++ {
		isSelectedRow := r == renderCursor
		rowNum := m.tableOffset + r + 1

		// Check if row has any nulls (across all columns, not just visible)
		rowHasNull := false
		for _, v := range m.tableData[r] {
			if v == "NULL" {
				rowHasNull = true
				break
			}
		}
		rowDot := " "
		if rowHasNull {
			rowDot = nullDot
		}

		// Row number
		var line string
		if isSelectedRow {
			line = rowDot + activeRowNumStyle.Render(fmt.Sprintf("%*d", tableRowNumW, rowNum))
		} else {
			line = rowDot + rowNumStyle.Render(fmt.Sprintf("%*d", tableRowNumW, rowNum))
		}
		line += m.renderRowCells(m.tableData[r], startCol, endCol, tableColWidth, cursorColIdx, isSelectedRow)
		lines = append(lines, line)
	}

	// Footer row with null counts
	if renderFooter {
		footerPrefix := strings.Repeat(" ", tableFooterPrefixW)
		footer := truncate(m.viewTableFooter(), max(0, w-tableRowPrefixW-tableFooterPrefixW))
		lines = append(lines, " "+rowNumStyle.Render(footerPrefix+footer))
	}

	return strings.Join(lines, "\n")
}

func (m Model) renderRowCells(row []string, startCol, endCol, colWidth, cursorColIdx int, isSelectedRow bool) string {
	var s string
	for i := startCol; i < endCol && i < len(row); i++ {
		val := truncate(row[i], colWidth-1)
		cell := fmt.Sprintf(" %-*s", colWidth-1, val)
		isNull := row[i] == "NULL"
		isSelectedCol := i == cursorColIdx

		switch {
		case isSelectedRow && isSelectedCol && isNull:
			s += crosshairNullStyle.Render(cell)
		case isSelectedRow && isSelectedCol:
			s += crosshairCellStyle.Render(cell)
		case isSelectedRow && isNull:
			s += activeRowNullStyle.Render(cell)
		case isSelectedRow:
			s += activeRowCellStyle.Render(cell)
		case isSelectedCol && isNull:
			s += activeColNullStyle.Render(cell)
		case isSelectedCol:
			s += activeColCellStyle.Render(cell)
		case isNull:
			s += nullStyle.Render(cell)
		default:
			s += cellStyle.Render(cell)
		}
	}
	return s
}

func (m Model) viewTableFooter() string {
	if len(m.tableData) == 0 {
		return ""
	}

	var parts []string

	// Row null count — clamp cursor defensively for transient states where
	// tableData may have shrunk (e.g. after filter/resize).
	rowCursor := m.tableRowCursor
	if len(m.tableData) > 0 {
		if rowCursor < 0 {
			rowCursor = 0
		}
		if rowCursor >= len(m.tableData) {
			rowCursor = len(m.tableData) - 1
		}
	}
	if rowCursor >= 0 && rowCursor < len(m.tableData) {
		row := m.tableData[rowCursor]
		nullCount := 0
		for _, v := range row {
			if v == "NULL" {
				nullCount++
			}
		}
		absRow := m.tableOffset + rowCursor + 1
		parts = append(parts, fmt.Sprintf("Row %d: %d/%d missing (projected)", absRow, nullCount, len(row)))
	}

	// Column info from profiling
	if m.selectedColName != "" {
		colName := truncate(m.selectedColName, 20)
		colType := truncate(m.columnType(m.selectedColName), 20)
		if s, ok := m.summaries[m.selectedColName]; ok && s.Loaded {
			parts = append(parts, fmt.Sprintf("Col %q (%s): %d missing (%.1f%%)", colName, colType, s.MissingCount, s.MissingPct))
		} else {
			parts = append(parts, fmt.Sprintf("Col %q (%s): ...", colName, colType))
		}
	}

	return strings.Join(parts, "    ")
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
	listHeight := m.columnsListHeight(h)
	startIdx := 0
	if m.colCursor >= listHeight {
		startIdx = m.colCursor - listHeight + 1
	}
	activeCol := m.columnsActiveColName()

	for i := startIdx; i < len(m.filteredCols) && i < startIdx+listHeight; i++ {
		col := m.filteredCols[i]
		isHighlighted := col.Name == activeCol

		name := truncate(col.Name, w-12)
		typeStr := truncate(col.DuckType, 8)
		statsStr := ""
		if s, ok := m.summaries[col.Name]; ok && s.Loaded {
			statsStr = fmt.Sprintf(" M:%.0f%% D:%.0f%%", s.MissingPct, s.DistinctPct)
		}

		if isHighlighted {
			// Build line from plain text so highlight style controls the whole row
			markChar := unselectedMarkGlyph
			if m.sel.IsSelected(col.Name) {
				markChar = selectedMarkGlyph
			}
			plain := fmt.Sprintf("%s %s %s%s", markChar, name, typeStr, statsStr)
			if m.focus == FocusColumns {
				lines = append(lines, highlightStyle.Width(w).Render(plain))
			} else {
				lines = append(lines, dimHighlightStyle.Width(w).Render(plain))
			}
		} else {
			mark := unselectedMark
			if m.sel.IsSelected(col.Name) {
				mark = selectedMark
			}
			typeBadge := typeBadgeStyle.Render(typeStr)
			stats := statStyle.Render(statsStr)
			lines = append(lines, fmt.Sprintf("%s %s %s%s", mark, name, typeBadge, stats))
		}
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
		{"s", "Toggle show selected columns only"},
		{"Space", "Page down (rows or columns list)"},
		{"Enter", "Open column detail"},
		{"", ""},
		{"── Columns Pane ──", ""},
		{"/", "Focus search"},
		{"Esc", "Unfocus search"},
		{"Ctrl+U", "Clear search"},
		{"↑/↓ or j/k", "Move cursor"},
		{"x", "Toggle selection (crosshair col)"},
		{"a", "Add all filtered to selection"},
		{"d", "Remove all filtered from selection"},
		{"A", "Select ALL columns"},
		{"X", "Clear all selections"},
		{"y", "Copy selected as Python list"},
		{"", ""},
		{"── Table Pane ──", ""},
		{"↑/↓ or j/k", "Move row cursor"},
		{"←/→ or h/l", "Move column cursor"},
		{"0 / $", "First / last column"},
		{"[ / ]", "Page columns left / right"},
		{"g / G", "Top / Bottom of file"},
		{"Ctrl+F / Space", "Page down"},
		{"Ctrl+B", "Page up"},
		{"Ctrl+D / Ctrl+U", "Half page down / up"},
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

	navigableRows := m.visibleTableRows()
	if m.pageSize > 0 && m.pageSize < navigableRows {
		navigableRows = m.pageSize
	}
	if navigableRows == 0 {
		return max(0, int(active)-1)
	}

	maxOffset := int(active) - navigableRows
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
