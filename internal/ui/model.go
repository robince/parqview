package ui

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

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
	OverlayFilePicker
)

// Messages
type profileBasicDoneMsg struct {
	colName string
	summary *types.ColumnSummary
	token   uint64
	err     error
}

type profileDetailDoneMsg struct {
	colName string
	summary *types.ColumnSummary
	token   uint64
	err     error
}

type previewDoneMsg struct {
	rows       [][]string
	colNames   []string
	totalRows  int64
	filterRows int64
	seq        uint64
	token      uint64
	err        error
}

type firstNullMsg struct {
	rowID  int64
	offset int64
	token  uint64
	err    error
}

type openFileDoneMsg struct {
	path string
	eng  *engine.Engine
	err  error
}

type filePickerItem struct {
	name     string
	path     string
	isDir    bool
	isParent bool
}

type statusMsg string

// Model is the root Bubble Tea model.
type Model struct {
	engine    *engine.Engine
	fileName  string
	launchDir string

	// Schema
	columns []types.ColumnInfo
	sel     *selection.Set

	// Search
	searchInput   textinput.Model
	searchFocused bool
	searchQuery   string

	// File picker
	pickerInput  textinput.Model
	pickerDir    string
	pickerItems  []filePickerItem
	pickerCursor int
	pickerQuery  string

	// Column list state
	filteredCols []types.ColumnInfo // columns matching search
	colCursor    int                // cursor in filteredCols

	// Unified column cursor — single source of truth across both panes
	selectedColName string

	// Table state
	tableData       [][]string
	tableRowHasNull []bool
	tableCols       []string // column names in current projection
	tableOffset     int      // row offset for pagination
	tableRowCursor  int      // row cursor position within visible page
	tableColOffHint int      // preferred column offset; -1 = auto
	showSelected    bool     // show only selected columns
	rowFilter       string   // active SQL filter
	totalRows       int64
	filterRows      int64 // -1 means no filter active

	// Profiling
	summaries map[string]*types.ColumnSummary

	// UI state
	focus           Focus
	overlay         Overlay
	detailCol       string // column shown in detail panel
	detailTab       int    // 0=TopValues, 1=Stats, 2=Histogram
	width           int
	height          int
	statusMsg       string
	ready           bool
	tableSplitPct   int
	draggingDivider bool

	pageSize         int // rows per page
	latestPreviewSeq uint64
	dataToken        uint64
}

// NewModel creates the initial model.
func NewModel(eng *engine.Engine, fileName, launchDir string) Model {
	ti := textinput.New()
	ti.Prompt = "/ "
	ti.CharLimit = 256

	pickerTI := textinput.New()
	pickerTI.Prompt = ""
	pickerTI.CharLimit = 256
	pickerTI.Focus()

	m := Model{
		engine:           nil,
		fileName:         "",
		columns:          nil,
		sel:              selection.New(nil),
		searchInput:      ti,
		pickerInput:      pickerTI,
		summaries:        make(map[string]*types.ColumnSummary),
		filterRows:       -1,
		tableColOffHint:  -1,
		totalRows:        0,
		pageSize:         50,
		focus:            FocusTable,
		selectedColName:  "",
		tableSplitPct:    tableSplitPct,
		latestPreviewSeq: 1,
	}
	m.setPickerRoot(launchDir)
	if eng != nil {
		m.applyEngine(eng, fileName)
	} else {
		m.updateFilteredCols()
		m.statusMsg = "No file loaded (Ctrl+O to open .parquet/.csv)"
	}
	return m
}

func (m *Model) Close() error {
	if m.engine == nil {
		return nil
	}
	err := m.engine.Close()
	m.engine = nil
	return err
}

func (m *Model) setPickerRoot(launchDir string) {
	root := launchDir
	if root == "" {
		root = "."
	}
	if abs, err := filepath.Abs(root); err == nil {
		root = abs
	}
	m.launchDir = root
	m.pickerDir = root
	m.pickerInput.SetValue("")
	m.pickerQuery = ""
	m.pickerCursor = 0
	m.refreshPickerItems()
}

func (m *Model) resetLoadedDataState() {
	m.columns = nil
	m.sel = selection.New(nil)
	m.searchInput.SetValue("")
	m.searchFocused = false
	m.searchQuery = ""
	m.filteredCols = nil
	m.colCursor = 0
	m.selectedColName = ""
	m.tableData = nil
	m.tableRowHasNull = nil
	m.tableCols = nil
	m.tableOffset = 0
	m.tableRowCursor = 0
	m.tableColOffHint = -1
	m.showSelected = false
	m.rowFilter = ""
	m.totalRows = 0
	m.filterRows = -1
	m.summaries = make(map[string]*types.ColumnSummary)
	m.detailCol = ""
	m.detailTab = 0
	m.updateFilteredCols()
}

func (m *Model) applyEngine(eng *engine.Engine, fileName string) {
	m.dataToken++
	m.latestPreviewSeq++
	m.engine = eng
	m.fileName = fileName
	m.resetLoadedDataState()

	cols := eng.Columns()
	m.columns = cols
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	m.sel = selection.New(names)
	m.totalRows = eng.TotalRows()
	if len(cols) > 0 {
		m.selectedColName = cols[0].Name
	}
	m.updateFilteredCols()
	m.statusMsg = fmt.Sprintf("Opened %s", fileName)
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
	if visibleCols <= 0 {
		return 0
	}
	cursor := m.tableColCursor()
	maxOff := len(m.tableCols) - visibleCols
	if maxOff < 0 {
		maxOff = 0
	}
	if h := m.tableColOffHint; h >= 0 && h <= maxOff && cursor >= h && cursor < h+visibleCols {
		return h
	}
	if cursor < visibleCols {
		return 0
	}
	return cursor - visibleCols + 1
}

func (m Model) splitPctBounds() (int, int) {
	if m.width <= 0 {
		return 50, 50
	}
	minPct := (minPaneOuterW*100 + m.width - 1) / m.width // ceil
	maxPct := 100 - minPct
	if minPct > maxPct {
		return 50, 50
	}
	return minPct, maxPct
}

func (m *Model) clampSplitPct() {
	if m.tableSplitPct <= 0 {
		m.tableSplitPct = tableSplitPct
	}
	minPct, maxPct := m.splitPctBounds()
	if m.tableSplitPct < minPct {
		m.tableSplitPct = minPct
	}
	if m.tableSplitPct > maxPct {
		m.tableSplitPct = maxPct
	}
}

func (m Model) tableOuterWidth() int {
	if m.width <= 0 {
		return 0
	}
	minPct, maxPct := m.splitPctBounds()
	pct := m.tableSplitPct
	if pct <= 0 {
		pct = tableSplitPct
	}
	if pct < minPct {
		pct = minPct
	}
	if pct > maxPct {
		pct = maxPct
	}
	w := m.width * pct / 100
	if w < 0 {
		return 0
	}
	if w > m.width {
		return m.width
	}
	return w
}

func (m Model) columnsOuterWidth() int {
	w := m.width - m.tableOuterWidth()
	if w < 0 {
		return 0
	}
	return w
}

func (m Model) mainHeight() int {
	h := m.height - statusBarH
	if h < 0 {
		return 0
	}
	return h
}

func (m Model) mainAreaContains(y int) bool {
	top := 1 // top bar occupies the first terminal row
	bottom := top + m.mainHeight()
	return y >= top && y < bottom
}

func (m Model) dividerX() int {
	return m.tableOuterWidth()
}

func (m Model) nearDivider(x int) bool {
	div := m.dividerX()
	return x >= div-dividerGrabRadius && x <= div+dividerGrabRadius
}

func (m *Model) setSplitFromMouseX(x int) {
	if m.width <= 0 {
		return
	}
	target := x + 1
	if target < 0 {
		target = 0
	}
	if target > m.width {
		target = m.width
	}
	m.tableSplitPct = (target*100 + m.width/2) / m.width
	m.clampSplitPct()
}

func (m Model) previewLimit() int {
	limit := m.visibleTableRows() + previewHeadroom
	if limit < previewMinRows {
		limit = previewMinRows
	}
	if limit > previewMaxRows {
		limit = previewMaxRows
	}
	if active := int(m.activeRowCount()); active > 0 && limit > active {
		limit = active
	}
	if limit < 1 {
		limit = 1
	}
	return limit
}

func (m Model) needsMorePreviewRows() bool {
	if len(m.tableCols) == 0 {
		return false
	}
	active := int(m.activeRowCount())
	if active <= 0 {
		return false
	}
	needed := m.previewLimit()
	remaining := active - m.tableOffset
	if remaining < needed {
		needed = max(0, remaining)
	}
	return len(m.tableData) < needed
}

func (m *Model) nextPreviewCmd() tea.Cmd {
	m.latestPreviewSeq++
	return m.loadPreviewCmd(m.latestPreviewSeq)
}

// TODO: add per-column widths; until then all columns use tableColWidth.
func (m Model) columnWidth() int {
	return tableColWidth
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
	if m.engine == nil {
		return nil
	}
	return tea.Batch(
		m.loadPreviewCmd(m.latestPreviewSeq),
		m.profileNext(),
	)
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.ready = true
		m.clampSplitPct()
		prevOffset := m.tableOffset
		m.clampTableOffset()
		m.clampTableRowCursor()
		if m.tableOffset != prevOffset {
			return m, m.nextPreviewCmd()
		}
		if m.needsMorePreviewRows() {
			return m, m.nextPreviewCmd()
		}
		return m, nil

	case tea.MouseMsg:
		return m.handleMouse(msg)

	case tea.KeyMsg:
		return m.handleKey(msg)

	case openFileDoneMsg:
		if msg.err != nil {
			m.statusMsg = fmt.Sprintf("Error opening file: %v", msg.err)
			return m, nil
		}
		if msg.eng == nil {
			m.statusMsg = "Error opening file: unknown error"
			return m, nil
		}
		old := m.engine
		m.applyEngine(msg.eng, filepath.Base(msg.path))
		if old != nil && old != msg.eng {
			_ = old.Close()
		}
		return m, tea.Batch(m.nextPreviewCmd(), m.profileNext())

	case previewDoneMsg:
		if msg.token != m.dataToken {
			return m, nil
		}
		if msg.seq < m.latestPreviewSeq {
			return m, nil
		}
		m.tableColOffHint = -1
		if msg.err != nil {
			m.statusMsg = fmt.Sprintf("Error: %v", msg.err)
		} else {
			m.tableData = msg.rows
			m.tableRowHasNull = rowHasNullFlags(msg.rows)
			m.tableCols = msg.colNames
			m.reconcileSelectedColNameWithTableCols()
			m.totalRows = msg.totalRows
			if msg.filterRows >= 0 {
				m.filterRows = msg.filterRows
			}
			prevOffset := m.tableOffset
			m.clampTableOffset()
			if m.tableOffset != prevOffset {
				return m, m.nextPreviewCmd()
			}
			m.clampTableRowCursor()
			if m.needsMorePreviewRows() {
				return m, m.nextPreviewCmd()
			}
		}
		return m, nil

	case profileBasicDoneMsg:
		if msg.token != m.dataToken {
			return m, nil
		}
		if msg.err == nil && msg.summary != nil {
			existing, exists := m.summaries[msg.colName]
			if !exists || existing == nil || !existing.DetailLoaded {
				m.summaries[msg.colName] = msg.summary
			}
		}
		// Chain: profile the next column
		return m, m.profileNext()

	case profileDetailDoneMsg:
		if msg.token != m.dataToken {
			return m, nil
		}
		if msg.err == nil && msg.summary != nil {
			m.summaries[msg.colName] = msg.summary
		}
		return m, nil

	case firstNullMsg:
		if msg.token != m.dataToken {
			return m, nil
		}
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
			return m, m.nextPreviewCmd()
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

	if key == "ctrl+o" {
		if m.overlay == OverlayFilePicker {
			m.overlay = OverlayNone
			return m, nil
		}
		m.openFilePicker()
		return m, textinput.Blink
	}

	if m.overlay == OverlayFilePicker {
		return m.handleFilePickerKey(msg)
	}

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
	case "ctrl+l":
		m.clampSplitPct()
		m.clampTableOffset()
		m.clampTableRowCursor()
		if m.needsMorePreviewRows() {
			return m, tea.Batch(tea.ClearScreen, m.nextPreviewCmd())
		}
		return m, tea.ClearScreen
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
		m.tableColOffHint = -1
		m.tableRowCursor = 0
		return m, m.nextPreviewCmd()
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

func (m Model) handleMouse(msg tea.MouseMsg) (tea.Model, tea.Cmd) {
	if (m.overlay != OverlayNone || m.searchFocused) && !(m.draggingDivider && msg.Action == tea.MouseActionRelease) {
		return m, nil
	}

	switch msg.Action {
	case tea.MouseActionPress:
		switch msg.Button {
		case tea.MouseButtonLeft:
			if m.mainAreaContains(msg.Y) && m.nearDivider(msg.X) {
				m.draggingDivider = true
				m.setSplitFromMouseX(msg.X)
			}
		case tea.MouseButtonWheelUp:
			if m.focus == FocusColumns {
				return m.handleColumnsKey("up")
			}
			return m.handleTableKey("up")
		case tea.MouseButtonWheelDown:
			if m.focus == FocusColumns {
				return m.handleColumnsKey("down")
			}
			return m.handleTableKey("down")
		}

	case tea.MouseActionMotion:
		if m.draggingDivider {
			m.setSplitFromMouseX(msg.X)
		}

	case tea.MouseActionRelease:
		if m.draggingDivider {
			m.draggingDivider = false
		}
	}

	return m, nil
}

func (m *Model) openFilePicker() {
	m.searchFocused = false
	m.overlay = OverlayFilePicker
	m.pickerDir = m.launchDir
	m.pickerInput.Focus()
	m.pickerInput.SetValue("")
	m.pickerQuery = ""
	m.pickerCursor = 0
	m.refreshPickerItems()
}

func (m Model) handleFilePickerKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	key := msg.String()
	switch key {
	case "esc":
		m.overlay = OverlayNone
		return m, nil
	case "up", "k":
		if m.pickerCursor > 0 {
			m.pickerCursor--
		}
		return m, nil
	case "down", "j":
		if m.pickerCursor < len(m.pickerItems)-1 {
			m.pickerCursor++
		}
		return m, nil
	case "home":
		if len(m.pickerItems) > 0 {
			m.pickerCursor = 0
		}
		return m, nil
	case "end":
		if len(m.pickerItems) > 0 {
			m.pickerCursor = len(m.pickerItems) - 1
		}
		return m, nil
	case "pgup", "ctrl+b":
		m.pickerCursor -= m.pickerPageStep()
		if m.pickerCursor < 0 {
			m.pickerCursor = 0
		}
		return m, nil
	case "pgdown", "ctrl+f", " ":
		m.pickerCursor += m.pickerPageStep()
		if m.pickerCursor >= len(m.pickerItems) {
			m.pickerCursor = max(0, len(m.pickerItems)-1)
		}
		return m, nil
	case "ctrl+u":
		m.pickerInput.SetValue("")
		m.pickerQuery = ""
		m.refreshPickerItems()
		return m, nil
	case "backspace":
		if m.pickerQuery == "" {
			m.pickerGoUp()
			return m, nil
		}
	case "enter":
		typed := strings.TrimSpace(m.pickerInput.Value())
		if typed != "" && (looksLikePathInput(typed) || isSupportedDataFile(typed)) {
			targetPath, targetIsDir, err := m.resolvePickerInputTarget(typed)
			if err == nil {
				if targetIsDir {
					m.pickerDir = targetPath
					m.pickerInput.SetValue("")
					m.pickerQuery = ""
					m.pickerCursor = 0
					m.refreshPickerItems()
					return m, nil
				}
				m.overlay = OverlayNone
				m.statusMsg = fmt.Sprintf("Opening %s...", filepath.Base(targetPath))
				return m, m.openFileCmd(targetPath)
			}
			m.statusMsg = fmt.Sprintf("Path not found: %v", err)
			return m, nil
		}

		if len(m.pickerItems) == 0 {
			return m, nil
		}
		item := m.pickerItems[m.pickerCursor]
		if item.isDir {
			m.pickerDir = item.path
			m.pickerInput.SetValue("")
			m.pickerQuery = ""
			m.pickerCursor = 0
			m.refreshPickerItems()
			return m, nil
		}
		m.overlay = OverlayNone
		m.statusMsg = fmt.Sprintf("Opening %s...", filepath.Base(item.path))
		return m, m.openFileCmd(item.path)
	}

	var cmd tea.Cmd
	m.pickerInput, cmd = m.pickerInput.Update(msg)
	m.pickerQuery = strings.TrimSpace(m.pickerInput.Value())
	m.refreshPickerItems()
	return m, cmd
}

func (m Model) resolvePickerInputTarget(input string) (path string, isDir bool, err error) {
	candidate := strings.TrimSpace(input)
	if candidate == "" {
		return "", false, fmt.Errorf("empty path")
	}

	candidate, err = expandTildePath(candidate)
	if err != nil {
		return "", false, err
	}
	if !filepath.IsAbs(candidate) {
		candidate = filepath.Join(m.pickerDir, candidate)
	}
	candidate = filepath.Clean(candidate)

	info, err := os.Stat(candidate)
	if err != nil {
		return "", false, err
	}
	if info.IsDir() {
		return candidate, true, nil
	}
	if !isSupportedDataFile(candidate) {
		return "", false, fmt.Errorf("unsupported file type: %s", filepath.Ext(candidate))
	}
	return candidate, false, nil
}

func looksLikePathInput(s string) bool {
	sep := string(filepath.Separator)
	return strings.HasPrefix(s, "~") ||
		strings.HasPrefix(s, ".") ||
		strings.HasPrefix(s, sep) ||
		strings.Contains(s, "/") ||
		// filepath.Separator is "/" on Unix, so this is only distinct on Windows.
		(sep != "/" && strings.Contains(s, sep))
}

func expandTildePath(path string) (string, error) {
	if path == "~" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		return home, nil
	}
	// "~/" is the Unix form; "~\" is the Windows form when filepath.Separator is "\".
	if strings.HasPrefix(path, "~/") ||
		(filepath.Separator != '/' && strings.HasPrefix(path, "~"+string(filepath.Separator))) {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		rest := strings.TrimPrefix(path, "~")
		rest = strings.TrimPrefix(rest, string(filepath.Separator))
		rest = strings.TrimPrefix(rest, "/")
		return filepath.Join(home, rest), nil
	}
	return path, nil
}

func (m Model) pickerPageStep() int {
	step := m.height - 10
	if step < 5 {
		step = 5
	}
	return step
}

func (m *Model) pickerGoUp() {
	if !m.pickerCanGoUp() {
		return
	}
	parent := filepath.Dir(m.pickerDir)
	m.pickerDir = parent
	m.pickerCursor = 0
	m.refreshPickerItems()
}

func (m Model) pickerCanGoUp() bool {
	dir := filepath.Clean(m.pickerDir)
	return filepath.Dir(dir) != dir
}

func (m *Model) refreshPickerItems() {
	query := strings.TrimSpace(m.pickerQuery)
	if query != "" && looksLikePathInput(query) {
		items, err := m.pathQueryItems(query)
		if err != nil {
			m.pickerItems = nil
			m.pickerCursor = 0
			return
		}
		m.pickerItems = items
	} else {
		entries, err := os.ReadDir(m.pickerDir)
		if err != nil {
			m.pickerItems = nil
			m.pickerCursor = 0
			m.statusMsg = fmt.Sprintf("Picker error: %v", err)
			return
		}

		dirs := make([]filePickerItem, 0, len(entries))
		files := make([]filePickerItem, 0, len(entries))
		for _, entry := range entries {
			name := entry.Name()
			fullPath := filepath.Join(m.pickerDir, name)
			if entry.IsDir() {
				dirs = append(dirs, filePickerItem{name: name, path: fullPath, isDir: true})
				continue
			}
			if isSupportedDataFile(name) {
				files = append(files, filePickerItem{name: name, path: fullPath})
			}
		}

		slices.SortFunc(dirs, func(a, b filePickerItem) int {
			return strings.Compare(strings.ToLower(a.name), strings.ToLower(b.name))
		})
		slices.SortFunc(files, func(a, b filePickerItem) int {
			return strings.Compare(strings.ToLower(a.name), strings.ToLower(b.name))
		})

		items := make([]filePickerItem, 0, len(dirs)+len(files)+1)
		if m.pickerCanGoUp() {
			items = append(items, filePickerItem{name: "..", path: filepath.Dir(m.pickerDir), isDir: true, isParent: true})
		}
		items = append(items, dirs...)
		items = append(items, files...)
		m.pickerItems = m.filterPickerItems(items, m.pickerQuery)
	}

	if len(m.pickerItems) == 0 {
		m.pickerCursor = 0
		return
	}
	if m.pickerCursor >= len(m.pickerItems) {
		m.pickerCursor = len(m.pickerItems) - 1
	}
	if m.pickerCursor < 0 {
		m.pickerCursor = 0
	}
}

func (m Model) pathQueryItems(query string) ([]filePickerItem, error) {
	baseDir, prefix, err := m.resolvePathQueryBase(query)
	if err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return nil, err
	}

	dirs := make([]filePickerItem, 0, len(entries))
	files := make([]filePickerItem, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(strings.ToLower(name), strings.ToLower(prefix)) {
			continue
		}
		fullPath := filepath.Join(baseDir, name)
		if entry.IsDir() {
			dirs = append(dirs, filePickerItem{name: name, path: fullPath, isDir: true})
			continue
		}
		if isSupportedDataFile(name) {
			files = append(files, filePickerItem{name: name, path: fullPath})
		}
	}

	slices.SortFunc(dirs, func(a, b filePickerItem) int {
		return strings.Compare(strings.ToLower(a.name), strings.ToLower(b.name))
	})
	slices.SortFunc(files, func(a, b filePickerItem) int {
		return strings.Compare(strings.ToLower(a.name), strings.ToLower(b.name))
	})

	items := make([]filePickerItem, 0, len(dirs)+len(files))
	items = append(items, dirs...)
	items = append(items, files...)
	return items, nil
}

func (m Model) resolvePathQueryBase(query string) (baseDir, prefix string, err error) {
	raw := strings.TrimSpace(query)
	if raw == "" {
		return m.pickerDir, "", nil
	}

	expanded, err := expandTildePath(raw)
	if err != nil {
		return "", "", err
	}
	if !filepath.IsAbs(expanded) {
		expanded = filepath.Join(m.pickerDir, expanded)
	}

	sep := string(filepath.Separator)
	hasTrailingSep := strings.HasSuffix(raw, sep) || strings.HasSuffix(raw, "/")
	if raw == "~" {
		hasTrailingSep = true
	}

	expanded = filepath.Clean(expanded)
	if hasTrailingSep {
		return expanded, "", nil
	}
	return filepath.Dir(expanded), filepath.Base(expanded), nil
}

func (m Model) filterPickerItems(items []filePickerItem, query string) []filePickerItem {
	q := strings.TrimSpace(query)
	if q == "" {
		return items
	}

	type scoredItem struct {
		item  filePickerItem
		score int
	}
	scored := make([]scoredItem, 0, len(items))
	for _, item := range items {
		if item.isParent {
			continue
		}
		if score, ok := fuzzyFileScore(item.name, q); ok {
			scored = append(scored, scoredItem{item: item, score: score})
		}
	}

	slices.SortFunc(scored, func(a, b scoredItem) int {
		if a.score != b.score {
			return b.score - a.score
		}
		if a.item.isDir != b.item.isDir {
			if a.item.isDir {
				return -1
			}
			return 1
		}
		return strings.Compare(strings.ToLower(a.item.name), strings.ToLower(b.item.name))
	})

	filtered := make([]filePickerItem, 0, len(scored))
	for _, s := range scored {
		filtered = append(filtered, s.item)
	}
	return filtered
}

func isSupportedDataFile(path string) bool {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".parquet", ".csv":
		return true
	default:
		return false
	}
}

func fuzzyFileScore(candidate, query string) (int, bool) {
	c := strings.ToLower(candidate)
	q := strings.ToLower(query)
	if q == "" {
		return 0, true
	}

	last := -1
	score := 0
	for len(q) > 0 {
		r, size := utf8.DecodeRuneInString(q)
		if r == utf8.RuneError && size == 1 {
			return 0, false
		}
		found := -1
		for i := last + 1; i < len(c); {
			cr, csize := utf8.DecodeRuneInString(c[i:])
			if cr == r {
				found = i
				break
			}
			i += csize
		}
		if found < 0 {
			return 0, false
		}

		score += 10
		if last >= 0 && found == last+1 {
			score += 8
		}
		if found == 0 || isWordBoundary(c, found-1) {
			score += 6
		}
		if found > last+1 {
			score -= found - (last + 1)
		}
		last = found
		q = q[size:]
	}

	score -= len(candidate) / 3
	return score, true
}

func isWordBoundary(s string, i int) bool {
	if i < 0 || i >= len(s) {
		return true
	}
	r, _ := utf8.DecodeLastRuneInString(s[:i+1])
	return !unicode.IsLetter(r) && !unicode.IsDigit(r)
}

func (m Model) openFileCmd(path string) tea.Cmd {
	absPath := path
	if abs, err := filepath.Abs(path); err == nil {
		absPath = abs
	}
	return func() tea.Msg {
		eng, err := engine.New(absPath)
		return openFileDoneMsg{path: absPath, eng: eng, err: err}
	}
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
	w := m.columnsOuterWidth() - paneBorderW
	h := m.mainHeight() - paneBorderH
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
	w := m.tableOuterWidth() - paneBorderW
	h := m.mainHeight() - paneBorderH
	if w < 0 {
		w = 0
	}
	if h < 0 {
		h = 0
	}
	return w, h
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
					if m.sel.Count() == 0 {
						m.showSelected = false
						m.statusMsg = "show-selected off (no columns selected)"
					}
					// updateFilteredCols handles cursor clamping and
					// selectedColName re-sync (including the case where
					// the deselected column was the highlighted one).
					m.updateFilteredCols()
				}
				return m, m.nextPreviewCmd()
			}
		}
	case "a":
		names := make([]string, len(m.filteredCols))
		for i, c := range m.filteredCols {
			names[i] = c.Name
		}
		m.sel.AddAll(names)
		if m.showSelected {
			return m, m.nextPreviewCmd()
		}
	case "d":
		names := make([]string, len(m.filteredCols))
		for i, c := range m.filteredCols {
			names[i] = c.Name
		}
		m.sel.RemoveAll(names)
		if m.showSelected {
			if m.sel.Count() == 0 {
				m.showSelected = false
				m.statusMsg = "show-selected off (no columns selected)"
				m.updateFilteredCols()
			}
			return m, m.nextPreviewCmd()
		}
	case "A":
		m.sel.SelectAll()
		if m.showSelected {
			return m, m.nextPreviewCmd()
		}
	case "X":
		m.sel.Clear()
		if m.showSelected {
			m.showSelected = false
			m.statusMsg = "show-selected off (no columns selected)"
			m.updateFilteredCols()
			return m, m.nextPreviewCmd()
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
	if m.visibleTableRows() == 0 {
		return m, nil
	}
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
	return m, m.nextPreviewCmd()
}

func (m Model) handleTableKey(key string) (tea.Model, tea.Cmd) {
	// Clamp on the local copy (value receiver); the returned m carries the clamped value.
	m.clampTableRowCursor()
	if (key == "up" || key == "k" || key == "down" || key == "j") && m.visibleTableRows() == 0 {
		return m, nil
	}
	if (key == "left" || key == "h" || key == "right" || key == "l") && m.visibleColCount() == 0 {
		return m, nil
	}
	switch key {
	case "up", "k":
		if m.tableRowCursor > 0 {
			m.tableRowCursor--
		} else if m.tableOffset > 0 {
			m.tableOffset--
			// tableRowCursor stays at 0
			return m, m.nextPreviewCmd()
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
				return m, m.nextPreviewCmd()
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
		return m, m.nextPreviewCmd()
	case "G":
		m.tableOffset = m.maxTableOffset()
		m.tableRowCursor = max(0, m.visibleTableRows()-1)
		return m, m.nextPreviewCmd()
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
		return m, m.nextPreviewCmd()
	}
	return m, nil
}

// visibleColCount returns how many columns fit in the table pane.
func (m Model) visibleColCount() int {
	w, _ := m.tablePaneDimensions()
	colAreaWidth := w - tableRowNumW - tableRowPrefixW
	if colAreaWidth < tableColMinWidth {
		return 0
	}
	// Uniform width assumed; see columnWidth TODO.
	return min(colAreaWidth/tableColWidth, len(m.tableCols))
}

// pageColumnsHorizontal scrolls the column viewport by one screenful.
// direction should be -1 (left) or +1 (right).
func (m Model) pageColumnsHorizontal(direction int) (tea.Model, tea.Cmd) {
	if len(m.tableCols) == 0 {
		return m, nil
	}
	visibleCols := m.visibleColCount()
	if visibleCols == 0 {
		return m, nil
	}
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

func (m Model) loadPreviewCmd(seq uint64) tea.Cmd {
	eng := m.engine
	if eng == nil {
		return nil
	}
	colNames := m.projectionCols()
	rowFilter := m.rowFilter
	limit := m.previewLimit()
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
			return previewDoneMsg{seq: seq, token: m.dataToken, err: err}
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
			seq:        seq,
			token:      m.dataToken,
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
	if eng == nil {
		return nil
	}
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
		return profileBasicDoneMsg{colName: colName, summary: summary, token: m.dataToken, err: err}
	}
}

func (m Model) loadDetail(colName string, existing *types.ColumnSummary, colType string) tea.Cmd {
	eng := m.engine
	if eng == nil {
		return nil
	}

	return func() tea.Msg {
		ctx := context.Background()
		summary := existing
		if summary == nil {
			var err error
			summary, err = eng.ProfileBasic(ctx, colName)
			if err != nil {
				return profileDetailDoneMsg{colName: colName, token: m.dataToken, err: err}
			}
		}

		if err := eng.ProfileDetail(ctx, colName, summary, colType); err != nil {
			return profileDetailDoneMsg{colName: colName, token: m.dataToken, err: err}
		}
		return profileDetailDoneMsg{colName: colName, summary: summary, token: m.dataToken}
	}
}

func (m Model) jumpToFirstNull(colName, rowFilter string) tea.Cmd {
	eng := m.engine
	if eng == nil {
		return nil
	}
	return func() tea.Msg {
		ctx := context.Background()
		rowID, err := eng.FirstNullRow(ctx, colName, rowFilter)
		if err != nil || rowID == 0 {
			return firstNullMsg{rowID: rowID, token: m.dataToken, err: err}
		}
		offset, err := eng.OffsetForRowID(ctx, rowID, rowFilter)
		return firstNullMsg{rowID: rowID, offset: offset, token: m.dataToken, err: err}
	}
}

// View

func (m Model) View() string {
	if !m.ready {
		return "Loading..."
	}

	if m.overlay == OverlayFilePicker {
		return m.viewFilePicker()
	}

	if m.overlay == OverlayHelp {
		return m.viewHelp()
	}

	// Layout: top bar, main area (table + columns), bottom bar
	topBar := m.viewTopBar()
	bottomBar := m.viewBottomBar()

	mainHeight := m.mainHeight() // top + bottom bars

	tableWidth := m.tableOuterWidth()
	colWidth := m.columnsOuterWidth()

	tableInnerW := max(0, tableWidth-paneBorderW)
	tableInnerH := max(0, mainHeight-paneBorderH)
	colInnerW := max(0, colWidth-paneBorderW)
	colInnerH := max(0, mainHeight-paneBorderH)
	tableView := m.viewTable(tableInnerW, tableInnerH)
	colView := m.viewColumns(colInnerW, colInnerH)

	// Apply borders based on focus
	var tablePane, colPane string
	if m.focus == FocusTable {
		tablePane = activeBorderStyle.Width(max(0, tableWidth-2)).Height(max(0, mainHeight-2)).Render(tableView)
		colPane = inactiveBorderStyle.Width(max(0, colWidth-2)).Height(max(0, mainHeight-2)).Render(colView)
	} else {
		tablePane = inactiveBorderStyle.Width(max(0, tableWidth-2)).Height(max(0, mainHeight-2)).Render(tableView)
		colPane = activeBorderStyle.Width(max(0, colWidth-2)).Height(max(0, mainHeight-2)).Render(colView)
	}

	// Detail overlay rendered on top of columns pane
	if m.overlay == OverlayDetail {
		colPane = activeBorderStyle.Width(max(0, colWidth-2)).Height(max(0, mainHeight-2)).Render(m.viewDetail(max(0, colWidth-4)))
	}

	main := lipgloss.JoinHorizontal(lipgloss.Top, tablePane, colPane)

	return lipgloss.JoinVertical(lipgloss.Left, topBar, main, bottomBar)
}

func (m Model) viewTopBar() string {
	fileLabel := m.fileName
	if fileLabel == "" {
		fileLabel = "(no file)"
	}
	left := fmt.Sprintf(" %s  %d rows × %d cols", fileLabel, m.totalRows, len(m.columns))
	rightPlain := ""
	if m.rowFilter != "" {
		filterInfo := "Filter: rows with nulls"
		if m.filterRows >= 0 {
			filterInfo += fmt.Sprintf(" (%d rows)", m.filterRows)
		}
		rightPlain = filterInfo
	}
	contentW := max(0, m.width-topBottomBarPadW)
	left = truncateDisplay(left, contentW)
	rightPlain = truncateDisplay(rightPlain, contentW)
	if lipgloss.Width(left)+lipgloss.Width(rightPlain) > contentW {
		left = truncateDisplay(left, max(0, contentW-lipgloss.Width(rightPlain)))
	}
	var right string
	if rightPlain != "" {
		right = filterStyle.Render(rightPlain)
	}
	gap := contentW - lipgloss.Width(left) - lipgloss.Width(right)
	if gap < 0 {
		gap = 0
	}
	return topBarStyle.Width(m.width).Render(left + strings.Repeat(" ", gap) + right)
}

func (m Model) viewBottomBar() string {
	selCount := m.sel.Count()
	var hints string
	if m.focus == FocusColumns {
		hints = "Ctrl+O:open  /:search  x:toggle  a:add  d:rm  y:copy  Enter:detail  wheel:cursor"
	} else {
		hints = "Ctrl+O:open  hjkl:move  space:pgdn  []:col-page  f:null-filter  drag:divider  Ctrl+L:redraw"
	}
	status := fmt.Sprintf("  Sel: %d/%d", selCount, len(m.columns))
	if m.showSelected {
		status += "  [show-sel]"
	}
	if m.statusMsg != "" {
		status += "  " + m.statusMsg
	}
	contentW := max(0, m.width-topBottomBarPadW)
	hints = truncateDisplay(hints, contentW)
	status = truncateDisplay(status, contentW)
	if lipgloss.Width(hints)+lipgloss.Width(status) > contentW {
		hints = truncateDisplay(hints, max(0, contentW-lipgloss.Width(status)))
	}
	gap := contentW - lipgloss.Width(hints) - lipgloss.Width(status)
	if gap < 0 {
		gap = 0
	}
	return bottomBarStyle.Width(m.width).Render(hints + strings.Repeat(" ", gap) + status)
}

func (m Model) viewTable(w, h int) string {
	if len(m.tableCols) == 0 {
		if m.engine == nil {
			return "No file loaded. Press Ctrl+O to open .parquet/.csv"
		}
		return "No data loaded"
	}
	if h <= 0 {
		return ""
	}
	if m.visibleTableRows() == 0 {
		return "Terminal too small to display rows"
	}

	// How many columns fit
	visibleCols := m.visibleColCount()
	if visibleCols == 0 {
		return "Terminal too small to display columns"
	}

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
		colW := m.columnWidth()
		name := truncate(m.tableCols[i], max(0, colW-2))
		nameStr := fmt.Sprintf(" %-*s", max(0, colW-2), name)
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

		rowHasNull := m.rowHasNullAt(r)
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
		line += m.renderRowCells(m.tableData[r], startCol, endCol, cursorColIdx, isSelectedRow)
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

func (m Model) renderRowCells(row []string, startCol, endCol, cursorColIdx int, isSelectedRow bool) string {
	var b strings.Builder
	b.Grow((endCol - startCol) * m.columnWidth())
	for i := startCol; i < endCol && i < len(row); i++ {
		colW := m.columnWidth()
		val := truncate(row[i], max(0, colW-1))
		cell := fmt.Sprintf(" %-*s", max(0, colW-1), val)
		isNull := row[i] == "NULL"
		isSelectedCol := i == cursorColIdx

		switch {
		case isSelectedRow && isSelectedCol && isNull:
			b.WriteString(crosshairNullStyle.Render(cell))
		case isSelectedRow && isSelectedCol:
			b.WriteString(crosshairCellStyle.Render(cell))
		case isSelectedRow && isNull:
			b.WriteString(activeRowNullStyle.Render(cell))
		case isSelectedRow:
			b.WriteString(activeRowCellStyle.Render(cell))
		case isSelectedCol && isNull:
			b.WriteString(activeColNullStyle.Render(cell))
		case isSelectedCol:
			b.WriteString(activeColCellStyle.Render(cell))
		case isNull:
			b.WriteString(nullStyle.Render(cell))
		default:
			b.WriteString(cellStyle.Render(cell))
		}
	}
	return b.String()
}

func rowHasNullFlags(rows [][]string) []bool {
	flags := make([]bool, len(rows))
	for i, row := range rows {
		flags[i] = rowHasNull(row)
	}
	return flags
}

// rowHasNullAt reports whether the row at rowIdx contains a NULL value.
// Falls back to a full live scan when the cache is out of sync with tableData.
func (m Model) rowHasNullAt(rowIdx int) bool {
	if rowIdx < 0 {
		return false
	}
	if len(m.tableRowHasNull) != len(m.tableData) {
		if rowIdx < len(m.tableData) {
			return rowHasNull(m.tableData[rowIdx])
		}
		return false
	}
	// When lengths match, len(tableRowHasNull) == len(tableData),
	// so rowIdx >= len(tableRowHasNull) implies rowIdx >= len(tableData) — no live scan needed.
	if rowIdx < len(m.tableRowHasNull) {
		return m.tableRowHasNull[rowIdx]
	}
	return false
}

func rowHasNull(row []string) bool {
	for _, v := range row {
		if v == "NULL" {
			return true
		}
	}
	return false
}

func (m Model) viewTableFooter() string {
	if len(m.tableData) == 0 {
		return ""
	}

	var parts []string

	// Clamp cursor defensively for transient states where
	// tableData may have shrunk (e.g. after filter/resize).
	rowCursor := m.tableRowCursor
	if rowCursor < 0 {
		rowCursor = 0
	}
	if rowCursor >= len(m.tableData) {
		rowCursor = len(m.tableData) - 1
	}
	row := m.tableData[rowCursor]
	nullCount := 0
	for _, v := range row {
		if v == "NULL" {
			nullCount++
		}
	}
	absRow := m.tableOffset + rowCursor + 1
	parts = append(parts, fmt.Sprintf("Row %d: %d/%d missing (projected)", absRow, nullCount, len(row)))

	// Column info from profiling
	if m.selectedColName != "" {
		colName := truncate(m.selectedColName, 20)
		colType := truncate(m.columnType(m.selectedColName), 20)
		typeInfo := ""
		if colType != "" {
			typeInfo = fmt.Sprintf(" (%s)", colType)
		}
		if s, ok := m.summaries[m.selectedColName]; ok && s.Loaded {
			parts = append(parts, fmt.Sprintf("Col %q%s: %d missing (%.1f%%)", colName, typeInfo, s.MissingCount, s.MissingPct))
		} else {
			parts = append(parts, fmt.Sprintf("Col %q%s: ...", colName, typeInfo))
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

		name := truncate(col.Name, max(0, w-12))
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
				lines = append(lines, clampLineWidth(highlightStyle.Width(w).Render(plain), w))
			} else {
				lines = append(lines, clampLineWidth(dimHighlightStyle.Width(w).Render(plain), w))
			}
		} else {
			mark := unselectedMark
			if m.sel.IsSelected(col.Name) {
				mark = selectedMark
			}
			typeBadge := typeBadgeStyle.Render(typeStr)
			stats := statStyle.Render(statsStr)
			lines = append(lines, clampLineWidth(fmt.Sprintf("%s %s %s%s", mark, name, typeBadge, stats), w))
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

func (m Model) viewFilePicker() string {
	panelW := max(24, m.width-4)
	panelH := max(10, m.height-2)

	var lines []string
	lines = append(lines, detailTitleStyle.Render("Open data file"))
	lines = append(lines, detailLabelStyle.Render("Current directory: ")+truncateDisplay(m.pickerDir, panelW-4))
	lines = append(lines, searchPromptStyle.Render("find> ")+m.pickerInput.View())
	lines = append(lines, "")

	listHeight := max(1, panelH-8)
	start := 0
	if m.pickerCursor >= listHeight {
		start = m.pickerCursor - listHeight + 1
	}
	if len(m.pickerItems) == 0 {
		lines = append(lines, detailLabelStyle.Render("No matching folders or .parquet/.csv files"))
	} else {
		for i := start; i < len(m.pickerItems) && i < start+listHeight; i++ {
			item := m.pickerItems[i]
			label := item.name
			if item.isParent {
				label = "../"
			} else if item.isDir {
				label += "/"
			}
			label = truncateDisplay(label, panelW-8)
			line := "  " + label
			if i == m.pickerCursor {
				lines = append(lines, clampLineWidth(highlightStyle.Width(panelW-4).Render(line), panelW-4))
			} else {
				lines = append(lines, clampLineWidth(line, panelW-4))
			}
		}
	}

	lines = append(lines, "")
	lines = append(lines, detailLabelStyle.Render("Enter: open/enter folder  Backspace: parent  Ctrl+U: clear  Esc: close"))

	content := strings.Join(lines, "\n")
	style := lipgloss.NewStyle().
		Width(panelW).
		Height(panelH).
		Padding(1, 2).
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("62"))

	return style.Render(content)
}

func (m Model) viewHelp() string {
	help := []struct{ key, desc string }{
		{"Tab", "Switch focus (Table ↔ Columns)"},
		{"q / Ctrl+C", "Quit"},
		{"Ctrl+O", "Open file picker (.parquet/.csv)"},
		{"Ctrl+L", "Redraw screen"},
		{"?", "Toggle help"},
		{"s", "Toggle show selected columns only"},
		{"Space", "Page down (rows or columns list)"},
		{"Enter", "Open column detail"},
		{"Mouse wheel", "Scroll cursor in focused pane"},
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
		{"Mouse drag divider", "Resize table/columns split"},
		{"", ""},
		{"── File Picker ──", ""},
		{"Enter", "Open selected file/folder"},
		{"Backspace", "Go to parent folder"},
		{"Ctrl+U", "Clear picker query"},
		{"Esc", "Close picker"},
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

func truncateDisplay(s string, maxW int) string {
	if maxW <= 0 {
		return ""
	}
	if lipgloss.Width(s) <= maxW {
		return s
	}
	if maxW == 1 {
		return "…"
	}
	limit := maxW - 1
	var b strings.Builder
	cur := 0
	for _, r := range s {
		rw := lipgloss.Width(string(r))
		if cur+rw > limit {
			break
		}
		b.WriteRune(r)
		cur += rw
	}
	return b.String() + "…"
}

func clampLineWidth(line string, w int) string {
	if w <= 0 {
		return ""
	}
	if lipgloss.Width(line) <= w {
		return line
	}
	return lipgloss.NewStyle().MaxWidth(w).Render(line)
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
