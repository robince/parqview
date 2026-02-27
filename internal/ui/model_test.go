package ui

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	_ "github.com/marcboeker/go-duckdb"

	"github.com/robince/parqview/internal/engine"
	"github.com/robince/parqview/internal/missing"
	"github.com/robince/parqview/internal/selection"
	"github.com/robince/parqview/internal/types"
)

func TestHandleTableKeyDownKeepsCursorWithinVisibleRowsAndScrolls(t *testing.T) {
	m := newCmdTestModel()
	m.width = 120
	m.height = 10
	m.pageSize = 50
	m.totalRows = 200
	m.tableData = make([][]string, 50)
	for i := range m.tableData {
		m.tableData[i] = []string{"v"}
	}

	for i := 0; i < 2; i++ {
		updated, cmd := m.handleTableKey("down")
		if cmd != nil {
			t.Fatalf("expected no load command before visible-row boundary, got %v", cmd)
		}
		m = updated.(Model)
	}

	if m.tableRowCursor != 2 {
		t.Fatalf("expected row cursor at last visible row (2), got %d", m.tableRowCursor)
	}

	updated, cmd := m.handleTableKey("down")
	m = updated.(Model)
	if cmd == nil {
		t.Fatal("expected load command when moving past visible rows")
	}
	if m.tableRowCursor != 2 {
		t.Fatalf("expected row cursor to stay on visible bottom row, got %d", m.tableRowCursor)
	}
	if m.tableOffset != 1 {
		t.Fatalf("expected tableOffset to advance, got %d", m.tableOffset)
	}
}

func TestHandleTableKeyDownCanReachFinalRowWithSmallViewport(t *testing.T) {
	m := newTestModel()
	m.width = 120
	m.height = 10
	m.pageSize = 50
	m.totalRows = 200
	m.tableData = make([][]string, 50)
	for i := range m.tableData {
		m.tableData[i] = []string{"v"}
	}

	for i := 0; i < 1000; i++ {
		updated, _ := m.handleTableKey("down")
		m = updated.(Model)
	}

	absRow := m.tableOffset + m.tableRowCursor + 1
	if absRow != int(m.totalRows) {
		t.Fatalf("expected to reach final row %d, got %d (offset=%d cursor=%d)", m.totalRows, absRow, m.tableOffset, m.tableRowCursor)
	}
}

func TestHandleTableKeyRFindsNextMissingIncludingNaN(t *testing.T) {
	m := newTestModel()
	m.tableCols = []string{"a", "b", "c"}
	m.filteredCols = []types.ColumnInfo{{Name: "a"}, {Name: "b"}, {Name: "c"}}
	m.selectedColName = "a"
	m.tableData = [][]string{{"1", "NaN", "NULL"}}
	m.tableRowCursor = 0

	updated, cmd := m.handleTableKey("r")
	if cmd != nil {
		t.Fatalf("expected no command for row-local missing jump, got %v", cmd)
	}
	m = updated.(Model)
	if m.selectedColName != "b" {
		t.Fatalf("expected first r to land on NaN column b, got %q", m.selectedColName)
	}

	updated, cmd = m.handleTableKey("r")
	if cmd != nil {
		t.Fatalf("expected no command for row-local missing jump, got %v", cmd)
	}
	m = updated.(Model)
	if m.selectedColName != "c" {
		t.Fatalf("expected second r to land on NULL column c, got %q", m.selectedColName)
	}

	updated, cmd = m.handleTableKey("R")
	if cmd != nil {
		t.Fatalf("expected no command for reverse row-local missing jump, got %v", cmd)
	}
	m = updated.(Model)
	if m.selectedColName != "b" {
		t.Fatalf("expected R to land back on NaN column b, got %q", m.selectedColName)
	}
}

func TestHandleTableKeyRNoMissingSetsStatus(t *testing.T) {
	m := newTestModel()
	m.tableCols = []string{"a", "b"}
	m.filteredCols = []types.ColumnInfo{{Name: "a"}, {Name: "b"}}
	m.selectedColName = "a"
	m.tableData = [][]string{{"1", "2"}}
	m.tableRowCursor = 0

	updated, cmd := m.handleTableKey("r")
	if cmd != nil {
		t.Fatalf("expected no command when no row-missing exists, got %v", cmd)
	}
	m = updated.(Model)
	if m.statusMsg != "No missing values in this row" {
		t.Fatalf("unexpected status: %q", m.statusMsg)
	}
}

func TestHandleTableKeyRDoesNotTreatCurrentCellAsNextMissing(t *testing.T) {
	if !missing.IsDisplayMissing("NULL") {
		t.Fatal(`test setup invalid: expected "NULL" to be treated as missing`)
	}

	m := newTestModel()
	m.tableCols = []string{"a", "b", "c"}
	m.filteredCols = []types.ColumnInfo{{Name: "a"}, {Name: "b"}, {Name: "c"}}
	m.selectedColName = "b"
	m.tableData = [][]string{{"1", "NULL", "2"}}
	m.tableRowCursor = 0

	updated, cmd := m.handleTableKey("r")
	if cmd != nil {
		t.Fatalf("expected no command when no other row-missing exists, got %v", cmd)
	}
	m = updated.(Model)
	if m.selectedColName != "b" {
		t.Fatalf("expected selected column to stay on b, got %q", m.selectedColName)
	}
	if m.statusMsg != "No missing values in this row" {
		t.Fatalf("unexpected status: %q", m.statusMsg)
	}
}

func TestHandleTableKeyCReturnsCommandWhenColumnSelected(t *testing.T) {
	m := newCmdTestModel()
	m.selectedColName = "alpha"
	m.tableCols = []string{"alpha"}
	m.tableData = [][]string{{"1"}}
	m.totalRows = 10

	updated, cmd := m.handleTableKey("c")
	m = updated.(Model)
	if cmd == nil {
		t.Fatal("expected command for column-missing jump")
	}

	updated, cmd = m.handleTableKey("C")
	m = updated.(Model)
	if cmd == nil {
		t.Fatal("expected command for reverse column-missing jump")
	}
}

func TestWindowSizeMsgClampsOffsetAndKeepsPageDownMonotonic(t *testing.T) {
	m := newCmdTestModel()
	m.width = 120
	m.height = 10
	m.pageSize = 50
	m.totalRows = 200
	m.tableOffset = 190
	m.tableData = make([][]string, 50)
	for i := range m.tableData {
		m.tableData[i] = []string{"v"}
	}

	updated, cmd := m.Update(tea.WindowSizeMsg{Width: 120, Height: 40})
	if cmd == nil {
		t.Fatal("expected load command when resize clamps offset")
	}
	m = updated.(Model)

	if m.tableOffset != 167 {
		t.Fatalf("expected tableOffset to clamp to 167 after resize, got %d", m.tableOffset)
	}

	before := m.tableOffset
	updated, cmd = m.handleTablePageDown()
	if cmd != nil {
		t.Fatal("expected no load command when page down cannot advance offset")
	}
	m = updated.(Model)
	if m.tableOffset < before {
		t.Fatalf("expected page down to be non-decreasing after resize clamp, before=%d after=%d", before, m.tableOffset)
	}
}

func TestHandleTableKeyCtrlPagingLoadsOnlyWhenOffsetChanges(t *testing.T) {
	base := newCmdTestModel()
	base.width = 120
	base.height = 20
	base.pageSize = 20
	base.totalRows = 200

	maxOff := base.maxTableOffset()
	if maxOff == 0 {
		t.Fatal("expected a positive max table offset for paging tests")
	}

	cases := []struct {
		name           string
		key            string
		startAtMax     bool
		expectedOffset int
		expectedCmd    bool
	}{
		{name: "ctrl+f at bottom", key: "ctrl+f", startAtMax: true, expectedOffset: maxOff, expectedCmd: false},
		{name: "ctrl+b at top", key: "ctrl+b", startAtMax: false, expectedOffset: 0, expectedCmd: false},
		{name: "ctrl+d at bottom", key: "ctrl+d", startAtMax: true, expectedOffset: maxOff, expectedCmd: false},
		{name: "ctrl+u at top", key: "ctrl+u", startAtMax: false, expectedOffset: 0, expectedCmd: false},
		{name: "ctrl+f from top", key: "ctrl+f", startAtMax: false, expectedOffset: base.pageSize, expectedCmd: true},
		{name: "ctrl+b from bottom", key: "ctrl+b", startAtMax: true, expectedOffset: maxOff - base.pageSize, expectedCmd: true},
		{name: "ctrl+d from top", key: "ctrl+d", startAtMax: false, expectedOffset: base.pageSize / 2, expectedCmd: true},
		{name: "ctrl+u from bottom", key: "ctrl+u", startAtMax: true, expectedOffset: maxOff - (base.pageSize / 2), expectedCmd: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := base
			if tc.startAtMax {
				m.tableOffset = maxOff
			}

			updated, cmd := m.handleTableKey(tc.key)
			m = updated.(Model)

			if m.tableOffset != tc.expectedOffset {
				t.Fatalf("expected %s to set offset to %d, got %d", tc.key, tc.expectedOffset, m.tableOffset)
			}
			if tc.expectedCmd && cmd == nil {
				t.Fatalf("expected load command for %s at offset %d", tc.key, m.tableOffset)
			}
			if !tc.expectedCmd && cmd != nil {
				t.Fatalf("expected no load command for %s at offset %d", tc.key, m.tableOffset)
			}
		})
	}
}

func TestHandleKeyToggleShowSelectedGlobalLoadsPreview(t *testing.T) {
	cases := []struct {
		name  string
		focus Focus
	}{
		{name: "table focus", focus: FocusTable},
		{name: "columns focus", focus: FocusColumns},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newCmdTestModel()
			m.focus = tc.focus
			m.columns = []types.ColumnInfo{{Name: "alpha"}}
			m.sel = selection.New(nil)
			m.tableRowCursor = 3

			updated, cmd := m.handleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'s'}})
			if cmd == nil {
				t.Fatal("expected load command when toggling showSelected with s")
			}
			m = updated.(Model)
			if !m.showSelected {
				t.Fatal("expected showSelected enabled after s")
			}
			if m.tableRowCursor != 0 {
				t.Fatalf("expected row cursor reset to 0 after s, got %d", m.tableRowCursor)
			}

			m.tableRowCursor = 2
			updated, cmd = m.handleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'S'}})
			if cmd == nil {
				t.Fatal("expected load command when toggling showSelected with S")
			}
			m = updated.(Model)
			if m.showSelected {
				t.Fatal("expected showSelected disabled after S")
			}
			if m.tableRowCursor != 0 {
				t.Fatalf("expected row cursor reset to 0 after S, got %d", m.tableRowCursor)
			}
		})
	}
}

func TestHandleKeyToggleShowSelectedInColumnsDoesNotLoadPreview(t *testing.T) {
	m := newCmdTestModel()
	m.focus = FocusColumns
	m.columns = []types.ColumnInfo{{Name: "alpha"}, {Name: "beta"}}
	m.sel = selection.New([]string{"alpha", "beta"})
	m.sel.Add("alpha")
	m.updateFilteredCols()

	updated, cmd := m.handleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'v'}})
	if cmd != nil {
		t.Fatalf("expected no load command when toggling column-list selected-only with v, got %v", cmd)
	}
	m = updated.(Model)
	if !m.showSelectedInCols {
		t.Fatal("expected showSelectedInCols enabled after v")
	}
	if len(m.filteredCols) != 1 || m.filteredCols[0].Name != "alpha" {
		t.Fatalf("expected filtered list to show selected columns only, got %#v", m.filteredCols)
	}

	updated, cmd = m.handleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'V'}})
	if cmd != nil {
		t.Fatalf("expected no load command when toggling column-list selected-only with V, got %v", cmd)
	}
	m = updated.(Model)
	if m.showSelectedInCols {
		t.Fatal("expected showSelectedInCols disabled after V")
	}
	if len(m.filteredCols) != 2 {
		t.Fatalf("expected all columns visible after V, got %d", len(m.filteredCols))
	}
}

func TestHandleKeyToggleShowSelectedInColumnsIgnoredInTableFocus(t *testing.T) {
	m := newCmdTestModel()
	m.focus = FocusTable
	m.columns = []types.ColumnInfo{{Name: "alpha"}, {Name: "beta"}}
	m.tableCols = []string{"alpha", "beta"}
	m.selectedColName = "alpha"
	m.sel = selection.New([]string{"alpha", "beta"})
	m.updateFilteredCols()

	updated, cmd := m.handleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'v'}})
	if cmd != nil {
		t.Fatalf("expected no command when pressing v in table focus, got %v", cmd)
	}
	m = updated.(Model)
	if m.showSelectedInCols {
		t.Fatal("expected showSelectedInCols unchanged in table focus")
	}
	if m.selectedColName != "alpha" {
		t.Fatalf("expected active table column unchanged, got %q", m.selectedColName)
	}
}

func TestHandleKeyToggleShowSelectedInColumnsWithNilSelectionDoesNotPanic(t *testing.T) {
	m := Model{
		focus:   FocusColumns,
		columns: []types.ColumnInfo{{Name: "alpha"}},
		sel:     nil,
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic: %v", r)
		}
	}()

	updated, cmd := m.handleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'v'}})
	if cmd != nil {
		t.Fatalf("expected no command, got %v", cmd)
	}
	m = updated.(Model)
	if !m.showSelectedInCols {
		t.Fatal("expected showSelectedInCols enabled after v")
	}
}

func TestColumnsBulkOpsWithBothSelectedTogglesEnabled(t *testing.T) {
	cases := []struct {
		name          string
		key           string
		wantSelCount  int
		wantFiltCount int
		wantDataSel   bool
		wantColsSel   bool
		wantCmd       bool
	}{
		{
			name:          "A selects all and keeps both toggles on",
			key:           "A",
			wantSelCount:  3,
			wantFiltCount: 3,
			wantDataSel:   true,
			wantColsSel:   true,
			wantCmd:       true,
		},
		{
			name:          "d clears selection and turns off both toggles",
			key:           "d",
			wantSelCount:  0,
			wantFiltCount: 3,
			wantDataSel:   false,
			wantColsSel:   false,
			wantCmd:       true,
		},
		{
			name:          "X clears selection and turns off both toggles",
			key:           "X",
			wantSelCount:  0,
			wantFiltCount: 3,
			wantDataSel:   false,
			wantColsSel:   false,
			wantCmd:       true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newCmdTestModel()
			m.focus = FocusColumns
			m.columns = []types.ColumnInfo{{Name: "alpha"}, {Name: "beta"}, {Name: "gamma"}}
			m.sel = selection.New([]string{"alpha", "beta", "gamma"})
			m.sel.Add("alpha")
			m.showSelected = true
			m.showSelectedInCols = true
			m.updateFilteredCols()
			if len(m.filteredCols) != 1 {
				t.Fatalf("expected initial cols filter to show selected columns only, got %d", len(m.filteredCols))
			}

			updated, cmd := m.handleColumnsKey(tc.key)
			m = updated.(Model)
			if tc.wantCmd && cmd == nil {
				t.Fatalf("expected command for %q", tc.key)
			}
			if !tc.wantCmd && cmd != nil {
				t.Fatalf("expected no command for %q, got %v", tc.key, cmd)
			}
			if got := m.sel.Count(); got != tc.wantSelCount {
				t.Fatalf("expected selection count %d after %q, got %d", tc.wantSelCount, tc.key, got)
			}
			if m.showSelected != tc.wantDataSel {
				t.Fatalf("expected showSelected=%v after %q, got %v", tc.wantDataSel, tc.key, m.showSelected)
			}
			if m.showSelectedInCols != tc.wantColsSel {
				t.Fatalf("expected showSelectedInCols=%v after %q, got %v", tc.wantColsSel, tc.key, m.showSelectedInCols)
			}
			if got := len(m.filteredCols); got != tc.wantFiltCount {
				t.Fatalf("expected filteredCols len %d after %q, got %d", tc.wantFiltCount, tc.key, got)
			}
		})
	}
}

func TestColumnsBulkOpsAutoDisablesColsSelectedOnlyWithoutDataSelected(t *testing.T) {
	cases := []struct {
		key  string
		name string
	}{
		{key: "d", name: "d removes and disables cols selected-only"},
		{key: "X", name: "X clears and disables cols selected-only"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newCmdTestModel()
			m.focus = FocusColumns
			m.columns = []types.ColumnInfo{{Name: "alpha"}, {Name: "beta"}, {Name: "gamma"}}
			m.sel = selection.New([]string{"alpha", "beta", "gamma"})
			m.sel.Add("alpha")
			m.showSelectedInCols = true
			m.updateFilteredCols()
			if len(m.filteredCols) != 1 {
				t.Fatalf("expected initial cols filter to show selected columns only, got %d", len(m.filteredCols))
			}

			updated, cmd := m.handleColumnsKey(tc.key)
			m = updated.(Model)
			if cmd != nil {
				t.Fatalf("expected no command for %q, got %v", tc.key, cmd)
			}
			if m.showSelectedInCols {
				t.Fatalf("expected showSelectedInCols=false after %q", tc.key)
			}
			if got := m.sel.Count(); got != 0 {
				t.Fatalf("expected selection count 0 after %q, got %d", tc.key, got)
			}
			if got := len(m.filteredCols); got != 3 {
				t.Fatalf("expected filteredCols len 3 after %q, got %d", tc.key, got)
			}
			if m.statusMsg != "cols selected-list off (no columns selected)" {
				t.Fatalf("expected status message to mention cols selected-list auto-off after %q, got %q", tc.key, m.statusMsg)
			}
		})
	}
}

func TestColumnsSingleDeselectAutoDisablesColsSelectedOnly(t *testing.T) {
	m := newCmdTestModel()
	m.focus = FocusColumns
	m.columns = []types.ColumnInfo{{Name: "alpha"}, {Name: "beta"}, {Name: "gamma"}}
	m.sel = selection.New([]string{"alpha", "beta", "gamma"})
	m.sel.Add("alpha")
	m.showSelectedInCols = true
	m.selectedColName = "alpha"
	m.updateFilteredCols()
	if len(m.filteredCols) != 1 || m.filteredCols[0].Name != "alpha" {
		t.Fatalf("expected initial cols filter to show alpha only, got %#v", m.filteredCols)
	}

	updated, cmd := m.handleColumnsKey("x")
	m = updated.(Model)
	if cmd != nil {
		t.Fatalf("expected no command for %q, got %v", "x", cmd)
	}
	if m.showSelectedInCols {
		t.Fatal("expected showSelectedInCols disabled after deselecting last column")
	}
	if got := m.sel.Count(); got != 0 {
		t.Fatalf("expected selection count 0 after %q, got %d", "x", got)
	}
	if got := len(m.filteredCols); got != 3 {
		t.Fatalf("expected filteredCols len 3 after %q, got %d", "x", got)
	}
	if m.statusMsg != "cols selected-list off (no columns selected)" {
		t.Fatalf("expected status message to mention cols selected-list auto-off after %q, got %q", "x", m.statusMsg)
	}
}

func TestHandleKeyEnterOpensDetailFromTableAndColumnsFocus(t *testing.T) {
	t.Run("table focus uses selected column", func(t *testing.T) {
		m := newCmdTestModel()
		m.focus = FocusTable
		m.columns = []types.ColumnInfo{{Name: "alpha", DuckType: "DOUBLE"}}
		m.selectedColName = "alpha"
		m.updateFilteredCols()

		updated, cmd := m.handleKey(tea.KeyMsg{Type: tea.KeyEnter})
		if cmd == nil {
			t.Fatal("expected detail load command from table focus enter")
		}
		m = updated.(Model)
		if m.detailCol != "alpha" {
			t.Fatalf("expected detail col alpha, got %q", m.detailCol)
		}
		if m.overlay != OverlayDetail {
			t.Fatalf("expected detail overlay, got %v", m.overlay)
		}
		if m.detailTab != 1 {
			t.Fatalf("expected stats tab (1) for DOUBLE column, got %d", m.detailTab)
		}
	})

	t.Run("columns focus uses active column", func(t *testing.T) {
		m := newCmdTestModel()
		m.focus = FocusColumns
		m.columns = []types.ColumnInfo{
			{Name: "alpha", DuckType: "DOUBLE"},
			{Name: "beta", DuckType: "VARCHAR"},
		}
		m.selectedColName = "alpha"
		m.colCursor = 1
		m.updateFilteredCols()

		updated, cmd := m.handleKey(tea.KeyMsg{Type: tea.KeyEnter})
		if cmd == nil {
			t.Fatal("expected detail load command from columns focus enter")
		}
		m = updated.(Model)
		if m.detailCol != "alpha" {
			t.Fatalf("expected detail col alpha from active column, got %q", m.detailCol)
		}
		if m.overlay != OverlayDetail {
			t.Fatalf("expected detail overlay, got %v", m.overlay)
		}
		if m.detailTab != 1 {
			t.Fatalf("expected stats tab (1) for DOUBLE column, got %d", m.detailTab)
		}
	})
}

func TestDefaultDetailTab(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want int
	}{
		{name: "double", in: "DOUBLE", want: 1},
		{name: "decimal parameterized", in: "DECIMAL(10,2)", want: 1},
		{name: "numeric parameterized", in: "NUMERIC(12,4)", want: 1},
		{name: "float4 alias", in: "FLOAT4", want: 1},
		{name: "float8 alias", in: "FLOAT8", want: 1},
		{name: "integer", in: "INTEGER", want: 0},
		{name: "varchar", in: "VARCHAR", want: 0},
		{name: "empty", in: "", want: 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := defaultDetailTab(tc.in); got != tc.want {
				t.Fatalf("defaultDetailTab(%q) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

func TestHandleTableKeyHorizontalNavigationTracksViewportPaging(t *testing.T) {
	m := newTestModel()
	m.width = 100
	m.tableCols = []string{"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9"}
	m.selectedColName = "c0"

	if got := m.visibleColCount(); got != 4 {
		t.Fatalf("expected visibleColCount=4 for test setup, got %d", got)
	}

	cases := []struct {
		name             string
		key              string
		expectedSelected string
		expectedStartCol int
	}{
		{name: "right moves cursor within first page", key: "right", expectedSelected: "c1", expectedStartCol: 0},
		{name: "end jumps to last column", key: "$", expectedSelected: "c9", expectedStartCol: 6},
		{name: "home jumps to first column", key: "0", expectedSelected: "c0", expectedStartCol: 0},
		{name: "page right shifts one full screenful", key: "]", expectedSelected: "c7", expectedStartCol: 4},
		{name: "page right clamps at final start", key: "]", expectedSelected: "c9", expectedStartCol: 6},
		{name: "page left lands at left edge", key: "[", expectedSelected: "c2", expectedStartCol: 2},
		{name: "page right round-trips back", key: "]", expectedSelected: "c9", expectedStartCol: 6},
		{name: "page left again", key: "[", expectedSelected: "c2", expectedStartCol: 2},
		{name: "page left back to first page", key: "[", expectedSelected: "c0", expectedStartCol: 0},
		{name: "right clears offset hint", key: "right", expectedSelected: "c1", expectedStartCol: 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			updated, cmd := m.handleTableKey(tc.key)
			if cmd != nil {
				t.Fatalf("expected no load command for horizontal key %q", tc.key)
			}
			m = updated.(Model)

			if m.selectedColName != tc.expectedSelected {
				t.Fatalf("expected selected column %q, got %q", tc.expectedSelected, m.selectedColName)
			}
			startCol := m.computeTableColOff(m.visibleColCount())
			if startCol != tc.expectedStartCol {
				t.Fatalf("expected start col %d after %q, got %d", tc.expectedStartCol, tc.key, startCol)
			}
		})
	}
}

func TestHandleTableKeyHorizontalNavigationPagingBoundaries(t *testing.T) {
	t.Run("page left at first page is no-op", func(t *testing.T) {
		m := newTestModel()
		m.width = 100
		m.tableCols = []string{"c0", "c1", "c2", "c3", "c4", "c5"}
		m.selectedColName = "c0"

		updated, cmd := m.handleTableKey("[")
		if cmd != nil {
			t.Fatalf("expected no load command for horizontal key %q", "[")
		}
		m = updated.(Model)

		if m.selectedColName != "c0" {
			t.Fatalf("expected selected column to remain %q, got %q", "c0", m.selectedColName)
		}
		if startCol := m.computeTableColOff(m.visibleColCount()); startCol != 0 {
			t.Fatalf("expected start col to remain 0, got %d", startCol)
		}
	})

	t.Run("page right is no-op when all columns fit", func(t *testing.T) {
		m := newTestModel()
		m.width = 200
		m.tableCols = []string{"c0", "c1", "c2"}
		m.selectedColName = "c1"
		if got, want := m.visibleColCount(), len(m.tableCols); got < want {
			t.Fatalf("expected all columns to fit in viewport, visibleColCount=%d cols=%d", got, want)
		}

		updated, cmd := m.handleTableKey("]")
		if cmd != nil {
			t.Fatalf("expected no load command for horizontal key %q", "]")
		}
		m = updated.(Model)

		if m.selectedColName != "c1" {
			t.Fatalf("expected selected column to remain %q, got %q", "c1", m.selectedColName)
		}
		if startCol := m.computeTableColOff(m.visibleColCount()); startCol != 0 {
			t.Fatalf("expected start col to remain 0, got %d", startCol)
		}
	})
}

func TestHandleTableKeyHorizontalLeftKeepsViewportUntilLeftEdge(t *testing.T) {
	m := newTestModel()
	m.width = 100
	m.tableCols = []string{"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9"}
	m.selectedColName = "c6"

	if got := m.visibleColCount(); got != 4 {
		t.Fatalf("expected visibleColCount=4 for test setup, got %d", got)
	}
	if startCol := m.computeTableColOff(m.visibleColCount()); startCol != 3 {
		t.Fatalf("expected initial start col 3 for selected c6, got %d", startCol)
	}

	cases := []struct {
		key              string
		expectedSelected string
		expectedStartCol int
	}{
		{key: "left", expectedSelected: "c5", expectedStartCol: 3},
		{key: "left", expectedSelected: "c4", expectedStartCol: 3},
		{key: "left", expectedSelected: "c3", expectedStartCol: 3},
		{key: "left", expectedSelected: "c2", expectedStartCol: 2},
	}

	for _, tc := range cases {
		updated, cmd := m.handleTableKey(tc.key)
		if cmd != nil {
			t.Fatalf("expected no load command for horizontal key %q", tc.key)
		}
		m = updated.(Model)

		if m.selectedColName != tc.expectedSelected {
			t.Fatalf("expected selected column %q, got %q", tc.expectedSelected, m.selectedColName)
		}
		startCol := m.computeTableColOff(m.visibleColCount())
		if startCol != tc.expectedStartCol {
			t.Fatalf("expected start col %d after %q, got %d", tc.expectedStartCol, tc.key, startCol)
		}
	}
}

func TestHandleTableKeyHorizontalRightKeepsViewportUntilRightEdge(t *testing.T) {
	m := newTestModel()
	m.width = 100
	m.tableCols = []string{"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9"}
	m.selectedColName = "c0"

	if got := m.visibleColCount(); got != 4 {
		t.Fatalf("expected visibleColCount=4 for test setup, got %d", got)
	}
	if startCol := m.computeTableColOff(m.visibleColCount()); startCol != 0 {
		t.Fatalf("expected initial start col 0 for selected c0, got %d", startCol)
	}

	cases := []struct {
		key              string
		expectedSelected string
		expectedStartCol int
	}{
		{key: "right", expectedSelected: "c1", expectedStartCol: 0},
		{key: "right", expectedSelected: "c2", expectedStartCol: 0},
		{key: "right", expectedSelected: "c3", expectedStartCol: 0},
		{key: "right", expectedSelected: "c4", expectedStartCol: 1},
	}

	for _, tc := range cases {
		updated, cmd := m.handleTableKey(tc.key)
		if cmd != nil {
			t.Fatalf("expected no load command for horizontal key %q", tc.key)
		}
		m = updated.(Model)

		if m.selectedColName != tc.expectedSelected {
			t.Fatalf("expected selected column %q, got %q", tc.expectedSelected, m.selectedColName)
		}
		startCol := m.computeTableColOff(m.visibleColCount())
		if startCol != tc.expectedStartCol {
			t.Fatalf("expected start col %d after %q, got %d", tc.expectedStartCol, tc.key, startCol)
		}
	}
}

func TestHandleTableKeyHorizontalLeftClampAtLeftBoundary(t *testing.T) {
	m := newTestModel()
	m.width = 100
	m.tableCols = []string{"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9"}
	m.selectedColName = "c2"
	// Pin viewport so c2 is visible at startCol=2 initially.
	m.tableColOffHint = 2

	if got := m.visibleColCount(); got != 4 {
		t.Fatalf("expected visibleColCount=4 for test setup, got %d", got)
	}

	// Navigate left from c2 to c0, exercising the max(0, startCol-1) clamp.
	cases := []struct {
		key              string
		expectedSelected string
		expectedStartCol int
	}{
		{key: "left", expectedSelected: "c1", expectedStartCol: 1},
		{key: "left", expectedSelected: "c0", expectedStartCol: 0},
	}

	for _, tc := range cases {
		updated, cmd := m.handleTableKey(tc.key)
		if cmd != nil {
			t.Fatalf("expected no load command for horizontal key %q", tc.key)
		}
		m = updated.(Model)

		if m.selectedColName != tc.expectedSelected {
			t.Fatalf("expected selected column %q, got %q", tc.expectedSelected, m.selectedColName)
		}
		startCol := m.computeTableColOff(m.visibleColCount())
		if startCol != tc.expectedStartCol {
			t.Fatalf("expected start col %d after %q, got %d", tc.expectedStartCol, tc.key, startCol)
		}
	}

	// One more left press at c0 should be a no-op (idx == 0, not idx > 0).
	updated, cmd := m.handleTableKey("left")
	if cmd != nil {
		t.Fatalf("expected no load command pressing left at c0")
	}
	m = updated.(Model)
	if m.selectedColName != "c0" {
		t.Fatalf("expected selectedColName to remain c0, got %q", m.selectedColName)
	}
	startCol := m.computeTableColOff(m.visibleColCount())
	if startCol != 0 {
		t.Fatalf("expected startCol to remain 0, got %d", startCol)
	}
}

func TestHandleTableKeyHorizontalRightClampAtRightBoundary(t *testing.T) {
	m := newTestModel()
	m.width = 100
	m.tableCols = []string{"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9"}
	m.selectedColName = "c4"
	// Pin viewport so c4..c7 are visible at startCol=4.
	m.tableColOffHint = 4

	if got := m.visibleColCount(); got != 4 {
		t.Fatalf("expected visibleColCount=4 for test setup, got %d", got)
	}

	// Navigate right from c4 to c9, exercising the min(maxStart, startCol+1) clamp.
	cases := []struct {
		key              string
		expectedSelected string
		expectedStartCol int
	}{
		{key: "right", expectedSelected: "c5", expectedStartCol: 4},
		{key: "right", expectedSelected: "c6", expectedStartCol: 4},
		{key: "right", expectedSelected: "c7", expectedStartCol: 4},
		{key: "right", expectedSelected: "c8", expectedStartCol: 5},
		{key: "right", expectedSelected: "c9", expectedStartCol: 6},
	}

	for _, tc := range cases {
		updated, cmd := m.handleTableKey(tc.key)
		if cmd != nil {
			t.Fatalf("expected no load command for horizontal key %q", tc.key)
		}
		m = updated.(Model)

		if m.selectedColName != tc.expectedSelected {
			t.Fatalf("expected selected column %q, got %q", tc.expectedSelected, m.selectedColName)
		}
		startCol := m.computeTableColOff(m.visibleColCount())
		if startCol != tc.expectedStartCol {
			t.Fatalf("expected start col %d after %q, got %d", tc.expectedStartCol, tc.key, startCol)
		}
	}

	// One more right press at c9 should be a no-op (idx == len-1).
	updated, cmd := m.handleTableKey("right")
	if cmd != nil {
		t.Fatalf("expected no load command pressing right at c9")
	}
	m = updated.(Model)
	if m.selectedColName != "c9" {
		t.Fatalf("expected selectedColName to remain c9, got %q", m.selectedColName)
	}
	startCol := m.computeTableColOff(m.visibleColCount())
	if startCol != 6 {
		t.Fatalf("expected startCol to remain 6, got %d", startCol)
	}
}

func TestHandleTableKeyHorizontalRoundTrip(t *testing.T) {
	m := newTestModel()
	m.width = 100
	m.tableCols = []string{"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9"}
	m.selectedColName = "c0"

	if got := m.visibleColCount(); got != 4 {
		t.Fatalf("expected visibleColCount=4 for test setup, got %d", got)
	}

	// Navigate right from c0 to c9.
	for i := 0; i < 9; i++ {
		updated, cmd := m.handleTableKey("right")
		if cmd != nil {
			t.Fatalf("right step %d: expected no load command", i+1)
		}
		m = updated.(Model)
	}
	if m.selectedColName != "c9" {
		t.Fatalf("expected c9 after full right traversal, got %q", m.selectedColName)
	}
	startCol := m.computeTableColOff(m.visibleColCount())
	if startCol != 6 {
		t.Fatalf("expected startCol=6 at c9, got %d", startCol)
	}

	// Navigate left from c9 back to c0.
	for i := 0; i < 9; i++ {
		updated, cmd := m.handleTableKey("left")
		if cmd != nil {
			t.Fatalf("left step %d: expected no load command", i+1)
		}
		m = updated.(Model)
	}
	if m.selectedColName != "c0" {
		t.Fatalf("expected c0 after full left traversal, got %q", m.selectedColName)
	}
	startCol = m.computeTableColOff(m.visibleColCount())
	if startCol != 0 {
		t.Fatalf("expected startCol=0 at c0, got %d", startCol)
	}
}

func TestHandleTableKeyGPositionsCursorAtFinalRow(t *testing.T) {
	m := newCmdTestModel()
	m.width = 120
	m.height = 10
	m.pageSize = 50
	m.totalRows = 200
	m.tableCols = []string{"c0"}

	updated, cmd := m.handleTableKey("G")
	if cmd == nil {
		t.Fatal("expected load command for G")
	}
	m = updated.(Model)

	if m.tableOffset != m.maxTableOffset() {
		t.Fatalf("expected tableOffset at max (%d), got %d", m.maxTableOffset(), m.tableOffset)
	}
	if m.tableRowCursor != m.visibleTableRows()-1 {
		t.Fatalf("expected row cursor at bottom visible row (%d), got %d", m.visibleTableRows()-1, m.tableRowCursor)
	}

	rows := make([][]string, 4)
	for i := range rows {
		rows[i] = []string{"v"}
	}
	m = updateModel(t, m, previewDoneMsg{
		rows:      rows,
		colNames:  []string{"c0"},
		totalRows: 200,
	})

	absRow := m.tableOffset + m.tableRowCursor + 1
	if absRow != int(m.totalRows) {
		t.Fatalf("expected cursor on final row %d, got %d (offset=%d cursor=%d)", m.totalRows, absRow, m.tableOffset, m.tableRowCursor)
	}
}

func TestHandleTableKeyGWithZeroVisibleRowsStaysWithinBounds(t *testing.T) {
	m := newCmdTestModel()
	m.width = 120
	m.height = 6
	m.pageSize = 50
	m.totalRows = 200
	m.tableCols = []string{"c0"}

	if got := m.visibleTableRows(); got != 0 {
		t.Fatalf("expected zero visible rows, got %d", got)
	}
	if got := m.maxTableOffset(); got != int(m.totalRows)-1 {
		t.Fatalf("expected maxTableOffset %d, got %d", int(m.totalRows)-1, got)
	}

	updated, cmd := m.handleTableKey("G")
	if cmd == nil {
		t.Fatal("expected load command for G")
	}
	m = updated.(Model)

	if m.tableOffset != int(m.totalRows)-1 {
		t.Fatalf("expected tableOffset at final row index %d, got %d", int(m.totalRows)-1, m.tableOffset)
	}
	if m.tableRowCursor != 0 {
		t.Fatalf("expected row cursor clamped to 0, got %d", m.tableRowCursor)
	}
}

func TestHandleTableKeyHorizontalNoOpWhenZeroVisibleCols(t *testing.T) {
	m := newTestModel()
	// width=10 is narrower than tableOuterWidth (border+padding) + tableColMinWidth,
	// so visibleColCount() returns 0. If layout constants change, the guard below will catch it.
	m.width = 10
	m.height = 20
	m.tableCols = []string{"a", "b"}
	m.selectedColName = "a"

	if got := m.visibleColCount(); got != 0 {
		t.Fatalf("expected zero visible columns at width %d, got %d", m.width, got)
	}

	// left/h: already at left edge, cursor stays; right/l: cursor advances but hint must not be corrupted.
	leftRightCases := []struct {
		key         string
		wantColName string
	}{
		{"left", "a"},
		{"h", "a"},
		{"right", "b"},
		{"l", "b"},
	}
	for _, tc := range leftRightCases {
		updated, cmd := m.handleTableKey(tc.key)
		if cmd != nil {
			t.Errorf("key %q: expected no command, got %v", tc.key, cmd)
		}
		um := updated.(Model)
		if um.selectedColName != tc.wantColName {
			t.Errorf("key %q: expected selectedColName %q, got %q", tc.key, tc.wantColName, um.selectedColName)
		}
		if um.tableColOffHint != m.tableColOffHint {
			t.Errorf("key %q: tableColOffHint changed from %d to %d", tc.key, m.tableColOffHint, um.tableColOffHint)
		}
		if h := um.tableColOffHint; h != -1 && (h < 0 || h >= len(m.tableCols)) {
			t.Errorf("key %q: tableColOffHint %d out of bounds for %d columns", tc.key, h, len(m.tableCols))
		}
	}

	m.selectedColName = "b"
	m.tableColOffHint = -1
	for _, key := range []string{"left", "h"} {
		updated, cmd := m.handleTableKey(key)
		if cmd != nil {
			t.Errorf("key %q from b: expected no command, got %v", key, cmd)
		}
		um := updated.(Model)
		if um.selectedColName != "a" {
			t.Errorf("key %q from b: expected selectedColName %q, got %q", key, "a", um.selectedColName)
		}
		if um.tableColOffHint != -1 {
			t.Errorf("key %q from b: expected tableColOffHint to remain -1, got %d", key, um.tableColOffHint)
		}
	}
}

func TestPreviewDoneMsgReconcilesSelectedColumnWhenProjectionChanges(t *testing.T) {
	m := newTestModel()
	m.columns = []types.ColumnInfo{
		{Name: "alpha"},
		{Name: "beta"},
		{Name: "gamma"},
	}
	m.selectedColName = "beta"
	m.colCursor = 1
	m.updateFilteredCols()

	m = updateModel(t, m, previewDoneMsg{
		rows:      [][]string{{"1", "2"}},
		colNames:  []string{"alpha", "gamma"},
		totalRows: 2,
	})

	if m.selectedColName != "alpha" {
		t.Fatalf("expected selectedColName to move to first visible column, got %q", m.selectedColName)
	}
	if m.tableColCursor() != 0 {
		t.Fatalf("expected table column cursor to point at visible column 0, got %d", m.tableColCursor())
	}
	if m.colCursor != 0 {
		t.Fatalf("expected column pane cursor to sync to alpha, got %d", m.colCursor)
	}
}

func TestPreviewDoneMsgReconcileNotFoundClampsColumnsListOffset(t *testing.T) {
	m := newTestModel()
	m.width = 120
	m.height = 18
	m.columns = make([]types.ColumnInfo, 30)
	for i := range m.columns {
		m.columns[i] = types.ColumnInfo{Name: fmt.Sprintf("c%02d", i)}
	}
	m.selectedColName = "c25"
	m.colCursor = 25
	m.colListOff = 20
	m.updateFilteredCols()

	m = updateModel(t, m, previewDoneMsg{
		rows:      [][]string{{"1", "2", "3"}},
		colNames:  []string{"c00", "c01", "c02"},
		totalRows: 1,
	})

	if m.selectedColName != "c00" {
		t.Fatalf("expected selectedColName to reconcile to c00, got %q", m.selectedColName)
	}
	if m.colCursor != 0 {
		t.Fatalf("expected column cursor to sync to c00 at index 0, got %d", m.colCursor)
	}
	if m.colListOff != 0 {
		t.Fatalf("expected colListOff to clamp to 0 after reconcile, got %d", m.colListOff)
	}
}

func TestPreviewDoneMsgKeepsColumnActionsConsistentWhenCursorCannotSync(t *testing.T) {
	m := newTestModel()
	m.columns = []types.ColumnInfo{
		{Name: "alpha"},
		{Name: "beta"},
		{Name: "gamma"},
	}
	m.sel = selection.New([]string{"alpha", "beta", "gamma"})
	m.selectedColName = "beta"
	m.colCursor = 1
	m.searchQuery = "beta"
	m.updateFilteredCols()

	m = updateModel(t, m, previewDoneMsg{
		rows:      [][]string{{"1", "2"}},
		colNames:  []string{"alpha", "gamma"},
		totalRows: 2,
	})

	if m.selectedColName != "alpha" {
		t.Fatalf("expected selectedColName to reconcile to alpha, got %q", m.selectedColName)
	}
	m.focus = FocusColumns

	out := m.viewColumns(30, 8)
	lines := strings.Split(out, "\n")
	if len(lines) < 3 {
		t.Fatalf("expected at least 3 lines from columns view, got %d", len(lines))
	}
	wantHighlighted := highlightStyle.Width(30).Render(fmt.Sprintf("%s %s %s%s", unselectedMarkGlyph, truncate("beta", 18), truncate("", 8), ""))
	if lines[2] != wantHighlighted {
		t.Fatalf("expected effective active row to remain visibly highlighted, got %q", lines[2])
	}

	updated, _ := m.handleColumnsKey("x")
	m = updated.(Model)
	if !m.sel.IsSelected("beta") {
		t.Fatal("expected x to toggle cursor column beta from filtered results")
	}
	if m.sel.IsSelected("alpha") {
		t.Fatal("expected x not to toggle hidden selected column alpha")
	}

	updated, _ = m.handleKey(tea.KeyMsg{Type: tea.KeyEnter})
	m = updated.(Model)
	if m.detailCol != "beta" {
		t.Fatalf("expected enter to open detail for cursor column beta, got %q", m.detailCol)
	}
}

func TestColumnsActionsPreferSelectedWhenCursorDiffersAndBothVisible(t *testing.T) {
	m := newTestModel()
	m.columns = []types.ColumnInfo{
		{Name: "alpha"},
		{Name: "beta"},
		{Name: "gamma"},
	}
	m.sel = selection.New([]string{"alpha", "beta", "gamma"})
	m.selectedColName = "alpha"
	m.colCursor = 1
	m.updateFilteredCols()
	m.focus = FocusColumns

	out := m.viewColumns(30, 8)
	lines := strings.Split(out, "\n")
	if len(lines) < 3 {
		t.Fatalf("expected at least 3 lines from columns view, got %d", len(lines))
	}
	wantHighlighted := highlightStyle.Width(30).Render(fmt.Sprintf("%s %s %s%s", unselectedMarkGlyph, truncate("alpha", 18), truncate("", 8), ""))
	if lines[2] != wantHighlighted {
		t.Fatalf("expected selected column alpha to remain highlighted, got %q", lines[2])
	}

	updated, _ := m.handleColumnsKey("x")
	m = updated.(Model)
	if !m.sel.IsSelected("alpha") {
		t.Fatal("expected x to toggle selected active column alpha")
	}
	if m.sel.IsSelected("beta") {
		t.Fatal("expected x not to toggle cursor column beta")
	}

	updated, _ = m.handleKey(tea.KeyMsg{Type: tea.KeyEnter})
	m = updated.(Model)
	if m.detailCol != "alpha" {
		t.Fatalf("expected enter to open detail for selected active column alpha, got %q", m.detailCol)
	}
}

func TestPreviewDoneMsgClampsRowCursorToVisibleRows(t *testing.T) {
	m := newTestModel()
	m.width = 120
	m.height = 10
	m.tableRowCursor = 40

	rows := make([][]string, 50)
	for i := range rows {
		rows[i] = []string{"v"}
	}

	m = updateModel(t, m, previewDoneMsg{
		rows:      rows,
		colNames:  []string{"alpha"},
		totalRows: 50,
	})

	if m.tableRowCursor != 2 {
		t.Fatalf("expected row cursor to clamp to last visible row (2), got %d", m.tableRowCursor)
	}
}

func TestUpdateFilteredColsResyncsSelectedColumn(t *testing.T) {
	t.Run("selected column filtered out resyncs to cursor", func(t *testing.T) {
		m := Model{
			columns: []types.ColumnInfo{
				{Name: "alpha"},
				{Name: "beta"},
			},
			selectedColName: "beta",
			colCursor:       1,
		}

		m.searchQuery = "alpha"
		m.updateFilteredCols()

		if m.colCursor != 0 {
			t.Fatalf("expected colCursor=0, got %d", m.colCursor)
		}
		if m.selectedColName != "alpha" {
			t.Fatalf("expected selectedColName=alpha, got %q", m.selectedColName)
		}
	})

	t.Run("no filtered columns clears selected column", func(t *testing.T) {
		m := Model{
			columns: []types.ColumnInfo{
				{Name: "alpha"},
			},
			selectedColName: "alpha",
			colCursor:       0,
		}

		m.searchQuery = "zzz"
		m.updateFilteredCols()

		if len(m.filteredCols) != 0 {
			t.Fatalf("expected no filtered columns, got %d", len(m.filteredCols))
		}
		if m.selectedColName != "" {
			t.Fatalf("expected selectedColName to be cleared, got %q", m.selectedColName)
		}
	})
}

func TestHandleKeyEscClearsFocusedSearch(t *testing.T) {
	m := newTestModel()
	m.focus = FocusColumns
	m.columns = []types.ColumnInfo{
		{Name: "alpha"},
		{Name: "beta"},
	}
	m.searchInput = textinput.New()
	m.searchInput.Prompt = "/ "
	m.searchInput.PromptStyle = searchPromptStyle
	m.searchInput.SetValue("alpha")
	m.searchQuery = "alpha"
	m.searchFocused = true
	m.updateFilteredCols()
	// filteredCols has 1 entry; set cursor out-of-bounds to verify clamping on Esc
	m.colCursor = 1

	updated, _ := m.handleKey(tea.KeyMsg{Type: tea.KeyEsc})
	m = updated.(Model)

	if m.searchFocused {
		t.Fatal("expected esc to unfocus search")
	}
	if m.searchInput.Value() != "" {
		t.Fatalf("expected esc to clear search input, got %q", m.searchInput.Value())
	}
	if m.searchQuery != "" {
		t.Fatalf("expected esc to clear search query, got %q", m.searchQuery)
	}
	if len(m.filteredCols) != 2 {
		t.Fatalf("expected esc clear to restore all columns, got %d", len(m.filteredCols))
	}
	if m.colCursor >= len(m.filteredCols) {
		t.Fatalf("expected colCursor clamped within filteredCols, got %d (len %d)", m.colCursor, len(m.filteredCols))
	}
}

func TestHandleKeyEscFocusedSearchRestoresColsSelectedOnlyMode(t *testing.T) {
	m := newTestModel()
	m.focus = FocusColumns
	m.columns = []types.ColumnInfo{
		{Name: "alpha"},
		{Name: "beta"},
		{Name: "gamma"},
	}
	m.sel = selection.New([]string{"alpha", "beta", "gamma"})
	m.sel.Add("alpha")
	m.showSelectedInCols = true
	m.searchInput = textinput.New()
	m.searchInput.Prompt = "/ "
	m.searchInput.PromptStyle = searchPromptStyle
	m.searchInput.SetValue("beta")
	m.searchQuery = "beta"
	m.searchFocused = true
	m.updateFilteredCols()

	updated, _ := m.handleKey(tea.KeyMsg{Type: tea.KeyEsc})
	m = updated.(Model)

	if m.searchFocused {
		t.Fatal("expected esc to unfocus search")
	}
	if m.searchQuery != "" {
		t.Fatalf("expected esc to clear search query, got %q", m.searchQuery)
	}
	if len(m.filteredCols) != 1 || m.filteredCols[0].Name != "alpha" {
		t.Fatalf("expected esc to restore selected-only columns, got %#v", m.filteredCols)
	}
}

func TestHandleKeyCtrlUThenEnterRestoresColsSelectedOnlyMode(t *testing.T) {
	m := newTestModel()
	m.focus = FocusColumns
	m.columns = []types.ColumnInfo{
		{Name: "alpha"},
		{Name: "beta"},
		{Name: "gamma"},
	}
	m.sel = selection.New([]string{"alpha", "beta", "gamma"})
	m.sel.Add("alpha")
	m.showSelectedInCols = true
	m.searchInput = textinput.New()
	m.searchInput.Prompt = "/ "
	m.searchInput.PromptStyle = searchPromptStyle
	m.searchInput.SetValue("beta")
	m.searchQuery = "beta"
	m.searchFocused = true
	m.updateFilteredCols()

	updated, _ := m.handleKey(tea.KeyMsg{Type: tea.KeyCtrlU})
	m = updated.(Model)
	if !m.searchFocused {
		t.Fatal("expected search to remain focused after ctrl+u")
	}
	if m.searchQuery != "" {
		t.Fatalf("expected ctrl+u to clear search query, got %q", m.searchQuery)
	}
	if len(m.filteredCols) != 3 {
		t.Fatalf("expected all columns while search remains focused, got %d", len(m.filteredCols))
	}

	updated, _ = m.handleKey(tea.KeyMsg{Type: tea.KeyEnter})
	m = updated.(Model)
	if m.searchFocused {
		t.Fatal("expected enter to unfocus search")
	}
	if len(m.filteredCols) != 1 || m.filteredCols[0].Name != "alpha" {
		t.Fatalf("expected enter to reapply selected-only columns, got %#v", m.filteredCols)
	}
}

func TestHandleColumnsKeyEscClearsSearchWhenUnfocused(t *testing.T) {
	m := newTestModel()
	m.focus = FocusColumns
	m.columns = []types.ColumnInfo{
		{Name: "alpha"},
		{Name: "beta"},
	}
	m.searchInput = textinput.New()
	m.searchInput.Prompt = "/ "
	m.searchInput.SetValue("alpha")
	m.searchQuery = "alpha"
	m.updateFilteredCols()
	// filteredCols has 1 entry; set cursor out-of-bounds to verify clamping on Esc
	m.colCursor = 1

	updated, _ := m.handleColumnsKey("esc")
	m = updated.(Model)

	if m.searchInput.Value() != "" {
		t.Fatalf("expected esc to clear search input, got %q", m.searchInput.Value())
	}
	if m.searchQuery != "" {
		t.Fatalf("expected esc to clear search query, got %q", m.searchQuery)
	}
	if len(m.filteredCols) != 2 {
		t.Fatalf("expected esc clear to restore all columns, got %d", len(m.filteredCols))
	}
	if m.colCursor >= len(m.filteredCols) {
		t.Fatalf("expected colCursor clamped within filteredCols, got %d (len %d)", m.colCursor, len(m.filteredCols))
	}
}

func TestHandleKeySearchFocusedAllowsSpaces(t *testing.T) {
	m := newTestModel()
	m.focus = FocusColumns
	m.columns = []types.ColumnInfo{
		{Name: "customer_account_identifier"},
		{Name: "status"},
	}
	m.searchInput = textinput.New()
	m.searchInput.PromptStyle = searchPromptStyle
	m.searchInput.Prompt = "/ "
	m.searchInput.Focus()
	m.searchFocused = true
	m.updateFilteredCols()

	for _, r := range "customer id" {
		updated, _ := m.handleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		m = updated.(Model)
	}

	if m.searchQuery != "customer id" {
		t.Fatalf("expected search query with space, got %q", m.searchQuery)
	}
	if len(m.filteredCols) != 1 || m.filteredCols[0].Name != "customer_account_identifier" {
		t.Fatalf("expected multi-term search to match underscore column, got %#v", m.filteredCols)
	}
}

func TestShowSelectedInColumnsIgnoredWhileSearchingAndRestoredAfterClear(t *testing.T) {
	m := newTestModel()
	m.focus = FocusColumns
	m.searchInput = textinput.New()
	m.searchInput.Prompt = "/ "
	m.searchInput.PromptStyle = searchPromptStyle
	m.columns = []types.ColumnInfo{
		{Name: "alpha"},
		{Name: "beta"},
		{Name: "gamma"},
	}
	m.sel = selection.New([]string{"alpha", "beta", "gamma"})
	m.sel.Add("alpha")
	m.showSelectedInCols = true
	m.updateFilteredCols()
	if len(m.filteredCols) != 1 || m.filteredCols[0].Name != "alpha" {
		t.Fatalf("expected selected-only list before search, got %#v", m.filteredCols)
	}

	updated, _ := m.handleColumnsKey("/")
	m = updated.(Model)
	for _, r := range "beta" {
		updated, _ = m.handleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		m = updated.(Model)
	}
	if !m.searchFocused {
		t.Fatal("expected search to stay focused while typing")
	}
	if m.searchQuery != "beta" {
		t.Fatalf("expected search query beta, got %q", m.searchQuery)
	}
	if len(m.filteredCols) != 1 || m.filteredCols[0].Name != "beta" {
		t.Fatalf("expected search to show matching unselected column while selected-only is enabled, got %#v", m.filteredCols)
	}

	updated, _ = m.handleKey(tea.KeyMsg{Type: tea.KeyEnter})
	m = updated.(Model)
	if m.searchFocused {
		t.Fatal("expected enter to unfocus search")
	}
	if len(m.filteredCols) != 1 || m.filteredCols[0].Name != "beta" {
		t.Fatalf("expected active query to keep showing all searchable columns, got %#v", m.filteredCols)
	}

	updated, _ = m.handleColumnsKey("esc")
	m = updated.(Model)
	if m.searchQuery != "" {
		t.Fatalf("expected esc to clear search query, got %q", m.searchQuery)
	}
	if len(m.filteredCols) != 1 || m.filteredCols[0].Name != "alpha" {
		t.Fatalf("expected selected-only list restored after search clear, got %#v", m.filteredCols)
	}
}

func TestTypingVWhileSearchFocusedDoesNotToggleColsSelectedOnly(t *testing.T) {
	m := newTestModel()
	m.focus = FocusColumns
	m.searchInput = textinput.New()
	m.searchInput.Prompt = "/ "
	m.searchInput.PromptStyle = searchPromptStyle
	m.searchInput.Focus()
	m.searchFocused = true
	m.columns = []types.ColumnInfo{
		{Name: "alpha"},
		{Name: "beta"},
	}
	m.sel = selection.New([]string{"alpha", "beta"})
	m.showSelectedInCols = true
	m.updateFilteredCols()

	updated, _ := m.handleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'v'}})
	m = updated.(Model)
	if !m.searchFocused {
		t.Fatal("expected search to remain focused while typing")
	}
	if m.searchQuery != "v" {
		t.Fatalf("expected search query v, got %q", m.searchQuery)
	}
	if !m.showSelectedInCols {
		t.Fatal("expected showSelectedInCols unchanged while search is focused")
	}
}

func TestViewColumnsSearchFocusShowsSingleSlash(t *testing.T) {
	m := newTestModel()
	m.columns = []types.ColumnInfo{
		{Name: "alpha"},
	}
	m.sel = selection.New([]string{"alpha"})
	m.searchInput = textinput.New()
	m.searchInput.Prompt = "/ "
	m.searchInput.PromptStyle = searchPromptStyle
	m.searchInput.SetValue("alpha")
	m.searchFocused = true
	m.updateFilteredCols()

	out := m.viewColumns(40, 8)
	line := strings.Split(out, "\n")[0]
	if strings.Count(line, "/") != 1 {
		t.Fatalf("expected a single slash in focused search line, got %q", line)
	}
}

func TestProfileSummaryOrderingPreservesDetail(t *testing.T) {
	colName := "score"

	t.Run("detail_then_basic_keeps_detail", func(t *testing.T) {
		m := newTestModel()

		detail := &types.ColumnSummary{
			Loaded:       true,
			DetailLoaded: true,
			Top3: []types.TopValue{
				{Value: "1", Count: 2},
			},
		}
		basic := &types.ColumnSummary{
			Loaded:       true,
			DetailLoaded: false,
		}

		m = updateModel(t, m, profileDetailDoneMsg{colName: colName, summary: detail})
		m = updateModel(t, m, profileBasicDoneMsg{colName: colName, summary: basic})

		got := m.summaries[colName]
		if got == nil {
			t.Fatal("expected summary to exist")
		}
		if !got.DetailLoaded {
			t.Fatal("expected DetailLoaded to remain true")
		}
		if len(got.Top3) != 1 || got.Top3[0].Value != "1" {
			t.Fatalf("expected detail data to be preserved, got %#v", got.Top3)
		}
	})

	t.Run("basic_then_detail_applies_detail", func(t *testing.T) {
		m := newTestModel()

		basic := &types.ColumnSummary{
			Loaded:       true,
			DetailLoaded: false,
		}
		detail := &types.ColumnSummary{
			Loaded:       true,
			DetailLoaded: true,
			Top3: []types.TopValue{
				{Value: "2", Count: 3},
			},
		}

		m = updateModel(t, m, profileBasicDoneMsg{colName: colName, summary: basic})
		m = updateModel(t, m, profileDetailDoneMsg{colName: colName, summary: detail})

		got := m.summaries[colName]
		if got == nil {
			t.Fatal("expected summary to exist")
		}
		if !got.DetailLoaded {
			t.Fatal("expected DetailLoaded to be true")
		}
		if len(got.Top3) != 1 || got.Top3[0].Value != "2" {
			t.Fatalf("expected detail data to be present, got %#v", got.Top3)
		}
	})
}

func TestViewTableNullDotsRenderOnlyWhenExpected(t *testing.T) {
	m := newTestModel()
	m.width = 120
	m.height = 10
	m.tableCols = []string{"a", "b"}
	m.selectedColName = "a"
	m.tableData = [][]string{
		{"NULL", "x"},
		{"y", "z"},
	}
	m.tableRowHasMissing = rowHasMissingFlags(m.tableData)
	m.summaries["a"] = &types.ColumnSummary{Loaded: true, MissingCount: 1}
	m.summaries["b"] = &types.ColumnSummary{Loaded: true, MissingCount: 0}

	out := m.viewTable(60, 6)
	lines := strings.Split(out, "\n")
	if len(lines) < 4 {
		t.Fatalf("expected at least 4 lines in table view, got %d", len(lines))
	}

	if strings.Count(lines[0], nullDotChar) != 1 {
		t.Fatalf("expected exactly one header null dot, got %q", lines[0])
	}
	if !strings.Contains(lines[1], nullDotChar) {
		t.Fatalf("expected null-dot row marker for row containing NULL, got %q", lines[1])
	}
	if strings.Contains(lines[2], nullDotChar) {
		t.Fatalf("expected no row marker for row without NULL, got %q", lines[2])
	}
}

func TestViewColumnsNullDotRendersNextToColumnName(t *testing.T) {
	t.Run("unhighlighted row", func(t *testing.T) {
		m := newTestModel()
		m.columns = []types.ColumnInfo{
			{Name: "alpha", DuckType: "BIGINT"},
			{Name: "beta", DuckType: "VARCHAR"},
		}
		m.sel = selection.New(nil)
		m.selectedColName = "beta"
		m.focus = FocusTable
		m.updateFilteredCols()
		m.summaries["alpha"] = &types.ColumnSummary{Loaded: true, MissingCount: 1}
		m.summaries["beta"] = &types.ColumnSummary{Loaded: true, MissingCount: 0}

		out := m.viewColumns(40, 8)
		if !strings.Contains(out, "alpha "+nullDot) {
			t.Fatalf("expected null dot directly after alpha in column list, got %q", out)
		}
		if strings.Contains(out, "beta "+nullDot) {
			t.Fatalf("expected no null dot for beta without nulls, got %q", out)
		}
	})

	t.Run("highlighted row", func(t *testing.T) {
		cases := []struct {
			name  string
			focus Focus
			style lipgloss.Style
		}{
			{name: "focused columns", focus: FocusColumns, style: highlightStyle},
			{name: "unfocused columns pane", focus: FocusTable, style: dimHighlightStyle},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				m := newTestModel()
				m.columns = []types.ColumnInfo{{Name: "alpha", DuckType: "BIGINT"}}
				m.sel = selection.New(nil)
				m.selectedColName = "alpha"
				m.focus = tc.focus
				m.updateFilteredCols()
				m.summaries["alpha"] = &types.ColumnSummary{Loaded: true, MissingCount: 1}

				out := m.viewColumns(40, 6)
				lines := strings.Split(out, "\n")
				if len(lines) < 3 {
					t.Fatalf("expected at least 3 lines from columns view, got %d", len(lines))
				}

				wantName := truncate("alpha", 40-12-inlineNullDotWidth())
				wantPlain := fmt.Sprintf("%s %s %s%s", unselectedMarkGlyph, wantName+" "+nullDotChar, truncate("BIGINT", 8), " M:0% D:0%")
				want := tc.style.Width(40).Render(wantPlain)
				if lines[2] != want {
					t.Fatalf("expected highlighted row render %q, got %q", want, lines[2])
				}
			})
		}
	})

	t.Run("not loaded suppresses dot", func(t *testing.T) {
		m := newTestModel()
		m.columns = []types.ColumnInfo{{Name: "alpha", DuckType: "BIGINT"}}
		m.sel = selection.New(nil)
		m.selectedColName = "alpha"
		m.focus = FocusTable
		m.updateFilteredCols()
		m.summaries["alpha"] = &types.ColumnSummary{Loaded: false, MissingCount: 5}

		out := m.viewColumns(40, 6)
		if strings.Contains(out, nullDot) {
			t.Fatalf("expected no null dot when summary not loaded, got %q", out)
		}
	})

	t.Run("no summary entry suppresses dot", func(t *testing.T) {
		m := newTestModel()
		m.columns = []types.ColumnInfo{{Name: "alpha", DuckType: "BIGINT"}}
		m.sel = selection.New(nil)
		m.selectedColName = "alpha"
		m.focus = FocusTable
		m.updateFilteredCols()
		// no entry in m.summaries

		out := m.viewColumns(40, 6)
		if strings.Contains(out, nullDot) {
			t.Fatalf("expected no null dot when column has no summary entry, got %q", out)
		}
	})

	t.Run("name truncated to make room for dot", func(t *testing.T) {
		// With w=20: nameWidth = max(0, 20-12) = 8; with hasMissing: 8-inlineNullDotWidth = 6.
		// truncate("verylongcolumnname", 6) = "veryl…" (5 chars + ellipsis).
		// Without the -inlineNullDotWidth adjustment nameWidth stays 8, giving "verylo…",
		// and the line would be 2 cells wider than w (hidden by clampLineWidth).
		m := newTestModel()
		m.columns = []types.ColumnInfo{{Name: "verylongcolumnname", DuckType: "BIGINT"}}
		m.sel = selection.New(nil)
		m.selectedColName = ""
		m.focus = FocusTable
		m.updateFilteredCols()
		m.summaries["verylongcolumnname"] = &types.ColumnSummary{Loaded: true, MissingCount: 1}

		out := m.viewColumns(20, 6)
		if !strings.Contains(out, nullDot) {
			t.Fatalf("expected null dot in output, got %q", out)
		}
		// "veryl…" is the 6-char truncation; "verylo…" would indicate the 8-char
		// (un-adjusted) truncation that the -inlineNullDotWidth fix is meant to prevent.
		if strings.Contains(out, "verylo…") {
			t.Fatalf("name not shortened for null dot: found 8-char truncation, got %q", out)
		}
		if !strings.Contains(out, "veryl…") {
			t.Fatalf("expected 6-char truncation 'veryl…' in output, got %q", out)
		}
	})

	t.Run("nameWidth zero suppresses dot and does not panic", func(t *testing.T) {
		// w=14: nameWidth = max(0, 14-12) = 2; with hasMissing: max(0, 2-2) = 0.
		// dot must be suppressed, name truncated to "", line must not exceed w.
		m := newTestModel()
		m.columns = []types.ColumnInfo{{Name: "alpha", DuckType: "BIGINT"}}
		m.sel = selection.New(nil)
		m.selectedColName = ""
		m.focus = FocusTable
		m.updateFilteredCols()
		m.summaries["alpha"] = &types.ColumnSummary{Loaded: true, MissingCount: 1}

		out := m.viewColumns(14, 6)
		if strings.Contains(out, nullDot) {
			t.Fatalf("expected dot suppressed when nameWidth==0, got %q", out)
		}
		for _, line := range strings.Split(out, "\n") {
			if lipgloss.Width(line) > 14 {
				t.Fatalf("line exceeds width 14: %q", line)
			}
		}
	})
}

func TestRowHasNullAtFallbackPath(t *testing.T) {
	data := [][]string{
		{"NULL", "x"},
		{"y", "z"},
	}

	t.Run("mismatched cache triggers live scan", func(t *testing.T) {
		m := newTestModel()
		m.tableData = data
		m.tableRowHasMissing = nil // length mismatch

		if !m.rowHasMissingAt(0) {
			t.Error("expected rowHasMissingAt(0) to return true via fallback scan")
		}
		if m.rowHasMissingAt(1) {
			t.Error("expected rowHasMissingAt(1) to return false via fallback scan")
		}
		if m.rowHasMissingAt(-1) {
			t.Error("expected rowHasMissingAt(-1) to return false")
		}
		if m.rowHasMissingAt(99) {
			t.Error("expected rowHasMissingAt(99) to return false for out-of-range index")
		}
	})

	t.Run("synced cache is consulted directly", func(t *testing.T) {
		m := newTestModel()
		m.tableData = data
		m.tableRowHasMissing = rowHasMissingFlags(m.tableData)

		if !m.rowHasMissingAt(0) {
			t.Error("expected rowHasMissingAt(0) to return true from cache")
		}
		if m.rowHasMissingAt(1) {
			t.Error("expected rowHasMissingAt(1) to return false from cache")
		}
		// Out-of-range with synced cache: no live scan, just false.
		if m.rowHasMissingAt(99) {
			t.Error("expected rowHasMissingAt(99) to return false for out-of-range with synced cache")
		}
	})
}

func TestViewTableDoesNotOverflowWidthWithRowPrefix(t *testing.T) {
	m := newTestModel()
	m.tableCols = []string{"c0", "c1", "c2", "c3"}
	m.selectedColName = "c0"
	m.tableData = [][]string{
		{"v0", "v1", "v2", "v3"},
	}

	w := 34
	out := m.viewTable(w, 4)
	for _, line := range strings.Split(out, "\n") {
		if got := lipgloss.Width(line); got > w {
			t.Fatalf("expected line width <= %d, got %d: %q", w, got, line)
		}
	}
}

func TestViewTableFooterStaysSingleLineWithLongColumnType(t *testing.T) {
	m := newTestModel()
	m.tableCols = []string{"nested"}
	m.selectedColName = "nested"
	m.columns = []types.ColumnInfo{
		{Name: "nested", DuckType: "STRUCT(a STRUCT(b STRUCT(c STRUCT(d VARCHAR))), e LIST<STRUCT(f BIGINT, g DOUBLE)>)"},
	}
	m.tableData = [][]string{{"x"}}
	m.summaries["nested"] = &types.ColumnSummary{Loaded: true, MissingCount: 0, MissingPct: 0}

	w := 50
	out := m.viewTable(w, 4)
	lines := strings.Split(out, "\n")
	footer := lines[len(lines)-1]
	if strings.Contains(footer, "\n") {
		t.Fatalf("expected single-line footer, got %q", footer)
	}
	if got := lipgloss.Width(footer); got > w {
		t.Fatalf("expected footer width <= %d, got %d: %q", w, got, footer)
	}
}

func TestViewTableFooterClarifiesProjectedMissingCounts(t *testing.T) {
	m := newTestModel()
	m.tableCols = []string{"a"}
	m.tableData = [][]string{{"x"}}

	footer := m.viewTableFooter()
	if !strings.Contains(footer, "missing (projected)") {
		t.Fatalf("expected footer to clarify projected-column missing count, got %q", footer)
	}
}

func TestViewTableTinyViewportDoesNotOverflowHeight(t *testing.T) {
	m := newTestModel()
	m.tableCols = []string{"a", "b"}
	m.tableData = [][]string{{"1", "2"}, {"3", "4"}}
	m.selectedColName = "a"

	for _, h := range []int{1, 2} {
		out := m.viewTable(40, h)
		lines := strings.Split(out, "\n")
		if len(lines) > h {
			t.Fatalf("expected at most %d lines for tiny viewport, got %d: %q", h, len(lines), out)
		}
	}
}

func TestHandleColumnsPagingAdvancesByRenderedListHeight(t *testing.T) {
	m := newTestModel()
	m.width = 120
	m.height = 18
	m.columns = make([]types.ColumnInfo, 20)
	for i := range m.columns {
		m.columns[i] = types.ColumnInfo{Name: fmt.Sprintf("c%02d", i), DuckType: "VARCHAR"}
	}
	m.sel = selection.New(nil)
	m.updateFilteredCols()
	m.colCursor = 0
	m.syncSelectedColFromCursor()

	paneW, paneH := m.columnsPaneDimensions()
	out := m.viewColumns(paneW, paneH)
	lines := strings.Split(out, "\n")
	renderedListHeight := len(lines) - 2
	if renderedListHeight < 0 {
		renderedListHeight = 0
	}

	updated, _ := m.handleColumnsPaging()
	m = updated.(Model)

	if m.colCursor != renderedListHeight {
		t.Fatalf("expected cursor to advance by rendered list height %d, got %d", renderedListHeight, m.colCursor)
	}
	wantOff := min(renderedListHeight, max(0, len(m.filteredCols)-renderedListHeight))
	if m.colListOff != wantOff {
		t.Fatalf("expected list offset to advance to %d, got %d", wantOff, m.colListOff)
	}
}

func TestHandleColumnsKeyCtrlPagingAndBounds(t *testing.T) {
	m := newTestModel()
	m.width = 120
	m.height = 18
	m.focus = FocusColumns
	m.columns = make([]types.ColumnInfo, 20)
	for i := range m.columns {
		m.columns[i] = types.ColumnInfo{Name: fmt.Sprintf("c%02d", i), DuckType: "VARCHAR"}
	}
	m.sel = selection.New(nil)
	m.updateFilteredCols()
	m.colCursor = 0
	m.colListOff = 0
	m.syncSelectedColFromCursor()

	listHeight := m.currentColumnsListHeight()

	updated, _ := m.handleColumnsKey("ctrl+f")
	m = updated.(Model)
	if m.colCursor != listHeight {
		t.Fatalf("expected ctrl+f to move cursor to %d, got %d", listHeight, m.colCursor)
	}
	wantOff := min(listHeight, max(0, len(m.filteredCols)-listHeight))
	if m.colListOff != wantOff {
		t.Fatalf("expected ctrl+f to move list offset to %d, got %d", wantOff, m.colListOff)
	}

	updated, _ = m.handleColumnsKey("ctrl+b")
	m = updated.(Model)
	if m.colCursor != 0 {
		t.Fatalf("expected ctrl+b to return cursor to top, got %d", m.colCursor)
	}
	if m.colListOff != 0 {
		t.Fatalf("expected ctrl+b to return list offset to top, got %d", m.colListOff)
	}

	// Test end-boundary clamping: two ctrl+f from top should clamp to last row.
	updated, _ = m.handleColumnsKey("ctrl+f")
	m = updated.(Model)
	updated, _ = m.handleColumnsKey("ctrl+f")
	m = updated.(Model)
	wantEndCursor := len(m.filteredCols) - 1
	wantEndOff := max(0, len(m.filteredCols)-listHeight)
	if m.colCursor != wantEndCursor {
		t.Fatalf("expected second ctrl+f to clamp cursor to %d, got %d", wantEndCursor, m.colCursor)
	}
	if m.colListOff != wantEndOff {
		t.Fatalf("expected second ctrl+f to clamp offset to %d, got %d", wantEndOff, m.colListOff)
	}
}

func TestHandleColumnsKeyHalfPagingAndBounds(t *testing.T) {
	m := newTestModel()
	m.width = 120
	m.height = 18
	m.focus = FocusColumns
	m.columns = make([]types.ColumnInfo, 30)
	for i := range m.columns {
		m.columns[i] = types.ColumnInfo{Name: fmt.Sprintf("c%02d", i), DuckType: "VARCHAR"}
	}
	m.sel = selection.New(nil)
	m.updateFilteredCols()
	m.colCursor = 0
	m.colListOff = 0
	m.syncSelectedColFromCursor()

	half := m.currentColumnsListHeight() / 2
	if half < 1 {
		half = 1
	}

	updated, _ := m.handleColumnsKey("ctrl+d")
	m = updated.(Model)
	if m.colCursor != half {
		t.Fatalf("expected ctrl+d to move cursor to %d, got %d", half, m.colCursor)
	}
	wantOff := min(half, max(0, len(m.filteredCols)-m.currentColumnsListHeight()))
	if m.colListOff != wantOff {
		t.Fatalf("expected ctrl+d to move list offset to %d, got %d", wantOff, m.colListOff)
	}

	updated, _ = m.handleColumnsKey("ctrl+u")
	m = updated.(Model)
	if m.colCursor != 0 {
		t.Fatalf("expected ctrl+u to return cursor to top, got %d", m.colCursor)
	}
	if m.colListOff != 0 {
		t.Fatalf("expected ctrl+u to return list offset to top, got %d", m.colListOff)
	}

	// Test end-boundary clamping: place cursor near end and ctrl+d should clamp.
	listHeight := m.currentColumnsListHeight()
	m.colCursor = len(m.filteredCols) - half - 1
	m.colListOff = max(0, m.colCursor-listHeight+1)
	updated, _ = m.handleColumnsKey("ctrl+d")
	m = updated.(Model)
	wantEndCursor := len(m.filteredCols) - 1
	wantEndOff := max(0, len(m.filteredCols)-listHeight)
	if m.colCursor != wantEndCursor {
		t.Fatalf("expected ctrl+d at end to clamp cursor to %d, got %d", wantEndCursor, m.colCursor)
	}
	if m.colListOff != wantEndOff {
		t.Fatalf("expected ctrl+d at end to clamp offset to %d, got %d", wantEndOff, m.colListOff)
	}
}

func TestHandleColumnsKeyGlobalAndViewportJumps(t *testing.T) {
	m := newTestModel()
	m.width = 120
	m.height = 18
	m.focus = FocusColumns
	m.columns = make([]types.ColumnInfo, 30)
	for i := range m.columns {
		m.columns[i] = types.ColumnInfo{Name: fmt.Sprintf("c%02d", i), DuckType: "VARCHAR"}
	}
	m.sel = selection.New(nil)
	m.updateFilteredCols()
	m.colCursor = 10
	m.colListOff = 7
	m.syncSelectedColFromCursor()

	listHeight := m.currentColumnsListHeight()

	updated, _ := m.handleColumnsKey("H")
	m = updated.(Model)
	if m.colListOff != 7 {
		t.Fatalf("expected H not to scroll list, got offset %d", m.colListOff)
	}
	if m.colCursor != 7 {
		t.Fatalf("expected H to jump cursor to visible top 7, got %d", m.colCursor)
	}

	updated, _ = m.handleColumnsKey("M")
	m = updated.(Model)
	wantMid := 7 + (listHeight-1)/2
	if m.colListOff != 7 {
		t.Fatalf("expected M not to scroll list, got offset %d", m.colListOff)
	}
	if m.colCursor != wantMid {
		t.Fatalf("expected M to jump cursor to visible middle %d, got %d", wantMid, m.colCursor)
	}

	updated, _ = m.handleColumnsKey("L")
	m = updated.(Model)
	wantBottom := 7 + listHeight - 1
	if wantBottom >= len(m.filteredCols) {
		wantBottom = len(m.filteredCols) - 1
	}
	if m.colListOff != 7 {
		t.Fatalf("expected L not to scroll list, got offset %d", m.colListOff)
	}
	if m.colCursor != wantBottom {
		t.Fatalf("expected L to jump cursor to visible bottom %d, got %d", wantBottom, m.colCursor)
	}

	updated, _ = m.handleColumnsKey("G")
	m = updated.(Model)
	wantOffBottom := max(0, len(m.filteredCols)-listHeight)
	if m.colCursor != len(m.filteredCols)-1 {
		t.Fatalf("expected G to jump to absolute bottom %d, got %d", len(m.filteredCols)-1, m.colCursor)
	}
	if m.colListOff != wantOffBottom {
		t.Fatalf("expected G to scroll offset to %d, got %d", wantOffBottom, m.colListOff)
	}

	updated, _ = m.handleColumnsKey("g")
	m = updated.(Model)
	if m.colCursor != 0 || m.colListOff != 0 {
		t.Fatalf("expected g to jump to absolute top, got cursor=%d offset=%d", m.colCursor, m.colListOff)
	}
}

func TestHandleColumnsKeyMSmallList(t *testing.T) {
	// When the list has fewer items than the viewport, M should land at the
	// midpoint of the actual content, not the midpoint of the viewport slot.
	m := newTestModel()
	m.width = 120
	m.height = 18
	m.focus = FocusColumns
	m.columns = make([]types.ColumnInfo, 5)
	for i := range m.columns {
		m.columns[i] = types.ColumnInfo{Name: fmt.Sprintf("c%02d", i), DuckType: "VARCHAR"}
	}
	m.sel = selection.New(nil)
	m.updateFilteredCols()
	m.colCursor = 0
	m.colListOff = 0
	m.syncSelectedColFromCursor()

	updated, _ := m.handleColumnsKey("M")
	m = updated.(Model)
	// visibleCount = min(listHeight, 5-0) = 5; midpoint = (5-1)/2 = 2
	wantMid := 2
	if m.colCursor != wantMid {
		t.Fatalf("expected M with small list to land at index %d, got %d", wantMid, m.colCursor)
	}

	// L should land at last item (index 4); M must not equal L
	updated, _ = m.handleColumnsKey("L")
	mL := updated.(Model)
	if mL.colCursor == m.colCursor {
		t.Fatalf("expected M (%d) and L (%d) to land on different rows for small list", m.colCursor, mL.colCursor)
	}
}

func TestHandleColumnsKeyHLNoOp(t *testing.T) {
	m := newTestModel()
	m.width = 120
	m.height = 18
	m.focus = FocusColumns
	m.columns = []types.ColumnInfo{{Name: "a"}, {Name: "b"}, {Name: "c"}}
	m.sel = selection.New(nil)
	m.updateFilteredCols()
	m.colCursor = 1
	m.syncSelectedColFromCursor()

	updated, _ := m.handleColumnsKey("h")
	m = updated.(Model)
	if m.colCursor != 1 {
		t.Fatalf("expected h to be no-op in columns pane, got cursor=%d", m.colCursor)
	}

	updated, _ = m.handleColumnsKey("l")
	m = updated.(Model)
	if m.colCursor != 1 {
		t.Fatalf("expected l to be no-op in columns pane, got cursor=%d", m.colCursor)
	}
}

func TestViewColumnsHighlightedRowUsesFullWidthHighlightFocusedAndUnfocused(t *testing.T) {
	base := newTestModel()
	base.columns = []types.ColumnInfo{
		{Name: "alpha", DuckType: "BIGINT"},
		{Name: "beta", DuckType: "VARCHAR"},
	}
	base.sel = selection.New([]string{"alpha", "beta"})
	base.selectedColName = "alpha"
	base.updateFilteredCols()

	w, h := 30, 8
	plain := fmt.Sprintf("%s %s %s%s",
		unselectedMarkGlyph,
		truncate("alpha", w-12),
		truncate("BIGINT", 8),
		"",
	)

	cases := []struct {
		name  string
		focus Focus
		style lipgloss.Style
	}{
		{name: "focused columns", focus: FocusColumns, style: highlightStyle},
		{name: "unfocused columns pane", focus: FocusTable, style: dimHighlightStyle},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := base
			m.focus = tc.focus

			out := m.viewColumns(w, h)
			lines := strings.Split(out, "\n")
			if len(lines) < 3 {
				t.Fatalf("expected at least 3 lines, got %d", len(lines))
			}

			want := tc.style.Width(w).Render(plain)
			if lines[2] != want {
				t.Fatalf("expected highlighted row render %q, got %q", want, lines[2])
			}
			if got := lipgloss.Width(lines[2]); got != w {
				t.Fatalf("expected highlighted row width %d, got %d", w, got)
			}
		})
	}
}

func TestViewColumnsHighlightedRowMarkMatchesSelectionState(t *testing.T) {
	m := newTestModel()
	m.columns = []types.ColumnInfo{{Name: "alpha", DuckType: "BIGINT"}}
	m.sel = selection.New([]string{"alpha"})
	m.selectedColName = "alpha"
	m.focus = FocusColumns
	m.updateFilteredCols()

	w, h := 30, 6
	baseName := truncate("alpha", w-12)
	baseType := truncate("BIGINT", 8)

	out := m.viewColumns(w, h)
	lines := strings.Split(out, "\n")
	wantUnselected := highlightStyle.Width(w).Render(fmt.Sprintf("%s %s %s%s", unselectedMarkGlyph, baseName, baseType, ""))
	if lines[2] != wantUnselected {
		t.Fatalf("expected unselected highlighted mark render %q, got %q", wantUnselected, lines[2])
	}

	m.sel.Add("alpha")
	out = m.viewColumns(w, h)
	lines = strings.Split(out, "\n")
	wantSelected := highlightStyle.Width(w).Render(fmt.Sprintf("%s %s %s%s", selectedMarkGlyph, baseName, baseType, ""))
	if lines[2] != wantSelected {
		t.Fatalf("expected selected highlighted mark render %q, got %q", wantSelected, lines[2])
	}
}

func TestMaxTableOffsetUsesVisibleRowsNotPageSize(t *testing.T) {
	m := newTestModel()
	m.width = 120
	m.height = 40
	m.pageSize = 10
	m.totalRows = 200

	want := int(m.totalRows) - m.visibleTableRows()
	if want < 0 {
		want = 0
	}
	if got := m.maxTableOffset(); got != want {
		t.Fatalf("expected maxTableOffset=%d from visible rows, got %d", want, got)
	}
}

func TestWindowSizeMsgReloadsWhenViewportGrowsBeyondLoadedRows(t *testing.T) {
	m := newCmdTestModel()
	m.width = 120
	m.height = 10
	m.pageSize = 50
	m.totalRows = 500
	m.tableCols = []string{"c0"}
	m.tableData = make([][]string, 50)
	for i := range m.tableData {
		m.tableData[i] = []string{"v"}
	}

	updated, cmd := m.Update(tea.WindowSizeMsg{Width: 120, Height: 80})
	if cmd == nil {
		t.Fatal("expected load command when resize increases viewport row demand")
	}
	m = updated.(Model)
	if m.tableOffset != 0 {
		t.Fatalf("expected tableOffset to remain unchanged, got %d", m.tableOffset)
	}
}

func TestPreviewDoneMsgIgnoresStaleSequence(t *testing.T) {
	m := newTestModel()
	m.latestPreviewSeq = 5
	m.tableCols = []string{"new_col"}
	m.tableData = [][]string{{"new"}}
	m.tableRowHasMissing = []bool{false}
	m.totalRows = 10

	stale := previewDoneMsg{
		seq:       4,
		rows:      [][]string{{"stale"}},
		colNames:  []string{"stale_col"},
		totalRows: 99,
	}
	updated, _ := m.Update(stale)
	m = updated.(Model)

	if got := m.tableCols[0]; got != "new_col" {
		t.Fatalf("expected stale preview to be ignored, got tableCols=%v", m.tableCols)
	}
	if got := m.tableData[0][0]; got != "new" {
		t.Fatalf("expected stale preview rows to be ignored, got %q", got)
	}
	if got := m.totalRows; got != 10 {
		t.Fatalf("expected stale preview totalRows to be ignored, got %d", got)
	}
}

func TestMouseDividerDragUpdatesSplitWithinBounds(t *testing.T) {
	m := newTestModel()
	m.width = 100
	m.height = 30
	m.ready = true
	m.tableSplitPct = tableSplitPct

	divider := m.dividerX()
	updated, _ := m.Update(tea.MouseMsg{
		X:      divider,
		Y:      1,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonLeft,
	})
	m = updated.(Model)
	if !m.draggingDivider {
		t.Fatal("expected divider drag to begin on near-divider press")
	}

	updated, _ = m.Update(tea.MouseMsg{
		X:      98,
		Y:      1,
		Action: tea.MouseActionMotion,
		Button: tea.MouseButtonLeft,
	})
	m = updated.(Model)
	_, maxPct := m.splitPctBounds()
	if m.tableSplitPct > maxPct {
		t.Fatalf("expected split clamped to max %d, got %d", maxPct, m.tableSplitPct)
	}

	updated, _ = m.Update(tea.MouseMsg{
		X:      1,
		Y:      1,
		Action: tea.MouseActionMotion,
		Button: tea.MouseButtonLeft,
	})
	m = updated.(Model)
	minPct, _ := m.splitPctBounds()
	if m.tableSplitPct < minPct {
		t.Fatalf("expected split clamped to min %d, got %d", minPct, m.tableSplitPct)
	}
}

func TestMouseDividerDragStartsOnlyNearDividerInMainArea(t *testing.T) {
	m := newTestModel()
	m.width = 100
	m.height = 30
	m.ready = true
	m.tableSplitPct = tableSplitPct

	updated, _ := m.Update(tea.MouseMsg{
		X:      2,
		Y:      1,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonLeft,
	})
	m = updated.(Model)
	if m.draggingDivider {
		t.Fatal("expected no drag when clicking far from divider")
	}

	updated, _ = m.Update(tea.MouseMsg{
		X:      m.dividerX(),
		Y:      0, // top bar, outside main area
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonLeft,
	})
	m = updated.(Model)
	if m.draggingDivider {
		t.Fatal("expected no drag when clicking divider outside main area")
	}
}

func TestMouseDividerDragReleaseStopsDragging(t *testing.T) {
	m := newTestModel()
	m.width = 100
	m.height = 30
	m.ready = true
	m.tableSplitPct = tableSplitPct
	m.draggingDivider = true

	updated, _ := m.Update(tea.MouseMsg{
		X:      m.dividerX(),
		Y:      1,
		Action: tea.MouseActionRelease,
		Button: tea.MouseButtonLeft,
	})
	m = updated.(Model)
	if m.draggingDivider {
		t.Fatal("expected divider drag to stop on release")
	}
}

func TestMouseWheelRoutesToFocusedTableOnly(t *testing.T) {
	m := newTestModel()
	m.focus = FocusTable
	m.width = 120
	m.height = 10
	m.totalRows = 200
	m.tableData = make([][]string, 50)
	for i := range m.tableData {
		m.tableData[i] = []string{"v"}
	}
	m.columns = []types.ColumnInfo{{Name: "a"}, {Name: "b"}, {Name: "c"}}
	m.updateFilteredCols()
	m.colCursor = 1
	m.tableRowCursor = 0

	updated, _ := m.Update(tea.MouseMsg{
		X:      5,
		Y:      3,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonWheelDown,
	})
	m = updated.(Model)
	if m.tableRowCursor != 1 {
		t.Fatalf("expected table row cursor to move to 1, got %d", m.tableRowCursor)
	}
	if m.colCursor != 1 {
		t.Fatalf("expected column cursor unchanged, got %d", m.colCursor)
	}
}

func TestMouseWheelRoutesToFocusedColumnsOnly(t *testing.T) {
	m := newTestModel()
	m.focus = FocusColumns
	m.width = 120
	m.height = 10
	m.totalRows = 200
	m.tableData = make([][]string, 50)
	for i := range m.tableData {
		m.tableData[i] = []string{"v"}
	}
	m.tableRowCursor = 2
	m.columns = []types.ColumnInfo{
		{Name: "a"},
		{Name: "b"},
		{Name: "c"},
	}
	m.updateFilteredCols()
	m.colCursor = 1
	m.syncSelectedColFromCursor()

	updated, _ := m.Update(tea.MouseMsg{
		X:      90,
		Y:      4,
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonWheelDown,
	})
	m = updated.(Model)
	if m.colCursor != 2 {
		t.Fatalf("expected column cursor to move to 2, got %d", m.colCursor)
	}
	if m.tableRowCursor != 2 {
		t.Fatalf("expected table row cursor unchanged, got %d", m.tableRowCursor)
	}
}

func TestMouseWheelColumnsUpdatesColListOff(t *testing.T) {
	m := newTestModel()
	m.focus = FocusColumns
	m.width = 120
	m.height = 18
	m.columns = make([]types.ColumnInfo, 30)
	for i := range m.columns {
		m.columns[i] = types.ColumnInfo{Name: fmt.Sprintf("c%02d", i), DuckType: "VARCHAR"}
	}
	m.sel = selection.New(nil)
	m.updateFilteredCols()

	// Place cursor at last visible row so the next scroll down must advance colListOff.
	listHeight := m.currentColumnsListHeight()
	m.colCursor = listHeight - 1
	m.colListOff = 0
	m.syncSelectedColFromCursor()

	updated, _ := m.Update(tea.MouseMsg{
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonWheelDown,
	})
	m = updated.(Model)
	if m.colCursor != listHeight {
		t.Fatalf("expected mouse wheel down to advance cursor to %d, got %d", listHeight, m.colCursor)
	}
	if m.colListOff != 1 {
		t.Fatalf("expected mouse wheel down to advance colListOff to 1, got %d", m.colListOff)
	}

	// Wheel up from top should clamp at 0.
	m.colCursor = 0
	m.colListOff = 0
	m.syncSelectedColFromCursor()
	updated, _ = m.Update(tea.MouseMsg{
		Action: tea.MouseActionPress,
		Button: tea.MouseButtonWheelUp,
	})
	m = updated.(Model)
	if m.colCursor != 0 || m.colListOff != 0 {
		t.Fatalf("expected mouse wheel up at top to be a no-op, got cursor=%d off=%d", m.colCursor, m.colListOff)
	}
}

func TestViewColumnsLongContentClampedToPaneWidth(t *testing.T) {
	m := newTestModel()
	m.columns = []types.ColumnInfo{
		{Name: strings.Repeat("very_long_col_name_", 8), DuckType: strings.Repeat("SUPERLONGTYPE", 3)},
	}
	m.sel = selection.New(nil)
	m.selectedColName = m.columns[0].Name
	m.focus = FocusColumns
	m.updateFilteredCols()
	m.summaries[m.columns[0].Name] = &types.ColumnSummary{
		Loaded:      true,
		MissingPct:  33,
		DistinctPct: 77,
	}

	w, h := 20, 6
	out := m.viewColumns(w, h)
	for _, line := range strings.Split(out, "\n") {
		if got := lipgloss.Width(line); got > w {
			t.Fatalf("expected column line width <= %d, got %d: %q", w, got, line)
		}
	}
}

func TestHelpAndBottomBarIncludeMouseDividerAndCtrlL(t *testing.T) {
	m := newTestModel()
	m.width = 200
	m.height = 30
	m.ready = true
	m.sel = selection.New(nil)

	help := m.viewHelp()
	if !strings.Contains(help, "Ctrl+L") {
		t.Fatalf("expected help to include Ctrl+L, got %q", help)
	}
	if !strings.Contains(help, "Mouse drag divider") {
		t.Fatalf("expected help to include mouse divider drag, got %q", help)
	}
	if !strings.Contains(help, "H / M / L") {
		t.Fatalf("expected help to include H/M/L bindings, got %q", help)
	}
	if !strings.Contains(help, "v / V") {
		t.Fatalf("expected help to include v/V binding, got %q", help)
	}

	m.focus = FocusTable
	bottom := m.viewBottomBar()
	if !strings.Contains(bottom, "drag:divider") {
		t.Fatalf("expected bottom bar to include divider hint, got %q", bottom)
	}
	if !strings.Contains(bottom, "Ctrl+L:redraw") {
		t.Fatalf("expected bottom bar to include ctrl+l hint, got %q", bottom)
	}

	m.focus = FocusColumns
	bottom = m.viewBottomBar()
	if !strings.Contains(bottom, "HML:jump") {
		t.Fatalf("expected columns bottom bar to include HML hint, got %q", bottom)
	}
	if !strings.Contains(bottom, "C-d/u:half") {
		t.Fatalf("expected columns bottom bar to include ctrl+d/u hint, got %q", bottom)
	}
	if !strings.Contains(bottom, "a/d/y:sel") {
		t.Fatalf("expected columns bottom bar to include a/d/y:sel hint, got %q", bottom)
	}
	if !strings.Contains(bottom, "v:sel-list") {
		t.Fatalf("expected columns bottom bar to include v:sel-list hint, got %q", bottom)
	}
}

func TestNewModelWithoutFileStartsEmpty(t *testing.T) {
	root := t.TempDir()
	m := NewModel(nil, "", root)

	if m.engine != nil {
		t.Fatal("expected no engine when starting without a file")
	}
	if m.fileName != "" {
		t.Fatalf("expected empty file name, got %q", m.fileName)
	}
	if len(m.columns) != 0 {
		t.Fatalf("expected no columns, got %d", len(m.columns))
	}
	if m.statusMsg == "" || !strings.Contains(m.statusMsg, "Ctrl+O") {
		t.Fatalf("expected startup status to mention Ctrl+O, got %q", m.statusMsg)
	}
}

func TestFilePickerListsOnlyDirectoriesAndSupportedFiles(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "nested"), 0o755); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}
	for _, name := range []string{"a.parquet", "b.csv", "c.txt"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("x"), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	m := NewModel(nil, "", root)
	m.openFilePicker()

	if m.overlay != OverlayFilePicker {
		t.Fatalf("expected file picker overlay, got %v", m.overlay)
	}

	var got []string
	for _, item := range m.pickerItems {
		got = append(got, item.name)
	}

	for _, want := range []string{"nested", "a.parquet", "b.csv"} {
		if !slices.Contains(got, want) {
			t.Fatalf("expected picker to include %q, got %v", want, got)
		}
	}
	if slices.Contains(got, "c.txt") {
		t.Fatalf("expected picker to exclude unsupported file c.txt, got %v", got)
	}
}

func TestFilePickerSupportsNavigationAndFuzzyFilter(t *testing.T) {
	root := t.TempDir()
	nested := filepath.Join(root, "nested")
	if err := os.Mkdir(nested, 0o755); err != nil {
		t.Fatalf("mkdir nested: %v", err)
	}
	for _, path := range []string{
		filepath.Join(root, "alpha_data.parquet"),
		filepath.Join(nested, "inside.csv"),
	} {
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}

	m := NewModel(nil, "", root)
	m.openFilePicker()

	m.pickerQuery = "adpq"
	m.refreshPickerItems()
	if len(m.pickerItems) == 0 || m.pickerItems[0].name != "alpha_data.parquet" {
		t.Fatalf("expected fuzzy query to match alpha_data.parquet first, got %+v", m.pickerItems)
	}

	m.pickerQuery = ""
	m.refreshPickerItems()
	for i, item := range m.pickerItems {
		if item.isDir && item.name == "nested" {
			m.pickerCursor = i
		}
	}

	updated, cmd := m.handleFilePickerKey(tea.KeyMsg{Type: tea.KeyEnter})
	m = updated.(Model)
	if cmd != nil {
		t.Fatal("expected no file-open command when entering a directory")
	}
	if m.pickerDir != nested {
		t.Fatalf("expected picker to navigate to %q, got %q", nested, m.pickerDir)
	}

	foundFile := false
	for i, item := range m.pickerItems {
		if !item.isDir && item.name == "inside.csv" {
			m.pickerCursor = i
			foundFile = true
			break
		}
	}
	if !foundFile {
		t.Fatalf("expected nested picker entries to include inside.csv, got %+v", m.pickerItems)
	}

	updated, cmd = m.handleFilePickerKey(tea.KeyMsg{Type: tea.KeyEnter})
	m = updated.(Model)
	if cmd == nil {
		t.Fatal("expected file-open command when selecting a file")
	}
	if m.overlay != OverlayNone {
		t.Fatalf("expected picker overlay to close after file selection, got %v", m.overlay)
	}
}

func TestFilePickerPathInputExpandsTilde(t *testing.T) {
	root := t.TempDir()
	home := t.TempDir()
	t.Setenv("HOME", home)

	target := filepath.Join(home, "from_go_home.csv")
	if err := os.WriteFile(target, []byte("x"), 0o644); err != nil {
		t.Fatalf("write target: %v", err)
	}

	m := NewModel(nil, "", root)
	m.openFilePicker()
	m.pickerInput.SetValue("~/from_go_home.csv")
	m.pickerQuery = "~/from_go_home.csv"

	updated, cmd := m.handleFilePickerKey(tea.KeyMsg{Type: tea.KeyEnter})
	m = updated.(Model)
	if cmd == nil {
		t.Fatal("expected file-open command for ~/ path")
	}
	if m.overlay != OverlayNone {
		t.Fatalf("expected overlay to close after opening by path input, got %v", m.overlay)
	}
}

func TestExpandTildePathSeparatorVariants(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("os.UserHomeDir: %v", err)
	}

	type tcase struct {
		name string
		in   string
		want string
	}

	cases := []tcase{
		{
			name: "double forward slash",
			in:   "~//foo",
			// Cross-platform: after trimming one separator, filepath.Join normalizes "/foo" as a child of home.
			want: filepath.Join(home, "foo"),
		},
	}

	if filepath.Separator == '/' {
		// On Unix, '\' is not a path separator, so these inputs are not tilde-expanded.
		cases = append(cases,
			tcase{
				name: "windows separator on unix",
				in:   "~\\foo",
				want: "~\\foo",
			},
			tcase{
				name: "mixed separators on unix",
				in:   "~\\/foo",
				want: "~\\/foo",
			},
		)
	} else {
		cases = append(cases,
			tcase{
				name: "double native separator",
				in:   "~\\\\foo",
				want: filepath.Join(home, "foo"),
			},
			tcase{
				name: "mixed separators on windows",
				in:   "~\\/foo",
				want: filepath.Join(home, "foo"),
			},
		)
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := expandTildePath(tc.in)
			if err != nil {
				t.Fatalf("expandTildePath(%q) error: %v", tc.in, err)
			}
			if got != tc.want {
				t.Fatalf("expandTildePath(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestFilePickerAllowsTypingQueryText(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "alpha.csv"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write alpha: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "gamma.csv"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write gamma: %v", err)
	}

	m := NewModel(nil, "", root)
	m.openFilePicker()

	for _, r := range []rune{'g', 'q'} {
		updated, _ := m.handleFilePickerKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		m = updated.(Model)
	}
	if got := m.pickerInput.Value(); got != "gq" {
		t.Fatalf("expected picker input to accept typed runes, got %q", got)
	}
}

func TestHandleKeyCtrlOOpensFilePickerAndReturnsInputInitCmd(t *testing.T) {
	m := NewModel(nil, "", t.TempDir())
	m.overlay = OverlayNone

	updated, cmd := m.handleKey(tea.KeyMsg{Type: tea.KeyCtrlO})
	m = updated.(Model)

	if m.overlay != OverlayFilePicker {
		t.Fatalf("expected overlay %v after ctrl+o, got %v", OverlayFilePicker, m.overlay)
	}
	if cmd == nil {
		t.Fatal("expected non-nil picker input init command after ctrl+o")
	}
}

func TestFilePickerCtrlCQuits(t *testing.T) {
	m := NewModel(nil, "", t.TempDir())
	m.openFilePicker()

	_, cmd := m.handleKey(tea.KeyMsg{Type: tea.KeyCtrlC})
	if cmd == nil {
		t.Fatal("expected quit command for ctrl+c in file picker")
	}
	if _, ok := cmd().(tea.QuitMsg); !ok {
		t.Fatalf("expected tea.QuitMsg, got %T", cmd())
	}
}

func TestOpenFileDoneIgnoresStaleRequest(t *testing.T) {
	m := NewModel(nil, "", t.TempDir())
	m.openReqID = 2

	currentPath := filepath.Join(t.TempDir(), "new.csv")
	if err := os.WriteFile(currentPath, []byte("a\n1\n"), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	currentEngine, err := engine.New(currentPath)
	if err != nil {
		t.Fatalf("engine.New(%q): %v", currentPath, err)
	}
	t.Cleanup(func() { _ = currentEngine.Close() })

	updated, _ := m.Update(openFileDoneMsg{
		path:  currentPath,
		eng:   currentEngine,
		reqID: 2,
	})
	m = updated.(Model)

	if m.engine != currentEngine {
		t.Fatal("expected current request engine to be applied")
	}
	if m.fileName != "new.csv" {
		t.Fatalf("expected current request file name to be applied, got %q", m.fileName)
	}
	if got := m.statusMsg; got != "Opened new.csv" {
		t.Fatalf("expected open status for current request, got %q", got)
	}

	updated, _ = m.Update(openFileDoneMsg{
		path:  "/tmp/old.csv",
		reqID: 1,
	})
	m = updated.(Model)

	if m.engine != currentEngine {
		t.Fatal("expected stale request to be ignored")
	}
	if m.fileName != "new.csv" {
		t.Fatalf("expected stale request to keep file name new.csv, got %q", m.fileName)
	}
	if got := m.statusMsg; got != "Opened new.csv" {
		t.Fatalf("expected stale request to keep status, got %q", got)
	}
}

func TestOpenFileDoneCurrentRequestErrorPreservesLoadedFile(t *testing.T) {
	m := NewModel(nil, "", t.TempDir())
	m.openReqID = 3

	currentPath := filepath.Join(t.TempDir(), "new.csv")
	if err := os.WriteFile(currentPath, []byte("a\n1\n"), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	currentEngine, err := engine.New(currentPath)
	if err != nil {
		t.Fatalf("engine.New(%q): %v", currentPath, err)
	}
	t.Cleanup(func() { _ = currentEngine.Close() })

	updated, _ := m.Update(openFileDoneMsg{
		path:  currentPath,
		eng:   currentEngine,
		reqID: 3,
	})
	m = updated.(Model)
	// openReqID stays at 3 after a successful open; only user-initiated opens advance it.

	openErr := fmt.Errorf("boom")
	updated, _ = m.Update(openFileDoneMsg{
		path:  filepath.Join(t.TempDir(), "missing.csv"),
		reqID: 3,
		err:   openErr,
	})
	m = updated.(Model)

	if m.engine != currentEngine {
		t.Fatal("expected current engine to remain loaded after open error")
	}
	if m.fileName != "new.csv" {
		t.Fatalf("expected file name to remain new.csv after open error, got %q", m.fileName)
	}
	if got := m.statusMsg; got != "Error opening file: boom" {
		t.Fatalf("expected error status for current request, got %q", got)
	}
}

func TestFilePickerPathQueryShowsHomeAutocomplete(t *testing.T) {
	root := t.TempDir()
	home := t.TempDir()
	t.Setenv("HOME", home)

	if err := os.Mkdir(filepath.Join(home, "datasets"), 0o755); err != nil {
		t.Fatalf("mkdir datasets: %v", err)
	}
	if err := os.WriteFile(filepath.Join(home, "from_home.csv"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write from_home.csv: %v", err)
	}
	if err := os.WriteFile(filepath.Join(home, "ignored.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write ignored.txt: %v", err)
	}

	m := NewModel(nil, "", root)
	m.openFilePicker()
	m.pickerInput.SetValue("~/")
	m.pickerQuery = "~/"
	m.refreshPickerItems()

	var got []string
	for _, item := range m.pickerItems {
		got = append(got, item.name)
	}
	if !slices.Contains(got, "datasets") {
		t.Fatalf("expected ~/ autocomplete to include datasets directory, got %v", got)
	}
	if !slices.Contains(got, "from_home.csv") {
		t.Fatalf("expected ~/ autocomplete to include csv file, got %v", got)
	}
	if slices.Contains(got, "ignored.txt") {
		t.Fatalf("expected ~/ autocomplete to exclude unsupported files, got %v", got)
	}
}

func TestFilePickerBackspaceCanMoveAboveLaunchDir(t *testing.T) {
	outer := t.TempDir()
	launchDir := filepath.Join(outer, "project", "sub")
	if err := os.MkdirAll(launchDir, 0o755); err != nil {
		t.Fatalf("mkdir launchDir: %v", err)
	}

	m := NewModel(nil, "", launchDir)
	m.openFilePicker()
	if m.pickerDir != launchDir {
		t.Fatalf("expected initial picker dir %q, got %q", launchDir, m.pickerDir)
	}

	updated, _ := m.handleFilePickerKey(tea.KeyMsg{Type: tea.KeyBackspace})
	m = updated.(Model)
	if m.pickerDir != filepath.Dir(launchDir) {
		t.Fatalf("expected picker dir to move to parent %q, got %q", filepath.Dir(launchDir), m.pickerDir)
	}
}
