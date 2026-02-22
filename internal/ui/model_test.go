package ui

import (
	"fmt"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/robince/parqview/internal/selection"
	"github.com/robince/parqview/internal/types"
)

func TestHandleTableKeyDownKeepsCursorWithinVisibleRowsAndScrolls(t *testing.T) {
	m := newTestModel()
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

func TestWindowSizeMsgClampsOffsetAndKeepsPageDownMonotonic(t *testing.T) {
	m := newTestModel()
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
	base := newTestModel()
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
			m := newTestModel()
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

func TestHandleKeyEnterOpensDetailFromTableAndColumnsFocus(t *testing.T) {
	t.Run("table focus uses selected column", func(t *testing.T) {
		m := newTestModel()
		m.focus = FocusTable
		m.columns = []types.ColumnInfo{{Name: "alpha"}}
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
	})

	t.Run("columns focus uses active column", func(t *testing.T) {
		m := newTestModel()
		m.focus = FocusColumns
		m.columns = []types.ColumnInfo{{Name: "alpha"}, {Name: "beta"}}
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
	})
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

func TestHandleTableKeyGPositionsCursorAtFinalRow(t *testing.T) {
	m := newTestModel()
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
	m := newTestModel()
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
	m.summaries["a"] = &types.ColumnSummary{Loaded: true, MissingCount: 1}
	m.summaries["b"] = &types.ColumnSummary{Loaded: true, MissingCount: 0}

	out := m.viewTable(60, 6)
	lines := strings.Split(out, "\n")
	if len(lines) < 4 {
		t.Fatalf("expected at least 4 lines in table view, got %d", len(lines))
	}

	if strings.Count(lines[0], "•") != 1 {
		t.Fatalf("expected exactly one header null dot, got %q", lines[0])
	}
	if !strings.Contains(lines[1], "•") {
		t.Fatalf("expected null-dot row marker for row containing NULL, got %q", lines[1])
	}
	if strings.Contains(lines[2], "•") {
		t.Fatalf("expected no row marker for row without NULL, got %q", lines[2])
	}
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
