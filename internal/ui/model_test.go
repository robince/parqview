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

	for _, key := range []string{"left", "h", "right", "l"} {
		updated, cmd := m.handleTableKey(key)
		if cmd != nil {
			t.Errorf("key %q: expected no command, got %v", key, cmd)
		}
		um := updated.(Model)
		if um.selectedColName != m.selectedColName {
			t.Errorf("key %q: expected selectedColName %q, got %q", key, m.selectedColName, um.selectedColName)
		}
		if um.tableColOffHint != m.tableColOffHint {
			t.Errorf("key %q: expected tableColOffHint %d, got %d", key, m.tableColOffHint, um.tableColOffHint)
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
	m.tableRowHasNull = rowHasNullFlags(m.tableData)
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

func TestRowHasNullAtFallbackPath(t *testing.T) {
	data := [][]string{
		{"NULL", "x"},
		{"y", "z"},
	}

	t.Run("mismatched cache triggers live scan", func(t *testing.T) {
		m := newTestModel()
		m.tableData = data
		m.tableRowHasNull = nil // length mismatch

		if !m.rowHasNullAt(0) {
			t.Error("expected rowHasNullAt(0) to return true via fallback scan")
		}
		if m.rowHasNullAt(1) {
			t.Error("expected rowHasNullAt(1) to return false via fallback scan")
		}
		if m.rowHasNullAt(-1) {
			t.Error("expected rowHasNullAt(-1) to return false")
		}
		if m.rowHasNullAt(99) {
			t.Error("expected rowHasNullAt(99) to return false for out-of-range index")
		}
	})

	t.Run("synced cache is consulted directly", func(t *testing.T) {
		m := newTestModel()
		m.tableData = data
		m.tableRowHasNull = rowHasNullFlags(m.tableData)

		if !m.rowHasNullAt(0) {
			t.Error("expected rowHasNullAt(0) to return true from cache")
		}
		if m.rowHasNullAt(1) {
			t.Error("expected rowHasNullAt(1) to return false from cache")
		}
		// Out-of-range with synced cache: no live scan, just false.
		if m.rowHasNullAt(99) {
			t.Error("expected rowHasNullAt(99) to return false for out-of-range with synced cache")
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
	m := newTestModel()
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
	m.tableRowHasNull = []bool{false}
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
}
