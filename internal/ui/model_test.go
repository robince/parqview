package ui

import (
	"testing"

	tea "github.com/charmbracelet/bubbletea"

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

	for i := 0; i < 3; i++ {
		updated, cmd := m.handleTableKey("down")
		if cmd != nil {
			t.Fatalf("expected no load command before visible-row boundary, got %v", cmd)
		}
		m = updated.(Model)
	}

	if m.tableRowCursor != 3 {
		t.Fatalf("expected row cursor at last visible row (3), got %d", m.tableRowCursor)
	}

	updated, cmd := m.handleTableKey("down")
	m = updated.(Model)
	if cmd == nil {
		t.Fatal("expected load command when moving past visible rows")
	}
	if m.tableRowCursor != 3 {
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

	if m.tableOffset != 166 {
		t.Fatalf("expected tableOffset to clamp to 166 after resize, got %d", m.tableOffset)
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

	updated, _ := m.handleColumnsKey("x")
	m = updated.(Model)
	if !m.sel.IsSelected("alpha") {
		t.Fatal("expected x to toggle reconciled selected column alpha")
	}
	if m.sel.IsSelected("beta") {
		t.Fatal("expected x not to toggle stale filtered cursor column beta")
	}

	updated, _ = m.handleKey(tea.KeyMsg{Type: tea.KeyEnter})
	m = updated.(Model)
	if m.detailCol != "alpha" {
		t.Fatalf("expected enter to open detail for alpha, got %q", m.detailCol)
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

	if m.tableRowCursor != 3 {
		t.Fatalf("expected row cursor to clamp to last visible row (3), got %d", m.tableRowCursor)
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
