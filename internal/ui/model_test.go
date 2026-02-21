package ui

import (
	"testing"

	"github.com/robince/parqview/internal/types"
)

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
