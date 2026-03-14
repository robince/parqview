package ui

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/robince/parqview/internal/types"
)

func TestParseColumnPredicate(t *testing.T) {
	tests := []struct {
		name    string
		colType string
		input   string
		wantOp  predicateOp
		want    string
		want2   string
		wantErr string
	}{
		{name: "string exact", colType: "VARCHAR", input: "abc123", wantOp: opEq, want: "abc123"},
		{name: "string neq", colType: "VARCHAR", input: "!= abc123", wantOp: opNeq, want: "abc123"},
		{name: "numeric gte", colType: "DOUBLE", input: ">= 10", wantOp: opGte, want: "10"},
		{name: "numeric range", colType: "BIGINT", input: "10..20", wantOp: opRange, want: "10", want2: "20"},
		{name: "string compare rejected", colType: "VARCHAR", input: "> 10", wantErr: "comparisons require a numeric column"},
		{name: "bad numeric rejected", colType: "DOUBLE", input: "abc", wantErr: `invalid numeric value "abc"`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseColumnPredicate("score", tc.colType, tc.input)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Op != tc.wantOp || got.Value != tc.want || got.Value2 != tc.want2 {
				t.Fatalf("unexpected predicate: %+v", got)
			}
		})
	}
}

func TestBuildPredicateFilterSortsAndCombines(t *testing.T) {
	filter := buildPredicateFilter(map[string]columnPredicate{
		"user_id": {Column: "user_id", Op: opEq, Value: "abc", Display: "abc"},
		"score":   {Column: "score", Op: opGt, Value: "10", Display: ">10", Numeric: true},
	})
	if want := `("score" > 10 AND "user_id" = 'abc')`; filter != want {
		t.Fatalf("unexpected filter %q want %q", filter, want)
	}
}

func TestActiveRowFilterCombinesPredicateAndMissing(t *testing.T) {
	m := newTestModel()
	m.predicates["user_id"] = columnPredicate{Column: "user_id", Op: opEq, Value: "abc", Display: "abc"}
	m.missingFilterActive = true
	m.missingFilterCols = []string{"score"}

	got := m.activeRowFilter()
	if !strings.Contains(got, `"user_id" = 'abc'`) {
		t.Fatalf("expected predicate filter in %q", got)
	}
	if !strings.Contains(got, `"score"`) {
		t.Fatalf("expected missing filter in %q", got)
	}
	if !strings.Contains(got, " AND ") {
		t.Fatalf("expected combined filter in %q", got)
	}
}

func TestHandleTableKeyEqualsOpensPredicatePrompt(t *testing.T) {
	m := newTestModel()
	m.selectedColName = "user_id"
	m.tableCols = []string{"user_id"}
	m.columns = []types.ColumnInfo{{Name: "user_id", DuckType: "VARCHAR"}}
	m.tableData = [][]string{{"abc123"}}

	updated, cmd := m.handleTableKey("=")
	m = updated.(Model)
	if cmd == nil {
		t.Fatal("expected focus command when opening predicate prompt")
	}
	if m.overlay != OverlayPredicatePrompt {
		t.Fatalf("expected predicate overlay, got %v", m.overlay)
	}
	if got := m.predicateInput.Value(); got != "abc123" {
		t.Fatalf("expected prompt prefilled with visible cell, got %q", got)
	}
}

func TestHandlePredicatePromptEnterAppliesPredicate(t *testing.T) {
	m := newCmdTestModel()
	m.selectedColName = "score"
	m.columns = []types.ColumnInfo{{Name: "score", DuckType: "DOUBLE"}}
	m.overlay = OverlayPredicatePrompt
	m.predicateCol = "score"
	m.predicateInput.SetValue("> 10")

	updated, cmd := m.handlePredicatePromptKey(tea.KeyMsg{Type: tea.KeyEnter})
	m = updated.(Model)
	if cmd == nil {
		t.Fatal("expected refresh command after applying predicate")
	}
	pred, ok := m.predicates["score"]
	if !ok {
		t.Fatal("expected predicate to be stored")
	}
	if pred.Op != opGt || pred.Value != "10" {
		t.Fatalf("unexpected predicate: %+v", pred)
	}
	if m.overlay != OverlayNone {
		t.Fatalf("expected prompt to close, got %v", m.overlay)
	}
}

func TestHandleTableKeyPCreatesExactMatchPredicate(t *testing.T) {
	m := newCmdTestModel()
	m.selectedColName = "user_id"
	m.tableCols = []string{"user_id"}
	m.columns = []types.ColumnInfo{{Name: "user_id", DuckType: "VARCHAR"}}
	m.tableData = [][]string{{"abc123"}}

	updated, cmd := m.handleTableKey("p")
	m = updated.(Model)
	if cmd == nil {
		t.Fatal("expected refresh command for pin")
	}
	if got := m.predicates["user_id"].Display; got != "abc123" {
		t.Fatalf("unexpected predicate display %q", got)
	}
}

func TestHandleTableKeyDashClearsActivePredicate(t *testing.T) {
	m := newCmdTestModel()
	m.selectedColName = "user_id"
	m.predicates["user_id"] = columnPredicate{Column: "user_id", Op: opEq, Value: "abc", Display: "abc"}

	updated, cmd := m.handleTableKey("-")
	m = updated.(Model)
	if cmd == nil {
		t.Fatal("expected refresh command when clearing predicate")
	}
	if _, ok := m.predicates["user_id"]; ok {
		t.Fatal("expected predicate to be removed")
	}
}

func TestHandleTableKeyUClearsAllPredicates(t *testing.T) {
	m := newCmdTestModel()
	m.predicates["user_id"] = columnPredicate{Column: "user_id", Op: opEq, Value: "abc", Display: "abc"}
	m.predicates["score"] = columnPredicate{Column: "score", Op: opGt, Value: "10", Display: ">10", Numeric: true}

	updated, cmd := m.handleTableKey("U")
	m = updated.(Model)
	if cmd == nil {
		t.Fatal("expected refresh command when clearing all predicates")
	}
	if len(m.predicates) != 0 {
		t.Fatalf("expected all predicates cleared, got %+v", m.predicates)
	}
}
