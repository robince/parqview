package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	_ "github.com/marcboeker/go-duckdb"
)

func testdataDir() string {
	// Walk up to find testdata
	dir, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(dir, "testdata", "sample.parquet")); err == nil {
			return filepath.Join(dir, "testdata")
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return ""
}

func openSampleParquet(t *testing.T) *Engine {
	t.Helper()
	td := testdataDir()
	if td == "" {
		t.Skip("testdata not found")
	}
	eng, err := New(filepath.Join(td, "sample.parquet"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return eng
}

func TestOpenParquet(t *testing.T) {
	eng := openSampleParquet(t)
	defer eng.Close()

	cols := eng.Columns()
	if len(cols) == 0 {
		t.Fatal("no columns")
	}
	t.Logf("Columns: %d, Rows: %d", len(cols), eng.TotalRows())

	if eng.TotalRows() == 0 {
		t.Fatal("no rows")
	}
}

func TestPreview(t *testing.T) {
	eng := openSampleParquet(t)
	defer eng.Close()

	cols := eng.Columns()
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}

	rows, err := eng.Preview(context.Background(), names, "", 10, 0)
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if len(rows) != 10 {
		t.Fatalf("expected 10 rows, got %d", len(rows))
	}
}

func TestPreviewWithEmptyColumnsReturnsUserColumns(t *testing.T) {
	eng := openSampleParquet(t)
	defer eng.Close()

	ctx := context.Background()
	rows, err := eng.Preview(ctx, []string{}, "", 1, 0)
	if err != nil {
		t.Fatalf("Preview empty columns: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if len(rows[0]) != len(eng.Columns()) {
		t.Fatalf("unexpected column count: got %d want %d", len(rows[0]), len(eng.Columns()))
	}

	names := make([]string, len(eng.Columns()))
	for i, c := range eng.Columns() {
		names[i] = c.Name
	}
	expRows, err := eng.Preview(ctx, names, "", 1, 0)
	if err != nil {
		t.Fatalf("Preview explicit columns: %v", err)
	}
	if len(expRows) != 1 {
		t.Fatalf("expected 1 explicit row, got %d", len(expRows))
	}
	if len(expRows[0]) != len(rows[0]) {
		t.Fatalf("shape mismatch: empty=%d explicit=%d", len(rows[0]), len(expRows[0]))
	}
	for i := range rows[0] {
		if rows[0][i] != expRows[0][i] {
			t.Fatalf("value mismatch at col %d: empty=%q explicit=%q", i, rows[0][i], expRows[0][i])
		}
	}
}

func TestProfileBasic(t *testing.T) {
	eng := openSampleParquet(t)
	defer eng.Close()

	col := eng.Columns()[0]
	summary, err := eng.ProfileBasic(context.Background(), col.Name)
	if err != nil {
		t.Fatalf("ProfileBasic: %v", err)
	}
	if !summary.Loaded {
		t.Fatal("expected Loaded=true")
	}
	t.Logf("Column %s: missing=%d (%.1f%%), distinct=~%d (%.1f%%)",
		col.Name, summary.MissingCount, summary.MissingPct, summary.DistinctApprox, summary.DistinctPct)
}

func TestProfileDetail(t *testing.T) {
	eng := openSampleParquet(t)
	defer eng.Close()

	ctx := context.Background()

	summary, err := eng.ProfileBasic(ctx, "score")
	if err != nil {
		t.Fatalf("ProfileBasic(score): %v", err)
	}
	if err := eng.ProfileDetail(ctx, "score", summary, "INTEGER"); err != nil {
		t.Fatalf("ProfileDetail(score): %v", err)
	}
	if !summary.DetailLoaded {
		t.Fatal("expected DetailLoaded=true")
	}
	if summary.Numeric == nil {
		t.Fatal("expected numeric stats for score")
	}

	catSummary, err := eng.ProfileBasic(ctx, "category")
	if err != nil {
		t.Fatalf("ProfileBasic(category): %v", err)
	}
	if err := eng.ProfileDetail(ctx, "category", catSummary, "VARCHAR"); err != nil {
		t.Fatalf("ProfileDetail(category): %v", err)
	}
	if !catSummary.DetailLoaded {
		t.Fatal("expected DetailLoaded=true for category")
	}
}

func TestFirstNullRowAndOffsetWithFilter(t *testing.T) {
	eng := openSampleParquet(t)
	defer eng.Close()

	ctx := context.Background()
	filter := BuildNullFilter([]string{"score", "category"})

	rowID, err := eng.FirstNullRow(ctx, "score", filter)
	if err != nil {
		t.Fatalf("FirstNullRow: %v", err)
	}
	if rowID == 0 {
		t.Fatal("expected at least one null score row")
	}

	var expectedRowID int64
	q := `SELECT min(` + quoteIdent(eng.internalRowIDCol) + `) FROM t_base WHERE "score" IS NULL AND (` + filter + `)`
	if err := eng.db.QueryRowContext(ctx, q).Scan(&expectedRowID); err != nil {
		t.Fatalf("query expected row id: %v", err)
	}
	if rowID != expectedRowID {
		t.Fatalf("row id mismatch: got %d want %d", rowID, expectedRowID)
	}

	offset, err := eng.OffsetForRowID(ctx, rowID, filter)
	if err != nil {
		t.Fatalf("OffsetForRowID: %v", err)
	}

	var expectedOffset int64
	oq := `SELECT count(*) FROM t_base WHERE ` + quoteIdent(eng.internalRowIDCol) + ` < ? AND (` + filter + `)`
	if err := eng.db.QueryRowContext(ctx, oq, rowID).Scan(&expectedOffset); err != nil {
		t.Fatalf("query expected offset: %v", err)
	}
	if offset != expectedOffset {
		t.Fatalf("offset mismatch: got %d want %d", offset, expectedOffset)
	}

	rows, err := eng.Preview(ctx, []string{"score"}, filter, 1, int(offset))
	if err != nil {
		t.Fatalf("Preview at offset: %v", err)
	}
	if len(rows) != 1 || len(rows[0]) != 1 {
		t.Fatalf("unexpected preview shape at offset: %#v", rows)
	}
	if rows[0][0] != "NULL" {
		t.Fatalf("expected null score at jump target, got %q", rows[0][0])
	}
}

func TestFirstNullRowStableAcrossQueries(t *testing.T) {
	eng := openSampleParquet(t)
	defer eng.Close()

	ctx := context.Background()
	filter := BuildNullFilter([]string{"score", "category"})

	rowID1, err := eng.FirstNullRow(ctx, "score", filter)
	if err != nil {
		t.Fatalf("FirstNullRow first call: %v", err)
	}
	if rowID1 == 0 {
		t.Fatal("expected first null row id")
	}

	// Exercise multiple queries in between repeated null-jump calls.
	if _, err := eng.Preview(ctx, []string{"id", "score"}, filter, 5, 0); err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if _, err := eng.Preview(ctx, []string{"id", "score"}, filter, 5, 3); err != nil {
		t.Fatalf("Preview with offset: %v", err)
	}

	rowID2, err := eng.FirstNullRow(ctx, "score", filter)
	if err != nil {
		t.Fatalf("FirstNullRow second call: %v", err)
	}
	if rowID1 != rowID2 {
		t.Fatalf("unstable row id across queries: first=%d second=%d", rowID1, rowID2)
	}
}

func TestPreviewOrderStableByOffset(t *testing.T) {
	eng := openSampleParquet(t)
	defer eng.Close()

	ctx := context.Background()
	rows, err := eng.Preview(ctx, []string{"id"}, "", 3, 0)
	if err != nil {
		t.Fatalf("Preview first page: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	offsetRow, err := eng.Preview(ctx, []string{"id"}, "", 1, 1)
	if err != nil {
		t.Fatalf("Preview offset row: %v", err)
	}
	if len(offsetRow) != 1 || len(offsetRow[0]) != 1 {
		t.Fatalf("unexpected preview shape: %#v", offsetRow)
	}

	if rows[1][0] != offsetRow[0][0] {
		t.Fatalf("offset row mismatch: got %s want %s", offsetRow[0][0], rows[1][0])
	}
}

func TestOpenCSV(t *testing.T) {
	td := testdataDir()
	if td == "" {
		t.Skip("testdata not found")
	}

	eng, err := New(filepath.Join(td, "sample.csv"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer eng.Close()

	if len(eng.Columns()) == 0 {
		t.Fatal("no columns")
	}
	if eng.TotalRows() == 0 {
		t.Fatal("no rows")
	}
}

func TestIsNumericType(t *testing.T) {
	if isNumericType("INTERVAL") {
		t.Fatal("INTERVAL should not be treated as numeric")
	}
	if !isNumericType("DECIMAL(10,2)") {
		t.Fatal("DECIMAL(10,2) should be treated as numeric")
	}
}

func TestInternalRowIDNameCollision(t *testing.T) {
	tests := []struct {
		name               string
		header             string
		expectedInternalID string
	}{
		{
			name:               "legacy_rowid_column",
			header:             "rowid",
			expectedInternalID: "__pv_rowid",
		},
		{
			name:               "base_name_collision",
			header:             "__pv_rowid",
			expectedInternalID: "__pv_rowid_1",
		},
		{
			name:               "mixed_case_base_name_collision",
			header:             "__PV_RowID",
			expectedInternalID: "__pv_rowid_1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "collision.csv")
			csv := tc.header + ",value\nuser-1,1\nuser-2,\nuser-3,3\n"
			if err := os.WriteFile(path, []byte(csv), 0o644); err != nil {
				t.Fatalf("write csv: %v", err)
			}

			eng, err := New(path)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer eng.Close()

			if eng.internalRowIDCol != tc.expectedInternalID {
				t.Fatalf("unexpected internal row id column: got %q want %q", eng.internalRowIDCol, tc.expectedInternalID)
			}

			colNames := make([]string, 0, len(eng.Columns()))
			for _, c := range eng.Columns() {
				colNames = append(colNames, c.Name)
			}
			if !slices.Contains(colNames, tc.header) {
				t.Fatalf("expected user column %q to be present, columns=%v", tc.header, colNames)
			}

			ctx := context.Background()
			rowID, err := eng.FirstNullRow(ctx, "value", "")
			if err != nil {
				t.Fatalf("FirstNullRow: %v", err)
			}
			if rowID != 2 {
				t.Fatalf("unexpected null row id: got %d want 2", rowID)
			}

			offset, err := eng.OffsetForRowID(ctx, rowID, "")
			if err != nil {
				t.Fatalf("OffsetForRowID: %v", err)
			}
			if offset != 1 {
				t.Fatalf("unexpected offset: got %d want 1", offset)
			}

			rows, err := eng.Preview(ctx, []string{tc.header, "value"}, "", 1, int(offset))
			if err != nil {
				t.Fatalf("Preview: %v", err)
			}
			if len(rows) != 1 || len(rows[0]) != 2 {
				t.Fatalf("unexpected preview shape: %v", rows)
			}
			if got := fmt.Sprintf("%v", rows[0][0]); got != "user-2" {
				t.Fatalf("unexpected user row id value: got %q want %q", got, "user-2")
			}
			if rows[0][1] != "NULL" {
				t.Fatalf("expected NULL in value column, got %q", rows[0][1])
			}
		})
	}
}
