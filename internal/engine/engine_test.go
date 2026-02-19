package engine

import (
	"context"
	"os"
	"path/filepath"
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
	q := `SELECT min("__pv_rowid") FROM t_base WHERE "score" IS NULL AND (` + filter + `)`
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
	oq := `SELECT count(*) FROM t_base WHERE "__pv_rowid" < ? AND (` + filter + `)`
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
