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

func TestOpenParquet(t *testing.T) {
	td := testdataDir()
	if td == "" {
		t.Skip("testdata not found")
	}

	eng, err := New(filepath.Join(td, "sample.parquet"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
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
	td := testdataDir()
	if td == "" {
		t.Skip("testdata not found")
	}

	eng, err := New(filepath.Join(td, "sample.parquet"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
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
	td := testdataDir()
	if td == "" {
		t.Skip("testdata not found")
	}

	eng, err := New(filepath.Join(td, "sample.parquet"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
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
