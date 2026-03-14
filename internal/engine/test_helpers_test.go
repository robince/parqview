package engine

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/marcboeker/go-duckdb"
)

// bg returns a background context for test convenience.
func bg() context.Context {
	return context.Background()
}

// openSampleEngine opens a test fixture file and registers cleanup.
func openSampleEngine(t *testing.T, file string) *Engine {
	t.Helper()
	td := testdataDir()
	if td == "" {
		t.Skip("testdata not found")
	}
	eng, err := New(filepath.Join(td, file))
	if err != nil {
		t.Fatalf("New(%s): %v", file, err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	return eng
}

// openSampleCSV opens testdata/sample.csv and registers cleanup.
func openSampleCSV(t *testing.T) *Engine {
	t.Helper()
	return openSampleEngine(t, "sample.csv")
}

// allColumnNames returns the column names from the engine schema.
func allColumnNames(eng *Engine) []string {
	cols := eng.Columns()
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return names
}

// requireOpenHasData asserts the engine has columns and rows.
func requireOpenHasData(t *testing.T, eng *Engine) {
	t.Helper()
	if len(eng.Columns()) == 0 {
		t.Fatal("no columns")
	}
	if eng.TotalRows() == 0 {
		t.Fatal("no rows")
	}
}

// mustPreview calls Preview and fails the test on error.
func mustPreview(t *testing.T, eng *Engine, cols []string, filter string, limit, offset int) [][]string {
	t.Helper()
	_, rows, err := eng.Preview(bg(), cols, filter, limit, offset)
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	return rows
}

// requirePreviewShape asserts a preview result has the expected dimensions.
func requirePreviewShape(t *testing.T, rows [][]string, wantRows, wantCols int) {
	t.Helper()
	if len(rows) != wantRows {
		t.Fatalf("expected %d rows, got %d", wantRows, len(rows))
	}
	if wantCols > 0 && len(rows) > 0 && len(rows[0]) != wantCols {
		t.Fatalf("expected %d cols, got %d", wantCols, len(rows[0]))
	}
}

// requireNullCell asserts that the first preview row has "NULL" in the given column.
func requireNullCell(t *testing.T, rows [][]string, col int) {
	t.Helper()
	if rows[0][col] != "NULL" {
		t.Fatalf("expected NULL at [0][%d], got %q", col, rows[0][col])
	}
}

// requireNaNCell asserts that the first preview row has a NaN value in the given column.
func requireNaNCell(t *testing.T, rows [][]string, col int) {
	t.Helper()
	if !strings.EqualFold(strings.TrimSpace(rows[0][col]), "nan") {
		t.Fatalf("expected NaN at [0][%d], got %q", col, rows[0][col])
	}
}

// mustWriteFile writes test content to a temp file and returns the path.
func mustWriteFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

// mustWriteCSV writes CSV content to a temp file and returns the path.
func mustWriteCSV(t *testing.T, dir, name, content string) string {
	t.Helper()
	return mustWriteFile(t, dir, name, content)
}
