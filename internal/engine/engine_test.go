package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

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
	t.Cleanup(func() { _ = eng.Close() })
	return eng
}

func TestOpenParquet(t *testing.T) {
	eng := openSampleParquet(t)
	requireOpenHasData(t, eng)
	t.Logf("Columns: %d, Rows: %d", len(eng.Columns()), eng.TotalRows())
}

func TestPreview(t *testing.T) {
	eng := openSampleParquet(t)
	names := allColumnNames(eng)

	rows := mustPreview(t, eng, names, "", 10, 0)
	requirePreviewShape(t, rows, 10, 0)
}

func TestPreviewWithEmptyColumnsReturnsUserColumns(t *testing.T) {
	eng := openSampleParquet(t)

	rows := mustPreview(t, eng, []string{}, "", 1, 0)
	requirePreviewShape(t, rows, 1, len(eng.Columns()))

	names := allColumnNames(eng)
	expRows := mustPreview(t, eng, names, "", 1, 0)
	requirePreviewShape(t, expRows, 1, len(rows[0]))

	for i := range rows[0] {
		if rows[0][i] != expRows[0][i] {
			t.Fatalf("value mismatch at col %d: empty=%q explicit=%q", i, rows[0][i], expRows[0][i])
		}
	}
}

func TestProfileBasic(t *testing.T) {
	eng := openSampleParquet(t)

	col := eng.Columns()[0]
	summary, err := eng.ProfileBasic(bg(), col.Name)
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
	ctx := bg()

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

func TestProfileDetailFloatAliasesAreNumeric(t *testing.T) {
	eng := openSampleParquet(t)
	ctx := bg()

	for _, duckType := range []string{"FLOAT4", "FLOAT8"} {
		t.Run(duckType, func(t *testing.T) {
			summary, err := eng.ProfileBasic(ctx, "score")
			if err != nil {
				t.Fatalf("ProfileBasic(score): %v", err)
			}
			if err := eng.ProfileDetail(ctx, "score", summary, duckType); err != nil {
				t.Fatalf("ProfileDetail(score): %v", err)
			}
			if summary.Numeric == nil {
				t.Fatalf("expected numeric stats for %s", duckType)
			}
			if summary.Hist == nil {
				t.Fatalf("expected histogram for %s", duckType)
			}
		})
	}
}

func TestFirstNullRowAndOffsetWithFilter(t *testing.T) {
	eng := openSampleParquet(t)
	ctx := bg()
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

	rows := mustPreview(t, eng, []string{"score"}, filter, 1, int(offset))
	requirePreviewShape(t, rows, 1, 1)
	requireNullCell(t, rows, 0, 0)
}

func TestRejectsUnsupportedFilter(t *testing.T) {
	eng := openSampleParquet(t)
	ctx := bg()

	t.Run("FirstNullRow", func(t *testing.T) {
		_, err := eng.FirstNullRow(ctx, "score", `1=1`)
		if err == nil {
			t.Fatal("expected unsupported filter error")
		}
	})

	t.Run("OffsetForRowID", func(t *testing.T) {
		_, err := eng.OffsetForRowID(ctx, 5, `1=1`)
		if err == nil {
			t.Fatal("expected unsupported filter error")
		}
	})
}

func TestFirstNullRowStableAcrossQueries(t *testing.T) {
	eng := openSampleParquet(t)
	ctx := bg()
	filter := BuildNullFilter([]string{"score", "category"})

	rowID1, err := eng.FirstNullRow(ctx, "score", filter)
	if err != nil {
		t.Fatalf("FirstNullRow baseline: %v", err)
	}
	if rowID1 == 0 {
		t.Fatal("expected first null row id")
	}

	for i := 0; i < 50; i++ {
		if _, err := eng.Preview(ctx, []string{"id", "score"}, filter, 5, i%4); err != nil {
			t.Fatalf("Preview iteration %d: %v", i, err)
		}

		rowID2, err := eng.FirstNullRow(ctx, "score", filter)
		if err != nil {
			t.Fatalf("FirstNullRow iteration %d: %v", i, err)
		}
		if rowID1 != rowID2 {
			t.Fatalf("unstable row id across queries at iter %d: first=%d now=%d", i, rowID1, rowID2)
		}

		offset, err := eng.OffsetForRowID(ctx, rowID2, filter)
		if err != nil {
			t.Fatalf("OffsetForRowID iteration %d: %v", i, err)
		}
		rows := mustPreview(t, eng, []string{"score"}, filter, 1, int(offset))
		requirePreviewShape(t, rows, 1, 1)
		requireNullCell(t, rows, 0, 0)
	}
}

func TestPreviewOrderStableByOffset(t *testing.T) {
	eng := openSampleParquet(t)

	rows := mustPreview(t, eng, []string{"id"}, "", 3, 0)
	requirePreviewShape(t, rows, 3, 1)

	offsetRow := mustPreview(t, eng, []string{"id"}, "", 1, 1)
	requirePreviewShape(t, offsetRow, 1, 1)

	if rows[1][0] != offsetRow[0][0] {
		t.Fatalf("offset row mismatch: got %s want %s", offsetRow[0][0], rows[1][0])
	}
}

func TestOpenCSV(t *testing.T) {
	eng := openSampleCSV(t)
	requireOpenHasData(t, eng)
}

func TestIsNumericType(t *testing.T) {
	if isNumericType("INTERVAL") {
		t.Fatal("INTERVAL should not be treated as numeric")
	}
	if !isNumericType("DECIMAL(10,2)") {
		t.Fatal("DECIMAL(10,2) should be treated as numeric")
	}
	if !isNumericType("FLOAT4") {
		t.Fatal("FLOAT4 should be treated as numeric")
	}
	if !isNumericType("FLOAT8") {
		t.Fatal("FLOAT8 should be treated as numeric")
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
			csv := tc.header + ",value\nuser-1,1\nuser-2,\nuser-3,3\n"
			path := mustWriteCSV(t, dir, "collision.csv", csv)

			eng, err := New(path)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = eng.Close() })

			if eng.internalRowIDCol != tc.expectedInternalID {
				t.Fatalf("unexpected internal row id column: got %q want %q", eng.internalRowIDCol, tc.expectedInternalID)
			}

			colNames := allColumnNames(eng)
			if !slices.Contains(colNames, tc.header) {
				t.Fatalf("expected user column %q to be present, columns=%v", tc.header, colNames)
			}

			ctx := bg()
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

			rows := mustPreview(t, eng, []string{tc.header, "value"}, "", 1, int(offset))
			requirePreviewShape(t, rows, 1, 2)
			if got := fmt.Sprintf("%v", rows[0][0]); got != "user-2" {
				t.Fatalf("unexpected user row id value: got %q want %q", got, "user-2")
			}
			requireNullCell(t, rows, 0, 1)
		})
	}
}

func TestOpenLargeCSVOptIn(t *testing.T) {
	if os.Getenv("PARQVIEW_LARGE_TEST") != "1" {
		t.Skip("set PARQVIEW_LARGE_TEST=1 to run large-file regression test")
	}

	const nRows = 300000
	dir := t.TempDir()

	var b strings.Builder
	b.Grow(32 + nRows*10)
	b.WriteString("id,score,category\n")
	for i := 0; i < nRows; i++ {
		b.WriteString(strconv.Itoa(i + 1))
		b.WriteString(",")
		if i%17 == 0 {
			b.WriteString(",")
		} else {
			b.WriteString(strconv.Itoa(i % 100))
			b.WriteString(",")
		}
		b.WriteString("group")
		b.WriteString(strconv.Itoa(i % 7))
		b.WriteString("\n")
	}
	path := mustWriteCSV(t, dir, "large.csv", b.String())

	start := time.Now()
	eng, err := New(path)
	if err != nil {
		t.Fatalf("New(large csv): %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	elapsed := time.Since(start)

	if eng.TotalRows() != nRows {
		t.Fatalf("unexpected row count: got %d want %d", eng.TotalRows(), nRows)
	}

	var tableCount int
	if err := eng.db.QueryRow(`SELECT count(*) FROM duckdb_tables() WHERE table_name = 't_base'`).Scan(&tableCount); err != nil {
		t.Fatalf("check duckdb_tables: %v", err)
	}
	if tableCount != 1 {
		t.Fatalf("expected t_base table entry, got %d", tableCount)
	}

	t.Logf("opened %d-row csv in %s", nRows, elapsed)
}

func TestParseNullFilterColumnsQuotedIdentifiers(t *testing.T) {
	tests := []struct {
		name      string
		filter    string
		wantCols  []string
		wantError bool
	}{
		{
			name:     "quoted identifier containing OR",
			filter:   `("A OR B" IS NULL OR "score" IS NULL)`,
			wantCols: []string{"A OR B", "score"},
		},
		{
			name:     "quoted identifier containing quote",
			filter:   `("a""b" IS NULL OR "score" IS NULL)`,
			wantCols: []string{`a"b`, "score"},
		},
		{
			name:      "unbalanced quote",
			filter:    `("a""b IS NULL OR "score" IS NULL)`,
			wantError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotCols, err := parseNullFilterColumns(tc.filter)
			if tc.wantError {
				if err == nil {
					t.Fatal("expected parse error")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseNullFilterColumns: %v", err)
			}
			if !slices.Equal(gotCols, tc.wantCols) {
				t.Fatalf("columns mismatch: got %v want %v", gotCols, tc.wantCols)
			}
		})
	}
}
