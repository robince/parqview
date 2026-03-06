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

	"github.com/robince/parqview/internal/missing"
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
	summary, err := eng.ProfileBasic(bg(), col.Name, missing.ModeNullAndNaN)
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

	summary, err := eng.ProfileBasic(ctx, "score", missing.ModeNullAndNaN)
	if err != nil {
		t.Fatalf("ProfileBasic(score): %v", err)
	}
	if err := eng.ProfileDetail(ctx, "score", summary, "INTEGER", missing.ModeNullAndNaN); err != nil {
		t.Fatalf("ProfileDetail(score): %v", err)
	}
	if !summary.DetailLoaded {
		t.Fatal("expected DetailLoaded=true")
	}
	if summary.Numeric == nil {
		t.Fatal("expected numeric stats for score")
	}

	catSummary, err := eng.ProfileBasic(ctx, "category", missing.ModeNullAndNaN)
	if err != nil {
		t.Fatalf("ProfileBasic(category): %v", err)
	}
	if err := eng.ProfileDetail(ctx, "category", catSummary, "VARCHAR", missing.ModeNullAndNaN); err != nil {
		t.Fatalf("ProfileDetail(category): %v", err)
	}
	if !catSummary.DetailLoaded {
		t.Fatal("expected DetailLoaded=true for category")
	}
}

func TestProfileDetailTreatsProvidedFloatAliasesAsNumeric(t *testing.T) {
	eng := openSampleParquet(t)
	ctx := bg()

	for _, duckType := range []string{"FLOAT4", "FLOAT8"} {
		t.Run(duckType, func(t *testing.T) {
			summary, err := eng.ProfileBasic(ctx, "score", missing.ModeNullAndNaN)
			if err != nil {
				t.Fatalf("ProfileBasic(score): %v", err)
			}
			if summary.Numeric != nil {
				t.Fatalf("expected numeric stats to be empty before ProfileDetail for %s", duckType)
			}
			if summary.Hist != nil {
				t.Fatalf("expected histogram to be empty before ProfileDetail for %s", duckType)
			}
			if err := eng.ProfileDetail(ctx, "score", summary, duckType, missing.ModeNullAndNaN); err != nil {
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
	filterCols := []string{"score", "category"}
	filter := BuildMissingFilter(filterCols, missing.ModeNullAndNaN)

	rowID, err := eng.FirstNullRow(ctx, "score", filterCols, missing.ModeNullAndNaN)
	if err != nil {
		t.Fatalf("FirstNullRow: %v", err)
	}
	if rowID == 0 {
		t.Fatal("expected at least one null score row")
	}

	var expectedRowID int64
	q := `SELECT min(` + quoteIdent(eng.internalRowIDCol) + `) FROM t_base WHERE ` + missing.ModeNullAndNaN.SQLPredicate(`"score"`) + ` AND ` + filter
	if err := eng.db.QueryRowContext(ctx, q).Scan(&expectedRowID); err != nil {
		t.Fatalf("query expected row id: %v", err)
	}
	if rowID != expectedRowID {
		t.Fatalf("row id mismatch: got %d want %d", rowID, expectedRowID)
	}

	offset, err := eng.OffsetForRowID(ctx, rowID, filterCols, missing.ModeNullAndNaN)
	if err != nil {
		t.Fatalf("OffsetForRowID: %v", err)
	}

	var expectedOffset int64
	oq := `SELECT count(*) FROM t_base WHERE ` + quoteIdent(eng.internalRowIDCol) + ` < ? AND ` + filter
	if err := eng.db.QueryRowContext(ctx, oq, rowID).Scan(&expectedOffset); err != nil {
		t.Fatalf("query expected offset: %v", err)
	}
	if offset != expectedOffset {
		t.Fatalf("offset mismatch: got %d want %d", offset, expectedOffset)
	}

	rows := mustPreview(t, eng, []string{"score"}, filter, 1, int(offset))
	requirePreviewShape(t, rows, 1, 1)
	requireNullCell(t, rows, 0)
}

func TestFirstNullRowStableAcrossQueries(t *testing.T) {
	eng := openSampleParquet(t)
	ctx := bg()
	filterCols := []string{"score", "category"}
	filter := BuildMissingFilter(filterCols, missing.ModeNullAndNaN)

	rowID1, err := eng.FirstNullRow(ctx, "score", filterCols, missing.ModeNullAndNaN)
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

		rowID2, err := eng.FirstNullRow(ctx, "score", filterCols, missing.ModeNullAndNaN)
		if err != nil {
			t.Fatalf("FirstNullRow iteration %d: %v", i, err)
		}
		if rowID1 != rowID2 {
			t.Fatalf("unstable row id across queries at iter %d: first=%d now=%d", i, rowID1, rowID2)
		}

		offset, err := eng.OffsetForRowID(ctx, rowID2, filterCols, missing.ModeNullAndNaN)
		if err != nil {
			t.Fatalf("OffsetForRowID iteration %d: %v", i, err)
		}
		rows := mustPreview(t, eng, []string{"score"}, filter, 1, int(offset))
		requirePreviewShape(t, rows, 1, 1)
		requireNullCell(t, rows, 0)
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
			rowID, err := eng.FirstNullRow(ctx, "value", nil, missing.ModeNullAndNaN)
			if err != nil {
				t.Fatalf("FirstNullRow: %v", err)
			}
			if rowID != 2 {
				t.Fatalf("unexpected null row id: got %d want 2", rowID)
			}

			offset, err := eng.OffsetForRowID(ctx, rowID, nil, missing.ModeNullAndNaN)
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
			requireNullCell(t, rows, 1)
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

func TestBuildMissingFilterUsesModeSpecificPredicate(t *testing.T) {
	filter := BuildMissingFilter([]string{"score", "A OR B"}, missing.ModeNaNOnly)
	if filter == "" {
		t.Fatal("expected non-empty filter")
	}
	if strings.Contains(filter, "IS NULL") {
		t.Fatalf("did not expect NULL predicate in NaN-only filter: %q", filter)
	}
	if strings.Count(filter, "isnan") != 2 {
		t.Fatalf("expected NaN predicate for both columns, got %q", filter)
	}
}

func TestProfileBasicUsesMissingPredicate(t *testing.T) {
	dir := t.TempDir()
	path := mustWriteCSV(t, dir, "nan.csv", "score\n1.0\nNaN\n\n2.5\n")
	eng, err := New(path)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })

	tests := []struct {
		name      string
		mode      missing.Mode
		wantCount int64
	}{
		{name: "null+nan", mode: missing.ModeNullAndNaN, wantCount: 2},
		{name: "null only", mode: missing.ModeNullOnly, wantCount: 1},
		{name: "nan only", mode: missing.ModeNaNOnly, wantCount: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			summary, err := eng.ProfileBasic(bg(), "score", tc.mode)
			if err != nil {
				t.Fatalf("ProfileBasic: %v", err)
			}
			if summary.MissingCount != tc.wantCount {
				t.Fatalf("missing count mismatch: got %d want %d", summary.MissingCount, tc.wantCount)
			}
		})
	}
}

func TestProfileBasicAndFilterVarcharWithModeNaNOnly(t *testing.T) {
	// Regression: ModeNaNOnly must not error on VARCHAR columns.
	// isnan() is numeric-only in DuckDB; SQLNaNPredicate uses TRY_CAST to be safe.
	dir := t.TempDir()
	path := mustWriteCSV(t, dir, "varchar_nan.csv", "category\nalpha\nNaN\n\nbeta\n")
	eng, err := New(path)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	ctx := bg()

	summary, err := eng.ProfileBasic(ctx, "category", missing.ModeNaNOnly)
	if err != nil {
		t.Fatalf("ProfileBasic(VARCHAR, ModeNaNOnly) unexpected error: %v", err)
	}
	// "NaN" is a literal string in a VARCHAR column; TRY_CAST("NaN" AS DOUBLE) = NaN,
	// so isnan() should return true for it.
	if summary.MissingCount != 1 {
		t.Fatalf("expected MissingCount=1 for NaN string in VARCHAR, got %d", summary.MissingCount)
	}

	filterCols := []string{"category"}
	rowID, err := eng.FirstNullRow(ctx, "category", filterCols, missing.ModeNaNOnly)
	if err != nil {
		t.Fatalf("FirstNullRow(VARCHAR, ModeNaNOnly) unexpected error: %v", err)
	}
	if rowID == 0 {
		t.Fatal("expected a NaN row for VARCHAR column under ModeNaNOnly")
	}
}

func TestProfileBasicDistinctStatsExcludeModeSpecificMissingValues(t *testing.T) {
	dir := t.TempDir()
	var csv strings.Builder
	csv.WriteString("category\n")
	for i := 0; i < 188; i++ {
		for j := 0; j < 10; j++ {
			fmt.Fprintf(&csv, "value-%03d\n", i)
		}
	}
	csv.WriteString("NaN\n")

	path := mustWriteCSV(t, dir, "distinct_mode_nan.csv", csv.String())
	eng, err := New(path)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })

	nullOnly, err := eng.ProfileBasic(bg(), "category", missing.ModeNullOnly)
	if err != nil {
		t.Fatalf("ProfileBasic: %v", err)
	}
	nanOnly, err := eng.ProfileBasic(bg(), "category", missing.ModeNaNOnly)
	if err != nil {
		t.Fatalf("ProfileBasic: %v", err)
	}
	if nullOnly.MissingCount != 0 {
		t.Fatalf("expected NULL-only MissingCount=0, got %d", nullOnly.MissingCount)
	}
	if nanOnly.MissingCount != 1 {
		t.Fatalf("expected NaN-only MissingCount=1, got %d", nanOnly.MissingCount)
	}
	if nanOnly.DistinctApprox >= nullOnly.DistinctApprox {
		t.Fatalf("expected NaN-only distinct count to exclude missing NaN: null-only=%d nan-only=%d", nullOnly.DistinctApprox, nanOnly.DistinctApprox)
	}
	if nanOnly.DistinctPct >= nullOnly.DistinctPct {
		t.Fatalf("expected NaN-only distinct pct to drop when NaN is missing: null-only=%v nan-only=%v", nullOnly.DistinctPct, nanOnly.DistinctPct)
	}
}

func TestProfileBasicAndDetailModeNaNOnlyIncludesSQLNullCategorically(t *testing.T) {
	dir := t.TempDir()
	path := mustWriteCSV(t, dir, "nan_only_mixed.csv", "category\nalpha\n\nNaN\nalpha\nbeta\n")
	eng, err := New(path)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })

	summary, err := eng.ProfileBasic(bg(), "category", missing.ModeNaNOnly)
	if err != nil {
		t.Fatalf("ProfileBasic: %v", err)
	}
	if summary.MissingCount != 1 {
		t.Fatalf("expected MissingCount=1, got %d", summary.MissingCount)
	}
	if summary.DistinctApprox != 3 {
		t.Fatalf("expected DistinctApprox=3 after excluding only NaN, got %d", summary.DistinctApprox)
	}
	if summary.DistinctPct < 74 || summary.DistinctPct > 76 {
		t.Fatalf("expected DistinctPct near 75, got %v", summary.DistinctPct)
	}

	if err := eng.ProfileDetail(bg(), "category", summary, "VARCHAR", missing.ModeNaNOnly); err != nil {
		t.Fatalf("ProfileDetail: %v", err)
	}
	if len(summary.Top3) != 3 {
		t.Fatalf("expected 3 top values after excluding only NaN, got %+v", summary.Top3)
	}
	if summary.Top3[0].Value != "alpha" || summary.Top3[0].Count != 2 {
		t.Fatalf("unexpected first top value: %+v", summary.Top3[0])
	}
	// Ties are ordered by value ascending, so the ASCII string sorts before the Unicode null sentinel.
	if summary.Top3[1].Value != "beta" || summary.Top3[1].Count != 1 {
		t.Fatalf("unexpected second top value: %+v", summary.Top3[1])
	}
	if summary.Top3[2].Value != "⟨SQL null⟩" || summary.Top3[2].Count != 1 {
		t.Fatalf("unexpected third top value: %+v", summary.Top3[2])
	}
	if summary.Top3[0].Pct < 49 || summary.Top3[0].Pct > 51 {
		t.Fatalf("expected alpha pct near 50, got %v", summary.Top3[0].Pct)
	}
	for _, tv := range summary.Top3 {
		if tv.Value == "" || strings.EqualFold(strings.TrimSpace(tv.Value), "nan") {
			t.Fatalf("unexpected missing-like top value under NaN-only mode: %+v", tv)
		}
	}
}

func TestProfileDetailModeNaNOnlyDisambiguatesSQLNullFromLiteralNULL(t *testing.T) {
	dir := t.TempDir()
	path := mustWriteCSV(t, dir, "nan_only_literal_null.csv", "category\nNULL\n\nNULL\nalpha\nNaN\n")
	eng, err := New(path)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })

	summary, err := eng.ProfileBasic(bg(), "category", missing.ModeNaNOnly)
	if err != nil {
		t.Fatalf("ProfileBasic: %v", err)
	}
	if err := eng.ProfileDetail(bg(), "category", summary, "VARCHAR", missing.ModeNaNOnly); err != nil {
		t.Fatalf("ProfileDetail: %v", err)
	}
	if len(summary.Top3) != 3 {
		t.Fatalf("expected 3 top values, got %+v", summary.Top3)
	}
	if summary.Top3[0].Value != "NULL" || summary.Top3[0].Count != 2 {
		t.Fatalf("unexpected first top value: %+v", summary.Top3[0])
	}
	// Ties are ordered by value ascending, so the ASCII string sorts before the Unicode null sentinel.
	if summary.Top3[1].Value != "alpha" || summary.Top3[1].Count != 1 {
		t.Fatalf("unexpected second top value: %+v", summary.Top3[1])
	}
	if summary.Top3[2].Value != "⟨SQL null⟩" || summary.Top3[2].Count != 1 {
		t.Fatalf("unexpected third top value: %+v", summary.Top3[2])
	}
}

func TestProfileDetailDisambiguatesSQLNullFromLiteralNullSentinel(t *testing.T) {
	dir := t.TempDir()
	path := mustWriteCSV(t, dir, "literal_null_sentinel.csv", "category\n⟨null⟩\n\n⟨null⟩\nalpha\n")
	eng, err := New(path)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })

	summary, err := eng.ProfileBasic(bg(), "category", missing.ModeNaNOnly)
	if err != nil {
		t.Fatalf("ProfileBasic: %v", err)
	}
	if err := eng.ProfileDetail(bg(), "category", summary, "VARCHAR", missing.ModeNaNOnly); err != nil {
		t.Fatalf("ProfileDetail: %v", err)
	}
	if len(summary.Top3) != 3 {
		t.Fatalf("expected 3 top values, got %+v", summary.Top3)
	}
	if summary.Top3[0].Value != "⟨null⟩" || summary.Top3[0].Count != 2 {
		t.Fatalf("unexpected first top value: %+v", summary.Top3[0])
	}
	if summary.Top3[1].Value != "alpha" || summary.Top3[1].Count != 1 {
		t.Fatalf("unexpected second top value: %+v", summary.Top3[1])
	}
	if summary.Top3[2].Value != "⟨SQL null⟩" || summary.Top3[2].Count != 1 {
		t.Fatalf("unexpected third top value: %+v", summary.Top3[2])
	}
	// ModeNaNOnly excludes only NaN here, so all 4 rows remain in the categorical denominator.
	if summary.Top3[0].Pct < 49 || summary.Top3[0].Pct > 51 {
		t.Fatalf("expected literal sentinel pct near 50, got %v", summary.Top3[0].Pct)
	}
	if summary.Top3[2].Pct < 24 || summary.Top3[2].Pct > 26 {
		t.Fatalf("expected SQL null pct near 25, got %v", summary.Top3[2].Pct)
	}
}

func TestProfileDetailModeNaNOnlyStillExcludesSQLNullFromNumericStats(t *testing.T) {
	dir := t.TempDir()
	path := mustWriteCSV(t, dir, "nan_only_numeric.csv", "score\n1.0\n\nNaN\n2.5\n")
	eng, err := New(path)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })

	summary, err := eng.ProfileBasic(bg(), "score", missing.ModeNaNOnly)
	if err != nil {
		t.Fatalf("ProfileBasic: %v", err)
	}
	if err := eng.ProfileDetail(bg(), "score", summary, "DOUBLE", missing.ModeNaNOnly); err != nil {
		t.Fatalf("ProfileDetail: %v", err)
	}
	if summary.Numeric == nil {
		t.Fatal("expected numeric stats")
	}
	if summary.Numeric.Min < 0.99 || summary.Numeric.Min > 1.01 {
		t.Fatalf("unexpected min: got %v", summary.Numeric.Min)
	}
	if summary.Numeric.Max < 2.49 || summary.Numeric.Max > 2.51 {
		t.Fatalf("unexpected max: got %v", summary.Numeric.Max)
	}
}

func TestProfileDetailModeNullOnlyStillExcludesNaNFromNumericStats(t *testing.T) {
	dir := t.TempDir()
	path := mustWriteCSV(t, dir, "null_only_numeric.csv", "score\n1.0\n\nNaN\n2.5\n")
	eng, err := New(path)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })

	summary, err := eng.ProfileBasic(bg(), "score", missing.ModeNullOnly)
	if err != nil {
		t.Fatalf("ProfileBasic: %v", err)
	}
	if err := eng.ProfileDetail(bg(), "score", summary, "DOUBLE", missing.ModeNullOnly); err != nil {
		t.Fatalf("ProfileDetail: %v", err)
	}
	if summary.Numeric == nil {
		t.Fatal("expected numeric stats")
	}
	if summary.Numeric.Min < 0.99 || summary.Numeric.Min > 1.01 {
		t.Fatalf("unexpected min: got %v", summary.Numeric.Min)
	}
	if summary.Numeric.Max < 2.49 || summary.Numeric.Max > 2.51 {
		t.Fatalf("unexpected max: got %v", summary.Numeric.Max)
	}
}

func TestProfileDetailExcludesMissingPredicate(t *testing.T) {
	dir := t.TempDir()
	// NaN appears 3× so it dominates Top3 under ModeNullOnly regardless of tie-breaking.
	path := mustWriteCSV(t, dir, "nan_detail.csv", "category\nNaN\nNaN\nNaN\nalpha\n\n")
	eng, err := New(path)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })

	t.Run("null+nan excludes NaN", func(t *testing.T) {
		summary, err := eng.ProfileBasic(bg(), "category", missing.ModeNullAndNaN)
		if err != nil {
			t.Fatalf("ProfileBasic: %v", err)
		}
		if !summary.IsDiscrete {
			t.Fatal("expected category to be discrete")
		}
		if err := eng.ProfileDetail(bg(), "category", summary, "VARCHAR", missing.ModeNullAndNaN); err != nil {
			t.Fatalf("ProfileDetail: %v", err)
		}
		for _, tv := range summary.Top3 {
			if strings.EqualFold(strings.TrimSpace(tv.Value), "nan") {
				t.Fatalf("unexpected NaN top value when NaN is configured as missing: %+v", tv)
			}
		}
	})

	t.Run("null only keeps NaN", func(t *testing.T) {
		summary, err := eng.ProfileBasic(bg(), "category", missing.ModeNullOnly)
		if err != nil {
			t.Fatalf("ProfileBasic: %v", err)
		}
		if err := eng.ProfileDetail(bg(), "category", summary, "VARCHAR", missing.ModeNullOnly); err != nil {
			t.Fatalf("ProfileDetail: %v", err)
		}
		foundNaN := false
		for _, tv := range summary.Top3 {
			if strings.EqualFold(strings.TrimSpace(tv.Value), "nan") {
				foundNaN = true
			}
		}
		if !foundNaN {
			t.Fatalf("expected NaN to remain a top value when only NULL is missing: %+v", summary.Top3)
		}
	})
}

func TestProfileDetailNumericExcludesMissingPredicate(t *testing.T) {
	dir := t.TempDir()
	path := mustWriteCSV(t, dir, "nan_detail_numeric.csv", "score\n1.0\nNaN\n\n2.5\n")
	eng, err := New(path)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })

	summary, err := eng.ProfileBasic(bg(), "score", missing.ModeNullAndNaN)
	if err != nil {
		t.Fatalf("ProfileBasic: %v", err)
	}
	if summary.IsDiscrete {
		t.Skip("score column unexpectedly classified as discrete; numeric path not exercised")
	}
	if err := eng.ProfileDetail(bg(), "score", summary, "DOUBLE", missing.ModeNullAndNaN); err != nil {
		t.Fatalf("ProfileDetail: %v", err)
	}
	if summary.Numeric == nil {
		t.Fatal("expected numeric stats")
	}
	if summary.Numeric.Min != summary.Numeric.Min || summary.Numeric.Max != summary.Numeric.Max {
		t.Fatalf("expected finite min/max, got min=%v max=%v", summary.Numeric.Min, summary.Numeric.Max)
	}
	if summary.Numeric.Min < 0.99 || summary.Numeric.Min > 1.01 {
		t.Fatalf("unexpected min: got %v", summary.Numeric.Min)
	}
	if summary.Numeric.Max < 2.49 || summary.Numeric.Max > 2.51 {
		t.Fatalf("unexpected max: got %v", summary.Numeric.Max)
	}
}

func TestNextNullRowWrapAndRowIDForOffset(t *testing.T) {
	eng := openSampleParquet(t)
	ctx := bg()

	first, err := eng.FirstNullRow(ctx, "score", nil, missing.ModeNullAndNaN)
	if err != nil {
		t.Fatalf("FirstNullRow: %v", err)
	}
	if first == 0 {
		t.Fatal("expected at least one missing score row")
	}

	rowIDAtZero, err := eng.RowIDForOffset(ctx, 0, nil, missing.ModeNullAndNaN)
	if err != nil {
		t.Fatalf("RowIDForOffset(0): %v", err)
	}
	if rowIDAtZero == 0 {
		t.Fatal("expected a row id at offset 0")
	}

	next, wrapped, err := eng.NextNullRow(ctx, "score", nil, missing.ModeNullAndNaN, eng.TotalRows()+1)
	if err != nil {
		t.Fatalf("NextNullRow(wrap): %v", err)
	}
	if !wrapped {
		t.Fatal("expected wrapped=true when searching after final row")
	}
	if next != first {
		t.Fatalf("wrapped row mismatch: got %d want %d", next, first)
	}
}

func TestPrevNullRowWrap(t *testing.T) {
	eng := openSampleParquet(t)
	ctx := bg()

	var last int64
	q := `SELECT max(` + quoteIdent(eng.internalRowIDCol) + `) FROM t_base WHERE ` + missing.ModeNullAndNaN.SQLPredicate(`"score"`)
	if err := eng.db.QueryRowContext(ctx, q).Scan(&last); err != nil {
		t.Fatalf("query last missing row: %v", err)
	}
	if last == 0 {
		t.Fatal("expected at least one missing score row")
	}

	prev, wrapped, err := eng.PrevNullRow(ctx, "score", nil, missing.ModeNullAndNaN, 1)
	if err != nil {
		t.Fatalf("PrevNullRow(wrap): %v", err)
	}
	if !wrapped {
		t.Fatal("expected wrapped=true when searching before first row")
	}
	if prev != last {
		t.Fatalf("wrapped row mismatch: got %d want %d", prev, last)
	}
}

func TestFirstNullRowModeNullOnly(t *testing.T) {
	dir := t.TempDir()
	// Row 2 is NULL, row 3 is NaN — ModeNullOnly should find only row 2.
	path := mustWriteCSV(t, dir, "null_and_nan.csv", "score\n1.0\n\nNaN\n2.5\n")
	eng, err := New(path)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	ctx := bg()

	rowID, err := eng.FirstNullRow(ctx, "score", nil, missing.ModeNullOnly)
	if err != nil {
		t.Fatalf("FirstNullRow(NullOnly): %v", err)
	}
	if rowID == 0 {
		t.Fatal("expected a NULL row under ModeNullOnly")
	}

	// Confirm the found row is actually NULL (not NaN).
	offset, err := eng.OffsetForRowID(ctx, rowID, nil, missing.ModeNullOnly)
	if err != nil {
		t.Fatalf("OffsetForRowID: %v", err)
	}
	rows := mustPreview(t, eng, []string{"score"}, "", 1, int(offset))
	requirePreviewShape(t, rows, 1, 1)
	requireNullCell(t, rows, 0)
}

func TestFirstNullRowModeNaNOnly(t *testing.T) {
	dir := t.TempDir()
	// Row 2 is NULL, row 3 is NaN — ModeNaNOnly should only find the NaN row.
	path := mustWriteCSV(t, dir, "null_and_nan2.csv", "score\n1.0\n\nNaN\n2.5\n")
	eng, err := New(path)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	ctx := bg()

	rowID, err := eng.FirstNullRow(ctx, "score", nil, missing.ModeNaNOnly)
	if err != nil {
		t.Fatalf("FirstNullRow(NaNOnly): %v", err)
	}
	if rowID == 0 {
		t.Fatal("expected a NaN row under ModeNaNOnly")
	}

	// Confirm the found row is actually NaN (not NULL).
	offset, err := eng.OffsetForRowID(ctx, rowID, nil, missing.ModeNaNOnly)
	if err != nil {
		t.Fatalf("OffsetForRowID: %v", err)
	}
	rows := mustPreview(t, eng, []string{"score"}, "", 1, int(offset))
	requirePreviewShape(t, rows, 1, 1)
	requireNaNCell(t, rows, 0)
}

func TestNextNullRowModeNaNOnly(t *testing.T) {
	dir := t.TempDir()
	// Three rows: NaN, 1.0, NaN — two NaN rows, zero NULL rows.
	path := mustWriteCSV(t, dir, "two_nan.csv", "score\nNaN\n1.0\nNaN\n")
	eng, err := New(path)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	ctx := bg()

	first, err := eng.FirstNullRow(ctx, "score", nil, missing.ModeNaNOnly)
	if err != nil {
		t.Fatalf("FirstNullRow(NaNOnly): %v", err)
	}
	if first == 0 {
		t.Fatal("expected at least one NaN row")
	}

	next, wrapped, err := eng.NextNullRow(ctx, "score", nil, missing.ModeNaNOnly, first)
	if err != nil {
		t.Fatalf("NextNullRow(NaNOnly): %v", err)
	}
	if next == 0 {
		t.Fatal("expected a second NaN row")
	}
	if next == first {
		t.Fatalf("NextNullRow returned same row as first: %d", next)
	}
	if wrapped {
		t.Fatal("expected wrapped=false when second NaN row exists after first")
	}
}

func TestPrevNullRowModeNullOnly(t *testing.T) {
	dir := t.TempDir()
	// Two NULL rows: rows 1 and 3 (1-indexed). Row 2 is 1.0 (non-missing).
	path := mustWriteCSV(t, dir, "two_null.csv", "score\n\n1.0\n\n2.0\n")
	eng, err := New(path)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	ctx := bg()

	// Get the last NULL row ID by finding it via wrap from row 1.
	prev, wrapped, err := eng.PrevNullRow(ctx, "score", nil, missing.ModeNullOnly, 1)
	if err != nil {
		t.Fatalf("PrevNullRow(NullOnly): %v", err)
	}
	if prev == 0 {
		t.Fatal("expected a previous NULL row (wrapped)")
	}
	if !wrapped {
		t.Fatal("expected wrapped=true when searching before first row")
	}

	// Confirm the found row is actually NULL (not NaN or a regular value).
	offset, err := eng.OffsetForRowID(ctx, prev, nil, missing.ModeNullOnly)
	if err != nil {
		t.Fatalf("OffsetForRowID: %v", err)
	}
	rows := mustPreview(t, eng, []string{"score"}, "", 1, int(offset))
	requirePreviewShape(t, rows, 1, 1)
	requireNullCell(t, rows, 0)
}

func TestNextPrevNullRowSingleMissingDoNotWrapToSameRow(t *testing.T) {
	dir := t.TempDir()
	path := mustWriteCSV(t, dir, "single_missing.csv", "score\n1\n\n2\n")
	eng, err := New(path)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	ctx := bg()

	rowID, err := eng.FirstNullRow(ctx, "score", nil, missing.ModeNullAndNaN)
	if err != nil {
		t.Fatalf("FirstNullRow: %v", err)
	}
	if rowID == 0 {
		t.Fatal("expected one missing row")
	}

	next, wrapped, err := eng.NextNullRow(ctx, "score", nil, missing.ModeNullAndNaN, rowID)
	if err != nil {
		t.Fatalf("NextNullRow: %v", err)
	}
	if wrapped {
		t.Fatal("expected wrapped=false when only current row is missing")
	}
	if next != 0 {
		t.Fatalf("expected no next missing row, got %d", next)
	}

	prev, wrapped, err := eng.PrevNullRow(ctx, "score", nil, missing.ModeNullAndNaN, rowID)
	if err != nil {
		t.Fatalf("PrevNullRow: %v", err)
	}
	if wrapped {
		t.Fatal("expected wrapped=false when only current row is missing")
	}
	if prev != 0 {
		t.Fatalf("expected no previous missing row, got %d", prev)
	}
}
