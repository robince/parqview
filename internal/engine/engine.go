package engine

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/robince/parqview/internal/missing"
	"github.com/robince/parqview/internal/types"
	"github.com/robince/parqview/internal/util"
)

// Engine wraps a DuckDB connection for querying a data file.
type Engine struct {
	db               *sql.DB
	totalRows        int64
	columns          []types.ColumnInfo
	internalRowIDCol string
}

// New creates a new engine, opens the file, and creates query objects.
func New(path string) (*Engine, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}

	ext := strings.ToLower(filepath.Ext(path))
	var sourceExpr string
	switch ext {
	case ".parquet":
		sourceExpr = fmt.Sprintf("read_parquet('%s')", escapeSQLString(path))
	case ".csv":
		sourceExpr = fmt.Sprintf("read_csv_auto('%s')", escapeSQLString(path))
	default:
		_ = db.Close()
		return nil, fmt.Errorf("unsupported file extension: %s", ext)
	}

	internalRowIDCol, err := uniqueInternalRowIDCol(db, sourceExpr)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("resolve internal row id column: %w", err)
	}

	query := fmt.Sprintf(`
		CREATE TABLE t_base AS
		SELECT row_number() OVER ()::BIGINT AS %s, * FROM %s;
		CREATE VIEW t AS
		SELECT * EXCLUDE (%s) FROM t_base;
	`, quoteIdent(internalRowIDCol), sourceExpr, quoteIdent(internalRowIDCol))

	if _, err := db.Exec(query); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("create base objects: %w", err)
	}

	e := &Engine{db: db, internalRowIDCol: internalRowIDCol}

	if err := e.loadSchema(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("load schema: %w", err)
	}

	if err := e.loadRowCount(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("count rows: %w", err)
	}

	return e, nil
}

func (e *Engine) loadSchema() error {
	rows, err := e.db.Query("DESCRIBE t")
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	var cols []types.ColumnInfo
	idx := 0
	for rows.Next() {
		var name, dtype string
		var null, key, def, extra sql.NullString
		if err := rows.Scan(&name, &dtype, &null, &key, &def, &extra); err != nil {
			return err
		}
		cols = append(cols, types.ColumnInfo{Name: name, DuckType: dtype, Index: idx})
		idx++
	}
	e.columns = cols
	return rows.Err()
}

func (e *Engine) loadRowCount() error {
	return e.db.QueryRow("SELECT count(*) FROM t").Scan(&e.totalRows)
}

// Columns returns the schema columns.
func (e *Engine) Columns() []types.ColumnInfo {
	return e.columns
}

// TotalRows returns the total row count.
func (e *Engine) TotalRows() int64 {
	return e.totalRows
}

// Preview fetches rows for the table view.
func (e *Engine) Preview(ctx context.Context, colNames []string, rowFilter string, limit, offset int) ([][]string, error) {
	var (
		q     string
		nCols int
	)
	if len(colNames) == 0 {
		q = fmt.Sprintf("SELECT * EXCLUDE (%s) FROM t_base", quoteIdent(e.internalRowIDCol))
		nCols = len(e.columns)
	} else {
		var proj strings.Builder
		for i, c := range colNames {
			if i > 0 {
				proj.WriteString(", ")
			}
			proj.WriteString(quoteIdent(c))
		}
		q = fmt.Sprintf("SELECT %s FROM t_base", proj.String())
		nCols = len(colNames)
	}
	if rowFilter != "" {
		q += " WHERE " + rowFilter
	}
	q += fmt.Sprintf(" ORDER BY %s LIMIT %d OFFSET %d", quoteIdent(e.internalRowIDCol), limit, offset)

	rows, err := e.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var result [][]string
	for rows.Next() {
		vals := make([]interface{}, nCols)
		ptrs := make([]interface{}, nCols)
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make([]string, nCols)
		for i, v := range vals {
			if v == nil {
				row[i] = "NULL"
			} else {
				row[i] = fmt.Sprintf("%v", v)
			}
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

// FilteredRowCount returns the count of rows matching the filter.
func (e *Engine) FilteredRowCount(ctx context.Context, rowFilter string) (int64, error) {
	q := "SELECT count(*) FROM t"
	if rowFilter != "" {
		q += " WHERE " + rowFilter
	}
	var count int64
	err := e.db.QueryRowContext(ctx, q).Scan(&count)
	return count, err
}

// ProfileBasic computes missing count, missing%, approx distinct, distinct% for a column.
func (e *Engine) ProfileBasic(ctx context.Context, colName string, mode missing.Mode) (*types.ColumnSummary, error) {
	col := quoteIdent(colName)
	missingExpr := mode.SQLPredicate(col)
	profiledExpr := categoricalProfileExpr(col, mode)
	profiledNonNullExpr := numericProfileExpr(col, mode)
	q := fmt.Sprintf(`SELECT
		sum(CASE WHEN %s THEN 1 ELSE 0 END)::BIGINT AS n_null,
		sum(CASE WHEN %s THEN 1 ELSE 0 END)::BIGINT AS n_profiled,
		sum(CASE WHEN %s AND %s IS NULL THEN 1 ELSE 0 END)::BIGINT AS n_profiled_null,
		approx_count_distinct(CASE WHEN %s THEN %s END) AS n_distinct_non_null
		FROM t`, missingExpr, profiledExpr, profiledExpr, col, profiledNonNullExpr, col)

	var nNull, nProfiled, nProfiledNull, nDistinctNonNull int64
	if err := e.db.QueryRowContext(ctx, q).Scan(&nNull, &nProfiled, &nProfiledNull, &nDistinctNonNull); err != nil {
		return nil, err
	}
	nDistinct := nDistinctNonNull
	if nProfiledNull > 0 {
		nDistinct++
	}
	if nDistinct > nProfiled {
		nDistinct = nProfiled
	}

	total := e.totalRows

	summary := &types.ColumnSummary{
		MissingCount:   nNull,
		DistinctApprox: nDistinct,
		Loaded:         true,
	}

	if total > 0 {
		summary.MissingPct = float64(nNull) / float64(total) * 100
	}
	if nProfiled > 0 {
		summary.DistinctPct = float64(nDistinct) / float64(nProfiled) * 100
	}

	// Determine if discrete-like
	summary.IsDiscrete = nDistinct <= 200 || (nProfiled > 0 && float64(nDistinct)/float64(nProfiled) <= 0.05)

	return summary, nil
}

// ProfileDetail computes top values and/or numeric stats for a column.
func (e *Engine) ProfileDetail(ctx context.Context, colName string, summary *types.ColumnSummary, colType string, mode missing.Mode) error {
	col := quoteIdent(colName)
	profiledExpr := categoricalProfileExpr(col, mode)
	numericExpr := numericProfileExpr(col, mode)

	// Top values for discrete-like columns
	if summary.IsDiscrete {
		q := fmt.Sprintf(`SELECT %s::VARCHAR AS value, %s IS NULL AS is_null, count(*) AS cnt
			FROM t WHERE %s
			GROUP BY 1, 2 ORDER BY cnt DESC, is_null ASC, value ASC LIMIT 3`, col, col, profiledExpr)
		rows, err := e.db.QueryContext(ctx, q)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()

		profiledCount, err := e.categoricalProfiledValueCount(ctx, col, mode)
		if err != nil {
			return err
		}
		var top3 []types.TopValue
		for rows.Next() {
			var tv types.TopValue
			var rawValue sql.NullString
			var isNull bool
			if err := rows.Scan(&rawValue, &isNull, &tv.Count); err != nil {
				return err
			}
			if isNull {
				tv.Value = "⟨null⟩"
			} else {
				tv.Value = rawValue.String
			}
			if profiledCount > 0 {
				tv.Pct = float64(tv.Count) / float64(profiledCount) * 100
			}
			top3 = append(top3, tv)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		summary.Top3 = top3
	}

	// Numeric stats
	if isNumericType(colType) {
		q := fmt.Sprintf(`SELECT min(%s)::DOUBLE, max(%s)::DOUBLE, avg(%s)::DOUBLE, stddev_pop(%s)::DOUBLE
			FROM t WHERE %s`, col, col, col, col, numericExpr)
		var ns types.NumericStats
		if err := e.db.QueryRowContext(ctx, q).Scan(&ns.Min, &ns.Max, &ns.Mean, &ns.Stddev); err != nil {
			return err
		}
		summary.Numeric = &ns

		// Histogram
		if ns.Min != ns.Max {
			binExpr := fmt.Sprintf(
				`LEAST(10, GREATEST(1, CAST(FLOOR(((%s::DOUBLE - ?) / NULLIF(?, 0)) * 10) AS INTEGER) + 1))`,
				col,
			)
			hq := fmt.Sprintf(`SELECT %s AS b, count(*) AS cnt
				FROM t WHERE %s
				GROUP BY b ORDER BY b`, binExpr, numericExpr)
			hrows, err := e.db.QueryContext(ctx, hq, ns.Min, ns.Max-ns.Min)
			if err != nil {
				return err
			}
			defer func() { _ = hrows.Close() }()

			binWidth := (ns.Max - ns.Min) / 10.0
			bins := make([]types.HistBin, 10)
			for i := range bins {
				bins[i].Low = ns.Min + float64(i)*binWidth
				bins[i].High = ns.Min + float64(i+1)*binWidth
			}

			for hrows.Next() {
				var bucket sql.NullInt64
				var cnt int64
				if err := hrows.Scan(&bucket, &cnt); err != nil {
					return err
				}
				if bucket.Valid && bucket.Int64 >= 1 && bucket.Int64 <= 10 {
					bins[bucket.Int64-1].Count = cnt
				}
			}
			summary.Hist = &types.Histogram{Bins: bins}
		} else {
			profiledCount, err := e.numericProfiledValueCount(ctx, col, mode)
			if err != nil {
				return err
			}
			summary.Hist = &types.Histogram{
				Bins: []types.HistBin{{Low: ns.Min, High: ns.Max, Count: profiledCount}},
			}
		}
	}

	summary.DetailLoaded = true
	return nil
}

func (e *Engine) categoricalProfiledValueCount(ctx context.Context, quotedCol string, mode missing.Mode) (int64, error) {
	q := fmt.Sprintf(`SELECT count(*)::BIGINT FROM t WHERE %s`, categoricalProfileExpr(quotedCol, mode))
	var count int64
	err := e.db.QueryRowContext(ctx, q).Scan(&count)
	return count, err
}

func (e *Engine) numericProfiledValueCount(ctx context.Context, quotedCol string, mode missing.Mode) (int64, error) {
	q := fmt.Sprintf(`SELECT count(*)::BIGINT FROM t WHERE %s`, numericProfileExpr(quotedCol, mode))
	var count int64
	err := e.db.QueryRowContext(ctx, q).Scan(&count)
	return count, err
}

func categoricalProfileExpr(quotedCol string, mode missing.Mode) string {
	return "NOT (" + mode.SQLPredicate(quotedCol) + ")"
}

func numericProfileExpr(quotedCol string, mode missing.Mode) string {
	return categoricalProfileExpr(quotedCol, mode) + " AND " + quotedCol + " IS NOT NULL"
}

// FirstNullRow returns the internal row id of the first missing-like value in a column, or 0 if none.
func (e *Engine) FirstNullRow(ctx context.Context, colName string, filterCols []string, mode missing.Mode) (int64, error) {
	col := quoteIdent(colName)
	q := fmt.Sprintf("SELECT min(%s) FROM t_base WHERE %s", quoteIdent(e.internalRowIDCol), mode.SQLPredicate(col))
	if len(filterCols) > 0 {
		q += " AND (" + buildMissingFilter(filterCols, mode) + ")"
	}
	var rn sql.NullInt64
	if err := e.db.QueryRowContext(ctx, q).Scan(&rn); err != nil {
		return 0, err
	}
	if !rn.Valid {
		return 0, nil
	}
	return rn.Int64, nil
}

// OffsetForRowID returns the row offset of rowID in the active (optionally filtered) view.
func (e *Engine) OffsetForRowID(ctx context.Context, rowID int64, filterCols []string, mode missing.Mode) (int64, error) {
	if rowID <= 1 {
		return 0, nil
	}

	q := fmt.Sprintf("SELECT count(*) FROM t_base WHERE %s < ?", quoteIdent(e.internalRowIDCol))
	if len(filterCols) > 0 {
		q += " AND (" + buildMissingFilter(filterCols, mode) + ")"
	}

	var offset int64
	if err := e.db.QueryRowContext(ctx, q, rowID).Scan(&offset); err != nil {
		return 0, err
	}
	return offset, nil
}

// RowIDForOffset returns the internal row id at a filtered-view offset.
func (e *Engine) RowIDForOffset(ctx context.Context, offset int64, filterCols []string, mode missing.Mode) (int64, error) {
	if offset < 0 {
		return 0, nil
	}

	q := fmt.Sprintf("SELECT %s FROM t_base", quoteIdent(e.internalRowIDCol))
	if len(filterCols) > 0 {
		q += " WHERE (" + buildMissingFilter(filterCols, mode) + ")"
	}
	q += fmt.Sprintf(" ORDER BY %s LIMIT 1 OFFSET %d", quoteIdent(e.internalRowIDCol), offset)

	var rowID int64
	err := e.db.QueryRowContext(ctx, q).Scan(&rowID)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return rowID, nil
}

// NextNullRow returns the next missing-like row id in a column after rowID in the active
// (optionally filtered) view. When no later row exists, it wraps and returns the first one.
func (e *Engine) NextNullRow(ctx context.Context, colName string, filterCols []string, mode missing.Mode, rowID int64) (nextRowID int64, wrapped bool, err error) {
	col := quoteIdent(colName)
	conds := []string{mode.SQLPredicate(col)}
	if len(filterCols) > 0 {
		conds = append(conds, "("+buildMissingFilter(filterCols, mode)+")")
	}

	baseWhere := strings.Join(conds, " AND ")
	baseQuery := fmt.Sprintf("SELECT min(%s) FROM t_base WHERE %s", quoteIdent(e.internalRowIDCol), baseWhere)
	q := baseQuery
	if rowID > 0 {
		q += fmt.Sprintf(" AND %s > %d", quoteIdent(e.internalRowIDCol), rowID)
	}

	var rn sql.NullInt64
	if err := e.db.QueryRowContext(ctx, q).Scan(&rn); err != nil {
		return 0, false, err
	}
	if rn.Valid {
		return rn.Int64, false, nil
	}
	if rowID <= 0 {
		return 0, false, nil
	}

	qWrap := baseQuery
	if err := e.db.QueryRowContext(ctx, qWrap).Scan(&rn); err != nil {
		return 0, false, err
	}
	if !rn.Valid || rn.Int64 == rowID {
		return 0, false, nil
	}
	return rn.Int64, true, nil
}

// PrevNullRow returns the previous missing-like row id in a column before rowID in the active
// (optionally filtered) view. When no earlier row exists, it wraps and returns the last one.
func (e *Engine) PrevNullRow(ctx context.Context, colName string, filterCols []string, mode missing.Mode, rowID int64) (prevRowID int64, wrapped bool, err error) {
	col := quoteIdent(colName)
	conds := []string{mode.SQLPredicate(col)}
	if len(filterCols) > 0 {
		conds = append(conds, "("+buildMissingFilter(filterCols, mode)+")")
	}

	baseWhere := strings.Join(conds, " AND ")
	baseQuery := fmt.Sprintf("SELECT max(%s) FROM t_base WHERE %s", quoteIdent(e.internalRowIDCol), baseWhere)
	q := baseQuery
	if rowID > 0 {
		q += fmt.Sprintf(" AND %s < %d", quoteIdent(e.internalRowIDCol), rowID)
	}

	var rn sql.NullInt64
	if err := e.db.QueryRowContext(ctx, q).Scan(&rn); err != nil {
		return 0, false, err
	}
	if rn.Valid {
		return rn.Int64, false, nil
	}
	if rowID <= 0 {
		return 0, false, nil
	}

	qWrap := baseQuery
	if err := e.db.QueryRowContext(ctx, qWrap).Scan(&rn); err != nil {
		return 0, false, err
	}
	if !rn.Valid || rn.Int64 == rowID {
		return 0, false, nil
	}
	return rn.Int64, true, nil
}

// BuildMissingFilter builds a SQL WHERE clause for rows with missing-like values in the given columns.
func BuildMissingFilter(colNames []string, mode missing.Mode) string {
	if len(colNames) == 0 {
		return ""
	}
	return "(" + buildMissingFilter(colNames, mode) + ")"
}

// Close closes the DuckDB connection.
func (e *Engine) Close() error {
	return e.db.Close()
}

func isNumericType(t string) bool {
	return util.IsNumericDuckType(t)
}

func quoteIdent(name string) string {
	return "\"" + strings.ReplaceAll(name, "\"", "\"\"") + "\""
}

func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func uniqueInternalRowIDCol(db *sql.DB, sourceExpr string) (string, error) {
	rows, err := db.Query(fmt.Sprintf("DESCRIBE SELECT * FROM %s", sourceExpr))
	if err != nil {
		return "", err
	}
	defer func() { _ = rows.Close() }()

	used := make(map[string]struct{})
	for rows.Next() {
		var name, dtype string
		var null, key, def, extra sql.NullString
		if err := rows.Scan(&name, &dtype, &null, &key, &def, &extra); err != nil {
			return "", err
		}
		used[strings.ToLower(name)] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	base := "__pv_rowid"
	candidate := base
	for i := 1; ; i++ {
		if _, exists := used[strings.ToLower(candidate)]; !exists {
			return candidate, nil
		}
		candidate = fmt.Sprintf("%s_%d", base, i)
	}
}

func buildMissingFilter(colNames []string, mode missing.Mode) string {
	parts := make([]string, len(colNames))
	for i, c := range colNames {
		parts[i] = mode.SQLPredicate(quoteIdent(c))
	}
	return strings.Join(parts, " OR ")
}
