package engine

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/robince/parqview/internal/types"
)

// Engine wraps a DuckDB connection for querying a data file.
type Engine struct {
	db               *sql.DB
	totalRows        int64
	columns          []types.ColumnInfo
	internalRowIDCol string
}

// New creates a new engine, opens the file and creates a view.
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
		db.Close()
		return nil, fmt.Errorf("unsupported file extension: %s", ext)
	}

	internalRowIDCol, err := uniqueInternalRowIDCol(db, sourceExpr)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("resolve internal row id column: %w", err)
	}

	query := fmt.Sprintf(`
		CREATE TABLE t_base AS
		SELECT row_number() OVER ()::BIGINT AS %s, * FROM %s;
		CREATE VIEW t AS
		SELECT * EXCLUDE (%s) FROM t_base;
	`, quoteIdent(internalRowIDCol), sourceExpr, quoteIdent(internalRowIDCol))

	if _, err := db.Exec(query); err != nil {
		db.Close()
		return nil, fmt.Errorf("create view: %w", err)
	}

	e := &Engine{db: db, internalRowIDCol: internalRowIDCol}

	if err := e.loadSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("load schema: %w", err)
	}

	if err := e.loadRowCount(); err != nil {
		db.Close()
		return nil, fmt.Errorf("count rows: %w", err)
	}

	return e, nil
}

func (e *Engine) loadSchema() error {
	rows, err := e.db.Query("DESCRIBE t")
	if err != nil {
		return err
	}
	defer rows.Close()

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
	if len(colNames) == 0 {
		return nil, nil
	}

	var proj strings.Builder
	for i, c := range colNames {
		if i > 0 {
			proj.WriteString(", ")
		}
		proj.WriteString(quoteIdent(c))
	}

	q := fmt.Sprintf("SELECT %s FROM t_base", proj.String())
	if rowFilter != "" {
		q += " WHERE " + rowFilter
	}
	q += fmt.Sprintf(" ORDER BY %s LIMIT %d OFFSET %d", quoteIdent(e.internalRowIDCol), limit, offset)

	rows, err := e.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	nCols := len(colNames)
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
func (e *Engine) ProfileBasic(ctx context.Context, colName string) (*types.ColumnSummary, error) {
	col := quoteIdent(colName)
	q := fmt.Sprintf(`SELECT
		sum(CASE WHEN %s IS NULL THEN 1 ELSE 0 END)::BIGINT AS n_null,
		approx_count_distinct(%s) AS n_distinct
		FROM t`, col, col)

	var nNull, nDistinct int64
	if err := e.db.QueryRowContext(ctx, q).Scan(&nNull, &nDistinct); err != nil {
		return nil, err
	}

	total := e.totalRows
	nonNull := total - nNull

	summary := &types.ColumnSummary{
		MissingCount:   nNull,
		DistinctApprox: nDistinct,
		Loaded:         true,
	}

	if total > 0 {
		summary.MissingPct = float64(nNull) / float64(total) * 100
	}
	if nonNull > 0 {
		summary.DistinctPct = float64(nDistinct) / float64(nonNull) * 100
	}

	// Determine if discrete-like
	summary.IsDiscrete = nDistinct <= 200 || (nonNull > 0 && float64(nDistinct)/float64(nonNull) <= 0.05)

	return summary, nil
}

// ProfileDetail computes top values and/or numeric stats for a column.
func (e *Engine) ProfileDetail(ctx context.Context, colName string, summary *types.ColumnSummary, colType string) error {
	col := quoteIdent(colName)

	// Top values for discrete-like columns
	if summary.IsDiscrete {
		q := fmt.Sprintf(`SELECT %s::VARCHAR AS value, count(*) AS cnt
			FROM t WHERE %s IS NOT NULL
			GROUP BY %s ORDER BY cnt DESC LIMIT 3`, col, col, col)
		rows, err := e.db.QueryContext(ctx, q)
		if err != nil {
			return err
		}
		defer rows.Close()

		nonNull := e.totalRows - summary.MissingCount
		var top3 []types.TopValue
		for rows.Next() {
			var tv types.TopValue
			if err := rows.Scan(&tv.Value, &tv.Count); err != nil {
				return err
			}
			if nonNull > 0 {
				tv.Pct = float64(tv.Count) / float64(nonNull) * 100
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
			FROM t WHERE %s IS NOT NULL`, col, col, col, col, col)
		var ns types.NumericStats
		if err := e.db.QueryRowContext(ctx, q).Scan(&ns.Min, &ns.Max, &ns.Mean, &ns.Stddev); err != nil {
			return err
		}
		summary.Numeric = &ns

		// Histogram
		if ns.Min != ns.Max {
			binExpr := fmt.Sprintf(
				`LEAST(10, GREATEST(1, CAST(FLOOR(((%s::DOUBLE - %f) / NULLIF(%f, 0)) * 10) AS INTEGER) + 1))`,
				col, ns.Min, ns.Max-ns.Min,
			)
			hq := fmt.Sprintf(`SELECT %s AS b, count(*) AS cnt
				FROM t WHERE %s IS NOT NULL
				GROUP BY b ORDER BY b`, binExpr, col)
			hrows, err := e.db.QueryContext(ctx, hq)
			if err != nil {
				return err
			}
			defer hrows.Close()

			binWidth := (ns.Max - ns.Min) / 10.0
			bins := make([]types.HistBin, 10)
			for i := range bins {
				bins[i].Low = ns.Min + float64(i)*binWidth
				bins[i].High = ns.Min + float64(i+1)*binWidth
			}

			for hrows.Next() {
				var bucket int
				var cnt int64
				if err := hrows.Scan(&bucket, &cnt); err != nil {
					return err
				}
				if bucket >= 1 && bucket <= 10 {
					bins[bucket-1].Count = cnt
				}
			}
			summary.Hist = &types.Histogram{Bins: bins}
		} else {
			// Single bin for constant column
			nonNull := e.totalRows - summary.MissingCount
			summary.Hist = &types.Histogram{
				Bins: []types.HistBin{{Low: ns.Min, High: ns.Max, Count: nonNull}},
			}
		}
	}

	summary.DetailLoaded = true
	return nil
}

// FirstNullRow returns the internal row id of the first null in a column, or 0 if none.
func (e *Engine) FirstNullRow(ctx context.Context, colName, rowFilter string) (int64, error) {
	col := quoteIdent(colName)
	q := fmt.Sprintf("SELECT min(%s) FROM t_base WHERE %s IS NULL", quoteIdent(e.internalRowIDCol), col)
	if rowFilter != "" {
		q += " AND (" + rowFilter + ")"
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
func (e *Engine) OffsetForRowID(ctx context.Context, rowID int64, rowFilter string) (int64, error) {
	if rowID <= 1 {
		return 0, nil
	}

	q := fmt.Sprintf("SELECT count(*) FROM t_base WHERE %s < ?", quoteIdent(e.internalRowIDCol))
	if rowFilter != "" {
		q += " AND (" + rowFilter + ")"
	}

	var offset int64
	if err := e.db.QueryRowContext(ctx, q, rowID).Scan(&offset); err != nil {
		return 0, err
	}
	return offset, nil
}

// BuildNullFilter builds a SQL WHERE clause for rows with nulls in the given columns.
func BuildNullFilter(colNames []string) string {
	if len(colNames) == 0 {
		return ""
	}
	parts := make([]string, len(colNames))
	for i, c := range colNames {
		parts[i] = quoteIdent(c) + " IS NULL"
	}
	return "(" + strings.Join(parts, " OR ") + ")"
}

// Close closes the DuckDB connection.
func (e *Engine) Close() error {
	return e.db.Close()
}

func isNumericType(t string) bool {
	t = strings.TrimSpace(strings.ToUpper(t))
	if t == "" {
		return false
	}
	base := strings.Fields(t)[0]
	if idx := strings.Index(base, "("); idx >= 0 {
		base = base[:idx]
	}
	switch base {
	case "TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT", "HUGEINT",
		"UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT",
		"FLOAT", "REAL", "DOUBLE", "DECIMAL", "NUMERIC":
		return true
	default:
		return false
	}
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
	defer rows.Close()

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
