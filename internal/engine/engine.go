package engine

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/robince/parqview/internal/missing"
	"github.com/robince/parqview/internal/types"
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
func (e *Engine) ProfileBasic(ctx context.Context, colName string) (*types.ColumnSummary, error) {
	col := quoteIdent(colName)
	missingExpr := missing.SQLPredicate(col)
	q := fmt.Sprintf(`SELECT
		sum(CASE WHEN %s THEN 1 ELSE 0 END)::BIGINT AS n_null,
		approx_count_distinct(%s) AS n_distinct
		FROM t`, missingExpr, col)

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
	nonMissingExpr := "NOT (" + missing.SQLPredicate(col) + ")"

	// Top values for discrete-like columns
	if summary.IsDiscrete {
		q := fmt.Sprintf(`SELECT %s::VARCHAR AS value, count(*) AS cnt
			FROM t WHERE %s
			GROUP BY %s ORDER BY cnt DESC LIMIT 3`, col, nonMissingExpr, col)
		rows, err := e.db.QueryContext(ctx, q)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()

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
			FROM t WHERE %s`, col, col, col, col, nonMissingExpr)
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
				GROUP BY b ORDER BY b`, binExpr, nonMissingExpr)
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

// FirstNullRow returns the internal row id of the first missing-like value in a column, or 0 if none.
// rowFilter must be empty or produced by BuildNullFilter.
func (e *Engine) FirstNullRow(ctx context.Context, colName, rowFilter string) (int64, error) {
	col := quoteIdent(colName)
	q := fmt.Sprintf("SELECT min(%s) FROM t_base WHERE %s", quoteIdent(e.internalRowIDCol), missing.SQLPredicate(col))
	filterCols, err := parseNullFilterColumns(rowFilter)
	if err != nil {
		return 0, err
	}
	if len(filterCols) > 0 {
		q += " AND (" + buildNullFilter(filterCols) + ")"
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
// rowFilter must be empty or produced by BuildNullFilter.
func (e *Engine) OffsetForRowID(ctx context.Context, rowID int64, rowFilter string) (int64, error) {
	if rowID <= 1 {
		return 0, nil
	}

	q := fmt.Sprintf("SELECT count(*) FROM t_base WHERE %s < ?", quoteIdent(e.internalRowIDCol))
	filterCols, err := parseNullFilterColumns(rowFilter)
	if err != nil {
		return 0, err
	}
	if len(filterCols) > 0 {
		q += " AND (" + buildNullFilter(filterCols) + ")"
	}

	var offset int64
	if err := e.db.QueryRowContext(ctx, q, rowID).Scan(&offset); err != nil {
		return 0, err
	}
	return offset, nil
}

// RowIDForOffset returns the internal row id at a filtered-view offset.
// rowFilter must be empty or produced by BuildNullFilter.
func (e *Engine) RowIDForOffset(ctx context.Context, offset int64, rowFilter string) (int64, error) {
	if offset < 0 {
		return 0, nil
	}

	q := fmt.Sprintf("SELECT %s FROM t_base", quoteIdent(e.internalRowIDCol))
	filterCols, err := parseNullFilterColumns(rowFilter)
	if err != nil {
		return 0, err
	}
	if len(filterCols) > 0 {
		q += " WHERE (" + buildNullFilter(filterCols) + ")"
	}
	q += fmt.Sprintf(" ORDER BY %s LIMIT 1 OFFSET %d", quoteIdent(e.internalRowIDCol), offset)

	var rowID int64
	err = e.db.QueryRowContext(ctx, q).Scan(&rowID)
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
// rowFilter must be empty or produced by BuildNullFilter.
func (e *Engine) NextNullRow(ctx context.Context, colName, rowFilter string, rowID int64) (nextRowID int64, wrapped bool, err error) {
	col := quoteIdent(colName)
	conds := []string{missing.SQLPredicate(col)}
	filterCols, err := parseNullFilterColumns(rowFilter)
	if err != nil {
		return 0, false, err
	}
	if len(filterCols) > 0 {
		conds = append(conds, "("+buildNullFilter(filterCols)+")")
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
// rowFilter must be empty or produced by BuildNullFilter.
func (e *Engine) PrevNullRow(ctx context.Context, colName, rowFilter string, rowID int64) (prevRowID int64, wrapped bool, err error) {
	col := quoteIdent(colName)
	conds := []string{missing.SQLPredicate(col)}
	filterCols, err := parseNullFilterColumns(rowFilter)
	if err != nil {
		return 0, false, err
	}
	if len(filterCols) > 0 {
		conds = append(conds, "("+buildNullFilter(filterCols)+")")
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

// BuildNullFilter builds a SQL WHERE clause for rows with missing-like values in the given columns.
func BuildNullFilter(colNames []string) string {
	if len(colNames) == 0 {
		return ""
	}
	return "(" + buildNullFilter(colNames) + ")"
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

func buildNullFilter(colNames []string) string {
	parts := make([]string, len(colNames))
	for i, c := range colNames {
		parts[i] = missing.SQLPredicate(quoteIdent(c))
	}
	return strings.Join(parts, " OR ")
}

func parseNullFilterColumns(rowFilter string) ([]string, error) {
	filter := strings.TrimSpace(rowFilter)
	if filter == "" {
		return nil, nil
	}
	if len(filter) < 2 || filter[0] != '(' || filter[len(filter)-1] != ')' {
		return nil, fmt.Errorf("unsupported null row filter format")
	}
	inner := strings.TrimSpace(filter[1 : len(filter)-1])
	if inner == "" {
		return nil, nil
	}

	rawParts, err := splitByOrOutsideQuotedIdent(inner)
	if err != nil {
		return nil, fmt.Errorf("unsupported null row filter format")
	}
	colNames := make([]string, 0, len(rawParts))
	for _, part := range rawParts {
		part = strings.TrimSpace(stripOuterParens(part))
		if part == "" {
			return nil, fmt.Errorf("unsupported null row filter term")
		}

		ident, remainder, ok := splitLeadingQuotedIdent(part)
		if !ok {
			return nil, fmt.Errorf("unsupported null row filter identifier")
		}
		remainder = strings.TrimSpace(remainder)
		if !strings.HasPrefix(remainder, "IS NULL") {
			return nil, fmt.Errorf("unsupported null row filter term")
		}
		tail := remainder[len("IS NULL"):]
		if tail != "" {
			r, _ := utf8.DecodeRuneInString(tail)
			if r == utf8.RuneError || !unicode.IsSpace(r) {
				return nil, fmt.Errorf("unsupported null row filter term")
			}
		}
		remainder = strings.TrimSpace(tail)

		col, ok := unquoteIdent(ident)
		if !ok {
			return nil, fmt.Errorf("unsupported null row filter identifier")
		}
		if remainder != "" {
			expected := "OR " + missing.SQLNaNPredicate(ident)
			if remainder != expected {
				return nil, fmt.Errorf("unsupported null row filter term")
			}
		}
		colNames = append(colNames, col)
	}
	return colNames, nil
}

func splitByOrOutsideQuotedIdent(s string) ([]string, error) {
	const sep = " OR "

	var (
		parts    []string
		start    int
		inQuotes bool
		depth    int
	)
	for i := 0; i < len(s); i++ {
		if s[i] == '"' {
			if inQuotes && i+1 < len(s) && s[i+1] == '"' {
				i++
				continue
			}
			inQuotes = !inQuotes
			continue
		}
		if inQuotes {
			continue
		}
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return nil, fmt.Errorf("unbalanced parentheses")
			}
		}
		if depth == 0 && i+len(sep) <= len(s) && s[i:i+len(sep)] == sep {
			parts = append(parts, s[start:i])
			i += len(sep) - 1
			start = i + 1
		}
	}
	if inQuotes || depth != 0 {
		return nil, fmt.Errorf("unbalanced filter")
	}
	parts = append(parts, s[start:])
	return parts, nil
}

func splitLeadingQuotedIdent(s string) (ident, remainder string, ok bool) {
	if s == "" || s[0] != '"' {
		return "", "", false
	}
	for i := 1; i < len(s); i++ {
		if s[i] != '"' {
			continue
		}
		if i+1 < len(s) && s[i+1] == '"' {
			i++
			continue
		}
		return s[:i+1], s[i+1:], true
	}
	return "", "", false
}

func stripOuterParens(s string) string {
	s = strings.TrimSpace(s)
	for len(s) >= 2 && s[0] == '(' && s[len(s)-1] == ')' {
		depth := 0
		inQuotes := false
		matches := true
		for i := 0; i < len(s); i++ {
			ch := s[i]
			if ch == '"' {
				if inQuotes && i+1 < len(s) && s[i+1] == '"' {
					i++
					continue
				}
				inQuotes = !inQuotes
				continue
			}
			if inQuotes {
				continue
			}
			switch ch {
			case '(':
				depth++
			case ')':
				depth--
				if depth < 0 {
					return s
				}
				if depth == 0 && i != len(s)-1 {
					matches = false
				}
			}
		}
		if inQuotes || depth != 0 || !matches {
			return s
		}
		s = strings.TrimSpace(s[1 : len(s)-1])
	}
	return s
}

func unquoteIdent(ident string) (string, bool) {
	if len(ident) < 2 || ident[0] != '"' || ident[len(ident)-1] != '"' {
		return "", false
	}
	inner := ident[1 : len(ident)-1]
	for i := 0; i < len(inner); i++ {
		if inner[i] == '"' {
			if i+1 < len(inner) && inner[i+1] == '"' {
				i++
				continue
			}
			return "", false
		}
	}
	return strings.ReplaceAll(inner, `""`, `"`), true
}
