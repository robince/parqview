package ui

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/robince/parqview/internal/util"
)

type predicateOp string

const (
	opEq    predicateOp = "eq"
	opNeq   predicateOp = "neq"
	opGt    predicateOp = "gt"
	opGte   predicateOp = "gte"
	opLt    predicateOp = "lt"
	opLte   predicateOp = "lte"
	opRange predicateOp = "range"
)

type columnPredicate struct {
	Column  string
	Op      predicateOp
	Value   string
	Value2  string
	Display string
	Numeric bool
}

func parseColumnPredicate(colName, colType, raw string) (columnPredicate, error) {
	if strings.TrimSpace(raw) == "" {
		return columnPredicate{}, nil
	}

	isNumeric := util.IsNumericDuckType(colType)
	text := strings.TrimSpace(raw)

	if isNumeric {
		if pred, ok, err := parseRangePredicate(colName, text); ok || err != nil {
			return pred, err
		}
	}

	for _, candidate := range []struct {
		prefix string
		op     predicateOp
	}{
		{prefix: "!=", op: opNeq},
		{prefix: ">=", op: opGte},
		{prefix: "<=", op: opLte},
		{prefix: ">", op: opGt},
		{prefix: "<", op: opLt},
	} {
		trimmedLeft := strings.TrimLeft(raw, " \t")
		if !strings.HasPrefix(trimmedLeft, candidate.prefix) {
			continue
		}
		value := strings.TrimPrefix(trimmedLeft, candidate.prefix)
		if len(value) > 0 && value[0] == ' ' {
			value = value[1:]
		}
		if value == "" {
			return columnPredicate{}, fmt.Errorf("missing value")
		}
		if candidate.op != opNeq && !isNumeric {
			return columnPredicate{}, fmt.Errorf("comparisons require a numeric column")
		}
		if isNumeric {
			normalized, err := normalizeNumericLiteral(value)
			if err != nil {
				return columnPredicate{}, err
			}
			value = normalized
		}
		return columnPredicate{
			Column:  colName,
			Op:      candidate.op,
			Value:   value,
			Display: strings.ReplaceAll(candidate.prefix, " ", "") + value,
			Numeric: isNumeric,
		}, nil
	}

	return exactMatchPredicate(colName, colType, raw)
}

func parseRangePredicate(colName, text string) (columnPredicate, bool, error) {
	left, right, ok := strings.Cut(text, "..")
	if !ok {
		return columnPredicate{}, false, nil
	}
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	if left == "" || right == "" {
		return columnPredicate{}, true, fmt.Errorf("range requires both bounds")
	}
	nLeft, err := normalizeNumericLiteral(left)
	if err != nil {
		return columnPredicate{}, true, err
	}
	nRight, err := normalizeNumericLiteral(right)
	if err != nil {
		return columnPredicate{}, true, err
	}
	return columnPredicate{
		Column:  colName,
		Op:      opRange,
		Value:   nLeft,
		Value2:  nRight,
		Display: nLeft + ".." + nRight,
		Numeric: true,
	}, true, nil
}

func normalizeNumericLiteral(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", fmt.Errorf("missing numeric value")
	}
	if _, err := strconv.ParseFloat(trimmed, 64); err != nil {
		return "", fmt.Errorf("invalid numeric value %q", trimmed)
	}
	return trimmed, nil
}

func predicateSQL(pred columnPredicate) string {
	col := quoteIdentSQL(pred.Column)
	switch pred.Op {
	case opEq:
		return col + " = " + predicateValueSQL(pred)
	case opNeq:
		return col + " != " + predicateValueSQL(pred)
	case opGt:
		return col + " > " + pred.Value
	case opGte:
		return col + " >= " + pred.Value
	case opLt:
		return col + " < " + pred.Value
	case opLte:
		return col + " <= " + pred.Value
	case opRange:
		return fmt.Sprintf("(%s >= %s AND %s <= %s)", col, pred.Value, col, pred.Value2)
	default:
		return ""
	}
}

func predicateValueSQL(pred columnPredicate) string {
	if pred.Numeric {
		return pred.Value
	}
	return sqlLiteral(pred.Value)
}

func exactMatchPredicate(colName, colType, value string) (columnPredicate, error) {
	pred := columnPredicate{
		Column:  colName,
		Op:      opEq,
		Value:   value,
		Display: value,
		Numeric: util.IsNumericDuckType(colType),
	}
	if pred.Numeric {
		normalized, err := normalizeNumericLiteral(value)
		if err != nil {
			return columnPredicate{}, err
		}
		pred.Value = normalized
		pred.Display = normalized
	}
	return pred, nil
}

func buildPredicateFilter(predicates map[string]columnPredicate) string {
	if len(predicates) == 0 {
		return ""
	}
	keys := make([]string, 0, len(predicates))
	for col := range predicates {
		keys = append(keys, col)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, col := range keys {
		if sql := predicateSQL(predicates[col]); sql != "" {
			parts = append(parts, sql)
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return "(" + strings.Join(parts, " AND ") + ")"
}

func predicateSummary(predicates map[string]columnPredicate) string {
	if len(predicates) == 0 {
		return ""
	}
	keys := make([]string, 0, len(predicates))
	for col := range predicates {
		keys = append(keys, col)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, col := range keys {
		pred := predicates[col]
		if pred.Display == "" {
			continue
		}
		display := pred.Display
		if pred.Op == opEq {
			display = "=" + display
		}
		parts = append(parts, fmt.Sprintf("%s%s", col, display))
	}
	return strings.Join(parts, " ")
}

func quoteIdentSQL(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func sqlLiteral(value string) string {
	return `'` + strings.ReplaceAll(value, `'`, `''`) + `'`
}
