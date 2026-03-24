package ui

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode"

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
	opLike  predicateOp = "like"
	opNLike predicateOp = "nlike"
)

type columnPredicate struct {
	Column        string
	Op            predicateOp
	Value         string
	Value2        string
	Display       string
	Numeric       bool
	CaseSensitive bool
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

	trimmedLeft := strings.TrimLeft(raw, " \t")
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
		if !isNumeric {
			if candidate.op != opNeq {
				return columnPredicate{}, fmt.Errorf("comparisons require a numeric column")
			}
			return parseStringPredicate(colName, candidate.op, value)
		}
		normalized, err := normalizeNumericLiteral(value)
		if err != nil {
			return columnPredicate{}, err
		}
		return columnPredicate{
			Column:  colName,
			Op:      candidate.op,
			Value:   normalized,
			Display: strings.ReplaceAll(candidate.prefix, " ", "") + normalized,
			Numeric: true,
		}, nil
	}

	if !isNumeric {
		return parseStringPredicate(colName, opEq, raw)
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

func parseStringPredicate(colName string, op predicateOp, value string) (columnPredicate, error) {
	if hasUnescapedPercent(value) {
		pattern := normalizePredicatePattern(value)
		display := value
		predOp := opLike
		if op == opNeq {
			predOp = opNLike
			display = "!= " + value
		}
		return columnPredicate{
			Column:        colName,
			Op:            predOp,
			Value:         pattern,
			Display:       display,
			CaseSensitive: containsUpper(value),
		}, nil
	}

	decoded := decodePredicateStringLiteral(value)
	display := decoded
	if op == opNeq {
		display = "!= " + decoded
	}
	return columnPredicate{
		Column:  colName,
		Op:      op,
		Value:   decoded,
		Display: display,
	}, nil
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
	case opLike:
		return fmt.Sprintf(`%s %s %s ESCAPE '\'`, col, predicateLikeOp(pred.CaseSensitive, false), sqlLiteral(pred.Value))
	case opNLike:
		return fmt.Sprintf(`%s %s %s ESCAPE '\'`, col, predicateLikeOp(pred.CaseSensitive, true), sqlLiteral(pred.Value))
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

func decodePredicateStringLiteral(value string) string {
	var b strings.Builder
	escaped := false
	for _, r := range value {
		if escaped {
			switch r {
			case '%', '_', '\\':
				b.WriteRune(r)
			default:
				b.WriteRune('\\')
				b.WriteRune(r)
			}
			escaped = false
			continue
		}
		if r == '\\' {
			escaped = true
			continue
		}
		b.WriteRune(r)
	}
	if escaped {
		b.WriteRune('\\')
	}
	return b.String()
}

func normalizePredicatePattern(value string) string {
	var b strings.Builder
	escaped := false
	for _, r := range value {
		if escaped {
			switch r {
			case '%':
				b.WriteString(`\%`)
			case '_':
				b.WriteString(`\_`)
			case '\\':
				b.WriteString(`\\`)
			default:
				b.WriteString(`\\`)
				b.WriteRune(r)
			}
			escaped = false
			continue
		}
		switch r {
		case '%':
			b.WriteRune('%')
		case '_':
			b.WriteString(`\_`)
		case '\\':
			escaped = true
		default:
			b.WriteRune(r)
		}
	}
	if escaped {
		b.WriteString(`\\`)
	}
	return b.String()
}

func hasUnescapedPercent(value string) bool {
	escaped := false
	for _, r := range value {
		if escaped {
			escaped = false
			continue
		}
		if r == '\\' {
			escaped = true
			continue
		}
		if r == '%' {
			return true
		}
	}
	return false
}

func containsUpper(value string) bool {
	for _, r := range value {
		if unicode.IsUpper(r) {
			return true
		}
	}
	return false
}

func predicateLikeOp(caseSensitive, negated bool) string {
	keyword := "ILIKE"
	if caseSensitive {
		keyword = "LIKE"
	}
	if negated {
		return "NOT " + keyword
	}
	return keyword
}

func escapePredicatePromptLiteral(value string) string {
	var b strings.Builder
	for _, r := range value {
		switch r {
		case '%', '_', '\\':
			b.WriteRune('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

func predicatePromptValue(pred columnPredicate) string {
	if pred.Numeric {
		return pred.Display
	}
	switch pred.Op {
	case opEq:
		return escapePredicatePromptLiteral(pred.Value)
	case opNeq:
		return "!= " + escapePredicatePromptLiteral(pred.Value)
	default:
		return pred.Display
	}
}
