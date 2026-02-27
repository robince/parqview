package util

import "strings"

// DuckTypeBase normalizes a DuckDB type name to its base token.
// Examples: "DECIMAL(10,2)" -> "DECIMAL", "DOUBLE PRECISION" -> "DOUBLE".
func DuckTypeBase(colType string) string {
	t := strings.TrimSpace(strings.ToUpper(colType))
	if t == "" {
		return ""
	}
	base := strings.Fields(t)[0]
	if idx := strings.Index(base, "("); idx >= 0 {
		base = base[:idx]
	}
	return base
}

// IsNumericDuckType reports whether a DuckDB type should be treated as numeric.
func IsNumericDuckType(colType string) bool {
	switch DuckTypeBase(colType) {
	case "TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT", "HUGEINT",
		"UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT",
		"FLOAT", "REAL", "DOUBLE", "DECIMAL", "NUMERIC", "FLOAT4", "FLOAT8":
		return true
	default:
		return false
	}
}
