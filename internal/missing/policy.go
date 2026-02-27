package missing

import "strings"

// IncludeNaNAsMissing controls whether NaN values are treated as missing.
// Default is true so missing-data workflows include both NULL and NaN.
const IncludeNaNAsMissing = true

// SQLNaNPredicate returns a SQL fragment that evaluates true for NaN values.
// The quotedIdent argument must already be a quoted SQL identifier.
func SQLNaNPredicate(quotedIdent string) string {
	return "coalesce(isnan(TRY_CAST(" + quotedIdent + " AS DOUBLE)), false)"
}

// SQLPredicate returns the SQL predicate for a missing-like value for a column.
// The quotedIdent argument must already be a quoted SQL identifier.
func SQLPredicate(quotedIdent string) string {
	if !IncludeNaNAsMissing {
		return quotedIdent + " IS NULL"
	}
	return "(" + quotedIdent + " IS NULL OR " + SQLNaNPredicate(quotedIdent) + ")"
}

// IsDisplayMissing reports whether a rendered table cell should be treated as missing.
func IsDisplayMissing(v string) bool {
	if v == "NULL" {
		return true
	}
	if !IncludeNaNAsMissing {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(v), "nan")
}
