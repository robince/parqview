package missing

import "strings"

// Mode controls which values are treated as missing.
type Mode uint8

// NOTE: internal/ui/styles.go indexes style-cache arrays by these ordinal values (0–2).
// If you reorder, rename, or insert modes here, update the compile-time assertions in styles.go.
const (
	ModeNullAndNaN Mode = iota
	ModeNullOnly
	ModeNaNOnly
)

// Next returns the next missing mode in the cycle order used by the UI toggle.
func (m Mode) Next() Mode {
	switch m {
	case ModeNullOnly:
		return ModeNaNOnly
	case ModeNaNOnly:
		return ModeNullAndNaN
	default:
		return ModeNullOnly
	}
}

// Label returns the user-facing long label for the mode.
func (m Mode) Label() string {
	switch m {
	case ModeNullOnly:
		return "NULL only"
	case ModeNaNOnly:
		return "NaN only"
	default:
		return "NULL+NaN"
	}
}

// ShortLabel returns a compact label for narrow UI layouts.
func (m Mode) ShortLabel() string {
	switch m {
	case ModeNullOnly:
		return "NULL"
	case ModeNaNOnly:
		return "NaN"
	default:
		return "NULL+NaN"
	}
}

// SQLNaNPredicate returns a SQL fragment that evaluates true for NaN values.
// The quotedIdent argument must already be a quoted SQL identifier.
func SQLNaNPredicate(quotedIdent string) string {
	return "coalesce(isnan(TRY_CAST(" + quotedIdent + " AS DOUBLE)), false)"
}

// SQLPredicate returns the SQL predicate for a missing-like value for a column.
// The quotedIdent argument must already be a quoted SQL identifier.
func (m Mode) SQLPredicate(quotedIdent string) string {
	switch m {
	case ModeNullOnly:
		return quotedIdent + " IS NULL"
	case ModeNaNOnly:
		return SQLNaNPredicate(quotedIdent)
	default:
		return "(" + quotedIdent + " IS NULL OR " + SQLNaNPredicate(quotedIdent) + ")"
	}
}

// IsDisplayMissing reports whether a rendered table cell should be treated as missing.
func (m Mode) IsDisplayMissing(v string) bool {
	isNull := v == "NULL"
	isNaN := strings.EqualFold(strings.TrimSpace(v), "nan")
	switch m {
	case ModeNullOnly:
		return isNull
	case ModeNaNOnly:
		return isNaN
	default:
		return isNull || isNaN
	}
}
