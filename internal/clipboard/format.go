package clipboard

import "strings"

// FormatPythonList formats column names as a Python list literal.
// e.g. ["col_a", "col_b", "col_c"]
func FormatPythonList(names []string) string {
	if len(names) == 0 {
		return "[]"
	}
	var b strings.Builder
	b.WriteString("[")
	for i, name := range names {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("\"")
		b.WriteString(escapePython(name))
		b.WriteString("\"")
	}
	b.WriteString("]")
	return b.String()
}

func escapePython(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "\"", "\\\"")
	s = strings.ReplaceAll(s, "\n", "\\n")
	s = strings.ReplaceAll(s, "\r", "\\r")
	s = strings.ReplaceAll(s, "\t", "\\t")
	return s
}
