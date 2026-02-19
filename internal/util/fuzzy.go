package util

import "strings"

// FuzzyMatch returns true if query is a case-insensitive substring of s.
func FuzzyMatch(s, query string) bool {
	if query == "" {
		return true
	}
	return strings.Contains(strings.ToLower(s), strings.ToLower(query))
}
