package util

import (
	"strings"
	"unicode"
)

// FuzzyMatch returns true when all query terms match the identifier.
// Matching is case-insensitive and supports fuzzy matches across
// underscore/camel-case components.
func FuzzyMatch(s, query string) bool {
	query = strings.TrimSpace(strings.ToLower(query))
	if query == "" {
		return true
	}

	parts := splitIdentifierParts(s)
	joined := strings.Join(parts, "")
	sNorm := strings.ToLower(s)
	acronym := componentAcronym(parts)

	for _, term := range strings.Fields(query) {
		if len(splitIdentifierParts(term)) == 0 {
			continue
		}
		if !matchTerm(term, sNorm, joined, acronym, parts) {
			return false
		}
	}
	return true
}

func matchTerm(term, sNorm, joined, acronym string, parts []string) bool {
	for _, token := range splitIdentifierParts(term) {
		if !matchToken(token, sNorm, joined, acronym) {
			return false
		}
	}
	return true
}

func matchToken(token, sNorm, joined, acronym string) bool {
	if strings.Contains(sNorm, token) || strings.Contains(joined, token) {
		return true
	}
	if acronym != "" && strings.Contains(acronym, token) {
		return true
	}
	return false
}

func componentAcronym(parts []string) string {
	var b strings.Builder
	for _, p := range parts {
		runes := []rune(p)
		if len(runes) == 0 {
			continue
		}
		b.WriteRune(runes[0])
	}
	return b.String()
}

func splitIdentifierParts(s string) []string {
	var normalized strings.Builder
	var prev rune
	for _, r := range s {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			normalized.WriteRune(' ')
			prev = 0
			continue
		}

		if prev != 0 {
			camelBoundary := unicode.IsUpper(r) && (unicode.IsLower(prev) || unicode.IsDigit(prev))
			digitBoundary := unicode.IsDigit(r) && unicode.IsLetter(prev)
			letterBoundary := unicode.IsLetter(r) && unicode.IsDigit(prev)
			if camelBoundary || digitBoundary || letterBoundary {
				normalized.WriteRune(' ')
			}
		}
		normalized.WriteRune(unicode.ToLower(r))
		prev = r
	}
	return strings.Fields(normalized.String())
}
