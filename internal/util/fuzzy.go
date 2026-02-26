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
		if !matchTerm(term, sNorm, joined, acronym) {
			return false
		}
	}
	return true
}

func matchTerm(term, sNorm, joined, acronym string) bool {
	tokens := splitIdentifierParts(term)
	for _, token := range tokens {
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
	runes := []rune(s)
	n := len(runes)
	var normalized strings.Builder
	var prev rune
	for i, r := range runes {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			normalized.WriteRune(' ')
			prev = 0
			continue
		}

		if prev != 0 {
			camelBoundary := unicode.IsUpper(r) && (unicode.IsLower(prev) || unicode.IsDigit(prev))
			// Consecutive uppercase followed by lowercase: HTMLParser → html + parser
			uppersToLower := unicode.IsUpper(r) && unicode.IsUpper(prev) && i+1 < n && unicode.IsLower(runes[i+1])
			digitBoundary := unicode.IsDigit(r) && unicode.IsLetter(prev)
			letterBoundary := unicode.IsLetter(r) && unicode.IsDigit(prev)
			if camelBoundary || uppersToLower || digitBoundary || letterBoundary {
				normalized.WriteRune(' ')
			}
		}
		normalized.WriteRune(unicode.ToLower(r))
		prev = r
	}
	return strings.Fields(normalized.String())
}
