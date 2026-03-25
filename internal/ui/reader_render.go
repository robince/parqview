package ui

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"
)

type readerMode int

const (
	readerModeRaw readerMode = iota
	readerModeJSONPretty
)

func (m readerMode) label() string {
	switch m {
	case readerModeJSONPretty:
		return "json pretty"
	default:
		return "raw"
	}
}

func readerModeBadge(mode readerMode) string {
	switch mode {
	case readerModeJSONPretty:
		return readerModeJSONBadgeStyle.Render("JSON")
	default:
		return readerModeRawBadgeStyle.Render("RAW")
	}
}

func readerModesForValue(value string) []readerMode {
	modes := []readerMode{readerModeRaw}
	if isJSONLikeValid(value) {
		modes = append(modes, readerModeJSONPretty)
	}
	return modes
}

func defaultReaderMode(modes []readerMode) readerMode {
	for _, mode := range modes {
		if mode == readerModeJSONPretty {
			return readerModeJSONPretty
		}
	}
	return readerModeRaw
}

func defaultReaderWrapForMode(mode readerMode, value string) bool {
	if mode == readerModeJSONPretty {
		return false
	}
	return defaultReaderWrap(value)
}

type readerRenderData struct {
	logicalLines []string
	ansiAware    bool
}

func newReaderRenderData(value string, mode readerMode) readerRenderData {
	if mode == readerModeJSONPretty {
		if pretty, ok := prettyJSONReaderValue(value); ok {
			return readerRenderData{
				logicalLines: highlightPrettyJSON(pretty),
				ansiAware:    true,
			}
		}
	}
	return readerRenderData{logicalLines: sanitizeMultilineLogicalLines(value)}
}

func (d readerRenderData) renderedLines(bodyW int, wrap bool, horizOff int) []string {
	if d.ansiAware {
		if wrap {
			return wrapANSILogicalLines(d.logicalLines, bodyW)
		}
		return sliceANSILogicalLines(d.logicalLines, horizOff, bodyW)
	}
	if wrap {
		return wrapLogicalLines(d.logicalLines, bodyW)
	}
	return sliceLogicalLines(d.logicalLines, horizOff, bodyW)
}

func (d readerRenderData) maxLineWidth() int {
	maxLineW := 0
	for _, line := range d.logicalLines {
		var w int
		if d.ansiAware {
			w = ansi.StringWidth(line)
		} else {
			w = lipgloss.Width(line)
		}
		if w > maxLineW {
			maxLineW = w
		}
	}
	return maxLineW
}

func padReaderLine(line string, targetW int, mode readerMode) string {
	if mode == readerModeJSONPretty {
		return padANSIRight(line, targetW)
	}
	return clampLineWidth(readerBodyStyle.Render(padDisplayRight(line, targetW)), targetW)
}

func isJSONLikeValid(value string) bool {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return false
	}
	switch trimmed[0] {
	case '{', '[':
	default:
		return false
	}
	return json.Valid([]byte(trimmed))
}

func prettyJSONReaderValue(value string) (string, bool) {
	trimmed := strings.TrimSpace(value)
	if !isJSONLikeValid(trimmed) {
		return "", false
	}

	var buf bytes.Buffer
	if err := json.Indent(&buf, []byte(trimmed), "", "  "); err != nil {
		return "", false
	}
	return buf.String(), true
}

func highlightPrettyJSON(pretty string) []string {
	lines := strings.Split(pretty, "\n")
	if len(lines) == 0 {
		return []string{""}
	}

	styled := make([]string, 0, len(lines))
	for _, line := range lines {
		styled = append(styled, highlightPrettyJSONLine(line))
	}
	return styled
}

func highlightPrettyJSONLine(line string) string {
	if line == "" {
		return ""
	}

	var b strings.Builder
	for i := 0; i < len(line); {
		switch line[i] {
		case ' ', '\t':
			b.WriteByte(line[i])
			i++
		case '{', '}', '[', ']', ':', ',':
			b.WriteString(readerJSONPunctuationStyle.Render(line[i : i+1]))
			i++
		case '"':
			end := scanJSONStringToken(line, i)
			token := line[i:end]
			if jsonStringTokenIsKey(line, end) {
				b.WriteString(readerJSONKeyStyle.Render(token))
			} else {
				b.WriteString(readerJSONStringStyle.Render(token))
			}
			i = end
		default:
			if keyword := jsonKeywordTokenAt(line, i); keyword != "" {
				b.WriteString(readerJSONKeywordStyle.Render(keyword))
				i += len(keyword)
				continue
			}
			if number := jsonNumberTokenAt(line, i); number != "" {
				b.WriteString(readerJSONNumberStyle.Render(number))
				i += len(number)
				continue
			}
			b.WriteString(readerBodyStyle.Render(line[i : i+1]))
			i++
		}
	}
	return b.String()
}

func scanJSONStringToken(line string, start int) int {
	i := start + 1
	for i < len(line) {
		switch line[i] {
		case '\\':
			i += 2
		case '"':
			return i + 1
		default:
			i++
		}
	}
	return len(line)
}

func jsonStringTokenIsKey(line string, end int) bool {
	for i := end; i < len(line); i++ {
		switch line[i] {
		case ' ', '\t':
			continue
		case ':':
			return true
		default:
			return false
		}
	}
	return false
}

func jsonKeywordTokenAt(line string, start int) string {
	for _, keyword := range []string{"true", "false", "null"} {
		if strings.HasPrefix(line[start:], keyword) && jsonTokenBoundary(line, start+len(keyword)) {
			return keyword
		}
	}
	return ""
}

func jsonNumberTokenAt(line string, start int) string {
	if start >= len(line) {
		return ""
	}
	if line[start] != '-' && (line[start] < '0' || line[start] > '9') {
		return ""
	}

	end := start
	for end < len(line) {
		ch := line[end]
		if (ch >= '0' && ch <= '9') || ch == '-' || ch == '+' || ch == '.' || ch == 'e' || ch == 'E' {
			end++
			continue
		}
		break
	}
	if end == start || !jsonTokenBoundary(line, end) {
		return ""
	}
	return line[start:end]
}

func jsonTokenBoundary(line string, pos int) bool {
	if pos >= len(line) {
		return true
	}
	switch line[pos] {
	case ' ', '\t', '\n', '\r', ',', ':', '}', ']':
		return true
	default:
		return false
	}
}

func wrapANSILogicalLines(lines []string, maxW int) []string {
	if maxW <= 0 {
		return []string{""}
	}
	var out []string
	for _, line := range lines {
		out = append(out, strings.Split(ansi.Hardwrap(line, maxW, true), "\n")...)
	}
	if len(out) == 0 {
		return []string{""}
	}
	return out
}

func sliceANSILogicalLines(lines []string, startW, maxW int) []string {
	if maxW <= 0 {
		return []string{""}
	}
	if startW < 0 {
		startW = 0
	}
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		out = append(out, ansi.Cut(line, startW, startW+maxW))
	}
	if len(out) == 0 {
		return []string{""}
	}
	return out
}

func padANSIRight(line string, targetW int) string {
	if targetW <= 0 {
		return ""
	}
	padding := targetW - ansi.StringWidth(line)
	if padding <= 0 {
		return line
	}
	return line + strings.Repeat(" ", padding)
}
