package ui

import (
	"github.com/charmbracelet/lipgloss"

	"github.com/robince/parqview/internal/missing"
)

const (
	selectedMarkGlyph   = "●"
	unselectedMarkGlyph = "○"
	nullDotChar         = "•"

	// Layout constants
	tableSplitPct     = 65 // default percentage of width for table pane
	statusBarH        = 2  // height reserved for status/bottom bar
	paneBorderW       = 2  // horizontal border+padding total (left + right)
	paneBorderH       = 2  // vertical border+padding total (top + bottom)
	minPaneOuterW     = 24 // minimum pane width (including border)
	dividerGrabRadius = 1  // mouse hit radius around divider
	previewHeadroom   = 20 // extra preview rows beyond visible viewport
	previewMinRows    = 50 // lower bound for preview fetch size
	previewMaxRows    = 500
	topBottomBarPadW  = 2 // horizontal padding in top/bottom bar styles

	// Table layout constants
	// Invariant: tableColWidth/tableColWideWidth >= tableColMinWidth (renderRowCells relies on this).
	tableColWidth      = 14 // default table column width
	tableColWideWidth  = 30 // wide-mode table column width
	tableColMinWidth   = 4
	tableRowNumW       = 6 // row number column width
	tableRowPrefixW    = 1 // prefix space for null dot alignment
	tableFooterPrefixW = 2 // leading spaces in footer row (for alignment with row-number gutter)
)

var (
	// Pane borders
	activeBorderStyle = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(lipgloss.Color("62"))

	inactiveBorderStyle = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(lipgloss.Color("240"))

	// Top bar
	topBarStyle = lipgloss.NewStyle().
			Background(lipgloss.Color("62")).
			Foreground(lipgloss.Color("230")).
			Padding(0, 1)

	// Bottom bar
	bottomBarStyle = lipgloss.NewStyle().
			Background(lipgloss.Color("236")).
			Foreground(lipgloss.Color("252")).
			Padding(0, 1)

	// Table
	headerStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("230")).
			Background(lipgloss.Color("62"))

	cellStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("252"))

	rowNumStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("243"))

	// Column cursor highlight in data pane
	activeColHeaderStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("230")).
				Background(lipgloss.Color("69"))

	activeColCellStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("255")).
				Background(lipgloss.Color("238"))

	// Row cursor highlight in data pane
	activeRowCellStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("252")).
				Background(lipgloss.Color("236"))

	activeRowNumStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("230"))

	// Crosshair (row + column intersection)
	crosshairCellStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("255")).
				Background(lipgloss.Color("240"))

	// Null indicator dots (pre-rendered strings, not reusable styles)
	inlineNullDotW      = lipgloss.Width(" " + nullDotChar) // inline indicator is rendered as " " + dot
	tableHeaderNullDotW = lipgloss.Width(nullDotChar)        // updated in init() to max of all styled header dots

	// Column list
	selectedMark   = lipgloss.NewStyle().Foreground(lipgloss.Color("42")).Render(selectedMarkGlyph)
	unselectedMark = lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Render(unselectedMarkGlyph)

	highlightStyle = lipgloss.NewStyle().
			Background(lipgloss.Color("25")).
			Foreground(lipgloss.Color("230"))

	dimHighlightStyle = lipgloss.NewStyle().
				Background(lipgloss.Color("238")).
				Foreground(lipgloss.Color("252"))

	typeBadgeStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("243"))

	statStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("243"))

	// Detail panel
	detailTitleStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("230"))

	detailLabelStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("243"))

	detailValueStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("252"))

	// Search
	searchPromptStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("62"))

	// Filter indicator
	filterStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("214")).
			Bold(true)

	// Help
	helpKeyStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("62")).
			Bold(true)

	helpDescStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("252"))
)

func inlineNullDotWidth() int {
	return inlineNullDotW
}

func tableHeaderNullDotWidth() int {
	return tableHeaderNullDotW
}

func missingAccentColor(mode missing.Mode) lipgloss.Color {
	switch mode {
	case missing.ModeNullOnly:
		return lipgloss.Color("179")
	case missing.ModeNaNOnly:
		return lipgloss.Color("80")
	default:
		return lipgloss.Color("214")
	}
}

// Compile-time assertion: cache arrays are indexed by missing.Mode value (0–2).
// If the missing package reorders or adds modes this will fail to compile.
var _ = [1]struct{}{}[missing.ModeNaNOnly-2]

// Per-mode style caches — computed once at startup, keyed by missing.Mode value (0–2).
var (
	cachedNullStyles          [3]lipgloss.Style
	cachedActiveColNullStyles [3]lipgloss.Style
	cachedActiveRowNullStyles [3]lipgloss.Style
	cachedCrosshairNullStyles [3]lipgloss.Style
	cachedMissingBadgeStyles  [3]lipgloss.Style
	cachedMissingDots         [3]string
	cachedMissingDotHeaders   [3]string
	cachedMissingDotActHeaders [3]string
)

func init() {
	for _, mode := range []missing.Mode{missing.ModeNullAndNaN, missing.ModeNullOnly, missing.ModeNaNOnly} {
		c := missingAccentColor(mode)
		cachedNullStyles[mode] = lipgloss.NewStyle().Foreground(c).Italic(true)
		cachedActiveColNullStyles[mode] = lipgloss.NewStyle().Foreground(c).Background(lipgloss.Color("238")).Italic(true)
		cachedActiveRowNullStyles[mode] = lipgloss.NewStyle().Foreground(c).Background(lipgloss.Color("236")).Italic(true)
		cachedCrosshairNullStyles[mode] = lipgloss.NewStyle().Bold(true).Foreground(c).Background(lipgloss.Color("240")).Italic(true)
		cachedMissingBadgeStyles[mode] = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("235")).Background(c).Padding(0, 1)
		cachedMissingDots[mode] = lipgloss.NewStyle().Foreground(c).Render(nullDotChar)
		cachedMissingDotHeaders[mode] = lipgloss.NewStyle().Foreground(c).Background(lipgloss.Color("62")).Render(nullDotChar)
		cachedMissingDotActHeaders[mode] = lipgloss.NewStyle().Foreground(c).Background(lipgloss.Color("69")).Render(nullDotChar)
		if w := max(lipgloss.Width(cachedMissingDotHeaders[mode]), lipgloss.Width(cachedMissingDotActHeaders[mode])); w > tableHeaderNullDotW {
			tableHeaderNullDotW = w
		}
	}
}

func missingModeBadge(mode missing.Mode, short bool) string {
	label := mode.Label()
	if short {
		label = mode.ShortLabel()
	}
	return cachedMissingBadgeStyles[mode].Render(label)
}

func nullStyle(mode missing.Mode) lipgloss.Style          { return cachedNullStyles[mode] }
func activeColNullStyle(mode missing.Mode) lipgloss.Style { return cachedActiveColNullStyles[mode] }
func activeRowNullStyle(mode missing.Mode) lipgloss.Style { return cachedActiveRowNullStyles[mode] }
func crosshairNullStyle(mode missing.Mode) lipgloss.Style { return cachedCrosshairNullStyles[mode] }

func missingDot(mode missing.Mode) string           { return cachedMissingDots[mode] }
func missingDotHeader(mode missing.Mode) string     { return cachedMissingDotHeaders[mode] }
func missingDotActiveHeader(mode missing.Mode) string { return cachedMissingDotActHeaders[mode] }
