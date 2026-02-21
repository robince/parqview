package ui

import "github.com/charmbracelet/lipgloss"

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

	nullStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("243")).
			Italic(true)

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

	activeColNullStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("246")).
				Background(lipgloss.Color("238")).
				Italic(true)

	// Row cursor highlight in data pane
	activeRowCellStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("252")).
				Background(lipgloss.Color("236"))

	activeRowNullStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("246")).
				Background(lipgloss.Color("236")).
				Italic(true)

	activeRowNumStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("230"))

	// Crosshair (row + column intersection)
	crosshairCellStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("255")).
				Background(lipgloss.Color("240"))

	crosshairNullStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("248")).
				Background(lipgloss.Color("240")).
				Italic(true)

	// Null indicator dots
	nullDot             = lipgloss.NewStyle().Foreground(lipgloss.Color("214")).Render("•")
	nullDotHeader       = lipgloss.NewStyle().Foreground(lipgloss.Color("214")).Background(lipgloss.Color("62")).Render("•")
	nullDotActiveHeader = lipgloss.NewStyle().Foreground(lipgloss.Color("214")).Background(lipgloss.Color("69")).Render("•")

	// Column list
	selectedMark   = lipgloss.NewStyle().Foreground(lipgloss.Color("42")).Render("●")
	unselectedMark = lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Render("○")

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
