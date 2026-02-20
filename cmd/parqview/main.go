package main

import (
	"fmt"
	"os"
	"path/filepath"

	tea "github.com/charmbracelet/bubbletea"
	_ "github.com/marcboeker/go-duckdb"

	"github.com/robince/parqview/internal/engine"
	"github.com/robince/parqview/internal/ui"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: parqview <file.parquet|file.csv>\n")
		os.Exit(1)
	}

	path := os.Args[1]

	// Resolve to absolute path
	absPath, err := filepath.Abs(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error resolving path: %v\n", err)
		os.Exit(1)
	}

	// Check file exists
	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "File not found: %s\n", absPath)
		os.Exit(1)
	}

	eng, err := engine.New(absPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening file: %v\n", err)
		os.Exit(1)
	}

	if err := runApp(eng, path); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runApp(eng *engine.Engine, path string) error {
	defer func() { _ = eng.Close() }()
	model := ui.NewModel(eng, filepath.Base(path))
	p := tea.NewProgram(model, tea.WithAltScreen())
	_, err := p.Run()
	return err
}
