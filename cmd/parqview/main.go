package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	_ "github.com/marcboeker/go-duckdb"

	"github.com/robince/parqview/internal/engine"
	"github.com/robince/parqview/internal/ui"
	"github.com/robince/parqview/internal/version"
)

func main() {
	if len(os.Args) == 2 && (os.Args[1] == "--version" || os.Args[1] == "-v") {
		fmt.Println(version.String())
		return
	}

	if len(os.Args) > 2 {
		fmt.Fprintf(os.Stderr, "Usage: parqview [%s]\n", supportedFileArgPattern())
		fmt.Fprintf(os.Stderr, "       parqview --version\n")
		os.Exit(1)
	}

	cwd, err := os.Getwd()
	if err != nil {
		cwd = "."
	}

	var (
		eng      *engine.Engine
		fileName string
	)

	if len(os.Args) == 2 {
		path := os.Args[1]
		absPath, err := filepath.Abs(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error resolving path: %v\n", err)
			os.Exit(1)
		}

		if _, statErr := os.Stat(absPath); os.IsNotExist(statErr) {
			fmt.Fprintf(os.Stderr, "File not found: %s\n", absPath)
			os.Exit(1)
		}

		eng, err = engine.New(absPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening file: %v\n", err)
			os.Exit(1)
		}
		fileName = filepath.Base(absPath)
	}

	if err := runApp(eng, fileName, cwd); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func supportedFileArgPattern() string {
	exts := engine.SupportedExtensions()
	parts := make([]string, 0, len(exts))
	for _, ext := range exts {
		parts = append(parts, "file"+ext)
	}
	return strings.Join(parts, "|")
}

func runApp(eng *engine.Engine, fileName, cwd string) error {
	model := ui.NewModel(eng, fileName, cwd)
	p := tea.NewProgram(model, tea.WithAltScreen(), tea.WithMouseCellMotion())
	finalModel, err := p.Run()
	if m, ok := finalModel.(ui.Model); ok {
		_ = m.Close()
	} else if eng != nil {
		_ = eng.Close()
	}
	return err
}
