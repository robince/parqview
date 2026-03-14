package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	_ "github.com/marcboeker/go-duckdb"

	"github.com/robince/parqview/internal/engine"
	"github.com/robince/parqview/internal/ui"
	"github.com/robince/parqview/internal/version"
)

func main() {
	parsed, err := parseArgs(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		fmt.Fprintf(os.Stderr, "Usage: parqview [+row] [%s]\n", supportedFileArgPattern())
		fmt.Fprintf(os.Stderr, "       parqview --version\n")
		os.Exit(1)
	}

	if parsed.showVersion {
		fmt.Println(version.String())
		return
	}

	cwd, err := os.Getwd()
	if err != nil {
		cwd = "."
	}

	var (
		eng      *engine.Engine
		fileName string
	)

	if parsed.path != "" {
		path := parsed.path
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

	if err := runApp(eng, fileName, cwd, parsed.startRowID); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

type cliArgs struct {
	showVersion bool
	path        string
	startRowID  int64
}

func parseArgs(args []string) (cliArgs, error) {
	switch len(args) {
	case 0:
		return cliArgs{}, nil
	case 1:
		switch args[0] {
		case "--version", "-v":
			return cliArgs{showVersion: true}, nil
		default:
			_, isStartRow, err := parseStartRowArg(args[0])
			if err != nil {
				return cliArgs{}, err
			}
			if isStartRow {
				return cliArgs{}, fmt.Errorf("missing file path for %s", args[0])
			}
			return cliArgs{path: args[0]}, nil
		}
	case 2:
		rowID, isStartRow, err := parseStartRowArg(args[0])
		if err != nil {
			return cliArgs{}, err
		}
		if isStartRow {
			return cliArgs{path: args[1], startRowID: rowID}, nil
		}
		rowID, isStartRow, err = parseStartRowArg(args[1])
		if err != nil {
			return cliArgs{}, err
		}
		if isStartRow {
			return cliArgs{path: args[0], startRowID: rowID}, nil
		}
	}
	return cliArgs{}, fmt.Errorf("invalid arguments")
}

func parseStartRowArg(arg string) (int64, bool, error) {
	if !strings.HasPrefix(arg, "+") {
		return 0, false, nil
	}
	if len(arg) < 2 {
		return 0, false, fmt.Errorf("invalid start row %q", arg)
	}
	rowID, err := strconv.ParseInt(arg[1:], 10, 64)
	if err != nil || rowID <= 0 {
		return 0, false, fmt.Errorf("invalid start row %q", arg)
	}
	return rowID, true, nil
}

func supportedFileArgPattern() string {
	exts := engine.SupportedExtensions()
	parts := make([]string, 0, len(exts))
	for _, ext := range exts {
		parts = append(parts, "file"+ext)
	}
	return strings.Join(parts, "|")
}

func runApp(eng *engine.Engine, fileName, cwd string, startRowID int64) error {
	model := ui.NewModelAtRow(eng, fileName, cwd, startRowID)
	p := tea.NewProgram(model, tea.WithAltScreen(), tea.WithMouseCellMotion())
	finalModel, err := p.Run()
	if m, ok := finalModel.(ui.Model); ok {
		_ = m.Close()
	} else if eng != nil {
		_ = eng.Close()
	}
	return err
}
