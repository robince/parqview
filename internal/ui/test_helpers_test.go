package ui

import (
	"testing"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/robince/parqview/internal/engine"
	"github.com/robince/parqview/internal/missing"
	"github.com/robince/parqview/internal/selection"
	"github.com/robince/parqview/internal/types"
)

// newTestModel creates a minimal Model suitable for unit tests.
func newTestModel() Model {
	return Model{
		engine:          nil,
		missingMode:     missing.ModeNullAndNaN,
		sel:             selection.New(nil),
		summaries:       make(map[string]*types.ColumnSummary),
		tableColWidths:  make(map[string]int),
		tableColOffHint: -1,
	}
}

// newCmdTestModel returns a model with a non-nil engine pointer so command
// paths that guard only on nil can be asserted without executing those commands.
func newCmdTestModel() Model {
	m := newTestModel()
	m.engine = &engine.Engine{}
	return m
}

func openTestEngine(t *testing.T, path string) *engine.Engine {
	t.Helper()
	eng, err := engine.New(path)
	if err != nil {
		t.Fatalf("engine.New(%q): %v", path, err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	return eng
}

// updateModel sends a message through Model.Update and returns the updated model.
func updateModel(t *testing.T, m Model, msg tea.Msg) Model {
	t.Helper()
	updated, _ := m.Update(msg)
	return updated.(Model)
}
