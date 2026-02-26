package ui

import (
	"testing"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/robince/parqview/internal/engine"
	"github.com/robince/parqview/internal/selection"
	"github.com/robince/parqview/internal/types"
)

// newTestModel creates a minimal Model suitable for unit tests.
func newTestModel() Model {
	return Model{
		engine:          &engine.Engine{},
		sel:             selection.New(nil),
		summaries:       make(map[string]*types.ColumnSummary),
		tableColOffHint: -1,
	}
}

// updateModel sends a message through Model.Update and returns the updated model.
func updateModel(t *testing.T, m Model, msg tea.Msg) Model {
	t.Helper()
	updated, _ := m.Update(msg)
	return updated.(Model)
}
