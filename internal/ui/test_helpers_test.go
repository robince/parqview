package ui

import (
	"testing"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/robince/parqview/internal/engine"
	"github.com/robince/parqview/internal/missing"
	"github.com/robince/parqview/internal/selection"
	"github.com/robince/parqview/internal/types"
)

// newTestModel creates a minimal Model suitable for unit tests.
func newTestModel() Model {
	search := textinput.New()
	search.Prompt = "/ "
	search.PromptStyle = searchPromptStyle

	predicate := textinput.New()

	return Model{
		engine:          nil,
		missingMode:     missing.ModeNullAndNaN,
		sel:             selection.New(nil),
		searchInput:     search,
		predicateInput:  predicate,
		predicates:      make(map[string]columnPredicate),
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

// updateModel sends a message through Model.Update and returns the updated model.
func updateModel(t *testing.T, m Model, msg tea.Msg) Model {
	t.Helper()
	updated, _ := m.Update(msg)
	return updated.(Model)
}
