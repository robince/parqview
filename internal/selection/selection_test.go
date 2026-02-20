package selection

import (
	"slices"
	"testing"
)

func TestToggle(t *testing.T) {
	s := New([]string{"a", "b", "c"})
	s.Toggle("b")
	if !s.IsSelected("b") {
		t.Fatal("expected b selected")
	}
	s.Toggle("b")
	if s.IsSelected("b") {
		t.Fatal("expected b deselected")
	}
}

func TestSelectedOrder(t *testing.T) {
	s := New([]string{"c", "a", "b"})
	s.Add("b")
	s.Add("c")
	got := s.Selected()
	want := []string{"c", "b"} // file order
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestAddAllRemoveAll(t *testing.T) {
	s := New([]string{"a", "b", "c", "d"})
	s.AddAll([]string{"b", "d"})
	if s.Count() != 2 {
		t.Fatalf("expected 2, got %d", s.Count())
	}
	s.RemoveAll([]string{"b"})
	if s.Count() != 1 {
		t.Fatalf("expected 1, got %d", s.Count())
	}
	if !s.IsSelected("d") {
		t.Fatal("expected d selected")
	}
}

func TestSelectAllClear(t *testing.T) {
	s := New([]string{"a", "b", "c"})
	s.SelectAll()
	if s.Count() != 3 {
		t.Fatalf("expected 3, got %d", s.Count())
	}
	s.Clear()
	if s.Count() != 0 {
		t.Fatalf("expected 0, got %d", s.Count())
	}
}
