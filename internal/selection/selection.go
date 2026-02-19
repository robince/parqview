package selection

// Set maintains an ordered selection of column names, preserving file order.
type Set struct {
	order    []string          // all column names in file order
	selected map[string]bool
}

// New creates a new selection set with the given column names in file order.
func New(columns []string) *Set {
	return &Set{
		order:    columns,
		selected: make(map[string]bool),
	}
}

// Toggle flips the selection state of a column.
func (s *Set) Toggle(name string) {
	if s.selected[name] {
		delete(s.selected, name)
	} else {
		s.selected[name] = true
	}
}

// Add adds a column to the selection.
func (s *Set) Add(name string) {
	s.selected[name] = true
}

// Remove removes a column from the selection.
func (s *Set) Remove(name string) {
	delete(s.selected, name)
}

// AddAll adds all given columns to the selection.
func (s *Set) AddAll(names []string) {
	for _, n := range names {
		s.selected[n] = true
	}
}

// RemoveAll removes all given columns from the selection.
func (s *Set) RemoveAll(names []string) {
	for _, n := range names {
		delete(s.selected, n)
	}
}

// SelectAll selects every column.
func (s *Set) SelectAll() {
	for _, n := range s.order {
		s.selected[n] = true
	}
}

// Clear removes all selections.
func (s *Set) Clear() {
	s.selected = make(map[string]bool)
}

// IsSelected returns whether a column is selected.
func (s *Set) IsSelected(name string) bool {
	return s.selected[name]
}

// Count returns the number of selected columns.
func (s *Set) Count() int {
	return len(s.selected)
}

// Selected returns selected column names in file order.
func (s *Set) Selected() []string {
	var result []string
	for _, n := range s.order {
		if s.selected[n] {
			result = append(result, n)
		}
	}
	return result
}

// All returns all column names in file order.
func (s *Set) All() []string {
	return s.order
}
