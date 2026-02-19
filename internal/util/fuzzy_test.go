package util

import "testing"

func TestFuzzyMatch(t *testing.T) {
	tests := []struct {
		s, query string
		want     bool
	}{
		{"hello_world", "", true},
		{"hello_world", "hello", true},
		{"hello_world", "HELLO", true},
		{"hello_world", "world", true},
		{"hello_world", "xyz", false},
		{"MyColumn", "mycol", true},
	}
	for _, tt := range tests {
		got := FuzzyMatch(tt.s, tt.query)
		if got != tt.want {
			t.Errorf("FuzzyMatch(%q, %q) = %v, want %v", tt.s, tt.query, got, tt.want)
		}
	}
}
