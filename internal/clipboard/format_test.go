package clipboard

import "testing"

func TestFormatPythonList(t *testing.T) {
	tests := []struct {
		input []string
		want  string
	}{
		{nil, "[]"},
		{[]string{"a"}, `["a"]`},
		{[]string{"col_a", "col_b"}, `["col_a", "col_b"]`},
		{[]string{`has"quote`}, `["has\"quote"]`},
		{[]string{"has\\back"}, `["has\\back"]`},
		{[]string{"has\nnewline"}, `["has\nnewline"]`},
	}
	for _, tt := range tests {
		got := FormatPythonList(tt.input)
		if got != tt.want {
			t.Errorf("FormatPythonList(%v) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
