package util

import "testing"

func TestDuckTypeBase(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{name: "bare type", in: "INTEGER", want: "INTEGER"},
		{name: "parameterized type", in: "DECIMAL(10,2)", want: "DECIMAL"},
		{name: "multi word type", in: "DOUBLE PRECISION", want: "DOUBLE"},
		{name: "empty", in: "", want: ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := DuckTypeBase(tc.in); got != tc.want {
				t.Fatalf("DuckTypeBase(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestIsNumericDuckType(t *testing.T) {
	if IsNumericDuckType("INTERVAL") {
		t.Fatal("INTERVAL should not be treated as numeric")
	}
	if !IsNumericDuckType("DECIMAL(10,2)") {
		t.Fatal("DECIMAL(10,2) should be treated as numeric")
	}
	if !IsNumericDuckType("FLOAT4") {
		t.Fatal("FLOAT4 should be treated as numeric")
	}
	if !IsNumericDuckType("FLOAT8") {
		t.Fatal("FLOAT8 should be treated as numeric")
	}
}
