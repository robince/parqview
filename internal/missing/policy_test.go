package missing

import "testing"

func TestModeNextCycles(t *testing.T) {
	if got := ModeNullAndNaN.Next(); got != ModeNullOnly {
		t.Fatalf("ModeNullAndNaN.Next() = %v, want %v", got, ModeNullOnly)
	}
	if got := ModeNullOnly.Next(); got != ModeNaNOnly {
		t.Fatalf("ModeNullOnly.Next() = %v, want %v", got, ModeNaNOnly)
	}
	if got := ModeNaNOnly.Next(); got != ModeNullAndNaN {
		t.Fatalf("ModeNaNOnly.Next() = %v, want %v", got, ModeNullAndNaN)
	}
}

func TestModeLabels(t *testing.T) {
	tests := []struct {
		mode      Mode
		wantLabel string
		wantShort string
	}{
		{mode: ModeNullAndNaN, wantLabel: "NULL+NaN", wantShort: "NULL+NaN"},
		{mode: ModeNullOnly, wantLabel: "NULL only", wantShort: "NULL"},
		{mode: ModeNaNOnly, wantLabel: "NaN only", wantShort: "NaN"},
	}
	for _, tc := range tests {
		if got := tc.mode.Label(); got != tc.wantLabel {
			t.Fatalf("Label(%v) = %q, want %q", tc.mode, got, tc.wantLabel)
		}
		if got := tc.mode.ShortLabel(); got != tc.wantShort {
			t.Fatalf("ShortLabel(%v) = %q, want %q", tc.mode, got, tc.wantShort)
		}
	}
}

func TestModeSQLPredicateAndDisplayMissing(t *testing.T) {
	tests := []struct {
		name          string
		mode          Mode
		wantPredicate string
		wantNull      bool
		wantNaN       bool
	}{
		{
			name:          "null+nan",
			mode:          ModeNullAndNaN,
			wantPredicate: `("score" IS NULL OR ` + SQLNaNPredicate(`"score"`) + `)`,
			wantNull:      true,
			wantNaN:       true,
		},
		{
			name:          "null only",
			mode:          ModeNullOnly,
			wantPredicate: `"score" IS NULL`,
			wantNull:      true,
			wantNaN:       false,
		},
		{
			name:          "nan only",
			mode:          ModeNaNOnly,
			wantPredicate: SQLNaNPredicate(`"score"`),
			wantNull:      false,
			wantNaN:       true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.mode.SQLPredicate(`"score"`); got != tc.wantPredicate {
				t.Fatalf("SQLPredicate() = %q, want %q", got, tc.wantPredicate)
			}
			if got := tc.mode.IsDisplayMissing("NULL"); got != tc.wantNull {
				t.Fatalf("IsDisplayMissing(NULL) = %v, want %v", got, tc.wantNull)
			}
			if got := tc.mode.IsDisplayMissing(" NaN "); got != tc.wantNaN {
				t.Fatalf("IsDisplayMissing(NaN) = %v, want %v", got, tc.wantNaN)
			}
		})
	}
}
