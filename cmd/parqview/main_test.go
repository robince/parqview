package main

import "testing"

func TestSupportedFileArgPatternIncludesJSONFormats(t *testing.T) {
	want := "file.parquet|file.csv|file.json|file.jsonl|file.ndjson"
	if got := supportedFileArgPattern(); got != want {
		t.Fatalf("supportedFileArgPattern() = %q, want %q", got, want)
	}
}
