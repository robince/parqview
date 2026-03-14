package main

import "testing"

func TestSupportedFileArgPatternIncludesJSONFormats(t *testing.T) {
	want := "file.parquet|file.csv|file.json|file.jsonl|file.ndjson"
	if got := supportedFileArgPattern(); got != want {
		t.Fatalf("supportedFileArgPattern() = %q, want %q", got, want)
	}
}

func TestParseArgs(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		want    cliArgs
		wantErr bool
	}{
		{name: "no args", args: nil, want: cliArgs{}},
		{name: "version long", args: []string{"--version"}, want: cliArgs{showVersion: true}},
		{name: "version short", args: []string{"-v"}, want: cliArgs{showVersion: true}},
		{name: "file only", args: []string{"sample.parquet"}, want: cliArgs{path: "sample.parquet"}},
		{name: "start row before file", args: []string{"+123", "sample.parquet"}, want: cliArgs{path: "sample.parquet", startRowID: 123}},
		{name: "start row after file", args: []string{"sample.parquet", "+123"}, want: cliArgs{path: "sample.parquet", startRowID: 123}},
		{name: "invalid start row zero", args: []string{"+0", "sample.parquet"}, wantErr: true},
		{name: "missing file for start row", args: []string{"+12"}, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseArgs(tc.args)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseArgs(%v): %v", tc.args, err)
			}
			if got != tc.want {
				t.Fatalf("parseArgs(%v) = %#v, want %#v", tc.args, got, tc.want)
			}
		})
	}
}
