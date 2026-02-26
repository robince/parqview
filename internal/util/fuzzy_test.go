package util

import "testing"

func TestFuzzyMatch(t *testing.T) {
	tests := []struct {
		name     string
		s, query string
		want     bool
	}{
		{name: "empty query", s: "hello_world", query: "", want: true},
		{name: "substring", s: "hello_world", query: "hello", want: true},
		{name: "case-insensitive", s: "hello_world", query: "HELLO", want: true},
		{name: "underscore component", s: "hello_world", query: "world", want: true},
		{name: "single token miss", s: "hello_world", query: "xyz", want: false},
		{name: "fuzzy camel-case", s: "MyColumn", query: "mycol", want: true},
		{name: "multi-term across components", s: "customer_account_identifier", query: "customer id", want: true},
		{name: "multi-term all required", s: "customer_account_identifier", query: "customer xyz", want: false},
		{name: "collapsed identifier search", s: "customer_account_identifier", query: "customeraccount", want: true},
		{name: "acronym match", s: "customer_account_identifier", query: "cai", want: true},
		{name: "term tokenized on underscores", s: "customer_account_identifier", query: "customer_account id", want: true},
		{name: "query with extra spaces", s: "customer_account_identifier", query: "  customer   id  ", want: true},
		{name: "multi-term narrows results", s: "normalized_mean_index", query: "nmi mean", want: true},
		{name: "multi-term excludes partial matches", s: "normalized_max_index", query: "nmi mean", want: false},
		{name: "multi-term excludes acronym-only match", s: "near_market_inventory", query: "nmi mean", want: false},
	}
	for _, tt := range tests {
		got := FuzzyMatch(tt.s, tt.query)
		if got != tt.want {
			t.Errorf("%s: FuzzyMatch(%q, %q) = %v, want %v", tt.name, tt.s, tt.query, got, tt.want)
		}
	}
}
