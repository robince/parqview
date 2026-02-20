package types

// ColumnInfo holds schema information for a single column.
type ColumnInfo struct {
	Name     string
	DuckType string
	Index    int // original file order (0-based)
}

// TopValue represents a value and its frequency.
type TopValue struct {
	Value string
	Count int64
	Pct   float64 // percentage of non-null values
}

// NumericStats holds numeric column statistics.
type NumericStats struct {
	Min    float64
	Max    float64
	Mean   float64
	Stddev float64
}

// HistBin represents a single histogram bin.
type HistBin struct {
	Low   float64
	High  float64
	Count int64
}

// Histogram holds histogram data for a numeric column.
type Histogram struct {
	Bins []HistBin
}

// ColumnSummary holds profiling results for a column.
type ColumnSummary struct {
	MissingCount   int64
	MissingPct     float64
	DistinctApprox int64
	DistinctPct    float64
	Top3           []TopValue
	Numeric        *NumericStats
	Hist           *Histogram
	IsDiscrete     bool
	Loaded         bool // true once basic summary is computed
	DetailLoaded   bool // true once detail (top3/stats/hist) is computed
}
