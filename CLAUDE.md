# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

parqview is a keyboard-first TUI Parquet/CSV inspector built with Go, Bubble Tea, and embedded DuckDB. It provides a DataWrangler-like experience: spreadsheet preview + column summaries + column subset selection + missing-value navigation. See `pqview_spec_and_tasks.md` for the full spec.

## Build & Run

```bash
go build -o parqview ./cmd/parqview
./parqview <file.parquet|file.csv>
```

## Tests

```bash
go test ./...                                    # all tests
go test ./internal/engine -v                     # engine integration tests
go test ./internal/engine -run TestOpenParquet -v # single test

# Regenerate test fixtures (sample.parquet + sample.csv)
go run testdata/gen_test_data.go
```

Tests use real DuckDB queries against fixture files in `testdata/`. Engine tests require `_ "github.com/marcboeker/go-duckdb"` blank import for the driver.

## Architecture

**Entry point** (`cmd/parqview/main.go`): Arg parsing, creates engine, launches Bubble Tea TUI.

**Engine** (`internal/engine/engine.go`): Wraps a DuckDB `database/sql` connection. Opens the file as `CREATE VIEW t`, then provides methods for schema discovery, paginated preview, profiling (missing/distinct/top values/histogram), null filtering, and jump-to-first-null. All queries are context-aware for cancellation.

**TUI** (`internal/ui/model.go`): Single Bubble Tea model managing two panes (table + columns), overlays (help + column detail), and async profiling. Profiling chains sequentially via `profileNext()` → `profileBasicDoneMsg` → `profileNext()`. The model is lock-free (single-goroutine Bubble Tea loop).

**Support packages:**
- `internal/types/` — Domain models (`ColumnInfo`, `ColumnSummary`, `NumericStats`, `Histogram`)
- `internal/selection/` — Ordered column selection set (preserves file order)
- `internal/clipboard/` — Python list formatter + macOS `pbcopy`
- `internal/util/` — Case-insensitive substring matching

## Key Conventions

- DuckDB binding is `github.com/marcboeker/go-duckdb` (the `duckdb/duckdb-go` GitHub repo declares itself as marcboeker internally)
- SQL identifiers quoted via `quoteIdent()` (double-quote escaping), string literals via `escapeSQLString()` (single-quote escaping)
- Clipboard is macOS-only (`pbcopy`)
- Column profiling is lazy: basic summaries computed sequentially on startup, detail (top-3, stats, histogram) computed on-demand when opening column detail panel
