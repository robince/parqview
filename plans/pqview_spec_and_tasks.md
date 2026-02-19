# parqview — Single-file Parquet/CSV Inspector (Go + Embedded DuckDB)
**Version:** v0.1 spec  
**Goal:** Fast, keyboard-first data inspection for a *single* Parquet (primary) or CSV (secondary) file with a DataWrangler-like experience: spreadsheet preview + column summaries + powerful column subset selection + missing-value navigation.

---

## 0) Summary
`parqview` opens a single file and shows:
- A **spreadsheet-like table view** (data visible immediately).
- A **column pane** with fuzzy search and a persistent **selection set** you can build iteratively (search → add visible → search again → add …).
- **Copy selected columns** to clipboard as a **Python list of quoted literal column names**.
- **DataWrangler-inspired profiles**: missing count/%; distinct count/%; top-3 values + “other %” for discrete-like columns; 10-bin histograms for numeric columns.
- **Missing-value helpers**: show rows with nulls; jump to first null; (future) next/prev missing cell navigation.

Target scale: hundreds of MB, hundreds–1000 columns, hundreds–thousands of rows.

---

## 1) Goals and Non-goals

### Goals (v1)
1. Open a single `.parquet` file; support `.csv` optionally.
2. Display data immediately in a spreadsheet-like table pane.
3. Column pane supports:
   - fuzzy name search
   - selection set operations (toggle/add/remove visible/select all/clear)
   - export selected columns to clipboard as Python list
4. Column summaries (DataWrangler-like):
   - missing count + missing %
   - distinct count (approx) + distinct %
   - top 3 values + “other %” for discrete-like columns
   - numeric stats + 10-bin histogram in column detail
5. Missing workflows:
   - filter to rows with any nulls (visible/selected columns)
   - jump to first null for a chosen column
6. Keep the interface snappy and simple; avoid heavy “cache build” UX.

### Non-goals (v1)
- Multi-file datasets / partition discovery
- Data cleaning & editing, write-back, transforms
- Full SQL IDE (optional later)
- Fancy plotting beyond text histograms

---

## 2) Tech Stack

### Core
- **Go** (CLI + TUI)
- **Embedded DuckDB** via Go bindings (CGO)

### UI
- **Bubble Tea** (model/update/view)
- **Bubbles** (table/list/textinput/viewport)
- **Lip Gloss** (styling)

### Clipboard (macOS-first)
- Use `pbcopy` subprocess for clipboard output

### Packaging
- `go build`
- Optional later: Homebrew formula

---

## 3) UX Layout (DataWrangler-inspired)

### Main screen: split panes
**Left pane — Table (Spreadsheet)**
- Shows data values (rows x columns)
- Header row fixed with column names
- Row index column at far left
- Horizontal + vertical scroll
- Option to show all visible columns or only selected columns

**Right pane — Columns**
- Fuzzy search input
- List of columns with:
  - checkbox (selected)
  - name
  - type badge
  - small inline stats (missing% and distinct% once computed)
- Enter opens a Column Detail panel

**Top bar**
- file name + type
- active filter indicator (e.g., `Filter: rows_with_nulls(selected)`)

**Bottom bar**
- key hints + selection count + status (profiling progress)

### Overlays / Panels
- **Column Detail panel**: expanded profile (top values / histogram / stats)
- **Help overlay**: keybindings reference
- (Optional later) **Filters menu** and/or **SQL scratchpad**

---

## 4) Core Workflows

### A) Rapid column subset building (primary)
1. `/` focus column search
2. type fuzzy query
3. `s` add all **visible filtered** columns to selection set
4. modify query and repeat
5. `d` remove all visible filtered from selection if needed
6. `y` copy selected columns to clipboard as Python list:
   ```python
   ["col_a", "col_b", "col_c"]
   ```

### B) Missing-value triage
- Apply filter: rows with any null in:
  - currently visible columns, or
  - selected columns (recommended)
- Jump to first null for a column from Column Detail
- (Later) navigate next/prev missing cell in the grid

### C) Sanity check values
- Column summaries reveal missing/distinct/top values
- Numeric histograms show shape quickly
- Table preview + horizontal scrolling validates expected values

---

## 5) Keybindings (initial)

### Global
- `q` quit
- `?` help overlay
- `Tab` toggle focus (Table ↔ Columns)

### Columns pane
- `/` focus search input
- `Esc` unfocus search (keep filter)
- `Ctrl+U` clear search text
- `↑/↓` move highlight
- `Space` toggle highlighted column in selection
- `s` add all visible (filtered list) to selection
- `d` remove all visible from selection
- `a` select all columns
- `x` clear selection
- `Enter` open Column Detail
- `y` copy selected columns as Python list (selected-only)

### Table pane
- Arrow keys / `h j k l` scroll
- `g` top, `G` bottom
- `[` `]` horizontal page scroll
- `S` toggle “show selected columns only”
- `f` open filters menu (or quick toggle)
- `n` (future) jump to next missing (context dependent)

### Column Detail panel
- `Esc` close
- `t` toggle detail tabs: `Top Values` / `Histogram` / `Stats`
- `n` jump to first null in this column (v1)

---

## 6) Column Profiling (DataWrangler-like)

### Type classification
Based on DuckDB type:
- numeric: int/float/decimal
- temporal: date/timestamp
- boolean
- text
- other / nested (display but keep profiling minimal v1)

### “Discrete-like” heuristic (for Top Values)
Compute top values only if:
- `approx_distinct <= 200` **or**
- `approx_distinct / non_null_count <= 0.05`
(tunable defaults)

### Summary metrics (shown in column list)
For each column:
- missing count + missing %
- approx distinct count + distinct %
- optional: a tiny badge or marker for discrete-like vs high-cardinality

> Compute summaries lazily and asynchronously (visible columns first).

### Column Detail metrics
- missing count + missing %
- approx distinct + distinct %
- Top values (top 3):
  - show top 3 values with % of non-null
  - show “Other %” = 100 - sum(top3%)
- Numeric stats:
  - min, max, mean, stddev
  - approx quantiles p50/p95 (optional v1.1)
  - **10-bin histogram** (text bars)

### Histogram rendering
- 10 equal-width bins between min and max (v1)
- edge case: `min == max` → single bin

---

## 7) Missing Values Features

### v1
1. **Row filter:** show only rows with nulls
   - Mode A: any null in selected columns
   - Mode B: any null in visible columns
2. **Jump to first null (per-column)**
   - From Column Detail: query the minimum row index where `col IS NULL`
3. **Highlight null cells** in the table (styling)

### Explicit future notes (include in roadmap)
- show only columns with missing > 0
- sort columns by missing%
- “next missing cell” spreadsheet navigation
- structured search tokens like `missing>0 type:numeric name:*id*`

---

## 8) Filtering / Query Support

### v1 approach: view transforms (not full SQL UI)
Model the current view as:
- `projection`: list of columns to show in table (selected-only or “first K” columns)
- `row_filter`: SQL predicate string (optional)

Examples:
- `row_filter = "(col_a IS NULL OR col_b IS NULL)"`

### Optional v1.1: mini SQL scratchpad
- Input box for SQL
- Executes against `t`
- Shows results in a read-only table

---

## 9) DuckDB Integration

### Source view
On open:
- Parquet:
  - `CREATE VIEW t AS SELECT * FROM read_parquet(?);`
- CSV:
  - `CREATE VIEW t AS SELECT * FROM read_csv_auto(?);`

### Schema
- `DESCRIBE t;` (or `PRAGMA table_info('t');`)

### Table preview
- `SELECT <projection> FROM t`
- append `WHERE row_filter` if active
- `LIMIT ? OFFSET ?`

> Offset pagination is fine at “thousands of rows”.

### Profiling SQL templates

Let `n_total` and `n_null`:
- `SELECT count(*) AS n_total, sum(c IS NULL)::BIGINT AS n_null FROM t;`

Let `n_non_null = n_total - n_null`.

Approx distinct:
- `SELECT approx_count_distinct(c) AS d FROM t WHERE c IS NOT NULL;`

Top values (discrete-like):
- `SELECT c AS value, count(*) AS cnt
   FROM t
   WHERE c IS NOT NULL
   GROUP BY c
   ORDER BY cnt DESC
   LIMIT 3;`

Numeric stats:
- `SELECT min(c), max(c), avg(c), stddev_pop(c)
   FROM t
   WHERE c IS NOT NULL;`

Histogram (10 bins; equal-width):
1) compute min/max
2) bucket:
- `SELECT width_bucket(c, ?, ?, 10) AS b, count(*) AS cnt
   FROM t
   WHERE c IS NOT NULL
   GROUP BY b
   ORDER BY b;`
Handle `min==max` separately.

### Performance policy
- Show table + schema immediately.
- Profile visible columns lazily.
- Cancel stale profiling jobs when search/filter changes.
- Limit concurrent profiling tasks (e.g. 4).

---

## 10) Caching Strategy

### v1
- In-memory cache per session:
  - `map[colName]ColumnSummary`
- No explicit “build cache” command.

### v1.1 (optional)
- Disk cache keyed by (path, mtime, size) in `~/.cache/pqview/`

---

## 11) Project Layout (Go)

```
parqview/
  cmd/parqview/
    main.go

  internal/app/
    app.go               // wires UI + engine + state

  internal/ui/
    model.go             // Bubble Tea root model
    view_table.go        // spreadsheet pane
    view_columns.go      // columns pane
    view_detail.go       // column detail panel
    keymap.go
    styles.go

  internal/engine/
    engine.go            // duckdb connection & query runner
    open.go              // create view t from parquet/csv
    schema.go            // schema discovery
    preview.go           // preview queries (limit/offset)
    profile.go           // profiling queries + histogram
    filters.go           // build row_filter predicates

  internal/selection/
    selection.go         // ordered selection set ops

  internal/clipboard/
    clipboard_darwin.go  // pbcopy
    format.go            // python list formatting/escaping

  internal/types/
    models.go            // ColumnInfo, ColumnSummary, TableModel, etc.

  internal/util/
    fuzzy.go             // fuzzy match helper
    cancel.go            // cancellation/concurrency helpers

  testdata/
```

### Key data structures
- `ColumnInfo { Name string; DuckType string }`
- `ColumnSummary { MissingCount int64; MissingPct float64; DistinctApprox int64; DistinctPct float64; Top3 []TopValue; Numeric *NumericStats; Hist *Histogram }`
- `SelectionSet { ordered []string; selected map[string]bool }` (preserve file order)
- `ViewState { searchQuery string; showSelectedOnly bool; rowFilter string; cursorRow int; cursorCol int; offset int }`

---

# Task List (Codex-friendly tickets)

## T0 — Repo skeleton + tooling
**Deliverables**
- Go module init
- basic `cmd/pqview/main.go` with argument parsing
- placeholder Bubble Tea app boots and quits cleanly

**Acceptance**
- `go test ./...` passes
- `parqview <file>` opens UI frame (even before file loading)

---

## T1 — DuckDB embedded: open file + create view
**Steps**
- Add DuckDB Go binding (CGO)
- Open DuckDB connection
- Create `VIEW t` from file based on extension:
  - `.parquet` → `read_parquet(path)`
  - `.csv` → `read_csv_auto(path)`

**Acceptance**
- On open, schema query works
- File type auto-detected by extension

---

## T2 — Schema load + columns list
**Steps**
- Run `DESCRIBE t`
- Populate `[]ColumnInfo`
- Render right pane list (names + type badges)

**Acceptance**
- Columns pane lists all columns (up to 1000+) without lag
- Basic scrolling works

---

## T3 — Table preview pane (spreadsheet)
**Steps**
- Implement preview query:
  - projection = first K columns by default (e.g., 30)
  - `LIMIT N OFFSET offset` (N default 50)
- Render as table with fixed header row and row index
- Support vertical/horizontal scrolling

**Acceptance**
- Data visible immediately after open
- Smooth scroll for thousands of rows

---

## T4 — Fuzzy search (columns)
**Steps**
- `/` focuses search input
- Filter columns list by fuzzy match on name
- `Esc` exits search focus, keeps filter
- `Ctrl+U` clears search

**Acceptance**
- Typing filters list responsively
- No engine calls required for filtering

---

## T5 — Selection set operations
**Steps**
- Implement SelectionSet preserving file order
- Keybinds:
  - `Space` toggle highlighted column
  - `s` add all visible (filtered)
  - `d` remove all visible
  - `a` select all
  - `x` clear

**Acceptance**
- Selection count shown in status bar
- Selection persists while search changes

---

## T6 — Show selected columns only (table projection)
**Steps**
- `S` toggles table projection:
  - off: first K columns (or all visible later)
  - on: selected columns
- Handle empty selection gracefully (e.g., show message or fallback)

**Acceptance**
- Toggle updates table content quickly
- Horizontal scrolling still works

---

## T7 — Clipboard: copy selected columns as Python list
**Steps**
- Implement formatter:
  - output `["col_a", "col_b"]`
  - escape quotes/backslashes safely
- Implement macOS clipboard via `pbcopy`
- `y` copies selected-only list

**Acceptance**
- Clipboard paste into Python produces valid list literal
- Ordering matches file order

---

## T8 — Column summaries (Tier 1) async
**Steps**
- For visible columns, compute:
  - missing count + %
  - approx distinct + %
- Implement worker pool (max concurrency 4)
- Cancel/restart profiling when:
  - search filter changes
  - file re-opened

**Acceptance**
- UI stays responsive while summaries compute
- Visible columns populate stats first

---

## T9 — Column detail panel (Tier 2 on-demand)
**Steps**
- `Enter` opens Column Detail for highlighted column
- Compute on demand:
  - top 3 values + other %
  - numeric stats (min/max/mean/std) if numeric
- Render tabs: `Top Values` / `Stats` (histogram later)

**Acceptance**
- Detail opens instantly with “loading” then fills
- Top values computed only when needed

---

## T10 — Numeric histogram (10 bins)
**Steps**
- For numeric column in detail:
  - compute min/max
  - compute 10-bin counts (width_bucket)
  - render as text bars

**Acceptance**
- Histogram shows 10 bins (or 1 for constant cols)
- Works on floats and ints

---

## T11 — Missing rows filter (v1)
**Steps**
- Implement filter toggle (simple):
  - mode selected columns: `(c1 IS NULL OR c2 IS NULL ...)`
- Apply `WHERE row_filter` in preview query
- Show active filter in top bar

**Acceptance**
- Toggle reduces table to rows with nulls (in selected cols)
- Turning off restores normal view

---

## T12 — Jump to first null (per-column)
**Steps**
- In Column Detail, `n`:
  - query minimal row index where `col IS NULL` (implementation options):
    - if you maintain a row_number() view:
      - `SELECT min(rn) FROM (SELECT row_number() OVER () rn, col FROM t) WHERE col IS NULL;`
    - or approximate with LIMIT + scanning chunks (acceptable at small row counts)
- Set table offset/cursor to bring that row into view

**Acceptance**
- Pressing `n` moves table to the first null occurrence for that column (when exists)
- If no nulls, show “no nulls found”

---

## T13 — CSV support (minimal)
**Steps**
- Ensure `.csv` works with `read_csv_auto`
- Add optional flag placeholders (future):
  - delimiter, header, sample_size

**Acceptance**
- Can open a CSV and see schema/table
- Column search/selection/copy works

---

## T14 — Polish + stability
**Steps**
- Help overlay (`?`) listing keybindings
- Status area: selection count, profiling progress, errors
- Error handling for bad files / empty files
- Basic tests:
  - selection operations
  - python list formatter escaping

**Acceptance**
- No crashes on invalid input (shows error view)
- Basic unit tests pass

---

# Roadmap Notes (explicit “future” items)
- Tokenized column search: `missing>0 type:numeric name:*id*`
- “Only show columns with missing > 0” and sorting by missing%
- “Next missing cell” navigation (grid-aware)
- Quantile-based histogram bins
- Optional SQL scratchpad panel
- Optional disk cache for summaries in `~/.cache/pqview/`

---

# Design Decisions (agreed)

1. **Binary/module name:** `parqview` (not `pqview`)
2. **DuckDB binding:** `github.com/duckdb/duckdb-go` (official binding)
3. **Table horizontal scrolling:** All columns reachable via horizontal scroll; column widths kept simple initially (can optimize later)
4. **Row numbering:** Original file row numbers (1-based), preserved even under filters
5. **Fuzzy search:** Simple case-insensitive substring match for v1
6. **Profiling strategy:** Auto-profile visible columns first, then non-visible in background; max 4 concurrent workers
7. **CSV performance:** No special optimization for v1; accept that large CSVs may take a moment
8. **`row_number()` ordering:** Rely on natural file order for v1 (DuckDB returns rows in file order for single-file Parquet reads); no explicit `ORDER BY` needed

---

# Open Questions (for later)

- **Clipboard formatter edge cases:** Should column names with newlines, embedded quotes, or other unusual characters get special handling beyond basic quote/backslash escaping? (Unlikely in practice but worth considering if encountered.)
- **Deterministic row ordering:** If `row_number() OVER ()` ever produces mismatches between the preview query and the jump-to-null query, add explicit `ORDER BY rowid` to both. Monitor for this.
- **Column width heuristics:** How to size columns in the table pane — fixed width, auto-fit to content sample, or user-resizable? Start simple (fixed/truncated), revisit based on usage.

---
