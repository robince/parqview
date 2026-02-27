# parqview

Terminal UI for exploring Parquet and CSV files with DuckDB-backed preview and profiling.

## Run

```bash
go run ./cmd/parqview <file.parquet|file.csv>
```

## Missing-Value Policy

By default, parqview treats both `NULL` and `NaN` as missing values for:

- Missing indicators (orange dots/marker styles)
- Missing-row filter (`f`)
- Missing navigation (`n`, `r`/`R`, `c`/`C`)
- Missing counts in profiling/footers

This behavior is controlled by [`internal/missing/policy.go`](internal/missing/policy.go):

- `IncludeNaNAsMissing = true` (default): `NULL` + `NaN`
- `IncludeNaNAsMissing = false`: `NULL` only

## Keyboard Shortcuts

Shortcuts below describe app-specific behavior. When the column search input is focused, normal text editing keys are handled by Bubble `textinput`.

### Global (no overlay open, search not focused)

| Key | Action |
| --- | --- |
| `Tab` | Switch focus between table and columns panes |
| `q`, `Ctrl+C` | Quit |
| `Ctrl+L` | Redraw screen |
| `?` | Open/close help overlay |
| `s`, `S` | Toggle show-selected-columns projection |
| `Enter` | Open detail panel for the active column |
| `Space` | Page down in focused pane (`table` or `columns`) |

### Columns Pane (focus on columns)

| Key | Action |
| --- | --- |
| `/` | Focus column search input |
| `Up`, `k` | Move cursor up 1 row |
| `Down`, `j` | Move cursor down 1 row |
| `Space`, `Ctrl+F` | Page down |
| `Ctrl+B` | Page up |
| `Ctrl+D` | Half-page down |
| `Ctrl+U` | Half-page up |
| `g`, `Home` | Jump to top of full list |
| `G`, `End` | Jump to bottom of full list |
| `H` | Jump to top of current visible list window (no scrolling) |
| `M` | Jump to middle of current visible list window (no scrolling) |
| `L` | Jump to bottom of current visible list window (no scrolling) |
| `x` | Toggle selection on active (crosshair) column |
| `a` | Add all filtered columns to selection |
| `d` | Remove all filtered columns from selection |
| `A` | Select all columns |
| `X` | Clear all selected columns |
| `y` | Copy selected columns as a Python list |
| `Enter` | Open detail panel for active column |

### Table Pane (focus on table)

| Key | Action |
| --- | --- |
| `Up`, `k` | Move row cursor up |
| `Down`, `j` | Move row cursor down |
| `Left`, `h` | Move selected column left |
| `Right`, `l` | Move selected column right |
| `0` | Jump to first visible table column |
| `$` | Jump to last visible table column |
| `[` | Page table columns left |
| `]` | Page table columns right |
| `g` | Jump to top row |
| `G` | Jump to bottom row |
| `Space`, `Ctrl+F` | Page down |
| `Ctrl+B` | Page up |
| `Ctrl+D` | Half-page down |
| `Ctrl+U` | Half-page up |
| `r` | Jump to next missing value in current row |
| `R` | Jump to previous missing value in current row |
| `c` | Jump to next row with missing value in selected column |
| `C` | Jump to previous row with missing value in selected column |
| `f` | Toggle missing-row filter |
| `Enter` | Open detail panel for selected column |

### Search Input Mode (columns search focused)

| Key | Action |
| --- | --- |
| `Esc` | Exit search input |
| `Enter` | Commit query and exit search input |
| `Ctrl+U` | Clear search query |
| Text editing keys | Edit search query |

### Help Overlay

| Key | Action |
| --- | --- |
| `Esc`, `?`, `q` | Close help |

### Detail Panel Overlay

| Key | Action |
| --- | --- |
| `t` | Cycle tabs (Top Values, Stats, Histogram) |
| `n` | Jump to first missing value for detail column |
| `Esc`, `q` | Close detail panel |

### Mouse

| Action | Behavior |
| --- | --- |
| Mouse wheel | Scroll cursor in focused pane |
| Left-drag divider | Resize table/columns split |
