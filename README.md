# parqview

Keyboard-first terminal UI for exploring Parquet, CSV, JSON, JSONL, and NDJSON files with DuckDB-backed preview and profiling.

## Why This Exists

`parqview` is for fast inspection of parquet, csv, json, jsonl, and ndjson files.

Key features:

- select a set of columns and toggle visibility
- copy the selected columns as Python list literal
- copy the active cell value from the data pane
- search column names
- summaries of column data
- fast vim-inspired keyboard navigation and scrolling
- predicate filtering 
- visual indication of missing data in rows and columns, jump to next missing value
- missing definition can toggle between NULL+NaN, NULL, NaN 
- adaptive column width controls with an expanded reader for long text columns
- resize panels with the mouse
- fast mouse scrolling

Use it to quickly catch:

- unexpected feature values
- unexpected NULLs or NaNs
- join failures (duplicate columns, mismatched keys, broken join coverage),

The aim is to provide smooth, low-friction, interactive data browsing with familiar and intuitive keyboard navigation.

## Screenshot

![parqview main workspace](docs/images/parqview-screenshot.png)

## Installation

### Homebrew (recommended)

```bash
brew install robince/tap/parqview
```

### Direct download from GitHub Releases

Download the archive for your platform from
`https://github.com/robince/parqview/releases`.

macOS arm64 example:

```bash
VERSION="1.1.0"
curl -fL -o parqview.tar.gz "https://github.com/robince/parqview/releases/download/v${VERSION}/parqview_${VERSION}_darwin_arm64.tar.gz"
tar -xzf parqview.tar.gz
./parqview --version
```

Linux amd64 example:

```bash
VERSION="1.1.0"
curl -fL -o parqview.tar.gz "https://github.com/robince/parqview/releases/download/v${VERSION}/parqview_${VERSION}_linux_amd64.tar.gz"
tar -xzf parqview.tar.gz
./parqview --version
```

Note for direct macOS downloads: if Gatekeeper blocks first run, remove quarantine:

```bash
xattr -dr com.apple.quarantine ./parqview
```

## Run

Open a file directly:

```bash
./parqview <file.parquet|file.csv|file.json|file.jsonl|file.ndjson>
```

Start with a specific row displayed:

```bash
./parqview +250 <file.parquet|file.csv|file.json|file.jsonl|file.ndjson>
```

Or run from source:

```bash
go run ./cmd/parqview <file.parquet|file.csv|file.json|file.jsonl|file.ndjson>
```

The `+row` argument can appear before or after the file path. Row numbers are 1-based and refer to the file's displayed row numbers, even when filters are active.

If the app starts without a file, press `Ctrl+O` to open the file picker.

JSON support uses DuckDB's default auto-detection readers:
- `.json` uses `read_json_auto`
- `.jsonl` and `.ndjson` use `read_ndjson_auto`
- nested `LIST` and `STRUCT` values are preserved as DuckDB returns them

## Screens and Keys

Shortcuts below describe app-specific behavior. When a search input is focused, normal text editing keys are handled by the Charmbracelet Bubbles `textinput` component.

### Global commands

These keys are global commands that work whichever pane has focus.

| Key | Action |
| --- | --- |
| `Tab` | Switch focus between table and columns panes |
| `Ctrl+O` | Open/close file picker (`.parquet`/`.csv`/`.json`/`.jsonl`/`.ndjson`) |
| `q`, `Ctrl+C` | Quit |
| `Ctrl+L` | Redraw screen |
| `?` | Open/close help overlay |
| `m` | Cycle missing mode (`NULL+NaN` → `NULL only` → `NaN only`) |
| `s`, `S` | Toggle selected-columns view in data table |
| `v`, `V` | Toggle selected-columns view in columns pane (columns focus) |
| `Enter` | Open detail panel for active column |
| `Space` | Page down in focused pane |

### Columns Pane

Use this pane to search, triage, and build a selection set of columns.

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
| `H` | Jump to top of current visible list window |
| `M` | Jump to middle of current visible list window |
| `L` | Jump to bottom of current visible list window |
| `x` | Toggle selection on active (crosshair) column |
| `a` | Add all filtered columns to selection |
| `d` | Remove all filtered columns from selection |
| `A` | Select all columns |
| `X` | Clear all selected columns |
| `y` | Copy selected columns as a Python list |
| `Enter` | Open detail panel for active column |

### Table Pane

Use this pane to inspect row values, navigate missingness, and filter the current result set.

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
| `w` | Toggle fit-width for ordinary values, or open the expanded reader when the visible column sample is too wide/multiline |
| `Ctrl+W` | Toggle global wide-columns mode |
| `gg` | Jump to top row |
| `G` | Jump to bottom row |
| `[count]gg`, `[count]G` | Jump to row number `count` |
| `Space`, `Ctrl+F` | Page down |
| `Ctrl+B` | Page up |
| `Ctrl+D` | Half-page down |
| `Ctrl+U` | Half-page up |
| `y` | Copy active cell value |
| `=` | Open predicate prompt for selected column |
| `p` | Pin current cell value as an exact-match predicate |
| `-` | Clear predicate for selected column |
| `U` | Clear all predicates |
| `r` | Jump to next missing value in current row |
| `R` | Jump to previous missing value in current row |
| `c` | Jump to next row with missing value in selected column |
| `C` | Jump to previous row with missing value in selected column |
| `f` | Toggle missing-row filter |
| `Enter` | Open detail panel for selected column |

Predicate prompt examples:

- string columns: `abc123`, `!= abc123`
- numeric columns: `42`, `!= 42`, `> 10`, `>= 10`, `< 10`, `<= 10`, `10..20`

Predicate notes:

- `=` opens the prompt prefilled with the current predicate for that column, or the active cell value when no predicate exists yet.
- `p` applies an exact-match predicate from the active cell without opening the prompt.
- Comparisons (`>`, `>=`, `<`, `<=`, `a..b`) require a numeric column.
- Multiple column predicates combine with `AND`.
- Reapplying a predicate on a column replaces the previous predicate for that column.
- Row jumps still use overall displayed row numbers; if that exact row is filtered out, parqview jumps to the next visible row and tells you which row it landed on.

### Column Search Input (in Columns Pane)

| Key | Action |
| --- | --- |
| `Esc` | Clear query and exit search |
| `Enter` | Commit query and exit search |
| `Ctrl+U` | Clear search query |
| Text editing keys | Edit search query |

### Detail Panel Overlay

| Key | Action |
| --- | --- |
| `t` | Cycle tabs (Top Values, Stats, Histogram) |
| `n` | Jump to first missing value for detail column |
| `Esc`, `q` | Close detail panel |

### Expanded Reader Overlay

Use this overlay for long text cells, JSON payloads, and other values that do not fit comfortably in the table.

| Key | Action |
| --- | --- |
| `Up`, `k` | Scroll up |
| `Down`, `j` | Scroll down |
| `Left`, `h` | Pan left when wrap is off |
| `Right`, `l` | Pan right when wrap is off |
| `n`, `p` | Move to next/previous row in the same column |
| `W` | Toggle wrap |
| `Space`, `Ctrl+F` | Page down |
| `Ctrl+B` | Page up |
| `Ctrl+D` | Half-page down |
| `Ctrl+U` | Half-page up |
| `g` | Jump to top of current cell |
| `G` | Jump to bottom of current cell |
| `Esc`, `q`, `w` | Close expanded reader |

### File Picker Overlay

| Key | Action |
| --- | --- |
| `Enter` | Open selected file/folder |
| `Backspace` | Go to parent folder (when query is empty) |
| `Ctrl+U` | Clear picker query |
| `Esc` | Close file picker |

### Predicate Prompt Overlay

| Key | Action |
| --- | --- |
| `Enter` | Apply predicate |
| `Ctrl+U` | Clear prompt input |
| `Esc` | Cancel |

### Help Overlay

| Key | Action |
| --- | --- |
| `Esc`, `?`, `q` | Close help |

### Mouse

| Action | Behavior |
| --- | --- |
| Mouse wheel | Scroll cursor in focused pane |
| Left-drag divider | Resize table/columns split |

## Technical Details: Missing Definition

parqview has a runtime missing-value mode, toggled with `m`, with three states:

- `NULL+NaN` (default),
- `NULL only`,
- `NaN only`.

The active mode is shown in the top-right status badge and affects:

- missing indicators and missing cell styling,
- missing-row filter (`f`),
- missing navigation (`n`, `r`/`R`, `c`/`C`),
- missing counts in profiling and footers.

For categorical profiling, the active missing mode determines which values are excluded.

For numeric stats and histograms, parqview always profiles only finite numeric values. That means:

- `NULL` is excluded from numeric stats/histograms even in `NaN only`,
- `NaN` is excluded from numeric stats/histograms even in `NULL only`.
