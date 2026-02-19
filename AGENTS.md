# Repository Guidelines

## Project Structure & Module Organization
- `cmd/parqview/main.go`: CLI entrypoint for the TUI app.
- `internal/engine`: file loading, DuckDB-backed querying, profiling, and preview logic.
- `internal/ui`: Bubble Tea model and Lip Gloss styling for terminal rendering.
- `internal/selection`, `internal/clipboard`, `internal/util`, `internal/types`: shared domain helpers and utilities.
- `testdata/`: local sample datasets used by integration-like tests (`sample.parquet`, `sample.csv`) plus generator source.
- `plans/`: planning/spec docs (non-runtime).

## Build, Test, and Development Commands
- `go run ./cmd/parqview <file.parquet|file.csv>`: run the app locally.
- `go build -o parqview ./cmd/parqview`: build the binary in repo root.
- `go test ./...`: run all unit/integration tests.
- `go test ./internal/engine -run TestOpenParquet -v`: run a focused test.
- `go run testdata/gen_test_data.go`: regenerate test fixtures when needed.

## Coding Style & Naming Conventions
- Follow standard Go style and keep code `gofmt`-clean (tabs, canonical imports, idiomatic layout).
- Use package-oriented naming and keep exported identifiers in `CamelCase`, unexported in `camelCase`.
- Prefer small, focused functions and context-aware methods in engine/query code.
- Keep TUI behavior/state transitions in `internal/ui` rather than engine packages.

## Testing Guidelines
- Place tests next to code as `*_test.go` (current pattern across `internal/*`).
- Name tests with `TestXxx` and use table-driven style where multiple scenarios exist.
- Run `go test ./...` before opening a PR.
- Engine tests rely on `testdata/sample.parquet`; regenerate via `go run testdata/gen_test_data.go` if fixtures drift.

## Architecture Overview
- `cmd/parqview` validates input paths, constructs an `engine.Engine`, then starts the Bubble Tea program.
- `internal/engine` is the data boundary: it opens parquet/CSV sources through DuckDB and exposes query/profile methods.
- `internal/ui` owns screen state, keyboard handling, and rendering; it calls engine methods and maps results into view models.
- `internal/selection` and `internal/clipboard` support row/column interaction flows without leaking terminal concerns into query code.

## Commit & Pull Request Guidelines
- Current history uses short, direct, lowercase commit subjects (example: `initial commit for app`). Keep subjects concise and imperative.
- One logical change per commit; include rationale in body when behavior changes.
- PRs should include:
  - what changed and why,
  - test evidence (`go test ./...` output summary),
  - screenshots/terminal captures for visible TUI changes,
  - linked issue/task when applicable.
