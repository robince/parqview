# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Copy active cell value to clipboard with `y` when in data pane
- Add jump to row command and CLI argument (all displayed row numbers now refer to overall file)
- Add predicate filtering rules on columns (`p`, `=` etc. )

### Changed

### Fixed
- Filter to missing row updates when selected columns change

## [1.2.0] - 2026-03-08

### Added
- Long-text field reader support
- JSON and JSONL/NDJSON support

## [1.1.1] - 2026-03-08

### Added
- Windows clipboard support

## [1.1.0] - 2026-03-06

### Added
- Missing mode toggle (NaN+NULL -> NULL -> NaN)

## [1.0.0] - 2026-02-28

Initial public release of parqview.

This v1.0.0 milestone delivers the original MVP scope in a usable, shareable state, including:
- Terminal UI for exploring Parquet and CSV data
- DuckDB-backed table preview and profiling
- Column browser, selection workflows, and search
- Missing-value navigation and filtering
- Column detail views (top values, stats, histogram)
- Keyboard and mouse-driven navigation
