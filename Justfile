set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

version := `git describe --tags --always --dirty 2>/dev/null || echo "dev"`
commit := `git rev-parse HEAD 2>/dev/null || echo "unknown"`
date := `date -u +"%Y-%m-%dT%H:%M:%SZ"`
bin := "parqview"
pkg := "./cmd/parqview"
version_pkg := "github.com/robince/parqview/internal/version"

ldflags := "-X " + version_pkg + ".Version=" + version + " -X " + version_pkg + ".Commit=" + commit + " -X " + version_pkg + ".Date=" + date
ldflags_release := ldflags + " -s -w"

default: help

# Show available commands.
help:
    @just --list

# Debug/local build with version metadata.
build:
    CGO_ENABLED=1 go build -ldflags '{{ldflags}}' -o {{bin}} {{pkg}}
    chmod +x {{bin}}

# Reproducible release-style local build (stripped, trimpath).
build-release:
    CGO_ENABLED=1 go build -trimpath -buildvcs=false -ldflags '{{ldflags_release}}' -o {{bin}} {{pkg}}
    chmod +x {{bin}}

# Run test suite.
test:
    go test ./...

# Run tests with verbose output.
test-v:
    go test -v ./...

# Format all packages.
fmt:
    go fmt ./...

# Keep module definitions tidy.
tidy:
    go mod tidy

# Lint using golangci-lint.
lint:
    if command -v golangci-lint >/dev/null 2>&1; then \
      golangci-lint run ./...; \
    else \
      echo "golangci-lint not found. Install with: go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.10.1"; \
      exit 1; \
    fi

# Run formatting, linting, and tests.
check: fmt lint test

# Display binary and linked-library details.
size:
    ls -lh {{bin}}
    file {{bin}}
    if command -v otool >/dev/null 2>&1; then otool -L {{bin}}; fi
    if command -v ldd >/dev/null 2>&1; then ldd {{bin}}; fi

# Build and run with optional args, e.g. `just run testdata/sample.parquet`.
run *args:
    go run {{pkg}} {{args}}

# Install to ~/.local/bin when available, otherwise GOBIN or GOPATH/bin.
install: build-release
    if [ -d "$HOME/.local/bin" ]; then \
      install -m 0755 {{bin}} "$HOME/.local/bin/{{bin}}"; \
      echo "Installed to $HOME/.local/bin/{{bin}}"; \
    else \
      install_dir="${GOBIN:-$(go env GOBIN)}"; \
      if [ -z "$install_dir" ]; then \
        gopath_first="$(go env GOPATH | cut -d: -f1)"; \
        install_dir="$gopath_first/bin"; \
      fi; \
      mkdir -p "$install_dir"; \
      install -m 0755 {{bin}} "$install_dir/{{bin}}"; \
      echo "Installed to $install_dir/{{bin}}"; \
    fi

# Local/dev ad-hoc signing only; this is not Developer ID signing or notarization.
sign-local:
    if [ "$(uname -s)" != "Darwin" ]; then \
      echo "sign-local is only supported on macOS"; \
      exit 1; \
    fi
    codesign --sign - {{bin}}

# Validate changelog entry for a release version (X.Y.Z).
changelog version:
    ./scripts/check-changelog.sh {{version}}

# Create and push an annotated release tag (X.Y.Z).
release version:
    ./scripts/release.sh {{version}}

# DevContainer helpers.
dc-up:
    devcontainer up --workspace-folder .

dc-shell:
    devcontainer exec --workspace-folder . bash

dc-rebuild:
    devcontainer up --workspace-folder . --remove-existing-container

# Remove local build artifacts.
clean:
    rm -f {{bin}}
