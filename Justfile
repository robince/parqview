version := `git describe --tags --always --dirty 2>/dev/null || echo "dev"`
commit := `git rev-parse HEAD 2>/dev/null || echo "unknown"`
date := `date -u +%Y-%m-%dT%H:%M:%SZ`
ldflags := "-X github.com/robince/parqview/internal/version.Version=" + version + " -X github.com/robince/parqview/internal/version.Commit=" + commit + " -X github.com/robince/parqview/internal/version.Date=" + date

build:
    go build -ldflags "{{ldflags}}" -o parqview ./cmd/parqview

test:
    go test ./...

lint:
    golangci-lint run ./...

install: build
    cp parqview ~/.local/bin/parqview

clean:
    rm -f parqview
