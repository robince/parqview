build:
    go build -o parqview ./cmd/parqview

# DevContainer helpers
dc-up:
    devcontainer up --workspace-folder .

dc-shell:
    devcontainer exec --workspace-folder . bash

dc-rebuild:
    devcontainer up --workspace-folder . --remove-existing-container

test:
    go test ./...

lint:
    golangci-lint run ./...

install: build
    cp parqview ~/.local/bin/parqview

clean:
    rm -f parqview
