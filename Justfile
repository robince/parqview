build:
    go build -o parqview ./cmd/parqview

test:
    go test ./...

lint:
    golangci-lint run ./...

install: build
    cp parqview ~/.local/bin/parqview

clean:
    rm -f parqview
