_default:
    @just --list

# Build the main service binary
build:
    go build -o bin/peerdb-playground ./cmd/peerdb-playground

# Build the cdcbench tool
build-cdcbench:
    go build -o bin/cdcbench ./cmd/cdcbench

# Build all binaries
build-all: build build-cdcbench

# Run the main service
run:
    go run ./cmd/peerdb-playground

# Generate protobuf code via buf
gen:
    buf generate

# Run unit tests (excludes e2e)
test:
    go test $(go list ./... | grep -v /e2e)

# Run end-to-end tests
test-e2e:
    go test -tags=e2e ./e2e/...

# Run all tests
test-all:
    go test ./...

# Run golangci-lint
lint:
    golangci-lint run ./...

# Auto-fix lint issues where possible
lint-fix:
    golangci-lint run --fix ./...

# go vet + go fmt
vet:
    go vet ./...
    gofmt -l -w .

# Tidy go.mod
tidy:
    go mod tidy

up-all: peerdb-up peers-up
down-all: peerdb-down peers-down

# Bring up the peerdb stack. Default starts api+worker; pass `api` or `worker` to start only one, or `none` for neither.
peerdb-up MODE="all":
    #!/usr/bin/env bash
    set -euo pipefail
    case "{{MODE}}" in
        all)    docker compose --profile api --profile worker up -d ;;
        api)    docker compose --profile api up -d ;;
        worker) docker compose --profile worker up -d ;;
        none)   docker compose up -d ;;
        *)      echo "unknown mode: {{MODE}} (use: all | api | worker | none)" >&2; exit 1 ;;
    esac

peerdb-down:
    docker compose down

# Tail logs from all services
logs:
    docker compose logs -f

# Bring up only the source/destination peers
peers-up:
    docker compose -f docker-compose-peers.yml up -d

# Tear down peers
peers-down:
    docker compose -f docker-compose-peers.yml down

# Run a query against the Postgres source peer (omit SQL for interactive shell)
psql-source *SQL:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -z "{{SQL}}" ]; then
        docker compose -f docker-compose-peers.yml exec pg-source \
            psql -U postgres -d source
    else
        docker compose -f docker-compose-peers.yml exec -T pg-source \
            psql -U postgres -d source -c "{{SQL}}"
    fi

# Run a query against the ClickHouse destination peer (omit SQL for interactive shell)
ch-dest *SQL:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -z "{{SQL}}" ]; then
        docker compose -f docker-compose-peers.yml exec ch-dest \
            clickhouse-client -u default --password clickhouse -d destination
    else
        docker compose -f docker-compose-peers.yml exec -T ch-dest \
            clickhouse-client -u default --password clickhouse -d destination \
            --query "{{SQL}}"
    fi

# Run a query against the PeerDB metastore (omit SQL for interactive shell)
psql-meta *SQL:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -z "{{SQL}}" ]; then
        docker compose exec peerdb-postgres \
            psql -U postgres -d peerdb
    else
        docker compose exec -T peerdb-postgres \
            psql -U postgres -d peerdb -c "{{SQL}}"
    fi

# Remove build artifacts
clean:
    rm -rf bin gen
