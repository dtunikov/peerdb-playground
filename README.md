# PeerDB Playground

A Change Data Capture (CDC) tool that replicates data from source databases to destination databases in real-time.

## Features

- **Initial snapshot** — full table copy from source to destination
- **CDC streaming** — continuous replication of inserts, updates, and deletes (coming soon)
- **Schema discovery** — automatically detects source table schemas and creates destination tables
- **At-least-once delivery** — safe against failures with automatic deduplication at the destination

## Supported Connectors

| Source     | Destination |
|------------|-------------|
| PostgreSQL | ClickHouse  |
| MySQL 8    | ClickHouse  |

## Getting Started

```bash
# Start all components (api and worker will be built from source)
docker compose --profile api --profile worker up -d
```

The `api` and `worker` services live behind compose profiles so you can opt out of either — for example, run only the API and start a worker locally via `go run`:

```bash
docker compose --profile api up -d
go run ./cmd/peerdb-playground -mode=worker
```

If you use [just](https://github.com/casey/just), `just peerdb-up` is the equivalent of the full command above; pass `api`, `worker`, or `none` to start a subset.

### Create a CDC flow

```bash
# 1. Register source and destination peers
grpcurl -plaintext -d '{
  "peer": {
    "name": "my-postgres",
    "type": 1,
    "postgres_config": {
      "host": "localhost", "port": 5433,
      "user": "postgres", "password": "postgres",
      "database": "source", "ssl_mode": "disable"
    }
  }
}' localhost:8080 peerdb.PeerdbService/CreatePeer

grpcurl -plaintext -d '{
  "peer": {
    "name": "my-clickhouse",
    "type": 2,
    "clickhouse_config": {
      "host": "localhost", "port": 9000,
      "user": "default", "password": "clickhouse",
      "database": "destination"
    }
  }
}' localhost:8080 peerdb.PeerdbService/CreatePeer

# 2. Create a CDC flow (starts snapshot automatically)
grpcurl -plaintext -d '{
  "cdc_flow": {
    "name": "pg-to-ch",
    "source": "<source-peer-id>",
    "destination": "<dest-peer-id>",
    "config": {
      "postgres_source": {}
    }
  }
}' localhost:8080 peerdb.PeerdbService/CreateCDCFlow
```

For MySQL sources, register a `MYSQL` peer with `mysql_config` and create the flow with `mysql_source`.

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed design decisions and project structure.

## Benchmarking

For manual CDC load-testing instructions, see [BENCHMARKS.md](BENCHMARKS.md).
