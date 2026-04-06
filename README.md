# PeerDB Playground

A Change Data Capture (CDC) tool that replicates data from source databases to destination databases in real-time.

## Features

- **Initial snapshot** — full table copy from source to destination
- **CDC streaming** — continuous replication of inserts, updates, and deletes (coming soon)
- **Schema discovery** — automatically detects source table schemas and creates destination tables
- **Parallel snapshots** — snapshot multiple tables concurrently with bounded parallelism
- **At-least-once delivery** — safe against failures with automatic deduplication at the destination

## Supported Connectors

| Source     | Destination |
|------------|-------------|
| PostgreSQL | ClickHouse  |
| MySQL 8    | ClickHouse  |

## Getting Started

```bash
# Start all components (api and worker will be built from source)
docker compose up -d
```

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
