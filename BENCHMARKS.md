# CDC Benchmarks

The repo includes a manual benchmark runner at `cmd/cdcbench` for exploratory CDC load testing.

## Prerequisites

- Docker running locally
- `temporal` CLI installed and available on `PATH`
- Go toolchain installed

The benchmark starts its own local Postgres, MySQL, ClickHouse, Temporal dev server, worker, and in-process API. It does not use your existing `docker compose` stack.

## Run

From the repo root:

```bash
go run ./cmd/cdcbench \
  -source=postgres \
  -scenario=steady \
  -output=./benchmarks
```

Other common variants:

```bash
go run ./cmd/cdcbench -source=mysql -scenario=steady -output=./benchmarks
go run ./cmd/cdcbench -source=both -scenario=ramp -output=./benchmarks
go run ./cmd/cdcbench -source=postgres -scenario=burst -output=./benchmarks
go run ./cmd/cdcbench -source=postgres -scenario=steady -duration=1m -rate=100 -poll-interval=10s -output=./benchmarks
go run ./cmd/cdcbench -source=postgres -scenario=steady -flush-interval-ms=500 -max-batch-size=5000 -output=./benchmarks
```

## Flags

- `-source=postgres|mysql|both`
- `-scenario=steady|ramp|burst`
- `-output=<dir>`
- `-rate=<int>` overrides the scenario's base rate
- `-duration=<Go duration>` like `30s`, `1m`, `8m`
- `-poll-interval=<Go duration>` like `5s`, `10s`
- `-flush-interval-ms=<int>`
- `-max-batch-size=<int>`

## Default Scenario Durations

- `steady`: 5 minutes
- `ramp`: up to 10 minutes
- `burst`: 8 minutes

Add some extra time for container startup and shutdown.

## Output

Each run writes:

- `<source>-<scenario>-<timestamp>.json`
- `<source>-<scenario>-<timestamp>.md`

into the directory passed via `-output`.

The benchmark also prints a one-line summary when it completes, for example:

```text
postgres/steady target=250.0 achieved=247.6 visible=228.0 p95=949.6ms max_backlog=115 output=./benchmarks
```

## How To Tell If It Finished

Treat the run as complete when you see the summary line above.

The log lines after that about the worker stopping, activity heartbeat failures, or `context canceled` are expected shutdown noise from terminating the long-running CDC workflow after the benchmark is done.

If you do not see the summary line, the run did not complete successfully.

## Reading The Summary

- `target`: configured write rate for the scenario
- `achieved`: actual source write rate reached by the benchmark writer
- `visible`: destination-side visible ingest rate
- `p50/p95/p99/max latency`: destination `_ingested_at - event_ts`
- `max_backlog`: largest observed gap between written source `seq` and visible destination `seq`
- `FINAL count` / `FINAL max seq`: one-off end-of-run validation using `FINAL`

## Practical Advice

- Start with `-duration=1m` for smoke checks.
- Use `steady` first to find a sustainable rate.
- Use `ramp` to find the point where backlog starts growing.
- Use `burst` to see recovery behavior after spikes.
