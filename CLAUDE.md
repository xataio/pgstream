# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

pgstream is a PostgreSQL Change Data Capture (CDC) tool and Go library. It captures WAL changes from PostgreSQL and routes them to multiple targets: PostgreSQL, Elasticsearch/OpenSearch, Kafka, or Webhooks. It supports schema tracking, DDL replication, data snapshots, and column-level transformations for anonymization.

## Common Commands

```bash
# Build
make build

# Run all unit tests (with race detector, 10m timeout)
make test

# Run a single test
go test -run TestName ./pkg/path/to/package

# Run integration tests (requires Docker for testcontainers)
PGSTREAM_INTEGRATION_TESTS=true go test -timeout 180s github.com/xataio/pgstream/pkg/stream/integration

# Lint (golangci-lint v2)
make lint

# Run code generation (CLI definition + transformer definition)
make generate

# Generate migration binaries
make gen-migrations

# Fuzz tests
make fuzz

# License header check
make license-check
```

## Architecture

### Pipeline Design

```
Source (PostgreSQL WAL / Kafka) → Listener
  → Processor chain: Filter → Injector → Transformer → Target Writer
    → Checkpointer (tracks LSN/offset progress)
```

The streaming pipeline is assembled in `pkg/stream/stream.go`. Sources produce WAL events, which flow through an ordered chain of processors before reaching the target writer. Each processor is optional and configured independently.

### Key Package Layout

- **`cmd/`** — CLI commands (cobra): `run`, `init`, `snapshot`, `status`, `destroy`, `validate`
- **`cmd/config/`** — Configuration parsing (YAML, env vars, CLI flags via viper)
- **`pkg/stream/`** — Pipeline orchestration: wires listeners, processors, and checkpointers together
- **`pkg/wal/listener/`** — WAL event sources (PostgreSQL replication, Kafka consumer)
- **`pkg/wal/processor/`** — Processing stages:
  - `postgres/` — PostgreSQL target writer
  - `search/` — Elasticsearch/OpenSearch indexer
  - `kafka/` — Kafka batch writer
  - `webhook/` — HTTP webhook notifier
  - `transformer/` — Column value transformations
  - `filter/` — Schema/table filtering
  - `injector/` — Metadata/ID injection
- **`pkg/wal/replication/`** — Replication slot management
- **`pkg/wal/checkpointer/`** — LSN/offset checkpoint tracking
- **`pkg/snapshot/`** — Initial and on-demand snapshot logic
- **`pkg/transformers/`** — Data transformation implementations (greenmask, neosync, go-masker integrations)
- **`internal/`** — Internal utilities (postgres client, search store, migrator, test helpers)
- **`migrations/`** — SQL migrations (core schema + injector), compiled to Go via go-bindata

### Configuration

Supports YAML files, `.env` files, and CLI flags. Config parsing lives in `cmd/config/`. Environment variables use the `PGSTREAM_` prefix.

### Integration Tests

Located in `pkg/stream/integration/`. They use testcontainers-go to spin up PostgreSQL, Elasticsearch, OpenSearch, and Kafka. Gated by `PGSTREAM_INTEGRATION_TESTS=true` env var.

## Code Conventions

- **Error wrapping**: Use `fmt.Errorf` with `%w`. The `github.com/pkg/errors` package is forbidden.
- **No fmt.Print**: Use structured logging (zerolog). `fmt.Print*` calls are blocked by linter.
- **Formatting**: gofumpt (enforced by golangci-lint).
- **License headers**: All `.go` files must start with `// SPDX-License-Identifier: Apache-2.0`.
- **No CGO**: Builds use `CGO_ENABLED=0`.
- **Interface-driven design**: Processors, listeners, and stores use interfaces extensively with mock implementations for testing.
