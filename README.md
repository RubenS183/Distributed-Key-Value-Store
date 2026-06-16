# KV Store

Distributed key-value database skeleton in modern C++.

## Status

Early implementation scaffold. Module boundaries, build system, tests, Docker, and docs are in place; runtime behavior is still being filled in.

## Features Planned

- TCP server with custom command protocol
- In-memory key-value engine
- TTL support
- Write-ahead log and snapshots
- Leader-follower replication
- LRU cache layer
- Metrics and profiling hooks
- Unit, integration, stress, and benchmark targets

## Layout

```text
include/kvstore/    Public module headers
src/                Implementations
tests/              Unit, integration, stress tests
benchmarks/         Benchmark targets
configs/            Example configuration
docker/             Dockerfile and Compose setup
docs/               Architecture and design notes
scripts/            Build, test, format helpers
```

## Requirements

- C++20 compiler
- CMake 3.22+
- Ninja
- Docker, optional

## Build

```sh
./scripts/build.sh
```

Manual:

```sh
cmake --preset dev
cmake --build --preset dev
```

Release:

```sh
cmake --preset release
cmake --build --preset release
```

## Test

```sh
./scripts/test.sh
```

Or:

```sh
ctest --test-dir build/dev --output-on-failure
```

## Run

```sh
./build/dev/kvstore
```

Example config:

```sh
configs/kvstore.example.yaml
```

Default service ports:

- `6379`: key-value TCP protocol
- `9090`: metrics

## Docker

```sh
docker compose -f docker/docker-compose.yml up --build
```

## Documentation

- [Architecture](docs/architecture.md)
- [Protocol](docs/protocol.md)
- [Storage](docs/storage.md)
- [Replication](docs/replication.md)
- [Operations](docs/operations.md)
- [Testing](docs/testing.md)

## Development

```sh
./scripts/format.sh
./scripts/build.sh
./scripts/test.sh
```

Keep headers as ownership boundaries and add behavior in the matching `src/` module.
