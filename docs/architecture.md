# Architecture

## Modules

- `server`: process lifecycle and dependency wiring
- `network`: TCP listener, sessions, connection pool
- `protocol`: command parsing and response encoding
- `storage`: in-memory key-value engine, TTL index
- `persistence`: snapshots and write-ahead log
- `replication`: leader-follower coordination, log shipping, state sync
- `cache`: LRU and eviction policies
- `concurrency`: thread pool, scheduling, lock-free primitives
- `metrics`: counters, timers, profiling hooks
- `utils`: config, logging, shared helpers

## Rule

Headers define ownership boundaries. Source files stay behavior-free until implementation begins.
