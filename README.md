# kvstore

A replicated, in-memory key-value store with a custom TCP protocol. See
`docs/architecture.md` for the current system design and wire format, and
`docs/limitations.md` for what it does not (yet) do.

**Current status:** all stages of the project's build order complete — core
store, TCP protocol, concurrency, WAL durability, snapshots, replication,
failover, and the stage-8 benchmarking pass — plus stage 9 (online
resharding). Thread-per-connection, hash-sharded across N independent
shards. Supports PUT/GET/DELETE over a custom length-prefixed
binary TCP protocol, routed by key hash to one of N shards, each an
independent `std::shared_mutex`-guarded store with its own write-ahead log
for crash durability and point-in-time snapshots that bound WAL replay time
on restart. Each shard can be replicated from one leader node to one or
more follower nodes (async op-log shipping over the same wire protocol,
WAL/snapshot-backed catch-up after a disconnect) — see
`docs/architecture.md`'s "Replication" section. As of stage 7, a cluster
with peers configured also runs heartbeat-based failover: a follower is
promoted (with an epoch bump) if the leader goes silent, and a stale leader
that reappears is told to step down — see `docs/architecture.md`'s
"Failover" section and `docs/limitations.md` for the (deliberately
non-quorum) guarantees this does and doesn't provide. As of stage 9, the
shard count itself can be changed online with no downtime
(`ShardedStore::rebalance_to()` — throttled copy, live writes forwarded
during the migration, one atomic routing cutover) — see
`docs/architecture.md`'s "Rebalancing" section. This is in-process
resharding within one node, not cross-node shard placement: every node
still hosts every shard (see `docs/limitations.md`).

## Setup

Requires CMake 3.20+ and a C++17 compiler. Catch2 (test framework) is fetched
automatically by CMake on first build.

```
./scripts/build.sh          # Debug build (default)
./scripts/build.sh Release  # Release build
```

Build artifacts land in `build/`.

## Running

```
./build/src/kvstore_server [port] [wal_dir] [snapshot_dir] [num_shards] [role] [peer_addrs]
# defaults: port 6380, wal_dir "wal_data", snapshot_dir "snapshot_data",
#           num_shards 8, role "leader", peer_addrs "" (no failover)
```

Each shard gets its own subdirectory: shard `i`'s WAL lives in
`<wal_dir>/shard_<i>`, its snapshot in `<snapshot_dir>/shard_<i>`. At
startup the server loads the latest snapshot for every shard (if any) and
replays only the WAL records written after it, logs how long that took,
then binds and blocks, accepting connections and spawning one thread per
connection. There is currently no way to trigger a snapshot on a running
server — see `docs/limitations.md`.

`role` is `leader` (default) or `follower` — this is only the *starting*
role; with `peer_addrs` configured (below), a follower can be promoted to
leader at runtime, and a leader can be demoted (see "Failover" below).
While in the leader role, a node rejects REPLICATE/CATCHUP_QUERY/HEARTBEAT
from a stale epoch; while in the follower role, it rejects direct client
PUT/GET/DELETE (`NOT_LEADER`).

`peer_addrs` is a comma-separated `host:port,host:port,...` list of every
*other* node in the cluster (regardless of role) — pass the same peer set
to every node. It's used two ways:

- The node that starts as leader opens one persistent `ReplicationLink` per
  address and streams every shard's writes to it asynchronously (as in
  stage 6).
- Every node (leader or follower) starts a `FailoverMonitor` against that
  peer list: heartbeating peers if it's currently leader, or watching for a
  heartbeat timeout and calling an election if it's a follower. Leaving
  `peer_addrs` empty disables failover entirely — the node then behaves
  exactly as it did before stage 7 (a role fixed for its whole process
  lifetime, no extra background threads).

Example, three processes on one machine, all configured with the full peer
list:

```
./build/src/kvstore_server 6380 wal_a snapshot_a 4 leader   127.0.0.1:6381,127.0.0.1:6382
./build/src/kvstore_server 6381 wal_b snapshot_b 4 follower 127.0.0.1:6380,127.0.0.1:6382
./build/src/kvstore_server 6382 wal_c snapshot_c 4 follower 127.0.0.1:6380,127.0.0.1:6381
```

See `docs/architecture.md`'s "Replication" and "Failover" sections for the
wire protocol and catch-up/election mechanics, `docs/design-decisions.md`'s
"Consistency Model" for exactly what an acked write does and doesn't
guarantee, and `docs/limitations.md` for what this failover mechanism does
*not* protect against (it is heartbeat-based, not quorum-based).

## Running Tests

```
./scripts/test.sh
```

This builds the project and runs the full Catch2 suite via `ctest`:
storage correctness, protocol framing (partial reads, message boundaries,
malformed input), concurrency (concurrent readers/writers on `Store`, a
multi-client TCP stress test, and a safe-shutdown-under-load test), an
end-to-end integration test that drives a real `TcpServer` instance over a
loopback socket using the test-only C++ client in `tests/kv_client.{h,cpp}`,
sharding (`tests/sharding_test.cpp`: hash distribution over 1M+ keys,
`ShardedStore` routing/recovery/snapshotting, and per-shard crash-recovery
independence), and replication (`tests/replication_test.cpp`: write_id
dedup on replayed/duplicate records, leader/follower role enforcement, an
end-to-end leader-to-follower replication test over real TCP, snapshot-
transfer catch-up for a far-behind follower, and killing a follower
mid-stream and confirming it reconnects and catches up byte-identical to
the leader), and failover (`tests/failover_test.cpp`: `ClusterState`
epoch/role semantics, the new HEARTBEAT/ELECTION_QUERY opcodes and the
stale-term rejection path, a follower being promoted after the leader goes
silent, the more up-to-date of two followers winning an election, a stale
leader stepping down when it reappears, and killing a leader mid-write-
stream and confirming a follower is promoted with no acknowledged write
lost), and rebalancing (`tests/rebalance_test.cpp`: `Store::apply_migrated`'s
per-key newest-write-id-wins reconciliation, the idle/migrating/cutover/done
state machine, rehashing every key correctly across a shard-count change,
a rebalance running under concurrent write load with no acknowledged write
lost, the throttle measurably pacing the copy, a committed rebalance's
manifest taking effect on the next restart, and killing the process
mid-migration and confirming it rolls back to the source shard count with
no data lost).

### Running with ThreadSanitizer

```
cmake -S . -B build-tsan -DKVSTORE_ENABLE_TSAN=ON
cmake --build build-tsan -j
./build-tsan/tests/kvstore_tests
```

`KVSTORE_ENABLE_TSAN` adds `-fsanitize=thread` to the build. Use a separate
build directory (as above) — don't turn it on in the same `build/` used for
normal development, since it roughly doubles memory use and slows execution.

## Running Benchmarks

All benchmarks assume a Release build. The whole suite (everything below)
can be reproduced in one run:

```
./scripts/build.sh Release
./bench/run_all.sh
```

Individual benchmarks follow.

Throughput / latency over the real TCP wire protocol (p50/p95/p99), single
node and leader-plus-two-followers:

```
./scripts/build.sh Release
./bench/bench_throughput.sh 8 20000 5000   # single node:        clients, ops/client, keys/client
./bench/bench_multinode.sh  8 20000 5000   # 1 leader + 2 followers, same args
```

Restore time (snapshot load + WAL-tail replay on a fresh process):

```
./scripts/build.sh Release
./tools/bench_restore.sh 100000   # or any other key count
```

Concurrency: single global lock vs. sharded locking, PUT throughput across
writer thread counts:

```
./scripts/build.sh Release
./tools/bench_concurrency.sh 5000 1 2 4 8 16 32   # ops_per_thread, then thread counts
```

Rebalance overhead: foreground PUT throughput with vs. without a 4->8 shard
`rebalance_to()` running concurrently, at a few throttle settings, plus a
data-loss check:

```
./scripts/build.sh Release
./tools/bench_rebalance.sh 2000 4 0 50000 10000   # ops/thread, thread count, then rate_bytes_per_sec values (0 = unlimited)
```

See `docs/benchmarks.md` for the methodology and measured numbers for all
of these, plus the **resume truth map** (each claim → the exact benchmark
that backs it).
