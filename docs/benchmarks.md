# Benchmarks

## Machine

All numbers on this page were measured on:

- **Model:** Apple M1 Pro (MacBook Pro)
- **OS:** macOS 26.5.1 (Darwin 25.5.0, arm64)
- **Build:** `Release` (`./scripts/build.sh Release`), CMake 3.20+, AppleClang, C++17
- **Disk:** internal Apple SSD (APFS)

Every number on this page has its exact reproduction command in the
"Reproducing" block of its own section, and the whole page can be
regenerated in one run with `./bench/run_all.sh` (after a Release build).
The **resume truth map** at the bottom maps each claim you'd put on a resume
to the specific section here that backs it.

## Restore Time (stage 5: snapshot + WAL recovery)

### Methodology

**What's measured:** wall-clock time from "a fresh process just started" to
"snapshot loaded + WAL tail replayed, ready to accept traffic" — exactly the
`store.load_snapshot(snapshot_dir); store.recover_from_wal();` block in
`src/main.cpp`, timed with `std::chrono::steady_clock`. `tools/bench_restore.cpp`
runs this same two-call sequence directly against `Store`/`WriteAheadLog`
(no TCP, no protocol framing) since network/protocol overhead isn't part of
what stage 5 changes.

**Procedure** (see `tools/bench_restore.sh` for the exact orchestration):

1. `kvstore_bench_restore load <dir> <num_keys>` starts a fresh WAL + `Store`,
   `PUT`s `<num_keys>` sequentially-keyed entries (`key0`/`value0` ...
   `keyN-1`/`valueN-1`), calls `Store::take_snapshot()`, writes a `.ready`
   marker file, then blocks in `pause()`.
2. The driving script polls for `.ready`, then sends the loader process
   `SIGKILL` — simulating a crash immediately after a completed snapshot,
   the scenario stage 5's build order asks this number to cover.
3. `kvstore_bench_restore restore <dir>` starts a **new** process over the
   same directory and times the load-snapshot + WAL-replay sequence above.

**Dataset:** single-key-per-record workload, no concurrent clients (restore
happens before any client connects, by construction — the server isn't
listening yet during this window). Keys/values are small ASCII strings
(`keyN`/`valueN`, a few bytes to ~13 bytes) — deliberately not testing
value-size scaling here, since restore time in this design is dominated by
entry *count* (one deserialize + one map insert per entry), not per-entry
byte size at these sizes.

### Results

| Keys loaded | Snapshot entries restored | WAL records replayed | Restore wall-clock time |
|-------------|---------------------------|-----------------------|--------------------------|
| 100,000     | 100,000                   | 0                     | ~23–25 ms (5 runs: 23.5, 24.6, 24.4, 24.7, 23.0 ms) |
| 1,000,000   | 1,000,000                 | 0                     | 364.7 ms |

**Target was "comfortably under 2 seconds" — met with over 50x headroom at
100K keys and ~5.5x headroom at 1M keys.**

`WAL records replayed` is 0 in both runs because `take_snapshot()` covers
every write made before it and `force_rotate()` + `truncate_before()` (see
`docs/architecture.md`'s "Snapshot flow") delete the now-fully-covered WAL
segment as part of taking the snapshot — so a restart immediately
afterward, with no further writes, has no WAL tail left to replay at all.
This is expected, not a benchmark artifact: it's precisely what a snapshot
followed by a hard crash is supposed to produce.

### Why it's fast (no bottleneck to report)

Since the number came in comfortably under the 2-second target, there's no
tuning to justify — but concretely, restore time here is dominated by:

1. Reading the single `snapshot.bin` file into memory (one sequential
   read — 100K small entries is a few MB; 1M is tens of MB).
2. Deserializing it into `SnapshotEntry` structs and inserting each into
   `Store`'s `unordered_map` (one hash + insert per entry, no per-entry
   disk I/O — the whole file was already read into memory in step 1).
3. Scanning whatever WAL segments remain on disk (here: one small, empty
   active segment) for `recover_from_wal()` — negligible when the tail is
   empty.

None of these do a syscall per entry (unlike the WAL's per-write `fsync`,
which is the deliberate cost stage 4 accepted for write durability) — the
snapshot is read and parsed as one buffer, and the in-memory
deserialize-and-insert loop is what scales roughly linearly with entry
count (100K → 1M is a ~10x entry increase and a ~15x time increase, close
enough to linear that no single step is disproportionately expensive at
this scale).

### Reproducing

```
./scripts/build.sh Release
./tools/bench_restore.sh 100000     # or any other key count, e.g. 1000000
```

`bench_restore.sh` builds nothing itself (it expects `./scripts/build.sh` to
have already run) — it drives `build/tools/kvstore_bench_restore` through
the load → SIGKILL → restore sequence described above and prints the final
timing line.

## Concurrency: Single Global Lock vs Sharded Locking

### Methodology

**What's measured:** aggregate PUT throughput (ops/sec across all writer
threads together) for two configurations, at increasing writer thread
counts:

- **single-lock:** one `Store` + one `WriteAheadLog`, as used through the
  pre-sharding design — every PUT takes `Store`'s single `std::shared_mutex`
  in write mode and does its fsync while holding it, so every writer
  (regardless of which key it's writing) serializes behind every other one.
- **sharded:** one `ShardedStore` with 8 shards (each its own `Store` +
  `WriteAheadLog`) — a PUT only takes the lock and does the fsync for the
  one shard its key hashes to, so writers whose keys land in different
  shards no longer wait on each other at all.

Both configurations use a real, fsync-per-write `WriteAheadLog` (not an
in-memory-only `Store`) — the WAL's fsync-per-write policy (see "Fsync
Policy" in `docs/design-decisions.md`) is why write throughput was already
capped at roughly `1 / fsync_latency` for a single shard, and it's exactly
that ceiling this benchmark shows sharding relieves: N shards means N
independent locks *and* N independent WAL files/file descriptors, so up to N
writers' fsyncs can be in flight at once instead of one at a time.

**Workload:** each of T writer threads PUTs a fixed number of ops (a
`--ops-per-thread` argument) to distinct, thread-prefixed keys
(`t<thread>-k<i>`) — no two threads ever write the same key, so what's
measured is lock/WAL fan-out across shards, not per-key contention (already
covered by `storage_test.cpp`'s concurrency tests). `tools/bench_concurrency.cpp`
runs both configurations back to back for each thread count and reports
`threads, single_lock_ops_per_sec, sharded_8_ops_per_sec, speedup`.

### Results

Machine as above (Apple M1 Pro, internal SSD, `Release` build).
`./tools/bench_concurrency.sh 5000 1 2 4 8 16 32`:

| Threads | single-lock ops/sec | sharded (8 shards) ops/sec | speedup |
|---------|----------------------|----------------------------|---------|
| 1       | 36,919               | 43,388                     | 1.18x   |
| 2       | 40,596               | 57,941                     | 1.43x   |
| 4       | 32,286               | 58,250                     | 1.80x   |
| 8       | 25,385               | 36,061                     | 1.42x   |
| 16      | 19,573               | 58,747                     | 3.00x   |
| 32      | 14,026               | 66,953                     | 4.77x   |

**Run-to-run variance is real and visible at this scale** — this machine's
SSD fsync latency is fast enough (~0.03 ms observed single-threaded) that a
handful of milliseconds of OS scheduling noise measurably moves the numbers;
a second run at the same settings produced speedups in the same broad shape
(roughly flat-to-mild-gain at 1-4 threads, clearly widening from 8 threads
on) but not identical values. The trend, not any single column, is the
claim: **single-lock throughput degrades as thread count rises** (more
threads contending for one lock and serializing their fsyncs one at a time —
14K-25K ops/sec at 8+ threads, falling as threads increase), while
**sharded throughput holds or improves with more threads** (writers spread
across 8 independent locks/WALs keep finding parallel work up to and past
32 threads). At 1 thread there's no contention to relieve, so the two are
roughly at parity (sharding's only overhead there is the extra hash
computation and 8 open file descriptors instead of 1) — the win shows up
precisely where the design predicted it would: once concurrent writers exist
to serialize.

### Reproducing

```
./scripts/build.sh Release
./tools/bench_concurrency.sh 5000 1 2 4 8 16 32   # ops_per_thread, then thread counts
```

## Rebalance Overhead (stage 9: online resharding)

### Methodology

**What's measured:** for each `rate_bytes_per_sec` throttle setting, two
independent `ShardedStore` instances (both preloaded with the same 3,000
keys), run side by side:

- **during:** 4 writer threads each PUT 5,000 distinct keys while a 4->8
  shard `ShardedStore::rebalance_to(8, rate)` runs concurrently on its own
  thread. `tools/bench_rebalance.cpp` times `rebalance_to()` itself
  (`migration_ms`) and the writer threads' own elapsed time, from which it
  derives `during_rebalance_ops_per_sec`.
- **baseline:** the identical preload and writer workload, no rebalance
  running, giving `baseline_ops_per_sec` for the same op count.

Every run also verifies, after the migration completes, that every one of
the 3,000 preloaded keys and all 20,000 written keys are present with the
correct value and that `store.size()` matches exactly — the data-loss
check `tools/bench_rebalance.cpp` fails on (nonzero exit) if it ever finds
otherwise.

Writer threads do a **fixed** op count rather than running for a duration —
see the comment at the top of `tools/bench_rebalance.cpp` for why: an
earlier version used unbounded writer threads against a low throttle rate
and discovered, empirically, that a migration's copy loop can fall
permanently behind when live writes land faster than
`rate_bytes_per_sec` lets the copy drain them (the same "dirty rate
exceeds migration bandwidth" non-convergence problem VM live migration has
to guard against — see `docs/limitations.md`). Bounding each writer
guarantees this benchmark always terminates.

### Results

Machine as above (Apple M1 Pro, internal SSD, `Release` build).
`./tools/bench_rebalance.sh 5000 4 0 50000 10000` (5 separate runs):

**`migration_ms` — reliable and reproducible across every run:**

| rate_bytes_per_sec | migration_ms (5 runs) |
|---------------------|------------------------|
| 0 (unlimited)        | 10, 10, 10, 10, 11 |
| 50,000                | 4059, 4134, 4158, 4220, 4001 |
| 10,000                | 22458, 22558, 22656, 22685, 22784 |

The throttle does exactly what it's designed to do: migration wall-clock
time scales with the configured byte rate (roughly 2x the naive
byte-volume-over-rate estimate at 10,000 B/s once the concurrent writers'
own traffic is folded into what the copy loop has to move — expected,
since the copy loop is also draining whatever the writers add to
not-yet-copied shards while it runs).

**Data loss: zero, across every one of these runs and every throttle
setting, including the two rebalance-under-concurrent-load tests in
`tests/rebalance_test.cpp` (one unthrottled, one throttled) and 60+
additional manual stress runs during development.** This is the headline
correctness claim this benchmark exists to support, and it held in every
run — see `docs/design-decisions.md`'s "Online Resharding" section for a
real data-loss bug this exact benchmark found and the fix that closed it
(`forward_if_migrating()` re-checking `dual_write` after a write lands,
not before).

**Foreground throughput dip (`baseline_ops_per_sec` vs.
`during_rebalance_ops_per_sec`): too noisy at this benchmark's scale to
report a single trustworthy percentage, stated plainly rather than
papered over with a cherry-picked run.** Across 5 runs per rate,
`dip_pct` ranged from roughly -50% to +55% with no consistent sign or
trend — `baseline_ops_per_sec` alone varies by more than 2x run-to-run
for the *identical* configuration (e.g. 15,320 to 58,641 ops/sec at
20,000 total ops), which points at OS-level scheduling/fsync-latency
variance dominating the signal at this op count, the same class of
run-to-run variance this page's "Concurrency" section above already
documents as real and visible at small scale on this machine. A
trustworthy dip percentage would need either many more repetitions
averaged together or a much larger, longer-running workload to amortize
that noise — not built here, since `migration_ms` and the zero-data-loss
result are this stage's actual resume claims; "a rebalance measurably
slows the foreground down while `dual_write` is active" is asserted
qualitatively (every write pays two fsyncs instead of one during that
window — see docs/design-decisions.md) but the *exact* percentage is not
claimed as a solid number here.

### Reproducing

```
./scripts/build.sh Release
./tools/bench_rebalance.sh 5000 4 0 50000 10000   # ops/thread, thread count, then rate_bytes_per_sec values (0 = unlimited)
```

## Throughput / Latency (stage 8)

### Methodology

**What's measured:** end-to-end client-observed throughput and per-operation
latency (p50/p95/p99), over the **real TCP wire protocol** against a running
`kvstore_server` process — not in-process against `Store`/`ShardedStore`
like the concurrency/restore/rebalance benchmarks above. This number
deliberately includes everything a real client pays: TCP round-trips,
length-prefixed frame encode/decode on both ends, the thread-per-connection
accept/dispatch path, key hashing to a shard, and (for writes) the shard
WAL's fsync-per-write. `tools/bench_throughput.cpp` is the load generator;
it reuses the same wire client the tests use (`tests/kv_client.*`), so it
speaks exactly the protocol documented in `docs/architecture.md`.

**Topology.** Two configurations, both with **8 shards**:

- **single-node:** one leader, no peers configured (so no replication links
  and no `FailoverMonitor` threads) — `bench/bench_throughput.sh`.
- **multi-node:** one leader plus **two followers**, the leader streaming
  every write to both over `ReplicationLink`s — `bench/bench_multinode.sh`.
  The followers are started with an empty peer list on purpose (fixed
  follower role, no `FailoverMonitor`) so they're pure replication targets
  and no spurious election can fire if a busy leader is briefly slow to
  heartbeat; this isolates *replication* cost from *failover* noise. See the
  script header and `docs/architecture.md`'s "Replication"/"Failover"
  sections.

**Clients / dataset.** 8 concurrent client connections, each its own thread
and TCP socket. Each client owns 5,000 distinct keys (`c<id>-k<i>`), all
PUT in an untimed preload phase so GETs hit live keys. The measured phase is
then 20,000 operations per client — **160,000 operations total** — each
operation picking a key at random from that client's range and an operation
type from the workload mix below. Values are a fixed 1-byte payload: this
benchmark measures request-rate/latency of the protocol+store+WAL path, not
value-size scaling (the WAL/snapshot sections above already speak to byte
volume). Latency is measured per operation with `steady_clock` around the
single blocking request/response call; throughput is total ops over the
measured phase's wall-clock.

**Workload mixes** (per operation, drawn from a per-client PRNG):

| Workload | GET | PUT | DELETE |
|----------|-----|-----|--------|
| read-heavy  | 90% | 10% | — |
| write-heavy | 10% | 90% | — |
| mixed       | 45% | 45% | 10% |

(`mixed`'s DELETEs against a client's own preloaded range naturally produce
some later NOT_FOUND GETs — still a served round-trip either way, which is
what's being timed.)

### Results — single node

Machine as above (Apple M1 Pro, internal SSD, `Release` build). 8 clients,
20,000 ops/client (160,000 total), 5,000 keys/client, 8 shards. Values below
are representative of 3 back-to-back runs; unlike the small-scale sections
above, run-to-run variance at this op count is small (throughput within a
few percent, percentiles within tens of microseconds).

| Workload | Throughput (ops/sec) | p50 | p95 | p99 |
|----------|----------------------|-----|-----|-----|
| read-heavy  | ~91,000–92,000 | ~78 µs  | ~135 µs | ~170–180 µs |
| write-heavy | ~50,000–52,000 | ~140 µs | ~270–283 µs | ~360–392 µs |
| mixed       | ~65,000–68,000 | ~115–118 µs | ~213–227 µs | ~286–306 µs |

Reads run at roughly **1.8x** the throughput of writes with about half the
p50 latency — the direct, expected consequence of the WAL's fsync-per-write
policy (`docs/design-decisions.md`'s "Fsync Policy"): a GET touches only the
in-memory map under a shared lock, while every PUT/DELETE does a durable
fsync before the server acks. `mixed` lands between the two, tracking its
55% write share.

### Results — multi node (1 leader + 2 followers)

Same client workload, run against the leader while it replicates every write
to two followers.

| Workload | Throughput (ops/sec) | p50 | p95 | p99 |
|----------|----------------------|-----|-----|-----|
| read-heavy  | ~68,000–71,000 | ~100 µs | ~175–192 µs | ~240–380 µs |
| write-heavy | ~35,000–41,000 | ~170–192 µs | ~335–410 µs | ~485–708 µs |
| mixed       | ~50,000–54,000 | ~139–145 µs | ~260–289 µs | ~351–430 µs |

**Replication's foreground cost is a ~20–25% throughput drop across all
three mixes**, with a longer p99 tail (most pronounced on write-heavy, where
one run's p99 reached ~708 µs). This is expected and honest: the leader is
running two background `ReplicationLink` threads that poll each shard's WAL
every ~20 ms and stream new records over TCP, competing for CPU and I/O with
the foreground request threads. Note the cost shows up even on **read-heavy**
(~91K → ~69K) — reads themselves aren't replicated, but the background
replication threads still contend for scheduling on the same machine. The
client-observed *write* path is unchanged in shape (still acked the instant
the leader's own WAL fsync returns, before any follower sees the record —
`docs/design-decisions.md`'s "Consistency Model"); the drop is contention,
not added synchronous work per write.

Because acks are leader-only, this benchmark does **not** show reads being
served from followers (they aren't — a follower rejects client reads with
`NOT_LEADER`, see `docs/limitations.md`), nor does it show read *scaling* by
adding nodes. "Multi-node throughput" here means "throughput of a leader
under replication load," which is the honest thing this topology can
measure.

### Reproducing

```
./scripts/build.sh Release
./bench/bench_throughput.sh 8 20000 5000   # single node:  clients, ops/client, keys/client
./bench/bench_multinode.sh  8 20000 5000   # leader + 2 followers, same args
```

Each script starts the server process(es), runs all three workload mixes
against them, prints one CSV row per workload, and tears the processes down
on exit. `./bench/run_all.sh` runs both of these plus every other benchmark
on this page in sequence.

## Resume Truth Map

Every claim below is backed by a specific section of *this file* and the code
that section measures. If a bullet isn't here, there's no benchmark for it —
don't put it on the resume.

| Resume claim | Backed by (this file) | Reproduce | Honest caveat to volunteer |
|--------------|-----------------------|-----------|-----------------------------|
| "Custom binary TCP protocol + thread-per-connection server sustaining ~90K read / ~50K write ops/sec single-node with sub-200µs p99 reads" | **Throughput / Latency → single node** | `./bench/bench_throughput.sh 8 20000 5000` | One machine, loopback TCP, 8 clients, 1-byte values — LAN RTT and larger values would lower it. |
| "p50/p95/p99 latency characterized per workload (read-heavy / write-heavy / mixed)" | **Throughput / Latency** tables | same as above | Nearest-rank percentiles over 160K ops; reads are map-only, writes fsync. |
| "Replication costs ~20–25% foreground throughput, measured not assumed" | **Throughput / Latency → multi node** | `./bench/bench_multinode.sh 8 20000 5000` | Cost is CPU/IO contention from background streaming, not synchronous replication (acks are leader-only, async). |
| "Sharded per-shard locking removes the single-lock write bottleneck; up to ~4.8x write throughput at 32 threads" | **Concurrency: Single Global Lock vs Sharded Locking** | `./tools/bench_concurrency.sh 5000 1 2 4 8 16 32` | In-process (no TCP); win is real but variance is high at small scale — the *trend* is the claim, not any single column. |
| "WAL + snapshot crash recovery restores 1M keys in <0.4s (100K in ~25ms)" | **Restore Time** | `./tools/bench_restore.sh 1000000` | Measures snapshot-load + WAL-tail replay only; entry-count-bound, not value-size-bound. |
| "Online resharding (N→M shards) under live write load with zero acknowledged-write loss" | **Rebalance Overhead** | `./tools/bench_rebalance.sh 5000 4 0 50000 10000` | In-process resharding, not cross-node migration; `migration_ms` is solid, the throughput-dip % is too noisy to claim. |
| "Bandwidth-throttled migration (measured, tunable)" | **Rebalance Overhead** (`migration_ms` table) | same as above | Static rate per call, not adaptive; can non-converge if write rate exceeds throttle (documented in `docs/limitations.md`). |

Claims this project does **not** support with a benchmark, and must not
appear on the resume: any cross-node/multi-shard *placement* throughput,
read-scaling off followers, failover time-to-recovery as a tuned number
(failover correctness is tested in `tests/failover_test.cpp` but not
benchmarked for latency), and anything about value-size or connection-count
(C10K) scaling. See `docs/limitations.md` for why each is out of scope.
