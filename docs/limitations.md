# Limitations

An honest list of what this system does NOT do, as of stage 9 (core store +
wire protocol + concurrency + WAL durability + snapshotting + hash-based
sharding + per-shard leader/follower replication + heartbeat-based
failover + online in-process resharding, still no cross-node shard
placement or automatic rebalance triggering).

- **Thread-per-connection does not scale to very high connection counts.**
  Each connection gets its own OS thread; there is no event loop and no
  bounded worker pool. The throughput benchmark (`docs/benchmarks.md`) drives
  it at 8 concurrent connections, where it's healthy; this design would not
  hold up under a C10K-style workload of thousands of concurrent idle
  connections, and that regime is deliberately not measured. Not a goal for
  this project — see `docs/design-decisions.md`.
- **The throughput/latency numbers are single-machine, loopback-TCP,
  small-value.** `docs/benchmarks.md`'s stage-8 numbers were taken with
  client and server on one host over the loopback interface with 1-byte
  values, so they exclude real network RTT, NIC/kernel network-stack cost
  under load, and any value-size effect — they characterize the
  protocol+store+WAL path, not deployment on a real network with realistic
  payloads. The multi-node ("leader + 2 followers") number likewise measures
  replication's foreground *contention* cost on one machine, not
  cross-machine replication latency, and does **not** measure read scaling
  (followers reject client reads — see below). Failover *correctness* is
  tested (`tests/failover_test.cpp`) but failover *time-to-recovery* is not
  benchmarked as a tuned number.
- **Nothing triggers a snapshot automatically.** `Store::take_snapshot()`
  exists and is exercised by `tests/snapshot_test.cpp` and
  `tools/bench_restore.cpp`, and the real server (`main.cpp`) *loads* the
  latest snapshot at startup — but nothing ever calls `take_snapshot()` on
  a running `kvstore_server`. There's no periodic background snapshot
  thread, no signal handler (e.g. `SIGUSR1`), and no wire-protocol command
  to request one. A long-lived server that's never manually snapshotted
  (there's currently no supported way to do that against a live process)
  gets no benefit from stage 5 and falls back to stage 4's behavior:
  replaying the entire WAL on restart. Wiring up a live trigger is
  deferred until a concrete need (e.g. a stage-8 benchmark showing WAL
  growth is a real problem for a long-running instance) justifies picking
  one of those mechanisms over the others.
- **Only one snapshot is ever kept, and it isn't retried if writing it
  fails.** `write_snapshot()` throws on any filesystem error (failed
  `open`/`write`/`fsync`/`rename`), and nothing calling `take_snapshot()`
  today catches that — same "unhandled exception on this thread kills the
  process" posture the WAL already has (see below). There's also no
  snapshot history: a corrupted or lost `snapshot.bin` has no older
  snapshot to fall back to, only whatever WAL segments happen to still be
  on disk (which, if that snapshot's `truncate_before()` already ran, may
  not cover the full history anymore).
- **WAL recovery assumes corruption can only occur at the tail of the
  currently-active segment** (i.e. only as a result of a crash mid-write to
  the segment that was open at the time). A checksum or shape mismatch
  found in any earlier, already-rotated segment is treated as a hard
  failure (`recover()` throws) rather than something recovery silently
  works around — this store does not defend against arbitrary on-disk
  corruption/bit rot in already-fsynced, previously-rotated segments (e.g.
  from failing storage hardware). That's a different problem
  (media integrity) from the one this WAL is built to solve (crash
  consistency), and is out of scope here.
- **WAL I/O failures (disk full, a failed `fsync`, permission errors) are
  not gracefully handled.** `WriteAheadLog::append_put`/`append_delete`
  throw `std::runtime_error` on any write/fsync failure, and nothing in
  `network/` currently catches exceptions thrown from `dispatch()` — an
  unhandled exception on a detached connection thread calls
  `std::terminate()` and kills the whole process. A production system would
  need a defined behavior here (reject the write with an error response,
  or shed the connection cleanly); modeling that is out of scope for this
  stage's milestone (getting durability itself correct) and isn't yet
  exercised by any test, since no test drives the store into ENOSPC or a
  permissions failure.
- **Failover is heartbeat-based with an epoch bump, not quorum-based — it
  can double-promote under a network partition.** This is deliberate scope
  for this project ("heartbeat-based detection and epoch bumping is enough,
  do not implement a general consensus protocol") and is stated here plainly,
  not glossed over: a candidate promotes itself if it has the best total
  committed write_id among whichever peers it can actually reach within a
  short RPC timeout — not a majority of the whole cluster. If a partition
  splits the cluster into two groups that can't see each other, each group
  can independently elect its own leader at the same moment, producing two
  simultaneous leaders until the partition heals and one of them observes
  the other's higher epoch (see `ClusterState::accept_epoch()` in
  `docs/design-decisions.md`'s "Failover" section). Real quorum-based
  election (only promote if a majority of the *known* peer set responds
  and agrees) would close this gap; it is the natural next step if this
  project's scope ever grows to need it, not something built here.
- **A leader that is genuinely partitioned (not crashed) keeps accepting
  client writes until it happens to exchange a message with a node that
  already knows about a newer epoch.** The split-brain guard
  (`ClusterState::accept_epoch()` demoting a node the instant it observes a
  strictly higher epoch) only fires when that exchange actually happens —
  a HEARTBEAT reaching the old leader directly, or a REPLICATE/HEARTBEAT it
  sends bouncing back `STALE_TERM`. While cut off from every node that has
  moved on, the old leader has no way to learn it's been superseded, so it
  keeps serving writes exactly as it did before the partition, durable on
  its own WAL (stage 4's guarantee is unaffected) but with no way to reach
  the cluster's new leader or its replicas. Those writes can permanently
  diverge from what the rest of the cluster accepts as canonical — there is
  no reconciliation mechanism (no rollback, no merge, no "which side wins"
  policy) once the partition heals and the old leader finally steps down.
  This is the concrete, worst-case cost of not requiring a quorum to keep
  writing, not just of the election itself.
- **Promotion picks "most up to date" using one scalar (summed committed
  write_id across every shard), not a per-shard comparison.** A follower
  that is ahead on one heavily-written shard but behind on another could
  outscore a follower that is slightly behind on every shard evenly — the
  sum can hide unevenness a per-shard vector comparison would catch. This
  project's replication model ships every shard to every follower as one
  unit over a single connection, so in every scenario this project's own
  tests exercise, a follower's per-shard lag tracks closely enough that this
  doesn't change the outcome — but it is a real simplification, not a
  proven-equivalent one, and would need revisiting if shards were ever
  placed/replicated independently of each other (see the "Sharding" bullets
  below).
- **An unreachable-but-genuinely-most-caught-up peer can neither win nor
  block an election.** Election only considers peers that actually respond
  within the RPC timeout; a peer that's simply slow to answer (not dead) is
  silently excluded from that round's comparison, and a *less* caught-up
  but reachable peer can be promoted instead. There is no retry/backoff
  before deciding, and no way for the excluded peer to contest the outcome
  after the fact.
- **Cluster membership for failover is static, exactly like every other
  membership decision in this project.** The peer list a node's
  `FailoverMonitor` watches is fixed at process startup (`main.cpp`'s
  `peer_addrs` argument) — there is no way to add or remove a node from a
  running cluster, and a genuinely new node cannot join an existing one and
  be discovered by the others. Consistent with `ShardMap::owning_node()`'s
  existing "no dynamic membership" stance (see the "Sharding" bullets
  below) and this project's YAGNI guidance — not built until a concrete need
  for it exists.
- **Follower reads are disallowed entirely, not served stale.** A follower
  node rejects PUT/GET/DELETE from ordinary clients with `NOT_LEADER` —
  there is no way to read (even accepting staleness) from a follower today.
  This was the simpler of the two options this stage's own requirements
  offered, and was chosen deliberately (see docs/design-decisions.md's
  "Consistency Model") — read scaling off followers is a future stretch
  goal, not built here.
- **Replication is asynchronous with no durability guarantee beyond the
  leader.** A client's write is acked the instant it's durable on the
  *leader's* WAL; it does not wait for any follower. If the leader's disk
  is lost before a follower has caught up (bounded in the steady state by
  one ~20ms poll interval plus network latency, but unbounded in principle
  if a follower has been disconnected), an acked write can be lost. See
  docs/design-decisions.md's "Consistency Model" for the full tradeoff and
  why synchronous/quorum acking was rejected for this stage.
- **Delivery is at-least-once, not exactly-once.** A leader restart or a
  replication connection drop can cause the same record to be resent to a
  follower; this is made safe (not exactly-once) by write_id dedup in
  `Store::apply_replicated()`, not by avoiding retransmission. See
  docs/design-decisions.md's "Replication Ack Policy".
- **`ReplicationLink::replicate_from()`'s WAL polling doesn't coordinate
  with a concurrent `take_snapshot()`/`truncate_before()` on the same
  leader shard.** In principle, a snapshot rotating/truncating WAL segments
  at the exact moment a replication poll is scanning them could race. In
  practice this can't happen today: nothing calls `take_snapshot()`
  automatically on a running server (see the snapshot-triggering bullet
  above), so a live leader's WAL segments are never mutated while serving
  replication traffic. Would need a lock if a live snapshot trigger is
  added later.
- **No cascading replication (no follower-of-a-follower).** `main.cpp`'s
  CLI only lets a `role=leader` node take a follower-address list; a
  `role=follower` node cannot itself replicate onward to a third node.
  Every follower replicates directly off the one leader.
- **Replication is per-shard-independent but not per-shard-placed.** Every
  shard of a given `ShardedStore` replicates to the exact same follower
  node(s) — there's no way to send different shards to different
  followers, since that would need real per-shard cluster placement
  (`ShardMap::owning_node()` still always returns `"local"`; see the
  "Sharding" bullets below). One `ReplicationLink` already multiplexes
  every shard over a single connection, so nothing here blocks a future
  phase from routing individual shards to different followers — it just
  isn't wired up, since this phase's whole store is still one process's
  worth of shards replicating as a unit.
- **No command set beyond PUT/GET/DELETE.** No CAS, SETNX, TTL/expiry, scan,
  or multi-key operations. Not planned unless a later stage's milestone
  actually requires one.
- **Deleted keys retain memory, and now retain disk space in every snapshot
  too.** DELETE marks a tombstone rather than erasing the map entry (needed
  for WAL/replication to represent a delete as a fact), and stage 5's
  `take_snapshot()` deliberately re-serializes tombstones rather than
  dropping them (see docs/design-decisions.md — dropping one would reset
  that key's version counter on its next write). There is still no
  tombstone garbage collection; a workload that writes and deletes many
  distinct keys will grow the in-memory map, and every snapshot taken
  after that, without bound. A GC policy (e.g. a snapshot dropping
  tombstones once they're older than the oldest data a follower could still
  need — meaningful once replication exists) is deferred, not built now.
- **No authentication, authorization, or transport encryption.** The
  protocol is a plaintext binary format over an unauthenticated TCP socket.
  Not a goal for this project.
- **No protocol versioning/negotiation.** There is exactly one wire format;
  client and server must agree on it out of band (i.e. by both being built
  from this codebase).
- **Sharding is static across nodes, and every shard's leader is still one
  process.** `ShardedStore` hashes keys across N `Store`+`WriteAheadLog`
  instances (`shard_id = hash(key) % num_shards`), all *led* by one process
  — stage 6 adds follower nodes that replicate that same process's shards
  (see docs/architecture.md's "Replication"), and stage 7's failover
  promotes one of those followers to lead the *entire* shard set as a unit
  when the leader dies — but there is still no cluster membership or
  placement mechanism that could put different shards' leadership on
  different nodes, and no way to migrate a single shard's data to a
  different node independently of the others; `ShardMap::owning_node()`
  always returns the same value regardless. Stage 9 changes `num_shards`
  itself online (see "Rebalancing" below) but does not touch any of this —
  it reshards *within* one process, not *across* nodes. Concretely still
  missing, all deliberately deferred until a later phase actually needs
  them:
  - **No cross-node request routing.** A client must already know which
    node is the leader for the shard it wants to write, and connect there
    directly — a follower rejects client writes (`NOT_LEADER`) rather than
    forwarding them to its leader. `ShardMap::owning_node()` still always
    returns `"local"`; there is still no cluster membership or discovery
    protocol telling a client (or another node) where a given shard's
    leader actually is.
  - **No per-shard load awareness.** Shard assignment is a pure hash of the
    key; there's no monitoring or rebalancing based on which shards are
    actually hot, so a workload with a skewed key distribution (many
    requests to keys that happen to hash to the same shard) doesn't get any
    better load spreading than the hash provides by chance. `rebalance_to()`
    (stage 9) changes the shard *count*, uniformly rehashing every key — it
    has no concept of "this shard is hot, move it" and no automatic trigger
    of any kind (see below).
  - **Per-key contention is unaffected by sharding or rebalancing.**
    Sharding parallelizes *across* keys that land on different shards; many
    concurrent writers to the *same* key still serialize on that one
    shard's lock exactly as they did on the single global lock before this
    phase (see `storage_test.cpp`'s per-key concurrency tests, unchanged by
    this phase).
  - **The default shard count (8) is not benchmark-tuned.** It was chosen
    to be large enough to demonstrate and measure the concurrency win in
    `docs/benchmarks.md`, not derived from a target throughput or profiled
    against this project's actual (currently unmeasured) production-scale
    workload.
- **Rebalancing (`ShardedStore::rebalance_to()`, stage 9) is in-process
  resharding, not cross-node shard migration or automatic rebalancing.**
  It changes a single process's shard count online (throttled copy, live
  writes forwarded during the migration, one atomic routing cutover — see
  docs/architecture.md's "Rebalancing" and docs/design-decisions.md's
  "Online Resharding") — this closes the "changing `num_shards` silently
  misroutes every key" gap the bullet above used to describe, but it is
  deliberately narrower than a production rebalancer in several concrete
  ways:
  - **Nothing decides *when* or *to what* to rebalance.** There is no
    monitoring, no hot-shard detection, and no automatic trigger of any
    kind — `rebalance_to(new_num_shards, rate)` is a primitive called
    directly by a test or operator (or, today, the benchmark tool), the
    same "real primitive, no live wire trigger yet" posture
    `Store::take_snapshot()` has had since stage 5. Wiring a
    `kvstore_server` command or admin endpoint to call it is future work,
    not built here.
  - **Two fsyncs per write while a migration is in flight, and a narrow
    read-staleness window right at cutover.** Every PUT/DELETE durably
    appends to the source shard's WAL *and* (in-memory only, via
    `Store::apply_migrated()`) updates the target shard's map for as long
    as the copy is running — measured concretely in
    `docs/benchmarks.md`'s rebalance-overhead numbers. Separately, because
    the cutover's exclusivity is scoped to PUT/DELETE only (not GET), a GET
    landing in the microseconds-wide window right at the routing swap could
    in principle be served by whichever plan its own read happened to see
    — no acknowledged write is ever lost by this (every write is durable on
    source or target's WAL exactly as designed either way), but it is a
    real, if vanishingly narrow, staleness edge case, not a hard
    linearizability guarantee.
  - **An interrupted rebalance rolls all the way back, it doesn't resume.**
    A crash mid-migration never reaches the manifest commit, so the source
    generation stays authoritative and the entire partially-copied target
    generation is discarded on the next restart (see
    `tests/rebalance_test.cpp`'s crash-mid-migration test) — re-running
    `rebalance_to()` starts the copy over from scratch rather than picking
    up where the last attempt left off. Fine at this project's tested data
    sizes; would be worth revisiting if this were ever used against a
    dataset large enough that re-copying everything after every interrupted
    attempt became the actual bottleneck.
  - **The throttle is a fixed rate for one call, not adaptive.**
    `rate_bytes_per_sec` is chosen once by the caller for the whole
    migration; there's no feedback loop that backs off automatically under
    real foreground load or speeds up when the system is idle.
  - **Still no cross-node shard placement.** Every shard, before and after
    a `rebalance_to()` call, is still led by the one process that ran it —
    this is the same limitation the bullet above already states; called
    out again here because it's the most likely follow-up question a
    "rebalancing" resume claim invites.
- **`version`/`write_id` are exposed over the wire only for replication, not
  to ordinary clients.** Stage 6's REPLICATE opcode carries both (see
  `docs/architecture.md`'s "Replication" section) so a follower can apply a
  record exactly as the leader recorded it, but no client-facing
  PUT/GET/DELETE request or response reads or returns either field — a
  regular client still has no way to observe a key's version or the
  write_id that produced its current value.
