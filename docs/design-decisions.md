# Design Decisions

For each major choice: what was chosen, what the alternatives were, and why.

## Key/Value Size Limits & Encoding

**Chosen:** `MAX_KEY_SIZE = 1024` bytes, `MAX_VALUE_SIZE = 1048576` bytes
(1 MiB). Keys and values are opaque binary byte strings — length-delimited,
not NUL-terminated, no required charset (UTF-8 is not enforced or assumed).
Empty values are allowed; empty keys are rejected (`PutResult::kEmptyKey`).

**Alternatives considered:**
- No limits at all — rejected: an unbounded key/value length means an
  unbounded length prefix, which means the framing layer has to allocate
  based on attacker/bug-controlled input before it can even validate the
  request. A cap lets the parser reject a bad length *before* allocating.
- NUL-terminated C-string keys/values — rejected: keys/values are meant to be
  arbitrary binary data (this is a byte-store, not a text-store), and
  NUL-termination would silently truncate any value containing a `\0` byte.

**Why these specific numbers:** 1 KiB keys and 1 MiB values are generous for
a resume-project benchmark workload (large enough to not be a realistic
bottleneck, small enough that `MAX_FRAME_LEN` stays small and a single
oversized-length attack can't force a huge allocation). They're arbitrary but
documented and enforced consistently in both `Store::put` and the protocol
parser's frame-length bound.

## Entry Struct: `version`, `write_id`, `tombstone`

**Chosen:** every stored key maps to an `Entry { value, version, write_id,
tombstone }`.
- `version`: per-key mutation counter, starts at 1 on first write, increments
  on every PUT or DELETE to that key.
- `write_id`: store-wide monotonic sequence number, incremented on every
  mutation across all keys.
- `tombstone`: DELETE sets this to `true` instead of erasing the map entry.

**Why now, even though nothing on the wire uses these fields yet:** these
fields exist specifically because stage 4 (WAL) needs a monotonic mutation
order to replay, and stage 6 (replication) needs a way to represent "this key
was deleted" as a fact that can be shipped to a follower, not just an absence
of a row. Building `Entry` with these fields now avoids a breaking change to
the storage layer's core data shape later. This is the one piece of "future"
engineering in Phase 1 that was deliberately kept — justified extra
engineering, not speculative generality.

**Tombstone vs. hard-erase on DELETE:** chosen to mark-and-keep rather than
erase, because a replication log entry saying "key X was deleted" only makes
sense if the leader can represent that state and hand it to a follower — hard
erasure loses the fact that a delete happened, indistinguishable from "never
written." Cost: deleted keys still occupy a map slot and memory. This is
called out as a known Phase-1 limitation (`docs/limitations.md`); a real
tombstone-GC policy is out of scope until snapshotting exists, since a
snapshot naturally gives a point to drop old tombstones.

**Not yet exposed:** `version`/`write_id` are populated but neither is on the
wire protocol yet (no opcode reads them, no response carries them). Adding
that now would be speculative — nothing in this phase or the client needs to
observe them externally. `Store::peek()` exists only to let tests assert on
them directly.

## Wire Protocol: Length-Prefixed Binary Framing

**Chosen:** `[4-byte length][1-byte opcode/status][8-byte request_id][payload]`,
all integers big-endian. Full spec in `docs/architecture.md`.

**Alternatives considered:**
- **Delimiter-based framing** (e.g. newline- or NUL-terminated messages) —
  rejected: keys/values are arbitrary binary data, so no byte value can be
  safely reserved as a delimiter without an escaping scheme, which adds
  complexity for no benefit over a length prefix.
- **Fixed-size messages** — rejected: keys and values vary in size by
  design; padding every message to `MAX_KEY_SIZE + MAX_VALUE_SIZE` would
  waste bandwidth on every small PUT/GET.
- **Text protocol (e.g. RESP-like, "PUT key value\r\n")** — rejected: adds
  parsing overhead (tokenizing, escaping binary data as text) for a project
  whose whole point is a from-scratch binary protocol and framing exercise;
  also complicates binary-safe keys/values.
- **Little-endian integers** — rejected in favor of big-endian (network byte
  order) purely as the conventional choice for wire protocols; either would
  have worked equally well since both client and server are code we control.

**Why a fixed 8-byte `request_id` in every frame, even though the server is
currently single-threaded and strictly request-then-response per
connection:** once concurrency (stage 3) allows a client to pipeline
multiple in-flight requests on one connection, or a server to interleave
responses across connections, something has to let the client match a
response to the request that produced it. Putting `request_id` in the frame
now means the wire format doesn't change shape when that lands — only
dispatch/concurrency logic changes. This is header plumbing, not unused
logic: `TcpServer` already echoes it on every response today, and
`kv_client`/tests already assert on it.

**`MAX_FRAME_LEN` bound:** the parser computes the largest legal `length`
field value as `9 (fixed header) + MAX_KEY_SIZE + MAX_VALUE_SIZE + 4 (inner
PUT key_len prefix)` and rejects anything larger *before* reading a payload
into memory. This defends against a corrupt or adversarial length prefix
forcing an oversized allocation — the cost is negligible (one integer
comparison) and the failure mode it prevents (unbounded allocation from an
untrusted 32-bit length field) is concrete enough to be worth the check.

## Concurrency: Threading Model

**Chosen:** thread-per-connection. `TcpServer::run()` blocks in `accept()` on
the calling thread; each accepted connection is handed to its own
`std::thread` that runs the existing blocking `serve_connection()` loop
(read → parse → dispatch → write) to completion, then exits. `dispatch()`
and `Store` are the only state shared across connection threads.

**Alternatives considered:**
- **Event loop (epoll/kqueue, single or few reactor threads)** — rejected
  for *this stage*: an event loop pays off when you need one thread to
  cheaply hold open thousands of mostly-idle connections (the C10K
  problem). This is a single-shard resume project benchmarked with a
  bounded, modest number of concurrent clients — the actual workload this
  needs to serve doesn't have thousands of concurrent connections. An event
  loop also means giving up the already-written, easy-to-read blocking
  `serve_connection()` loop for callback/state-machine-based I/O, which is
  real complexity with no corresponding benefit at this scale.
- **Thread pool with a bounded worker count** — rejected as premature: it
  solves a problem (unbounded thread creation under a huge number of
  concurrent connections) this project doesn't have yet. One OS thread per
  connection is cheap enough (megabytes of stack, microseconds to spawn)
  for the connection counts this store is ever benchmarked against. If a
  benchmark run in stage 8 shows thread creation/teardown or thread count
  itself is the bottleneck, a bounded pool is the natural next step — not
  worth building until a benchmark proves the need.
- **Staying single-threaded** — not an option: stage 3 explicitly requires
  real concurrent client handling.

**Why this is a defensible choice to state in an interview:** thread-per-
connection is the simplest model that satisfies "handle multiple clients at
once," reuses 100% of the existing blocking I/O code from stage 2 unchanged,
and its main known weakness (doesn't scale to very high connection counts)
is explicitly a non-goal here — see `docs/limitations.md`.

## Locking Strategy

**Chosen:** a single `std::shared_mutex` inside `Store`, guarding the whole
`unordered_map` and the `next_write_id_` counter. `put()`/`del()` take a
unique (write) lock; `get()`/`peek()`/`size()` take a shared (read) lock.

**Alternatives considered:**
- **Per-key locking (e.g. a `std::mutex` per `Entry`, or striped/sharded
  locks over key hash buckets)** — rejected for now: this is a single-shard
  store with one map; per-shard/per-key locking is exactly the kind of
  premature complexity this project's YAGNI stance calls out ("do not
  pre-build per-shard locking yet, there's only one shard"). It would also
  be actively wrong
  today: `map_[key]` on an `unordered_map` can trigger a rehash, which
  invalidates *all* iterators/references across *every* key, not just the
  one being written — so any correct fine-grained scheme still needs to
  briefly take a whole-map lock around insertions anyway. A single lock is
  both simpler and avoids that hazard entirely.
- **Lock-free structures (e.g. a concurrent hash map)** — rejected: real
  lock-free maps are a nontrivial correctness surface (memory reclamation,
  ABA, etc.) to hand-roll, and pulling in a third-party one contradicts the
  "prefer the standard library" rule for a resume project whose story is
  about system design, not concurrent-data-structure engineering.
- **A single plain `std::mutex`** — rejected in favor of `shared_mutex`
  purely because GET is expected to dominate PUT/DELETE in the benchmark
  workload (stage 8), and a shared/exclusive lock lets concurrent GETs run
  in parallel while still serializing all writers — a small, free upgrade
  over a plain mutex with the same amount of code.

**Why a single lock is fine here, and what would change that:** a global
lock serializes every write against every other write (and against reads,
briefly). That's a real scalability ceiling — this section originally left
sharding the map as future work once a benchmark showed the lock was the
bottleneck. That work has now landed; see "Sharding" below for what changed
and docs/benchmarks.md for the throughput numbers that motivated it.

## Sharding

**Chosen: hash-based sharding, `shard_id = hash(key) % num_shards`, with a
fixed shard count decided at process startup.** Each shard is an
independent `Store` + `WriteAheadLog` pair with its own subdirectory
(`ShardedStore`, see `sharding.h`/`sharded_store.h`) — `Store`,
`WriteAheadLog`, and the snapshot format are all reused completely
unchanged; sharding only adds a routing layer that hashes a key and calls
through to the right shard's already-existing, already-tested `Store`.

**Hash-based vs. range partitioning:** the alternative to `hash(key) %
num_shards` is range partitioning — assigning contiguous key ranges (e.g.
lexicographic ranges of the key string) to shards, which is what you'd want
if the workload needed ordered range scans across keys or needed to
rebalance by splitting/merging adjacent ranges as data grows unevenly. This
store has neither requirement: the wire protocol is PUT/GET/DELETE by exact
key only (see docs/limitations.md — no scan/range operation exists or is
planned), so there is no query this store ever needs to answer by key
range, and there is no rebalancing mechanism in this phase (shard count is
fixed at startup, per this project's explicit scope for this stage — cluster
membership and shard migration are out of scope until a second node
exists). Range partitioning's actual advantages (ordered scans, splittable
ranges for rebalancing) are therefore paid for with zero benefit here, while
its main weakness — hot-spotting on skewed key distributions (e.g. all keys
sharing a common lexicographic prefix landing on one shard) — is a real
risk this workload has no defense against. Hash-based sharding has the
opposite tradeoff: it can't do ordered range scans (not needed) and gives up
easy rebalancing (not built yet either way), but a decent hash spreads any
key distribution — including adversarial or skewed real-world keys —
evenly across shards by construction, which is the one property this store
actually needs from its partitioning scheme today.

**Why FNV-1a instead of `std::hash<std::string>`:** shard assignment has to
be *stable across process restarts*, because each shard's WAL and snapshot
live at a fixed, shard-id-keyed directory — if a key ever hashed to a
different shard than it did when it was written, that key's prior data
wouldn't be lost, but it would become silently unreachable (still on disk,
just under the wrong shard's directory, since every current
`Store`/`WriteAheadLog` API is scoped to one shard's own path). The C++
standard does not specify `std::hash<std::string>`'s algorithm or guarantee
it's stable across standard library versions or vendors (libstdc++ vs.
libc++, or even different versions of the same one) — relying on it here
would mean a routing decision baked into on-disk layout could silently
change out from under existing data after a toolchain upgrade. FNV-1a is a
fully specified, ~10-line algorithm this project owns end to end (see
`sharding.cpp`), so shard assignment is guaranteed identical forever,
independent of any library implementation detail. This is the one piece of
"extra" engineering in this phase worth justifying under this project's
YAGNI stance — it costs a few lines and is directly why the crash
recovery and reopen tests in `tests/sharding_test.cpp` pass reliably instead
of by luck.

**Shard count: fixed at 8 by default, chosen at process startup, not
dynamic.** `ShardedStore`'s constructor takes `num_shards` once; there is no
API to add/remove/split shards on a running store. Alternatives considered:

- **A single shard (status quo)** — this is exactly what motivated this
  phase: `docs/benchmarks.md`'s new "Concurrency" section shows single-lock
  write throughput degrading as writer thread count increases (one lock
  serializing every writer's fsync), while 8 shards' independent
  locks/WAL files keep scaling through 32 threads in the same run.
- **A very large shard count (e.g. hundreds)** — rejected for now: more
  shards means more open WAL file descriptors and more separate small
  snapshot files for the same total data, with no evidence this project's
  benchmarked scale (a single node, `docs/benchmarks.md`'s restore-time
  numbers up to 1M keys) needs more parallelism than 8 independent
  locks/WALs already provide. 8 is a modest, round number chosen to
  demonstrate and measure the concurrency win, not a value tuned against a
  specific target throughput.
- **Dynamic/runtime-configurable shard count (resharding)** — explicitly
  out of scope per this phase's own instructions ("single-node... cluster
  comes next phase... do not build cluster membership yet"). Changing
  `num_shards` after data exists would require rehashing and physically
  moving every key that maps to a different shard under the new count — a
  real rebalancing mechanism this phase deliberately does not build. See
  docs/limitations.md.

**Shard-to-node ownership: `ShardMap`, single-node today.** Requirement 3 of
this phase asks for "a shard map: shard_id -> owning node," explicitly as a
placeholder for cluster membership rather than cluster membership itself:
`ShardMap::owning_node(ShardId)` always returns the same value ("local")
because there is exactly one node and always has been so far in this
project. This is not stubbed-out dead code for a case that can't occur
(the usual objection to that pattern) — it's the named seam a
future replication/clustering phase will extend by actually varying its
return value once a second node exists; today it does the one honest thing
it can do. No cluster membership protocol, node discovery, or rebalancing
logic exists yet — see docs/limitations.md.

**Request routing:** `ShardedStore::put/get/del/peek` each compute
`shard_for_key(key, num_shards)` once, then call directly through to that
shard's `Store::put/get/del/peek` — the same functions `dispatch()` already
called pre-sharding, just now on a per-shard instance instead of one global
one. `dispatch()` itself (`server.cpp`) needed no logic change at all: it
already just calls through to whatever "the store" is; only its parameter
type (`ShardedStore&` instead of `Store&`) changed.

## Safe Shutdown

**Chosen:** `TcpServer` tracks every currently-active connection's file
descriptor in an `active_fds_` set (guarded by `conn_mutex_`). `stop()`
closes the listening socket (unblocking `accept()`) and calls
`shutdown(fd, SHUT_RDWR)` on every fd in `active_fds_` (unblocking any
connection thread currently parked in a blocking `read()`/`write()`, which
then observes EOF/EPIPE and returns). `run()` does not return until
`active_fds_` is empty — signaled via a `std::condition_variable` — so a
caller that does `server.stop(); thread.join();` (as `main()` and every test
fixture do) is guaranteed no request is still being served once `join()`
returns.

**Alternatives considered:**
- **`stop()` only closes the listening socket** — rejected: this is what
  the code did before this stage (safe then, because the server was
  single-threaded and never had two connections open at once). Under
  concurrency, a connection thread can be blocked in `read()` on a socket
  that has nothing more to do with the listening socket at all — closing
  only the listener would leave that thread (and the request/response it's
  mid-handling) hanging forever whenever a client goes quiet after
  `stop()` is called.
- **Detach connection threads and don't wait for them at shutdown** —
  rejected: a detached thread that outlives `run()` returning could still
  be touching `store_` (a reference into the caller's `Store`) after the
  caller assumes the server has fully stopped and started tearing down —
  exactly the kind of use-after-free stage 4+ would inherit as a landmine.
  Draining before `run()` returns is what makes "the server is stopped"
  actually mean stopped.

**A genuine bug this stage's ThreadSanitizer run caught:** the first version
of this shutdown logic read/wrote the raw `int listen_fd_` from `run()`
(the accept-loop thread) and `stop()`/`~TcpServer()` (the caller's thread)
with no synchronization between them. TSan flagged it as a real data race
(`server.cpp:99` read vs. `server.cpp:136` write) even though the two runs
of the full test suite that triggered it (37/37 tests, 132 assertions)
otherwise passed — a torn or reordered read of that fd is exactly the class
of bug that's invisible without a race detector. Fixed by making
`listen_fd_` a `std::atomic<int>`, with `stop()`/`~TcpServer()` using
`exchange(-1)` so a concurrent `stop()` + destructor pair can't both try to
close the same fd. This is the one piece of "extra" synchronization added
beyond the minimum the tests demanded, and it's justified by a concrete,
observed failure, not speculation.

**ThreadSanitizer verification:** built with
`cmake -S . -B build-tsan -DKVSTORE_ENABLE_TSAN=ON && cmake --build build-tsan`
(adds `-fsanitize=thread -g -O1` via the `KVSTORE_ENABLE_TSAN` CMake option),
then ran `./build-tsan/tests/kvstore_tests` — the full suite, not just the
concurrency tests, so a race touching non-concurrency code paths would still
surface. After the `listen_fd_` fix, three consecutive full-suite runs
(37/37 tests, 132 assertions each) reported zero races.

## Consistency Model

**Chosen: leader persists to its own WAL and acks the client immediately;
followers replicate asynchronously, with no follower reads.** A PUT/DELETE
is acknowledged the instant the leader's own `Store::put()`/`del()` call
returns (i.e., the instant its WAL append+fsync completes) — the same
durability point stage 4 already established, completely unchanged by this
stage. Replication to followers happens afterward, on the leader's own
background `ReplicationLink` thread (see `docs/architecture.md`'s
"Replication" section), fully decoupled from the client-facing write path.
A client is never made to wait on any follower.

**What this means concretely: reads on followers are disallowed entirely,
not served stale.** `dispatch()` rejects PUT/GET/DELETE on a
`ReplicationRole::kFollower` node with `NOT_LEADER` rather than answering a
GET from whatever the follower has applied so far. The alternative —
allowing follower reads — was explicitly offered as a simpler option by
this stage's own requirements, and disallowing them is in fact the simpler
choice to implement *and* to reason about: it means this phase never has to
define or bound "how stale," document a staleness metric, or give clients
any way to request a consistency level. A future stage that wants to serve
reads off a follower for scaling would need to add exactly one thing this
phase deliberately doesn't: a bounded-staleness contract (e.g. "this read
may be up to N replicated writes / T milliseconds behind the leader"),
which isn't needed until read scaling is an actual goal.

**Alternatives considered:**
- **Synchronous replication (leader waits for at least one follower's ack
  before acking the client)** — rejected for this stage: it turns every
  write's latency into `leader_fsync + one_network_round_trip +
  follower_fsync` at minimum, and — more importantly — it raises a question
  this phase explicitly isn't ready to answer: what happens to a client's
  write if the one follower is down? Blocking forever defeats replication's
  purpose; timing out and acking anyway silently degrades to the async
  model anyway but with worse latency in the common case. That tradeoff is
  exactly what leader election/failover (stage 7) would need to reason
  about properly (e.g. a quorum size), so it's deferred rather than half-
  built here.
- **Quorum/majority acknowledgment (Raft-style, ack once a majority of
  replicas — including the leader — have durably applied a write)** —
  rejected: this only pays off once there's a mechanism to reason about
  what "majority" means when membership can change (a node joining,
  leaving, or being promoted), which is leader election/failover
  territory — explicitly out of scope for this stage. Without
  election, "majority of a fixed, never-changing set of nodes" buys none of
  quorum replication's actual benefit (tolerating a minority of failures
  without losing availability), just its latency cost.
- **Chain replication (leader -> follower 1 -> follower 2 -> ...)** —
  rejected: it reduces the leader's fan-out cost at the price of tail
  latency proportional to chain length and a more complex failure model
  (a mid-chain node's failure needs a re-linking protocol future work isn't
  building yet). This phase's fan-out (`ReplicationLink` per follower, all
  reading independently and directly off the leader's own WAL) is simpler
  and entirely sufficient at the N=1-to-a-handful-of-followers scale this
  project is ever exercised at.

**What a client can and cannot rely on:** an OK response to a PUT/DELETE
means that write is durable on the leader and will survive the leader
process crashing and restarting (stage 4/5's guarantee, unchanged). It does
*not* mean any follower has the write yet — if the leader's disk is lost
before any follower catches up, that acked write is gone. Making that
window small is what "asynchronous, but polled every 20ms" (see
"Replication Ack Policy" below) is for, not eliminating it — eliminating it
is exactly the synchronous-replication tradeoff rejected above.

## Replication Ack Policy

**Chosen: at-least-once delivery over a single persistent connection per
follower, made safe by idempotent apply (write_id dedup) on the follower,
with a leader-side poll rather than a push callback.**

**Why "at least once," not "exactly once":** the leader's
`ReplicationLink` doesn't persist which REPLICATE frames a follower has
acked anywhere durable — `ack_index_` is an in-memory
`ReplicationLink` member, lost if the leader process restarts. A leader
restart or a connection drop-and-reconnect can therefore cause the same
record to be sent again. Building true exactly-once delivery (e.g.
persisting per-follower ack state on the leader, surviving leader
restarts) is real additional machinery this phase doesn't need, because
at-least-once is already sufficient *given* an idempotent apply on the
receiving end — see below.

**Why apply is idempotent (`Store::apply_replicated()`) rather than
avoiding retransmission:** every replicated record carries the leader's own
`write_id`, which is store-wide monotonic per shard. `apply_replicated()`
skips a record outright — no WAL append, no map mutation — the instant
`record.write_id < next_write_id_` (this shard has already applied this
write_id or a later one). Retransmitting a record that already landed is
therefore a safe no-op instead of a duplicate WAL entry or a version
counter moving backward. This is what makes the required duplicate-request
test (`tests/replication_test.cpp`) hold: replaying the same op twice
produces exactly one WAL record and exactly one map mutation, not two.

**Why a leader-side poll (`ReplicationLink` reading
`Store::replay_wal_after()` every 20ms) instead of a callback wired into
`Store::put()`/`del()`:** a callback would need `Store`'s write path — which
today has zero knowledge that replication exists — to notify some observer
while still holding its `unique_lock`, or to push onto a queue a background
thread drains. Either adds real synchronization surface (a registration
API, a queue, backpressure if a slow follower falls behind) to the one code
path this whole project is most performance-sensitive about
(`docs/design-decisions.md`'s own "Fsync Policy" section already treats
`Store::put()`/`del()`'s hot path carefully for exactly this reason). A
poll costs nothing on the write path — `Store::replay_wal_after()` just
reads WAL segment files fresh each call — at the price of up to one poll
interval (20ms) of extra replication lag on top of network latency. Given
this stage's consistency model already accepts unbounded-in-principle
asynchronous lag (see "Consistency Model" above), a bounded, small,
constant polling delay is a cost this project is already willing to pay,
and avoiding new synchronization inside `Store` was judged worth it.

**Why one persistent TCP connection per follower (not per shard, not a new
connection per record):** REPLICATE records don't carry a shard id — the
follower re-derives which shard a record belongs to by hashing its key, the
same way the leader did — so one connection can safely multiplex every
shard's traffic. A new connection per record would pay a TCP handshake on
every single write; a connection per shard would work but cost `num_shards`
sockets/threads per follower for no benefit this phase's test cluster scale
needs (see docs/limitations.md's replication section for when that might
change).

**Why catch-up always re-queries the follower (CATCHUP_QUERY) instead of
trusting the leader's last-known `ack_index_`:** the leader's in-memory
`ack_index_` reflects what the leader *sent* and got an OK for, but a
follower that crashed and restarted has its own, independently-recovered
`last_applied_write_id()` — which could be lower (if some acked-but-not-yet-
fsynced-record's ack was actually a race, though `apply_replicated()`'s WAL
append+fsync happens before the OK response, so this shouldn't occur in
practice) or is simply the authoritative answer either way. Re-querying
means there is exactly one source of truth for "how caught up is this
follower" (the follower itself, via its own recovered `Store` state), not
two that could disagree.

**Alternatives considered:**
- **Persisting ack state durably on the leader (e.g. writing each
  follower's last-acked write_id to disk)** — rejected: this only matters
  for surviving a *leader* restart without re-replicating already-acked
  records to a follower that's still up and caught up — a pure efficiency
  concern (avoiding redundant, harmless resends), not a correctness one,
  since `apply_replicated()`'s dedup already makes resending safe. Not
  worth the extra disk writes until a benchmark shows redundant replication
  traffic after leader restarts is a real cost.
- **Sequence-number gaps / NACK-based retransmission** (follower detects a
  gap in write_ids and explicitly asks for the missing range) — rejected:
  the single-persistent-connection design means TCP already guarantees
  in-order, no-duplicate delivery *within* a connection; the only way a
  follower ever sees a gap is a full reconnect, which the CATCHUP_QUERY
  handshake already handles by asking the follower's real position rather
  than requiring it to detect and report a gap itself.

## WAL Format

**Chosen:** see `docs/architecture.md`'s "Write-Ahead Log" section for the
exact byte layout. Summary of the choices behind it:

- **Self-describing, length-prefixed records** rather than fixed-size
  records — rejected fixed-size for the same reason the wire protocol
  rejected it (see "Wire Protocol" above): keys and values vary in size, and
  padding every record to the max would waste disk space and I/O bandwidth
  on every small write.
- **A CRC-32 checksum per record.** This is the one piece of "extra"
  engineering in this stage worth justifying under this project's YAGNI
  stance, so: without a checksum, a torn write during a crash
  (e.g. the 4-byte `record_len` lands on disk but the payload doesn't, or a
  power loss mid-`write()` leaves some prefix of the payload bytes) reads
  back as *structurally plausible* data — right field widths, but wrong
  content — with no way to distinguish it from a real, intended record.
  Recovery would then either apply corrupted key/value bytes to the store,
  or worse, misinterpret a corrupted `key_len`/`value_len` and read
  completely unrelated bytes as if they were the next several records. A
  checksum turns "silently wrong" into "detectably wrong, drop and stop."
  Implemented as a ~30-line table-driven CRC-32 (standard IEEE 802.3
  polynomial) local to `wal.cpp` — no third-party dependency, consistent
  with "prefer the standard library."
- **`version` and `write_id` are stored per-record**, not just `key`/`value`
  — they were already added to `Entry` in stage 1 specifically so this stage
  wouldn't need a breaking storage-layer change (see "Entry Struct" above).
  `write_id` gives recovery a monotonic order to advance `next_write_id_`
  past; `version` is replayed as-is so a key's version count survives a
  restart unchanged, not reset to 1.
- **Record checksums do not cover `record_len` itself** — considered
  covering it, rejected as unnecessary complexity: any corruption of
  `record_len` shifts the byte range everything else is read from, which
  reliably shows up as either a bounds failure (claimed length overruns the
  segment) or a checksum mismatch (the shifted bytes don't check out) — a
  second, independent checksum over the length field would catch a
  vanishingly small class of corruption (one where `record_len` is wrong in
  exactly the way that keeps the rest of the record's bytes self-consistent
  anyway) that isn't worth the extra field.

**Alternatives considered:**
- **A single monolithic WAL file, never rotated** — rejected: see "Segment
  Rotation" below.
- **Storing records as length-prefixed protobuf/JSON/other serialization** —
  rejected: adds a dependency for a fixed, small set of fields this project
  fully controls on both the write and read side; a hand-rolled binary
  layout is a few dozen lines and is exactly the kind of from-scratch
  protocol work this project already does for the wire format.

## Fsync Policy

**Chosen: fsync every write (synchronous, no batching/group commit).**
`WriteAheadLog::append_record()` calls `write(2)` followed immediately by
`fsync(2)`, and does not return until `fsync` completes. `Store::put()`/
`del()` call this while still holding their `std::unique_lock`, so a PUT or
DELETE is not acknowledged to the client until its WAL record is durably on
disk — this is a direct, literal implementation of stage 4's requirement
("PUT and DELETE must go through the WAL before being acknowledged").

**Alternatives considered:**
- **Batched/group-commit fsync** (buffer N writes or wait T milliseconds,
  fsync once, ack all of them together) — rejected for this stage, not
  forever: it's a legitimate throughput optimization (one fsync amortized
  over many writes instead of one fsync per write), but it fundamentally
  changes the ack contract — a client's ack now means "durable once this
  batch's fsync happens," which requires either delaying every ack until the
  batch closes (adds latency to every write, not just bursty ones) or
  tracking which in-flight connection threads are waiting on which pending
  fsync and waking them after it completes (a real synchronization
  mechanism: a condition variable or future per pending batch, plus a
  policy for when a batch closes — size threshold? time threshold? both?).
  That's real design surface this project doesn't need yet: this store's
  benchmarked workload (stage 8) isn't shown to be fsync-bound, and
  thread-per-connection already serializes all writers behind `Store`'s
  single `unique_lock`, so there's no concurrent-writer batching opportunity
  sitting unused today — every write is already effectively fsync'd one at
  a time regardless. If stage 8's benchmark shows fsync latency dominating
  write throughput, group commit is the documented next step, not built now.
- **No fsync at all (rely on the OS page cache + periodic flush)** —
  rejected outright: this is the one policy that would make the "acked
  writes survive a crash" guarantee false. `write(2)` alone only guarantees
  the OS page cache has the bytes; a crash (or `SIGKILL`, per the required
  crash-and-restart test) can still lose them if they hadn't reached disk
  yet. Since durability-before-ack is this stage's entire point, this
  isn't a real option.
- **fsync on a timer (e.g. every 5ms) regardless of write volume** —
  rejected: same ack-timing problem as batching (a write's ack still has to
  wait for the *next* timer tick's fsync, which is itself a form of batching
  with a fixed rather than size-based trigger), for no simplicity benefit
  over the size/count-based batching option above.

**What this costs, concretely:** write throughput is capped at roughly
`1 / fsync_latency` — typically single-digit milliseconds on a spinning
disk, sub-millisecond to low-single-digit-milliseconds on an SSD without a
battery-backed write cache, and this project makes no claim about which
kind of disk it's benchmarked on until stage 8 actually measures it. GET
throughput is unaffected (GET never touches the WAL). This is the honest,
current ceiling on write throughput and will be reported as a measured
number, not a guess, in `docs/benchmarks.md` once stage 8 lands.

**Why fsync (not fdatasync):** `fsync(2)` flushes both file data and
metadata (e.g. file size after growth); `fdatasync(2)` skips metadata not
required to read the data back and is marginally cheaper on some platforms.
The difference is a micro-optimization this project doesn't need to chase
yet — `fsync` is simpler to reason about (it's the one every reader already
knows) and more portable, and nothing here is bottlenecked on the gap
between the two.

## Segment Rotation

**Chosen:** size-bounded segments (default 64 MiB, configurable via
`WriteAheadLog`'s `segment_bytes` constructor parameter), named by a
zero-padded ascending sequence number, with `WriteAheadLog` always appending
to the highest-numbered segment and rotating to a new one when the next
record would push the active segment past the threshold.

**Alternatives considered:**
- **One unbounded WAL file** — rejected: an unbounded file makes both
  recovery (would have to scan one arbitrarily large file from byte 0 every
  restart — true either way until snapshotting exists, but at least bounded
  per-segment work is the natural unit snapshotting will later truncate) and
  the corruption model harder to reason about. With segments, only the
  *active* segment can ever be torn by a crash (every earlier one was
  closed, fully written, and fsynced before rotation moved past it) — that
  invariant is what makes `recover()`'s "truncate only the last segment,
  hard-fail on corruption anywhere else" policy correct instead of a guess.
- **Time-based rotation (new segment every N minutes)** — rejected: ties
  rotation to wall-clock time for no benefit this project needs; a
  low-traffic period would still rotate (wasting a mostly-empty segment)
  and a high-traffic burst could still produce an oversized segment before
  the next time-based boundary. Size-based rotation bounds the thing that
  actually matters (how much one segment costs to scan/hold).
- **A fixed segment count with wraparound (ring buffer)** — rejected:
  that's a retention/eviction policy, which only makes sense once there's a
  mechanism (snapshotting, stage 5) that lets old segments be safely
  deleted. Rotating without ever removing old segments is the right amount
  of mechanism for this stage; removal is explicitly stage 5's job, not
  built here.

## Snapshot Format & Flow (stage 5)

**Chosen:** a single fixed-path file per store (`<snapshot_dir>/snapshot.bin`),
holding a header (`magic`, `format_version`, `boundary_write_id`,
`entry_count`) followed by every live `Store` entry — including
tombstones — as `(key_len, value_len, version, write_id, tombstone, key,
value)`. Full byte layout in `docs/architecture.md`.

**Boundary marker: a logical `write_id`, not a physical WAL byte
offset/segment+offset pair.** Stage 5's requirement is a "WAL boundary
marker (the WAL offset/index the snapshot is consistent with)."
`write_id` already *is* that index: it's the store-wide monotonic mutation
counter every WAL record carries, assigned in the same order records are
appended, and `Store` already needs it (for `next_write_id_`) regardless of
snapshotting. Using it instead of a literal byte offset means:
- The boundary survives WAL segment rotation and truncation unchanged — a
  physical `(segment_seq, offset)` pair would need updating/invalidating
  every time a covered segment gets deleted; a `write_id` doesn't.
- Recovery's "skip already-covered records" check
  (`apply_recovered()`: `if (record.write_id <= boundary) return;`) is a
  single integer comparison against a field every record already carries,
  not a segment/offset lookup.
- No new WAL-side bookkeeping was needed — `write_id` already existed and
  was already exposed per-record.

The only cost: a boundary phrased this way says nothing about *where on
disk* it lives — but nothing in this system needs that; both recovery and
truncation only ever ask "is this record's write_id <= the boundary?", never
"where is the boundary?".

**Why tombstones are included in the snapshot rather than dropped:** dropping
a tombstoned key would make it indistinguishable from a key that was never
written at all once the map is rebuilt — losing not just the delete-fact
(already flagged as a Phase 1 limitation) but also its `version` counter. A
later `put()` to that key would then compute `version = 1` (key absent
from the map) instead of continuing from wherever the tombstone's version
count left off, silently breaking the "version is a per-key mutation
counter that survives a restart unchanged" invariant the WAL's replay
already upholds. Including tombstones costs a byte-for-byte-identical
re-serialization of the live map (no filtering logic needed at all) and
keeps that invariant true across a snapshot+restore, not just across a
plain WAL replay.

**No per-record or whole-file checksum, unlike the WAL.** This is the
inverse of the WAL's checksum decision, and deliberately so: a WAL record
can be torn by a crash *mid-append*, because `append()` writes directly
into the file readers will later replay from. A snapshot is never read
from the path it's *written* to — `write_snapshot()` writes the full file
to `snapshot.tmp`, `fsync`s it, and only then `rename()`s it over
`snapshot.bin` (also `fsync`ing the directory so the rename itself is
durable). `rename()` on a POSIX filesystem is atomic: `load_latest()` can
only ever observe the complete previous file or the complete new one,
never a half-written one. The torn-write failure mode a checksum defends
against in the WAL structurally cannot happen here, so adding one would be
defending against a scenario that can't occur given how this file is
written — the same "don't handle a case that can't currently occur" rule
this project applies elsewhere. (Media bit-rot on an already-fsynced file is a
separate, out-of-scope concern — see `docs/limitations.md`, same carve-out
already made for the WAL.)

**Only one snapshot ever kept — no numbered snapshot history/retention
policy.** `write_snapshot()` always targets the same fixed filename via
temp-file-then-rename, so each new snapshot atomically replaces the last.
Rejected: keeping N previous snapshots (or one per some retention window) —
this store's recovery is single-node and has no use for an older
snapshot once a newer, strictly-more-complete one exists (there's no
point-in-time restore or backup/export feature to build here); keeping old
ones would only be paying disk space for snapshots nothing ever reads.

**`Store::take_snapshot()` releases its lock before doing any disk I/O.**
The map copy happens under a *shared* lock (blocking writers, not readers)
just long enough to `push_back` every entry into a `vector` and read
`next_write_id_ - 1`; serialization, the temp-file write, both `fsync`s,
and the `rename()` all happen afterward, unlocked. A write that lands in
the gap between releasing the lock and `WriteAheadLog::force_rotate()`
running is still handled correctly: it's appended to what was, at that
moment, still the "old" active segment (write_id > boundary), so
`truncate_before()` correctly refuses to delete that segment (its highest
write_id exceeds the boundary) — the store just retains one segment's
worth of already-covered records slightly longer than strictly necessary,
which the *next* snapshot cleans up. Holding the unique (write) lock across
the entire snapshot — including the disk I/O — was rejected: it would
block every PUT/DELETE for as long as serialization + `fsync` +
`rename` + WAL truncation takes, for a benefit (a marginally tighter WAL
retention window) that isn't needed here.

**Why `force_rotate()` on every `take_snapshot()` call, not just when the
active segment happens to be full:** without it, the segment that was
active while the snapshot was taken never becomes eligible for
`truncate_before()` (which never deletes the active segment), even once
every record in it is fully covered by the new snapshot's boundary. That
segment would sit on disk forever, and — more importantly — every future
recovery would still have to scan and skip every record in it, defeating
snapshotting's entire purpose for exactly the common case (take one
snapshot, then restart) `docs/benchmarks.md`'s restore-time number
measures. `force_rotate()` is a no-op (skips creating a pointless empty
segment) when the active segment is already empty, so calling
`take_snapshot()` repeatedly with no writes in between doesn't accumulate
useless empty `*.wal` files.

**Alternatives considered:**
- **Protobuf/JSON/other serialization for the snapshot body** — rejected
  for the same reason the WAL rejected it: a small, fully-controlled, fixed
  set of fields doesn't need a third-party dependency.
- **Copy-on-write / MVCC snapshotting (no lock needed at all)** — rejected
  as premature: this store's benchmarked workload (stage 8) doesn't need a
  snapshot that never blocks a writer even briefly; the shared-lock-for-copy
  approach above already keeps that block to "however long copying entries
  into a vector takes," not "however long serialization + disk I/O takes."

## Failover

**Chosen: heartbeat-based failure detection with an epoch bump, not a
consensus protocol.** Every node in a cluster with peers configured runs a
`FailoverMonitor` (see `docs/architecture.md`'s "Failover" section for the
mechanics). A leader heartbeats every peer on a fixed interval; a follower
that hasn't heard from a current-or-newer epoch within a timeout calls an
election by querying every peer's `(epoch, total committed write_id)` and
promoting itself if its own total is the best among reachable respondents.
The requirement for this stage was explicit: "heartbeat-based
detection and epoch bumping is enough, do not implement a general consensus
protocol (no Raft/Paxos)" — so this section is really about *what that
simplification costs*, stated plainly rather than glossed over, since that
honesty is the whole point of `docs/limitations.md`.

**Why one shared, mutex-guarded `ClusterState` instead of two atomics
(`role`, `epoch`):** the same reasoning `Store`'s locking section already
gives for one `std::shared_mutex` over the whole map — an observer reading
`role` and `epoch` via two independent atomics could see them mid-update
(the new epoch already visible, the demote-to-follower not yet, or vice
versa), a real (if narrow) window for a stale leader to keep accepting
writes one instruction longer than it should. `ClusterState`'s mutations
happen a handful of times over a node's entire lifetime (an election, or
observing a newer epoch) — nowhere near the hot path `Store::put()`/`get()`
already have to be careful about — so paying one mutex lock on every
`dispatch()` call (to read `role()`/check `accept_epoch()`) is free by this
project's own standard for when a lock's cost actually matters.

**Why "most up to date" is a single summed scalar (`total_committed()` —
`Store::last_applied_write_id()` summed across every shard) rather than a
per-shard vector comparison:** a per-shard comparison is the *more*
correct answer in principle — a follower could be ahead on shard 0 but
behind on shard 3, and a vector comparison would surface that, where a sum
can hide it (one follower a long way ahead on one busy shard can outscore
another that's slightly behind on every shard, even if the latter is
"more caught up" in a pointwise sense). Rejected anyway, for now: this
project's replication model ships every shard to every follower as one
unit over a single `ReplicationLink` (see `docs/architecture.md`'s "Why the
leader-side driver is one thread per follower, not per shard") — in the
steady state, every follower's per-shard lag tracks the others' closely
enough that a summed scalar and a pointwise comparison pick the same
winner in every scenario this project's own tests exercise. A per-shard
vector comparison would need a total order over vectors that don't
dominate each other (say, follower A ahead on shard 0, follower B ahead on
shard 3) — an actual policy decision (whichever shard's staleness matters
more? lexicographic by shard id?) that has no natural answer yet and isn't
forced by anything this project currently tests. Documented explicitly as
a real, if narrow, limitation — see `docs/limitations.md`.

**Why heartbeats and election queries use a one-shot connection (connect,
send, read one response, close) instead of riding `ReplicationLink`'s
existing persistent per-follower connection:** the two concerns are
genuinely separate — liveness (are you still there, and what epoch do you
think it is) versus data movement (replicate this WAL record) — and
merging them would mean `ReplicationLink`'s already-nontrivial
connect/reconnect/catch-up state machine also has to reason about
heartbeat cadence and election RPCs. A fresh short-lived TCP connection per
heartbeat/election call is the "boring over clever" choice consistent with
every other transport decision in this project (see the "Wire Protocol"
section above): it costs one extra handshake every `kHeartbeatIntervalMs`
per peer, which is negligible at this project's tested scale (a handful of
nodes, heartbeats tens of milliseconds apart), in exchange for zero shared
state between the two subsystems. `SO_RCVTIMEO`/`SO_SNDTIMEO` (not a
separate timer thread) bound each one-shot call so one unreachable peer
can't stall a whole monitor pass — the standard, minimal way to put a
deadline on a blocking socket call.

**Why the heartbeat interval/timeout/RPC-timeout are 30ms/150ms/100ms:**
chosen to make failover detection fast enough that this project's own
tests (killing a leader and waiting for promotion) run in well under a
second, not tuned against any real deployment's network RTT — this is a
localhost, single-machine test cluster, and these are ordinary named
constants in `failover.cpp` a real deployment would need to re-tune (larger
timeout relative to the interval, to tolerate real network jitter) rather
than a value this project claims is production-ready. `kRpcTimeoutMs`
(100ms) is deliberately shorter than `kHeartbeatTimeoutMs` (150ms) so one
slow/hung one-shot call can't itself consume the whole detection budget.

**The split-brain guard, stated plainly (documented explicitly rather than
glossed over):** `ClusterState::accept_epoch()` demotes a node to
follower the instant it observes a strictly higher epoch than its own,
regardless of which opcode carried it (a HEARTBEAT from a new leader
reaching it directly, or a STALE_TERM rejection bouncing back from a
REPLICATE/HEARTBEAT it sent). This is real and tested (see
`tests/failover_test.cpp`'s stale-leader-steps-down test) — but it only
closes the loop once the deposed leader can exchange *some* message with
*some* node that already knows about the new epoch. **What it does not
do:** while a leader is genuinely partitioned — unable to reach any node
that has heard about a new election — it keeps believing it is leader and
keeps accepting client writes, because nothing has told it otherwise yet.
Those writes are durable on its own WAL (stage 4's guarantee, unaffected),
but they may never reach the new leader's replica set, silently diverging
from the cluster's accepted history with no reconciliation mechanism (no
rollback, no merge — not built, and a real gap this project does not
paper over). See `docs/limitations.md` for this stated as a first-class
limitation, not a footnote.

**Why election is not quorum-based, and what that costs concretely:** a
candidate promotes itself if it has the best `total_committed()` among
whichever peers it could actually reach within `kRpcTimeoutMs` — not a
majority of the whole cluster. Under a network partition that splits the
cluster into two groups that can't see each other, each group can
independently conclude "the best node *I* can see should be leader" and
each promote a leader at the same epoch-bump-worth of increment, producing
two simultaneous leaders (a real double-promotion, not just a theoretical
one) until the partition heals and one of them observes the other's higher
epoch. A quorum requirement (only promote if a majority of the *total*
known peer set responds and agrees) would close this — the explicit
instruction for this stage was not to build that (no Raft/Paxos);
implementing real quorum-based leader election is the natural next step if
this project's scope ever grows to need it, and is called out as such
rather than silently assumed away — see `docs/limitations.md`.

## Online Resharding (stage 9)

**Chosen: in-process resharding (shard count N -> M within one process),
not cross-node shard placement/migration.** `ShardedStore::rebalance_to()`
copies every key into a freshly hashed N'-shard layout, forwards live
writes to it for the duration, then does one atomic routing swap. This was
a real scope decision, not the obvious reading of "rebalancing" — a
resharding request phrased around "target node" and "routing cutover"
points just as naturally at *cross-node* shard migration (move shard 3 from
node A to node B). That was rejected for this project specifically: every
other piece of cluster state here — `ShardMap::owning_node()` (always
`"local"`), `ClusterState::role()` (one role for the *entire* node, not
per-shard) — models a node-wide, single-role deployment with no per-shard
placement to migrate between. Building real per-shard node ownership would
mean adding a whole routing/placement layer (which node owns shard K right
now, and a MOVED-style redirect for clients that ask the wrong node) that
doesn't exist anywhere else in this codebase and partly conflicts with the
failover model (a promoted follower takes over *every* shard as a unit,
not one). In-process resharding needs none of that: it's testable in one
process, and it directly closes this project's own longest-standing
documented gap — `docs/limitations.md` has said since the sharding stage
that changing `num_shards` between restarts silently misroutes every
existing key. Cross-node shard placement remains unbuilt and is named as
the natural *next* phase, not something this stage quietly reinterpreted
away — see `docs/limitations.md`.

**Routing during migration: an atomically-swapped `RoutingPlan`, not a
lock.** `ShardedStore` used to route every request through one fixed array
of `Store`+`WriteAheadLog` pairs. Resharding needs two such arrays to exist
briefly (the old "source" layout and the new "target" layout being filled
in), so routing became a small struct:

```cpp
struct Layout { size_t num_shards; vector<...> wals; vector<...> stores; ... };
struct RoutingPlan { shared_ptr<Layout> primary; shared_ptr<Layout> secondary; bool dual_write; };
```

`ShardedStore` holds one `shared_ptr<RoutingPlan> plan_`, read via
`atomic_load(&plan_)` and published via `atomic_store(&plan_)` (the
pre-C++20 free-function form, since this project targets C++17). Every
PUT/GET/DELETE does exactly one atomic pointer load, then proceeds against
whatever `Layout` it got — no mutex, and no torn reads, since a concurrent
`atomic_store` can only ever be observed as fully-before or fully-after,
never partially. **Alternatives considered:**
- **A single mutex guarding "which layout is active," taken by every
  PUT/GET/DELETE** — rejected: this is precisely the "one lock serializes
  every request" shape the "Sharding" stage's whole story was about
  removing (see that section above). A rebalance is rare and short-lived;
  making every request pay a lock for it, forever, to support an operation
  that isn't normally happening, is the wrong trade.
- **`std::atomic<shared_ptr<T>>`** (the C++20 way to do this without the
  free-function calls) — not available: this project is pinned to C++17
  (`CMakeLists.txt`), and the free-function `atomic_load`/`atomic_store`
  form gives the identical guarantee on this standard.

**States as plans, not a mode flag Store has to know about:** idle/done is
`{primary=active, secondary=null, dual_write=false}` — byte-identical
routing to before resharding existed. Migrating is `{primary=source,
secondary=target, dual_write=true}`: PUT/DELETE acknowledge from `source`
(unchanged durability contract) and then forward the resulting entry into
`target`; GET/peek only ever read `source` — a client never observes a
half-migrated key. Cutover is one more `atomic_store` to
`{primary=target, secondary=null, dual_write=false}`. `Store` itself has
zero awareness that resharding exists; every one of these states is
encoded entirely in which `Layout` object(s) `ShardedStore` currently holds.

**Reconciling a migration target's incoming entries: per-key "newest
write_id wins," not the leader/follower dedup rule.** A migration target
shard receives entries from potentially *several* source shards (going
from 8 shards to 4 means each target shard merges 2 old shards' keys), each
with its own, independent, totally-ordered `write_id` sequence.
`Store::apply_replicated()`'s existing dedup (`record.write_id <
next_write_id_`, a single store-wide counter) is correct for replication
specifically because a follower's `write_id`s all come from *one* leader's
*one* sequence — comparing against a running counter is exactly comparing
against "have I seen this far yet." That comparison is meaningless once
entries arrive interleaved from multiple independent sequences (shard A's
write_id 400 and shard B's write_id 30 aren't comparable to each other at
all). `Store::apply_migrated()` instead compares **per key**: apply an
incoming record only if this key is new here or the existing entry's
`write_id` is older than the incoming one's. This is correct regardless of
arrival order because every version of a *given* key, wherever it comes
from, is still totally ordered by its own origin shard's `write_id` — "did
I already record a later state of this specific key" is a well-formed
question even when different keys' `write_id`s aren't comparable to each
other.

**Why `apply_migrated()` bypasses the WAL entirely — a real invariant this
project depends on elsewhere, not a shortcut.** `WriteAheadLog::
truncate_before()`/`recover()` rely on `write_id` increasing monotonically
in *append order* (see "WAL Format" above and "Segment Rotation" above) —
true for every existing writer (`Store::put()`/`del()`/`apply_replicated()`
all assign or receive `write_id`s in a single increasing sequence before
appending). A migration target's incoming entries are explicitly *not* in
`write_id` order (interleaved from multiple source sequences, per the
paragraph above), so writing them to the WAL as they arrive would silently
break that invariant — a later restart's `recover()` could replay an older
record *after* a newer one for the same key purely because of file
position, quietly regressing state. `apply_migrated()` instead updates only
the in-memory map (the same "apply directly, bypass `put()`/`del()`"
pattern `load_snapshot()` already uses), and `rebalance_to()` calls
`Store::take_snapshot()` on the target once migration completes — which
makes the whole merged, reconciled state durable in one step, on top of a
WAL that has never had a single out-of-order record land in it. This is
also why `Store::snapshot_entries()` exists: it's `take_snapshot()`'s own
under-a-shared-lock copy step, factored out so `rebalance_to()` can read a
source shard's full live state without a `snapshot.bin` round trip.

**A real TSan-caught race, and the lock it justified.** The first version
of the cutover step took `target`'s snapshot *after* publishing the
routing swap that makes `target` reachable for direct client PUT/DELETE.
ThreadSanitizer caught a genuine data race: `Store::take_snapshot()` calls
`WriteAheadLog::force_rotate()`/`truncate_before()` *after* releasing
`Store`'s own `mutex_` (a deliberate choice — see "Snapshot Format & Flow"
above — so a snapshot's disk I/O never blocks a writer), which is safe
exactly as long as nothing else is concurrently appending to that same
WAL. That was always true before this stage (`docs/limitations.md` has
long said nothing ever calls `take_snapshot()` on a store also serving
live traffic), so it was never exercised — `rebalance_to()` is the first
caller that takes a snapshot of a store *right as it becomes reachable*
for ordinary writes. The fix is `ShardedStore::cutover_mutex_`, held in
shared mode by every PUT/DELETE for their whole call and exclusively by
`rebalance_to()` for the brief window where it snapshots the target,
writes the manifest, and swaps the routing plan. While that exclusive
section holds, no PUT/DELETE anywhere (source or target) can be
mid-flight, which — as a side effect — also removes any need to separately
track and drain "in-flight forwarders": nothing can be mid-forward while
the lock is held exclusively, so every write that was ever going to reach
`target` already has, by the time it's snapshotted. This mirrors
`docs/design-decisions.md`'s own "Safe Shutdown" story almost exactly (a
TSan run catching a real bug, fixed with the minimum synchronization that
closes it) — a lock that's essentially free when uncontended (the
overwhelming common case: no rebalance in progress) is not a
reintroduction of the single global lock the "Sharding" stage removed; it
costs about what `Store`'s own per-shard `shared_mutex` already costs on
every GET, and multiple concurrent PUT/DELETE calls still never block each
other on it.

**Why `cutover_mutex_` is a hand-rolled `WritePreferringLock`, not
`std::shared_mutex` — a second real bug, this one a hang, not a race.**
The first version used a plain `std::shared_mutex`. Benchmarking
`rebalance_to()` under a genuinely continuous write workload (several
writer threads calling PUT back-to-back with no idle gaps) produced a
multi-minute hang with no forward progress and climbing memory — real,
reproducible, not a one-off. `std::shared_mutex` gives no fairness
guarantee between shared and exclusive waiters, and on this platform's
implementation, a steady stream of new shared-lock (PUT/DELETE) requests
was enough to starve `rebalance_to()`'s pending exclusive lock
indefinitely. A first fix attempt (an `atomic<bool> cutover_pending_`
courtesy flag PUT/DELETE checked before requesting the shared lock, so
they'd voluntarily back off once a writer was waiting) reduced but did not
reliably eliminate the hang — there's an inherent TOCTOU gap between
checking the flag and acquiring the lock that a fast-enough stream of
readers can still exploit. `WritePreferringLock`
(`rebalance.h`/`.cpp`) replaces `std::shared_mutex` entirely with a small
mutex+condition-variable implementation that makes "a waiting writer blocks
all new readers" an actual invariant the class enforces, not a hope about
the platform's rwlock behavior. This is real, self-contained synchronization
logic added specifically because two independent, empirically-observed
failures (the hang, and the reduced-but-not-eliminated hang after the first
fix attempt) demanded it — not a hypothetical worst case.

**A real, reproducible data-loss bug, and why `forward_if_migrating()`
re-checks `dual_write` after the write lands, not before.** Benchmarking
`rebalance_to()` under concurrent write load *combined with* a throttled
rate (the two correctness tests added during development each exercised
only one of those at a time) intermittently lost exactly one acknowledged
write per failing run. Root cause, confirmed by tracing every PUT/copy/
forward event for the missing key: a writer thread's `put()` calls
`atomic_load(&plan_)` *before* the actual shard-level `Store::put()` call —
if that call is then delayed by ordinary per-shard lock contention (queued
behind other writers on the same shard) until *after* `rebalance_to()`'s
copy loop has already taken that shard's `snapshot_entries()` snapshot, the
write lands in neither: not in the copy (already taken) and not forwarded
(because the `plan` this writer captured, before its write was delayed,
still had `dual_write == false` from before the migration started). The
fix: `forward_if_migrating()` takes no `plan` parameter and instead calls
`atomic_load(&plan_)` itself, *after* the caller's write has already
landed. This is safe specifically because the caller still holds
`cutover_mutex_`'s shared lock at that point (unchanged since before the
write), which guarantees `plan_` can only be "idle" or "migrating" there —
never "cutover" — since the exclusive lock a cutover requires cannot have
been granted while this call's shared lock is still held. That guarantee is
what makes the fix airtight rather than merely "usually right": whichever
of "the copy already saw this write" or "dual_write is true right now" is
true, at least one of them always is, so every write is captured by
exactly one path, never neither. `tests/rebalance_test.cpp`'s dedicated
regression test for this (concurrent load *and* a throttled rate together)
is what a plain unit test of either factor alone would not have caught.

**The honest cost: two fsyncs per write while a migration is in flight,
and a narrow read-staleness window right at cutover.** While `dual_write`
is active, every PUT/DELETE durably appends to `source`'s WAL *and*
(inside `forward_if_migrating()`) mutates `target`'s in-memory map — the
map mutation is cheap (no fsync, no WAL), but the client-visible cost is
still real: write latency and throughput during a migration are worse than
steady state, by design, and `docs/benchmarks.md`'s rebalance-overhead
numbers measure exactly this. Separately: because `cutover_mutex_` only
gates PUT/DELETE, not GET, a GET that lands in the vanishingly small window
between the routing swap and the *next* time a given key is written could
in principle still be served by whichever plan its own `atomic_load`
happened to see — this doesn't lose any acknowledged write (every write
durably lands on `source` or `target`'s WAL exactly as designed either
way), it's a bounded-microseconds read-freshness edge case, not a
durability one. See `docs/limitations.md`.

**Why a token-bucket throttle (`RateLimiter`, bytes/sec) rather than an
unthrottled copy or a fixed sleep-per-batch.** An unthrottled copy finishes
fast but maximizes how long the foreground write path pays dual-write's
extra cost per unit of *data moved* — `rate_bytes_per_sec` lets a caller
trade migration wall-clock time against foreground throughput impact,
which is the actual tradeoff a live rebalance needs to make. A fixed
sleep-per-entry was rejected: pacing by byte count (not entry count) means
the throttle means the same thing regardless of whether a workload has
tiny or large values, and refilling the bucket continuously from elapsed
wall-clock time (rather than a background timer thread) means the only
extra thread this feature ever spawns is `rebalance_to()`'s own — the
limiter itself is just arithmetic on `steady_clock::now()`.

**Manifest + rollback: a fixed-path file naming the active generation,
absent by default.** `<wal_dir>/shards.manifest` records `(generation,
num_shards)`, written atomically (temp file + fsync + rename + directory
fsync — the same pattern `snapshot.cpp`'s `write_snapshot()` already
established) at the very end of the cutover's exclusive section — the
single commit point after which a restart uses the new shard count.
Generation 0 (a store that has never been rebalanced) has no manifest file
at all and lives at exactly `<wal_dir>/shard_<i>` — today's original,
pre-stage-9 layout — so every existing deployment, test, and tool is
completely unaffected until `rebalance_to()` is actually called once.
Generation k > 0 lives at `<wal_dir>/gen_<k>/shard_<i>`, so a migration's
target never collides on disk with the source it's copying from. On
startup, `ShardedStore`'s constructor discards any `gen_*` directory that
isn't the manifest's active generation (or, with no manifest, discards all
of them) — this is the rollback: a crash mid-migration never gets to write
the manifest, so the source generation stays authoritative and the
abandoned, incomplete target generation is simply swept away. **Alternative
considered:** journaling migration progress so an interrupted rebalance
could *resume* instead of restarting from scratch — rejected as
unnecessary machinery for this project's scale: a `rebalance_to()` call
that gets interrupted just gets re-issued, and it re-copies from `source`
(untouched throughout, since it stays authoritative until the manifest
commits) exactly as the first attempt did.
