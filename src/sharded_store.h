#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "rebalance.h"
#include "sharding.h"
#include "store.h"
#include "wal.h"

using namespace std;

namespace kvstore {

// Owns `num_shards` independent shards — each a Store + WriteAheadLog pair
// with its own subdirectory — and routes every request to the correct shard
// by hashing its key (shard_for_key(), see sharding.h). Store, WriteAheadLog,
// and the snapshot format are used completely unchanged: each already took a
// directory param scoped to one instance, so sharding them is "make N of
// them, each pointed at its own subdirectory," not a rewrite of storage or
// durability logic. See docs/design-decisions.md's "Sharding" section.
//
// Stage 9 adds online resharding (rebalance_to()): changing the shard count
// while the store keeps serving traffic. Routing is therefore not simply
// "N fixed Store instances" any more — see the RoutingPlan/Layout comments
// below and docs/design-decisions.md's "Online Resharding" section.
class ShardedStore {
 public:
  // Shard i's WAL lives in `<wal_dir>/shard_<i>`, its snapshot in
  // `<snapshot_dir>/shard_<i>` — unless a prior rebalance_to() call has
  // committed a manifest naming a later generation and shard count, in
  // which case the manifest is authoritative and `num_shards` here is
  // ignored (see shard_dir()/ShardManifest in rebalance.h). `segment_bytes`
  // is forwarded to every shard's WriteAheadLog unchanged (default
  // kDefaultSegmentBytes) — exposed here for the same reason
  // WriteAheadLog itself exposes it, so a test can force rotation with a
  // handful of small records instead of 64 MiB of real writes.
  ShardedStore(size_t num_shards, string wal_dir, string snapshot_dir,
               size_t segment_bytes = kDefaultSegmentBytes);

  size_t num_shards() const;
  // Rebuilt fresh on every call (a plain int + a fixed "local" answer, see
  // ShardMap) rather than stored, since num_shards can now change under
  // rebalance_to() and this accessor's only caller today is future code
  // that hasn't been written yet (see sharding.h) — nothing currently reads
  // it, so it isn't part of any hot or even tested path.
  ShardMap shard_map() const { return ShardMap(num_shards()); }

  // Routing: hash the key, then call straight through to that shard's Store,
  // which takes its own shared_mutex internally (unchanged from
  // stage 3). Two keys landing in two different shards now proceed fully in
  // parallel instead of contending on one global lock. While a rebalance is
  // in progress (see rebalance_to()), a successful PUT/DELETE is also
  // forwarded to the migration target; GET/peek always read only the
  // current shard count's authoritative copy — never the migration target
  // — so no caller ever observes a partially-migrated read.
  //
  // PUT/DELETE also take a shared lock on cutover_mutex_ for their whole
  // call — cheap and uncontended except during the brief moment
  // rebalance_to() holds it exclusively (see cutover_mutex_'s comment
  // below); this is not the global lock stage 3/"Sharding" removed.
  PutResult put(const string& key, const string& value);
  optional<string> get(const string& key) const;
  DeleteResult del(const string& key);
  optional<Entry> peek(const string& key) const;

  // Sum of every shard's size().
  size_t size() const;

  // Startup recovery: load_snapshots() then recover_from_wal(), across every
  // shard, before any client traffic — the same ordering requirement Store
  // itself documents, just applied once per shard. Each shard's recovery
  // runs independently: a corrupt or missing shard's data doesn't touch any
  // other shard's WAL/Store instance. Return values are totals across all
  // shards, matching Store::load_snapshot()/recover_from_wal()'s per-shard
  // meaning.
  size_t load_snapshots();
  size_t recover_from_wal();

  // Snapshots every shard independently (one take_snapshot() call per
  // shard's own Store/WriteAheadLog).
  void take_snapshot();

  // Routes a replicated record to the same shard the leader assigned it to
  // — hashing record.key the same way put()/get()/del() already do, since
  // shard assignment is a pure function of the key and both ends share the
  // same num_shards. See Store::apply_replicated().
  void apply_replicated(const Record& record);

  // Direct per-shard access: used by tests to assert distribution and to
  // drive/inspect one shard's recovery in isolation. Not safe to call
  // concurrently with an in-flight rebalance_to() on this instance — these
  // return a reference into whichever Layout is active *right now*, with no
  // guarantee it stays active for the reference's lifetime if a rebalance
  // swaps it out concurrently (test/debug accessors only, same "caller must
  // serialize with any concurrent mutation" contract Store's own peek()
  // implicitly has).
  Store& shard(ShardId id);
  const string& wal_dir(ShardId id) const;
  const string& snapshot_dir(ShardId id) const;

  // --- Online resharding (stage 9) ---
  //
  // Reshards this store from its current shard count to new_num_shards
  // online: throttled copy of every key into freshly hashed target shards
  // (paced to at most rate_bytes_per_sec bytes/sec; 0 = unlimited), live
  // PUT/DELETE forwarded to the target for the duration, then an atomic
  // routing cutover once every forwarded write has drained. Blocks the
  // calling thread for the whole migration; other threads' PUT/GET/DELETE
  // traffic is served throughout, just at reduced throughput while the
  // migration is in flight (two fsyncs per write — see docs/benchmarks.md's
  // rebalance-overhead numbers).
  //
  // This is in-process resharding (shard count N -> M within one process),
  // not cross-node shard placement/migration — this project has no
  // per-shard node ownership to migrate between (ShardMap::owning_node()
  // always returns "local"; see docs/limitations.md). See
  // docs/design-decisions.md's "Online Resharding" section for the full
  // design and its consistency/durability tradeoffs.
  //
  // Not safe to call concurrently with another rebalance_to() on the same
  // instance — callers (main.cpp, tests, the benchmark tool) only ever
  // drive it from one thread at a time.
  RebalanceState rebalance_to(size_t new_num_shards, size_t rate_bytes_per_sec = 0);

  RebalanceState rebalance_state() const { return rebalance_state_.load(); }

 private:
  // One full shard fan-out: num_shards independent Store+WriteAheadLog
  // pairs and the directories they live in. ShardedStore owns exactly one
  // "active" Layout at rest, and briefly two (source + target) while
  // rebalance_to() is migrating — see RoutingPlan below.
  struct Layout {
    size_t num_shards = 0;
    vector<string> wal_dirs;
    vector<string> snapshot_dirs;
    vector<unique_ptr<WriteAheadLog>> wals;
    vector<unique_ptr<Store>> stores;
  };

  // What put()/get()/del() currently route through, read via
  // atomic_load(&plan_)/published via atomic_store(&plan_) — one atomic
  // shared_ptr load per request, no mutex on the hot path, matching the
  // "no lock reintroduced by a later stage" bar the sharding stage this one
  // builds on already set (see docs/design-decisions.md's "Sharding"
  // section). `primary` is always what GET/peek read and what PUT/DELETE
  // are acknowledged from; when dual_write is set, a successful PUT/DELETE
  // on primary is also forwarded to `secondary` (the migration target) via
  // Store::apply_migrated(). See docs/design-decisions.md's "Online
  // Resharding" section.
  struct RoutingPlan {
    shared_ptr<Layout> primary;
    shared_ptr<Layout> secondary;
    bool dual_write = false;
  };

  static shared_ptr<Layout> build_layout(size_t num_shards, const string& wal_dir_root,
                                          const string& snapshot_dir_root, uint64_t generation,
                                          size_t segment_bytes);

  // Forwards one successful PUT/DELETE's resulting entry (already durable
  // on `source_shard`) to the migration target, if a migration is
  // currently in progress — re-checking plan_ fresh via atomic_load rather
  // than reusing whatever plan the caller captured before doing the write.
  // This matters: Store-level lock contention can delay a PUT/DELETE's
  // actual mutation until after rebalance_to()'s copy loop has already
  // snapshotted that shard, and by then dual_write may have become true
  // even though it was false when the caller started. Re-checking after
  // the write lands, while still holding cutover_mutex_'s shared lock (so
  // plan_ can only be "idle" or "migrating" here, never "cutover" — see
  // cutover_mutex_'s comment), guarantees every write is captured by
  // exactly one of "the copy loop already saw it" or "dual_write was true
  // when it's rechecked here" — never neither. A real, reproducible data
  // loss (found by benchmarking rebalance_to() under concurrent load) is
  // what motivated re-checking here instead of trusting the caller's
  // earlier snapshot — see docs/design-decisions.md's "Online Resharding"
  // section.
  void forward_if_migrating(ShardId source_shard, const string& key);

  string wal_dir_root_;
  string snapshot_dir_root_;
  size_t segment_bytes_;
  // Only ever read/written from rebalance_to() (not safe to call
  // concurrently with itself — see its doc comment above) and the
  // constructor, so this needs no lock of its own.
  uint64_t generation_;

  shared_ptr<RoutingPlan> plan_;
  atomic<RebalanceState> rebalance_state_{RebalanceState::kIdle};

  // Held in shared mode by every PUT/DELETE for their whole call, and
  // exclusively by rebalance_to() for the brief window where it calls
  // take_snapshot() on the migration target and then publishes the cutover
  // routing plan. Needed because Store::take_snapshot() calls
  // WriteAheadLog::force_rotate()/truncate_before() *after* releasing
  // Store's own mutex_ (by design — see docs/design-decisions.md's
  // "Snapshot Format & Flow" section on why: to not hold a write lock
  // across disk I/O) — those two calls mutate the WAL's fd_/active_seq_
  // with no synchronization of their own, relying entirely on the caller
  // never invoking them concurrently with Store::put()/del() on the same
  // store. That was always true pre-stage-9 (nothing ever called
  // take_snapshot() on a store also serving live traffic — see
  // docs/limitations.md); rebalance_to() is the first caller that does
  // (on the migration target, right as it becomes reachable for direct
  // PUT/DELETE), so it's the first caller that has to hold this out
  // itself. Cheap and uncontended in the overwhelmingly common case (no
  // rebalance in progress): shared-lock acquisition costs about what
  // Store's own per-shard shared_mutex already costs on every GET, and
  // multiple concurrent PUT/DELETE calls still don't block each other here
  // — this is not a reintroduction of the single global lock the
  // "Sharding" stage removed. A genuine TSan-caught data race motivated
  // adding this lock at all, exactly like TcpServer's listen_fd_ fix; a
  // genuine (non-simulated) writer-starvation hang, caught while
  // benchmarking, motivated it being a WritePreferringLock rather than a
  // plain std::shared_mutex — see docs/design-decisions.md's "Online
  // Resharding" section for both.
  WritePreferringLock cutover_mutex_;
};

}  // namespace kvstore
