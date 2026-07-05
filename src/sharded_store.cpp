#include "sharded_store.h"

#include <filesystem>
#include <shared_mutex>

using namespace std;

namespace kvstore {

namespace fs = filesystem;

ShardedStore::ShardedStore(size_t num_shards, string wal_dir, string snapshot_dir,
                           size_t segment_bytes)
    : wal_dir_root_(move(wal_dir)),
      snapshot_dir_root_(move(snapshot_dir)),
      segment_bytes_(segment_bytes),
      generation_(0) {
  size_t active_num_shards = num_shards;
  if (auto manifest = read_shard_manifest(wal_dir_root_)) {
    generation_ = manifest->generation;
    active_num_shards = manifest->num_shards;
  }

  // Discard any staging generation left behind by a rebalance_to() call
  // that never reached cutover (a crash mid-migration) — the manifest (if
  // any) still names the last *committed* generation, so anything else
  // under the wal/snapshot roots is safely abandoned; the shards it was
  // copying from were never staged under a gen_ directory in the first
  // place (or, if they were an earlier rebalance's target, they're exactly
  // generation_ and therefore kept). See docs/design-decisions.md's
  // "Online Resharding" section.
  discard_stale_generations(wal_dir_root_, generation_);
  discard_stale_generations(snapshot_dir_root_, generation_);

  auto layout = build_layout(active_num_shards, wal_dir_root_, snapshot_dir_root_, generation_,
                              segment_bytes_);
  auto plan = make_shared<RoutingPlan>();
  plan->primary = move(layout);
  plan_ = move(plan);
}

shared_ptr<ShardedStore::Layout> ShardedStore::build_layout(size_t num_shards,
                                                             const string& wal_dir_root,
                                                             const string& snapshot_dir_root,
                                                             uint64_t generation,
                                                             size_t segment_bytes) {
  auto layout = make_shared<Layout>();
  layout->num_shards = num_shards;
  layout->wal_dirs.reserve(num_shards);
  layout->snapshot_dirs.reserve(num_shards);
  layout->wals.reserve(num_shards);
  layout->stores.reserve(num_shards);

  for (size_t i = 0; i < num_shards; ++i) {
    layout->wal_dirs.push_back(shard_dir(wal_dir_root, generation, i));
    layout->snapshot_dirs.push_back(shard_dir(snapshot_dir_root, generation, i));
    layout->wals.push_back(make_unique<WriteAheadLog>(layout->wal_dirs.back(), segment_bytes));
    layout->stores.push_back(make_unique<Store>(layout->wals.back().get()));
  }
  return layout;
}

void ShardedStore::forward_if_migrating(ShardId source_shard, const string& key) {
  // Re-fetch plan_ *after* the caller's write already landed, rather than
  // reusing whatever plan the caller captured before doing the write — see
  // this method's doc comment in sharded_store.h for the race this closes.
  auto plan = atomic_load(&plan_);
  if (!plan->dual_write) return;

  // peek() is guaranteed to find this key: the caller's PUT/DELETE on
  // `source_shard` just succeeded, and Store never erases a map entry
  // (DELETE tombstones it, see Entry::tombstone in store.h) — so there is
  // always something here to forward.
  Entry entry = *plan->primary->stores[source_shard]->peek(key);
  ShardId dst_shard = shard_for_key(key, plan->secondary->num_shards);
  Record record{entry.tombstone ? RecordType::kDelete : RecordType::kPut, entry.write_id,
                entry.version, key, entry.tombstone ? string() : entry.value};
  plan->secondary->stores[dst_shard]->apply_migrated(record);
}

PutResult ShardedStore::put(const string& key, const string& value) {
  shared_lock cutover_lock(cutover_mutex_);
  auto plan = atomic_load(&plan_);
  ShardId shard_id = shard_for_key(key, plan->primary->num_shards);
  PutResult result = plan->primary->stores[shard_id]->put(key, value);
  if (result == PutResult::kOk) forward_if_migrating(shard_id, key);
  return result;
}

optional<string> ShardedStore::get(const string& key) const {
  auto plan = atomic_load(&plan_);
  return plan->primary->stores[shard_for_key(key, plan->primary->num_shards)]->get(key);
}

DeleteResult ShardedStore::del(const string& key) {
  shared_lock cutover_lock(cutover_mutex_);
  auto plan = atomic_load(&plan_);
  ShardId shard_id = shard_for_key(key, plan->primary->num_shards);
  DeleteResult result = plan->primary->stores[shard_id]->del(key);
  if (result == DeleteResult::kOk) forward_if_migrating(shard_id, key);
  return result;
}

optional<Entry> ShardedStore::peek(const string& key) const {
  auto plan = atomic_load(&plan_);
  return plan->primary->stores[shard_for_key(key, plan->primary->num_shards)]->peek(key);
}

size_t ShardedStore::size() const {
  auto plan = atomic_load(&plan_);
  size_t total = 0;
  for (const auto& store : plan->primary->stores) total += store->size();
  return total;
}

size_t ShardedStore::load_snapshots() {
  auto plan = atomic_load(&plan_);
  size_t total = 0;
  for (size_t i = 0; i < plan->primary->stores.size(); ++i) {
    total += plan->primary->stores[i]->load_snapshot(plan->primary->snapshot_dirs[i]);
  }
  return total;
}

size_t ShardedStore::recover_from_wal() {
  auto plan = atomic_load(&plan_);
  size_t total = 0;
  for (auto& store : plan->primary->stores) total += store->recover_from_wal();
  return total;
}

void ShardedStore::take_snapshot() {
  auto plan = atomic_load(&plan_);
  for (size_t i = 0; i < plan->primary->stores.size(); ++i) {
    plan->primary->stores[i]->take_snapshot(plan->primary->snapshot_dirs[i]);
  }
}

Store& ShardedStore::shard(ShardId id) {
  auto plan = atomic_load(&plan_);
  return *plan->primary->stores[id];
}

const string& ShardedStore::wal_dir(ShardId id) const {
  auto plan = atomic_load(&plan_);
  return plan->primary->wal_dirs[id];
}

const string& ShardedStore::snapshot_dir(ShardId id) const {
  auto plan = atomic_load(&plan_);
  return plan->primary->snapshot_dirs[id];
}

void ShardedStore::apply_replicated(const Record& record) {
  auto plan = atomic_load(&plan_);
  ShardId shard_id = shard_for_key(record.key, plan->primary->num_shards);
  plan->primary->stores[shard_id]->apply_replicated(record);
}

size_t ShardedStore::num_shards() const {
  auto plan = atomic_load(&plan_);
  return plan->primary->num_shards;
}

RebalanceState ShardedStore::rebalance_to(size_t new_num_shards, size_t rate_bytes_per_sec) {
  shared_ptr<Layout> source = atomic_load(&plan_)->primary;

  uint64_t target_generation = generation_ + 1;
  shared_ptr<Layout> target = build_layout(new_num_shards, wal_dir_root_, snapshot_dir_root_,
                                            target_generation, segment_bytes_);

  // idle -> migrating: publish the dual-write plan. From this instant every
  // live PUT/DELETE landing on `source` is also forwarded to `target` (see
  // forward_if_migrating()); GET/peek keep reading only `source` — no
  // client ever observes a partially-migrated read.
  auto migrating_plan = make_shared<RoutingPlan>();
  migrating_plan->primary = source;
  migrating_plan->secondary = target;
  migrating_plan->dual_write = true;
  atomic_store(&plan_, migrating_plan);
  rebalance_state_.store(RebalanceState::kMigrating);

  // Throttled copy: for each source shard's point-in-time snapshot (taken
  // under that shard's own shared lock — see Store::snapshot_entries()),
  // rehash every entry into its new shard and apply it via
  // Store::apply_migrated(), which resolves races against concurrently
  // forwarded live writes to the same key by "newest write_id wins" —
  // correct regardless of arrival order since a target shard's incoming
  // entries all originate from some single source shard's own
  // totally-ordered write_id sequence. See docs/design-decisions.md's
  // "Online Resharding" section.
  RateLimiter limiter(rate_bytes_per_sec);
  for (size_t i = 0; i < source->stores.size(); ++i) {
    auto copied = source->stores[i]->snapshot_entries();
    for (const auto& e : copied.first) {
      ShardId dst_shard = shard_for_key(e.key, new_num_shards);
      Record record{e.tombstone ? RecordType::kDelete : RecordType::kPut, e.write_id, e.version,
                    e.key, e.tombstone ? string() : e.value};
      target->stores[dst_shard]->apply_migrated(record);
      limiter.consume(e.key.size() + e.value.size());
    }
  }

  rebalance_state_.store(RebalanceState::kCutover);

  // Brief exclusive section: block every in-flight and new PUT/DELETE
  // (cutover_mutex_'s shared-lock holders — see its comment in
  // sharded_store.h) so `target`'s WAL is guaranteed to have zero
  // concurrent callers while we make it durable and flip routing to it.
  // Without this, a PUT/DELETE landing on `target` right after the routing
  // swap (an ordinary, already-correct call to Store::put()/del(), which
  // appends to target's WAL under its own mutex_) could run concurrently
  // with this function's own take_snapshot() call on the very same store —
  // whose force_rotate()/truncate_before() mutate WAL-internal state
  // *after* Store releases mutex_ (see docs/design-decisions.md's "Snapshot
  // Format & Flow"), so nothing serializes the two. A TSan run caught
  // exactly this race the first time this function was written; this lock
  // is the fix, not a defensive guess. Because no PUT/DELETE anywhere (on
  // `source` or `target`) can be mid-flight while this section holds the
  // lock exclusively, every write that was ever going to be forwarded into
  // `target` has already completed by the time take_snapshot() runs below —
  // no separate straggler-draining step is needed.
  {
    unique_lock cutover_lock(cutover_mutex_);

    for (size_t i = 0; i < target->stores.size(); ++i) {
      target->stores[i]->take_snapshot(target->snapshot_dirs[i]);
    }

    // Commit point: once this returns, a restart uses `target` — see
    // ShardedStore's constructor. Every write acknowledged from `source`
    // is already reflected in the snapshot just written above.
    write_shard_manifest(wal_dir_root_, ShardManifest{target_generation, new_num_shards});
    generation_ = target_generation;

    // The atomic routing cutover: from this instant every PUT/GET/DELETE
    // routes straight to `target` via its own normal Store::put()/get()/
    // del() — no more forwarding, no more dual-write.
    auto cutover_plan = make_shared<RoutingPlan>();
    cutover_plan->primary = target;
    atomic_store(&plan_, cutover_plan);
  }
  rebalance_state_.store(RebalanceState::kDone);

  // Best-effort cleanup: the manifest above already makes `source`
  // unreachable after any future restart (discard_stale_generations() in
  // the constructor would delete it anyway) — deleting it now just
  // reclaims disk space immediately. Not safety-critical: `source`'s
  // Store/WriteAheadLog objects are still alive via this function's own
  // `source`/`migrating_plan` locals, so any of their still-open file
  // descriptors keep working fine against the now-unlinked files until
  // this function returns and they're destroyed.
  for (const auto& dir : source->wal_dirs) {
    error_code ec;
    fs::remove_all(dir, ec);
  }
  for (const auto& dir : source->snapshot_dirs) {
    error_code ec;
    fs::remove_all(dir, ec);
  }

  return RebalanceState::kDone;
}

}  // namespace kvstore
