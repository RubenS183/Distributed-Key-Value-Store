#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "snapshot.h"
#include "wal.h"

using namespace std;

namespace kvstore {

// 1 KiB / 1 MiB. See docs/design-decisions.md for rationale.
inline constexpr size_t kMaxKeySize = 1024;
inline constexpr size_t kMaxValueSize = 1024 * 1024;

enum class PutResult { kOk, kKeyTooLarge, kValueTooLarge, kEmptyKey };
enum class DeleteResult { kOk, kNotFound };

// One versioned slot in the store. `version` and `write_id` are not yet
// surfaced over the wire; they exist now so later stages (WAL, replication)
// don't require reshaping this struct.
struct Entry {
  string value;
  uint64_t version = 0;    // per-key mutation count, starts at 1 on first write
  uint64_t write_id = 0;   // store-wide monotonic mutation sequence
  bool tombstone = false;  // true if this key was deleted; entry is kept, not erased
};

// Thread-safe in-memory key-value map. A single shared_mutex guards the
// whole map: put()/del() take a unique (write) lock, get()/peek()/size() take
// a shared (read) lock. One lock for the whole map rather than per-key or
// per-shard locking, because this is a single-shard store with one
// `unordered_map` — see docs/design-decisions.md for why a per-shard scheme
// would be premature here.
class Store {
 public:
  // `wal` is optional (nullptr by default) so tests that only care about
  // in-memory semantics can keep constructing a bare `Store` with no
  // durability at all. When non-null, put()/del() append to `wal` (fully
  // fsynced) before returning — see docs/design-decisions.md's "Fsync
  // Policy" section for why every write is synced rather than batched.
  explicit Store(WriteAheadLog* wal = nullptr) : wal_(wal) {}

  PutResult put(const string& key, const string& value);

  // Returns the entry's value if the key exists and is not tombstoned.
  optional<string> get(const string& key) const;

  DeleteResult del(const string& key);

  // Returns the full Entry, including tombstoned ones, or nullopt if the key
  // was never written. Exposes version/write_id/tombstone directly, since
  // get() only surfaces the value; used by tests now and by WAL/replication
  // code once those stages need per-entry metadata.
  optional<Entry> peek(const string& key) const;

  size_t size() const;

  // Rebuilds this store's map by replaying every valid record from the WAL
  // passed to the constructor, skipping any record already covered by a
  // snapshot loaded via load_snapshot() (call load_snapshot() first, if at
  // all). No-op (returns 0) if this Store was constructed without a WAL.
  // Must be called once at startup, before any put()/get()/del() call and
  // before any client traffic is served.
  size_t recover_from_wal();

  // Loads `<snapshot_dir>/snapshot.bin` (if it exists) and applies its
  // entries directly to the map, then remembers its boundary write_id so a
  // subsequent recover_from_wal() call skips WAL records already reflected
  // in it. No-op (returns 0) if no snapshot exists yet at that path. Must
  // be called at most once, before recover_from_wal() and before any
  // put()/get()/del() call.
  size_t load_snapshot(const string& snapshot_dir);

  // Freezes a consistent, point-in-time view of the live map (entries,
  // including tombstones), serializes it to `<snapshot_dir>/snapshot.bin`
  // (temp file + fsync + atomic rename — see snapshot.h), then
  // rotates the WAL to a fresh active segment and deletes whatever WAL
  // segments are now fully covered by the new snapshot's boundary. No-op
  // if this Store was constructed without a WAL — a snapshot without a WAL
  // to truncate has nothing to bound recovery time for.
  void take_snapshot(const string& snapshot_dir);

  // --- Replication (stage 6) ---
  //
  // Applies one record replicated from a shard's leader, exactly as
  // received: write_id/version come from the leader, not assigned here.
  // Idempotent — a record whose write_id is already covered (< next_write_id_)
  // is skipped entirely, including the WAL append, which is what makes
  // redelivering the same record after a lost ack or a reconnect safe. See
  // docs/design-decisions.md's "Replication Ack Policy".
  void apply_replicated(const Record& record);

  // Highest write_id this store has durably applied — from real client
  // writes if this is a leader's Store, or from replicated records if this
  // is a follower's. 0 means nothing has been applied yet. Used by the
  // leader's catch-up logic to learn how far behind a (re)connecting
  // follower is.
  uint64_t last_applied_write_id() const;

  // Replays every WAL record with write_id > after_write_id, in ascending
  // order, without mutating this store — see WriteAheadLog::replay_after().
  // No-op (returns 0) if this Store was constructed without a WAL.
  size_t replay_wal_after(uint64_t after_write_id,
                                const function<void(const Record&)>& apply) const;

  // --- Online resharding (stage 9) ---
  //
  // Point-in-time copy of every live entry (including tombstones) plus this
  // store's current boundary (highest write_id ever assigned), taken under
  // a shared lock -- exactly the copy take_snapshot() already made, factored
  // out so ShardedStore::rebalance_to() can reuse it to read a source
  // shard's full state directly into memory, with no snapshot.bin round
  // trip.
  pair<vector<SnapshotEntry>, uint64_t> snapshot_entries() const;

  // Applies one entry copied or forwarded from another shard during
  // ShardedStore::rebalance_to(), keeping this key's most up-to-date state:
  // applied only if this key is missing here or the existing entry's
  // write_id is older than record.write_id. Unlike apply_replicated()
  // (whose dedup compares against next_write_id_ -- correct because a
  // follower's write_ids are one leader's single totally-ordered sequence),
  // a rebalance target aggregates entries from multiple source shards, each
  // with its own independent write_id sequence, so only a per-key
  // comparison is meaningful here.
  //
  // Deliberately bypasses the WAL (like load_snapshot()): the mutations
  // being applied are already durable on their *source* shard's WAL, and
  // writing them here in arrival order -- which is not write_id order,
  // since entries interleave from multiple source sequences -- would
  // violate the WAL's "write_id increases monotonically in append order"
  // invariant that recover()/truncate_before() rely on. The caller
  // (ShardedStore::rebalance_to()) is responsible for calling
  // take_snapshot() on this store once migration completes, making the
  // merged state durable in one step on top of a WAL that has never had an
  // out-of-order record land in it. See docs/design-decisions.md's "Online
  // Resharding" section.
  void apply_migrated(const Record& record);

 private:
  // Applies one already-validated WAL record directly to the map, bypassing
  // put()/del() so recovery doesn't re-append the record it just read.
  // Skips records already covered by a loaded snapshot (see
  // snapshot_boundary_write_id_).
  void apply_recovered(const Record& record);

  mutable shared_mutex mutex_;
  unordered_map<string, Entry> map_;
  uint64_t next_write_id_ = 1;
  WriteAheadLog* wal_ = nullptr;
  // Highest write_id already reflected in a snapshot loaded via
  // load_snapshot(); 0 (its default) means "no snapshot loaded" and is
  // indistinguishable from — and behaves identically to — a snapshot whose
  // boundary happens to be 0, since write_id assignment starts at 1.
  uint64_t snapshot_boundary_write_id_ = 0;
};

}  // namespace kvstore
