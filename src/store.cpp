#include "store.h"

#include <vector>

using namespace std;

namespace kvstore {

PutResult Store::put(const string& key, const string& value) {
  if (key.empty()) return PutResult::kEmptyKey;
  if (key.size() > kMaxKeySize) return PutResult::kKeyTooLarge;
  if (value.size() > kMaxValueSize) return PutResult::kValueTooLarge;

  unique_lock lock(mutex_);
  auto it = map_.find(key);
  uint64_t version = (it != map_.end()) ? it->second.version + 1 : 1;
  uint64_t write_id = next_write_id_;

  // Durability point: append_put() does not return until the record is
  // fsynced, and put() does not return until append_put() does — so a
  // caller that sends the client its ack only after put() returns never
  // acks a write that isn't already durable.
  if (wal_) wal_->append_put(key, value, version, write_id);

  Entry& entry = map_[key];
  entry.value = value;
  entry.version = version;
  entry.write_id = write_id;
  entry.tombstone = false;
  next_write_id_ = write_id + 1;
  return PutResult::kOk;
}

optional<string> Store::get(const string& key) const {
  shared_lock lock(mutex_);
  auto it = map_.find(key);
  if (it == map_.end() || it->second.tombstone) return nullopt;
  return it->second.value;
}

optional<Entry> Store::peek(const string& key) const {
  shared_lock lock(mutex_);
  auto it = map_.find(key);
  if (it == map_.end()) return nullopt;
  return it->second;
}

DeleteResult Store::del(const string& key) {
  unique_lock lock(mutex_);
  auto it = map_.find(key);
  if (it == map_.end() || it->second.tombstone) return DeleteResult::kNotFound;

  uint64_t version = it->second.version + 1;
  uint64_t write_id = next_write_id_;

  // Same durability point as put(): fsynced before del() returns.
  if (wal_) wal_->append_delete(key, version, write_id);

  Entry& entry = it->second;
  entry.tombstone = true;
  entry.value.clear();
  entry.version = version;
  entry.write_id = write_id;
  next_write_id_ = write_id + 1;
  return DeleteResult::kOk;
}

size_t Store::size() const {
  shared_lock lock(mutex_);
  return map_.size();
}

void Store::apply_recovered(const Record& record) {
  unique_lock lock(mutex_);
  // Already reflected in a snapshot loaded via load_snapshot() — the WAL
  // segment(s) covering it may not even have been deleted yet (truncation
  // only removes segments that are *fully* covered), so this check, not
  // just truncate_before(), is what guarantees a snapshotted mutation is
  // never re-applied on top of itself.
  if (record.write_id <= snapshot_boundary_write_id_) return;

  Entry& entry = map_[record.key];
  entry.version = record.version;
  entry.write_id = record.write_id;
  entry.tombstone = (record.type == RecordType::kDelete);
  entry.value = entry.tombstone ? string() : record.value;
  if (record.write_id >= next_write_id_) next_write_id_ = record.write_id + 1;
}

size_t Store::recover_from_wal() {
  if (!wal_) return 0;
  return wal_->recover([this](const Record& record) { apply_recovered(record); });
}

size_t Store::load_snapshot(const string& snapshot_dir) {
  optional<LoadedSnapshot> loaded = load_latest(snapshot_dir);
  if (!loaded) return 0;

  unique_lock lock(mutex_);
  for (const auto& e : loaded->entries) {
    Entry& entry = map_[e.key];
    entry.value = e.value;
    entry.version = e.version;
    entry.write_id = e.write_id;
    entry.tombstone = e.tombstone;
  }
  if (loaded->boundary_write_id + 1 > next_write_id_) {
    next_write_id_ = loaded->boundary_write_id + 1;
  }
  snapshot_boundary_write_id_ = loaded->boundary_write_id;
  return loaded->entries.size();
}

pair<vector<SnapshotEntry>, uint64_t> Store::snapshot_entries() const {
  // A shared (read) lock is enough: it blocks concurrent writers for the
  // duration of the copy (giving a true point-in-time view) without
  // blocking concurrent readers. A write landing in the gap between this
  // block ending and a caller's subsequent disk I/O (take_snapshot()'s
  // force_rotate()/truncate_before(), or rebalance_to()'s copy into a
  // target shard) is still handled correctly — see the comment on
  // take_snapshot() in docs/design-decisions.md.
  shared_lock lock(mutex_);
  vector<SnapshotEntry> entries;
  entries.reserve(map_.size());
  for (const auto& [key, entry] : map_) {
    entries.push_back(
        SnapshotEntry{key, entry.value, entry.version, entry.write_id, entry.tombstone});
  }
  return {move(entries), next_write_id_ - 1};
}

void Store::apply_migrated(const Record& record) {
  unique_lock lock(mutex_);
  auto it = map_.find(record.key);
  // Already have this key's data at least as fresh — a concurrent live
  // write forwarded here ahead of the bulk copy reaching the same key, or a
  // redelivery. See this method's doc comment in store.h for why this is a
  // per-key comparison rather than apply_replicated()'s store-wide dedup.
  if (it != map_.end() && it->second.write_id >= record.write_id) return;

  Entry& entry = map_[record.key];
  entry.version = record.version;
  entry.write_id = record.write_id;
  entry.tombstone = (record.type == RecordType::kDelete);
  entry.value = entry.tombstone ? string() : record.value;
  if (record.write_id >= next_write_id_) next_write_id_ = record.write_id + 1;
}

void Store::take_snapshot(const string& snapshot_dir) {
  auto [entries, boundary] = snapshot_entries();
  write_snapshot(snapshot_dir, boundary, entries);

  if (wal_) {
    // Close the segment that was active while the snapshot was taken, so
    // it becomes eligible for deletion below instead of staying "active"
    // (and therefore untouchable) forever.
    wal_->force_rotate();
    wal_->truncate_before(boundary);
  }
}

void Store::apply_replicated(const Record& record) {
  unique_lock lock(mutex_);
  // Idempotent skip: this exact record (or a later one) has already been
  // applied. Skipping before the WAL append too is what makes redelivering
  // the same record after a lost ack safe — see docs/design-decisions.md's
  // "Replication Ack Policy".
  if (record.write_id < next_write_id_) return;

  if (wal_) {
    if (record.type == RecordType::kPut) {
      wal_->append_put(record.key, record.value, record.version, record.write_id);
    } else {
      wal_->append_delete(record.key, record.version, record.write_id);
    }
  }

  Entry& entry = map_[record.key];
  entry.version = record.version;
  entry.write_id = record.write_id;
  entry.tombstone = (record.type == RecordType::kDelete);
  entry.value = entry.tombstone ? string() : record.value;
  next_write_id_ = record.write_id + 1;
}

uint64_t Store::last_applied_write_id() const {
  shared_lock lock(mutex_);
  return next_write_id_ - 1;
}

size_t Store::replay_wal_after(uint64_t after_write_id,
                                     const function<void(const Record&)>& apply) const {
  if (!wal_) return 0;
  return wal_->replay_after(after_write_id, apply);
}

}  // namespace kvstore
