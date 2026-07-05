#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

using namespace std;

namespace kvstore {

using ShardId = size_t;

// shard_id = hash(key) % num_shards. See docs/design-decisions.md's
// "Sharding" section for hash- vs range-partitioning rationale.
//
// Uses a hand-rolled FNV-1a (not hash<string>) because shard
// assignment must be stable across process restarts forever: each shard's
// WAL/snapshot live at a fixed directory keyed by shard id, so if a key ever
// hashed to a different shard than it did when it was written, that key's
// data would silently appear "missing" (it's still on disk, just under the
// wrong shard's directory). hash<string>'s implementation is
// unspecified by the standard and can differ across standard library
// versions/vendors — not safe to rely on for an on-disk routing decision
// that must outlive a single process. FNV-1a is a fixed, fully-specified
// algorithm this project controls end to end.
uint64_t fnv1a_hash(const string& data);

ShardId shard_for_key(const string& key, size_t num_shards);

// Which node owns each shard. Single-node deployment: every shard is owned
// by this process — there is no cluster membership, no multi-node ownership,
// and no rebalancing. This class exists now only to name the seam a later
// replication/clustering stage would extend (shard_id -> node), per this
// phase's explicit request; it is deliberately not more than that until a
// second node actually exists. See docs/limitations.md.
class ShardMap {
 public:
  explicit ShardMap(size_t num_shards) : num_shards_(num_shards) {}

  size_t num_shards() const { return num_shards_; }

  // Every shard's owning node today. Not parameterized by ShardId because
  // there is only one possible answer until a second node exists.
  static constexpr const char* kLocalNode = "local";
  const char* owning_node(ShardId /*id*/) const { return kLocalNode; }

 private:
  size_t num_shards_;
};

}  // namespace kvstore
