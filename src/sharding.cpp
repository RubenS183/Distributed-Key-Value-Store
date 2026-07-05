#include "sharding.h"

using namespace std;

namespace kvstore {

uint64_t fnv1a_hash(const string& data) {
  // FNV-1a, 64-bit. Fixed offset basis / prime from the public FNV
  // specification — not a project-specific choice, just the standard
  // constants for this algorithm.
  constexpr uint64_t kOffsetBasis = 0xcbf29ce484222325ULL;
  constexpr uint64_t kPrime = 0x100000001b3ULL;

  uint64_t hash = kOffsetBasis;
  for (unsigned char c : data) {
    hash ^= c;
    hash *= kPrime;
  }
  return hash;
}

ShardId shard_for_key(const string& key, size_t num_shards) {
  return static_cast<ShardId>(fnv1a_hash(key) % num_shards);
}

}  // namespace kvstore
