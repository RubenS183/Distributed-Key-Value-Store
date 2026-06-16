#pragma once

namespace kvstore::cache {

enum class EvictionPolicy {
  None,
  Lru,
  Ttl,
};

} // namespace kvstore::cache
