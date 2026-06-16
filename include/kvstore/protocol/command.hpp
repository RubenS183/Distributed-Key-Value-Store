#pragma once

namespace kvstore::protocol {

enum class CommandType {
  Set,
  Get,
  Delete,
  Expire,
  Ttl,
  Unknown,
};

} // namespace kvstore::protocol
