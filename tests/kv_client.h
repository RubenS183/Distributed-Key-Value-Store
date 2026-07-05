#pragma once

#include <cstdint>
#include <string>

#include "protocol.h"

using namespace std;

namespace kvstore {

// Minimal blocking TCP client that speaks the kvstore wire protocol, used to
// drive the server in tests. Not a user-facing client (YAGNI) — just enough
// to send one request and read one response at a time.
class KvClient {
 public:
  KvClient(const string& host, uint16_t port);
  ~KvClient();

  KvClient(const KvClient&) = delete;
  KvClient& operator=(const KvClient&) = delete;

  // Sends a request built from opcode/key/value with a freshly generated
  // request_id, blocks for exactly one response, and verifies the response's
  // request_id echoes what was sent. Throws runtime_error on any socket
  // failure or protocol mismatch.
  Response send(Opcode opcode, const string& key, const string& value = "");

  // Sends `payload` verbatim as the frame's payload under `opcode`, with a
  // freshly generated request_id — for opcodes whose payload shape isn't
  // PUT/GET/DELETE's (e.g. REPLICATE/CATCHUP_QUERY), which send() doesn't
  // know how to build. Otherwise identical to send().
  Response send_raw(uint8_t opcode, const string& payload);

 private:
  Response send_and_wait(uint8_t opcode, const string& payload);

  int fd_ = -1;
  uint64_t next_request_id_ = 1;
};

}  // namespace kvstore
