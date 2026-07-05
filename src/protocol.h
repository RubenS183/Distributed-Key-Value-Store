#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "sharding.h"
#include "store.h"

using namespace std;

namespace kvstore {

// Wire format (see docs/architecture.md for the full spec):
//   [4B length, big-endian][1B opcode/status][8B request_id][payload]
// `length` counts every byte after the length field itself.
inline constexpr size_t kLengthFieldSize = 4;
inline constexpr size_t kTypeByteSize = 1;
inline constexpr size_t kRequestIdSize = 8;
inline constexpr size_t kFixedHeaderAfterLength = kTypeByteSize + kRequestIdSize;  // 9

// Largest fixed (non-key/value) overhead any request payload shape needs.
// REPLICATE's [8B term][1B type][8B write_id][8B version][4B key_len] = 29
// bytes is larger than PUT's plain 4-byte key_len prefix, so it — not PUT —
// sets the bound. See docs/architecture.md's "Replication" wire format.
inline constexpr size_t kMaxPayloadFixedOverhead = 8 + 1 + 8 + 8 + 4;  // 29
inline constexpr size_t kMaxPayloadLen = kMaxPayloadFixedOverhead + kMaxKeySize + kMaxValueSize;
// Largest legal value of the on-wire `length` field. Any frame claiming more
// is rejected before any payload buffer is allocated.
inline constexpr uint32_t kMaxFrameLen =
    static_cast<uint32_t>(kFixedHeaderAfterLength + kMaxPayloadLen);

enum class Opcode : uint8_t {
  kPut = 0x01,
  kGet = 0x02,
  kDelete = 0x03,
  // Stage 6 (replication): a leader forwarding one WAL record to a follower,
  // or a follower's reconnect-time query for how far behind it is. See
  // docs/architecture.md's "Replication" section.
  kReplicate = 0x04,
  kCatchupQuery = 0x05,
  // Stage 7 (failover): a liveness probe carrying the sender's epoch (sent
  // by whichever node currently believes it is leader to every peer), and a
  // candidate's query for a peer's epoch/log position during an election.
  // See docs/architecture.md's "Failover" section.
  kHeartbeat = 0x06,
  kElectionQuery = 0x07,
};
enum class Status : uint8_t {
  kOk = 0x00,
  kNotFound = 0x01,
  kBadRequest = 0x02,
  // A client sent PUT/GET/DELETE to a node running in the follower role.
  // Follower reads are disallowed entirely in this phase (see
  // docs/design-decisions.md's "Replication Ack Policy") rather than served
  // stale, so this is returned instead of executing the request.
  kNotLeader = 0x03,
  // Stage 7 (failover): the sender's epoch (REPLICATE/CATCHUP_QUERY/
  // HEARTBEAT's term field) is lower than this node's current epoch — the
  // sender is a stale leader from a previous epoch and must step down. The
  // response payload carries this node's current epoch (see
  // encode_epoch_payload()) so the stale sender learns the correct value to
  // adopt instead of guessing. See docs/design-decisions.md's "Failover"
  // section — this is the split-brain guard.
  kStaleTerm = 0x04,
};

// A fully-decoded frame, before request-level interpretation: just the
// type byte (opcode or status, depending on direction), request id, and
// raw payload bytes.
struct Frame {
  uint8_t type_byte = 0;
  uint64_t request_id = 0;
  string payload;
};

struct Request {
  Opcode opcode = Opcode::kGet;
  uint64_t request_id = 0;
  string key;
  string value;  // populated only for Put

  // Populated only for Replicate/CatchupQuery (stage 6 replication) — see
  // docs/architecture.md's "Replication" section for each field's meaning.
  uint64_t term = 0;
  RecordType record_type = RecordType::kPut;  // Replicate only
  uint64_t write_id = 0;                      // Replicate only
  uint64_t version = 0;                       // Replicate only
  ShardId shard_id = 0;                       // CatchupQuery only
};

struct Response {
  Status status = Status::kOk;
  uint64_t request_id = 0;
  string payload;  // value bytes for a successful Get; empty otherwise
};

enum class ParseStatus { kNeedMore, kFrame, kError };

// Incrementally decodes frames from a byte stream. Bytes can arrive in any
// chunking (partial header, partial payload, multiple frames at once) —
// feed() buffers raw bytes, next() extracts one complete frame at a time.
class FrameParser {
 public:
  void feed(const char* data, size_t len);
  void feed(const string& data) { feed(data.data(), data.size()); }

  // Extracts the next complete frame, if enough bytes are buffered.
  // Once kError is returned, the connection must be closed — the parser
  // gives up resynchronizing on malformed input rather than guessing.
  ParseStatus next(Frame& out);

 private:
  string buf_;
  bool errored_ = false;
};

// Encodes a complete frame (length prefix + type byte + request_id + payload).
string encode_frame(uint8_t type_byte, uint64_t request_id, const string& payload);

// Interprets a decoded Frame's type_byte as a request Opcode and splits its
// payload into key/value. Returns false if the opcode is unknown or the
// payload doesn't match the shape required by that opcode (e.g. a PUT whose
// inner key_len prefix overruns the payload).
bool parse_request(const Frame& frame, Request& out);

// Encodes a Response as wire bytes ready to write to the socket.
string encode_response(const Response& response);

// --- Replication (stage 6) wire helpers ---
//
// REPLICATE payload: [8B term][1B record type][8B write_id][8B version]
// [4B key_len][key bytes][value bytes]. CATCHUP_QUERY payload: [8B term]
// [8B shard_id]. Both request payloads are built by the leader's
// ReplicationLink and decoded by parse_request() on the follower. The
// CATCHUP_QUERY response reuses Response::payload (like GET already does
// for value bytes) to carry the follower's last_applied_write_id as an 8B
// big-endian integer.
string encode_replicate_payload(uint64_t term, const Record& record);
string encode_catchup_query_payload(uint64_t term, ShardId shard_id);
string encode_last_applied_payload(uint64_t last_applied_write_id);
uint64_t decode_last_applied_payload(const string& payload);

// --- Failover (stage 7) wire helpers ---
//
// HEARTBEAT payload: [8B term], no key/value — the sender's current epoch.
// A rejected (kStaleTerm) response to HEARTBEAT/REPLICATE/CATCHUP_QUERY
// reuses Response::payload for the receiver's current epoch, same 8-byte
// shape as encode_epoch_payload() below (deliberately not reusing
// encode_last_applied_payload(), even though the wire shape is identical —
// see docs/design-decisions.md's "Failover" section for why this uses its
// own name). ELECTION_QUERY has no request payload (a pure read of the
// receiver's state); its response carries the receiver's own
// [epoch, total committed write_id across every shard] pair.
string encode_epoch_payload(uint64_t epoch);
uint64_t decode_epoch_payload(const string& payload);
string encode_election_response_payload(uint64_t epoch, uint64_t total_write_id);
void decode_election_response_payload(const string& payload, uint64_t& epoch,
                                       uint64_t& total_write_id);

}  // namespace kvstore
