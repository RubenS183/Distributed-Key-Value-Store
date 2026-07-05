#include "protocol.h"

#include <cstring>

using namespace std;

namespace kvstore {

namespace {

uint32_t read_u32_be(const uint8_t* p) {
  return (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16) |
         (static_cast<uint32_t>(p[2]) << 8) | static_cast<uint32_t>(p[3]);
}

uint64_t read_u64_be(const uint8_t* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v = (v << 8) | p[i];
  return v;
}

void write_u32_be(char* p, uint32_t v) {
  p[0] = static_cast<char>((v >> 24) & 0xFF);
  p[1] = static_cast<char>((v >> 16) & 0xFF);
  p[2] = static_cast<char>((v >> 8) & 0xFF);
  p[3] = static_cast<char>(v & 0xFF);
}

void write_u64_be(char* p, uint64_t v) {
  for (int i = 0; i < 8; ++i) p[i] = static_cast<char>((v >> (8 * (7 - i))) & 0xFF);
}

void append_u32_be(string& out, uint32_t v) {
  out.push_back(static_cast<char>((v >> 24) & 0xFF));
  out.push_back(static_cast<char>((v >> 16) & 0xFF));
  out.push_back(static_cast<char>((v >> 8) & 0xFF));
  out.push_back(static_cast<char>(v & 0xFF));
}

void append_u64_be(string& out, uint64_t v) {
  for (int i = 7; i >= 0; --i) out.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}

}  // namespace

void FrameParser::feed(const char* data, size_t len) { buf_.append(data, len); }

ParseStatus FrameParser::next(Frame& out) {
  if (errored_) return ParseStatus::kError;
  if (buf_.size() < kLengthFieldSize) return ParseStatus::kNeedMore;

  uint32_t length = read_u32_be(reinterpret_cast<const uint8_t*>(buf_.data()));
  // A legal frame always has at least the fixed opcode+request_id header,
  // and can never exceed what a PUT of max key+value could produce.
  if (length < kFixedHeaderAfterLength || length > kMaxFrameLen) {
    errored_ = true;
    return ParseStatus::kError;
  }

  size_t total_frame_size = kLengthFieldSize + length;
  if (buf_.size() < total_frame_size) return ParseStatus::kNeedMore;

  const uint8_t* p = reinterpret_cast<const uint8_t*>(buf_.data()) + kLengthFieldSize;
  out.type_byte = p[0];
  out.request_id = read_u64_be(p + kTypeByteSize);
  size_t payload_len = length - kFixedHeaderAfterLength;
  out.payload.assign(reinterpret_cast<const char*>(p + kFixedHeaderAfterLength), payload_len);

  buf_.erase(0, total_frame_size);
  return ParseStatus::kFrame;
}

string encode_frame(uint8_t type_byte, uint64_t request_id, const string& payload) {
  uint32_t length = static_cast<uint32_t>(kFixedHeaderAfterLength + payload.size());
  string out(kLengthFieldSize + kFixedHeaderAfterLength + payload.size(), '\0');
  write_u32_be(&out[0], length);
  out[kLengthFieldSize] = static_cast<char>(type_byte);
  write_u64_be(&out[kLengthFieldSize + kTypeByteSize], request_id);
  memcpy(&out[kLengthFieldSize + kFixedHeaderAfterLength], payload.data(), payload.size());
  return out;
}

bool parse_request(const Frame& frame, Request& out) {
  out.request_id = frame.request_id;

  switch (static_cast<Opcode>(frame.type_byte)) {
    case Opcode::kPut: {
      if (frame.payload.size() < 4) return false;
      uint32_t key_len = read_u32_be(reinterpret_cast<const uint8_t*>(frame.payload.data()));
      if (4 + static_cast<size_t>(key_len) > frame.payload.size()) return false;
      out.opcode = Opcode::kPut;
      out.key = frame.payload.substr(4, key_len);
      out.value = frame.payload.substr(4 + key_len);
      return true;
    }
    case Opcode::kGet:
      out.opcode = Opcode::kGet;
      out.key = frame.payload;
      out.value.clear();
      return true;
    case Opcode::kDelete:
      out.opcode = Opcode::kDelete;
      out.key = frame.payload;
      out.value.clear();
      return true;
    case Opcode::kReplicate: {
      // [8B term][1B type][8B write_id][8B version][4B key_len][key][value]
      constexpr size_t kFixed = 8 + 1 + 8 + 8 + 4;
      if (frame.payload.size() < kFixed) return false;
      const uint8_t* p = reinterpret_cast<const uint8_t*>(frame.payload.data());
      uint8_t type_byte = p[8];
      if (type_byte != static_cast<uint8_t>(RecordType::kPut) &&
          type_byte != static_cast<uint8_t>(RecordType::kDelete)) {
        return false;
      }
      uint32_t key_len = read_u32_be(p + 25);
      if (kFixed + static_cast<size_t>(key_len) > frame.payload.size()) return false;

      out.opcode = Opcode::kReplicate;
      out.term = read_u64_be(p);
      out.record_type = static_cast<RecordType>(type_byte);
      out.write_id = read_u64_be(p + 9);
      out.version = read_u64_be(p + 17);
      out.key = frame.payload.substr(kFixed, key_len);
      out.value = frame.payload.substr(kFixed + key_len);
      return true;
    }
    case Opcode::kCatchupQuery: {
      // [8B term][8B shard_id], no key/value.
      if (frame.payload.size() != 16) return false;
      const uint8_t* p = reinterpret_cast<const uint8_t*>(frame.payload.data());
      out.opcode = Opcode::kCatchupQuery;
      out.term = read_u64_be(p);
      out.shard_id = static_cast<ShardId>(read_u64_be(p + 8));
      return true;
    }
    case Opcode::kHeartbeat: {
      // [8B term], no key/value/shard.
      if (frame.payload.size() != 8) return false;
      out.opcode = Opcode::kHeartbeat;
      out.term = read_u64_be(reinterpret_cast<const uint8_t*>(frame.payload.data()));
      return true;
    }
    case Opcode::kElectionQuery: {
      // No payload at all — a pure read of the receiver's own state.
      if (!frame.payload.empty()) return false;
      out.opcode = Opcode::kElectionQuery;
      return true;
    }
  }
  return false;  // unknown opcode
}

string encode_response(const Response& response) {
  return encode_frame(static_cast<uint8_t>(response.status), response.request_id,
                       response.payload);
}

string encode_replicate_payload(uint64_t term, const Record& record) {
  string payload;
  payload.reserve(8 + 1 + 8 + 8 + 4 + record.key.size() + record.value.size());
  append_u64_be(payload, term);
  payload.push_back(static_cast<char>(record.type));
  append_u64_be(payload, record.write_id);
  append_u64_be(payload, record.version);
  append_u32_be(payload, static_cast<uint32_t>(record.key.size()));
  payload += record.key;
  payload += record.value;
  return payload;
}

string encode_catchup_query_payload(uint64_t term, ShardId shard_id) {
  string payload;
  payload.reserve(16);
  append_u64_be(payload, term);
  append_u64_be(payload, static_cast<uint64_t>(shard_id));
  return payload;
}

string encode_last_applied_payload(uint64_t last_applied_write_id) {
  string payload;
  append_u64_be(payload, last_applied_write_id);
  return payload;
}

uint64_t decode_last_applied_payload(const string& payload) {
  if (payload.size() < 8) return 0;
  return read_u64_be(reinterpret_cast<const uint8_t*>(payload.data()));
}

string encode_epoch_payload(uint64_t epoch) {
  string payload;
  append_u64_be(payload, epoch);
  return payload;
}

uint64_t decode_epoch_payload(const string& payload) {
  if (payload.size() < 8) return 0;
  return read_u64_be(reinterpret_cast<const uint8_t*>(payload.data()));
}

string encode_election_response_payload(uint64_t epoch, uint64_t total_write_id) {
  string payload;
  payload.reserve(16);
  append_u64_be(payload, epoch);
  append_u64_be(payload, total_write_id);
  return payload;
}

void decode_election_response_payload(const string& payload, uint64_t& epoch,
                                       uint64_t& total_write_id) {
  if (payload.size() < 16) {
    epoch = 0;
    total_write_id = 0;
    return;
  }
  const uint8_t* p = reinterpret_cast<const uint8_t*>(payload.data());
  epoch = read_u64_be(p);
  total_write_id = read_u64_be(p + 8);
}

}  // namespace kvstore
