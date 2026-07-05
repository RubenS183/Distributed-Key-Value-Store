#include <catch2/catch_test_macros.hpp>

#include "protocol.h"

using namespace std;
using namespace kvstore;

namespace {

string encode_put_payload(const string& key, const string& value) {
  string payload(4, '\0');
  uint32_t key_len = static_cast<uint32_t>(key.size());
  payload[0] = static_cast<char>((key_len >> 24) & 0xFF);
  payload[1] = static_cast<char>((key_len >> 16) & 0xFF);
  payload[2] = static_cast<char>((key_len >> 8) & 0xFF);
  payload[3] = static_cast<char>(key_len & 0xFF);
  return payload + key + value;
}

}  // namespace

TEST_CASE("round-trip encode/decode a PUT frame", "[protocol]") {
  string payload = encode_put_payload("key1", "value1");
  string wire = encode_frame(static_cast<uint8_t>(Opcode::kPut), 42, payload);

  FrameParser parser;
  parser.feed(wire);
  Frame frame;
  REQUIRE(parser.next(frame) == ParseStatus::kFrame);
  REQUIRE(frame.type_byte == static_cast<uint8_t>(Opcode::kPut));
  REQUIRE(frame.request_id == 42);
  REQUIRE(frame.payload == payload);

  Request request;
  REQUIRE(parse_request(frame, request));
  REQUIRE(request.opcode == Opcode::kPut);
  REQUIRE(request.key == "key1");
  REQUIRE(request.value == "value1");
}

TEST_CASE("round-trip encode/decode a GET frame", "[protocol]") {
  string wire = encode_frame(static_cast<uint8_t>(Opcode::kGet), 7, "mykey");
  FrameParser parser;
  parser.feed(wire);
  Frame frame;
  REQUIRE(parser.next(frame) == ParseStatus::kFrame);

  Request request;
  REQUIRE(parse_request(frame, request));
  REQUIRE(request.opcode == Opcode::kGet);
  REQUIRE(request.key == "mykey");
  REQUIRE(request.value.empty());
}

TEST_CASE("round-trip encode/decode a DELETE frame", "[protocol]") {
  string wire = encode_frame(static_cast<uint8_t>(Opcode::kDelete), 9, "mykey");
  FrameParser parser;
  parser.feed(wire);
  Frame frame;
  REQUIRE(parser.next(frame) == ParseStatus::kFrame);

  Request request;
  REQUIRE(parse_request(frame, request));
  REQUIRE(request.opcode == Opcode::kDelete);
  REQUIRE(request.key == "mykey");
}

TEST_CASE("round-trip encode/decode a response", "[protocol]") {
  Response response;
  response.status = Status::kOk;
  response.request_id = 123;
  response.payload = "hello";

  string wire = encode_response(response);
  FrameParser parser;
  parser.feed(wire);
  Frame frame;
  REQUIRE(parser.next(frame) == ParseStatus::kFrame);
  REQUIRE(static_cast<Status>(frame.type_byte) == Status::kOk);
  REQUIRE(frame.request_id == 123);
  REQUIRE(frame.payload == "hello");
}

TEST_CASE("partial reads: parser waits until the header is complete", "[protocol]") {
  string wire = encode_frame(static_cast<uint8_t>(Opcode::kGet), 1, "k");
  FrameParser parser;
  Frame frame;

  // Feed one byte at a time; a frame should only become available once the
  // very last byte has arrived.
  for (size_t i = 0; i + 1 < wire.size(); ++i) {
    parser.feed(&wire[i], 1);
    REQUIRE(parser.next(frame) == ParseStatus::kNeedMore);
  }
  parser.feed(&wire[wire.size() - 1], 1);
  REQUIRE(parser.next(frame) == ParseStatus::kFrame);
  REQUIRE(frame.request_id == 1);
}

TEST_CASE("partial reads: payload arrives in a separate chunk from the header", "[protocol]") {
  string wire = encode_frame(static_cast<uint8_t>(Opcode::kGet), 2, "longkey");
  FrameParser parser;
  Frame frame;

  // Fixed header is length(4) + opcode(1) + request_id(8) = 13 bytes.
  parser.feed(wire.data(), 13);
  REQUIRE(parser.next(frame) == ParseStatus::kNeedMore);

  parser.feed(wire.data() + 13, wire.size() - 13);
  REQUIRE(parser.next(frame) == ParseStatus::kFrame);
  REQUIRE(frame.payload == "longkey");
}

TEST_CASE("message boundaries: two frames delivered in a single chunk", "[protocol]") {
  string wire1 = encode_frame(static_cast<uint8_t>(Opcode::kGet), 1, "a");
  string wire2 = encode_frame(static_cast<uint8_t>(Opcode::kGet), 2, "b");

  FrameParser parser;
  parser.feed(wire1 + wire2);

  Frame frame1;
  Frame frame2;
  REQUIRE(parser.next(frame1) == ParseStatus::kFrame);
  REQUIRE(frame1.request_id == 1);
  REQUIRE(frame1.payload == "a");

  REQUIRE(parser.next(frame2) == ParseStatus::kFrame);
  REQUIRE(frame2.request_id == 2);
  REQUIRE(frame2.payload == "b");
}

TEST_CASE("message boundaries: a frame split across two feeds, followed by a full frame",
          "[protocol]") {
  string wire1 = encode_frame(static_cast<uint8_t>(Opcode::kGet), 1, "a");
  string wire2 = encode_frame(static_cast<uint8_t>(Opcode::kGet), 2, "b");
  string combined = wire1 + wire2;

  FrameParser parser;
  Frame frame;

  // Split in the middle of the first frame.
  parser.feed(combined.data(), wire1.size() - 2);
  REQUIRE(parser.next(frame) == ParseStatus::kNeedMore);

  parser.feed(combined.data() + wire1.size() - 2, combined.size() - (wire1.size() - 2));
  REQUIRE(parser.next(frame) == ParseStatus::kFrame);
  REQUIRE(frame.request_id == 1);
  REQUIRE(parser.next(frame) == ParseStatus::kFrame);
  REQUIRE(frame.request_id == 2);
}

TEST_CASE("malformed: length field below the fixed header size is an error", "[protocol]") {
  string wire(4, '\0');
  wire[3] = 3;  // length = 3, less than the fixed 9-byte opcode+request_id header
  FrameParser parser;
  parser.feed(wire);
  Frame frame;
  REQUIRE(parser.next(frame) == ParseStatus::kError);
}

TEST_CASE("malformed: length field larger than the max frame size is an error", "[protocol]") {
  string wire(4, '\0');
  wire[0] = static_cast<char>(0xFF);
  wire[1] = static_cast<char>(0xFF);
  wire[2] = static_cast<char>(0xFF);
  wire[3] = static_cast<char>(0xFF);
  FrameParser parser;
  parser.feed(wire);
  Frame frame;
  REQUIRE(parser.next(frame) == ParseStatus::kError);
}

TEST_CASE("malformed: unknown opcode is rejected by parse_request", "[protocol]") {
  string wire = encode_frame(0x7F, 1, "somekey");
  FrameParser parser;
  parser.feed(wire);
  Frame frame;
  REQUIRE(parser.next(frame) == ParseStatus::kFrame);

  Request request;
  REQUIRE_FALSE(parse_request(frame, request));
}

TEST_CASE("malformed: PUT key_len overruns the payload", "[protocol]") {
  string payload(4, '\0');
  payload[3] = static_cast<char>(100);  // claims a 100-byte key but payload has none
  string wire = encode_frame(static_cast<uint8_t>(Opcode::kPut), 1, payload);

  FrameParser parser;
  parser.feed(wire);
  Frame frame;
  REQUIRE(parser.next(frame) == ParseStatus::kFrame);

  Request request;
  REQUIRE_FALSE(parse_request(frame, request));
}

TEST_CASE("malformed: PUT payload too short to even hold key_len", "[protocol]") {
  string wire = encode_frame(static_cast<uint8_t>(Opcode::kPut), 1, "ab");  // only 2 bytes
  FrameParser parser;
  parser.feed(wire);
  Frame frame;
  REQUIRE(parser.next(frame) == ParseStatus::kFrame);

  Request request;
  REQUIRE_FALSE(parse_request(frame, request));
}

// --- Replication (stage 6): REPLICATE / CATCHUP_QUERY frame round-trips ---

TEST_CASE("round-trip encode/decode a REPLICATE frame (PUT record)", "[protocol][replication]") {
  Record record{RecordType::kPut, /*write_id=*/42, /*version=*/3, "somekey", "someval"};
  string payload = encode_replicate_payload(7, record);
  string wire = encode_frame(static_cast<uint8_t>(Opcode::kReplicate), 1, payload);

  FrameParser parser;
  parser.feed(wire);
  Frame frame;
  REQUIRE(parser.next(frame) == ParseStatus::kFrame);

  Request request;
  REQUIRE(parse_request(frame, request));
  REQUIRE(request.opcode == Opcode::kReplicate);
  REQUIRE(request.term == 7);
  REQUIRE(request.record_type == RecordType::kPut);
  REQUIRE(request.write_id == 42);
  REQUIRE(request.version == 3);
  REQUIRE(request.key == "somekey");
  REQUIRE(request.value == "someval");
}

TEST_CASE("round-trip encode/decode a REPLICATE frame (DELETE record, empty value)",
          "[protocol][replication]") {
  Record record{RecordType::kDelete, 5, 2, "somekey", ""};
  string payload = encode_replicate_payload(1, record);
  string wire = encode_frame(static_cast<uint8_t>(Opcode::kReplicate), 2, payload);

  FrameParser parser;
  parser.feed(wire);
  Frame frame;
  REQUIRE(parser.next(frame) == ParseStatus::kFrame);

  Request request;
  REQUIRE(parse_request(frame, request));
  REQUIRE(request.record_type == RecordType::kDelete);
  REQUIRE(request.write_id == 5);
  REQUIRE(request.value.empty());
}

TEST_CASE("round-trip encode/decode a CATCHUP_QUERY frame", "[protocol][replication]") {
  string payload = encode_catchup_query_payload(9, /*shard_id=*/3);
  string wire = encode_frame(static_cast<uint8_t>(Opcode::kCatchupQuery), 5, payload);

  FrameParser parser;
  parser.feed(wire);
  Frame frame;
  REQUIRE(parser.next(frame) == ParseStatus::kFrame);

  Request request;
  REQUIRE(parse_request(frame, request));
  REQUIRE(request.opcode == Opcode::kCatchupQuery);
  REQUIRE(request.term == 9);
  REQUIRE(request.shard_id == 3);
}

TEST_CASE("round-trip encode/decode a last_applied_write_id payload", "[protocol][replication]") {
  string payload = encode_last_applied_payload(123456789);
  REQUIRE(decode_last_applied_payload(payload) == 123456789);
}

TEST_CASE("malformed: REPLICATE payload too short to hold the fixed header", "[protocol][replication]") {
  string wire = encode_frame(static_cast<uint8_t>(Opcode::kReplicate), 1, "short");
  FrameParser parser;
  parser.feed(wire);
  Frame frame;
  REQUIRE(parser.next(frame) == ParseStatus::kFrame);

  Request request;
  REQUIRE_FALSE(parse_request(frame, request));
}

TEST_CASE("malformed: CATCHUP_QUERY payload with wrong size is rejected", "[protocol][replication]") {
  string wire = encode_frame(static_cast<uint8_t>(Opcode::kCatchupQuery), 1, "tooshort");
  FrameParser parser;
  parser.feed(wire);
  Frame frame;
  REQUIRE(parser.next(frame) == ParseStatus::kFrame);

  Request request;
  REQUIRE_FALSE(parse_request(frame, request));
}
