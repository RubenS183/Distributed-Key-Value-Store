#include "kv_client.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <stdexcept>

using namespace std;

namespace kvstore {

KvClient::KvClient(const string& host, uint16_t port) {
  fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd_ < 0) throw runtime_error("KvClient: socket() failed");

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
    throw runtime_error("KvClient: invalid host " + host);
  }

  if (::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    throw runtime_error("KvClient: connect() failed");
  }
}

KvClient::~KvClient() {
  if (fd_ >= 0) ::close(fd_);
}

Response KvClient::send(Opcode opcode, const string& key,
                                   const string& value) {
  string payload;
  if (opcode == Opcode::kPut) {
    string key_len_bytes(4, '\0');
    uint32_t key_len = static_cast<uint32_t>(key.size());
    key_len_bytes[0] = static_cast<char>((key_len >> 24) & 0xFF);
    key_len_bytes[1] = static_cast<char>((key_len >> 16) & 0xFF);
    key_len_bytes[2] = static_cast<char>((key_len >> 8) & 0xFF);
    key_len_bytes[3] = static_cast<char>(key_len & 0xFF);
    payload = key_len_bytes + key + value;
  } else {
    payload = key;
  }
  return send_and_wait(static_cast<uint8_t>(opcode), payload);
}

Response KvClient::send_raw(uint8_t opcode, const string& payload) {
  return send_and_wait(opcode, payload);
}

Response KvClient::send_and_wait(uint8_t opcode, const string& payload) {
  uint64_t request_id = next_request_id_++;
  string encoded = encode_frame(opcode, request_id, payload);

  size_t written = 0;
  while (written < encoded.size()) {
    ssize_t w = ::write(fd_, encoded.data() + written, encoded.size() - written);
    if (w <= 0) throw runtime_error("KvClient: write() failed");
    written += static_cast<size_t>(w);
  }

  FrameParser parser;
  Frame frame;
  array<char, 4096> buf;
  while (true) {
    ParseStatus status = parser.next(frame);
    if (status == ParseStatus::kFrame) break;
    if (status == ParseStatus::kError) {
      throw runtime_error("KvClient: malformed response frame");
    }
    ssize_t n = ::read(fd_, buf.data(), buf.size());
    if (n <= 0) {
      throw runtime_error("KvClient: connection closed while waiting for response");
    }
    parser.feed(buf.data(), static_cast<size_t>(n));
  }

  if (frame.request_id != request_id) {
    throw runtime_error("KvClient: response request_id does not match request");
  }

  Response response;
  response.status = static_cast<Status>(frame.type_byte);
  response.request_id = frame.request_id;
  response.payload = frame.payload;
  return response;
}

}  // namespace kvstore
