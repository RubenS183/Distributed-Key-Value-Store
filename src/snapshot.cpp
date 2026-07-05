#include "snapshot.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <stdexcept>

using namespace std;

namespace kvstore {

namespace fs = filesystem;

namespace {

constexpr char kMagic[4] = {'K', 'V', 'S', 'N'};
constexpr uint8_t kFormatVersion = 1;
// magic(4) + format_version(1) + boundary_write_id(8) + entry_count(8).
constexpr size_t kHeaderSize = 4 + 1 + 8 + 8;
constexpr const char* kFinalName = "snapshot.bin";
constexpr const char* kTempName = "snapshot.tmp";

void append_u32_be(string& out, uint32_t v) {
  out.push_back(static_cast<char>((v >> 24) & 0xFF));
  out.push_back(static_cast<char>((v >> 16) & 0xFF));
  out.push_back(static_cast<char>((v >> 8) & 0xFF));
  out.push_back(static_cast<char>(v & 0xFF));
}

void append_u64_be(string& out, uint64_t v) {
  for (int i = 7; i >= 0; --i) out.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}

uint32_t read_u32_be(const char* p) {
  const auto* u = reinterpret_cast<const unsigned char*>(p);
  return (static_cast<uint32_t>(u[0]) << 24) | (static_cast<uint32_t>(u[1]) << 16) |
         (static_cast<uint32_t>(u[2]) << 8) | static_cast<uint32_t>(u[3]);
}

uint64_t read_u64_be(const char* p) {
  const auto* u = reinterpret_cast<const unsigned char*>(p);
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v = (v << 8) | u[i];
  return v;
}

// fsync()ing the directory itself (not just the file) is what makes the
// rename() below durable: without it, a crash right after rename() returns
// could, on some filesystems, leave the directory entry pointing at the old
// file (or nothing) even though rename() already reported success.
void fsync_dir(const string& dir) {
  int fd = ::open(dir.c_str(), O_RDONLY);
  if (fd < 0) {
    throw runtime_error("snapshot: failed to open directory '" + dir +
                              "' for fsync: " + strerror(errno));
  }
  int rc = ::fsync(fd);
  ::close(fd);
  if (rc != 0) {
    throw runtime_error("snapshot: fsync of directory '" + dir +
                              "' failed: " + strerror(errno));
  }
}

}  // namespace

void write_snapshot(const string& dir, uint64_t boundary_write_id,
                     const vector<SnapshotEntry>& entries) {
  error_code ec;
  fs::create_directories(dir, ec);
  if (ec) {
    throw runtime_error("snapshot: failed to create directory '" + dir + "': " + ec.message());
  }

  // Whole-file layout: [4B magic]["KVSN"][1B format_version][8B
  // boundary_write_id][8B entry_count], then entry_count entries of
  // [4B key_len][4B value_len][8B version][8B write_id][1B tombstone]
  // [key bytes][value bytes]. See docs/architecture.md's "Snapshot Format".
  string buf;
  buf.reserve(kHeaderSize + entries.size() * 32);
  buf.append(kMagic, sizeof(kMagic));
  buf.push_back(static_cast<char>(kFormatVersion));
  append_u64_be(buf, boundary_write_id);
  append_u64_be(buf, static_cast<uint64_t>(entries.size()));

  for (const auto& e : entries) {
    append_u32_be(buf, static_cast<uint32_t>(e.key.size()));
    append_u32_be(buf, static_cast<uint32_t>(e.value.size()));
    append_u64_be(buf, e.version);
    append_u64_be(buf, e.write_id);
    buf.push_back(e.tombstone ? 1 : 0);
    buf += e.key;
    buf += e.value;
  }

  string tmp_path = dir + "/" + kTempName;
  string final_path = dir + "/" + kFinalName;

  int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) {
    throw runtime_error("snapshot: failed to open '" + tmp_path + "': " + strerror(errno));
  }

  size_t written = 0;
  while (written < buf.size()) {
    ssize_t w = ::write(fd, buf.data() + written, buf.size() - written);
    if (w < 0) {
      ::close(fd);
      throw runtime_error(string("snapshot: write failed: ") + strerror(errno));
    }
    written += static_cast<size_t>(w);
  }

  // Durability point: the temp file must be fully fsynced before it's
  // renamed into place, or the rename could publish a file whose bytes
  // aren't actually on disk yet.
  if (::fsync(fd) != 0) {
    ::close(fd);
    throw runtime_error(string("snapshot: fsync failed: ") + strerror(errno));
  }
  ::close(fd);

  // Same directory, so this is a single atomic rename on any POSIX
  // filesystem — a reader (load_latest) never observes a partially written
  // snapshot.bin, only the previous complete one or the new complete one.
  fs::rename(tmp_path, final_path, ec);
  if (ec) {
    throw runtime_error("snapshot: rename '" + tmp_path + "' -> '" + final_path +
                              "' failed: " + ec.message());
  }

  fsync_dir(dir);
}

optional<LoadedSnapshot> load_latest(const string& dir) {
  string final_path = dir + "/" + kFinalName;
  if (!fs::exists(final_path)) return nullopt;

  ifstream in(final_path, ios::binary);
  if (!in) throw runtime_error("snapshot: failed to open '" + final_path + "'");
  string data((istreambuf_iterator<char>(in)), istreambuf_iterator<char>());
  in.close();

  if (data.size() < kHeaderSize || memcmp(data.data(), kMagic, sizeof(kMagic)) != 0) {
    throw runtime_error("snapshot: '" + final_path + "' has an invalid header");
  }
  uint8_t version_byte = static_cast<uint8_t>(data[4]);
  if (version_byte != kFormatVersion) {
    throw runtime_error("snapshot: '" + final_path + "' has unsupported format version " +
                              to_string(version_byte));
  }

  LoadedSnapshot result;
  result.boundary_write_id = read_u64_be(data.data() + 5);
  uint64_t entry_count = read_u64_be(data.data() + 13);
  result.entries.reserve(entry_count);

  size_t offset = kHeaderSize;
  for (uint64_t i = 0; i < entry_count; ++i) {
    constexpr size_t kEntryFixedLen = 4 + 4 + 8 + 8 + 1;
    if (offset + kEntryFixedLen > data.size()) {
      throw runtime_error("snapshot: '" + final_path + "' truncated in entry header");
    }
    uint32_t key_len = read_u32_be(data.data() + offset);
    uint32_t value_len = read_u32_be(data.data() + offset + 4);
    uint64_t version = read_u64_be(data.data() + offset + 8);
    uint64_t write_id = read_u64_be(data.data() + offset + 16);
    bool tombstone = data[offset + 24] != 0;
    offset += kEntryFixedLen;

    if (offset + key_len + value_len > data.size()) {
      throw runtime_error("snapshot: '" + final_path + "' truncated in entry payload");
    }

    SnapshotEntry entry;
    entry.key.assign(data.data() + offset, key_len);
    offset += key_len;
    entry.value.assign(data.data() + offset, value_len);
    offset += value_len;
    entry.version = version;
    entry.write_id = write_id;
    entry.tombstone = tombstone;
    result.entries.push_back(move(entry));
  }

  return result;
}

}  // namespace kvstore
