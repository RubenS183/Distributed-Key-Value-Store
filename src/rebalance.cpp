#include "rebalance.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <thread>

using namespace std;

namespace kvstore {

namespace fs = filesystem;

RateLimiter::RateLimiter(size_t rate_bytes_per_sec)
    : rate_bytes_per_sec_(rate_bytes_per_sec),
      tokens_(0.0),
      last_refill_(chrono::steady_clock::now()) {}

void RateLimiter::consume(size_t bytes) {
  if (rate_bytes_per_sec_ == 0) return;  // unlimited

  auto now = chrono::steady_clock::now();
  double elapsed_s = chrono::duration<double>(now - last_refill_).count();
  last_refill_ = now;
  tokens_ += elapsed_s * static_cast<double>(rate_bytes_per_sec_);
  // Cap the bucket at one second's worth so a long gap since the last
  // consume() call (e.g. before the very first entry) can't let an initial
  // burst blow straight through the configured rate.
  double max_tokens = static_cast<double>(rate_bytes_per_sec_);
  if (tokens_ > max_tokens) tokens_ = max_tokens;

  tokens_ -= static_cast<double>(bytes);
  if (tokens_ < 0.0) {
    double deficit_s = -tokens_ / static_cast<double>(rate_bytes_per_sec_);
    this_thread::sleep_for(chrono::duration<double>(deficit_s));
    tokens_ = 0.0;
    last_refill_ = chrono::steady_clock::now();
  }
}

void WritePreferringLock::lock_shared() {
  unique_lock<mutex> lock(mutex_);
  cv_.wait(lock, [this] { return waiting_writers_ == 0 && !writer_active_; });
  ++active_readers_;
}

void WritePreferringLock::unlock_shared() {
  unique_lock<mutex> lock(mutex_);
  if (--active_readers_ == 0) cv_.notify_all();
}

void WritePreferringLock::lock() {
  unique_lock<mutex> lock(mutex_);
  ++waiting_writers_;
  cv_.wait(lock, [this] { return active_readers_ == 0 && !writer_active_; });
  --waiting_writers_;
  writer_active_ = true;
}

void WritePreferringLock::unlock() {
  {
    unique_lock<mutex> lock(mutex_);
    writer_active_ = false;
  }
  cv_.notify_all();
}

namespace {

constexpr char kManifestMagic[4] = {'K', 'V', 'S', 'M'};
constexpr uint8_t kManifestVersion = 1;
// magic(4) + format_version(1) + generation(8) + num_shards(8).
constexpr size_t kManifestSize = 4 + 1 + 8 + 8;
constexpr const char* kManifestName = "shards.manifest";
constexpr const char* kManifestTempName = "shards.manifest.tmp";

void append_u64_be(string& out, uint64_t v) {
  for (int i = 7; i >= 0; --i) out.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}

uint64_t read_u64_be(const char* p) {
  const auto* u = reinterpret_cast<const unsigned char*>(p);
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v = (v << 8) | u[i];
  return v;
}

// Mirrors snapshot.cpp's fsync_dir(): durably persists the rename() above by
// flushing the directory entry itself, not just the manifest file's bytes.
void fsync_dir(const string& dir) {
  int fd = ::open(dir.c_str(), O_RDONLY);
  if (fd < 0) {
    throw runtime_error("rebalance: failed to open directory '" + dir +
                              "' for fsync: " + strerror(errno));
  }
  int rc = ::fsync(fd);
  ::close(fd);
  if (rc != 0) {
    throw runtime_error("rebalance: fsync of directory '" + dir + "' failed: " + strerror(errno));
  }
}

}  // namespace

string shard_dir(const string& root, uint64_t generation, size_t shard_index) {
  string base = (generation == 0) ? root : (root + "/gen_" + to_string(generation));
  return base + "/shard_" + to_string(shard_index);
}

optional<ShardManifest> read_shard_manifest(const string& wal_dir) {
  string path = wal_dir + "/" + kManifestName;
  ifstream in(path, ios::binary);
  if (!in) return nullopt;

  string buf((istreambuf_iterator<char>(in)), istreambuf_iterator<char>());
  if (buf.size() != kManifestSize || memcmp(buf.data(), kManifestMagic, 4) != 0 ||
      static_cast<uint8_t>(buf[4]) != kManifestVersion) {
    throw runtime_error("read_shard_manifest: malformed manifest at '" + path + "'");
  }

  ShardManifest manifest;
  manifest.generation = read_u64_be(buf.data() + 5);
  manifest.num_shards = static_cast<size_t>(read_u64_be(buf.data() + 13));
  return manifest;
}

void write_shard_manifest(const string& wal_dir, const ShardManifest& manifest) {
  error_code ec;
  fs::create_directories(wal_dir, ec);
  if (ec) {
    throw runtime_error("write_shard_manifest: failed to create '" + wal_dir +
                              "': " + ec.message());
  }

  string body;
  body.append(kManifestMagic, sizeof(kManifestMagic));
  body.push_back(static_cast<char>(kManifestVersion));
  append_u64_be(body, manifest.generation);
  append_u64_be(body, static_cast<uint64_t>(manifest.num_shards));

  string tmp_path = wal_dir + "/" + kManifestTempName;
  string final_path = wal_dir + "/" + kManifestName;

  int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) {
    throw runtime_error("write_shard_manifest: open() failed for '" + tmp_path +
                              "': " + strerror(errno));
  }

  size_t written = 0;
  while (written < body.size()) {
    ssize_t w = ::write(fd, body.data() + written, body.size() - written);
    if (w < 0) {
      ::close(fd);
      throw runtime_error(string("write_shard_manifest: write() failed: ") + strerror(errno));
    }
    written += static_cast<size_t>(w);
  }

  if (::fsync(fd) != 0) {
    ::close(fd);
    throw runtime_error(string("write_shard_manifest: fsync() failed: ") + strerror(errno));
  }
  ::close(fd);

  fs::rename(tmp_path, final_path, ec);
  if (ec) {
    throw runtime_error("write_shard_manifest: rename '" + tmp_path + "' -> '" + final_path +
                              "' failed: " + ec.message());
  }

  fsync_dir(wal_dir);
}

void discard_stale_generations(const string& root, uint64_t active_generation) {
  error_code ec;
  if (!fs::exists(root, ec) || ec) return;

  for (const auto& entry : fs::directory_iterator(root, ec)) {
    if (ec) return;
    if (!entry.is_directory()) continue;

    string name = entry.path().filename().string();
    if (name.rfind("gen_", 0) != 0) continue;

    uint64_t gen = strtoull(name.c_str() + 4, nullptr, 10);
    if (gen != active_generation) {
      error_code rm_ec;
      fs::remove_all(entry.path(), rm_ec);  // best-effort; see header comment
    }
  }
}

}  // namespace kvstore
