#include "wal.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>

using namespace std;

namespace kvstore {

namespace fs = filesystem;

namespace {

// Minimum body size (everything after the 4-byte record_len field): type(1)
// + write_id(8) + version(8) + key_len(4) + value_len(4) + checksum(4).
constexpr size_t kMinBodyLen = 1 + 8 + 8 + 4 + 4 + 4;
constexpr int kSegmentSeqDigits = 10;

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

void append_u32_be(string& out, uint32_t v) {
  out.push_back(static_cast<char>((v >> 24) & 0xFF));
  out.push_back(static_cast<char>((v >> 16) & 0xFF));
  out.push_back(static_cast<char>((v >> 8) & 0xFF));
  out.push_back(static_cast<char>(v & 0xFF));
}

void append_u64_be(string& out, uint64_t v) {
  for (int i = 7; i >= 0; --i) out.push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}

// Standard CRC-32 (IEEE 802.3, polynomial 0xEDB88320), table-driven. This is
// the one "extra" piece of engineering worth justifying under this project's
// YAGNI stance: without it, a torn write during a crash
// (length prefix lands on disk but payload bytes don't, or vice versa)
// would silently read back as a structurally-plausible but wrong record
// instead of being detected and dropped. No third-party dependency is
// needed for it, so it doesn't cost anything against "prefer the standard
// library."
uint32_t crc32(const unsigned char* data, size_t len) {
  static const array<uint32_t, 256> table = [] {
    array<uint32_t, 256> t{};
    for (uint32_t i = 0; i < 256; ++i) {
      uint32_t c = i;
      for (int k = 0; k < 8; ++k) {
        c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
      }
      t[i] = c;
    }
    return t;
  }();

  uint32_t crc = 0xFFFFFFFFu;
  for (size_t i = 0; i < len; ++i) {
    crc = table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
  }
  return crc ^ 0xFFFFFFFFu;
}

// Encodes one record: [4B record_len][1B type][8B write_id][8B version]
// [4B key_len][4B value_len][key][value][4B checksum]. `record_len` counts
// every byte after itself, matching the same convention the wire protocol
// already uses for its length field (see protocol.h). The checksum covers
// everything between record_len and itself (type..value) — see
// docs/architecture.md for the full field-by-field layout.
string encode_record(RecordType type, const string& key, const string& value,
                           uint64_t version, uint64_t write_id) {
  string body;
  body.reserve(1 + 8 + 8 + 4 + 4 + key.size() + value.size());
  body.push_back(static_cast<char>(type));
  append_u64_be(body, write_id);
  append_u64_be(body, version);
  append_u32_be(body, static_cast<uint32_t>(key.size()));
  append_u32_be(body, static_cast<uint32_t>(value.size()));
  body += key;
  body += value;

  uint32_t checksum = crc32(reinterpret_cast<const unsigned char*>(body.data()), body.size());

  string record;
  record.reserve(4 + body.size() + 4);
  append_u32_be(record, static_cast<uint32_t>(body.size() + 4));  // + checksum
  record += body;
  append_u32_be(record, checksum);
  return record;
}

string zero_padded(uint64_t seq) {
  ostringstream oss;
  oss.fill('0');
  oss.width(kSegmentSeqDigits);
  oss << seq;
  return oss.str();
}

}  // namespace

WriteAheadLog::WriteAheadLog(string dir, size_t segment_bytes)
    : dir_(move(dir)), segment_bytes_(segment_bytes) {
  error_code ec;
  fs::create_directories(dir_, ec);
  if (ec) {
    throw runtime_error("wal: failed to create directory '" + dir_ + "': " + ec.message());
  }

  vector<uint64_t> segs = list_segment_seqs();
  active_seq_ = segs.empty() ? 1 : segs.back();
  open_active_segment();
}

WriteAheadLog::~WriteAheadLog() {
  if (fd_ >= 0) ::close(fd_);
}

string WriteAheadLog::segment_path(uint64_t seq) const {
  return dir_ + "/" + zero_padded(seq) + ".wal";
}

vector<uint64_t> WriteAheadLog::list_segment_seqs() const {
  vector<uint64_t> segs;
  if (!fs::exists(dir_)) return segs;

  for (const auto& entry : fs::directory_iterator(dir_)) {
    if (!entry.is_regular_file()) continue;
    const string name = entry.path().filename().string();
    if (name.size() != static_cast<size_t>(kSegmentSeqDigits) + 4) continue;  // "NNNNNNNNNN.wal"
    if (name.substr(kSegmentSeqDigits) != ".wal") continue;
    if (name.find_first_not_of("0123456789") < static_cast<size_t>(kSegmentSeqDigits)) continue;
    segs.push_back(stoull(name.substr(0, kSegmentSeqDigits)));
  }
  sort(segs.begin(), segs.end());
  return segs;
}

void WriteAheadLog::open_active_segment() {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }

  string path = segment_path(active_seq_);
  fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
  if (fd_ < 0) {
    throw runtime_error("wal: failed to open segment '" + path + "': " + strerror(errno));
  }

  struct stat st{};
  if (::fstat(fd_, &st) != 0) {
    throw runtime_error("wal: fstat failed on '" + path + "': " + strerror(errno));
  }
  active_size_ = static_cast<size_t>(st.st_size);
}

void WriteAheadLog::rotate_if_needed(size_t incoming_bytes) {
  // Guard active_size_ > 0 so a single record that alone exceeds
  // segment_bytes_ still lands somewhere instead of rotating forever; in
  // practice this can't happen given kMaxKeySize/kMaxValueSize are far
  // below the default segment size, but the guard costs nothing.
  if (active_size_ > 0 && active_size_ + incoming_bytes > segment_bytes_) {
    active_seq_ += 1;
    open_active_segment();
  }
}

void WriteAheadLog::append_record(RecordType type, const string& key, const string& value,
                                   uint64_t version, uint64_t write_id) {
  string record = encode_record(type, key, value, version, write_id);
  rotate_if_needed(record.size());

  size_t written = 0;
  while (written < record.size()) {
    ssize_t w = ::write(fd_, record.data() + written, record.size() - written);
    if (w < 0) {
      throw runtime_error(string("wal: write failed: ") + strerror(errno));
    }
    written += static_cast<size_t>(w);
  }

  // The durability contract (PUT/DELETE acked only once durable) lives
  // here: append_record() — and therefore put()/del() — does not return
  // until this fsync completes.
  if (::fsync(fd_) != 0) {
    throw runtime_error(string("wal: fsync failed: ") + strerror(errno));
  }
  active_size_ += record.size();
}

void WriteAheadLog::append_put(const string& key, const string& value, uint64_t version,
                                uint64_t write_id) {
  append_record(RecordType::kPut, key, value, version, write_id);
}

void WriteAheadLog::append_delete(const string& key, uint64_t version, uint64_t write_id) {
  append_record(RecordType::kDelete, key, string(), version, write_id);
}

size_t WriteAheadLog::scan_segment(const string& path,
                                         const function<void(const Record&)>& apply) const {
  ifstream in(path, ios::binary);
  if (!in) throw runtime_error("wal: failed to open segment for scan: " + path);
  string data((istreambuf_iterator<char>(in)), istreambuf_iterator<char>());
  in.close();

  size_t offset = 0;
  while (true) {
    if (data.size() - offset < 4) break;

    uint32_t record_len = read_u32_be(data.data() + offset);
    size_t total_needed = 4 + static_cast<size_t>(record_len);
    if (record_len < kMinBodyLen || offset + total_needed > data.size()) break;

    const char* body_ptr = data.data() + offset + 4;
    size_t body_len = record_len - 4;  // exclude checksum
    uint32_t stored_crc = read_u32_be(body_ptr + body_len);
    uint32_t computed_crc = crc32(reinterpret_cast<const unsigned char*>(body_ptr), body_len);

    uint8_t type_byte = static_cast<uint8_t>(body_ptr[0]);
    uint64_t write_id = read_u64_be(body_ptr + 1);
    uint64_t version = read_u64_be(body_ptr + 9);
    uint32_t key_len = read_u32_be(body_ptr + 17);
    uint32_t value_len = read_u32_be(body_ptr + 21);
    bool shape_ok = (type_byte == static_cast<uint8_t>(RecordType::kPut) ||
                     type_byte == static_cast<uint8_t>(RecordType::kDelete)) &&
                    (25 + static_cast<size_t>(key_len) + value_len == body_len);

    if (stored_crc != computed_crc || !shape_ok) break;

    Record rec;
    rec.type = static_cast<RecordType>(type_byte);
    rec.write_id = write_id;
    rec.version = version;
    rec.key.assign(body_ptr + 25, key_len);
    rec.value.assign(body_ptr + 25 + key_len, value_len);
    apply(rec);
    offset += total_needed;
  }

  return offset;
}

size_t WriteAheadLog::recover(const function<void(const Record&)>& apply) {
  vector<uint64_t> segs = list_segment_seqs();
  size_t total = 0;

  for (uint64_t seq : segs) {
    bool is_active = (seq == active_seq_);
    string path = segment_path(seq);

    size_t valid_end = scan_segment(path, [&](const Record& r) {
      apply(r);
      ++total;
    });
    size_t file_size = static_cast<size_t>(fs::file_size(path));

    if (valid_end < file_size) {
      if (!is_active) {
        throw runtime_error(
            "wal: corrupt record in already-rotated segment '" + path + "' at offset " +
            to_string(valid_end) + " — this segment should have been complete before rotation");
      }
      // The only place a real crash can leave a partial/garbled record:
      // the segment that was open for writing when the process died.
      // Drop the tail so future append()s start from known-good state.
      if (::ftruncate(fd_, static_cast<off_t>(valid_end)) != 0) {
        throw runtime_error(string("wal: ftruncate failed during recovery: ") +
                                  strerror(errno));
      }
      active_size_ = valid_end;
    }
  }

  return total;
}

uint64_t WriteAheadLog::max_write_id_in_segment(uint64_t seq) const {
  string path = segment_path(seq);
  uint64_t max_write_id = 0;
  size_t valid_end = scan_segment(path, [&](const Record& r) { max_write_id = r.write_id; });
  size_t file_size = static_cast<size_t>(fs::file_size(path));

  if (valid_end < file_size) {
    throw runtime_error(
        "wal: corrupt record found while scanning already-rotated segment '" + path +
        "' for snapshot-truncation eligibility");
  }
  return max_write_id;
}

size_t WriteAheadLog::replay_after(uint64_t after_write_id,
                                         const function<void(const Record&)>& apply) const {
  size_t total = 0;
  for (uint64_t seq : list_segment_seqs()) {
    // Deliberately ignore scan_segment()'s return value here: recover() and
    // max_write_id_in_segment() use it to detect a torn tail (real crash
    // corruption vs. a benign concurrent write), but this method runs
    // against a live, still-being-appended-to log — a partial trailing
    // record just means the writer isn't done yet, not corruption, and
    // there's nothing to truncate or fail on.
    scan_segment(segment_path(seq), [&](const Record& r) {
      if (r.write_id > after_write_id) {
        apply(r);
        ++total;
      }
    });
  }
  return total;
}

void WriteAheadLog::force_rotate() {
  if (active_size_ == 0) return;  // nothing written to the active segment; nothing to close
  active_seq_ += 1;
  open_active_segment();
}

void WriteAheadLog::truncate_before(uint64_t boundary_write_id) {
  vector<uint64_t> segs = list_segment_seqs();
  for (uint64_t seq : segs) {
    if (seq == active_seq_) continue;

    uint64_t max_write_id = max_write_id_in_segment(seq);
    // Segments are appended in strictly increasing write_id order (one
    // store-wide counter, written in order), so once a segment isn't fully
    // covered, no later (higher-seq) segment is either.
    if (max_write_id > boundary_write_id) break;

    error_code ec;
    fs::remove(segment_path(seq), ec);
    if (ec) {
      throw runtime_error("wal: failed to remove fully-covered segment '" +
                                segment_path(seq) + "': " + ec.message());
    }
  }
}

}  // namespace kvstore
