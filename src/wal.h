#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

using namespace std;

namespace kvstore {

enum class RecordType : uint8_t { kPut = 0x01, kDelete = 0x02 };

// One durable mutation as replayed from the log at startup. See
// docs/architecture.md for the exact on-disk record layout.
struct Record {
  RecordType type = RecordType::kPut;
  uint64_t write_id = 0;
  uint64_t version = 0;
  string key;
  string value;  // empty for kDelete
};

// Default segment rotation threshold (64 MiB). See docs/design-decisions.md
// for why segments are size-bounded at all.
inline constexpr size_t kDefaultSegmentBytes = 64ull * 1024 * 1024;

// Append-only, fsync-per-write write-ahead log, stored as a sequence of
// size-bounded segment files (`<10-digit seq>.wal`) inside one directory.
//
// Not thread-safe on its own: append_put()/append_delete() assume the caller
// serializes calls (Store already holds its single shared_mutex in
// write mode across the whole put()/del() call that appends, so in practice
// only one thread is ever inside the WAL at a time).
class WriteAheadLog {
 public:
  // Creates `dir` if it doesn't exist and opens (or resumes) the active
  // segment — the highest-numbered `*.wal` file in `dir`, or a fresh
  // `0000000001.wal` if the directory is empty. Does not replay any
  // records; call recover() for that. Throws runtime_error on any
  // filesystem failure.
  explicit WriteAheadLog(string dir, size_t segment_bytes = kDefaultSegmentBytes);
  ~WriteAheadLog();

  WriteAheadLog(const WriteAheadLog&) = delete;
  WriteAheadLog& operator=(const WriteAheadLog&) = delete;

  // Replays every segment in ascending sequence order, calling `apply` for
  // each structurally valid, checksum-verified record. Stops at the first
  // invalid record (a partial trailing write or a checksum/shape mismatch):
  // if that record is in the active (last) segment — the only place a
  // crash can ever leave a partial write — the segment file is truncated
  // on disk to the end of the last valid record, so subsequent append()
  // calls resume cleanly. Invalid data found in any earlier (already
  // rotated, supposedly closed-and-complete) segment is treated as a hard
  // error instead of silently dropped, since that isn't a crash-tail
  // scenario this log is designed to tolerate.
  //
  // Must be called at most once, after construction and before any
  // append_put()/append_delete() call. Returns the number of valid records
  // replayed.
  size_t recover(const function<void(const Record&)>& apply);

  // Appends a PUT record and fsyncs the active segment before returning —
  // the call does not return until the record is durable on disk. Rotates
  // to a new segment first if appending would push the active segment past
  // segment_bytes.
  void append_put(const string& key, const string& value, uint64_t version,
                   uint64_t write_id);

  // Appends a DELETE (tombstone) record and fsyncs before returning.
  void append_delete(const string& key, uint64_t version, uint64_t write_id);

  // Closes the active segment and opens a fresh, empty one, unconditionally
  // (unlike rotate_if_needed(), which only rotates when the segment is
  // full). A no-op if the active segment is already empty — there's
  // nothing to close. Used by Store::take_snapshot() so the
  // just-closed segment (now guaranteed to contain nothing written after
  // the snapshot's boundary) becomes eligible for truncate_before() instead
  // of staying "active" (and therefore untouchable) forever.
  void force_rotate();

  // Deletes every non-active segment whose highest write_id is <=
  // `boundary_write_id` — i.e. every record it contains is already
  // reflected in a snapshot with that boundary. Segments are scanned in
  // ascending sequence order and deletion stops at the first segment that
  // isn't fully covered, since write_id increases monotonically across
  // segments in append order (see docs/design-decisions.md). The active
  // segment is never deleted. Throws runtime_error if a segment fails
  // checksum/shape validation (the same "corruption in an already-rotated
  // segment is a hard error" policy recover() applies) or if deletion
  // itself fails.
  void truncate_before(uint64_t boundary_write_id);

  // Replays every record with write_id > `after_write_id`, across all
  // current segments in ascending order, calling `apply` for each. Read-only
  // and safe to call concurrently with append_put()/append_delete() on the
  // same log (used by the leader's replication catch-up/tail logic while
  // the log is still being actively written): unlike recover(), a torn
  // trailing record in the active segment is silently ignored rather than
  // truncated or treated as an error — it just means a concurrent append()
  // hasn't finished fsyncing yet, and the next call (the replication
  // tailer's next poll) will pick it up once it has. Returns the number of
  // records replayed.
  size_t replay_after(uint64_t after_write_id,
                            const function<void(const Record&)>& apply) const;

 private:
  void append_record(RecordType type, const string& key, const string& value,
                      uint64_t version, uint64_t write_id);
  void rotate_if_needed(size_t incoming_bytes);
  void open_active_segment();
  vector<uint64_t> list_segment_seqs() const;
  string segment_path(uint64_t seq) const;

  // Reads `path` from the start and calls `apply` for each structurally
  // valid, checksum-verified record in order. Returns the byte offset of
  // the first invalid record found (a partial trailing write, or a
  // checksum/shape mismatch), or the full file size if every record in the
  // file is valid. Shared by recover() (which decides whether that means
  // "truncate the active segment's tail" or "corruption, throw") and
  // max_write_id_in_segment() (which uses it purely to find the last valid
  // record's write_id).
  size_t scan_segment(const string& path,
                            const function<void(const Record&)>& apply) const;

  // Returns the highest write_id among valid records in the (already
  // rotated, closed) segment `seq`, or 0 if it contains none. Throws if the
  // segment doesn't scan cleanly to its end — a closed segment is never
  // expected to have a corrupt tail (only the active segment can be torn by
  // a crash), so this treats that the same way recover() does for
  // already-rotated segments.
  uint64_t max_write_id_in_segment(uint64_t seq) const;

  string dir_;
  size_t segment_bytes_;
  uint64_t active_seq_ = 1;
  int fd_ = -1;
  size_t active_size_ = 0;
};

}  // namespace kvstore
