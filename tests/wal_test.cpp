#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "store.h"
#include "wal.h"

using namespace std;

using kvstore::DeleteResult;
using kvstore::PutResult;
using kvstore::Record;
using kvstore::RecordType;
using kvstore::Store;
using kvstore::WriteAheadLog;

namespace {

namespace fs = filesystem;

// RAII temp directory, unique per test case, removed on destruction so
// repeated test runs don't accumulate files under the system temp dir.
class TempDir {
 public:
  explicit TempDir(const string& tag) {
    path_ = fs::temp_directory_path() /
            ("kvstore_wal_test_" + tag + "_" +
             to_string(chrono::steady_clock::now().time_since_epoch().count()));
  }
  ~TempDir() {
    error_code ec;
    fs::remove_all(path_, ec);
  }
  string string() const { return path_.string(); }

 private:
  fs::path path_;
};

}  // namespace

TEST_CASE("append then recover replays PUT records in order", "[wal]") {
  TempDir dir("put_replay");
  {
    WriteAheadLog wal(dir.string());
    wal.append_put("a", "1", /*version=*/1, /*write_id=*/1);
    wal.append_put("b", "2", /*version=*/1, /*write_id=*/2);
  }

  WriteAheadLog wal2(dir.string());
  vector<Record> replayed;
  size_t count = wal2.recover([&](const Record& r) { replayed.push_back(r); });

  REQUIRE(count == 2);
  REQUIRE(replayed.size() == 2);
  REQUIRE(replayed[0].key == "a");
  REQUIRE(replayed[0].value == "1");
  REQUIRE(replayed[1].key == "b");
  REQUIRE(replayed[1].value == "2");
}

TEST_CASE("recover replays DELETE as a tombstone record with no value", "[wal]") {
  TempDir dir("delete_replay");
  {
    WriteAheadLog wal(dir.string());
    wal.append_put("a", "1", 1, 1);
    wal.append_delete("a", 2, 2);
  }

  WriteAheadLog wal2(dir.string());
  vector<Record> replayed;
  wal2.recover([&](const Record& r) { replayed.push_back(r); });

  REQUIRE(replayed.size() == 2);
  REQUIRE(replayed[1].type == RecordType::kDelete);
  REQUIRE(replayed[1].value.empty());
}

TEST_CASE("Store + WAL: put/delete survive a fresh Store+WAL reopen", "[wal][storage]") {
  TempDir dir("store_reopen");
  {
    WriteAheadLog wal(dir.string());
    Store store(&wal);
    REQUIRE(store.put("k1", "v1") == PutResult::kOk);
    REQUIRE(store.put("k2", "v2") == PutResult::kOk);
    REQUIRE(store.del("k1") == DeleteResult::kOk);
  }

  WriteAheadLog wal2(dir.string());
  Store store2(&wal2);
  size_t replayed = store2.recover_from_wal();
  REQUIRE(replayed == 3);

  REQUIRE_FALSE(store2.get("k1").has_value());  // deleted
  REQUIRE(store2.get("k2").has_value());
  REQUIRE(*store2.get("k2") == "v2");

  auto k1_entry = store2.peek("k1");
  REQUIRE(k1_entry.has_value());
  REQUIRE(k1_entry->tombstone);
}

TEST_CASE("a Store constructed without a WAL never touches disk", "[wal][storage]") {
  Store store;  // wal_ == nullptr
  REQUIRE(store.put("a", "1") == PutResult::kOk);
  REQUIRE(store.recover_from_wal() == 0);
}

TEST_CASE("segment rotation splits records across multiple files past the threshold", "[wal]") {
  TempDir dir("rotation");
  // Small threshold so a handful of small records forces several rotations.
  constexpr size_t kTinySegment = 128;
  {
    WriteAheadLog wal(dir.string(), kTinySegment);
    for (int i = 0; i < 20; ++i) {
      wal.append_put("key" + to_string(i), "value" + to_string(i), 1,
                      static_cast<uint64_t>(i + 1));
    }
  }

  size_t wal_files = 0;
  for (const auto& entry : fs::directory_iterator(dir.string())) {
    if (entry.path().extension() == ".wal") ++wal_files;
  }
  REQUIRE(wal_files > 1);

  WriteAheadLog wal2(dir.string(), kTinySegment);
  size_t count = 0;
  size_t replayed = wal2.recover([&](const Record&) { ++count; });
  REQUIRE(replayed == 20);
  REQUIRE(count == 20);
}

TEST_CASE("corrupted tail: truncating a WAL file mid-record drops only that record",
          "[wal][crash]") {
  TempDir dir("corrupt_tail");
  string segment_path;
  uintmax_t size_after_two = 0;
  uintmax_t full_size = 0;
  {
    WriteAheadLog wal(dir.string());
    wal.append_put("good1", "v1", 1, 1);
    wal.append_put("good2", "v2", 1, 2);

    for (const auto& entry : fs::directory_iterator(dir.string())) {
      segment_path = entry.path().string();
    }
    REQUIRE_FALSE(segment_path.empty());
    size_after_two = fs::file_size(segment_path);

    wal.append_put("will-be-corrupted", "v3", 1, 3);
    full_size = fs::file_size(segment_path);
  }

  // Sanity: the third record is comfortably bigger than the 5 bytes we're
  // about to chop off, so this truncation lands inside it, not before it.
  REQUIRE(full_size > size_after_two + 5);
  fs::resize_file(segment_path, full_size - 5);

  WriteAheadLog wal2(dir.string());
  vector<Record> replayed;
  size_t count = wal2.recover([&](const Record& r) { replayed.push_back(r); });

  REQUIRE(count == 2);
  REQUIRE(replayed[0].key == "good1");
  REQUIRE(replayed[1].key == "good2");

  // Recovery must truncate the segment file back to exactly the end of the
  // last valid record, so a stray partial record doesn't sit between
  // recovered data and whatever gets appended next.
  REQUIRE(fs::file_size(segment_path) == size_after_two);

  // The WAL must still be usable after recovery: a fresh append should
  // land right after "good2", and a subsequent recovery pass should see
  // exactly the three good records (two originals plus this new one).
  wal2.append_put("after-recovery", "v4", 1, 4);

  WriteAheadLog wal3(dir.string());
  size_t count3 = 0;
  wal3.recover([&](const Record&) { ++count3; });
  REQUIRE(count3 == 3);
}

TEST_CASE("corrupted tail: a length prefix with fewer than 4 bytes is dropped as partial",
          "[wal][crash]") {
  TempDir dir("corrupt_tail_short_prefix");
  string segment_path;
  uintmax_t size_after_one = 0;
  {
    WriteAheadLog wal(dir.string());
    wal.append_put("only-good-key", "v", 1, 1);
    for (const auto& entry : fs::directory_iterator(dir.string())) {
      segment_path = entry.path().string();
    }
    size_after_one = fs::file_size(segment_path);
  }

  // Simulate a crash that only managed to flush 2 stray bytes of the next
  // record's length prefix.
  fs::resize_file(segment_path, size_after_one + 2);

  WriteAheadLog wal2(dir.string());
  vector<Record> replayed;
  size_t count = wal2.recover([&](const Record& r) { replayed.push_back(r); });

  REQUIRE(count == 1);
  REQUIRE(replayed[0].key == "only-good-key");
  REQUIRE(fs::file_size(segment_path) == size_after_one);
}
