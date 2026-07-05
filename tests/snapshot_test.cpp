#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <string>
#include <vector>

#include "snapshot.h"
#include "store.h"
#include "wal.h"

using namespace std;

using kvstore::DeleteResult;
using kvstore::LoadedSnapshot;
using kvstore::PutResult;
using kvstore::SnapshotEntry;
using kvstore::Store;
using kvstore::WriteAheadLog;

namespace {

namespace fs = filesystem;

// RAII temp directory, unique per test case. See tests/wal_test.cpp for the
// identical pattern this is copied from.
class TempDir {
 public:
  explicit TempDir(const string& tag) {
    path_ = fs::temp_directory_path() /
            ("kvstore_snapshot_test_" + tag + "_" +
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

size_t count_wal_segments(const string& wal_dir) {
  size_t count = 0;
  for (const auto& entry : fs::directory_iterator(wal_dir)) {
    if (entry.path().extension() == ".wal") ++count;
  }
  return count;
}

}  // namespace

TEST_CASE("write_snapshot then load_latest round-trips entries and boundary", "[snapshot]") {
  TempDir dir("roundtrip");

  vector<SnapshotEntry> entries;
  entries.push_back(SnapshotEntry{"a", "1", /*version=*/1, /*write_id=*/1, /*tombstone=*/false});
  entries.push_back(SnapshotEntry{"b", "", /*version=*/2, /*write_id=*/3, /*tombstone=*/true});

  kvstore::write_snapshot(dir.string(), /*boundary_write_id=*/3, entries);

  optional<LoadedSnapshot> loaded = kvstore::load_latest(dir.string());
  REQUIRE(loaded.has_value());
  REQUIRE(loaded->boundary_write_id == 3);
  REQUIRE(loaded->entries.size() == 2);

  REQUIRE(loaded->entries[0].key == "a");
  REQUIRE(loaded->entries[0].value == "1");
  REQUIRE(loaded->entries[0].version == 1);
  REQUIRE(loaded->entries[0].write_id == 1);
  REQUIRE_FALSE(loaded->entries[0].tombstone);

  REQUIRE(loaded->entries[1].key == "b");
  REQUIRE(loaded->entries[1].value.empty());
  REQUIRE(loaded->entries[1].tombstone);
}

TEST_CASE("load_latest returns nullopt when no snapshot file exists", "[snapshot]") {
  TempDir dir("missing");
  REQUIRE_FALSE(kvstore::load_latest(dir.string()).has_value());
}

TEST_CASE("a second write_snapshot atomically replaces the first", "[snapshot]") {
  TempDir dir("replace");

  kvstore::write_snapshot(dir.string(), 1, {SnapshotEntry{"a", "1", 1, 1, false}});
  kvstore::write_snapshot(dir.string(), 2, {SnapshotEntry{"b", "2", 1, 2, false}});

  auto loaded = kvstore::load_latest(dir.string());
  REQUIRE(loaded.has_value());
  REQUIRE(loaded->boundary_write_id == 2);
  REQUIRE(loaded->entries.size() == 1);
  REQUIRE(loaded->entries[0].key == "b");
}

TEST_CASE("Store::take_snapshot + reopen: recovery loads the snapshot and replays only the WAL "
          "records written after it",
          "[snapshot][storage][wal]") {
  TempDir dir("store_integration");
  string wal_dir = dir.string() + "/wal";
  string snap_dir = dir.string() + "/snap";

  {
    WriteAheadLog wal(wal_dir);
    Store store(&wal);
    REQUIRE(store.put("a", "v_a") == PutResult::kOk);   // write_id 1
    REQUIRE(store.put("b", "v_b") == PutResult::kOk);   // write_id 2
    REQUIRE(store.del("a") == DeleteResult::kOk);       // write_id 3

    store.take_snapshot(snap_dir);

    // The snapshot's boundary (write_id 3) covers every record written so
    // far, and take_snapshot() force-rotates the WAL first — so the
    // segment holding those 3 records should already be gone.
    REQUIRE(count_wal_segments(wal_dir) == 1);  // just the new, empty active segment

    REQUIRE(store.put("c", "v_c") == PutResult::kOk);  // write_id 4, after the snapshot
  }

  WriteAheadLog wal2(wal_dir);
  Store store2(&wal2);
  size_t snapshot_entries = store2.load_snapshot(snap_dir);
  size_t wal_records = store2.recover_from_wal();

  REQUIRE(snapshot_entries == 2);  // "a" (tombstone) and "b"
  REQUIRE(wal_records == 1);       // only "c" — write_ids 1-3 were skipped as snapshot-covered

  REQUIRE_FALSE(store2.get("a").has_value());
  auto a_entry = store2.peek("a");
  REQUIRE(a_entry.has_value());
  REQUIRE(a_entry->tombstone);

  REQUIRE(store2.get("b").has_value());
  REQUIRE(*store2.get("b") == "v_b");

  REQUIRE(store2.get("c").has_value());
  REQUIRE(*store2.get("c") == "v_c");

  // A write after reopening should continue the write_id sequence rather
  // than restarting it, proving the snapshot boundary was picked up.
  REQUIRE(store2.put("d", "v_d") == PutResult::kOk);
  REQUIRE(store2.peek("d")->write_id == 5);
}

TEST_CASE("Store constructed without a WAL can still take and load a snapshot", "[snapshot]") {
  TempDir dir("no_wal");
  string snap_dir = dir.string() + "/snap";

  Store store;  // no WAL
  REQUIRE(store.put("a", "1") == PutResult::kOk);
  store.take_snapshot(snap_dir);

  Store store2;
  REQUIRE(store2.load_snapshot(snap_dir) == 1);
  REQUIRE(store2.get("a").has_value());
  REQUIRE(*store2.get("a") == "1");
}
