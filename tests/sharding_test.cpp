#include <catch2/catch_test_macros.hpp>

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "sharded_store.h"
#include "sharding.h"
#include "store.h"
#include "wal.h"

using namespace std;

using kvstore::DeleteResult;
using kvstore::PutResult;
using kvstore::ShardedStore;
using kvstore::shard_for_key;
using kvstore::Store;
using kvstore::WriteAheadLog;

namespace {

namespace fs = filesystem;

// RAII temp directory, unique per instance — same pattern as
// wal_test.cpp/crash_recovery_test.cpp's TempDir.
class TempDir {
 public:
  explicit TempDir(const string& tag) {
    path_ = fs::temp_directory_path() /
            ("kvstore_sharding_test_" + tag + "_" +
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

// --- shard_for_key: routing correctness and distribution ---
//
// Distribution is tested against shard_for_key() directly, over the full
// 1M+ keyspace the milestone asks for, rather than by driving that many
// PUTs through a real WAL-backed ShardedStore: routing is a pure function
// of the key and num_shards, so testing it at this scale costs nothing
// (pure hashing, no disk I/O) and isolates "does the hash distribute
// evenly" from "is fsync-per-write fast enough to PUT a million keys in a
// unit test" — the second question belongs in docs/benchmarks.md, not here.
// End-to-end ShardedStore correctness (routing + real Store/WAL together)
// is covered below at a much smaller, still-representative scale.

TEST_CASE("shard_for_key is deterministic for the same key", "[sharding]") {
  REQUIRE(shard_for_key("hello", 8) == shard_for_key("hello", 8));
  REQUIRE(shard_for_key("", 8) == shard_for_key("", 8));
}

TEST_CASE("shard_for_key always returns an id within range", "[sharding]") {
  constexpr size_t kNumShards = 8;
  for (int i = 0; i < 10000; ++i) {
    REQUIRE(shard_for_key("key" + to_string(i), kNumShards) < kNumShards);
  }
}

TEST_CASE("shard_for_key distributes 1M+ keys evenly across shards", "[sharding]") {
  constexpr int kNumKeys = 1'200'000;
  for (size_t num_shards : {size_t{8}, size_t{16}}) {
    vector<size_t> counts(num_shards, 0);
    for (int i = 0; i < kNumKeys; ++i) {
      counts[shard_for_key("key" + to_string(i), num_shards)]++;
    }

    double mean = static_cast<double>(kNumKeys) / static_cast<double>(num_shards);
    for (size_t shard = 0; shard < num_shards; ++shard) {
      double deviation = abs(static_cast<double>(counts[shard]) - mean) / mean;
      // FNV-1a over sequential "keyN" strings measured well under 1%
      // deviation at this scale; 10% leaves comfortable margin without
      // being loose enough to hide a real distribution bug (e.g.
      // accidentally hashing only a fixed prefix of the key).
      REQUIRE(deviation < 0.10);
    }
  }
}

// --- ShardedStore: routing + real Store/WAL together ---

TEST_CASE("ShardedStore put/get/delete route correctly and are visible after routing",
          "[sharding][storage]") {
  TempDir wal_dir("basic_wal");
  TempDir snapshot_dir("basic_snapshot");
  ShardedStore store(8, wal_dir.string(), snapshot_dir.string());

  constexpr int kNumKeys = 2000;
  for (int i = 0; i < kNumKeys; ++i) {
    string key = "key" + to_string(i);
    REQUIRE(store.put(key, "value" + to_string(i)) == PutResult::kOk);
  }
  for (int i = 0; i < kNumKeys; ++i) {
    string key = "key" + to_string(i);
    auto value = store.get(key);
    REQUIRE(value.has_value());
    REQUIRE(*value == "value" + to_string(i));
  }
  REQUIRE(store.size() == static_cast<size_t>(kNumKeys));

  REQUIRE(store.del("key0") == DeleteResult::kOk);
  REQUIRE_FALSE(store.get("key0").has_value());
  // size() still counts the tombstone (Store keeps deleted entries as
  // tombstones rather than erasing them — see docs/design-decisions.md),
  // same as unsharded Store::size() already does.
  REQUIRE(store.size() == static_cast<size_t>(kNumKeys));
}

TEST_CASE("ShardedStore spreads keys across more than one shard's Store", "[sharding][storage]") {
  TempDir wal_dir("spread_wal");
  TempDir snapshot_dir("spread_snapshot");
  constexpr size_t kNumShards = 8;
  ShardedStore store(kNumShards, wal_dir.string(), snapshot_dir.string());

  for (int i = 0; i < 2000; ++i) {
    store.put("key" + to_string(i), "v");
  }

  size_t shards_with_data = 0;
  for (size_t s = 0; s < kNumShards; ++s) {
    if (store.shard(s).size() > 0) ++shards_with_data;
  }
  // Not asserting an exact even split here (that's shard_for_key's test
  // above) — just that routing actually reaches more than one shard's
  // Store, not e.g. every key silently landing in shard 0.
  REQUIRE(shards_with_data == kNumShards);
}

TEST_CASE("ShardedStore: put/delete survive a fresh reopen across every shard",
          "[sharding][wal]") {
  TempDir wal_dir("reopen_wal");
  TempDir snapshot_dir("reopen_snapshot");
  constexpr size_t kNumShards = 4;
  constexpr int kNumKeys = 500;

  {
    ShardedStore store(kNumShards, wal_dir.string(), snapshot_dir.string());
    for (int i = 0; i < kNumKeys; ++i) {
      store.put("key" + to_string(i), "value" + to_string(i));
    }
    REQUIRE(store.del("key0") == DeleteResult::kOk);
  }

  ShardedStore store2(kNumShards, wal_dir.string(), snapshot_dir.string());
  size_t replayed = store2.recover_from_wal();
  REQUIRE(replayed == static_cast<size_t>(kNumKeys) + 1);  // + 1 delete record

  REQUIRE_FALSE(store2.get("key0").has_value());
  for (int i = 1; i < kNumKeys; ++i) {
    string key = "key" + to_string(i);
    auto value = store2.get(key);
    REQUIRE(value.has_value());
    REQUIRE(*value == "value" + to_string(i));
  }
}

TEST_CASE("ShardedStore: take_snapshot bounds WAL replay per shard, same as unsharded Store",
          "[sharding][snapshot]") {
  TempDir wal_dir("snapshot_wal");
  TempDir snapshot_dir("snapshot_snapshot");
  constexpr size_t kNumShards = 4;

  {
    ShardedStore store(kNumShards, wal_dir.string(), snapshot_dir.string());
    for (int i = 0; i < 500; ++i) {
      store.put("key" + to_string(i), "v" + to_string(i));
    }
    store.take_snapshot();
    // Written after the snapshot: only these should show up as replayed
    // WAL records on the next restart.
    store.put("post-snapshot", "v");
  }

  ShardedStore store2(kNumShards, wal_dir.string(), snapshot_dir.string());
  size_t snapshot_entries = store2.load_snapshots();
  size_t replayed = store2.recover_from_wal();

  REQUIRE(snapshot_entries == 500);
  REQUIRE(replayed == 1);  // just "post-snapshot", not all 500 pre-snapshot puts
  REQUIRE(store2.size() == 501);
  REQUIRE(*store2.get("post-snapshot") == "v");
}

// --- Per-shard independence: a crash/corruption in one shard must not
// affect any other shard's data or ability to recover. ---

TEST_CASE("per-shard recovery is independent: corrupting one shard's WAL "
          "does not affect another shard's data or recovery",
          "[sharding][crash]") {
  TempDir wal_dir("independent_wal");
  TempDir snapshot_dir("independent_snapshot");
  constexpr size_t kNumShards = 4;
  // Small enough that ~200 keys (~50/shard) forces several rotations per
  // shard — needed so shard 0 ends up with an *earlier*, already-rotated
  // segment to corrupt: recover() only treats corruption in the active
  // (still-being-written) segment as a tolerable crash tail (truncate and
  // stop); corruption anywhere earlier is a hard error, per
  // docs/limitations.md. A single-segment shard has no "earlier" segment to
  // demonstrate that with.
  constexpr size_t kTinySegmentBytes = 256;

  {
    ShardedStore store(kNumShards, wal_dir.string(), snapshot_dir.string(), kTinySegmentBytes);
    for (int i = 0; i < 200; ++i) {
      store.put("key" + to_string(i), "v" + to_string(i));
    }
  }

  // Corrupt the earliest (lowest-numbered, already-rotated) segment in
  // shard 0 by flipping a byte inside a record's checksum-covered range.
  string shard0_wal_dir = wal_dir.string() + "/shard_0";
  string earliest_segment_path;
  for (const auto& entry : fs::directory_iterator(shard0_wal_dir)) {
    if (entry.path().extension() != ".wal") continue;
    if (earliest_segment_path.empty() || entry.path().string() < earliest_segment_path) {
      earliest_segment_path = entry.path().string();
    }
  }
  REQUIRE_FALSE(earliest_segment_path.empty());
  {
    // Byte offset 10 falls inside the first record's write_id field
    // (offset 5-12 within a record — see docs/architecture.md's WAL record
    // layout), which the record's trailing CRC-32 covers.
    fstream f(earliest_segment_path, ios::in | ios::out | ios::binary);
    REQUIRE(f.is_open());
    f.seekp(10);
    char corrupt = '\xFF';
    f.write(&corrupt, 1);
  }

  // Shard 1's directory is untouched: constructing a fresh Store+WAL over
  // just its directory (bypassing ShardedStore's aggregate recover, which
  // would itself throw the moment it reaches the corrupted shard) proves
  // shard 1's on-disk state and recovery are completely unaffected by shard
  // 0's corruption — they are, and always were, separate files.
  string shard1_wal_dir = wal_dir.string() + "/shard_1";
  WriteAheadLog wal1(shard1_wal_dir);
  Store recovered_shard1(&wal1);
  REQUIRE_NOTHROW(recovered_shard1.recover_from_wal());

  // Shard 0's own recovery, in isolation, does surface the corruption as a
  // hard failure rather than silently dropping or misreading data.
  WriteAheadLog wal0(shard0_wal_dir);
  Store recovered_shard0(&wal0);
  REQUIRE_THROWS_AS(recovered_shard0.recover_from_wal(), runtime_error);
}

// --- Real crash-and-restart across a sharded store (SIGKILL, not a clean
// shutdown) — same technique as crash_recovery_test.cpp, applied to
// ShardedStore to prove the full multi-shard recovery path holds up
// against an actual crash, not just a destructor running normally. ---

TEST_CASE("ShardedStore crash-and-restart: SIGKILL after N acknowledged writes "
          "loses nothing and gains nothing",
          "[sharding][crash]") {
  TempDir wal_dir("crash_wal");
  TempDir snapshot_dir("crash_snapshot");
  constexpr size_t kNumShards = 4;
  constexpr int kTotalKeys = 40;
  constexpr int kKillAfter = 17;

  int ack_pipe[2];
  int cont_pipe[2];
  REQUIRE(pipe(ack_pipe) == 0);
  REQUIRE(pipe(cont_pipe) == 0);

  pid_t pid = fork();
  REQUIRE(pid >= 0);

  if (pid == 0) {
    // Child: no Catch2 assertions past this point (not fork-safe).
    ::close(ack_pipe[0]);
    ::close(cont_pipe[1]);

    ShardedStore store(kNumShards, wal_dir.string(), snapshot_dir.string());
    for (int i = 0; i < kTotalKeys; ++i) {
      store.put("key" + to_string(i), "value" + to_string(i));

      char ack = 1;
      ssize_t written = ::write(ack_pipe[1], &ack, 1);
      (void)written;

      if (i == kKillAfter - 1) {
        char buf;
        ssize_t n = ::read(cont_pipe[0], &buf, 1);
        (void)n;
      }
    }
    _exit(0);
  }

  // Parent.
  ::close(ack_pipe[1]);
  ::close(cont_pipe[0]);

  for (int i = 0; i < kKillAfter; ++i) {
    char ack = 0;
    ssize_t n = ::read(ack_pipe[0], &ack, 1);
    REQUIRE(n == 1);
  }
  REQUIRE(kill(pid, SIGKILL) == 0);

  int status = 0;
  REQUIRE(waitpid(pid, &status, 0) == pid);
  REQUIRE(WIFSIGNALED(status));
  REQUIRE(WTERMSIG(status) == SIGKILL);

  ::close(ack_pipe[0]);
  ::close(cont_pipe[1]);

  ShardedStore store2(kNumShards, wal_dir.string(), snapshot_dir.string());
  size_t replayed = store2.recover_from_wal();

  REQUIRE(replayed == static_cast<size_t>(kKillAfter));
  for (int i = 0; i < kKillAfter; ++i) {
    string key = "key" + to_string(i);
    auto value = store2.get(key);
    REQUIRE(value.has_value());
    REQUIRE(*value == "value" + to_string(i));
  }
  for (int i = kKillAfter; i < kTotalKeys; ++i) {
    REQUIRE_FALSE(store2.get("key" + to_string(i)).has_value());
  }
}
