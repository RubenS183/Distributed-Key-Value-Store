#include <catch2/catch_test_macros.hpp>

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include "rebalance.h"
#include "sharded_store.h"
#include "sharding.h"
#include "store.h"
#include "wal.h"

using namespace std;

using kvstore::DeleteResult;
using kvstore::PutResult;
using kvstore::Record;
using kvstore::RecordType;
using kvstore::RebalanceState;
using kvstore::ShardedStore;
using kvstore::ShardId;
using kvstore::shard_for_key;
using kvstore::Store;
using kvstore::WriteAheadLog;

namespace {

namespace fs = filesystem;

// Same RAII temp directory pattern as sharding_test.cpp/wal_test.cpp.
class TempDir {
 public:
  explicit TempDir(const string& tag) {
    path_ = fs::temp_directory_path() /
            ("kvstore_rebalance_test_" + tag + "_" +
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

// --- Store::apply_migrated: the per-key "newer wins" primitive rebalancing
// is built on. Tested directly against Store, isolated from ShardedStore's
// routing/threading, since it's a pure per-key comparison. ---

TEST_CASE("Store::apply_migrated keeps the newest write_id regardless of arrival order",
          "[rebalance][storage]") {
  TempDir wal_dir("newer_wins");
  WriteAheadLog wal(wal_dir.string());
  Store store(&wal);

  Record newer{RecordType::kPut, /*write_id=*/10, /*version=*/3, "k", "newer-value"};
  Record older{RecordType::kPut, /*write_id=*/4, /*version=*/1, "k", "older-value"};

  SECTION("newer arrives first") {
    store.apply_migrated(newer);
    store.apply_migrated(older);
  }
  SECTION("older arrives first") {
    store.apply_migrated(older);
    store.apply_migrated(newer);
  }

  auto entry = store.peek("k");
  REQUIRE(entry.has_value());
  REQUIRE(entry->value == "newer-value");
  REQUIRE(entry->write_id == 10);
  REQUIRE(entry->version == 3);
}

TEST_CASE("Store::apply_migrated applies a tombstone and bypasses the WAL", "[rebalance][storage]") {
  TempDir wal_dir("bypasses_wal");
  WriteAheadLog wal(wal_dir.string());
  Store store(&wal);

  Record put_record{RecordType::kPut, 1, 1, "k", "v"};
  store.apply_migrated(put_record);
  REQUIRE(store.get("k").has_value());

  Record delete_record{RecordType::kDelete, 2, 2, "k", ""};
  store.apply_migrated(delete_record);
  REQUIRE_FALSE(store.get("k").has_value());
  auto tomb = store.peek("k");
  REQUIRE(tomb.has_value());
  REQUIRE(tomb->tombstone);

  // Reopening and replaying the WAL finds nothing -- apply_migrated() never
  // appends to it (see store.h's doc comment for why: a rebalance target's
  // incoming entries aren't in write_id order, and the WAL's
  // truncate_before()/recover() rely on append order being write_id order).
  WriteAheadLog wal2(wal_dir.string());
  Store reopened(&wal2);
  REQUIRE(reopened.recover_from_wal() == 0);
}

// --- ShardedStore::rebalance_to(): state machine, rehash correctness,
// concurrency safety, throttling, and manifest durability. ---

TEST_CASE("rebalance_to transitions idle -> migrating -> done and exposes state", "[rebalance]") {
  TempDir wal_dir("state_wal"), snapshot_dir("state_snapshot");
  ShardedStore store(4, wal_dir.string(), snapshot_dir.string());

  constexpr int kNumKeys = 2000;
  string value(50, 'x');
  for (int i = 0; i < kNumKeys; ++i) store.put("key" + to_string(i), value);

  REQUIRE(store.rebalance_state() == RebalanceState::kIdle);

  // Throttled just enough that the state machine's "migrating" window is
  // wide enough for the polling loop below to reliably observe it.
  thread rebalancer([&store] { store.rebalance_to(8, /*rate_bytes_per_sec=*/20000); });

  bool observed_migrating = false;
  for (int i = 0; i < 1000 && !observed_migrating; ++i) {
    if (store.rebalance_state() == RebalanceState::kMigrating) observed_migrating = true;
    this_thread::sleep_for(chrono::milliseconds(2));
  }
  rebalancer.join();

  REQUIRE(observed_migrating);
  REQUIRE(store.rebalance_state() == RebalanceState::kDone);
  REQUIRE(store.num_shards() == 8);
}

TEST_CASE("rebalance_to rehashes every key, preserving values/versions/tombstones",
          "[rebalance]") {
  TempDir wal_dir("rehash_wal"), snapshot_dir("rehash_snapshot");
  constexpr size_t kOldShards = 4;
  constexpr size_t kNewShards = 6;
  constexpr int kNumKeys = 3000;
  ShardedStore store(kOldShards, wal_dir.string(), snapshot_dir.string());

  for (int i = 0; i < kNumKeys; ++i) {
    store.put("key" + to_string(i), "value" + to_string(i));
  }
  REQUIRE(store.del("key0") == DeleteResult::kOk);

  REQUIRE(store.rebalance_to(kNewShards) == RebalanceState::kDone);
  REQUIRE(store.num_shards() == kNewShards);
  REQUIRE(store.size() == static_cast<size_t>(kNumKeys));  // tombstone still counted

  for (int i = 1; i < kNumKeys; ++i) {
    string key = "key" + to_string(i);
    auto value = store.get(key);
    REQUIRE(value.has_value());
    REQUIRE(*value == "value" + to_string(i));

    ShardId expected_shard = shard_for_key(key, kNewShards);
    REQUIRE(store.shard(expected_shard).peek(key).has_value());
  }

  REQUIRE_FALSE(store.get("key0").has_value());
  auto tomb = store.peek("key0");
  REQUIRE(tomb.has_value());
  REQUIRE(tomb->tombstone);
  REQUIRE(tomb->version == 2);  // one put, then one delete
}

TEST_CASE("rebalance_to under concurrent write load: no acknowledged write is lost",
          "[rebalance][concurrency]") {
  TempDir wal_dir("load_wal"), snapshot_dir("load_snapshot");
  constexpr size_t kOldShards = 4;
  constexpr size_t kNewShards = 8;
  constexpr int kThreads = 4;
  constexpr int kOpsPerThread = 300;
  ShardedStore store(kOldShards, wal_dir.string(), snapshot_dir.string());

  atomic<bool> all_acked{true};
  vector<thread> writers;
  for (int t = 0; t < kThreads; ++t) {
    writers.emplace_back([&store, t, &all_acked] {
      for (int i = 0; i < kOpsPerThread; ++i) {
        string key = "t" + to_string(t) + "-k" + to_string(i);
        if (store.put(key, "v" + to_string(i)) != PutResult::kOk) all_acked.store(false);
      }
    });
  }

  // Runs genuinely concurrently with the writers above -- the point of this
  // test is real interleaving (writes landing before, during, and after
  // every phase of rebalance_to()), not a scripted ordering.
  thread rebalancer([&store] { store.rebalance_to(kNewShards); });

  for (auto& w : writers) w.join();
  rebalancer.join();

  REQUIRE(all_acked.load());
  REQUIRE(store.rebalance_state() == RebalanceState::kDone);
  REQUIRE(store.num_shards() == kNewShards);

  for (int t = 0; t < kThreads; ++t) {
    for (int i = 0; i < kOpsPerThread; ++i) {
      string key = "t" + to_string(t) + "-k" + to_string(i);
      auto value = store.get(key);
      REQUIRE(value.has_value());
      REQUIRE(*value == "v" + to_string(i));
    }
  }
  REQUIRE(store.size() == static_cast<size_t>(kThreads * kOpsPerThread));
}

// Regression test for a real, reproducible data-loss bug found while
// benchmarking: a writer thread can capture the routing plan (with
// dual_write still false, before rebalance_to() published the migrating
// plan) but have its actual Store::put() delayed by per-shard lock
// contention until *after* that shard's copy-loop snapshot was already
// taken -- landing in neither the copy nor a forward. Repro required a
// throttled rate (so the copy loop takes long enough for this ordering to
// actually occur) *combined with* concurrent load -- the two tests above
// only ever exercised one of those at a time. Fixed by having
// forward_if_migrating() re-check dual_write via a fresh atomic_load after
// the write lands, rather than trusting the caller's earlier snapshot of
// it -- see docs/design-decisions.md's "Online Resharding" section.
TEST_CASE("rebalance_to under concurrent write load with a throttled rate: "
          "no acknowledged write is lost",
          "[rebalance][concurrency]") {
  TempDir wal_dir("load_throttled_wal"), snapshot_dir("load_throttled_snapshot");
  constexpr size_t kOldShards = 4;
  constexpr size_t kNewShards = 8;
  constexpr int kThreads = 4;
  constexpr int kOpsPerThread = 2000;
  constexpr int kPreloadKeys = 3000;
  constexpr size_t kRateBytesPerSec = 50000;
  ShardedStore store(kOldShards, wal_dir.string(), snapshot_dir.string());

  for (int i = 0; i < kPreloadKeys; ++i) {
    store.put("pre-k" + to_string(i), "v" + to_string(i));
  }

  atomic<bool> all_acked{true};
  vector<thread> writers;
  for (int t = 0; t < kThreads; ++t) {
    writers.emplace_back([&store, t, &all_acked] {
      for (int i = 0; i < kOpsPerThread; ++i) {
        string key = "w" + to_string(t) + "-k" + to_string(i);
        if (store.put(key, "v") != PutResult::kOk) all_acked.store(false);
      }
    });
  }

  thread rebalancer([&store] { store.rebalance_to(kNewShards, kRateBytesPerSec); });

  for (auto& w : writers) w.join();
  rebalancer.join();

  REQUIRE(all_acked.load());
  REQUIRE(store.rebalance_state() == RebalanceState::kDone);
  REQUIRE(store.num_shards() == kNewShards);

  for (int t = 0; t < kThreads; ++t) {
    for (int i = 0; i < kOpsPerThread; ++i) {
      string key = "w" + to_string(t) + "-k" + to_string(i);
      auto value = store.get(key);
      REQUIRE(value.has_value());
      REQUIRE(*value == "v");
    }
  }
  for (int i = 0; i < kPreloadKeys; ++i) {
    auto value = store.get("pre-k" + to_string(i));
    REQUIRE(value.has_value());
    REQUIRE(*value == "v" + to_string(i));
  }
  REQUIRE(store.size() == static_cast<size_t>(kPreloadKeys + kThreads * kOpsPerThread));
}

TEST_CASE("rebalance_to's throttle measurably slows the copy down", "[rebalance]") {
  constexpr size_t kShards = 2;
  constexpr int kNumKeys = 200;
  string value(150, 'x');

  TempDir wal_dir_a("throttle_a_wal"), snapshot_dir_a("throttle_a_snapshot");
  ShardedStore unlimited(kShards, wal_dir_a.string(), snapshot_dir_a.string());
  for (int i = 0; i < kNumKeys; ++i) unlimited.put("key" + to_string(i), value);

  TempDir wal_dir_b("throttle_b_wal"), snapshot_dir_b("throttle_b_snapshot");
  ShardedStore throttled(kShards, wal_dir_b.string(), snapshot_dir_b.string());
  for (int i = 0; i < kNumKeys; ++i) throttled.put("key" + to_string(i), value);

  auto t0 = chrono::steady_clock::now();
  unlimited.rebalance_to(4, /*rate_bytes_per_sec=*/0);
  double unlimited_ms = chrono::duration<double, milli>(chrono::steady_clock::now() - t0).count();

  auto t1 = chrono::steady_clock::now();
  throttled.rebalance_to(4, /*rate_bytes_per_sec=*/20000);
  double throttled_ms = chrono::duration<double, milli>(chrono::steady_clock::now() - t1).count();

  // Loose bound (2x, not a tight ratio) to avoid flakiness on a loaded
  // machine -- the point is "throttling measurably slows the copy down,"
  // not an exact bandwidth guarantee. See docs/benchmarks.md for the actual
  // measured throttle-vs-throughput numbers.
  REQUIRE(throttled_ms > unlimited_ms * 2);
}

TEST_CASE("rebalance_to commits a manifest: a fresh ShardedStore over the same "
          "directories uses the new shard count",
          "[rebalance][durability]") {
  TempDir wal_dir("manifest_wal"), snapshot_dir("manifest_snapshot");
  constexpr size_t kOldShards = 4;
  constexpr size_t kNewShards = 8;
  constexpr int kNumKeys = 400;

  {
    ShardedStore store(kOldShards, wal_dir.string(), snapshot_dir.string());
    for (int i = 0; i < kNumKeys; ++i) {
      store.put("key" + to_string(i), "value" + to_string(i));
    }
    REQUIRE(store.rebalance_to(kNewShards) == RebalanceState::kDone);
  }

  // Constructor arg is deliberately the *old* shard count -- the manifest
  // rebalance_to() committed above must override it, exactly like a real
  // restart with main.cpp's CLI still passing its old num_shards argument.
  ShardedStore store2(kOldShards, wal_dir.string(), snapshot_dir.string());
  REQUIRE(store2.num_shards() == kNewShards);

  size_t loaded = store2.load_snapshots();
  size_t replayed = store2.recover_from_wal();
  REQUIRE(loaded == static_cast<size_t>(kNumKeys));
  REQUIRE(replayed == 0);  // rebalance_to()'s cutover snapshot already covers everything

  for (int i = 0; i < kNumKeys; ++i) {
    auto value = store2.get("key" + to_string(i));
    REQUIRE(value.has_value());
    REQUIRE(*value == "value" + to_string(i));
  }
}

// --- Real crash mid-migration (SIGKILL, not a clean shutdown) — same
// technique as sharding_test.cpp's crash-and-restart test, applied to
// rebalance_to() to prove an interrupted rebalance actually rolls back
// instead of leaving a half-committed shard layout. ---

TEST_CASE("rebalance_to crash mid-migration: restart rolls back to the source "
          "generation, no data lost",
          "[rebalance][crash]") {
  TempDir wal_dir("crash_wal"), snapshot_dir("crash_snapshot");
  constexpr size_t kOldShards = 4;
  constexpr int kNumKeys = 300;

  {
    ShardedStore store(kOldShards, wal_dir.string(), snapshot_dir.string());
    for (int i = 0; i < kNumKeys; ++i) {
      store.put("key" + to_string(i), "value" + to_string(i));
    }
  }

  int ready_pipe[2];
  REQUIRE(pipe(ready_pipe) == 0);

  pid_t pid = fork();
  REQUIRE(pid >= 0);

  if (pid == 0) {
    // Child: no Catch2 assertions past this point (not fork-safe).
    ::close(ready_pipe[0]);

    ShardedStore store(kOldShards, wal_dir.string(), snapshot_dir.string());
    store.load_snapshots();
    store.recover_from_wal();

    // Heavily throttled so the copy loop is still running (well before
    // cutover/manifest commit) when the parent signals it's time to kill
    // this process.
    thread t([&store] { store.rebalance_to(8, /*rate_bytes_per_sec=*/50); });
    this_thread::sleep_for(chrono::milliseconds(50));
    char ready = 1;
    ssize_t w = ::write(ready_pipe[1], &ready, 1);
    (void)w;
    t.join();  // never reached -- the parent SIGKILLs this process first
    _exit(0);
  }

  // Parent.
  ::close(ready_pipe[1]);
  char buf = 0;
  REQUIRE(::read(ready_pipe[0], &buf, 1) == 1);
  REQUIRE(kill(pid, SIGKILL) == 0);

  int status = 0;
  REQUIRE(waitpid(pid, &status, 0) == pid);
  REQUIRE(WIFSIGNALED(status));
  REQUIRE(WTERMSIG(status) == SIGKILL);
  ::close(ready_pipe[0]);

  // No manifest was ever committed (rebalance_to() was killed mid-copy), so
  // a fresh ShardedStore falls back to generation 0 -- the still-intact
  // source shards -- and discards the abandoned gen_1 staging directory.
  ShardedStore store2(kOldShards, wal_dir.string(), snapshot_dir.string());
  REQUIRE(store2.num_shards() == kOldShards);
  REQUIRE_FALSE(fs::exists(wal_dir.string() + "/gen_1"));
  REQUIRE_FALSE(fs::exists(snapshot_dir.string() + "/gen_1"));

  store2.recover_from_wal();
  for (int i = 0; i < kNumKeys; ++i) {
    auto value = store2.get("key" + to_string(i));
    REQUIRE(value.has_value());
    REQUIRE(*value == "value" + to_string(i));
  }
}
