// Concurrency benchmark (stage "sharding" resume claim): measures concurrent
// PUT throughput of the old single-lock, single-shard Store (Phase 1b/stage
// 3's design) against the new N-shard ShardedStore, across a range of
// writer thread counts. See docs/benchmarks.md for methodology and results.
//
// Both configurations use a real, fsync-per-write WriteAheadLog (not an
// in-memory-only Store) — the whole point of this benchmark is to show
// where the "one global lock serializes every writer, including the fsync
// each one does while holding it" ceiling documented in
// docs/design-decisions.md's "Locking Strategy" section actually shows up,
// and sharding's fix for it: N shards means N independent locks *and* N
// independent WAL files, so writers to different shards no longer wait on
// each other's fsync at all.
//
// Usage:
//   kvstore_bench_concurrency <ops_per_thread> [thread_count...]
//   (default thread counts: 1 2 4 8 16)
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "sharded_store.h"
#include "store.h"
#include "wal.h"

using namespace std;

namespace fs = filesystem;

namespace {

// Fixed shard count for the "sharded" side of the comparison. Not swept
// across multiple values here: the thing under test is "one lock vs many,"
// not "how does throughput scale with shard count" (a separate question
// docs/limitations.md notes isn't measured here).
constexpr size_t kNumShards = 8;

string make_temp_dir(const string& tag) {
  fs::path path = fs::temp_directory_path() /
                  ("kvstore_bench_concurrency_" + tag + "_" +
                   to_string(chrono::steady_clock::now().time_since_epoch().count()));
  return path.string();
}

// Runs `num_threads` writer threads, each PUTting `ops_per_thread` distinct
// keys (thread-prefixed, so no two threads ever contend on the same key —
// what's being measured is lock/WAL fan-out across shards, not per-key
// contention, which storage_test.cpp already covers separately). Returns
// achieved throughput in ops/sec.
double run_single_lock(int num_threads, int ops_per_thread) {
  string wal_dir = make_temp_dir("single");
  kvstore::WriteAheadLog wal(wal_dir);
  kvstore::Store store(&wal);

  auto start = chrono::steady_clock::now();
  vector<thread> threads;
  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&store, t, ops_per_thread] {
      for (int i = 0; i < ops_per_thread; ++i) {
        string key = "t" + to_string(t) + "-k" + to_string(i);
        store.put(key, "v");
      }
    });
  }
  for (auto& th : threads) th.join();
  double elapsed_s =
      chrono::duration<double>(chrono::steady_clock::now() - start).count();

  error_code ec;
  fs::remove_all(wal_dir, ec);
  return (num_threads * ops_per_thread) / elapsed_s;
}

double run_sharded(int num_threads, int ops_per_thread) {
  string wal_dir = make_temp_dir("sharded_wal");
  string snapshot_dir = make_temp_dir("sharded_snapshot");
  kvstore::ShardedStore store(kNumShards, wal_dir, snapshot_dir);

  auto start = chrono::steady_clock::now();
  vector<thread> threads;
  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&store, t, ops_per_thread] {
      for (int i = 0; i < ops_per_thread; ++i) {
        string key = "t" + to_string(t) + "-k" + to_string(i);
        store.put(key, "v");
      }
    });
  }
  for (auto& th : threads) th.join();
  double elapsed_s =
      chrono::duration<double>(chrono::steady_clock::now() - start).count();

  error_code ec;
  fs::remove_all(wal_dir, ec);
  fs::remove_all(snapshot_dir, ec);
  return (num_threads * ops_per_thread) / elapsed_s;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    cerr << "usage: " << argv[0] << " <ops_per_thread> [thread_count...]\n";
    return 1;
  }
  int ops_per_thread = atoi(argv[1]);
  vector<int> thread_counts;
  if (argc > 2) {
    for (int i = 2; i < argc; ++i) thread_counts.push_back(atoi(argv[i]));
  } else {
    thread_counts = {1, 2, 4, 8, 16};
  }

  cout << fixed << setprecision(0);
  cout << "threads,single_lock_ops_per_sec,sharded_" << kNumShards << "_ops_per_sec,speedup\n";
  for (int threads : thread_counts) {
    double single = run_single_lock(threads, ops_per_thread);
    double sharded = run_sharded(threads, ops_per_thread);
    cout << threads << "," << single << "," << sharded << "," << setprecision(2)
              << (sharded / single) << fixed << setprecision(0) << "\n";
  }
  return 0;
}
