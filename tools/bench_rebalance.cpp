// Rebalance-overhead benchmark (stage 9 resume claim): measures the
// foreground PUT-throughput cost of running ShardedStore::rebalance_to()
// concurrently with live writers, at a few throttle settings, and confirms
// no data is lost across the migration. See docs/benchmarks.md for
// methodology and results.
//
// Writer threads do a *fixed* number of ops each (not "run until told to
// stop") deliberately: an early version of this benchmark used unbounded
// writer threads and a low throttle rate, and discovered the hard way that
// a migration's copy loop can fall permanently behind when live writes
// land faster than the configured rate_bytes_per_sec lets the copy drain
// them — the exact same "dirty rate exceeds migration bandwidth"
// non-convergence problem VM live-migration has to guard against (see
// docs/limitations.md). Bounding each writer to ops_per_thread guarantees
// the backlog rebalance_to() has to work through is always finite, so this
// benchmark always terminates regardless of how low a rate is passed —
// see docs/design-decisions.md's "Online Resharding" section for why the
// throttle has no such bound in general.
//
// For each rate_bytes_per_sec in the given list:
//   1. "baseline": `threads` writer threads each PUT `ops_per_thread` keys,
//      no rebalance running — the ceiling this project's sharding stage
//      already established (see docs/benchmarks.md's "Concurrency"
//      section).
//   2. "during": the same preloaded store and writer workload, but with a
//      4->8 shard rebalance_to() running concurrently on its own thread.
//      Every write here pays dual-write's two fsyncs (source and
//      forwarded target) for as long as the copy is still in progress —
//      see docs/design-decisions.md's "Online Resharding" section.
// Ops/sec is each run's own total ops divided by its own elapsed time;
// dip_pct is how much lower "during" is than "baseline".
//
// Usage:
//   kvstore_bench_rebalance <ops_per_thread> <threads> [rate_bytes_per_sec...]
//   (default: 2000 ops/thread, 4 threads, rates 0 (unlimited) 50000 10000)
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "sharded_store.h"

using namespace std;

namespace fs = filesystem;

namespace {

constexpr size_t kOldShards = 4;
constexpr size_t kNewShards = 8;
constexpr int kPreloadKeys = 3000;

string make_temp_dir(const string& tag) {
  fs::path path = fs::temp_directory_path() /
                  ("kvstore_bench_rebalance_" + tag + "_" +
                   to_string(chrono::steady_clock::now().time_since_epoch().count()));
  return path.string();
}

void preload(kvstore::ShardedStore& store) {
  for (int i = 0; i < kPreloadKeys; ++i) {
    store.put("pre-k" + to_string(i), "v" + to_string(i));
  }
}

struct RunResult {
  double ops_per_sec = 0;
  double elapsed_ms = 0;
};

// Runs `threads` writer threads, each PUTting `ops_per_thread` distinct,
// thread-prefixed keys (so no two threads contend on the same key — this
// measures rebalance overhead on the write path, not per-key contention,
// which storage_test.cpp already covers).
RunResult run_writers(kvstore::ShardedStore& store, int threads, int ops_per_thread) {
  auto start = chrono::steady_clock::now();
  vector<thread> writers;
  for (int t = 0; t < threads; ++t) {
    writers.emplace_back([&store, t, ops_per_thread] {
      for (int i = 0; i < ops_per_thread; ++i) {
        store.put("w" + to_string(t) + "-k" + to_string(i), "v");
      }
    });
  }
  for (auto& w : writers) w.join();
  double elapsed_ms =
      chrono::duration<double, milli>(chrono::steady_clock::now() - start).count();
  return {(threads * ops_per_thread) / (elapsed_ms / 1000.0), elapsed_ms};
}

bool verify_no_data_loss(kvstore::ShardedStore& store, int threads, int ops_per_thread) {
  bool ok = true;
  for (int t = 0; t < threads; ++t) {
    for (int i = 0; i < ops_per_thread; ++i) {
      auto value = store.get("w" + to_string(t) + "-k" + to_string(i));
      if (!value.has_value() || *value != "v") {
        ok = false;
        cerr << "MISSING/WRONG: w" << t << "-k" << i << " value="
             << (value.has_value() ? *value : "<absent>") << "\n";
      }
    }
  }
  for (int i = 0; i < kPreloadKeys; ++i) {
    auto value = store.get("pre-k" + to_string(i));
    if (!value.has_value() || *value != "v" + to_string(i)) {
      ok = false;
      cerr << "MISSING/WRONG: pre-k" << i << " value=" << (value.has_value() ? *value : "<absent>")
           << "\n";
    }
  }
  size_t expected = static_cast<size_t>(kPreloadKeys) + static_cast<size_t>(threads) * ops_per_thread;
  if (store.size() != expected) {
    cerr << "SIZE MISMATCH: expected=" << expected << " actual=" << store.size() << "\n";
  }
  return ok && store.size() == expected;
}

}  // namespace

int main(int argc, char** argv) {
  int ops_per_thread = (argc > 1) ? atoi(argv[1]) : 2000;
  int threads = (argc > 2) ? atoi(argv[2]) : 4;
  vector<size_t> rates;
  if (argc > 3) {
    for (int i = 3; i < argc; ++i) rates.push_back(static_cast<size_t>(strtoull(argv[i], nullptr, 10)));
  } else {
    rates = {0, 50000, 10000};
  }

  cout << fixed << setprecision(0);
  cout << "rate_bytes_per_sec,migration_ms,baseline_ops_per_sec,during_rebalance_ops_per_sec,"
          "dip_pct,data_loss\n";

  bool any_data_loss = false;
  for (size_t rate : rates) {
    string baseline_wal = make_temp_dir("baseline_wal");
    string baseline_snapshot = make_temp_dir("baseline_snapshot");
    kvstore::ShardedStore baseline_store(kOldShards, baseline_wal, baseline_snapshot);
    preload(baseline_store);
    RunResult baseline = run_writers(baseline_store, threads, ops_per_thread);

    string wal_dir = make_temp_dir("wal");
    string snapshot_dir = make_temp_dir("snapshot");
    kvstore::ShardedStore store(kOldShards, wal_dir, snapshot_dir);
    preload(store);

    double migration_ms = 0;
    thread rebalancer([&store, rate, &migration_ms] {
      auto t0 = chrono::steady_clock::now();
      store.rebalance_to(kNewShards, rate);
      migration_ms = chrono::duration<double, milli>(chrono::steady_clock::now() - t0).count();
    });
    RunResult during = run_writers(store, threads, ops_per_thread);
    rebalancer.join();

    bool no_loss = verify_no_data_loss(store, threads, ops_per_thread);
    if (!no_loss) any_data_loss = true;

    double dip_pct = (baseline.ops_per_sec - during.ops_per_sec) / baseline.ops_per_sec * 100.0;
    cout << rate << "," << migration_ms << "," << baseline.ops_per_sec << ","
              << during.ops_per_sec << "," << setprecision(1) << dip_pct << fixed
              << setprecision(0) << "," << (no_loss ? "ok" : "DATA_LOSS") << "\n";

    error_code ec;
    fs::remove_all(baseline_wal, ec);
    fs::remove_all(baseline_snapshot, ec);
    fs::remove_all(wal_dir, ec);
    fs::remove_all(snapshot_dir, ec);
  }

  if (any_data_loss) {
    cerr << "kvstore_bench_rebalance: DATA LOSS DETECTED — see rows marked DATA_LOSS above\n";
    return 1;
  }
  return 0;
}
