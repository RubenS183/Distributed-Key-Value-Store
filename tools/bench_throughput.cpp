// Throughput / latency benchmark (stage 8): drives a *running*
// kvstore_server over the real TCP wire protocol with N concurrent client
// connections, a configurable read/write/delete workload mix, and reports
// aggregate throughput plus p50/p95/p99 op latency. See docs/benchmarks.md
// for methodology and results.
//
// Unlike the other bench tools (bench_concurrency/bench_restore/
// bench_rebalance), this one goes over TCP against a real server process
// rather than in-process against Store/ShardedStore directly — the whole
// point of this number is to measure the served system end to end (protocol
// framing + thread-per-connection accept/dispatch + sharding + fsync-per-
// write WAL), which is exactly what a "single-node throughput / p99 latency"
// resume claim is about. The server is started/stopped by the harness in
// bench/ (single-node or leader+followers); this tool only knows how to
// hammer one host:port. See bench/bench_throughput.sh and
// bench/bench_multinode.sh.
//
// Usage:
//   kvstore_bench_throughput <host> <port> <clients> <ops_per_client> \
//       <workload> <keys_per_client>
//   workload: read-heavy | write-heavy | mixed
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "kv_client.h"
#include "protocol.h"

using namespace std;

namespace {

// A workload is just three cumulative cut points over a 0-99 roll: any op
// whose roll is < get_cut is a GET, < put_cut is a PUT, otherwise a DELETE.
// (write-heavy/read-heavy carry no DELETEs; only "mixed" does — a DELETE
// stream against a preloaded range naturally produces some NOT_FOUND GETs
// afterward, which is realistic and still a served op either way.)
struct Workload {
  int get_cut;
  int put_cut;  // delete share is the remainder up to 100
};

bool parse_workload(const string& name, Workload& out) {
  if (name == "read-heavy") {
    out = {90, 100};  // 90% GET, 10% PUT, 0% DELETE
    return true;
  }
  if (name == "write-heavy") {
    out = {10, 100};  // 10% GET, 90% PUT, 0% DELETE
    return true;
  }
  if (name == "mixed") {
    out = {45, 90};  // 45% GET, 45% PUT, 10% DELETE
    return true;
  }
  return false;
}

string make_key(int client_id, int index) {
  return "c" + to_string(client_id) + "-k" + to_string(index);
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 7) {
    cerr << "usage: " << argv[0]
         << " <host> <port> <clients> <ops_per_client> <workload> <keys_per_client>\n"
         << "  workload: read-heavy | write-heavy | mixed\n";
    return 1;
  }
  string host = argv[1];
  uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
  int clients = atoi(argv[3]);
  int ops_per_client = atoi(argv[4]);
  string workload_name = argv[5];
  int keys_per_client = atoi(argv[6]);

  Workload workload;
  if (!parse_workload(workload_name, workload)) {
    cerr << argv[0] << ": unknown workload '" << workload_name
         << "' (expected read-heavy | write-heavy | mixed)\n";
    return 1;
  }
  if (clients <= 0 || ops_per_client <= 0 || keys_per_client <= 0) {
    cerr << argv[0] << ": clients, ops_per_client, and keys_per_client must all be > 0\n";
    return 1;
  }

  // Preload phase (not timed): every client PUTs its own key range so that
  // GETs in the measured phase hit existing keys. Done up front, serially
  // per client on its own connection, so the measured phase starts from a
  // fully-populated store. A failure here (e.g. the server rejects the write
  // because it's a follower) aborts the whole run rather than silently
  // measuring against an empty keyspace.
  const string kFill = "v";  // small fixed value — see docs/benchmarks.md
  try {
    vector<thread> loaders;
    atomic<bool> preload_failed{false};
    for (int c = 0; c < clients; ++c) {
      loaders.emplace_back([&, c] {
        kvstore::KvClient client(host, port);
        for (int i = 0; i < keys_per_client; ++i) {
          kvstore::Response r = client.send(kvstore::Opcode::kPut, make_key(c, i), kFill);
          if (r.status != kvstore::Status::kOk) {
            preload_failed.store(true);
            return;
          }
        }
      });
    }
    for (auto& t : loaders) t.join();
    if (preload_failed.load()) {
      cerr << argv[0] << ": preload PUT was rejected by the server (is it the leader?)\n";
      return 1;
    }
  } catch (const exception& e) {
    cerr << argv[0] << ": preload failed: " << e.what() << "\n";
    return 1;
  }

  // Measured phase: each client runs ops_per_client ops picked by the
  // workload mix, recording per-op latency (in microseconds) into its own
  // pre-sized vector — no shared state on the hot path, so nothing here
  // contends across client threads except the server itself.
  vector<vector<uint64_t>> latencies(clients);
  for (auto& v : latencies) v.reserve(ops_per_client);
  atomic<bool> run_failed{false};

  auto wall_start = chrono::steady_clock::now();
  {
    vector<thread> workers;
    for (int c = 0; c < clients; ++c) {
      workers.emplace_back([&, c] {
        try {
          kvstore::KvClient client(host, port);
          mt19937 rng(static_cast<unsigned>(c) + 1);
          uniform_int_distribution<int> op_roll(0, 99);
          uniform_int_distribution<int> key_roll(0, keys_per_client - 1);
          auto& out = latencies[c];
          for (int i = 0; i < ops_per_client; ++i) {
            int roll = op_roll(rng);
            string key = make_key(c, key_roll(rng));
            auto op_start = chrono::steady_clock::now();
            if (roll < workload.get_cut) {
              client.send(kvstore::Opcode::kGet, key);
            } else if (roll < workload.put_cut) {
              client.send(kvstore::Opcode::kPut, key, kFill);
            } else {
              client.send(kvstore::Opcode::kDelete, key);
            }
            auto op_us = chrono::duration_cast<chrono::microseconds>(
                             chrono::steady_clock::now() - op_start)
                             .count();
            out.push_back(static_cast<uint64_t>(op_us));
          }
        } catch (const exception& e) {
          cerr << argv[0] << ": client " << c << " failed: " << e.what() << "\n";
          run_failed.store(true);
        }
      });
    }
    for (auto& t : workers) t.join();
  }
  auto wall_end = chrono::steady_clock::now();

  if (run_failed.load()) return 1;

  // Merge every client's latencies into one sorted vector for percentiles.
  vector<uint64_t> all;
  all.reserve(static_cast<size_t>(clients) * ops_per_client);
  for (auto& v : latencies) all.insert(all.end(), v.begin(), v.end());
  sort(all.begin(), all.end());

  auto pct = [&all](double p) -> uint64_t {
    if (all.empty()) return 0;
    // Nearest-rank: the smallest value at or above the p-th percentile.
    size_t idx = static_cast<size_t>(p / 100.0 * all.size());
    if (idx >= all.size()) idx = all.size() - 1;
    return all[idx];
  };

  double elapsed_s = chrono::duration<double>(wall_end - wall_start).count();
  uint64_t total_ops = all.size();
  double throughput = total_ops / elapsed_s;

  cout << fixed << setprecision(3);
  cout << "workload,clients,ops_per_client,total_ops,elapsed_s,throughput_ops_per_sec,"
          "p50_us,p95_us,p99_us\n";
  cout << workload_name << "," << clients << "," << ops_per_client << "," << total_ops << ","
       << elapsed_s << "," << setprecision(0) << throughput << "," << pct(50) << "," << pct(95)
       << "," << pct(99) << "\n";
  return 0;
}
