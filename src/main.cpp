#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "failover.h"
#include "replication.h"
#include "server.h"
#include "sharded_store.h"

using namespace std;

namespace {
constexpr uint16_t kDefaultPort = 6380;
constexpr const char* kDefaultWalDir = "wal_data";
constexpr const char* kDefaultSnapshotDir = "snapshot_data";
// See docs/design-decisions.md's "Sharding" section for why 8 (a modest,
// fixed fan-out for a single-node deployment, not tuned to any measured
// workload) rather than 1 or a much larger number.
constexpr size_t kDefaultNumShards = 8;
constexpr const char* kDefaultRole = "leader";

// Splits "host:port,host:port,..." into (host, port) pairs. As of stage 7
// this is every *other* node in the cluster, given to every node
// (leader or follower) alike — used for the starting leader's
// ReplicationLinks (as it always was) and, new this stage, for every
// node's FailoverMonitor (heartbeats to peers if leader, election queries
// against peers if a follower that suspects the leader is dead). Empty
// (the default) means "no peers configured" — the server runs exactly as
// it did before failover existed, with no ClusterState/FailoverMonitor at
// all. See docs/architecture.md's "Failover" section.
vector<pair<string, uint16_t>> parse_peer_addrs(const string& csv) {
  vector<pair<string, uint16_t>> addrs;
  stringstream ss(csv);
  string entry;
  while (getline(ss, entry, ',')) {
    if (entry.empty()) continue;
    size_t colon = entry.find(':');
    if (colon == string::npos) {
      throw runtime_error("kvstore_server: malformed peer address '" + entry +
                                "' (expected host:port)");
    }
    string host = entry.substr(0, colon);
    uint16_t port = static_cast<uint16_t>(atoi(entry.substr(colon + 1).c_str()));
    addrs.emplace_back(move(host), port);
  }
  return addrs;
}
}  // namespace

int main(int argc, char** argv) {
  uint16_t port = kDefaultPort;
  if (argc > 1) {
    port = static_cast<uint16_t>(atoi(argv[1]));
  }
  string wal_dir = (argc > 2) ? argv[2] : kDefaultWalDir;
  string snapshot_dir = (argc > 3) ? argv[3] : kDefaultSnapshotDir;
  size_t num_shards =
      (argc > 4) ? static_cast<size_t>(strtoull(argv[4], nullptr, 10)) : kDefaultNumShards;
  string role_arg = (argc > 5) ? argv[5] : kDefaultRole;
  string peer_addrs_arg = (argc > 6) ? argv[6] : "";

  if (role_arg != "leader" && role_arg != "follower") {
    cerr << "kvstore_server: role must be 'leader' or 'follower', got '" << role_arg << "'\n";
    return 1;
  }
  kvstore::ReplicationRole role =
      (role_arg == "follower") ? kvstore::ReplicationRole::kFollower : kvstore::ReplicationRole::kLeader;
  auto peer_pairs = parse_peer_addrs(peer_addrs_arg);

  kvstore::ShardedStore store(num_shards, wal_dir, snapshot_dir);

  // "Ready to serve" (docs/benchmarks.md's restore-time number) is measured
  // as exactly this block: snapshot load + WAL-tail replay per shard, before
  // the server starts accepting connections.
  auto recovery_start = chrono::steady_clock::now();
  size_t snapshot_entries = store.load_snapshots();
  size_t replayed = store.recover_from_wal();
  auto recovery_ms = chrono::duration<double, milli>(
                          chrono::steady_clock::now() - recovery_start)
                          .count();
  // endl (not "\n"): this is a one-time startup diagnostic, not a hot
  // path, and it needs to actually reach the log file before any subsequent
  // crash — cout is fully buffered, not line-buffered, once redirected.
  cout << "kvstore_server: " << num_shards << " shard(s), role=" << role_arg << ", loaded "
            << snapshot_entries << " snapshot entry(ies) from '" << snapshot_dir << "', replayed "
            << replayed << " WAL record(s) from '" << wal_dir << "' in " << recovery_ms << " ms"
            << endl;

  // A ClusterState (and the FailoverMonitor that goes with it) only exists
  // when peers are actually configured — with none, this node has nothing
  // to fail over to or from, so it runs exactly as it did in stage 6: a
  // fixed role for its whole process lifetime, zero extra background
  // threads. See docs/architecture.md's "Failover" section.
  unique_ptr<kvstore::ClusterState> cluster;
  unique_ptr<kvstore::TcpServer> server;
  if (peer_pairs.empty()) {
    server = make_unique<kvstore::TcpServer>(store, port, role);
  } else {
    cluster = make_unique<kvstore::ClusterState>(role, kvstore::kCurrentTerm);
    server = make_unique<kvstore::TcpServer>(store, port, *cluster);
  }

  try {
    server->start();
  } catch (const exception& e) {
    cerr << "kvstore_server: failed to start: " << e.what() << "\n";
    return 1;
  }

  // Replication links replicate data to every peer; only meaningful for the
  // node that starts out as leader (stage 6: single leader, N followers, no
  // cascading follower-of-follower chains). A follower promoted later gets
  // its own links started by its FailoverMonitor (see promote_self()).
  vector<unique_ptr<kvstore::ReplicationLink>> links;
  if (role == kvstore::ReplicationRole::kLeader) {
    for (const auto& [host, peer_port] : peer_pairs) {
      auto link = make_unique<kvstore::ReplicationLink>(store, host, peer_port, cluster.get());
      link->start();
      links.push_back(move(link));
    }
  }

  // The FailoverMonitor sends heartbeats (while leader) or watches for a
  // heartbeat timeout and calls an election (while follower) against every
  // peer — see docs/design-decisions.md's "Failover" section.
  unique_ptr<kvstore::FailoverMonitor> monitor;
  if (cluster) {
    vector<kvstore::PeerAddr> peers;
    peers.reserve(peer_pairs.size());
    for (const auto& [host, peer_port] : peer_pairs) peers.push_back({host, peer_port});
    monitor = make_unique<kvstore::FailoverMonitor>(store, *cluster, move(peers), port);
    monitor->start();
  }

  cout << "kvstore_server listening on port " << server->port()
            << " (thread-per-connection)\n";
  server->run();
  return 0;
}
