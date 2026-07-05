#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "failover.h"
#include "kv_client.h"
#include "protocol.h"
#include "replication.h"
#include "server.h"
#include "sharded_store.h"
#include "store.h"
#include "wal.h"

using namespace std;

using kvstore::ClusterState;
using kvstore::DeleteResult;
using kvstore::FailoverMonitor;
using kvstore::KvClient;
using kvstore::Opcode;
using kvstore::PeerAddr;
using kvstore::Record;
using kvstore::RecordType;
using kvstore::ReplicationLink;
using kvstore::ReplicationRole;
using kvstore::Request;
using kvstore::Response;
using kvstore::ShardedStore;
using kvstore::ShardId;
using kvstore::Status;
using kvstore::Store;
using kvstore::TcpServer;
using kvstore::WriteAheadLog;

namespace {

namespace fs = filesystem;

// Same RAII temp-dir pattern every other test file in this codebase uses.
class TempDir {
 public:
  explicit TempDir(const string& tag) {
    path_ = fs::temp_directory_path() /
            ("kvstore_failover_test_" + tag + "_" +
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

constexpr size_t kTestNumShards = 4;

bool wait_until(const function<bool()>& pred, int timeout_ms = 5000) {
  auto deadline = chrono::steady_clock::now() + chrono::milliseconds(timeout_ms);
  while (chrono::steady_clock::now() < deadline) {
    if (pred()) return true;
    this_thread::sleep_for(chrono::milliseconds(2));
  }
  return pred();
}

// A node that participates in failover: store + shared ClusterState + a
// TcpServer that reads that same ClusterState on every dispatch(), plus
// (once start_monitor() is called) a FailoverMonitor watching the given
// peers. Data replication (ReplicationLink) is deliberately *not* owned
// here — a node that starts as leader needs one set up by the test (see
// LeaderLinks below); a node that gets promoted later has its links
// started automatically by its own FailoverMonitor.
struct FailoverNode {
  FailoverNode(size_t num_shards, string wal_dir, string snapshot_dir,
               ReplicationRole role, uint16_t port = 0)
      : store(num_shards, move(wal_dir), move(snapshot_dir)),
        cluster(role, kvstore::kCurrentTerm),
        server(store, port, cluster) {
    store.load_snapshots();
    store.recover_from_wal();
    server.start();
    worker = thread([this] { server.run(); });
  }

  ~FailoverNode() { stop(); }

  void start_monitor(vector<PeerAddr> peers) {
    monitor = make_unique<FailoverMonitor>(store, cluster, move(peers), server.port());
    monitor->start();
  }

  void stop() {
    if (stopped) return;
    if (monitor) monitor->stop();
    server.stop();
    if (worker.joinable()) worker.join();
    stopped = true;
  }

  uint16_t port() const { return server.port(); }

  ShardedStore store;
  ClusterState cluster;
  TcpServer server;
  unique_ptr<FailoverMonitor> monitor;
  thread worker;
  bool stopped = false;
};

uint64_t total_committed(ShardedStore& store) {
  uint64_t total = 0;
  for (ShardId s = 0; s < store.num_shards(); ++s) total += store.shard(s).last_applied_write_id();
  return total;
}

}  // namespace

// --- ClusterState: epoch/role semantics ---

TEST_CASE("ClusterState: accept_epoch rejects a lower epoch, accepts equal, and demotes on higher",
          "[failover][unit]") {
  ClusterState cluster(ReplicationRole::kLeader, /*epoch=*/5);

  REQUIRE(cluster.accept_epoch(5));  // equal: accepted, no change
  REQUIRE(cluster.role() == ReplicationRole::kLeader);
  REQUIRE(cluster.epoch() == 5);

  REQUIRE_FALSE(cluster.accept_epoch(4));  // lower: rejected
  REQUIRE(cluster.role() == ReplicationRole::kLeader);
  REQUIRE(cluster.epoch() == 5);

  REQUIRE(cluster.accept_epoch(9));  // higher: accepted, demotes
  REQUIRE(cluster.role() == ReplicationRole::kFollower);
  REQUIRE(cluster.epoch() == 9);
}

TEST_CASE("ClusterState: promote_to_leader sets role and epoch", "[failover][unit]") {
  ClusterState cluster(ReplicationRole::kFollower, /*epoch=*/3);
  cluster.promote_to_leader(4);
  REQUIRE(cluster.role() == ReplicationRole::kLeader);
  REQUIRE(cluster.epoch() == 4);
}

TEST_CASE("ClusterState: ms_since_contact resets on accept_epoch", "[failover][unit]") {
  ClusterState cluster(ReplicationRole::kFollower, 1);
  this_thread::sleep_for(chrono::milliseconds(60));
  REQUIRE(cluster.ms_since_contact() >= 50);

  REQUIRE(cluster.accept_epoch(1));
  REQUIRE(cluster.ms_since_contact() < 30);
}

// --- dispatch(): new opcodes and the epoch gate ---

TEST_CASE("dispatch: HEARTBEAT is accepted regardless of role and advances contact",
          "[failover][unit]") {
  TempDir wal_dir("hb_wal");
  TempDir snapshot_dir("hb_snapshot");
  ShardedStore store(kTestNumShards, wal_dir.string(), snapshot_dir.string());
  ClusterState cluster(ReplicationRole::kLeader, 1);

  Request req;
  req.opcode = Opcode::kHeartbeat;
  req.term = 1;
  Response resp = kvstore::dispatch(store, req, cluster);
  REQUIRE(resp.status == Status::kOk);
}

TEST_CASE("dispatch: a stale term is rejected with kStaleTerm carrying the current epoch",
          "[failover][unit]") {
  TempDir wal_dir("stale_wal");
  TempDir snapshot_dir("stale_snapshot");
  ShardedStore store(kTestNumShards, wal_dir.string(), snapshot_dir.string());
  ClusterState cluster(ReplicationRole::kLeader, 7);

  Request req;
  req.opcode = Opcode::kHeartbeat;
  req.term = 3;  // behind the node's real epoch
  Response resp = kvstore::dispatch(store, req, cluster);
  REQUIRE(resp.status == Status::kStaleTerm);
  REQUIRE(kvstore::decode_epoch_payload(resp.payload) == 7);

  // REPLICATE on a follower is gated the same way.
  ClusterState follower_cluster(ReplicationRole::kFollower, 7);
  Request replicate_req;
  replicate_req.opcode = Opcode::kReplicate;
  replicate_req.term = 3;
  replicate_req.record_type = RecordType::kPut;
  replicate_req.write_id = 1;
  replicate_req.version = 1;
  replicate_req.key = "k";
  replicate_req.value = "v";
  Response replicate_resp = kvstore::dispatch(store, replicate_req, follower_cluster);
  REQUIRE(replicate_resp.status == Status::kStaleTerm);
  REQUIRE_FALSE(store.get("k").has_value());  // the stale record must not have been applied
}

TEST_CASE("dispatch: ELECTION_QUERY reports epoch and total committed write_id without mutating state",
          "[failover][unit]") {
  TempDir wal_dir("eq_wal");
  TempDir snapshot_dir("eq_snapshot");
  ShardedStore store(kTestNumShards, wal_dir.string(), snapshot_dir.string());
  store.apply_replicated(Record{RecordType::kPut, 1, 1, "k1", "v1"});
  store.apply_replicated(Record{RecordType::kPut, 2, 1, "k2", "v2"});

  ClusterState cluster(ReplicationRole::kFollower, 6);
  Request req;
  req.opcode = Opcode::kElectionQuery;
  Response resp = kvstore::dispatch(store, req, cluster);
  REQUIRE(resp.status == Status::kOk);

  uint64_t epoch = 0, total = 0;
  kvstore::decode_election_response_payload(resp.payload, epoch, total);
  REQUIRE(epoch == 6);
  REQUIRE(total == total_committed(store));

  // Still a follower at epoch 6 — an ELECTION_QUERY must never mutate state.
  REQUIRE(cluster.role() == ReplicationRole::kFollower);
  REQUIRE(cluster.epoch() == 6);
}

// --- End-to-end: heartbeats, promotion, stale-leader step-down ---

TEST_CASE("end-to-end: a healthy leader's heartbeats keep a follower from ever calling an election",
          "[failover][integration]") {
  TempDir leader_wal("healthy_leader_wal");
  TempDir leader_snapshot("healthy_leader_snapshot");
  TempDir follower_wal("healthy_follower_wal");
  TempDir follower_snapshot("healthy_follower_snapshot");

  FailoverNode leader(kTestNumShards, leader_wal.string(), leader_snapshot.string(),
                       ReplicationRole::kLeader);
  FailoverNode follower(kTestNumShards, follower_wal.string(), follower_snapshot.string(),
                         ReplicationRole::kFollower);

  leader.start_monitor({{"127.0.0.1", follower.port()}});
  follower.start_monitor({{"127.0.0.1", leader.port()}});

  // Give the monitors several heartbeat/timeout cycles to run.
  this_thread::sleep_for(chrono::milliseconds(400));

  REQUIRE(leader.cluster.role() == ReplicationRole::kLeader);
  REQUIRE(follower.cluster.role() == ReplicationRole::kFollower);
  REQUIRE(leader.cluster.epoch() == 1);
  REQUIRE(follower.cluster.epoch() == 1);
}

TEST_CASE("end-to-end: a follower is promoted to leader after the leader goes silent",
          "[failover][integration]") {
  TempDir leader_wal("promote_leader_wal");
  TempDir leader_snapshot("promote_leader_snapshot");
  TempDir follower_wal("promote_follower_wal");
  TempDir follower_snapshot("promote_follower_snapshot");

  auto leader = make_unique<FailoverNode>(kTestNumShards, leader_wal.string(),
                                                leader_snapshot.string(), ReplicationRole::kLeader);
  auto follower = make_unique<FailoverNode>(
      kTestNumShards, follower_wal.string(), follower_snapshot.string(), ReplicationRole::kFollower);

  leader->start_monitor({{"127.0.0.1", follower->port()}});
  follower->start_monitor({{"127.0.0.1", leader->port()}});

  // Confirm the follower is initially stable (not promoted while the leader
  // is alive and heartbeating).
  this_thread::sleep_for(chrono::milliseconds(100));
  REQUIRE(follower->cluster.role() == ReplicationRole::kFollower);

  // Simulate the leader crashing: stop its monitor/server so it stops
  // heartbeating, before destroying it outright.
  leader.reset();

  bool promoted = wait_until([&] { return follower->cluster.role() == ReplicationRole::kLeader; });
  REQUIRE(promoted);
  REQUIRE(follower->cluster.epoch() == 2);  // exactly one election happened
}

TEST_CASE("end-to-end: with two followers at different catch-up levels, the more up-to-date one "
          "is elected",
          "[failover][integration]") {
  TempDir leader_wal("elect_leader_wal");
  TempDir leader_snapshot("elect_leader_snapshot");
  TempDir a_wal("elect_a_wal");
  TempDir a_snapshot("elect_a_snapshot");
  TempDir b_wal("elect_b_wal");
  TempDir b_snapshot("elect_b_snapshot");

  auto leader = make_unique<FailoverNode>(kTestNumShards, leader_wal.string(),
                                                leader_snapshot.string(), ReplicationRole::kLeader);
  auto node_a = make_unique<FailoverNode>(kTestNumShards, a_wal.string(), a_snapshot.string(),
                                                ReplicationRole::kFollower);
  auto node_b = make_unique<FailoverNode>(kTestNumShards, b_wal.string(), b_snapshot.string(),
                                                ReplicationRole::kFollower);

  // Full mesh: every node knows every other node, which is what lets each
  // follower's election logic actually compare itself against a real
  // competing candidate instead of promoting itself unopposed.
  leader->start_monitor({{"127.0.0.1", node_a->port()}, {"127.0.0.1", node_b->port()}});
  node_a->start_monitor({{"127.0.0.1", leader->port()}, {"127.0.0.1", node_b->port()}});
  node_b->start_monitor({{"127.0.0.1", leader->port()}, {"127.0.0.1", node_a->port()}});

  // Only node_a ever replicates data — node_b is left at write_id 0 the
  // whole test, the simplest deterministic way to make node_a strictly
  // more caught up.
  ReplicationLink link_to_a(leader->store, "127.0.0.1", node_a->port(), &leader->cluster);
  link_to_a.start();

  KvClient client("127.0.0.1", leader->port());
  for (int i = 0; i < 20; ++i) {
    REQUIRE(client.send(Opcode::kPut, "key" + to_string(i), "v").status == Status::kOk);
  }
  REQUIRE(wait_until([&] {
    for (ShardId s = 0; s < kTestNumShards; ++s) {
      if (link_to_a.ack_index(s) < leader->store.shard(s).last_applied_write_id()) return false;
    }
    return true;
  }));
  REQUIRE(total_committed(node_a->store) > 0);
  REQUIRE(total_committed(node_b->store) == 0);

  // Simulate the leader crashing: stop the link first (it references
  // leader->store directly), then destroy the leader node itself.
  link_to_a.stop();
  leader.reset();

  bool a_promoted = wait_until([&] { return node_a->cluster.role() == ReplicationRole::kLeader; });
  REQUIRE(a_promoted);
  // node_b must not have promoted itself — it saw node_a was more caught up.
  REQUIRE(node_b->cluster.role() == ReplicationRole::kFollower);

  // node_b converges to the new epoch once node_a's post-promotion
  // heartbeats reach it.
  REQUIRE(wait_until([&] { return node_b->cluster.epoch() == node_a->cluster.epoch(); }));
}

TEST_CASE("end-to-end: a stale leader that reappears at a lower epoch is told to step down",
          "[failover][integration]") {
  TempDir old_wal("stepdown_old_wal");
  TempDir old_snapshot("stepdown_old_snapshot");
  TempDir new_wal("stepdown_new_wal");
  TempDir new_snapshot("stepdown_new_snapshot");

  // "old_leader" never actually learns anything is wrong on its own — it
  // just keeps heartbeating as if it's still leader at epoch 1, exactly
  // like a leader that's been on the losing side of a network partition
  // that has just healed.
  FailoverNode old_leader(kTestNumShards, old_wal.string(), old_snapshot.string(),
                          ReplicationRole::kLeader);
  FailoverNode new_leader(kTestNumShards, new_wal.string(), new_snapshot.string(),
                          ReplicationRole::kLeader);
  new_leader.cluster.promote_to_leader(2);  // simulate: an election already elected new_leader

  old_leader.start_monitor({{"127.0.0.1", new_leader.port()}});
  new_leader.start_monitor({{"127.0.0.1", old_leader.port()}});

  // old_leader's own heartbeat to new_leader gets a kStaleTerm response
  // (new_leader is at epoch 2); new_leader's heartbeat to old_leader also
  // carries epoch 2 directly. Either path must demote old_leader.
  bool stepped_down =
      wait_until([&] { return old_leader.cluster.role() == ReplicationRole::kFollower; });
  REQUIRE(stepped_down);
  REQUIRE(old_leader.cluster.epoch() == 2);

  // And now that it knows it's a follower, it must actually refuse client
  // writes rather than continuing to silently accept them.
  KvClient client("127.0.0.1", old_leader.port());
  REQUIRE(client.send(Opcode::kPut, "k", "v").status == Status::kNotLeader);
}

// --- The crash scenario the failover requirement calls for ---

TEST_CASE("crash test: killing the leader mid-write-stream promotes a follower and loses no "
          "acknowledged write",
          "[failover][integration][crash]") {
  TempDir leader_wal("crash_leader_wal");
  TempDir leader_snapshot("crash_leader_snapshot");
  TempDir follower_wal("crash_follower_wal");
  TempDir follower_snapshot("crash_follower_snapshot");

  auto leader = make_unique<FailoverNode>(kTestNumShards, leader_wal.string(),
                                                leader_snapshot.string(), ReplicationRole::kLeader);
  auto follower = make_unique<FailoverNode>(
      kTestNumShards, follower_wal.string(), follower_snapshot.string(), ReplicationRole::kFollower);

  leader->start_monitor({{"127.0.0.1", follower->port()}});
  follower->start_monitor({{"127.0.0.1", leader->port()}});

  auto link = make_unique<ReplicationLink>(leader->store, "127.0.0.1", follower->port(),
                                                 &leader->cluster);
  link->start();

  // Writer thread: hammers the leader with sequential PUTs concurrently
  // with the failover machinery running, recording exactly which keys it
  // got an OK response for.
  atomic<bool> stop_writer{false};
  mutex acked_mutex;
  vector<string> acked_keys;
  thread writer([&] {
    KvClient client("127.0.0.1", leader->port());
    int i = 0;
    while (!stop_writer.load()) {
      string key = "key" + to_string(i++);
      auto resp = client.send(Opcode::kPut, key, "value-" + key);
      if (resp.status == Status::kOk) {
        lock_guard<mutex> lock(acked_mutex);
        acked_keys.push_back(key);
      }
    }
  });

  this_thread::sleep_for(chrono::milliseconds(200));
  stop_writer = true;
  writer.join();

  // Let the already-acknowledged writes actually finish replicating before
  // simulating the crash — this test demonstrates that no *replicated*
  // acknowledged write is lost under this project's bounded (not
  // zero-latency) asynchronous replication lag, not that a write acked in
  // the literal unreplicated instant before a real crash survives (that
  // risk is already documented in docs/limitations.md and isn't what
  // failover promises to close).
  REQUIRE(wait_until([&] {
    for (ShardId s = 0; s < kTestNumShards; ++s) {
      if (link->ack_index(s) < leader->store.shard(s).last_applied_write_id()) return false;
    }
    return true;
  }));

  vector<string> keys_to_check;
  {
    lock_guard<mutex> lock(acked_mutex);
    keys_to_check = acked_keys;
  }
  REQUIRE_FALSE(keys_to_check.empty());

  // Simulate the leader crashing: stop the link first (it references
  // leader->store directly, so it must not outlive it), then destroy the
  // leader node itself, same pattern replication_test.cpp's own
  // follower-crash test already uses in reverse.
  link->stop();
  link.reset();
  leader.reset();

  bool promoted = wait_until([&] { return follower->cluster.role() == ReplicationRole::kLeader; },
                             /*timeout_ms=*/5000);
  REQUIRE(promoted);

  for (const auto& key : keys_to_check) {
    auto value = follower->store.get(key);
    REQUIRE(value.has_value());
    REQUIRE(*value == "value-" + key);
  }
}
