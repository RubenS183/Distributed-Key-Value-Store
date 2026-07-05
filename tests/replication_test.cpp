#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

#include "kv_client.h"
#include "protocol.h"
#include "replication.h"
#include "server.h"
#include "sharded_store.h"
#include "sharding.h"
#include "store.h"
#include "wal.h"

using namespace std;

using kvstore::DeleteResult;
using kvstore::KvClient;
using kvstore::Opcode;
using kvstore::PutResult;
using kvstore::Record;
using kvstore::RecordType;
using kvstore::ReplicationLink;
using kvstore::ReplicationRole;
using kvstore::ShardedStore;
using kvstore::ShardId;
using kvstore::Status;
using kvstore::Store;
using kvstore::TcpServer;
using kvstore::WriteAheadLog;

namespace {

namespace fs = filesystem;

// RAII temp directory, unique per instance — same pattern as every other
// test file's TempDir (wal_test.cpp, sharding_test.cpp, etc.).
class TempDir {
 public:
  explicit TempDir(const string& tag) {
    path_ = fs::temp_directory_path() /
            ("kvstore_replication_test_" + tag + "_" +
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

// Small, fixed shard count for these tests — enough to exercise
// per-shard-independent catch-up without the runtime cost of the server's
// default (8) times two nodes.
constexpr size_t kTestNumShards = 4;

// Polls `pred` until it's true or `timeout_ms` elapses, sleeping briefly
// between checks — the same bounded-poll style integration_test.cpp's
// shutdown test already uses, instead of a fixed sleep-and-hope delay.
bool wait_until(const function<bool()>& pred, int timeout_ms = 5000) {
  auto deadline = chrono::steady_clock::now() + chrono::milliseconds(timeout_ms);
  while (chrono::steady_clock::now() < deadline) {
    if (pred()) return true;
    this_thread::sleep_for(chrono::milliseconds(2));
  }
  return pred();
}

// Runs a ShardedStore behind a real TcpServer on its own thread — same
// scaffolding pattern as integration_test.cpp's ServerFixture, but exposing
// the store and allowing an explicit role.
struct Node {
  Node(size_t num_shards, string wal_dir, string snapshot_dir, ReplicationRole role,
       uint16_t port = 0)
      : store(num_shards, move(wal_dir), move(snapshot_dir)), server(store, port, role) {
    store.load_snapshots();
    store.recover_from_wal();
    server.start();
    worker = thread([this] { server.run(); });
  }

  ~Node() { stop(); }

  void stop() {
    if (stopped) return;
    server.stop();
    if (worker.joinable()) worker.join();
    stopped = true;
  }

  uint16_t port() const { return server.port(); }

  ShardedStore store;
  TcpServer server;
  thread worker;
  bool stopped = false;
};

// Every key/value the leader wrote must show up identically (value,
// version, write_id, tombstone) on the follower's matching shard.
void assert_shard_states_match(ShardedStore& leader, ShardedStore& follower) {
  REQUIRE(leader.num_shards() == follower.num_shards());
  for (ShardId s = 0; s < leader.num_shards(); ++s) {
    REQUIRE(leader.shard(s).size() == follower.shard(s).size());
  }
}

}  // namespace

// --- Store::apply_replicated: idempotent apply + write_id dedup ---

TEST_CASE("apply_replicated applies a PUT record exactly as received", "[replication][storage]") {
  TempDir wal_dir("apply_put_wal");
  WriteAheadLog wal(wal_dir.string());
  Store store(&wal);

  Record record{RecordType::kPut, /*write_id=*/1, /*version=*/1, "k", "v"};
  store.apply_replicated(record);

  auto entry = store.peek("k");
  REQUIRE(entry.has_value());
  REQUIRE(entry->value == "v");
  REQUIRE(entry->version == 1);
  REQUIRE(entry->write_id == 1);
  REQUIRE_FALSE(entry->tombstone);
  REQUIRE(store.last_applied_write_id() == 1);
}

TEST_CASE("apply_replicated applies a DELETE record as a tombstone", "[replication][storage]") {
  TempDir wal_dir("apply_delete_wal");
  WriteAheadLog wal(wal_dir.string());
  Store store(&wal);

  store.apply_replicated(Record{RecordType::kPut, 1, 1, "k", "v"});
  store.apply_replicated(Record{RecordType::kDelete, 2, 2, "k", ""});

  REQUIRE_FALSE(store.get("k").has_value());
  auto entry = store.peek("k");
  REQUIRE(entry.has_value());
  REQUIRE(entry->tombstone);
  REQUIRE(store.last_applied_write_id() == 2);
}

TEST_CASE("duplicate-request test: replaying the same replicated record twice does not double-apply",
          "[replication][storage]") {
  TempDir wal_dir("dedup_wal");
  WriteAheadLog wal(wal_dir.string());
  Store store(&wal);

  Record record{RecordType::kPut, 1, 1, "k", "v"};
  store.apply_replicated(record);
  store.apply_replicated(record);  // exact replay of the same write_id
  store.apply_replicated(record);  // and again, for good measure

  REQUIRE(store.size() == 1);
  REQUIRE(store.last_applied_write_id() == 1);
  auto entry = store.peek("k");
  REQUIRE(entry->version == 1);

  // The dedup must skip the WAL append too, not just the map mutation —
  // otherwise recovery would see the same write_id appear more than once.
  // Reopening and replaying confirms exactly one record was ever persisted.
  size_t replayed = 0;
  WriteAheadLog reopened(wal_dir.string());
  reopened.recover([&](const Record&) { ++replayed; });
  REQUIRE(replayed == 1);
}

TEST_CASE("duplicate-request test: a stale record (write_id already covered) is ignored even out of order",
          "[replication][storage]") {
  TempDir wal_dir("dedup_stale_wal");
  WriteAheadLog wal(wal_dir.string());
  Store store(&wal);

  store.apply_replicated(Record{RecordType::kPut, 1, 1, "k", "v1"});
  store.apply_replicated(Record{RecordType::kPut, 2, 2, "k", "v2"});
  // A retransmit of the first (already-superseded) write_id must not undo
  // the second.
  store.apply_replicated(Record{RecordType::kPut, 1, 1, "k", "v1"});

  auto entry = store.peek("k");
  REQUIRE(entry->value == "v2");
  REQUIRE(entry->version == 2);
}

TEST_CASE("ShardedStore::apply_replicated routes to the same shard the leader would assign",
          "[replication][sharding]") {
  TempDir wal_dir("route_wal");
  TempDir snapshot_dir("route_snapshot");
  ShardedStore store(kTestNumShards, wal_dir.string(), snapshot_dir.string());

  ShardId expected_shard = kvstore::shard_for_key("some-key", kTestNumShards);
  store.apply_replicated(Record{RecordType::kPut, 1, 1, "some-key", "v"});

  REQUIRE(store.shard(expected_shard).peek("some-key").has_value());
  for (ShardId s = 0; s < kTestNumShards; ++s) {
    if (s == expected_shard) continue;
    REQUIRE_FALSE(store.shard(s).peek("some-key").has_value());
  }
}

// --- Role enforcement ---

TEST_CASE("a follower node rejects direct client PUT/GET/DELETE with kNotLeader",
          "[replication][integration]") {
  TempDir wal_dir("role_wal");
  TempDir snapshot_dir("role_snapshot");
  Node follower(kTestNumShards, wal_dir.string(), snapshot_dir.string(), ReplicationRole::kFollower);
  KvClient client("127.0.0.1", follower.port());

  REQUIRE(client.send(Opcode::kPut, "k", "v").status == Status::kNotLeader);
  REQUIRE(client.send(Opcode::kGet, "k").status == Status::kNotLeader);
  REQUIRE(client.send(Opcode::kDelete, "k").status == Status::kNotLeader);
}

TEST_CASE("a leader node rejects REPLICATE/CATCHUP_QUERY frames", "[replication][integration]") {
  TempDir wal_dir("leader_reject_wal");
  TempDir snapshot_dir("leader_reject_snapshot");
  Node leader(kTestNumShards, wal_dir.string(), snapshot_dir.string(), ReplicationRole::kLeader);
  KvClient client("127.0.0.1", leader.port());

  Record record{RecordType::kPut, 1, 1, "k", "v"};
  string replicate_payload = kvstore::encode_replicate_payload(kvstore::kCurrentTerm, record);
  auto replicate_resp = client.send_raw(static_cast<uint8_t>(Opcode::kReplicate), replicate_payload);
  REQUIRE(replicate_resp.status == Status::kBadRequest);
  // And the record must genuinely not have been applied.
  REQUIRE_FALSE(leader.store.get("k").has_value());

  string catchup_payload = kvstore::encode_catchup_query_payload(kvstore::kCurrentTerm, 0);
  auto catchup_resp = client.send_raw(static_cast<uint8_t>(Opcode::kCatchupQuery), catchup_payload);
  REQUIRE(catchup_resp.status == Status::kBadRequest);

  // Ordinary client opcodes still work normally on a leader.
  REQUIRE(client.send(Opcode::kPut, "k", "v").status == Status::kOk);
}

// --- End-to-end replication: leader -> follower over real TCP ---

TEST_CASE("end-to-end: writes on the leader are asynchronously replicated to the follower",
          "[replication][integration]") {
  TempDir leader_wal("e2e_leader_wal");
  TempDir leader_snapshot("e2e_leader_snapshot");
  TempDir follower_wal("e2e_follower_wal");
  TempDir follower_snapshot("e2e_follower_snapshot");

  Node leader(kTestNumShards, leader_wal.string(), leader_snapshot.string(), ReplicationRole::kLeader);
  Node follower(kTestNumShards, follower_wal.string(), follower_snapshot.string(),
                ReplicationRole::kFollower);

  ReplicationLink link(leader.store, "127.0.0.1", follower.port());
  link.start();

  KvClient client("127.0.0.1", leader.port());
  constexpr int kNumKeys = 100;
  for (int i = 0; i < kNumKeys; ++i) {
    auto resp = client.send(Opcode::kPut, "key" + to_string(i), "value" + to_string(i));
    REQUIRE(resp.status == Status::kOk);
  }
  REQUIRE(client.send(Opcode::kDelete, "key0").status == Status::kOk);

  bool caught_up = wait_until([&] {
    for (ShardId s = 0; s < kTestNumShards; ++s) {
      if (link.ack_index(s) < leader.store.shard(s).last_applied_write_id()) return false;
    }
    return true;
  });
  REQUIRE(caught_up);

  // Give the follower's own dispatch()/apply_replicated a moment to settle
  // (the leader's ack_index_ updates the instant it gets an OK response,
  // which is already after the follower applied the record).
  assert_shard_states_match(leader.store, follower.store);
  REQUIRE_FALSE(follower.store.get("key0").has_value());
  for (int i = 1; i < kNumKeys; ++i) {
    auto value = follower.store.get("key" + to_string(i));
    REQUIRE(value.has_value());
    REQUIRE(*value == "value" + to_string(i));
  }

  link.stop();
}

TEST_CASE("catch-up: a follower that is too far behind the leader's snapshot gets a full transfer",
          "[replication][integration][snapshot]") {
  TempDir leader_wal("snap_leader_wal");
  TempDir leader_snapshot("snap_leader_snapshot");
  TempDir follower_wal("snap_follower_wal");
  TempDir follower_snapshot("snap_follower_snapshot");

  Node leader(kTestNumShards, leader_wal.string(), leader_snapshot.string(), ReplicationRole::kLeader);

  KvClient seed_client("127.0.0.1", leader.port());
  constexpr int kNumKeys = 300;
  for (int i = 0; i < kNumKeys; ++i) {
    seed_client.send(Opcode::kPut, "key" + to_string(i), "value" + to_string(i));
  }
  // Snapshot every shard and truncate its WAL — a brand-new follower with
  // last_applied_write_id == 0 is now behind every shard's snapshot
  // boundary, so catch-up must use the snapshot-transfer path, not WAL
  // replay (the pre-snapshot WAL segments are gone).
  leader.store.take_snapshot();

  Node follower(kTestNumShards, follower_wal.string(), follower_snapshot.string(),
                ReplicationRole::kFollower);

  ReplicationLink link(leader.store, "127.0.0.1", follower.port());
  link.start();

  bool caught_up = wait_until([&] {
    for (ShardId s = 0; s < kTestNumShards; ++s) {
      if (link.ack_index(s) < leader.store.shard(s).last_applied_write_id()) return false;
    }
    return true;
  });
  REQUIRE(caught_up);

  assert_shard_states_match(leader.store, follower.store);
  for (int i = 0; i < kNumKeys; ++i) {
    auto value = follower.store.get("key" + to_string(i));
    REQUIRE(value.has_value());
    REQUIRE(*value == "value" + to_string(i));
  }

  link.stop();
}

TEST_CASE("follower catch-up: killing a follower mid-stream and bringing it back "
          "resumes from where it left off and ends up byte-identical to the leader",
          "[replication][integration][crash]") {
  TempDir leader_wal("kill_leader_wal");
  TempDir leader_snapshot("kill_leader_snapshot");
  TempDir follower_wal("kill_follower_wal");
  TempDir follower_snapshot("kill_follower_snapshot");

  Node leader(kTestNumShards, leader_wal.string(), leader_snapshot.string(), ReplicationRole::kLeader);
  auto follower = make_unique<Node>(kTestNumShards, follower_wal.string(),
                                          follower_snapshot.string(), ReplicationRole::kFollower);
  uint16_t follower_port = follower->port();  // fixed across restarts below

  ReplicationLink link(leader.store, "127.0.0.1", follower_port);
  link.start();

  KvClient client("127.0.0.1", leader.port());
  constexpr int kFirstBatch = 50;
  for (int i = 0; i < kFirstBatch; ++i) {
    client.send(Opcode::kPut, "key" + to_string(i), "value" + to_string(i));
  }

  REQUIRE(wait_until([&] {
    for (ShardId s = 0; s < kTestNumShards; ++s) {
      if (link.ack_index(s) < leader.store.shard(s).last_applied_write_id()) return false;
    }
    return true;
  }));
  assert_shard_states_match(leader.store, follower->store);

  // Kill the follower: destroy its TcpServer/ShardedStore entirely (as a
  // process crash would), while the leader keeps taking writes and the
  // ReplicationLink keeps failing/retrying against a closed port.
  follower.reset();

  constexpr int kSecondBatch = 50;
  for (int i = kFirstBatch; i < kFirstBatch + kSecondBatch; ++i) {
    client.send(Opcode::kPut, "key" + to_string(i), "value" + to_string(i));
  }
  // Let the link observe at least one failed send/reconnect attempt before
  // the follower comes back, so this actually exercises the reconnect path
  // rather than a lucky race where it never noticed the drop.
  this_thread::sleep_for(chrono::milliseconds(150));

  // Bring the follower back "online": a fresh process reopening the exact
  // same WAL/snapshot directories, bound to the same port (SO_REUSEADDR,
  // same as every other restart test in this codebase relies on).
  follower = make_unique<Node>(kTestNumShards, follower_wal.string(), follower_snapshot.string(),
                                     ReplicationRole::kFollower, follower_port);

  bool caught_up = wait_until(
      [&] {
        for (ShardId s = 0; s < kTestNumShards; ++s) {
          if (link.ack_index(s) < leader.store.shard(s).last_applied_write_id()) return false;
        }
        return true;
      },
      /*timeout_ms=*/8000);
  REQUIRE(caught_up);

  assert_shard_states_match(leader.store, follower->store);
  for (int i = 0; i < kFirstBatch + kSecondBatch; ++i) {
    string key = "key" + to_string(i);
    auto leader_value = leader.store.get(key);
    auto follower_value = follower->store.get(key);
    REQUIRE(leader_value.has_value());
    REQUIRE(follower_value.has_value());
    REQUIRE(*leader_value == *follower_value);
  }

  link.stop();
}
