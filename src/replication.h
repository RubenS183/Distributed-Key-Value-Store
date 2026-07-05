#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "failover.h"
#include "protocol.h"
#include "sharded_store.h"

using namespace std;

namespace kvstore {

// Leader-side replication driver for one follower node. Replicates every
// shard of `store` to the matching shard on that follower over a single
// persistent TCP connection — one ReplicationLink per follower, not per
// shard, since which shard a given REPLICATE record belongs to is already
// fully determined by hashing its key the same way on both ends
// (ShardedStore::apply_replicated()), so no shard_id needs to travel with
// it. Only the CATCHUP_QUERY handshake names a shard explicitly, since it
// isn't tied to any one key.
//
// Runs a single background thread that, per shard, in a loop: connects (or
// reconnects after a drop), asks the follower how far behind it is
// (CATCHUP_QUERY), streams a full snapshot transfer first if the follower
// is behind what the leader's WAL can still replay from, replays the WAL
// tail, then polls for and forwards any new records. This is the
// asynchronous half of "leader persists to its own WAL and acks the client
// immediately; followers replicate asynchronously" — see
// docs/design-decisions.md's "Consistency Model" — and is completely
// decoupled from Store::put()/del()'s call path: nothing in the client
// write path calls into this class, waits on it, or even knows it exists.
class ReplicationLink {
 public:
  // `cluster`, if non-null, makes this link failover-aware: it stamps
  // cluster->epoch() (not the fixed kCurrentTerm) on every REPLICATE/
  // CATCHUP_QUERY frame, stops replicating the moment cluster->role() is no
  // longer kLeader (this node was demoted), and — if a follower rejects a
  // frame with kStaleTerm — adopts the newer epoch the rejection carries and
  // steps this node down itself, rather than treating the rejection as a
  // generic connection failure to retry. Left null (the default), this link
  // behaves exactly as it did in stage 6: a fixed term, and it keeps trying
  // to replicate until stop() is called. See docs/design-decisions.md's
  // "Failover" section.
  ReplicationLink(ShardedStore& store, string follower_host, uint16_t follower_port,
                   ClusterState* cluster = nullptr);
  ~ReplicationLink();

  ReplicationLink(const ReplicationLink&) = delete;
  ReplicationLink& operator=(const ReplicationLink&) = delete;

  // Starts the background thread. Safe to call at most once.
  void start();

  // Signals the background thread to stop, unblocks it if it's parked in a
  // blocking socket call, and joins it. Safe to call more than once (a
  // no-op after the first call) and safe to skip (the destructor calls it).
  void stop();

  // This link's current record of how far the follower has been brought up
  // to date for `shard` — exposed for tests to assert catch-up progress
  // without polling the follower directly or sleeping-and-hoping.
  uint64_t ack_index(ShardId shard) const;

 private:
  void run();
  bool connect_to_follower();
  void disconnect();

  // running_ (still requested to run) and, if a ClusterState was given,
  // still actually the leader — a demoted node has nothing left to
  // replicate. See the constructor's doc comment above.
  bool should_keep_running() const;

  // cluster_ ? cluster_->epoch() : kCurrentTerm — see the constructor's doc
  // comment above.
  uint64_t current_term() const;

  // Sends one request frame and blocks for its matching response. Returns
  // false (connection presumed dead) on any socket error, protocol error,
  // or request_id mismatch.
  bool send_request(uint8_t opcode, const string& payload, Response& out);

  // One-time per (re)connect: learns how far behind `shard` is and brings
  // it fully up to date (snapshot transfer first if needed, then the WAL
  // tail), updating ack_index_[shard] as records are confirmed applied.
  bool catch_up_shard(ShardId shard);

  // Replays and sends every record after `after_write_id` on `shard`,
  // updating ack_index_[shard] as records are confirmed applied. Shared by
  // catch_up_shard() (the one-time backlog) and the steady-state poll loop.
  bool replicate_from(ShardId shard, uint64_t after_write_id);

  bool send_replicated_record(ShardId shard, const Record& record);

  ShardedStore& store_;
  string follower_host_;
  uint16_t follower_port_;
  ClusterState* cluster_;  // nullptr for the stage-6 (non-failover-aware) behavior

  thread thread_;
  atomic<bool> running_{false};
  atomic<int> fd_{-1};
  uint64_t next_request_id_ = 1;

  mutable mutex ack_mutex_;
  vector<uint64_t> ack_index_;
};

}  // namespace kvstore
