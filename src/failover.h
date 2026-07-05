#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "protocol.h"
#include "sharded_store.h"

using namespace std;

namespace kvstore {

// The epoch every node starts at before any election has ever happened.
// Also what a ReplicationLink stamps on REPLICATE/CATCHUP_QUERY frames when
// it isn't given a ClusterState (see ReplicationLink's constructor in
// replication.h) — every pre-stage-7 caller, and every test that doesn't
// exercise failover, since without a ClusterState there is no election that
// could ever produce a second epoch.
inline constexpr uint64_t kCurrentTerm = 1;

class ReplicationLink;  // failover.cpp only; kept out of this header to avoid a
                         // failover.h <-> replication.h include cycle (replication.h
                         // needs ClusterState* for its optional step-down check).

// Which role this node plays for the shards it hosts. Mutable at runtime as
// of stage 7 (failover) — a follower can be promoted to leader, and a stale
// leader that reappears after a partition is told to step down. Before
// stage 7 this was fixed for a node's whole process lifetime; see
// docs/design-decisions.md's "Failover" section for what changed and why.
enum class ReplicationRole { kLeader, kFollower };

// One cluster peer's fixed address, known at startup. Membership is static
// — there is no join/leave protocol, matching every other "no dynamic
// membership" choice this project has made so far (ShardMap::owning_node(),
// the fixed follower list, etc.) — see docs/limitations.md.
struct PeerAddr {
  string host;
  uint16_t port = 0;
};

// Shared, mutable per-node cluster state: this node's role, its current
// epoch, and when it last heard from a current-or-newer epoch. Every node
// (leader or follower) owns exactly one instance; dispatch() (server.cpp),
// ReplicationLink, and FailoverMonitor all read and/or write it, which is
// what lets a follower's promotion or a stale leader's step-down actually
// take effect on connections already being served — a plain enum captured
// once at TcpServer construction (stage 6's model) cannot change after the
// fact.
//
// Locking: role, epoch, and last-contact are read on every request dispatch
// but only ever written a handful of times over a node's whole lifetime (an
// election, or observing a newer epoch) — not a hot path — so one mutex
// guarding all three is simpler to reason about than independent atomics
// that could tear relative to each other (e.g. an observer seeing the new
// epoch but the old role, mid-promotion). See docs/design-decisions.md's
// "Locking Strategy" section for the same reasoning applied to Store.
class ClusterState {
 public:
  explicit ClusterState(ReplicationRole role, uint64_t epoch)
      : role_(role), epoch_(epoch), last_contact_ms_(now_ms()) {}

  ReplicationRole role() const;
  uint64_t epoch() const;

  // How long since this node last accepted a message carrying a
  // current-or-newer epoch (a REPLICATE/CATCHUP_QUERY/HEARTBEAT from the
  // leader it currently recognizes) — the follower-side half of heartbeat
  // detection: FailoverMonitor calls an election once this exceeds its
  // timeout.
  int64_t ms_since_contact() const;

  // Called for every REPLICATE/CATCHUP_QUERY/HEARTBEAT frame received,
  // carrying that frame's term/epoch. Returns false — caller must reject
  // with Status::kStaleTerm — if remote_epoch is behind this node's current
  // epoch. Otherwise records contact (resetting the heartbeat-timeout
  // clock) and, if remote_epoch is strictly ahead, adopts it and demotes
  // this node to follower if it had been acting as leader: the split-brain
  // guard. A node only ever acts as leader for the highest epoch it has
  // observed. See docs/design-decisions.md's "Failover" section.
  bool accept_epoch(uint64_t remote_epoch);

  // Called only by the winner of an election (FailoverMonitor): adopts
  // new_epoch and becomes leader. Not guarded against new_epoch <= current
  // epoch — the only caller already computed new_epoch as current+1 off the
  // same ClusterState immediately beforehand (see FailoverMonitor), so a
  // defensive check here would guard a case the code never actually drives
  // it into.
  void promote_to_leader(uint64_t new_epoch);

 private:
  static int64_t now_ms();

  mutable mutex mutex_;
  ReplicationRole role_;
  uint64_t epoch_;
  int64_t last_contact_ms_;
};

// Runs one background thread per node that either sends heartbeats (if this
// node currently believes it is leader) or watches for a heartbeat timeout
// and runs an election (if it's a follower) — switching behavior
// automatically whenever ClusterState's role changes, including right after
// this monitor promotes its own node. See docs/design-decisions.md's
// "Failover" section for the full algorithm and docs/limitations.md for its
// split-brain caveat: this is heartbeat-based detection and epoch bumping,
// not a quorum/consensus protocol (no Raft/Paxos), so it can double-promote
// under a network partition.
class FailoverMonitor {
 public:
  // `peers` is every *other* node in the cluster (not including this one).
  // `self_port` is used only to break ties deterministically when two
  // candidates report the same total committed write_id during an
  // election.
  FailoverMonitor(ShardedStore& store, ClusterState& cluster, vector<PeerAddr> peers,
                   uint16_t self_port);
  ~FailoverMonitor();

  FailoverMonitor(const FailoverMonitor&) = delete;
  FailoverMonitor& operator=(const FailoverMonitor&) = delete;

  void start();
  void stop();

  // Number of ReplicationLinks this node has started as a result of winning
  // an election (0 if it has never been promoted). Test-observability only.
  size_t active_links_after_promotion() const;

 private:
  void run();
  void send_heartbeats_as_leader();
  void maybe_run_election();
  void promote_self(uint64_t new_epoch);
  uint64_t total_committed() const;

  // One-shot RPC: connect, send one frame, block for one response (bounded
  // by timeout_ms via SO_RCVTIMEO/SO_SNDTIMEO), close. Deliberately not
  // shared with ReplicationLink::send_request(), which manages a persistent,
  // reconnecting connection — heartbeats/election queries are periodic and
  // infrequent enough (tens of milliseconds apart) that paying one TCP
  // handshake per call is simpler than adding connection reuse for no
  // measured benefit at this project's scale.
  static bool send_one_shot(const string& host, uint16_t port, uint8_t opcode,
                             const string& payload, int timeout_ms, Response& out);

  ShardedStore& store_;
  ClusterState& cluster_;
  vector<PeerAddr> peers_;
  uint16_t self_port_;

  thread thread_;
  atomic<bool> running_{false};

  mutable mutex links_mutex_;
  vector<unique_ptr<ReplicationLink>> links_;
};

}  // namespace kvstore
