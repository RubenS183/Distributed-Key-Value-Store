#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_set>

#include "failover.h"
#include "protocol.h"
#include "sharded_store.h"

using namespace std;

namespace kvstore {

// Executes one parsed request against the store and produces the response.
// This is the only place the protocol types and the store meet — protocol.h
// and store.h otherwise know nothing about each other. Routing to the right
// shard happens inside ShardedStore's put()/get()/del() (hash the key, call
// through to that shard's Store) — dispatch() itself doesn't need to know
// shards exist.
//
// `cluster` gates which opcodes are actually executed, read fresh on every
// call rather than fixed at construction (stage 7: a node's role can change
// at runtime — see docs/design-decisions.md's "Failover" section):
//   - PUT/GET/DELETE only run when cluster.role() is kLeader (a follower
//     returns kNotLeader without touching the store — follower reads are
//     disallowed entirely in this phase, see docs/design-decisions.md's
//     "Replication Ack Policy").
//   - REPLICATE/CATCHUP_QUERY only run when cluster.role() is kFollower (a
//     leader returns kBadRequest — a leader is never the receiving end of
//     its own replication stream), and only if the frame's term passes
//     cluster.accept_epoch() (otherwise kStaleTerm — the split-brain guard).
//   - HEARTBEAT runs regardless of role (a demoted-but-not-yet-informed
//     leader must still be able to receive one, so it can learn about the
//     newer epoch and step down) — gated only by accept_epoch().
//   - ELECTION_QUERY runs regardless of role and never touches epoch state;
//     it's a pure read of this node's own (epoch, total committed write_id).
Response dispatch(ShardedStore& store, const Request& request, ClusterState& cluster);

// Thread-per-connection TCP server: accept() runs on the calling thread, and
// each accepted connection is handed to its own thread that reads,
// parses, dispatches, and writes for that connection until it closes. See
// docs/design-decisions.md for why thread-per-connection was chosen over an
// event loop for this project's scale.
class TcpServer {
 public:
  // port == 0 binds an OS-assigned ephemeral port (used by tests). `role`
  // defaults to kLeader so every pre-stage-7 caller (and every test that
  // doesn't care about replication/failover) is unaffected: this
  // constructor owns a private ClusterState fixed at that role for the
  // node's whole lifetime, exactly like stage 6's behavior.
  TcpServer(ShardedStore& store, uint16_t port, ReplicationRole role = ReplicationRole::kLeader);

  // Stage 7: shares an externally-owned ClusterState (also handed to this
  // node's FailoverMonitor and, if it starts as leader, its
  // ReplicationLinks) so a promotion or step-down actually changes what
  // dispatch() does on every connection this server serves.
  TcpServer(ShardedStore& store, uint16_t port, ClusterState& cluster);
  ~TcpServer();

  TcpServer(const TcpServer&) = delete;
  TcpServer& operator=(const TcpServer&) = delete;

  // Binds and listens. Throws runtime_error on failure.
  void start();

  // Blocks, accepting connections and spawning one thread per connection,
  // until stop() is called from another thread. Does not return until every
  // in-flight connection thread has finished, so no request is dropped
  // mid-flight when run() returns.
  void run();

  // Unblocks a concurrent run(): closes the listening socket (so accept()
  // returns) and shuts down every currently-active connection socket (so any
  // thread blocked in read()/write() unblocks and exits instead of hanging).
  void stop();

  // Actual bound port; useful when constructed with port 0.
  uint16_t port() const { return port_; }

  // This node's shared cluster state — exposed so a caller (main.cpp, or a
  // test) can hand the same ClusterState to a FailoverMonitor/
  // ReplicationLink. Valid for the server's whole lifetime regardless of
  // which constructor was used.
  ClusterState& cluster() { return cluster_; }

 private:
  void serve_connection(int client_fd);

  ShardedStore& store_;
  uint16_t port_;
  // Non-null only for the legacy (fixed-role) constructor, which owns its
  // ClusterState privately; null when a caller supplies its own. cluster_
  // always refers to a valid instance either way.
  unique_ptr<ClusterState> owned_cluster_;
  ClusterState& cluster_;
  // Read from run()'s accept loop and written from stop() on another
  // thread with no lock between them — must be atomic, not a plain int.
  atomic<int> listen_fd_{-1};

  // Tracks sockets currently owned by an in-flight connection thread, so
  // stop() can shut them down and run() can wait for them to drain before
  // returning.
  mutex conn_mutex_;
  condition_variable conn_cv_;
  unordered_set<int> active_fds_;
};

}  // namespace kvstore
