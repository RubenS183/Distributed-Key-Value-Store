#include "failover.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <chrono>

#include "protocol.h"
#include "replication.h"

using namespace std;

namespace kvstore {

namespace {

// How often a leader sends a HEARTBEAT to every peer, and how often a
// follower re-checks whether it has timed out. How long a follower waits
// with no current-or-newer-epoch contact before calling an election.
// kHeartbeatTimeoutMs is a small multiple of kHeartbeatIntervalMs so a
// handful of missed beats (scheduling jitter, a slow one-shot RPC) doesn't
// trigger a false election, while still detecting a real crash quickly.
// kRpcTimeoutMs bounds one heartbeat/election one-shot call so a single
// unreachable peer can't stall a whole monitor pass; it is shorter than
// kHeartbeatTimeoutMs so a hung RPC can't itself blow through the
// detection threshold. All three are tuned for this project's test-cluster
// scale (localhost, no real network latency) — see
// docs/design-decisions.md's "Failover" section.
constexpr int kHeartbeatIntervalMs = 30;
constexpr int kHeartbeatTimeoutMs = 150;
constexpr int kRpcTimeoutMs = 100;

int64_t steady_now_ms() {
  return chrono::duration_cast<chrono::milliseconds>(
             chrono::steady_clock::now().time_since_epoch())
      .count();
}

}  // namespace

int64_t ClusterState::now_ms() { return steady_now_ms(); }

ReplicationRole ClusterState::role() const {
  lock_guard<mutex> lock(mutex_);
  return role_;
}

uint64_t ClusterState::epoch() const {
  lock_guard<mutex> lock(mutex_);
  return epoch_;
}

int64_t ClusterState::ms_since_contact() const {
  lock_guard<mutex> lock(mutex_);
  return now_ms() - last_contact_ms_;
}

bool ClusterState::accept_epoch(uint64_t remote_epoch) {
  lock_guard<mutex> lock(mutex_);
  if (remote_epoch < epoch_) return false;  // stale — caller must reject
  if (remote_epoch > epoch_) {
    epoch_ = remote_epoch;
    role_ = ReplicationRole::kFollower;  // split-brain guard: a newer epoch always demotes
  }
  last_contact_ms_ = now_ms();
  return true;
}

void ClusterState::promote_to_leader(uint64_t new_epoch) {
  lock_guard<mutex> lock(mutex_);
  epoch_ = new_epoch;
  role_ = ReplicationRole::kLeader;
  last_contact_ms_ = now_ms();
}

FailoverMonitor::FailoverMonitor(ShardedStore& store, ClusterState& cluster,
                                  vector<PeerAddr> peers, uint16_t self_port)
    : store_(store), cluster_(cluster), peers_(move(peers)), self_port_(self_port) {}

FailoverMonitor::~FailoverMonitor() { stop(); }

void FailoverMonitor::start() {
  running_ = true;
  thread_ = thread([this] { run(); });
}

void FailoverMonitor::stop() {
  running_ = false;
  if (thread_.joinable()) thread_.join();

  // Safe to touch links_ without racing run()/promote_self() now: the
  // monitor thread that could still be mutating it has already joined.
  lock_guard<mutex> lock(links_mutex_);
  for (auto& link : links_) link->stop();
  links_.clear();
}

size_t FailoverMonitor::active_links_after_promotion() const {
  lock_guard<mutex> lock(links_mutex_);
  return links_.size();
}

void FailoverMonitor::run() {
  while (running_.load()) {
    if (cluster_.role() == ReplicationRole::kLeader) {
      send_heartbeats_as_leader();
    } else {
      maybe_run_election();
    }
    this_thread::sleep_for(chrono::milliseconds(kHeartbeatIntervalMs));
  }
}

void FailoverMonitor::send_heartbeats_as_leader() {
  string payload = encode_epoch_payload(cluster_.epoch());
  for (const auto& peer : peers_) {
    Response resp;
    if (!send_one_shot(peer.host, peer.port, static_cast<uint8_t>(Opcode::kHeartbeat), payload,
                        kRpcTimeoutMs, resp)) {
      continue;  // unreachable this pass — best-effort, not fatal
    }
    if (resp.status == Status::kStaleTerm) {
      // Some peer already knows about a newer epoch than ours — we were
      // superseded (e.g. a partition healed). Adopt it and stop heartbeating
      // as leader; accept_epoch() demotes us since the epoch is strictly
      // newer.
      cluster_.accept_epoch(decode_epoch_payload(resp.payload));
      return;
    }
  }
}

void FailoverMonitor::maybe_run_election() {
  if (cluster_.ms_since_contact() < kHeartbeatTimeoutMs) return;

  uint64_t my_epoch = cluster_.epoch();
  uint64_t proposed_epoch = my_epoch + 1;
  uint64_t highest_seen_epoch = my_epoch;

  uint64_t best_total = total_committed();
  bool i_am_best = true;

  for (const auto& peer : peers_) {
    Response resp;
    if (!send_one_shot(peer.host, peer.port, static_cast<uint8_t>(Opcode::kElectionQuery), "",
                        kRpcTimeoutMs, resp) ||
        resp.status != Status::kOk) {
      continue;  // unreachable — excluded from this election's comparison
                 // (documented split-brain caveat: a genuinely up-to-date
                 // but unreachable peer can't be chosen, and can't be
                 // outvoted either).
    }
    uint64_t peer_epoch = 0, peer_total = 0;
    decode_election_response_payload(resp.payload, peer_epoch, peer_total);
    highest_seen_epoch = max(highest_seen_epoch, peer_epoch);

    if (peer_epoch >= proposed_epoch) {
      // Someone has already been elected at or beyond the epoch we were
      // about to propose — don't contend, just resynchronize below.
      i_am_best = false;
      continue;
    }
    // "Most up to date": compare total committed write_id across every
    // shard (see total_committed()); tie-break by lowest port for a
    // deterministic, arbitrary-but-consistent order. See
    // docs/design-decisions.md's "Failover" section for why a single
    // summed scalar (not a per-shard vector comparison) was chosen.
    if (peer_total > best_total || (peer_total == best_total && peer.port < self_port_)) {
      i_am_best = false;
    }
  }

  if (highest_seen_epoch > my_epoch) {
    cluster_.accept_epoch(highest_seen_epoch);
    return;
  }

  if (i_am_best) promote_self(proposed_epoch);
}

void FailoverMonitor::promote_self(uint64_t new_epoch) {
  cluster_.promote_to_leader(new_epoch);

  lock_guard<mutex> lock(links_mutex_);
  for (auto& link : links_) link->stop();
  links_.clear();
  for (const auto& peer : peers_) {
    auto link = make_unique<ReplicationLink>(store_, peer.host, peer.port, &cluster_);
    link->start();
    links_.push_back(move(link));
  }
}

uint64_t FailoverMonitor::total_committed() const {
  uint64_t total = 0;
  for (ShardId s = 0; s < store_.num_shards(); ++s) {
    total += store_.shard(s).last_applied_write_id();
  }
  return total;
}

bool FailoverMonitor::send_one_shot(const string& host, uint16_t port, uint8_t opcode,
                                     const string& payload, int timeout_ms, Response& out) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return false;

  timeval tv{};
  tv.tv_sec = timeout_ms / 1000;
  tv.tv_usec = (timeout_ms % 1000) * 1000;
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
    ::close(fd);
    return false;
  }
  if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    ::close(fd);
    return false;
  }

  string encoded = encode_frame(opcode, /*request_id=*/1, payload);
  size_t written = 0;
  while (written < encoded.size()) {
    ssize_t w = ::write(fd, encoded.data() + written, encoded.size() - written);
    if (w <= 0) {
      ::close(fd);
      return false;
    }
    written += static_cast<size_t>(w);
  }

  FrameParser parser;
  Frame frame;
  array<char, 4096> buf;
  bool got_frame = false;
  while (true) {
    ParseStatus status = parser.next(frame);
    if (status == ParseStatus::kFrame) {
      got_frame = true;
      break;
    }
    if (status == ParseStatus::kError) break;
    ssize_t n = ::read(fd, buf.data(), buf.size());
    if (n <= 0) break;  // timeout, EOF, or error
    parser.feed(buf.data(), static_cast<size_t>(n));
  }
  ::close(fd);
  if (!got_frame) return false;

  out.status = static_cast<Status>(frame.type_byte);
  out.request_id = frame.request_id;
  out.payload = frame.payload;
  return true;
}

}  // namespace kvstore
