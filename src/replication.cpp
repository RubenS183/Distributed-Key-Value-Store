#include "replication.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <chrono>

#include "protocol.h"
#include "snapshot.h"

using namespace std;

namespace kvstore {

namespace {

// How long to wait before retrying a failed/dropped connection to the
// follower. Short and fixed, not backed off: this is a test-cluster-scale
// replication link (N=1 follower), not a scenario where a tight reconnect
// loop from one leader risks overwhelming anything.
constexpr int kReconnectDelayMs = 100;

// How often the steady-state loop checks each shard's WAL for new records
// once caught up. A poll, not a push callback from Store::put()/del(), by
// deliberate choice — see docs/design-decisions.md's "Consistency Model":
// this keeps replication fully decoupled from the client write path, at the
// cost of up to this much extra replication lag on top of network latency.
constexpr int kPollIntervalMs = 20;

}  // namespace

ReplicationLink::ReplicationLink(ShardedStore& store, string follower_host,
                                  uint16_t follower_port, ClusterState* cluster)
    : store_(store),
      follower_host_(move(follower_host)),
      follower_port_(follower_port),
      cluster_(cluster),
      ack_index_(store.num_shards(), 0) {}

ReplicationLink::~ReplicationLink() { stop(); }

bool ReplicationLink::should_keep_running() const {
  return running_.load() && (!cluster_ || cluster_->role() == ReplicationRole::kLeader);
}

uint64_t ReplicationLink::current_term() const { return cluster_ ? cluster_->epoch() : kCurrentTerm; }

void ReplicationLink::start() {
  running_ = true;
  thread_ = thread([this] { run(); });
}

void ReplicationLink::stop() {
  running_ = false;
  disconnect();  // unblocks a thread parked in a blocking read()/write()
  if (thread_.joinable()) thread_.join();
}

uint64_t ReplicationLink::ack_index(ShardId shard) const {
  lock_guard<mutex> lock(ack_mutex_);
  return ack_index_[shard];
}

bool ReplicationLink::connect_to_follower() {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return false;

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(follower_port_);
  if (::inet_pton(AF_INET, follower_host_.c_str(), &addr.sin_addr) != 1) {
    ::close(fd);
    return false;
  }
  if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    ::close(fd);
    return false;
  }

  fd_.store(fd);
  return true;
}

void ReplicationLink::disconnect() {
  int fd = fd_.exchange(-1);
  if (fd >= 0) {
    ::shutdown(fd, SHUT_RDWR);
    ::close(fd);
  }
}

bool ReplicationLink::send_request(uint8_t opcode, const string& payload, Response& out) {
  int fd = fd_.load();
  if (fd < 0) return false;

  uint64_t request_id = next_request_id_++;
  string encoded = encode_frame(opcode, request_id, payload);

  size_t written = 0;
  while (written < encoded.size()) {
    ssize_t w = ::write(fd, encoded.data() + written, encoded.size() - written);
    if (w <= 0) return false;
    written += static_cast<size_t>(w);
  }

  FrameParser parser;
  Frame frame;
  array<char, 4096> buf;
  while (true) {
    ParseStatus status = parser.next(frame);
    if (status == ParseStatus::kFrame) break;
    if (status == ParseStatus::kError) return false;
    ssize_t n = ::read(fd, buf.data(), buf.size());
    if (n <= 0) return false;
    parser.feed(buf.data(), static_cast<size_t>(n));
  }
  if (frame.request_id != request_id) return false;

  out.status = static_cast<Status>(frame.type_byte);
  out.request_id = frame.request_id;
  out.payload = frame.payload;

  // A follower rejecting our epoch means we (or whoever we thought we were
  // replicating for) has been superseded. Adopt the newer epoch it carries
  // and step this node down ourselves — the same split-brain guard
  // ClusterState::accept_epoch() applies on the receiving side of a
  // HEARTBEAT — instead of treating this as a generic connection failure
  // to blindly retry. Only meaningful when this link is failover-aware.
  if (out.status == Status::kStaleTerm && cluster_) {
    cluster_->accept_epoch(decode_epoch_payload(out.payload));
  }
  return true;
}

bool ReplicationLink::send_replicated_record(ShardId shard, const Record& record) {
  Response resp;
  if (!send_request(static_cast<uint8_t>(Opcode::kReplicate),
                     encode_replicate_payload(current_term(), record), resp)) {
    return false;
  }
  if (resp.status != Status::kOk) return false;

  lock_guard<mutex> lock(ack_mutex_);
  if (record.write_id > ack_index_[shard]) ack_index_[shard] = record.write_id;
  return true;
}

bool ReplicationLink::replicate_from(ShardId shard, uint64_t after_write_id) {
  bool ok = true;
  store_.shard(shard).replay_wal_after(after_write_id, [&](const Record& record) {
    if (ok) ok = send_replicated_record(shard, record);
  });
  return ok;
}

bool ReplicationLink::catch_up_shard(ShardId shard) {
  Response resp;
  if (!send_request(static_cast<uint8_t>(Opcode::kCatchupQuery),
                     encode_catchup_query_payload(current_term(), shard), resp)) {
    return false;
  }
  if (resp.status != Status::kOk) return false;
  uint64_t follower_last_applied = decode_last_applied_payload(resp.payload);

  // Ground truth for "is the follower behind what the WAL alone can still
  // replay" is the snapshot file actually on disk right now — not
  // Store::snapshot_boundary_write_id_, which this process's in-memory
  // Store only ever sets by *loading* a snapshot at startup recovery, never
  // by calling take_snapshot() itself. A live leader that has taken its own
  // snapshot(s) since starting up would otherwise always read back 0 there
  // and wrongly think no truncation has ever happened.
  auto snap = load_latest(store_.snapshot_dir(shard));
  if (snap && follower_last_applied < snap->boundary_write_id) {
    // The follower is too far behind for the WAL alone: the segments
    // covering write_ids in (follower_last_applied, boundary] may already
    // have been deleted by truncate_before() after a snapshot. Ship the
    // current snapshot instead, then fall through to the WAL-tail replay
    // below starting from its boundary.
    //
    // apply_replicated()'s idempotency check (write_id < next_write_id_)
    // assumes monotonically increasing delivery, but a snapshot's entries
    // aren't stored in write_id order (they're serialized straight from an
    // unordered_map) — sort them first so out-of-order entries don't get
    // skipped as "already applied".
    vector<SnapshotEntry> entries = snap->entries;
    sort(entries.begin(), entries.end(),
              [](const SnapshotEntry& a, const SnapshotEntry& b) { return a.write_id < b.write_id; });
    for (const auto& e : entries) {
      Record record{e.tombstone ? RecordType::kDelete : RecordType::kPut, e.write_id, e.version,
                     e.key, e.value};
      if (!send_replicated_record(shard, record)) return false;
    }
    follower_last_applied = snap->boundary_write_id;
  }

  return replicate_from(shard, follower_last_applied);
}

void ReplicationLink::run() {
  while (should_keep_running()) {
    if (!connect_to_follower()) {
      this_thread::sleep_for(chrono::milliseconds(kReconnectDelayMs));
      continue;
    }

    bool healthy = true;
    for (ShardId s = 0; healthy && should_keep_running() && s < store_.num_shards(); ++s) {
      healthy = catch_up_shard(s);
    }

    while (healthy && should_keep_running()) {
      for (ShardId s = 0; healthy && s < store_.num_shards(); ++s) {
        uint64_t cursor = ack_index(s);
        healthy = replicate_from(s, cursor);
      }
      if (healthy) this_thread::sleep_for(chrono::milliseconds(kPollIntervalMs));
    }

    disconnect();
    if (should_keep_running()) this_thread::sleep_for(chrono::milliseconds(kReconnectDelayMs));
  }
}

}  // namespace kvstore
