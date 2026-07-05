#include "server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <array>
#include <csignal>
#include <stdexcept>
#include <string>
#include <thread>

using namespace std;

namespace kvstore {

Response dispatch(ShardedStore& store, const Request& request, ClusterState& cluster) {
  Response response;
  response.request_id = request.request_id;

  switch (request.opcode) {
    case Opcode::kPut: {
      if (cluster.role() == ReplicationRole::kFollower) {
        response.status = Status::kNotLeader;
        return response;
      }
      PutResult result = store.put(request.key, request.value);
      response.status = (result == PutResult::kOk) ? Status::kOk : Status::kBadRequest;
      return response;
    }
    case Opcode::kGet: {
      if (cluster.role() == ReplicationRole::kFollower) {
        response.status = Status::kNotLeader;
        return response;
      }
      auto value = store.get(request.key);
      if (value.has_value()) {
        response.status = Status::kOk;
        response.payload = move(*value);
      } else {
        response.status = Status::kNotFound;
      }
      return response;
    }
    case Opcode::kDelete: {
      if (cluster.role() == ReplicationRole::kFollower) {
        response.status = Status::kNotLeader;
        return response;
      }
      DeleteResult result = store.del(request.key);
      response.status = (result == DeleteResult::kOk) ? Status::kOk : Status::kNotFound;
      return response;
    }
    case Opcode::kReplicate: {
      // A leader is never the receiving end of a REPLICATE frame — only a
      // follower applies one.
      if (cluster.role() == ReplicationRole::kLeader) {
        response.status = Status::kBadRequest;
        return response;
      }
      if (!cluster.accept_epoch(request.term)) {
        response.status = Status::kStaleTerm;
        response.payload = encode_epoch_payload(cluster.epoch());
        return response;
      }
      Record record;
      record.type = request.record_type;
      record.write_id = request.write_id;
      record.version = request.version;
      record.key = request.key;
      record.value = request.value;
      store.apply_replicated(record);
      response.status = Status::kOk;
      return response;
    }
    case Opcode::kCatchupQuery: {
      if (cluster.role() == ReplicationRole::kLeader) {
        response.status = Status::kBadRequest;
        return response;
      }
      if (request.shard_id >= store.num_shards()) {
        response.status = Status::kBadRequest;
        return response;
      }
      if (!cluster.accept_epoch(request.term)) {
        response.status = Status::kStaleTerm;
        response.payload = encode_epoch_payload(cluster.epoch());
        return response;
      }
      response.status = Status::kOk;
      response.payload =
          encode_last_applied_payload(store.shard(request.shard_id).last_applied_write_id());
      return response;
    }
    case Opcode::kHeartbeat: {
      // Runs regardless of role: a leader that has actually been superseded
      // must still be able to receive this and learn about it (see
      // ClusterState::accept_epoch()'s split-brain guard) rather than being
      // gated out by a role check it no longer honestly satisfies.
      if (!cluster.accept_epoch(request.term)) {
        response.status = Status::kStaleTerm;
        response.payload = encode_epoch_payload(cluster.epoch());
        return response;
      }
      response.status = Status::kOk;
      return response;
    }
    case Opcode::kElectionQuery: {
      // A pure read of this node's own state — never mutates epoch/role, so
      // a candidate's proposed epoch can't be adopted just by asking.
      uint64_t total = 0;
      for (ShardId s = 0; s < store.num_shards(); ++s) {
        total += store.shard(s).last_applied_write_id();
      }
      response.status = Status::kOk;
      response.payload = encode_election_response_payload(cluster.epoch(), total);
      return response;
    }
  }
  response.status = Status::kBadRequest;  // unreachable for a valid Opcode
  return response;
}

TcpServer::TcpServer(ShardedStore& store, uint16_t port, ReplicationRole role)
    : store_(store),
      port_(port),
      owned_cluster_(make_unique<ClusterState>(role, kCurrentTerm)),
      cluster_(*owned_cluster_) {}

TcpServer::TcpServer(ShardedStore& store, uint16_t port, ClusterState& cluster)
    : store_(store), port_(port), owned_cluster_(nullptr), cluster_(cluster) {}

TcpServer::~TcpServer() {
  int fd = listen_fd_.exchange(-1);
  if (fd >= 0) ::close(fd);
}

void TcpServer::start() {
  // Concurrency means a client can close its socket while a connection
  // thread is mid-write (or stop() can shut a socket down from under a
  // thread that's blocked in write()). Both raise SIGPIPE by default, which
  // kills the whole process; ignoring it makes write() report a normal -1/
  // EPIPE instead, which serve_connection already handles by dropping the
  // connection. Process-wide, but a TCP server has no legitimate use for
  // the default SIGPIPE-kills-the-process behavior, so this is set here
  // once rather than duplicated in every caller (main.cpp, tests).
  ::signal(SIGPIPE, SIG_IGN);

  listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ < 0) throw runtime_error("socket() failed");

  int reuse = 1;
  ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(port_);

  if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    throw runtime_error("bind() failed on port " + to_string(port_));
  }

  // Resolve the actual bound port (matters when constructed with port 0).
  socklen_t addr_len = sizeof(addr);
  if (::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &addr_len) == 0) {
    port_ = ntohs(addr.sin_port);
  }

  if (::listen(listen_fd_, /*backlog=*/16) < 0) {
    throw runtime_error("listen() failed");
  }
}

void TcpServer::run() {
  while (true) {
    int client_fd = ::accept(listen_fd_.load(), nullptr, nullptr);
    if (client_fd < 0) {
      // Either stop() closed the listening socket or a real accept error —
      // both mean there's nothing left to serve.
      break;
    }

    {
      lock_guard<mutex> lock(conn_mutex_);
      active_fds_.insert(client_fd);
    }

    // Detached: this thread's lifetime is tracked via active_fds_/conn_cv_,
    // not via a joinable thread handle, so run() doesn't need to keep
    // a growing collection of thread objects around for the server's whole
    // lifetime.
    thread([this, client_fd] {
      serve_connection(client_fd);
      ::close(client_fd);
      lock_guard<mutex> lock(conn_mutex_);
      active_fds_.erase(client_fd);
      if (active_fds_.empty()) conn_cv_.notify_all();
    }).detach();
  }

  // The listener is closed at this point (stop() was called). Any
  // connections still in flight were told to unblock by stop()'s shutdown()
  // calls; wait here so run() never returns while a request is still being
  // served.
  unique_lock<mutex> lock(conn_mutex_);
  conn_cv_.wait(lock, [this] { return active_fds_.empty(); });
}

void TcpServer::stop() {
  // exchange(), not load()-then-close(), so a concurrent stop() call (or the
  // destructor) can't race on which caller gets to close listen_fd_ — each
  // fd value comes out of the exchange exactly once.
  int fd = listen_fd_.exchange(-1);
  if (fd >= 0) {
    ::shutdown(fd, SHUT_RDWR);
    ::close(fd);
  }

  // Unblock any connection thread currently parked in a blocking read()/
  // write() so it observes EOF/EPIPE and exits instead of holding a request
  // open indefinitely.
  lock_guard<mutex> lock(conn_mutex_);
  for (int fd : active_fds_) {
    ::shutdown(fd, SHUT_RDWR);
  }
}

void TcpServer::serve_connection(int client_fd) {
  FrameParser parser;
  array<char, 4096> buf;

  while (true) {
    ssize_t n = ::read(client_fd, buf.data(), buf.size());
    if (n <= 0) return;  // client closed the connection or a read error occurred
    parser.feed(buf.data(), static_cast<size_t>(n));

    Frame frame;
    while (true) {
      ParseStatus status = parser.next(frame);
      if (status == ParseStatus::kNeedMore) break;
      if (status == ParseStatus::kError) return;  // malformed stream: drop the connection

      Request request;
      Response response;
      if (!parse_request(frame, request)) {
        response.status = Status::kBadRequest;
        response.request_id = frame.request_id;
      } else {
        response = dispatch(store_, request, cluster_);
      }

      string encoded = encode_response(response);
      size_t written = 0;
      while (written < encoded.size()) {
        ssize_t w = ::write(client_fd, encoded.data() + written, encoded.size() - written);
        if (w <= 0) return;
        written += static_cast<size_t>(w);
      }
    }
  }
}

}  // namespace kvstore
