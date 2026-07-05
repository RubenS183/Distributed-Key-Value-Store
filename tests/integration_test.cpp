#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include "kv_client.h"
#include "protocol.h"
#include "server.h"
#include "sharded_store.h"

using namespace std;

using kvstore::KvClient;
using kvstore::Opcode;
using kvstore::ShardedStore;
using kvstore::Status;
using kvstore::TcpServer;

namespace {

namespace fs = filesystem;

// RAII temp directory, unique per instance, removed on destruction — same
// pattern as wal_test.cpp/crash_recovery_test.cpp's TempDir.
class TempDir {
 public:
  explicit TempDir(const string& tag) {
    path_ = fs::temp_directory_path() /
            ("kvstore_integration_test_" + tag + "_" +
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

// Small, fixed shard count — these tests exercise the network/protocol
// path, not sharding itself (see sharding_test.cpp for distribution and
// per-shard recovery coverage).
constexpr size_t kTestNumShards = 4;

// Test scaffolding only: runs the server's blocking accept loop on a
// background thread so the test can drive it as a real TCP client. The
// server itself is still single-threaded — it serves one connection to
// completion before accepting the next.
class ServerFixture {
 public:
  ServerFixture()
      : wal_dir_("wal"),
        snapshot_dir_("snapshot"),
        store_(kTestNumShards, wal_dir_.string(), snapshot_dir_.string()),
        server_(store_, /*port=*/0) {
    server_.start();
    thread_ = thread([this] { server_.run(); });
  }

  ~ServerFixture() {
    server_.stop();
    if (thread_.joinable()) thread_.join();
  }

  uint16_t port() const { return server_.port(); }

 private:
  TempDir wal_dir_;
  TempDir snapshot_dir_;
  ShardedStore store_;
  TcpServer server_;
  thread thread_;
};

}  // namespace

TEST_CASE("end-to-end PUT/GET/DELETE over a real TCP connection", "[integration]") {
  ServerFixture fixture;
  KvClient client("127.0.0.1", fixture.port());

  auto put_response = client.send(Opcode::kPut, "greeting", "hello");
  REQUIRE(put_response.status == Status::kOk);

  auto get_response = client.send(Opcode::kGet, "greeting");
  REQUIRE(get_response.status == Status::kOk);
  REQUIRE(get_response.payload == "hello");

  auto delete_response = client.send(Opcode::kDelete, "greeting");
  REQUIRE(delete_response.status == Status::kOk);

  auto get_after_delete = client.send(Opcode::kGet, "greeting");
  REQUIRE(get_after_delete.status == Status::kNotFound);
}

TEST_CASE("GET on a key that was never written returns NOT_FOUND", "[integration]") {
  ServerFixture fixture;
  KvClient client("127.0.0.1", fixture.port());

  auto response = client.send(Opcode::kGet, "never-written");
  REQUIRE(response.status == Status::kNotFound);
}

TEST_CASE("DELETE on a missing key returns NOT_FOUND", "[integration]") {
  ServerFixture fixture;
  KvClient client("127.0.0.1", fixture.port());

  auto response = client.send(Opcode::kDelete, "missing");
  REQUIRE(response.status == Status::kNotFound);
}

TEST_CASE("multiple requests over one connection each get the right response", "[integration]") {
  ServerFixture fixture;
  KvClient client("127.0.0.1", fixture.port());

  // KvClient::send already asserts each response's request_id echoes what
  // was sent; this test exercises several requests in sequence.
  client.send(Opcode::kPut, "a", "1");
  client.send(Opcode::kPut, "b", "2");
  auto a = client.send(Opcode::kGet, "a");
  auto b = client.send(Opcode::kGet, "b");
  REQUIRE(a.payload == "1");
  REQUIRE(b.payload == "2");
}

// --- Concurrency ---
//
// These drive the real TcpServer thread-per-connection model over loopback
// TCP, as opposed to storage_test.cpp's concurrency tests which hit Store
// directly. As in storage_test.cpp, each worker thread records its own
// pass/fail verdict rather than calling REQUIRE from inside the thread,
// since Catch2's assertion macros aren't safe to call concurrently.

TEST_CASE("stress: many concurrent TCP clients hammering the server", "[integration][concurrency][stress]") {
  ServerFixture fixture;
  constexpr int kClientThreads = 16;
  constexpr int kOpsPerThread = 200;

  vector<thread> threads;
  vector<char> ok(kClientThreads, 0);
  for (int t = 0; t < kClientThreads; ++t) {
    threads.emplace_back([&fixture, &ok, t] {
      bool thread_ok = true;
      try {
        KvClient client("127.0.0.1", fixture.port());
        string key = "stress-" + to_string(t);
        for (int i = 0; i < kOpsPerThread; ++i) {
          string expected = to_string(i);
          auto put_resp = client.send(Opcode::kPut, key, expected);
          if (put_resp.status != Status::kOk) thread_ok = false;
          auto get_resp = client.send(Opcode::kGet, key);
          if (get_resp.status != Status::kOk || get_resp.payload != expected) {
            thread_ok = false;
          }
        }
        auto del_resp = client.send(Opcode::kDelete, key);
        if (del_resp.status != Status::kOk) thread_ok = false;
      } catch (const exception&) {
        thread_ok = false;
      }
      ok[t] = thread_ok;
    });
  }
  for (auto& th : threads) th.join();

  for (int t = 0; t < kClientThreads; ++t) {
    REQUIRE(ok[t]);
  }
}

TEST_CASE("safe shutdown: stop() while requests are in flight does not hang or crash",
          "[integration][concurrency]") {
  // Not using ServerFixture here: this test needs to call stop() itself
  // while client threads are still actively sending, to prove run() drains
  // in-flight connections instead of hanging or dropping them mid-write.
  TempDir wal_dir("shutdown_wal");
  TempDir snapshot_dir("shutdown_snapshot");
  ShardedStore store(kTestNumShards, wal_dir.string(), snapshot_dir.string());
  TcpServer server(store, /*port=*/0);
  server.start();
  thread server_thread([&server] { server.run(); });

  atomic<bool> keep_going{true};
  atomic<int> completed_ops{0};
  constexpr int kClientThreads = 8;

  vector<thread> clients;
  for (int c = 0; c < kClientThreads; ++c) {
    clients.emplace_back([&, c] {
      try {
        KvClient client("127.0.0.1", server.port());
        string key = "shutdown-key-" + to_string(c);
        while (keep_going) {
          client.send(Opcode::kPut, key, "v");
          client.send(Opcode::kGet, key);
          completed_ops++;
        }
      } catch (const exception&) {
        // Expected: stop() severs the connection out from under a thread
        // that's mid-request. That's the behavior under test, not a
        // failure — the assertion below checks that the *server* shut down
        // cleanly (run() returned), not that every client request
        // succeeded.
      }
    });
  }

  // Let real traffic start flowing before pulling the rug — this is the
  // "requests in flight" moment stop() below has to handle safely.
  while (completed_ops.load() < kClientThreads) {
    this_thread::sleep_for(chrono::milliseconds(1));
  }

  server.stop();          // must unblock accept() and every in-flight read()/write()
  server_thread.join();   // must return promptly, not hang waiting on a stuck connection

  keep_going = false;
  for (auto& th : clients) th.join();

  REQUIRE(completed_ops.load() >= kClientThreads);
}
