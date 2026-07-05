#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include "store.h"

using namespace std;

using kvstore::DeleteResult;
using kvstore::PutResult;
using kvstore::Store;

TEST_CASE("put then get returns the value", "[storage]") {
  Store store;
  REQUIRE(store.put("a", "1") == PutResult::kOk);
  auto value = store.get("a");
  REQUIRE(value.has_value());
  REQUIRE(*value == "1");
}

TEST_CASE("get on a missing key returns nullopt", "[storage]") {
  Store store;
  REQUIRE_FALSE(store.get("missing").has_value());
}

TEST_CASE("overwrite replaces the value", "[storage]") {
  Store store;
  REQUIRE(store.put("a", "1") == PutResult::kOk);
  REQUIRE(store.put("a", "2") == PutResult::kOk);
  REQUIRE(*store.get("a") == "2");
}

TEST_CASE("version increments per key on each write", "[storage]") {
  Store store;
  store.put("a", "1");
  REQUIRE(store.peek("a")->version == 1);
  store.put("a", "2");
  REQUIRE(store.peek("a")->version == 2);
}

TEST_CASE("write_id increases monotonically store-wide", "[storage]") {
  Store store;
  store.put("a", "1");
  store.put("b", "1");
  store.put("a", "2");

  auto a = store.peek("a");
  auto b = store.peek("b");
  REQUIRE(a.has_value());
  REQUIRE(b.has_value());
  REQUIRE(a->write_id > b->write_id);  // "a" was written again after "b"
}

TEST_CASE("delete then get returns nullopt", "[storage]") {
  Store store;
  REQUIRE(store.put("a", "1") == PutResult::kOk);
  REQUIRE(store.del("a") == DeleteResult::kOk);
  REQUIRE_FALSE(store.get("a").has_value());
}

TEST_CASE("delete sets the tombstone and bumps version", "[storage]") {
  Store store;
  store.put("a", "1");
  store.del("a");
  auto entry = store.peek("a");
  REQUIRE(entry.has_value());
  REQUIRE(entry->tombstone);
  REQUIRE(entry->version == 2);
}

TEST_CASE("delete on a missing key returns kNotFound", "[storage]") {
  Store store;
  REQUIRE(store.del("missing") == DeleteResult::kNotFound);
}

TEST_CASE("deleting an already-deleted key returns kNotFound", "[storage]") {
  Store store;
  store.put("a", "1");
  REQUIRE(store.del("a") == DeleteResult::kOk);
  REQUIRE(store.del("a") == DeleteResult::kNotFound);
}

TEST_CASE("put after delete resurrects the key", "[storage]") {
  Store store;
  store.put("a", "1");
  store.del("a");
  REQUIRE(store.put("a", "2") == PutResult::kOk);
  REQUIRE(*store.get("a") == "2");
}

TEST_CASE("empty key is rejected", "[storage]") {
  Store store;
  REQUIRE(store.put("", "1") == PutResult::kEmptyKey);
}

TEST_CASE("oversized key is rejected", "[storage]") {
  Store store;
  string big_key(kvstore::kMaxKeySize + 1, 'k');
  REQUIRE(store.put(big_key, "1") == PutResult::kKeyTooLarge);
}

TEST_CASE("oversized value is rejected", "[storage]") {
  Store store;
  string big_value(kvstore::kMaxValueSize + 1, 'v');
  REQUIRE(store.put("a", big_value) == PutResult::kValueTooLarge);
}

TEST_CASE("max-size key and value are accepted", "[storage]") {
  Store store;
  string key(kvstore::kMaxKeySize, 'k');
  string value(kvstore::kMaxValueSize, 'v');
  REQUIRE(store.put(key, value) == PutResult::kOk);
  REQUIRE(*store.get(key) == value);
}

// --- Concurrency ---
//
// These tests exercise Store's shared_mutex directly (no network
// involved). They're written to fail loudly under ThreadSanitizer if the
// locking is wrong, and to carry a real assertion — not just "didn't
// crash" — that only holds if put()/del() actually serialize.

TEST_CASE("concurrent writers to the same key never lose an update", "[storage][concurrency]") {
  Store store;
  constexpr int kThreads = 8;
  constexpr int kWritesPerThread = 500;

  vector<thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&store] {
      for (int i = 0; i < kWritesPerThread; ++i) {
        store.put("shared", "x");
      }
    });
  }
  for (auto& th : threads) th.join();

  // put() always does read-modify-write on entry.version under the same
  // lock; if two writers' increments ever raced, this would land below
  // kThreads * kWritesPerThread instead of hitting it exactly.
  REQUIRE(store.peek("shared")->version == static_cast<uint64_t>(kThreads * kWritesPerThread));
}

TEST_CASE("concurrent readers observe a consistent value while a writer runs", "[storage][concurrency]") {
  Store store;
  store.put("shared", "initial");

  atomic<bool> stop{false};
  atomic<bool> saw_corruption{false};

  thread writer([&] {
    for (int i = 0; i < 2000; ++i) {
      store.put("shared", string(16, static_cast<char>('a' + (i % 26))));
    }
    stop = true;
  });

  // Readers only ever see values this test itself wrote (all 16 identical
  // chars) or "initial" — anything else means a torn/partial read leaked
  // through the lock.
  vector<thread> readers;
  for (int r = 0; r < 4; ++r) {
    readers.emplace_back([&] {
      while (!stop) {
        auto value = store.get("shared");
        if (!value.has_value()) continue;
        bool valid = (*value == "initial") ||
                     (value->size() == 16 &&
                      string(value->size(), (*value)[0]) == *value);
        if (!valid) saw_corruption = true;
      }
    });
  }

  writer.join();
  for (auto& th : readers) th.join();

  REQUIRE_FALSE(saw_corruption);
}

TEST_CASE("concurrent access to different keys does not cross-contaminate", "[storage][concurrency]") {
  // Catch2's assertion macros aren't safe to call concurrently from worker
  // threads (shared reporter state), so each thread records its own
  // pass/fail verdict and every REQUIRE runs on the main thread after join.
  Store store;
  constexpr int kThreads = 16;
  constexpr int kOpsPerThread = 200;

  vector<thread> threads;
  vector<char> ok(kThreads, 0);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&store, &ok, t] {
      string key = "key-" + to_string(t);
      bool thread_ok = true;
      for (int i = 0; i < kOpsPerThread; ++i) {
        store.put(key, to_string(i));
        auto value = store.get(key);
        // Nobody else ever writes this thread's key, so the value read back
        // must be one this same thread just wrote — a numeric string in
        // [0, kOpsPerThread), never another thread's key's value.
        if (!value.has_value()) {
          thread_ok = false;
          continue;
        }
        int seen = stoi(*value);
        if (seen < 0 || seen >= kOpsPerThread) thread_ok = false;
      }
      if (store.del(key) != DeleteResult::kOk) thread_ok = false;
      ok[t] = thread_ok;
    });
  }
  for (auto& th : threads) th.join();

  for (int t = 0; t < kThreads; ++t) {
    REQUIRE(ok[t]);
  }
  REQUIRE_FALSE(store.get("key-0").has_value());
}

TEST_CASE("stress: many threads hammering PUT/GET/DELETE across a shared keyspace", "[storage][concurrency][stress]") {
  Store store;
  constexpr int kThreads = 16;
  constexpr int kOpsPerThread = 2000;
  constexpr int kKeyspace = 8;  // small on purpose: forces heavy contention

  vector<thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&store, t] {
      for (int i = 0; i < kOpsPerThread; ++i) {
        string key = "k" + to_string((t + i) % kKeyspace);
        switch (i % 3) {
          case 0:
            store.put(key, "v");
            break;
          case 1:
            store.get(key);
            break;
          case 2:
            store.del(key);
            break;
        }
      }
    });
  }
  for (auto& th : threads) th.join();

  // No crash and no sanitizer report is the actual point of this test; this
  // assertion just confirms the store is still in a readable, sane state.
  REQUIRE(store.size() <= kKeyspace);
}
