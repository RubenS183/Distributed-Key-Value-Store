#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>

using namespace std;

namespace kvstore {

// Where ShardedStore::rebalance_to() currently is in the online-resharding
// state machine: idle (no rebalance ever run, or the last one finished) ->
// migrating (throttled copy + live dual-write in progress) -> cutover
// (copy done, draining in-flight writers and making the merged state
// durable) -> done. See docs/design-decisions.md's "Online Resharding"
// section.
enum class RebalanceState { kIdle, kMigrating, kCutover, kDone };

// Paces ShardedStore::rebalance_to()'s copy loop: a plain token bucket over
// bytes copied, refilled by elapsed wall-clock time rather than a
// background timer thread, since the copy loop itself is the only consumer
// and can just compute "how many tokens exist right now" whenever it needs
// to spend some. rate_bytes_per_sec == 0 means unlimited (consume() never
// sleeps) -- the default for a rebalance_to() call that isn't throttled.
class RateLimiter {
 public:
  explicit RateLimiter(size_t rate_bytes_per_sec);

  // Blocks (sleeps) as needed so that, averaged over time, no more than
  // rate_bytes_per_sec bytes are consumed per second across every call.
  void consume(size_t bytes);

 private:
  size_t rate_bytes_per_sec_;
  double tokens_;
  chrono::steady_clock::time_point last_refill_;
};

// Persisted record of which generation of shard directories is currently
// active. Read by ShardedStore's constructor (authoritative over the
// num_shards constructor argument when present) and written atomically by
// rebalance_to() at cutover. Absent entirely -- the common case, a store
// that has never been rebalanced -- means "generation 0,
// <wal_dir>/shard_<i>": today's exact, unchanged directory layout, so every
// pre-stage-9 deployment/test is unaffected. See shard_dir() below and
// docs/design-decisions.md's "Online Resharding" section.
struct ShardManifest {
  uint64_t generation = 0;
  size_t num_shards = 0;
};

// Reads `<wal_dir>/shards.manifest`, or nullopt if it doesn't exist (a store
// that predates rebalancing, or one that has never been rebalanced). Throws
// runtime_error if the file exists but fails to parse.
optional<ShardManifest> read_shard_manifest(const string& wal_dir);

// Atomically (temp file + fsync + rename + directory fsync, mirroring
// snapshot.cpp's write_snapshot()) writes `<wal_dir>/shards.manifest` --
// the single commit point that makes a rebalance's new generation the one a
// future restart will use. Throws runtime_error on any filesystem failure.
void write_shard_manifest(const string& wal_dir, const ShardManifest& manifest);

// Write-preferring reader-writer lock: implements lock()/unlock() and
// lock_shared()/unlock_shared(), so std::unique_lock/std::shared_lock work
// with it directly as drop-in replacements for std::shared_mutex. Exists
// specifically for ShardedStore::cutover_mutex_ because std::shared_mutex
// gives *no* fairness guarantee between shared and exclusive waiters, and
// this was not a theoretical concern: benchmarking rebalance_to() with a
// few writer threads calling PUT back-to-back with zero idle gaps
// measurably starved its pending exclusive lock indefinitely on this
// platform's std::shared_mutex. This class instead guarantees a waiting
// writer is admitted as soon as currently-active readers finish — no new
// reader is admitted once a writer is waiting or active, so a rebalance's
// brief cutover section can never be starved by continuous foreground
// write traffic. See docs/design-decisions.md's "Online Resharding"
// section.
class WritePreferringLock {
 public:
  void lock_shared();
  void unlock_shared();
  void lock();
  void unlock();

 private:
  mutex mutex_;
  condition_variable cv_;
  int active_readers_ = 0;
  int waiting_writers_ = 0;
  bool writer_active_ = false;
};

// Shard directory naming: generation 0 is exactly today's `<root>/shard_<i>`
// (so a store that has never been rebalanced has byte-identical paths to
// every pre-stage-9 test/tool); generation k > 0 is
// `<root>/gen_<k>/shard_<i>`, used for the duration of one rebalance_to()
// call and, once committed, for as long as that generation stays active.
string shard_dir(const string& root, uint64_t generation, size_t shard_index);

// Removes any `<root>/gen_<k>` directory whose k isn't `active_generation`
// (and, if active_generation is 0, every `gen_*` directory) -- leftover
// staging data from a rebalance_to() call that never reached cutover (e.g.
// a crash mid-migration). Safe to call unconditionally at startup: the
// source shards a rebalance was copying *from* are never staged under a
// `gen_*` directory (generation 0 lives directly at `<root>/shard_<i>`,
// and an already-committed generation's directory equals active_generation
// and is therefore kept), so discarding an uncommitted generation never
// touches data that's still authoritative anywhere. Best-effort: filesystem
// errors are swallowed, not thrown -- a leftover staging directory wastes
// disk space but is otherwise harmless and gets swept on the next restart.
void discard_stale_generations(const string& root, uint64_t active_generation);

}  // namespace kvstore
