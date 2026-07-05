// Restore-time benchmark tool (stage 5 resume-claim number): measures the
// wall-clock time to go from "process just started" to "snapshot + WAL tail
// fully applied, ready to serve" — the same recovery path main.cpp runs at
// real server startup. Talks to Store/WriteAheadLog directly (no TCP, no
// protocol framing) since loading N keys over the network is not the thing
// being measured here; see docs/benchmarks.md for the full methodology and
// bench_restore.sh for how the two subcommands below are driven together.
//
// Usage:
//   kvstore_bench_restore load <dir> <num_keys>
//     Starts a fresh WAL+Store under <dir>, PUTs <num_keys> generated
//     keys, takes a snapshot, writes a "<dir>/.ready" marker file, then
//     blocks (pause()) so the driving script can SIGKILL this process —
//     simulating a crash immediately after a completed snapshot.
//   kvstore_bench_restore restore <dir>
//     Starts a fresh WAL+Store over the same <dir> and times
//     load_snapshot() + recover_from_wal(), then prints the result.
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>

#include "store.h"
#include "wal.h"

using namespace std;

namespace {

string wal_subdir(const string& dir) { return dir + "/wal_data"; }
string snapshot_subdir(const string& dir) { return dir + "/snapshot_data"; }

int run_load(const string& dir, size_t num_keys) {
  kvstore::WriteAheadLog wal(wal_subdir(dir));
  kvstore::Store store(&wal);

  for (size_t i = 0; i < num_keys; ++i) {
    string key = "key" + to_string(i);
    string value = "value" + to_string(i);
    store.put(key, value);
  }

  store.take_snapshot(snapshot_subdir(dir));

  ofstream marker(dir + "/.ready");
  marker.close();

  cout << "kvstore_bench_restore: loaded and snapshotted " << num_keys
            << " keys, waiting to be killed\n"
            << flush;

  ::pause();  // never returns; the driving script SIGKILLs this process
  return 0;
}

int run_restore(const string& dir) {
  auto start = chrono::steady_clock::now();

  kvstore::WriteAheadLog wal(wal_subdir(dir));
  kvstore::Store store(&wal);
  size_t snapshot_entries = store.load_snapshot(snapshot_subdir(dir));
  size_t replayed = store.recover_from_wal();

  auto elapsed_ms =
      chrono::duration<double, milli>(chrono::steady_clock::now() - start).count();

  cout << "kvstore_bench_restore: restore complete\n"
            << "  snapshot entries loaded: " << snapshot_entries << "\n"
            << "  WAL records replayed:    " << replayed << "\n"
            << "  final key count:         " << store.size() << "\n"
            << "  wall-clock time:          " << elapsed_ms << " ms\n";
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 3) {
    cerr << "usage: " << argv[0] << " load <dir> <num_keys>\n"
              << "       " << argv[0] << " restore <dir>\n";
    return 1;
  }

  string subcommand = argv[1];
  string dir = argv[2];

  if (subcommand == "load") {
    if (argc < 4) {
      cerr << "usage: " << argv[0] << " load <dir> <num_keys>\n";
      return 1;
    }
    return run_load(dir, static_cast<size_t>(strtoull(argv[3], nullptr, 10)));
  }
  if (subcommand == "restore") {
    return run_restore(dir);
  }

  cerr << "unknown subcommand '" << subcommand << "'\n";
  return 1;
}
