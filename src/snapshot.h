#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

using namespace std;

namespace kvstore {

// One entry as captured from Store's map at snapshot time. Mirrors
// storage::Entry's fields exactly, including tombstones — a deleted key's
// version/write_id must survive a restore unchanged (not reset to 1 on the
// next write to that key), so tombstones are snapshotted like any other
// entry rather than dropped. See docs/architecture.md's "Snapshot Format"
// section for the on-disk byte layout and docs/design-decisions.md for why
// tombstones are included.
struct SnapshotEntry {
  string key;
  string value;  // empty for tombstones
  uint64_t version = 0;
  uint64_t write_id = 0;
  bool tombstone = false;
};

// Result of loading `<dir>/snapshot.bin`.
struct LoadedSnapshot {
  // Highest WAL write_id already reflected in `entries`. A caller replaying
  // the WAL on top of this snapshot must skip every record with
  // write_id <= boundary_write_id.
  uint64_t boundary_write_id = 0;
  vector<SnapshotEntry> entries;
};

// Serializes `entries` to `<dir>/snapshot.bin`: written in full to
// `<dir>/snapshot.tmp`, fsynced, then atomically renamed over any previous
// snapshot at that fixed path (the directory is also fsynced after the
// rename, so the rename itself survives a crash). `dir` is created if it
// doesn't exist. Only ever one snapshot file exists at the final path —
// there is no retention of older snapshots; see docs/design-decisions.md
// for why. Throws runtime_error on any filesystem failure.
void write_snapshot(const string& dir, uint64_t boundary_write_id,
                     const vector<SnapshotEntry>& entries);

// Loads `<dir>/snapshot.bin`, or nullopt if it doesn't exist (e.g. a server
// that has never been snapshotted). Throws runtime_error if the file
// exists but fails to parse (bad magic/version, or truncated).
optional<LoadedSnapshot> load_latest(const string& dir);

}  // namespace kvstore
