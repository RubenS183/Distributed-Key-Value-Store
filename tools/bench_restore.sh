#!/usr/bin/env bash
# Restore-time benchmark: load N keys, take a snapshot, SIGKILL the process,
# then measure wall-clock time for a fresh process to load the snapshot +
# replay the WAL tail ("ready to serve"). See docs/benchmarks.md for
# methodology, results, and how to interpret this number.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${ROOT_DIR}/build"
BIN="${BUILD_DIR}/tools/kvstore_bench_restore"
NUM_KEYS="${1:-100000}"

if [ ! -x "${BIN}" ]; then
  echo "bench_restore.sh: ${BIN} not found — build first: ./scripts/build.sh Release" >&2
  exit 1
fi

DATA_DIR="$(mktemp -d)"
trap 'rm -rf "${DATA_DIR}"' EXIT

echo "Loading ${NUM_KEYS} keys and taking a snapshot in ${DATA_DIR} ..."
"${BIN}" load "${DATA_DIR}" "${NUM_KEYS}" &
LOAD_PID=$!

# Poll for the marker file instead of sleeping a guessed duration — avoids
# either killing the loader before the snapshot's fsync+rename completes,
# or wasting time waiting longer than necessary.
while [ ! -f "${DATA_DIR}/.ready" ]; do
  sleep 0.02
done

echo "Snapshot complete — SIGKILLing pid ${LOAD_PID} to simulate a crash ..."
kill -9 "${LOAD_PID}"
wait "${LOAD_PID}" 2>/dev/null || true

echo "Restoring from a cold process ..."
"${BIN}" restore "${DATA_DIR}"
