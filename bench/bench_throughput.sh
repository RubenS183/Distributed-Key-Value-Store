#!/usr/bin/env bash
# Single-node throughput / latency harness: starts one leader kvstore_server
# (no peers, so no replication and no failover threads), then runs the
# networked load generator against it for each workload mix at a given client
# count. Prints one CSV row per workload. See docs/benchmarks.md for
# methodology and recorded numbers.
#
# Usage: bench/bench_throughput.sh [clients] [ops_per_client] [keys_per_client]
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
require_bins "${SERVER_BIN}" "${THROUGHPUT_BIN}"

CLIENTS="${1:-8}"
OPS_PER_CLIENT="${2:-20000}"
KEYS_PER_CLIENT="${3:-5000}"
PORT=6390
SHARDS=8

DATA_DIR="$(mktemp -d)"
SERVER_PID=""
cleanup() { stop_server "${SERVER_PID}"; rm -rf "${DATA_DIR}"; }
trap cleanup EXIT

echo "# single-node: 1 leader, ${SHARDS} shards, ${CLIENTS} clients, ${OPS_PER_CLIENT} ops/client, ${KEYS_PER_CLIENT} keys/client"
SERVER_PID="$(start_server "${PORT}" "${DATA_DIR}/wal" "${DATA_DIR}/snap" "${SHARDS}" leader "" "${DATA_DIR}/server.log")"

for workload in read-heavy write-heavy mixed; do
  # Header only from the first run; data row from every run.
  if [ "${workload}" = "read-heavy" ]; then
    "${THROUGHPUT_BIN}" 127.0.0.1 "${PORT}" "${CLIENTS}" "${OPS_PER_CLIENT}" "${workload}" "${KEYS_PER_CLIENT}"
  else
    "${THROUGHPUT_BIN}" 127.0.0.1 "${PORT}" "${CLIENTS}" "${OPS_PER_CLIENT}" "${workload}" "${KEYS_PER_CLIENT}" | tail -n +2
  fi
done
