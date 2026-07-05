#!/usr/bin/env bash
# Multi-node throughput / latency harness: starts one leader plus two
# followers and runs the same networked load generator against the leader,
# so the resulting numbers include the cost of the leader streaming every
# write to two replicas in the background. Compare these rows directly
# against bench_throughput.sh's single-node rows to read off replication's
# foreground overhead. See docs/benchmarks.md.
#
# The two followers are started with an EMPTY peer list on purpose: that
# gives them a fixed follower role with no FailoverMonitor, so they act as
# pure replication targets and no spurious leader election can fire mid-run
# if a heavily-loaded leader is briefly slow to heartbeat. Replication
# itself is unaffected — it's driven by the leader's ReplicationLinks, which
# only need the leader's own peer list. See docs/architecture.md's
# "Replication" and "Failover" sections.
#
# Usage: bench/bench_multinode.sh [clients] [ops_per_client] [keys_per_client]
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
require_bins "${SERVER_BIN}" "${THROUGHPUT_BIN}"

CLIENTS="${1:-8}"
OPS_PER_CLIENT="${2:-20000}"
KEYS_PER_CLIENT="${3:-5000}"
LEADER_PORT=6390
F1_PORT=6391
F2_PORT=6392
SHARDS=8

DATA_DIR="$(mktemp -d)"
LEADER_PID=""; F1_PID=""; F2_PID=""
cleanup() {
  stop_server "${LEADER_PID}"; stop_server "${F1_PID}"; stop_server "${F2_PID}"
  rm -rf "${DATA_DIR}"
}
trap cleanup EXIT

echo "# multi-node: 1 leader + 2 followers, ${SHARDS} shards, ${CLIENTS} clients, ${OPS_PER_CLIENT} ops/client, ${KEYS_PER_CLIENT} keys/client"

# Followers first (empty peers → pure replication targets, see header), then
# the leader pointed at both so its ReplicationLinks connect on startup.
F1_PID="$(start_server "${F1_PORT}" "${DATA_DIR}/wal_f1" "${DATA_DIR}/snap_f1" "${SHARDS}" follower "" "${DATA_DIR}/f1.log")"
F2_PID="$(start_server "${F2_PORT}" "${DATA_DIR}/wal_f2" "${DATA_DIR}/snap_f2" "${SHARDS}" follower "" "${DATA_DIR}/f2.log")"
LEADER_PID="$(start_server "${LEADER_PORT}" "${DATA_DIR}/wal_l" "${DATA_DIR}/snap_l" "${SHARDS}" leader "127.0.0.1:${F1_PORT},127.0.0.1:${F2_PORT}" "${DATA_DIR}/leader.log")"

for workload in read-heavy write-heavy mixed; do
  if [ "${workload}" = "read-heavy" ]; then
    "${THROUGHPUT_BIN}" 127.0.0.1 "${LEADER_PORT}" "${CLIENTS}" "${OPS_PER_CLIENT}" "${workload}" "${KEYS_PER_CLIENT}"
  else
    "${THROUGHPUT_BIN}" 127.0.0.1 "${LEADER_PORT}" "${CLIENTS}" "${OPS_PER_CLIENT}" "${workload}" "${KEYS_PER_CLIENT}" | tail -n +2
  fi
done
