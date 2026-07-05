#!/usr/bin/env bash
# Rebalance-overhead benchmark: foreground PUT throughput with vs. without a
# 4->8 shard rebalance_to() running concurrently, at a few throttle
# settings, plus a data-loss check. See docs/benchmarks.md for methodology
# and results.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${ROOT_DIR}/build"
BIN="${BUILD_DIR}/tools/kvstore_bench_rebalance"
OPS_PER_THREAD="${1:-2000}"
THREADS="${2:-4}"
shift || true
shift || true

if [ ! -x "${BIN}" ]; then
  echo "bench_rebalance.sh: ${BIN} not found — build first: ./scripts/build.sh Release" >&2
  exit 1
fi

"${BIN}" "${OPS_PER_THREAD}" "${THREADS}" "$@"
