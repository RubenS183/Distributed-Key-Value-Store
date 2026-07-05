#!/usr/bin/env bash
# Concurrency benchmark: single global lock (Store) vs sharded locking
# (ShardedStore) PUT throughput across writer thread counts. See
# docs/benchmarks.md for methodology and results.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${ROOT_DIR}/build"
BIN="${BUILD_DIR}/tools/kvstore_bench_concurrency"
OPS_PER_THREAD="${1:-2000}"
shift || true

if [ ! -x "${BIN}" ]; then
  echo "bench_concurrency.sh: ${BIN} not found — build first: ./scripts/build.sh Release" >&2
  exit 1
fi

"${BIN}" "${OPS_PER_THREAD}" "$@"
