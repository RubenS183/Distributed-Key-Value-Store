#!/usr/bin/env bash
# Master benchmark harness: runs every benchmark in docs/benchmarks.md in one
# pass. Assumes a Release build already exists (./scripts/build.sh Release) —
# it does not build anything itself. Each section prints the exact same
# command you could run standalone.
#
# IMPORTANT: the numbers in docs/benchmarks.md are the *isolated* per-command
# numbers (each run on an otherwise-idle machine). Running everything
# back-to-back here leaves the machine warm/loaded, so the values this script
# prints — especially restore time and absolute throughput — run higher/
# noisier than the documented ones. Use the standalone command each section
# echoes (also in docs/benchmarks.md's "Reproducing" blocks) to reproduce a
# specific documented number cleanly; use this script to sanity-check that
# the whole suite still runs green.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

hr() { printf '\n========== %s ==========\n' "$1"; }

hr "single-node throughput / latency (bench/bench_throughput.sh)"
"${ROOT_DIR}/bench/bench_throughput.sh" 8 20000 5000

hr "multi-node throughput / latency — leader + 2 followers (bench/bench_multinode.sh)"
"${ROOT_DIR}/bench/bench_multinode.sh" 8 20000 5000

hr "concurrency: single-lock vs sharded (tools/bench_concurrency.sh)"
"${ROOT_DIR}/tools/bench_concurrency.sh" 5000 1 2 4 8 16 32

hr "restore time, 100K keys (tools/bench_restore.sh)"
"${ROOT_DIR}/tools/bench_restore.sh" 100000

hr "restore time, 1M keys (tools/bench_restore.sh)"
"${ROOT_DIR}/tools/bench_restore.sh" 1000000

hr "rebalance overhead (tools/bench_rebalance.sh)"
"${ROOT_DIR}/tools/bench_rebalance.sh" 5000 4 0 50000 10000

hr "done"
