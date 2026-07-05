# Shared helpers for the bench/ harness scripts. Sourced, not executed.
# Provides: resolved ROOT_DIR/BUILD_DIR/binary paths, a require_bins check,
# and start_server/stop_server helpers that manage real kvstore_server
# processes over loopback TCP. See docs/benchmarks.md for what each harness
# measures.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${ROOT_DIR}/build"
SERVER_BIN="${BUILD_DIR}/src/kvstore_server"
THROUGHPUT_BIN="${BUILD_DIR}/tools/kvstore_bench_throughput"

# Fails with a build hint unless every path passed to it is an executable.
require_bins() {
  for bin in "$@"; do
    if [ ! -x "${bin}" ]; then
      echo "bench: ${bin} not found — build first: ./scripts/build.sh Release" >&2
      exit 1
    fi
  done
}

# start_server <port> <wal_dir> <snapshot_dir> <shards> <role> <peers> <logfile>
# Launches one kvstore_server in the background, blocks until the port is
# actually accepting connections (so callers never race the accept loop), and
# echoes its PID. We poll the socket rather than the log because the server's
# "listening" line is written to a fully-buffered cout when redirected and
# won't reach the file until the process exits.
start_server() {
  local port="$1" wal_dir="$2" snap_dir="$3" shards="$4" role="$5" peers="$6" logfile="$7"
  "${SERVER_BIN}" "${port}" "${wal_dir}" "${snap_dir}" "${shards}" "${role}" "${peers}" \
    >"${logfile}" 2>&1 &
  local pid=$!
  local waited=0
  while ! (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; do
    sleep 0.05
    waited=$((waited + 1))
    if ! kill -0 "${pid}" 2>/dev/null; then
      echo "bench: server on port ${port} exited during startup — log follows:" >&2
      cat "${logfile}" >&2
      exit 1
    fi
    if [ "${waited}" -gt 200 ]; then  # 10s
      echo "bench: server on port ${port} never started accepting — log follows:" >&2
      cat "${logfile}" >&2
      kill -9 "${pid}" 2>/dev/null || true
      exit 1
    fi
  done
  exec 3>&- 2>/dev/null || true
  echo "${pid}"
}

# stop_server <pid> — SIGKILL (we don't care about a clean shutdown for a
# throwaway benchmark server) and reap it.
stop_server() {
  local pid="$1"
  [ -n "${pid}" ] || return 0
  kill -9 "${pid}" 2>/dev/null || true
  wait "${pid}" 2>/dev/null || true
}
