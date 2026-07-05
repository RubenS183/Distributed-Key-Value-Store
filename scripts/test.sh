#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${ROOT_DIR}/build"

"${ROOT_DIR}/scripts/build.sh"
ctest --test-dir "${BUILD_DIR}" --output-on-failure
