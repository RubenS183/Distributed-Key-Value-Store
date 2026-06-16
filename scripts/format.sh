#!/usr/bin/env sh
set -eu
find include src tests benchmarks \( -name '*.hpp' -o -name '*.cpp' \) | xargs clang-format -i
