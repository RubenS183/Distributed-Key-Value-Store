#!/usr/bin/env sh
set -eu
ctest --test-dir build/dev --output-on-failure
