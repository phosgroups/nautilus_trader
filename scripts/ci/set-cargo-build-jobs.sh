#!/usr/bin/env bash
set -euo pipefail

# Throttle CARGO_BUILD_JOBS on cold caches to avoid OOM on CI runners.
# Warm cache (*.d files present) uses the cargo default (all CPUs);
# cold cache limits to 4 jobs (all cores on ubuntu-latest).
# CR-0028: raised from 2 -> 4; wheels are now built with thin LTO
# (CARGO_PROFILE_RELEASE_LTO=thin), which uses far less memory than the
# previous fat LTO, so 4 parallel jobs fit on the 4-core runner.

if find "$CARGO_TARGET_DIR" -name '*.d' -print -quit 2> /dev/null | grep -q .; then
  echo "Cache is warm, using default cargo parallelism"
else
  echo "Cold cache detected, limiting to 4 build jobs"
  echo "CARGO_BUILD_JOBS=4" >> "$GITHUB_ENV"
fi
