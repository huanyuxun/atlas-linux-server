#!/usr/bin/env bash
# Run from repository root. No dependency installation or global OS changes.
set -u
mkdir -p results
: > results/validation-status.txt
cmake --preset release > results/configure-release.log 2>&1 &&
cmake --build --preset release -j2 > results/build-release.log 2>&1 &&
ctest --preset release > results/ctest-release.log 2>&1 || exit 1
python3 tests/integration.py --server build/atlas_server --soak-seconds "${SOAK_SECONDS:-120}" --artifacts results/integration > results/integration.log 2>&1
echo "integration_exit=$?" >> results/validation-status.txt
python3 tools/perf_run.py --server build/atlas_server --idle-connections 4096 --output results/performance > results/performance.log 2>&1
echo "performance_exit=$?" >> results/validation-status.txt
cmake --preset asan > results/configure-asan.log 2>&1 &&
cmake --build --preset asan -j2 > results/build-asan.log 2>&1 &&
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1 ctest --preset asan > results/ctest-asan.log 2>&1
echo "asan_exit=$?" >> results/validation-status.txt
cmake --preset tsan > results/configure-tsan.log 2>&1 &&
cmake --build --preset tsan -j2 > results/build-tsan.log 2>&1 &&
TSAN_OPTIONS=halt_on_error=1 ctest --preset tsan > results/ctest-tsan.log 2>&1
echo "tsan_exit=$?" >> results/validation-status.txt
echo done >> results/validation-status.txt
if grep -Eq '_exit=[1-9]' results/validation-status.txt; then
  exit 1
fi
