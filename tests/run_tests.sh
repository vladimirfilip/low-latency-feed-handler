#!/usr/bin/env bash
# Builds and runs the whole test suite with plain g++ (no CMake needed).
# Exits non-zero if any test fails.
#
# Uses the same global IPC names as the benchmark (/dev/shm/feed_handler_norm_ring,
# /tmp/feed_handler_norm.sock): don't run it concurrently with the benchmark.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$ROOT/build/tests"
BIN="$OUT/bin"
mkdir -p "$BIN"

CXX="${CXX:-g++}"
RELEASE=(-std=c++20 -O3 -DNDEBUG -fno-strict-aliasing)        # CMake Release flags, strict ISO mode
TEST=(-std=c++20 -O2 -g -fno-strict-aliasing -Wall -Wextra -pthread)
INC=(-I"$ROOT/src" -I"$ROOT/ipc")

build() { # out, flags..., sources...
    local out="$1"; shift
    if ! "$CXX" "$@" -o "$out" -lrt; then
        echo "BUILD FAILED: $out" >&2
        exit 2
    fi
}

echo "== building binaries under test (Release flags)"
build "$BIN/feed_handler"      "${RELEASE[@]}" "${INC[@]}" "$ROOT/src/main.cpp"
build "$BIN/replay"            "${RELEASE[@]}" "${INC[@]}" "$ROOT/replay/replay.cpp"
build "$BIN/ipc_producer_ring" "${RELEASE[@]}" "${INC[@]}" "$ROOT/ipc/producer_main.cpp"
build "$BIN/ipc_consumer_ring" "${RELEASE[@]}" "${INC[@]}" "$ROOT/ipc/consumer_main.cpp"
build "$BIN/ipc_producer_unix" "${RELEASE[@]}" "${INC[@]}" -DUSE_UNIX_SOCKET "$ROOT/ipc/producer_main.cpp"
build "$BIN/ipc_consumer_unix" "${RELEASE[@]}" "${INC[@]}" -DUSE_UNIX_SOCKET "$ROOT/ipc/consumer_main.cpp"

echo "== building unit tests"
build "$OUT/test_parsing"    "${TEST[@]}" "${INC[@]}" "$ROOT/tests/test_parsing.cpp"
build "$OUT/test_core"       "${TEST[@]}" "${INC[@]}" "$ROOT/tests/test_core.cpp"
build "$OUT/test_transports" "${TEST[@]}" "${INC[@]}" "$ROOT/tests/test_transports.cpp"
# Same unit tests under ASan/UBSan, to catch memory errors the assertions miss.
SAN=(-fsanitize=address,undefined -fno-omit-frame-pointer)
build "$OUT/test_parsing_asan" "${TEST[@]}" "${SAN[@]}" "${INC[@]}" "$ROOT/tests/test_parsing.cpp"
build "$OUT/test_core_asan"    "${TEST[@]}" "${SAN[@]}" "${INC[@]}" "$ROOT/tests/test_core.cpp"

status=0
run() {
    echo
    echo "== $*"
    "$@" || status=1
}
export CXX FH_RELEASE_FLAGS="${RELEASE[*]}" # for the integration suite's build checks
export ASAN_OPTIONS="detect_leaks=0:abort_on_error=1"
export UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1"
run "$OUT/test_parsing"
run "$OUT/test_core"
run "$OUT/test_transports"
run "$OUT/test_parsing_asan"
run "$OUT/test_core_asan"
run python3 "$ROOT/tests/integration_test.py" "$BIN"

echo
if [ "$status" -eq 0 ]; then echo "ALL SUITES PASSED"; else echo "SOME TESTS FAILED"; fi
exit "$status"
