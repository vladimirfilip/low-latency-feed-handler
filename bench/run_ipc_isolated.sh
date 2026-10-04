#!/usr/bin/env bash
# Runs the IPC benchmark on isolated CPUs: isolate_cores.sh, then
# run_ipc_comparison.sh pinned to those CPUs, then isolate_cleanup.sh (always,
# even if the benchmark fails). Requires root and the one-time boot setup in
# the README.
#
# Usage: sudo bash bench/run_ipc_isolated.sh [PRODUCER_CPU CONSUMER_CPU]
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$BENCH_DIR")"
STATE=/run/bench-isolation.state

if [ "$EUID" -ne 0 ]; then
    echo "Error: requires root (use: sudo $0)" >&2
    exit 1
fi

# Build as the invoking user so root doesn't leave root-owned files in build/.
if [ -n "${SUDO_USER:-}" ]; then
    echo "Building as $SUDO_USER..."
    sudo -u "$SUDO_USER" cmake -S "$ROOT" -B "$ROOT/build" >/dev/null
    sudo -u "$SUDO_USER" cmake --build "$ROOT/build" -j >/dev/null
fi

cleanup() {
    echo ""
    bash "$BENCH_DIR/isolate_cleanup.sh"
    if [ -n "${SUDO_USER:-}" ]; then
        chown -R "$SUDO_USER": "$ROOT/bench/results" "$ROOT/build"
    fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

bash "$BENCH_DIR/isolate_cores.sh" "$@"
echo ""

IFS='|' read -r _ producer_cpu consumer_cpu < <(grep '^cpus|' "$STATE")
BENCH_PRODUCER_CORE="$producer_cpu" BENCH_CONSUMER_CORE="$consumer_cpu" \
    bash "$BENCH_DIR/run_ipc_comparison.sh"
