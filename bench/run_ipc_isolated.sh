#!/usr/bin/env bash
# Run the IPC benchmark with core isolation enabled.
# Automatically sets up isolation before the benchmark and cleans up after.
#
# Usage:
#   sudo bash bench/run_ipc_isolated.sh [ISOLATED_CORES]
#
# Example:
#   sudo bash bench/run_ipc_isolated.sh "10,11"
#   # Runs with cores 10 and 11 isolated

set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$BENCH_DIR")"

if [ "$EUID" -ne 0 ]; then
    echo "Error: this script requires root (use: sudo $0)" >&2
    exit 1
fi

ISOLATED_CORES="${1:-}"

# Enable isolation
echo "=== Enabling core isolation ==="
bash "$BENCH_DIR/isolate_cores.sh" "$ISOLATED_CORES"
echo ""

# Trap cleanup on exit (whether benchmark succeeds or fails)
cleanup() {
    echo ""
    echo "=== Cleaning up core isolation ==="
    bash "$BENCH_DIR/isolate_cleanup.sh"
}
trap cleanup EXIT

# Run benchmark
echo "=== Running IPC benchmark with core isolation ==="
echo ""
bash "$BENCH_DIR/run_ipc_comparison.sh"
