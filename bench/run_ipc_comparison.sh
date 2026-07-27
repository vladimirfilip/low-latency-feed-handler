#!/usr/bin/env bash
# Producer and consumer are separate OS processes. This drives both
# IPC transports end to end and compares end-to-end latency (producer
# ingest -> consumer observe) and throughput:
#
#   ring  - SPSC shared-memory ring buffer (mmap, no syscall per message)
#   unix  - AF_UNIX SOCK_SEQPACKET socket (syscall + kernel copy per message)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="$ROOT/build"
RESULTS_DIR="$ROOT/bench/results"
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

if ! command -v taskset >/dev/null 2>&1; then
    echo "taskset not found (needed for core pinning); install util-linux." >&2
    exit 1
fi

# Producer and consumer are pinned to distinct cores — they're separate
# processes exchanging data cross-core, and pinning both to the same core
# would just serialize them via preemption, hiding the IPC cost we're trying
# to measure. Override with BENCH_PRODUCER_CORE / BENCH_CONSUMER_CORE.
NPROC="$(nproc)"
PRODUCER_CORE="${BENCH_PRODUCER_CORE:-$((NPROC - 1))}"
CONSUMER_CORE="${BENCH_CONSUMER_CORE:-$((NPROC - 2))}"
if [ "$PRODUCER_CORE" = "$CONSUMER_CORE" ] || [ "$CONSUMER_CORE" -lt 0 ]; then
    echo "Need at least 2 cores to pin producer/consumer separately (nproc=$NPROC)." >&2
    exit 1
fi

mkdir -p "$RESULTS_DIR"

echo "Building producer/consumer binaries for both transports..."
cmake -S "$ROOT" -B "$BUILD_DIR" >/dev/null
cmake --build "$BUILD_DIR" -j >/dev/null

get_metric() {
    grep -oP "^${1}: \K[0-9.]+" "$2" || echo "n/a"
}

run_variant() {
    local variant="$1" # ring | unix
    local producer_bin="$BUILD_DIR/ipc_producer_${variant}"
    local consumer_bin="$BUILD_DIR/ipc_consumer_${variant}"
    local hist_out="$RESULTS_DIR/ipc_histogram_${variant}.csv"
    local producer_log="$TMPDIR/producer_${variant}.log"
    local consumer_log="$TMPDIR/consumer_${variant}.log"

    # Retry loops in both transports (ring: attach-until-created; unix:
    # connect-until-listening) make launch order safe either way — the
    # consumer is started first here only so it's ready to instrument from
    # the very first message rather than because correctness needs it.
    taskset -c "$CONSUMER_CORE" "$consumer_bin" "$hist_out" >"$consumer_log" 2>&1 &
    local consumer_pid=$!

    taskset -c "$PRODUCER_CORE" "$producer_bin" >"$producer_log" 2>&1 &
    local producer_pid=$!

    if ! wait "$producer_pid"; then
        echo "producer (${variant}) failed:" >&2
        cat "$producer_log" >&2
        exit 1
    fi
    if ! wait "$consumer_pid"; then
        echo "consumer (${variant}) failed:" >&2
        cat "$consumer_log" >&2
        exit 1
    fi

    local published received
    published="$(grep -oP 'published \K[0-9]+' "$producer_log" || echo "n/a")"
    received="$(get_metric count "$consumer_log")"
    if [ "$published" != "$received" ]; then
        echo "warning: ${variant} published=${published} received=${received} (mismatch)" >&2
    fi
}

echo "Pinning producer -> core $PRODUCER_CORE, consumer -> core $CONSUMER_CORE"
echo "Running ring-buffer transport..."
run_variant ring
echo "Running AF_UNIX socket transport..."
run_variant unix

printf "\n%-20s %20s %20s\n" "metric" "ring buffer" "unix socket"
printf "%-20s %20s %20s\n" "--------------------" "--------------------" "--------------------"
for metric in count p50 p95 p99 p99.9 max throughput; do
    ring_val="$(get_metric "$metric" "$TMPDIR/consumer_ring.log")"
    unix_val="$(get_metric "$metric" "$TMPDIR/consumer_unix.log")"
    label="$metric"
    case "$metric" in
        p50|p95|p99|p99.9|max) label="${metric} (ns)" ;;
        throughput) label="throughput (msg/s)" ;;
    esac
    printf "%-20s %20s %20s\n" "$label" "$ring_val" "$unix_val"
done

summary_csv="$RESULTS_DIR/ipc_summary.csv"
{
    echo "variant,count,p50_ns,p95_ns,p99_ns,p99.9_ns,max_ns,throughput_msgs_per_sec"
    for variant in ring unix; do
        log="$TMPDIR/consumer_${variant}.log"
        echo "${variant},$(get_metric count "$log"),$(get_metric p50 "$log"),$(get_metric p95 "$log"),$(get_metric p99 "$log"),$(get_metric p99.9 "$log"),$(get_metric max "$log"),$(get_metric throughput "$log")"
    done
} > "$summary_csv"
echo ""
echo "Wrote per-bucket histograms to $RESULTS_DIR/ipc_histogram_{ring,unix}.csv"
echo "Wrote summary to $summary_csv"
