# Low-Latency Feed Handler

A C++20 market-data feed handler: ingest a raw NASDAQ ITCH 5.0 feed (mmap'd
file or live UDP replay), parse and normalize each message into a fixed
64-byte struct, and publish it to a separate consumer process. The core of
the project is comparing two IPC transports for that hand-off — a lock-free
SPSC ring buffer in shared memory vs. an `AF_UNIX` socket — on end-to-end
latency and throughput.

## Design overview

- **Ingestion** (`src/ingest_source.hpp`, `ingest_mmap.hpp`, `ingest_socket.hpp`) —
  a common `IngestSource` interface over two modes: file mode `mmap`s the
  `.itch` file directly (zero-copy, no network involved), and socket mode
  reads MoldUDP64-framed UDP datagrams (NASDAQ's real batching protocol). A
  `replay` tool re-emits a file over UDP so socket mode can be exercised
  end-to-end, either at max rate or paced to the original inter-message gaps.
  Both sources drop malformed input instead of handing it on: records
  shorter than their message type's wire size, and (over UDP) lengths or
  message counts that run past the datagram, plus sequence numbers already
  delivered. A missing or unreadable data file is an error, not an empty feed.
- **Dispatch & handlers** (`src/dispatch.hpp`, `handlers.hpp`) — a `switch` on
  the message-type byte routes each record to a handler that byte-swaps the
  packed wire struct into a normalized message, returned by value.
- **Normalization** (`src/normalise.hpp`) — the order-book-relevant message
  types (`S R A F E C X D U P`) collapse into one fixed-size, 64-byte,
  trivially-copyable `NormalisedMessage` (one cache line), so it can be pushed
  straight into a shared-memory ring buffer or written raw over a socket with
  no serialization step. Other types are dropped. Order Replace (`U`) carries
  both the new and the original order reference.
- **IPC bus** (`src/spsc-ring-buffer.hpp`, `ipc/`) — `producer_main` ingests
  and dispatches like a normal feed handler, then publishes each
  `NormalisedMessage` to a standalone `consumer_main` over one of two
  transports, chosen at compile time: a shared-memory SPSC ring buffer
  (acquire/release atomics, no syscalls), or an `AF_UNIX SOCK_SEQPACKET`
  socket (the baseline — a syscall plus a kernel-mediated copy per message).
  The ring's shared-memory segment (owner-only, `0600`) has a small handshake:
  the producer publishes nothing until a consumer has claimed the ring, a
  consumer ignores segments left behind by a dead producer, and either side
  notices if its peer dies instead of waiting forever. A consumer whose
  stream ends without the producer's end marker exits non-zero, since its
  results are partial.
- **Latency measurement** (`src/latency_histogram.hpp`) — a zero-allocation,
  power-of-two-bucketed histogram, fed `rdtsc` cycles for in-process
  parse/dispatch latency and `CLOCK_MONOTONIC` nanoseconds for cross-process
  end-to-end IPC latency.

## Build

```bash
cmake -S . -B build
cmake --build build -j
```

Produces `feed_handler` (in-process ingest+dispatch, no IPC — a diagnostic
tool), `ipc_producer_ring`/`ipc_consumer_ring`, `ipc_producer_unix`/
`ipc_consumer_unix`, and `replay`. All default to file mode; pass
`--udp=PORT` to switch to socket mode (fed by `replay`) with no rebuild:

```bash
./build/ipc_consumer_ring [histogram.csv] &   # launch order doesn't matter
./build/ipc_producer_ring                     # file mode; waits for the consumer
```

(swap `_ring` for `_unix` for the socket baseline).

## Tests

```bash
tests/run_tests.sh
```

Builds everything with plain `g++` into `build/tests/` (no CMake or data
download needed) and runs the whole suite; exits non-zero if any test fails.

- `tests/test_parsing.cpp`: CLI args, ITCH struct layout, handlers and
  dispatch, MoldUDP64 framing, and the mmap and UDP ingestion sources.
- `tests/test_core.cpp`: the SPSC ring (including a two-thread ordering
  stress test), the latency histogram, and the timers.
- `tests/test_transports.cpp`: both IPC transports across real processes.
  Covers the happy path in both launch orders, a peer dying, stale or
  garbage shared-memory segments, two consumers competing for one ring,
  and cleanup on scope exit.
- `tests/integration_test.py`: drives the built binaries end to end on
  synthetic ITCH data. Covers `feed_handler`, `replay` framing and pacing,
  the producer/consumer pairs, killing either side mid-stream, bad inputs,
  `bench/run_ipc_comparison.sh` (run from a throwaway copy with `cmake`
  stubbed out, so it never writes to `bench/results/`), and build checks
  (standalone headers, multi-TU linking, no warnings).

The parsing and core tests also run under ASan/UBSan. Each C++ test runs in
its own forked process with a deadline, so a crash or hang shows up as a
failed test rather than stopping the run. The suite uses the same shared-memory
and socket names as the benchmark, so don't run the two at the same time.

## Benchmarking

Run `bash download_data.sh` first to fetch the NASDAQ sample feed into `data/`.

### Standard benchmark

`bench/run_ipc_isolated.sh` is the standard benchmarking method. It enables CPU
core isolation to reduce latency variance from scheduler noise and frequency
scaling, runs both transport benchmarks, then restores the system to baseline.
Requires root.

```bash
sudo bash bench/run_ipc_isolated.sh
```

Core isolation includes:
- Disabling CPU frequency scaling (performance governor)
- Adjusting scheduler settings for latency predictability
- Redirecting system IRQs away from isolated cores
- Disabling C-state power management

Results are written to `bench/results/ipc_summary.csv` plus per-bucket histograms.

### Baseline benchmark (no isolation)

For comparison or on systems where root is unavailable:

```bash
bash bench/run_ipc_comparison.sh
```

This pins producer/consumer to separate cores via `taskset` but does not apply
system-level isolation, so results will include scheduler noise and frequency-scaling variance.

### Visualization

Generate latency and throughput plots from benchmark results:

```bash
jupyter nbconvert --to notebook --execute --inplace bench/benchmark_plots.ipynb
```

## Results

Same 200MB ITCH slice, 6,948,075 messages published end to end (published
and received counts matched exactly — neither transport drops messages),
producer and consumer pinned to separate cores.

![End-to-end latency percentiles and sustained throughput, ring buffer vs. AF_UNIX socket](bench/plots/ipc_summary.png)

The ring buffer holds an **8×** latency advantage at p50/p95/p99 and a
**~29×** throughput advantage. This tracks the design: the ring buffer path
is one shared-memory write with no syscall, while the socket path pays a
`send()`/`recv()` syscall pair and two kernel-mediated copies per message.

![Full end-to-end latency distribution, per power-of-two bucket, ring buffer vs. AF_UNIX socket](bench/plots/ipc_latency_distribution.png)

The full distribution makes the same point more concretely: the socket
path's entire mass sits an order of magnitude higher than the ring buffer's,
not just its tail.
