# Low-Latency Feed Handler

A C++20 market-data feed handler that parses a NASDAQ ITCH 5.0 feed and
publishes normalized messages to a separate consumer process. The point of the
project is comparing two transports for that hand-off on end-to-end latency
and throughput: a lock-free SPSC ring buffer in shared memory and an
`AF_UNIX` socket.

## Design overview

- **Ingestion** (`src/ingest_source.hpp`, `ingest_mmap.hpp`, `ingest_socket.hpp`):
  one `IngestSource` interface with two modes. File mode `mmap`s the `.itch`
  file. Socket mode reads MoldUDP64-framed UDP datagrams, sent by the `replay`
  tool at max rate or at the feed's original pacing. Both drop malformed input
  (truncated records, lengths or counts that overrun a datagram, repeated
  sequence numbers). A missing data file is an error, not an empty feed.
- **Dispatch & handlers** (`src/dispatch.hpp`, `handlers.hpp`): a `switch` on
  the message-type byte routes each record to a handler that byte-swaps the
  packed wire struct into a normalized message.
- **Normalization** (`src/normalise.hpp`): the order-book message types
  (`S R A F E C X D U P`) become one 64-byte, trivially copyable
  `NormalisedMessage`. That's one cache line, and it goes into the ring or onto
  the socket with no serialization. Other types are dropped.
- **IPC** (`src/spsc-ring-buffer.hpp`, `ipc/`): `producer_main` ingests and
  dispatches, then publishes to `consumer_main` over a transport chosen at
  compile time. The ring uses acquire/release atomics and no syscalls. The
  `SOCK_SEQPACKET` socket baseline costs a syscall and a kernel copy per
  message. The ring's shared-memory segment (mode `0600`) has a handshake: the
  producer waits for a consumer to claim it, consumers ignore segments left by
  a dead producer, and either side notices if its peer dies. A consumer that
  never sees the end marker exits non-zero, since its results are partial.
- **Latency measurement** (`src/latency_histogram.hpp`): a zero-allocation
  histogram with power-of-two buckets, fed `rdtsc` cycles for in-process
  parse/dispatch latency and `CLOCK_MONOTONIC` nanoseconds for cross-process
  latency.

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

Fetch the NASDAQ sample feed into `data/` first:

```bash
bash download_data.sh
```

Both benchmark scripts build the project, run the ring and socket pairs back
to back, and write `bench/results/ipc_summary.csv` plus per-bucket histograms
`ipc_histogram_{ring,unix}.csv`. Each run overwrites the previous results.

### Isolated benchmark (standard, needs root)

Producer and consumer run on two CPUs that the kernel keeps free of other
work. Pick two CPUs on different physical cores, i.e. not SMT siblings (check
`/sys/devices/system/cpu/cpu<N>/topology/thread_siblings_list`). The examples
use 12 and 14.

**One-time boot setup.** Add the isolation parameters to
`GRUB_CMDLINE_LINUX_DEFAULT` in `/etc/default/grub`:

```
GRUB_CMDLINE_LINUX_DEFAULT="quiet splash isolcpus=domain,managed_irq,12,14 nohz_full=12,14 rcu_nocbs=12,14"
```

These keep other tasks, managed IRQs, the scheduler tick and RCU callbacks
off those CPUs. The kernel needs `CONFIG_NO_HZ_FULL` and `CONFIG_RCU_NOCB_CPU`
(stock Ubuntu kernels have both). Regenerate the config, reboot, and check:

```bash
sudo update-grub      # Fedora/RHEL: sudo grub2-mkconfig -o /boot/grub2/grub.cfg
sudo reboot
cat /sys/devices/system/cpu/isolated    # should print 12,14
cat /sys/devices/system/cpu/nohz_full   # should print 12,14
```

The CPUs stay reserved until you remove the parameters and repeat the
`update-grub` and reboot.

**Each run:**

```bash
sudo bash bench/run_ipc_isolated.sh [PRODUCER_CPU CONSUMER_CPU]
```

By default the producer gets the higher isolated CPU and the consumer the
lower. The script refuses to run if the boot parameters aren't active, then
applies runtime isolation (SMT siblings offline, remaining IRQs moved away,
idle states off, maximum frequency) and restores everything afterwards, even
on failure. For repeated runs you can apply and undo that by hand with
`sudo bash bench/isolate_cores.sh [PRODUCER CONSUMER]` and
`sudo bash bench/isolate_cleanup.sh`.

### Baseline benchmark (no root)

```bash
bash bench/run_ipc_comparison.sh
```

Pins producer and consumer to two physical cores with `taskset` and changes
nothing else, so results include scheduler, interrupt and frequency-scaling
noise.

### Plots

Create the notebook's virtualenv once (`bench/.venv`, gitignored):

```bash
python3 -m venv bench/.venv
bench/.venv/bin/pip install nbconvert ipykernel matplotlib pandas
```

Then regenerate `bench/plots/` from the current results. The executed notebook
goes to a temp dir so the committed copy stays free of outputs:

```bash
bench/.venv/bin/jupyter nbconvert --to notebook --execute --output-dir "$(mktemp -d)" bench/benchmark_plots.ipynb
```

## Results

Isolated benchmark on a 200MB ITCH slice: 6,948,075 messages, all delivered by
both transports. Latencies are the lower edge of their power-of-two bucket, so
128ns means 128–255ns.

![End-to-end latency percentiles and sustained throughput, ring buffer vs. AF_UNIX socket](bench/plots/ipc_summary.png)

| | Ring buffer | AF_UNIX socket | Ring buffer advantage |
|---|---:|---:|---:|
| p50 | 128ns | 2,048ns | 16× |
| p95 | 128ns | 4,096ns | 32× |
| p99 | 128ns | 4,096ns | 32× |
| p99.9 | 256ns | 4,096ns | 16× |
| Throughput | 8.39M msg/s | 752K msg/s | 11× |

`nohz_full` adds about 130ns to every syscall on the isolated CPUs, which the
socket transport pays on each `send()` and `recv()`.

![Full end-to-end latency distribution, per power-of-two bucket, ring buffer vs. AF_UNIX socket](bench/plots/ipc_latency_distribution.png)

99.8% of ring-buffer messages land in the 128ns bucket. The socket has nothing
below 1,024ns and 99.9% of its messages between 1,024 and 8,192ns.
