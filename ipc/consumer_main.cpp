// Standalone consumer process: reads normalized messages off whichever IPC
// transport this binary was built with and records end-to-end latency
// (producer ingest -> this process observing the message) plus throughput.
// Same Histogram as src/main.cpp's parse-latency benchmark, but fed
// monotonic-clock nanoseconds directly instead of rdtsc cycles, since these
// timestamps are compared across process boundaries.
#include <chrono>
#include <cstdio>
#include <exception>

#include <latency_histogram.hpp>
#include <normalise.hpp>

#ifdef USE_UNIX_SOCKET
#include <unix_socket_transport.hpp>
using ConsumerTransport = UnixSocketConsumerTransport;
#else
#include <ring_transport.hpp>
using ConsumerTransport = RingConsumerTransport;
#endif

static int run(int argc, char** argv) {
    ConsumerTransport transport;
    transport.open();

    Histogram hist;
    NormalisedMessage msg;
    uint64_t received = 0;
    bool saw_end = false;

    auto wall_start = std::chrono::steady_clock::now();
    while (transport.recv(msg)) {
        if (msg.msg_type == STREAM_END_MSG_TYPE) {
            saw_end = true;
            break;
        }
        uint64_t t_observe = monotonic_ns();
        uint64_t latency_ns = t_observe > msg.ingest_ns ? t_observe - msg.ingest_ns : 0;
        hist.record(latency_ns);
        ++received;
    }
    auto wall_end = std::chrono::steady_clock::now();
    transport.close();

    double elapsed_s = std::chrono::duration<double>(wall_end - wall_start).count();
    double throughput = elapsed_s > 0 ? static_cast<double>(received) / elapsed_s : 0.0;

    hist.print();
    std::printf("throughput: %.0f msgs/sec\n", throughput);
    if (argc > 1) {
        hist.write_csv(argv[1]);
    }
    if (!saw_end) {
        std::fprintf(stderr, "consumer: producer went away before end of stream; results are partial\n");
        return 1;
    }
    return 0;
}

int main(int argc, char** argv) {
    try {
        return run(argc, argv);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "consumer: %s\n", e.what());
        return 1;
    }
}
