#include <memory>
#include <string>

#include <cli_args.hpp>
#include <dispatch.hpp>
#include <ingest_mmap.hpp>
#include <ingest_socket.hpp>
#include <latency_histogram.hpp>

const std::string DATA_PATH = "data/03272019.NASDAQ_ITCH50.200MB";

// Tells the compiler `value` is used, so the work producing it can't be
// optimised out of the timed region.
template <typename T>
inline void do_not_optimize(const T& value) {
    asm volatile("" : : "r"(&value) : "memory");
}

int main(int argc, char** argv) {
    ParsedArgs args = parse_args(argc, argv);

    std::unique_ptr<IngestSource> source;
    if (args.options.count("udp")) {
        // Socket mode: expects a replay tool (replay/replay.cpp) sending
        // MoldUDP64-framed records to this port.
        source = std::make_unique<UdpMoldIngestSource>(
            static_cast<uint16_t>(std::stoi(args.options.at("udp"))));
    } else {
        source = std::make_unique<mmap_buffer>(args.get("data", DATA_PATH));
    }

    RdtscTimer timer;
    timer.calibrate();

    Histogram hist;
    const uint8_t* record;
    while ((record = source->next_record()) != nullptr) {
        uint64_t t0 = timer.now();
        NormalisedMessage norm = dispatch(record);
        do_not_optimize(norm);
        uint64_t t1 = timer.now();
        uint64_t delta = t1 - t0;
        hist.record(delta > timer.overhead_cycles ? delta - timer.overhead_cycles : 0);
    }
    source->close();

    hist.print(timer);
    if (!args.positional.empty()) {
        hist.write_csv(timer, args.positional[0]);
    }
    return 0;
}