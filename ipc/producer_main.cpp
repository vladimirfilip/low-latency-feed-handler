// Feed-handler variant that publishes normalized messages across an IPC bus
// to a separate consumer process, instead of measuring parse latency
// in-process like src/main.cpp. Same ingestion + dispatch as main.cpp; the
// new piece is timestamping each record at ingest and handing the
// normalized result off to whichever transport this binary was built with.
#include <cstdio>
#include <memory>
#include <string>

#include <cli_args.hpp>
#include <dispatch.hpp>
#include <ingest_mmap.hpp>
#include <ingest_socket.hpp>
#include <latency_histogram.hpp>
#include <normalise.hpp>

#ifdef USE_UNIX_SOCKET
#include <unix_socket_transport.hpp>
using ProducerTransport = UnixSocketProducerTransport;
#else
#include <ring_transport.hpp>
using ProducerTransport = RingProducerTransport;
#endif

const std::string DATA_PATH = "data/03272019.NASDAQ_ITCH50.200MB";

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

    ProducerTransport transport;
    transport.open();

    const uint8_t* record;
    uint64_t published = 0;
    while ((record = source->next_record()) != nullptr) {
        // Timestamp at ingest, before dispatch/normalise — this is "message
        // received" for the end-to-end latency measured on the consumer
        // side, so it captures dispatch + handler + IPC transit, not just
        // the IPC hop in isolation.
        uint64_t t_ingest = monotonic_ns();
        NormalisedMessage norm = dispatch(record);
        if (norm.msg_type == UNPOPULATED) {
            continue; // unsupported record type — not part of the normalized stream
        }
        norm.ingest_ns = t_ingest;
        transport.send(norm);
        ++published;
    }
    source->close();

    NormalisedMessage end{};
    end.msg_type = STREAM_END_MSG_TYPE;
    transport.send(end);

    std::fprintf(stderr, "producer: published %llu normalised messages\n",
                 static_cast<unsigned long long>(published));

    transport.close();
    return 0;
}
