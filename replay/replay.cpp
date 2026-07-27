// Replay tool: reads the .itch file the same way file-mode ingestion
// does, batches records under MoldUDP64 framing, and sends them over
// UDP to a feed handler running in socket mode (--udp=PORT). Two modes:
//
//   max-rate (default) - batch up to the caps below and send immediately.
//   --paced            - batch only records that share an identical wire
//                         timestamp (many ITCH messages do — same-instant
//                         bursts are common), and sleep between batches so
//                         the gap between sends matches the gap between the
//                         original timestamps, for realistic burst shapes.
//
// Batches are capped at MAX_PACKET_BYTES so a batch can't force IP
// fragmentation over a typical 1500-byte-MTU Ethernet path (1500 minus
// IP/UDP headers, rounded down for margin), and at MAX_BATCH_MESSAGES as a
// belt-and-suspenders limit in case a pathological number of records share
// one timestamp.
#include <cstdio>
#include <cstring>
#include <chrono>
#include <string>
#include <thread>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cli_args.hpp>
#include <handlers.hpp> // read_timestamp48 — same 6-byte window at offset 5 in every ITCH message type
#include <ingest_mmap.hpp>
#include <mold_udp64.hpp>

constexpr size_t MAX_PACKET_BYTES = 1400;
constexpr size_t MAX_BATCH_MESSAGES = 200;
constexpr char SESSION_ID[MOLD_SESSION_BYTES] = {'R', 'E', 'P', 'L', 'A', 'Y', '0', '0', '0', '1'};

const std::string DEFAULT_DATA_PATH = "data/03272019.NASDAQ_ITCH50.200MB";

int main(int argc, char** argv) {
    ParsedArgs args = parse_args(argc, argv);
    const std::string host = args.get("host", "127.0.0.1");
    const uint16_t port = static_cast<uint16_t>(std::stoi(args.get("port", "30001")));
    const std::string data_path = args.get("data", DEFAULT_DATA_PATH);
    const bool paced = args.has_flag("paced");

    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd == -1) {
        std::perror("socket");
        return 1;
    }

    sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(port);
    if (inet_pton(AF_INET, host.c_str(), &dest.sin_addr) != 1) {
        std::fprintf(stderr, "invalid --host=%s\n", host.c_str());
        return 1;
    }

    mmap_buffer buffer(data_path);

    uint8_t packet[MOLD_HEADER_BYTES + MAX_PACKET_BYTES + 4096]; // header + batch, generous margin over the byte cap
    size_t packet_len = MOLD_HEADER_BYTES;
    size_t batch_count = 0;
    uint64_t seq = 1;
    uint64_t published = 0;

    bool have_batch_ts = false;
    uint64_t batch_ts_ns = 0;
    bool have_first_ts = false;
    uint64_t first_ts_ns = 0;
    auto replay_start = std::chrono::steady_clock::now();

    auto flush = [&]() {
        if (batch_count == 0) return;
        mold_write_header(packet, SESSION_ID, seq, static_cast<uint16_t>(batch_count));
        ssize_t n = sendto(fd, packet, packet_len, 0,
                            reinterpret_cast<sockaddr*>(&dest), sizeof(dest));
        if (n < 0) {
            std::perror("sendto");
        }
        seq += batch_count;
        published += batch_count;
        batch_count = 0;
        packet_len = MOLD_HEADER_BYTES;
        have_batch_ts = false;
    };

    auto pace_until = [&](uint64_t msg_ts_ns) {
        uint64_t delta_ns = msg_ts_ns - first_ts_ns;
        auto target = replay_start + std::chrono::nanoseconds(delta_ns);
        std::this_thread::sleep_until(target);
    };

    const uint8_t* record;
    while ((record = buffer.next_record()) != nullptr) {
        uint16_t len = buffer.last_len;
        uint64_t msg_ts_ns = read_timestamp48(record + 5);

        if (!have_first_ts) {
            first_ts_ns = msg_ts_ns;
            have_first_ts = true;
        }

        bool timestamp_changed = have_batch_ts && msg_ts_ns != batch_ts_ns;
        bool batch_full = batch_count >= MAX_BATCH_MESSAGES ||
                           packet_len + 2 + len > MOLD_HEADER_BYTES + MAX_PACKET_BYTES;
        if (have_batch_ts && (timestamp_changed || batch_full)) {
            if (paced && timestamp_changed) {
                pace_until(batch_ts_ns);
            }
            flush();
        }

        if (!have_batch_ts) {
            batch_ts_ns = msg_ts_ns;
            have_batch_ts = true;
        }

        uint16_t be_len = htons(len);
        std::memcpy(packet + packet_len, &be_len, 2);
        packet_len += 2;
        std::memcpy(packet + packet_len, record, len);
        packet_len += len;
        ++batch_count;
    }
    buffer.close();

    if (paced && have_batch_ts) {
        pace_until(batch_ts_ns);
    }
    flush();

    // MoldUDP64's real end-of-session sentinel: header only, message count
    // 0xFFFF, so the consumer's next_record() returns nullptr the same way
    // mmap_buffer does at EOF.
    uint8_t end_packet[MOLD_HEADER_BYTES];
    mold_write_header(end_packet, SESSION_ID, seq, MOLD_END_OF_SESSION);
    sendto(fd, end_packet, sizeof(end_packet), 0,
           reinterpret_cast<sockaddr*>(&dest), sizeof(dest));

    std::fprintf(stderr, "replay: sent %llu records to %s:%u (%s)\n",
                 static_cast<unsigned long long>(published), host.c_str(), port,
                 paced ? "paced" : "max-rate");

    ::close(fd);
    return 0;
}
