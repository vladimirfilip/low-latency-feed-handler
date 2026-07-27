#pragma once

// Socket-mode ingestion: receives ITCH records carried over MoldUDP64,
// NASDAQ's real framing protocol for batching multiple ITCH messages into
// one UDP datagram. Each datagram is:
//
//   [10-byte session][8-byte big-endian sequence number][2-byte big-endian
//   message count], followed by `message count` sub-messages, each a
//   [2-byte big-endian length][payload] pair — the same length-prefix shape
//   as the file-mode `.itch` records, just batched under a session header.
//
// A message count of 0 is a heartbeat (no messages, keep listening); 0xFFFF
// is MoldUDP64's real "end of session" sentinel, used here by the replay
// tool to signal it has finished the file — next_record() returns nullptr
// on it, same EOF contract as mmap_buffer.
//
// What's deliberately NOT implemented: MoldUDP64's gap-fill/retransmission
// request mechanism. That exists to recover UDP packets lost between
// separate machines; this project's replay tool and feed handler talk over
// loopback, where drops in practice mean the receiver's socket buffer
// overflowing under load, not the network losing packets. SO_RCVBUF is
// sized generously below to make that practically unlikely for benchmarking
// purposes, but it isn't corrected for if it happens — a dropped batch is
// just gone, unlike file mode's data (mmap'd, so no in-flight loss). Worth
// knowing if socket-mode counts ever come up short of file-mode's.
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <stdexcept>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <ingest_source.hpp>
#include <mold_udp64.hpp>

constexpr size_t UDP_MAX_DATAGRAM = 65536;
constexpr int UDP_RCVBUF_BYTES = 4 * 1024 * 1024;

struct UdpMoldIngestSource : IngestSource {
    int fd = -1;
    uint8_t buf[UDP_MAX_DATAGRAM];
    size_t read_offset = 0;      // next unread byte within buf, past the Mold header
    uint16_t remaining = 0;      // sub-messages left unread in the current datagram
    bool ended = false;

    explicit UdpMoldIngestSource(uint16_t port) {
        fd = socket(AF_INET, SOCK_DGRAM, 0);
        if (fd == -1)
            throw std::runtime_error("socket failed");

        int rcvbuf = UDP_RCVBUF_BYTES;
        setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        addr.sin_port = htons(port);
        if (bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == -1)
            throw std::runtime_error("bind failed");
    }

    // Pointers returned by next_record() point into `buf`, which the next
    // datagram overwrites — same "use before the next call" contract the
    // dispatch loop already follows for every ingestion source, but unlike
    // mmap_buffer's pointers (valid for the process's whole lifetime) these
    // specifically are not.
    const uint8_t* next_record() override {
        if (ended) {
            return nullptr;
        }
        while (remaining == 0) {
            if (!recv_next_datagram()) {
                return nullptr;
            }
        }
        uint16_t len;
        std::memcpy(&len, buf + read_offset, 2);
        len = ntohs(len);
        read_offset += 2;
        const uint8_t* record = buf + read_offset;
        read_offset += len;
        --remaining;
        return record;
    }

    void close() override {
        ::close(fd);
    }

private:
    // Returns false on end-of-session; otherwise blocks until it has a
    // datagram with at least one sub-message queued up (skipping heartbeats).
    bool recv_next_datagram() {
        for (;;) {
            ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
            if (n < 0) {
                if (errno == EINTR) continue;
                throw std::runtime_error("recv failed");
            }
            if (static_cast<size_t>(n) < MOLD_HEADER_BYTES) {
                continue; // short/malformed datagram, drop it and wait for the next one
            }

            uint16_t msg_count;
            std::memcpy(&msg_count, buf + MOLD_SESSION_BYTES + 8, 2);
            msg_count = ntohs(msg_count);

            if (msg_count == MOLD_END_OF_SESSION) {
                ended = true;
                return false;
            }
            if (msg_count == 0) {
                continue; // heartbeat: no messages in this datagram
            }

            remaining = msg_count;
            read_offset = MOLD_HEADER_BYTES;
            return true;
        }
    }
};
