#pragma once

// AF_UNIX baseline IPC bus, the reference point the ring buffer is compared
// against: a SOCK_SEQPACKET socket, so — unlike a raw stream socket — each
// send() is delivered as exactly one recv() with no manual length-framing
// needed, but unlike the ring buffer, every message still costs a syscall
// plus a kernel-mediated copy on each side.

#include <cerrno>
#include <chrono>
#include <cstring>
#include <stdexcept>
#include <thread>

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <normalise.hpp>

constexpr char UNIX_SOCKET_PATH[] = "/tmp/feed_handler_norm.sock";

inline void fill_sockaddr(sockaddr_un& addr) {
    std::memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, UNIX_SOCKET_PATH, sizeof(addr.sun_path) - 1);
}

struct UnixSocketConsumerTransport {
    int listen_fd = -1;
    int conn_fd = -1;

    UnixSocketConsumerTransport() = default;
    UnixSocketConsumerTransport(const UnixSocketConsumerTransport&) = delete;
    UnixSocketConsumerTransport& operator=(const UnixSocketConsumerTransport&) = delete;
    ~UnixSocketConsumerTransport() { close(); }

    // Server side: binds and blocks in accept() until the producer connects.
    void open() {
        unlink(UNIX_SOCKET_PATH);
        listen_fd = socket(AF_UNIX, SOCK_SEQPACKET, 0);
        if (listen_fd == -1)
            throw std::runtime_error("socket failed");

        sockaddr_un addr;
        fill_sockaddr(addr);
        if (bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == -1)
            throw std::runtime_error("bind failed");
        if (listen(listen_fd, 1) == -1)
            throw std::runtime_error("listen failed");

        conn_fd = accept(listen_fd, nullptr, nullptr);
        if (conn_fd == -1)
            throw std::runtime_error("accept failed");
    }

    // Returns false at end of stream (peer closed) or on a socket error.
    bool recv(NormalisedMessage& msg) {
        for (;;) {
            ssize_t n = ::recv(conn_fd, &msg, sizeof(msg), MSG_WAITALL);
            if (n < 0 && errno == EINTR)
                continue; // interrupted by a signal, not end of stream
            return n == static_cast<ssize_t>(sizeof(msg));
        }
    }

    void close() {
        if (listen_fd == -1)
            return;
        if (conn_fd != -1)
            ::close(conn_fd);
        ::close(listen_fd);
        conn_fd = listen_fd = -1;
        unlink(UNIX_SOCKET_PATH);
    }
};

struct UnixSocketProducerTransport {
    int fd = -1;

    UnixSocketProducerTransport() = default;
    UnixSocketProducerTransport(const UnixSocketProducerTransport&) = delete;
    UnixSocketProducerTransport& operator=(const UnixSocketProducerTransport&) = delete;
    ~UnixSocketProducerTransport() { close(); }

    // Client side: retries connect() until the consumer's listen socket
    // exists, so launch order between the two processes doesn't matter.
    void open() {
        fd = socket(AF_UNIX, SOCK_SEQPACKET, 0);
        if (fd == -1)
            throw std::runtime_error("socket failed");

        sockaddr_un addr;
        fill_sockaddr(addr);
        for (;;) {
            if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0)
                return;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    void send(const NormalisedMessage& msg) {
        ssize_t n;
        do {
            n = ::send(fd, &msg, sizeof(msg), 0);
        } while (n < 0 && errno == EINTR);
        if (n != static_cast<ssize_t>(sizeof(msg)))
            throw std::runtime_error("send failed / short write");
    }

    void close() {
        if (fd == -1)
            return;
        ::close(fd);
        fd = -1;
    }
};
