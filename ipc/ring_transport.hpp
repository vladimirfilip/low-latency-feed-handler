#pragma once

#include <chrono>
#include <thread>

#include <normalise.hpp>
#include <spsc-ring-buffer.hpp>

constexpr char RING_SHM_NAME[] = "/feed_handler_norm_ring";
constexpr int BUSY_WAIT_ITERS = 1000;

struct RingProducerTransport {
    int fd = -1;
    SPSCRingBuffer<NormalisedMessage>* ring = nullptr;

    void open() {
        delete_shared(RING_SHM_NAME);
        auto [f, r] = create_shared<NormalisedMessage>(RING_SHM_NAME);
        fd = f;
        ring = r;
    }

    void send(const NormalisedMessage& msg) {
        for (;;) {
            for (int i = 0; i < BUSY_WAIT_ITERS; i++) {
                if (ring->push(msg))
                    return;
                _mm_pause();
            }
            std::this_thread::yield();
        }
    }

    void close() {
        detach_shared(fd, ring);
        delete_shared(RING_SHM_NAME);
    }
};

struct RingConsumerTransport {
    int fd = -1;
    SPSCRingBuffer<NormalisedMessage>* ring = nullptr;

    void open() {
        for (;;) {
            try {
                auto [f, r] = attach_shared<NormalisedMessage>(RING_SHM_NAME);
                fd = f;
                ring = r;
                return;
            } catch (const std::runtime_error&) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
    }

    bool recv(NormalisedMessage& msg) {
        for (;;) {
            for (int i = 0; i < BUSY_WAIT_ITERS; i++) {
                if (ring->pop(msg))
                    return true;
                _mm_pause();
            }
            std::this_thread::yield();
        }
    }

    void close() {
        detach_shared(fd, ring);
    }
};
