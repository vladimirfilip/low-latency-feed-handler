#pragma once

#include <chrono>
#include <thread>

#include <immintrin.h>

#include <normalise.hpp>
#include <spsc-ring-buffer.hpp>

constexpr char RING_SHM_NAME[] = "/feed_handler_norm_ring";
constexpr int BUSY_WAIT_ITERS = 1000;

using NormRing = SPSCRingBuffer<NormalisedMessage>;

struct RingProducerTransport {
    NormRing* ring = nullptr;

    void open() {
        delete_shared(RING_SHM_NAME);
        ring = create_shared<NormRing>(RING_SHM_NAME);
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
        detach_shared(ring);
        delete_shared(RING_SHM_NAME);
    }
};

struct RingConsumerTransport {
    NormRing* ring = nullptr;

    // Blocks until the producer's segment exists at full size.
    void open() {
        while ((ring = attach_shared<NormRing>(RING_SHM_NAME)) == nullptr) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
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
        detach_shared(ring);
    }
};
