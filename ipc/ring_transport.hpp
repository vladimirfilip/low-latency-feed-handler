#pragma once

// Shared-memory SPSC ring transport. The segment carries a small control
// block next to the ring so the two processes agree on *which* segment
// they're using:
//
//   producer: unlink any leftover segment -> create + construct a fresh one
//             -> publish its pid -> release-store `magic` (segment is valid)
//             -> wait for a consumer to claim it -> start publishing.
//   consumer: poll until a segment exists, is full-size, carries `magic`,
//             and its producer is alive -> claim it by CAS-ing its own pid
//             into `consumer_pid`.
//
// Waiting for the claim means nothing is timestamped before anyone can read
// it, and a short stream can't be published and unlinked before the
// consumer attaches. Rejecting segments whose producer is dead skips those
// left behind by a crashed run.

#include <chrono>
#include <thread>

#include <immintrin.h>
#include <signal.h>
#include <sys/types.h>
#include <unistd.h>

#include <normalise.hpp>
#include <spsc-ring-buffer.hpp>

constexpr char RING_SHM_NAME[] = "/feed_handler_norm_ring";
constexpr int BUSY_WAIT_ITERS = 1000;

// "FHRING" + layout version; bump the version whenever RingSegment changes
// so a consumer from a different build refuses the segment instead of
// misreading it.
constexpr uint64_t RING_MAGIC = 0x464852494e470001ull;

struct alignas(CACHE_LINE) RingControl {
    std::atomic<uint64_t> magic{0};     // RING_MAGIC once fully constructed
    std::atomic<pid_t> producer_pid{0};
    std::atomic<pid_t> consumer_pid{0}; // non-zero once a consumer has claimed the ring
};

static_assert(std::atomic<pid_t>::is_always_lock_free,
    "RingControl is shared across processes; only lock-free atomics are valid in shared memory");

struct RingSegment {
    RingControl ctl;
    SPSCRingBuffer<NormalisedMessage> ring;
};

// kill(pid, 0) fails with EPERM for a live process owned by another user.
// An unreaped zombie or a reused pid also counts as alive.
inline bool process_alive(pid_t pid) {
    return pid > 0 && (kill(pid, 0) == 0 || errno == EPERM);
}

struct RingProducerTransport {
    RingSegment* seg = nullptr;

    RingProducerTransport() = default;
    RingProducerTransport(const RingProducerTransport&) = delete;
    RingProducerTransport& operator=(const RingProducerTransport&) = delete;
    ~RingProducerTransport() { close(); }

    // Blocks until a consumer has claimed the ring.
    void open() {
        delete_shared(RING_SHM_NAME); // clear any segment a crashed run left behind
        seg = create_shared<RingSegment>(RING_SHM_NAME);
        seg->ctl.producer_pid.store(getpid(), std::memory_order_relaxed);
        seg->ctl.magic.store(RING_MAGIC, std::memory_order_release);
        while (seg->ctl.consumer_pid.load(std::memory_order_acquire) == 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }

    void send(const NormalisedMessage& msg) {
        for (;;) {
            for (int i = 0; i < BUSY_WAIT_ITERS; i++) {
                if (seg->ring.push(msg))
                    return;
                _mm_pause();
            }
            std::this_thread::yield();
        }
    }

    void close() {
        if (seg == nullptr)
            return;
        detach_shared(seg);
        seg = nullptr;
        delete_shared(RING_SHM_NAME);
    }
};

struct RingConsumerTransport {
    RingSegment* seg = nullptr;

    RingConsumerTransport() = default;
    RingConsumerTransport(const RingConsumerTransport&) = delete;
    RingConsumerTransport& operator=(const RingConsumerTransport&) = delete;
    ~RingConsumerTransport() { close(); }

    // Blocks until it has claimed a live producer's ring.
    void open() {
        while (!try_claim()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    bool recv(NormalisedMessage& msg) {
        for (;;) {
            for (int i = 0; i < BUSY_WAIT_ITERS; i++) {
                if (seg->ring.pop(msg))
                    return true;
                _mm_pause();
            }
            std::this_thread::yield();
        }
    }

    void close() {
        if (seg == nullptr)
            return;
        detach_shared(seg);
        seg = nullptr;
    }

private:
    bool try_claim() {
        RingSegment* s = attach_shared<RingSegment>(RING_SHM_NAME);
        if (s == nullptr)
            return false;

        uint64_t magic = s->ctl.magic.load(std::memory_order_acquire);
        if (magic != RING_MAGIC) {
            detach_shared(s);
            if (magic != 0)
                throw std::runtime_error("ring segment has an incompatible layout (built from a different version?)");
            return false; // producer still constructing it
        }

        pid_t unclaimed = 0;
        if (!process_alive(s->ctl.producer_pid.load(std::memory_order_relaxed)) ||
            !s->ctl.consumer_pid.compare_exchange_strong(unclaimed, getpid(), std::memory_order_acq_rel)) {
            detach_shared(s); // stale segment from a dead producer, or another consumer owns it
            return false;
        }
        seg = s;
        return true;
    }
};
