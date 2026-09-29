// Unit tests for the SPSC ring buffer itself (in-process) and the latency
// measurement pieces: the histogram and the timers.

#include <cstddef>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>

#include <latency_histogram.hpp>
#include <normalise.hpp>
#include <spsc-ring-buffer.hpp>

#include "harness.hpp"

namespace {

template <typename T>
struct AlignedRing {
    SPSCRingBuffer<T>* r;
    AlignedRing() {
        void* mem = std::aligned_alloc(alignof(SPSCRingBuffer<T>), sizeof(SPSCRingBuffer<T>));
        r = new (mem) SPSCRingBuffer<T>;
    }
    ~AlignedRing() {
        r->~SPSCRingBuffer<T>();
        std::free(r);
    }
    SPSCRingBuffer<T>* operator->() { return r; }
};

// Writes the histogram's CSV to a temp file and returns its contents.
template <typename... TimerArg>
std::string csv_of(const Histogram& h, const TimerArg&... timer) {
    const char* dir = std::getenv("TMPDIR");
    std::string path = std::string(dir ? dir : "/tmp") + "/fh_test_hist_" + std::to_string(getpid()) + ".csv";
    h.write_csv(timer..., path);
    std::ifstream f(path);
    std::stringstream ss;
    ss << f.rdbuf();
    unlink(path.c_str());
    return ss.str();
}

} // namespace

// ----------------------------------------------------------- SPSCRingBuffer

TEST("ring: pop on an empty ring fails and leaves the output untouched") {
    AlignedRing<uint64_t> ring;
    uint64_t v = 42;
    CHECK(!ring->pop(v));
    CHECK_EQ(v, uint64_t{42});
}

TEST("ring: FIFO order over a short run") {
    AlignedRing<uint64_t> ring;
    for (uint64_t i = 1; i <= 10; ++i) CHECK(ring->push(i));
    for (uint64_t i = 1; i <= 10; ++i) {
        uint64_t v = 0;
        CHECK(ring->pop(v));
        CHECK_EQ(v, i);
    }
    uint64_t v;
    CHECK(!ring->pop(v));
}

TEST("ring: usable capacity is CAPACITY - 1 (one slot distinguishes full from empty)") {
    AlignedRing<uint64_t> ring;
    size_t pushed = 0;
    while (ring->push(pushed)) ++pushed;
    CHECK_EQ(pushed, CAPACITY - 1);
    uint64_t v;
    CHECK(ring->pop(v));
    CHECK_EQ(v, uint64_t{0});
    CHECK(ring->push(999)); // one pop frees exactly one slot
    CHECK(!ring->push(1000));
}

TEST("ring: indices wrap around correctly over many laps") {
    AlignedRing<uint64_t> ring;
    uint64_t next_in = 0, next_out = 0;
    // Uneven push/pop batch sizes so head and tail cross the wrap point at
    // every possible offset.
    for (int round = 0; round < 5000; ++round) {
        int n_push = 1 + (round * 7) % 600;
        for (int i = 0; i < n_push && ring->push(next_in); ++i) ++next_in;
        int n_pop = 1 + (round * 13) % 600;
        uint64_t v;
        for (int i = 0; i < n_pop && ring->pop(v); ++i) {
            CHECK_EQ(v, next_out);
            ++next_out;
        }
    }
    uint64_t v;
    while (ring->pop(v)) CHECK_EQ(v, next_out++);
    CHECK_EQ(next_in, next_out);
    CHECK(next_in > 10 * CAPACITY);
}

TEST("ring: head, tail and buffer each start on their own cache line") {
    using R = SPSCRingBuffer<NormalisedMessage>;
    CHECK_EQ(offsetof(R, head), size_t{0});
    CHECK_EQ(offsetof(R, tail), CACHE_LINE);
    CHECK_EQ(offsetof(R, buffer), 2 * CACHE_LINE);
    CHECK_EQ(alignof(R), CACHE_LINE);
    CHECK((CAPACITY & (CAPACITY - 1)) == 0); // mask arithmetic needs a power of two
    CHECK(std::atomic<uint64_t>::is_always_lock_free); // required to be valid in shared memory
}

TEST_T("ring: two threads transfer 5M messages losslessly and in order", 60000) {
    AlignedRing<NormalisedMessage> ring;
    constexpr uint64_t N = 5'000'000;
    std::thread producer([&] {
        for (uint64_t i = 0; i < N; ++i) {
            NormalisedMessage m{};
            m.order_ref = i;
            m.price = static_cast<uint32_t>(i * 3);
            m.timestamp_ns = ~i;
            while (!ring->push(m)) {}
        }
    });
    for (uint64_t expected = 0; expected < N;) {
        NormalisedMessage m;
        if (!ring->pop(m)) continue;
        // Check every field so a torn read shows up. Failing _exit()s, which
        // also stops the producer thread.
        CHECK_MSG(m.order_ref == expected && m.price == static_cast<uint32_t>(expected * 3) &&
                      m.timestamp_ns == ~expected,
                  "out-of-order or torn message at index " + std::to_string(expected));
        ++expected;
    }
    producer.join();
}

// --------------------------------------------------------------- Histogram

TEST("histogram: values land in power-of-two buckets [2^(b-1), 2^b)") {
    Histogram h;
    h.record(0);
    h.record(1);
    h.record(2);
    h.record(3);
    h.record(4);
    h.record(1023);
    h.record(1024);
    CHECK_EQ(h.counts[0], uint64_t{1}); // 0
    CHECK_EQ(h.counts[1], uint64_t{1}); // 1
    CHECK_EQ(h.counts[2], uint64_t{2}); // 2, 3
    CHECK_EQ(h.counts[3], uint64_t{1}); // 4
    CHECK_EQ(h.counts[10], uint64_t{1}); // 1023
    CHECK_EQ(h.counts[11], uint64_t{1}); // 1024
    CHECK_EQ(h.total, uint64_t{7});
    CHECK_EQ(h.max_cycles, uint64_t{1024});
}

TEST("histogram: percentiles report the lower bound of the containing bucket") {
    Histogram h;
    for (int i = 0; i < 90; ++i) h.record(100); // bucket [64,128)
    for (int i = 0; i < 9; ++i) h.record(5000); // bucket [4096,8192)
    h.record(1'000'000);                        // bucket [524288,1048576)
    CHECK_EQ(h.percentile(0.50), uint64_t{64});
    CHECK_EQ(h.percentile(0.89), uint64_t{64});
    CHECK_EQ(h.percentile(0.95), uint64_t{4096});
    CHECK_EQ(h.percentile(0.999), uint64_t{524288});
    CHECK_EQ(h.percentile(1.0), uint64_t{524288});
    CHECK_EQ(h.max_cycles, uint64_t{1'000'000});
}

TEST("histogram: empty histogram reports zeros") {
    Histogram h;
    CHECK_EQ(h.percentile(0.5), uint64_t{0});
    CHECK_EQ(h.percentile(0.999), uint64_t{0});
    CHECK_EQ(h.total, uint64_t{0});
}

TEST("histogram: write_csv emits one row per bucket up to the highest used") {
    Histogram h;
    h.record(0);
    h.record(3);
    h.record(3);
    h.record(9);
    CHECK_EQ(csv_of(h), std::string("bucket_lower_ns,bucket_upper_ns,count\n"
                              "0,1,1\n"
                              "1,2,0\n"
                              "2,4,2\n"
                              "4,8,0\n"
                              "8,16,1\n"));
}

TEST("histogram: rdtsc write_csv converts bucket bounds with cycles_per_ns") {
    Histogram h;
    h.record(3);
    RdtscTimer t;
    t.cycles_per_ns = 2.0;
    CHECK_EQ(csv_of(h, t), std::string("bucket_lower_ns,bucket_upper_ns,count\n"
                              "0.000,0.500,0\n"
                              "0.500,1.000,0\n"
                              "1.000,2.000,1\n"));
}

TEST("histogram: recording a value >= 2^63 stays in bounds") {
    // An off-by-one here writes counts[64], which aliases `total`.
    Histogram h;
    h.record(UINT64_MAX);
    uint64_t sum = 0;
    for (auto c : h.counts) sum += c;
    CHECK_MSG(h.total == 1, "total is " + std::to_string(h.total) +
                                " after one record() — counts[64] was written, past the array");
    CHECK_EQ(sum, uint64_t{1});
}

TEST("histogram: write_csv to an unwritable path reports the failure") {
    Histogram h;
    h.record(1);
    bool threw = false;
    try {
        h.write_csv("/nonexistent-dir/hist.csv");
    } catch (const std::exception&) {
        threw = true;
    }
    CHECK_MSG(threw, "write_csv silently lost the results");
}

// ------------------------------------------------------------------ timers

TEST("monotonic_ns: non-decreasing and tracks wall time") {
    uint64_t prev = monotonic_ns();
    for (int i = 0; i < 100000; ++i) {
        uint64_t now = monotonic_ns();
        CHECK(now >= prev);
        prev = now;
    }
    uint64_t a = monotonic_ns();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    uint64_t b = monotonic_ns();
    CHECK(b - a >= 20'000'000ull);
    CHECK(b - a < 2'000'000'000ull);
}

TEST("RdtscTimer: calibration yields a plausible TSC rate and overhead") {
    RdtscTimer t;
    t.calibrate();
    CHECK_MSG(t.cycles_per_ns > 0.1 && t.cycles_per_ns < 10.0,
              "cycles_per_ns = " + std::to_string(t.cycles_per_ns));
    CHECK_MSG(t.overhead_cycles > 0 && t.overhead_cycles < 10000,
              "overhead_cycles = " + std::to_string(t.overhead_cycles));
    uint64_t a = RdtscTimer::now();
    uint64_t b = RdtscTimer::now();
    CHECK(b > a);
}

TEST_MAIN()
