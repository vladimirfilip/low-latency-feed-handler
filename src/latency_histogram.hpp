#pragma once

#include <stdint.h>
#include <x86intrin.h>
#include <chrono>
#include <thread>
#include <cstdio>
#include <string>
#include <time.h>

// CLOCK_MONOTONIC, not rdtsc: these timestamps get compared *across
// processes* (producer ingest -> consumer observe), and while this
// machine's TSC is invariant/constant_tsc, clock_gettime sidesteps any
// cross-core TSC synchronization question entirely — its ~20ns call
// overhead is negligible next to the IPC latencies being measured.
inline uint64_t monotonic_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ull + static_cast<uint64_t>(ts.tv_nsec);
}

struct RdtscTimer {
    uint64_t overhead_cycles = 0;
    double cycles_per_ns = 0.0;

    static inline uint64_t now() {
        unsigned aux;
        uint64_t t = __rdtscp(&aux);
        _mm_lfence();
        return t;
    }

    // Measures the timer's overhead and
    // derives a cycles/ns coefficient
    void calibrate() {
        constexpr int kOverheadSamples = 100000;
        uint64_t min_delta = UINT64_MAX;
        for (int i = 0; i < kOverheadSamples; ++i) {
            uint64_t t0 = now();
            uint64_t t1 = now();
            uint64_t delta = t1 - t0;
            if (delta < min_delta) min_delta = delta;
        }
        overhead_cycles = min_delta;

        auto wall_start = std::chrono::steady_clock::now();
        uint64_t tsc_start = now();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        uint64_t tsc_end = now();
        auto wall_end = std::chrono::steady_clock::now();

        double elapsed_ns = std::chrono::duration<double, std::nano>(wall_end - wall_start).count();
        cycles_per_ns = static_cast<double>(tsc_end - tsc_start) / elapsed_ns;
    }
};

// Fixed-size, zero-heap-allocation latency histogram, 
// bucketed by power of two
struct Histogram {
    static constexpr int kBuckets = 64;
    uint64_t counts[kBuckets] = {};
    uint64_t total = 0;
    uint64_t max_cycles = 0;

    void record(uint64_t delta_cycles) {
        int bucket = delta_cycles == 0 ? 0 : (64 - __builtin_clzll(delta_cycles));
        counts[bucket]++;
        total++;
        if (delta_cycles > max_cycles) max_cycles = delta_cycles;
    }

    uint64_t percentile(double p) const {
        if (total == 0) return 0;
        uint64_t target = static_cast<uint64_t>(p * static_cast<double>(total));
        if (target >= total) target = total - 1;
        uint64_t cumulative = 0;
        for (int b = 0; b < kBuckets; ++b) {
            cumulative += counts[b];
            if (cumulative > target) {
                return b == 0 ? 0 : (1ull << (b - 1));
            }
        }
        return max_cycles;
    }

    void print(const RdtscTimer& timer) const {
        auto to_ns = [&](uint64_t cycles) {
            return static_cast<double>(cycles) / timer.cycles_per_ns;
        };
        std::printf("calibration: rdtsc overhead ~= %.1f ns (%llu cycles), cycles_per_ns ~= %.3f\n",
                    to_ns(timer.overhead_cycles),
                    static_cast<unsigned long long>(timer.overhead_cycles),
                    timer.cycles_per_ns);
        std::printf("count: %llu\n", static_cast<unsigned long long>(total));
        std::printf("p50: %.1f ns\n", to_ns(percentile(0.50)));
        std::printf("p95: %.1f ns\n", to_ns(percentile(0.95)));
        std::printf("p99: %.1f ns\n", to_ns(percentile(0.99)));
        std::printf("p99.9: %.1f ns\n", to_ns(percentile(0.999)));
        std::printf("max: %.1f ns\n", to_ns(max_cycles));
    }

    // Dumps per-bucket [lower_ns, upper_ns) counts for plotting the raw
    // distribution shape, not just the summary percentiles. Only emits
    // buckets up to the highest one that ever recorded a sample.
    void write_csv(const RdtscTimer& timer, const std::string& path) const {
        auto to_ns = [&](uint64_t cycles) {
            return static_cast<double>(cycles) / timer.cycles_per_ns;
        };
        int highest = 0;
        for (int b = 0; b < kBuckets; ++b) {
            if (counts[b] > 0) highest = b;
        }
        std::FILE* f = std::fopen(path.c_str(), "w");
        std::fprintf(f, "bucket_lower_ns,bucket_upper_ns,count\n");
        for (int b = 0; b <= highest; ++b) {
            uint64_t lower_cycles = b == 0 ? 0 : (1ull << (b - 1));
            uint64_t upper_cycles = (1ull << b);
            std::fprintf(f, "%.3f,%.3f,%llu\n",
                         to_ns(lower_cycles), to_ns(upper_cycles),
                         static_cast<unsigned long long>(counts[b]));
        }
        std::fclose(f);
    }

    // No-timer variants for histograms already recorded in nanoseconds
    // (e.g. cross-process IPC latency via monotonic_ns()) — bucketing is
    // unit-agnostic, so these just skip the cycles->ns conversion above.
    void print() const {
        std::printf("count: %llu\n", static_cast<unsigned long long>(total));
        std::printf("p50: %.1f ns\n", static_cast<double>(percentile(0.50)));
        std::printf("p95: %.1f ns\n", static_cast<double>(percentile(0.95)));
        std::printf("p99: %.1f ns\n", static_cast<double>(percentile(0.99)));
        std::printf("p99.9: %.1f ns\n", static_cast<double>(percentile(0.999)));
        std::printf("max: %.1f ns\n", static_cast<double>(max_cycles));
    }

    void write_csv(const std::string& path) const {
        int highest = 0;
        for (int b = 0; b < kBuckets; ++b) {
            if (counts[b] > 0) highest = b;
        }
        std::FILE* f = std::fopen(path.c_str(), "w");
        std::fprintf(f, "bucket_lower_ns,bucket_upper_ns,count\n");
        for (int b = 0; b <= highest; ++b) {
            uint64_t lower_ns = b == 0 ? 0 : (1ull << (b - 1));
            uint64_t upper_ns = (1ull << b);
            std::fprintf(f, "%llu,%llu,%llu\n",
                         static_cast<unsigned long long>(lower_ns),
                         static_cast<unsigned long long>(upper_ns),
                         static_cast<unsigned long long>(counts[b]));
        }
        std::fclose(f);
    }
};
