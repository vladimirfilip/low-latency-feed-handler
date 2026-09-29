#pragma once

#include <atomic>
#include <new>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <algorithm>
#include <stdexcept>
#include <type_traits>

constexpr size_t CACHE_LINE = 64;
constexpr size_t CAPACITY = 1024;
constexpr uint64_t mask = CAPACITY - 1;

template<typename T>
struct SPSCRingBuffer {
    static_assert(std::is_trivially_copyable_v<T>,
        "SPSCRingBuffer is used across shared memory; T must be trivially copyable");

    alignas(CACHE_LINE)
    std::atomic<uint64_t> head{0};

    char pad1[CACHE_LINE - sizeof(std::atomic<uint64_t>)];

    alignas(CACHE_LINE)
    std::atomic<uint64_t> tail{0};

    char pad2[CACHE_LINE - sizeof(std::atomic<uint64_t>)];

    alignas(CACHE_LINE)
    T buffer[CAPACITY];


    bool push(T value) {
        auto t = tail.load(std::memory_order_relaxed);
        auto next = (t + 1) & mask;

        if (next == head.load(std::memory_order_acquire))
            return false; // full

        buffer[t] = std::move(value);

        tail.store(next, std::memory_order_release);

        return true;
    }

    bool pop(T& value) {
        auto h = head.load(std::memory_order_relaxed);
        
        if (h == tail.load(std::memory_order_acquire))
            return false; // empty

        value = std::move(buffer[h]);

        head.store((h + 1) & mask, std::memory_order_release);

        return true;
    }
};

template<typename T>
std::pair<int, SPSCRingBuffer<T>*> create_shared(const char SHM_NAME[]) {
    int fd = shm_open(SHM_NAME, O_CREAT | O_RDWR, 0666);
    if (fd == -1)
        throw std::runtime_error("shm_open failed");

    if (ftruncate(fd, sizeof(SPSCRingBuffer<T>)) == -1)
        throw std::runtime_error("ftruncate failed");

    void* mem = mmap(nullptr, sizeof(SPSCRingBuffer<T>), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mem == MAP_FAILED)
        throw std::runtime_error("mmap failed");

    auto* queue = new (mem) SPSCRingBuffer<T>;
    return {fd, queue};
}

template<typename T>
std::pair<int, SPSCRingBuffer<T>*> attach_shared(const char SHM_NAME[]) {
    int fd = shm_open(
        SHM_NAME,
        O_RDWR,
        0666);
    if (fd == -1)
        throw std::runtime_error("shm_open failed");

    void* mem = mmap(
        nullptr,
        sizeof(SPSCRingBuffer<T>),
        PROT_READ | PROT_WRITE,
        MAP_SHARED,
        fd,
        0);
    if (mem == MAP_FAILED)
        throw std::runtime_error("mmap failed");

    return {fd, static_cast<SPSCRingBuffer<T>*>(mem)};
}

template<typename T>
void detach_shared(int fd, SPSCRingBuffer<T>* queue) {
    munmap(queue, sizeof(SPSCRingBuffer<T>));
    close(fd);
}

inline void delete_shared(const char SHM_NAME[]) {
    shm_unlink(SHM_NAME);
}
