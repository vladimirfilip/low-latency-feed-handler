#pragma once

#include <atomic>
#include <cerrno>
#include <cstring>
#include <new>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>
#include <stdexcept>
#include <string>
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

// Shared-memory helpers, templated on the whole mapped object. The fd is
// closed as soon as the mapping exists; the mapping keeps the segment alive.

inline std::runtime_error shm_error(const char* what) {
    return std::runtime_error(std::string(what) + ": " + std::strerror(errno));
}

// Creates a fresh segment and constructs S in it. O_EXCL: a leftover segment
// of the same name is an error, never silently reused; callers
// delete_shared() first.
template<typename S>
S* create_shared(const char* name) {
    int fd = shm_open(name, O_CREAT | O_EXCL | O_RDWR, 0666);
    if (fd == -1)
        throw shm_error("shm_open(create) failed");

    if (ftruncate(fd, sizeof(S)) == -1) {
        auto err = shm_error("ftruncate failed");
        close(fd);
        shm_unlink(name);
        throw err;
    }

    void* mem = mmap(nullptr, sizeof(S), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mem == MAP_FAILED) {
        auto err = shm_error("mmap failed");
        close(fd);
        shm_unlink(name);
        throw err;
    }
    close(fd);

    return new (mem) S;
}

// Maps an existing segment, or returns nullptr if it isn't there yet. A
// segment smaller than S also counts as "not yet": the creator is between
// shm_open() and ftruncate(), and touching that mapping would SIGBUS.
template<typename S>
S* attach_shared(const char* name) {
    int fd = shm_open(name, O_RDWR, 0);
    if (fd == -1) {
        if (errno == ENOENT)
            return nullptr;
        throw shm_error("shm_open(attach) failed");
    }

    struct stat st;
    if (fstat(fd, &st) == -1) {
        auto err = shm_error("fstat failed");
        close(fd);
        throw err;
    }
    if (st.st_size < static_cast<off_t>(sizeof(S))) {
        close(fd);
        return nullptr;
    }

    void* mem = mmap(nullptr, sizeof(S), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mem == MAP_FAILED) {
        auto err = shm_error("mmap failed");
        close(fd);
        throw err;
    }
    close(fd);

    return static_cast<S*>(mem);
}

template<typename S>
void detach_shared(S* seg) {
    munmap(seg, sizeof(S));
}

inline void delete_shared(const char* name) {
    shm_unlink(name);
}
