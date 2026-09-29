#pragma once

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <stdint.h>
#include <cassert>

#include <ingest_source.hpp>

#define LENGTH_PREFIX_BYTES 2

struct mmap_buffer : IngestSource {
    int fd;
    off_t size;
    const uint8_t* data;
    off_t offset = 0;
    bool opened = false;
    uint16_t last_len = 0; // length of the record last returned by next_record()

    mmap_buffer(const std::string& path) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd == -1)
            throw std::runtime_error("open " + path + ": " + std::strerror(errno));
        struct stat sb;
        if (fstat(fd, &sb) == -1) {
            int err = errno;
            ::close(fd);
            throw std::runtime_error("fstat " + path + ": " + std::strerror(err));
        }
        size = sb.st_size;
        data = nullptr;
        if (size > 0) { // mmap rejects a zero length; an empty file just has no records
            void* mapped = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
            if (mapped == MAP_FAILED) {
                int err = errno;
                ::close(fd);
                throw std::runtime_error("mmap " + path + ": " + std::strerror(err));
            }
            data = static_cast<const uint8_t*>(mapped);
        }
        offset = 0;
        opened = true;
    }

    mmap_buffer(const mmap_buffer&) = delete;
    mmap_buffer& operator=(const mmap_buffer&) = delete;
    ~mmap_buffer() override { close(); }

    const uint8_t* next_record() override {
        if (offset > size - LENGTH_PREFIX_BYTES) {
            return nullptr;
        }
        const uint16_t* big_endian_len = reinterpret_cast<const uint16_t*>(data + offset);
        const uint16_t len = __builtin_bswap16(*big_endian_len);
        offset += LENGTH_PREFIX_BYTES;
        if (offset > size - len) {
            return nullptr;
        }
        const uint8_t* res = data + offset;
        offset += len;
        last_len = len;
        assert(offset <= size);
        return res;
    }

    // Unmaps the file: pointers from next_record() are invalid afterwards.
    void close() override {
        if (!opened)
            return;
        if (data != nullptr)
            munmap(const_cast<uint8_t*>(data), size);
        ::close(fd);
        opened = false;
    }
};