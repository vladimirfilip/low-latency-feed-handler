#pragma once

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
        struct stat sb;
        fstat(fd, &sb);
        size = sb.st_size;
        void* mapped = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        data = static_cast<const uint8_t*>(mapped);
        offset = 0;
        opened = true;
    }

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

    void close() override {
        ::close(fd);
        opened = false;
    }
};