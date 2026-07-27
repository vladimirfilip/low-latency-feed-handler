#pragma once

#include <arpa/inet.h>
#include <cstdint>
#include <cstring>

constexpr size_t MOLD_SESSION_BYTES = 10;
constexpr size_t MOLD_HEADER_SEQ_BYTES = sizeof(uint64_t);
constexpr size_t MOLD_HEADER_COUNT_BYTES = sizeof(uint16_t);
constexpr size_t MOLD_HEADER_BYTES = MOLD_SESSION_BYTES + MOLD_HEADER_SEQ_BYTES + MOLD_HEADER_COUNT_BYTES;
constexpr uint16_t MOLD_END_OF_SESSION = 0xFFFF;

inline void mold_write_header(uint8_t* out, const char session[MOLD_SESSION_BYTES],
                               uint64_t seq_num, uint16_t msg_count) {
    std::memcpy(out, session, MOLD_SESSION_BYTES);
    uint64_t be_seq = __builtin_bswap64(seq_num);
    std::memcpy(out + MOLD_SESSION_BYTES, &be_seq, MOLD_HEADER_SEQ_BYTES);
    uint16_t be_count = htons(msg_count);
    std::memcpy(out + MOLD_SESSION_BYTES + MOLD_HEADER_SEQ_BYTES, &be_count, MOLD_HEADER_COUNT_BYTES);
}
