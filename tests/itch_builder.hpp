#pragma once

// Builds raw big-endian ITCH 5.0 records byte by byte, independently of the
// packed structs in src/itch_messages.hpp, so the parser is checked against
// the wire format rather than against itself.

#include <cstdint>
#include <cstring>
#include <vector>

namespace itch {

struct Writer {
    std::vector<uint8_t> b;
    Writer& u8(uint8_t v) { b.push_back(v); return *this; }
    Writer& ch(char v) { b.push_back(static_cast<uint8_t>(v)); return *this; }
    Writer& u16(uint16_t v) { return u8(v >> 8).u8(v & 0xff); }
    Writer& u32(uint32_t v) { return u16(v >> 16).u16(v & 0xffff); }
    Writer& u48(uint64_t v) {
        for (int i = 5; i >= 0; --i) u8((v >> (8 * i)) & 0xff);
        return *this;
    }
    Writer& u64(uint64_t v) { return u32(v >> 32).u32(v & 0xffffffff); }
    Writer& str(const char* s, size_t n) {
        size_t len = std::strlen(s);
        for (size_t i = 0; i < n; ++i) ch(i < len ? s[i] : ' ');
        return *this;
    }
    // type, stock_locate, tracking_no, 48-bit timestamp: common to every message
    Writer& header(char type, uint16_t locate, uint16_t tracking, uint64_t ts) {
        return ch(type).u16(locate).u16(tracking).u48(ts);
    }
};

inline std::vector<uint8_t> system_event(uint16_t locate, uint64_t ts, char code) {
    return Writer{}.header('S', locate, 1, ts).ch(code).b;
}

inline std::vector<uint8_t> stock_directory(uint16_t locate, uint64_t ts, const char* symbol) {
    return Writer{}.header('R', locate, 1, ts)
        .str(symbol, 8).ch('Q').ch('N').u32(100).ch('N').ch('C').str("Z", 2)
        .ch('P').ch('N').ch('N').ch('1').ch('N').u32(0).ch('N').b;
}

inline std::vector<uint8_t> add_order(uint16_t locate, uint64_t ts, uint64_t ref, char side,
                                      uint32_t shares, const char* symbol, uint32_t price) {
    return Writer{}.header('A', locate, 1, ts).u64(ref).ch(side).u32(shares).str(symbol, 8).u32(price).b;
}

inline std::vector<uint8_t> add_order_mpid(uint16_t locate, uint64_t ts, uint64_t ref, char side,
                                           uint32_t shares, const char* symbol, uint32_t price,
                                           const char* mpid) {
    return Writer{}.header('F', locate, 1, ts).u64(ref).ch(side).u32(shares).str(symbol, 8)
        .u32(price).str(mpid, 4).b;
}

inline std::vector<uint8_t> order_executed(uint16_t locate, uint64_t ts, uint64_t ref,
                                           uint32_t shares, uint64_t match) {
    return Writer{}.header('E', locate, 1, ts).u64(ref).u32(shares).u64(match).b;
}

inline std::vector<uint8_t> order_executed_with_price(uint16_t locate, uint64_t ts, uint64_t ref,
                                                      uint32_t shares, uint64_t match,
                                                      char printable, uint32_t price) {
    return Writer{}.header('C', locate, 1, ts).u64(ref).u32(shares).u64(match).ch(printable).u32(price).b;
}

inline std::vector<uint8_t> order_cancel(uint16_t locate, uint64_t ts, uint64_t ref, uint32_t shares) {
    return Writer{}.header('X', locate, 1, ts).u64(ref).u32(shares).b;
}

inline std::vector<uint8_t> order_delete(uint16_t locate, uint64_t ts, uint64_t ref) {
    return Writer{}.header('D', locate, 1, ts).u64(ref).b;
}

inline std::vector<uint8_t> order_replace(uint16_t locate, uint64_t ts, uint64_t orig_ref,
                                          uint64_t new_ref, uint32_t shares, uint32_t price) {
    return Writer{}.header('U', locate, 1, ts).u64(orig_ref).u64(new_ref).u32(shares).u32(price).b;
}

inline std::vector<uint8_t> trade(uint16_t locate, uint64_t ts, uint64_t ref, char side,
                                  uint32_t shares, const char* symbol, uint32_t price, uint64_t match) {
    return Writer{}.header('P', locate, 1, ts).u64(ref).ch(side).u32(shares).str(symbol, 8)
        .u32(price).u64(match).b;
}

// File-mode framing: [2-byte big-endian length][record]
inline void append_framed(std::vector<uint8_t>& out, const std::vector<uint8_t>& rec) {
    out.push_back(static_cast<uint8_t>(rec.size() >> 8));
    out.push_back(static_cast<uint8_t>(rec.size() & 0xff));
    out.insert(out.end(), rec.begin(), rec.end());
}

} // namespace itch
