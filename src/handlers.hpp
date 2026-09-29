#pragma once

#include <stdint.h>

#include <itch_messages.hpp>
#include <normalise.hpp>

// Returned by value: NormalisedMessage is a trivially-copyable 64-byte
// struct, so this is a stack copy, not a heap allocation — required both to
// keep the hot path alloc-free and so the result can be pushed directly into
// the SPSC ring buffer or written raw to a socket.
using handler_func = NormalisedMessage (*)(const uint8_t*);

inline uint64_t read_timestamp48(const uint8_t timestamp[6]) {
    uint64_t raw = 0;
    // Bytes land at raw's byte offsets [2,7]: on this little-endian target,
    // that's bit positions [16,63) pre-swap. bswap64 then reverses all 8
    // bytes at once, which both restores big-endian byte order *and* moves
    // the value down to bit position 0 in the same step — the two zero
    // padding bytes end up as the new high-order byte, not a low-order
    // remainder, so no further shift is needed.
    __builtin_memcpy(reinterpret_cast<uint8_t*>(&raw) + 2, timestamp, 6);
    return __builtin_bswap64(raw);
}

inline NormalisedMessage system_event(const uint8_t* record) {
    const auto* msg = reinterpret_cast<const SystemEvent*>(record);
    NormalisedMessage norm;
    norm.msg_type     = msg->msg_type;
    norm.stock_locate = __builtin_bswap16(msg->stock_locate);
    norm.timestamp_ns = read_timestamp48(msg->timestamp);
    return norm;
}

inline NormalisedMessage stock_directory(const uint8_t* record) {
    const auto* msg = reinterpret_cast<const StockDirectory*>(record);
    NormalisedMessage norm;
    norm.msg_type     = msg->msg_type;
    norm.stock_locate = __builtin_bswap16(msg->stock_locate);
    norm.timestamp_ns = read_timestamp48(msg->timestamp);
    return norm;
}

inline NormalisedMessage add_order_no_mpid(const uint8_t* record) {
    const auto* msg = reinterpret_cast<const AddOrderNoMPID*>(record);
    NormalisedMessage norm;
    norm.msg_type = msg->msg_type;
    norm.stock_locate = __builtin_bswap16(msg->stock_locate);
    norm.timestamp_ns = read_timestamp48(msg->timestamp);
    norm.order_ref    = __builtin_bswap64(msg->order_reference_no);
    norm.price        = __builtin_bswap32(msg->price);
    norm.shares       = __builtin_bswap32(msg->shares);
    norm.side         = msg->buy_sell_indicator;
    return norm;
}

inline NormalisedMessage add_order_with_mpid(const uint8_t* record) {
    const auto* msg = reinterpret_cast<const AddOrderWithMPID*>(record);
    NormalisedMessage norm;
    norm.msg_type     = msg->msg_type;
    norm.stock_locate = __builtin_bswap16(msg->stock_locate);
    norm.timestamp_ns = read_timestamp48(msg->timestamp);
    norm.order_ref    = __builtin_bswap64(msg->order_reference_no);
    norm.price        = __builtin_bswap32(msg->price);
    norm.shares       = __builtin_bswap32(msg->shares);
    norm.side         = msg->buy_sell_indicator;
    return norm;
}

inline NormalisedMessage order_executed(const uint8_t* record) {
    const auto* msg = reinterpret_cast<const OrderExecuted*>(record);
    NormalisedMessage norm;
    norm.msg_type     = msg->msg_type;
    norm.stock_locate = __builtin_bswap16(msg->stock_locate);
    norm.timestamp_ns = read_timestamp48(msg->timestamp);
    norm.order_ref    = __builtin_bswap64(msg->order_reference_no);
    norm.shares       = __builtin_bswap32(msg->executed_shares);
    return norm;
}

inline NormalisedMessage order_replace(const uint8_t* record) {
    const auto* msg = reinterpret_cast<const OrderReplace*>(record);
    NormalisedMessage norm;
    norm.msg_type       = msg->msg_type;
    norm.stock_locate   = __builtin_bswap16(msg->stock_locate);
    norm.timestamp_ns   = read_timestamp48(msg->timestamp);
    norm.order_ref      = __builtin_bswap64(msg->new_order_reference_no);
    norm.orig_order_ref = __builtin_bswap64(msg->original_order_reference_no);
    norm.price          = __builtin_bswap32(msg->price);
    norm.shares         = __builtin_bswap32(msg->shares);
    return norm;
}

inline NormalisedMessage order_cancel(const uint8_t* record) {
    const auto* msg = reinterpret_cast<const OrderCancel*>(record);
    NormalisedMessage norm;
    norm.msg_type     = msg->msg_type;
    norm.stock_locate = __builtin_bswap16(msg->stock_locate);
    norm.timestamp_ns = read_timestamp48(msg->timestamp);
    norm.order_ref    = __builtin_bswap64(msg->order_reference_no);
    norm.shares       = __builtin_bswap32(msg->cancelled_shares);
    return norm;
}

inline NormalisedMessage order_delete(const uint8_t* record) {
    const auto* msg = reinterpret_cast<const OrderDelete*>(record);
    NormalisedMessage norm;
    norm.msg_type     = msg->msg_type;
    norm.stock_locate = __builtin_bswap16(msg->stock_locate);
    norm.timestamp_ns = read_timestamp48(msg->timestamp);
    norm.order_ref    = __builtin_bswap64(msg->order_reference_no);
    return norm;
}

inline NormalisedMessage trade_non_cross(const uint8_t* record) {
    const auto* msg = reinterpret_cast<const TradeNonCross*>(record);
    NormalisedMessage norm;
    norm.msg_type     = msg->msg_type;
    norm.stock_locate = __builtin_bswap16(msg->stock_locate);
    norm.timestamp_ns = read_timestamp48(msg->timestamp);
    norm.order_ref    = __builtin_bswap64(msg->order_reference_no);
    norm.price        = __builtin_bswap32(msg->price);
    norm.shares       = __builtin_bswap32(msg->shares);
    norm.side         = msg->buy_sell_indicator;
    return norm;
}

inline NormalisedMessage unsupported(const uint8_t* /*record*/) {
    return NormalisedMessage{}; // msg_type == UNPOPULATED signals "don't publish"
}
