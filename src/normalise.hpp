#pragma once

#include <stdint.h>
#include <type_traits>
#include <itch_messages.hpp>

constexpr char UNPOPULATED = 0;

// Sentinel msg_type published by the IPC producer to tell the consumer the
// stream is over. Not a real ITCH type (those are all uppercase ASCII) and
// distinct from UNPOPULATED, which instead marks "unsupported record, don't
// publish this one".
constexpr char STREAM_END_MSG_TYPE = 0x7f;

// Fixed-size, trivially-copyable normalized message — one cache line, so it
// can be pushed by value into the SPSC ring buffer or written raw over a
// socket with no serialization step.
// Fields ordered widest-to-narrowest so natural alignment alone packs them
// with no compiler-inserted gaps before the explicit tail padding.
struct NormalisedMessage {
    uint64_t timestamp_ns = UNPOPULATED;   // wire timestamp: ITCH ns-since-midnight
    uint64_t order_ref = UNPOPULATED;
    uint64_t ingest_ns = UNPOPULATED;      // CLOCK_MONOTONIC ns when the producer ingested this record
    uint64_t orig_order_ref = UNPOPULATED; // 'U' only: the order being replaced (order_ref is the new one)
    uint32_t price = UNPOPULATED;
    uint32_t shares = UNPOPULATED;
    uint16_t stock_locate = UNPOPULATED;
    char msg_type = UNPOPULATED;
    char side = UNPOPULATED;
    char padding[20];
};

static_assert(sizeof(NormalisedMessage) == 64, "NormalisedMessage must fit one cache line");
static_assert(std::is_trivially_copyable_v<NormalisedMessage>,
    "NormalisedMessage crosses process boundaries (ring buffer / socket) and must be trivially copyable");