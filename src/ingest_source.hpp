#pragma once

#include <stdint.h>

// Common interface over "where raw ITCH records come from": mmap'd
// file vs. UDP/MoldUDP64 socket. next_record() is only ever called outside
// the timed dispatch region in main.cpp/producer_main.cpp, so the vtable
// indirection here doesn't pollute the parse/IPC latency histograms — it's
// paid once per message, before t0 is taken, not inside it.
struct IngestSource {
    virtual const uint8_t* next_record() = 0;
    virtual void close() = 0;
    virtual ~IngestSource() = default;
};
