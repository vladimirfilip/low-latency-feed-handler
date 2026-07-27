#pragma once

#include <stdint.h>

#include <handlers.hpp>
#include <normalise.hpp>

// switch on the message-type byte, one case per handler. GCC lowers
// this into its own internal jump table when the case values are dense
// enough, and — because every handler here is only ever reached through
// this switch — inlines each handler body directly into its case. A
// function-pointer table was also built and measured here; it was slower
// (see README.md) because taking a handler's address to build that table
// forces it to stay a real, out-of-line, call/ret function, which this
// switch's free inlining beats outright for this handler shape. Removed
// rather than kept as a slower alternative.
inline NormalisedMessage dispatch(const uint8_t* record) {
    switch (record[0]) {
        case 'S': return system_event(record);
        case 'R': return stock_directory(record);
        case 'A': return add_order_no_mpid(record);
        case 'F': return add_order_with_mpid(record);
        case 'E': return order_executed(record);
        case 'X': return order_cancel(record);
        case 'D': return order_delete(record);
        case 'P': return trade_non_cross(record);
        default: return unsupported(record);
    }
}
