#pragma once

#include <stddef.h>
#include <stdint.h>

#pragma pack(push, 1)

struct SystemEvent {
    char msg_type; // = 'S'
    uint16_t stock_locate; // = '0'
    uint16_t tracking_no;
    uint8_t timestamp[6];
    char event_code;
};

struct StockDirectory {
    char msg_type; // = 'R'
    uint16_t stock_locate;
    uint16_t tracking_no;
    uint8_t timestamp[6];
    char symbol[8];
    char market_category;
    char financial_status_indicator;
    uint32_t round_lot_size;
    uint8_t round_lots_only;
    char issue_classification;
    char issue_subtype[2];
    char authenticity;
    char short_sale_threshold_indicator;
    char ipo_flag;
    char luld_reference_price_tier;
    char etp_flag;
    uint32_t etp_leverage_factor;
    char inverse_indicator;
};

struct AddOrderNoMPID {
    char msg_type; // = 'A'
    uint16_t stock_locate;
    uint16_t tracking_no;
    uint8_t timestamp[6];
    uint64_t order_reference_no;
    char buy_sell_indicator;
    uint32_t shares;
    char symbol[8];
    uint32_t price;
};

struct AddOrderWithMPID {
    char msg_type; // = 'F'
    uint16_t stock_locate;
    uint16_t tracking_no;
    uint8_t timestamp[6];
    uint64_t order_reference_no;
    char buy_sell_indicator;
    uint32_t shares;
    char symbol[8];
    uint32_t price;
    char attribution[4];
};

struct OrderExecuted {
    char msg_type; // = 'E'
    uint16_t stock_locate;
    uint16_t tracking_no;
    uint8_t timestamp[6];
    uint64_t order_reference_no;
    uint32_t executed_shares;
    uint64_t match_number;
};

struct OrderReplace {
    char msg_type; // = 'U'
    uint16_t stock_locate;
    uint16_t tracking_no;
    uint8_t timestamp[6];
    uint64_t original_order_reference_no;
    uint64_t new_order_reference_no;
    uint32_t shares;
    uint32_t price;
};

struct OrderExecutedWithPrice {
    char msg_type; // = 'C'
    uint16_t stock_locate;
    uint16_t tracking_no;
    uint8_t timestamp[6];
    uint64_t order_reference_no;
    uint32_t executed_shares;
    uint64_t match_number;
    char printable;
    uint32_t execution_price;
};

struct OrderCancel {
    char msg_type; // = 'X'
    uint16_t stock_locate;
    uint16_t tracking_no;
    uint8_t timestamp[6];
    uint64_t order_reference_no;
    uint32_t cancelled_shares;
};

struct OrderDelete {
    char msg_type; // = 'D'
    uint16_t stock_locate;
    uint16_t tracking_no;
    uint8_t timestamp[6];
    uint64_t order_reference_no;
};

struct TradeNonCross {
    char msg_type; // = 'P'
    uint16_t stock_locate;
    uint16_t tracking_no;
    uint8_t timestamp[6];
    uint64_t order_reference_no;
    char buy_sell_indicator;
    uint32_t shares;
    char symbol[8];
    uint32_t price;
    uint64_t match_no;
};

#pragma pack(pop)

// Every ITCH message starts with the same 11 bytes: type, stock locate,
// tracking number and 48-bit timestamp.
constexpr size_t ITCH_HEADER_BYTES = 11;

// Shortest valid record of the given type. Ingestion drops anything shorter,
// since handlers (and replay's timestamp read) read a fixed number of bytes.
inline size_t itch_min_length(uint8_t type) {
    switch (type) {
        case 'S': return sizeof(SystemEvent);
        case 'R': return sizeof(StockDirectory);
        case 'A': return sizeof(AddOrderNoMPID);
        case 'F': return sizeof(AddOrderWithMPID);
        case 'E': return sizeof(OrderExecuted);
        case 'C': return sizeof(OrderExecutedWithPrice);
        case 'U': return sizeof(OrderReplace);
        case 'X': return sizeof(OrderCancel);
        case 'D': return sizeof(OrderDelete);
        case 'P': return sizeof(TradeNonCross);
        default: return ITCH_HEADER_BYTES;
    }
}
