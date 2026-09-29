// Unit tests for the parsing path: CLI args, ITCH wire structs, handlers and
// dispatch, MoldUDP64 framing, and both ingestion sources.

#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <new>
#include <string>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <cli_args.hpp>
#include <dispatch.hpp>
#include <ingest_mmap.hpp>
#include <ingest_socket.hpp>
#include <mold_udp64.hpp>

#include "harness.hpp"
#include "itch_builder.hpp"

namespace {

ParsedArgs parse(std::vector<std::string> args) {
    args.insert(args.begin(), "prog");
    std::vector<char*> argv;
    for (auto& a : args) argv.push_back(a.data());
    return parse_args(static_cast<int>(argv.size()), argv.data());
}

// Per-run scratch directory, removed by main() after all tests.
std::string g_tmpdir;

std::string temp_path(const char* tag) {
    std::string path = g_tmpdir + "/" + tag + "_XXXXXX";
    int fd = mkstemp(path.data());
    CHECK_MSG(fd >= 0, "mkstemp failed");
    ::close(fd);
    return path;
}

std::string write_file(const char* tag, const std::vector<uint8_t>& bytes) {
    std::string path = temp_path(tag);
    std::ofstream(path, std::ios::binary).write(reinterpret_cast<const char*>(bytes.data()),
                                                static_cast<std::streamsize>(bytes.size()));
    return path;
}

// Asks the kernel for an unused port. Racy in principle (it's released
// before the source binds it), but only against other local binds.
uint16_t free_udp_port() {
    int s = socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in a{};
    a.sin_family = AF_INET;
    socklen_t len = sizeof(a);
    CHECK(bind(s, reinterpret_cast<sockaddr*>(&a), sizeof(a)) == 0);
    CHECK(getsockname(s, reinterpret_cast<sockaddr*>(&a), &len) == 0);
    ::close(s);
    return ntohs(a.sin_port);
}

int count_mappings_of(const std::string& path) {
    std::ifstream maps("/proc/self/maps");
    std::string line;
    int n = 0;
    while (std::getline(maps, line))
        if (line.find(path) != std::string::npos) ++n;
    return n;
}

} // namespace

// ---------------------------------------------------------------- cli_args

TEST("cli_args: options, flags and positionals are separated") {
    auto a = parse({"--data=/x.itch", "--paced", "out.csv", "--port=30001", "extra"});
    CHECK_EQ(a.get("data", ""), std::string("/x.itch"));
    CHECK_EQ(a.get("port", ""), std::string("30001"));
    CHECK(a.has_flag("paced"));
    CHECK(!a.has_flag("data"));
    CHECK_EQ(a.positional.size(), size_t{2});
    CHECK_EQ(a.positional[0], std::string("out.csv"));
    CHECK_EQ(a.positional[1], std::string("extra"));
}

TEST("cli_args: get() falls back when the option is absent") {
    auto a = parse({});
    CHECK_EQ(a.get("host", "127.0.0.1"), std::string("127.0.0.1"));
    CHECK(!a.has_flag("paced"));
    CHECK(a.positional.empty());
}

TEST("cli_args: value may be empty or contain '='") {
    auto a = parse({"--k=", "--a=b=c"});
    CHECK_EQ(a.options.count("k"), size_t{1});
    CHECK_EQ(a.get("k", "fallback"), std::string(""));
    CHECK_EQ(a.get("a", ""), std::string("b=c"));
}

TEST("cli_args: later option overrides earlier; single dash is positional") {
    auto a = parse({"--port=1", "--port=2", "-v"});
    CHECK_EQ(a.get("port", ""), std::string("2"));
    CHECK_EQ(a.positional.size(), size_t{1});
    CHECK_EQ(a.positional[0], std::string("-v"));
}

// ------------------------------------------------------- ITCH wire structs

TEST("itch structs: sizes match the ITCH 5.0 spec") {
    CHECK_EQ(sizeof(SystemEvent), size_t{12});
    CHECK_EQ(sizeof(StockDirectory), size_t{39});
    CHECK_EQ(sizeof(AddOrderNoMPID), size_t{36});
    CHECK_EQ(sizeof(AddOrderWithMPID), size_t{40});
    CHECK_EQ(sizeof(OrderExecuted), size_t{31});
    CHECK_EQ(sizeof(OrderCancel), size_t{23});
    CHECK_EQ(sizeof(OrderDelete), size_t{19});
    CHECK_EQ(sizeof(TradeNonCross), size_t{44});
}

TEST("itch structs: common header fields sit at spec offsets") {
    CHECK_EQ(offsetof(AddOrderNoMPID, stock_locate), size_t{1});
    CHECK_EQ(offsetof(AddOrderNoMPID, tracking_no), size_t{3});
    CHECK_EQ(offsetof(AddOrderNoMPID, timestamp), size_t{5});
    CHECK_EQ(offsetof(AddOrderNoMPID, order_reference_no), size_t{11});
    CHECK_EQ(offsetof(AddOrderNoMPID, price), size_t{32});
    CHECK_EQ(offsetof(StockDirectory, symbol), size_t{11});
    CHECK_EQ(offsetof(StockDirectory, inverse_indicator), size_t{38});
    CHECK_EQ(offsetof(OrderExecuted, match_number), size_t{23});
    CHECK_EQ(offsetof(TradeNonCross, match_no), size_t{36});
    // replay.cpp reads the timestamp at offset 5 for *every* type
    CHECK_EQ(offsetof(SystemEvent, timestamp), size_t{5});
    CHECK_EQ(offsetof(TradeNonCross, timestamp), size_t{5});
}

TEST("itch builder: generated records have spec lengths") {
    CHECK_EQ(itch::system_event(1, 1, 'O').size(), size_t{12});
    CHECK_EQ(itch::stock_directory(1, 1, "AAPL").size(), size_t{39});
    CHECK_EQ(itch::add_order(1, 1, 1, 'B', 1, "AAPL", 1).size(), size_t{36});
    CHECK_EQ(itch::add_order_mpid(1, 1, 1, 'B', 1, "AAPL", 1, "GSCO").size(), size_t{40});
    CHECK_EQ(itch::order_executed(1, 1, 1, 1, 1).size(), size_t{31});
    CHECK_EQ(itch::order_executed_with_price(1, 1, 1, 1, 1, 'Y', 1).size(), size_t{36});
    CHECK_EQ(itch::order_cancel(1, 1, 1, 1).size(), size_t{23});
    CHECK_EQ(itch::order_delete(1, 1, 1).size(), size_t{19});
    CHECK_EQ(itch::order_replace(1, 1, 1, 2, 1, 1).size(), size_t{35});
    CHECK_EQ(itch::trade(1, 1, 1, 'B', 1, "AAPL", 1, 1).size(), size_t{44});
}

// ------------------------------------------------------ handlers/dispatch

TEST("read_timestamp48: decodes big-endian 48-bit values") {
    uint8_t zero[6] = {0, 0, 0, 0, 0, 0};
    uint8_t max[6] = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff};
    uint8_t v[6] = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06};
    CHECK_EQ(read_timestamp48(zero), uint64_t{0});
    CHECK_EQ(read_timestamp48(max), (uint64_t{1} << 48) - 1);
    CHECK_EQ(read_timestamp48(v), uint64_t{0x010203040506});
    // 23:59:59.999999999 — the largest real ITCH timestamp
    auto rec = itch::system_event(0, 86'399'999'999'999ull, 'C');
    CHECK_EQ(read_timestamp48(rec.data() + 5), uint64_t{86'399'999'999'999ull});
}

TEST("dispatch: S system event") {
    auto rec = itch::system_event(0, 34'200'000'000'000ull, 'Q');
    NormalisedMessage m = dispatch(rec.data());
    CHECK_EQ(m.msg_type, 'S');
    CHECK_EQ(m.stock_locate, uint16_t{0});
    CHECK_EQ(m.timestamp_ns, uint64_t{34'200'000'000'000ull});
    CHECK_EQ(m.order_ref, uint64_t{0});
}

TEST("dispatch: R stock directory") {
    auto rec = itch::stock_directory(0x1234, 1000, "MSFT");
    NormalisedMessage m = dispatch(rec.data());
    CHECK_EQ(m.msg_type, 'R');
    CHECK_EQ(m.stock_locate, uint16_t{0x1234});
    CHECK_EQ(m.timestamp_ns, uint64_t{1000});
}

TEST("dispatch: A add order (no MPID)") {
    auto rec = itch::add_order(7, 0xABCDEF012345ull, 0x0102030405060708ull, 'S', 300, "AAPL", 1'234'500);
    NormalisedMessage m = dispatch(rec.data());
    CHECK_EQ(m.msg_type, 'A');
    CHECK_EQ(m.stock_locate, uint16_t{7});
    CHECK_EQ(m.timestamp_ns, uint64_t{0xABCDEF012345ull});
    CHECK_EQ(m.order_ref, uint64_t{0x0102030405060708ull});
    CHECK_EQ(m.side, 'S');
    CHECK_EQ(m.shares, uint32_t{300});
    CHECK_EQ(m.price, uint32_t{1'234'500});
}

TEST("dispatch: F add order with MPID") {
    auto rec = itch::add_order_mpid(9, 55, 42, 'B', 100, "IBM", 999'900, "GSCO");
    NormalisedMessage m = dispatch(rec.data());
    CHECK_EQ(m.msg_type, 'F');
    CHECK_EQ(m.stock_locate, uint16_t{9});
    CHECK_EQ(m.timestamp_ns, uint64_t{55});
    CHECK_EQ(m.order_ref, uint64_t{42});
    CHECK_EQ(m.side, 'B');
    CHECK_EQ(m.shares, uint32_t{100});
    CHECK_EQ(m.price, uint32_t{999'900});
}

TEST("dispatch: E order executed") {
    auto rec = itch::order_executed(3, 77, 0xFFFFFFFFFFFFFFFEull, 0x80000001u, 12345);
    NormalisedMessage m = dispatch(rec.data());
    CHECK_EQ(m.msg_type, 'E');
    CHECK_EQ(m.stock_locate, uint16_t{3});
    CHECK_EQ(m.timestamp_ns, uint64_t{77});
    CHECK_EQ(m.order_ref, uint64_t{0xFFFFFFFFFFFFFFFEull});
    CHECK_EQ(m.shares, uint32_t{0x80000001u});
    CHECK_EQ(m.price, uint32_t{0});
}

TEST("dispatch: X order cancel") {
    auto rec = itch::order_cancel(4, 88, 1001, 50);
    NormalisedMessage m = dispatch(rec.data());
    CHECK_EQ(m.msg_type, 'X');
    CHECK_EQ(m.stock_locate, uint16_t{4});
    CHECK_EQ(m.order_ref, uint64_t{1001});
    CHECK_EQ(m.shares, uint32_t{50});
}

TEST("dispatch: D order delete") {
    auto rec = itch::order_delete(5, 99, 2002);
    NormalisedMessage m = dispatch(rec.data());
    CHECK_EQ(m.msg_type, 'D');
    CHECK_EQ(m.stock_locate, uint16_t{5});
    CHECK_EQ(m.timestamp_ns, uint64_t{99});
    CHECK_EQ(m.order_ref, uint64_t{2002});
    CHECK_EQ(m.shares, uint32_t{0});
}

TEST("dispatch: P trade (non-cross)") {
    auto rec = itch::trade(6, 111, 3003, 'B', 25, "TSLA", 2'500'000, 777);
    NormalisedMessage m = dispatch(rec.data());
    CHECK_EQ(m.msg_type, 'P');
    CHECK_EQ(m.stock_locate, uint16_t{6});
    CHECK_EQ(m.timestamp_ns, uint64_t{111});
    CHECK_EQ(m.order_ref, uint64_t{3003});
    CHECK_EQ(m.side, 'B');
    CHECK_EQ(m.shares, uint32_t{25});
    CHECK_EQ(m.price, uint32_t{2'500'000});
}

TEST("dispatch: types outside the normalised set are UNPOPULATED") {
    for (char t : {'H', 'Y', 'L', 'V', 'W', 'K', 'J', 'h', 'Q', 'B', 'I', 'N', '\0', '\x7f'}) {
        uint8_t rec[64] = {};
        rec[0] = static_cast<uint8_t>(t);
        NormalisedMessage m = dispatch(rec);
        CHECK_EQ(m.msg_type, UNPOPULATED);
        CHECK_EQ(m.order_ref, uint64_t{0});
    }
}

TEST("normalise: value-initialised message is UNPOPULATED; sentinels are distinct") {
    CHECK(STREAM_END_MSG_TYPE != UNPOPULATED);
    NormalisedMessage m{};
    CHECK_EQ(m.msg_type, UNPOPULATED);
}

TEST("dispatch: U order replace is normalised, not dropped") {
    auto rec = itch::order_replace(8, 500, 1111, 2222, 400, 1'000'100);
    NormalisedMessage m = dispatch(rec.data());
    CHECK_MSG(m.msg_type == 'U', "Order Replace ('U') came back UNPOPULATED, so it is never "
                                 "published and downstream can't track the order");
    CHECK_EQ(m.stock_locate, uint16_t{8});
    CHECK_EQ(m.timestamp_ns, uint64_t{500});
    CHECK_EQ(m.order_ref, uint64_t{2222});
    CHECK_EQ(m.orig_order_ref, uint64_t{1111});
    CHECK_EQ(m.shares, uint32_t{400});
    CHECK_EQ(m.price, uint32_t{1'000'100});
}

TEST("dispatch: C executed-with-price is normalised, not dropped") {
    auto rec = itch::order_executed_with_price(8, 600, 3333, 70, 9999, 'Y', 1'500'000);
    NormalisedMessage m = dispatch(rec.data());
    CHECK_MSG(m.msg_type == 'C', "Order Executed With Price ('C') came back UNPOPULATED, so "
                                 "those executions are lost");
    CHECK_EQ(m.order_ref, uint64_t{3333});
    CHECK_EQ(m.shares, uint32_t{70});
    CHECK_EQ(m.price, uint32_t{1'500'000});
}

TEST("dispatch: every handler zeroes the padding (all 64 bytes cross IPC)") {
    std::vector<std::vector<uint8_t>> recs = {
        itch::system_event(0, 1, 'O'),          itch::stock_directory(1, 1, "AAPL"),
        itch::add_order(1, 1, 1, 'B', 1, "X", 1), itch::add_order_mpid(1, 1, 1, 'B', 1, "X", 1, "GSCO"),
        itch::order_executed(1, 1, 1, 1, 1),    itch::order_cancel(1, 1, 1, 1),
        itch::order_delete(1, 1, 1),            itch::trade(1, 1, 1, 'B', 1, "X", 1, 1),
        itch::order_replace(1, 1, 1, 2, 1, 1),  itch::order_executed_with_price(1, 1, 1, 1, 1, 'Y', 1),
    };
    for (auto& rec : recs) {
        // The result is constructed in place (guaranteed elision) over a
        // 0xAA fill, so untouched padding keeps the fill.
        alignas(NormalisedMessage) unsigned char storage[sizeof(NormalisedMessage)];
        std::memset(storage, 0xAA, sizeof(storage));
        asm volatile("" ::: "memory"); // keep GCC from dropping the memset as a dead store
        new (storage) NormalisedMessage(dispatch(rec.data()));
        const unsigned char* pad = storage + offsetof(NormalisedMessage, padding);
        size_t dirty = 0;
        for (size_t i = 0; i < sizeof(NormalisedMessage::padding); ++i) dirty += pad[i] != 0;
        CHECK_MSG(dirty == 0, std::string("'") + char(rec[0]) + "' handler left " + std::to_string(dirty) +
                                  " of " + std::to_string(sizeof(NormalisedMessage::padding)) +
                                  " padding bytes uninitialised");
    }
}

// ------------------------------------------------------------- MoldUDP64

TEST("mold_write_header: session, big-endian sequence and count") {
    uint8_t out[MOLD_HEADER_BYTES];
    const char session[MOLD_SESSION_BYTES] = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'};
    mold_write_header(out, session, 0x0102030405060708ull, 0xBEEF);
    CHECK_EQ(MOLD_HEADER_BYTES, size_t{20});
    CHECK(std::memcmp(out, "ABCDEFGHIJ", 10) == 0);
    const uint8_t seq[8] = {1, 2, 3, 4, 5, 6, 7, 8};
    CHECK(std::memcmp(out + 10, seq, 8) == 0);
    CHECK_EQ(out[18], uint8_t{0xBE});
    CHECK_EQ(out[19], uint8_t{0xEF});
}

// ------------------------------------------------------------ mmap_buffer

TEST("mmap_buffer: returns every framed record in order, then EOF") {
    std::vector<std::vector<uint8_t>> recs = {
        itch::system_event(0, 1, 'O'),
        itch::add_order(1, 2, 10, 'B', 100, "AAPL", 5),
        itch::trade(1, 3, 11, 'S', 5, "AAPL", 6, 1),
        itch::order_delete(1, 4, 10),
    };
    std::vector<uint8_t> file;
    for (auto& r : recs) itch::append_framed(file, r);
    std::string path = write_file("mmap", file);

    mmap_buffer buf(path);
    for (auto& r : recs) {
        const uint8_t* p = buf.next_record();
        CHECK(p != nullptr);
        CHECK_EQ(buf.last_len, static_cast<uint16_t>(r.size()));
        CHECK(std::memcmp(p, r.data(), r.size()) == 0);
    }
    CHECK(buf.next_record() == nullptr);
    CHECK(buf.next_record() == nullptr);
    buf.close();
}

TEST("mmap_buffer: empty file yields no records") {
    std::string path = write_file("empty", {});
    mmap_buffer buf(path);
    CHECK(buf.next_record() == nullptr);
    buf.close();
}

TEST("mmap_buffer: truncated final record is not returned") {
    std::vector<uint8_t> file;
    itch::append_framed(file, itch::system_event(0, 1, 'O')); // even length: keeps the next prefix aligned
    itch::append_framed(file, itch::trade(1, 2, 2, 'B', 1, "X", 1, 1));
    file.resize(file.size() - 10); // chop the tail off the second record
    std::string path = write_file("trunc", file);

    mmap_buffer buf(path);
    CHECK(buf.next_record() != nullptr);
    CHECK(buf.next_record() == nullptr);
    buf.close();
}

TEST("mmap_buffer: a lone trailing byte is not read as a length prefix") {
    std::vector<uint8_t> file;
    itch::append_framed(file, itch::order_delete(1, 1, 1));
    file.push_back(0x00);
    std::string path = write_file("tail", file);

    mmap_buffer buf(path);
    CHECK(buf.next_record() != nullptr);
    CHECK(buf.next_record() == nullptr);
    buf.close();
}

TEST("mmap_buffer: records shorter than their type's wire size are not returned") {
    // Callers read a fixed number of bytes per type, so a 0-byte record or a
    // 2-byte 'P' would be parsed from whatever follows it. Skipping them or
    // stopping at them are both fine; handing them out is not. (Even lengths
    // keep prefixes aligned, so the UBSan build tests this, not alignment.)
    std::vector<uint8_t> file = {0x00, 0x00, 0x00, 0x02, 'P', 0x00};
    itch::append_framed(file, itch::order_delete(1, 1, 7));
    std::string path = write_file("short", file);

    mmap_buffer buf(path);
    const uint8_t* r;
    while ((r = buf.next_record()) != nullptr) {
        CHECK_MSG(buf.last_len == 19 && r[0] == 'D',
                  "returned a " + std::to_string(buf.last_len) + "-byte record");
    }
    buf.close();
}

TEST("mmap_buffer: odd-length records (length prefix at an odd offset) parse cleanly") {
    // Most ITCH records have odd lengths, so length prefixes sit at odd
    // addresses; reading one as uint16_t is UB (caught by the UBSan build).
    std::vector<std::vector<uint8_t>> recs = {
        itch::order_delete(1, 1, 1),        // 19 bytes
        itch::order_cancel(1, 2, 2, 5),     // 23 bytes
        itch::order_executed(1, 3, 3, 1, 1) // 31 bytes
    };
    std::vector<uint8_t> file;
    for (auto& r : recs) itch::append_framed(file, r);
    std::string path = write_file("odd", file);

    mmap_buffer buf(path);
    for (auto& r : recs) {
        const uint8_t* p = buf.next_record();
        CHECK(p != nullptr);
        CHECK_EQ(buf.last_len, static_cast<uint16_t>(r.size()));
        CHECK(std::memcmp(p, r.data(), r.size()) == 0);
    }
    CHECK(buf.next_record() == nullptr);
    buf.close();
}

TEST("mmap_buffer: missing file throws instead of reading garbage") {
    bool threw = false;
    try {
        mmap_buffer buf("/nonexistent/definitely/missing.itch");
        (void)buf.next_record();
    } catch (const std::exception&) {
        threw = true;
    }
    CHECK_MSG(threw, "constructing mmap_buffer on a missing file did not throw");
}

TEST("mmap_buffer: mapping is released after close + destruction") {
    std::vector<uint8_t> file;
    itch::append_framed(file, itch::order_delete(1, 1, 1));
    std::string path = write_file("unmap", file);
    {
        mmap_buffer buf(path);
        CHECK_EQ(count_mappings_of(path), 1);
        buf.close();
    }
    CHECK_MSG(count_mappings_of(path) == 0, "file is still mapped after mmap_buffer was closed and destroyed");
}

// ------------------------------------------------------ UdpMoldIngestSource

namespace {

struct UdpHarness {
    std::unique_ptr<UdpMoldIngestSource> src;
    int tx = -1;
    sockaddr_in dest{};
    uint64_t seq = 1;

    UdpHarness() {
        dest.sin_family = AF_INET;
        dest.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        dest.sin_port = htons(free_udp_port());
        src = std::make_unique<UdpMoldIngestSource>(ntohs(dest.sin_port));
        tx = socket(AF_INET, SOCK_DGRAM, 0);
        CHECK(tx >= 0);
    }
    ~UdpHarness() {
        ::close(tx);
        src->close();
    }

    void send_raw(const std::vector<uint8_t>& d) {
        CHECK(sendto(tx, d.data(), d.size(), 0, reinterpret_cast<sockaddr*>(&dest), sizeof(dest)) ==
              static_cast<ssize_t>(d.size()));
    }

    static std::vector<uint8_t> datagram(uint64_t seq, uint16_t count,
                                         const std::vector<std::vector<uint8_t>>& recs) {
        std::vector<uint8_t> d(MOLD_HEADER_BYTES);
        const char session[MOLD_SESSION_BYTES] = {'T', 'E', 'S', 'T', '0', '0', '0', '0', '0', '1'};
        mold_write_header(d.data(), session, seq, count);
        for (auto& r : recs) itch::append_framed(d, r);
        return d;
    }

    // Sends a well-formed datagram and advances the sequence number.
    void send(const std::vector<std::vector<uint8_t>>& recs) {
        send_raw(datagram(seq, static_cast<uint16_t>(recs.size()), recs));
        seq += recs.size();
    }
    void send_end() { send_raw(datagram(seq, MOLD_END_OF_SESSION, {})); }

    // Drains until EOF, returning (type, order_ref) of each record.
    std::vector<std::pair<char, uint64_t>> drain() {
        std::vector<std::pair<char, uint64_t>> out;
        const uint8_t* r;
        while ((r = src->next_record()) != nullptr && out.size() < 1000) {
            NormalisedMessage m = dispatch(r);
            out.push_back({static_cast<char>(r[0]), m.order_ref});
        }
        return out;
    }
};

} // namespace

TEST("udp: batched sub-messages are returned in order, then EOF") {
    UdpHarness h;
    auto a = itch::add_order(1, 1, 100, 'B', 1, "X", 1);
    auto e = itch::order_executed(1, 2, 100, 1, 1);
    auto d = itch::order_delete(1, 3, 101);
    h.send({a, e});
    h.send({d});
    h.send_end();

    const uint8_t* r = h.src->next_record();
    CHECK(r != nullptr);
    CHECK(std::memcmp(r, a.data(), a.size()) == 0);
    r = h.src->next_record();
    CHECK(r != nullptr);
    CHECK(std::memcmp(r, e.data(), e.size()) == 0);
    r = h.src->next_record();
    CHECK(r != nullptr);
    CHECK(std::memcmp(r, d.data(), d.size()) == 0);
    CHECK(h.src->next_record() == nullptr);
    CHECK(h.src->next_record() == nullptr); // stays at EOF
}

TEST("udp: heartbeats and short datagrams are skipped") {
    UdpHarness h;
    h.send_raw(UdpHarness::datagram(1, 0, {}));         // heartbeat
    h.send_raw({'j', 'u', 'n', 'k'});                   // shorter than a Mold header
    h.send({itch::order_delete(1, 1, 5)});
    h.send_raw(UdpHarness::datagram(h.seq, 0, {}));     // heartbeat
    h.send({itch::order_delete(1, 2, 6)});
    h.send_end();
    auto got = h.drain();
    CHECK_EQ(got.size(), size_t{2});
    CHECK_EQ(got[0].second, uint64_t{5});
    CHECK_EQ(got[1].second, uint64_t{6});
}

TEST("udp: message count larger than the datagram is not trusted") {
    // Header claims 3 sub-messages, datagram carries 1. The parser must not
    // walk past the received bytes into whatever the buffer held before.
    UdpHarness h;
    h.send_raw(UdpHarness::datagram(1, 3, {itch::add_order(1, 1, 100, 'B', 1, "X", 1)}));
    h.send_raw(UdpHarness::datagram(4, 1, {itch::order_delete(1, 2, 200)}));
    h.send_raw(UdpHarness::datagram(5, MOLD_END_OF_SESSION, {}));
    auto got = h.drain();
    CHECK_MSG(got.size() <= 2, "returned " + std::to_string(got.size()) +
                                   " records from 2 real ones (read past the datagram)");
    CHECK(!got.empty());
    CHECK_EQ(got.back().first, 'D');
    CHECK_EQ(got.back().second, uint64_t{200});
}

TEST("udp: sub-message length past the datagram end is not trusted") {
    // A 'P' record whose length prefix says 44 but only 12 bytes arrived.
    UdpHarness h;
    auto trade = itch::trade(1, 1, 300, 'B', 1, "X", 1, 1);
    auto d = UdpHarness::datagram(1, 1, {trade});
    d.resize(MOLD_HEADER_BYTES + 2 + 12);
    h.send_raw(d);
    h.send_raw(UdpHarness::datagram(2, 1, {itch::order_delete(1, 2, 400)}));
    h.send_raw(UdpHarness::datagram(3, MOLD_END_OF_SESSION, {}));
    auto got = h.drain();
    CHECK_MSG(got.size() == 1, "truncated record was returned to the caller (" +
                                   std::to_string(got.size()) + " records)");
    CHECK_EQ(got[0].second, uint64_t{400});
}

TEST("udp: sub-message shorter than its type's wire size is not returned") {
    // A 1-byte 'P' followed by a D: parsing the P would read the D's bytes.
    UdpHarness h;
    auto d = UdpHarness::datagram(1, 2, {{'P'}, itch::order_delete(1, 1, 5)});
    h.send_raw(d);
    h.send_raw(UdpHarness::datagram(3, 1, {itch::order_delete(1, 2, 6)}));
    h.send_raw(UdpHarness::datagram(4, MOLD_END_OF_SESSION, {}));
    auto got = h.drain();
    for (auto& [type, ref] : got)
        CHECK_MSG(type == 'D' && (ref == 5 || ref == 6), std::string("returned a malformed '") + type + "' record");
    CHECK(!got.empty() && got.back().second == 6);
}

TEST("udp: duplicate datagram (same sequence number) is delivered once") {
    UdpHarness h;
    auto dgram = UdpHarness::datagram(1, 2, {itch::order_delete(1, 1, 1), itch::order_delete(1, 2, 2)});
    h.send_raw(dgram);
    h.send_raw(dgram); // MoldUDP64 receivers must drop already-seen sequence numbers
    h.send_raw(UdpHarness::datagram(3, 1, {itch::order_delete(1, 3, 3)}));
    h.send_raw(UdpHarness::datagram(4, MOLD_END_OF_SESSION, {}));
    auto got = h.drain();
    CHECK_MSG(got.size() == 3, "got " + std::to_string(got.size()) + " records, expected 3 (duplicates re-delivered)");
}

int main() {
    const char* base = std::getenv("TMPDIR");
    std::string tmpl = std::string(base ? base : "/tmp") + "/fh_test_parsing_XXXXXX";
    if (!mkdtemp(tmpl.data())) {
        std::perror("mkdtemp");
        return 1;
    }
    g_tmpdir = tmpl;
    int rc = th::run_all();
    std::filesystem::remove_all(g_tmpdir);
    return rc;
}
