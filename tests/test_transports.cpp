// Multi-process tests for the IPC transports (shared-memory ring and
// AF_UNIX socket), driven only through their public open/send/recv/close
// interface.
//
// Each test body is a supervisor: it forks the producer and consumer as
// separate child processes, the same shape as the real benchmark, and
// watches them with deadlines. Dead children are reaped right away (see
// th::Child::kill_and_reap).

#include <string>
#include <type_traits>
#include <vector>

#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <latency_histogram.hpp>
#include <normalise.hpp>
#include <ring_transport.hpp>
#include <unix_socket_transport.hpp>

#include "harness.hpp"

namespace {

// Exit codes the producer/consumer children report with.
constexpr int kOk = 0;
constexpr int kNoEndMarker = 10;   // recv() returned false before STREAM_END
constexpr int kBadContent = 11;    // a message didn't match what was sent
constexpr int kThrew = 12;         // open/send/recv threw (producer: send only)
constexpr int kSlowStart = 13;     // first message stamped long before it was readable
constexpr int kOpenFailed = 15;    // producer's open() threw
constexpr int kGotGarbage = 20;    // received a message from a segment no producer created

const std::string SHM_PATH = std::string("/dev/shm") + RING_SHM_NAME;

void cleanup_ipc_names() {
    shm_unlink(RING_SHM_NAME);
    unlink(UNIX_SOCKET_PATH);
}

struct Cleanup {
    Cleanup() { cleanup_ipc_names(); }
    ~Cleanup() { cleanup_ipc_names(); }
};

NormalisedMessage make_msg(uint64_t base, uint64_t i) {
    NormalisedMessage m{};
    m.msg_type = 'A';
    m.order_ref = base + i;
    m.price = static_cast<uint32_t>(i * 7);
    m.shares = static_cast<uint32_t>(i);
    m.side = (i & 1) ? 'S' : 'B';
    m.ingest_ns = monotonic_ns();
    return m;
}

NormalisedMessage end_msg() {
    NormalisedMessage m{};
    m.msg_type = STREAM_END_MSG_TYPE;
    return m;
}

struct ProducerOpts {
    uint64_t base = 0;
    uint64_t n = 1000;
    bool die_after_sending = false; // SIGKILL itself instead of finishing (crash simulation)
    int pause_after_first_ms = 0;
    bool umask0 = false; // so a segment's mode reflects what the code asks for
};

// Opens a producer transport, exiting kOpenFailed if that throws, so a
// start-up failure can't pass for "send() noticed a dead consumer".
template <typename Transport>
void open_producer(Transport& t) {
    try {
        t.open();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "    producer: open() threw: %s\n", e.what());
        _exit(kOpenFailed);
    }
}

template <typename Transport>
void producer_body(const ProducerOpts& o) {
    if (o.umask0) umask(0);
    Transport t;
    open_producer(t);
    try {
        for (uint64_t i = 0; i < o.n; ++i) {
            t.send(make_msg(o.base, i));
            if (i == 0 && o.pause_after_first_ms) usleep(o.pause_after_first_ms * 1000);
        }
        if (o.die_after_sending) raise(SIGKILL);
        t.send(end_msg());
        t.close();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "    producer: threw: %s\n", e.what());
        _exit(kThrew);
    }
}

struct ConsumerOpts {
    uint64_t base = 0;
    uint64_t expect_n = 1000;
    uint64_t quit_after = UINT64_MAX; // exit early (simulating a consumer that goes away)
    uint64_t max_first_latency_ms = 0; // 0 = don't check
    bool eintr_handler = false;
    int ready_fd = -1; // if set, a byte is written here after the first message
};

extern "C" void noop_handler(int) {}

template <typename Transport>
void consumer_body(const ConsumerOpts& o) {
    if (o.eintr_handler) {
        struct sigaction sa{};
        sa.sa_handler = noop_handler;
        sigemptyset(&sa.sa_mask);
        sa.sa_flags = 0; // no SA_RESTART: blocking syscalls return EINTR
        sigaction(SIGUSR1, &sa, nullptr);
    }
    uint64_t count = 0;
    bool bad = false;
    try {
        Transport t;
        t.open();
        NormalisedMessage m;
        while (t.recv(m)) {
            if (m.msg_type == STREAM_END_MSG_TYPE) {
                t.close();
                if (bad) _exit(kBadContent);
                if (count != o.expect_n) {
                    std::fprintf(stderr, "    consumer: got %llu messages, expected %llu\n",
                                 (unsigned long long)count, (unsigned long long)o.expect_n);
                    _exit(kBadContent);
                }
                _exit(kOk);
            }
            if (count == 0 && o.max_first_latency_ms) {
                uint64_t lat_ms = (monotonic_ns() - m.ingest_ns) / 1'000'000;
                if (lat_ms > o.max_first_latency_ms) {
                    std::fprintf(stderr, "    consumer: first message was stamped %llu ms before it "
                                         "could be read (published before a consumer existed)\n",
                                 (unsigned long long)lat_ms);
                    _exit(kSlowStart);
                }
            }
            NormalisedMessage want = make_msg(o.base, count);
            if (!bad && (m.order_ref != want.order_ref || m.price != want.price ||
                         m.shares != want.shares || m.side != want.side || m.msg_type != 'A')) {
                std::fprintf(stderr, "    consumer: message #%llu has order_ref %llu, expected %llu\n",
                             (unsigned long long)count, (unsigned long long)m.order_ref,
                             (unsigned long long)want.order_ref);
                bad = true;
            }
            if (count == 0 && o.ready_fd >= 0 && write(o.ready_fd, "r", 1) != 1) _exit(kThrew);
            ++count;
            if (count >= o.quit_after) _exit(kOk);
        }
        std::fprintf(stderr, "    consumer: stream ended without end marker after %llu of %llu messages\n",
                     (unsigned long long)count, (unsigned long long)o.expect_n);
        _exit(bad || count != o.expect_n ? kBadContent : kNoEndMarker);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "    consumer: threw: %s\n", e.what());
        _exit(kThrew);
    }
}

template <typename T>
th::Child spawn_producer(ProducerOpts o) {
    return th::Child::spawn([o] { producer_body<T>(o); });
}
template <typename T>
th::Child spawn_consumer(ConsumerOpts o) {
    return th::Child::spawn([o] { consumer_body<T>(o); });
}

void expect_exit(const char* file, int line, th::Child& c, const char* who, int timeout_ms, int want_code) {
    int status;
    if (!c.wait_for(timeout_ms, status))
        th::fail(file, line, std::string(who) + " still running after " + std::to_string(timeout_ms) + " ms (hang)");
    if (!(WIFEXITED(status) && WEXITSTATUS(status) == want_code))
        th::fail(file, line, std::string(who) + " finished with " + th::describe_status(status) +
                                 ", expected exit " + std::to_string(want_code));
}
#define EXPECT_EXIT(child, who, timeout_ms, code) expect_exit(__FILE__, __LINE__, child, who, timeout_ms, code)

template <typename P, typename C>
void happy_path(bool consumer_first, uint64_t n) {
    Cleanup cleanup;
    ProducerOpts po;
    po.n = n;
    ConsumerOpts co;
    co.expect_n = n;
    th::Child c, p;
    if (consumer_first) {
        c = spawn_consumer<C>(co);
        usleep(50'000);
        p = spawn_producer<P>(po);
    } else {
        p = spawn_producer<P>(po);
        usleep(300'000);
        c = spawn_consumer<C>(co);
    }
    EXPECT_EXIT(p, "producer", 20000, kOk);
    EXPECT_EXIT(c, "consumer", 5000, kOk);
}

} // namespace

// ----------------------------------------------------------------- happy path

TEST_T("ring: consumer-first, 200k messages arrive complete and in order", 30000) {
    happy_path<RingProducerTransport, RingConsumerTransport>(true, 200'000);
}

TEST_T("ring: producer-first, 200k messages arrive complete and in order", 30000) {
    happy_path<RingProducerTransport, RingConsumerTransport>(false, 200'000);
}

TEST_T("unix: consumer-first, 200k messages arrive complete and in order", 30000) {
    happy_path<UnixSocketProducerTransport, UnixSocketConsumerTransport>(true, 200'000);
}

TEST_T("unix: producer-first (connect retried until listening), 200k messages", 30000) {
    happy_path<UnixSocketProducerTransport, UnixSocketConsumerTransport>(false, 200'000);
}

TEST_T("ring: an empty stream isn't lost if the producer finishes before the consumer attaches", 30000) {
    happy_path<RingProducerTransport, RingConsumerTransport>(false, 0);
}

TEST_T("unix: producer killed mid-stream -> consumer sees EOF", 15000) {
    Cleanup cleanup;
    auto c = spawn_consumer<UnixSocketConsumerTransport>({.expect_n = 100});
    auto p = spawn_producer<UnixSocketProducerTransport>({.n = 100, .die_after_sending = true});
    int status;
    CHECK(p.wait_for(5000, status)); // reaped immediately
    EXPECT_EXIT(c, "consumer", 5000, kNoEndMarker);
}

// ------------------------------------------- lifecycle and failure handling

template <typename T>
bool copyable_v = std::is_copy_constructible_v<T> || std::is_copy_assignable_v<T>;

TEST("transports: none of the four are copyable (they own a mapping / fd)") {
    std::string copyable;
    if (copyable_v<RingProducerTransport>) copyable += " RingProducerTransport";
    if (copyable_v<RingConsumerTransport>) copyable += " RingConsumerTransport";
    if (copyable_v<UnixSocketProducerTransport>) copyable += " UnixSocketProducerTransport";
    if (copyable_v<UnixSocketConsumerTransport>) copyable += " UnixSocketConsumerTransport";
    CHECK_MSG(copyable.empty(), "copyable (a copy would double-unmap/close):" + copyable);
}

TEST_T("ring: producer leaving scope without close() removes the shm segment", 15000) {
    Cleanup cleanup;
    auto c = spawn_consumer<RingConsumerTransport>({.expect_n = 10});
    auto p = th::Child::spawn([] {
        try {
            RingProducerTransport t;
            t.open();
            for (uint64_t i = 0; i < 10; ++i) t.send(make_msg(0, i));
            t.send(end_msg());
            throw std::runtime_error("simulated error before close()");
        } catch (const std::runtime_error&) {
        }
        struct stat st;
        _exit(stat(SHM_PATH.c_str(), &st) == 0 ? 1 : 0);
    });
    EXPECT_EXIT(c, "consumer", 5000, kOk); // the transport really carried the stream
    int status;
    CHECK(p.wait_for(5000, status));
    CHECK_MSG(WIFEXITED(status), "producer " + th::describe_status(status));
    CHECK_MSG(WEXITSTATUS(status) == 0, "segment " + SHM_PATH + " leaked after the producer unwound on an exception");
}

TEST_T("unix: consumer leaving scope without close() removes the socket file", 15000) {
    Cleanup cleanup;
    auto p = spawn_producer<UnixSocketProducerTransport>({.n = 0});
    auto c = th::Child::spawn([] {
        try {
            UnixSocketConsumerTransport t;
            t.open();
            NormalisedMessage m;
            if (!t.recv(m) || m.msg_type != STREAM_END_MSG_TYPE) _exit(2);
            throw std::runtime_error("simulated error before close()");
        } catch (const std::runtime_error&) {
        }
        struct stat st;
        _exit(stat(UNIX_SOCKET_PATH, &st) == 0 ? 1 : 0);
    });
    int status;
    CHECK(c.wait_for(5000, status));
    CHECK_MSG(WIFEXITED(status), "consumer " + th::describe_status(status));
    CHECK_MSG(WEXITSTATUS(status) != 2, "setup: consumer didn't receive the producer's end marker");
    CHECK_MSG(WEXITSTATUS(status) == 0,
              std::string(UNIX_SOCKET_PATH) + " leaked after the consumer unwound on an exception");
}

TEST_T("ring: segment left by a crashed producer is not attached to", 30000) {
    // Attaching to the dead segment shows up as a content mismatch (its
    // order_refs start at 1e9) or as a hang.
    Cleanup cleanup;
    {
        auto dead = spawn_producer<RingProducerTransport>({.base = 1'000'000'000, .n = 100'000'000});
        usleep(200'000);
        dead.kill_and_reap();
    }
    struct stat st;
    CHECK_MSG(stat(SHM_PATH.c_str(), &st) == 0, "setup: the killed producer should have left its segment behind");

    auto c = spawn_consumer<RingConsumerTransport>({.base = 0, .expect_n = 5000});
    usleep(200'000);
    auto p = spawn_producer<RingProducerTransport>({.base = 0, .n = 5000});
    EXPECT_EXIT(c, "consumer", 5000, kOk);
    EXPECT_EXIT(p, "new producer", 5000, kOk);
}

TEST_T("ring: producer notices a dead consumer instead of spinning forever", 20000) {
    Cleanup cleanup;
    auto c = spawn_consumer<RingConsumerTransport>({.expect_n = 1'000'000, .quit_after = 10});
    auto p = spawn_producer<RingProducerTransport>({.n = 1'000'000});
    EXPECT_EXIT(c, "consumer", 5000, kOk); // exits after 10 messages; reaped now
    EXPECT_EXIT(p, "producer (ring full, consumer gone)", 5000, kThrew);
}

TEST_T("ring: consumer notices a dead producer instead of spinning forever", 20000) {
    Cleanup cleanup;
    auto c = spawn_consumer<RingConsumerTransport>({.expect_n = 100});
    auto p = spawn_producer<RingProducerTransport>({.n = 100, .die_after_sending = true});
    int status;
    CHECK(p.wait_for(5000, status)); // producer SIGKILLs itself after sending; reap it now
    // Expected: consumer drains the 100 messages, then recv() returns false.
    EXPECT_EXIT(c, "consumer (producer gone, no end marker)", 5000, kNoEndMarker);
}

TEST_T("unix: send to a consumer that has gone throws", 15000) {
    Cleanup cleanup;
    auto c = spawn_consumer<UnixSocketConsumerTransport>({.expect_n = 1'000'000, .quit_after = 10});
    auto p = th::Child::spawn([] {
        UnixSocketProducerTransport t;
        open_producer(t);
        try {
            for (uint64_t i = 0;; ++i) t.send(make_msg(0, i)); // until the consumer (gone after 10) makes it throw
        } catch (const std::exception&) {
            _exit(kThrew);
        }
    });
    EXPECT_EXIT(c, "consumer", 5000, kOk);
    EXPECT_EXIT(p, "producer", 5000, kThrew);
}

TEST_T("ring: consumer never touches a segment the producer hasn't sized yet (SIGBUS)", 15000) {
    // A producer between shm_open(O_CREAT) and ftruncate(): the object
    // exists with size 0. Mapping it and touching the ring faults.
    Cleanup cleanup;
    int fd = shm_open(RING_SHM_NAME, O_CREAT | O_RDWR, 0600);
    CHECK(fd >= 0);
    ::close(fd);
    auto c = spawn_consumer<RingConsumerTransport>({});
    int status;
    if (!c.wait_for(1000, status))
        return; // still waiting for a usable segment: correct
    CHECK_MSG(!WIFSIGNALED(status), "consumer crashed: " + th::describe_status(status));
}

TEST_T("ring: consumer rejects a segment no live producer created", 15000) {
    // Full-size segment of garbage, no producer process behind it.
    Cleanup cleanup;
    int fd = shm_open(RING_SHM_NAME, O_CREAT | O_RDWR, 0600);
    CHECK(fd >= 0);
    std::vector<uint8_t> junk(1 << 20);
    for (size_t i = 0; i < junk.size(); ++i) junk[i] = static_cast<uint8_t>((i * 37 + 11) & 0xff);
    CHECK(write(fd, junk.data(), junk.size()) == static_cast<ssize_t>(junk.size()));
    ::close(fd);

    auto c = th::Child::spawn([] {
        try {
            RingConsumerTransport t;
            t.open();
            NormalisedMessage m;
            if (t.recv(m)) _exit(kGotGarbage);
        } catch (const std::exception&) {
            _exit(kThrew); // refusing the segment loudly is fine
        }
    });
    int status;
    if (!c.wait_for(1000, status))
        return; // still waiting for a real producer: correct
    CHECK_MSG(WIFEXITED(status) && WEXITSTATUS(status) == kThrew,
              "consumer attached to a garbage segment: " +
                  (WIFEXITED(status) && WEXITSTATUS(status) == kGotGarbage ? std::string("it returned a message")
                                                                          : th::describe_status(status)));
}

TEST_T("ring: a second consumer cannot attach to an already-claimed ring", 30000) {
    Cleanup cleanup;
    constexpr uint64_t N = 200'000;
    auto c1 = spawn_consumer<RingConsumerTransport>({.expect_n = N});
    auto c2 = spawn_consumer<RingConsumerTransport>({.expect_n = N});
    usleep(50'000);
    auto p = spawn_producer<RingProducerTransport>({.n = N});
    EXPECT_EXIT(p, "producer", 15000, kOk);

    // One consumer gets the whole stream (kOk). The other must not have
    // taken any of it: it keeps waiting or refuses with an error (kThrew).
    th::Child* cs[2] = {&c1, &c2};
    bool done[2] = {false, false};
    int st[2];
    auto exited_with = [&](int i, int code) { return done[i] && WIFEXITED(st[i]) && WEXITSTATUS(st[i]) == code; };
    for (int ms = 0; ms < 5000 && !exited_with(0, kOk) && !exited_with(1, kOk); ++ms) {
        for (int i = 0; i < 2; ++i)
            if (!done[i]) done[i] = cs[i]->wait_for(0, st[i]);
        usleep(1000);
    }
    int w = exited_with(0, kOk) ? 0 : exited_with(1, kOk) ? 1 : -1;
    auto report = [&](int i) { return done[i] ? th::describe_status(st[i]) : std::string("running"); };
    CHECK_MSG(w >= 0, "no consumer received the whole stream (consumer 1: " + report(0) +
                          ", consumer 2: " + report(1) + ")");
    int l = 1 - w;
    if (!done[l]) done[l] = cs[l]->wait_for(200, st[l]);
    CHECK_MSG(!done[l] || exited_with(l, kThrew), "second consumer also consumed from the ring: " + report(l));
}

TEST_T("ring: nothing is timestamped before a consumer can read it", 15000) {
    // Consumer starts 500 ms after the producer; a message stamped before
    // it attached would show ~500 ms of latency.
    Cleanup cleanup;
    // n exceeds the ring's capacity, so a producer that publishes early is
    // still blocked (segment intact) when the consumer arrives.
    auto p = spawn_producer<RingProducerTransport>({.n = 5000});
    usleep(500'000);
    auto c = spawn_consumer<RingConsumerTransport>({.expect_n = 5000, .max_first_latency_ms = 250});
    EXPECT_EXIT(c, "consumer", 5000, kOk);
    EXPECT_EXIT(p, "producer", 5000, kOk);
}

TEST_T("ring: shm segment is not accessible to other users", 15000) {
    // Checked before any consumer exists, while the segment is necessarily
    // present (n exceeds the ring, so even an early-publishing producer waits).
    Cleanup cleanup;
    auto p = spawn_producer<RingProducerTransport>({.n = 2000, .umask0 = true});
    struct stat st;
    bool exists = false;
    for (int ms = 0; ms < 5000 && !(exists = stat(SHM_PATH.c_str(), &st) == 0); ++ms) usleep(1000);
    CHECK_MSG(exists, "producer never created " + SHM_PATH);
    char mode[8];
    std::snprintf(mode, sizeof(mode), "%03o", st.st_mode & 0777);
    CHECK_MSG((st.st_mode & 077) == 0, std::string("segment mode is ") + mode + ", accessible to other users");
    auto c = spawn_consumer<RingConsumerTransport>({.expect_n = 2000});
    EXPECT_EXIT(p, "producer", 5000, kOk);
    EXPECT_EXIT(c, "consumer", 5000, kOk);
}

TEST_T("unix: a signal during recv() is not mistaken for end of stream", 15000) {
    Cleanup cleanup;
    int ready[2];
    CHECK(pipe(ready) == 0);
    auto c = spawn_consumer<UnixSocketConsumerTransport>(
        {.expect_n = 1000, .eintr_handler = true, .ready_fd = ready[1]});
    auto p = spawn_producer<UnixSocketProducerTransport>({.n = 1000, .pause_after_first_ms = 500});
    // Once the consumer has the first message it's blocked in recv() for the
    // rest of the producer's pause: signal it then.
    pollfd pfd{ready[0], POLLIN, 0};
    CHECK_MSG(poll(&pfd, 1, 5000) == 1, "consumer never received the first message");
    usleep(50'000);
    ::kill(c.pid, SIGUSR1);
    EXPECT_EXIT(c, "consumer", 5000, kOk);
    EXPECT_EXIT(p, "producer", 5000, kOk);
}

int main() {
    // Failing tests _exit() without unwinding, so clear the global IPC
    // names around the whole run as well as around each test.
    cleanup_ipc_names();
    int rc = th::run_all();
    cleanup_ipc_names();
    return rc;
}
