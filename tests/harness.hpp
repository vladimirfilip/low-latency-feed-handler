#pragma once

// Minimal self-contained test harness (no gtest/Catch2 dependency).
//
// Every test runs in its own forked child, in its own process group, under a
// wall-clock deadline, so segfaults, SIGBUS, stack smashing and processes
// that spin forever are reported as ordinary test failures instead of
// taking the whole run down with them.
//
// Tests are registered with TEST(name), or TEST_T(name, timeout_ms) for a
// non-default deadline. The binary runs every registered test and exits
// non-zero if any failed.

#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>
#include <type_traits>
#include <vector>

#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

namespace th {

struct TestCase {
    const char* name;
    int timeout_ms;
    void (*fn)();
};

inline std::vector<TestCase>& registry() {
    static std::vector<TestCase> tests;
    return tests;
}

struct Registrar {
    Registrar(const char* name, int timeout_ms, void (*fn)()) {
        registry().push_back({name, timeout_ms, fn});
    }
};

// Exit code a test child uses to report an assertion failure (as opposed to
// dying from a signal or hitting its deadline).
constexpr int kAssertFailed = 101;

[[noreturn]] inline void fail(const char* file, int line, const std::string& msg) {
    std::fprintf(stderr, "    %s:%d: %s\n", file, line, msg.c_str());
    std::fflush(stderr);
    _exit(kAssertFailed);
}

inline std::string show(char c) {
    return std::isprint(static_cast<unsigned char>(c)) ? std::string("'") + c + "'" : std::to_string(int(c));
}

template <typename A, typename B>
std::string describe(const A& a, const B& b) {
    if constexpr (std::is_same_v<A, char> && std::is_same_v<B, char>) {
        return "got " + show(a) + ", expected " + show(b);
    } else if constexpr (std::is_arithmetic_v<A> && std::is_arithmetic_v<B>) {
        return "got " + std::to_string(a) + ", expected " + std::to_string(b);
    } else if constexpr (std::is_convertible_v<A, std::string> && std::is_convertible_v<B, std::string>) {
        return "got \"" + std::string(a) + "\", expected \"" + std::string(b) + "\"";
    } else {
        return "values differ";
    }
}

inline void flush_all() {
    std::fflush(stdout);
    std::fflush(stderr);
}

// A forked child process running `body` (then _exit(0)). Killed and reaped
// on destruction.
struct Child {
    pid_t pid = -1;

    Child() = default;
    Child(const Child&) = delete;
    Child& operator=(const Child&) = delete;
    Child(Child&& o) noexcept : pid(o.pid) { o.pid = -1; }
    Child& operator=(Child&& o) noexcept {
        if (this != &o) {
            kill_and_reap();
            pid = o.pid;
            o.pid = -1;
        }
        return *this;
    }

    template <typename F>
    static Child spawn(F body) {
        flush_all();
        Child c;
        c.pid = fork();
        if (c.pid == 0) {
            body();
            flush_all();
            _exit(0);
        }
        if (c.pid < 0) {
            std::perror("fork");
            _exit(kAssertFailed);
        }
        return c;
    }

    // Waits up to timeout_ms. Returns true and fills `status` if it exited.
    bool wait_for(int timeout_ms, int& status) {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
        for (;;) {
            pid_t r = waitpid(pid, &status, WNOHANG);
            if (r == pid) {
                pid = -1;
                return true;
            }
            if (std::chrono::steady_clock::now() >= deadline)
                return false;
            usleep(1000);
        }
    }

    // SIGKILLs (if still running) and reaps. Always reap: a zombie still
    // passes kill(pid, 0), so an unreaped dead peer looks alive to any
    // liveness check the code under test does.
    void kill_and_reap() {
        if (pid <= 0)
            return;
        ::kill(pid, SIGKILL);
        int status;
        waitpid(pid, &status, 0);
        pid = -1;
    }

    ~Child() { kill_and_reap(); }
};

inline std::string describe_status(int status) {
    if (WIFEXITED(status))
        return "exit " + std::to_string(WEXITSTATUS(status));
    if (WIFSIGNALED(status))
        return std::string("killed by ") + strsignal(WTERMSIG(status));
    return "status " + std::to_string(status);
}

inline int run_all() {
    int passed = 0, failed = 0;
    std::vector<std::string> failures;
    for (auto& t : registry()) {
        std::printf("[ RUN  ] %s\n", t.name);
        flush_all();

        auto start = std::chrono::steady_clock::now();
        pid_t pid = fork();
        if (pid < 0) {
            std::perror("fork");
            std::printf("[ FAIL ] %s — could not fork\n", t.name);
            ++failed;
            failures.push_back(std::string(t.name) + " — could not fork");
            continue;
        }
        if (pid == 0) {
            setpgid(0, 0); // own group, so a timeout also kills anything it forked
            t.fn();
            flush_all();
            _exit(0);
        }
        setpgid(pid, pid);

        int status = 0;
        bool exited = false;
        auto deadline = start + std::chrono::milliseconds(t.timeout_ms);
        while (std::chrono::steady_clock::now() < deadline) {
            if (waitpid(pid, &status, WNOHANG) == pid) {
                exited = true;
                break;
            }
            usleep(1000);
        }
        if (!exited) {
            ::kill(-pid, SIGKILL);
            waitpid(pid, &status, 0);
        }
        // Kill any grandchildren the test left behind even if it exited.
        ::kill(-pid, SIGKILL);

        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - start).count();
        bool ok = exited && WIFEXITED(status) && WEXITSTATUS(status) == 0;
        std::string why;
        if (!exited) why = "timed out after " + std::to_string(t.timeout_ms) + " ms (hang)";
        else if (!ok && !(WIFEXITED(status) && WEXITSTATUS(status) == kAssertFailed))
            why = "crashed: " + describe_status(status);
        else if (!ok) why = "assertion failed";

        if (ok) {
            std::printf("[  OK  ] %s (%lld ms)\n", t.name, static_cast<long long>(ms));
            ++passed;
        } else {
            std::printf("[ FAIL ] %s — %s\n", t.name, why.c_str());
            ++failed;
            failures.push_back(std::string(t.name) + " — " + why);
        }
        flush_all();
    }

    std::printf("\n%d passed, %d failed\n", passed, failed);
    for (auto& f : failures) std::printf("  FAILED: %s\n", f.c_str());
    return failed == 0 ? 0 : 1;
}

} // namespace th

#define TH_CAT2(a, b) a##b
#define TH_CAT(a, b) TH_CAT2(a, b)

#define TEST_T(name, timeout_ms)                                                    \
    static void TH_CAT(th_test_, __LINE__)();                                        \
    static th::Registrar TH_CAT(th_reg_, __LINE__)(name, timeout_ms,                 \
                                                   TH_CAT(th_test_, __LINE__));      \
    static void TH_CAT(th_test_, __LINE__)()
#define TEST(name) TEST_T(name, 10000)

#define CHECK(cond)                                                                 \
    do {                                                                            \
        if (!(cond)) th::fail(__FILE__, __LINE__, "CHECK(" #cond ") failed");       \
    } while (0)

#define CHECK_MSG(cond, msg)                                                        \
    do {                                                                            \
        if (!(cond)) th::fail(__FILE__, __LINE__, std::string(msg));                \
    } while (0)

#define CHECK_EQ(a, b)                                                              \
    do {                                                                            \
        const auto& th_a = (a);                                                     \
        const auto& th_b = (b);                                                     \
        if (!(th_a == th_b))                                                        \
            th::fail(__FILE__, __LINE__,                                            \
                     "CHECK_EQ(" #a ", " #b "): " + th::describe(th_a, th_b));      \
    } while (0)

#define TEST_MAIN()                                                                 \
    int main() { return th::run_all(); }
