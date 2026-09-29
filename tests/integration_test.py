#!/usr/bin/env python3
"""Process-level tests: drive the real binaries end to end.

Covers feed_handler, replay, and the four IPC producer/consumer binaries on
synthetic ITCH data, what happens when one side of an IPC pair is killed,
bad inputs, bench/run_ipc_comparison.sh, and build hygiene checks.

Every process runs under a deadline and is SIGKILLed and reaped if it
overruns, so a hang shows up as a failure rather than wedging the run. The
IPC names (/dev/shm/feed_handler_norm_ring, /tmp/feed_handler_norm.sock) are
global, so tests run serially and clear them before and after each test.

Run via tests/run_tests.sh, which builds BIN_DIR and sets CXX/FH_RELEASE_FLAGS.
"""

import argparse
import os
import re
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = None  # set in main()
SHM_PATH = "/dev/shm/feed_handler_norm_ring"
SOCK_PATH = "/tmp/feed_handler_norm.sock"
# Set by run_tests.sh, so the build checks use the same compiler and flags.
if "CXX" not in os.environ or "FH_RELEASE_FLAGS" not in os.environ:
    sys.exit("run this via tests/run_tests.sh")
CXX = os.environ["CXX"]
RELEASE_FLAGS = os.environ["FH_RELEASE_FLAGS"].split()


# ------------------------------------------------------------- ITCH records

def ts48(ns):
    return ns.to_bytes(6, "big")


def hdr(t, locate, ts):
    return t.encode() + struct.pack(">HH", locate, 1) + ts48(ts)


def rec_S(ts):
    return hdr("S", 0, ts) + b"O"


def rec_R(locate, ts, sym):
    return (hdr("R", locate, ts) + sym.ljust(8).encode() + b"QN" + struct.pack(">I", 100)
            + b"NCZ PNN1N" + struct.pack(">I", 0) + b"N")


def rec_A(locate, ts, ref, shares=100, price=10000):
    return hdr("A", locate, ts) + struct.pack(">Q", ref) + b"B" + struct.pack(">I", shares) + b"AAPL    " + struct.pack(">I", price)


def rec_F(locate, ts, ref):
    return hdr("F", locate, ts) + struct.pack(">Q", ref) + b"S" + struct.pack(">I", 5) + b"MSFT    " + struct.pack(">I", 7) + b"GSCO"


def rec_E(locate, ts, ref):
    return hdr("E", locate, ts) + struct.pack(">QIQ", ref, 10, 1)


def rec_X(locate, ts, ref):
    return hdr("X", locate, ts) + struct.pack(">QI", ref, 1)


def rec_D(locate, ts, ref):
    return hdr("D", locate, ts) + struct.pack(">Q", ref)


def rec_P(locate, ts, ref):
    return hdr("P", locate, ts) + struct.pack(">Q", ref) + b"B" + struct.pack(">I", 3) + b"AAPL    " + struct.pack(">IQ", 9, 2)


def rec_H(locate, ts):
    return hdr("H", locate, ts) + b"AAPL    " + b"T" + b" " + b"    "


NORMALISED = set("SRAFEXDP")


def mixed_records(n):
    """n records cycling through normalised types plus one that is never
    normalised (H). U and C are left out: whether they're normalised is
    covered by the unit tests, and shouldn't change the counts here. The wire
    timestamp advances by 0.5 ms every 3 records, so replay forms real
    batches and a --paced replay of 3000 records takes ~0.5 s."""
    makers = [
        lambda i, ts: rec_A(1, ts, i), lambda i, ts: rec_F(2, ts, i), lambda i, ts: rec_E(1, ts, i),
        lambda i, ts: rec_X(1, ts, i), lambda i, ts: rec_D(1, ts, i), lambda i, ts: rec_P(3, ts, i),
        lambda i, ts: rec_H(1, ts), lambda i, ts: rec_R(4, ts, "IBM"),
    ]
    recs = [rec_S(34_200_000_000_000)]
    for i in range(1, n):
        ts = 34_200_000_000_000 + (i // 3) * 500_000
        recs.append(makers[i % len(makers)](i, ts))
    return recs


def frame(recs):
    return b"".join(struct.pack(">H", len(r)) + r for r in recs)


def count_normalised(recs):
    return sum(1 for r in recs if chr(r[0]) in NORMALISED)


# ------------------------------------------------------------ process utils

def clean_ipc_names():
    for p in (SHM_PATH, SOCK_PATH):
        try:
            os.unlink(p)
        except FileNotFoundError:
            pass


class Proc:
    """A child process with a deadline. Output goes to a temp file so a
    blocked pipe can never stall it."""

    live = []

    def __init__(self, args):
        self.out = tempfile.TemporaryFile()
        self.p = subprocess.Popen(args, stdout=self.out, stderr=subprocess.STDOUT, cwd=ROOT)
        Proc.live.append(self)

    def wait(self, timeout):
        """Returns the exit code, or None if it's still running at the deadline."""
        try:
            return self.p.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            return None

    def kill(self):
        # Always reap straight away: a zombie still passes kill(pid, 0), so
        # the other side would think this process is alive.
        if self.p.poll() is None:
            self.p.kill()
        self.p.wait()

    def output(self):
        self.out.seek(0)
        return self.out.read().decode(errors="replace")

    @classmethod
    def kill_all(cls):
        for pr in cls.live:
            pr.kill()
            pr.out.close()
        cls.live = []


def run(args, timeout=30):
    pr = Proc(args)
    rc = pr.wait(timeout)
    if rc is None:
        pr.kill()
    return rc, pr.output()


def describe_rc(rc):
    if rc is None:
        return "still running at deadline (hang)"
    if rc < 0:
        return "killed by %s" % signal.Signals(-rc).name
    return "exit %d" % rc


def metric(out, name):
    m = re.search(r"^%s: ([0-9.]+)" % re.escape(name), out, re.M)
    return float(m.group(1)) if m else None


def published(out):
    m = re.search(r"published (\d+)", out)
    return int(m.group(1)) if m else None


def free_udp_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class UdpFeed:
    """Sends ITCH records to a --udp=PORT process as MoldUDP64 datagrams."""

    def __init__(self, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.dest = ("127.0.0.1", port)
        self.seq = 1

    def send(self, recs, per_datagram=50):
        for i in range(0, len(recs), per_datagram):
            batch = recs[i:i + per_datagram]
            d = b"TEST000001" + struct.pack(">QH", self.seq, len(batch)) + frame(batch)
            self.sock.sendto(d, self.dest)
            self.seq += len(batch)
            time.sleep(0.001)  # stay well inside the receiver's socket buffer

    def close(self):
        self.sock.close()


class Base(unittest.TestCase):
    tmp = None

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp(prefix="fh_itest_")
        cls.small_recs = mixed_records(3000)
        cls.small = cls.write("small.itch", frame(cls.small_recs))

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    @classmethod
    def write(cls, name, data):
        path = os.path.join(cls.tmp, name)
        with open(path, "wb") as f:
            f.write(data)
        return path

    def setUp(self):
        clean_ipc_names()

    def tearDown(self):
        Proc.kill_all()
        clean_ipc_names()

    def bin(self, name):
        return os.path.join(BIN, name)


# ================================================================ feed_handler

class FeedHandler(Base):
    def test_file_mode_times_every_record(self):
        csv = os.path.join(self.tmp, "fh.csv")
        rc, out = run([self.bin("feed_handler"), "--data=" + self.small, csv])
        self.assertEqual(rc, 0, out)
        self.assertEqual(metric(out, "count"), len(self.small_recs), out)
        for k in ("p50", "p95", "p99", "p99.9", "max"):
            self.assertIsNotNone(metric(out, k), "missing %s in output:\n%s" % (k, out))
        with open(csv) as f:
            lines = f.read().splitlines()
        self.assertEqual(lines[0], "bucket_lower_ns,bucket_upper_ns,count")
        self.assertEqual(sum(int(l.split(",")[2]) for l in lines[1:]), len(self.small_recs))

    def test_empty_file(self):
        empty = self.write("empty.itch", b"")
        rc, out = run([self.bin("feed_handler"), "--data=" + empty])
        self.assertEqual(rc, 0, out)
        self.assertEqual(metric(out, "count"), 0)

    def test_missing_data_file_is_an_error_not_a_crash(self):
        rc, out = run([self.bin("feed_handler"), "--data=/nonexistent/x.itch"])
        self.assertTrue(rc is not None and rc > 0,
                        "expected a clean non-zero exit, got %s\n%s" % (describe_rc(rc), out))

    def test_timed_region_contains_the_dispatch(self):
        # main() times `dispatch(record)` between two rdtscp reads. If the
        # result isn't consumed the compiler deletes the call at -O3 and the
        # benchmark times nothing. dispatch() switches on record[0], so the
        # timed region must contain at least one branch or call.
        dis = subprocess.run(["objdump", "-d", "--no-show-raw-insn", "-C", self.bin("feed_handler")],
                             capture_output=True, text=True, check=True).stdout
        main = re.search(r"<main>:\n(.*?)\n\n", dis, re.S)
        self.assertIsNotNone(main, "couldn't find main in the disassembly")
        insns = [re.sub(r"^(notrack|bnd)\s+", "", l.split("\t", 1)[-1].strip())
                 for l in main.group(1).splitlines() if "\t" in l]
        stamps = [i for i, ins in enumerate(insns) if ins.startswith("rdtscp")]
        # Exactly one start/stop pair is expected in main(); anything else
        # (e.g. calibrate() inlined) means the timed region can't be
        # identified reliably, and the check would be meaningless.
        self.assertEqual(len(stamps), 2, "can't identify the timed region: %d rdtscp in main()" % len(stamps))
        region = insns[stamps[0] + 1:stamps[1]]
        self.assertTrue(any(re.match(r"(j[a-z]+|call)\b", ins) for ins in region),
                        "timed region contains no branch or call, so dispatch() was optimised "
                        "away:\n  " + " | ".join(region))


# ================================================================ IPC binaries

class Ipc(Base):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # ~70 MB, for a large end-to-end run.
        one = frame([rec_A(1, 34_200_000_000_000, 7)])
        cls.big_n = 2_000_000
        cls.big = cls.write("big.itch", one * cls.big_n)

    def pair(self, variant, data, consumer_first=True):
        csv = os.path.join(self.tmp, "h_%s.csv" % variant)
        cons = [self.bin("ipc_consumer_" + variant), csv]
        prod = [self.bin("ipc_producer_" + variant), "--data=" + data]
        if consumer_first:
            c = Proc(cons)
            time.sleep(0.05)
            p = Proc(prod)
        else:
            p = Proc(prod)
            time.sleep(0.05)
            c = Proc(cons)
        return p, c, csv

    def check_complete_run(self, variant, consumer_first):
        p, c, csv = self.pair(variant, self.small, consumer_first)
        prc, crc = p.wait(20), c.wait(10)
        self.assertEqual(prc, 0, "producer %s\n%s" % (describe_rc(prc), p.output()))
        self.assertEqual(crc, 0, "consumer %s\n%s" % (describe_rc(crc), c.output()))
        want = count_normalised(self.small_recs)
        self.assertEqual(published(p.output()), want, p.output())
        self.assertEqual(metric(c.output(), "count"), want, c.output())
        self.assertIsNotNone(metric(c.output(), "throughput"), c.output())
        # Latencies come from the producer's ingest stamp; a missing stamp
        # would show up as ~system uptime.
        self.assertLess(metric(c.output(), "max"), 10e9, c.output())
        with open(csv) as f:
            rows = f.read().splitlines()[1:]
        self.assertEqual(sum(int(r.split(",")[2]) for r in rows), want)
        self.assertFalse(os.path.exists(SHM_PATH if variant == "ring" else SOCK_PATH), "IPC name left behind")

    def test_ring_consumer_first(self):
        self.check_complete_run("ring", True)

    def test_ring_producer_first(self):
        self.check_complete_run("ring", False)

    def test_unix_consumer_first(self):
        self.check_complete_run("unix", True)

    def test_unix_producer_first(self):
        self.check_complete_run("unix", False)

    def test_ring_large_stream(self):
        p, c, _ = self.pair("ring", self.big)
        prc, crc = p.wait(60), c.wait(10)
        self.assertEqual((prc, crc), (0, 0), p.output() + c.output())
        self.assertEqual(metric(c.output(), "count"), self.big_n)

    def check_producer_rejects_missing_data_file(self, variant):
        # "published 0" with exit 0 would pass the bench script's count check.
        Proc([self.bin("ipc_consumer_" + variant)])
        rc, out = run([self.bin("ipc_producer_" + variant), "--data=/nonexistent/x.itch"], timeout=10)
        self.assertTrue(rc is not None and rc > 0,
                        "%s producer with a missing data file: %s\n%s" % (variant, describe_rc(rc), out))

    def test_ring_producer_missing_data_file_fails(self):
        self.check_producer_rejects_missing_data_file("ring")

    def test_unix_producer_missing_data_file_fails(self):
        self.check_producer_rejects_missing_data_file("unix")

    # ---- one side of the pair dies. The producer runs in --udp mode, fed by
    # the test, so it's still mid-stream when one side is killed.

    def assert_running(self, *procs):
        # Both sides must be up and mid-stream, or a start-up failure could
        # pass for "noticed its peer died".
        for pr in procs:
            self.assertIsNone(pr.p.poll(), "exited before the kill: %s\n%s" % (describe_rc(pr.p.returncode), pr.output()))

    def udp_pair(self, variant):
        port = free_udp_port()
        c = Proc([self.bin("ipc_consumer_" + variant)])
        p = Proc([self.bin("ipc_producer_" + variant), "--udp=%d" % port])
        feed = UdpFeed(port)
        self.addCleanup(feed.close)
        time.sleep(0.3)  # both sides up and connected
        return p, c, feed

    def check_producer_fails_when_consumer_killed(self, variant):
        p, c, feed = self.udp_pair(variant)
        feed.send([rec_D(1, 1, i) for i in range(100)])
        time.sleep(0.2)
        self.assert_running(p, c)
        c.kill()
        # More than the ring holds, so a producer that doesn't notice the
        # dead consumer is left blocked in send().
        feed.send([rec_D(1, 1, i) for i in range(5000)])
        rc = p.wait(5)
        self.assertTrue(rc is not None and rc > 0,
                        "%s producer after its consumer died: %s (want a clean non-zero exit)\n%s"
                        % (variant, describe_rc(rc), p.output()))

    def test_ring_producer_fails_when_consumer_killed(self):
        self.check_producer_fails_when_consumer_killed("ring")

    def test_unix_producer_fails_when_consumer_killed(self):
        self.check_producer_fails_when_consumer_killed("unix")

    def check_consumer_fails_when_producer_killed(self, variant):
        p, c, feed = self.udp_pair(variant)
        feed.send([rec_D(1, 1, i) for i in range(100)])
        time.sleep(0.2)
        self.assert_running(p, c)
        p.kill()
        rc = c.wait(5)
        # Partial results must not look like a successful run.
        self.assertTrue(rc is not None and rc > 0,
                        "%s consumer after its producer died: %s (want a non-zero exit)\n%s"
                        % (variant, describe_rc(rc), c.output()))

    def test_ring_consumer_fails_when_producer_killed(self):
        self.check_consumer_fails_when_producer_killed("ring")

    def test_unix_consumer_fails_when_producer_killed(self):
        self.check_consumer_fails_when_producer_killed("unix")


# ====================================================================== replay

def parse_mold(d):
    session, seq, count = d[:10], int.from_bytes(d[10:18], "big"), int.from_bytes(d[18:20], "big")
    recs, off = [], 20
    if count not in (0, 0xFFFF):
        for _ in range(count):
            n = int.from_bytes(d[off:off + 2], "big")
            recs.append(d[off + 2:off + 2 + n])
            off += 2 + n
    return session, seq, count, recs, off


class Replay(Base):
    def capture(self, data):
        rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        rx.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8 << 20)
        rx.bind(("127.0.0.1", 0))
        rx.settimeout(5)
        port = rx.getsockname()[1]
        pr = Proc([self.bin("replay"), "--port=%d" % port, "--data=" + data])
        grams = []
        try:
            while True:
                d = rx.recv(70000)
                grams.append(d)
                if d[18:20] == b"\xff\xff":
                    break
        except socket.timeout:
            pass
        rx.close()
        return pr.wait(10), pr.output(), grams

    def test_framing_sequence_and_batch_limits(self):
        # Max-rate; ~100 datagrams, so the capture socket's buffer can't
        # overflow. The 500 same-timestamp records fill datagrams to the byte
        # cap. (The 200-message cap is unreachable with real records: at
        # least 13 framed bytes each.)
        sent = self.small_recs[:300] + [rec_D(1, 34_300_000_000_000, i) for i in range(500)]
        rc, out, grams = self.capture(self.write("framing.itch", frame(sent)))
        self.assertEqual(rc, 0, out)
        self.assertTrue(grams and grams[-1][18:20] == b"\xff\xff", "no end-of-session datagram")
        expected_seq, got = 1, []
        for d in grams[:-1]:
            session, seq, count, recs, used = parse_mold(d)
            self.assertEqual(session, b"REPLAY0001")
            self.assertEqual(seq, expected_seq, "sequence numbers must be contiguous")
            self.assertGreaterEqual(count, 1)
            self.assertLessEqual(len(d), 20 + 1400)
            self.assertEqual(used, len(d), "datagram has trailing bytes")
            expected_seq += count
            got += recs
        self.assertGreater(max(len(d) for d in grams), 20 + 1400 - 21, "byte cap never reached")
        _, end_seq, _, _, _ = parse_mold(grams[-1])
        self.assertEqual(end_seq, expected_seq)
        self.assertEqual(got, sent, "replayed records differ from the file")
        self.assertIn("sent %d records" % len(sent), out)

    # Receivers are fed --paced: at max rate, loopback UDP can overrun their
    # socket buffer (capped by net.core.rmem_max) and drop datagrams.

    def paced_replay(self, port):
        start = time.monotonic()
        rc, out = run([self.bin("replay"), "--port=%d" % port, "--data=" + self.small, "--paced"])
        elapsed = time.monotonic() - start
        self.assertEqual(rc, 0, out)
        self.assertIn("(paced)", out)
        # 1000 distinct timestamps 0.5 ms apart -> ~0.5 s of wire time
        self.assertGreaterEqual(elapsed, 0.45, "paced replay didn't follow the wire timestamps")
        self.assertLess(elapsed, 5.0)

    def test_paced_replay_into_feed_handler(self):
        port = free_udp_port()
        fh = Proc([self.bin("feed_handler"), "--udp=%d" % port])
        time.sleep(0.3)  # feed_handler calibrates (~50 ms) after binding
        self.paced_replay(port)
        frc = fh.wait(10)
        self.assertEqual(frc, 0, "feed_handler: %s\n%s" % (describe_rc(frc), fh.output()))
        self.assertEqual(metric(fh.output(), "count"), len(self.small_recs), fh.output())

    def test_paced_replay_into_ipc_producer(self):
        port = free_udp_port()
        c = Proc([self.bin("ipc_consumer_ring")])
        p = Proc([self.bin("ipc_producer_ring"), "--udp=%d" % port])
        time.sleep(0.3)
        self.paced_replay(port)
        prc, crc = p.wait(10), c.wait(10)
        self.assertEqual((prc, crc), (0, 0), p.output() + c.output())
        self.assertEqual(metric(c.output(), "count"), count_normalised(self.small_recs), c.output())

    def test_invalid_host_is_rejected(self):
        rc, out = run([self.bin("replay"), "--host=not-an-ip", "--data=" + self.small])
        self.assertEqual(rc, 1)
        self.assertIn("invalid --host", out)

    def test_oversized_record_does_not_smash_the_stack(self):
        # Larger than any datagram and than replay's packet buffer. Replay
        # must either skip it and still send the next record, or fail cleanly.
        big = b"Z" + b"\x00" * 6999
        after = rec_D(1, 1, 1)
        rc, out, grams = self.capture(self.write("oversize.itch", frame([big, after])))
        self.assertTrue(rc is not None and rc >= 0,
                        "replay crashed on a 7000-byte record: %s" % describe_rc(rc))
        delivered = any(after in parse_mold(d)[3] for d in grams)
        self.assertTrue(rc > 0 or delivered,
                        "replay exited 0 but never sent the record after the oversized one\n" + out)


# ============================================================= bench script

class BenchScript(Base):
    """Runs a throwaway copy of bench/run_ipc_comparison.sh. It derives ROOT
    from its own path, so it writes to the copy's build/ and bench/results/,
    never the committed ones. cmake and taskset are stubbed out; the
    prebuilt binaries are copied in."""

    def setUp(self):
        super().setUp()
        self.git_bench_before = self.git_bench_status()
        self.root = tempfile.mkdtemp(prefix="fh_bench_", dir=self.tmp)
        os.makedirs(os.path.join(self.root, "bench"))
        shutil.copy(os.path.join(ROOT, "bench", "run_ipc_comparison.sh"), os.path.join(self.root, "bench"))
        os.makedirs(os.path.join(self.root, "build"))
        for v in ("ring", "unix"):
            for side in ("producer", "consumer"):
                shutil.copy(self.bin("ipc_%s_%s" % (side, v)), os.path.join(self.root, "build"))
        stub = os.path.join(self.root, "stub")
        os.makedirs(stub)
        for name, body in (("cmake", "exit 0"), ("taskset", 'shift 2; exec "$@"')):  # taskset -c CORE cmd...
            with open(os.path.join(stub, name), "w") as f:
                f.write("#!/bin/sh\n%s\n" % body)
            os.chmod(os.path.join(stub, name), 0o755)
        # Fixed cores so the script's core-count check passes on any machine.
        self.env = dict(os.environ, PATH=stub + os.pathsep + os.environ["PATH"],
                        BENCH_PRODUCER_CORE="1", BENCH_CONSUMER_CORE="0")

    def tearDown(self):
        for pid in self.leftover_processes():
            try:
                os.kill(pid, 9)
            except ProcessLookupError:
                pass
        super().tearDown()
        self.assertEqual(self.git_bench_status(), self.git_bench_before,
                         "bench script test modified files under the repo's bench/")

    def git_bench_status(self):
        return subprocess.run(["git", "status", "--porcelain", "bench/"], cwd=ROOT,
                              capture_output=True, text=True).stdout

    def leftover_processes(self):
        """PIDs of any process still running one of this copy's binaries."""
        build = os.path.join(self.root, "build")
        pids = []
        for d in os.listdir("/proc"):
            if d.isdigit():
                try:
                    if os.readlink("/proc/%s/exe" % d).startswith(build):
                        pids.append(int(d))
                except OSError:
                    pass
        return pids

    def provide_data(self):
        # Producers run without --data, so they read the default relative
        # path from the script's working directory.
        os.makedirs(os.path.join(self.root, "data"))
        os.symlink(self.small, os.path.join(self.root, "data", "03272019.NASDAQ_ITCH50.200MB"))

    def run_script(self, timeout=60):
        pr = subprocess.Popen(["bash", os.path.join(self.root, "bench", "run_ipc_comparison.sh")],
                              cwd=self.root, env=self.env, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, start_new_session=True)
        try:
            out, _ = pr.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(pr.pid, 9)
            out, _ = pr.communicate()
            return None, out.decode(errors="replace")
        return pr.returncode, out.decode(errors="replace")

    def test_writes_summary_with_matching_counts(self):
        self.provide_data()
        rc, out = self.run_script()
        self.assertEqual(rc, 0, out)
        self.assertNotIn("mismatch", out)
        results = os.path.join(self.root, "bench", "results")
        with open(os.path.join(results, "ipc_summary.csv")) as f:
            rows = [l.split(",") for l in f.read().splitlines()]
        self.assertEqual(rows[0][:2], ["variant", "count"])
        want = count_normalised(self.small_recs)
        self.assertEqual({r[0]: int(r[1]) for r in rows[1:]}, {"ring": want, "unix": want})
        for v in ("ring", "unix"):
            self.assertTrue(os.path.exists(os.path.join(results, "ipc_histogram_%s.csv" % v)))

    def test_missing_data_file_fails_the_run(self):
        rc, out = self.run_script(timeout=20)
        self.assertTrue(rc is not None and rc > 0,
                        "bench script with no data file: %s\n%s" % (describe_rc(rc), out))
        # It must fail cleanly on the first variant, not via a crash later on.
        for crash in ("Segmentation fault", "Aborted", "core dumped"):
            self.assertNotIn(crash, out)
        self.assertNotIn("Running AF_UNIX", out, "script carried on past a run with no data")

    def test_failed_producer_leaves_no_consumer_running(self):
        self.provide_data()
        with open(os.path.join(self.root, "build", "ipc_producer_ring"), "w") as f:
            f.write("#!/bin/sh\necho 'simulated startup failure' >&2\nexit 1\n")
        rc, out = self.run_script()
        self.assertTrue(rc is not None and rc != 0, "script should report the failed producer\n" + out)
        time.sleep(0.5)
        left = self.leftover_processes()
        self.assertEqual(left, [], "consumer processes still running after the script exited: %s" % left)


# ======================================================================= build

class Build(Base):
    def compile(self, sources, extra=()):
        cmd = [CXX, *RELEASE_FLAGS, "-I" + os.path.join(ROOT, "src"), "-I" + os.path.join(ROOT, "ipc"),
               *extra, *sources, "-o", os.path.join(self.tmp, "a.out"), "-lrt"]
        r = subprocess.run(cmd, capture_output=True, text=True)
        return r.returncode, r.stderr

    def test_headers_compile_standalone(self):
        for d in ("src", "ipc"):
            for h in sorted(f for f in os.listdir(os.path.join(ROOT, d)) if f.endswith(".hpp")):
                with self.subTest(header=h):
                    src = self.write("inc.cpp", b"#include <%s>\nint main() {}\n" % h.encode())
                    rc, err = self.compile([src])
                    self.assertEqual(rc, 0, err[-2000:])

    def test_ring_header_can_be_included_from_two_translation_units(self):
        a = self.write("a.cpp", b"#include <spsc-ring-buffer.hpp>\nint main() {}\n")
        b = self.write("b.cpp", b"#include <spsc-ring-buffer.hpp>\nvoid other() {}\n")
        rc, err = self.compile([a, b])
        self.assertEqual(rc, 0, "linking two TUs failed:\n" + err[-2000:])

    def test_all_binaries_build_warning_free(self):
        targets = [
            (["src/main.cpp"], []), (["replay/replay.cpp"], []),
            (["ipc/producer_main.cpp"], []), (["ipc/consumer_main.cpp"], []),
            (["ipc/producer_main.cpp"], ["-DUSE_UNIX_SOCKET"]), (["ipc/consumer_main.cpp"], ["-DUSE_UNIX_SOCKET"]),
        ]
        for srcs, defs in targets:
            with self.subTest(target=" ".join(defs + srcs)):
                rc, err = self.compile([os.path.join(ROOT, s) for s in srcs], ["-Wall", "-Wextra", *defs])
                self.assertEqual(rc, 0, err[-2000:])
                self.assertNotIn("warning:", err)


# ====================================================================== runner

def main():
    global BIN
    ap = argparse.ArgumentParser()
    ap.add_argument("bin_dir")
    BIN = os.path.abspath(ap.parse_args().bin_dir)

    loader = unittest.TestLoader()
    suite = unittest.TestSuite(loader.loadTestsFromTestCase(c) for c in (FeedHandler, Ipc, Replay, BenchScript, Build))
    res = unittest.TextTestRunner(verbosity=2, stream=sys.stdout).run(suite)
    clean_ipc_names()
    return 0 if res.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
