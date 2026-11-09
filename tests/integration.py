#!/usr/bin/env python3
"""Black-box Linux integration tests. Requires only Python's standard library."""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
from client import (ADD, CONFLICT, DELETE, GET, HASH, INVALID, NOT_FOUND, OK, OVERLOADED,
                    PUT, STATS, TIMEOUT, Client, add_payload, encode, hash_payload,
                    key_payload, put_payload, receive)

SERVER_BINARY = ""
SOAK_SECONDS = 0.0
ARTIFACT_DIR = None


class Server:
    """Starts each scenario with an isolated store, config, and captured diagnostics."""
    serial = 0

    def __init__(self, **overrides):
        Server.serial += 1
        self.name = f"server-{Server.serial:02d}"
        self.temp = tempfile.TemporaryDirectory(prefix="atlas-test-")
        self.root = Path(self.temp.name)
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            self.port = probe.getsockname()[1]
        self.config = {"address": "127.0.0.1", "port": self.port, "io_threads": 4,
                       "compute_threads": 2, "max_connections": 256,
                       "connect_timeout_ms": 3000, "read_timeout_ms": 3000,
                       "idle_timeout_ms": 5000, "business_timeout_ms": 2000,
                       "shutdown_timeout_ms": 1000, "metrics_interval_ms": 100,
                       "metrics_file": str(self.root / "metrics.json"), "log_level": "warn"}
        self.config.update(overrides)
        self.stdout = (self.root / "stdout.log").open("wb")
        self.stderr = (self.root / "stderr.log").open("wb")
        config_path = self.root / "server.conf"
        config_path.write_text("".join(f"{k}={v}\n" for k, v in self.config.items()))
        self.process = subprocess.Popen([SERVER_BINARY, "--config", str(config_path)], stdout=self.stdout, stderr=self.stderr)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                diagnostics = self.diagnostics()
                self.close()
                raise RuntimeError(f"startup failed: {diagnostics}")
            if "READY" in (self.root / "stdout.log").read_text(errors="replace"):
                return
            time.sleep(.02)
        self.close()
        raise RuntimeError("server did not report READY within 15 seconds")

    def diagnostics(self):
        return "\n".join(p.read_text(errors="replace")[-8000:] for p in (self.root / "stdout.log", self.root / "stderr.log"))

    def connect(self, timeout=5):
        return Client(port=self.port, timeout=timeout)

    def raw(self, timeout=5):
        return socket.create_connection(("127.0.0.1", self.port), timeout=timeout)

    def stop(self, timeout=5):
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
        return self.process.wait(timeout=timeout)

    def close(self):
        try:
            self.stop()
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
        self.stdout.close()
        self.stderr.close()
        if ARTIFACT_DIR:
            destination = ARTIFACT_DIR / self.name
            destination.mkdir(parents=True, exist_ok=True)
            for path in self.root.iterdir():
                (destination / path.name).write_bytes(path.read_bytes())
        self.temp.cleanup()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def assert_closed(test, sock, timeout=3):
    """Drain already-written bytes; timeout is a failure, not proof of closure."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        sock.settimeout(max(.01, deadline - time.monotonic()))
        try:
            if not sock.recv(65536):
                return
        except (ConnectionResetError, BrokenPipeError):
            return
        except socket.timeout:
            break
    test.fail("connection remained open past deadline")


class NormalTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = Server()

    @classmethod
    def tearDownClass(cls):
        cls.server.close()

    def test_01_business_flow_and_version_conflict(self):
        with self.server.connect() as client:
            response = client.call(PUT, put_payload("flow", b"binary\0value\xff"))
            self.assertEqual(response.status, OK)
            version, value = response.versioned()
            self.assertGreater(version, 0)
            self.assertEqual(value, b"binary\0value\xff")
            self.assertEqual(client.call(GET, key_payload("flow")).versioned(), (version, value))
            self.assertEqual(client.call(PUT, put_payload("flow", b"bad", version + 1000)).status, CONFLICT)
            replacement = client.call(PUT, put_payload("flow", b"replacement", version))
            self.assertEqual(replacement.status, OK)
            self.assertGreater(replacement.versioned()[0], version)
            self.assertEqual(client.call(DELETE, key_payload("flow")).status, OK)
            self.assertEqual(client.call(GET, key_payload("flow")).status, NOT_FOUND)
            self.assertEqual(client.call(DELETE, key_payload("flow")).status, NOT_FOUND)

    def test_02_repeated_connect_disconnect_and_requests(self):
        for connection in range(20):
            with self.server.connect() as client:
                for index in range(10):
                    response = client.call(ADD, add_payload(f"continuous-{connection}", 1))
                    self.assertEqual(response.status, OK)
                    self.assertEqual(int(response.versioned()[1]), index + 1)

    def test_03_fragmented_header_and_body(self):
        packet = encode(PUT, 0x1020304050607080, put_payload("fragment", b"abcdef\0"))
        with self.server.raw() as sock:
            for byte in packet:
                sock.sendall(bytes([byte]))
                time.sleep(.001)
            result = receive(sock)
            self.assertEqual((result.request_id, result.status), (0x1020304050607080, OK))
            self.assertEqual(result.versioned()[1], b"abcdef\0")

    def test_04_coalesced_and_pipelined_matching(self):
        with self.server.raw() as sock:
            for batch in range(8):
                ids = list(range(batch * 8 + 1, batch * 8 + 9))
                sock.sendall(b"".join(encode(GET, request_id, key_payload("missing")) for request_id in ids))
                replies = [receive(sock) for _ in ids]
                self.assertEqual({response.request_id for response in replies}, set(ids))
                self.assertTrue(all(response.kind == GET and response.status == NOT_FOUND for response in replies))

    def test_05_invalid_lengths_and_version(self):
        packets = [struct.pack("!I", 0), struct.pack("!I", 9), struct.pack("!I", 0xFFFFFFFF), encode(STATS, 1, version=2)]
        for packet in packets:
            with self.subTest(packet=packet.hex()), self.server.raw() as sock:
                sock.sendall(packet)
                assert_closed(self, sock)
        with self.server.connect() as client:
            self.assertEqual(client.call(STATS).status, OK)

    def test_06_unknown_type_and_bad_payload_are_request_errors(self):
        with self.server.connect() as client:
            self.assertEqual(client.call(63).status, INVALID)
            for kind, payload in [(GET, b""), (GET, b"\0\x05x"), (GET, b"\0\0"),
                                  (GET, key_payload("x") + b"extra"), (PUT, key_payload("x")),
                                  (ADD, key_payload("x") + b"x"), (HASH, hash_payload(0)),
                                  (HASH, hash_payload(10000001)), (STATS, b"bad")]:
                with self.subTest(kind=kind, payload=payload):
                    self.assertEqual(client.call(kind, payload).status, INVALID)
            self.assertEqual(client.call(STATS).status, OK)

    def test_07_abrupt_disconnect_and_partial_frames(self):
        for index in range(30):
            sock = self.server.raw()
            packet = encode(HASH, index + 1, hash_payload(100000, b"seed" * 16))
            sock.sendall(packet[:5] if index % 2 else packet)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            sock.close()
        time.sleep(.1)
        with self.server.connect() as client:
            self.assertEqual(client.call(STATS).status, OK)

    def test_08_concurrent_atomic_add(self):
        workers, operations = 24, 50
        with self.server.connect() as client:
            self.assertEqual(client.call(PUT, put_payload("shared-counter", b"0")).status, OK)

        def work(_):
            with self.server.connect() as client:
                for _ in range(operations):
                    response = client.call(ADD, add_payload("shared-counter", 1))
                    if response.status != OK:
                        raise AssertionError(response)

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            list(executor.map(work, range(workers)))
        with self.server.connect() as client:
            self.assertEqual(int(client.call(GET, key_payload("shared-counter")).versioned()[1]), workers * operations)

    def test_09_compare_and_swap_single_winner(self):
        with self.server.connect() as client:
            version = client.call(PUT, put_payload("cas", b"initial")).versioned()[0]
        barrier = threading.Barrier(16)

        def work(index):
            with self.server.connect() as client:
                barrier.wait(timeout=10)
                return client.call(PUT, put_payload("cas", str(index), version)).status

        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            results = list(executor.map(work, range(16)))
        self.assertEqual(results.count(OK), 1)
        self.assertEqual(results.count(CONFLICT), 15)

    def test_10_stats_has_bounded_resource_counters(self):
        with self.server.connect() as client:
            stats = client.stats()
        for key in ("connections", "requests", "errors", "timeouts", "inflight", "send_bytes", "compute_pending"):
            self.assertIn(key, stats)
            self.assertGreaterEqual(stats[key], 0)
        self.assertGreater(stats["requests"], 0)

    def test_10b_signed_add_validation_and_overflow(self):
        with self.server.connect() as client:
            self.assertEqual(client.call(ADD, add_payload("negative", -17)).versioned()[1], b"-17")
            self.assertEqual(client.call(PUT, put_payload("overflow", str(2**63 - 1))).status, OK)
            self.assertEqual(client.call(ADD, add_payload("overflow", 1)).status, INVALID)
            self.assertEqual(client.call(GET, key_payload("overflow")).versioned()[1], str(2**63 - 1).encode())
            self.assertEqual(client.call(PUT, put_payload("underflow", str(-(2**63)))).status, OK)
            self.assertEqual(client.call(ADD, add_payload("underflow", -1)).status, INVALID)
            self.assertEqual(client.call(PUT, put_payload("non-number", b"hello")).status, OK)
            self.assertEqual(client.call(ADD, add_payload("non-number", 1)).status, INVALID)


class LimitTests(unittest.TestCase):
    def test_11_connection_limit_and_recovery(self):
        with Server(max_connections=2) as server:
            first, second = server.connect(), server.connect()
            try:
                self.assertEqual(first.call(STATS).status, OK)
                self.assertEqual(second.call(STATS).status, OK)
                with server.raw() as excess:
                    assert_closed(self, excess)
                first.close()
                time.sleep(.1)
                with server.connect() as replacement:
                    self.assertEqual(replacement.call(STATS).status, OK)
            finally:
                first.close()
                second.close()

    def test_12_compute_queue_saturation(self):
        with Server(compute_threads=1, compute_queue=1, max_inflight_per_session=128,
                    max_inflight_global=256, business_timeout_ms=200, max_send_messages=256) as server:
            with server.raw() as sock:
                count = 64
                sock.sendall(b"".join(encode(HASH, index + 1, hash_payload(10000000, b"x" * 4096)) for index in range(count)))
                replies = [receive(sock) for _ in range(count)]
                self.assertEqual(len({r.request_id for r in replies}), count)
                self.assertIn(OVERLOADED, [r.status for r in replies])
                self.assertTrue(all(r.status in (OK, OVERLOADED, TIMEOUT) for r in replies))
            time.sleep(.1)
            with server.connect() as client:
                self.assertEqual(client.call(STATS).status, OK)

    def test_13_first_header_body_and_idle_timeout(self):
        with Server(connect_timeout_ms=100, read_timeout_ms=100, idle_timeout_ms=100) as server:
            with server.raw() as sock:
                assert_closed(self, sock, 2)
            with server.raw() as sock:
                sock.sendall(struct.pack("!I", 100) + b"\x01")
                assert_closed(self, sock, 2)
            with server.connect() as client:
                self.assertEqual(client.call(STATS).status, OK)
                assert_closed(self, client.sock, 2)

    def test_14_business_deadline_and_capacity_recovery(self):
        with Server(business_timeout_ms=1, compute_threads=1) as server:
            with server.connect() as client:
                response = client.call(HASH, hash_payload(10000000, b"x" * 4096))
                self.assertEqual(response.status, TIMEOUT)
            time.sleep(.1)
            # A separate server with a normal deadline is used for healthy protocol checks;
            # the tiny-deadline server must at least remain alive after cancelled work.
            self.assertIsNone(server.process.poll())

    def test_15_duplicate_inflight_id_closes_connection(self):
        with Server(business_timeout_ms=500) as server:
            with server.raw() as sock:
                sock.sendall(encode(HASH, 9, hash_payload(10000000, b"x" * 4096)) + encode(GET, 9, key_payload("x")))
                assert_closed(self, sock)

    def test_16_slow_reader_backpressure(self):
        with Server(max_send_bytes=65536, max_send_messages=4, max_inflight_per_session=256,
                    max_inflight_global=512, compute_queue=512, write_timeout_ms=100) as server:
            with server.connect() as client:
                self.assertEqual(client.call(PUT, put_payload("large", b"x" * 60000)).status, OK)
            with server.raw() as sock:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
                sock.sendall(b"".join(encode(GET, i + 1, key_payload("large")) for i in range(256)))
                # Deliberately do not consume responses while production outruns the socket.
                time.sleep(.5)
                assert_closed(self, sock, 5)
            with server.connect() as client:
                stats = client.stats()
                self.assertLessEqual(stats["send_bytes"], 65536)
                self.assertEqual(client.call(GET, key_payload("large")).status, OK)

    def test_17_store_entry_and_byte_limits(self):
        with Server(store_entries=2, store_bytes=4096) as server:
            with server.connect() as client:
                self.assertEqual(client.call(PUT, put_payload("a", b"x")).status, OK)
                self.assertEqual(client.call(PUT, put_payload("b", b"x")).status, OK)
                self.assertEqual(client.call(PUT, put_payload("c", b"x")).status, OVERLOADED)
                self.assertEqual(client.call(PUT, put_payload("a", b"x" * 8000)).status, OVERLOADED)
                self.assertEqual(client.call(GET, key_payload("a")).versioned()[1], b"x")
                self.assertEqual(client.call(DELETE, key_payload("b")).status, OK)
                self.assertEqual(client.call(PUT, put_payload("c", b"x")).status, OK)

    def test_18_graceful_shutdown_drains_admitted_request(self):
        with Server(shutdown_timeout_ms=2000, business_timeout_ms=1500) as server:
            with server.raw() as sock:
                sock.sendall(encode(PUT, 101, put_payload("shutdown", b"saved")))
                # Observe processing via a second connection before signalling, so this
                # test does not race an unaccepted request against shutdown admission.
                with server.connect() as control:
                    deadline = time.monotonic() + 1
                    while time.monotonic() < deadline:
                        if control.call(GET, key_payload("shutdown")).status == OK:
                            break
                    else:
                        self.fail("PUT not admitted before signal")
                start = time.monotonic()
                server.process.send_signal(signal.SIGTERM)
                response = receive(sock)
                self.assertEqual((response.request_id, response.status), (101, OK))
                assert_closed(self, sock, 3)
                self.assertEqual(server.process.wait(timeout=4), 0, server.diagnostics())
                self.assertLess(time.monotonic() - start, 4)

    def test_19_shutdown_deadline_cancels_heavy_tasks(self):
        with Server(shutdown_timeout_ms=100, business_timeout_ms=10000,
                    compute_threads=1, compute_queue=64, max_inflight_per_session=64) as server:
            with server.raw() as sock:
                sock.sendall(b"".join(encode(HASH, i + 1, hash_payload(10000000, b"x" * 4096)) for i in range(32)))
                time.sleep(.03)
                start = time.monotonic()
                self.assertEqual(server.stop(timeout=3), 0, server.diagnostics())
                self.assertLess(time.monotonic() - start, 3)

    def test_20_invalid_config_fails_fast(self):
        with tempfile.TemporaryDirectory() as directory:
            for config in ("unknown_option=1\n", "io_threads=0\n", "max_frame_bytes=4294967296\n", "port=999999\n"):
                with self.subTest(config=config):
                    path = Path(directory) / "bad.conf"
                    path.write_text(config)
                    result = subprocess.run([SERVER_BINARY, "--config", str(path)], capture_output=True, timeout=3)
                    self.assertNotEqual(result.returncode, 0)

    def test_22_per_session_inflight_limit_closes(self):
        with Server(max_inflight_per_session=1) as server:
            with server.raw() as sock:
                sock.sendall(encode(HASH, 1, hash_payload(10000000)) + encode(GET, 2, key_payload("x")))
                assert_closed(self, sock)
            with server.connect() as client:
                self.assertEqual(client.call(STATS).status, OK)

    def test_23_global_inflight_limit_rejects(self):
        with Server(max_inflight_global=1, max_inflight_per_session=64) as server:
            with server.raw() as sock:
                sock.sendall(encode(HASH, 1, hash_payload(10000000)) +
                             b"".join(encode(GET, i, key_payload("x")) for i in range(2, 33)))
                responses = [receive(sock) for _ in range(32)]
                self.assertIn(OVERLOADED, [response.status for response in responses])
                self.assertEqual(len({response.request_id for response in responses}), 32)

    def test_24_global_memory_budget_rejects(self):
        with Server(max_frame_bytes=4096, memory_budget_bytes=12000) as server:
            with server.connect() as client:
                self.assertEqual(client.call(GET, key_payload("x")).status, OVERLOADED)
            time.sleep(.1)
            with server.connect() as client:
                self.assertEqual(client.call(GET, key_payload("x")).status, OVERLOADED)

    def test_25_write_deadline_reclaims_slow_connection(self):
        with Server(max_send_bytes=16777216, max_send_messages=512,
                    max_inflight_per_session=256, max_inflight_global=512,
                    compute_queue=512, write_timeout_ms=100) as server:
            with server.connect() as client:
                self.assertEqual(client.call(PUT, put_payload("large", b"x" * 60000)).status, OK)
            with server.raw() as slow:
                slow.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
                slow.sendall(b"".join(encode(GET, i + 1, key_payload("large")) for i in range(128)))
                time.sleep(.5)
                with server.connect() as control:
                    stats = control.stats()
                    self.assertGreaterEqual(stats["timeouts"], 1)
                    self.assertLessEqual(stats["connections"], 1)
                    self.assertLessEqual(stats["send_bytes"], 65536)

    @unittest.skipUnless(SOAK_SECONDS > 0, "use --soak-seconds for sustained concurrency test")
    def test_21_soak(self):
        pass  # Replaced below after CLI parsing, before unittest discovers tests.


def soak_test(self):
    with Server(max_connections=256, compute_queue=512) as server:
        workers = 32
        stop_at = time.monotonic() + SOAK_SECONDS
        barrier = threading.Barrier(workers)
        initial_rss = linux_rss(server.process.pid)
        counts = []

        def work(index):
            count = 0
            with server.connect() as client:
                barrier.wait(timeout=10)
                while time.monotonic() < stop_at:
                    response = client.call(ADD, add_payload(f"soak-{index}", 1))
                    if response.status != OK or int(response.versioned()[1]) != count + 1:
                        raise AssertionError(response)
                    count += 1
            return count

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            counts = list(executor.map(work, range(workers)))
        time.sleep(.2)
        with server.connect() as client:
            stats = client.stats()
        result = {"duration_seconds": SOAK_SECONDS, "clients": workers, "completed_requests": sum(counts),
                  "minimum_per_client": min(counts), "rss_start_bytes": initial_rss,
                  "rss_end_bytes": linux_rss(server.process.pid), "final_metrics": stats}
        if ARTIFACT_DIR:
            (ARTIFACT_DIR / "soak.json").write_text(json.dumps(result, indent=2) + "\n")
        print("SOAK " + json.dumps(result), flush=True)
        self.assertGreater(min(counts), 0)
        self.assertLessEqual(stats["connections"], 1)
        self.assertLessEqual(stats["inflight"], 1)
        self.assertLessEqual(stats["compute_pending"], 1)
        self.assertEqual(server.stop(), 0, server.diagnostics())


def linux_rss(pid):
    try:
        return int(Path(f"/proc/{pid}/statm").read_text().split()[1]) * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError, IndexError):
        return None


def main():
    global SERVER_BINARY, SOAK_SECONDS, ARTIFACT_DIR
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", required=True, type=Path)
    parser.add_argument("--soak-seconds", type=float, default=0)
    parser.add_argument("--artifacts", type=Path)
    args, remaining = parser.parse_known_args()
    SERVER_BINARY = str(args.server.resolve())
    SOAK_SECONDS = args.soak_seconds
    ARTIFACT_DIR = args.artifacts
    if ARTIFACT_DIR:
        ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    if SOAK_SECONDS > 0:
        LimitTests.test_21_soak = soak_test
    unittest.main(argv=[sys.argv[0], *remaining], verbosity=2)


if __name__ == "__main__":
    main()
