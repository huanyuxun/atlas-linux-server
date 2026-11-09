#!/usr/bin/env python3
"""Bounded-memory asyncio load generator; percentiles use <=2% logarithmic buckets."""
from __future__ import annotations

import argparse
import asyncio
import collections
import json
import math
import os
from pathlib import Path
import platform
import struct
import time

from client import Client, GET, HASH, OK, PUT, decode_response, encode, hash_payload, key_payload, put_payload


class Histogram:
    def __init__(self):
        self.counts = [0] * 1200
        self.count = 0
        self.total_us = 0.0
        self.max_us = 0.0

    def add(self, microseconds):
        index = min(len(self.counts) - 1, max(0, math.ceil(math.log(max(1, microseconds), 1.02))))
        self.counts[index] += 1
        self.count += 1
        self.total_us += microseconds
        self.max_us = max(self.max_us, microseconds)

    def percentile(self, p):
        if not self.count:
            return None
        target = math.ceil(self.count * p)
        for index, count in enumerate(self.counts):
            target -= count
            if target <= 0:
                return round(1.02 ** index / 1000, 4)

    def result(self):
        return {"mean_ms": round(self.total_us / self.count / 1000, 4) if self.count else None,
                "p95_ms": self.percentile(.95), "p99_ms": self.percentile(.99),
                "max_ms": round(self.max_us / 1000, 4), "percentile_relative_error_max": 0.02}


def process_sample(pid):
    if not pid:
        return None
    try:
        # Fields after the final ')' begin at field 3, accounting for spaces in comm.
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        ticks = int(fields[11]) + int(fields[12])
        rss = int(fields[21]) * os.sysconf("SC_PAGE_SIZE")
        return time.monotonic(), ticks / os.sysconf("SC_CLK_TCK"), rss
    except (OSError, ValueError, IndexError):
        return None


async def run_case(args, concurrency):
    # Seed outside the timed interval. For pure GET, verify every returned value.
    seed = b"x" * args.value_bytes
    with Client(args.host, args.port, args.timeout) as control:
        response = control.call(PUT, put_payload("benchmark", seed))
        if response.status != OK:
            raise RuntimeError(f"benchmark seed failed: {response}")
    start_event = asyncio.Event()
    ready = asyncio.Queue()
    connect_gate = asyncio.Semaphore(128)
    totals = collections.Counter()
    statuses = collections.Counter()
    histogram = Histogram()
    begin, deadline = 0.0, 0.0
    error_examples = []

    async def worker(index):
        writer = None
        pending = {}
        next_id = 1
        try:
            async with connect_gate:
                reader, writer = await asyncio.wait_for(asyncio.open_connection(args.host, args.port), args.timeout)
            totals["connections_opened"] += 1
            ready.put_nowait(True)
            await start_event.wait()

            def enqueue():
                nonlocal next_id
                kind = HASH if args.operation == "hash" else PUT if args.operation == "mixed" and next_id % 5 == 0 else GET
                payload = hash_payload(args.hash_iterations) if kind == HASH else put_payload("benchmark", seed) if kind == PUT else key_payload("benchmark")
                pending[next_id] = (time.perf_counter(), kind)
                writer.write(encode(kind, next_id, payload))
                next_id += 1
                totals["requests_sent"] += 1

            for _ in range(args.pipeline):
                enqueue()
            await writer.drain()
            while pending:
                length = struct.unpack("!I", await asyncio.wait_for(reader.readexactly(4), args.timeout))[0]
                if not 12 <= length <= (1 << 20):
                    raise ValueError(f"invalid response length {length}")
                response = decode_response(await asyncio.wait_for(reader.readexactly(length), args.timeout))
                request_start, expected_kind = pending.pop(response.request_id)
                if response.kind != expected_kind:
                    raise ValueError("response type mismatch")
                statuses[str(response.status)] += 1
                totals["responses"] += 1
                if response.status == OK:
                    if expected_kind == GET and response.versioned()[1] != seed:
                        raise ValueError("GET payload corruption")
                    if expected_kind == HASH and len(response.payload) != 8:
                        raise ValueError("HASH response length")
                    totals["successful_requests"] += 1
                    histogram.add((time.perf_counter() - request_start) * 1e6)
                else:
                    totals["failed_requests"] += 1
                if time.perf_counter() < deadline:
                    enqueue()
                    await writer.drain()
            totals["connections_completed"] += 1
        except Exception as error:
            if writer is None:
                ready.put_nowait(False)
            totals["connection_errors"] += 1
            totals["unanswered_requests"] += len(pending)
            if len(error_examples) < 8:
                error_examples.append(f"client {index}: {type(error).__name__}: {error}")
        finally:
            if writer is not None:
                writer.close()
                try:
                    await asyncio.wait_for(writer.wait_closed(), 1)
                except Exception:
                    pass

    tasks = [asyncio.create_task(worker(i)) for i in range(concurrency)]
    for _ in tasks:
        await ready.get()
    before = process_sample(args.pid)
    rss_peak = before[2] if before else None
    stop_monitor = asyncio.Event()

    async def monitor():
        nonlocal rss_peak
        while not stop_monitor.is_set():
            sample = process_sample(args.pid)
            if sample:
                rss_peak = max(rss_peak or 0, sample[2])
            try:
                await asyncio.wait_for(stop_monitor.wait(), .2)
            except asyncio.TimeoutError:
                pass

    monitor_task = asyncio.create_task(monitor())
    begin = time.perf_counter()
    deadline = begin + args.duration
    start_event.set()
    await asyncio.gather(*tasks)
    elapsed = time.perf_counter() - begin
    after = process_sample(args.pid)
    stop_monitor.set()
    await monitor_task
    cpu = (after[1] - before[1]) / (after[0] - before[0]) * 100 if before and after and after[0] > before[0] else None
    result = {"connections_requested": concurrency, "pipeline": args.pipeline,
              "duration_requested_seconds": args.duration, "duration_seconds": round(elapsed, 4),
              "qps": round(totals["successful_requests"] / elapsed, 2),
              "counts": dict(totals), "response_status_counts": dict(statuses),
              "latency_successful_requests": histogram.result(),
              "server_cpu_percent_one_core_100": round(cpu, 2) if cpu is not None else None,
              "server_rss_peak_bytes": rss_peak, "server_rss_end_bytes": after[2] if after else None,
              "stable": totals["connection_errors"] == 0 and totals["failed_requests"] == 0 and totals["connections_completed"] == concurrency,
              "error_examples": error_examples}
    print(json.dumps(result), flush=True)
    return result


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7777)
    parser.add_argument("--connections", default="1,32,128,512")
    parser.add_argument("--duration", type=float, default=10)
    parser.add_argument("--pipeline", type=int, default=1)
    parser.add_argument("--operation", choices=("get", "mixed", "hash"), default="get")
    parser.add_argument("--hash-iterations", type=int, default=10000)
    parser.add_argument("--value-bytes", type=int, default=128)
    parser.add_argument("--timeout", type=float, default=10)
    parser.add_argument("--pid", type=int, help="server PID on this Linux machine for /proc CPU/RSS sampling")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    counts = [int(x) for x in args.connections.split(",")]
    if min(counts) <= 0 or args.pipeline <= 0 or args.duration <= 0 or not 0 <= args.value_bytes <= 60000:
        parser.error("positive connections, pipeline and duration; value-bytes must be 0..60000")
    results = []
    for count in counts:
        results.append(await run_case(args, count))
    report = {"schema": 1, "time_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "platform": platform.platform(), "python": platform.python_version(), "cpu_count": os.cpu_count(),
              "operation": args.operation, "value_bytes": args.value_bytes,
              "notes": ["Client and server may share CPU; this is end-to-end measured throughput, not a server capacity claim.",
                        "Connection setup and seeding excluded; final pipeline drain included.",
                        "Percentiles are bounded-memory logarithmic histogram upper bounds with <=2% relative bucket error.",
                        "CPU is process CPU: 100% equals one core; RSS sampled every 200 ms; null means unavailable."],
              "results": results}
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    return 0 if all(result["stable"] for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
