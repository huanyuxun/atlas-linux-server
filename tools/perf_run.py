#!/usr/bin/env python3
"""Launch a dedicated Atlas server, run measured workload matrices, preserve artifacts."""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter
import json
import os
from pathlib import Path
import resource
import signal
import struct
import subprocess
import sys
import time

from bench import process_sample
from client import OK, STATS, decode_response, encode


async def idle_test(port, count, seconds, server_pid):
    """Complete the first request, hold all connections, then verify each once more."""
    gate = asyncio.Semaphore(128)
    connections = []
    counts = Counter()
    examples = []
    rss_peak = 0

    def error(index, exc):
        counts["errors"] += 1
        if len(examples) < 8:
            examples.append(f"client {index}: {type(exc).__name__}: {exc}")

    async def exchange(reader, writer, request_id):
        writer.write(encode(STATS, request_id))
        await writer.drain()
        length = struct.unpack("!I", await asyncio.wait_for(reader.readexactly(4), 10))[0]
        if not 12 <= length <= 65536:
            raise ValueError("invalid STATS frame length")
        response = decode_response(await asyncio.wait_for(reader.readexactly(length), 10))
        if (response.kind, response.request_id, response.status) != (STATS, request_id, OK):
            raise ValueError(f"unexpected STATS response: {response}")

    async def open_one(index):
        writer = None
        try:
            async with gate:
                reader, writer = await asyncio.wait_for(asyncio.open_connection("127.0.0.1", port), 10)
                counts["connections_opened"] += 1
                await exchange(reader, writer, 1)
                connections.append((index, reader, writer))
                counts["initial_health_success"] += 1
        except Exception as exc:
            error(index, exc)
            if writer:
                writer.close()

    async def verify_one(index, reader, writer):
        try:
            async with gate:
                await exchange(reader, writer, 2)
                counts["final_health_success"] += 1
        except Exception as exc:
            error(index, exc)
        finally:
            writer.close()
            try:
                await asyncio.wait_for(writer.wait_closed(), 1)
            except Exception:
                pass

    started = time.monotonic()
    await asyncio.gather(*(open_one(index) for index in range(count)))
    fully_open = time.monotonic()
    before = process_sample(server_pid)
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        sample = process_sample(server_pid)
        if sample:
            rss_peak = max(rss_peak, sample[2])
        await asyncio.sleep(min(.2, max(0, until - time.monotonic())))
    after = process_sample(server_pid)
    await asyncio.gather(*(verify_one(*connection) for connection in connections))
    cpu = (after[1] - before[1]) / (after[0] - before[0]) * 100 if before and after and after[0] > before[0] else None
    return {"connections_requested": count, "hold_seconds": seconds,
            "connection_setup_seconds": round(fully_open - started, 4),
            "total_seconds": round(time.monotonic() - started, 4), "counts": dict(counts),
            "server_idle_cpu_percent_one_core_100": round(cpu, 2) if cpu is not None else None,
            "server_rss_peak_bytes": rss_peak or None, "error_examples": examples,
            "stable": counts["final_health_success"] == count and counts["errors"] == 0}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", required=True, type=Path)
    parser.add_argument("--output", type=Path, default=Path("results/performance"))
    parser.add_argument("--duration", type=float, default=10)
    parser.add_argument("--connections", default="1,32,128,512,1024")
    parser.add_argument("--idle-connections", type=int, default=0, help="optional idle stability case, e.g. 4096")
    parser.add_argument("--idle-seconds", type=float, default=5)
    args = parser.parse_args()
    if args.duration <= 0 or not 0 <= args.idle_connections <= 8192 or args.idle_seconds < 0:
        parser.error("duration must be positive; idle-connections 0..8192; idle-seconds nonnegative")
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    wanted = 32768 if hard == resource.RLIM_INFINITY else min(32768, hard)
    fd_error = None
    if soft < wanted:
        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (wanted, hard))
        except OSError as exc:
            fd_error = str(exc)
    config = {"address": "127.0.0.1", "port": 0, "io_threads": 4, "compute_threads": 2,
              "max_frame_bytes": 8192, "max_connections": 8192, "compute_queue": 4096,
              "max_inflight_global": 8192, "memory_budget_bytes": 268435456,
              "max_send_bytes": 262144, "idle_timeout_ms": 30000,
              "business_timeout_ms": 5000, "shutdown_timeout_ms": 3000,
              "log_level": "warn", "metrics_interval_ms": 1000,
              "metrics_file": str(args.output / "server-metrics.json")}
    config_file = args.output / "benchmark.conf"
    config_file.write_text("".join(f"{key}={value}\n" for key, value in config.items()))
    report = {"schema": 1, "time_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "server_binary": str(args.server.resolve()), "config": config,
              "rlimit_nofile": list(resource.getrlimit(resource.RLIMIT_NOFILE)),
              "rlimit_error": fd_error, "benchmarks": []}
    all_stable = True
    server = None
    try:
        with (args.output / "server.stdout.log").open("wb") as out, (args.output / "server.stderr.log").open("wb") as err:
            server = subprocess.Popen([str(args.server.resolve()), "--config", str(config_file)],
                                      stdin=subprocess.DEVNULL, stdout=out, stderr=err)
            deadline = time.monotonic() + 15
            port = None
            while time.monotonic() < deadline:
                if server.poll() is not None:
                    raise RuntimeError(f"server exited during startup: {server.returncode}; see server.stderr.log")
                for line in (args.output / "server.stdout.log").read_text(errors="replace").splitlines():
                    if line.startswith("READY "):
                        port = int(line.split()[1])
                        break
                if port is not None:
                    break
                time.sleep(.02)
            if port is None:
                raise RuntimeError("server did not report READY within 15 seconds")
            report.update(server_pid=server.pid, port=port)
            for name, operation, connections, pipeline in [("get", "get", args.connections, 1), ("mixed", "mixed", "32,128", 4)]:
                result_file = args.output / f"{name}-matrix.json"
                command = [sys.executable, str(Path(__file__).with_name("bench.py")), "--port", str(port),
                           "--operation", operation, "--connections", connections, "--pipeline", str(pipeline),
                           "--duration", str(args.duration), "--pid", str(server.pid), "--output", str(result_file)]
                print("RUN " + " ".join(command), flush=True)
                with (args.output / f"{name}-client.stdout.log").open("wb") as bout, (args.output / f"{name}-client.stderr.log").open("wb") as berr:
                    completed = subprocess.run(command, stdout=bout, stderr=berr,
                                               timeout=len(connections.split(",")) * args.duration + 120)
                report["benchmarks"].append({"operation": operation, "returncode": completed.returncode, "result": result_file.name})
                all_stable &= completed.returncode == 0
                print(f"DONE {name}: exit={completed.returncode}", flush=True)
                if server.poll() is not None:
                    raise RuntimeError("server exited during benchmark")
            if args.idle_connections:
                idle = asyncio.run(idle_test(port, args.idle_connections, args.idle_seconds, server.pid))
                (args.output / "idle-connections.json").write_text(json.dumps(idle, indent=2) + "\n")
                report["idle_result"] = "idle-connections.json"
                all_stable &= idle["stable"]
                print("IDLE " + json.dumps(idle), flush=True)
    except Exception as exc:
        all_stable = False
        report["error"] = f"{type(exc).__name__}: {exc}"
        print(report["error"], file=sys.stderr, flush=True)
    finally:
        if server is not None:
            start = time.monotonic()
            if server.poll() is None:
                server.send_signal(signal.SIGTERM)
            try:
                report["server_exit"] = server.wait(timeout=8)
            except subprocess.TimeoutExpired:
                server.kill()
                report["server_exit"] = server.wait(timeout=3)
                report["forced_kill"] = True
            report["shutdown_seconds"] = round(time.monotonic() - start, 4)
            all_stable &= report["server_exit"] == 0
        report["stable"] = bool(all_stable)
        (args.output / "run.json").write_text(json.dumps(report, indent=2) + "\n")
    return 0 if all_stable else 1


if __name__ == "__main__":
    raise SystemExit(main())
