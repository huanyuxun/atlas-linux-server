# Client, integration tests, and benchmark

All tools require Python 3.10+ and use only the standard library. The codec is
independent of the C++ implementation, which helps catch byte-order and framing
mistakes. Binary values are accepted by the importable `Client` API; the CLI uses
UTF-8 text and also prints returned bytes as hex.

```sh
python3 tools/client.py --port 7777 put greeting hello
python3 tools/client.py --port 7777 get greeting
python3 tools/client.py --port 7777 add counter 1
python3 tools/client.py --port 7777 hash 100000 seed
python3 tools/client.py --port 7777 stats
python3 tests/integration.py --server build/atlas_server --artifacts test-results
python3 tests/integration.py --server build/atlas_server --soak-seconds 120 --artifacts test-results
```

Integration tests launch isolated server processes, temporary configurations and
stores, and test success and failure paths. `--artifacts` preserves every process's
configuration, stdout, stderr and metrics. Without it, temporary files are cleaned
up. Specific unittest names may be appended, for example `LimitTests.test_16_slow_reader_backpressure`.

Start the server separately and run a load matrix against it:

```sh
./build/atlas_server --config config/server.conf >server.log 2>&1 &
server_pid=$!
python3 tools/bench.py --connections 1,32,128,512,1024 --duration 15 \
  --pipeline 1 --pid "$server_pid" --output results/get-matrix.json
python3 tools/bench.py --connections 32,128 --duration 15 --operation mixed \
  --pipeline 4 --pid "$server_pid" --output results/mixed-matrix.json
kill -TERM "$server_pid"
wait "$server_pid"
```

For a repeatable, isolated run that starts and stops the server and saves all
configuration, diagnostics, results, and shutdown status automatically:

```sh
python3 tools/perf_run.py --server build/atlas_server --duration 10 \
  --idle-connections 4096 --output results/performance
```

The default matrix is GET at 1/32/128/512/1024 connections, followed by mixed GET/PUT
at 32/128 connections with four requests in flight per connection. The optional
idle case opens 4096 connections, completes a STATS handshake, holds them for five
seconds, and checks every connection again. The runner raises its soft descriptor
limit within the existing hard limit, records the actual value, uses a separate
benchmark configuration with 8192-byte frames, and enforces subprocess deadlines.
`--duration`, `--connections`, and `--idle-seconds` permit shorter smoke runs or
longer measurements. The runner's exit status is nonzero if any stage is unstable.

`--operation get` reads a seeded 128-byte value; `mixed` performs four GETs then one
unconditional PUT on the same key; `hash` requests bounded CPU work. `--value-bytes`
and `--hash-iterations` adjust workload size. Each connection has exactly
`--pipeline` or fewer outstanding requests, and responses are matched by ID without
assuming ordering. Every GET response is checked for data corruption.

Reported QPS counts only successful validated replies. Setup and seeding are
outside timing; draining the last requests is inside timing. Counts separately
report connection errors, non-OK responses, and unanswered requests. The tool
returns nonzero if any matrix row is unstable. Latency mean is exact; P95/P99 use
fixed-size logarithmic buckets whose upper bounds have at most 2% relative bucket
error. Memory does not grow with the number of completed requests.

`--pid` enables Linux `/proc` server CPU/RSS sampling. CPU 100% means one occupied
core; RSS is sampled every 200 ms and is not a precise allocation high-water mark.
Use the server's PID in the same PID namespace. Null values mean sampling was not
available. A client on the same VM competes with the server for CPU; these results
are reproducible end-to-end measurements, not claims about maximum server capacity.
No benchmark result is included unless an actual run produced it.

For a fully managed run, `python3 tools/perf_run.py --server build/atlas_server
--idle-connections 4096 --output results/performance` starts a separate server,
runs both matrices, validates every held connection, saves configuration and raw
results, and terminates the server. It raises only its own inherited file-descriptor
soft limit within the existing hard limit. The managed benchmark uses an 8192-byte
frame limit and a larger compute admission limit; it records the exact profile.

The soak test verifies 32 concurrent clients continually updating independent
counters and checks connection/task reclamation afterward. A short soak cannot
establish multi-day reliability. Run longer durations in the deployment
environment and inspect metrics/logs alongside RSS trends.
