#include "atlas/protocol.hpp"
#include <future>
#include <iostream>
#include <stdexcept>
#include <thread>

namespace {
void check(bool ok, const char *why) {
    if (!ok)
        throw std::runtime_error(why);
}
} // namespace
int main() {
    using namespace atlas;
    try {
        Config cfg;
        cfg.validate();
        Budget tiny(100);
        check(tiny.acquire(100), "budget full allocation");
        check(!tiny.acquire(1), "budget ceiling");
        tiny.release(100);
        check(tiny.used() == 0, "budget release");
        auto packet = wire::encode(2, 0x0102030405060708ULL, {0, {0, 255, 42}});
        check(wire::length(std::span(packet).first(4)) == 15, "wire body length");
        auto decoded = wire::decode({packet.begin() + 4, packet.end()});
        check(decoded.type == 0x82 && decoded.id == 0x0102030405060708ULL, "wire endian");
        check(decoded.payload == std::vector<uint8_t>({0, 0, 0, 255, 42}), "binary-safe payload");
        bool invalid = false;
        try {
            wire::decode({2, 1, 0, 0, 0, 0, 0, 0, 0, 0});
        } catch (const std::invalid_argument &) {
            invalid = true;
        }
        check(invalid, "invalid version");
        Budget budget(1024 * 1024);
        Metrics metrics;
        Cancel cancel;
        {
            MemoryRepository repo(cfg, budget);
            auto p = repo.put("key", "one", 0, cancel);
            check(p.status == Status::ok && p.record, "put");
            const auto version = p.record->version;
            check(repo.put("key", "bad", version + 1, cancel).status == Status::conflict,
                  "compare-and-set mismatch");
            check(repo.put("key", "two", version, cancel).status == Status::ok, "compare-and-set success");
            check(repo.get("key", cancel).record->value == "two", "get committed value");
            std::vector<std::thread> threads;
            std::atomic<unsigned> failures = 0;
            for (unsigned i = 0; i < 8; ++i)
                threads.emplace_back([&] {
                    for (unsigned j = 0; j < 1000; ++j)
                        if (repo.add("counter", 1, cancel).status != Status::ok)
                            ++failures;
                });
            for (auto &t : threads)
                t.join();
            check(failures == 0 && repo.get("counter", cancel).record->value == "8000",
                  "atomic concurrent increments");
            check(repo.put("max", "9223372036854775807", 0, cancel).status == Status::ok, "int64 setup");
            check(repo.add("max", 1, cancel).status == Status::invalid, "integer overflow rejected");
            Cancel expired;
            expired.deadline = std::chrono::steady_clock::now() - std::chrono::seconds(1);
            check(repo.put("expired", "x", 0, expired).status == Status::timeout, "cancel before mutation");
            check(repo.get("expired", cancel).status == Status::not_found, "no cancelled mutation");
            check(repo.erase("key", cancel).status == Status::ok, "erase");
            check(repo.get("key", cancel).status == Status::not_found, "missing key");
        }
        check(budget.used() == 0, "store destructor releases budget");
        {
            Config small;
            small.store_entries = 1;
            small.store_bytes = 256;
            MemoryRepository repo(small, budget);
            check(repo.put("a", "x", 0, cancel).status == Status::ok, "small store insert");
            check(repo.put("b", "x", 0, cancel).status == Status::overloaded, "store entries bounded");
            check(repo.put("a", std::string(300, 'x'), 0, cancel).status == Status::overloaded,
                  "store bytes bounded");
            check(repo.get("a", cancel).record->value == "x", "failed update atomic");
        }
        {
            BoundedPool pool(1, 1, metrics);
            std::promise<void> started, release;
            auto future = release.get_future().share();
            check(pool.submit([&] {
                started.set_value();
                future.wait();
            }),
                  "task accepted");
            started.get_future().wait();
            check(!pool.submit([] {}), "running tasks retain pool capacity");
            pool.stop();
            check(!pool.submit([] {}), "stopped admission");
            release.set_value();
            pool.join();
            check(metrics.compute_pending == 0, "pool releases capacity");
        }
        std::cout << "unit: all assertions passed\n";
        return 0;
    } catch (const std::exception &e) {
        std::cerr << "unit: " << e.what() << '\n';
        return 1;
    }
}
