#pragma once

#include <array>
#include <atomic>
#include <boost/asio/thread_pool.hpp>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

namespace atlas {

struct Config {
    std::string address = "127.0.0.1";
    std::uint16_t port = 7777;
    std::size_t io_threads = 4, compute_threads = 2;
    std::size_t max_connections = 4096, max_frame_bytes = 65536;
    std::size_t max_send_bytes = 262144, max_send_messages = 64;
    std::size_t max_inflight_per_session = 16, max_inflight_global = 4096;
    std::size_t compute_queue = 256, memory_budget_bytes = 268435456;
    std::size_t store_bytes = 67108864, store_entries = 100000;
    int connect_timeout_ms = 5000, read_timeout_ms = 10000;
    int idle_timeout_ms = 30000, write_timeout_ms = 5000;
    int business_timeout_ms = 2000, downstream_timeout_ms = 1000;
    int shutdown_timeout_ms = 5000, metrics_interval_ms = 1000;
    std::string log_level = "info", metrics_file;
    std::size_t log_queue = 4096;
    static Config load(const std::string &path);
    void validate() const;
};

// Logical application-managed memory. Allocator, Asio, stacks and kernel socket
// buffers have separate structural bounds and are not represented as RSS here.
class Budget {
  public:
    explicit Budget(std::size_t limit) : limit_(limit) {}
    bool acquire(std::size_t bytes) noexcept;
    void release(std::size_t bytes) noexcept;
    std::size_t used() const noexcept {
        return used_.load(std::memory_order_relaxed);
    }
    std::size_t limit() const noexcept {
        return limit_;
    }

  private:
    const std::size_t limit_;
    std::atomic<std::size_t> used_{0};
};

struct Metrics {
    std::atomic<std::uint64_t> connections{0}, requests{0}, errors{0}, timeouts{0};
    std::atomic<std::uint64_t> rejected{0}, inflight{0}, send_bytes{0}, send_messages{0};
    std::atomic<std::uint64_t> compute_pending{0}, log_dropped{0};
    void observe(std::uint64_t microseconds) noexcept;
    std::string json(const Budget &) const;
    // Only the single monitor thread calls this: procfs never runs on I/O workers.
    void sample_process();

  private:
    std::array<std::atomic<std::uint64_t>, 32> latency_{};
    std::atomic<std::uint64_t> completed_{0}, latency_sum_us_{0};
    std::atomic<std::uint64_t> rss_bytes_{0};
    std::atomic<double> cpu_percent_{0}, recent_qps_{0};
    std::chrono::steady_clock::time_point started_ = std::chrono::steady_clock::now();
    std::chrono::steady_clock::time_point last_sample_ = started_;
    std::uint64_t last_ticks_ = 0, last_requests_ = 0;
    bool sampled_ = false;
};

// Producers never wait for the sink or the queue mutex. Excess logs are dropped.
class Logger {
  public:
    Logger(std::size_t capacity, std::string level);
    ~Logger();
    Logger(const Logger &) = delete;
    Logger &operator=(const Logger &) = delete;
    void log(std::string level, std::uint64_t connection, std::uint64_t request, std::string text) noexcept;
    std::uint64_t dropped() const noexcept {
        return dropped_.load();
    }

  private:
    struct Entry {
        std::chrono::system_clock::time_point time;
        std::size_t thread;
        std::uint64_t connection, request;
        std::string level, text;
    };
    void run() noexcept;
    const std::size_t capacity_;
    const int level_;
    std::mutex mutex_;
    std::condition_variable wake_;
    std::deque<Entry> queue_;
    bool stopping_ = false;
    std::chrono::steady_clock::time_point flush_deadline_ = std::chrono::steady_clock::time_point::max();
    std::atomic<std::uint64_t> dropped_{0};
    int original_stderr_flags_ = -1;
    std::thread worker_;
};

// Admission bounds queued + executing closures, unlike raw asio::thread_pool.
class BoundedPool {
  public:
    BoundedPool(std::size_t threads, std::size_t capacity, Metrics &metrics);
    ~BoundedPool();
    bool submit(std::function<void()> task);
    void stop();
    void join();
    bool stopping() const noexcept {
        return stopping_.load(std::memory_order_acquire);
    }

  private:
    boost::asio::thread_pool pool_;
    const std::size_t capacity_;
    Metrics &metrics_;
    std::mutex admission_;
    std::atomic<bool> stopping_{false};
    bool joined_ = false; // owned by shutdown thread only
};

} // namespace atlas
