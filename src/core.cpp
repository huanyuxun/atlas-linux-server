#include "atlas/core.hpp"
#include <algorithm>
#include <bit>
#include <boost/asio/post.hpp>
#include <cassert>
#include <cerrno>
#include <charconv>
#include <cmath>
#include <ctime>
#include <fcntl.h>
#include <fstream>
#include <iomanip>
#include <limits>
#include <set>
#include <sstream>
#include <stdexcept>
#include <unistd.h>

namespace atlas {
namespace {
std::string trim(std::string s) {
    auto first = s.find_first_not_of(" \t\r\n");
    if (first == std::string::npos)
        return {};
    return s.substr(first, s.find_last_not_of(" \t\r\n") - first + 1);
}
std::uint64_t number(const std::string &key, const std::string &value) {
    std::uint64_t n = 0;
    auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), n);
    if (error != std::errc{} || end != value.data() + value.size() || value.empty())
        throw std::runtime_error("invalid unsigned integer for " + key);
    return n;
}
int severity(const std::string &level) {
    if (level == "debug")
        return 0;
    if (level == "info")
        return 1;
    if (level == "warn")
        return 2;
    if (level == "error")
        return 3;
    if (level == "off")
        return 4;
    throw std::runtime_error("unknown log level: " + level);
}
std::string escape(const std::string &value) {
    std::string out;
    out.reserve(value.size() + 16);
    constexpr char hex[] = "0123456789abcdef";
    for (unsigned char c : value) {
        if (c == '"' || c == '\\') {
            out += '\\';
            out += static_cast<char>(c);
        } else if (c < 32 || c >= 127) {
            out += "\\u00";
            out += hex[c >> 4];
            out += hex[c & 15];
        } else
            out += static_cast<char>(c);
    }
    return out;
}
} // namespace

Config Config::load(const std::string &path) {
    Config config;
    if (path.empty()) {
        config.validate();
        return config;
    }
    std::ifstream input(path);
    if (!input)
        throw std::runtime_error("cannot read config: " + path);
    std::set<std::string> seen;
    std::string line;
    std::size_t line_number = 0;
    while (std::getline(input, line)) {
        ++line_number;
        if (line.size() > 4096)
            throw std::runtime_error("config line too long");
        line = trim(line.substr(0, line.find('#')));
        if (line.empty())
            continue;
        auto eq = line.find('=');
        if (eq == std::string::npos)
            throw std::runtime_error("expected key=value at config line " + std::to_string(line_number));
        auto key = trim(line.substr(0, eq));
        auto value = trim(line.substr(eq + 1));
        if (!seen.insert(key).second)
            throw std::runtime_error("duplicate config key: " + key);
        if (key == "address") {
            config.address = value;
            continue;
        }
        if (key == "metrics_file") {
            config.metrics_file = value;
            continue;
        }
        if (key == "log_level") {
            config.log_level = value;
            continue;
        }
        auto n = number(key, value);
        if (key == "port") {
            if (n > 65535)
                throw std::runtime_error("port must fit uint16");
            config.port = static_cast<std::uint16_t>(n);
            continue;
        }
#define SIZE_FIELD(name)                                                                                     \
    if (key == #name) {                                                                                      \
        if (n > std::numeric_limits<std::size_t>::max())                                                     \
            throw std::runtime_error("overflow: " + key);                                                    \
        config.name = static_cast<std::size_t>(n);                                                           \
        continue;                                                                                            \
    }
        SIZE_FIELD(io_threads)
        SIZE_FIELD(compute_threads) SIZE_FIELD(max_connections) SIZE_FIELD(max_frame_bytes)
            SIZE_FIELD(max_send_bytes) SIZE_FIELD(max_send_messages) SIZE_FIELD(max_inflight_per_session)
                SIZE_FIELD(max_inflight_global) SIZE_FIELD(compute_queue) SIZE_FIELD(memory_budget_bytes)
                    SIZE_FIELD(store_bytes) SIZE_FIELD(store_entries) SIZE_FIELD(log_queue)
#undef SIZE_FIELD
#define TIME_FIELD(name)                                                                                     \
    if (key == #name) {                                                                                      \
        if (n > static_cast<std::uint64_t>(std::numeric_limits<int>::max()))                                 \
            throw std::runtime_error("overflow: " + key);                                                    \
        config.name = static_cast<int>(n);                                                                   \
        continue;                                                                                            \
    }
                        TIME_FIELD(connect_timeout_ms) TIME_FIELD(read_timeout_ms) TIME_FIELD(idle_timeout_ms)
                            TIME_FIELD(write_timeout_ms) TIME_FIELD(business_timeout_ms)
                                TIME_FIELD(downstream_timeout_ms) TIME_FIELD(shutdown_timeout_ms)
                                    TIME_FIELD(metrics_interval_ms)
#undef TIME_FIELD
                                        throw std::runtime_error("unknown config key: " + key);
    }
    if (input.bad())
        throw std::runtime_error("error reading config: " + path);
    config.validate();
    return config;
}

void Config::validate() const {
    if (address.empty())
        throw std::runtime_error("address cannot be empty");
    (void)severity(log_level);
    auto positive = [](std::size_t n, const char *key) {
        if (n == 0 || n > (std::uint64_t{1} << 40))
            throw std::runtime_error(std::string(key) + " must be between 1 and 2^40");
    };
#define POS(name) positive(name, #name)
    POS(max_connections);
    POS(max_send_bytes);
    POS(max_send_messages);
    POS(max_inflight_per_session);
    POS(max_inflight_global);
    POS(compute_queue);
    POS(memory_budget_bytes);
    POS(store_bytes);
    POS(store_entries);
    POS(log_queue);
#undef POS
    if (!io_threads || io_threads > 256 || !compute_threads || compute_threads > 256)
        throw std::runtime_error("worker thread count must be 1..256");
    if (max_frame_bytes < 16 || max_frame_bytes > 16 * 1024 * 1024)
        throw std::runtime_error("max_frame_bytes must be 16..16777216");
    for (int timeout : {connect_timeout_ms, read_timeout_ms, idle_timeout_ms, write_timeout_ms,
                        business_timeout_ms, downstream_timeout_ms, shutdown_timeout_ms, metrics_interval_ms})
        if (timeout < 1 || timeout > 86400000)
            throw std::runtime_error("timeouts and metrics interval must be 1..86400000 ms");
    if (log_queue > 1000000)
        throw std::runtime_error("log_queue must be <=1000000");
}

bool Budget::acquire(std::size_t bytes) noexcept {
    auto current = used_.load(std::memory_order_relaxed);
    do {
        if (bytes > limit_ || current > limit_ - bytes)
            return false;
    } while (!used_.compare_exchange_weak(current, current + bytes, std::memory_order_relaxed));
    return true;
}
void Budget::release(std::size_t bytes) noexcept {
    auto before = used_.fetch_sub(bytes, std::memory_order_relaxed);
    assert(before >= bytes);
    (void)before;
}

void Metrics::observe(std::uint64_t us) noexcept {
    // Power-of-two upper bounds in microseconds; no unbounded sample vector.
    auto index = us <= 1 ? 0U : std::min(31U, static_cast<unsigned>(std::bit_width(us - 1)));
    latency_[index].fetch_add(1, std::memory_order_relaxed);
    latency_sum_us_.fetch_add(us, std::memory_order_relaxed);
    completed_.fetch_add(1, std::memory_order_relaxed);
}

std::string Metrics::json(const Budget &budget) const {
    std::array<std::uint64_t, 32> bins{};
    std::uint64_t count = 0;
    for (std::size_t i = 0; i < bins.size(); ++i) {
        bins[i] = latency_[i].load(std::memory_order_relaxed);
        count += bins[i];
    }
    auto percentile = [&](double p) -> std::uint64_t {
        if (!count)
            return 0;
        auto target = static_cast<std::uint64_t>(std::ceil(static_cast<double>(count) * p));
        std::uint64_t cumulative = 0;
        for (std::size_t i = 0; i < bins.size(); ++i) {
            cumulative += bins[i];
            if (cumulative >= target)
                return std::uint64_t{1} << i;
        }
        return std::uint64_t{1} << 31;
    };
    auto n = requests.load(std::memory_order_relaxed);
    auto complete = completed_.load(std::memory_order_relaxed);
    auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - started_).count();
    std::ostringstream out;
    out << std::fixed << std::setprecision(3) << "{\"connections\":" << connections.load()
        << ",\"requests\":" << n << ",\"completed\":" << complete << ",\"qps\":" << recent_qps_.load()
        << ",\"lifetime_qps\":" << static_cast<double>(n) / std::max(elapsed, .001)
        << ",\"errors\":" << errors.load()
        << ",\"error_rate\":" << (n ? static_cast<double>(errors.load()) / static_cast<double>(n) : 0)
        << ",\"rejected\":" << rejected.load() << ",\"timeouts\":" << timeouts.load()
        << ",\"inflight\":" << inflight.load() << ",\"compute_pending\":" << compute_pending.load()
        << ",\"send_bytes\":" << send_bytes.load() << ",\"send_messages\":" << send_messages.load()
        << ",\"managed_bytes\":" << budget.used() << ",\"memory_used_bytes\":" << budget.used()
        << ",\"memory_budget_bytes\":" << budget.limit() << ",\"rss_bytes\":" << rss_bytes_.load()
        << ",\"cpu_percent\":" << cpu_percent_.load() << ",\"log_dropped\":" << log_dropped.load()
        << ",\"mean_latency_us\":"
        << (complete ? static_cast<double>(latency_sum_us_.load()) / static_cast<double>(complete) : 0)
        << ",\"p95_latency_us_upper\":" << percentile(.95) << ",\"p99_latency_us_upper\":" << percentile(.99)
        << ",\"uptime_seconds\":" << elapsed << '}';
    return out.str();
}

void Metrics::sample_process() {
    auto now = std::chrono::steady_clock::now();
    double seconds = std::chrono::duration<double>(now - last_sample_).count();
    auto n = requests.load(std::memory_order_relaxed);
    if (seconds > 0)
        recent_qps_.store(static_cast<double>(n - last_requests_) / seconds);
    last_requests_ = n;
    last_sample_ = now;
    std::ifstream stat("/proc/self/stat");
    std::string line;
    if (std::getline(stat, line)) {
        // comm (field 2) may contain spaces and parentheses.
        auto end = line.rfind(')');
        if (end != std::string::npos) {
            std::istringstream fields(line.substr(end + 2));
            std::string token;
            std::uint64_t ticks = 0;
            bool valid = true;
            for (int field = 3; field <= 15; ++field) {
                if (!(fields >> token)) {
                    valid = false;
                    break;
                }
                if (field == 14 || field == 15) {
                    std::uint64_t value = 0;
                    auto [ptr, ec] = std::from_chars(token.data(), token.data() + token.size(), value);
                    if (ec != std::errc{} || ptr != token.data() + token.size()) {
                        valid = false;
                        break;
                    }
                    ticks += value;
                }
            }
            auto hz = ::sysconf(_SC_CLK_TCK);
            if (valid && hz > 0) {
                if (sampled_ && seconds > 0 && ticks >= last_ticks_)
                    cpu_percent_.store(100.0 * static_cast<double>(ticks - last_ticks_) /
                                       static_cast<double>(hz) / seconds);
                last_ticks_ = ticks;
                sampled_ = true;
            }
        }
    }
    std::ifstream statm("/proc/self/statm");
    std::uint64_t pages = 0, resident = 0;
    auto page_size = ::sysconf(_SC_PAGESIZE);
    if ((statm >> pages >> resident) && page_size > 0)
        rss_bytes_.store(resident * static_cast<std::uint64_t>(page_size));
}

Logger::Logger(std::size_t capacity, std::string level) : capacity_(capacity), level_(severity(level)) {
    if (!capacity_)
        throw std::invalid_argument("logger capacity must be positive");
    // Nonblocking pipes/terminals prevent a slow collector from delaying shutdown.
    // Regular filesystem writes remain subject to the OS/filesystem, as usual.
    original_stderr_flags_ = ::fcntl(STDERR_FILENO, F_GETFL);
    if (original_stderr_flags_ >= 0)
        (void)::fcntl(STDERR_FILENO, F_SETFL, original_stderr_flags_ | O_NONBLOCK);
    try {
        worker_ = std::thread([this] { run(); });
    } catch (...) {
        if (original_stderr_flags_ >= 0)
            (void)::fcntl(STDERR_FILENO, F_SETFL, original_stderr_flags_);
        throw;
    }
}
Logger::~Logger() {
    {
        std::lock_guard lock(mutex_);
        stopping_ = true;
        flush_deadline_ = std::chrono::steady_clock::now() + std::chrono::milliseconds(100);
    }
    wake_.notify_one();
    if (worker_.joinable())
        worker_.join();
    if (original_stderr_flags_ >= 0)
        (void)::fcntl(STDERR_FILENO, F_SETFL, original_stderr_flags_);
}
void Logger::log(std::string level, std::uint64_t connection, std::uint64_t request,
                 std::string text) noexcept {
    try {
        if (severity(level) < level_)
            return;
        if (text.size() > 512)
            text.resize(512);
        Entry entry{std::chrono::system_clock::now(),
                    std::hash<std::thread::id>{}(std::this_thread::get_id()),
                    connection,
                    request,
                    std::move(level),
                    std::move(text)};
        std::unique_lock lock(mutex_, std::try_to_lock);
        if (!lock || stopping_ || queue_.size() >= capacity_) {
            ++dropped_;
            return;
        }
        queue_.push_back(std::move(entry));
        lock.unlock();
        wake_.notify_one();
    } catch (...) {
        ++dropped_;
    }
}
void Logger::run() noexcept {
    for (;;) {
        Entry entry;
        {
            std::unique_lock lock(mutex_);
            wake_.wait(lock, [this] { return stopping_ || !queue_.empty(); });
            if (stopping_ && std::chrono::steady_clock::now() >= flush_deadline_) {
                dropped_.fetch_add(queue_.size());
                queue_.clear();
                return;
            }
            if (queue_.empty())
                return;
            entry = std::move(queue_.front());
            queue_.pop_front();
        }
        try {
            auto timestamp = std::chrono::system_clock::to_time_t(entry.time);
            std::tm utc{};
            ::gmtime_r(&timestamp, &utc);
            auto ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(entry.time.time_since_epoch()).count() %
                1000;
            std::ostringstream out;
            out << "{\"time\":\"" << std::put_time(&utc, "%Y-%m-%dT%H:%M:%S") << '.' << std::setw(3)
                << std::setfill('0') << ms << "Z\",\"level\":\"" << entry.level
                << "\",\"thread\":" << entry.thread << ",\"connection\":" << entry.connection
                << ",\"request\":" << entry.request << ",\"message\":\"" << escape(entry.text) << "\"}\n";
            auto line = out.str();
            auto result = ::write(STDERR_FILENO, line.data(), line.size());
            // Logs are best effort: never retry a congested or failed sink.
            if (result < 0 || static_cast<std::size_t>(result) != line.size())
                ++dropped_;
        } catch (...) {
            ++dropped_;
        }
    }
}

BoundedPool::BoundedPool(std::size_t threads, std::size_t capacity, Metrics &metrics)
    : pool_(threads), capacity_(capacity), metrics_(metrics) {
    if (!threads || !capacity)
        throw std::invalid_argument("pool limits must be positive");
}
BoundedPool::~BoundedPool() {
    stop();
    join();
}
bool BoundedPool::submit(std::function<void()> task) {
    // A short mutex ensures stop() cannot race with posting a new admitted task.
    std::lock_guard lock(admission_);
    if (stopping_.load(std::memory_order_relaxed) || metrics_.compute_pending.load() >= capacity_)
        return false;
    metrics_.compute_pending.fetch_add(1);
    try {
        boost::asio::post(pool_, [this, task = std::move(task)]() mutable noexcept {
            try {
                task();
            } catch (...) {
                metrics_.errors.fetch_add(1);
            }
            // Completion of the actual worker releases its permit. Cancelling a
            // request never makes running/queued work disappear from admission.
            metrics_.compute_pending.fetch_sub(1);
        });
    } catch (...) {
        metrics_.compute_pending.fetch_sub(1);
        throw;
    }
    return true;
}
void BoundedPool::stop() {
    std::lock_guard lock(admission_);
    stopping_.store(true, std::memory_order_release);
}
void BoundedPool::join() {
    stop();
    if (!joined_) {
        pool_.join();
        joined_ = true;
    }
}

} // namespace atlas
