#include "atlas/service.hpp"
#include <algorithm>
#include <bit>
#include <charconv>
#include <limits>
#include <map>
#include <stdexcept>

namespace atlas {
namespace {
constexpr std::size_t entry_overhead = 128;
RepositoryResult failure(Status status, std::string message) {
    return {status, std::nullopt, std::move(message)};
}
Response error(Status status, std::string_view message) {
    return {static_cast<std::uint16_t>(status), {message.begin(), message.end()}};
}
void append64(std::vector<std::uint8_t> &out, std::uint64_t value) {
    for (int shift = 56; shift >= 0; shift -= 8)
        out.push_back(static_cast<std::uint8_t>(value >> shift));
}
std::uint64_t read64(const std::vector<std::uint8_t> &data, std::size_t offset) {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < 8; ++i)
        value = (value << 8) | data[offset + i];
    return value;
}
Response to_response(RepositoryResult result) {
    if (result.status != Status::ok)
        return error(result.status, result.message);
    Response response;
    if (result.record) {
        response.payload.reserve(8 + result.record->value.size());
        append64(response.payload, result.record->version);
        response.payload.insert(response.payload.end(), result.record->value.begin(),
                                result.record->value.end());
    }
    return response;
}
} // namespace

struct MemoryRepository::Impl {
    Impl(const Config &config, Budget &b)
        : budget(b), max_bytes(config.store_bytes), max_entries(config.store_entries),
          downstream_timeout(config.downstream_timeout_ms) {}
    ~Impl() {
        budget.release(bytes);
    }
    Budget &budget;
    const std::size_t max_bytes, max_entries;
    const std::chrono::milliseconds downstream_timeout;
    std::timed_mutex mutex;
    std::map<std::string, Record, std::less<>> records;
    std::size_t bytes = 0;
    std::uint64_t next_version = 1;

    bool lock(std::unique_lock<std::timed_mutex> &guard, const Cancel &cancel) {
        const auto deadline =
            std::min(cancel.deadline, std::chrono::steady_clock::now() + downstream_timeout);
        // Cancellation is cooperative even while waiting for a contended adapter.
        // A database adapter should use its driver's deadline/cancellation API.
        while (!cancel.stopped()) {
            auto now = std::chrono::steady_clock::now();
            if (now >= deadline)
                return false;
            if (guard.try_lock_until(std::min(deadline, now + std::chrono::milliseconds(5))))
                return !cancel.stopped();
        }
        return false;
    }
    RepositoryResult put_locked(std::string_view key, std::string_view value, std::uint64_t expected,
                                const Cancel &cancel) {
        if (cancel.stopped())
            return failure(Status::timeout, "request cancelled or timed out");
        auto existing = records.find(key);
        if (expected && (existing == records.end() || existing->second.version != expected))
            return failure(Status::conflict, "version mismatch");
        if (existing == records.end() && records.size() >= max_entries)
            return failure(Status::overloaded, "store entry limit");
        if (next_version == std::numeric_limits<std::uint64_t>::max())
            return failure(Status::overloaded, "version space exhausted");
        const auto old_cost = existing == records.end()
                                  ? 0
                                  : entry_overhead + existing->first.size() + existing->second.value.size();
        const auto new_cost = entry_overhead + key.size() + value.size();
        if (new_cost > max_bytes || bytes - old_cost > max_bytes - new_cost)
            return failure(Status::overloaded, "store byte limit");
        const auto extra = new_cost > old_cost ? new_cost - old_cost : 0;
        if (!budget.acquire(extra))
            return failure(Status::overloaded, "global memory limit");
        try {
            Record new_record{next_version, std::string(value)};
            if (existing == records.end()) {
                existing = records.emplace(std::string(key), std::move(new_record)).first;
            } else {
                existing->second.value.swap(new_record.value); // no-throw commit
                existing->second.version = next_version;
            }
        } catch (...) {
            budget.release(extra);
            throw;
        }
        ++next_version;
        bytes = bytes - old_cost + new_cost;
        if (old_cost > new_cost)
            budget.release(old_cost - new_cost);
        // The commit is linearized under the mutex. A later timeout or transport
        // failure cannot undo this write; callers may verify version after retry.
        return {Status::ok, existing->second, {}};
    }
};

MemoryRepository::MemoryRepository(const Config &config, Budget &budget)
    : impl_(std::make_unique<Impl>(config, budget)) {}
MemoryRepository::~MemoryRepository() = default;

RepositoryResult MemoryRepository::get(std::string_view key, const Cancel &cancel) {
    std::unique_lock lock(impl_->mutex, std::defer_lock);
    if (!impl_->lock(lock, cancel))
        return failure(Status::timeout, "repository deadline");
    auto it = impl_->records.find(key);
    if (it == impl_->records.end())
        return failure(Status::not_found, "key not found");
    return {Status::ok, it->second, {}};
}
RepositoryResult MemoryRepository::put(std::string_view key, std::string_view value, std::uint64_t expected,
                                       const Cancel &cancel) {
    std::unique_lock lock(impl_->mutex, std::defer_lock);
    if (!impl_->lock(lock, cancel))
        return failure(Status::timeout, "repository deadline");
    return impl_->put_locked(key, value, expected, cancel);
}
RepositoryResult MemoryRepository::erase(std::string_view key, const Cancel &cancel) {
    std::unique_lock lock(impl_->mutex, std::defer_lock);
    if (!impl_->lock(lock, cancel))
        return failure(Status::timeout, "repository deadline");
    auto it = impl_->records.find(key);
    if (it == impl_->records.end())
        return failure(Status::not_found, "key not found");
    auto cost = entry_overhead + it->first.size() + it->second.value.size();
    impl_->records.erase(it);
    impl_->bytes -= cost;
    impl_->budget.release(cost);
    return {};
}
RepositoryResult MemoryRepository::add(std::string_view key, std::int64_t delta, const Cancel &cancel) {
    std::unique_lock lock(impl_->mutex, std::defer_lock);
    if (!impl_->lock(lock, cancel))
        return failure(Status::timeout, "repository deadline");
    auto it = impl_->records.find(key);
    std::int64_t value = 0;
    if (it != impl_->records.end()) {
        const auto &text = it->second.value;
        auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
        if (ec != std::errc{} || end != text.data() + text.size() || text.empty())
            return failure(Status::invalid, "value is not a signed 64-bit decimal integer");
    }
    if ((delta > 0 && value > std::numeric_limits<std::int64_t>::max() - delta) ||
        (delta < 0 && value < std::numeric_limits<std::int64_t>::min() - delta))
        return failure(Status::invalid, "integer overflow");
    return impl_->put_locked(key, std::to_string(value + delta), 0, cancel);
}

Service::Service(const Config &config, Budget &budget, Metrics &metrics)
    : Service(std::make_unique<MemoryRepository>(config, budget), budget, metrics) {}
Service::Service(std::unique_ptr<Repository> repository, Budget &budget, Metrics &metrics)
    : repository_(std::move(repository)), budget_(budget), metrics_(metrics) {
    if (!repository_)
        throw std::invalid_argument("repository must not be null");
}

Response Service::execute(const Request &request, const Cancel &cancel) {
    try {
        if (cancel.stopped())
            return error(Status::timeout, "request cancelled or timed out");
        const auto &payload = request.payload;
        if (request.type >= 1 && request.type <= 4) {
            if (payload.size() < 2)
                return error(Status::invalid, "missing key length");
            const auto length =
                static_cast<std::size_t>((static_cast<unsigned>(payload[0]) << 8) | payload[1]);
            if (!length || length > 128 || payload.size() < 2 + length)
                return error(Status::invalid, "key must contain 1..128 bytes");
            const auto offset = 2 + length;
            std::string_view key(reinterpret_cast<const char *>(payload.data() + 2), length);
            switch (request.type) {
            case 1: {
                if (payload.size() < offset + 8)
                    return error(Status::invalid, "missing expected version");
                const auto expected = read64(payload, offset);
                std::string_view value(reinterpret_cast<const char *>(payload.data() + offset + 8),
                                       payload.size() - offset - 8);
                return to_response(repository_->put(key, value, expected, cancel));
            }
            case 2:
                if (payload.size() != offset)
                    return error(Status::invalid, "unexpected GET data");
                return to_response(repository_->get(key, cancel));
            case 3:
                if (payload.size() != offset)
                    return error(Status::invalid, "unexpected DELETE data");
                return to_response(repository_->erase(key, cancel));
            case 4:
                if (payload.size() != offset + 8)
                    return error(Status::invalid, "ADD requires signed 64-bit delta");
                return to_response(
                    repository_->add(key, std::bit_cast<std::int64_t>(read64(payload, offset)), cancel));
            default:
                break;
            }
        }
        if (request.type == 5) {
            if (payload.size() < 5 || payload.size() > 4100)
                return error(Status::invalid, "HASH requires iterations and 1..4096 seed bytes");
            std::uint32_t iterations = 0;
            for (std::size_t i = 0; i < 4; ++i)
                iterations = (iterations << 8) | payload[i];
            if (!iterations || iterations > 10000000)
                return error(Status::invalid, "HASH iterations must be 1..10000000");
            // Deterministic demonstrative CPU job, NOT a cryptographic hash.
            std::uint64_t hash = 14695981039346656037ULL;
            for (std::size_t i = 4; i < payload.size(); ++i)
                hash = (hash ^ payload[i]) * 1099511628211ULL;
            const auto seed_length = payload.size() - 4;
            for (std::uint32_t i = 0; i < iterations; ++i) {
                if ((i & 255U) == 0 && cancel.stopped())
                    return error(Status::timeout, "computation deadline");
                hash ^= static_cast<std::uint64_t>(payload[4 + i % seed_length]) + i;
                hash *= 1099511628211ULL;
                hash = std::rotl(hash, 13);
            }
            if (cancel.stopped())
                return error(Status::timeout, "computation deadline");
            Response response;
            append64(response.payload, hash);
            return response;
        }
        if (request.type == 6) {
            if (!payload.empty())
                return error(Status::invalid, "STATS payload must be empty");
            auto snapshot = metrics_.json(budget_);
            return {0, {snapshot.begin(), snapshot.end()}};
        }
        return error(Status::invalid, "unknown message type");
    } catch (const std::bad_alloc &) {
        return error(Status::overloaded, "allocation failed");
    } catch (const std::exception &) {
        return error(Status::internal, "business operation failed");
    } catch (...) {
        return error(Status::internal, "unknown business failure");
    }
}

} // namespace atlas
