#pragma once

#include "atlas/core.hpp"
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

namespace atlas {

struct Request {
    std::uint8_t type = 0;
    std::uint64_t id = 0;
    std::vector<std::uint8_t> payload;
};
struct Response {
    std::uint16_t status = 0;
    std::vector<std::uint8_t> payload;
};
struct Cancel {
    std::atomic<bool> cancelled{false};
    std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::time_point::max();
    bool stopped() const noexcept {
        return cancelled.load(std::memory_order_relaxed) || std::chrono::steady_clock::now() >= deadline;
    }
};

enum class Status : std::uint16_t {
    ok = 0,
    invalid = 1,
    not_found = 2,
    conflict = 3,
    overloaded = 4,
    timeout = 5,
    internal = 6,
    shutting_down = 7
};

struct Record {
    std::uint64_t version;
    std::string value;
};
struct RepositoryResult {
    Status status = Status::ok;
    std::optional<Record> record;
    std::string message;
};

// Synchronous adapter contract, invoked exclusively on compute workers. Future
// database adapters must provide bounded acquisition/operation timeouts and use
// a bounded connection pool; this demo has no database connections or persistence.
class Repository {
  public:
    virtual ~Repository() = default;
    virtual RepositoryResult get(std::string_view key, const Cancel &) = 0;
    virtual RepositoryResult put(std::string_view key, std::string_view value, std::uint64_t expected,
                                 const Cancel &) = 0;
    virtual RepositoryResult erase(std::string_view key, const Cancel &) = 0;
    virtual RepositoryResult add(std::string_view key, std::int64_t delta, const Cancel &) = 0;
};

class MemoryRepository final : public Repository {
  public:
    MemoryRepository(const Config &, Budget &);
    ~MemoryRepository() override;
    RepositoryResult get(std::string_view, const Cancel &) override;
    RepositoryResult put(std::string_view, std::string_view, std::uint64_t, const Cancel &) override;
    RepositoryResult erase(std::string_view, const Cancel &) override;
    RepositoryResult add(std::string_view, std::int64_t, const Cancel &) override;

  private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

class Service {
  public:
    Service(const Config &, Budget &, Metrics &);
    Service(std::unique_ptr<Repository>, Budget &, Metrics &);
    Response execute(const Request &, const Cancel &);

  private:
    std::unique_ptr<Repository> repository_;
    Budget &budget_;
    Metrics &metrics_;
};

} // namespace atlas
