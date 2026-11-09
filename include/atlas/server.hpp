#pragma once
#include "atlas/session.hpp"
#include <unordered_map>
namespace atlas {
class Server {
  public:
    Server(asio::io_context &, const Config &, Budget &, Metrics &, Logger &, BoundedPool &, Service &);
    void start();
    void shutdown();
    uint16_t port() const;

  private:
    asio::awaitable<void> accept_loop();
    void check_shutdown();
    asio::io_context &io_;
    asio::strand<asio::io_context::executor_type> strand_;
    tcp::acceptor acceptor_;
    asio::steady_timer shutdown_timer_, retry_timer_;
    const Config &cfg_;
    Budget &budget_;
    Metrics &metrics_;
    Logger &logger_;
    BoundedPool &pool_;
    Service &service_;
    std::unordered_map<uint64_t, std::shared_ptr<Session>> sessions_;
    std::atomic<bool> stopping_{false};
    uint64_t next_id_ = 0;
};
} // namespace atlas
