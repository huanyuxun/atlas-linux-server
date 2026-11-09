#pragma once
#include "atlas/core.hpp"
#include "atlas/protocol.hpp"
#include <boost/asio.hpp>
#include <deque>
#include <unordered_map>
#include <utility>

namespace atlas {
namespace asio = boost::asio;
using tcp = asio::ip::tcp;
// All mutable Session state belongs to socket_.get_executor(), a unique strand.
class Session : public std::enable_shared_from_this<Session> {
  public:
    Session(uint64_t id, tcp::socket socket, const Config &, Budget &, Metrics &, Logger &, BoundedPool &,
            Service &, std::atomic<bool> &stopping, std::function<void(uint64_t)> on_close);
    ~Session();
    void start();
    void drain();
    void force_close();
    static size_t base_charge(const Config &c) {
        return 4096 + c.max_frame_bytes;
    }

  private:
    struct Job;
    struct Frame;
    asio::awaitable<void> read_loop();
    asio::awaitable<void> write_loop();
    void dispatch(Request);
    void completed(const std::shared_ptr<Job> &, Response);
    void reply(uint8_t, uint64_t, const Response &);
    void close(std::string reason);
    void maybe_finish();
    void arm_read(int milliseconds);
    uint64_t id_;
    tcp::socket socket_;
    const Config &cfg_;
    Budget &budget_;
    Metrics &metrics_;
    Logger &logger_;
    BoundedPool &pool_;
    Service &service_;
    std::atomic<bool> &stopping_;
    std::function<void(uint64_t)> on_close_;
    asio::steady_timer read_timer_, write_timer_;
    uint64_t read_epoch_ = 0, write_epoch_ = 0;
    std::unordered_map<uint64_t, std::shared_ptr<Job>> jobs_;
    std::deque<std::shared_ptr<Frame>> send_;
    size_t send_bytes_ = 0;
    bool closed_ = false, draining_ = false, writing_ = false, first_ = true;
};
} // namespace atlas
