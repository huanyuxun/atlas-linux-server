#include "atlas/server.hpp"
#include <iostream>
namespace atlas {
Server::Server(asio::io_context &io, const Config &c, Budget &b, Metrics &m, Logger &l, BoundedPool &p,
               Service &svc)
    : io_(io), strand_(asio::make_strand(io)), acceptor_(strand_), shutdown_timer_(strand_),
      retry_timer_(strand_), cfg_(c), budget_(b), metrics_(m), logger_(l), pool_(p), service_(svc) {
    tcp::endpoint ep(asio::ip::make_address(c.address), c.port);
    acceptor_.open(ep.protocol());
    acceptor_.set_option(tcp::acceptor::reuse_address(true));
    acceptor_.bind(ep);
    acceptor_.listen();
}
uint16_t Server::port() const {
    return acceptor_.local_endpoint().port();
}
void Server::start() {
    asio::co_spawn(strand_, accept_loop(), [this](std::exception_ptr e) {
        if (e) {
            logger_.log("error", 0, 0, "accept loop failed");
            shutdown();
        }
    });
}
asio::awaitable<void> Server::accept_loop() {
    while (!stopping_) {
        tcp::socket socket(asio::make_strand(io_));
        boost::system::error_code ec;
        co_await acceptor_.async_accept(socket, asio::redirect_error(asio::use_awaitable, ec));
        if (stopping_)
            break;
        if (ec) {
            logger_.log("warn", 0, 0, "accept: " + ec.message());
            retry_timer_.expires_after(std::chrono::milliseconds(100));
            co_await retry_timer_.async_wait(asio::redirect_error(asio::use_awaitable, ec));
            continue;
        }
        if (sessions_.size() >= cfg_.max_connections || !budget_.acquire(Session::base_charge(cfg_))) {
            ++metrics_.rejected;
            socket.close(ec);
            continue;
        }
        socket.set_option(tcp::no_delay(true), ec);
        if (ec) {
            budget_.release(Session::base_charge(cfg_));
            socket.close(ec);
            ++metrics_.errors;
            continue;
        }
        const auto id = ++next_id_;
        std::shared_ptr<Session> session;
        try {
            session = std::make_shared<Session>(id, std::move(socket), cfg_, budget_, metrics_, logger_,
                                                pool_, service_, stopping_, [this](uint64_t key) {
                                                    asio::post(strand_, [this, key] {
                                                        sessions_.erase(key);
                                                        check_shutdown();
                                                    });
                                                });
        } catch (...) {
            budget_.release(Session::base_charge(cfg_));
            throw;
        }
        sessions_.emplace(id, session);
        ++metrics_.connections;
        session->start();
    }
}
void Server::shutdown() {
    asio::dispatch(strand_, [this] {
        if (stopping_.exchange(true))
            return;
        logger_.log("info", 0, 0, "shutdown: admission closed, draining sessions");
        boost::system::error_code ec;
        acceptor_.close(ec);
        retry_timer_.cancel();
        pool_.stop();
        for (auto &[id, s] : sessions_) {
            (void)id;
            s->drain();
        }
        shutdown_timer_.expires_after(std::chrono::milliseconds(cfg_.shutdown_timeout_ms));
        shutdown_timer_.async_wait([this](boost::system::error_code e) {
            if (!e) {
                logger_.log("warn", 0, 0, "shutdown deadline: cancelling sessions");
                for (auto &[id, s] : sessions_) {
                    (void)id;
                    s->force_close();
                }
            }
        });
        check_shutdown();
    });
}
void Server::check_shutdown() {
    if (stopping_ && sessions_.empty())
        shutdown_timer_.cancel();
}
} // namespace atlas
