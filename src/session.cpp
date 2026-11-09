#include "atlas/session.hpp"

namespace atlas {
using namespace std::chrono;
using boost::system::error_code;
struct Session::Job {
    Request request;
    std::shared_ptr<Cancel> cancel;
    asio::steady_timer timer;
    Budget &budget;
    Metrics &metrics;
    size_t charge;
    steady_clock::time_point start = steady_clock::now();
    bool replied = false; // strand only; cancel is the only worker-visible mutable field
    Job(Request r, asio::any_io_executor ex, Budget &b, Metrics &m, size_t bytes)
        : request(std::move(r)), cancel(std::make_shared<Cancel>()), timer(ex), budget(b), metrics(m),
          charge(bytes) {}
    ~Job() {
        budget.release(charge);
        metrics.inflight.fetch_sub(1);
    }
};
struct Session::Frame {
    std::vector<uint8_t> bytes;
    Budget &budget;
    Metrics &metrics;
    size_t charge;
    Frame(std::vector<uint8_t> b, Budget &mem, Metrics &met, size_t n)
        : bytes(std::move(b)), budget(mem), metrics(met), charge(n) {
        metrics.send_bytes += bytes.size();
        ++metrics.send_messages;
    }
    ~Frame() {
        metrics.send_bytes -= bytes.size();
        --metrics.send_messages;
        budget.release(charge);
    }
};
Session::Session(uint64_t id, tcp::socket s, const Config &c, Budget &b, Metrics &m, Logger &l,
                 BoundedPool &p, Service &svc, std::atomic<bool> &stopping,
                 std::function<void(uint64_t)> on_close)
    : id_(id), socket_(std::move(s)), cfg_(c), budget_(b), metrics_(m), logger_(l), pool_(p), service_(svc),
      stopping_(stopping), on_close_(std::move(on_close)), read_timer_(socket_.get_executor()),
      write_timer_(socket_.get_executor()) {}
Session::~Session() {
    budget_.release(base_charge(cfg_));
}
void Session::start() {
    auto self = shared_from_this();
    asio::co_spawn(socket_.get_executor(), self->read_loop(), [self](std::exception_ptr e) {
        if (e)
            self->close("reader exception");
    });
}
void Session::arm_read(int ms) {
    auto epoch = ++read_epoch_;
    read_timer_.expires_after(milliseconds(ms));
    read_timer_.async_wait([weak = weak_from_this(), epoch](error_code ec) {
        if (auto s = weak.lock(); s && !ec && !s->closed_ && epoch == s->read_epoch_) {
            ++s->metrics_.timeouts;
            s->close("read timeout");
        }
    });
}
asio::awaitable<void> Session::read_loop() {
    try {
        while (!closed_ && !draining_) {
            std::array<uint8_t, 4> header{};
            arm_read(first_ ? cfg_.connect_timeout_ms : cfg_.idle_timeout_ms);
            error_code ec;
            co_await asio::async_read(socket_, asio::buffer(header),
                                      asio::redirect_error(asio::use_awaitable, ec));
            ++read_epoch_;
            read_timer_.cancel();
            if (draining_ || closed_)
                break;
            if (ec) {
                close("read header: " + ec.message());
                break;
            }
            first_ = false;
            auto n = wire::length(header);
            if (n < wire::header_size || n > cfg_.max_frame_bytes) {
                ++metrics_.errors;
                close("invalid frame length");
                break;
            }
            // Session reserves one maximum input frame before it is admitted.
            std::vector<uint8_t> body(n);
            arm_read(cfg_.read_timeout_ms);
            co_await asio::async_read(socket_, asio::buffer(body),
                                      asio::redirect_error(asio::use_awaitable, ec));
            ++read_epoch_;
            read_timer_.cancel();
            if (draining_ || closed_)
                break;
            if (ec) {
                close("read body: " + ec.message());
                break;
            }
            dispatch(wire::decode(std::move(body)));
        }
    } catch (const std::exception &e) {
        ++metrics_.errors;
        close(e.what());
    }
    maybe_finish();
}
void Session::dispatch(Request req) {
    if (stopping_.load()) {
        drain();
        return;
    }
    ++metrics_.requests;
    if (jobs_.contains(req.id)) {
        ++metrics_.errors;
        close("duplicate outstanding request id");
        return;
    }
    if (jobs_.size() >= cfg_.max_inflight_per_session) {
        ++metrics_.rejected;
        close("session inflight limit");
        return;
    }
    auto current = metrics_.inflight.load();
    while (current < cfg_.max_inflight_global &&
           !metrics_.inflight.compare_exchange_weak(current, current + 1)) {
    }
    if (current >= cfg_.max_inflight_global) {
        ++metrics_.rejected;
        reply(req.type, req.id, wire::error(4, "global inflight limit"));
        return;
    }
    // Conservatively cover request, response and encode copies plus task metadata.
    size_t charge = 3 * cfg_.max_frame_bytes + 2048;
    if (!budget_.acquire(charge)) {
        --metrics_.inflight;
        ++metrics_.rejected;
        reply(req.type, req.id, wire::error(4, "memory budget"));
        return;
    }
    std::shared_ptr<Job> job;
    try {
        job = std::make_shared<Job>(std::move(req), socket_.get_executor(), budget_, metrics_, charge);
    } catch (...) {
        budget_.release(charge);
        --metrics_.inflight;
        throw;
    }
    job->cancel->deadline = steady_clock::now() + milliseconds(cfg_.business_timeout_ms);
    jobs_.emplace(job->request.id, job);
    job->timer.expires_at(job->cancel->deadline);
    job->timer.async_wait([weak = weak_from_this(), wj = std::weak_ptr<Job>(job)](error_code ec) {
        auto s = weak.lock();
        auto j = wj.lock();
        if (!ec && s && j && !s->closed_ && !j->replied) {
            j->cancel->cancelled = true;
            j->replied = true;
            ++s->metrics_.timeouts;
            s->metrics_.observe(
                static_cast<uint64_t>(duration_cast<microseconds>(steady_clock::now() - j->start).count()));
            s->reply(j->request.type, j->request.id, wire::error(5, "business timeout"));
        }
    });
    auto weak = weak_from_this();
    auto ex = socket_.get_executor();
    auto *svc = &service_;
    bool accepted = pool_.submit([weak, job, ex, svc] {
        Response result;
        try {
            result = svc->execute(job->request, *job->cancel);
        } catch (const std::exception &) {
            result = wire::error(6, "business exception");
        } catch (...) {
            result = wire::error(6, "unknown business exception");
        }
        // No Session member is touched on the worker. The completion owns the job
        // reservation until the strand consumes it, even if the socket has gone.
        asio::post(ex, [weak, job, result = std::move(result)]() mutable {
            if (auto s = weak.lock())
                s->completed(job, std::move(result));
        });
    });
    if (!accepted) {
        ++metrics_.rejected;
        completed(job, wire::error(4, "compute queue full"));
    }
}
void Session::completed(const std::shared_ptr<Job> &job, Response result) {
    job->timer.cancel();
    if (!closed_ && !job->replied) {
        job->replied = true;
        if (job->cancel->stopped() && result.status != 5) {
            result = wire::error(5, "business deadline");
        }
        if (result.status == 5)
            ++metrics_.timeouts;
        metrics_.observe(
            static_cast<uint64_t>(duration_cast<microseconds>(steady_clock::now() - job->start).count()));
        reply(job->request.type, job->request.id, result);
    }
    jobs_.erase(job->request.id);
    maybe_finish();
}
void Session::reply(uint8_t type, uint64_t id, const Response &result) {
    if (closed_)
        return;
    if (result.status) {
        ++metrics_.errors;
        logger_.log("debug", id_, id, "response status=" + std::to_string(result.status));
    }
    size_t size = result.payload.size() + 16;
    if (size > cfg_.max_frame_bytes + 4 || size > cfg_.max_send_bytes ||
        send_bytes_ > cfg_.max_send_bytes - size || send_.size() >= cfg_.max_send_messages) {
        ++metrics_.rejected;
        close("slow client/send queue limit");
        return;
    }
    const size_t charge = size + 128;
    if (!budget_.acquire(charge)) {
        ++metrics_.rejected;
        close("send memory budget");
        return;
    }
    std::shared_ptr<Frame> f;
    try {
        f = std::make_shared<Frame>(wire::encode(type, id, result), budget_, metrics_, charge);
    } catch (...) {
        budget_.release(charge);
        throw;
    }
    send_.push_back(std::move(f));
    send_bytes_ += size;
    if (!writing_) {
        writing_ = true;
        auto self = shared_from_this();
        asio::co_spawn(socket_.get_executor(), write_loop(), [self](std::exception_ptr e) {
            if (e)
                self->close("writer exception");
        });
    }
}
asio::awaitable<void> Session::write_loop() {
    while (!closed_ && !send_.empty()) {
        auto frame = send_.front(); // owns bytes across cancellation and close clearing send_
        auto epoch = ++write_epoch_;
        write_timer_.expires_after(milliseconds(cfg_.write_timeout_ms));
        write_timer_.async_wait([weak = weak_from_this(), epoch](error_code ec) {
            if (auto s = weak.lock(); s && !ec && !s->closed_ && epoch == s->write_epoch_) {
                ++s->metrics_.timeouts;
                s->close("write timeout");
            }
        });
        error_code ec;
        co_await asio::async_write(socket_, asio::buffer(frame->bytes),
                                   asio::redirect_error(asio::use_awaitable, ec));
        ++write_epoch_;
        write_timer_.cancel();
        if (closed_)
            break;
        if (ec) {
            close("write: " + ec.message());
            break;
        }
        send_bytes_ -= frame->bytes.size();
        send_.pop_front();
    }
    writing_ = false;
    maybe_finish();
}
void Session::maybe_finish() {
    if (draining_ && jobs_.empty() && send_.empty() && !writing_)
        close("drained");
}
void Session::drain() {
    auto self = shared_from_this();
    asio::dispatch(socket_.get_executor(), [self] {
        if (self->closed_ || self->draining_)
            return;
        self->draining_ = true;
        ++self->read_epoch_;
        self->read_timer_.cancel();
        // Stop inbound requests without cancelling pending output.
        error_code ec;
        self->socket_.shutdown(tcp::socket::shutdown_receive, ec);
        self->maybe_finish();
    });
}
void Session::force_close() {
    auto self = shared_from_this();
    asio::dispatch(socket_.get_executor(), [self] { self->close("shutdown deadline"); });
}
void Session::close(std::string reason) {
    if (closed_)
        return;
    closed_ = true;
    ++read_epoch_;
    ++write_epoch_;
    read_timer_.cancel();
    write_timer_.cancel();
    error_code ec;
    socket_.cancel(ec);
    socket_.shutdown(tcp::socket::shutdown_both, ec);
    socket_.close(ec);
    for (auto &[id, j] : jobs_) {
        (void)id;
        j->cancel->cancelled = true;
        j->timer.cancel();
    }
    jobs_.clear();
    send_.clear();
    send_bytes_ = 0;
    --metrics_.connections;
    logger_.log("debug", id_, 0, std::move(reason));
    on_close_(id_);
}
} // namespace atlas
