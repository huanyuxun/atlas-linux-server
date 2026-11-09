#include "atlas/server.hpp"
#include <condition_variable>
#include <csignal>
#include <cstdio>
#include <exception>
#include <fstream>
#include <iostream>
#include <thread>

int main(int argc, char **argv) {
    try {
        // A disconnected log collector must never terminate the process.
        std::signal(SIGPIPE, SIG_IGN);
        std::string path;
        if (argc == 3 && std::string(argv[1]) == "--config")
            path = argv[2];
        else if (argc != 1) {
            std::cerr << "Usage: atlas_server [--config FILE]\n";
            return 2;
        }
        auto cfg = path.empty() ? atlas::Config{} : atlas::Config::load(path);
        cfg.validate();
        atlas::Budget budget(cfg.memory_budget_bytes);
        atlas::Metrics metrics;
        atlas::Logger logger(cfg.log_queue, cfg.log_level);
        // Dependencies outlive pool tasks and every I/O completion handler.
        atlas::Service service(cfg, budget, metrics);
        atlas::BoundedPool pool(cfg.compute_threads, cfg.compute_queue, metrics);
        atlas::asio::io_context io;
        auto guard = atlas::asio::make_work_guard(io);
        atlas::Server server(io, cfg, budget, metrics, logger, pool, service);
        const auto bound_port = server.port();
        atlas::asio::signal_set signals(io, SIGINT, SIGTERM);
        signals.async_wait([&](boost::system::error_code ec, int) {
            if (!ec)
                server.shutdown();
        });

        std::mutex monitor_mutex;
        std::condition_variable monitor_cv;
        bool monitor_stop = false;
        std::thread monitor;
        std::vector<std::thread> workers;
        // No vector reallocation may throw after a joinable thread was created.
        workers.reserve(cfg.io_threads);
        auto run_io = [&] {
            for (;;) {
                try {
                    io.run();
                    break;
                } catch (const std::exception &e) {
                    logger.log("error", 0, 0, e.what());
                    server.shutdown();
                } catch (...) {
                    logger.log("error", 0, 0, "I/O worker exception");
                    server.shutdown();
                }
            }
        };

        std::exception_ptr startup_error;
        try {
            server.start();
            for (std::size_t i = 0; i < cfg.io_threads; ++i)
                workers.emplace_back(run_io);
            monitor = std::thread([&] {
                try {
                    const auto temporary = cfg.metrics_file + ".tmp";
                    std::unique_lock lock(monitor_mutex);
                    while (!monitor_stop) {
                        lock.unlock();
                        metrics.sample_process();
                        metrics.log_dropped = logger.dropped();
                        if (!cfg.metrics_file.empty()) {
                            // A bounded, atomic latest snapshot, not an ever-growing
                            // telemetry log. File I/O belongs only to this thread.
                            std::ofstream out(temporary, std::ios::trunc);
                            out << metrics.json(budget) << '\n';
                            out.close();
                            if (!out || std::rename(temporary.c_str(), cfg.metrics_file.c_str()) != 0)
                                logger.log("warn", 0, 0, "cannot publish metrics snapshot");
                        }
                        lock.lock();
                        monitor_cv.wait_for(lock, std::chrono::milliseconds(cfg.metrics_interval_ms),
                                            [&] { return monitor_stop; });
                    }
                } catch (const std::exception &e) {
                    logger.log("error", 0, 0, std::string("monitor: ") + e.what());
                    server.shutdown();
                } catch (...) {
                    logger.log("error", 0, 0, "monitor exception");
                    server.shutdown();
                }
            });
            std::cout << "READY " << bound_port << std::endl;
            while (!pool.stopping())
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
        } catch (...) {
            // Thread creation failure must unwind through explicit joins, never
            // through a joinable std::thread destructor (which would terminate).
            startup_error = std::current_exception();
        }

        server.shutdown();
        // Programmatic shutdown (e.g. accept/monitor failure) may precede a signal.
        // Cancel the outstanding signal wait or io.run would never finish.
        atlas::asio::post(io, [&] { signals.cancel(); });
        if (workers.empty()) {
            guard.reset();
            run_io();
            pool.join();
            // Catch any completion posted while draining the compute pool.
            io.restart();
            run_io();
        } else {
            while (!pool.stopping())
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            // I/O remains live during join: shutdown timers can cancel workers,
            // and worker results retain reservations until their completion runs.
            pool.join();
            guard.reset();
            for (auto &worker : workers)
                worker.join();
        }
        {
            std::lock_guard lock(monitor_mutex);
            monitor_stop = true;
        }
        monitor_cv.notify_one();
        if (monitor.joinable())
            monitor.join();
        if (startup_error)
            std::rethrow_exception(startup_error);
        metrics.sample_process();
        std::cout << "FINAL " << metrics.json(budget) << std::endl;
        return 0;
    } catch (const std::exception &e) {
        std::cerr << "fatal: " << e.what() << '\n';
        return 1;
    }
}
