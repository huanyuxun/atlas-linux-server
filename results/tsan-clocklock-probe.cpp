// Standalone one-thread standard-library probe. No Atlas code involved.
#include <chrono>
#include <mutex>
int main() {
    std::timed_mutex mutex;
    std::unique_lock guard(mutex, std::defer_lock);
    if (!guard.try_lock_until(std::chrono::steady_clock::now() + std::chrono::seconds(1)))
        return 1;
    return 0;
}
