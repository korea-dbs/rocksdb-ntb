#pragma once

#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

class ThreadPool {
 public:
  explicit ThreadPool(size_t n) {
    for (size_t i = 0; i < n; ++i) {
      workers_.emplace_back([this]() {
        while (true) {
          std::function<void()> job;

          {
            std::unique_lock<std::mutex> lk(mu_);
            cv_.wait(lk, [this]() { return stop_ || !jobs_.empty(); });

            if (stop_ && jobs_.empty())
              return;

            job = std::move(jobs_.front());
            jobs_.pop();
          }

          job();
        }
      });
    }
  }

  ~ThreadPool() {
    {
      std::lock_guard<std::mutex> lk(mu_);
      stop_ = true;
    }

    cv_.notify_all();

    for (auto& t : workers_)
      t.join();
  }

  void Submit(std::function<void()> job) {
    {
      std::lock_guard<std::mutex> lk(mu_);
      jobs_.push(std::move(job));
    }

    cv_.notify_one();
  }

 private:
  std::vector<std::thread> workers_;
  std::queue<std::function<void()>> jobs_;
  std::mutex mu_;
  std::condition_variable cv_;
  bool stop_ = false;
};
