#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

class WorkerDaemon {
 public:
  WorkerDaemon(std::string shared_root,
               std::string worker_name,
               int num_workers)
      : shared_root_(std::move(shared_root)),
        worker_name_(std::move(worker_name)),
        num_workers_(num_workers),
        stop_(false) {}

  void Run() {
    const fs::path worker_dir = fs::path(shared_root_) / worker_name_;

    std::error_code ec;
    fs::create_directories(worker_dir, ec);

    std::cout << "[daemon] shared_root=" << shared_root_ << "\n";
    std::cout << "[daemon] worker_name=" << worker_name_ << "\n";
    std::cout << "[daemon] num_workers=" << num_workers_ << "\n";

    // worker thread 생성
    for (int i = 0; i < num_workers_; ++i) {
      workers_.emplace_back(&WorkerDaemon::WorkerLoop, this, i);
    }

    // join
    for (auto& th : workers_) {
      th.join();
    }
  }

 private:

  void WorkerLoop(int tid) {
    const fs::path worker_dir = fs::path(shared_root_) / worker_name_;

    {
      std::lock_guard<std::mutex> lk(io_mu_);
      std::cout << "[daemon] thread started tid=" << tid << "\n";
    }

    while (!stop_.load()) {
      bool found_job = false;

      try {
        for (const auto& entry : fs::directory_iterator(worker_dir)) {

          if (!entry.is_regular_file())
            continue;

          const std::string name = entry.path().filename().string();

          if (name.size() < 4)
            continue;

          if (name.substr(name.size() - 4) != ".req")
            continue;

          const std::string job_id =
              name.substr(0, name.size() - 4);

          const fs::path req_path = entry.path();
          const fs::path inprog_path =
              worker_dir / (job_id + ".req.inprogress");

          // job claim (atomic rename)
          std::error_code ec;
          fs::rename(req_path, inprog_path, ec);

          if (ec) {
            continue; // 다른 thread가 먼저 잡음
          }

          found_job = true;

          {
            std::lock_guard<std::mutex> lk(io_mu_);
            std::cout << "[daemon] tid=" << tid
                      << " picked job=" << job_id << "\n";
          }

          RunWorker(job_id);

          fs::remove(inprog_path, ec);
        }

      } catch (const std::exception& e) {
        std::lock_guard<std::mutex> lk(io_mu_);
        std::cerr << "[daemon] scan error: "
                  << e.what() << "\n";
      }

      if (!found_job) {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(200));
      }
    }
  }

  void RunWorker(const std::string& job_id) {

    std::string worker_bin = "nfs_compaction_worker";

    const std::string cmd =
        worker_bin + " " +
        job_id + " " +
        shared_root_ + " " +
        worker_name_;

    {
      std::lock_guard<std::mutex> lk(io_mu_);
      std::cout << "[daemon] run: " << cmd << "\n";
    }

    int rc = std::system(cmd.c_str());

    {
      std::lock_guard<std::mutex> lk(io_mu_);
      std::cout << "[daemon] job=" << job_id
                << " rc=" << rc << "\n";
    }
  }

 private:

  std::string shared_root_;
  std::string worker_name_;
  int num_workers_;

  std::atomic<bool> stop_;

  std::vector<std::thread> workers_;

  std::mutex io_mu_;
};

int main(int argc, char** argv) {

  if (argc < 4) {
    std::cerr
      << "usage: nfs_worker_daemon "
      << "<shared_root> <worker_name> <num_workers>\n";
    return 1;
  }

  const std::string shared_root = argv[1];
  const std::string worker_name = argv[2];
  const int num_workers = std::stoi(argv[3]);

  if (num_workers <= 0) {
    std::cerr << "num_workers must be > 0\n";
    return 1;
  }

  WorkerDaemon daemon(shared_root,
                      worker_name,
                      num_workers);

  daemon.Run();

  return 0;
}
