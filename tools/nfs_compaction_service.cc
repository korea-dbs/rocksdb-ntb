#include "tools/nfs_compaction_service.h"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>

namespace fs = std::filesystem;

namespace {

std::string ReadAllFile(const std::string& path) {
  std::ifstream f(path, std::ios::binary);
  if (!f.good()) return "";
  return std::string((std::istreambuf_iterator<char>(f)),
                     std::istreambuf_iterator<char>());
}

// Writes path.tmp and renames it, so the reader never sees a partial file.
bool WriteAllFileAtomic(const fs::path& path, const std::string& data) {
  fs::path tmp = path;
  tmp += ".tmp";
  {
    std::ofstream f(tmp, std::ios::binary | std::ios::trunc);
    if (!f.good()) return false;
    f.write(data.data(), static_cast<std::streamsize>(data.size()));
    f.flush();
    if (!f.good()) return false;
  }
  std::error_code ec;
  fs::rename(tmp, path, ec);
  if (ec) {
    fs::remove(tmp, ec);
    return false;
  }
  return true;
}

std::string EnvOr(const char* var, const std::string& fallback) {
  const char* v = std::getenv(var);
  return (v && *v) ? std::string(v) : fallback;
}

int EnvOrInt(const char* var, int fallback) {
  const char* v = std::getenv(var);
  if (v && *v) {
    try {
      return std::stoi(v);
    } catch (...) {
    }
  }
  return fallback;
}

std::vector<std::string> ParseWorkers(const std::string& s) {
  std::vector<std::string> out;
  size_t i = 0;
  while (i < s.size()) {
    size_t j = s.find(',', i);
    std::string tok = s.substr(i, (j == std::string::npos ? s.size() : j) - i);
    size_t a = tok.find_first_not_of(" \t");
    size_t b = tok.find_last_not_of(" \t");
    if (a != std::string::npos) out.emplace_back(tok.substr(a, b - a + 1));
    if (j == std::string::npos) break;
    i = j + 1;
  }
  if (out.empty()) out.emplace_back("worker");
  return out;
}

}  // namespace

namespace ROCKSDB_NAMESPACE {

NFSCompactionService::NFSCompactionService()
    : shared_dir_(EnvOr("RCMP_SHARED_DIR", "")),
      min_offload_level_(EnvOrInt("RCMP_MIN_LEVEL", 1)),
      max_offload_level_(EnvOrInt("RCMP_MAX_LEVEL", 99)),
      poll_ms_(EnvOrInt("RCMP_POLL_MS", 1)),
      workers_(ParseWorkers(EnvOr("RCMP_WORKERS", "worker"))) {
  for (const auto& w : workers_)
    fs::create_directories(fs::path(shared_dir_) / "rcmp_jobs" / w);
  fs::create_directories(fs::path(shared_dir_) / "rcmp_work");

  std::cout << "[RCMP] NFSCompactionService start shared_dir=" << shared_dir_
            << " min_level=" << min_offload_level_
            << " max_level=" << max_offload_level_
            << " poll_ms=" << poll_ms_
            << " workers=";
  for (const auto& w : workers_) std::cout << w << ",";
  std::cout << std::endl;

  for (const auto& w : workers_)
    pollers_.emplace_back([this, w] { PollLoop(w); });
}

NFSCompactionService::~NFSCompactionService() {
  stop_.store(true);
  cv_.notify_all();
  for (auto& t : pollers_)
    if (t.joinable()) t.join();
}

const char* NFSCompactionService::Name() const {
  return "NFSCompactionService";
}

std::string NFSCompactionService::PickWorker() {
  size_t i = rr_.fetch_add(1, std::memory_order_relaxed);
  return workers_[i % workers_.size()];
}

CompactionServiceScheduleResponse NFSCompactionService::Schedule(
    const CompactionServiceJobInfo& info,
    const std::string& compaction_input) {
  if (info.output_level < min_offload_level_ ||
      info.output_level > max_offload_level_) {
    return CompactionServiceScheduleResponse(
        CompactionServiceJobStatus::kUseLocal);
  }
  std::string job_id = std::to_string(info.job_id);
  std::string worker = PickWorker();
  fs::path job_dir = fs::path(shared_dir_) / "rcmp_jobs" / worker / job_id;

  std::error_code ec;
  fs::create_directories(job_dir, ec);
  if (ec) {
    std::cerr << "[RCMP] failed to create job dir=" << job_dir << std::endl;
    return CompactionServiceScheduleResponse(
        CompactionServiceJobStatus::kUseLocal);
  }

  // The worker reports meta_ms = time from this write until it picks the job.
  long ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();
  if (!WriteAllFileAtomic(job_dir / "meta",
                          "TS:" + std::to_string(ts) + "\n" +
                              compaction_input)) {
    std::cerr << "[RCMP] failed to write meta job=" << job_id << std::endl;
    return CompactionServiceScheduleResponse(
        CompactionServiceJobStatus::kUseLocal);
  }
  if (!WriteAllFileAtomic(job_dir / "req", "run")) {
    std::cerr << "[RCMP] failed to write req job=" << job_id << std::endl;
    return CompactionServiceScheduleResponse(
        CompactionServiceJobStatus::kUseLocal);
  }

  {
    std::lock_guard<std::mutex> lk(mu_);
    JobState st;
    st.worker_name = worker;
    st.start_time = std::chrono::steady_clock::now();
    jobs_[job_id] = st;
  }
  std::cout << "[REMOTE_JOB] job=" << job_id << " level=" << info.output_level
            << " worker=" << worker << " job_dir=" << job_dir << std::endl;
  return CompactionServiceScheduleResponse(
      job_id, CompactionServiceJobStatus::kSuccess);
}

// A failed remote job makes RocksDB run the compaction locally (kUseLocal).
CompactionServiceJobStatus NFSCompactionService::Wait(
    const std::string& job_id, std::string* result) {
  std::unique_lock<std::mutex> lk(mu_);
  cv_.wait(lk, [&] {
    auto it = jobs_.find(job_id);
    return stop_.load() || it == jobs_.end() || it->second.finished;
  });

  auto it = jobs_.find(job_id);
  if (it == jobs_.end()) return CompactionServiceJobStatus::kUseLocal;

  bool ok = it->second.success;
  if (ok && result) *result = std::move(it->second.result);

  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - it->second.start_time)
                     .count();
  std::cout << "[RCMP_WAIT] job=" << job_id << " elapsed_ms=" << elapsed
            << " ok=" << ok << std::endl;

  jobs_.erase(it);
  return ok ? CompactionServiceJobStatus::kSuccess
            : CompactionServiceJobStatus::kUseLocal;
}

// Job directories are not removed.
void NFSCompactionService::OnInstallation(
    const std::string& job_id, const CompactionServiceJobStatus status) {
  std::cout << "[RCMP_INSTALL] job=" << job_id
            << " status=" << static_cast<int>(status) << std::endl;
}

// Polls the job directories of one worker for done/fail markers.
void NFSCompactionService::PollLoop(const std::string& worker_name) {
  fs::path worker_root = fs::path(shared_dir_) / "rcmp_jobs" / worker_name;

  while (!stop_.load()) {
    std::vector<std::string> pending;
    {
      std::lock_guard<std::mutex> lk(mu_);
      for (const auto& kv : jobs_) {
        if (kv.second.worker_name == worker_name && !kv.second.finished)
          pending.push_back(kv.first);
      }
    }

    for (const auto& job_id : pending) {
      fs::path job_dir = worker_root / job_id;
      bool finished = false, success = false;
      std::string res;
      std::error_code ec;

      if (fs::exists(job_dir / "fail", ec)) {
        finished = true;
      } else if (fs::exists(job_dir / "done", ec)) {
        finished = true;
        success = true;
        res = ReadAllFile((job_dir / "result").string());
      }

      if (finished) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = jobs_.find(job_id);
        if (it != jobs_.end()) {
          it->second.finished = true;
          it->second.success = success;
          it->second.result = std::move(res);
        }
        cv_.notify_all();
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(poll_ms_));
  }
}

}  // namespace ROCKSDB_NAMESPACE
