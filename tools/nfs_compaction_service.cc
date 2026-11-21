// tools/nfs_compaction_service.cc
//
// GFS2 버전 기반 + NFS(srv3→srv4) 방향 + GC 제거
//

#include "tools/nfs_compaction_service.h"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>

namespace fs = std::filesystem;

// ----------------------------------------------------------------------------
// 파일 헬퍼
// ----------------------------------------------------------------------------
static std::string ReadAllFile(const std::string& path) {
  std::ifstream f(path, std::ios::binary);
  if (!f.good()) return "";
  return std::string((std::istreambuf_iterator<char>(f)),
                     std::istreambuf_iterator<char>());
}

static bool WriteAllFileAtomic(const fs::path& path, const std::string& data) {
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
  if (ec) { fs::remove(tmp, ec); return false; }
  return true;
}

static std::string EnvOr(const char* var, const std::string& fallback) {
  const char* v = std::getenv(var);
  if (v && *v) return std::string(v);
  return fallback;
}

static int EnvOrInt(const char* var, int fallback) {
  const char* v = std::getenv(var);
  if (v && *v) {
    try { return std::stoi(v); } catch (...) {}
  }
  return fallback;
}

static std::vector<std::string> ParseWorkers(const char* raw) {
  if (!raw || !*raw) return {"srv3"};
  std::vector<std::string> out;
  std::string s(raw);
  size_t i = 0;
  while (i < s.size()) {
    size_t j = s.find(',', i);
    std::string tok = s.substr(i, (j == std::string::npos ? s.size() : j) - i);
    size_t a = tok.find_first_not_of(" \t");
    size_t b = tok.find_last_not_of(" \t");
    if (a != std::string::npos)
      out.emplace_back(tok.substr(a, b - a + 1));
    if (j == std::string::npos) break;
    i = j + 1;
  }
  return out.empty() ? std::vector<std::string>{"srv3"} : out;
}

// ----------------------------------------------------------------------------

namespace ROCKSDB_NAMESPACE {

NFSCompactionService::NFSCompactionService(std::string /*shared_dir_arg*/,
                                           int /*min_offload_level_arg*/,
                                           int /*max_inflight_arg*/)
    : shared_dir_(EnvOr("RCMP_SHARED_DIR", "/mnt/shared_nvme")),
      min_offload_level_(EnvOrInt("RCMP_MIN_LEVEL", 3)),
      max_inflight_(EnvOrInt("RCMP_MAX_INFLIGHT", 64)),
      workers_(ParseWorkers(std::getenv("RCMP_WORKERS"))) {

  for (const auto& w : workers_)
    fs::create_directories(fs::path(shared_dir_) / "rcmp_jobs" / w);
  fs::create_directories(fs::path(shared_dir_) / "rcmp_work");

  std::cout << "[RCMP] NFSCompactionService start"
            << " shared_dir="   << shared_dir_
            << " min_level="    << min_offload_level_ << " max_level=" << EnvOrInt("RCMP_MAX_LEVEL", 99)
            << " max_inflight=" << max_inflight_
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

// ----------------------------------------------------------------------------
// Schedule()
// ----------------------------------------------------------------------------
CompactionServiceScheduleResponse NFSCompactionService::Schedule(
    const CompactionServiceJobInfo& info,
    const std::string& compaction_input) {

  if ((info.output_level < min_offload_level_ || info.output_level > ([] { static const int v = EnvOrInt("RCMP_MAX_LEVEL", 99); return v; }()))) {
    return CompactionServiceScheduleResponse(
        CompactionServiceJobStatus::kUseLocal);
  }

  {
    std::lock_guard<std::mutex> lk(mu_);
    if (static_cast<int>(jobs_.size()) >= max_inflight_) {
      std::cerr << "[RCMP] max_inflight reached, local fallback job="
                << info.job_id << std::endl;
      return CompactionServiceScheduleResponse(
          CompactionServiceJobStatus::kUseLocal);
    }
  }

  std::string job_id = std::to_string(info.job_id);
  std::string worker = PickWorker();
  fs::path    job_dir =
      fs::path(shared_dir_) / "rcmp_jobs" / worker / job_id;

  std::error_code ec;
  fs::create_directories(job_dir, ec);
  if (ec) {
    std::cerr << "[RCMP] failed to create job dir=" << job_dir << std::endl;
    return CompactionServiceScheduleResponse(
        CompactionServiceJobStatus::kUseLocal);
  }

  auto now = std::chrono::system_clock::now();
  long ts  = std::chrono::duration_cast<std::chrono::milliseconds>(
                 now.time_since_epoch()).count();
  std::string meta_with_ts =
      "TS:" + std::to_string(ts) + "\n" + compaction_input;

  if (!WriteAllFileAtomic(job_dir / "meta", meta_with_ts)) {
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
    st.worker_name   = worker;
    st.start_time    = std::chrono::steady_clock::now();
    st.meta_write_ts = ts;
    jobs_[job_id]    = st;
  }

  std::cout << "[REMOTE_JOB] job=" << job_id
            << " level="   << info.output_level
            << " worker="  << worker
            << " job_dir=" << job_dir << std::endl;

  return CompactionServiceScheduleResponse(
      job_id, CompactionServiceJobStatus::kSuccess);
}

// ----------------------------------------------------------------------------
// Wait()
// ----------------------------------------------------------------------------
CompactionServiceJobStatus NFSCompactionService::Wait(
    const std::string& job_id, std::string* result) {

  std::unique_lock<std::mutex> lk(mu_);
  cv_.wait(lk, [&] {
    auto it = jobs_.find(job_id);
    return stop_.load() || it == jobs_.end() || it->second.finished;
  });

  auto it = jobs_.find(job_id);
  if (it == jobs_.end()) return CompactionServiceJobStatus::kUseLocal /* v-uselocal */;

  bool ok = it->second.success;
  if (ok && result)
    *result = std::move(it->second.result);

  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - it->second.start_time).count();
  std::cout << "[RCMP_WAIT] job=" << job_id
            << " elapsed_ms=" << elapsed
            << " ok=" << ok << std::endl;

  jobs_.erase(it);
  return ok ? CompactionServiceJobStatus::kSuccess
            : CompactionServiceJobStatus::kUseLocal /* v-uselocal */;
}

// ----------------------------------------------------------------------------
// OnInstallation(): GC 없으므로 로그만 찍고 끝
// ----------------------------------------------------------------------------
void NFSCompactionService::OnInstallation(
    const std::string& job_id,
    const CompactionServiceJobStatus status) {
  std::cout << "[RCMP_INSTALL] job=" << job_id
            << " status=" << static_cast<int>(status) << std::endl;
}

// ----------------------------------------------------------------------------
// PollLoop(): done/fail 감지
// ----------------------------------------------------------------------------
void NFSCompactionService::PollLoop(const std::string& worker_name) {
  fs::path worker_root =
      fs::path(shared_dir_) / "rcmp_jobs" / worker_name;

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
      fs::path done    = job_dir / "done";
      fs::path fail    = job_dir / "fail";
      fs::path result  = job_dir / "result";

      bool finished = false, success = false;
      std::string res;
      std::error_code ec;

      if (fs::exists(fail, ec)) {
        finished = true; success = false;
      } else if (fs::exists(done, ec)) {
        finished = true; success = true;
        res = ReadAllFile(result.string());
      }

      if (finished) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = jobs_.find(job_id);
        if (it != jobs_.end()) {
          it->second.finished = true;
          it->second.success  = success;
          it->second.result   = std::move(res);
        }
        cv_.notify_all();
      }
    }

    { static const int kPollMs = [] { int v = EnvOrInt("RCMP_POLL_MS", 200); std::cout << "[RCMP] poll_ms=" << v << std::endl; return v; }();  // v-rcmpenv
      std::this_thread::sleep_for(std::chrono::milliseconds(kPollMs)); }
  }
}

}  // namespace ROCKSDB_NAMESPACE
