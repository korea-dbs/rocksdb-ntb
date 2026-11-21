// tools/nfs_compaction_service.cc
//
// 잘 동작하던 원본 코드 기반 + 잡별 디렉토리 구조
//
// 원본과 달라진 부분만:
//   Schedule(): 잡 디렉토리 생성 후 meta/req 기록
//   PollLoop(): 잡별 디렉토리 안의 done/fail 감지
//   OnInstallation(): 잡별 디렉토리에 installed 기록
//

#include "tools/nfs_compaction_service.h"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>

using namespace ROCKSDB_NAMESPACE;
namespace fs = std::filesystem;

// ----------------------------------------------------------------------------
// 파일 헬퍼 (원본과 동일)
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
  if (ec) {
    fs::remove(tmp, ec);
    return false;
  }
  return true;
}

static std::vector<std::string> ParseWorkersEnv(const char* raw) {
  std::vector<std::string> out;
  if (raw == nullptr || *raw == '\0') {
    out.push_back("srv3");
    return out;
  }
  std::string s(raw);
  size_t i = 0;
  while (i < s.size()) {
    size_t j = s.find(',', i);
    std::string tok = s.substr(i, (j == std::string::npos ? s.size() : j) - i);
    size_t a = tok.find_first_not_of(" \t\r\n");
    size_t b = tok.find_last_not_of(" \t\r\n");
    if (a != std::string::npos)
      out.emplace_back(tok.substr(a, b - a + 1));
    if (j == std::string::npos) break;
    i = j + 1;
  }
  if (out.empty()) out.push_back("srv3");
  return out;
}

static std::string EnvOr(const char* var, const std::string& fallback) {
  const char* v = std::getenv(var);
  if (v && *v) return std::string(v);
  return fallback;
}

static int EnvOrInt(const char* var, int fallback) {
  const char* v = std::getenv(var);
  if (v && *v) {
    try { return std::stoi(v); } catch (...) { return fallback; }
  }
  return fallback;
}

namespace ROCKSDB_NAMESPACE {

NFSCompactionService::NFSCompactionService(std::string /*shared_dir_arg*/,
                                           int /*min_offload_level_arg*/,
                                           int /*max_inflight_arg*/)
    : shared_dir_(EnvOr("RCMP_SHARED_DIR", "/mnt/shared_nvme")),
      min_offload_level_(EnvOrInt("RCMP_MIN_LEVEL", 3)),
      max_inflight_(EnvOrInt("RCMP_MAX_INFLIGHT", 64)),
      workers_(ParseWorkersEnv(std::getenv("RCMP_WORKERS"))) {

  // 워커별 루트 디렉토리만 생성
  // 잡별 서브디렉토리는 Schedule()에서 생성
  for (const auto& w : workers_)
    fs::create_directories(fs::path(shared_dir_) / "rcmp_jobs" / w);
  fs::create_directories(fs::path(shared_dir_) / "rcmp_work");

  std::cout << "[RCMP] NFSCompactionService(shared_nvme, per-job-dir) start"
            << " shared_dir=" << shared_dir_
            << " min_level=" << min_offload_level_
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
// Schedule() — 잡별 디렉토리에 meta/req 기록
// ----------------------------------------------------------------------------
CompactionServiceScheduleResponse NFSCompactionService::Schedule(
    const CompactionServiceJobInfo& info,
    const std::string& compaction_input) {

  if (info.output_level < min_offload_level_) {
    return CompactionServiceScheduleResponse(
        CompactionServiceJobStatus::kUseLocal);
  }

  std::string job_id = std::to_string(info.job_id);
  std::string worker = PickWorker();

  // 잡별 디렉토리: rcmp_jobs/<worker>/<job_id>/
  fs::path job_dir = fs::path(shared_dir_) / "rcmp_jobs" / worker / job_id;

  std::error_code ec;
  fs::create_directories(job_dir, ec);
  if (ec) {
    std::cerr << "[RCMP] failed to create job dir=" << job_dir << std::endl;
    return CompactionServiceScheduleResponse(
        CompactionServiceJobStatus::kUseLocal);
  }

  auto now = std::chrono::system_clock::now();
  long ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch()).count();
  std::string meta_with_ts =
      "TS:" + std::to_string(ts) + "\n" + compaction_input;

  // meta 먼저, req 나중 (원본과 동일한 순서)
  if (!WriteAllFileAtomic(job_dir / "meta", meta_with_ts)) {
    std::cerr << "[RCMP] failed to write meta for job " << job_id << std::endl;
    return CompactionServiceScheduleResponse(
        CompactionServiceJobStatus::kUseLocal);
  }
  if (!WriteAllFileAtomic(job_dir / "req", "run")) {
    std::cerr << "[RCMP] failed to write req for job " << job_id << std::endl;
    return CompactionServiceScheduleResponse(
        CompactionServiceJobStatus::kUseLocal);
  }

  {
    std::lock_guard<std::mutex> lk(mu_);
    JobState st;
    st.worker_name   = worker;
    st.start_time    = std::chrono::steady_clock::now();
    st.meta_write_ts = ts;
    jobs_[job_id] = st;
  }

  std::cout << "[REMOTE_JOB] job=" << job_id
            << " level=" << info.output_level
            << " worker=" << worker << std::endl;

  return CompactionServiceScheduleResponse(
      job_id, CompactionServiceJobStatus::kSuccess);
}

// ----------------------------------------------------------------------------
// Wait() — 원본과 동일
// ----------------------------------------------------------------------------
CompactionServiceJobStatus NFSCompactionService::Wait(
    const std::string& job_id, std::string* result) {

  std::unique_lock<std::mutex> lk(mu_);
  cv_.wait(lk, [&] {
    auto it = jobs_.find(job_id);
    return stop_.load() || it == jobs_.end() || it->second.finished;
  });

  auto it = jobs_.find(job_id);
  if (it == jobs_.end()) return CompactionServiceJobStatus::kFailure;

  bool ok = it->second.success;
  if (ok && result)
    *result = std::move(it->second.result);

  jobs_.erase(it);
  return ok ? CompactionServiceJobStatus::kSuccess
            : CompactionServiceJobStatus::kFailure;
}

// ----------------------------------------------------------------------------
// OnInstallation() — 잡별 디렉토리에 installed 기록
// ----------------------------------------------------------------------------
void NFSCompactionService::OnInstallation(
    const std::string& job_id,
    const CompactionServiceJobStatus status) {

  std::cout << "[RCMP_INSTALL] job=" << job_id
            << " status=" << static_cast<int>(status) << std::endl;
  if (status != CompactionServiceJobStatus::kSuccess) return;

  for (const auto& w : workers_) {
    fs::path job_dir =
        fs::path(shared_dir_) / "rcmp_jobs" / w / job_id;
    std::error_code ec;
    if (fs::exists(job_dir, ec))
      WriteAllFileAtomic(job_dir / "installed", "installed\n");
  }
}

// ----------------------------------------------------------------------------
// PollLoop() — 잡별 디렉토리 안의 done/fail 감지
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

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
}

}  // namespace ROCKSDB_NAMESPACE

