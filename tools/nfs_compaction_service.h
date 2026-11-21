// tools/nfs_compaction_service.h
//
// 잘 동작하던 원본 코드 기반 + 잡별 디렉토리 구조
//
// 변경점:
//   rcmp_jobs/<worker>/42.meta  → rcmp_jobs/<worker>/42/meta
//   rcmp_jobs/<worker>/42.req   → rcmp_jobs/<worker>/42/req
//   rcmp_jobs/<worker>/42.result→ rcmp_jobs/<worker>/42/result
//   rcmp_jobs/<worker>/42.done  → rcmp_jobs/<worker>/42/done
//
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

class NFSCompactionService : public CompactionService {
 public:
  NFSCompactionService(std::string shared_dir_arg,
                       int min_offload_level_arg,
                       int max_inflight_arg);
  ~NFSCompactionService() override;

  const char* Name() const override;

  CompactionServiceScheduleResponse Schedule(
      const CompactionServiceJobInfo& info,
      const std::string& compaction_input) override;

  CompactionServiceJobStatus Wait(const std::string& job_id,
                                  std::string* result) override;

  void OnInstallation(const std::string& job_id,
                      CompactionServiceJobStatus status) override;

 private:
  struct JobState {
    std::string worker_name;
    std::chrono::steady_clock::time_point start_time;
    long meta_write_ts = 0;
    bool finished = false;
    bool success  = false;
    std::string result;
  };

  std::string PickWorker();
  void PollLoop(const std::string& worker_name);

  const std::string shared_dir_;
  const int min_offload_level_;
  const int max_inflight_;
  std::vector<std::string> workers_;

  std::mutex mu_;
  std::condition_variable cv_;
  std::unordered_map<std::string, JobState> jobs_;
  std::atomic<size_t> rr_{0};
  std::atomic<bool> stop_{false};
  std::vector<std::thread> pollers_;
};

using MyNFSCompactionService = NFSCompactionService;

}  // namespace ROCKSDB_NAMESPACE

