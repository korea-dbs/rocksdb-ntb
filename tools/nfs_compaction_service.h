// NFSCompactionService: the primary side of the file-based baselines (NFS and
// GFS2). It implements RocksDB's CompactionService by handing jobs to a worker
// process (nfs_compaction_worker) through a directory that both hosts can
// access:
//
//   <shared>/rcmp_jobs/<worker>/<job id>/meta     serialized CompactionServiceInput
//   <shared>/rcmp_jobs/<worker>/<job id>/req      job is ready
//   <shared>/rcmp_jobs/<worker>/<job id>/result   serialized CompactionServiceResult
//   <shared>/rcmp_jobs/<worker>/<job id>/done     (or fail)
//
// The worker runs DB::OpenAndCompact() on the primary's DB directory and
// writes the outputs under <shared>/rcmp_work; RocksDB then renames them into
// the DB and installs them.
//
// Configuration (environment variables):
//   RCMP_SHARED_DIR    shared directory (db_bench enables the service only
//                      when this is set)
//   RCMP_WORKERS       comma-separated worker names (default "worker")
//   RCMP_MIN_LEVEL     offload compactions whose output level is >= this (1)
//   RCMP_MAX_LEVEL     ... and <= this (99)
//   RCMP_POLL_MS       polling interval for finished jobs (1)
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
  NFSCompactionService();
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
    bool finished = false;
    bool success = false;
    std::string result;
  };

  std::string PickWorker();
  void PollLoop(const std::string& worker_name);

  const std::string shared_dir_;
  const int min_offload_level_;
  const int max_offload_level_;
  const int poll_ms_;
  std::vector<std::string> workers_;

  std::mutex mu_;
  std::condition_variable cv_;
  std::unordered_map<std::string, JobState> jobs_;
  std::atomic<size_t> rr_{0};
  std::atomic<bool> stop_{false};
  std::vector<std::thread> pollers_;
};

}  // namespace ROCKSDB_NAMESPACE
