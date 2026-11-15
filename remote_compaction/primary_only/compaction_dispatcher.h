#pragma once
// ============================================================
//
//
//
//
// ============================================================

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "../shared/compaction_spec.h"
#include "../shared/ntb_channel.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

class Compaction;
class ColumnFamilyData;
struct ImmutableOptions;
struct MutableCFOptions;

// ── SSTReservationTable ───────────────────────────────────────────
class SSTReservationTable {
 public:
  bool TryReserve(const std::vector<uint64_t>& fnums);
  void Release(const std::vector<uint64_t>& fnums);
  bool IsReserved(uint64_t fnum) const;

 private:
  mutable std::mutex mu_;
  std::set<uint64_t> reserved_;
};

// ── DispatchOptions ───────────────────────────────────────────────
struct DispatchOptions {
  uint32_t    worker_node_id   = 8;
  std::string db_path;
  uint64_t    target_file_size = 64ULL << 20;
};

// ── AsyncJob ──────────────────────────────────────────────────────
struct AsyncJob {
  CompactionSpec                 spec;
  std::vector<uint64_t>          fnums;
  CompactionResult               result;
  std::vector<std::vector<char>> out_bufs;
  std::shared_ptr<Compaction>    compaction_holder;
  std::function<void(Status,
                     CompactionSpec&&,
                     CompactionResult&&,
                     std::vector<std::vector<char>>&&)> on_complete;

  std::chrono::steady_clock::time_point t_total_start;
  std::chrono::steady_clock::time_point t_send_done;
  std::chrono::steady_clock::time_point enqueue_time;
};

struct SendTask {
  uint64_t            job_id = 0;
  std::vector<size_t> file_indices;
};

struct PendingSend {
  AsyncJob             job;
  std::atomic<int>     remaining_files{0};
  std::atomic<bool>    aborted{false};
  int                  responsible_seg = -1;
  bool                 result_ready    = false;
  CompactionResult     early_result;
  int                  early_fixed_seg = 0;
};

// ── FinishJob ─────────────────────────────────────────────────────
struct FinishJob {
  Status   status;
  AsyncJob job;
  std::chrono::steady_clock::time_point enqueue_time = std::chrono::steady_clock::now();
};

// ── CompactionDispatcher ──────────────────────────────────────────
class CompactionDispatcher {
 public:
  explicit CompactionDispatcher(const DispatchOptions& opts);
  ~CompactionDispatcher();

  Status Connect();
  void   Disconnect();

  Status Dispatch(Compaction*                         c,
                  ColumnFamilyData*                   cfd,
                  const ImmutableOptions&             iopts,
                  const MutableCFOptions&             mutable_cf,
                  const std::vector<SequenceNumber>&  snapshots,
                  SequenceNumber                      ewc_snapshot,
                  SequenceNumber                      preserve_seqnum,
                  CompactionResult*                   result_out,
                  std::vector<std::vector<char>>*     out_sst_bufs);

  Status DispatchAsync(
      CompactionSpec                      spec,
      std::shared_ptr<Compaction>         compaction_holder,
      std::function<void(Status,
                         CompactionSpec&&,
                         CompactionResult&&,
                         std::vector<std::vector<char>>&&)> on_complete);

  Status BuildSpec(Compaction*                         c,
                   ColumnFamilyData*                   cfd,
                   const ImmutableOptions&             iopts,
                   const MutableCFOptions&             mutable_cf,
                   const std::vector<SequenceNumber>&  snapshots,
                   SequenceNumber                      ewc_snapshot,
                   SequenceNumber                      preserve_seqnum,
                   CompactionSpec*                     spec_out);

  void ReleaseReservation(const std::vector<uint64_t>& fnums);
  SSTReservationTable* reservation_table() { return &reservation_; }

  bool IsAsyncBusy() const {
    for (int i = 0; i < kNumSegments; i++) {
      std::lock_guard<std::mutex> lk(send_mu_[i]);
      if (!send_queues_[i].empty()) return true;
    }
    {
      std::lock_guard<std::mutex> lk(gsend_mu_);
      if (!gsend_queue_.empty()) return true;
    }
    return false;
  }

 private:
  struct PendingOutput {
    AsyncJob job;
    std::chrono::steady_clock::time_point enqueue_time = std::chrono::steady_clock::now();
    bool discard = false;
  };

  void SendLoop(int seg_idx);
  void CtrlRecvLoop();
  void OutputRecvLoop(int seg_idx);
  void DeliverResult(AsyncJob&& job, CompactionResult&& result, int fixed_seg);
  void FinishWorkerLoop(int idx);
  Status ExecuteJobSync(AsyncJob& job);
  int  PickLeastBusySegment();
  void ReapStaleInFlightJobs();  // v30: CtrlRecvLoop watchdog
  struct OutJob {
    AsyncJob                    job;
    std::map<uint64_t, size_t>  idx_by_fnum;
    std::vector<bool>           done;
    std::vector<std::chrono::steady_clock::time_point> t_file;
    size_t files_left = 0;
    int    busy       = 0;
    bool   failed     = false;
    bool   discard    = false;
    bool   seen       = false;
    std::chrono::steady_clock::time_point t_reg, t_first, t_last;
    bool provisional = false;
    std::vector<uint32_t> crc_actual, crc_expect;
    std::vector<size_t>   order;
  };
  void RegisterOutJob(AsyncJob&& job, bool discard);
  void OutputRecvLoopAny(int seg_idx);
  void CompleteOutJob(uint64_t job_id, std::shared_ptr<OutJob> oj);
  void ReapStaleOutJobs();
  void RetireOutJobLocked(uint64_t job_id);

  static constexpr int kFinishThreads = 54;
  static constexpr int kFinishQueueSoftLimit = kFinishThreads * 2;

  // v30: CtrlRecvLoop watchdog. A single RecvResultMsg timeout only
  // means "nothing arrived in this 10s window" -- it says nothing about
  // WHICH job (if any) is stuck, since every job's result shares this
  // one ctrl channel. kJobResultTimeoutMs bounds how long a job may sit
  // in in_flight_ (measured from t_send_done, i.e. once SendLoop
  // finished pushing all its inputs) before ReapStaleInFlightJobs()
  // force-fails it. Keep this in rough sync with ntb_channel.h's
  // kOutputResultTimeoutMs -- both are trying to bound the same
  // underlying thing (worker compaction time) from two different
  // vantage points, and both are placeholders that need tuning against
  // this deployment's real max compaction duration.
  static constexpr int kJobResultTimeoutMs     = 300000;  // 5 min (was 10 min)
  static constexpr int kWatchdogScanIntervalMs = 5000;    // scan cadence

  DispatchOptions       opts_;
  NtbChannel            channel_;
  SSTReservationTable   reservation_;
  std::atomic<uint64_t> job_id_{1};
  std::atomic<bool>     channel_open_{false};

  mutable std::mutex           send_mu_[kNumSegments];
  std::condition_variable      send_cv_[kNumSegments];
  std::queue<SendTask>         send_queues_[kNumSegments];
  mutable std::mutex           gsend_mu_;
  std::condition_variable      gsend_cv_;
  std::deque<SendTask>         gsend_queue_;
  std::atomic<bool>            stop_{false};
  std::mutex                   ctrl_send_mu_[kNumSegments];

  std::mutex                   inflight_mu_;
  std::map<uint64_t, AsyncJob> in_flight_;
  std::mutex job_seg_mu_;
  std::map<uint64_t, int> job_seg_map_;

  std::mutex job_registered_mu_;
  std::condition_variable job_registered_cv_;
  std::map<uint64_t, bool> job_registered_;

  std::mutex pending_send_mu_;
  std::map<uint64_t, std::shared_ptr<PendingSend>> pending_sends_;
  std::mutex seg_pick_mu_;
  std::atomic<uint64_t> next_seg_{0};

  mutable std::mutex           output_mu_[kNumSegments];
  std::condition_variable      output_cv_[kNumSegments];
  std::queue<PendingOutput>    output_queues_[kNumSegments];
  std::mutex                                   out_jobs_mu_;
  std::map<uint64_t, std::shared_ptr<OutJob>>  out_jobs_;
  std::map<uint64_t, bool>                     out_retired_;
  std::queue<uint64_t>                         out_retired_q_;

  std::mutex               chunk_ack_mu_[kNumSegments];
  std::condition_variable  chunk_ack_cv_[kNumSegments];
  bool                     chunk_ack_ready_[kNumSegments] = {false};
  uint64_t                 chunk_ack_file_[kNumSegments]  = {0};
  bool                     chunk_ack_failed_[kNumSegments] = {false};

  std::thread send_threads_[kNumSegments];
  std::thread ctrl_recv_thread_;
  std::thread output_recv_threads_[kNumSegments];

  std::vector<std::thread>     finish_pool_;
  mutable std::mutex           finish_mu_;
  std::condition_variable      finish_cv_;
  std::queue<FinishJob>        finish_queue_;
  bool                         stop_finish_{false};
};

}  // namespace ROCKSDB_NAMESPACE
