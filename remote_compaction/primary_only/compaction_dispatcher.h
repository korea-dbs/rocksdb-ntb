#pragma once
// ============================================================
//  primary_only/compaction_dispatcher.h  (v8 – per-seg RecvLoop)
//
//    - CtrlRecvLoop: RecvResultMsg → job_id % kNumSegments → output_queues_[seg]
//    - recv_thread_ → ctrl_recv_thread_ + output_recv_threads_[kNumSegments]
//
//    SendLoop[0/1] ─────────────────────────────→ in_flight_
//    CtrlRecvLoop ──(job_id%seg)─────────────────→ output_queues_[seg]
//    OutputRecvLoop[0/1] ─(StreamRecvOutputSST)─→ finish_queue_
//    FinishWorkerLoop[0..3] ──(callback)─────────→ done
// ============================================================

#include <atomic>
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
};

// ── FinishJob ─────────────────────────────────────────────────────
struct FinishJob {
  Status   status;
  AsyncJob job;
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
    return false;
  }

 private:
  struct PendingOutput {
    AsyncJob job;
  };

  void SendLoop(int seg_idx);
  void CtrlRecvLoop();
  void OutputRecvLoop(int seg_idx);
  void FinishWorkerLoop();
  Status ExecuteJobSync(AsyncJob& job);

  static constexpr int kFinishThreads = 4;

  DispatchOptions       opts_;
  NtbChannel            channel_;
  SSTReservationTable   reservation_;
  std::atomic<uint64_t> job_id_{1};
  std::atomic<bool>     channel_open_{false};

  mutable std::mutex           send_mu_[kNumSegments];
  std::condition_variable      send_cv_[kNumSegments];
  std::queue<AsyncJob>         send_queues_[kNumSegments];
  std::atomic<bool>            stop_{false};
  std::mutex                   ctrl_send_mu_;

  // in_flight
  std::mutex                   inflight_mu_;
  std::map<uint64_t, AsyncJob> in_flight_;

  mutable std::mutex           output_mu_[kNumSegments];
  std::condition_variable      output_cv_[kNumSegments];
  std::queue<PendingOutput>    output_queues_[kNumSegments];

  std::thread send_threads_[kNumSegments];
  std::thread ctrl_recv_thread_;                    // [v8]
  std::thread output_recv_threads_[kNumSegments];   // [v8]

  std::vector<std::thread>     finish_pool_;
  mutable std::mutex           finish_mu_;
  std::condition_variable      finish_cv_;
  std::queue<FinishJob>        finish_queue_;
  bool                         stop_finish_{false};
};

}  // namespace ROCKSDB_NAMESPACE
