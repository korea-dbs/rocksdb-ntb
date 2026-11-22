#pragma once
// CompactionDispatcher: primary-side engine that moves compaction jobs to the
// worker over the NTB channel and brings their outputs back.
//
// Threads (all started by Connect()):
//   SendLoop[seg]        one per segment. Pops input-file tasks from a global
//                        queue and DMAs each input SST into its own segment.
//                        The first SendLoop that touches a job also sends the
//                        job spec over the control channel.
//   CtrlRecvLoop         receives control messages (results, receive failures)
//                        and runs the watchdogs.
//   OutputRecvLoop[seg]  one per segment. Picks up output chunks from whatever
//                        segment the worker used, matches them to jobs by the
//                        (job_id, file_number) in the chunk header, and hands
//                        each finished file to the persist thread.
//   persist thread       writes each received output SST to a temporary file
//                        as soon as it arrives (see RcStreamSubmit).
//   FinishWorkerLoop     installs finished jobs one at a time
//                        (RemoteCompactionJob::FinishJob).
//
// Outputs may arrive before the job's result message. Such outputs are kept in
// a provisional entry and merged when the result is registered.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
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

// Input SSTs of in-flight jobs. A file can belong to at most one remote job.
class SSTReservationTable {
 public:
  bool TryReserve(const std::vector<uint64_t>& fnums);
  void Release(const std::vector<uint64_t>& fnums);
  bool IsReserved(uint64_t fnum) const;

 private:
  mutable std::mutex mu_;
  std::set<uint64_t> reserved_;
};

struct DispatchOptions {
  uint32_t worker_node_id = 8;
  std::string db_path;
  uint64_t target_file_size = 64ULL << 20;
};

using DispatchCallback =
    std::function<void(Status, CompactionSpec&&, CompactionResult&&,
                       std::vector<std::vector<char>>&&)>;

struct AsyncJob {
  CompactionSpec spec;
  std::vector<uint64_t> fnums;
  CompactionResult result;
  std::vector<std::vector<char>> out_bufs;
  std::shared_ptr<Compaction> compaction_holder;
  DispatchCallback on_complete;

  std::chrono::steady_clock::time_point t_total_start;
  std::chrono::steady_clock::time_point t_send_done;
  std::chrono::steady_clock::time_point enqueue_time;
};

// One unit of send work: input file(s) of one job.
struct SendTask {
  uint64_t job_id = 0;
  std::vector<size_t> file_indices;
};

// A job whose inputs are still being sent. Guarded by pending_send_mu_.
struct PendingSend {
  AsyncJob job;
  std::atomic<int> remaining_files{0};
  std::atomic<bool> aborted{false};
  int responsible_seg = -1;  // segment of the SendLoop that sent the spec
  // The result may arrive before all inputs are sent; it is parked here and
  // delivered by the last SendLoop.
  bool result_ready = false;
  CompactionResult early_result;
};

struct FinishJob {
  Status status;
  AsyncJob job;
  std::chrono::steady_clock::time_point enqueue_time =
      std::chrono::steady_clock::now();
};

class CompactionDispatcher {
 public:
  explicit CompactionDispatcher(const DispatchOptions& opts);
  ~CompactionDispatcher();

  Status Connect();
  void Disconnect();

  // Queues a job. Returns Busy without queuing if an input SST is already
  // reserved by another remote job; the caller then compacts locally.
  Status DispatchAsync(CompactionSpec spec,
                       std::shared_ptr<Compaction> compaction_holder,
                       DispatchCallback on_complete);

  Status BuildSpec(Compaction* c, ColumnFamilyData* cfd,
                   const ImmutableOptions& iopts,
                   const MutableCFOptions& mutable_cf,
                   const std::vector<SequenceNumber>& snapshots,
                   SequenceNumber ewc_snapshot, SequenceNumber preserve_seqnum,
                   CompactionSpec* spec_out);

  void ReleaseReservation(const std::vector<uint64_t>& fnums);
  SSTReservationTable* reservation_table() { return &reservation_; }

  bool IsAsyncBusy() const {
    std::lock_guard<std::mutex> lk(gsend_mu_);
    return !gsend_queue_.empty();
  }

 private:
  // Receive state of one job's outputs. Guarded by out_jobs_mu_; busy > 0
  // means an OutputRecvLoop is copying a chunk into out_bufs.
  struct OutJob {
    AsyncJob job;
    std::map<uint64_t, size_t> idx_by_fnum;  // worker file number -> buffer
    std::vector<bool> done;
    std::vector<std::chrono::steady_clock::time_point> t_file;
    size_t files_left = 0;
    int busy = 0;
    bool failed = false;
    bool discard = false;  // outputs of an abandoned job: receive and drop
    bool seen = false;
    std::chrono::steady_clock::time_point t_reg, t_first, t_last;
    bool provisional = false;   // created by outputs that beat the result
    std::vector<size_t> order;  // result index -> arrival index
  };

  void SendLoop(int seg_idx);
  void CtrlRecvLoop();
  void OutputRecvLoop(int seg_idx);
  void FinishWorkerLoop(int idx);
  void DeliverResult(AsyncJob&& job, CompactionResult&& result);
  void RegisterOutJob(AsyncJob&& job, bool discard);
  void CompleteOutJob(uint64_t job_id, std::shared_ptr<OutJob> oj);
  void RetireOutJobLocked(uint64_t job_id);  // requires out_jobs_mu_
  void FailInFlightJob(uint64_t job_id, const Status& why);
  void ReapStaleInFlightJobs();
  void ReapStaleOutJobs();

  static constexpr int kFinishThreads = 1;  // installs are serialized
  // Safety net only: a job that makes no progress for this long (worker died,
  // link lost) is failed so that its input reservation is released.
  static constexpr int kJobResultTimeoutMs = 300000;
  static constexpr int kWatchdogScanIntervalMs = 5000;

  DispatchOptions opts_;
  NtbChannel channel_;
  SSTReservationTable reservation_;
  std::atomic<uint64_t> job_id_{1};
  std::atomic<bool> channel_open_{false};
  std::atomic<bool> stop_{false};

  mutable std::mutex gsend_mu_;
  std::condition_variable gsend_cv_;
  std::deque<SendTask> gsend_queue_;
  std::mutex ctrl_send_mu_[kNumSegments];

  std::mutex pending_send_mu_;  // lock order: pending_send_mu_ -> inflight_mu_
  std::map<uint64_t, std::shared_ptr<PendingSend>> pending_sends_;
  std::mutex inflight_mu_;
  std::map<uint64_t, AsyncJob> in_flight_;  // all inputs sent, awaiting result

  std::mutex out_jobs_mu_;
  std::map<uint64_t, std::shared_ptr<OutJob>> out_jobs_;
  std::map<uint64_t, bool> out_retired_;    // finished jobs; late chunks dropped
  std::queue<uint64_t> out_retired_q_;      // bounds out_retired_

  std::thread send_threads_[kNumSegments];
  std::thread ctrl_recv_thread_;
  std::thread output_recv_threads_[kNumSegments];

  std::vector<std::thread> finish_pool_;
  mutable std::mutex finish_mu_;
  std::condition_variable finish_cv_;
  std::deque<FinishJob> finish_queue_;
  bool stop_finish_ = false;
};

}  // namespace ROCKSDB_NAMESPACE
