#pragma once
// ============================================================
//
//      RecvSpecMsg → fixed_seg = job_id % kNumSegments
//
//
//      compact_queue_ pop → HandleJob → SendResultMsg
//      → send_output_mu_[job.fixed_seg] lock
//      → StreamSendOutputSST(fixed_seg=job.fixed_seg)
//
// ============================================================

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <map>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "../shared/compaction_spec.h"
#include "../shared/ntb_channel.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "table/internal_iterator.h"
#include "table/table_reader.h"

namespace ROCKSDB_NAMESPACE {

struct WorkerOptions {
  uint32_t primary_node_id = 8;
  Env*     env             = nullptr;
};

class WorkerServer {
 public:
  explicit WorkerServer(const WorkerOptions& opts);
  ~WorkerServer();

  Status Connect();

  void Stop();

  void RequestStop() noexcept;

  void Run();

 private:
  struct WorkerJob {
    CompactionSpec                 spec;
    int                            fixed_seg = 0;
    std::vector<std::vector<char>> sst_bufs;
  };

  struct RecvTask {
    uint64_t            job_id = 0;
    std::vector<size_t> file_indices;
  };

  struct JobAssembly {
    CompactionSpec                 spec;
    std::vector<std::vector<char>> sst_bufs;
    std::atomic<int>               remaining_files{0};
    std::atomic<bool>              failed{false};
  };

  struct PendingRecv {
    uint64_t                     job_id     = 0;
    size_t                       file_index = 0;
    std::shared_ptr<JobAssembly> ja;
  };

  void DispatcherLoop();
  void RecvLoop(int seg_idx);
  void CompactionLoop(int thread_id);
  void FinalizeJobAssembly(uint64_t job_id, std::shared_ptr<JobAssembly> ja);

  const Comparator* FindComparator(const std::string& name);

  Status OpenInputIterators(
      const CompactionSpec&                 spec,
      const Options&                        opts,
      const ReadOptions&                    ro,
      const ImmutableOptions&               iopts,
      const std::vector<std::vector<char>>& sst_bufs,
      std::vector<InternalIterator*>*       iters_out,
      std::vector<TableReader*>*            readers_out);

  Status WriteOutputSSTs(
      const CompactionSpec&           spec,
      const Options&                  opts,
      const ReadOptions&              ro,
      const ImmutableOptions&         iopts,
      InternalIterator*               merged_iter,
      CompactionResult*               result_out,
      std::vector<std::vector<char>>* out_bufs);

  Status HandleJob(
      const CompactionSpec&                 spec,
      const std::vector<std::vector<char>>& sst_bufs,
      CompactionResult*                     result_out,
      std::vector<std::vector<char>>*       out_bufs);

  static constexpr int kNumCompactionThreads = 8;

  static constexpr int kShutdownJoinTimeoutSec = 20;

  static bool WaitThreadDone(std::atomic<bool>& done_flag, int timeout_sec);

  WorkerOptions     opts_;
  NtbChannel        channel_;
  std::atomic<bool> stop_{false};

  std::queue<RecvTask>     spec_queues_[kNumSegments];
  std::mutex               spec_mu_[kNumSegments];
  std::condition_variable  spec_cv_[kNumSegments];
  std::map<uint64_t, PendingRecv> pending_recv_[kNumSegments];
  std::mutex                      pending_mu_[kNumSegments];
  std::mutex               job_seg_mu_;
  std::map<uint64_t, int>  job_seg_map_;

  std::mutex                                       job_assembly_mu_;
  std::map<uint64_t, std::shared_ptr<JobAssembly>>  job_assembly_;

  std::mutex               send_output_mu_[kNumSegments];
  std::mutex               ack_send_mu_[kNumSegments];

  std::queue<WorkerJob>    compact_queue_;
  std::mutex               compact_mu_;
  std::condition_variable  compact_cv_;

  std::thread              recv_threads_[kNumSegments];
  std::vector<std::thread> compact_pool_;

  std::atomic<bool>        recv_thread_done_[kNumSegments]{};
  std::atomic<bool>        compact_thread_done_[kNumCompactionThreads]{};
};

}  // namespace ROCKSDB_NAMESPACE
