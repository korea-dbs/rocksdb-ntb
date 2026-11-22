#pragma once
// WorkerServer: the compaction worker. It receives job specs and input SSTs
// from the primary over NTB, compacts them in memory, and streams the output
// SSTs back. It never touches a file system.
//
// Threads (all started by Run()):
//   DispatcherLoop       receives job specs on the control channel, allocates
//                        the input buffers and registers every input file
//                        (by file number) so the receivers can match chunks.
//   RecvLoop[seg]        one per data segment. Copies each incoming chunk into
//                        its job's input buffer; a job with all inputs
//                        received goes to the compaction queue.
//   CompactionLoop[n]    --threads of them. Runs one job at a time
//                        (HandleJob); each finished output SST is queued for
//                        sending immediately, and the result message is sent
//                        after all outputs of the job have left.
//   OutSendLoop[seg]     one per data segment. Sends queued output SSTs on its
//                        own segment.
//
// Input data may arrive before the job spec; RecvLoop waits until the spec is
// registered.

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
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

// Worker-side subcompactions: a job is split into user-key ranges that are
// compacted by separate threads, like RocksDB subcompactions.
enum class WorkerSubcompaction {
  kNone,  // never split
  kL0,    // split only jobs whose output level is L1 (L0 -> L1)
  kAll,   // split any job
};

struct WorkerOptions {
  uint32_t primary_node_id = 0;
  int compaction_threads = 16;
  WorkerSubcompaction subcompaction = WorkerSubcompaction::kNone;
  // Upper bound on ranges per job.
  int max_subcompactions = 8;
  // Jobs with less input than this are never split.
  uint64_t subcompaction_min_input_bytes = 64ull << 20;
  // Minimum bytes per range; 0 means the job's target output file size
  // (as in RocksDB's GenSubcompactionBoundaries).
  uint64_t subcompaction_min_range_bytes = 0;
  // Input buffers of finished jobs are kept for reuse up to this many bytes.
  uint64_t input_pool_bytes = 32ull << 30;
};

class WorkerServer {
 public:
  explicit WorkerServer(const WorkerOptions& opts);
  ~WorkerServer();

  Status Connect();
  void Run();

  // Safe to call from a signal handler (one atomic store). All wait loops
  // re-check the flag at least every 200 ms.
  void RequestStop() noexcept;

 private:
  struct WorkerJob {
    CompactionSpec spec;
    std::vector<std::vector<char>> sst_bufs;
  };

  // Input buffers of a job whose inputs are still arriving.
  struct JobAssembly {
    CompactionSpec spec;
    std::vector<std::vector<char>> sst_bufs;
    std::atomic<int> remaining_files{0};
    std::atomic<bool> failed{false};
  };

  // Registration of one expected input file.
  struct PendingRecv {
    uint64_t job_id = 0;
    size_t file_index = 0;
    std::shared_ptr<JobAssembly> ja;
  };

  // Outputs of one job that are queued or being sent.
  struct OutputsInFlight {
    std::mutex mu;
    std::condition_variable cv;
    int left = 0;
    bool failed = false;
  };

  struct OutTask {
    uint64_t job_id = 0;
    uint64_t file_number = 0;
    std::vector<char> data;
    std::shared_ptr<OutputsInFlight> inflight;
  };

  void DispatcherLoop();
  void RecvLoop(int seg_idx);
  void CompactionLoop(int thread_id);
  void OutSendLoop(int seg_idx);
  void FinalizeJobAssembly(uint64_t job_id, std::shared_ptr<JobAssembly> ja);
  void SendResult(int seg, const CompactionResult& result);

  // Queues a finished output SST of job_id for sending (consumes *buf).
  void EmitOutput(uint64_t job_id, uint64_t file_number,
                  std::vector<char>* buf);

  // Input buffer pool.
  void TakeInputBuffer(std::vector<char>* out, size_t n);
  void ReturnInputBuffers(std::vector<std::vector<char>>* bufs);

  const Comparator* FindComparator(const std::string& name);

  Status OpenInputIterators(const CompactionSpec& spec, const Options& opts,
                            const ReadOptions& ro,
                            const ImmutableOptions& iopts,
                            const std::vector<std::vector<char>>& sst_bufs,
                            std::vector<InternalIterator*>* iters_out,
                            std::vector<TableReader*>* readers_out);

  Status WriteOutputSSTs(const CompactionSpec& spec, const Options& opts,
                         InternalIterator* merged_iter,
                         CompactionResult* result_out);

  std::vector<std::string> PickSubcompactionBounds(
      const CompactionSpec& spec, const Comparator* ucmp,
      const ReadOptions& ro, const std::vector<TableReader*>& readers,
      uint64_t input_bytes);

  Status HandleJob(const CompactionSpec& spec,
                   const std::vector<std::vector<char>>& sst_bufs,
                   CompactionResult* result_out);

  // On shutdown a thread blocked inside a wedged DMA call cannot be
  // interrupted; such a thread is detached after this timeout.
  static constexpr int kShutdownJoinTimeoutSec = 20;
  static bool WaitThreadDone(std::atomic<bool>& done_flag, int timeout_sec);
  void JoinOrDetach(std::thread& t, std::atomic<bool>& done,
                    const char* name, int idx);

  WorkerOptions opts_;
  NtbChannel channel_;
  std::atomic<bool> stop_{false};

  // Expected input files per segment, keyed by file number.
  std::map<uint64_t, PendingRecv> pending_recv_[kNumSegments];
  std::mutex pending_mu_[kNumSegments];
  // Bumped whenever DispatcherLoop registers a job.
  std::mutex reg_mu_;
  std::condition_variable reg_cv_;
  uint64_t reg_epoch_ = 0;

  std::mutex job_assembly_mu_;
  std::map<uint64_t, std::shared_ptr<JobAssembly>> job_assembly_;

  std::mutex compact_mu_;
  std::condition_variable compact_cv_;
  std::queue<WorkerJob> compact_queue_;

  std::mutex outq_mu_;
  std::condition_variable outq_cv_;
  std::queue<OutTask> outq_;
  std::mutex inflight_mu_;
  std::map<uint64_t, std::shared_ptr<OutputsInFlight>> inflight_;

  std::mutex pool_mu_;
  std::multimap<size_t, std::vector<char>> pool_;  // capacity -> buffer
  uint64_t pool_bytes_ = 0;

  std::thread recv_threads_[kNumSegments];
  std::atomic<bool> recv_done_[kNumSegments]{};
  std::thread send_threads_[kNumSegments];
  std::atomic<bool> send_done_[kNumSegments]{};
  std::vector<std::thread> compact_threads_;
  std::unique_ptr<std::atomic<bool>[]> compact_done_;
};

}  // namespace ROCKSDB_NAMESPACE
