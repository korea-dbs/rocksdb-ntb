#pragma once
// ntb_channel.h
// Shared NTB/SISCI transport layer used by both primary and worker
// processes for a remote RocksDB compaction offload system between two
// nodes connected via a Dolphin PXH830 adapter (NTB).
//
// Current config: kNumSegments = 25 parallel slots, kChunkDataSize~=64MB
// per slot (~1600MB total per direction). Requires BAR2 (prefetch space)
// >= 8192MB and ntb_memory_preallocation_size_mb >= 3072 on both nodes.
//
// v10a (this file): on top of v9's 8-worker DMA submission pool, removes
// SCI_FLAG_DMA_WAIT from the actual SCIStartDmaTransfer call and replaces
// it with an async submit + our own SCIDMAQueueState() poll loop with an
// explicit timeout (kDmaTimeoutMs). This directly targets the soft-lockup
// symptom reported to Dolphin: with SCI_FLAG_DMA_WAIT, a pool worker
// thread that hits a wedged physical channel (e.g. channel 2) never
// returns from the library call, pins a CPU core indefinitely, and can't
// be killed with SIGKILL. With v10a, that worker instead times out,
// reports Status::TimedOut for that one work item, and retires itself
// permanently (its queue is presumed wedged and is never reused) --
// leaving the remaining (up to 7) workers to keep servicing the shared
// FIFO. Ctrl messages are UNCHANGED in this version (still PIO via
// CtrlPioSend, see ntb_channel.cc) -- see v10b for that.
//
// Known limitation carried over from this design: if every one of the
// kNumDmaWorkers workers eventually times out (multiple channels wedge
// over the life of one run), a later SubmitDma() call has no live worker
// left to pick it up and will block forever, same as before v10a. Fixing
// that fully would require heap-allocating DmaWorkItem (so a caller-side
// timeout could abandon it safely) instead of the current stack-allocated
// item -- not done here since it's a materially bigger change and this
// run is specifically about the single-channel-hang case.
//
// The big real segments (InputDataSegId / OutputDataSegId) and the
// registered DMA-source stage buffers (input_stage_/output_stage_) are
// each created/registered ONCE, sized kNumSegments * kDataSegSize (plus a
// small ack region appended to the stage buffers -- see below), and
// sliced into per-slot views via pointer/offset arithmetic. This is
// deliberate: SCIRegisterSegmentMemory was found to support only ONE
// concurrent EMPTY|PRIVATE+LOCK_USER_MEM registration on this adapter
// (SCI_ERR_SEGMENTID_USED when a 2nd was attempted, even with a
// completely different segment id).
//
// v8 (all-DMA, ack included): mixing PIO and DMA operations on the same
// channel makes the memory-ordering story more complex than DMA alone.
// Applied to BOTH the ack path (previously always PIO) and the data
// path's PIO fallback. InputPioSend/OutputPioSend/InputAckPioSend/
// OutputAckPioSend and the PIO-only map-sequence objects are removed
// entirely.
//
// v9 (DMA submission pool -- fixed 8 workers/queues, decoupled from slot
// count): the adapter only has kNumDmaWorkers=8 physical DMA engines
// (intel_dma_num_channels=8), confirmed by Dolphin's own guidance and by
// ntb_parallel_fill_test.cc, which deliberately ties its default thread
// count to that number. Prior to v9, every slot owned its OWN
// SCICreateDMAQueue (up to kNumSegments*2 = 80 queues/node at 40 slots),
// and nothing capped how many of those could hit the hardware at the
// same instant -- unlike the standalone test tool, which only ever has
// 8 threads, each reusing ITS OWN single queue strictly sequentially
// (never more than 8 concurrent SCIStartDmaTransfer calls, by
// construction, regardless of how many total slots exist). This
// mismatch is the leading (still unconfirmed by Dolphin) hypothesis for
// the repeated dis-px-dma channel watchdog hangs seen under real
// db_bench load.
//
// v9 fixes this by moving the actual SCIStartDmaTransfer call off the
// caller's thread and into a fixed pool of kNumDmaWorkers=8 dedicated
// worker threads, each owning exactly ONE persistent DMA queue for the
// life of the connection. InputDmaSend/OutputDmaSend/InputAckDmaSend/
// OutputAckDmaSend now just compute the local/remote offsets, enqueue a
// DmaWorkItem, and block on a std::future for the result -- functionally
// identical (synchronous, SCI_FLAG_DMA_WAIT semantics) from the caller's
// point of view, but the hardware never sees more than kNumDmaWorkers
// transfers in flight at once, no matter how many of the kNumSegments
// slots are active concurrently. This mirrors
// ntb_parallel_fill_test.cc's structural guarantee exactly, and requires
// NO changes to worker_server.cc / compaction_dispatcher.cc -- every
// caller of the *DmaSend functions is unaffected.
//
// Because SCIRegisterSegmentMemory allows only ONE concurrent
// registration per node, each node's ack-push stage buffer is appended
// AFTER the main data region inside the SAME registered stage buffer.
// The ack DESTINATION segments (InputAckSegId(i)/OutputAckSegId(i)) are
// independent small SCICreateSegment/advertise calls (no such limit
// applies there) and remain independent per-slot DMA targets -- v9 does
// not change this, only which queue actually executes the transfer.
//
// GLOBAL DMA (SCI_FLAG_DMA_GLOBAL) is used for all transfers after
// SCI_FLAG_DMA_SYSDMA was found to silently report SCI_ERR_OK /
// SCI_DMAQUEUE_DONE while delivering zero bytes under real load.

#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <future>
#include <atomic>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

inline std::string NtbTsNow() {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  struct tm tmv;
  localtime_r(&ts.tv_sec, &tmv);
  char buf[32];
  snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%06ld",
           tmv.tm_hour, tmv.tm_min, tmv.tm_sec,
           static_cast<long>(ts.tv_nsec / 1000));
  return std::string(buf);
}

static constexpr uint32_t kAdapterNo        = 0;
static constexpr uint32_t kPrimaryCtrlSegId = 0x1000;
static constexpr uint32_t kWorkerCtrlSegId  = 0x1001;
static constexpr int      kNumSegments      = 8;

// v9: fixed DMA submission pool size -- matches the adapter's physical
// DMA engine count (intel_dma_num_channels=8), NOT kNumSegments. Do not
// raise this past the real hardware channel count.
static constexpr int kNumDmaWorkers = 8;

// v10a: per-transfer timeout for the async-submit + poll loop in
// DmaTransferWithTimeout(). Must be comfortably above any legitimate
// single-chunk transfer time (chunks are ~64MB) and comfortably below
// "so long it looks like a hang" -- 5s is a first guess; tune against
// real observed per-chunk completion times if this fires under normal
// load. kDmaPollIntervalUs is how often we call SCIDMAQueueState() while
// waiting -- sleep-based, not a busy spin, so this does not reproduce
// the soft-lockup problem itself.
static constexpr int kDmaTimeoutMs     = 5000;
static constexpr int kDmaPollIntervalUs = 200;

// v10a: InputSendChunk/OutputSendChunk's consumed-flag poll and
// InputRecvChunk's ready-flag poll had NO timeout at all before this
// version (plain std::this_thread::yield() loop, forever). These all
// wait on something bounded by the peer's own DMA send, which is itself
// now capped at kDmaTimeoutMs -- 30s is generous headroom above that.
static constexpr int kChunkPollTimeoutMs = 30000;

// OutputRecvChunk's ready-flag poll is different: the primary is waiting
// for the WORKER to finish actual RocksDB compaction (not just a DMA
// send) before the output chunk shows up. Compaction time is workload-
// dependent and can legitimately run much longer than a single chunk
// transfer. 600000ms (10 minutes) is a placeholder -- tune this to
// comfortably exceed the largest compaction job this deployment expects,
// or this will false-fail long-running jobs.
static constexpr int kOutputResultTimeoutMs = 300000;  // 5 min, kept in
                                                        // sync with
                                                        // compaction_dispatcher.h's
                                                        // kJobResultTimeoutMs

// InputDataSegId / OutputDataSegId are single big segments (not
// per-slot) -- kNumSegments slots are sliced out of ONE registration via
// offset = slot_idx * kDataSegSize.
static constexpr uint32_t kInputDataSegId  = 0x5010;
static constexpr uint32_t kOutputDataSegId = 0x5020;

// Ack DESTINATION segments stay small and independent per slot (never
// hit the registration-count limit -- that limit is specific to
// SCIRegisterSegmentMemory, and CreateAndAdvertise/SCICreateSegment does
// not have it). Only the ack SOURCE (stage) buffers were folded into the
// shared registered stage buffer.
static constexpr uint32_t InputAckSegId(int i)  { return 0x5030 + i; }  // owned by primary, pushed by worker
static constexpr uint32_t OutputAckSegId(int i) { return 0x5040 + i; }  // owned by worker, pushed by primary

// Stage buffers (DMA source) are single big registrations. Each one now
// holds BOTH the direction's data slots AND that node's ack push slots,
// laid out back to back: [ data slot 0 .. data slot N-1 | ack slot 0 ..
// ack slot N-1 ].
static constexpr uint32_t kInputStageSegId  = 0x7050;  // wraps input_stage_base_ (primary): input data + output acks
static constexpr uint32_t kOutputStageSegId = 0x7060;  // wraps output_stage_base_ (worker): output data + input acks

static constexpr size_t kCtrlSegSize    = 4096 * 64;
static constexpr size_t kCtrlPayloadMax = kCtrlSegSize - 64;

// Per-slot chunk data size. Total footprint = kNumSegments * kDataSegSize
// (~2560MB with 40 slots x 64MB) per direction's data region, plus a
// negligible kNumSegments * kChunkHeaderSize ack region appended to each
// node's stage buffer.
static constexpr uint64_t kChunkDataSize   = (90ULL << 20) - 64;  // v36 (2026-10-29): 100MB->90MB per slot -- 8 segments x 90MB == 720MB total (vs ntb_memory_preallocation_size_mb(1024MB)), more margin for ctrl/ack segments + any other local-resource consumer. Changed to chase a recurring "SCICreateSegment: Out of local resources" on worker_main startup.
static constexpr size_t   kChunkHeaderSize = 64;
static constexpr size_t   kDataSegSize     = kChunkHeaderSize + kChunkDataSize;  // per-slot stride

struct NtbCtrlHeader {
  volatile uint32_t msg_ready;
  volatile uint32_t msg_len;
  volatile uint32_t ack;
  uint32_t _pad[13];
};

struct NtbChunkHeader {
  volatile uint32_t ready;
  volatile uint32_t consumed;
  volatile uint32_t chunk_bytes;
  volatile uint32_t is_last;
  volatile uint64_t file_number;
  volatile uint64_t file_offset;
  volatile uint64_t job_id;
  uint8_t  _pad[64 - 40];
};
static_assert(sizeof(NtbChunkHeader) == 64, "NtbChunkHeader size mismatch");

// v9: one unit of work for the DMA submission pool. A caller (InputDmaSend
// etc.) fills this in, pushes a pointer to it onto dma_pool_queue_, and
// blocks on the future until one of the kNumDmaWorkers threads executes
// the transfer and sets the result. The struct lives on the calling
// thread's stack for the duration of the (synchronous) call, so the
// pointer stays valid the whole time the pool can see it.
struct DmaWorkItem {
  void*  lseg          = nullptr;  // sci_local_segment_t
  void*  rseg           = nullptr;  // sci_remote_segment_t
  size_t local_offset   = 0;
  size_t remote_offset  = 0;
  size_t nbytes         = 0;
  // for a normal write, | SCI_FLAG_DMA_READ for a read-back verification
  // transfer). See InputDmaReadBack/OutputDmaReadBack below.
  unsigned int flags    = 0;
  // v13 diag: steady_clock microseconds at the moment SubmitDma() pushed
  // this item onto dma_pool_items_ -- lets DmaPoolWorkerLoop report how
  // long the item sat in queue before a worker picked it up.
  uint64_t enqueue_ts_us = 0;
  std::promise<Status> result;
};

class NtbChannel {
 public:
  NtbChannel();
  ~NtbChannel();

  Status OpenAsPrimary(uint32_t worker_node_id);
  Status OpenAsWorker(uint32_t primary_node_id);
  void   Close();

  // ctrl
  Status SendSpecMsg(const std::string& payload);
  Status RecvSpecMsg(std::string* out);
  Status SendResultMsg(int seg_idx, const std::string& payload);
  Status RecvResultMsg(std::string* out);
  Status TryRecvResultMsg(int seg_idx, std::string* out);
  Status SendCtrlMsg(const std::string& payload);
  Status RecvCtrlMsg(std::string* out);

  // data
  Status StreamSendSST(const std::string& local_path,
                        uint64_t file_number, uint64_t file_size,
                        int fixed_seg = 0,
                        uint64_t job_id = 0);
  Status StreamRecvSST(uint64_t file_number, uint64_t file_size,
                        std::vector<char>* buf_out, int fixed_seg = 0);
  Status StreamSendOutputSST(const std::vector<char>& buf,
                              uint64_t file_number, int fixed_seg = 0,
                              uint64_t job_id = 0);
  Status StreamRecvOutputSST(uint64_t file_number, uint64_t file_size,
                              std::vector<char>* buf_out, int fixed_seg = 0,
                              uint64_t job_id = 0);
  Status StreamRecvOutputSSTToFd(uint64_t file_number, uint64_t file_size,
                                  int fd, uint64_t* actual_size_out,
                                  int fixed_seg = 0);

  Status InputPeekChunk(int seg_idx, uint64_t* file_out, uint64_t* off_out,
                        uint64_t* bytes_out, bool* last_out,
                        uint64_t* job_out = nullptr);
  void   InputConsumeChunk(int seg_idx, char* dest, uint64_t bytes);
  bool   OutputTryPeekChunk(int seg_idx, uint64_t* file_out, uint64_t* off_out,
                            uint64_t* bytes_out, bool* last_out, uint64_t* job_out);
  void   OutputConsumeChunk(int seg_idx, char* dest, uint64_t bytes);

  bool is_open() const { return open_; }

 private:
  Status InitSISCI();
  void   CleanupSISCI();

  // v9: DMA submission pool -- shared by whichever two *DmaSend
  // functions this node owns (primary: InputDmaSend + OutputAckDmaSend;
  // worker: OutputDmaSend + InputAckDmaSend). See top-of-file note.
  Status StartDmaPool(void* sci);
  void   StopDmaPool();
  void   DmaPoolWorkerLoop(int worker_idx);
  Status SubmitDma(int seg_idx, void* lseg, void* rseg, size_t local_offset,
                    size_t remote_offset, size_t nbytes, unsigned int flags);

  // v10a: async submit + SCIDMAQueueState() poll with timeout, used by
  // DmaPoolWorkerLoop instead of a blocking SCI_FLAG_DMA_WAIT call. Takes
  // raw dq/lseg/rseg handles (not wrapped types) to keep this header free
  // of the sisci_api.h include, matching the rest of the class.
  Status DmaTransferWithTimeout(void* dq, void* lseg, void* rseg,
                                 size_t local_offset, size_t remote_offset,
                                 size_t nbytes, unsigned int flags);

  // input channel (primary -> worker)
  Status InputDmaSend(int seg_idx, size_t offset, size_t nbytes);
  Status InputDmaReadBack(int seg_idx, size_t offset, size_t nbytes);
  Status InputSendChunk(int seg_idx, uint64_t file_number,
                        uint64_t file_offset, uint64_t chunk_bytes, bool is_last,
                        uint64_t job_id = 0);
  Status InputRecvChunk(int seg_idx, uint64_t* file_out, uint64_t* off_out,
                        uint64_t* bytes_out, bool* last_out);
  void   InputAckChunk(int seg_idx);

  // output channel (worker -> primary)
  Status OutputDmaSend(int seg_idx, size_t offset, size_t nbytes);
  Status OutputDmaReadBack(int seg_idx, size_t offset, size_t nbytes);
  Status OutputSendChunk(int seg_idx, uint64_t file_number,
                         uint64_t file_offset, uint64_t chunk_bytes, bool is_last,
                         uint64_t job_id = 0);
  Status OutputRecvChunk(int seg_idx, uint64_t* file_out, uint64_t* off_out,
                         uint64_t* bytes_out, bool* last_out);
  void   OutputAckChunk(int seg_idx);

  // ack pushes (DMA, via the same pool as data sends)
  Status InputAckDmaSend(int seg_idx);
  Status OutputAckDmaSend(int seg_idx);

  bool   open_       = false;
  bool   is_primary_ = false;
  void*  sci_        = nullptr;

  // ctrl segments
  void* local_primary_ctrl_             = nullptr;
  void* local_primary_ctrl_map_         = nullptr;
  void* local_primary_ctrl_map_handle_  = nullptr;
  void* remote_primary_ctrl_            = nullptr;
  void* remote_primary_ctrl_map_        = nullptr;
  void* remote_primary_ctrl_map_handle_ = nullptr;

  void* local_worker_ctrl_              = nullptr;
  void* local_worker_ctrl_base_map_     = nullptr;
  void* local_worker_ctrl_map_handle_   = nullptr;
  void* local_worker_ctrl_map_[kNumSegments] = {};
  void* remote_worker_ctrl_             = nullptr;
  void* remote_worker_ctrl_base_map_    = nullptr;
  void* remote_worker_ctrl_map_handle_  = nullptr;
  void* remote_worker_ctrl_map_[kNumSegments] = {};
  void* remote_primary_ctrl_sequence_   = nullptr;
  void* remote_worker_ctrl_sequence_[kNumSegments] = {};

  // ---- input data: ONE big real segment (worker-owned), sliced into
  // kNumSegments slots via offset. Created/connected ONCE.
  void* local_input_data_seg_        = nullptr;  // worker: SCI handle, owns cleanup
  void* local_input_data_base_map_   = nullptr;  // worker: mapped base pointer
  void* local_input_data_base_maph_  = nullptr;  // worker: map handle, owns cleanup
  void* remote_input_data_seg_       = nullptr;  // primary: connected handle, owns cleanup -- DMA target for InputDmaSend
  void* remote_input_data_base_map_  = nullptr;  // primary: mapped base pointer (unused by DMA path, harmless)
  void* remote_input_data_base_maph_ = nullptr;  // primary: map handle, owns cleanup
  // derived per-slot views (pointer arithmetic only, no independent SCI objects)
  void* local_input_data_map_[kNumSegments]  = {};
  void* remote_input_data_map_[kNumSegments] = {};

  // ---- output data: ONE big real segment (primary-owned), sliced into
  // kNumSegments slots via offset. Mirrors input data above.
  void* local_output_data_seg_        = nullptr;
  void* local_output_data_base_map_   = nullptr;
  void* local_output_data_base_maph_  = nullptr;
  void* remote_output_data_seg_       = nullptr;  // worker: connected handle -- DMA target for OutputDmaSend
  void* remote_output_data_base_map_  = nullptr;
  void* remote_output_data_base_maph_ = nullptr;
  void* local_output_data_map_[kNumSegments]  = {};
  void* remote_output_data_map_[kNumSegments] = {};

  // consumed-ack DESTINATION segments (dedicated, independent per slot,
  // pushed via DMA through the shared pool).
  void* local_input_ack_[kNumSegments]             = {};  // owned by primary
  void* local_input_ack_map_[kNumSegments]         = {};
  void* local_input_ack_map_handle_[kNumSegments]  = {};
  void* remote_input_ack_[kNumSegments]            = {};  // connected by worker -- DMA target for InputAckDmaSend
  void* remote_input_ack_map_[kNumSegments]        = {};  // unused by DMA path, kept from ConnectAndMap (harmless)
  void* remote_input_ack_map_handle_[kNumSegments] = {};

  void* local_output_ack_[kNumSegments]             = {};  // owned by worker
  void* local_output_ack_map_[kNumSegments]         = {};
  void* local_output_ack_map_handle_[kNumSegments]  = {};
  void* remote_output_ack_[kNumSegments]            = {};  // connected by primary -- DMA target for OutputAckDmaSend
  void* remote_output_ack_map_[kNumSegments]        = {};  // unused by DMA path, kept from ConnectAndMap (harmless)
  void* remote_output_ack_map_handle_[kNumSegments] = {};

  // v9: fixed pool of kNumDmaWorkers persistent DMA queues + worker
  // threads, replacing the old per-slot queue arrays
  // (input_dma_queue_[kNumSegments] etc.). One pool per node, shared by
  // both of that node's *DmaSend functions.
  void*                    dma_pool_queue_[kNumDmaWorkers] = {};
  std::thread              dma_pool_threads_[kNumDmaWorkers];
  std::mutex               dma_pool_mu_[kNumDmaWorkers];
  std::condition_variable  dma_pool_cv_[kNumDmaWorkers];
  std::queue<DmaWorkItem*> dma_pool_items_[kNumDmaWorkers];
  std::atomic<bool>        dma_pool_stop_{false};
  bool                     dma_pool_started_ = false;
  // v10a: set true (under dma_pool_mu_) the moment worker i's transfer
  // times out. That worker thread then returns from DmaPoolWorkerLoop for
  // good -- its queue is presumed wedged and StopDmaPool() will skip
  // SCIRemoveDMAQueue for it rather than risk hanging on the teardown
  // call too.
  bool                     dma_worker_dead_[kNumDmaWorkers] = {};

  // ---- staging buffers (DMA source), ONE big posix_memalign'd
  // allocation each, wrapped ONCE in a registered SCI local segment
  // (SCICreateSegment(SCI_FLAG_EMPTY|SCI_FLAG_PRIVATE) +
  // SCIRegisterSegmentMemory(SCI_FLAG_LOCK_USER_MEM)), sliced into
  // kNumSegments data slots via pointer arithmetic PLUS kNumSegments ack
  // slots appended right after the data region.
  //
  // v11 (2026-09-17, per Dolphin support) tried switching these to a
  // SISCI-allocated ("regular") segment instead, for physical
  // contiguity -- reverted in v12 after it failed on the worker node with
  // "Out of local resources" (a segment this size draws from the same
  // adapter-local resource pool the InputData/OutputData segment is
  // already sized against; a second same-sized one blew the budget).
  // Back to LOCK_USER_MEM; see the RegisterUserMemSegment comment in
  // ntb_channel.cc for the current approach (madvise(MADV_HUGEPAGE) as
  // a zero-risk, best-effort physical-contiguity mitigation instead).
  char* output_stage_base_     = nullptr;  // worker: base of the big buffer (data + input acks)
  void* output_stage_seg_base_ = nullptr;  // worker: registered-segment handle, owns cleanup
  char* input_stage_base_      = nullptr;  // primary: base of the big buffer (data + output acks)
  void* input_stage_seg_base_  = nullptr;  // primary: registered-segment handle, owns cleanup
  char* output_stage_[kNumSegments] = {};  // = output_stage_base_ + i*kDataSegSize
  char* input_stage_[kNumSegments]  = {};  // = input_stage_base_  + i*kDataSegSize

  // derived views into the ack region appended after the data region in
  // the SAME buffer/registration as above (input_ack_stage_ lives in
  // output_stage_base_ on the worker; output_ack_stage_ lives in
  // input_stage_base_ on the primary). Not independently
  // posix_memalign'd/freed -- CleanupSISCI must NOT free() these
  // individually.
  char* input_ack_stage_[kNumSegments]  = {};
  char* output_ack_stage_[kNumSegments] = {};

  // per-segment mutexes serializing InputSendChunk/OutputSendChunk so the
  // consumed-flag poll+reset and the subsequent send are never entered
  // concurrently for the same seg_idx.
  std::mutex input_send_mu_[kNumSegments];
  std::mutex output_send_mu_[kNumSegments];

  std::mutex spec_send_mu_;
  std::mutex result_send_mu_[kNumSegments];
};

}  // namespace ROCKSDB_NAMESPACE
