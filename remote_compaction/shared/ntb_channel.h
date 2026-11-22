#pragma once
// NtbChannel: transport between the primary and the worker over a Dolphin
// PXH830 NTB (PCIe non-transparent bridge), using the SISCI API.
//
// Layout (one instance per process; is_primary_ selects the role):
//   * Control mailboxes: the primary owns one slot for job specs
//     (primary -> worker); the worker owns kNumSegments slots for results
//     and receive reports (worker -> primary). Control messages are small
//     PIO copies with a ready flag.
//   * Data segments: one large SISCI segment per direction, split into
//     kNumSegments slots of kDataSegSize bytes. Each slot carries one chunk
//     at a time: a 64-byte NtbChunkHeader (file number, offset, size, last,
//     job id) followed by up to kChunkDataSize bytes of SST data.
//   * Acks: the receiver copies a chunk out of its slot and then sets
//     `consumed` in the sender's ack region, which frees the slot.
//
// Data and acks move by DMA. The DMA engines are the host's Intel CBDMA
// channels exposed through the Dolphin driver (intel_dma_num_channels=8);
// a fixed pool of kNumDmaWorkers threads, one DMA queue each, submits all
// transfers, and every transfer has a timeout (kDmaTimeoutMs).
//
// Sizing: kNumSegments * kDataSegSize per direction must fit into the
// driver's preallocated NTB memory (ntb_memory_preallocation_size_mb) and
// the prefetchable BAR window; with 8 slots of 90 MiB this is 720 MiB.
// Larger values fail with "SCICreateSegment: Out of local resources".

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

// Idle-wait policy of all flag-polling loops in this process (default
// kBackoff). The worker selects kYield.
enum class NtbWaitMode { kBackoff, kYield };
void SetNtbWaitMode(NtbWaitMode mode);

static constexpr uint32_t kAdapterNo        = 0;
static constexpr uint32_t kPrimaryCtrlSegId = 0x1000;
static constexpr uint32_t kWorkerCtrlSegId  = 0x1001;
static constexpr int      kNumSegments      = 8;

// One DMA queue/thread per CBDMA channel given to the driver.
static constexpr int kNumDmaWorkers = 8;

// A DMA that does not complete in time fails instead of hanging the caller.
static constexpr int kDmaTimeoutMs     = 5000;
static constexpr int kDmaPollIntervalUs = 200;

static constexpr int kChunkPollTimeoutMs = 30000;

static constexpr uint32_t kInputDataSegId  = 0x5010;
static constexpr uint32_t kOutputDataSegId = 0x5020;

static constexpr uint32_t InputAckSegId(int i)  { return 0x5030 + i; }  // owned by primary, pushed by worker
static constexpr uint32_t OutputAckSegId(int i) { return 0x5040 + i; }  // owned by worker, pushed by primary

static constexpr uint32_t kInputStageSegId  = 0x7050;  // wraps input_stage_base_ (primary): input data + output acks
static constexpr uint32_t kOutputStageSegId = 0x7060;  // wraps output_stage_base_ (worker): output data + input acks

// 256 KiB per control slot, large enough for a spec with grandparent bounds.
static constexpr size_t kCtrlSegSize    = 4096 * 64;
static constexpr size_t kCtrlPayloadMax = kCtrlSegSize - 64;

static constexpr uint64_t kChunkDataSize   = (90ULL << 20) - 64;
static constexpr size_t   kChunkHeaderSize = 64;
static constexpr size_t   kDataSegSize     = kChunkHeaderSize + kChunkDataSize;  // per-slot stride

struct NtbCtrlHeader {
  volatile uint32_t msg_ready;
  volatile uint32_t msg_len;
  volatile uint32_t ack;
  uint32_t _pad[13];
};

// Header at the start of every data slot. job_id = 0 means untagged.
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

struct DmaWorkItem {
  void*  lseg          = nullptr;  // sci_local_segment_t
  void*  rseg           = nullptr;  // sci_remote_segment_t
  size_t local_offset   = 0;
  size_t remote_offset  = 0;
  size_t nbytes         = 0;
  unsigned int flags    = 0;
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

  // Control messages. Spec: primary -> worker. Result: worker -> primary,
  // one mailbox slot per segment. SendCtrlMsg picks the direction by role.
  Status SendSpecMsg(const std::string& payload);
  Status RecvSpecMsg(std::string* out);
  Status SendResultMsg(int seg_idx, const std::string& payload);
  Status TryRecvResultMsg(int seg_idx, std::string* out);
  Status SendCtrlMsg(const std::string& payload);

  // Reads an input SST from disk and sends it through slot `fixed_seg`
  // (primary -> worker), chunk by chunk.
  Status StreamSendSST(const std::string& local_path,
                        uint64_t file_number, uint64_t file_size,
                        int fixed_seg = 0,
                        uint64_t job_id = 0);
  // Sends an in-memory output SST through slot `fixed_seg` (worker -> primary).
  Status StreamSendOutputSST(const std::vector<char>& buf,
                              uint64_t file_number, int fixed_seg = 0,
                              uint64_t job_id = 0);

  // Worker side: reads the header of the next input chunk in a slot without
  // consuming it. InputConsumeChunk() must follow exactly once (dest may be
  // nullptr to drop the payload); it copies the payload and acks the slot.
  Status InputPeekChunk(int seg_idx, uint64_t* file_out, uint64_t* off_out,
                        uint64_t* bytes_out, bool* last_out,
                        uint64_t* job_out = nullptr);
  void   InputConsumeChunk(int seg_idx, char* dest, uint64_t bytes);
  // Primary side, non-blocking: true if an output chunk is ready in the
  // slot. OutputConsumeChunk() must follow exactly once (dest may be nullptr
  // to drop it); it copies the payload and acks the slot.
  bool   OutputTryPeekChunk(int seg_idx, uint64_t* file_out, uint64_t* off_out,
                            uint64_t* bytes_out, bool* last_out, uint64_t* job_out);
  void   OutputConsumeChunk(int seg_idx, char* dest, uint64_t bytes);

  bool is_open() const { return open_; }

 private:
  Status InitSISCI();
  void   CleanupSISCI();

  Status StartDmaPool(void* sci);
  void   StopDmaPool();
  void   DmaPoolWorkerLoop(int worker_idx);
  Status SubmitDma(int seg_idx, void* lseg, void* rseg, size_t local_offset,
                    size_t remote_offset, size_t nbytes, unsigned int flags);

  Status DmaTransferWithTimeout(void* dq, void* lseg, void* rseg,
                                 size_t local_offset, size_t remote_offset,
                                 size_t nbytes, unsigned int flags);

  Status InputDmaSend(int seg_idx, size_t offset, size_t nbytes);
  Status InputDmaReadBack(int seg_idx, size_t offset, size_t nbytes);
  Status InputSendChunk(int seg_idx, uint64_t file_number,
                        uint64_t file_offset, uint64_t chunk_bytes, bool is_last,
                        uint64_t job_id = 0);
  Status InputRecvChunk(int seg_idx, uint64_t* file_out, uint64_t* off_out,
                        uint64_t* bytes_out, bool* last_out);
  void   InputAckChunk(int seg_idx);

  Status OutputDmaSend(int seg_idx, size_t offset, size_t nbytes);
  Status OutputDmaReadBack(int seg_idx, size_t offset, size_t nbytes);
  Status OutputSendChunk(int seg_idx, uint64_t file_number,
                         uint64_t file_offset, uint64_t chunk_bytes, bool is_last,
                         uint64_t job_id = 0);
  void   OutputAckChunk(int seg_idx);

  Status InputAckDmaSend(int seg_idx);
  Status OutputAckDmaSend(int seg_idx);

  bool   open_       = false;
  bool   is_primary_ = false;
  void*  sci_        = nullptr;

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

  void* local_input_data_seg_        = nullptr;  // worker: SCI handle, owns cleanup
  void* local_input_data_base_map_   = nullptr;  // worker: mapped base pointer
  void* local_input_data_base_maph_  = nullptr;  // worker: map handle, owns cleanup
  void* remote_input_data_seg_       = nullptr;  // primary: connected handle, owns cleanup -- DMA target for InputDmaSend
  void* remote_input_data_base_map_  = nullptr;  // primary: mapped base pointer (unused by DMA path, harmless)
  void* remote_input_data_base_maph_ = nullptr;  // primary: map handle, owns cleanup
  void* local_input_data_map_[kNumSegments]  = {};
  void* remote_input_data_map_[kNumSegments] = {};

  void* local_output_data_seg_        = nullptr;
  void* local_output_data_base_map_   = nullptr;
  void* local_output_data_base_maph_  = nullptr;
  void* remote_output_data_seg_       = nullptr;  // worker: connected handle -- DMA target for OutputDmaSend
  void* remote_output_data_base_map_  = nullptr;
  void* remote_output_data_base_maph_ = nullptr;
  void* local_output_data_map_[kNumSegments]  = {};
  void* remote_output_data_map_[kNumSegments] = {};

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

  void*                    dma_pool_queue_[kNumDmaWorkers] = {};
  std::thread              dma_pool_threads_[kNumDmaWorkers];
  std::mutex               dma_pool_mu_[kNumDmaWorkers];
  std::condition_variable  dma_pool_cv_[kNumDmaWorkers];
  std::queue<DmaWorkItem*> dma_pool_items_[kNumDmaWorkers];
  std::atomic<bool>        dma_pool_stop_{false};
  bool                     dma_pool_started_ = false;
  bool                     dma_worker_dead_[kNumDmaWorkers] = {};

  char* output_stage_base_     = nullptr;  // worker: base of the big buffer (data + input acks)
  void* output_stage_seg_base_ = nullptr;  // worker: registered-segment handle, owns cleanup
  char* input_stage_base_      = nullptr;  // primary: base of the big buffer (data + output acks)
  void* input_stage_seg_base_  = nullptr;  // primary: registered-segment handle, owns cleanup
  char* output_stage_[kNumSegments] = {};  // = output_stage_base_ + i*kDataSegSize
  char* input_stage_[kNumSegments]  = {};  // = input_stage_base_  + i*kDataSegSize

  char* input_ack_stage_[kNumSegments]  = {};
  char* output_ack_stage_[kNumSegments] = {};

  std::mutex input_send_mu_[kNumSegments];
  std::mutex output_send_mu_[kNumSegments];

  std::mutex spec_send_mu_;
  std::mutex result_send_mu_[kNumSegments];
};

}  // namespace ROCKSDB_NAMESPACE
