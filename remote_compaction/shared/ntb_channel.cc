// Implementation of the NTB/SISCI transport layer (see ntb_channel.h).
// Shared between the primary process and the worker process.

#include "ntb_channel.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>

#include <sisci_api.h>
#include <sisci_error.h>

namespace {
// Idle wait used by every flag-polling loop.
//   kBackoff (primary): spin, then yield, then short sleeps. A gap longer
//                       than ~0.3 ms between calls starts a new wait.
//   kYield   (worker):  always std::this_thread::yield().
constexpr uint32_t kSpinRounds = 64;
constexpr uint32_t kYieldRounds = 8;
constexpr uint32_t kSleepUs = 20;
std::atomic<int> g_wait_mode{0};  // ROCKSDB_NAMESPACE::NtbWaitMode

inline void NtbBackoff() {
  if (g_wait_mode.load(std::memory_order_relaxed) != 0) {
    std::this_thread::yield();
    return;
  }
  thread_local uint32_t n = 0;
  thread_local uint64_t last_ns = 0;
  const uint64_t now = (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
  const uint64_t gap = (uint64_t)kSleepUs * 4000ULL + 200000ULL;
  if (now - last_ns > gap) n = 0;
  last_ns = now;
  ++n;
  if (n <= kSpinRounds) {
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#endif
    return;
  }
  if (n <= kSpinRounds + kYieldRounds) {
    std::this_thread::yield();
    return;
  }
  std::this_thread::sleep_for(std::chrono::microseconds(kSleepUs));
}
}  // namespace

namespace ROCKSDB_NAMESPACE {

void SetNtbWaitMode(NtbWaitMode mode) {
  g_wait_mode.store(mode == NtbWaitMode::kYield ? 1 : 0,
                    std::memory_order_relaxed);
}

// Total per-direction data-region size and per-node ack-region size,
// shared by OpenAsPrimary/OpenAsWorker (buffer sizing/layout) and
// InputAckDmaSend/OutputAckDmaSend (offset math). kAckRegionSize is
// appended right after the data region inside the same registered stage
// buffer.
static constexpr size_t kTotalDataSize = static_cast<size_t>(kNumSegments) * kDataSegSize;
static constexpr size_t kAckRegionSize = static_cast<size_t>(kNumSegments) * kChunkHeaderSize;

static Status SciErr(const char* what, sci_error_t e) {
  return Status::IOError(std::string(what) + ": " + SCIGetErrorString(e));
}

// Waits (with backoff) until *flag == want, or fails after timeout_ms so
// that a dead peer or a hung DMA cannot block the caller forever.
static Status WaitAtomicFlag(volatile uint32_t* flag, uint32_t want,
                             int timeout_ms, const char* what) {
  auto t_start = std::chrono::steady_clock::now();
  while (__atomic_load_n(flag, __ATOMIC_ACQUIRE) != want) {
    if (std::chrono::steady_clock::now() - t_start >
        std::chrono::milliseconds(timeout_ms)) {
      return Status::TimedOut(std::string(what) + ": poll timeout after " +
                              std::to_string(timeout_ms) + "ms");
    }
    NtbBackoff();
  }
  return Status::OK();
}

static Status CreateAndAdvertise(sci_desc_t sci, uint32_t seg_id,
                                  size_t size, void** seg_out,
                                  void** map_handle_out, void** map_out) {
  sci_error_t e = SCI_ERR_OK;
  SCICreateSegment(sci, reinterpret_cast<sci_local_segment_t*>(seg_out),
                    seg_id, size, nullptr, nullptr, 0, &e);
  if (e != SCI_ERR_OK) return SciErr("SCICreateSegment", e);

  SCIPrepareSegment(*reinterpret_cast<sci_local_segment_t*>(seg_out),
                     kAdapterNo, 0, &e);
  if (e != SCI_ERR_OK) return SciErr("SCIPrepareSegment", e);

  SCISetSegmentAvailable(*reinterpret_cast<sci_local_segment_t*>(seg_out),
                          kAdapterNo, 0, &e);
  if (e != SCI_ERR_OK) return SciErr("SCISetSegmentAvailable", e);

  *map_out = SCIMapLocalSegment(
      *reinterpret_cast<sci_local_segment_t*>(seg_out),
      reinterpret_cast<sci_map_t*>(map_handle_out), 0, size, nullptr, 0, &e);
  if (e != SCI_ERR_OK) return SciErr("SCIMapLocalSegment", e);

  return Status::OK();
}

static Status ConnectAndMap(sci_desc_t sci, uint32_t node_id, uint32_t seg_id,
                             size_t size, void** seg_out,
                             void** map_handle_out, void** map_out,
                             int retries = 60) {
  sci_error_t e = SCI_ERR_OK;
  for (int i = 0; i < retries; i++) {
    SCIConnectSegment(sci, reinterpret_cast<sci_remote_segment_t*>(seg_out),
                       node_id, seg_id, kAdapterNo, nullptr, nullptr, 0, 0,
                       &e);
    if (e == SCI_ERR_OK) break;
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  if (e != SCI_ERR_OK) return SciErr("SCIConnectSegment", e);

  volatile void* vmap = SCIMapRemoteSegment(
      *reinterpret_cast<sci_remote_segment_t*>(seg_out),
      reinterpret_cast<sci_map_t*>(map_handle_out), 0, size, nullptr, 0, &e);
  if (e != SCI_ERR_OK) return SciErr("SCIMapRemoteSegment", e);
  *map_out = const_cast<void*>(vmap);

  return Status::OK();
}

// The stage buffers are ordinary posix_memalign'd memory registered with
// LOCK_USER_MEM. A SISCI-owned segment of this size would draw from the
// adapter's local resource pool and exceed it ("Out of local resources").
// Physical contiguity is requested only as a best-effort THP hint.

// Wrap an already-allocated (posix_memalign'd) user buffer in its own SCI
// local segment so DMA sends can use SCIStartDmaTransfer() on the segment
// instead of SCIStartDmaTransferMem() on the raw pointer:
//   SCICreateSegment(SCI_FLAG_EMPTY | SCI_FLAG_PRIVATE)
//   -> SCIRegisterSegmentMemory(SCI_FLAG_LOCK_USER_MEM)
//   -> SCIPrepareSegment
//
// Called ONCE per direction on a big buffer instead of once per slot --
// SCIRegisterSegmentMemory was found to support only ONE concurrent
// SCI_FLAG_EMPTY|SCI_FLAG_PRIVATE + SCI_FLAG_LOCK_USER_MEM registration on
// this adapter (SCI_ERR_SEGMENTID_USED on a 2nd attempt, even with a
// different segment id). That one big buffer also carries this node's
// ack-push region appended after the data region (kTotalDataSize +
// kAckRegionSize total) -- see OpenAsPrimary/OpenAsWorker.
static Status RegisterUserMemSegment(sci_desc_t sci, uint32_t seg_id,
                                      void* user_mem, size_t size,
                                      void** seg_out) {
  sci_error_t e = SCI_ERR_OK;
  SCICreateSegment(sci, reinterpret_cast<sci_local_segment_t*>(seg_out),
                    seg_id, size, nullptr, nullptr,
                    SCI_FLAG_EMPTY | SCI_FLAG_PRIVATE, &e);
  if (e != SCI_ERR_OK) return SciErr("SCICreateSegment (user mem)", e);

  SCIRegisterSegmentMemory(
      user_mem, size, *reinterpret_cast<sci_local_segment_t*>(seg_out),
      SCI_FLAG_LOCK_USER_MEM, &e);
  if (e != SCI_ERR_OK) return SciErr("SCIRegisterSegmentMemory", e);

  SCIPrepareSegment(*reinterpret_cast<sci_local_segment_t*>(seg_out),
                     kAdapterNo, 0, &e);
  if (e != SCI_ERR_OK) return SciErr("SCIPrepareSegment (user mem)", e);

  return Status::OK();
}

// best-effort THP hint for a stage buffer -- logs a warning and
// continues on failure (never fatal; this is advisory only, not a
// resource request the driver can run out of).
static void HintHugePage(void* addr, size_t size, const char* which) {
  if (madvise(addr, size, MADV_HUGEPAGE) != 0) {
    fprintf(stderr,
            "[ntb] warning: madvise(MADV_HUGEPAGE) on %s failed: %s "
            "(non-fatal, continuing with regular pages)\n",
            which, strerror(errno));
  }
}

NtbChannel::NtbChannel() = default;
NtbChannel::~NtbChannel() { Close(); }

Status NtbChannel::InitSISCI() {
  sci_error_t e = SCI_ERR_OK;
  SCIInitialize(0, &e);
  if (e != SCI_ERR_OK) return SciErr("SCIInitialize", e);
  SCIOpen(reinterpret_cast<sci_desc_t*>(&sci_), 0, &e);
  if (e != SCI_ERR_OK) return SciErr("SCIOpen", e);
  return Status::OK();
}

// ---- DMA submission pool -------------------------------------------
// Fixed kNumDmaWorkers (=8, matches the adapter's physical DMA engine
// count) persistent queues + worker threads, replacing the old
// per-slot-owned queues. Every *DmaSend function on this node funnels
// through SubmitDma() -> the shared work queue -> whichever of the 8
// threads is free next. This caps real concurrent hardware usage at 8
// no matter how many of the kNumSegments slots are active at once,
// mirroring ntb_parallel_fill_test.cc's fixed 8-thread design.
Status NtbChannel::StartDmaPool(void* sci_void) {
  sci_desc_t sci = reinterpret_cast<sci_desc_t>(sci_void);
  sci_error_t e = SCI_ERR_OK;
  for (int i = 0; i < kNumDmaWorkers; i++) {
    SCICreateDMAQueue(sci, reinterpret_cast<sci_dma_queue_t*>(&dma_pool_queue_[i]),
                       kAdapterNo, 1, SCI_FLAG_DMA_USER_MEM, &e);
    if (e != SCI_ERR_OK) return SciErr("SCICreateDMAQueue (pool)", e);
  }
  for (int i = 0; i < kNumDmaWorkers; i++) dma_worker_dead_[i] = false;
  dma_pool_stop_ = false;
  for (int i = 0; i < kNumDmaWorkers; i++) {
    dma_pool_threads_[i] = std::thread(&NtbChannel::DmaPoolWorkerLoop, this, i);
  }
  dma_pool_started_ = true;
  return Status::OK();
}

void NtbChannel::StopDmaPool() {
  if (!dma_pool_started_) return;
  dma_pool_stop_.store(true, std::memory_order_release);
  for (int i = 0; i < kNumDmaWorkers; i++) dma_pool_cv_[i].notify_all();
  for (int i = 0; i < kNumDmaWorkers; i++) {
    if (dma_pool_threads_[i].joinable()) dma_pool_threads_[i].join();
  }
  sci_error_t e = SCI_ERR_OK;
  for (int i = 0; i < kNumDmaWorkers; i++) {
    if (dma_worker_dead_[i]) {
      fprintf(stderr,
             "[ntb] StopDmaPool: worker[%d]'s queue was retired after a "
             "timeout earlier in this run -- skipping SCIRemoveDMAQueue "
             "for it (removal itself could hang on a still-wedged "
             "channel). This handle is deliberately leaked; the node "
             "will need a reboot to fully clear it, same as before.\n", i);
      dma_pool_queue_[i] = nullptr;
      continue;
    }
    if (dma_pool_queue_[i]) {
      SCIRemoveDMAQueue(reinterpret_cast<sci_dma_queue_t>(dma_pool_queue_[i]), 0, &e);
      dma_pool_queue_[i] = nullptr;
    }
  }
  dma_pool_started_ = false;
}

// DMA submit is asynchronous (no SCI_FLAG_DMA_WAIT) and completion is polled
// with SCIDMAQueueState() and a timeout: with SCI_FLAG_DMA_WAIT a hung
// channel pinned the calling thread in the driver indefinitely. Only
// SCI_DMAQUEUE_DONE counts as success; anything else until the timeout is
// treated as still running.
//
// g_dma_active_workers counts pool threads with a transfer outstanding; it
// is only logged.
static std::atomic<int> g_dma_active_workers{0};

static inline uint64_t NowUs() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

Status NtbChannel::DmaTransferWithTimeout(void* dq_void, void* lseg_void,
                                          void* rseg_void, size_t local_offset,
                                          size_t remote_offset, size_t nbytes,
                                          unsigned int flags) {
  sci_dma_queue_t      dq   = reinterpret_cast<sci_dma_queue_t>(dq_void);
  sci_local_segment_t  lseg = reinterpret_cast<sci_local_segment_t>(lseg_void);
  sci_remote_segment_t rseg = reinterpret_cast<sci_remote_segment_t>(rseg_void);
  sci_error_t e = SCI_ERR_OK;

  SCIStartDmaTransfer(dq, lseg, rseg, local_offset, nbytes, remote_offset,
                      nullptr, nullptr, flags, &e);
  if (e != SCI_ERR_OK) {
    return Status::IOError(std::string("DmaTransferWithTimeout: async submit failed: ") +
                           SCIGetErrorString(e));
  }

  auto t_start = std::chrono::steady_clock::now();
  for (;;) {
    sci_dma_queue_state_t st = SCIDMAQueueState(dq);
    if (st == SCI_DMAQUEUE_DONE) return Status::OK();

    if (std::chrono::steady_clock::now() - t_start >
        std::chrono::milliseconds(kDmaTimeoutMs)) {
      return Status::TimedOut(
          "DmaTransferWithTimeout: SCIDMAQueueState never reached DONE "
          "within timeout (channel likely hung)");
    }
    std::this_thread::sleep_for(std::chrono::microseconds(kDmaPollIntervalUs));
  }
}

void NtbChannel::DmaPoolWorkerLoop(int worker_idx) {
  for (;;) {
    DmaWorkItem* item = nullptr;
    {
      std::unique_lock<std::mutex> lk(dma_pool_mu_[worker_idx]);
      dma_pool_cv_[worker_idx].wait(lk, [&] {
        return !dma_pool_items_[worker_idx].empty() ||
               dma_pool_stop_.load(std::memory_order_acquire);
      });
      if (dma_pool_stop_.load(std::memory_order_acquire) &&
          dma_pool_items_[worker_idx].empty()) return;
      if (dma_pool_items_[worker_idx].empty()) continue;
      item = dma_pool_items_[worker_idx].front();
      dma_pool_items_[worker_idx].pop();
    }

    // see the comment above DmaTransferWithTimeout.
    const uint64_t t_pop_us      = NowUs();
    const uint64_t queue_wait_us = t_pop_us - item->enqueue_ts_us;
    const int active_start =
        g_dma_active_workers.fetch_add(1, std::memory_order_relaxed) + 1;
    const bool is_ack = (item->nbytes == kChunkHeaderSize);

    Status s = DmaTransferWithTimeout(dma_pool_queue_[worker_idx], item->lseg,
                                      item->rseg, item->local_offset,
                                      item->remote_offset, item->nbytes,
                                      item->flags);

    const int active_end =
        g_dma_active_workers.fetch_sub(1, std::memory_order_relaxed) - 1;
    const uint64_t xfer_us = NowUs() - t_pop_us;
    fprintf(stderr,
           "[ntb][DMA] worker=%d type=%s bytes=%zu queue_wait_us=%llu "
           "xfer_us=%llu active_start=%d active_end=%d ts=%s\n",
           worker_idx, is_ack ? "ACK" : "DATA", item->nbytes,
           (unsigned long long)queue_wait_us, (unsigned long long)xfer_us,
           active_start, active_end, NtbTsNow().c_str());

    item->result.set_value(s);

    if (!s.ok()) {
      bool is_hang = s.IsTimedOut();
      fprintf(stderr,
             "[ntb] DmaPool[%d]: %s (%s)%s\n", worker_idx,
             is_hang ? "TIMED OUT" : "transfer failed", s.ToString().c_str(),
             is_hang ? " -- retiring this worker permanently, remaining "
                       "workers continue"
                     : " -- no retry for this item, worker continues");
      if (is_hang) {
        std::lock_guard<std::mutex> lk(dma_pool_mu_[worker_idx]);
        dma_worker_dead_[worker_idx] = true;
        return;  // this thread's queue is presumed wedged -- do not reuse it
      }
    }
  }
}

// Blocking submit: enqueues the work item and waits for whichever pool
// thread picks it up to finish the transfer. Semantically identical to
// the old direct SCIStartDmaTransfer(..., SCI_FLAG_DMA_WAIT, ...) call
// from the caller's point of view -- still synchronous, still returns
// only once the transfer has completed (or failed).
Status NtbChannel::SubmitDma(int seg_idx, void* lseg, void* rseg, size_t local_offset,
                              size_t remote_offset, size_t nbytes,
                              unsigned int flags) {
  DmaWorkItem item;
  item.lseg          = lseg;
  item.rseg          = rseg;
  item.local_offset  = local_offset;
  item.remote_offset = remote_offset;
  item.nbytes        = nbytes;
  item.flags         = flags;
  item.enqueue_ts_us  = NowUs();
  std::future<Status> fut = item.result.get_future();

  // route this item to the worker/engine dedicated to this
  // segment (kNumSegments == kNumDmaWorkers) instead of a shared queue
  // any of the kNumDmaWorkers threads could pop from -- guarantees a
  // chunk's WRITE and its READ-BACK always go through the SAME
  // physical DMA engine.
  const int w = seg_idx % kNumDmaWorkers;
  {
    std::lock_guard<std::mutex> lk(dma_pool_mu_[w]);
    dma_pool_items_[w].push(&item);
  }
  dma_pool_cv_[w].notify_one();

  return fut.get();
}

// ---- OpenAsPrimary --------------------------------------------------
Status NtbChannel::OpenAsPrimary(uint32_t worker_node_id) {
  is_primary_ = true;
  Status s = InitSISCI();
  if (!s.ok()) return s;

  sci_desc_t sci = reinterpret_cast<sci_desc_t>(sci_);
  sci_error_t e = SCI_ERR_OK;
  (void)e;

  s = CreateAndAdvertise(sci, kPrimaryCtrlSegId, kCtrlSegSize,
                         &local_primary_ctrl_, &local_primary_ctrl_map_handle_,
                         &local_primary_ctrl_map_);
  if (!s.ok()) return s;
  memset(local_primary_ctrl_map_, 0, sizeof(NtbCtrlHeader));

  s = CreateAndAdvertise(sci, kWorkerCtrlSegId, kNumSegments * kCtrlSegSize,
                         &local_worker_ctrl_, &local_worker_ctrl_map_handle_,
                         &local_worker_ctrl_base_map_);
  if (!s.ok()) return s;
  memset(local_worker_ctrl_base_map_, 0, kNumSegments * kCtrlSegSize);
  for (int i = 0; i < kNumSegments; i++) {
    local_worker_ctrl_map_[i] =
        static_cast<char*>(local_worker_ctrl_base_map_) + i * kCtrlSegSize;
  }

  // Output data segment, owned by primary (worker writes into it), is ONE
  // big real segment sized kNumSegments * kDataSegSize -- sliced into
  // per-slot views below via plain pointer arithmetic.
  s = CreateAndAdvertise(sci, kOutputDataSegId, kTotalDataSize,
                         &local_output_data_seg_, &local_output_data_base_maph_,
                         &local_output_data_base_map_);
  if (!s.ok()) return s;
  memset(local_output_data_base_map_, 0, kTotalDataSize);
  for (int i = 0; i < kNumSegments; i++) {
    local_output_data_map_[i] =
        static_cast<char*>(local_output_data_base_map_) + i * kDataSegSize;
    auto* hdr = reinterpret_cast<NtbChunkHeader*>(local_output_data_map_[i]);
    hdr->consumed = 1;
    hdr->ready    = 0;
  }

  for (int i = 0; i < kNumSegments; i++) {
    s = CreateAndAdvertise(sci, InputAckSegId(i), kChunkHeaderSize,
                           &local_input_ack_[i], &local_input_ack_map_handle_[i],
                           &local_input_ack_map_[i]);
    if (!s.ok()) return s;
    memset(local_input_ack_map_[i], 0, kChunkHeaderSize);
    auto* ihdr = reinterpret_cast<NtbChunkHeader*>(local_input_ack_map_[i]);
    ihdr->consumed = 1;
  }

  // input_stage_base_ (DMA source buffer) is ONE big posix_memalign'd
  // allocation, kTotalDataSize (data slots) + kAckRegionSize (this node's
  // output-ack push slots, appended right after) -- wrapped in ONE
  // registered SCI segment (LOCK_USER_MEM, see RegisterUserMemSegment;
  // MADV_HUGEPAGE below is a best-effort contiguity hint).
  if (posix_memalign(reinterpret_cast<void**>(&input_stage_base_), 4096,
                     kTotalDataSize + kAckRegionSize))
    return Status::IOError("input_stage_base_ posix_memalign failed");
  HintHugePage(input_stage_base_, kTotalDataSize + kAckRegionSize, "input_stage_base_");
  memset(input_stage_base_, 0, kTotalDataSize + kAckRegionSize);
  for (int i = 0; i < kNumSegments; i++) {
    input_stage_[i] = input_stage_base_ + i * kDataSegSize;
    output_ack_stage_[i] = input_stage_base_ + kTotalDataSize + i * kChunkHeaderSize;
  }

  s = RegisterUserMemSegment(sci, kInputStageSegId, input_stage_base_,
                             kTotalDataSize + kAckRegionSize, &input_stage_seg_base_);
  if (!s.ok()) return s;

  s = ConnectAndMap(sci, worker_node_id, kPrimaryCtrlSegId, kCtrlSegSize,
                    &remote_primary_ctrl_, &remote_primary_ctrl_map_handle_,
                    &remote_primary_ctrl_map_);
  if (!s.ok()) return s;
  {
    sci_error_t se = SCI_ERR_OK;
    SCICreateMapSequence(
        *reinterpret_cast<sci_map_t*>(&remote_primary_ctrl_map_handle_),
        reinterpret_cast<sci_sequence_t*>(&remote_primary_ctrl_sequence_), 0,
        &se);
    if (se != SCI_ERR_OK)
      return SciErr("SCICreateMapSequence primary_ctrl", se);
  }

  s = ConnectAndMap(sci, worker_node_id, kWorkerCtrlSegId,
                    kNumSegments * kCtrlSegSize,
                    &remote_worker_ctrl_, &remote_worker_ctrl_map_handle_,
                    &remote_worker_ctrl_base_map_);
  if (!s.ok()) return s;
  for (int i = 0; i < kNumSegments; i++) {
    remote_worker_ctrl_map_[i] =
        static_cast<char*>(remote_worker_ctrl_base_map_) + i * kCtrlSegSize;
    sci_error_t se = SCI_ERR_OK;
    SCICreateMapSequence(
        *reinterpret_cast<sci_map_t*>(&remote_worker_ctrl_map_handle_),
        reinterpret_cast<sci_sequence_t*>(&remote_worker_ctrl_sequence_[i]), 0,
        &se);
    if (se != SCI_ERR_OK)
      return SciErr("SCICreateMapSequence worker_ctrl", se);
  }

  // Input data segment (worker-owned) is ONE big segment -- connect to it
  // ONCE, then derive kNumSegments per-slot views via pointer arithmetic.
  s = ConnectAndMap(sci, worker_node_id, kInputDataSegId, kTotalDataSize,
                    &remote_input_data_seg_, &remote_input_data_base_maph_,
                    &remote_input_data_base_map_);
  if (!s.ok()) return s;
  for (int i = 0; i < kNumSegments; i++) {
    remote_input_data_map_[i] =
        static_cast<char*>(remote_input_data_base_map_) + i * kDataSegSize;
  }

  // no more per-slot queue creation here -- one shared 8-worker pool
  // for the whole node, covering both InputDmaSend and OutputAckDmaSend.
  s = StartDmaPool(sci_);
  if (!s.ok()) return s;

  // Ack destination segments are reached via their connected
  // sci_remote_segment_t handle directly (DMA target for
  // OutputAckDmaSend).
  for (int i = 0; i < kNumSegments; i++) {
    s = ConnectAndMap(sci, worker_node_id, OutputAckSegId(i),
                      kChunkHeaderSize, &remote_output_ack_[i],
                      &remote_output_ack_map_handle_[i],
                      &remote_output_ack_map_[i]);
    if (!s.ok()) return s;
  }

  open_ = true;

  for (int i = 0; i < kNumSegments; i++) {
    auto* rhdr = reinterpret_cast<NtbChunkHeader*>(remote_input_data_map_[i]);
    while (__atomic_load_n(&rhdr->consumed, __ATOMIC_ACQUIRE) != 1)
      NtbBackoff();
  }

  return Status::OK();
}

// ---- OpenAsWorker -----------------------------------------------------
Status NtbChannel::OpenAsWorker(uint32_t primary_node_id) {
  is_primary_ = false;
  Status s = InitSISCI();
  if (!s.ok()) return s;

  sci_desc_t sci = reinterpret_cast<sci_desc_t>(sci_);

  s = CreateAndAdvertise(sci, kWorkerCtrlSegId, kNumSegments * kCtrlSegSize,
                         &local_worker_ctrl_, &local_worker_ctrl_map_handle_,
                         &local_worker_ctrl_base_map_);
  if (!s.ok()) return s;
  memset(local_worker_ctrl_base_map_, 0, kNumSegments * kCtrlSegSize);
  for (int i = 0; i < kNumSegments; i++) {
    local_worker_ctrl_map_[i] =
        static_cast<char*>(local_worker_ctrl_base_map_) + i * kCtrlSegSize;
  }

  s = CreateAndAdvertise(sci, kPrimaryCtrlSegId, kCtrlSegSize,
                         &local_primary_ctrl_, &local_primary_ctrl_map_handle_,
                         &local_primary_ctrl_map_);
  if (!s.ok()) return s;
  memset(local_primary_ctrl_map_, 0, sizeof(NtbCtrlHeader));

  // Input data segment (worker-owned) is ONE big real segment -- see the
  // matching note in OpenAsPrimary above.
  s = CreateAndAdvertise(sci, kInputDataSegId, kTotalDataSize,
                         &local_input_data_seg_, &local_input_data_base_maph_,
                         &local_input_data_base_map_);
  if (!s.ok()) return s;
  memset(local_input_data_base_map_, 0, kTotalDataSize);
  for (int i = 0; i < kNumSegments; i++) {
    local_input_data_map_[i] =
        static_cast<char*>(local_input_data_base_map_) + i * kDataSegSize;
    auto* lhdr = reinterpret_cast<NtbChunkHeader*>(local_input_data_map_[i]);
    lhdr->consumed = 1;
    lhdr->ready    = 0;
  }

  for (int i = 0; i < kNumSegments; i++) {
    s = CreateAndAdvertise(sci, OutputAckSegId(i), kChunkHeaderSize,
                           &local_output_ack_[i],
                           &local_output_ack_map_handle_[i],
                           &local_output_ack_map_[i]);
    if (!s.ok()) return s;
    memset(local_output_ack_map_[i], 0, kChunkHeaderSize);
    auto* ohdr = reinterpret_cast<NtbChunkHeader*>(local_output_ack_map_[i]);
    ohdr->consumed = 1;
  }

  // output_stage_base_ (DMA source buffer) is ONE big posix_memalign'd
  // allocation, kTotalDataSize (data slots) + kAckRegionSize (this node's
  // input-ack push slots, appended right after) -- wrapped in ONE
  // registered SCI segment (back to LOCK_USER_MEM -- see the note
  // above RegisterUserMemSegment).
  if (posix_memalign(reinterpret_cast<void**>(&output_stage_base_), 4096,
                     kTotalDataSize + kAckRegionSize))
    return Status::IOError("output_stage_base_ posix_memalign failed");
  HintHugePage(output_stage_base_, kTotalDataSize + kAckRegionSize, "output_stage_base_");
  memset(output_stage_base_, 0, kTotalDataSize + kAckRegionSize);
  for (int i = 0; i < kNumSegments; i++) {
    output_stage_[i] = output_stage_base_ + i * kDataSegSize;
    input_ack_stage_[i] = output_stage_base_ + kTotalDataSize + i * kChunkHeaderSize;
  }

  s = RegisterUserMemSegment(sci, kOutputStageSegId, output_stage_base_,
                             kTotalDataSize + kAckRegionSize, &output_stage_seg_base_);
  if (!s.ok()) return s;

  s = ConnectAndMap(sci, primary_node_id, kPrimaryCtrlSegId, kCtrlSegSize,
                    &remote_primary_ctrl_, &remote_primary_ctrl_map_handle_,
                    &remote_primary_ctrl_map_, 300);
  if (!s.ok()) return s;
  {
    sci_error_t se = SCI_ERR_OK;
    SCICreateMapSequence(
        *reinterpret_cast<sci_map_t*>(&remote_primary_ctrl_map_handle_),
        reinterpret_cast<sci_sequence_t*>(&remote_primary_ctrl_sequence_), 0,
        &se);
    if (se != SCI_ERR_OK)
      return SciErr("SCICreateMapSequence primary_ctrl", se);
  }

  s = ConnectAndMap(sci, primary_node_id, kWorkerCtrlSegId,
                    kNumSegments * kCtrlSegSize,
                    &remote_worker_ctrl_, &remote_worker_ctrl_map_handle_,
                    &remote_worker_ctrl_base_map_, 300);
  if (!s.ok()) return s;
  for (int i = 0; i < kNumSegments; i++) {
    remote_worker_ctrl_map_[i] =
        static_cast<char*>(remote_worker_ctrl_base_map_) + i * kCtrlSegSize;
    sci_error_t se = SCI_ERR_OK;
    SCICreateMapSequence(
        *reinterpret_cast<sci_map_t*>(&remote_worker_ctrl_map_handle_),
        reinterpret_cast<sci_sequence_t*>(&remote_worker_ctrl_sequence_[i]), 0,
        &se);
    if (se != SCI_ERR_OK)
      return SciErr("SCICreateMapSequence worker_ctrl", se);
  }

  // Output data segment (primary-owned) is ONE big segment -- connect
  // ONCE, derive kNumSegments per-slot views.
  s = ConnectAndMap(sci, primary_node_id, kOutputDataSegId, kTotalDataSize,
                    &remote_output_data_seg_, &remote_output_data_base_maph_,
                    &remote_output_data_base_map_, 300);
  if (!s.ok()) return s;
  for (int i = 0; i < kNumSegments; i++) {
    remote_output_data_map_[i] =
        static_cast<char*>(remote_output_data_base_map_) + i * kDataSegSize;
  }

  // shared 8-worker DMA pool for this node, covering both
  // OutputDmaSend and InputAckDmaSend.
  s = StartDmaPool(sci_);
  if (!s.ok()) return s;

  for (int i = 0; i < kNumSegments; i++) {
    s = ConnectAndMap(sci, primary_node_id, InputAckSegId(i),
                      kChunkHeaderSize, &remote_input_ack_[i],
                      &remote_input_ack_map_handle_[i],
                      &remote_input_ack_map_[i], 300);
    if (!s.ok()) return s;
  }

  // The input slot headers are deliberately not reset here: the primary may
  // already have written the first chunk of job 1 into a slot.

  for (int i = 0; i < kNumSegments; i++) {
    auto* rhdr = reinterpret_cast<NtbChunkHeader*>(remote_output_data_map_[i]);
    while (__atomic_load_n(&rhdr->consumed, __ATOMIC_ACQUIRE) != 1)
      NtbBackoff();
  }

  open_ = true;
  return Status::OK();
}

// ---- CleanupSISCI -------------------------------------------------
void NtbChannel::CleanupSISCI() {
  sci_error_t e = SCI_ERR_OK;
  sci_desc_t sci = reinterpret_cast<sci_desc_t>(sci_);
  (void)sci;

  // stop the DMA pool FIRST -- joins all 8 worker threads (so no
  // thread is mid-transfer against a segment we're about to tear down)
  // and removes the 8 pooled queues, before any per-slot segment
  // cleanup below.
  StopDmaPool();

  // Per-slot independent objects: ack destination segments/maps, and the
  // ack stage views -- which are derived pointers into
  // input_stage_base_/output_stage_base_ and must NOT be free()'d
  // individually; the base buffer is freed once below, after its
  // registered segment is torn down.
  for (int i = kNumSegments - 1; i >= 0; i--) {
    if (local_input_ack_map_handle_[i])
      SCIUnmapSegment(*reinterpret_cast<sci_map_t*>(&local_input_ack_map_handle_[i]), 0, &e);
    if (remote_input_ack_map_handle_[i])
      SCIUnmapSegment(*reinterpret_cast<sci_map_t*>(&remote_input_ack_map_handle_[i]), 0, &e);
    if (local_input_ack_[i])
      SCIRemoveSegment(reinterpret_cast<sci_local_segment_t>(local_input_ack_[i]), 0, &e);
    if (remote_input_ack_[i])
      SCIDisconnectSegment(reinterpret_cast<sci_remote_segment_t>(remote_input_ack_[i]), 0, &e);

    if (local_output_ack_map_handle_[i])
      SCIUnmapSegment(*reinterpret_cast<sci_map_t*>(&local_output_ack_map_handle_[i]), 0, &e);
    if (remote_output_ack_map_handle_[i])
      SCIUnmapSegment(*reinterpret_cast<sci_map_t*>(&remote_output_ack_map_handle_[i]), 0, &e);
    if (local_output_ack_[i])
      SCIRemoveSegment(reinterpret_cast<sci_local_segment_t>(local_output_ack_[i]), 0, &e);
    if (remote_output_ack_[i])
      SCIDisconnectSegment(reinterpret_cast<sci_remote_segment_t>(remote_output_ack_[i]), 0, &e);

    // Derived views only -- do not free(). Just clear the pointers.
    output_ack_stage_[i] = nullptr;
    input_ack_stage_[i]  = nullptr;
  }

  // The big shared segments (real input/output data segments, and the
  // registered DMA-source stage-buffer segments) are each ONE object --
  // torn down ONCE here. The derived per-slot view arrays
  // (local_input_data_map_[i], etc.) are plain pointer arithmetic into
  // these buffers and own nothing separately -- no per-slot cleanup
  // needed for them.
  if (local_input_data_base_maph_) {
    SCIUnmapSegment(*reinterpret_cast<sci_map_t*>(&local_input_data_base_maph_), 0, &e);
    local_input_data_base_maph_ = nullptr;
  }
  if (remote_input_data_base_maph_) {
    SCIUnmapSegment(*reinterpret_cast<sci_map_t*>(&remote_input_data_base_maph_), 0, &e);
    remote_input_data_base_maph_ = nullptr;
  }
  if (local_input_data_seg_) {
    SCIRemoveSegment(reinterpret_cast<sci_local_segment_t>(local_input_data_seg_), 0, &e);
    local_input_data_seg_ = nullptr;
  }
  if (remote_input_data_seg_) {
    SCIDisconnectSegment(reinterpret_cast<sci_remote_segment_t>(remote_input_data_seg_), 0, &e);
    remote_input_data_seg_ = nullptr;
  }

  if (local_output_data_base_maph_) {
    SCIUnmapSegment(*reinterpret_cast<sci_map_t*>(&local_output_data_base_maph_), 0, &e);
    local_output_data_base_maph_ = nullptr;
  }
  if (remote_output_data_base_maph_) {
    SCIUnmapSegment(*reinterpret_cast<sci_map_t*>(&remote_output_data_base_maph_), 0, &e);
    remote_output_data_base_maph_ = nullptr;
  }
  if (local_output_data_seg_) {
    SCIRemoveSegment(reinterpret_cast<sci_local_segment_t>(local_output_data_seg_), 0, &e);
    local_output_data_seg_ = nullptr;
  }
  if (remote_output_data_seg_) {
    SCIDisconnectSegment(reinterpret_cast<sci_remote_segment_t>(remote_output_data_seg_), 0, &e);
    remote_output_data_seg_ = nullptr;
  }

  // Registered DMA-source segments must be torn down before the raw
  // buffers they wrap are free()'d (back to posix_memalign'd
  // buffers -- see the note above RegisterUserMemSegment).
  if (output_stage_seg_base_) {
    SCIRemoveSegment(reinterpret_cast<sci_local_segment_t>(output_stage_seg_base_), 0, &e);
    output_stage_seg_base_ = nullptr;
  }
  if (input_stage_seg_base_) {
    SCIRemoveSegment(reinterpret_cast<sci_local_segment_t>(input_stage_seg_base_), 0, &e);
    input_stage_seg_base_ = nullptr;
  }
  if (output_stage_base_) { free(output_stage_base_); output_stage_base_ = nullptr; }
  if (input_stage_base_)  { free(input_stage_base_);  input_stage_base_  = nullptr; }

  auto cleanup_ctrl = [&](void*& lmh, void*& rmh, void*& ls, void*& rs) {
    if (lmh) { SCIUnmapSegment(*reinterpret_cast<sci_map_t*>(&lmh), 0, &e); lmh = nullptr; }
    if (rmh) { SCIUnmapSegment(*reinterpret_cast<sci_map_t*>(&rmh), 0, &e); rmh = nullptr; }
    if (ls)  { SCIRemoveSegment(reinterpret_cast<sci_local_segment_t>(ls), 0, &e); ls = nullptr; }
    if (rs)  { SCIDisconnectSegment(reinterpret_cast<sci_remote_segment_t>(rs), 0, &e); rs = nullptr; }
  };

  if (remote_primary_ctrl_sequence_) {
    SCIRemoveSequence(reinterpret_cast<sci_sequence_t>(remote_primary_ctrl_sequence_), 0, &e);
    remote_primary_ctrl_sequence_ = nullptr;
  }
  for (int i = 0; i < kNumSegments; i++) {
    if (remote_worker_ctrl_sequence_[i]) {
      SCIRemoveSequence(
          reinterpret_cast<sci_sequence_t>(remote_worker_ctrl_sequence_[i]), 0, &e);
      remote_worker_ctrl_sequence_[i] = nullptr;
    }
  }

  cleanup_ctrl(local_primary_ctrl_map_handle_, remote_primary_ctrl_map_handle_,
              local_primary_ctrl_, remote_primary_ctrl_);
  cleanup_ctrl(local_worker_ctrl_map_handle_, remote_worker_ctrl_map_handle_,
              local_worker_ctrl_, remote_worker_ctrl_);

  if (sci_) {
    SCIClose(reinterpret_cast<sci_desc_t>(sci_), 0, &e);
    sci_ = nullptr;
  }
  SCITerminate();
}

void NtbChannel::Close() {
  if (open_) { CleanupSISCI(); open_ = false; }
}

// ---- InputDmaSend (primary -> worker, DMA) ---------------------------
// input_stage_seg_base_ (the ONE registered SCI segment wrapping the
// whole input_stage_base_ buffer) and remote_input_data_seg_ (the ONE
// connected remote segment covering all slots) are shared across all
// kNumSegments slots -- so both the local and remote DMA offsets need
// `base` (this slot's byte offset within those big segments) added, on
// top of the existing per-call `offset`. the actual transfer now
// runs on whichever of the kNumDmaWorkers pool threads picks up this
// request -- SubmitDma() blocks until it's done, so this call is still
// fully synchronous from the caller's point of view.
//
// added SCI_FLAG_DMA_BARRIER per Dolphin
// support's guidance for the "Bad table magic number" corruption report
// -- a large transfer (header+payload, see InputSendChunk) can split
// across multiple HW DMA descriptors with no completion-ordering
// guarantee under PCIe Relaxed Ordering, which matches the observed
// corruption pattern (confined to the last 4 bytes of the SST, i.e. the
// tail descriptor). This flag forces strict completion ordering across
// those descriptors. InputDmaReadBack() below is a separate,
// pre-existing mechanism for a different problem (confirming a
// completed write has actually landed remotely) and is deliberately
// left in place for this test -- do not remove it in the same change as
// this flag; test barrier-alone first, remove read-back (if at all) as
// its own later, separately-verified step.
Status NtbChannel::InputDmaSend(int seg_idx, size_t offset, size_t nbytes) {
  const size_t base = static_cast<size_t>(seg_idx) * kDataSegSize;
  return SubmitDma(seg_idx, input_stage_seg_base_, remote_input_data_seg_,
                   base + offset, base + offset, nbytes,
                   SCI_FLAG_DMA_GLOBAL);  // [reverted] 2026-10-30: SCI_FLAG_DMA_BARRIER not defined in official DIS_RELEASE_5_25_3 headers (only existed in support's custom 2696f658f5 CI snapshot) -- removed so this builds against the reinstalled official release
}

// re-reads the bytes we just wrote back from the remote segment
// using SCI_FLAG_DMA_READ. Dolphin support confirmed SCIDMAQueueState()==DONE
// only means the local DMA engine finished issuing the write, not that it has
// landed in remote memory ("in practice it almost always has... if you
// absolutely want to make sure, you can perform a DMA read from the remote
// buffer -- due to PCIe ordering rules, the completion of this read
// guarantees that the writes have completed"). This read's completion
// guarantees the preceding write(s) to the same destination have actually
// landed remotely.
Status NtbChannel::InputDmaReadBack(int seg_idx, size_t offset, size_t nbytes) {
  const size_t base = static_cast<size_t>(seg_idx) * kDataSegSize;
  return SubmitDma(seg_idx, input_stage_seg_base_, remote_input_data_seg_,
                   base + offset, base + offset, nbytes,
                   SCI_FLAG_DMA_GLOBAL | SCI_FLAG_DMA_READ);
}

// ---- InputAckDmaSend (worker -> primary, consumed-ack push via DMA) ---
// input_ack_stage_[seg_idx] is a derived pointer into
// output_stage_seg_base_ (the worker's single registered stage-buffer
// segment, which holds this node's data-send region followed by its
// ack-push region). The local DMA source offset is therefore
// kTotalDataSize + seg_idx*kChunkHeaderSize within that one registered
// segment; the remote target is the per-slot InputAckSegId(seg_idx)
// connected segment (remote_input_ack_[seg_idx], offset 0 -- each ack
// destination segment is still its own dedicated small segment,
// unaffected by the stage-buffer merge). goes through the same
// SubmitDma()/pool path as InputDmaSend -- both share this node's 8
// worker threads.
Status NtbChannel::InputAckDmaSend(int seg_idx) {
  const size_t local_off = kTotalDataSize + static_cast<size_t>(seg_idx) * kChunkHeaderSize;
  return SubmitDma(seg_idx, output_stage_seg_base_, remote_input_ack_[seg_idx],
                   local_off, 0, kChunkHeaderSize, SCI_FLAG_DMA_GLOBAL);
}

// ---- InputSendChunk ---------------------------------------------------
// Sends header + payload as a SINGLE transfer (offset 0, size =
// header+data). These are sent as ONE transfer rather than two (data
// then header separately) because a two-transfer split previously let
// the receiver observe a valid header (ready=1) before the DMA-written
// payload had actually landed -- 100% reproducible from the very first
// job. Merging into one transfer removed that symptom entirely.
//
// The whole poll->reset->build-header->send sequence is wrapped in a
// per-segment mutex. The consumed-flag poll+reset just above is a plain
// load then plain store, not an atomic compare-and-swap -- if two
// threads ever called this function with the same seg_idx concurrently,
// both could pass the poll before either cleared it, then both write
// into the same input_stage_[seg_idx] at once. The mutex makes this
// function mutually exclusive per segment regardless of what the caller
// does.
//
// seg_idx selects an independent mutex and an independent stage-buffer
// view; kNumSegments callers with different seg_idx values can still be
// inside InputSendChunk (and its InputDmaSend) at the same time; the DMA
// pool only decides which pool thread executes each transfer.
Status NtbChannel::InputSendChunk(int seg_idx, uint64_t file_number,
                                  uint64_t file_offset, uint64_t chunk_bytes,
                                  bool is_last, uint64_t job_id) {
  std::lock_guard<std::mutex> lk(input_send_mu_[seg_idx]);

  auto* rhdr = reinterpret_cast<NtbChunkHeader*>(local_input_ack_map_[seg_idx]);
  Status ws = WaitAtomicFlag(&rhdr->consumed, 1u, kChunkPollTimeoutMs,
                             "InputSendChunk: consumed-flag poll");
  if (!ws.ok()) return ws;
  __atomic_store_n(&rhdr->consumed, 0u, __ATOMIC_RELEASE);

  size_t data_size  = (static_cast<size_t>(chunk_bytes) + 63) & ~size_t(63);
  size_t total_size = kChunkHeaderSize + data_size;
  fprintf(stderr,
          "[ntb][SEND] dir=IN seg=%d file=%llu off=%llu bytes=%llu last=%d "
          "path=DMA total=%zu ts=%s\n",
          seg_idx, (unsigned long long)file_number,
          (unsigned long long)file_offset, (unsigned long long)chunk_bytes,
          is_last ? 1 : 0, total_size, NtbTsNow().c_str());

  // Ordering: if the header (ready=1) travelled in the same DMA as the
  // payload, the receiver could see ready=1 before the payload tail landed
  // (observed as corrupted SST footers). So: payload -> read back the
  // payload tail (guarantees remote arrival) -> header with ready=1 ->
  // read back the header.
  if (data_size > 0) {
    Status s = InputDmaSend(seg_idx, kChunkHeaderSize, data_size);
    if (!s.ok()) return s;
    Status rs = InputDmaReadBack(seg_idx, kChunkHeaderSize + data_size - 64, 64);
    if (!rs.ok()) return rs;
  }

  auto* shdr        = reinterpret_cast<NtbChunkHeader*>(input_stage_[seg_idx]);
  shdr->chunk_bytes = static_cast<uint32_t>(chunk_bytes);
  shdr->is_last     = is_last ? 1u : 0u;
  shdr->file_number = file_number;
  shdr->file_offset = file_offset;
  shdr->job_id      = job_id;  // 0 = untagged
  shdr->consumed    = 0;
  shdr->ready       = 1;
  __atomic_thread_fence(__ATOMIC_RELEASE);

  Status hs = InputDmaSend(seg_idx, 0, kChunkHeaderSize);
  if (!hs.ok()) return hs;
  Status hrs = InputDmaReadBack(seg_idx, 0, kChunkHeaderSize);
  if (!hrs.ok()) return hrs;

  return Status::OK();
}

// ---- InputRecvChunk -----------------------------------------------------
Status NtbChannel::InputRecvChunk(int seg_idx, uint64_t* file_out,
                                  uint64_t* off_out, uint64_t* bytes_out,
                                  bool* last_out) {
  auto* hdr = reinterpret_cast<NtbChunkHeader*>(local_input_data_map_[seg_idx]);

  Status ws = WaitAtomicFlag(&hdr->ready, 1u, kChunkPollTimeoutMs,
                             "InputRecvChunk: ready-flag poll");
  if (!ws.ok()) return ws;

  *file_out  = hdr->file_number;
  *off_out   = hdr->file_offset;
  *bytes_out = hdr->chunk_bytes;
  *last_out  = (hdr->is_last != 0);
  fprintf(stderr,
          "[ntb][RECV] dir=IN seg=%d file=%llu off=%llu bytes=%llu last=%d ts=%s\n",
          seg_idx, (unsigned long long)*file_out, (unsigned long long)*off_out,
          (unsigned long long)*bytes_out, *last_out ? 1 : 0, NtbTsNow().c_str());
  return Status::OK();
}

// Exposes the header of the chunk currently in the segment, without assuming
// which file it belongs to (the worker routes chunks by file number).
Status NtbChannel::InputPeekChunk(int seg_idx, uint64_t* file_out,
                                  uint64_t* off_out, uint64_t* bytes_out,
                                  bool* last_out, uint64_t* job_out) {
  Status s = InputRecvChunk(seg_idx, file_out, off_out, bytes_out, last_out);
  if (s.ok() && job_out != nullptr) {
    auto* hdr = reinterpret_cast<NtbChunkHeader*>(local_input_data_map_[seg_idx]);
    *job_out = hdr->job_id;
  }
  return s;
}

// Copies the payload of the chunk seen by InputPeekChunk() to dest (skipped
// when dest is nullptr) and acks it. The sender cannot reuse the segment
// before the ack, so chunks without a receiver must still be acked.
void NtbChannel::InputConsumeChunk(int seg_idx, char* dest, uint64_t bytes) {
  if (dest != nullptr && bytes > 0) {
    const char* data_ptr = static_cast<const char*>(local_input_data_map_[seg_idx])
                          + kChunkHeaderSize;
    memcpy(dest, data_ptr, bytes);
  }
  InputAckChunk(seg_idx);
}

// ---- InputAckChunk ---------------------------------------------------
void NtbChannel::InputAckChunk(int seg_idx) {
  auto* lhdr = reinterpret_cast<NtbChunkHeader*>(local_input_data_map_[seg_idx]);
  fprintf(stderr,
          "[ntb][ACK] dir=IN seg=%d file=%llu off=%llu bytes=%llu ts=%s\n",
          seg_idx, (unsigned long long)lhdr->file_number,
          (unsigned long long)lhdr->file_offset,
          (unsigned long long)lhdr->chunk_bytes, NtbTsNow().c_str());
  __atomic_store_n(&lhdr->ready,    0u, __ATOMIC_RELEASE);
  __atomic_store_n(&lhdr->consumed, 1u, __ATOMIC_RELEASE);

  {
    auto* ahdr        = reinterpret_cast<NtbChunkHeader*>(input_ack_stage_[seg_idx]);
    ahdr->consumed    = 1;
    ahdr->file_number = lhdr->file_number;
    Status as = InputAckDmaSend(seg_idx);
    if (!as.ok()) {
      fprintf(stderr, "[ntb] InputAckChunk: ack push failed seg=%d: %s\n",
             seg_idx, as.ToString().c_str());
    }
  }

  __sync_synchronize();
}

// ---- OutputDmaSend (worker -> primary, DMA) --------------------------
// See InputDmaSend's comment above -- same change, mirrored: uses the
// single shared output_stage_seg_base_ / remote_output_data_seg_
// segments, `base + offset` on both sides, routed through SubmitDma().
//
// same SCI_FLAG_DMA_BARRIER addition as
// InputDmaSend above, for the same reason. See that function's comment
// for the full rationale; OutputDmaReadBack() below is kept unchanged.
Status NtbChannel::OutputDmaSend(int seg_idx, size_t offset, size_t nbytes) {
  const size_t base = static_cast<size_t>(seg_idx) * kDataSegSize;
  return SubmitDma(seg_idx, output_stage_seg_base_, remote_output_data_seg_,
                   base + offset, base + offset, nbytes,
                   SCI_FLAG_DMA_GLOBAL);  // [reverted] 2026-10-30: SCI_FLAG_DMA_BARRIER not defined in official DIS_RELEASE_5_25_3 headers (only existed in support's custom 2696f658f5 CI snapshot) -- removed so this builds against the reinstalled official release
}

// see InputDmaReadBack comment above.
Status NtbChannel::OutputDmaReadBack(int seg_idx, size_t offset, size_t nbytes) {
  const size_t base = static_cast<size_t>(seg_idx) * kDataSegSize;
  return SubmitDma(seg_idx, output_stage_seg_base_, remote_output_data_seg_,
                   base + offset, base + offset, nbytes,
                   SCI_FLAG_DMA_GLOBAL | SCI_FLAG_DMA_READ);
}

// ---- OutputAckDmaSend (primary -> worker, consumed-ack push via DMA) --
// Mirrors InputAckDmaSend: output_ack_stage_[seg_idx] is a derived
// pointer into input_stage_seg_base_ (the primary's single registered
// stage-buffer segment). Remote target is remote_output_ack_[seg_idx]
// (connected OutputAckSegId(seg_idx)), offset 0. shares this node's
// 8-worker pool with InputDmaSend.
Status NtbChannel::OutputAckDmaSend(int seg_idx) {
  const size_t local_off = kTotalDataSize + static_cast<size_t>(seg_idx) * kChunkHeaderSize;
  return SubmitDma(seg_idx, input_stage_seg_base_, remote_output_ack_[seg_idx],
                   local_off, 0, kChunkHeaderSize, SCI_FLAG_DMA_GLOBAL);
}

// ---- OutputSendChunk (mirrors InputSendChunk) --------------------------
Status NtbChannel::OutputSendChunk(int seg_idx, uint64_t file_number,
                                   uint64_t file_offset, uint64_t chunk_bytes,
                                   bool is_last, uint64_t job_id) {
  std::lock_guard<std::mutex> lk(output_send_mu_[seg_idx]);

  auto* rhdr = reinterpret_cast<NtbChunkHeader*>(local_output_ack_map_[seg_idx]);
  Status ws = WaitAtomicFlag(&rhdr->consumed, 1u, kChunkPollTimeoutMs,
                             "OutputSendChunk: consumed-flag poll");
  if (!ws.ok()) return ws;
  __atomic_store_n(&rhdr->consumed, 0u, __ATOMIC_RELEASE);

  size_t data_size  = (static_cast<size_t>(chunk_bytes) + 63) & ~size_t(63);
  size_t total_size = kChunkHeaderSize + data_size;
  fprintf(stderr,
          "[ntb][SEND] dir=OUT seg=%d file=%llu off=%llu bytes=%llu last=%d "
          "path=DMA total=%zu job=%llu ts=%s\n",
          seg_idx, (unsigned long long)file_number,
          (unsigned long long)file_offset, (unsigned long long)chunk_bytes,
          is_last ? 1 : 0, total_size, (unsigned long long)job_id,
          NtbTsNow().c_str());

  // Same ordering as InputSendChunk: payload, tail read-back, then the
  // ready=1 header.
  if (data_size > 0) {
    Status s = OutputDmaSend(seg_idx, kChunkHeaderSize, data_size);
    if (!s.ok()) return s;
    Status rs = OutputDmaReadBack(seg_idx, kChunkHeaderSize + data_size - 64, 64);
    if (!rs.ok()) return rs;
  }

  auto* shdr        = reinterpret_cast<NtbChunkHeader*>(output_stage_[seg_idx]);
  shdr->chunk_bytes = static_cast<uint32_t>(chunk_bytes);
  shdr->is_last     = is_last ? 1u : 0u;
  shdr->file_number = file_number;
  shdr->file_offset = file_offset;
  shdr->job_id      = job_id;
  shdr->consumed    = 0;
  shdr->ready       = 1;
  __atomic_thread_fence(__ATOMIC_RELEASE);

  Status hs = OutputDmaSend(seg_idx, 0, kChunkHeaderSize);
  if (!hs.ok()) return hs;
  Status hrs = OutputDmaReadBack(seg_idx, 0, kChunkHeaderSize);
  if (!hrs.ok()) return hrs;

  return Status::OK();
}

// ---- OutputAckChunk ---------------------------------------------------
void NtbChannel::OutputAckChunk(int seg_idx) {
  auto* hdr = reinterpret_cast<NtbChunkHeader*>(local_output_data_map_[seg_idx]);
  fprintf(stderr,
          "[ntb][ACK] dir=OUT seg=%d file=%llu off=%llu bytes=%llu ts=%s\n",
          seg_idx, (unsigned long long)hdr->file_number,
          (unsigned long long)hdr->file_offset,
          (unsigned long long)hdr->chunk_bytes, NtbTsNow().c_str());
  __atomic_store_n(&hdr->ready,    0u, __ATOMIC_RELEASE);
  __atomic_store_n(&hdr->consumed, 1u, __ATOMIC_RELEASE);

  {
    auto* ahdr        = reinterpret_cast<NtbChunkHeader*>(output_ack_stage_[seg_idx]);
    ahdr->consumed    = 1;
    ahdr->file_number = hdr->file_number;
    Status as = OutputAckDmaSend(seg_idx);
    if (!as.ok()) {
      fprintf(stderr, "[ntb] OutputAckChunk: ack push failed seg=%d: %s\n",
             seg_idx, as.ToString().c_str());
    }
  }

  __sync_synchronize();
}

// ---- OutputTryPeekChunk / OutputConsumeChunk ---------------------
bool NtbChannel::OutputTryPeekChunk(int seg_idx, uint64_t* file_out,
                                    uint64_t* off_out, uint64_t* bytes_out,
                                    bool* last_out, uint64_t* job_out) {
  auto* hdr = reinterpret_cast<NtbChunkHeader*>(local_output_data_map_[seg_idx]);
  if (__atomic_load_n(&hdr->ready, __ATOMIC_ACQUIRE) != 1u) return false;
  *file_out  = hdr->file_number;
  *off_out   = hdr->file_offset;
  *bytes_out = hdr->chunk_bytes;
  *last_out  = (hdr->is_last != 0);
  *job_out   = hdr->job_id;
  return true;
}

void NtbChannel::OutputConsumeChunk(int seg_idx, char* dest, uint64_t bytes) {
  if (dest != nullptr && bytes > 0) {
    const char* data_ptr = static_cast<const char*>(local_output_data_map_[seg_idx])
                          + kChunkHeaderSize;
    memcpy(dest, data_ptr, bytes);
  }
  OutputAckChunk(seg_idx);
}

// ---- ctrl messages ------------------------------------------------------
// ctrl segments are tiny and infrequent (one per job, not per chunk), and
// use PIO.
// remote_base is where this slot starts inside the remote map. The local
// stage is written from 0, the remote side from remote_base.
static Status CtrlPioSend(sci_sequence_t seq, void* stage, sci_map_t map,
                          size_t offset, size_t nbytes,
                          size_t remote_base = 0) {
  static constexpr int kMaxRetries = 20;
  sci_error_t e = SCI_ERR_OK;

  for (int attempt = 0; attempt < kMaxRetries; attempt++) {
    SCIStartSequence(seq, 0, &e);
    if (e != SCI_ERR_OK) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      continue;
    }

    SCIMemCpy(seq, static_cast<char*>(stage) + offset, map, remote_base + offset,
              nbytes, 0, &e);
    if (e != SCI_ERR_OK) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      continue;
    }

    sci_sequence_status_t st = SCICheckSequence(seq, 0, &e);
    if (st == SCI_SEQ_OK) return Status::OK();

    fprintf(stderr,
           "[ntb] CtrlPioSend: SCICheckSequence status=%d err=%s(%d) "
           "attempt=%d/%d offset=%zu nbytes=%zu\n",
           (int)st, SCIGetErrorString(e), (int)e, attempt + 1, kMaxRetries,
           offset, nbytes);
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return Status::IOError("CtrlPioSend SCICheckSequence failed after retries");
}

static Status SendCtrlMsgImpl(void* remote_map, void* remote_map_handle,
                              void* sequence_handle,
                              const std::string& payload,
                              size_t remote_base = 0) {
  if (payload.size() > kCtrlPayloadMax)
    return Status::InvalidArgument("ctrl payload too large");

  auto* hdr = reinterpret_cast<NtbCtrlHeader*>(remote_map);
  {
    auto t_start = std::chrono::steady_clock::now();
    while (__atomic_load_n(&hdr->msg_ready, __ATOMIC_ACQUIRE) != 0) {
      if (std::chrono::steady_clock::now() - t_start > std::chrono::seconds(10)) {
        fprintf(stderr, "[ntb] SendCtrlMsg: msg_ready poll TIMEOUT "
                       "(previous message not yet consumed)\n");
        return Status::TimedOut("SendCtrlMsg: msg_ready poll timeout");
      }
      NtbBackoff();
    }
  }

  sci_sequence_t seq = reinterpret_cast<sci_sequence_t>(sequence_handle);
  sci_map_t      map = *reinterpret_cast<sci_map_t*>(&remote_map_handle);

  size_t real_size    = sizeof(NtbCtrlHeader) + payload.size();
  size_t aligned_size = (real_size + 3) & ~size_t(3);
  std::vector<char> stage(aligned_size, 0);

  auto* shdr      = reinterpret_cast<NtbCtrlHeader*>(stage.data());
  shdr->msg_len   = static_cast<uint32_t>(payload.size());
  shdr->msg_ready = 0;
  shdr->ack       = 0;
  memcpy(stage.data() + sizeof(NtbCtrlHeader), payload.data(), payload.size());

  Status s = CtrlPioSend(seq, stage.data(), map, 0, aligned_size, remote_base);
  if (!s.ok()) return s;
  SCIStoreBarrier(seq, 0);

  shdr->msg_ready = 1;
  s = CtrlPioSend(seq, stage.data(), map, offsetof(NtbCtrlHeader, msg_ready),
                 sizeof(shdr->msg_ready), remote_base);
  if (!s.ok()) return s;
  SCIStoreBarrier(seq, 0);

  return Status::OK();
}

static Status RecvCtrlMsgImpl(void* local_map, std::string* out) {
  auto* lhdr  = reinterpret_cast<NtbCtrlHeader*>(local_map);
  auto* ldata = static_cast<char*>(local_map) + sizeof(NtbCtrlHeader);

  {
    auto t_start = std::chrono::steady_clock::now();
    while (__atomic_load_n(&lhdr->msg_ready, __ATOMIC_ACQUIRE) == 0) {
      if (std::chrono::steady_clock::now() - t_start > std::chrono::seconds(10))
        return Status::TimedOut("RecvCtrlMsg: msg_ready poll timeout");
      NtbBackoff();
    }
  }
  __sync_synchronize();

  uint32_t len = __atomic_load_n(&lhdr->msg_len, __ATOMIC_ACQUIRE);
  if (len > kCtrlPayloadMax) return Status::Corruption("ctrl recv: length overflow");
  out->assign(ldata, len);

  __atomic_store_n(&lhdr->msg_ready, 0u, __ATOMIC_RELEASE);
  __sync_synchronize();
  return Status::OK();
}

Status NtbChannel::SendSpecMsg(const std::string& payload) {
  std::lock_guard<std::mutex> lk(spec_send_mu_);
  return SendCtrlMsgImpl(remote_primary_ctrl_map_, remote_primary_ctrl_map_handle_,
                        remote_primary_ctrl_sequence_, payload);
}

Status NtbChannel::RecvSpecMsg(std::string* out) {
  return RecvCtrlMsgImpl(local_primary_ctrl_map_, out);
}

Status NtbChannel::SendResultMsg(int seg_idx, const std::string& payload) {
  std::lock_guard<std::mutex> lk(result_send_mu_[seg_idx]);
  // The map handle covers all kNumSegments slots, so pass the offset of
  // slot seg_idx.
  return SendCtrlMsgImpl(remote_worker_ctrl_map_[seg_idx],
                        remote_worker_ctrl_map_handle_,
                        remote_worker_ctrl_sequence_[seg_idx], payload,
                        static_cast<size_t>(seg_idx) * kCtrlSegSize);
}

Status NtbChannel::TryRecvResultMsg(int seg_idx, std::string* out) {
  auto* lhdr = reinterpret_cast<NtbCtrlHeader*>(local_worker_ctrl_map_[seg_idx]);
  if (__atomic_load_n(&lhdr->msg_ready, __ATOMIC_ACQUIRE) == 0)
    return Status::NotFound("TryRecvResultMsg: no message");
  __sync_synchronize();
  auto* ldata = static_cast<char*>(local_worker_ctrl_map_[seg_idx]) +
                sizeof(NtbCtrlHeader);
  uint32_t len = __atomic_load_n(&lhdr->msg_len, __ATOMIC_ACQUIRE);
  if (len > kCtrlPayloadMax) return Status::Corruption("ctrl recv: length overflow");
  out->assign(ldata, len);
  __atomic_store_n(&lhdr->msg_ready, 0u, __ATOMIC_RELEASE);
  __sync_synchronize();
  return Status::OK();
}

Status NtbChannel::SendCtrlMsg(const std::string& payload) {
  return is_primary_ ? SendSpecMsg(payload) : SendResultMsg(0, payload);
}

// ---- StreamSendSST (primary -> worker) --------------------------------
// Input SSTs are read with O_DIRECT in 2 MiB pieces (the same unit as
// compaction_readahead_size for local compaction), falling back to buffered
// reads if direct I/O is not possible.
static constexpr uint64_t kReadPiece = 2ull << 20;

// Reads [off, off+len) of `path` into dst. With direct=true, `off` must be
// 4 KiB aligned and an aligned bounce buffer is used.
static bool ReadFileRange(const std::string& path, bool direct, uint64_t off,
                          uint64_t len, char* dst) {
  int fd = ::open(path.c_str(), O_RDONLY | (direct ? O_DIRECT : 0));
  if (fd < 0) return false;
  char* bounce = nullptr;
  if (direct && posix_memalign(reinterpret_cast<void**>(&bounce), 4096, kReadPiece) != 0) {
    ::close(fd);
    return false;
  }
  bool ok = true;
  uint64_t done = 0;
  while (done < len) {
    uint64_t want = std::min<uint64_t>(len - done, kReadPiece);
    if (direct) {
      size_t req = static_cast<size_t>((want + 4095) & ~uint64_t(4095));
      ssize_t n = ::pread(fd, bounce, req, static_cast<off_t>(off + done));
      if (n < static_cast<ssize_t>(want)) {  // short read: caller retries buffered
        ok = false;
        break;
      }
      memcpy(dst + done, bounce, want);
      done += want;
    } else {
      ssize_t n = ::pread(fd, dst + done, static_cast<size_t>(want),
                          static_cast<off_t>(off + done));
      if (n <= 0) {
        ok = false;
        break;
      }
      done += static_cast<uint64_t>(n);
    }
  }
  free(bounce);
  ::close(fd);
  return ok;
}

static bool ReadChunk(const std::string& path, uint64_t off, uint64_t len,
                      char* dst, bool* direct_out) {
  const bool direct = (off % 4096 == 0);
  bool ok = ReadFileRange(path, direct, off, len, dst);
  if (!ok && direct) ok = ReadFileRange(path, false, off, len, dst);
  *direct_out = direct;
  return ok;
}

Status NtbChannel::StreamSendSST(const std::string& local_path,
                                 uint64_t file_number, uint64_t file_size,
                                 int fixed_seg, uint64_t job_id) {
  uint64_t file_offset = 0;
  double read_ms = 0, dma_ms = 0;
  bool direct = false;
  while (file_offset < file_size) {
    char*    stage_data  = input_stage_[fixed_seg] + kChunkHeaderSize;
    uint64_t chunk_bytes = std::min(file_size - file_offset, kChunkDataSize);

    auto t_r0 = std::chrono::steady_clock::now();
    if (!ReadChunk(local_path, file_offset, chunk_bytes, stage_data, &direct))
      return Status::IOError("StreamSendSST read failed: " + local_path);
    auto t_r1 = std::chrono::steady_clock::now();

    bool is_last = (file_offset + chunk_bytes >= file_size);

    Status s = InputSendChunk(fixed_seg, file_number, file_offset, chunk_bytes, is_last,
                              job_id);
    auto t_d1 = std::chrono::steady_clock::now();
    read_ms += std::chrono::duration<double, std::milli>(t_r1 - t_r0).count();
    dma_ms  += std::chrono::duration<double, std::milli>(t_d1 - t_r1).count();
    if (!s.ok()) return s;

    file_offset += chunk_bytes;
  }

  fprintf(stderr, "[timing] rc file_read job=%llu file=%llu seg=%d bytes=%llu read_ms=%.1f dma_ms=%.1f threads=%d direct=%d\n",
          (unsigned long long)job_id, (unsigned long long)file_number, fixed_seg,
          (unsigned long long)file_size, read_ms, dma_ms, 1, direct ? 1 : 0);
  return Status::OK();
}

// ---- StreamSendOutputSST (worker -> primary) --------------------------
Status NtbChannel::StreamSendOutputSST(const std::vector<char>& buf,
                                       uint64_t file_number, int fixed_seg,
                                       uint64_t job_id) {
  uint64_t file_size   = static_cast<uint64_t>(buf.size());
  uint64_t file_offset = 0;

  while (file_offset < file_size) {
    char*    data_ptr    = output_stage_[fixed_seg] + kChunkHeaderSize;
    uint64_t chunk_bytes = std::min(file_size - file_offset, kChunkDataSize);
    memcpy(data_ptr, buf.data() + file_offset, chunk_bytes);

    bool is_last = (file_offset + chunk_bytes >= file_size);

    Status s = OutputSendChunk(fixed_seg, file_number, file_offset, chunk_bytes, is_last,
                               job_id);
    if (!s.ok()) return s;

    file_offset += chunk_bytes;
  }

  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
