// Implementation of the NTB/SISCI transport layer (see ntb_channel.h).
// Shared between the primary process and the worker process.

#include "ntb_channel.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <chrono>
#include <atomic>

#include <sisci_api.h>
#include <sisci_error.h>

namespace ROCKSDB_NAMESPACE {

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

// v10a: bounded wait for an atomic header flag to reach `want`. Used by
// the four chunk-header poll loops below (InputSendChunk/OutputSendChunk's
// consumed-poll, InputRecvChunk/OutputRecvChunk's ready-poll), which
// previously had NO timeout at all -- unlike the ctrl message paths
// (SendCtrlMsgImpl/RecvCtrlMsgImpl), which already had a 10s cap. If the
// peer never shows up (crashed, or the DMA that would have delivered
// this flag update itself hung), the waiting thread used to spin via
// std::this_thread::yield() forever -- cooperatively yielding the CPU
// each iteration (so not the same mechanism as the SCI_FLAG_DMA_WAIT
// soft-lockup reported to Dolphin, which does not appear to yield at
// all), but still an unrecoverable hang from the application's point of
// view: the thread never returns and the job it was servicing never
// completes.
static Status WaitAtomicFlag(volatile uint32_t* flag, uint32_t want,
                             int timeout_ms, const char* what) {
  auto t_start = std::chrono::steady_clock::now();
  while (__atomic_load_n(flag, __ATOMIC_ACQUIRE) != want) {
    if (std::chrono::steady_clock::now() - t_start >
        std::chrono::milliseconds(timeout_ms)) {
      return Status::TimedOut(std::string(what) + ": poll timeout after " +
                              std::to_string(timeout_ms) + "ms");
    }
    std::this_thread::yield();
  }
  return Status::OK();
}

static Status CreateAndAdvertise(sci_desc_t sci, uint32_t seg_id,
                                  size_t size, void** seg_out,
                                  void** map_handle_out, void** map_out) {
  sci_error_t e;
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

// v12 (2026-09-17): v11 tried switching the stage buffers to a
// SISCI-allocated ("regular") segment instead of LOCK_USER_MEM, to give
// Dolphin support's requested physical contiguity. That failed
// immediately on dbs-srv-04 with "SCICreateSegment (sisci-owned): Out of
// local resources" -- a SISCI-owned segment of this size
// (kTotalDataSize + kAckRegionSize, the same size as the InputData/
// OutputData segment) draws from the adapter's own local resource pool
// (the BAR2/prefetch budget noted at the top of this file), which is
// already sized for exactly ONE segment that big per direction. Adding a
// second same-sized SISCI-owned segment roughly doubles that demand and
// blew the budget. Reverted to LOCK_USER_MEM below (wraps ordinary
// process memory -- does not draw from that pool at all), and the
// physical-contiguity request is instead addressed with a
// madvise(MADV_HUGEPAGE) hint on the posix_memalign'd buffer (see the
// call sites in OpenAsPrimary/OpenAsWorker): transparent huge pages,
// best-effort, silently ignored if the kernel can't oblige, so it adds
// no new resource-exhaustion risk the way the v11 attempt did.

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
  sci_error_t e;
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

// v12: best-effort THP hint for a stage buffer -- logs a warning and
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
  sci_error_t e;
  SCIInitialize(0, &e);
  if (e != SCI_ERR_OK) return SciErr("SCIInitialize", e);
  SCIOpen(reinterpret_cast<sci_desc_t*>(&sci_), 0, &e);
  if (e != SCI_ERR_OK) return SciErr("SCIOpen", e);
  return Status::OK();
}

// ---- v9: DMA submission pool -------------------------------------------
// Fixed kNumDmaWorkers (=8, matches the adapter's physical DMA engine
// count) persistent queues + worker threads, replacing the old
// per-slot-owned queues. Every *DmaSend function on this node funnels
// through SubmitDma() -> the shared work queue -> whichever of the 8
// threads is free next. This caps real concurrent hardware usage at 8
// no matter how many of the kNumSegments slots are active at once,
// mirroring ntb_parallel_fill_test.cc's fixed 8-thread design.
Status NtbChannel::StartDmaPool(void* sci_void) {
  sci_desc_t sci = reinterpret_cast<sci_desc_t>(sci_void);
  sci_error_t e;
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
  sci_error_t e;
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

// v10a: async submit (no SCI_FLAG_DMA_WAIT) + our own SCIDMAQueueState()
// poll loop with an explicit timeout. See the header comment above
// kDmaTimeoutMs for why: SCI_FLAG_DMA_WAIT gave the driver no escape
// hatch when a channel hangs, and the calling thread pinned a CPU core
// indefinitely (observed as a kernel soft-lockup, un-killable by
// SIGKILL). Polling with sleep_for() in between checks does not
// reproduce that -- the thread is genuinely idle/schedulable between
// polls, not spinning.
//
// We deliberately only branch on SCI_DMAQUEUE_DONE vs "timeout reached".
// We have not verified the other sci_dma_queue_state_t enum values
// against Dolphin's headers closely enough to safely treat any of them
// as a definitive error signal, so we don't try to distinguish "still
// legitimately running" from anything else short of DONE -- that
// distinction only matters for how fast we can report a hang, not for
// correctness.
// v13 diag: how many of the kNumDmaWorkers pool threads are currently
// inside DmaTransferWithTimeout (i.e. have an SCIStartDmaTransfer
// outstanding), and a monotonic microsecond clock -- both exist only to
// answer two questions from the outside: (1) do all 8 workers actually
// run concurrently in practice, or does occupancy stall well below 8;
// (2) how much of each worker's busy time goes to small ACK pushes
// (nbytes == kChunkHeaderSize) vs real data chunks. Logged, never acted
// on -- pure instrumentation, no behavior change.
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
  sci_error_t e;

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

    // v13 diag: see the comment above DmaTransferWithTimeout.
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
  item.flags         = flags;  // v-readback
  item.enqueue_ts_us  = NowUs();  // v13 diag, see DmaPoolWorkerLoop
  std::future<Status> fut = item.result.get_future();

  // v-pin: route this item to the worker/engine dedicated to this
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
  sci_error_t e;
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
  // registered SCI segment (v12: back to LOCK_USER_MEM, see the note
  // above RegisterUserMemSegment for why the v11 SISCI-owned attempt
  // failed; MADV_HUGEPAGE below is the physical-contiguity mitigation).
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
    sci_error_t se;
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
    sci_error_t se;
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

  // v9: no more per-slot queue creation here -- one shared 8-worker pool
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
      std::this_thread::yield();
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
  // registered SCI segment (v12: back to LOCK_USER_MEM -- see the note
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
    sci_error_t se;
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
    sci_error_t se;
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

  // v9: shared 8-worker DMA pool for this node, covering both
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

  {  // v-noreset: 연결 직후 입력 헤더 재초기화를 기본으로 하지 않음 (job 1 입력 소실 방지)
    int rc_ready = 0;
    for (int i = 0; i < kNumSegments; i++) {
      auto* lhdr = reinterpret_cast<NtbChunkHeader*>(local_input_data_map_[i]);
      if (__atomic_load_n(&lhdr->ready, __ATOMIC_ACQUIRE) != 0) rc_ready++;
    }
    const char* rc_e = getenv("RC_WORKER_POSTCONNECT_RESET");
    const bool rc_reset = (rc_e != nullptr && atoi(rc_e) != 0);
    fprintf(stderr, "[ntb] v-noreset: input segs already ready at connect end=%d reset=%d\n",
            rc_ready, rc_reset ? 1 : 0);
    if (rc_reset) {
      for (int i = 0; i < kNumSegments; i++) {
        auto* lhdr = reinterpret_cast<NtbChunkHeader*>(local_input_data_map_[i]);
        __atomic_store_n(&lhdr->consumed, 1u, __ATOMIC_RELEASE);
        __atomic_store_n(&lhdr->ready, 0u, __ATOMIC_RELEASE);
        __sync_synchronize();
      }
    }
  }

  for (int i = 0; i < kNumSegments; i++) {
    auto* rhdr = reinterpret_cast<NtbChunkHeader*>(remote_output_data_map_[i]);
    while (__atomic_load_n(&rhdr->consumed, __ATOMIC_ACQUIRE) != 1)
      std::this_thread::yield();
  }

  open_ = true;
  return Status::OK();
}

// ---- CleanupSISCI -------------------------------------------------
void NtbChannel::CleanupSISCI() {
  sci_error_t e;
  sci_desc_t sci = reinterpret_cast<sci_desc_t>(sci_);
  (void)sci;

  // v9: stop the DMA pool FIRST -- joins all 8 worker threads (so no
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
  // buffers they wrap are free()'d (v12: back to posix_memalign'd
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
// top of the existing per-call `offset`. v9: the actual transfer now
// runs on whichever of the kNumDmaWorkers pool threads picks up this
// request -- SubmitDma() blocks until it's done, so this call is still
// fully synchronous from the caller's point of view.
//
// v-barrier (2026-10-28): added SCI_FLAG_DMA_BARRIER per Dolphin
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

// v-readback: re-reads the bytes we just wrote back from the remote segment
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
// unaffected by the stage-buffer merge). v9: goes through the same
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
// inside InputSendChunk (and its InputDmaSend) at the same time -- v9
// only changes which of the 8 pool threads actually executes each
// individual transfer, not how many callers can be queued up waiting.
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

  // v-hdrlast (2026-11-01): header(ready=1)를 payload와 한 전송으로 보내면
  // header가 전송 맨 앞에 있어서 수신자가 ready=1을 먼저 보고 payload
  // 꼬리가 도착하기 전에 읽을 수 있다 (ntb_cpu.log: 손상이 전부 SST 끝의
  // index/footer). 순서를 payload -> payload 꼬리 read-back(원격 도달
  // 보장) -> header(ready=1) -> header read-back 으로 바꾼다.
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
  shdr->job_id      = job_id;  // v-nowait: 워커가 등록과 맞춰 본다 (0 = 미지정)
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

// v-byfile: InputRecvChunk를 그대로 외부(WorkerServer)에 노출만 한다 --
// file_number를 미리 가정하지 않고 "지금 도착한 헤더가 뭔지"만 본다.
Status NtbChannel::InputPeekChunk(int seg_idx, uint64_t* file_out,
                                  uint64_t* off_out, uint64_t* bytes_out,
                                  bool* last_out, uint64_t* job_out) {
  Status s = InputRecvChunk(seg_idx, file_out, off_out, bytes_out, last_out);
  if (s.ok() && job_out != nullptr) {  // v-nowait
    auto* hdr = reinterpret_cast<NtbChunkHeader*>(local_input_data_map_[seg_idx]);
    *job_out = hdr->job_id;
  }
  return s;
}

// v-byfile: InputPeekChunk()로 이미 읽은 헤더에 대응하는 payload를 dest로
// 복사하고(dest==nullptr이면 생략) ack한다. ack 전까지 sender는 다음
// 청크를 못 보내므로, dest를 못 찾은 경우에도(기다리는 수신자가 없는
// chunk) 반드시 ack는 해줘야 segment가 안 막힌다.
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
// v-barrier (2026-10-28): same SCI_FLAG_DMA_BARRIER addition as
// InputDmaSend above, for the same reason. See that function's comment
// for the full rationale; OutputDmaReadBack() below is kept unchanged.
Status NtbChannel::OutputDmaSend(int seg_idx, size_t offset, size_t nbytes) {
  const size_t base = static_cast<size_t>(seg_idx) * kDataSegSize;
  return SubmitDma(seg_idx, output_stage_seg_base_, remote_output_data_seg_,
                   base + offset, base + offset, nbytes,
                   SCI_FLAG_DMA_GLOBAL);  // [reverted] 2026-10-30: SCI_FLAG_DMA_BARRIER not defined in official DIS_RELEASE_5_25_3 headers (only existed in support's custom 2696f658f5 CI snapshot) -- removed so this builds against the reinstalled official release
}

// v-readback: see InputDmaReadBack comment above.
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
// (connected OutputAckSegId(seg_idx)), offset 0. v9: shares this node's
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

  // v-hdrlast: InputSendChunk와 동일 -- payload 먼저, 꼬리 read-back,
  // 그 다음에 ready=1 header.
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
  shdr->job_id      = job_id;  // v-stale
  shdr->consumed    = 0;
  shdr->ready       = 1;
  __atomic_thread_fence(__ATOMIC_RELEASE);

  Status hs = OutputDmaSend(seg_idx, 0, kChunkHeaderSize);
  if (!hs.ok()) return hs;
  Status hrs = OutputDmaReadBack(seg_idx, 0, kChunkHeaderSize);
  if (!hrs.ok()) return hrs;

  return Status::OK();
}

// ---- OutputRecvChunk ---------------------------------------------------
Status NtbChannel::OutputRecvChunk(int seg_idx, uint64_t* file_out,
                                   uint64_t* off_out, uint64_t* bytes_out,
                                   bool* last_out) {
  auto* hdr = reinterpret_cast<NtbChunkHeader*>(local_output_data_map_[seg_idx]);
  Status ws = WaitAtomicFlag(&hdr->ready, 1u, kOutputResultTimeoutMs,
                             "OutputRecvChunk: ready-flag poll (worker "
                             "compaction + send)");
  if (!ws.ok()) return ws;

  *file_out  = hdr->file_number;
  *off_out   = hdr->file_offset;
  *bytes_out = hdr->chunk_bytes;
  *last_out  = (hdr->is_last != 0);
  fprintf(stderr,
          "[ntb][RECV] dir=OUT seg=%d file=%llu off=%llu bytes=%llu last=%d ts=%s\n",
          seg_idx, (unsigned long long)*file_out, (unsigned long long)*off_out,
          (unsigned long long)*bytes_out, *last_out ? 1 : 0, NtbTsNow().c_str());
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

// ---- v-outq: OutputTryPeekChunk / OutputConsumeChunk ---------------------
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
// ctrl segments are tiny, infrequent (one per job, not per chunk), and
// stay PIO -- unaffected by v9.
// v-ctrlslot: remote_base = 맵 안에서 이 슬롯이 시작하는 위치. stage(로컬)는
// 0부터, 원격은 remote_base부터 쓴다.
static Status CtrlPioSend(sci_sequence_t seq, void* stage, sci_map_t map,
                          size_t offset, size_t nbytes,
                          size_t remote_base = 0) {
  static constexpr int kMaxRetries = 20;
  sci_error_t e;

  for (int attempt = 0; attempt < kMaxRetries; attempt++) {
    SCIStartSequence(seq, 0, &e);
    if (e != SCI_ERR_OK) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      continue;
    }

    SCIMemCpy(seq, static_cast<char*>(stage) + offset, map, remote_base + offset,
              nbytes, 0, &e);  // v-ctrlslot
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
                              size_t remote_base = 0) {  // v-ctrlslot
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
      std::this_thread::yield();
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
      std::this_thread::yield();
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
  // v-ctrlslot: 맵 핸들은 8개 슬롯 전체(kNumSegments * kCtrlSegSize)라서
  // 슬롯 seg_idx의 시작 오프셋을 넘겨야 그 슬롯에 써진다.
  return SendCtrlMsgImpl(remote_worker_ctrl_map_[seg_idx],
                        remote_worker_ctrl_map_handle_,
                        remote_worker_ctrl_sequence_[seg_idx], payload,
                        static_cast<size_t>(seg_idx) * kCtrlSegSize);
}

Status NtbChannel::RecvResultMsg(std::string* out) {
  // v34: 더이상 쓰지 않음 -- CtrlRecvLoop는 TryRecvResultMsg로 세그먼트별
  // 라운드로빈 폴링한다. 레거시 호출부가 남아있어도 컴파일되게 0번만 봄.
  return RecvCtrlMsgImpl(local_worker_ctrl_map_[0], out);
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

Status NtbChannel::RecvCtrlMsg(std::string* out) {
  return is_primary_ ? RecvResultMsg(out) : RecvSpecMsg(out);
}

// ---- StreamSendSST (primary -> worker) --------------------------------
Status NtbChannel::StreamSendSST(const std::string& local_path,
                                 uint64_t file_number, uint64_t file_size,
                                 int fixed_seg, uint64_t job_id) {
  int fd = ::open(local_path.c_str(), O_RDONLY);
  if (fd < 0) return Status::IOError("StreamSendSST open failed: " + local_path);

  uint64_t file_offset = 0;
  while (file_offset < file_size) {
    char*    stage_data  = input_stage_[fixed_seg] + kChunkHeaderSize;
    uint64_t chunk_bytes = std::min(file_size - file_offset, kChunkDataSize);

    uint64_t done = 0;
    while (done < chunk_bytes) {
      ssize_t n = ::read(fd, stage_data + done,
                        static_cast<size_t>(chunk_bytes - done));
      if (n <= 0) { ::close(fd); return Status::IOError("StreamSendSST read failed"); }
      done += static_cast<uint64_t>(n);
    }

    bool is_last = (file_offset + chunk_bytes >= file_size);

    Status s = InputSendChunk(fixed_seg, file_number, file_offset, chunk_bytes, is_last,
                              job_id);  // v-nowait
    if (!s.ok()) { ::close(fd); return s; }

    file_offset += chunk_bytes;
  }

  ::close(fd);
  return Status::OK();
}

// ---- StreamRecvSST (worker <- primary) --------------------------------
Status NtbChannel::StreamRecvSST(uint64_t file_number, uint64_t file_size,
                                 std::vector<char>* buf_out, int fixed_seg) {
  buf_out->resize(file_size + kChunkDataSize);

  while (true) {
    uint64_t recv_file;
    uint64_t file_offset;
    uint64_t chunk_bytes;
    bool     is_last;
    Status s = InputRecvChunk(fixed_seg, &recv_file, &file_offset, &chunk_bytes, &is_last);
    if (!s.ok()) return s;

    if (recv_file != file_number) {
      fprintf(stderr,
             "[FILE_MISMATCH] expect=%llu hdr=%llu seg=%d off=%llu "
             "bytes=%llu last=%d\n",
             (unsigned long long)file_number, (unsigned long long)recv_file,
             fixed_seg, (unsigned long long)file_offset,
             (unsigned long long)chunk_bytes, is_last);
      InputAckChunk(fixed_seg);
      return Status::Corruption("StreamRecvSST file number mismatch");
    }

    const char* data_ptr = static_cast<const char*>(local_input_data_map_[fixed_seg])
                          + kChunkHeaderSize;
    if (file_offset + chunk_bytes > buf_out->size())
      buf_out->resize(file_offset + chunk_bytes + kChunkDataSize);
    memcpy(buf_out->data() + file_offset, data_ptr, chunk_bytes);

    InputAckChunk(fixed_seg);
    if (is_last) break;
  }

  buf_out->resize(file_size);
  return Status::OK();
}

// ---- StreamSendOutputSST (worker -> primary) --------------------------
Status NtbChannel::StreamSendOutputSST(const std::vector<char>& buf,
                                       uint64_t file_number, int fixed_seg,
                                       uint64_t job_id) {  // v-stale
  uint64_t file_size   = static_cast<uint64_t>(buf.size());
  uint64_t file_offset = 0;

  while (file_offset < file_size) {
    char*    data_ptr    = output_stage_[fixed_seg] + kChunkHeaderSize;
    uint64_t chunk_bytes = std::min(file_size - file_offset, kChunkDataSize);
    memcpy(data_ptr, buf.data() + file_offset, chunk_bytes);

    bool is_last = (file_offset + chunk_bytes >= file_size);

    Status s = OutputSendChunk(fixed_seg, file_number, file_offset, chunk_bytes, is_last,
                               job_id);  // v-stale
    if (!s.ok()) return s;

    file_offset += chunk_bytes;
  }

  return Status::OK();
}

// ---- StreamRecvOutputSST (primary <- worker) ---------------------------
// v-stale (2026-11-01): 워커는 잡마다 첫 출력 파일을 10000번으로 매기므로
// file_number만으로는 "이전 잡이 세그먼트에 남겨둔 출력"과 "이번 잡의 출력"을
// 구분할 수 없다. 프라이머리가 포기한 잡(등록 ack/chunk-ack TIMEOUT,
// unknown job_id)의 출력이 세그먼트에 ready=1로 남아 있으면, 같은
// 세그먼트의 다음 잡이 그걸 자기 출력으로 받아버리고, 자기 출력은 또 그
// 다음 잡에게 넘어가는 연쇄가 생긴다 (ntb_cpu.log: actual_crc ==
// 직전 잡 expected_crc). header의 job_id가 다르면 그 청크는 ack해서
// 비우고(워커가 다음 청크를 보낼 수 있게) 계속 기다린다.
static constexpr int kMaxStaleDrain = 64;

Status NtbChannel::StreamRecvOutputSST(uint64_t file_number, uint64_t file_size,
                                       std::vector<char>* buf_out, int fixed_seg,
                                       uint64_t job_id) {
  buf_out->resize(file_size + kChunkDataSize);
  uint64_t actual_size = 0;
  int stale_drained = 0;

  while (true) {
    uint64_t recv_file;
    uint64_t file_offset, chunk_bytes;
    bool     is_last;
    Status s = OutputRecvChunk(fixed_seg, &recv_file, &file_offset, &chunk_bytes, &is_last);
    if (!s.ok()) return s;

    const uint64_t hdr_job =
        reinterpret_cast<NtbChunkHeader*>(local_output_data_map_[fixed_seg])->job_id;
    if (job_id != 0 && hdr_job != 0 && hdr_job != job_id) {
      fprintf(stderr,
             "[OUTPUT_STALE] seg=%d expect_job=%llu hdr_job=%llu hdr_file=%llu "
             "off=%llu bytes=%llu -- drained\n",
             fixed_seg, (unsigned long long)job_id, (unsigned long long)hdr_job,
             (unsigned long long)recv_file, (unsigned long long)file_offset,
             (unsigned long long)chunk_bytes);
      OutputAckChunk(fixed_seg);
      if (++stale_drained > kMaxStaleDrain)
        return Status::Corruption("StreamRecvOutputSST: too many stale chunks");
      continue;
    }

    if (recv_file != file_number) {
      fprintf(stderr,
             "[OUTPUT_MISMATCH] expect=%llu hdr=%llu seg=%d off=%llu "
             "bytes=%llu last=%d\n",
             (unsigned long long)file_number, (unsigned long long)recv_file,
             fixed_seg, (unsigned long long)file_offset,
             (unsigned long long)chunk_bytes, is_last);
      OutputAckChunk(fixed_seg);
      return Status::Corruption("StreamRecvOutputSST file number mismatch");
    }

    const char* data_ptr = static_cast<const char*>(local_output_data_map_[fixed_seg])
                          + kChunkHeaderSize;
    if (file_offset + chunk_bytes > buf_out->size())
      buf_out->resize(file_offset + chunk_bytes + kChunkDataSize);
    memcpy(buf_out->data() + file_offset, data_ptr, chunk_bytes);
    actual_size = file_offset + chunk_bytes;

    OutputAckChunk(fixed_seg);
    if (is_last) break;
  }

  buf_out->resize(actual_size);
  return Status::OK();
}

// ---- StreamRecvOutputSSTToFd -------------------------------------------
Status NtbChannel::StreamRecvOutputSSTToFd(uint64_t file_number, uint64_t file_size,
                                           int fd, uint64_t* actual_size_out,
                                           int fixed_seg) {
  uint64_t actual_size = 0;

  while (true) {
    uint64_t recv_file;
    uint64_t file_offset, chunk_bytes;
    bool     is_last;
    Status s = OutputRecvChunk(fixed_seg, &recv_file, &file_offset, &chunk_bytes, &is_last);
    if (!s.ok()) return s;

    if (recv_file != file_number) {
      fprintf(stderr,
             "[OUTPUT_MISMATCH] (ToFd) expect=%llu hdr=%llu seg=%d off=%llu "
             "bytes=%llu last=%d\n",
             (unsigned long long)file_number, (unsigned long long)recv_file,
             fixed_seg, (unsigned long long)file_offset,
             (unsigned long long)chunk_bytes, is_last);
      OutputAckChunk(fixed_seg);
      return Status::Corruption("StreamRecvOutputSSTToFd file number mismatch");
    }

    const char* data_ptr = static_cast<const char*>(local_output_data_map_[fixed_seg])
                          + kChunkHeaderSize;
    uint64_t written = 0;
    while (written < chunk_bytes) {
      ssize_t n = ::write(fd, data_ptr + written,
                         static_cast<size_t>(chunk_bytes - written));
      if (n <= 0) {
        OutputAckChunk(fixed_seg);
        return Status::IOError("StreamRecvOutputSSTToFd: write failed");
      }
      written += static_cast<uint64_t>(n);
    }
    actual_size = file_offset + chunk_bytes;

    OutputAckChunk(fixed_seg);
    if (is_last) break;
  }

  if (actual_size_out) *actual_size_out = actual_size;
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
