// ============================================================
//
//
//
//
//
//
//
//
// ============================================================
#include <map>
#include "db/compaction/clipping_iterator.h"
#include <thread>
#include <algorithm>
#include "worker_server.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <functional>

#include "../shared/compaction_spec.h"
#include "../shared/ntb_channel.h"
#include "../shared/ntb_mem_file.h"

#include "db/dbformat.h"
#include "file/random_access_file_reader.h"
#include "file/writable_file_writer.h"
#include "options/cf_options.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/internal_iterator.h"
#include "table/merging_iterator.h"
#include "table/table_builder.h"
#include "table/table_reader.h"
#include "util/coding.h"
#include "db/compaction/compaction_iterator.h"
#include "db/merge_helper.h"
#include "db/range_del_aggregator.h"
#include "rocksdb/table_properties.h"

namespace ROCKSDB_NAMESPACE {

namespace {

constexpr char kMsgTypeSpec      = 0x01;
constexpr char kMsgTypeResult    = 0x02;
constexpr char kMsgTypeChunkAck  = 0x03;
constexpr char kMsgTypeJobRegistered = 0x04;
constexpr char kMsgTypeOutputAck = 0x04;

std::mutex              g_output_ack_mu[kNumSegments];
std::condition_variable g_output_ack_cv[kNumSegments];
bool                    g_output_ack_ready[kNumSegments]  = {false};
uint64_t                g_output_ack_file[kNumSegments]   = {0};
bool                    g_output_ack_failed[kNumSegments] = {false};

bool RcOutAnySeg() {
  static const bool on = [] {
    const char* e = getenv("RC_OUTPUT_ANYSEG");
    bool v = !(e != nullptr && atoi(e) == 0);
    fprintf(stderr, "[worker] output segment: %s\n",
            v ? "any free (v-outq)" : "job-fixed");
    return v;
  }();
  return on;
}
struct OutJobDone {
  std::mutex              mu;
  std::condition_variable cv;
  int                     left   = 0;
  bool                    failed = false;
};
struct OutTask {
  uint64_t                                         job_id = 0;
  uint64_t                                         fnum   = 0;
  std::shared_ptr<std::vector<std::vector<char>>>  bufs;
  size_t                                           idx    = 0;
  std::shared_ptr<OutJobDone>                      done;
  std::shared_ptr<std::vector<char>>               one;
};
std::mutex              g_outq_mu;
std::condition_variable g_outq_cv;
std::queue<OutTask>     g_outq;
std::once_flag          g_outq_once;
std::thread*            g_outq_threads = nullptr;
std::atomic<bool>       g_outq_done[kNumSegments];
std::atomic<int>        g_outq_exited{0};

std::mutex              g_reg_mu;
std::condition_variable g_reg_cv;
uint64_t                g_reg_epoch = 0;

bool RcEnvOn(const char* name) {
  const char* e = getenv(name);
  return e != nullptr && atoi(e) != 0;
}
bool RcRegWait() {
  static const bool on = [] {
    bool v = RcEnvOn("RC_REG_WAIT");
    fprintf(stderr, "[worker] reg-ack send: %s\n", v ? "on (v-reg)" : "off (v-nowait)");
    return v;
  }();
  return on;
}
bool RcChunkAckWait() {
  static const bool on = [] {
    bool v = RcEnvOn("RC_CHUNK_ACK_WAIT");
    fprintf(stderr, "[worker] success chunk-ack send: %s\n", v ? "on (v22)" : "off (v-nowait)");
    return v;
  }();
  return on;
}
bool RcOutputAck() {
  static const bool on = [] {
    bool v = RcEnvOn("RC_OUTPUT_ACK");
    fprintf(stderr, "[worker] output-ack wait: %s\n", v ? "on (v12)" : "off (v-nowait)");
    return v;
  }();
  return on;
}

std::string TagMessage(char tag, const std::string& payload) {
  std::string out;
  out.reserve(payload.size() + 1);
  out.push_back(tag);
  out.append(payload);
  return out;
}

std::string MakeChunkAckPayload(uint64_t job_id, uint64_t file_number,
                                 bool success, int seg) {
  std::string out;
  out.reserve(1 + 1 + sizeof(job_id) + sizeof(file_number) + sizeof(uint32_t));
  out.push_back(kMsgTypeChunkAck);
  uint8_t ok = success ? 1 : 0;
  out.push_back(static_cast<char>(ok));
  out.append(reinterpret_cast<const char*>(&job_id), sizeof(job_id));
  out.append(reinterpret_cast<const char*>(&file_number), sizeof(file_number));
  uint32_t seg_u32 = static_cast<uint32_t>(seg);
  out.append(reinterpret_cast<const char*>(&seg_u32), sizeof(seg_u32));
  return out;
}

}  // namespace

namespace {
static int64_t WpoolCapBytes() {
  static const int64_t v = [] {
    const char* e = std::getenv("RC_WPOOL_MB");
    long long mb = (e && *e) ? std::atoll(e) : 32768;
    if (mb < 0) mb = 0;
    fprintf(stderr, "[worker] v-wpool: RC_WPOOL_MB=%lld\n", mb);
    return static_cast<int64_t>(mb) << 20;
  }();
  return v;
}
static std::mutex                               g_wpool_mu;
static std::multimap<size_t, std::vector<char>> g_wpool;  // capacity -> buffer
static int64_t                                  g_wpool_bytes = 0;
static std::atomic<uint64_t>                    g_wpool_hit{0}, g_wpool_miss{0};

static void WpoolTake(std::vector<char>* out, size_t n) {
  if (WpoolCapBytes() > 0) {
    std::lock_guard<std::mutex> lk(g_wpool_mu);
    auto it = g_wpool.lower_bound(n);
    if (it != g_wpool.end()) {
      g_wpool_bytes -= static_cast<int64_t>(it->first);
      *out = std::move(it->second);
      g_wpool.erase(it);
      g_wpool_hit.fetch_add(1, std::memory_order_relaxed);
      out->resize(n);
      return;
    }
  }
  g_wpool_miss.fetch_add(1, std::memory_order_relaxed);
  out->resize(n);
}

static void WpoolGive(std::vector<std::vector<char>>* bufs) {
  const int64_t cap = WpoolCapBytes();
  if (cap > 0) {
    for (auto& b : *bufs) {
      if (b.capacity() == 0) continue;
      b.resize(b.capacity());
      const size_t c = b.capacity();
      std::lock_guard<std::mutex> lk(g_wpool_mu);
      if (g_wpool_bytes + static_cast<int64_t>(c) > cap) continue;
      g_wpool_bytes += static_cast<int64_t>(c);
      g_wpool.emplace(c, std::move(b));
    }
  }
  bufs->clear();
}

static long long WpoolMb() {
  std::lock_guard<std::mutex> lk(g_wpool_mu);
  return static_cast<long long>(g_wpool_bytes >> 20);
}

struct WpoolReturnGuard {
  std::vector<std::vector<char>>* bufs;
  explicit WpoolReturnGuard(std::vector<std::vector<char>>* b) : bufs(b) {}
  ~WpoolReturnGuard() { if (bufs) WpoolGive(bufs); }
};

class NullCompactionProxy : public CompactionIterator::CompactionProxy {
 public:
  NullCompactionProxy(bool bottommost, int start_level, int output_level)
      : bottommost_(bottommost),
        start_level_(start_level),
        output_level_(output_level) {}

  int level() const override { return start_level_; }
  bool KeyNotExistsBeyondOutputLevel(
      const Slice& /*user_key*/,
      std::vector<size_t>* /*level_ptrs*/) const override {
    return bottommost_;
  }
  bool bottommost_level() const override { return bottommost_; }
  int number_levels() const override { return output_level_ + 1; }
  Slice GetLargestUserKey() const override { return Slice(); }
  bool allow_ingest_behind() const override { return false; }
  bool allow_mmap_reads() const override { return false; }
  bool enable_blob_garbage_collection() const override { return false; }
  double blob_garbage_collection_age_cutoff() const override { return 0.0; }
  uint64_t blob_compaction_readahead_size() const override { return 0; }
  const Version* input_version() const override { return nullptr; }
  bool DoesInputReferenceBlobFiles() const override { return false; }
  const Compaction* real_compaction() const override { return nullptr; }
  bool SupportsPerKeyPlacement() const override { return false; }

 private:
  bool bottommost_;
  int  start_level_;
  int  output_level_;
};
}  // namespace

WorkerServer::WorkerServer(const WorkerOptions& opts) : opts_(opts) {}
WorkerServer::~WorkerServer() { Stop(); }

Status WorkerServer::Connect() {
  Status s = channel_.OpenAsWorker(opts_.primary_node_id);
  if (!s.ok()) return s;
  return Status::OK();
}

void WorkerServer::Stop() {
  stop_.store(true, std::memory_order_release);
  for (int i = 0; i < kNumSegments; i++) spec_cv_[i].notify_all();
  compact_cv_.notify_all();
}

void WorkerServer::RequestStop() noexcept {
  stop_.store(true, std::memory_order_release);
}

bool WorkerServer::WaitThreadDone(std::atomic<bool>& done_flag,
                                   int timeout_sec) {
  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::seconds(timeout_sec);
  while (std::chrono::steady_clock::now() < deadline) {
    if (done_flag.load(std::memory_order_acquire)) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return done_flag.load(std::memory_order_acquire);
}

void WorkerServer::Run() {
  for (int i = 0; i < kNumSegments; i++) {
    int seg = i;
    recv_thread_done_[seg].store(false, std::memory_order_release);
    recv_threads_[i] = std::thread([this, seg]() {
      RecvLoop(seg);
      recv_thread_done_[seg].store(true, std::memory_order_release);
    });
  }

  compact_pool_.reserve(kNumCompactionThreads);
  for (int i = 0; i < kNumCompactionThreads; i++) {
    int tid = i;
    compact_thread_done_[tid].store(false, std::memory_order_release);
    compact_pool_.emplace_back([this, tid]() {
      CompactionLoop(tid);
      compact_thread_done_[tid].store(true, std::memory_order_release);
    });
  }

  DispatcherLoop();

  for (int i = 0; i < kNumSegments; i++) spec_cv_[i].notify_all();
  for (int i = 0; i < kNumSegments; i++) {
    if (WaitThreadDone(recv_thread_done_[i], kShutdownJoinTimeoutSec)) {
      recv_threads_[i].join();
    } else {
      fprintf(stderr,
              "[worker] Run(): RecvLoop[%d] did not stop within %ds — "
              "it is very likely stuck inside a wedged NTB/DMA channel "
              "call that software cannot cancel. Abandoning this thread "
              "(detached) so the rest of shutdown can proceed; this node "
              "will need a reboot to fully clear it.\n",
              i, kShutdownJoinTimeoutSec);
      recv_threads_[i].detach();
    }
  }

  compact_cv_.notify_all();
  for (size_t i = 0; i < compact_pool_.size(); i++) {
    if (WaitThreadDone(compact_thread_done_[i], kShutdownJoinTimeoutSec)) {
      compact_pool_[i].join();
    } else {
      fprintf(stderr,
              "[worker] Run(): CompactionLoop[%zu] did not stop within "
              "%ds — it is very likely stuck inside a wedged NTB/DMA "
              "channel call that software cannot cancel. Abandoning this "
              "thread (detached) so the rest of shutdown can proceed; "
              "this node will need a reboot to fully clear it.\n",
              i, kShutdownJoinTimeoutSec);
      compact_pool_[i].detach();
    }
  }

  fprintf(stderr, "[worker] Run(): shutdown sequence complete.\n");
}

// ── DispatcherLoop ────────────────────────────────────────────────
void WorkerServer::DispatcherLoop() {
  while (!stop_.load(std::memory_order_acquire)) {
    std::string raw;
    Status s = channel_.RecvSpecMsg(&raw);
    if (!s.ok()) {
      if (stop_.load(std::memory_order_acquire)) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      continue;
    }

    if (!raw.empty() && raw[0] == kMsgTypeOutputAck) {
      if (raw.size() < 1 + 1 + sizeof(uint64_t) * 2) {
        fprintf(stderr,
                "[worker] DispatcherLoop: malformed output-ack (size=%zu)\n",
                raw.size());
        continue;
      }
      uint8_t  success_byte   = static_cast<uint8_t>(raw[1]);
      uint64_t ack_job_id     = 0, ack_file_number = 0;
      memcpy(&ack_job_id, raw.data() + 2, sizeof(ack_job_id));
      memcpy(&ack_file_number, raw.data() + 2 + sizeof(ack_job_id),
             sizeof(ack_file_number));
      bool success = (success_byte != 0);
      int seg = 0; { std::lock_guard<std::mutex> lk(job_seg_mu_); auto sit = job_seg_map_.find(ack_job_id); seg = (sit != job_seg_map_.end()) ? sit->second : static_cast<int>(ack_job_id % kNumSegments); }
      {
        std::lock_guard<std::mutex> lk(g_output_ack_mu[seg]);
        g_output_ack_ready[seg]  = true;
        g_output_ack_file[seg]   = ack_file_number;
        g_output_ack_failed[seg] = !success;
      }
      g_output_ack_cv[seg].notify_one();
      continue;
    }

    if (raw.empty() || raw[0] != kMsgTypeSpec) {
      fprintf(stderr,
              "[worker] DispatcherLoop: unexpected ctrl tag (size=%zu)\n",
              raw.size());
      continue;
    }
    Slice spec_payload(raw.data() + 1, raw.size() - 1);

    CompactionSpec spec;
    if (!CompactionSpec::Deserialize(spec_payload, &spec)) {
      fprintf(stderr, "[worker] DispatcherLoop: deserialize failed\n");
      continue;
    }
    uint64_t job_id = spec.job_id;
    int seg = spec.seg;
    { std::lock_guard<std::mutex> lk(job_seg_mu_); job_seg_map_[job_id] = seg; }

    std::map<int, std::vector<size_t>> by_seg;
    for (size_t i = 0; i < spec.inputs.size(); i++) {
      int aseg = spec.inputs[i].assigned_seg;
      if (aseg < 0 || aseg >= kNumSegments) aseg = seg;
      by_seg[aseg].push_back(i);
    }
    if (by_seg.empty()) by_seg[seg];

    auto ja = std::make_shared<JobAssembly>();
    ja->sst_bufs.resize(spec.inputs.size());
    ja->remaining_files.store(static_cast<int>(spec.inputs.size()),
                               std::memory_order_relaxed);
    auto rc_alloc_t0 = std::chrono::steady_clock::now();
    for (size_t i = 0; i < spec.inputs.size(); i++) {
      WpoolTake(&ja->sst_bufs[i], spec.inputs[i].file_size);
    }
    fprintf(stderr, "[worker][timing] job=%llu alloc_us=%lld files=%zu\n",
            (unsigned long long)job_id,
            (long long)std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - rc_alloc_t0).count(),
            spec.inputs.size());
    fprintf(stderr, "[worker][wpool] job=%llu hit=%llu miss=%llu pool_mb=%lld\n",
            (unsigned long long)job_id, (unsigned long long)g_wpool_hit.load(),
            (unsigned long long)g_wpool_miss.load(), WpoolMb());
    ja->spec = std::move(spec);

    {
      std::lock_guard<std::mutex> lk(job_assembly_mu_);
      job_assembly_[job_id] = ja;
    }

    for (size_t fi = 0; fi < ja->spec.inputs.size(); fi++) {
      uint64_t old_fnum = ja->spec.inputs[fi].file_number;
      for (int k = 0; k < kNumSegments; k++) {
        std::lock_guard<std::mutex> lk(pending_mu_[k]);
        pending_recv_[k].erase(old_fnum);
      }
    }

    for (auto& kv : by_seg) {
      int s = kv.first;
      std::lock_guard<std::mutex> lk(pending_mu_[s]);
      for (size_t i : kv.second) {
        uint64_t fnum = ja->spec.inputs[i].file_number;
        PendingRecv pr;
        pr.job_id     = job_id;
        pr.file_index = i;
        pr.ja         = ja;
        pending_recv_[s][fnum] = std::move(pr);
      }
    }

    {
      std::string reg_payload;
      reg_payload.push_back(kMsgTypeJobRegistered);
      reg_payload.append(reinterpret_cast<const char*>(&job_id), sizeof(job_id));
      if (RcRegWait())
        channel_.SendResultMsg(static_cast<int>(job_id % kNumSegments), reg_payload);
    }
    {
      std::lock_guard<std::mutex> rlk(g_reg_mu);
      g_reg_epoch++;
    }
    g_reg_cv.notify_all();

    if (ja->remaining_files.load(std::memory_order_acquire) <= 0) {
      FinalizeJobAssembly(job_id, ja);
    }
  }
}

// ── RecvLoop ──────────────────────────────────────────────────────
static constexpr int kPendingLookupTimeoutMs = 30000;

void WorkerServer::FinalizeJobAssembly(uint64_t job_id,
                                       std::shared_ptr<JobAssembly> ja) {
  {
    std::lock_guard<std::mutex> lk(job_assembly_mu_);
    job_assembly_.erase(job_id);
  }

  if (ja->failed.load(std::memory_order_relaxed)) {
    CompactionResult err;
    err.job_id    = job_id;
    err.success   = false;
    err.error_msg = "SST receive failed";
    std::lock_guard<std::mutex> out_lk(ack_send_mu_[0]);
    channel_.SendResultMsg(static_cast<int>(job_id % kNumSegments),
                           TagMessage(kMsgTypeResult, err.Serialize()));
    return;
  }

  fprintf(stderr, "[worker][timing] job=%llu enqueue t_us=%lld\n",
          (unsigned long long)job_id,
          (long long)std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now().time_since_epoch()).count());
  {
    WorkerJob job;
    job.spec      = std::move(ja->spec);
    job.fixed_seg = job.spec.seg;
    job.sst_bufs  = std::move(ja->sst_bufs);
    std::lock_guard<std::mutex> lk(compact_mu_);
    compact_queue_.push(std::move(job));
  }
  compact_cv_.notify_one();
}

void WorkerServer::RecvLoop(int seg_idx) {
  while (!stop_.load(std::memory_order_acquire)) {
    uint64_t recv_file = 0, file_offset = 0, chunk_bytes = 0;
    bool is_last = false;
    uint64_t hdr_job = 0;
    Status ps = channel_.InputPeekChunk(seg_idx, &recv_file, &file_offset,
                                        &chunk_bytes, &is_last, &hdr_job);
    if (!ps.ok()) {
      if (stop_.load(std::memory_order_acquire)) break;
      continue;
    }

    PendingRecv pr;
    bool found = false;
    bool stale_data = false;
    auto t_wait0 = std::chrono::steady_clock::now();
    for (;;) {
      uint64_t epoch0;
      {
        std::lock_guard<std::mutex> rlk(g_reg_mu);
        epoch0 = g_reg_epoch;
      }
      for (int k = 0; k < kNumSegments && !found && !stale_data; k++) {
        int s2 = (seg_idx + k) % kNumSegments;
        std::lock_guard<std::mutex> lk(pending_mu_[s2]);
        auto it = pending_recv_[s2].find(recv_file);
        if (it == pending_recv_[s2].end()) continue;
        if (hdr_job != 0 && it->second.job_id > hdr_job) { stale_data = true; break; }
        if (hdr_job != 0 && it->second.job_id < hdr_job) continue;
        pr = it->second;
        found = true;
        if (is_last) pending_recv_[s2].erase(it);
      }
      if (found || stale_data) break;
      if (stop_.load(std::memory_order_acquire)) break;
      if (std::chrono::steady_clock::now() - t_wait0 >
          std::chrono::milliseconds(kPendingLookupTimeoutMs)) {
        break;
      }
      std::unique_lock<std::mutex> rlk(g_reg_mu);
      g_reg_cv.wait_for(rlk, std::chrono::milliseconds(100),
                        [epoch0] { return g_reg_epoch != epoch0; });
    }

    if (!found) {
      fprintf(stderr,
              "[worker] RecvLoop[%d]: file_number=%llu hdr_job=%llu %s -- 버림\n",
              seg_idx, (unsigned long long)recv_file,
              (unsigned long long)hdr_job,
              stale_data ? "옛 잡의 남은 데이터"
                         : "도착했는데 기다리는 job을 못 찾음 (상한 초과)");
      channel_.InputConsumeChunk(seg_idx, nullptr, 0);
      if (!stale_data && hdr_job != 0) {
        std::string ack = MakeChunkAckPayload(hdr_job, recv_file,
                                               /*success=*/false, seg_idx);
        std::lock_guard<std::mutex> out_lk(ack_send_mu_[seg_idx]);
        channel_.SendResultMsg(seg_idx, ack);
      }
      continue;
    }

    char* dest = nullptr;
    {
      auto& rbuf = pr.ja->sst_bufs[pr.file_index];
      if (file_offset + chunk_bytes <= rbuf.size()) {
        dest = rbuf.data() + file_offset;
      } else {
        fprintf(stderr,
                "[worker] RecvLoop[%d]: v-nopad OVERFLOW job=%llu file=%llu "
                "off=%llu bytes=%llu buf=%zu -- 잡 실패 처리\n",
                seg_idx, (unsigned long long)pr.job_id,
                (unsigned long long)recv_file,
                (unsigned long long)file_offset,
                (unsigned long long)chunk_bytes, rbuf.size());
        pr.ja->failed.store(true, std::memory_order_relaxed);
      }
    }
    channel_.InputConsumeChunk(seg_idx, dest, chunk_bytes);

    if (RcChunkAckWait()) {
      std::string ack = MakeChunkAckPayload(pr.job_id, recv_file,
                                             /*success=*/true, seg_idx);
      std::lock_guard<std::mutex> out_lk(ack_send_mu_[seg_idx]);
      channel_.SendResultMsg(seg_idx, ack);
    }

    fprintf(stderr,
            "[worker][timing] job=%llu seg=%d input_file=%llu idx=%zu "
            "arrive_t_us=%lld\n",
            (unsigned long long)pr.job_id, seg_idx,
            (unsigned long long)recv_file, pr.file_index,
            (long long)std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count());

    if (is_last) {
      pr.ja->sst_bufs[pr.file_index].resize(
          pr.ja->spec.inputs[pr.file_index].file_size);
      int remaining = pr.ja->remaining_files.fetch_sub(
                           1, std::memory_order_acq_rel) - 1;
      if (remaining <= 0) {
        FinalizeJobAssembly(pr.job_id, pr.ja);
      }
    }
  }
}

std::mutex                                      g_sjobs_mu;
std::map<uint64_t, std::shared_ptr<OutJobDone>> g_sjobs;
bool RcStreamGen() {
  static const bool on = [] {
    const char* e = getenv("RC_STREAM_PERSIST");
    bool v = (e != nullptr && atoi(e) == 2) && RcOutAnySeg();
    fprintf(stderr, "[worker] v-stream send-on-finish: %s\n", v ? "on" : "off");
    return v;
  }();
  return on;
}
void RcStreamEmit(uint64_t job_id, uint64_t fnum, std::vector<char>* buf) {
  std::shared_ptr<OutJobDone> d;
  {
    std::lock_guard<std::mutex> g(g_sjobs_mu);
    auto it = g_sjobs.find(job_id);
    if (it == g_sjobs.end()) return;
    d = it->second;
  }
  OutTask t;
  t.job_id = job_id;
  t.fnum   = fnum;
  t.idx    = 0;
  t.done   = d;
  t.one    = std::make_shared<std::vector<char>>(std::move(*buf));
  {
    std::lock_guard<std::mutex> dl(d->mu);
    d->left++;
  }
  {
    std::lock_guard<std::mutex> ql(g_outq_mu);
    g_outq.push(std::move(t));
  }
  g_outq_cv.notify_all();
}

void WorkerServer::CompactionLoop(int thread_id) {
  uint64_t total_jobs   = 0;
  uint64_t success_jobs = 0;
  uint64_t failed_jobs  = 0;

  while (!stop_.load(std::memory_order_acquire)) {
    WorkerJob job;
    {
      std::unique_lock<std::mutex> lk(compact_mu_);
      while (compact_queue_.empty() &&
             !stop_.load(std::memory_order_acquire)) {
        compact_cv_.wait_for(lk, std::chrono::milliseconds(200));
      }
      if (stop_.load(std::memory_order_acquire) &&
          compact_queue_.empty()) break;
      job = std::move(compact_queue_.front());
      compact_queue_.pop();
    }

    total_jobs++;
    int seg = job.fixed_seg;
    fprintf(stderr, "[worker][timing] job=%llu start t_us=%lld\n",
            (unsigned long long)job.spec.job_id,
            (long long)std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());

    std::shared_ptr<OutJobDone> sdone;
    if (RcStreamGen()) {
      std::call_once(g_outq_once, [this]() {
                g_outq_threads = new std::thread[kNumSegments];
                for (int i = 0; i < kNumSegments; i++) {
                  g_outq_done[i].store(false, std::memory_order_release);
                  g_outq_threads[i] = std::thread([this, i]() {
                    while (true) {
                      OutTask t;
                      {
                        std::unique_lock<std::mutex> lk(g_outq_mu);
                        while (g_outq.empty() && !stop_.load(std::memory_order_acquire))
                          g_outq_cv.wait_for(lk, std::chrono::milliseconds(200));
                        if (stop_.load(std::memory_order_acquire)) break;
                        t = std::move(g_outq.front());
                        g_outq.pop();
                      }
                      Status ws = channel_.StreamSendOutputSST(t.one ? *t.one : (*t.bufs)[t.idx], t.fnum, i, t.job_id);
                      if (!ws.ok()) {
                        fprintf(stderr,
                                "[worker] OutSendLoop[%d]: StreamSendOutputSST failed "
                                "job=%llu fnum=%llu: %s\n",
                                i, (unsigned long long)t.job_id, (unsigned long long)t.fnum,
                                ws.ToString().c_str());
                      }
                      {
                        std::lock_guard<std::mutex> dlk(t.done->mu);
                        if (!ws.ok()) t.done->failed = true;
                        t.done->left--;
                      }
                      t.done->cv.notify_all();
                    }
                    g_outq_done[i].store(true, std::memory_order_release);
                  });
                }
                fprintf(stderr, "[worker] v-outq: %d output sender threads started\n", kNumSegments);
              });
      sdone = std::make_shared<OutJobDone>();
      std::lock_guard<std::mutex> sg(g_sjobs_mu);
      g_sjobs[job.spec.job_id] = sdone;
    }
    CompactionResult               result;
    std::vector<std::vector<char>> out_bufs;
    Status s = HandleJob(job.spec, job.sst_bufs, &result, &out_bufs);
    WpoolReturnGuard wpool_guard(&job.sst_bufs);
    fprintf(stderr, "[worker][timing] job=%llu handle_done t_us=%lld\n",
            (unsigned long long)job.spec.job_id,
            (long long)std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
    if (sdone) {
      {
        std::lock_guard<std::mutex> sg(g_sjobs_mu);
        g_sjobs.erase(job.spec.job_id);
      }
      std::unique_lock<std::mutex> sdl(sdone->mu);
      while (sdone->left > 0 && !stop_.load(std::memory_order_acquire))
        sdone->cv.wait_for(sdl, std::chrono::milliseconds(200));
      if (sdone->failed && s.ok()) s = Status::IOError("v-stream: output send failed");
    }

    if (!s.ok()) {
      failed_jobs++;
      fprintf(stderr,
              "[worker] CompactionLoop[%d]: job=%llu failed "
              "(ok=%llu fail=%llu): %s\n",
              thread_id, (unsigned long long)job.spec.job_id,
              (unsigned long long)success_jobs,
              (unsigned long long)failed_jobs,
              s.ToString().c_str());

      if (getenv("RC_DIAG_DUMP")) {
      for (size_t vi = 0; vi < job.sst_bufs.size(); vi++) {
        uint64_t vfnum = job.spec.inputs[vi].file_number;
        std::string vpath = std::string(getenv("HOME") ? getenv("HOME") : ".") +
                             "/dma_corrupt_dumps/job" +
                             std::to_string(job.spec.job_id) + "_f" +
                             std::to_string(vfnum) + ".bin";
        FILE* vfp = fopen(vpath.c_str(), "wb");
        if (vfp) {
          fwrite(job.sst_bufs[vi].data(), 1, job.sst_bufs[vi].size(), vfp);
          fclose(vfp);
          fprintf(stderr, "[diag] dumped job=%llu file=%llu size=%zu -> %s\n",
                  (unsigned long long)job.spec.job_id,
                  (unsigned long long)vfnum, job.sst_bufs[vi].size(),
                  vpath.c_str());
        } else {
          fprintf(stderr, "[diag] dump FAILED to open %s\n", vpath.c_str());
        }
      }
      }

      result.job_id    = job.spec.job_id;
      result.success   = false;
      result.error_msg = s.ToString();
      std::lock_guard<std::mutex> out_lk(send_output_mu_[seg]);
      {
        Status rs_;
        while (!(rs_ = channel_.SendResultMsg(seg, TagMessage(kMsgTypeResult, result.Serialize()))).ok() &&
               !stop_.load(std::memory_order_acquire)) {
          fprintf(stderr, "[worker] Result send failed job=%llu seg=%d: %s -- retry (v-ctrlslot)\n",
                  (unsigned long long)job.spec.job_id, seg, rs_.ToString().c_str());
        }
      }
      continue;
    }

    success_jobs++;

    if (RcOutAnySeg()) {
      {
        Status rs_;
        while (!(rs_ = channel_.SendResultMsg(seg, TagMessage(kMsgTypeResult, result.Serialize()))).ok() &&
               !stop_.load(std::memory_order_acquire)) {
          fprintf(stderr, "[worker] Result send failed job=%llu seg=%d: %s -- retry (v-ctrlslot)\n",
                  (unsigned long long)job.spec.job_id, seg, rs_.ToString().c_str());
        }
      }
      if (!out_bufs.empty() && !sdone) {
        std::call_once(g_outq_once, [this]() {
          g_outq_threads = new std::thread[kNumSegments];
          for (int i = 0; i < kNumSegments; i++) {
            g_outq_done[i].store(false, std::memory_order_release);
            g_outq_threads[i] = std::thread([this, i]() {
              while (true) {
                OutTask t;
                {
                  std::unique_lock<std::mutex> lk(g_outq_mu);
                  while (g_outq.empty() && !stop_.load(std::memory_order_acquire))
                    g_outq_cv.wait_for(lk, std::chrono::milliseconds(200));
                  if (stop_.load(std::memory_order_acquire)) break;
                  t = std::move(g_outq.front());
                  g_outq.pop();
                }
                Status ws = channel_.StreamSendOutputSST(t.one ? *t.one : (*t.bufs)[t.idx], t.fnum, i, t.job_id);
                if (!ws.ok()) {
                  fprintf(stderr,
                          "[worker] OutSendLoop[%d]: StreamSendOutputSST failed "
                          "job=%llu fnum=%llu: %s\n",
                          i, (unsigned long long)t.job_id, (unsigned long long)t.fnum,
                          ws.ToString().c_str());
                }
                {
                  std::lock_guard<std::mutex> dlk(t.done->mu);
                  if (!ws.ok()) t.done->failed = true;
                  t.done->left--;
                }
                t.done->cv.notify_all();
              }
              g_outq_done[i].store(true, std::memory_order_release);
            });
          }
          fprintf(stderr, "[worker] v-outq: %d output sender threads started\n", kNumSegments);
        });
        auto bufs = std::make_shared<std::vector<std::vector<char>>>(std::move(out_bufs));
        auto done = std::make_shared<OutJobDone>();
        done->left = static_cast<int>(bufs->size());
        {
          std::lock_guard<std::mutex> qlk(g_outq_mu);
          for (size_t i = 0; i < bufs->size(); i++) {
            OutTask t;
            t.job_id = job.spec.job_id;
            t.fnum   = result.output_files[i].file_number;
            t.bufs   = bufs;
            t.idx    = i;
            t.done   = done;
            g_outq.push(std::move(t));
          }
        }
        g_outq_cv.notify_all();
        std::unique_lock<std::mutex> dlk(done->mu);
        while (done->left > 0 && !stop_.load(std::memory_order_acquire))
          done->cv.wait_for(dlk, std::chrono::milliseconds(200));
        if (done->failed)
          fprintf(stderr, "[worker] CompactionLoop[%d]: job=%llu some outputs failed to send (v-outq)\n",
                  thread_id, (unsigned long long)job.spec.job_id);
      }
      continue;
    }

    {
      std::lock_guard<std::mutex> out_lk(send_output_mu_[seg]);
      {
        Status rs_;
        while (!(rs_ = channel_.SendResultMsg(seg, TagMessage(kMsgTypeResult, result.Serialize()))).ok() &&
               !stop_.load(std::memory_order_acquire)) {
          fprintf(stderr, "[worker] Result send failed job=%llu seg=%d: %s -- retry (v-ctrlslot)\n",
                  (unsigned long long)job.spec.job_id, seg, rs_.ToString().c_str());
        }
      }
      for (size_t i = 0; i < out_bufs.size(); i++) {
        uint64_t fnum = result.output_files[i].file_number;
        Status   ws   = channel_.StreamSendOutputSST(out_bufs[i], fnum, seg,
                                                     job.spec.job_id);
        if (!ws.ok()) {
          fprintf(stderr,
                  "[worker] CompactionLoop[%d]: StreamSendOutputSST failed "
                  "job=%llu fnum=%llu: %s\n",
                  thread_id, (unsigned long long)job.spec.job_id,
                  (unsigned long long)fnum, ws.ToString().c_str());
          break;
        }

        if (RcOutputAck()) {
          std::unique_lock<std::mutex> ack_lk(g_output_ack_mu[seg]);
          uint64_t expect_file = fnum;
          bool got = g_output_ack_cv[seg].wait_for(
              ack_lk, std::chrono::seconds(30),
              [this, seg, expect_file] {
                return (g_output_ack_ready[seg] &&
                        g_output_ack_file[seg] == expect_file) ||
                       stop_.load(std::memory_order_acquire);
              });
          if (stop_.load(std::memory_order_acquire)) break;
          if (!got) {
            fprintf(stderr,
                    "[worker] CompactionLoop[%d]: output-ack TIMEOUT "
                    "job=%llu file=%llu\n",
                    thread_id, (unsigned long long)job.spec.job_id,
                    (unsigned long long)fnum);
            break;
          }
          bool failed = g_output_ack_failed[seg];
          g_output_ack_ready[seg]  = false;
          g_output_ack_failed[seg] = false;
          if (failed) {
            fprintf(stderr,
                    "[worker] CompactionLoop[%d]: output-ack FAILED "
                    "job=%llu file=%llu\n",
                    thread_id, (unsigned long long)job.spec.job_id,
                    (unsigned long long)fnum);
            break;
          }
        }
      }
    }
  }

  if (g_outq_exited.fetch_add(1) + 1 == kNumCompactionThreads && g_outq_threads != nullptr) {
    g_outq_cv.notify_all();
    for (int i = 0; i < kNumSegments; i++) {
      if (!g_outq_threads[i].joinable()) continue;
      if (WaitThreadDone(g_outq_done[i], kShutdownJoinTimeoutSec)) {
        g_outq_threads[i].join();
      } else {
        fprintf(stderr, "[worker] OutSendLoop[%d] did not finish in %ds -- detach (v-outq)\n",
                i, kShutdownJoinTimeoutSec);
        g_outq_threads[i].detach();
      }
    }
  }

  fprintf(stderr,
          "[worker] CompactionLoop[%d] exiting: "
          "total=%llu success=%llu failed=%llu\n",
          thread_id,
          (unsigned long long)total_jobs,
          (unsigned long long)success_jobs,
          (unsigned long long)failed_jobs);
}

// ── FindComparator ────────────────────────────────────────────────
const Comparator* WorkerServer::FindComparator(const std::string& name) {
  if (name == BytewiseComparator()->Name()) return BytewiseComparator();
  if (name == ReverseBytewiseComparator()->Name())
    return ReverseBytewiseComparator();
  return nullptr;
}

// ── OpenInputIterators ────────────────────────────────────────────
Status WorkerServer::OpenInputIterators(
    const CompactionSpec&                 spec,
    const Options&                        opts,
    const ReadOptions&                    ro,
    const ImmutableOptions&               iopts,
    const std::vector<std::vector<char>>& sst_bufs,
    std::vector<InternalIterator*>*       iters_out,
    std::vector<TableReader*>*            readers_out) {

  for (size_t i = 0; i < spec.inputs.size(); i++) {
    const auto& f   = spec.inputs[i];
    const auto& buf = sst_bufs[i];

    auto file_reader = MakeMemFileReader(
        buf.data(), static_cast<uint64_t>(buf.size()),
        "mem_in_" + std::to_string(f.file_number));

    TableReaderOptions tro(
        iopts,
        /*prefix_extractor=*/nullptr,
        /*compression_manager=*/nullptr,
        EnvOptions(),
        iopts.internal_comparator,
        /*block_protection_bytes_per_key=*/0,
        /*skip_filters=*/false,
        /*immortal=*/false,
        /*force_direct_prefetch=*/false,
        f.level);

    std::unique_ptr<TableReader> reader;
    Status s = opts.table_factory->NewTableReader(
        tro,
        std::move(file_reader),
        static_cast<uint64_t>(buf.size()),
        &reader);
    if (!s.ok()) {
      fprintf(stderr,
              "[worker] OpenInputIterators: NewTableReader failed "
              "file=%llu: %s\n",
              (unsigned long long)f.file_number, s.ToString().c_str());
      return s;
    }

    {
      auto props = reader->GetTableProperties();
      if (props && (props->num_range_deletions > 0 ||
                    props->num_merge_operands > 0)) {
        fprintf(stderr,
                "[worker] OpenInputIterators: file=%llu range_dels=%llu "
                "merge_ops=%llu -- not supported by remote worker\n",
                (unsigned long long)f.file_number,
                (unsigned long long)props->num_range_deletions,
                (unsigned long long)props->num_merge_operands);
        return Status::NotSupported(
            "remote worker: input has range deletions or merge operands");
      }
    }

    InternalIterator* iter = reader->NewIterator(
        ro, nullptr, nullptr, false, TableReaderCaller::kCompaction);
    readers_out->push_back(reader.release());
    iters_out->push_back(iter);
  }
  return Status::OK();
}

// ── WriteOutputSSTs ───────────────────────────────────────────────
Status WorkerServer::WriteOutputSSTs(
    const CompactionSpec&           spec,
    const Options&                  opts,
    const ReadOptions&              ro,
    const ImmutableOptions&         iopts,
    InternalIterator*               merged_iter,
    CompactionResult*               result_out,
    std::vector<std::vector<char>>* out_bufs) {
  (void)ro;
  (void)iopts;

  static const bool kGpCutEnv = [] {
    const char* e = std::getenv("RC_GP_CUT");
    bool v = !(e && e[0] == '0');
    fprintf(stderr, "[worker] output cut by grandparent boundary: %s (v-gpcut)\n", v ? "on" : "off");
    return v;
  }();
  const bool gp_on = kGpCutEnv && spec.output_level > 0 && !spec.bottommost &&
                     !spec.output.grandparents.empty() && spec.output.gp_target_file_size > 0 &&
                     spec.output.gp_max_file_size > 0;
  const uint64_t target_size = gp_on ? spec.output.gp_max_file_size : spec.output.target_file_size;
  uint64_t fn_counter = (spec.first_output_file_number != 0)
                            ? spec.first_output_file_number
                            : 10000;
  const Comparator* ucmp = opts.comparator;

  std::vector<InternalKey> gp_lo, gp_hi;
  std::vector<uint64_t>    gp_sz;
  if (gp_on) {
    for (const auto& g : spec.output.grandparents) {
      gp_lo.emplace_back(); gp_lo.back().DecodeFrom(g.smallest_ikey);
      gp_hi.emplace_back(); gp_hi.back().DecodeFrom(g.largest_ikey);
      gp_sz.push_back(g.file_size);
    }
  }
  size_t   gp_index = 0, gp_switched = 0, gp_cut_cnt = 0, size_cut_cnt = 0;
  bool     gp_gap = true, gp_seen = false;
  uint64_t gp_overlap = 0;
  const uint64_t gp_target = spec.output.gp_target_file_size;
  const uint64_t gp_maxcb  = spec.output.gp_max_compaction_bytes;
  auto gp_cmp = [&](const Slice& a, const InternalKey& bk) -> int {
    const Slice b = bk.Encode();
    int c = ucmp->CompareWithoutTimestamp(ExtractUserKey(a), ExtractUserKey(b));
    if (c != 0) return c;
    const uint64_t af = ExtractInternalKeyFooter(a), bf = ExtractInternalKeyFooter(b);
    if (af == kRangeTombstoneSentinel) { if (bf != kRangeTombstoneSentinel) return -1; }
    else if (bf == kRangeTombstoneSentinel) { return 1; }
    return 0;
  };
  // GetCurrentKeyGrandparentOverlappedBytes
  auto gp_cur_overlap = [&](const Slice& ik) -> uint64_t {
    if (gp_gap || gp_index >= gp_lo.size()) return 0;
    uint64_t b = gp_sz[gp_index];
    for (int64_t i = static_cast<int64_t>(gp_index) - 1; i >= 0 && gp_cmp(ik, gp_hi[i]) == 0; i--) b += gp_sz[i];
    return b;
  };
  auto gp_update = [&](const Slice& ik) -> size_t {
    size_t crossed = 0;
    if (gp_lo.empty()) return 0;
    while (gp_index < gp_lo.size()) {
      if (gp_gap) {
        if (gp_cmp(ik, gp_lo[gp_index]) < 0) break;
        if (gp_seen) { crossed++; gp_overlap += gp_sz[gp_index]; gp_switched++; }
        gp_gap = false;
      } else {
        const int c = gp_cmp(ik, gp_hi[gp_index]);
        if (c < 0 || (c == 0 && (gp_index == gp_lo.size() - 1 || gp_cmp(ik, gp_lo[gp_index + 1]) < 0))) break;
        if (gp_seen) { crossed++; gp_switched++; }
        gp_gap = true;
        gp_index++;
      }
    }
    if (!gp_seen && !gp_gap) gp_overlap = gp_cur_overlap(ik);
    gp_seen = true;
    return crossed;
  };
  auto gp_should_cut = [&](uint64_t cur, size_t crossed, uint64_t prev_overlap) -> bool {
    if (crossed == 0) return false;
    if (gp_maxcb > 0 && gp_overlap + cur > gp_maxcb) return true;
    const size_t skippable = gp_gap ? 2 : 3;
    if (crossed >= skippable && gp_overlap - prev_overlap > gp_target / 8) return true;
    return cur >= ((gp_target + 99) / 100) * (50 + std::min<size_t>(gp_switched * 5, 40));
  };
  auto gp_reset = [&](const Slice& next_ik) { gp_switched = 0; gp_overlap = gp_cur_overlap(next_ik); };

  std::vector<SequenceNumber> snaps = spec.snapshots.list;
  std::sort(snaps.begin(), snaps.end());
  const SequenceNumber earliest_snapshot =
      snaps.empty() ? kMaxSequenceNumber : snaps.front();
  SequenceNumber ewc_snapshot = spec.snapshots.earliest_write_conflict_snapshot;
  if (ewc_snapshot == 0) ewc_snapshot = kMaxSequenceNumber;

  int start_level = spec.output_level;
  for (const auto& f : spec.inputs) start_level = std::min(start_level, f.level);

  MergeHelper merge_helper(Env::Default(), ucmp,
                           /*user_merge_operator=*/nullptr,
                           /*compaction_filter=*/nullptr,
                           /*logger=*/nullptr,
                           /*assert_valid_internal_key=*/false,
                           /*latest_snapshot=*/snaps.empty() ? 0 : snaps.back(),
                           /*snapshot_checker=*/nullptr, start_level);

  InternalKeyComparator icmp(ucmp);
  CompactionRangeDelAggregator range_del_agg(&icmp, snaps);
  static const std::atomic<bool> kNotCanceled{false};

  merged_iter->SeekToFirst();
  if (!merged_iter->status().ok()) return merged_iter->status();

  CompactionIterator c_iter(
      merged_iter, ucmp, &merge_helper,
      /*last_sequence=*/kMaxSequenceNumber, &snaps, earliest_snapshot,
      ewc_snapshot, /*job_snapshot=*/kMaxSequenceNumber,
      /*snapshot_checker=*/nullptr, Env::Default(),
      /*report_detailed_time=*/false, &range_del_agg,
      /*blob_file_builder=*/nullptr, /*allow_data_in_errors=*/false,
      /*enforce_single_del_contracts=*/true, kNotCanceled,
      std::make_unique<NullCompactionProxy>(spec.bottommost, start_level,
                                            spec.output_level),
      /*must_count_input_entries=*/false);
  c_iter.SeekToFirst();
  if (gp_on && c_iter.Valid()) gp_update(c_iter.key());

  uint64_t    total_out_entries = 0;
  std::string last_user_key;

  while (c_iter.Valid()) {
    out_bufs->emplace_back();
    std::vector<char>& cur_buf = out_bufs->back();

    auto writer = MakeMemFileWriter(
        &cur_buf, "mem_out_" + std::to_string(fn_counter));

    BlockBasedTableOptions bbto;
    Options local_opts = opts;
    local_opts.table_factory.reset(NewBlockBasedTableFactory(bbto));
    ImmutableOptions local_iopts(local_opts);
    MutableCFOptions mopts(local_opts);
    ReadOptions  tbo_ro;
    WriteOptions tbo_wo;

    static InternalTblPropCollFactories empty_factories;
    TableBuilderOptions tbo(
        local_iopts,
        mopts,
        tbo_ro,
        tbo_wo,
        local_iopts.internal_comparator,
        &empty_factories,
        spec.output.compression,
        spec.output.compression_opts,
        /*column_family_id=*/0,
        /*column_family_name=*/"default",
        /*level=*/spec.output_level,
        /*newest_key_time=*/0,
        /*is_bottommost=*/spec.bottommost,
        TableFileCreationReason::kCompaction,
        /*oldest_key_time=*/0,
        /*file_creation_time=*/0,
        /*db_id=*/"",
        /*db_session_id=*/"",
        target_size,
        fn_counter);

    std::unique_ptr<TableBuilder> builder(
        local_opts.table_factory->NewTableBuilder(tbo, writer.get()));

    bool        has_entries = false;
    InternalKey smallest, largest;
    uint64_t    num_entries   = 0;
    uint64_t    num_deletions = 0;
    SequenceNumber min_seqno  = kMaxSequenceNumber;
    SequenceNumber max_seqno  = 0;

    while (c_iter.Valid()) {
      const Slice ikey = c_iter.key();
      const Slice val  = c_iter.value();

      ParsedInternalKey pikey;
      if (ParseInternalKey(ikey, &pikey, false).ok()) {
        if (pikey.type == kTypeDeletion ||
            pikey.type == kTypeSingleDeletion) num_deletions++;
        if (pikey.sequence < min_seqno) min_seqno = pikey.sequence;
        if (pikey.sequence > max_seqno) max_seqno = pikey.sequence;
      }

      if (!has_entries) { smallest.DecodeFrom(ikey); has_entries = true; }
      largest.DecodeFrom(ikey);
      builder->Add(ikey, val);
      num_entries++;

      const Slice uk = ExtractUserKey(ikey);
      last_user_key.assign(uk.data(), uk.size());
      c_iter.Next();

      const uint64_t gp_prev_overlap = gp_overlap;
      const size_t   gp_crossed = (gp_on && c_iter.Valid()) ? gp_update(c_iter.key()) : 0;
      const bool next_diff_key = c_iter.Valid() &&
          ucmp->Compare(ExtractUserKey(c_iter.key()), Slice(last_user_key)) != 0;
      if (next_diff_key) {
        const uint64_t cur = builder->FileSize();
        const bool by_size = cur >= target_size;
        const bool by_gp   = !by_size && gp_on && gp_should_cut(cur, gp_crossed, gp_prev_overlap);
        if (by_size || by_gp) {
          if (gp_on) { gp_reset(c_iter.key()); if (by_gp) gp_cut_cnt++; else size_cut_cnt++; }
          break;
        }
      }
    }

    if (!c_iter.status().ok()) {
      builder->Abandon();
      return c_iter.status();
    }
    if (!has_entries) {
      builder->Abandon();
      out_bufs->pop_back();
      break;
    }

    Status s = builder->Finish();
    if (!s.ok()) return s;

    IOOptions io_opts;
    IOStatus ios = writer->Sync(io_opts, /*use_fsync=*/false);
    if (!ios.ok()) return ios;

    CompactionResult::OutputFile of;
    of.file_number    = fn_counter++;
    of.file_size      = static_cast<uint64_t>(cur_buf.size());
    of.smallest_ikey  = smallest.Encode().ToString();
    of.largest_ikey   = largest.Encode().ToString();
    of.smallest_seqno = (min_seqno == kMaxSequenceNumber) ? 0 : min_seqno;
    of.largest_seqno  = max_seqno;
    of.num_entries    = num_entries;
    of.num_deletions  = num_deletions;
    of.content_crc32 = NtbDiagCrc32(cur_buf.data(), cur_buf.size());
    fprintf(stderr,
            "[diag][crc] WriteOutputSSTs: file=%llu size=%zu crc32=%u\n",
            (unsigned long long)of.file_number, cur_buf.size(),
            of.content_crc32);

    total_out_entries += num_entries;
    result_out->output_files.push_back(std::move(of));
    result_out->bytes_written += cur_buf.size();
    if (RcStreamGen()) {
      writer.reset();
      RcStreamEmit(spec.job_id, result_out->output_files.back().file_number, &cur_buf);
    }
  }

  if (!c_iter.status().ok()) return c_iter.status();
  if (gp_on) {
    fprintf(stderr, "[worker][gpcut] job=%llu level=%d grandparents=%zu out_files=%zu cut_by_gp=%zu cut_by_size=%zu\n",
            (unsigned long long)spec.job_id, spec.output_level, gp_lo.size(), out_bufs->size(), gp_cut_cnt, size_cut_cnt);
  }

  const CompactionIterationStats& st = c_iter.iter_stats();
  fprintf(stderr,
          "[worker][citer] job=%llu in_records=%llu out_records=%llu "
          "drop_hidden=%llu drop_obsolete=%llu out_files=%zu bottommost=%d "
          "snaps=%zu\n",
          (unsigned long long)spec.job_id,
          (unsigned long long)st.num_input_records,
          (unsigned long long)total_out_entries,
          (unsigned long long)st.num_record_drop_hidden,
          (unsigned long long)st.num_record_drop_obsolete,
          out_bufs->size(), spec.bottommost ? 1 : 0, snaps.size());

  return merged_iter->status();
}

// ── HandleJob ─────────────────────────────────────────────────────
Status WorkerServer::HandleJob(
    const CompactionSpec&                 spec,
    const std::vector<std::vector<char>>& sst_bufs,
    CompactionResult*                     result_out,
    std::vector<std::vector<char>>*       out_bufs) {
  auto t_compact_start = std::chrono::steady_clock::now();

  const Comparator* ucmp = FindComparator(spec.comparator_name);
  if (!ucmp)
    return Status::InvalidArgument(
        "unknown comparator: " + spec.comparator_name);

  Options          opts;
  opts.comparator = ucmp;
  opts.env        = Env::Default();
  BlockBasedTableOptions bbto;
  bbto.no_block_cache = true;
  opts.table_factory.reset(NewBlockBasedTableFactory(bbto));

  ImmutableOptions iopts(opts);
  ReadOptions      ro;

  std::vector<InternalIterator*> iters;
  std::vector<TableReader*>      readers;

  Status s = OpenInputIterators(spec, opts, ro, iopts, sst_bufs,
                                &iters, &readers);
  if (!s.ok()) {
    for (auto* r : readers) delete r;
    for (auto* it : iters)  delete it;
    return s;
  }

  static const int kWsub = [] {
    const char* e = std::getenv("RC_WSUB");
    int v = (e && *e) ? std::atoi(e) : 1;
    if (v < 1) v = 1;
    if (v > 64) v = 64;
    fprintf(stderr, "[worker] v-wsub: RC_WSUB=%d\n", v);
    return v;
  }();
  static const uint64_t kWsubMinBytes = [] {
    const char* e = std::getenv("RC_WSUB_MIN_MB");
    long v = (e && *e) ? std::atol(e) : 64;
    if (v < 0) v = 0;
    fprintf(stderr, "[worker] v-wsub: RC_WSUB_MIN_MB=%ld\n", v);
    return static_cast<uint64_t>(v) << 20;
  }();

  result_out->job_id     = spec.job_id;
  result_out->success    = true;
  result_out->bytes_read = 0;
  for (const auto& buf : sst_bufs) result_out->bytes_read += buf.size();

  std::vector<std::string> wsub_bounds;
  if (kWsub > 1 && result_out->bytes_read >= kWsubMinBytes) {
    struct WsubAnc { std::string k; uint64_t sz; };
    std::vector<WsubAnc> anc;
    uint64_t anc_total = 0;
    for (auto* r : readers) {
      std::vector<TableReader::Anchor> av;
      Status as = r->ApproximateKeyAnchors(ro, av);
      if (!as.ok()) continue;
      for (auto& x : av) {
        anc_total += static_cast<uint64_t>(x.range_size);
        anc.push_back({std::move(x.user_key), static_cast<uint64_t>(x.range_size)});
      }
    }
    std::sort(anc.begin(), anc.end(),
              [&](const WsubAnc& x, const WsubAnc& y) { return ucmp->Compare(x.k, y.k) < 0; });
    if (anc_total > 0 && anc.size() > 1) {
      const uint64_t step = anc_total / static_cast<uint64_t>(kWsub);
      uint64_t acc = 0, next = step;
      for (size_t i = 0; i + 1 < anc.size(); i++) {
        acc += anc[i].sz;
        if (acc >= next) {
          if (wsub_bounds.empty() || ucmp->Compare(wsub_bounds.back(), anc[i].k) < 0)
            wsub_bounds.push_back(anc[i].k);
          while (next <= acc) next += step;
          if (wsub_bounds.size() + 1 >= static_cast<size_t>(kWsub)) break;
        }
      }
    }
    fprintf(stderr, "[worker][wsub2] job=%llu anchors=%zu bounds=%zu\n",
            (unsigned long long)spec.job_id, anc.size(), wsub_bounds.size());
  }

  if (wsub_bounds.empty()) {
    Arena             arena;
    InternalIterator* merged = NewMergingIterator(
        &iopts.internal_comparator,
        iters.data(), static_cast<int>(iters.size()), &arena);
    s = WriteOutputSSTs(spec, opts, ro, iopts, merged, result_out, out_bufs);
  } else {
    const size_t   K       = wsub_bounds.size() + 1;
    const uint64_t fn_base = (spec.first_output_file_number != 0)
                                 ? spec.first_output_file_number : 10000;
    std::vector<CompactionResult>               sub_res(K);
    std::vector<std::vector<std::vector<char>>> sub_bufs(K);
    std::vector<Status>                         sub_st(K);
    std::vector<uint64_t>                       sub_ms(K, 0);

    auto run_sub = [&](size_t k) {
      const auto t0 = std::chrono::steady_clock::now();
      CompactionSpec sspec = spec;
      sspec.first_output_file_number = fn_base + static_cast<uint64_t>(k) * 100000;
      std::vector<InternalIterator*> it_k;
      std::vector<TableReader*>      rd_k;
      Status st = OpenInputIterators(sspec, opts, ro, iopts, sst_bufs, &it_k, &rd_k);
      if (st.ok()) {
        Arena             arena_k;
        InternalIterator* merged_k = NewMergingIterator(
            &iopts.internal_comparator,
            it_k.data(), static_cast<int>(it_k.size()), &arena_k);
        InternalKey  lo_ik, hi_ik;
        Slice        lo_s, hi_s;
        const Slice* lo = nullptr;
        const Slice* hi = nullptr;
        if (k > 0) {
          lo_ik.Set(wsub_bounds[k - 1], kMaxSequenceNumber, kValueTypeForSeek);
          lo_s = lo_ik.Encode();
          lo = &lo_s;
        }
        if (k + 1 < K) {
          hi_ik.Set(wsub_bounds[k], kMaxSequenceNumber, kValueTypeForSeek);
          hi_s = hi_ik.Encode();
          hi = &hi_s;
        }
        ClippingIterator clip(merged_k, lo, hi, &iopts.internal_comparator);
        sub_res[k].job_id  = spec.job_id;
        sub_res[k].success = true;
        st = WriteOutputSSTs(sspec, opts, ro, iopts, &clip, &sub_res[k], &sub_bufs[k]);
        if (st.ok() && sub_bufs[k].size() != sub_res[k].output_files.size())
          st = Status::Corruption("v-wsub: out_bufs/output_files size mismatch");
      }
      for (auto* it : it_k) delete it;
      for (auto* r : rd_k)  delete r;
      sub_st[k] = st;
      sub_ms[k] = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - t0).count());
    };

    std::vector<std::thread> ths;
    ths.reserve(K - 1);
    for (size_t k = 1; k < K; k++) ths.emplace_back(run_sub, k);
    run_sub(0);
    for (auto& t : ths) t.join();

    for (size_t k = 0; k < K && s.ok(); k++) {
      if (!sub_st[k].ok()) s = sub_st[k];
    }
    if (s.ok()) {
      uint64_t fn = fn_base;
      for (size_t k = 0; k < K; k++) {
        for (size_t j = 0; j < sub_res[k].output_files.size(); j++) {
          CompactionResult::OutputFile of = std::move(sub_res[k].output_files[j]);
          if (RcStreamGen()) fn++; else of.file_number = fn++;
          result_out->output_files.push_back(std::move(of));
          out_bufs->push_back(std::move(sub_bufs[k][j]));
        }
        result_out->bytes_written += sub_res[k].bytes_written;
      }
    }
    std::string ms_str;
    for (size_t k = 0; k < K; k++) {
      if (k) ms_str += ",";
      ms_str += std::to_string(sub_ms[k]) + "/" + std::to_string(sub_res[k].output_files.size());
    }
    fprintf(stderr,
            "[worker][wsub] job=%llu out_level=%d K=%zu in_mb=%.1f out_files=%zu sub_ms/files=%s ok=%d\n",
            (unsigned long long)spec.job_id, spec.output_level, K,
            result_out->bytes_read / 1048576.0, result_out->output_files.size(),
            ms_str.c_str(), s.ok() ? 1 : 0);
  }
  result_out->elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t_compact_start).count();

  for (auto* it : iters)  delete it;
  for (auto* r : readers) delete r;

  if (!s.ok()) {
    result_out->success   = false;
    result_out->error_msg = s.ToString();
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
