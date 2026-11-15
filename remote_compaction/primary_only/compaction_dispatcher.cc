// ============================================================
//
//
//
//
//
//
//   tag(1) + success(1) + job_id(8) + file_number(8).
//
//   chunk-ack TIMEOUT/FAILED, malformed/unknown ctrl tag, deserialize
// ============================================================
#include "compaction_dispatcher.h"
#include <cstdlib>
namespace {
inline unsigned RcIdleYield() { static const unsigned v = [] { const char* e = std::getenv("RC_IDLE_YIELD"); return e ? (unsigned)std::strtoul(e, nullptr, 10) : 16u; }(); return v; }
inline unsigned RcIdleSleepUs() { static const unsigned v = [] { const char* e = std::getenv("RC_IDLE_SLEEP_US"); return e ? (unsigned)std::strtoul(e, nullptr, 10) : 200u; }(); return v; }
}  // namespace
#include <pthread.h>
#include <cstdio>

#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <sched.h>

#include <chrono>
#include <cstdio>
#include <cstring>

#include "db/compaction/compaction.h"
#include "db/column_family.h"
#include "rocksdb/comparator.h"
#include "rocksdb/options.h"

#include <cerrno>
#include <condition_variable>
#include <cstring>
#include <map>
#include <queue>
#include <thread>
int RcStreamWriteFileExt(const std::string& dst, const char* data, size_t size);

namespace ROCKSDB_NAMESPACE {

namespace {

constexpr char kMsgTypeSpec = 0x01;
constexpr char kMsgTypeResult = 0x02;
constexpr char kMsgTypeChunkAck = 0x03;
constexpr char kMsgTypeJobRegistered = 0x04;
constexpr char kMsgTypeOutputAck = 0x04;

std::string TagMessage(char tag, const std::string& payload) {
  std::string out;
  out.reserve(payload.size() + 1);
  out.push_back(tag);
  out.append(payload);
  return out;
}

std::string MakeOutputAckPayload(uint64_t job_id, uint64_t file_number,
                                  bool success) {
  std::string out;
  out.reserve(1 + 1 + sizeof(job_id) + sizeof(file_number));
  out.push_back(kMsgTypeOutputAck);
  uint8_t ok = success ? 1 : 0;
  out.push_back(static_cast<char>(ok));
  out.append(reinterpret_cast<const char*>(&job_id), sizeof(job_id));
  out.append(reinterpret_cast<const char*>(&file_number),
             sizeof(file_number));
  return out;
}

void PinCurrentThreadToCore(int core_id) {
  // [ablation] CPU affinity pinning disabled for perf comparison.
  (void)core_id;
}
}  // namespace

// ── SSTReservationTable ───────────────────────────────────────────
bool SSTReservationTable::TryReserve(const std::vector<uint64_t>& fnums) {
  std::lock_guard<std::mutex> lk(mu_);
  for (uint64_t fn : fnums)
    if (reserved_.count(fn)) return false;
  for (uint64_t fn : fnums) reserved_.insert(fn);
  return true;
}

void SSTReservationTable::Release(const std::vector<uint64_t>& fnums) {
  std::lock_guard<std::mutex> lk(mu_);
  for (uint64_t fn : fnums) reserved_.erase(fn);
}

bool SSTReservationTable::IsReserved(uint64_t fn) const {
  std::lock_guard<std::mutex> lk(mu_);
  return reserved_.count(fn) > 0;
}

// ── CompactionDispatcher ──────────────────────────────────────────
CompactionDispatcher::CompactionDispatcher(const DispatchOptions& opts)
    : opts_(opts) {}

CompactionDispatcher::~CompactionDispatcher() {
  Disconnect();
  {
    std::lock_guard<std::mutex> lk(finish_mu_);
    stop_finish_ = true;
  }
  finish_cv_.notify_all();
  for (auto& t : finish_pool_)
    if (t.joinable()) t.join();
}

Status CompactionDispatcher::Connect() {
  Status s = channel_.OpenAsPrimary(opts_.worker_node_id);
  if (!s.ok()) return s;

  stop_.store(false, std::memory_order_release);
  channel_open_.store(false, std::memory_order_release);
  {
    std::lock_guard<std::mutex> lk(finish_mu_);
    stop_finish_ = false;
  }

  for (int i = 0; i < kNumSegments; i++) {
    int seg = i;
    send_threads_[i] = std::thread([this, seg]() { SendLoop(seg); });
  }
  ctrl_recv_thread_ = std::thread([this]() { CtrlRecvLoop(); });
  for (int i = 0; i < kNumSegments; i++) {
    int seg = i;
    output_recv_threads_[i] =
        std::thread([this, seg]() { OutputRecvLoop(seg); });
  }
  const int rc_fin_n = [] { const char* e = std::getenv("RC_FINISH_THREADS"); int n = e ? std::atoi(e) : kFinishThreads; if (n < 1) n = 1; if (n > kFinishThreads) n = kFinishThreads;
    fprintf(stderr, "[rc] finish threads=%d (RC_FINISH_THREADS, max %d) (v-finthr)\n", n, (int)kFinishThreads); return n; }();
  for (int i = 0; i < rc_fin_n; i++)
    finish_pool_.emplace_back([this, i]() { FinishWorkerLoop(i); });

  channel_open_.store(true, std::memory_order_release);
  for (int i = 0; i < kNumSegments; i++) send_cv_[i].notify_all();
  return s;
}

void CompactionDispatcher::Disconnect() {
  stop_.store(true, std::memory_order_release);
  {
    std::lock_guard<std::mutex> lk(gsend_mu_);
  }
  gsend_cv_.notify_all();
  for (int i = 0; i < kNumSegments; i++) send_cv_[i].notify_all();
  for (int i = 0; i < kNumSegments; i++) output_cv_[i].notify_all();
  for (int i = 0; i < kNumSegments; i++) chunk_ack_cv_[i].notify_all();

  for (int i = 0; i < kNumSegments; i++)
    if (send_threads_[i].joinable()) send_threads_[i].join();
  if (ctrl_recv_thread_.joinable()) ctrl_recv_thread_.join();
  for (int i = 0; i < kNumSegments; i++)
    if (output_recv_threads_[i].joinable()) output_recv_threads_[i].join();

  {
    std::lock_guard<std::mutex> lk(finish_mu_);
    stop_finish_ = true;
  }
  finish_cv_.notify_all();
  for (auto& t : finish_pool_)
    if (t.joinable()) t.join();
  finish_pool_.clear();

  channel_.Close();
}

// ── BuildSpec ────────────────────────────────────────────────────
Status CompactionDispatcher::BuildSpec(
    Compaction* c, ColumnFamilyData* cfd, const ImmutableOptions& iopts,
    const MutableCFOptions& mutable_cf,
    const std::vector<SequenceNumber>& snapshots,
    SequenceNumber ewc_snapshot, SequenceNumber preserve_seqnum,
    CompactionSpec* spec_out) {
  CompactionSpec& s = *spec_out;
  s.job_id = job_id_.fetch_add(1, std::memory_order_relaxed);
  s.output_level = c->output_level();
  s.bottommost = c->bottommost_level();
  s.comparator_name = iopts.user_comparator->Name();
  if (iopts.compaction_filter)
    s.compaction_filter_name = iopts.compaction_filter->Name();

  {
    Slice smallest = c->GetSmallestUserKey();
    if (!smallest.empty()) {
      s.has_start = true;
      s.start_user_key = smallest.ToString();
    }
  }
  {
    Slice largest = c->GetLargestUserKey();
    if (!largest.empty()) {
      s.has_end = true;
      s.end_user_key = largest.ToString();
    }
  }

  for (size_t li = 0; li < c->num_input_levels(); li++) {
    int level = c->level(li);
    for (size_t i = 0; i < c->num_input_files(li); i++) {
      const FileMetaData* fmd = c->input(li, i);
      SSTFileSpec f;
      f.file_number = fmd->fd.GetNumber();
      f.file_size = fmd->fd.GetFileSize();
      f.smallest_seqno = fmd->fd.smallest_seqno;
      f.largest_seqno = fmd->fd.largest_seqno;
      f.smallest_ikey = fmd->smallest.Encode().ToString();
      f.largest_ikey = fmd->largest.Encode().ToString();
      f.level = level;
      s.inputs.push_back(f);
    }
  }

  s.snapshots.list = snapshots;
  s.snapshots.earliest_write_conflict_snapshot = ewc_snapshot;
  s.snapshots.preserve_deletes_seqnum = preserve_seqnum;

  s.output.target_file_size =
      std::min(mutable_cf.target_file_size_base, kChunkDataSize);
  s.output.compression = mutable_cf.compression;
  s.output.compression_opts = mutable_cf.compression_opts;

  {
    static constexpr size_t kGpBudget = kCtrlPayloadMax - (32 << 10);
    static std::atomic<uint64_t> gp_sent{0}, gp_skipped{0};
    s.output.gp_target_file_size =
        std::min<uint64_t>(c->target_output_file_size(), kChunkDataSize);
    s.output.gp_max_file_size =
        std::min<uint64_t>(c->max_output_file_size(), kChunkDataSize);
    s.output.gp_max_compaction_bytes = c->max_compaction_bytes();
    s.output.grandparents.clear();
    size_t gp_bytes = 0;
    for (const FileMetaData* g : c->grandparents()) {
      GPFileSpec gf;
      gf.smallest_ikey = g->smallest.Encode().ToString();
      gf.largest_ikey  = g->largest.Encode().ToString();
      gf.file_size     = g->fd.GetFileSize();
      gp_bytes += gf.smallest_ikey.size() + gf.largest_ikey.size() + 16;
      s.output.grandparents.push_back(std::move(gf));
    }
    if (gp_bytes > kGpBudget) {
      fprintf(stderr, "[rc] gpcut: job=%llu grandparents=%zu (%zu bytes) > budget -> size-only cut (skipped=%llu)\n",
              (unsigned long long)s.job_id, s.output.grandparents.size(), gp_bytes,
              (unsigned long long)(gp_skipped.fetch_add(1) + 1));
      s.output.grandparents.clear();
    } else if (gp_sent.fetch_add(1) == 0) {
      fprintf(stderr, "[rc] gpcut: sending grandparent boundaries to worker (v-gpcut) target=%llu max=%llu max_compaction_bytes=%llu\n",
              (unsigned long long)s.output.gp_target_file_size, (unsigned long long)s.output.gp_max_file_size,
              (unsigned long long)s.output.gp_max_compaction_bytes);
    }
  }

  {
    const Comparator* ucmp = iopts.user_comparator;
    std::string out_smallest, out_largest;
    for (size_t li = 0; li < c->num_input_levels(); li++) {
      for (size_t i = 0; i < c->num_input_files(li); i++) {
        const FileMetaData* fmd = c->input(li, i);
        Slice fsmall = fmd->smallest.user_key();
        Slice flarge = fmd->largest.user_key();
        if (out_smallest.empty() ||
            ucmp->CompareWithoutTimestamp(fsmall, out_smallest) < 0)
          out_smallest = fsmall.ToString();
        if (out_largest.empty() ||
            ucmp->CompareWithoutTimestamp(flarge, out_largest) > 0)
          out_largest = flarge.ToString();
      }
    }
    if (!out_smallest.empty()) {
      c->SetOutputRange(out_smallest, out_largest);
    }
  }

  return Status::OK();
}

static std::string MakeSSTPath(const std::string& db_path,
                                uint64_t file_number) {
  char fname[32];
  snprintf(fname, sizeof(fname), "%06llu.sst",
           (unsigned long long)file_number);
  return db_path + "/" + fname;
}

Status CompactionDispatcher::Dispatch(
    Compaction* c, ColumnFamilyData* cfd, const ImmutableOptions& iopts,
    const MutableCFOptions& mutable_cf,
    const std::vector<SequenceNumber>& snapshots,
    SequenceNumber ewc_snapshot, SequenceNumber preserve_seqnum,
    CompactionResult* result_out,
    std::vector<std::vector<char>>* out_sst_bufs) {
  int fixed_seg = 0;
  CompactionSpec spec;
  Status s = BuildSpec(c, cfd, iopts, mutable_cf, snapshots, ewc_snapshot,
                        preserve_seqnum, &spec);
  if (!s.ok()) return s;

  std::vector<uint64_t> fnums;
  for (const auto& f : spec.inputs) fnums.push_back(f.file_number);
  if (!reservation_.TryReserve(fnums))
    return Status::Busy("input SSTs already reserved");

  s = channel_.SendCtrlMsg(TagMessage(kMsgTypeSpec, spec.Serialize()));
  if (!s.ok()) {
    reservation_.Release(fnums);
    return s;
  }

  for (const auto& f : spec.inputs) {
    std::string path = MakeSSTPath(opts_.db_path, f.file_number);
    s = channel_.StreamSendSST(path, f.file_number, f.file_size, fixed_seg);
    if (!s.ok()) {
      reservation_.Release(fnums);
      return Status::IOError("StreamSendSST failed: " + s.ToString());
    }
  }

  std::string raw;
  s = channel_.RecvCtrlMsg(&raw);
  if (!s.ok()) {
    reservation_.Release(fnums);
    return s;
  }
  if (raw.empty() || raw[0] != kMsgTypeResult) {
    reservation_.Release(fnums);
    return Status::Corruption("Dispatch: unexpected ctrl message tag");
  }
  Slice result_payload(raw.data() + 1, raw.size() - 1);
  if (!CompactionResult::Deserialize(result_payload, result_out)) {
    reservation_.Release(fnums);
    return Status::Corruption("CompactionResult deserialize failed");
  }
  if (!result_out->success) {
    reservation_.Release(fnums);
    return Status::IOError("worker compaction failed: " +
                            result_out->error_msg);
  }

  out_sst_bufs->resize(result_out->output_files.size());
  for (size_t i = 0; i < result_out->output_files.size(); i++) {
    const auto& of = result_out->output_files[i];
    s = channel_.StreamRecvOutputSST(of.file_number, of.file_size,
                                      &(*out_sst_bufs)[i], fixed_seg);
    if (!s.ok()) {
      reservation_.Release(fnums);
      return Status::IOError("StreamRecvOutputSST failed: " + s.ToString());
    }
  }

  return Status::OK();
}

void CompactionDispatcher::ReleaseReservation(
    const std::vector<uint64_t>& fnums) {
  reservation_.Release(fnums);
}

// ── DispatchAsync ─────────────────────────────────────────────────
int CompactionDispatcher::PickLeastBusySegment() { int start = static_cast<int>(next_seg_.fetch_add(1, std::memory_order_relaxed) % kNumSegments); int best = start; size_t best_sz = 0; bool first = true; for (int k = 0; k < kNumSegments; k++) { int i = (start + k) % kNumSegments; std::lock_guard<std::mutex> lk(send_mu_[i]); size_t sz = send_queues_[i].size(); if (first || sz < best_sz) { best_sz = sz; best = i; first = false; } if (sz == 0) break; } return best; }
static bool RcGlobalSendQ() {
  static const bool on = [] {
    const char* e = getenv("RC_GLOBAL_SENDQ");
    bool v = !(e != nullptr && atoi(e) == 0) && getenv("RC_SINGLE_SEG") == nullptr;
    fprintf(stderr, "[rc] send queue mode: %s\n",
            v ? "global (v-gsendq)" : "per-segment");
    return v;
  }();
  return on;
}

static bool RcEnvOn(const char* name) {
  const char* e = getenv(name);
  return e != nullptr && atoi(e) != 0;
}
static bool RcRegWait() {
  static const bool on = [] {
    bool v = RcEnvOn("RC_REG_WAIT");
    fprintf(stderr, "[rc] reg-ack wait: %s\n", v ? "on (v-reg)" : "off (v-nowait)");
    return v;
  }();
  return on;
}
static bool RcOutputAck() {
  static const bool on = [] {
    bool v = RcEnvOn("RC_OUTPUT_ACK");
    fprintf(stderr, "[rc] output-ack: %s\n", v ? "on (v31)" : "off (v-nowait)");
    return v;
  }();
  return on;
}

static bool RcChunkAckWait() {
  static const bool on = [] {
    const char* e = getenv("RC_CHUNK_ACK_WAIT");
    bool v = (e != nullptr && atoi(e) != 0);
    fprintf(stderr, "[rc] chunk-ack wait: %s\n", v ? "on (v22)" : "off (v-noack)");
    return v;
  }();
  return on;
}

static bool RcFileSplit() {
  static const bool on = [] {
    const char* e = getenv("RC_FILE_SPLIT");
    bool v = !(e != nullptr && atoi(e) == 0);
    fprintf(stderr, "[rc] send unit: %s\n", v ? "file (v-filesplit)" : "job");
    return v;
  }();
  return on;
}

static bool RcOutAnySeg() {
  static const bool on = [] {
    const char* e = getenv("RC_OUTPUT_ANYSEG");
    bool v = !(e != nullptr && atoi(e) == 0);
    fprintf(stderr, "[rc] output segment: %s\n", v ? "any free (v-outq)" : "job-fixed");
    return v;
  }();
  return on;
}

Status CompactionDispatcher::DispatchAsync(
    CompactionSpec spec, std::shared_ptr<Compaction> compaction_holder,
    std::function<void(Status, CompactionSpec&&, CompactionResult&&,
                        std::vector<std::vector<char>>&&)>
        on_complete) {
  uint64_t job_id = spec.job_id;

  {
    size_t cur_pending, cur_inflight;
    {
      std::lock_guard<std::mutex> lk(pending_send_mu_);
      cur_pending = pending_sends_.size();
    }
    {
      std::lock_guard<std::mutex> lk(inflight_mu_);
      cur_inflight = in_flight_.size();
    }
    fprintf(stderr,
            "[rc] DispatchAsync: backlog pending=%zu in_flight=%zu "
            "total=%zu job=%llu\n",
            cur_pending, cur_inflight, cur_pending + cur_inflight,
            (unsigned long long)job_id);
  }

  std::vector<uint64_t> fnums;
  for (const auto& f : spec.inputs) fnums.push_back(f.file_number);

  if (!reservation_.TryReserve(fnums)) {
    fprintf(stderr,
            "[rc] DispatchAsync: input SSTs already reserved, job=%llu\n",
            (unsigned long long)job_id);
    return Status::Busy("input SSTs already reserved");
  }

  AsyncJob job;
  job.spec = std::move(spec);
  job.fnums = std::move(fnums);
  job.compaction_holder = std::move(compaction_holder);
  job.on_complete = std::move(on_complete);
  job.t_total_start = std::chrono::steady_clock::now();
  job.enqueue_time = std::chrono::steady_clock::now();

  std::map<int, std::vector<size_t>> by_seg;
  const int job_seg = RcGlobalSendQ()
                          ? -1
                          : (getenv("RC_SINGLE_SEG") ? 0 : PickLeastBusySegment());
  for (size_t i = 0; i < job.spec.inputs.size(); i++) {
    job.spec.inputs[i].assigned_seg = job_seg;
    by_seg[job_seg].push_back(i);
  }
  if (by_seg.empty()) {
    by_seg[job_seg];
  }
  int responsible_seg = by_seg.begin()->first;
  job.spec.seg = responsible_seg;

  auto ps = std::make_shared<PendingSend>();
  ps->remaining_files.store(static_cast<int>(job.spec.inputs.size()),
                             std::memory_order_relaxed);
  ps->responsible_seg = responsible_seg;
  ps->job = std::move(job);

  if (responsible_seg >= 0) {
    std::lock_guard<std::mutex> lk(job_seg_mu_);
    job_seg_map_[job_id] = responsible_seg;
  }
  {
    std::lock_guard<std::mutex> lk(pending_send_mu_);
    pending_sends_[job_id] = ps;
  }

  for (auto& kv : by_seg) {
    int seg = kv.first;
    SendTask task;
    task.job_id = job_id;
    task.file_indices = std::move(kv.second);
    if (seg < 0) {
      if (RcFileSplit() && task.file_indices.size() > 1) {
        {
          std::lock_guard<std::mutex> lk(gsend_mu_);
          for (size_t fi : task.file_indices) {
            SendTask one;
            one.job_id = job_id;
            one.file_indices.push_back(fi);
            gsend_queue_.push_back(std::move(one));
          }
        }
        gsend_cv_.notify_all();
        continue;
      }
      {
        std::lock_guard<std::mutex> lk(gsend_mu_);
        gsend_queue_.push_back(std::move(task));
      }
      gsend_cv_.notify_one();
      continue;
    }
    {
      std::lock_guard<std::mutex> lk(send_mu_[seg]);
      send_queues_[seg].push(std::move(task));
    }
    send_cv_[seg].notify_one();
  }
  return Status::OK();
}

void CompactionDispatcher::SendLoop(int seg_idx) {
  { char rc_tn[16]; snprintf(rc_tn, sizeof(rc_tn), "rc_send%d", seg_idx); pthread_setname_np(pthread_self(), rc_tn); }
  PinCurrentThreadToCore(8 + seg_idx);
  while (!channel_open_.load(std::memory_order_acquire)) {
    if (stop_.load(std::memory_order_acquire)) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  while (true) {
    SendTask task;
    if (RcGlobalSendQ()) {
      std::unique_lock<std::mutex> lk(gsend_mu_);
      gsend_cv_.wait(lk, [this] {
        return stop_.load(std::memory_order_acquire) || !gsend_queue_.empty();
      });
      if (stop_.load(std::memory_order_acquire) && gsend_queue_.empty())
        break;
      task = std::move(gsend_queue_.front());
      gsend_queue_.pop_front();
    } else {
      std::unique_lock<std::mutex> lk(send_mu_[seg_idx]);
      send_cv_[seg_idx].wait(lk, [this, seg_idx] {
        return stop_.load(std::memory_order_acquire) ||
               !send_queues_[seg_idx].empty();
      });
      if (stop_.load(std::memory_order_acquire) &&
          send_queues_[seg_idx].empty())
        break;
      task = std::move(send_queues_[seg_idx].front());
      send_queues_[seg_idx].pop();
    }

    uint64_t job_id = task.job_id;
    std::shared_ptr<PendingSend> ps;
    {
      std::lock_guard<std::mutex> lk(pending_send_mu_);
      auto it = pending_sends_.find(job_id);
      if (it == pending_sends_.end()) {
        fprintf(stderr, "[rc] SendLoop[%d]: unknown job_id=%llu (이미 정리됨?)\n",
                seg_idx, (unsigned long long)job_id);
        continue;
      }
      ps = it->second;
    }

    bool send_spec = false;
    {
      std::lock_guard<std::mutex> blk(pending_send_mu_);
      if (ps->responsible_seg < 0) {
        ps->responsible_seg = seg_idx;
        ps->job.spec.seg = seg_idx;
        for (auto& in : ps->job.spec.inputs) in.assigned_seg = seg_idx;
        send_spec = true;
        std::lock_guard<std::mutex> lk(job_seg_mu_);
        job_seg_map_[job_id] = seg_idx;
      } else if (!RcGlobalSendQ()) {
        send_spec = (seg_idx == ps->responsible_seg);
      }
    }

    auto t1_start = std::chrono::steady_clock::now();
    auto queue_wait_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1_start - ps->job.enqueue_time).count();

    Status s;
    bool seg_failed = false;
    if (send_spec) {
      std::lock_guard<std::mutex> ctrl_lk(ctrl_send_mu_[seg_idx]);
      s = channel_.SendCtrlMsg(TagMessage(kMsgTypeSpec, ps->job.spec.Serialize()));
      if (!s.ok()) {
        fprintf(stderr, "[rc] SendLoop[%d]: SendCtrlMsg failed job=%llu: %s\n",
                seg_idx, (unsigned long long)job_id, s.ToString().c_str());
        seg_failed = true;
        ps->aborted.store(true, std::memory_order_relaxed);
      }
    }

    {
      std::lock_guard<std::mutex> ack_lk(chunk_ack_mu_[seg_idx]);
      chunk_ack_ready_[seg_idx] = false;
      chunk_ack_failed_[seg_idx] = false;
    }

    bool send_ok = !seg_failed;

    if (RcRegWait() && send_ok &&
        !ps->aborted.load(std::memory_order_relaxed)) {
      std::unique_lock<std::mutex> reg_lk(job_registered_mu_);
      bool reg_ok = job_registered_cv_.wait_for(
          reg_lk, std::chrono::seconds(5),
          [this, job_id] {
            return job_registered_.count(job_id) > 0 ||
                   stop_.load(std::memory_order_acquire);
          });
      if (stop_.load(std::memory_order_acquire)) {
        send_ok = false;
      } else if (!reg_ok) {
        fprintf(stderr,
                "[rc] SendLoop[%d]: job=%llu 등록 ack TIMEOUT -- 포기\n",
                seg_idx, (unsigned long long)job_id);
        send_ok = false;
        ps->aborted.store(true, std::memory_order_relaxed);
      }
    }

    for (size_t ti = 0; send_ok && !ps->aborted.load(std::memory_order_relaxed) &&
                        ti < task.file_indices.size(); ti++) {
      size_t fi = task.file_indices[ti];
      const auto& f = ps->job.spec.inputs[fi];
      auto t_file0 = std::chrono::steady_clock::now();
      s = channel_.StreamSendSST(MakeSSTPath(opts_.db_path, f.file_number),
                                  f.file_number, f.file_size, seg_idx,
                                  job_id);
      auto t_file1 = std::chrono::steady_clock::now();
      fprintf(stderr,
          "[timing] rc job=%llu seg=%d file=%llu size=%llu idx=%zu step_file_send=%lldms ok=%d\n",
          (unsigned long long)job_id, seg_idx,
          (unsigned long long)f.file_number, (unsigned long long)f.file_size, fi,
          (long long)std::chrono::duration_cast<std::chrono::milliseconds>(t_file1 - t_file0).count(),
          s.ok() ? 1 : 0);
      if (!s.ok()) {
        fprintf(stderr, "[rc] SendLoop[%d]: StreamSendSST failed: %s\n",
                seg_idx, s.ToString().c_str());
        send_ok = false;
        ps->aborted.store(true, std::memory_order_relaxed);
        break;
      }

      if (RcChunkAckWait()) {
        std::unique_lock<std::mutex> ack_lk(chunk_ack_mu_[seg_idx]);
        uint64_t expect_file = f.file_number;
        bool got = chunk_ack_cv_[seg_idx].wait_for(
            ack_lk, std::chrono::seconds(30),
            [this, seg_idx, expect_file] {
              return (chunk_ack_ready_[seg_idx] &&
                      chunk_ack_file_[seg_idx] == expect_file) ||
                     stop_.load(std::memory_order_acquire);
            });
        if (stop_.load(std::memory_order_acquire)) {
          send_ok = false;
          break;
        }
        if (!got) {
          fprintf(stderr,
                  "[rc] SendLoop[%d]: chunk-ack TIMEOUT job=%llu file=%llu\n",
                  seg_idx, (unsigned long long)job_id,
                  (unsigned long long)f.file_number);
          send_ok = false;
          ps->aborted.store(true, std::memory_order_relaxed);
          break;
        }
        bool failed = chunk_ack_failed_[seg_idx];
        chunk_ack_ready_[seg_idx] = false;
        chunk_ack_failed_[seg_idx] = false;
        if (failed) {
          fprintf(stderr,
                  "[rc] SendLoop[%d]: chunk-ack FAILED (fast) job=%llu "
                  "file=%llu -> abandon job\n",
                  seg_idx, (unsigned long long)job_id,
                  (unsigned long long)f.file_number);
          send_ok = false;
          ps->aborted.store(true, std::memory_order_relaxed);
          break;
        }
      }
    }

    int remaining = ps->remaining_files.fetch_sub(
                        static_cast<int>(task.file_indices.size()),
                        std::memory_order_acq_rel) -
                    static_cast<int>(task.file_indices.size());
    if (remaining > 0) {
      continue;
    }

    auto t1_end = std::chrono::steady_clock::now();
    bool aborted_now = false;
    bool have_early  = false;
    CompactionResult early;
    int early_seg = 0;
    {
      std::lock_guard<std::mutex> lk(pending_send_mu_);
      pending_sends_.erase(job_id);
      aborted_now = ps->aborted.load(std::memory_order_relaxed);
      if (ps->result_ready) {
        have_early = true;
        early      = std::move(ps->early_result);
        early_seg  = ps->early_fixed_seg;
        ps->result_ready = false;
      }
      if (!aborted_now && !have_early) {
        ps->job.t_send_done = t1_end;
        std::lock_guard<std::mutex> ilk(inflight_mu_);
        in_flight_[job_id] = std::move(ps->job);
      }
    }

    if (aborted_now) {
      reservation_.Release(ps->job.fnums);
      if (ps->job.on_complete)
        ps->job.on_complete(Status::IOError("multi-seg StreamSendSST failed"),
                             std::move(ps->job.spec), std::move(ps->job.result),
                             std::move(ps->job.out_bufs));
      if (have_early && early.success && !early.output_files.empty()) {
        PendingOutput po;
        po.job.result = std::move(early);
        po.job.out_bufs.resize(po.job.result.output_files.size());
        po.discard = true;
        if (RcOutAnySeg()) {
          RegisterOutJob(std::move(po.job), /*discard=*/true);
        } else {
          {
            std::lock_guard<std::mutex> olk(output_mu_[early_seg]);
            output_queues_[early_seg].push(std::move(po));
          }
          output_cv_[early_seg].notify_one();
        }
      }
      continue;
    }

    fprintf(stderr, "[timing] rc job=%llu step1_send_sst=%lldms queue_wait=%lldms\n", (unsigned long long)job_id, (long long)std::chrono::duration_cast<std::chrono::milliseconds>(t1_end - t1_start).count(), (long long)queue_wait_ms);
    if (have_early) {
      ps->job.t_send_done = t1_end;
      fprintf(stderr, "[rc] SendLoop[%d]: job=%llu result arrived before send finished -- delivered now (v-nowait)\n",
              seg_idx, (unsigned long long)job_id);
      DeliverResult(std::move(ps->job), std::move(early), early_seg);
    } else {
      std::lock_guard<std::mutex> lk(inflight_mu_);
      fprintf(stderr, "[timing] rc job=%llu in_flight_size=%zu\n", (unsigned long long)job_id, in_flight_.size());
    }
  }
}

// ── CtrlRecvLoop ────────────────────────────────────────────────
void CompactionDispatcher::CtrlRecvLoop() {
  pthread_setname_np(pthread_self(), "rc_ctrl");
  PinCurrentThreadToCore(16);
  while (!channel_open_.load(std::memory_order_acquire)) {
    if (stop_.load(std::memory_order_acquire)) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  // v32: watchdog scan cadence tracker -- checked every loop iteration
  // (cheap clock compare), actual scan only every kWatchdogScanIntervalMs.
  // Deliberately placed at the top of the loop, not inside the !s.ok()
  // branch below, so it still runs even while real messages are flowing
  // constantly -- a busy system is exactly when a worker crash is most
  // likely, so the watchdog can't afford to only run while idle.
  auto last_watchdog_scan = std::chrono::steady_clock::now();
  int idle_spins = 0;

  while (true) {
    {
      auto now = std::chrono::steady_clock::now();
      if (now - last_watchdog_scan >=
          std::chrono::milliseconds(kWatchdogScanIntervalMs)) {
        last_watchdog_scan = now;
        ReapStaleInFlightJobs();
        if (RcOutAnySeg()) ReapStaleOutJobs();
      }
    }

    std::string raw;
    bool got_one = false;
    for (int poll_seg = 0; poll_seg < kNumSegments; poll_seg++) {
      Status s = channel_.TryRecvResultMsg(poll_seg, &raw);
      if (s.ok()) { got_one = true; break; }
    }
    if (!got_one) {
      if (stop_.load(std::memory_order_acquire)) break;
      if (++idle_spins < RcIdleYield()) std::this_thread::yield();
      else std::this_thread::sleep_for(std::chrono::microseconds(RcIdleSleepUs()));
      continue;
    }
    idle_spins = 0;
    if (raw.empty()) {
      continue;
    }

    char tag = raw[0];

    if (tag == kMsgTypeJobRegistered) {
      if (raw.size() < 1 + sizeof(uint64_t)) {
        fprintf(stderr,
                "[rc] CtrlRecvLoop: malformed job-registered ack (size=%zu)\n",
                raw.size());
        continue;
      }
      uint64_t reg_job_id = 0;
      memcpy(&reg_job_id, raw.data() + 1, sizeof(reg_job_id));
      {
        std::lock_guard<std::mutex> lk(job_registered_mu_);
        job_registered_[reg_job_id] = true;
      }
      job_registered_cv_.notify_all();
      continue;
    }

    if (tag == kMsgTypeChunkAck) {
      constexpr size_t kChunkAckMinSize =
          1 /*tag*/ + 1 /*success*/ + sizeof(uint64_t) * 2 + sizeof(uint32_t);
      if (raw.size() < kChunkAckMinSize) {
        fprintf(stderr,
                "[rc] CtrlRecvLoop: malformed chunk-ack (size=%zu)\n",
                raw.size());
        continue;
      }
      uint8_t success_byte = static_cast<uint8_t>(raw[1]);
      uint64_t ack_job_id = 0, ack_file_number = 0;
      memcpy(&ack_job_id, raw.data() + 2, sizeof(ack_job_id));
      memcpy(&ack_file_number, raw.data() + 2 + sizeof(ack_job_id),
             sizeof(ack_file_number));
      uint32_t ack_seg_u32 = 0;
      memcpy(&ack_seg_u32,
             raw.data() + 2 + sizeof(ack_job_id) + sizeof(ack_file_number),
             sizeof(ack_seg_u32));
      bool success = (success_byte != 0);
      int seg = static_cast<int>(ack_seg_u32);
      if (seg < 0 || seg >= kNumSegments) {
        seg = static_cast<int>(ack_job_id % static_cast<uint64_t>(kNumSegments));
      }
      {
        std::lock_guard<std::mutex> lk(chunk_ack_mu_[seg]);
        chunk_ack_ready_[seg] = true;
        chunk_ack_file_[seg] = ack_file_number;
        chunk_ack_failed_[seg] = !success;
      }
      chunk_ack_cv_[seg].notify_one();
      if (!success) {
        fprintf(stderr,
                "[rc] CtrlRecvLoop: chunk-ack FAILED job=%llu file=%llu "
                "seg=%d\n",
                (unsigned long long)ack_job_id,
                (unsigned long long)ack_file_number, seg);
        if (!RcChunkAckWait()) {
          {
            std::lock_guard<std::mutex> plk(pending_send_mu_);
            auto pit = pending_sends_.find(ack_job_id);
            if (pit != pending_sends_.end())
              pit->second->aborted.store(true, std::memory_order_relaxed);
          }
          bool found_inflight = false;
          AsyncJob failed_job;
          {
            std::lock_guard<std::mutex> ilk(inflight_mu_);
            auto iit = in_flight_.find(ack_job_id);
            if (iit != in_flight_.end()) {
              failed_job = std::move(iit->second);
              in_flight_.erase(iit);
              found_inflight = true;
            }
          }
          if (found_inflight) {
            {
              std::lock_guard<std::mutex> jlk(job_seg_mu_);
              job_seg_map_.erase(ack_job_id);
            }
            reservation_.Release(failed_job.fnums);
            {
              std::lock_guard<std::mutex> flk(finish_mu_);
              finish_queue_.push(
                  {Status::IOError("worker reported input file receive failure"),
                   std::move(failed_job)});
            }
            finish_cv_.notify_one();
            fprintf(stderr, "[rc] CtrlRecvLoop: job=%llu failed by chunk-ack FAILED (v-noack)\n",
                    (unsigned long long)ack_job_id);
          }
        }
      }
      continue;
    }

    if (tag != kMsgTypeResult) {
      fprintf(stderr,
              "[rc] CtrlRecvLoop: unknown ctrl tag=0x%02x (size=%zu), skip\n",
              (unsigned char)tag, raw.size());
      continue;
    }

    Slice result_payload(raw.data() + 1, raw.size() - 1);
    CompactionResult result;
    if (!CompactionResult::Deserialize(result_payload, &result)) {
      fprintf(stderr, "[rc] CtrlRecvLoop: deserialize failed\n");
      continue;
    }

    uint64_t job_id = result.job_id;
    int fixed_seg = 0; { std::lock_guard<std::mutex> lk(job_seg_mu_); auto sit = job_seg_map_.find(job_id); if (sit != job_seg_map_.end()) { fixed_seg = sit->second; job_seg_map_.erase(sit); } else { fixed_seg = static_cast<int>(job_id % static_cast<uint64_t>(kNumSegments)); } }
    { std::lock_guard<std::mutex> lk(job_registered_mu_); job_registered_.erase(job_id); }

    AsyncJob job;
    bool have_job = false;
    bool parked   = false;
    {
      std::lock_guard<std::mutex> plk(pending_send_mu_);
      auto pit = pending_sends_.find(job_id);
      if (pit != pending_sends_.end() &&
          !pit->second->aborted.load(std::memory_order_relaxed)) {
        pit->second->early_result    = std::move(result);
        pit->second->early_fixed_seg = fixed_seg;
        pit->second->result_ready    = true;
        parked = true;
      } else {
        std::lock_guard<std::mutex> ilk(inflight_mu_);
        auto it = in_flight_.find(job_id);
        if (it != in_flight_.end()) {
          job = std::move(it->second);
          in_flight_.erase(it);
          have_job = true;
        }
      }
    }
    if (parked) {
      fprintf(stderr, "[rc] CtrlRecvLoop: job=%llu result parked (inputs still sending)\n",
              (unsigned long long)job_id);
      continue;
    }
    if (!have_job) {
      fprintf(stderr, "[rc] CtrlRecvLoop: unknown job_id=%llu (outputs=%zu, drain)\n",
              (unsigned long long)job_id, result.output_files.size());
      if (result.success && !result.output_files.empty()) {
        PendingOutput po;
        po.job.result = std::move(result);
        po.job.out_bufs.resize(po.job.result.output_files.size());
        po.discard = true;
        if (RcOutAnySeg()) {
          RegisterOutJob(std::move(po.job), /*discard=*/true);
        } else {
          {
            std::lock_guard<std::mutex> olk(output_mu_[fixed_seg]);
            output_queues_[fixed_seg].push(std::move(po));
          }
          output_cv_[fixed_seg].notify_one();
        }
      }
      continue;
    }

    DeliverResult(std::move(job), std::move(result), fixed_seg);
  }

  for (int i = 0; i < kNumSegments; i++) output_cv_[i].notify_all();
}

void CompactionDispatcher::DeliverResult(AsyncJob&& job_in,
                                         CompactionResult&& result,
                                         int fixed_seg) {
  AsyncJob job = std::move(job_in);
  uint64_t job_id = result.job_id;
  if (!result.success) {
    fprintf(stderr, "[rc] CtrlRecvLoop: worker failed job=%llu: %s\n",
            (unsigned long long)job_id, result.error_msg.c_str());
    reservation_.Release(job.fnums);
    {
      std::lock_guard<std::mutex> lk(finish_mu_);
      finish_queue_.push(
          {Status::IOError("worker failed: " + result.error_msg),
           std::move(job)});
    }
    finish_cv_.notify_one();
    return;
  }

  fprintf(stderr, "[timing] rc job=%llu step2_worker_roundtrip=%lldms\n", (unsigned long long)job_id, (long long)std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - job.t_send_done).count());
  job.result = std::move(result);
  job.out_bufs.resize(job.result.output_files.size());
  if (RcOutAnySeg()) {
    RegisterOutJob(std::move(job), /*discard=*/false);
    return;
  }
  {
    std::lock_guard<std::mutex> lk(output_mu_[fixed_seg]);
    output_queues_[fixed_seg].push({std::move(job)});
  }
  output_cv_[fixed_seg].notify_one();
}

// ── ReapStaleInFlightJobs (new, v32) ────────────────────────────────
// Scans in_flight_ for jobs that have been waiting past
// kJobResultTimeoutMs since t_send_done (i.e. since SendLoop finished
// pushing all of that job's input files) for a CompactionResult that
// never arrived -- worker crashed, hung, or the NTB link died after the
// inputs went out. Force-fails each one exactly the way the normal
// !result.success path in CtrlRecvLoop already does: release the
// reservation, drop the job_seg_map_ entry, push a TimedOut FinishJob
// for FinishWorkerLoop to pick up.
//
// Without this, such a job sat in in_flight_ forever: its input files'
// reservation never released, its manifest edit never written.
void CompactionDispatcher::ReapStaleInFlightJobs() {
  auto now = std::chrono::steady_clock::now();
  std::vector<std::pair<uint64_t, AsyncJob>> stale;

  {
    std::lock_guard<std::mutex> lk(inflight_mu_);
    for (auto it = in_flight_.begin(); it != in_flight_.end(); ) {
      auto age_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
          now - it->second.t_send_done).count();
      if (age_ms > kJobResultTimeoutMs) {
        fprintf(stderr,
               "[rc] CtrlRecvLoop watchdog: job=%llu waited %lldms for a "
               "CompactionResult that never arrived (limit=%dms) -- "
               "force-failing it\n",
               (unsigned long long)it->first, (long long)age_ms,
               kJobResultTimeoutMs);
        stale.emplace_back(it->first, std::move(it->second));
        it = in_flight_.erase(it);
      } else {
        ++it;
      }
    }
  }
  if (stale.empty()) return;

  for (auto& kv : stale) {
    uint64_t job_id = kv.first;
    AsyncJob& job    = kv.second;

    {
      std::lock_guard<std::mutex> lk(job_seg_mu_);
      job_seg_map_.erase(job_id);
    }
    reservation_.Release(job.fnums);

    std::lock_guard<std::mutex> lk(finish_mu_);
    finish_queue_.push(
        {Status::TimedOut("CtrlRecvLoop watchdog: no CompactionResult "
                          "within timeout, worker presumed dead/stuck"),
         std::move(job)});
  }
  finish_cv_.notify_all();
}

// ── OutputRecvLoop ─────────────────────────────────────────────────
void CompactionDispatcher::OutputRecvLoop(int seg_idx) {
  { char rc_tn[16]; snprintf(rc_tn, sizeof(rc_tn), "rc_out%d", seg_idx); pthread_setname_np(pthread_self(), rc_tn); }
  PinCurrentThreadToCore(17 + seg_idx);
  while (!channel_open_.load(std::memory_order_acquire)) {
    if (stop_.load(std::memory_order_acquire)) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  if (RcOutAnySeg()) { OutputRecvLoopAny(seg_idx); return; }

  while (true) {
    PendingOutput pending;
    {
      std::unique_lock<std::mutex> lk(output_mu_[seg_idx]);
      output_cv_[seg_idx].wait(lk, [this, seg_idx] {
        return stop_.load(std::memory_order_acquire) ||
               !output_queues_[seg_idx].empty();
      });
      if (stop_.load(std::memory_order_acquire) &&
          output_queues_[seg_idx].empty())
        break;
      pending = std::move(output_queues_[seg_idx].front());
      output_queues_[seg_idx].pop();
    }

    AsyncJob& job = pending.job;
    uint64_t job_id = job.result.job_id;
    fprintf(stderr, "[timing] rc job=%llu output_queue_wait=%lldms\n", (unsigned long long)job_id, (long long)std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - pending.enqueue_time).count());
    bool recv_ok = true;
    auto t3_start = std::chrono::steady_clock::now();

    for (size_t i = 0; i < job.result.output_files.size(); i++) {
      const auto& of = job.result.output_files[i];
      auto t_of0 = std::chrono::steady_clock::now();
      Status s = channel_.StreamRecvOutputSST(of.file_number, of.file_size,
                                               &job.out_bufs[i], seg_idx,
                                               job_id);
      auto t_of1 = std::chrono::steady_clock::now();
      fprintf(stderr,
          "[timing] rc job=%llu seg=%d merge_file=%llu size=%llu idx=%zu step_file_recv=%lldms ok=%d\n",
          (unsigned long long)job_id, seg_idx,
          (unsigned long long)of.file_number, (unsigned long long)of.file_size, i,
          (long long)std::chrono::duration_cast<std::chrono::milliseconds>(t_of1 - t_of0).count(),
          s.ok() ? 1 : 0);
      if (!s.ok()) {
        fprintf(stderr,
                "[rc] OutputRecvLoop[%d]: StreamRecvOutputSST failed "
                "job=%llu: %s\n",
                seg_idx, (unsigned long long)job_id, s.ToString().c_str());
        if (RcOutputAck()) {
          std::lock_guard<std::mutex> ctrl_lk(ctrl_send_mu_[seg_idx]);
          Status as = channel_.SendSpecMsg(MakeOutputAckPayload(
              job_id, of.file_number, /*success=*/false));
          fprintf(stderr,
                  "[rc] OutputRecvLoop[%d]: sent FAILED output-ack "
                  "job=%llu file=%llu status=%s\n",
                  seg_idx, (unsigned long long)job_id,
                  (unsigned long long)of.file_number, as.ToString().c_str());
        }
        recv_ok = false;
        break;
      }

      {
        uint32_t actual_crc =
            NtbDiagCrc32(job.out_bufs[i].data(), job.out_bufs[i].size());
        if (actual_crc != of.content_crc32) {
          fprintf(stderr,
                  "[CRC_MISMATCH] job=%llu file=%llu seg=%d "
                  "expected_crc=%u actual_crc=%u size=%zu\n",
                  (unsigned long long)job_id,
                  (unsigned long long)of.file_number, seg_idx,
                  of.content_crc32, actual_crc, job.out_bufs[i].size());
          if (RcOutputAck()) {
            std::lock_guard<std::mutex> ctrl_lk(ctrl_send_mu_[seg_idx]);
            Status as = channel_.SendSpecMsg(MakeOutputAckPayload(
                job_id, of.file_number, /*success=*/false));
            fprintf(stderr,
                    "[rc] OutputRecvLoop[%d]: sent FAILED(CRC) output-ack "
                    "job=%llu file=%llu status=%s\n",
                    seg_idx, (unsigned long long)job_id,
                    (unsigned long long)of.file_number, as.ToString().c_str());
          }
          recv_ok = false;
          break;
        }
      }

      if (RcOutputAck()) {
        std::lock_guard<std::mutex> ctrl_lk(ctrl_send_mu_[seg_idx]);
        channel_.SendSpecMsg(MakeOutputAckPayload(job_id, of.file_number,
                                                   /*success=*/true));
      }
    }

    fprintf(stderr, "[timing] rc job=%llu step3_recv_sst=%lldms worker_compact=%lldms\n", (unsigned long long)job_id, (long long)std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t3_start).count(), (long long)(job.result.elapsed_us / 1000));
    if (pending.discard) {
      fprintf(stderr, "[rc] OutputRecvLoop[%d]: drained outputs of abandoned job=%llu ok=%d\n",
              seg_idx, (unsigned long long)job_id, recv_ok ? 1 : 0);
      continue;
    }
    Status finish_status =
        recv_ok ? Status::OK()
                : Status::IOError("StreamRecvOutputSST failed");
    if (!recv_ok) reservation_.Release(job.fnums);

    {
      std::lock_guard<std::mutex> lk(finish_mu_);
      finish_queue_.push({finish_status, std::move(pending.job)});
      fprintf(stderr, "[timing] rc job=%llu finish_queue_depth=%zu\n", (unsigned long long)job_id, finish_queue_.size());
    }
    finish_cv_.notify_one();
  }
}


constexpr size_t kRcStreamMaxFiles = 4096;
struct RcStreamJob {
  std::mutex mu;
  std::condition_variable cv;
  int pending = 0;
  bool failed = false;
  std::map<uint64_t, std::string> tmp;
};
struct RcStreamTask {
  std::shared_ptr<RcStreamJob> sj;
  uint64_t job_id = 0, wfnum = 0;
  std::string path;
  const char* data = nullptr;
  size_t size = 0;
};
static std::mutex g_rs_mu;
static std::map<uint64_t, std::shared_ptr<RcStreamJob>> g_rs_jobs;
static std::mutex& g_rs_qmu = *new std::mutex;
static std::condition_variable& g_rs_qcv = *new std::condition_variable;
static std::queue<RcStreamTask>& g_rs_q = *new std::queue<RcStreamTask>;
static std::once_flag g_rs_once;

int RcStreamMode() {
  static const int v = [] {
    const char* e = getenv("RC_STREAM_PERSIST");
    int x = e ? atoi(e) : 0;
    if (x < 0 || x > 2) x = 0;
    fprintf(stderr, "[rc] v-stream persist mode: %d (0=off 1=on-arrival 2=before-result)\n", x);
    return x;
  }();
  return v;
}
static int RcStreamThreads() {
  static const int v = [] {
    const char* e = getenv("RC_STREAM_PERSIST_THREADS");
    int x = e ? atoi(e) : 4;
    if (x < 1) x = 1;
    if (x > 64) x = 64;
    fprintf(stderr, "[rc] v-stream persist threads: %d\n", x);
    return x;
  }();
  return v;
}
static std::shared_ptr<RcStreamJob> RcStreamGet(uint64_t job_id, bool create) {
  std::lock_guard<std::mutex> lk(g_rs_mu);
  auto it = g_rs_jobs.find(job_id);
  if (it != g_rs_jobs.end()) return it->second;
  if (!create) return nullptr;
  auto sj = std::make_shared<RcStreamJob>();
  g_rs_jobs[job_id] = sj;
  return sj;
}
static void RcStreamStart() {
  std::call_once(g_rs_once, [] {
    for (int i = 0; i < RcStreamThreads(); i++) {
      std::thread([i] {
        char tn[16]; snprintf(tn, sizeof(tn), "rc_sp%d", i); pthread_setname_np(pthread_self(), tn);
        while (true) {
          RcStreamTask t;
          {
            std::unique_lock<std::mutex> lk(g_rs_qmu);
            g_rs_qcv.wait(lk, [] { return !g_rs_q.empty(); });
            t = std::move(g_rs_q.front());
            g_rs_q.pop();
          }
          auto t0 = std::chrono::steady_clock::now();
          int err = ::RcStreamWriteFileExt(t.path, t.data, t.size);
          if (err != 0) ::remove(t.path.c_str());
          fprintf(stderr, "[timing] rc stream_persist job=%llu wfile=%llu bytes=%zu ms=%lld err=%d\n",
                  (unsigned long long)t.job_id, (unsigned long long)t.wfnum, t.size,
                  (long long)std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - t0).count(), err);
          {
            std::lock_guard<std::mutex> lk(t.sj->mu);
            if (err != 0) t.sj->failed = true;
            t.sj->pending--;
          }
          t.sj->cv.notify_all();
        }
      }).detach();
    }
  });
}
static void RcStreamSubmit(uint64_t job_id, uint64_t wfnum, const std::string& db_path,
                           const char* data, size_t size) {
  RcStreamStart();
  auto sj = RcStreamGet(job_id, true);
  char nm[80];
  snprintf(nm, sizeof(nm), "/rcs_%llu_%llu.tmp", (unsigned long long)job_id, (unsigned long long)wfnum);
  RcStreamTask t;
  t.sj = sj; t.job_id = job_id; t.wfnum = wfnum; t.path = db_path + nm; t.data = data; t.size = size;
  {
    std::lock_guard<std::mutex> lk(sj->mu);
    sj->pending++;
    sj->tmp[wfnum] = t.path;
  }
  {
    std::lock_guard<std::mutex> lk(g_rs_qmu);
    g_rs_q.push(std::move(t));
  }
  g_rs_qcv.notify_one();
}
static void RcStreamWait(uint64_t job_id) {
  auto sj = RcStreamGet(job_id, false);
  if (!sj) return;
  std::unique_lock<std::mutex> lk(sj->mu);
  sj->cv.wait(lk, [&] { return sj->pending == 0; });
}
static void RcStreamAbort(uint64_t job_id) {
  auto sj = RcStreamGet(job_id, false);
  if (!sj) return;
  RcStreamWait(job_id);
  {
    std::lock_guard<std::mutex> lk(sj->mu);
    for (auto& kv : sj->tmp) ::remove(kv.second.c_str());
    sj->tmp.clear();
  }
  std::lock_guard<std::mutex> lk(g_rs_mu);
  g_rs_jobs.erase(job_id);
}
Status RcStreamFinalize(uint64_t job_id, const CompactionResult& wres,
                        const CompactionResult& fixed, const std::string& db_path) {
  auto t0 = std::chrono::steady_clock::now();
  const size_t n = fixed.output_files.size();
  auto sj = RcStreamGet(job_id, false);
  if (!sj) return n == 0 ? Status::OK() : Status::Corruption("v-stream: no streamed outputs for job");
  RcStreamWait(job_id);
  Status s;
  std::vector<std::string> renamed;
  {
    std::lock_guard<std::mutex> lk(sj->mu);
    if (sj->failed) s = Status::IOError("v-stream: streamed persist failed");
    for (size_t i = 0; i < n && s.ok(); i++) {
      if (i >= wres.output_files.size()) { s = Status::Corruption("v-stream: result size mismatch"); break; }
      auto it = sj->tmp.find(wres.output_files[i].file_number);
      if (it == sj->tmp.end()) { s = Status::Corruption("v-stream: missing streamed file"); break; }
      char fname[32];
      snprintf(fname, sizeof(fname), "%06llu.sst", (unsigned long long)fixed.output_files[i].file_number);
      const std::string dst = db_path + "/" + fname;
      if (::rename(it->second.c_str(), dst.c_str()) != 0) {
        s = Status::IOError("v-stream: rename failed " + it->second + " -> " + dst + ": " + strerror(errno));
        break;
      }
      renamed.push_back(dst);
      sj->tmp.erase(it);
    }
  }
  if (!s.ok()) for (const auto& p : renamed) ::remove(p.c_str());
  RcStreamAbort(job_id);
  fprintf(stderr, "[timing] rc job=%llu stream_finalize=%lldms files=%zu ok=%d\n",
          (unsigned long long)job_id,
          (long long)std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::steady_clock::now() - t0).count(), n, s.ok() ? 1 : 0);
  return s;
}

void CompactionDispatcher::RetireOutJobLocked(uint64_t job_id) {
  out_retired_[job_id] = true;
  out_retired_q_.push(job_id);
  while (out_retired_q_.size() > 4096) {
    out_retired_.erase(out_retired_q_.front());
    out_retired_q_.pop();
  }
}

void CompactionDispatcher::RegisterOutJob(AsyncJob&& job, bool discard) {

  if (RcStreamMode() == 2) {
    const uint64_t jid = job.result.job_id;
    std::shared_ptr<OutJob> fin;
    bool merged = false;
    {
      std::lock_guard<std::mutex> lk(out_jobs_mu_);
      auto it = out_jobs_.find(jid);
      if (it != out_jobs_.end() && it->second->provisional) {
        auto oj = it->second;
        merged = true;
        auto saved = std::move(oj->job.out_bufs);
        oj->job = std::move(job);
        oj->job.out_bufs = std::move(saved);
        oj->provisional = false;
        oj->discard = discard;
        const auto& ofs = oj->job.result.output_files;
        const size_t n = ofs.size();
        oj->order.assign(n, 0);
        oj->crc_expect.resize(oj->job.out_bufs.size(), 0);
        size_t done_cnt = 0;
        for (size_t i = 0; i < n; i++) {
          auto f = oj->idx_by_fnum.find(ofs[i].file_number);
          size_t a;
          if (f == oj->idx_by_fnum.end()) {
            if (oj->job.out_bufs.size() >= kRcStreamMaxFiles) { oj->failed = true; continue; }
            a = oj->job.out_bufs.size();
            oj->job.out_bufs.emplace_back();
            oj->done.push_back(false);
            oj->t_file.push_back(oj->t_reg);
            oj->crc_actual.push_back(0);
            oj->crc_expect.push_back(0);
            oj->idx_by_fnum[ofs[i].file_number] = a;
          } else {
            a = f->second;
          }
          oj->order[i] = a;
          oj->crc_expect[a] = ofs[i].content_crc32;
          if (oj->done[a]) {
            done_cnt++;
            if (oj->crc_actual[a] != ofs[i].content_crc32) {
              oj->failed = true;
              fprintf(stderr, "[CRC_MISMATCH] job=%llu file=%llu expected_crc=%u actual_crc=%u (v-stream early)\n",
                      (unsigned long long)jid, (unsigned long long)ofs[i].file_number,
                      ofs[i].content_crc32, oj->crc_actual[a]);
            }
          }
        }
        if (oj->idx_by_fnum.size() != n) {
          oj->failed = true;
          fprintf(stderr, "[rc] v-stream: job=%llu got %zu files but result lists %zu -- fail\n",
                  (unsigned long long)jid, oj->idx_by_fnum.size(), n);
        }
        oj->files_left = n - done_cnt;
        if (oj->files_left == 0 && oj->busy == 0) {
          out_jobs_.erase(it);
          RetireOutJobLocked(jid);
          fin = oj;
        }
      }
    }
    if (merged) {
      if (fin) CompleteOutJob(jid, fin);
      return;
    }
  }
  auto oj = std::make_shared<OutJob>();
  oj->job     = std::move(job);
  oj->discard = discard;
  oj->t_reg   = oj->t_last = std::chrono::steady_clock::now();
  const size_t n = oj->job.result.output_files.size();
  oj->job.out_bufs.resize(n);
  oj->done.assign(n, false);
  oj->t_file.assign(n, oj->t_reg);
  for (size_t i = 0; i < n; i++) {
    const auto& of = oj->job.result.output_files[i];
    oj->idx_by_fnum[of.file_number] = i;
    if (!discard) oj->job.out_bufs[i].resize(of.file_size);
  }
  oj->files_left = n;
  const uint64_t job_id = oj->job.result.job_id;
  if (n == 0) {
    CompleteOutJob(job_id, oj);
    return;
  }
  std::lock_guard<std::mutex> lk(out_jobs_mu_);
  out_jobs_[job_id] = oj;
}

void CompactionDispatcher::CompleteOutJob(uint64_t job_id, std::shared_ptr<OutJob> oj) {
  auto now = std::chrono::steady_clock::now();
  auto ms = [](std::chrono::steady_clock::duration d) {
    return (long long)std::chrono::duration_cast<std::chrono::milliseconds>(d).count();
  };
  fprintf(stderr, "[timing] rc job=%llu output_queue_wait=%lldms\n",
          (unsigned long long)job_id, oj->seen ? ms(oj->t_first - oj->t_reg) : 0LL);
  fprintf(stderr, "[timing] rc job=%llu step3_recv_sst=%lldms worker_compact=%lldms\n",
          (unsigned long long)job_id, ms(now - (oj->seen ? oj->t_first : oj->t_reg)),
          (long long)(oj->job.result.elapsed_us / 1000));
  if (oj->discard) {
    fprintf(stderr, "[rc] OutputRecvLoopAny: drained outputs of abandoned job=%llu failed=%d\n",
            (unsigned long long)job_id, oj->failed ? 1 : 0);
    if (RcStreamMode()) RcStreamAbort(job_id);
    return;
  }
  if (!oj->order.empty()) {
    std::vector<std::vector<char>> r(oj->order.size());
    for (size_t i = 0; i < oj->order.size(); i++) r[i] = std::move(oj->job.out_bufs[oj->order[i]]);
    oj->job.out_bufs = std::move(r);
  }
  Status st = oj->failed ? Status::IOError("output receive failed (v-outq)") : Status::OK();
  if (oj->failed) reservation_.Release(oj->job.fnums);
  {
    std::lock_guard<std::mutex> lk(finish_mu_);
    finish_queue_.push({st, std::move(oj->job)});
    fprintf(stderr, "[timing] rc job=%llu finish_queue_depth=%zu\n",
            (unsigned long long)job_id, finish_queue_.size());
  }
  finish_cv_.notify_one();
}

void CompactionDispatcher::OutputRecvLoopAny(int seg_idx) {
  { char rc_tn[16]; snprintf(rc_tn, sizeof(rc_tn), "rc_outA%d", seg_idx); pthread_setname_np(pthread_self(), rc_tn); }
  int idle = 0;
  uint64_t wait_job = 0;
  std::chrono::steady_clock::time_point wait_since;
  auto backoff = [&idle]() {
    if (++idle < RcIdleYield()) std::this_thread::yield();
    else std::this_thread::sleep_for(std::chrono::microseconds(RcIdleSleepUs()));
  };

  while (!stop_.load(std::memory_order_acquire)) {
    uint64_t hf = 0, hoff = 0, hbytes = 0, hjob = 0;
    bool hlast = false;
    if (!channel_.OutputTryPeekChunk(seg_idx, &hf, &hoff, &hbytes, &hlast, &hjob)) {
      backoff();
      continue;
    }

    std::shared_ptr<OutJob> oj;
    size_t idx = 0;
    bool retired = false, bad_file = false;
    {
      std::lock_guard<std::mutex> lk(out_jobs_mu_);
      auto it = out_jobs_.find(hjob);
      if (it == out_jobs_.end()) {
        retired = out_retired_.count(hjob) > 0;
        if (!retired && hoff == 0 && RcStreamMode() == 2) {
          auto p = std::make_shared<OutJob>();
          p->provisional = true;
          p->job.spec.job_id = hjob;
          p->job.result.job_id = hjob;
          p->job.out_bufs.reserve(kRcStreamMaxFiles);
          p->t_reg = p->t_last = std::chrono::steady_clock::now();
          p->files_left = static_cast<size_t>(1) << 40;
          out_jobs_[hjob] = p;
          it = out_jobs_.find(hjob);
        }
      }
      if (it != out_jobs_.end()) {
        oj = it->second;
        auto fit = oj->idx_by_fnum.find(hf);
        if (fit == oj->idx_by_fnum.end() && oj->provisional && hoff == 0 &&
            oj->job.out_bufs.size() < kRcStreamMaxFiles) {
          const size_t a = oj->job.out_bufs.size();
          oj->job.out_bufs.emplace_back();
          oj->done.push_back(false);
          oj->t_file.push_back(std::chrono::steady_clock::now());
          oj->crc_actual.push_back(0);
          fit = oj->idx_by_fnum.emplace(hf, a).first;
        }
        if (fit == oj->idx_by_fnum.end() || oj->done[fit->second]) {
          bad_file = true;
          oj->failed = true;
        } else {
          idx = fit->second;
          oj->busy++;
          auto now = std::chrono::steady_clock::now();
          if (!oj->seen) { oj->seen = true; oj->t_first = now; }
          if (hoff == 0) oj->t_file[idx] = now;
        }
      }
    }

    if (!oj && !retired) {
      auto now = std::chrono::steady_clock::now();
      if (wait_job != hjob) {
        wait_job = hjob;
        wait_since = now;
      } else if (now - wait_since > std::chrono::milliseconds(kJobResultTimeoutMs)) {
        fprintf(stderr,
                "[OUTPUT_ORPHAN] seg=%d hdr_job=%llu hdr_file=%llu -- no result "
                "registered within %dms, drained\n",
                seg_idx, (unsigned long long)hjob, (unsigned long long)hf,
                kJobResultTimeoutMs);
        channel_.OutputConsumeChunk(seg_idx, nullptr, 0);
        wait_job = 0;
        continue;
      }
      backoff();
      continue;
    }
    idle = 0;
    wait_job = 0;

    if (!oj || bad_file) {
      fprintf(stderr,
              "[OUTPUT_STALE] seg=%d hdr_job=%llu hdr_file=%llu off=%llu bytes=%llu "
              "(%s) -- drained\n",
              seg_idx, (unsigned long long)hjob, (unsigned long long)hf,
              (unsigned long long)hoff, (unsigned long long)hbytes,
              bad_file ? "unknown file of live job" : "finished job");
      channel_.OutputConsumeChunk(seg_idx, nullptr, 0);
      continue;
    }

    bool crc_ok = true;
    uint32_t s_crc = 0;
    bool s_crc_pending = false;
    if (oj->discard) {
      channel_.OutputConsumeChunk(seg_idx, nullptr, 0);
    } else {
      std::vector<char>& buf = oj->job.out_bufs[idx];
      if (hoff + hbytes > buf.size()) buf.resize(hoff + hbytes);
      channel_.OutputConsumeChunk(seg_idx, buf.data() + hoff, hbytes);
      if (hlast) {
        buf.resize(hoff + hbytes);
        uint32_t actual_crc = NtbDiagCrc32(buf.data(), buf.size());
        if (RcStreamMode() == 2) { s_crc = actual_crc; s_crc_pending = true; }
        else if (actual_crc != oj->job.result.output_files[idx].content_crc32) {
          crc_ok = false;
          fprintf(stderr,
                  "[CRC_MISMATCH] job=%llu file=%llu seg=%d expected_crc=%u "
                  "actual_crc=%u size=%zu\n",
                  (unsigned long long)hjob, (unsigned long long)hf, seg_idx,
                  oj->job.result.output_files[idx].content_crc32, actual_crc, buf.size());
        }
        fprintf(stderr,
                "[timing] rc job=%llu seg=%d merge_file=%llu size=%llu idx=%zu "
                "step_file_recv=%lldms ok=%d\n",
                (unsigned long long)hjob, seg_idx, (unsigned long long)hf,
                (unsigned long long)buf.size(), idx,
                (long long)std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - oj->t_file[idx]).count(),
                crc_ok ? 1 : 0);
        if (RcStreamMode() && crc_ok) RcStreamSubmit(hjob, hf, opts_.db_path, buf.data(), buf.size());
      }
    }

    std::shared_ptr<OutJob> fin;
    {
      std::lock_guard<std::mutex> lk(out_jobs_mu_);
      oj->busy--;
      oj->t_last = std::chrono::steady_clock::now();
      if (hlast) {
        oj->done[idx] = true;
        if (oj->files_left > 0) oj->files_left--;
        if (!crc_ok) oj->failed = true;
        if (s_crc_pending) {
          if (oj->provisional) {
            oj->crc_actual[idx] = s_crc;
          } else {
            const uint32_t e = oj->crc_expect.empty() ? oj->job.result.output_files[idx].content_crc32 : oj->crc_expect[idx];
            if (s_crc != e) {
              oj->failed = true;
              fprintf(stderr, "[CRC_MISMATCH] job=%llu file=%llu seg=%d expected_crc=%u actual_crc=%u (v-stream)\n",
                      (unsigned long long)hjob, (unsigned long long)hf, seg_idx, e, s_crc);
            }
          }
        }
      }
      if (oj->files_left == 0 && oj->busy == 0) {
        auto it = out_jobs_.find(hjob);
        if (it != out_jobs_.end() && it->second == oj) {
          out_jobs_.erase(it);
          RetireOutJobLocked(hjob);
          fin = oj;
        }
      }
    }
    if (fin) CompleteOutJob(hjob, fin);
  }
}

void CompactionDispatcher::ReapStaleOutJobs() {
  auto now = std::chrono::steady_clock::now();
  std::vector<std::pair<uint64_t, std::shared_ptr<OutJob>>> stale;
  {
    std::lock_guard<std::mutex> lk(out_jobs_mu_);
    for (auto it = out_jobs_.begin(); it != out_jobs_.end();) {
      auto& oj = it->second;
      if (oj->busy == 0 &&
          now - oj->t_last > std::chrono::milliseconds(kJobResultTimeoutMs)) {
        fprintf(stderr,
                "[rc] output watchdog: job=%llu no output progress for %dms "
                "(files_left=%zu) -- force-failing (v-outq)\n",
                (unsigned long long)it->first, kJobResultTimeoutMs, oj->files_left);
        oj->failed = true;
        RetireOutJobLocked(it->first);
        stale.emplace_back(it->first, oj);
        it = out_jobs_.erase(it);
      } else {
        ++it;
      }
    }
  }
  for (auto& kv : stale) CompleteOutJob(kv.first, kv.second);
}

// ── FinishWorkerLoop ───────────────────────────────────────────────
void CompactionDispatcher::FinishWorkerLoop(int idx) {
  { char rc_tn[16]; snprintf(rc_tn, sizeof(rc_tn), "rc_fin%d", idx); pthread_setname_np(pthread_self(), rc_tn); }
  int core = 25 + idx; if (core >= 52) core += 25; PinCurrentThreadToCore(core);
  while (true) {
    FinishJob fjob;
    {
      std::unique_lock<std::mutex> lk(finish_mu_);
      finish_cv_.wait(
          lk, [this] { return stop_finish_ || !finish_queue_.empty(); });
      if (stop_finish_ && finish_queue_.empty()) break;
      fjob = std::move(finish_queue_.front());
      finish_queue_.pop();
    }

    AsyncJob& job = fjob.job;
    const uint64_t rs_jid = job.spec.job_id;
    if (RcStreamMode()) RcStreamWait(rs_jid);
    fprintf(stderr, "[timing] rc job=%llu finish_queue_wait=%lldms\n", (unsigned long long)job.spec.job_id, (long long)std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - fjob.enqueue_time).count());
    if (!fjob.status.ok()) {
      if (job.on_complete)
        job.on_complete(fjob.status, std::move(job.spec),
                         std::move(job.result), std::move(job.out_bufs));
      if (RcStreamMode()) RcStreamAbort(rs_jid);
      continue;
    }

    if (job.on_complete)
      job.on_complete(Status::OK(), std::move(job.spec),
                       std::move(job.result), std::move(job.out_bufs));
  }
}

Status CompactionDispatcher::ExecuteJobSync(AsyncJob& job) {
  (void)job;
  return Status::NotSupported("ExecuteJobSync not implemented");
}

}  // namespace ROCKSDB_NAMESPACE
