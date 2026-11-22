// CompactionDispatcher: see compaction_dispatcher.h for the thread layout.
#include "compaction_dispatcher.h"

#include <pthread.h>

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>

#include "db/column_family.h"
#include "db/compaction/compaction.h"
#include "rocksdb/comparator.h"
#include "rocksdb/options.h"

// Defined in remote_compaction_job.cc.
int RcStreamWriteFileExt(const std::string& dst, const char* data, size_t size);

namespace ROCKSDB_NAMESPACE {

namespace {

using Clock = std::chrono::steady_clock;

long long Ms(Clock::duration d) {
  return static_cast<long long>(
      std::chrono::duration_cast<std::chrono::milliseconds>(d).count());
}

// Control message tags. Must match worker_server.cc.
constexpr char kMsgTypeSpec = 0x01;          // primary -> worker
constexpr char kMsgTypeResult = 0x02;        // worker -> primary
constexpr char kMsgTypeChunkAck = 0x03;      // worker -> primary (receive status)
constexpr char kMsgTypeJobRegistered = 0x04; // worker -> primary (ignored)

// Idle polling: yield a few times, then sleep briefly.
constexpr unsigned kIdleYields = 16;
constexpr unsigned kIdleSleepUs = 200;

std::string TagMessage(char tag, const std::string& payload) {
  std::string out;
  out.reserve(payload.size() + 1);
  out.push_back(tag);
  out.append(payload);
  return out;
}

std::string MakeSSTPath(const std::string& db_path, uint64_t file_number) {
  char fname[32];
  snprintf(fname, sizeof(fname), "%06llu.sst", (unsigned long long)file_number);
  return db_path + "/" + fname;
}

void SetThreadName(const char* prefix, int idx) {
  std::string name = prefix;
  if (idx >= 0) name += std::to_string(idx);
  pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
}

// ---------------------------------------------------------------------------
// Streamed persist of output SSTs.
//
// Each output file is written to <db>/rcs_<job>_<worker file number>.tmp by a
// single persist thread as soon as it has been received. FinishJob renames
// the temporary files to their final numbers (RcStreamFinalize). Failed or
// abandoned jobs delete them (RcStreamAbort).
// ---------------------------------------------------------------------------
constexpr size_t kMaxFilesPerJob = 4096;

struct StreamJob {
  std::mutex mu;
  std::condition_variable cv;
  int pending = 0;
  bool failed = false;
  std::map<uint64_t, std::string> tmp;  // worker file number -> temporary path
};

struct StreamTask {
  std::shared_ptr<StreamJob> sj;
  uint64_t job_id = 0, wfnum = 0;
  std::string path;
  const char* data = nullptr;  // owned by the job's out_bufs until finished
  size_t size = 0;
};

std::mutex g_stream_mu;
std::map<uint64_t, std::shared_ptr<StreamJob>> g_stream_jobs;
// The persist thread is detached; its queue is intentionally leaked so that
// no destructor runs while it may still be waiting at process exit.
std::mutex& g_sq_mu = *new std::mutex;
std::condition_variable& g_sq_cv = *new std::condition_variable;
std::queue<StreamTask>& g_sq = *new std::queue<StreamTask>;
std::once_flag g_stream_once;

std::shared_ptr<StreamJob> StreamGet(uint64_t job_id, bool create) {
  std::lock_guard<std::mutex> lk(g_stream_mu);
  auto it = g_stream_jobs.find(job_id);
  if (it != g_stream_jobs.end()) return it->second;
  if (!create) return nullptr;
  auto sj = std::make_shared<StreamJob>();
  g_stream_jobs[job_id] = sj;
  return sj;
}

void StreamPersistLoop() {
  SetThreadName("rc_sp", 0);
  while (true) {
    StreamTask t;
    {
      std::unique_lock<std::mutex> lk(g_sq_mu);
      g_sq_cv.wait(lk, [] { return !g_sq.empty(); });
      t = std::move(g_sq.front());
      g_sq.pop();
    }
    auto t0 = Clock::now();
    int err = ::RcStreamWriteFileExt(t.path, t.data, t.size);
    if (err != 0) ::remove(t.path.c_str());
    fprintf(stderr,
            "[timing] rc stream_persist job=%llu wfile=%llu bytes=%zu ms=%lld err=%d\n",
            (unsigned long long)t.job_id, (unsigned long long)t.wfnum, t.size,
            Ms(Clock::now() - t0), err);
    {
      std::lock_guard<std::mutex> lk(t.sj->mu);
      if (err != 0) t.sj->failed = true;
      t.sj->pending--;
    }
    t.sj->cv.notify_all();
  }
}

void RcStreamSubmit(uint64_t job_id, uint64_t wfnum, const std::string& db_path,
                    const char* data, size_t size) {
  std::call_once(g_stream_once, [] { std::thread(StreamPersistLoop).detach(); });
  auto sj = StreamGet(job_id, true);
  char nm[80];
  snprintf(nm, sizeof(nm), "/rcs_%llu_%llu.tmp", (unsigned long long)job_id,
           (unsigned long long)wfnum);
  StreamTask t;
  t.sj = sj;
  t.job_id = job_id;
  t.wfnum = wfnum;
  t.path = db_path + nm;
  t.data = data;
  t.size = size;
  {
    std::lock_guard<std::mutex> lk(sj->mu);
    sj->pending++;
    sj->tmp[wfnum] = t.path;
  }
  {
    std::lock_guard<std::mutex> lk(g_sq_mu);
    g_sq.push(std::move(t));
  }
  g_sq_cv.notify_one();
}

void RcStreamWait(uint64_t job_id) {
  auto sj = StreamGet(job_id, false);
  if (!sj) return;
  std::unique_lock<std::mutex> lk(sj->mu);
  sj->cv.wait(lk, [&] { return sj->pending == 0; });
}

// Waits for pending writes, deletes leftover temporary files, drops the entry.
void RcStreamAbort(uint64_t job_id) {
  auto sj = StreamGet(job_id, false);
  if (!sj) return;
  RcStreamWait(job_id);
  {
    std::lock_guard<std::mutex> lk(sj->mu);
    for (auto& kv : sj->tmp) ::remove(kv.second.c_str());
    sj->tmp.clear();
  }
  std::lock_guard<std::mutex> lk(g_stream_mu);
  g_stream_jobs.erase(job_id);
}

}  // namespace

Status RcStreamFinalize(uint64_t job_id, const CompactionResult& wres,
                        const CompactionResult& fixed, const std::string& db_path) {
  auto t0 = Clock::now();
  const size_t n = fixed.output_files.size();
  auto sj = StreamGet(job_id, false);
  if (!sj) {
    return n == 0 ? Status::OK()
                  : Status::Corruption("stream persist: no outputs for job");
  }
  RcStreamWait(job_id);
  Status s;
  std::vector<std::string> renamed;
  {
    std::lock_guard<std::mutex> lk(sj->mu);
    if (sj->failed) s = Status::IOError("stream persist: write failed");
    for (size_t i = 0; i < n && s.ok(); i++) {
      if (i >= wres.output_files.size()) {
        s = Status::Corruption("stream persist: result size mismatch");
        break;
      }
      auto it = sj->tmp.find(wres.output_files[i].file_number);
      if (it == sj->tmp.end()) {
        s = Status::Corruption("stream persist: missing output file");
        break;
      }
      const std::string dst = MakeSSTPath(db_path, fixed.output_files[i].file_number);
      if (::rename(it->second.c_str(), dst.c_str()) != 0) {
        s = Status::IOError("stream persist: rename failed " + it->second +
                            " -> " + dst + ": " + strerror(errno));
        break;
      }
      renamed.push_back(dst);
      sj->tmp.erase(it);
    }
  }
  if (!s.ok()) {
    for (const auto& p : renamed) ::remove(p.c_str());
  }
  RcStreamAbort(job_id);
  fprintf(stderr, "[timing] rc job=%llu stream_finalize=%lldms files=%zu ok=%d\n",
          (unsigned long long)job_id, Ms(Clock::now() - t0), n, s.ok() ? 1 : 0);
  return s;
}

// ── SSTReservationTable ────────────────────────────────────────────────────
bool SSTReservationTable::TryReserve(const std::vector<uint64_t>& fnums) {
  std::lock_guard<std::mutex> lk(mu_);
  for (uint64_t fn : fnums) {
    if (reserved_.count(fn)) return false;
  }
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

// ── Lifecycle ──────────────────────────────────────────────────────────────
CompactionDispatcher::CompactionDispatcher(const DispatchOptions& opts)
    : opts_(opts) {}

CompactionDispatcher::~CompactionDispatcher() { Disconnect(); }

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
    send_threads_[i] = std::thread([this, i]() { SendLoop(i); });
  }
  ctrl_recv_thread_ = std::thread([this]() { CtrlRecvLoop(); });
  for (int i = 0; i < kNumSegments; i++) {
    output_recv_threads_[i] = std::thread([this, i]() { OutputRecvLoop(i); });
  }
  for (int i = 0; i < kFinishThreads; i++) {
    finish_pool_.emplace_back([this, i]() { FinishWorkerLoop(i); });
  }
  channel_open_.store(true, std::memory_order_release);
  return s;
}

void CompactionDispatcher::Disconnect() {
  stop_.store(true, std::memory_order_release);
  {
    std::lock_guard<std::mutex> lk(gsend_mu_);
  }
  gsend_cv_.notify_all();
  for (int i = 0; i < kNumSegments; i++) {
    if (send_threads_[i].joinable()) send_threads_[i].join();
  }
  if (ctrl_recv_thread_.joinable()) ctrl_recv_thread_.join();
  for (int i = 0; i < kNumSegments; i++) {
    if (output_recv_threads_[i].joinable()) output_recv_threads_[i].join();
  }
  {
    std::lock_guard<std::mutex> lk(finish_mu_);
    stop_finish_ = true;
  }
  finish_cv_.notify_all();
  for (auto& t : finish_pool_) {
    if (t.joinable()) t.join();
  }
  finish_pool_.clear();
  channel_.Close();
}

// ── BuildSpec ──────────────────────────────────────────────────────────────
Status CompactionDispatcher::BuildSpec(
    Compaction* c, ColumnFamilyData* /*cfd*/, const ImmutableOptions& iopts,
    const MutableCFOptions& mutable_cf, const std::vector<SequenceNumber>& snapshots,
    SequenceNumber ewc_snapshot, SequenceNumber preserve_seqnum,
    CompactionSpec* spec_out) {
  CompactionSpec& s = *spec_out;
  s.job_id = job_id_.fetch_add(1, std::memory_order_relaxed);
  s.output_level = c->output_level();
  s.bottommost = c->bottommost_level();
  s.comparator_name = iopts.user_comparator->Name();
  if (iopts.compaction_filter) {
    s.compaction_filter_name = iopts.compaction_filter->Name();
  }
  Slice smallest = c->GetSmallestUserKey();
  if (!smallest.empty()) {
    s.has_start = true;
    s.start_user_key = smallest.ToString();
  }
  Slice largest = c->GetLargestUserKey();
  if (!largest.empty()) {
    s.has_end = true;
    s.end_user_key = largest.ToString();
  }

  for (size_t li = 0; li < c->num_input_levels(); li++) {
    const int level = c->level(li);
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

  // Grandparent boundaries and size limits, so that the worker cuts output
  // files exactly like RocksDB's CompactionOutputs. If they do not fit in the
  // control mailbox, the job falls back to size-only cuts.
  {
    static constexpr size_t kGpBudget = kCtrlPayloadMax - (32 << 10);
    static std::atomic<uint64_t> gp_skipped{0};
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
      gf.largest_ikey = g->largest.Encode().ToString();
      gf.file_size = g->fd.GetFileSize();
      gp_bytes += gf.smallest_ikey.size() + gf.largest_ikey.size() + 16;
      s.output.grandparents.push_back(std::move(gf));
    }
    if (gp_bytes > kGpBudget) {
      fprintf(stderr,
              "[rc] job=%llu grandparents=%zu (%zu bytes) exceed the control "
              "message budget, size-only cuts (skipped=%llu)\n",
              (unsigned long long)s.job_id, s.output.grandparents.size(), gp_bytes,
              (unsigned long long)(gp_skipped.fetch_add(1) + 1));
      s.output.grandparents.clear();
    }
  }

  // Register the union of the input key ranges so that
  // RangeOverlapWithCompaction sees this job while it is in flight.
  const Comparator* ucmp = iopts.user_comparator;
  std::string out_smallest, out_largest;
  for (size_t li = 0; li < c->num_input_levels(); li++) {
    for (size_t i = 0; i < c->num_input_files(li); i++) {
      const FileMetaData* fmd = c->input(li, i);
      Slice lo = fmd->smallest.user_key();
      Slice hi = fmd->largest.user_key();
      if (out_smallest.empty() || ucmp->CompareWithoutTimestamp(lo, out_smallest) < 0) {
        out_smallest = lo.ToString();
      }
      if (out_largest.empty() || ucmp->CompareWithoutTimestamp(hi, out_largest) > 0) {
        out_largest = hi.ToString();
      }
    }
  }
  if (!out_smallest.empty()) c->SetOutputRange(out_smallest, out_largest);
  return Status::OK();
}

void CompactionDispatcher::ReleaseReservation(const std::vector<uint64_t>& fnums) {
  reservation_.Release(fnums);
}

// ── DispatchAsync ──────────────────────────────────────────────────────────
// Splits the job into one send task per input file and puts them on the
// global queue. Whichever SendLoop pops a file sends it through its own
// segment, so a job's inputs move in parallel across idle segments.
Status CompactionDispatcher::DispatchAsync(
    CompactionSpec spec, std::shared_ptr<Compaction> compaction_holder,
    DispatchCallback on_complete) {
  const uint64_t job_id = spec.job_id;
  {
    size_t pending, inflight;
    {
      std::lock_guard<std::mutex> lk(pending_send_mu_);
      pending = pending_sends_.size();
    }
    {
      std::lock_guard<std::mutex> lk(inflight_mu_);
      inflight = in_flight_.size();
    }
    fprintf(stderr,
            "[rc] DispatchAsync: backlog pending=%zu in_flight=%zu total=%zu job=%llu\n",
            pending, inflight, pending + inflight, (unsigned long long)job_id);
  }

  std::vector<uint64_t> fnums;
  for (const auto& f : spec.inputs) fnums.push_back(f.file_number);
  if (!reservation_.TryReserve(fnums)) {
    fprintf(stderr, "[rc] DispatchAsync: input SSTs already reserved, job=%llu\n",
            (unsigned long long)job_id);
    return Status::Busy("input SSTs already reserved");
  }

  AsyncJob job;
  job.spec = std::move(spec);
  job.fnums = std::move(fnums);
  job.compaction_holder = std::move(compaction_holder);
  job.on_complete = std::move(on_complete);
  job.t_total_start = Clock::now();
  job.enqueue_time = job.t_total_start;
  // The segment is chosen by the SendLoop that sends the spec.
  job.spec.seg = -1;
  for (auto& in : job.spec.inputs) in.assigned_seg = -1;

  const size_t num_inputs = job.spec.inputs.size();
  auto ps = std::make_shared<PendingSend>();
  ps->remaining_files.store(static_cast<int>(num_inputs), std::memory_order_relaxed);
  ps->job = std::move(job);
  {
    std::lock_guard<std::mutex> lk(pending_send_mu_);
    pending_sends_[job_id] = ps;
  }

  {
    std::lock_guard<std::mutex> lk(gsend_mu_);
    if (num_inputs <= 1) {
      SendTask task;  // also covers a job without inputs: the spec still goes out
      task.job_id = job_id;
      if (num_inputs == 1) task.file_indices.push_back(0);
      gsend_queue_.push_back(std::move(task));
    } else {
      for (size_t i = 0; i < num_inputs; i++) {
        SendTask one;
        one.job_id = job_id;
        one.file_indices.push_back(i);
        gsend_queue_.push_back(std::move(one));
      }
    }
  }
  if (num_inputs <= 1) {
    gsend_cv_.notify_one();
  } else {
    gsend_cv_.notify_all();
  }
  return Status::OK();
}

// ── SendLoop ───────────────────────────────────────────────────────────────
void CompactionDispatcher::SendLoop(int seg_idx) {
  SetThreadName("rc_send", seg_idx);
  while (!channel_open_.load(std::memory_order_acquire)) {
    if (stop_.load(std::memory_order_acquire)) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  while (true) {
    SendTask task;
    {
      std::unique_lock<std::mutex> lk(gsend_mu_);
      gsend_cv_.wait(lk, [this] {
        return stop_.load(std::memory_order_acquire) || !gsend_queue_.empty();
      });
      if (stop_.load(std::memory_order_acquire) && gsend_queue_.empty()) break;
      task = std::move(gsend_queue_.front());
      gsend_queue_.pop_front();
    }

    const uint64_t job_id = task.job_id;
    std::shared_ptr<PendingSend> ps;
    bool send_spec = false;
    {
      std::lock_guard<std::mutex> lk(pending_send_mu_);
      auto it = pending_sends_.find(job_id);
      if (it == pending_sends_.end()) {
        fprintf(stderr, "[rc] SendLoop[%d]: unknown job_id=%llu\n", seg_idx,
                (unsigned long long)job_id);
        continue;
      }
      ps = it->second;
      // The first SendLoop that touches the job binds it to its segment and
      // sends the spec; the worker finds each input by its file number.
      if (ps->responsible_seg < 0) {
        ps->responsible_seg = seg_idx;
        ps->job.spec.seg = seg_idx;
        for (auto& in : ps->job.spec.inputs) in.assigned_seg = seg_idx;
        send_spec = true;
      }
    }

    auto t1_start = Clock::now();
    const long long queue_wait_ms = Ms(t1_start - ps->job.enqueue_time);
    bool send_ok = true;
    if (send_spec) {
      std::lock_guard<std::mutex> ctrl_lk(ctrl_send_mu_[seg_idx]);
      Status s = channel_.SendCtrlMsg(TagMessage(kMsgTypeSpec, ps->job.spec.Serialize()));
      if (!s.ok()) {
        fprintf(stderr, "[rc] SendLoop[%d]: SendCtrlMsg failed job=%llu: %s\n",
                seg_idx, (unsigned long long)job_id, s.ToString().c_str());
        send_ok = false;
        ps->aborted.store(true, std::memory_order_relaxed);
      }
    }

    for (size_t fi : task.file_indices) {
      if (!send_ok || ps->aborted.load(std::memory_order_relaxed)) break;
      const auto& f = ps->job.spec.inputs[fi];
      auto t_file = Clock::now();
      Status s = channel_.StreamSendSST(MakeSSTPath(opts_.db_path, f.file_number),
                                        f.file_number, f.file_size, seg_idx, job_id);
      fprintf(stderr,
              "[timing] rc job=%llu seg=%d file=%llu size=%llu idx=%zu "
              "step_file_send=%lldms ok=%d\n",
              (unsigned long long)job_id, seg_idx, (unsigned long long)f.file_number,
              (unsigned long long)f.file_size, fi, Ms(Clock::now() - t_file),
              s.ok() ? 1 : 0);
      if (!s.ok()) {
        fprintf(stderr, "[rc] SendLoop[%d]: StreamSendSST failed: %s\n", seg_idx,
                s.ToString().c_str());
        send_ok = false;
        ps->aborted.store(true, std::memory_order_relaxed);
      }
    }

    const int n = static_cast<int>(task.file_indices.size());
    if (ps->remaining_files.fetch_sub(n, std::memory_order_acq_rel) - n > 0) {
      continue;  // another SendLoop finishes this job
    }

    // Last sender: move the job from pending_sends_ to in_flight_ under one
    // lock, so a result arriving at any moment finds it in one of the two.
    auto t1_end = Clock::now();
    bool aborted = false, have_early = false;
    CompactionResult early;
    {
      std::lock_guard<std::mutex> lk(pending_send_mu_);
      pending_sends_.erase(job_id);
      aborted = ps->aborted.load(std::memory_order_relaxed);
      if (ps->result_ready) {
        have_early = true;
        early = std::move(ps->early_result);
        ps->result_ready = false;
      }
      if (!aborted && !have_early) {
        ps->job.t_send_done = t1_end;
        std::lock_guard<std::mutex> ilk(inflight_mu_);
        in_flight_[job_id] = std::move(ps->job);
      }
    }

    if (aborted) {
      reservation_.Release(ps->job.fnums);
      if (ps->job.on_complete) {
        ps->job.on_complete(Status::IOError("sending inputs failed"),
                            std::move(ps->job.spec), std::move(ps->job.result),
                            std::move(ps->job.out_bufs));
      }
      if (have_early && early.success && !early.output_files.empty()) {
        // The worker is already sending outputs: receive and drop them so
        // that the segments do not stay occupied.
        AsyncJob drop;
        drop.result = std::move(early);
        RegisterOutJob(std::move(drop), /*discard=*/true);
      }
      continue;
    }

    fprintf(stderr, "[timing] rc job=%llu step1_send_sst=%lldms queue_wait=%lldms\n",
            (unsigned long long)job_id, Ms(t1_end - t1_start), queue_wait_ms);
    if (have_early) {
      ps->job.t_send_done = t1_end;
      DeliverResult(std::move(ps->job), std::move(early));
    } else {
      std::lock_guard<std::mutex> lk(inflight_mu_);
      fprintf(stderr, "[timing] rc job=%llu in_flight_size=%zu\n",
              (unsigned long long)job_id, in_flight_.size());
    }
  }
}

// ── CtrlRecvLoop ───────────────────────────────────────────────────────────
void CompactionDispatcher::CtrlRecvLoop() {
  SetThreadName("rc_ctrl", -1);
  while (!channel_open_.load(std::memory_order_acquire)) {
    if (stop_.load(std::memory_order_acquire)) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  auto last_scan = Clock::now();
  unsigned idle = 0;
  while (true) {
    // Watchdogs run on a timer, also while messages keep flowing.
    if (Clock::now() - last_scan >= std::chrono::milliseconds(kWatchdogScanIntervalMs)) {
      last_scan = Clock::now();
      ReapStaleInFlightJobs();
      ReapStaleOutJobs();
    }

    std::string raw;
    bool got = false;
    for (int seg = 0; seg < kNumSegments && !got; seg++) {
      got = channel_.TryRecvResultMsg(seg, &raw).ok();
    }
    if (!got) {
      if (stop_.load(std::memory_order_acquire)) break;
      if (++idle < kIdleYields) {
        std::this_thread::yield();
      } else {
        std::this_thread::sleep_for(std::chrono::microseconds(kIdleSleepUs));
      }
      continue;
    }
    idle = 0;
    if (raw.empty()) continue;
    const char tag = raw[0];

    if (tag == kMsgTypeJobRegistered) continue;  // not used

    if (tag == kMsgTypeChunkAck) {
      // tag(1) + success(1) + job_id(8) + file_number(8) + seg(4)
      constexpr size_t kSize = 1 + 1 + 8 + 8 + 4;
      if (raw.size() < kSize) {
        fprintf(stderr, "[rc] CtrlRecvLoop: malformed chunk-ack (size=%zu)\n",
                raw.size());
        continue;
      }
      const bool success = static_cast<uint8_t>(raw[1]) != 0;
      if (success) continue;  // flow control is done inside the channel
      uint64_t ack_job = 0, ack_file = 0;
      memcpy(&ack_job, raw.data() + 2, 8);
      memcpy(&ack_file, raw.data() + 10, 8);
      fprintf(stderr, "[rc] CtrlRecvLoop: input receive FAILED job=%llu file=%llu\n",
              (unsigned long long)ack_job, (unsigned long long)ack_file);
      {
        std::lock_guard<std::mutex> lk(pending_send_mu_);
        auto it = pending_sends_.find(ack_job);
        if (it != pending_sends_.end()) {
          it->second->aborted.store(true, std::memory_order_relaxed);
        }
      }
      FailInFlightJob(ack_job, Status::IOError("worker reported input receive failure"));
      continue;
    }

    if (tag != kMsgTypeResult) {
      fprintf(stderr, "[rc] CtrlRecvLoop: unknown ctrl tag=0x%02x (size=%zu)\n",
              (unsigned char)tag, raw.size());
      continue;
    }
    CompactionResult result;
    if (!CompactionResult::Deserialize(Slice(raw.data() + 1, raw.size() - 1), &result)) {
      fprintf(stderr, "[rc] CtrlRecvLoop: deserialize failed\n");
      continue;
    }

    const uint64_t job_id = result.job_id;
    AsyncJob job;
    bool have_job = false, parked = false;
    {
      std::lock_guard<std::mutex> plk(pending_send_mu_);
      auto pit = pending_sends_.find(job_id);
      if (pit != pending_sends_.end() &&
          !pit->second->aborted.load(std::memory_order_relaxed)) {
        // Inputs are still being sent; the last SendLoop delivers it.
        pit->second->early_result = std::move(result);
        pit->second->result_ready = true;
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
      // Abandoned or reaped job: its outputs are still drained.
      fprintf(stderr, "[rc] CtrlRecvLoop: unknown job_id=%llu (outputs=%zu, drain)\n",
              (unsigned long long)job_id, result.output_files.size());
      if (result.success && !result.output_files.empty()) {
        AsyncJob drop;
        drop.result = std::move(result);
        RegisterOutJob(std::move(drop), /*discard=*/true);
      }
      continue;
    }
    DeliverResult(std::move(job), std::move(result));
  }
}

void CompactionDispatcher::DeliverResult(AsyncJob&& job_in, CompactionResult&& result) {
  AsyncJob job = std::move(job_in);
  if (!result.success) {
    fprintf(stderr, "[rc] worker failed job=%llu: %s\n",
            (unsigned long long)result.job_id, result.error_msg.c_str());
    reservation_.Release(job.fnums);
    {
      std::lock_guard<std::mutex> lk(finish_mu_);
      finish_queue_.push_back({Status::IOError("worker failed: " + result.error_msg),
                               std::move(job)});
    }
    finish_cv_.notify_one();
    return;
  }
  fprintf(stderr, "[timing] rc job=%llu step2_worker_roundtrip=%lldms\n",
          (unsigned long long)result.job_id, Ms(Clock::now() - job.t_send_done));
  job.result = std::move(result);
  RegisterOutJob(std::move(job), /*discard=*/false);
}

void CompactionDispatcher::FailInFlightJob(uint64_t job_id, const Status& why) {
  AsyncJob job;
  {
    std::lock_guard<std::mutex> lk(inflight_mu_);
    auto it = in_flight_.find(job_id);
    if (it == in_flight_.end()) return;
    job = std::move(it->second);
    in_flight_.erase(it);
  }
  reservation_.Release(job.fnums);
  {
    std::lock_guard<std::mutex> lk(finish_mu_);
    finish_queue_.push_back({why, std::move(job)});
  }
  finish_cv_.notify_one();
  fprintf(stderr, "[rc] job=%llu failed: %s\n", (unsigned long long)job_id,
          why.ToString().c_str());
}

// A job whose result never arrives (worker died or hung after receiving its
// inputs) would otherwise keep its input reservation forever.
void CompactionDispatcher::ReapStaleInFlightJobs() {
  const auto now = Clock::now();
  std::vector<AsyncJob> stale;
  {
    std::lock_guard<std::mutex> lk(inflight_mu_);
    for (auto it = in_flight_.begin(); it != in_flight_.end();) {
      const long long age_ms = Ms(now - it->second.t_send_done);
      if (age_ms > kJobResultTimeoutMs) {
        fprintf(stderr,
                "[rc] watchdog: job=%llu no result for %lldms (limit %dms), failing\n",
                (unsigned long long)it->first, age_ms, kJobResultTimeoutMs);
        stale.push_back(std::move(it->second));
        it = in_flight_.erase(it);
      } else {
        ++it;
      }
    }
  }
  if (stale.empty()) return;
  {
    std::lock_guard<std::mutex> lk(finish_mu_);
    for (auto& job : stale) {
      reservation_.Release(job.fnums);
      finish_queue_.push_back(
          {Status::TimedOut("no compaction result from worker"), std::move(job)});
    }
  }
  finish_cv_.notify_all();
}

// ── Output receive ─────────────────────────────────────────────────────────
void CompactionDispatcher::RetireOutJobLocked(uint64_t job_id) {
  out_retired_[job_id] = true;
  out_retired_q_.push(job_id);
  while (out_retired_q_.size() > 4096) {
    out_retired_.erase(out_retired_q_.front());
    out_retired_q_.pop();
  }
}

void CompactionDispatcher::RegisterOutJob(AsyncJob&& job, bool discard) {
  const uint64_t jid = job.result.job_id;
  // Merge with a provisional entry created by outputs that beat the result.
  {
    std::shared_ptr<OutJob> fin;
    bool merged = false;
    {
      std::lock_guard<std::mutex> lk(out_jobs_mu_);
      auto it = out_jobs_.find(jid);
      if (it != out_jobs_.end() && it->second->provisional) {
        auto oj = it->second;
        merged = true;
        // Keep the buffer vector itself: other threads may hold pointers
        // into its elements.
        auto saved = std::move(oj->job.out_bufs);
        oj->job = std::move(job);
        oj->job.out_bufs = std::move(saved);
        oj->provisional = false;
        oj->discard = discard;
        const auto& ofs = oj->job.result.output_files;
        const size_t n = ofs.size();
        oj->order.assign(n, 0);
        size_t done_cnt = 0;
        for (size_t i = 0; i < n; i++) {
          auto f = oj->idx_by_fnum.find(ofs[i].file_number);
          size_t a;
          if (f == oj->idx_by_fnum.end()) {
            if (oj->job.out_bufs.size() >= kMaxFilesPerJob) {
              oj->failed = true;
              continue;
            }
            a = oj->job.out_bufs.size();
            oj->job.out_bufs.emplace_back();
            oj->done.push_back(false);
            oj->t_file.push_back(oj->t_reg);
            oj->idx_by_fnum[ofs[i].file_number] = a;
          } else {
            a = f->second;
          }
          oj->order[i] = a;
          if (oj->done[a]) done_cnt++;
        }
        if (oj->idx_by_fnum.size() != n) {
          oj->failed = true;
          fprintf(stderr, "[rc] job=%llu received %zu output files, result lists %zu\n",
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
  oj->job = std::move(job);
  oj->discard = discard;
  oj->t_reg = oj->t_last = Clock::now();
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
  if (n == 0) {
    CompleteOutJob(jid, oj);
    return;
  }
  std::unique_lock<std::mutex> lk(out_jobs_mu_);
  // A first output chunk may have created a provisional entry after the merge
  // check above; never overwrite it (that file would be lost).
  auto pit = out_jobs_.find(jid);
  if (pit != out_jobs_.end() && pit->second->provisional) {
    lk.unlock();
    RegisterOutJob(std::move(oj->job), discard);
    return;
  }
  out_jobs_[jid] = oj;
}

// Hands a job whose outputs are complete (or failed) to the install thread.
void CompactionDispatcher::CompleteOutJob(uint64_t job_id, std::shared_ptr<OutJob> oj) {
  const auto now = Clock::now();
  fprintf(stderr, "[timing] rc job=%llu output_queue_wait=%lldms\n",
          (unsigned long long)job_id, oj->seen ? Ms(oj->t_first - oj->t_reg) : 0LL);
  fprintf(stderr, "[timing] rc job=%llu step3_recv_sst=%lldms worker_compact=%lldms\n",
          (unsigned long long)job_id, Ms(now - (oj->seen ? oj->t_first : oj->t_reg)),
          (long long)(oj->job.result.elapsed_us / 1000));
  if (oj->discard) {
    fprintf(stderr, "[rc] drained outputs of abandoned job=%llu failed=%d\n",
            (unsigned long long)job_id, oj->failed ? 1 : 0);
    RcStreamAbort(job_id);
    return;
  }
  if (!oj->order.empty()) {  // arrival order -> result order
    std::vector<std::vector<char>> r(oj->order.size());
    for (size_t i = 0; i < oj->order.size(); i++) {
      r[i] = std::move(oj->job.out_bufs[oj->order[i]]);
    }
    oj->job.out_bufs = std::move(r);
  }
  Status st = oj->failed ? Status::IOError("output receive failed") : Status::OK();
  if (oj->failed) reservation_.Release(oj->job.fnums);
  {
    std::lock_guard<std::mutex> lk(finish_mu_);
    finish_queue_.push_back({st, std::move(oj->job)});
    fprintf(stderr, "[timing] rc job=%llu finish_queue_depth=%zu\n",
            (unsigned long long)job_id, finish_queue_.size());
  }
  finish_cv_.notify_one();
}

// The worker sends each output file through any free segment. This loop
// serves one segment: it peeks the chunk header, finds the owning job by
// (job_id, file_number), copies the chunk out (which frees the segment) and
// submits each completed file for persisting.
void CompactionDispatcher::OutputRecvLoop(int seg_idx) {
  SetThreadName("rc_outA", seg_idx);
  while (!channel_open_.load(std::memory_order_acquire)) {
    if (stop_.load(std::memory_order_acquire)) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  unsigned idle = 0;
  uint64_t wait_job = 0;
  Clock::time_point wait_since;
  auto backoff = [&idle]() {
    if (++idle < kIdleYields) {
      std::this_thread::yield();
    } else {
      std::this_thread::sleep_for(std::chrono::microseconds(kIdleSleepUs));
    }
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
        if (!retired && hoff == 0) {
          // Output before result: open a provisional entry.
          auto p = std::make_shared<OutJob>();
          p->provisional = true;
          p->job.spec.job_id = hjob;
          p->job.result.job_id = hjob;
          p->job.out_bufs.reserve(kMaxFilesPerJob);  // never reallocated
          p->t_reg = p->t_last = Clock::now();
          p->files_left = static_cast<size_t>(1) << 40;
          it = out_jobs_.emplace(hjob, p).first;
        }
      }
      if (it != out_jobs_.end()) {
        oj = it->second;
        auto fit = oj->idx_by_fnum.find(hf);
        if (fit == oj->idx_by_fnum.end() && oj->provisional && hoff == 0 &&
            oj->job.out_bufs.size() < kMaxFilesPerJob) {
          const size_t a = oj->job.out_bufs.size();
          oj->job.out_bufs.emplace_back();
          oj->done.push_back(false);
          oj->t_file.push_back(Clock::now());
          fit = oj->idx_by_fnum.emplace(hf, a).first;
        }
        if (fit == oj->idx_by_fnum.end() || oj->done[fit->second]) {
          bad_file = true;  // unknown or duplicate file of a live job
          oj->failed = true;
        } else {
          idx = fit->second;
          oj->busy++;
          const auto now = Clock::now();
          if (!oj->seen) {
            oj->seen = true;
            oj->t_first = now;
          }
          if (hoff == 0) oj->t_file[idx] = now;
        }
      }
    }

    if (!oj && !retired) {
      // Middle chunk of a job that is not registered: leave it for now, but
      // drain it if it stays orphaned for the watchdog limit.
      const auto now = Clock::now();
      if (wait_job != hjob) {
        wait_job = hjob;
        wait_since = now;
      } else if (now - wait_since > std::chrono::milliseconds(kJobResultTimeoutMs)) {
        fprintf(stderr, "[rc] seg=%d orphan output chunk job=%llu file=%llu, drained\n",
                seg_idx, (unsigned long long)hjob, (unsigned long long)hf);
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
              "[rc] seg=%d stale output chunk job=%llu file=%llu off=%llu bytes=%llu "
              "(%s), drained\n",
              seg_idx, (unsigned long long)hjob, (unsigned long long)hf,
              (unsigned long long)hoff, (unsigned long long)hbytes,
              bad_file ? "unknown file of live job" : "finished job");
      channel_.OutputConsumeChunk(seg_idx, nullptr, 0);
      continue;
    }

    if (oj->discard) {
      channel_.OutputConsumeChunk(seg_idx, nullptr, 0);
    } else {
      std::vector<char>& buf = oj->job.out_bufs[idx];  // one file, one segment
      if (hoff + hbytes > buf.size()) buf.resize(hoff + hbytes);
      channel_.OutputConsumeChunk(seg_idx, buf.data() + hoff, hbytes);
      if (hlast) {
        buf.resize(hoff + hbytes);
        fprintf(stderr,
                "[timing] rc job=%llu seg=%d merge_file=%llu size=%llu idx=%zu "
                "step_file_recv=%lldms ok=1\n",
                (unsigned long long)hjob, seg_idx, (unsigned long long)hf,
                (unsigned long long)buf.size(), idx, Ms(Clock::now() - oj->t_file[idx]));
        RcStreamSubmit(hjob, hf, opts_.db_path, buf.data(), buf.size());
      }
    }

    std::shared_ptr<OutJob> fin;
    {
      std::lock_guard<std::mutex> lk(out_jobs_mu_);
      oj->busy--;
      oj->t_last = Clock::now();
      if (hlast) {
        oj->done[idx] = true;
        if (oj->files_left > 0) oj->files_left--;
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

// A job whose outputs stop arriving (worker died mid-transfer) is failed
// after kJobResultTimeoutMs without progress.
void CompactionDispatcher::ReapStaleOutJobs() {
  const auto now = Clock::now();
  std::vector<std::pair<uint64_t, std::shared_ptr<OutJob>>> stale;
  {
    std::lock_guard<std::mutex> lk(out_jobs_mu_);
    for (auto it = out_jobs_.begin(); it != out_jobs_.end();) {
      auto& oj = it->second;
      if (oj->busy == 0 && now - oj->t_last > std::chrono::milliseconds(kJobResultTimeoutMs)) {
        fprintf(stderr,
                "[rc] output watchdog: job=%llu no progress for %dms (files_left=%zu), failing\n",
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

// ── FinishWorkerLoop ───────────────────────────────────────────────────────
void CompactionDispatcher::FinishWorkerLoop(int idx) {
  SetThreadName("rc_fin", idx);
  while (true) {
    FinishJob fjob;
    long long idle_ms = 0;
    size_t qlen = 0;
    {
      std::unique_lock<std::mutex> lk(finish_mu_);
      auto t_wait = Clock::now();
      finish_cv_.wait(lk, [this] { return stop_finish_ || !finish_queue_.empty(); });
      idle_ms = Ms(Clock::now() - t_wait);
      if (stop_finish_ && finish_queue_.empty()) break;
      fjob = std::move(finish_queue_.front());
      finish_queue_.pop_front();
      qlen = finish_queue_.size();
    }

    AsyncJob& job = fjob.job;
    const uint64_t jid = job.spec.job_id;
    fprintf(stderr, "[timing] rc job=%llu fin_idle_ms=%lld fin_qlen=%zu\n",
            (unsigned long long)jid, idle_ms, qlen);
    const auto t_deq = Clock::now();
    RcStreamWait(jid);  // outputs must be on disk before the install
    fprintf(stderr, "[timing] rc job=%llu finish_deq_ms=%lld stream_block_ms=%lld\n",
            (unsigned long long)jid, Ms(t_deq - fjob.enqueue_time), Ms(Clock::now() - t_deq));
    fprintf(stderr, "[timing] rc job=%llu finish_queue_wait=%lldms\n",
            (unsigned long long)jid, Ms(Clock::now() - fjob.enqueue_time));
    if (!fjob.status.ok()) {
      if (job.on_complete) {
        job.on_complete(fjob.status, std::move(job.spec), std::move(job.result),
                        std::move(job.out_bufs));
      }
      RcStreamAbort(jid);
      continue;
    }
    if (job.on_complete) {
      job.on_complete(Status::OK(), std::move(job.spec), std::move(job.result),
                      std::move(job.out_bufs));
    }
    auto t_free = Clock::now();
    {
      std::vector<std::vector<char>> tmp;
      tmp.swap(job.out_bufs);
    }
    fprintf(stderr, "[timing] rc job=%llu fin_free_ms=%lld\n", (unsigned long long)jid,
            Ms(Clock::now() - t_free));
  }
}

}  // namespace ROCKSDB_NAMESPACE
