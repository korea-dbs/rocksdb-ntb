// ============================================================
//  primary_only/compaction_dispatcher.cc  (v8 – per-seg RecvLoop)
//
//    - CtrlRecvLoop: RecvResultMsg → fixed_seg = job_id%kNumSegments
//                   → in_flight_ pop → output_queues_[fixed_seg] push
//    - OutputRecvLoop[seg]: output_queues_[seg] pop
//                   → StreamRecvOutputSST(fnum, fsize, buf, seg)
//                   → finish_queue_ push
// ============================================================
#include "compaction_dispatcher.h"

#include <chrono>
#include <cstdio>
#include <thread>

#include "db/compaction/compaction.h"
#include "db/column_family.h"
#include "rocksdb/comparator.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

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
    : opts_(opts) {
  finish_pool_.reserve(kFinishThreads);
  for (int i = 0; i < kFinishThreads; i++)
    finish_pool_.emplace_back([this]() { FinishWorkerLoop(); });

  for (int i = 0; i < kNumSegments; i++) {
    int seg = i;
    send_threads_[seg] = std::thread([this, seg]() { SendLoop(seg); });
  }

  ctrl_recv_thread_ = std::thread([this]() { CtrlRecvLoop(); });
  for (int i = 0; i < kNumSegments; i++) {
    int seg = i;
    output_recv_threads_[seg] = std::thread([this, seg]() { OutputRecvLoop(seg); });
  }
}

CompactionDispatcher::~CompactionDispatcher() {
  stop_.store(true, std::memory_order_release);

  for (int i = 0; i < kNumSegments; i++) send_cv_[i].notify_all();
  for (int i = 0; i < kNumSegments; i++)
    if (send_threads_[i].joinable()) send_threads_[i].join();

  if (ctrl_recv_thread_.joinable()) ctrl_recv_thread_.join();

  for (int i = 0; i < kNumSegments; i++) output_cv_[i].notify_all();
  for (int i = 0; i < kNumSegments; i++)
    if (output_recv_threads_[i].joinable()) output_recv_threads_[i].join();

  {
    std::lock_guard<std::mutex> lk(finish_mu_);
    stop_finish_ = true;
  }
  finish_cv_.notify_all();
  for (auto& t : finish_pool_)
    if (t.joinable()) t.join();

  Disconnect();
}

Status CompactionDispatcher::Connect() {
  Status s = channel_.OpenAsPrimary(opts_.worker_node_id);
  if (s.ok()) {
    channel_open_.store(true, std::memory_order_release);
    for (int i = 0; i < kNumSegments; i++) send_cv_[i].notify_all();
  }
  return s;
}

void CompactionDispatcher::Disconnect() { channel_.Close(); }

// ── BuildSpec ────────────────────────────────────────────────────

Status CompactionDispatcher::BuildSpec(
    Compaction*                         c,
    ColumnFamilyData*                   cfd,
    const ImmutableOptions&             iopts,
    const MutableCFOptions&             mutable_cf,
    const std::vector<SequenceNumber>&  snapshots,
    SequenceNumber                      ewc_snapshot,
    SequenceNumber                      preserve_seqnum,
    CompactionSpec*                     spec_out) {

  CompactionSpec& s = *spec_out;
  s.job_id       = job_id_.fetch_add(1, std::memory_order_relaxed);
  s.output_level = c->output_level();
  s.bottommost   = c->bottommost_level();
  s.comparator_name = iopts.user_comparator->Name();

  if (iopts.compaction_filter)
    s.compaction_filter_name = iopts.compaction_filter->Name();

  {
    Slice smallest = c->GetSmallestUserKey();
    if (!smallest.empty()) { s.has_start = true; s.start_user_key = smallest.ToString(); }
  }
  {
    Slice largest = c->GetLargestUserKey();
    if (!largest.empty()) { s.has_end = true; s.end_user_key = largest.ToString(); }
  }

  for (size_t li = 0; li < c->num_input_levels(); li++) {
    int level = c->level(li);
    for (size_t i = 0; i < c->num_input_files(li); i++) {
      const FileMetaData* fmd = c->input(li, i);
      SSTFileSpec f;
      f.file_number    = fmd->fd.GetNumber();
      f.file_size      = fmd->fd.GetFileSize();
      f.smallest_seqno = fmd->fd.smallest_seqno;
      f.largest_seqno  = fmd->fd.largest_seqno;
      f.smallest_ikey  = fmd->smallest.Encode().ToString();
      f.largest_ikey   = fmd->largest.Encode().ToString();
      f.level          = level;
      s.inputs.push_back(f);
    }
  }

  s.snapshots.list                             = snapshots;
  s.snapshots.earliest_write_conflict_snapshot = ewc_snapshot;
  s.snapshots.preserve_deletes_seqnum          = preserve_seqnum;

  s.output.target_file_size = std::min(opts_.target_file_size, kChunkDataSize);
  s.output.compression      = mutable_cf.compression;
  s.output.compression_opts = mutable_cf.compression_opts;

  return Status::OK();
}

static std::string MakeSSTPath(const std::string& db_path, uint64_t file_number) {
  char fname[32];
  snprintf(fname, sizeof(fname), "%06llu.sst", (unsigned long long)file_number);
  return db_path + "/" + fname;
}

void CompactionDispatcher::SendLoop(int seg_idx) {
  {
    std::unique_lock<std::mutex> lk(send_mu_[seg_idx]);
    send_cv_[seg_idx].wait(lk, [this] {
      return stop_.load(std::memory_order_acquire) ||
             channel_open_.load(std::memory_order_acquire);
    });
    if (stop_.load(std::memory_order_acquire)) return;
  }

  fprintf(stderr, "[rc] SendLoop[%d] started\n", seg_idx);

  while (true) {
    AsyncJob job;
    {
      std::unique_lock<std::mutex> lk(send_mu_[seg_idx]);
      send_cv_[seg_idx].wait(lk, [this, seg_idx] {
        return stop_.load(std::memory_order_acquire) ||
               !send_queues_[seg_idx].empty();
      });
      if (stop_.load(std::memory_order_acquire) &&
          send_queues_[seg_idx].empty()) break;
      job = std::move(send_queues_[seg_idx].front());
      send_queues_[seg_idx].pop();
    }

    uint64_t job_id = job.spec.job_id;

    Status s;
    {
      std::lock_guard<std::mutex> ctrl_lk(ctrl_send_mu_);
      s = channel_.SendCtrlMsg(job.spec.Serialize());
    }
    if (!s.ok()) {
      fprintf(stderr, "[rc] SendLoop[%d]: SendCtrlMsg failed job=%llu: %s\n",
              seg_idx, (unsigned long long)job_id, s.ToString().c_str());
      reservation_.Release(job.fnums);
      if (job.on_complete)
        job.on_complete(s, std::move(job.spec), std::move(job.result),
                        std::move(job.out_bufs));
      continue;
    }

    bool send_ok = true;
    for (const auto& f : job.spec.inputs) {
      std::string path = MakeSSTPath(opts_.db_path, f.file_number);
      s = channel_.StreamSendSST(path, f.file_number, f.file_size, seg_idx);
      if (!s.ok()) {
        fprintf(stderr, "[rc] SendLoop[%d]: StreamSendSST failed: %s\n",
                seg_idx, s.ToString().c_str());
        send_ok = false;
        break;
      }
    }

    if (!send_ok) {
      reservation_.Release(job.fnums);
      if (job.on_complete)
        job.on_complete(Status::IOError("StreamSendSST failed"),
                        std::move(job.spec), std::move(job.result),
                        std::move(job.out_bufs));
      continue;
    }

    {
      std::lock_guard<std::mutex> lk(inflight_mu_);
      in_flight_[job_id] = std::move(job);
    }
    fprintf(stderr, "[rc] SendLoop[%d]: job submitted job=%llu\n",
            seg_idx, (unsigned long long)job_id);
  }

  fprintf(stderr, "[rc] SendLoop[%d] exiting\n", seg_idx);
}

void CompactionDispatcher::CtrlRecvLoop() {
  while (!channel_open_.load(std::memory_order_acquire)) {
    if (stop_.load(std::memory_order_acquire)) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  fprintf(stderr, "[rc] CtrlRecvLoop started\n");

  while (!stop_.load(std::memory_order_acquire)) {
    std::string raw;
    Status s = channel_.RecvCtrlMsg(&raw);
    if (!s.ok()) {
      if (stop_.load(std::memory_order_acquire)) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      continue;
    }

    CompactionResult result;
    if (!CompactionResult::Deserialize(Slice(raw), &result)) {
      fprintf(stderr, "[rc] CtrlRecvLoop: deserialize failed\n");
      continue;
    }

    uint64_t job_id   = result.job_id;
    int fixed_seg = static_cast<int>(
        job_id % static_cast<uint64_t>(kNumSegments));

    AsyncJob job;
    {
      std::lock_guard<std::mutex> lk(inflight_mu_);
      auto it = in_flight_.find(job_id);
      if (it == in_flight_.end()) {
        fprintf(stderr, "[rc] CtrlRecvLoop: unknown job_id=%llu\n",
                (unsigned long long)job_id);
        continue;
      }
      job = std::move(it->second);
      in_flight_.erase(it);
    }

    if (!result.success) {
      fprintf(stderr, "[rc] CtrlRecvLoop: worker failed job=%llu: %s\n",
              (unsigned long long)job_id, result.error_msg.c_str());
      reservation_.Release(job.fnums);
      std::lock_guard<std::mutex> lk(finish_mu_);
      finish_queue_.push({Status::IOError("worker failed: " + result.error_msg),
                          std::move(job)});
      finish_cv_.notify_one();
      continue;
    }

    job.result = std::move(result);
    job.out_bufs.resize(job.result.output_files.size());

    {
      std::lock_guard<std::mutex> lk(output_mu_[fixed_seg]);
      output_queues_[fixed_seg].push({std::move(job)});
    }
    output_cv_[fixed_seg].notify_one();
    fprintf(stderr, "[rc] CtrlRecvLoop: job=%llu → OutputRecvLoop[%d]\n",
            (unsigned long long)job_id, fixed_seg);
  }

  for (int i = 0; i < kNumSegments; i++) output_cv_[i].notify_all();
  fprintf(stderr, "[rc] CtrlRecvLoop exiting\n");
}

// output_queues_[seg_idx] pop → StreamRecvOutputSST(fixed_seg=seg_idx)
void CompactionDispatcher::OutputRecvLoop(int seg_idx) {
  while (!channel_open_.load(std::memory_order_acquire)) {
    if (stop_.load(std::memory_order_acquire)) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  fprintf(stderr, "[rc] OutputRecvLoop[%d] started\n", seg_idx);

  while (true) {
    PendingOutput pending;
    {
      std::unique_lock<std::mutex> lk(output_mu_[seg_idx]);
      output_cv_[seg_idx].wait(lk, [this, seg_idx] {
        return stop_.load(std::memory_order_acquire) ||
               !output_queues_[seg_idx].empty();
      });
      if (stop_.load(std::memory_order_acquire) &&
          output_queues_[seg_idx].empty()) break;
      pending = std::move(output_queues_[seg_idx].front());
      output_queues_[seg_idx].pop();
    }

    AsyncJob& job    = pending.job;
    uint64_t job_id  = job.result.job_id;
    bool recv_ok     = true;

    for (size_t i = 0; i < job.result.output_files.size(); i++) {
      const auto& of = job.result.output_files[i];
      Status s = channel_.StreamRecvOutputSST(of.file_number, of.file_size,
                                               &job.out_bufs[i], seg_idx);
      if (!s.ok()) {
        fprintf(stderr,
                "[rc] OutputRecvLoop[%d]: StreamRecvOutputSST failed job=%llu: %s\n",
                seg_idx, (unsigned long long)job_id, s.ToString().c_str());
        recv_ok = false;
        break;
      }
    }

    Status finish_status = recv_ok
        ? Status::OK()
        : Status::IOError("StreamRecvOutputSST failed");
    if (!recv_ok) reservation_.Release(job.fnums);

    {
      std::lock_guard<std::mutex> lk(finish_mu_);
      finish_queue_.push({finish_status, std::move(job)});
    }
    finish_cv_.notify_one();
    fprintf(stderr, "[rc] OutputRecvLoop[%d]: job=%llu done\n",
            seg_idx, (unsigned long long)job_id);
  }

  fprintf(stderr, "[rc] OutputRecvLoop[%d] exiting\n", seg_idx);
}

// ── FinishWorkerLoop ──────────────────────────────────────────────
void CompactionDispatcher::FinishWorkerLoop() {
  while (true) {
    FinishJob fjob;
    {
      std::unique_lock<std::mutex> lk(finish_mu_);
      finish_cv_.wait(lk, [this] {
        return stop_finish_ || !finish_queue_.empty();
      });
      if (stop_finish_ && finish_queue_.empty()) break;
      fjob = std::move(finish_queue_.front());
      finish_queue_.pop();
    }

    if (fjob.job.on_complete) {
      fjob.job.on_complete(fjob.status,
                           std::move(fjob.job.spec),
                           std::move(fjob.job.result),
                           std::move(fjob.job.out_bufs));
    }
  }
}

// ── DispatchAsync ─────────────────────────────────────────────────
Status CompactionDispatcher::DispatchAsync(
    CompactionSpec                      spec,
    std::shared_ptr<Compaction>         compaction_holder,
    std::function<void(Status,
                       CompactionSpec&&,
                       CompactionResult&&,
                       std::vector<std::vector<char>>&&)> on_complete) {

  std::vector<uint64_t> fnums;
  for (const auto& f : spec.inputs) fnums.push_back(f.file_number);
  if (!reservation_.TryReserve(fnums))
    return Status::Busy("input SSTs already reserved");

  size_t num_inputs = spec.inputs.size();
  int fixed_seg = static_cast<int>(
      spec.job_id % static_cast<uint64_t>(kNumSegments));

  AsyncJob job;
  job.spec              = std::move(spec);
  job.fnums             = std::move(fnums);
  job.compaction_holder = std::move(compaction_holder);
  job.on_complete       = std::move(on_complete);

  {
    std::lock_guard<std::mutex> lk(send_mu_[fixed_seg]);
    send_queues_[fixed_seg].push(std::move(job));
  }
  send_cv_[fixed_seg].notify_one();

  fprintf(stderr, "[rc] async job queued seg[%d], %zu input SSTs\n",
          fixed_seg, num_inputs);
  return Status::OK();
}

// ── ExecuteJobSync ────────────────────────────────────────────────
Status CompactionDispatcher::ExecuteJobSync(AsyncJob& job) {
  int fixed_seg = static_cast<int>(
      job.spec.job_id % static_cast<uint64_t>(kNumSegments));

  Status s;
  {
    std::lock_guard<std::mutex> ctrl_lk(ctrl_send_mu_);
    s = channel_.SendCtrlMsg(job.spec.Serialize());
  }
  if (!s.ok()) return s;

  for (const auto& f : job.spec.inputs) {
    std::string path = MakeSSTPath(opts_.db_path, f.file_number);
    s = channel_.StreamSendSST(path, f.file_number, f.file_size, fixed_seg);
    if (!s.ok()) {
      reservation_.Release(job.fnums);
      return Status::IOError("StreamSendSST failed: " + s.ToString());
    }
  }

  std::string raw;
  s = channel_.RecvCtrlMsg(&raw);
  if (!s.ok()) { reservation_.Release(job.fnums); return s; }

  if (!CompactionResult::Deserialize(Slice(raw), &job.result)) {
    reservation_.Release(job.fnums);
    return Status::Corruption("CompactionResult deserialize failed");
  }
  if (!job.result.success) {
    reservation_.Release(job.fnums);
    return Status::IOError("worker failed: " + job.result.error_msg);
  }

  job.out_bufs.resize(job.result.output_files.size());
  for (size_t i = 0; i < job.result.output_files.size(); i++) {
    const auto& of = job.result.output_files[i];
    s = channel_.StreamRecvOutputSST(of.file_number, of.file_size,
                                      &job.out_bufs[i], fixed_seg);
    if (!s.ok()) {
      reservation_.Release(job.fnums);
      return Status::IOError("StreamRecvOutputSST failed: " + s.ToString());
    }
  }
  return Status::OK();
}

Status CompactionDispatcher::Dispatch(
    Compaction*                         c,
    ColumnFamilyData*                   cfd,
    const ImmutableOptions&             iopts,
    const MutableCFOptions&             mutable_cf,
    const std::vector<SequenceNumber>&  snapshots,
    SequenceNumber                      ewc_snapshot,
    SequenceNumber                      preserve_seqnum,
    CompactionResult*                   result_out,
    std::vector<std::vector<char>>*     out_sst_bufs) {

  AsyncJob job;
  Status s = BuildSpec(c, cfd, iopts, mutable_cf,
                       snapshots, ewc_snapshot, preserve_seqnum, &job.spec);
  if (!s.ok()) return s;

  for (const auto& f : job.spec.inputs) job.fnums.push_back(f.file_number);
  if (!reservation_.TryReserve(job.fnums))
    return Status::Busy("input SSTs already reserved");

  s = ExecuteJobSync(job);
  if (!s.ok()) return s;

  *result_out   = std::move(job.result);
  *out_sst_bufs = std::move(job.out_bufs);
  reservation_.Release(job.fnums);
  return Status::OK();
}

void CompactionDispatcher::ReleaseReservation(
    const std::vector<uint64_t>& fnums) {
  reservation_.Release(fnums);
}

}  // namespace ROCKSDB_NAMESPACE
