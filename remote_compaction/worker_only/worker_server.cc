#include "worker_server.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "../shared/compaction_spec.h"
#include "../shared/ntb_channel.h"
#include "db/compaction/clipping_iterator.h"
#include "db/compaction/compaction_iterator.h"
#include "db/dbformat.h"
#include "db/merge_helper.h"
#include "db/range_del_aggregator.h"
#include "ntb_mem_file.h"
#include "options/cf_options.h"
#include "rocksdb/comparator.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/merging_iterator.h"
#include "table/table_builder.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// Control message tags (first byte of every control payload). Must match
// compaction_dispatcher.cc.
constexpr char kMsgTypeSpec = 0x01;
constexpr char kMsgTypeResult = 0x02;
constexpr char kMsgTypeChunkAck = 0x03;

constexpr int kWaitSliceMs = 200;  // stop_ re-check interval of idle waits
// How long a chunk waits for its job spec before it is dropped.
constexpr int kPendingLookupTimeoutMs = 30000;

int64_t NowUs() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

std::string TagMessage(char tag, const std::string& payload) {
  std::string out;
  out.reserve(payload.size() + 1);
  out.push_back(tag);
  out.append(payload);
  return out;
}

// Receive-failure report for one input file:
// tag(1) + success(1) + job_id(8) + file_number(8) + seg(4).
std::string MakeChunkAckPayload(uint64_t job_id, uint64_t file_number,
                                bool success, int seg) {
  std::string out;
  out.push_back(kMsgTypeChunkAck);
  out.push_back(static_cast<char>(success ? 1 : 0));
  out.append(reinterpret_cast<const char*>(&job_id), sizeof(job_id));
  out.append(reinterpret_cast<const char*>(&file_number), sizeof(file_number));
  uint32_t seg_u32 = static_cast<uint32_t>(seg);
  out.append(reinterpret_cast<const char*>(&seg_u32), sizeof(seg_u32));
  return out;
}

// Minimal CompactionProxy so that CompactionIterator can run without a
// Compaction object. KeyNotExistsBeyondOutputLevel() is true only for a
// bottommost output, so tombstones are dropped only when no older version
// can exist below.
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
  int start_level_;
  int output_level_;
};

const char* SubcompactionName(WorkerSubcompaction m) {
  switch (m) {
    case WorkerSubcompaction::kL0:
      return "l0";
    case WorkerSubcompaction::kAll:
      return "all";
    default:
      return "none";
  }
}

}  // namespace

WorkerServer::WorkerServer(const WorkerOptions& opts) : opts_(opts) {
  if (opts_.compaction_threads < 1) opts_.compaction_threads = 1;
  if (opts_.max_subcompactions < 1) opts_.max_subcompactions = 1;
  if (opts_.max_subcompactions > 64) opts_.max_subcompactions = 64;
}

WorkerServer::~WorkerServer() { RequestStop(); }

Status WorkerServer::Connect() {
  return channel_.OpenAsWorker(opts_.primary_node_id);
}

void WorkerServer::RequestStop() noexcept {
  stop_.store(true, std::memory_order_release);
}

bool WorkerServer::WaitThreadDone(std::atomic<bool>& done_flag,
                                  int timeout_sec) {
  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(timeout_sec);
  while (std::chrono::steady_clock::now() < deadline) {
    if (done_flag.load(std::memory_order_acquire)) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return done_flag.load(std::memory_order_acquire);
}

void WorkerServer::JoinOrDetach(std::thread& t, std::atomic<bool>& done,
                                const char* name, int idx) {
  if (!t.joinable()) return;
  if (WaitThreadDone(done, kShutdownJoinTimeoutSec)) {
    t.join();
  } else {
    fprintf(stderr,
            "[worker] %s[%d] did not stop within %ds (probably blocked in a "
            "DMA call); detaching it\n",
            name, idx, kShutdownJoinTimeoutSec);
    t.detach();
  }
}

void WorkerServer::Run() {
  fprintf(stderr,
          "[worker] threads=%d subcompaction=%s max_subcompactions=%d "
          "subcompaction_min_mb=%llu subcompaction_min_range_mb=%llu "
          "input_pool_mb=%llu\n",
          opts_.compaction_threads, SubcompactionName(opts_.subcompaction),
          opts_.max_subcompactions,
          (unsigned long long)(opts_.subcompaction_min_input_bytes >> 20),
          (unsigned long long)(opts_.subcompaction_min_range_bytes >> 20),
          (unsigned long long)(opts_.input_pool_bytes >> 20));

  for (int i = 0; i < kNumSegments; i++) {
    recv_done_[i].store(false, std::memory_order_release);
    recv_threads_[i] = std::thread([this, i]() {
      RecvLoop(i);
      recv_done_[i].store(true, std::memory_order_release);
    });
    send_done_[i].store(false, std::memory_order_release);
    send_threads_[i] = std::thread([this, i]() {
      OutSendLoop(i);
      send_done_[i].store(true, std::memory_order_release);
    });
  }

  const int n = opts_.compaction_threads;
  compact_done_.reset(new std::atomic<bool>[n]);
  compact_threads_.reserve(n);
  for (int i = 0; i < n; i++) {
    compact_done_[i].store(false, std::memory_order_release);
    compact_threads_.emplace_back([this, i]() {
      CompactionLoop(i);
      compact_done_[i].store(true, std::memory_order_release);
    });
  }

  DispatcherLoop();  // returns once stop_ is set

  reg_cv_.notify_all();
  compact_cv_.notify_all();
  outq_cv_.notify_all();
  for (int i = 0; i < kNumSegments; i++)
    JoinOrDetach(recv_threads_[i], recv_done_[i], "RecvLoop", i);
  for (int i = 0; i < n; i++)
    JoinOrDetach(compact_threads_[i], compact_done_[i], "CompactionLoop", i);
  for (int i = 0; i < kNumSegments; i++)
    JoinOrDetach(send_threads_[i], send_done_[i], "OutSendLoop", i);
  fprintf(stderr, "[worker] shutdown complete\n");
}

// ---- Input buffer pool --------------------------------------------------
// Allocating and zero-filling a fresh buffer per job (about 1.2 GB for an
// L0 -> L1 job) costs ~0.5 s of page faults. Buffers of finished jobs are
// kept, and a new job takes the smallest one that is large enough and
// shrinks it (no allocation, no fill).

void WorkerServer::TakeInputBuffer(std::vector<char>* out, size_t n) {
  if (opts_.input_pool_bytes > 0) {
    std::lock_guard<std::mutex> lk(pool_mu_);
    auto it = pool_.lower_bound(n);
    if (it != pool_.end()) {
      pool_bytes_ -= it->first;
      *out = std::move(it->second);
      pool_.erase(it);
      out->resize(n);
      return;
    }
  }
  out->resize(n);
}

void WorkerServer::ReturnInputBuffers(std::vector<std::vector<char>>* bufs) {
  if (opts_.input_pool_bytes > 0) {
    for (auto& b : *bufs) {
      if (b.capacity() == 0) continue;
      // Grow back to full capacity now, so that TakeInputBuffer only shrinks.
      b.resize(b.capacity());
      const size_t c = b.capacity();
      std::lock_guard<std::mutex> lk(pool_mu_);
      if (pool_bytes_ + c > opts_.input_pool_bytes) continue;  // freed
      pool_bytes_ += c;
      pool_.emplace(c, std::move(b));
    }
  }
  bufs->clear();
}

// ---- DispatcherLoop -----------------------------------------------------
void WorkerServer::DispatcherLoop() {
  while (!stop_.load(std::memory_order_acquire)) {
    std::string raw;
    Status s = channel_.RecvSpecMsg(&raw);
    if (!s.ok()) continue;  // poll timeout

    if (raw.empty() || raw[0] != kMsgTypeSpec) {
      fprintf(stderr, "[worker] unexpected control message (tag=%d size=%zu)\n",
              raw.empty() ? -1 : static_cast<int>(raw[0]), raw.size());
      continue;
    }
    CompactionSpec spec;
    if (!CompactionSpec::Deserialize(Slice(raw.data() + 1, raw.size() - 1),
                                     &spec)) {
      fprintf(stderr, "[worker] failed to parse job spec\n");
      continue;
    }
    const uint64_t job_id = spec.job_id;

    auto ja = std::make_shared<JobAssembly>();
    ja->sst_bufs.resize(spec.inputs.size());
    ja->remaining_files.store(static_cast<int>(spec.inputs.size()),
                              std::memory_order_relaxed);
    const int64_t t_alloc = NowUs();
    for (size_t i = 0; i < spec.inputs.size(); i++)
      TakeInputBuffer(&ja->sst_bufs[i], spec.inputs[i].file_size);
    fprintf(stderr, "[worker][timing] job=%llu alloc_us=%lld files=%zu\n",
            (unsigned long long)job_id, (long long)(NowUs() - t_alloc),
            spec.inputs.size());
    ja->spec = std::move(spec);
    const auto& inputs = ja->spec.inputs;
    // Decided now: once the last input arrives, FinalizeJobAssembly() moves
    // ja->spec away (inputs then reads as empty).
    const bool no_inputs = inputs.empty();

    // Drop stale registrations of the same file numbers (e.g. from a failed
    // earlier job) so that new data cannot be routed to a dead job.
    for (const auto& f : inputs) {
      for (int k = 0; k < kNumSegments; k++) {
        std::lock_guard<std::mutex> lk(pending_mu_[k]);
        pending_recv_[k].erase(f.file_number);
      }
    }
    for (size_t i = 0; i < inputs.size(); i++) {
      int seg = inputs[i].assigned_seg;
      if (seg < 0 || seg >= kNumSegments) seg = ja->spec.seg;
      if (seg < 0 || seg >= kNumSegments) seg = 0;
      PendingRecv pr;
      pr.job_id = job_id;
      pr.file_index = i;
      pr.ja = ja;
      std::lock_guard<std::mutex> lk(pending_mu_[seg]);
      pending_recv_[seg][inputs[i].file_number] = std::move(pr);
    }
    {
      std::lock_guard<std::mutex> lk(reg_mu_);
      reg_epoch_++;
    }
    reg_cv_.notify_all();

    if (no_inputs) FinalizeJobAssembly(job_id, ja);
  }
}

// ---- RecvLoop -----------------------------------------------------------
void WorkerServer::FinalizeJobAssembly(uint64_t job_id,
                                       std::shared_ptr<JobAssembly> ja) {
  if (ja->failed.load(std::memory_order_relaxed)) {
    CompactionResult err;
    err.job_id = job_id;
    err.success = false;
    err.error_msg = "SST receive failed";
    SendResult(ja->spec.seg, err);
    ReturnInputBuffers(&ja->sst_bufs);
    return;
  }

  fprintf(stderr, "[worker][timing] job=%llu enqueue t_us=%lld\n",
          (unsigned long long)job_id, (long long)NowUs());
  {
    WorkerJob job;
    job.spec = std::move(ja->spec);
    job.sst_bufs = std::move(ja->sst_bufs);
    std::lock_guard<std::mutex> lk(compact_mu_);
    compact_queue_.push(std::move(job));
  }
  compact_cv_.notify_one();
}

void WorkerServer::RecvLoop(int seg_idx) {
  while (!stop_.load(std::memory_order_acquire)) {
    uint64_t recv_file = 0, file_offset = 0, chunk_bytes = 0, hdr_job = 0;
    bool is_last = false;
    Status ps = channel_.InputPeekChunk(seg_idx, &recv_file, &file_offset,
                                        &chunk_bytes, &is_last, &hdr_job);
    if (!ps.ok()) continue;  // poll timeout

    // Match the chunk to a registered input by (file_number, job_id); the
    // file may have been sent on any segment. A registration of an older
    // job is skipped (the new one is coming); a registration of a newer job
    // means this chunk is leftover data of an abandoned job.
    PendingRecv pr;
    bool found = false;
    bool stale_data = false;
    const auto t_wait0 = std::chrono::steady_clock::now();
    for (;;) {
      uint64_t epoch0;
      {
        std::lock_guard<std::mutex> lk(reg_mu_);
        epoch0 = reg_epoch_;
      }
      for (int k = 0; k < kNumSegments && !found && !stale_data; k++) {
        const int s2 = (seg_idx + k) % kNumSegments;
        std::lock_guard<std::mutex> lk(pending_mu_[s2]);
        auto it = pending_recv_[s2].find(recv_file);
        if (it == pending_recv_[s2].end()) continue;
        if (hdr_job != 0 && it->second.job_id > hdr_job) {
          stale_data = true;
          break;
        }
        if (hdr_job != 0 && it->second.job_id < hdr_job) continue;
        pr = it->second;
        found = true;
        if (is_last) pending_recv_[s2].erase(it);
      }
      if (found || stale_data || stop_.load(std::memory_order_acquire)) break;
      if (std::chrono::steady_clock::now() - t_wait0 >
          std::chrono::milliseconds(kPendingLookupTimeoutMs))
        break;
      // Data may arrive before its spec: wait for the next registration.
      std::unique_lock<std::mutex> lk(reg_mu_);
      reg_cv_.wait_for(lk, std::chrono::milliseconds(100),
                       [&] { return reg_epoch_ != epoch0; });
    }

    if (!found) {
      fprintf(stderr,
              "[worker] RecvLoop[%d]: dropping chunk of file=%llu job=%llu "
              "(%s)\n",
              seg_idx, (unsigned long long)recv_file,
              (unsigned long long)hdr_job,
              stale_data ? "leftover of an abandoned job"
                         : "no matching job spec");
      channel_.InputConsumeChunk(seg_idx, nullptr, 0);
      if (!stale_data && hdr_job != 0) {
        // Tell the primary so it fails the job right away.
        channel_.SendResultMsg(
            seg_idx, MakeChunkAckPayload(hdr_job, recv_file,
                                         /*success=*/false, seg_idx));
      }
      continue;
    }

    char* dest = nullptr;
    auto& rbuf = pr.ja->sst_bufs[pr.file_index];
    if (file_offset + chunk_bytes <= rbuf.size()) {
      dest = rbuf.data() + file_offset;
    } else {
      fprintf(stderr,
              "[worker] RecvLoop[%d]: chunk exceeds input buffer job=%llu "
              "file=%llu off=%llu bytes=%llu buf=%zu; failing the job\n",
              seg_idx, (unsigned long long)pr.job_id,
              (unsigned long long)recv_file, (unsigned long long)file_offset,
              (unsigned long long)chunk_bytes, rbuf.size());
      pr.ja->failed.store(true, std::memory_order_relaxed);
    }
    channel_.InputConsumeChunk(seg_idx, dest, chunk_bytes);

    fprintf(stderr,
            "[worker][timing] job=%llu seg=%d input_file=%llu idx=%zu "
            "arrive_t_us=%lld\n",
            (unsigned long long)pr.job_id, seg_idx,
            (unsigned long long)recv_file, pr.file_index, (long long)NowUs());

    if (is_last && pr.ja->remaining_files.fetch_sub(
                       1, std::memory_order_acq_rel) == 1) {
      FinalizeJobAssembly(pr.job_id, pr.ja);
    }
  }
}

// ---- Output sending -----------------------------------------------------
void WorkerServer::EmitOutput(uint64_t job_id, uint64_t file_number,
                              std::vector<char>* buf) {
  std::shared_ptr<OutputsInFlight> f;
  {
    std::lock_guard<std::mutex> lk(inflight_mu_);
    auto it = inflight_.find(job_id);
    if (it == inflight_.end()) return;  // cannot happen: registered first
    f = it->second;
  }
  {
    std::lock_guard<std::mutex> lk(f->mu);
    f->left++;
  }
  OutTask t;
  t.job_id = job_id;
  t.file_number = file_number;
  t.data = std::move(*buf);
  t.inflight = std::move(f);
  {
    std::lock_guard<std::mutex> lk(outq_mu_);
    outq_.push(std::move(t));
  }
  outq_cv_.notify_all();
}

void WorkerServer::OutSendLoop(int seg_idx) {
  while (true) {
    OutTask t;
    {
      std::unique_lock<std::mutex> lk(outq_mu_);
      while (outq_.empty() && !stop_.load(std::memory_order_acquire))
        outq_cv_.wait_for(lk, std::chrono::milliseconds(kWaitSliceMs));
      if (stop_.load(std::memory_order_acquire)) break;
      t = std::move(outq_.front());
      outq_.pop();
    }
    Status ws = channel_.StreamSendOutputSST(t.data, t.file_number, seg_idx,
                                             t.job_id);
    if (!ws.ok()) {
      fprintf(stderr,
              "[worker] OutSendLoop[%d]: send failed job=%llu file=%llu: %s\n",
              seg_idx, (unsigned long long)t.job_id,
              (unsigned long long)t.file_number, ws.ToString().c_str());
    }
    {
      std::lock_guard<std::mutex> lk(t.inflight->mu);
      if (!ws.ok()) t.inflight->failed = true;
      t.inflight->left--;
    }
    t.inflight->cv.notify_all();
  }
}

// The result is sent exactly once per job, so a failed send is retried.
void WorkerServer::SendResult(int seg, const CompactionResult& result) {
  if (seg < 0 || seg >= kNumSegments) seg = 0;
  const std::string msg = TagMessage(kMsgTypeResult, result.Serialize());
  Status rs;
  while (!(rs = channel_.SendResultMsg(seg, msg)).ok() &&
         !stop_.load(std::memory_order_acquire)) {
    fprintf(stderr, "[worker] result send failed job=%llu seg=%d: %s; retry\n",
            (unsigned long long)result.job_id, seg, rs.ToString().c_str());
  }
}

// ---- CompactionLoop -----------------------------------------------------
void WorkerServer::CompactionLoop(int thread_id) {
  uint64_t ok_jobs = 0, failed_jobs = 0;

  while (true) {
    WorkerJob job;
    {
      std::unique_lock<std::mutex> lk(compact_mu_);
      while (compact_queue_.empty() && !stop_.load(std::memory_order_acquire))
        compact_cv_.wait_for(lk, std::chrono::milliseconds(kWaitSliceMs));
      if (stop_.load(std::memory_order_acquire)) break;
      job = std::move(compact_queue_.front());
      compact_queue_.pop();
    }
    const uint64_t job_id = job.spec.job_id;
    fprintf(stderr, "[worker][timing] job=%llu start t_us=%lld\n",
            (unsigned long long)job_id, (long long)NowUs());

    auto inflight = std::make_shared<OutputsInFlight>();
    {
      std::lock_guard<std::mutex> lk(inflight_mu_);
      inflight_[job_id] = inflight;
    }

    CompactionResult result;
    Status s = HandleJob(job.spec, job.sst_bufs, &result);
    fprintf(stderr, "[worker][timing] job=%llu handle_done t_us=%lld\n",
            (unsigned long long)job_id, (long long)NowUs());

    // Outputs were queued as they were produced; the result goes out after
    // all of them have been sent.
    {
      std::lock_guard<std::mutex> lk(inflight_mu_);
      inflight_.erase(job_id);
    }
    {
      std::unique_lock<std::mutex> lk(inflight->mu);
      while (inflight->left > 0 && !stop_.load(std::memory_order_acquire))
        inflight->cv.wait_for(lk, std::chrono::milliseconds(kWaitSliceMs));
      if (inflight->failed && s.ok())
        s = Status::IOError("output send failed");
    }

    if (s.ok()) {
      ok_jobs++;
    } else {
      failed_jobs++;
      fprintf(stderr, "[worker] CompactionLoop[%d]: job=%llu failed: %s\n",
              thread_id, (unsigned long long)job_id, s.ToString().c_str());
      result.job_id = job_id;
      result.success = false;
      result.error_msg = s.ToString();
    }
    SendResult(job.spec.seg, result);
    ReturnInputBuffers(&job.sst_bufs);
  }

  fprintf(stderr, "[worker] CompactionLoop[%d] exiting: ok=%llu failed=%llu\n",
          thread_id, (unsigned long long)ok_jobs,
          (unsigned long long)failed_jobs);
}

// ---- Compaction ---------------------------------------------------------
const Comparator* WorkerServer::FindComparator(const std::string& name) {
  if (name == BytewiseComparator()->Name()) return BytewiseComparator();
  if (name == ReverseBytewiseComparator()->Name())
    return ReverseBytewiseComparator();
  return nullptr;
}

Status WorkerServer::OpenInputIterators(
    const CompactionSpec& spec, const Options& opts, const ReadOptions& ro,
    const ImmutableOptions& iopts,
    const std::vector<std::vector<char>>& sst_bufs,
    std::vector<InternalIterator*>* iters_out,
    std::vector<TableReader*>* readers_out) {
  for (size_t i = 0; i < spec.inputs.size(); i++) {
    const auto& f = spec.inputs[i];
    const auto& buf = sst_bufs[i];
    auto file_reader =
        MakeMemFileReader(buf.data(), static_cast<uint64_t>(buf.size()),
                          "mem_in_" + std::to_string(f.file_number));

    TableReaderOptions tro(iopts, /*prefix_extractor=*/nullptr,
                           /*compression_manager=*/nullptr, EnvOptions(),
                           iopts.internal_comparator,
                           /*block_protection_bytes_per_key=*/0,
                           /*skip_filters=*/false, /*immortal=*/false,
                           /*force_direct_prefetch=*/false, f.level);
    std::unique_ptr<TableReader> reader;
    Status s = opts.table_factory->NewTableReader(
        tro, std::move(file_reader), static_cast<uint64_t>(buf.size()),
        &reader);
    if (!s.ok()) {
      fprintf(stderr, "[worker] cannot open input file=%llu: %s\n",
              (unsigned long long)f.file_number, s.ToString().c_str());
      return s;
    }

    // Range deletions and merge operands are not supported by the worker.
    // The primary keeps such compactions local; fail loudly if one slips
    // through instead of silently dropping or resurrecting data.
    auto props = reader->GetTableProperties();
    if (props &&
        (props->num_range_deletions > 0 || props->num_merge_operands > 0)) {
      return Status::NotSupported(
          "remote worker: input has range deletions or merge operands");
    }

    InternalIterator* iter = reader->NewIterator(
        ro, nullptr, nullptr, false, TableReaderCaller::kCompaction);
    readers_out->push_back(reader.release());
    iters_out->push_back(iter);
  }
  return Status::OK();
}

// Runs CompactionIterator over merged_iter (snapshot-aware version dropping,
// tombstone removal at the bottommost level) and writes output SSTs into
// memory. Each finished output is handed to EmitOutput() immediately.
//
// Output files are cut like CompactionOutputs::ShouldStopBefore(): at the
// maximum output file size, or at grandparent boundaries (with the same
// overlap heuristics), and never between two versions of the same user key.
Status WorkerServer::WriteOutputSSTs(const CompactionSpec& spec,
                                     const Options& opts,
                                     InternalIterator* merged_iter,
                                     CompactionResult* result_out) {
  const Comparator* ucmp = opts.comparator;
  const auto& out = spec.output;
  const bool gp_on = spec.output_level > 0 && !spec.bottommost &&
                     !out.grandparents.empty() && out.gp_target_file_size > 0 &&
                     out.gp_max_file_size > 0;
  const uint64_t max_file_size =
      gp_on ? out.gp_max_file_size : out.target_file_size;
  uint64_t fn_counter = spec.first_output_file_number != 0
                            ? spec.first_output_file_number
                            : 10000;

  // ---- Grandparent state (as in CompactionOutputs) ----
  // It persists across output files; only switched/overlap are reset when
  // a file is cut.
  std::vector<InternalKey> gp_lo, gp_hi;
  std::vector<uint64_t> gp_sz;
  if (gp_on) {
    for (const auto& g : out.grandparents) {
      gp_lo.emplace_back();
      gp_lo.back().DecodeFrom(g.smallest_ikey);
      gp_hi.emplace_back();
      gp_hi.back().DecodeFrom(g.largest_ikey);
      gp_sz.push_back(g.file_size);
    }
  }
  size_t gp_index = 0, gp_switched = 0;
  bool gp_gap = true, gp_seen = false;
  uint64_t gp_overlap = 0;
  const uint64_t gp_target = out.gp_target_file_size;
  const uint64_t gp_max_compaction_bytes = out.gp_max_compaction_bytes;

  // sstableKeyCompare() in db/compaction/compaction.cc.
  auto gp_cmp = [&](const Slice& a, const InternalKey& bk) -> int {
    const Slice b = bk.Encode();
    int c = ucmp->CompareWithoutTimestamp(ExtractUserKey(a), ExtractUserKey(b));
    if (c != 0) return c;
    const uint64_t af = ExtractInternalKeyFooter(a);
    const uint64_t bf = ExtractInternalKeyFooter(b);
    if (af == kRangeTombstoneSentinel) {
      if (bf != kRangeTombstoneSentinel) return -1;
    } else if (bf == kRangeTombstoneSentinel) {
      return 1;
    }
    return 0;
  };
  // GetCurrentKeyGrandparentOverlappedBytes().
  auto gp_cur_overlap = [&](const Slice& ik) -> uint64_t {
    if (gp_gap || gp_index >= gp_lo.size()) return 0;
    uint64_t b = gp_sz[gp_index];
    for (int64_t i = static_cast<int64_t>(gp_index) - 1;
         i >= 0 && gp_cmp(ik, gp_hi[i]) == 0; i--)
      b += gp_sz[i];
    return b;
  };
  // UpdateGrandparentBoundaryInfo(): returns the number of grandparent
  // boundaries crossed up to ik.
  auto gp_update = [&](const Slice& ik) -> size_t {
    size_t crossed = 0;
    if (gp_lo.empty()) return 0;
    while (gp_index < gp_lo.size()) {
      if (gp_gap) {
        if (gp_cmp(ik, gp_lo[gp_index]) < 0) break;
        if (gp_seen) {
          crossed++;
          gp_overlap += gp_sz[gp_index];
          gp_switched++;
        }
        gp_gap = false;
      } else {
        const int c = gp_cmp(ik, gp_hi[gp_index]);
        if (c < 0 ||
            (c == 0 && (gp_index == gp_lo.size() - 1 ||
                        gp_cmp(ik, gp_lo[gp_index + 1]) < 0)))
          break;
        if (gp_seen) {
          crossed++;
          gp_switched++;
        }
        gp_gap = true;
        gp_index++;
      }
    }
    if (!gp_seen && !gp_gap) gp_overlap = gp_cur_overlap(ik);
    gp_seen = true;
    return crossed;
  };
  // The grandparent rules of ShouldStopBefore() (the size limit is checked
  // separately).
  auto gp_should_cut = [&](uint64_t cur, size_t crossed,
                           uint64_t prev_overlap) -> bool {
    if (crossed == 0) return false;
    if (gp_max_compaction_bytes > 0 &&
        gp_overlap + cur > gp_max_compaction_bytes)
      return true;
    const size_t skippable = gp_gap ? 2 : 3;
    if (crossed >= skippable && gp_overlap - prev_overlap > gp_target / 8)
      return true;
    return cur >= ((gp_target + 99) / 100) *
                      (50 + std::min<size_t>(gp_switched * 5, 40));
  };

  // ---- CompactionIterator (same arguments as CompactionJob) ----
  std::vector<SequenceNumber> snaps = spec.snapshots.list;
  std::sort(snaps.begin(), snaps.end());
  const SequenceNumber earliest_snapshot =
      snaps.empty() ? kMaxSequenceNumber : snaps.front();
  SequenceNumber ewc_snapshot = spec.snapshots.earliest_write_conflict_snapshot;
  if (ewc_snapshot == 0) ewc_snapshot = kMaxSequenceNumber;

  int start_level = spec.output_level;
  for (const auto& f : spec.inputs) start_level = std::min(start_level, f.level);

  MergeHelper merge_helper(Env::Default(), ucmp, /*user_merge_operator=*/nullptr,
                           /*compaction_filter=*/nullptr, /*logger=*/nullptr,
                           /*assert_valid_internal_key=*/false,
                           /*latest_snapshot=*/snaps.empty() ? 0 : snaps.back(),
                           /*snapshot_checker=*/nullptr, start_level);
  InternalKeyComparator icmp(ucmp);
  // Inputs with range deletions are rejected in OpenInputIterators.
  CompactionRangeDelAggregator range_del_agg(&icmp, snaps);
  static const std::atomic<bool> kNotCanceled{false};

  merged_iter->SeekToFirst();
  if (!merged_iter->status().ok()) return merged_iter->status();

  CompactionIterator c_iter(
      merged_iter, ucmp, &merge_helper, /*last_sequence=*/kMaxSequenceNumber,
      &snaps, earliest_snapshot, ewc_snapshot,
      /*job_snapshot=*/kMaxSequenceNumber, /*snapshot_checker=*/nullptr,
      Env::Default(), /*report_detailed_time=*/false, &range_del_agg,
      /*blob_file_builder=*/nullptr, /*allow_data_in_errors=*/false,
      /*enforce_single_del_contracts=*/true, kNotCanceled,
      std::make_unique<NullCompactionProxy>(spec.bottommost, start_level,
                                            spec.output_level),
      /*must_count_input_entries=*/false);
  c_iter.SeekToFirst();
  if (gp_on && c_iter.Valid()) gp_update(c_iter.key());

  // Output table options: default block-based table, the job's compression.
  Options out_opts = opts;
  out_opts.table_factory.reset(NewBlockBasedTableFactory(BlockBasedTableOptions()));
  const ImmutableOptions out_iopts(out_opts);
  const MutableCFOptions out_mopts(out_opts);
  const ReadOptions tbo_ro;
  const WriteOptions tbo_wo;
  static InternalTblPropCollFactories empty_factories;

  uint64_t total_out_entries = 0;
  size_t num_outputs = 0;
  std::string last_user_key;

  while (c_iter.Valid()) {
    std::vector<char> cur_buf;
    auto writer =
        MakeMemFileWriter(&cur_buf, "mem_out_" + std::to_string(fn_counter));
    TableBuilderOptions tbo(
        out_iopts, out_mopts, tbo_ro, tbo_wo, out_iopts.internal_comparator,
        &empty_factories, out.compression, out.compression_opts,
        /*column_family_id=*/0, /*column_family_name=*/"default",
        spec.output_level, /*newest_key_time=*/0, spec.bottommost,
        TableFileCreationReason::kCompaction, /*oldest_key_time=*/0,
        /*file_creation_time=*/0, /*db_id=*/"", /*db_session_id=*/"",
        max_file_size, fn_counter);
    std::unique_ptr<TableBuilder> builder(
        out_opts.table_factory->NewTableBuilder(tbo, writer.get()));

    bool has_entries = false;
    InternalKey smallest, largest;
    uint64_t num_entries = 0, num_deletions = 0;
    SequenceNumber min_seqno = kMaxSequenceNumber, max_seqno = 0;

    while (c_iter.Valid()) {
      const Slice ikey = c_iter.key();
      ParsedInternalKey pikey;
      if (ParseInternalKey(ikey, &pikey, false).ok()) {
        if (pikey.type == kTypeDeletion || pikey.type == kTypeSingleDeletion)
          num_deletions++;
        min_seqno = std::min(min_seqno, pikey.sequence);
        max_seqno = std::max(max_seqno, pikey.sequence);
      }
      if (!has_entries) {
        smallest.DecodeFrom(ikey);
        has_entries = true;
      }
      largest.DecodeFrom(ikey);
      builder->Add(ikey, c_iter.value());
      num_entries++;
      const Slice uk = ExtractUserKey(ikey);
      last_user_key.assign(uk.data(), uk.size());
      c_iter.Next();

      // Update grandparent state with the next key, once per key, as
      // ShouldStopBefore() does.
      const uint64_t prev_overlap = gp_overlap;
      const size_t crossed =
          (gp_on && c_iter.Valid()) ? gp_update(c_iter.key()) : 0;
      const bool next_is_new_user_key =
          c_iter.Valid() && ucmp->Compare(ExtractUserKey(c_iter.key()),
                                          Slice(last_user_key)) != 0;
      if (next_is_new_user_key) {
        const uint64_t cur = builder->FileSize();
        if (cur >= max_file_size ||
            (gp_on && gp_should_cut(cur, crossed, prev_overlap))) {
          if (gp_on) {  // reset as in CompactionOutputs::AddToOutput()
            gp_switched = 0;
            gp_overlap = gp_cur_overlap(c_iter.key());
          }
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
      break;
    }
    Status s = builder->Finish();
    if (!s.ok()) return s;
    IOStatus ios = writer->Sync(IOOptions(), /*use_fsync=*/false);
    if (!ios.ok()) return ios;
    writer.reset();

    CompactionResult::OutputFile of;
    of.file_number = fn_counter++;
    of.file_size = static_cast<uint64_t>(cur_buf.size());
    of.smallest_ikey = smallest.Encode().ToString();
    of.largest_ikey = largest.Encode().ToString();
    of.smallest_seqno = (min_seqno == kMaxSequenceNumber) ? 0 : min_seqno;
    of.largest_seqno = max_seqno;
    of.num_entries = num_entries;
    of.num_deletions = num_deletions;
    total_out_entries += num_entries;
    num_outputs++;
    result_out->bytes_written += cur_buf.size();
    result_out->output_files.push_back(std::move(of));
    EmitOutput(spec.job_id, result_out->output_files.back().file_number,
               &cur_buf);
  }
  if (!c_iter.status().ok()) return c_iter.status();

  const CompactionIterationStats& st = c_iter.iter_stats();
  fprintf(stderr,
          "[worker][citer] job=%llu in_records=%llu out_records=%llu "
          "drop_hidden=%llu drop_obsolete=%llu out_files=%zu bottommost=%d "
          "snaps=%zu\n",
          (unsigned long long)spec.job_id,
          (unsigned long long)st.num_input_records,
          (unsigned long long)total_out_entries,
          (unsigned long long)st.num_record_drop_hidden,
          (unsigned long long)st.num_record_drop_obsolete, num_outputs,
          spec.bottommost ? 1 : 0, snaps.size());
  return merged_iter->status();
}

// Splits the job's key space into up to max_subcompactions ranges of about
// equal input bytes, using the index anchors of all input files (as RocksDB's
// GenSubcompactionBoundaries). Returns the K-1 inner boundaries, or nothing
// if the job is not split.
std::vector<std::string> WorkerServer::PickSubcompactionBounds(
    const CompactionSpec& spec, const Comparator* ucmp, const ReadOptions& ro,
    const std::vector<TableReader*>& readers, uint64_t input_bytes) {
  std::vector<std::string> bounds;
  const int k_max = opts_.max_subcompactions;
  if (opts_.subcompaction == WorkerSubcompaction::kNone || k_max <= 1 ||
      input_bytes < opts_.subcompaction_min_input_bytes)
    return bounds;
  if (opts_.subcompaction == WorkerSubcompaction::kL0 &&
      spec.output_level != 1)
    return bounds;

  struct Anchor {
    std::string key;
    uint64_t bytes;
  };
  std::vector<Anchor> anchors;
  uint64_t total = 0;
  for (auto* r : readers) {
    std::vector<TableReader::Anchor> av;
    if (!r->ApproximateKeyAnchors(ro, av).ok()) continue;
    for (auto& x : av) {
      total += static_cast<uint64_t>(x.range_size);
      anchors.push_back({std::move(x.user_key),
                         static_cast<uint64_t>(x.range_size)});
    }
  }
  std::sort(anchors.begin(), anchors.end(),
            [&](const Anchor& x, const Anchor& y) {
              return ucmp->Compare(x.key, y.key) < 0;
            });
  if (total == 0 || anchors.size() < 2) return bounds;

  // Each range holds at least one output file's worth of input.
  uint64_t min_range = opts_.subcompaction_min_range_bytes;
  if (min_range == 0)
    min_range = spec.output.gp_target_file_size > 0
                    ? spec.output.gp_target_file_size
                    : spec.output.target_file_size;
  const uint64_t step =
      std::max<uint64_t>(total / static_cast<uint64_t>(k_max), min_range);
  uint64_t acc = 0, next = step;
  for (size_t i = 0; i + 1 < anchors.size(); i++) {
    acc += anchors[i].bytes;
    if (acc < next) continue;
    if (bounds.empty() || ucmp->Compare(bounds.back(), anchors[i].key) < 0)
      bounds.push_back(anchors[i].key);
    while (next <= acc) next += step;
    if (bounds.size() + 1 >= static_cast<size_t>(k_max)) break;
  }
  return bounds;
}

Status WorkerServer::HandleJob(const CompactionSpec& spec,
                               const std::vector<std::vector<char>>& sst_bufs,
                               CompactionResult* result_out) {
  const auto t_start = std::chrono::steady_clock::now();
  const Comparator* ucmp = FindComparator(spec.comparator_name);
  if (!ucmp)
    return Status::InvalidArgument("unknown comparator: " +
                                   spec.comparator_name);

  Options opts;
  opts.comparator = ucmp;
  opts.env = Env::Default();
  BlockBasedTableOptions bbto;
  // Inputs are read once, sequentially, from memory. The block cache is not
  // used: its keys are not unique across in-memory files of one job.
  bbto.no_block_cache = true;
  opts.table_factory.reset(NewBlockBasedTableFactory(bbto));
  const ImmutableOptions iopts(opts);
  const ReadOptions ro;

  result_out->job_id = spec.job_id;
  result_out->success = true;
  result_out->bytes_read = 0;
  for (const auto& buf : sst_bufs) result_out->bytes_read += buf.size();

  std::vector<InternalIterator*> iters;
  std::vector<TableReader*> readers;
  Status s =
      OpenInputIterators(spec, opts, ro, iopts, sst_bufs, &iters, &readers);
  std::vector<std::string> bounds;
  if (s.ok())
    bounds = PickSubcompactionBounds(spec, ucmp, ro, readers,
                                     result_out->bytes_read);

  if (s.ok() && bounds.empty()) {
    Arena arena;
    InternalIterator* merged =
        NewMergingIterator(&iopts.internal_comparator, iters.data(),
                           static_cast<int>(iters.size()), &arena);
    // merged lives in the arena and is not deleted; the child iterators are
    // deleted below.
    s = WriteOutputSSTs(spec, opts, merged, result_out);
  } else if (s.ok()) {
    // Subcompactions: range k covers user keys [bounds[k-1], bounds[k]) and
    // has its own input iterators and output state. Output file numbers are
    // spaced apart per range; the primary assigns final numbers anyway.
    const size_t K = bounds.size() + 1;
    const uint64_t fn_base = spec.first_output_file_number != 0
                                 ? spec.first_output_file_number
                                 : 10000;
    std::vector<CompactionResult> sub_res(K);
    std::vector<Status> sub_st(K);
    std::vector<uint64_t> sub_ms(K, 0);

    auto run_sub = [&](size_t k) {
      const auto t0 = std::chrono::steady_clock::now();
      CompactionSpec sspec = spec;
      sspec.first_output_file_number = fn_base + static_cast<uint64_t>(k) * 100000;
      std::vector<InternalIterator*> it_k;
      std::vector<TableReader*> rd_k;
      Status st = OpenInputIterators(sspec, opts, ro, iopts, sst_bufs, &it_k,
                                     &rd_k);
      if (st.ok()) {
        Arena arena_k;
        InternalIterator* merged_k =
            NewMergingIterator(&iopts.internal_comparator, it_k.data(),
                               static_cast<int>(it_k.size()), &arena_k);
        // (user_key, kMaxSequenceNumber) sorts before every version of
        // user_key, so the clip falls on user-key boundaries.
        InternalKey lo_ik, hi_ik;
        Slice lo_s, hi_s;
        const Slice* lo = nullptr;
        const Slice* hi = nullptr;
        if (k > 0) {
          lo_ik.Set(bounds[k - 1], kMaxSequenceNumber, kValueTypeForSeek);
          lo_s = lo_ik.Encode();
          lo = &lo_s;
        }
        if (k + 1 < K) {
          hi_ik.Set(bounds[k], kMaxSequenceNumber, kValueTypeForSeek);
          hi_s = hi_ik.Encode();
          hi = &hi_s;
        }
        {
          ClippingIterator clip(merged_k, lo, hi, &iopts.internal_comparator);
          sub_res[k].job_id = spec.job_id;
          sub_res[k].success = true;
          st = WriteOutputSSTs(sspec, opts, &clip, &sub_res[k]);
        }
      }
      for (auto* it : it_k) delete it;
      for (auto* r : rd_k) delete r;
      sub_st[k] = st;
      sub_ms[k] = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::steady_clock::now() - t0)
              .count());
    };

    std::vector<std::thread> ths;
    ths.reserve(K - 1);
    for (size_t k = 1; k < K; k++) ths.emplace_back(run_sub, k);
    run_sub(0);
    for (auto& t : ths) t.join();

    for (size_t k = 0; k < K && s.ok(); k++) s = sub_st[k];
    if (s.ok()) {
      for (size_t k = 0; k < K; k++) {
        for (auto& of : sub_res[k].output_files)
          result_out->output_files.push_back(std::move(of));
        result_out->bytes_written += sub_res[k].bytes_written;
      }
    }
    std::string ms_str;
    for (size_t k = 0; k < K; k++) {
      if (k) ms_str += ",";
      ms_str += std::to_string(sub_ms[k]) + "/" +
                std::to_string(sub_res[k].output_files.size());
    }
    fprintf(stderr,
            "[worker][wsub] job=%llu out_level=%d K=%zu in_mb=%.1f "
            "out_files=%zu sub_ms/files=%s ok=%d\n",
            (unsigned long long)spec.job_id, spec.output_level, K,
            result_out->bytes_read / 1048576.0,
            result_out->output_files.size(), ms_str.c_str(), s.ok() ? 1 : 0);
  }

  result_out->elapsed_us = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - t_start)
          .count());
  for (auto* it : iters) delete it;
  for (auto* r : readers) delete r;
  if (!s.ok()) {
    result_out->success = false;
    result_out->error_msg = s.ToString();
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
