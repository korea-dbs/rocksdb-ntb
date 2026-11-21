// ============================================================
//  worker_only/worker_server.cc
//  [WORKER (srv3) 전용]
//
//  이 RocksDB 버전의 실제 API:
//    TableReaderOptions  : UnownedPtr<CompressionManager> 3번째 인자
//    MergeHelper         : snapshot_checker 8번째, level 9번째
//    CompactionProxy     : 순수추상클래스 → NullProxy 로 구현
//    CompactionIterator  : 25개 인자
//    TableBuilderOptions : MutableCFOptions + ReadOptions + WriteOptions 필요
//    NewTableBuilder     : raw pointer 반환
//    builder->Add()      : void 반환
//    NewMergingIterator  : InternalKeyComparator* 필요
// ============================================================
#include "worker_server.h"

#include <atomic>
#include <chrono>
#include <memory>

#include "db/compaction/compaction_iterator.h"
#include "db/merge_helper.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/merging_iterator.h"
#include "table/table_builder.h"
#include "table/table_reader.h"

#include "../shared/ntb_mem_file.h"

namespace ROCKSDB_NAMESPACE {

// ── CompactionProxy 최소 구현 ─────────────────────────────────────
// CompactionProxy 는 순수추상클래스이므로 worker 용 구현체를 만든다.
class NullCompactionProxy : public CompactionIterator::CompactionProxy {
 public:
  explicit NullCompactionProxy(bool bottommost, int output_level)
      : bottommost_(bottommost), output_level_(output_level) {}

  int  level()          const override { return output_level_; }
  bool bottommost_level() const override { return bottommost_; }
  int  number_levels()  const override { return output_level_ + 1; }

  bool KeyNotExistsBeyondOutputLevel(
      const Slice& /*key*/,
      std::vector<size_t>* /*level_ptrs*/) const override { return false; }

  Slice GetLargestUserKey() const override { return Slice(); }

  bool allow_ingest_behind()           const override { return false; }
  bool allow_mmap_reads()              const override { return false; }
  bool enable_blob_garbage_collection() const override { return false; }
  double blob_garbage_collection_age_cutoff() const override { return 0.0; }
  uint64_t blob_compaction_readahead_size()   const override { return 0; }
  const Version* input_version()       const override { return nullptr; }
  bool DoesInputReferenceBlobFiles()   const override { return false; }
  const Compaction* real_compaction()  const override { return nullptr; }
  bool SupportsPerKeyPlacement()       const override { return false; }

 private:
  bool bottommost_;
  int  output_level_;
};

// ─────────────────────────────────────────────────────────────────

WorkerServer::WorkerServer(const WorkerOptions& opts) : opts_(opts) {
  if (!opts_.env) opts_.env = Env::Default();
}
WorkerServer::~WorkerServer() { channel_.Close(); }

Status WorkerServer::Connect() {
  return channel_.OpenAsWorker(opts_.primary_node_id);
}

void WorkerServer::Stop() { stop_.store(true, std::memory_order_release); }

// ── 이벤트 루프 ──────────────────────────────────────────────────
void WorkerServer::Run() {
  fprintf(stderr, "[worker] ready. listening for jobs...\n");

  while (!stop_.load(std::memory_order_acquire)) {
    std::string raw;
    Status s = channel_.RecvCtrlMsg(&raw);
    if (!s.ok()) {
      fprintf(stderr, "[worker] RecvCtrlMsg error: %s\n", s.ToString().c_str());
      continue;
    }

    CompactionSpec spec;
    if (!CompactionSpec::Deserialize(Slice(raw), &spec)) {
      CompactionResult err;
      err.success   = false;
      err.error_msg = "spec deserialization failed";
      channel_.SendCtrlMsg(err.Serialize());
      continue;
    }

    fprintf(stderr, "[worker] job_id=%llu, %zu input SSTs\n",
            (unsigned long long)spec.job_id, spec.inputs.size());

    std::vector<std::vector<char>> sst_bufs(spec.inputs.size());
    bool recv_ok = true;
    for (size_t i = 0; i < spec.inputs.size(); i++) {
      const auto& f = spec.inputs[i];
      s = channel_.StreamRecvSST(f.file_number, f.file_size, &sst_bufs[i]);
      if (!s.ok()) {
        fprintf(stderr, "[worker] StreamRecvSST failed file=%llu: %s\n",
                (unsigned long long)f.file_number, s.ToString().c_str());
        recv_ok = false;
        break;
      }
      fprintf(stderr, "[worker] SST #%llu received (%llu bytes)\n",
              (unsigned long long)f.file_number,
              (unsigned long long)f.file_size);
    }

    CompactionResult               result;
    std::vector<std::vector<char>> out_bufs;
    result.job_id = spec.job_id;

    if (!recv_ok) {
      result.success   = false;
      result.error_msg = "SST receive failed";
      channel_.SendCtrlMsg(result.Serialize());
      continue;
    }

    s = HandleJob(spec, sst_bufs, &result, &out_bufs);
    result.success = s.ok();
    if (!s.ok()) result.error_msg = s.ToString();

    channel_.SendCtrlMsg(result.Serialize());

    if (result.success) {
      for (size_t i = 0; i < result.output_files.size(); i++) {
        const auto& of = result.output_files[i];
        Status ss = channel_.StreamSendOutputSST(out_bufs[i], of.file_number);
        if (!ss.ok())
          fprintf(stderr, "[worker] StreamSendOutputSST failed file=%llu: %s\n",
                  (unsigned long long)of.file_number, ss.ToString().c_str());
      }
    }
  }
}

// ── FindComparator ────────────────────────────────────────────────
const Comparator* WorkerServer::FindComparator(const std::string& name) {
  if (name == BytewiseComparator()->Name())        return BytewiseComparator();
  if (name == ReverseBytewiseComparator()->Name()) return ReverseBytewiseComparator();
  return nullptr;
}

// ── OpenInputIterators ────────────────────────────────────────────
Status WorkerServer::OpenInputIterators(
    const CompactionSpec&                 spec,
    const Options&                        opts,
    const std::vector<std::vector<char>>& sst_bufs,
    std::vector<InternalIterator*>*       iters_out,
    std::vector<TableReader*>*            readers_out) {

  ImmutableOptions iopts(opts);

  for (size_t i = 0; i < spec.inputs.size(); i++) {
    const auto& f   = spec.inputs[i];
    const auto& buf = sst_bufs[i];

    std::string label = "mem_in_" + std::to_string(f.file_number);
    auto file_reader  = MakeMemFileReader(buf.data(), buf.size(), label);

    // 실제 시그니처:
    // TableReaderOptions(ioptions, prefix_extractor,
    //                    compression_manager,   ← UnownedPtr<CompressionManager>
    //                    env_options, internal_comparator,
    //                    block_protection_bytes_per_key, skip_filters,
    //                    immortal, force_direct_prefetch, level,
    //                    block_cache_tracer, max_file_size_for_l0_meta_pin,
    //                    cur_db_session_id, cur_file_num,
    //                    unique_id, largest_seqno, tail_size,
    //                    user_defined_timestamps_persisted)
    TableReaderOptions tro(
        iopts,
        /*prefix_extractor=*/nullptr,
        /*compression_manager=*/nullptr,  // UnownedPtr → nullptr 허용
        EnvOptions{},
        iopts.internal_comparator,
        /*block_protection_bytes_per_key=*/(uint8_t)0,
        /*skip_filters=*/false,
        /*immortal=*/false,
        /*force_direct_prefetch=*/false,
        /*level=*/f.level,
        /*block_cache_tracer=*/nullptr,
        /*max_file_size_for_l0_meta_pin=*/(size_t)0,
        /*cur_db_session_id=*/"",
        /*cur_file_num=*/f.file_number,
        /*unique_id=*/kNullUniqueId64x2,
        /*largest_seqno=*/(SequenceNumber)0,
        /*tail_size=*/(uint64_t)0,
        /*user_defined_timestamps_persisted=*/true);

    std::unique_ptr<TableReader> reader;
    Status s = opts.table_factory->NewTableReader(
        tro, std::move(file_reader), f.file_size, &reader);
    if (!s.ok()) {
      for (auto* r : *readers_out) delete r;
      return Status::IOError("NewTableReader failed file=" +
                             std::to_string(f.file_number) + ": " + s.ToString());
    }

    ReadOptions ro;
    ro.verify_checksums = true;
    ro.fill_cache       = false;

    InternalIterator* iter = reader->NewIterator(
        ro, nullptr, nullptr, false, TableReaderCaller::kCompaction);

    iters_out->push_back(iter);
    readers_out->push_back(reader.release());
  }
  return Status::OK();
}

// ── WriteOutputSSTs ───────────────────────────────────────────────
Status WorkerServer::WriteOutputSSTs(
    const CompactionSpec&           spec,
    const Options&                  opts,
    InternalIterator*               merged_iter,
    CompactionResult*               result_out,
    std::vector<std::vector<char>>* out_bufs) {

  auto t0 = std::chrono::steady_clock::now();

  ImmutableOptions      iopts(opts);
  InternalKeyComparator ikc(opts.comparator);
  MutableCFOptions      mopts(opts);

  std::vector<SequenceNumber> snaps = spec.snapshots.list;

  // MergeHelper 실제 시그니처:
  // (env, comparator, merge_operator, compaction_filter, logger,
  //  assert_valid_internal_key, preserve_deletes_seqnum,
  //  snapshot_checker, level, statistics, shutting_down)
  MergeHelper merge_helper(
      opts.env,
      opts.comparator,
      opts.merge_operator.get(),
      opts.compaction_filter,
      /*logger=*/nullptr,
      /*assert_valid_internal_key=*/true,
      spec.snapshots.preserve_deletes_seqnum,
      /*snapshot_checker=*/nullptr,
      /*level=*/0,
      opts.statistics.get());

  // CompactionProxy 구현체
  auto proxy = std::make_unique<NullCompactionProxy>(
      spec.bottommost, spec.output_level);

  merged_iter->SeekToFirst();

  // 필요한 atomic 변수들
  static const std::atomic<bool> kFalse{false};

  // CompactionIterator 실제 시그니처 (25개 인자):
  // input, comparator, merge_helper,
  // last_sequence, snapshots,
  // earliest_write_conflict_snapshot,
  // earliest_snapshot_seqnum_for_snapshot_checker (= 0),
  // preserve_time_min_seqnum (= 0),
  // snapshot_checker, env, report_detailed_time,
  // range_del_agg, blob_file_builder,
  // allow_data_in_errors, enforce_single_del_contracts,
  // shutting_down,
  // proxy (unique_ptr), bottommost_level,
  // compaction_filter, manual_compaction_paused,
  // info_log, full_history_ts_low,
  // penultimate_level_cutoff_seqnum,
  // input_version, io_activity
  CompactionIterator c_iter(
      merged_iter,
      opts.comparator,
      &merge_helper,
      /*last_sequence=*/kMaxSequenceNumber,
      &snaps,
      spec.snapshots.earliest_write_conflict_snapshot,
      /*earliest_snapshot_seqnum_for_snapshot_checker=*/(SequenceNumber)0,
      /*preserve_time_min_seqnum=*/(SequenceNumber)0,
      /*snapshot_checker=*/nullptr,
      opts.env,
      /*report_detailed_time=*/false,
      /*range_del_agg=*/nullptr,
      /*blob_file_builder=*/nullptr,
      /*allow_data_in_errors=*/false,
      /*enforce_single_del_contracts=*/false,
      /*shutting_down=*/kFalse,
      std::move(proxy),
      spec.bottommost,
      /*compaction_filter=*/nullptr,
      /*manual_compaction_paused=*/nullptr,
      /*info_log=*/nullptr,
      /*full_history_ts_low=*/nullptr,
      /*penultimate_level_cutoff_seqnum=*/std::nullopt,
      /*input_version=*/nullptr,
      /*io_activity=*/Env::IOActivity::kUnknown);

  c_iter.SeekToFirst();

  uint64_t fn_counter    = spec.first_output_file_number;
  uint64_t bytes_read    = 0;
  uint64_t bytes_written = 0;

  struct OutputCtx {
    TableBuilder*     builder    = nullptr;  // raw pointer (NewTableBuilder 반환)
    std::vector<char> buf;
    uint64_t          file_number   = 0;
    std::string       first_ikey;
    std::string       last_ikey;
    uint64_t          num_entries   = 0;
    uint64_t          num_deletions = 0;
    SequenceNumber    min_seqno = kMaxSequenceNumber;
    SequenceNumber    max_seqno = 0;
  };

  auto open_output = [&]() -> std::unique_ptr<OutputCtx> {
    auto ctx = std::make_unique<OutputCtx>();
    ctx->file_number = fn_counter++;

    auto writer = MakeMemFileWriter(
        &ctx->buf, "mem_out_" + std::to_string(ctx->file_number));

    // TableBuilderOptions 실제 시그니처:
    // (ioptions, moptions, read_options, write_options,
    //  internal_comparator, int_tbl_prop_collector_factories,
    //  compression_type, compression_opts,
    //  column_family_id, column_family_name,
    //  level, newest_key_time,
    //  is_bottommost=false,
    //  reason=kMisc, oldest_key_time=0, file_creation_time=0,
    //  db_id="", db_session_id="",
    //  target_file_size=0, cur_file_num=0,
    //  last_level_inclusive_max_seqno_threshold=kMaxSequenceNumber)
    ReadOptions  ro;
    WriteOptions wo;
    TableBuilderOptions tbo(
        iopts,
        mopts,
        ro,
        wo,
        ikc,
        /*int_tbl_prop_collector_factories=*/nullptr,
        spec.output.compression,
        spec.output.compression_opts,
        /*column_family_id=*/(uint32_t)0,
        /*column_family_name=*/"default",
        /*level=*/spec.output_level,
        /*newest_key_time=*/(int64_t)0,
        /*is_bottommost=*/spec.bottommost,
        /*reason=*/TableFileCreationReason::kCompaction,
        /*oldest_key_time=*/(int64_t)0,
        /*file_creation_time=*/(uint64_t)0,
        /*db_id=*/"",
        /*db_session_id=*/"",
        /*target_file_size=*/(uint64_t)spec.output.target_file_size,
        /*cur_file_num=*/ctx->file_number);

    // NewTableBuilder 는 raw pointer 반환
    ctx->builder = opts.table_factory->NewTableBuilder(tbo, writer.release());
    return ctx;
  };

  auto close_output = [&](std::unique_ptr<OutputCtx>& ctx) -> Status {
    if (!ctx || !ctx->builder) return Status::OK();
    Status s = ctx->builder->Finish();
    delete ctx->builder;
    ctx->builder = nullptr;
    if (!s.ok()) return s;

    bytes_written += ctx->buf.size();

    CompactionResult::OutputFile of;
    of.file_number    = ctx->file_number;
    of.file_size      = ctx->buf.size();
    of.smallest_ikey  = ctx->first_ikey;
    of.largest_ikey   = ctx->last_ikey;
    of.smallest_seqno = (ctx->min_seqno == kMaxSequenceNumber) ? 0 : ctx->min_seqno;
    of.largest_seqno  = ctx->max_seqno;
    of.num_entries    = ctx->num_entries;
    of.num_deletions  = ctx->num_deletions;
    result_out->output_files.push_back(of);
    out_bufs->push_back(std::move(ctx->buf));
    ctx.reset();
    return Status::OK();
  };

  std::unique_ptr<OutputCtx> cur;

  for (; c_iter.Valid(); c_iter.Next()) {
    if (!cur) cur = open_output();

    Slice ikey = c_iter.key();
    Slice val  = c_iter.value();

    if (cur->first_ikey.empty()) cur->first_ikey = ikey.ToString();
    cur->last_ikey = ikey.ToString();

    ParsedInternalKey pik;
    if (ParseInternalKey(ikey, &pik, /*log_err_key=*/false) == Status::OK()) {
      if (pik.sequence < cur->min_seqno) cur->min_seqno = pik.sequence;
      if (pik.sequence > cur->max_seqno) cur->max_seqno = pik.sequence;
      if (pik.type == kTypeDeletion || pik.type == kTypeSingleDeletion)
        cur->num_deletions++;
    }
    cur->num_entries++;
    bytes_read += ikey.size() + val.size();

    // Add() 는 void 반환
    cur->builder->Add(ikey, val);

    if (cur->builder->FileSize() >= spec.output.target_file_size) {
      Status cs = close_output(cur);
      if (!cs.ok()) return cs;
    }
  }

  if (cur) {
    Status cs = close_output(cur);
    if (!cs.ok()) return cs;
  }

  Status iter_s = c_iter.status();
  if (!iter_s.ok()) return iter_s;

  auto t1 = std::chrono::steady_clock::now();
  result_out->bytes_read    = bytes_read;
  result_out->bytes_written = bytes_written;
  result_out->elapsed_us    = std::chrono::duration_cast<
      std::chrono::microseconds>(t1 - t0).count();
  return Status::OK();
}

// ── HandleJob ─────────────────────────────────────────────────────
Status WorkerServer::HandleJob(
    const CompactionSpec&                 spec,
    const std::vector<std::vector<char>>& sst_bufs,
    CompactionResult*                     result_out,
    std::vector<std::vector<char>>*       out_bufs) {

  const Comparator* ucmp = FindComparator(spec.comparator_name);
  if (!ucmp)
    return Status::InvalidArgument("unknown comparator: " + spec.comparator_name);

  Options opts;
  opts.comparator       = ucmp;
  opts.compression      = spec.output.compression;
  opts.compression_opts = spec.output.compression_opts;
  opts.env              = opts_.env;

  InternalKeyComparator ikc(ucmp);

  std::vector<InternalIterator*> iters;
  std::vector<TableReader*>      readers;

  Status s = OpenInputIterators(spec, opts, sst_bufs, &iters, &readers);
  if (!s.ok()) return s;

  InternalIterator* merged = nullptr;
  if (iters.size() == 1) {
    merged = iters[0];
  } else {
    // NewMergingIterator 는 InternalKeyComparator* 필요
    merged = NewMergingIterator(&ikc,
                                iters.data(),
                                static_cast<int>(iters.size()));
  }

  s = WriteOutputSSTs(spec, opts, merged, result_out, out_bufs);

  if (iters.size() > 1) delete merged;
  else if (!iters.empty()) delete iters[0];
  for (auto* r : readers) delete r;

  return s;
}

}  // namespace ROCKSDB_NAMESPACE
