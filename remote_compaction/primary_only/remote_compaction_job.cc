// RemoteCompactionJob: primary-side lifecycle of one offloaded compaction.
// See remote_compaction_job.h for the overall flow.
#include "remote_compaction_job.h"

#include <fcntl.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "db/column_family.h"
#include "db/compaction/compaction.h"
#include "db/db_impl/db_impl.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "file/writable_file_writer.h"
#include "options/cf_options.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/options.h"
#include "rocksdb/table_properties.h"
#include "util/autovector.h"
#include "util/mutexlock.h"

namespace {

using Clock = std::chrono::steady_clock;

long long ElapsedMs(Clock::time_point since) {
  return static_cast<long long>(
      std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - since)
          .count());
}

// Output files are opened in the table cache before the install (outside the
// DB mutex). The tail size learned there is handed to AddFile() through this
// map so that the install itself does not have to read the file again.
std::mutex g_tail_mu;
std::unordered_map<uint64_t, uint64_t> g_tail_size;

void PutTailSize(uint64_t fnum, uint64_t tail) {
  std::lock_guard<std::mutex> lk(g_tail_mu);
  g_tail_size[fnum] = tail;
}

uint64_t TakeTailSize(uint64_t fnum) {
  std::lock_guard<std::mutex> lk(g_tail_mu);
  auto it = g_tail_size.find(fnum);
  if (it == g_tail_size.end()) return 0;
  const uint64_t v = it->second;
  g_tail_size.erase(it);
  return v;
}

void RemoveOutputFiles(const ROCKSDB_NAMESPACE::CompactionResult& result,
                       const std::string& db_path) {
  for (const auto& of : result.output_files) {
    char fname[32];
    snprintf(fname, sizeof(fname), "%06llu.sst",
             static_cast<unsigned long long>(of.file_number));
    ::remove((db_path + "/" + fname).c_str());
  }
}

}  // namespace

// Writes one received output SST to `dst` the same way a local compaction
// does: WritableFileWriter with direct I/O, preallocation, and fdatasync.
// Called by the dispatcher's persist thread while outputs stream in.
// Returns 0 on success, otherwise an errno-style code.
int RcStreamWriteFileExt(const std::string& dst, const char* data, size_t size) {
  using namespace ROCKSDB_NAMESPACE;
  constexpr size_t kWriteBufferBytes = 1 << 20;  // 1 MiB
  std::shared_ptr<FileSystem> fs = FileSystem::Default();
  FileOptions fo;
  fo.use_direct_writes = true;
  fo.writable_file_max_buffer_size = kWriteBufferBytes;
  std::unique_ptr<FSWritableFile> wf;
  IOStatus s = fs->NewWritableFile(dst, fo, &wf, nullptr);
  if (!s.ok()) {
    fprintf(stderr, "[rc] persist: NewWritableFile %s: %s\n", dst.c_str(),
            s.ToString().c_str());
    return EIO;
  }
  wf->SetPreallocationBlockSize(size);
  WritableFileWriter w(std::move(wf), dst, fo);
  IOOptions io;
  s = w.Append(io, Slice(data, size));
  if (s.ok()) s = w.Sync(io, /*use_fsync=*/false);
  IOStatus cs = w.Close(io);
  if (s.ok()) s = cs;
  if (!s.ok()) {
    fprintf(stderr, "[rc] persist: write %s: %s\n", dst.c_str(),
            s.ToString().c_str());
    return EIO;
  }
  return 0;
}

namespace ROCKSDB_NAMESPACE {

// Defined in compaction_dispatcher.cc: renames the already-persisted temporary
// output files of `job_id` to their final file numbers.
Status RcStreamFinalize(uint64_t job_id, const CompactionResult& wres,
                        const CompactionResult& fixed,
                        const std::string& db_path);

RemoteCompactionJob::RemoteCompactionJob(DBImpl* db, Compaction* c,
                                         ColumnFamilyData* cfd,
                                         CompactionDispatcher* dispatcher,
                                         FSDirectory* db_dir)
    : db_(db), compaction_(c), cfd_(cfd), dispatcher_(dispatcher), db_dir_(db_dir) {}

Status RemoteCompactionJob::RunAsync(
    const std::vector<SequenceNumber>& snapshots, SequenceNumber ewc_snapshot,
    SequenceNumber preserve_seqnum, std::shared_ptr<Compaction> compaction_holder,
    std::function<void(Status)> release_cb) {
  const ImmutableOptions& iopts = cfd_->ioptions();
  const MutableCFOptions& mutable_cf = cfd_->GetLatestMutableCFOptions();

  CompactionSpec spec;
  Status s = dispatcher_->BuildSpec(compaction_, cfd_, iopts, mutable_cf,
                                    snapshots, ewc_snapshot, preserve_seqnum,
                                    &spec);
  if (!s.ok()) return s;

  // Protect file numbers allocated from now on (the final output numbers are
  // assigned in FinishJob) from obsolete-file purging until the job is done.
  std::shared_ptr<std::list<uint64_t>::iterator> pout_keep;
  {
    InstrumentedMutexLock l(db_->mutex());
    // The worker names its outputs with temporary numbers; FinishJob assigns
    // the real ones, so nothing is reserved here.
    spec.first_output_file_number = 0;

    auto pout_iter = std::make_shared<std::list<uint64_t>::iterator>(
        db_->CaptureCurrentFileNumberInPendingOutputsForRemote());
    pout_keep = pout_iter;
    auto orig_release_cb = std::move(release_cb);
    release_cb = [this_db = db_, pout_iter,
                  orig_cb = std::move(orig_release_cb)](Status st) mutable {
      {
        // pending_outputs_ is guarded by the DB mutex. This callback runs on a
        // dispatcher thread without it; orig_cb takes the mutex itself.
        InstrumentedMutexLock lk(this_db->mutex());
        auto v = std::make_unique<std::list<uint64_t>::iterator>(*pout_iter);
        this_db->ReleaseFileNumberFromPendingOutputsForRemote(std::move(v));
      }
      if (orig_cb) orig_cb(st);
    };
  }

  auto self = shared_from_this();
  auto t_total_start = std::make_shared<Clock::time_point>(Clock::now());
  // release_cb must run exactly once: from on_complete when the job was queued,
  // or below when DispatchAsync fails before queuing it.
  auto release_cb_shared =
      std::make_shared<std::function<void(Status)>>(std::move(release_cb));

  s = dispatcher_->DispatchAsync(
      std::move(spec), std::move(compaction_holder),
      [self, release_cb_shared, t_total_start](
          Status st, CompactionSpec&& spec_back, CompactionResult&& result,
          std::vector<std::vector<char>>&& out_bufs) mutable {
        if (!st.ok()) {
          fprintf(stderr, "[rc] async job failed: %s\n", st.ToString().c_str());
          (*release_cb_shared)(st);
          return;
        }
        auto t_finish = Clock::now();
        Status finish_s = self->FinishJob(spec_back, result, out_bufs);
        const long long finish_ms = ElapsedMs(t_finish);
        fprintf(stderr, "[timing] rc job=%llu total=%lldms\n",
                (unsigned long long)spec_back.job_id, ElapsedMs(*t_total_start));
        auto t_release = Clock::now();
        (*release_cb_shared)(finish_s);
        fprintf(stderr,
                "[timing] rc job=%llu fin_finishjob_ms=%lld fin_release_cb_ms=%lld\n",
                (unsigned long long)spec_back.job_id, finish_ms,
                ElapsedMs(t_release));
        if (!finish_s.ok()) {
          fprintf(stderr, "[rc] async install failed: %s\n",
                  finish_s.ToString().c_str());
        }
      });

  if (!s.ok()) {
    // DispatchAsync failed before queuing (e.g. an input SST is reserved by
    // another job). The caller then runs the compaction locally, so only the
    // file-number capture taken above is released here; the full release_cb
    // would also release the compaction's input files.
    fprintf(stderr, "[rc] RunAsync: DispatchAsync failed before enqueue: %s\n",
            s.ToString().c_str());
    if (pout_keep) {
      InstrumentedMutexLock l(db_->mutex());
      auto v = std::make_unique<std::list<uint64_t>::iterator>(*pout_keep);
      db_->ReleaseFileNumberFromPendingOutputsForRemote(std::move(v));
    }
  }
  return s;
}

Status RemoteCompactionJob::FinishJob(
    const CompactionSpec& spec, const CompactionResult& result,
    const std::vector<std::vector<char>>& out_bufs) {
  const ImmutableOptions& iopts = cfd_->ioptions();
  std::vector<uint64_t> fnums;
  for (const auto& f : spec.inputs) fnums.push_back(f.file_number);
  const std::string& db_path = iopts.cf_paths[0].path;

  CompactionResult result_fixed = result;
  for (size_t i = 0; i < result_fixed.output_files.size() && i < out_bufs.size();
       i++) {
    // Outputs persisted while streaming may leave the buffer empty; keep the
    // worker-reported size in that case.
    if (out_bufs[i].empty()) continue;
    result_fixed.output_files[i].file_size =
        static_cast<uint64_t>(out_bufs[i].size());
  }

  // Assign final file numbers and protect them from purging.
  std::unique_ptr<std::list<uint64_t>::iterator> finish_pout_iter;
  {
    auto t_wait = Clock::now();
    InstrumentedMutexLock l(db_->mutex());
    fprintf(stderr, "[timing] rc job=%llu step4m_fnum_mutex_wait=%lldms\n",
            (unsigned long long)spec.job_id, ElapsedMs(t_wait));
    for (auto& of : result_fixed.output_files) {
      of.file_number = db_->GetVersionSet()->NewFileNumber();
    }
    finish_pout_iter = std::make_unique<std::list<uint64_t>::iterator>(
        db_->CaptureCurrentFileNumberInPendingOutputsForRemote());
  }
  auto release_pout = [&]() {
    if (finish_pout_iter) {
      db_->ReleaseFileNumberFromPendingOutputsForRemote(std::move(finish_pout_iter));
    }
  };

  // The outputs were written to temporary files as they arrived; rename them
  // to the final numbers and persist the directory entries.
  auto t_persist_start = Clock::now();
  Status s = RcStreamFinalize(spec.job_id, result, result_fixed, db_path);
  auto t_files_done = Clock::now();
  if (s.ok()) {
    int dfd = ::open(db_path.c_str(), O_RDONLY | O_DIRECTORY);
    int rc = (dfd >= 0) ? ::fsync(dfd) : -1;
    int err = errno;
    if (dfd >= 0) ::close(dfd);
    if (rc != 0) {
      // The files are not in the MANIFEST yet; the next full obsolete-file
      // scan removes them.
      s = Status::IOError("FinishJob: dir fsync failed for " + db_path + ": " +
                          strerror(err));
    }
  }
  {
    unsigned long long bytes = 0;
    for (const auto& b : out_bufs) bytes += b.size();
    fprintf(stderr,
            "[timing] rc persist_split files=%zu bytes=%llu write_sync_ms=%lld "
            "dirsync_ms=%lld direct=1\n",
            out_bufs.size(), bytes,
            (long long)std::chrono::duration_cast<std::chrono::milliseconds>(
                t_files_done - t_persist_start)
                .count(),
            ElapsedMs(t_files_done));
  }
  if (!s.ok()) {
    {
      InstrumentedMutexLock l(db_->mutex());
      release_pout();
    }
    dispatcher_->ReleaseReservation(fnums);
    return s;
  }
  fprintf(stderr, "[timing] rc job=%llu step4a_persist=%lldms\n",
          (unsigned long long)spec.job_id, ElapsedMs(t_persist_start));

  // Open the outputs in the table cache outside the DB mutex, so that the
  // install under the mutex does not pay for it.
  {
    auto t_pre = Clock::now();
    const MutableCFOptions& mcf = cfd_->GetLatestMutableCFOptions();
    ReadOptions ro;
    size_t opened = 0, with_tail = 0;
    for (const auto& of : result_fixed.output_files) {
      FileMetaData fm;
      fm.fd = FileDescriptor(of.file_number, 0, of.file_size, of.smallest_seqno,
                             of.largest_seqno);
      fm.smallest.DecodeFrom(of.smallest_ikey);
      fm.largest.DecodeFrom(of.largest_ikey);
      std::shared_ptr<const TableProperties> tp;
      Status ps = cfd_->table_cache()->GetTableProperties(
          *cfd_->soptions(), ro, cfd_->internal_comparator(), fm, &tp, mcf);
      if (!ps.ok() || !tp) {
        fprintf(stderr, "[rc] preopen failed job=%llu file=%llu: %s\n",
                (unsigned long long)spec.job_id,
                (unsigned long long)of.file_number, ps.ToString().c_str());
        continue;
      }
      opened++;
      if (tp->tail_start_offset > 0 && tp->tail_start_offset < of.file_size) {
        PutTailSize(of.file_number, of.file_size - tp->tail_start_offset);
        with_tail++;
      }
    }
    fprintf(stderr,
            "[timing] rc job=%llu step4p_preopen=%lldms files=%zu ok=%zu tail=%zu\n",
            (unsigned long long)spec.job_id, ElapsedMs(t_pre),
            result_fixed.output_files.size(), opened, with_tail);
  }

  VersionEdit edit;
  s = BuildVersionEdit(result_fixed, spec, &edit);
  if (!s.ok()) {
    RemoveOutputFiles(result_fixed, db_path);
    {
      InstrumentedMutexLock l(db_->mutex());
      release_pout();
    }
    dispatcher_->ReleaseReservation(fnums);
    return s;
  }

  static std::atomic<int> g_concurrent_installers{0};
  const int concurrent_now = ++g_concurrent_installers;
  fprintf(stderr, "[timing] rc job=%llu finish_concurrent=%d\n",
          (unsigned long long)spec.job_id, concurrent_now);
  auto t_lock = Clock::now();
  {
    InstrumentedMutexLock l(db_->mutex());
    fprintf(stderr, "[timing] rc job=%llu step4b_mutex_wait=%lldms\n",
            (unsigned long long)spec.job_id, ElapsedMs(t_lock));

    // Another compaction may have changed the output level between persist
    // and install. Abort if an output overlaps a file that is not an input;
    // RocksDB then reschedules the work.
    if (spec.output_level != 0) {
      auto* vstorage = cfd_->current()->storage_info();
      const Comparator* ucmp = cfd_->user_comparator();
      std::unordered_set<uint64_t> input_fnums(fnums.begin(), fnums.end());
      for (const auto& of : result_fixed.output_files) {
        InternalKey out_smallest, out_largest;
        out_smallest.DecodeFrom(of.smallest_ikey);
        out_largest.DecodeFrom(of.largest_ikey);
        for (const auto* fmd : vstorage->LevelFiles(spec.output_level)) {
          if (input_fnums.count(fmd->fd.GetNumber())) continue;
          if (ucmp->Compare(out_smallest.user_key(), fmd->largest.user_key()) <= 0 &&
              ucmp->Compare(fmd->smallest.user_key(), out_largest.user_key()) <= 0) {
            fprintf(stderr,
                    "[rc] FinishJob: key range overlap: output fnum=%llu vs "
                    "existing fnum=%llu in L%d, aborting\n",
                    (unsigned long long)of.file_number,
                    (unsigned long long)fmd->fd.GetNumber(), spec.output_level);
            RemoveOutputFiles(result_fixed, db_path);
            release_pout();
            --g_concurrent_installers;
            dispatcher_->ReleaseReservation(fnums);
            return Status::Corruption(
                "remote compaction output overlaps existing L" +
                std::to_string(spec.output_level) + " file");
          }
        }
      }
    }

    auto t_lna = Clock::now();
    s = InstallUnderLock(&edit, spec.job_id);
    release_pout();
    fprintf(stderr, "[timing] rc job=%llu step4c_logandapply=%lldms level=%d\n",
            (unsigned long long)spec.job_id, ElapsedMs(t_lna), spec.output_level);
  }
  --g_concurrent_installers;
  fprintf(stderr, "[timing] rc job=%llu step4_persist_install=%lldms\n",
          (unsigned long long)spec.job_id, ElapsedMs(t_persist_start));

  if (!s.ok()) RemoveOutputFiles(result_fixed, db_path);
  dispatcher_->ReleaseReservation(fnums);
  return s;
}

Status RemoteCompactionJob::BuildVersionEdit(const CompactionResult& result,
                                             const CompactionSpec& spec,
                                             VersionEdit* edit_out) {
  edit_out->SetColumnFamily(cfd_->GetID());
  for (const auto& f : spec.inputs) {
    edit_out->DeleteFile(f.level, f.file_number);
  }

  // Same rule as a local CompactionJob: oldest ancester time is the minimum
  // over the inputs, or the current time if unknown.
  int64_t now_i = 0;
  if (!cfd_->ioptions().clock->GetCurrentTime(&now_i).ok()) now_i = 0;
  const uint64_t now = static_cast<uint64_t>(now_i);
  uint64_t oat = compaction_
                     ? compaction_->MinInputFileOldestAncesterTime(nullptr, nullptr)
                     : std::numeric_limits<uint64_t>::max();
  if (oat == std::numeric_limits<uint64_t>::max() || oat == 0) oat = now;

  for (const auto& of : result.output_files) {
    InternalKey smallest, largest;
    smallest.DecodeFrom(of.smallest_ikey);
    largest.DecodeFrom(of.largest_ikey);
    edit_out->AddFile(
        spec.output_level, of.file_number, /*file_path_id=*/0, of.file_size,
        smallest, largest, of.smallest_seqno, of.largest_seqno,
        /*marked_for_compaction=*/false, Temperature::kUnknown,
        /*oldest_blob_file_number=*/kInvalidBlobFileNumber,
        /*oldest_ancester_time=*/oat, /*file_creation_time=*/now,
        /*epoch_number=*/(spec.output_level == 0) ? cfd_->NewEpochNumber()
                                                   : (uint64_t)1,
        /*file_checksum=*/"", /*file_checksum_func_name=*/"", kNullUniqueId64x2,
        /*compensated_range_deletion_size=*/(uint64_t)0,
        /*tail_size=*/TakeTailSize(of.file_number),
        /*user_defined_timestamps_persisted=*/true);
  }
  return Status::OK();
}

// Applies the edit through the regular MANIFEST path (one writer, serialized
// with local compactions and flushes), then purges obsolete files.
Status RemoteCompactionJob::InstallUnderLock(VersionEdit* edit, uint64_t job_id) {
  const ReadOptions read_opts(Env::IOActivity::kCompaction);
  const WriteOptions write_opts(Env::IOActivity::kCompaction);
  autovector<VersionEdit*> edit_list;
  edit_list.emplace_back(edit);

  Status s = db_->GetVersionSet()->LogAndApply(cfd_, read_opts, write_opts,
                                               edit_list, db_->mutex(), db_dir_);
  if (!s.ok()) return Status::IOError("LogAndApply failed: " + s.ToString());

  auto t_purge = Clock::now();
  static std::atomic<int> g_obsolete_job_id{0};
  JobContext job_context(g_obsolete_job_id.fetch_add(1), true);
  db_->FindObsoleteFiles(&job_context, false);
  const bool had_work =
      job_context.HaveSomethingToClean() || job_context.HaveSomethingToDelete();
  if (had_work) {
    db_->mutex()->Unlock();
    if (job_context.HaveSomethingToDelete()) db_->PurgeObsoleteFiles(job_context);
    job_context.Clean();
    db_->mutex()->Lock();
  }
  fprintf(stderr, "[timing] rc job=%llu step4d_obsolete_purge=%lldms had_work=%d\n",
          (unsigned long long)job_id, ElapsedMs(t_purge), had_work ? 1 : 0);
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
