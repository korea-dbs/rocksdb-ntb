// ============================================================
//  primary_only/remote_compaction_job.cc
//  [PRIMARY (srv4) 전용]
// ============================================================
#include "remote_compaction_job.h"

#include <filesystem>
#include <fstream>

#include "db/compaction/compaction.h"
#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/table_reader.h"
#include "util/mutexlock.h"

#include "../shared/ntb_mem_file.h"

namespace ROCKSDB_NAMESPACE {

RemoteCompactionJob::RemoteCompactionJob(DBImpl*               db,
                                          Compaction*           c,
                                          ColumnFamilyData*     cfd,
                                          CompactionDispatcher* dispatcher)
    : db_(db), compaction_(c), cfd_(cfd), dispatcher_(dispatcher) {}

// ── Run ───────────────────────────────────────────────────────────

Status RemoteCompactionJob::Run(
    const std::vector<SequenceNumber>& snapshots,
    SequenceNumber                     ewc_snapshot,
    SequenceNumber                     preserve_seqnum) {

  const ImmutableOptions& iopts      = *cfd_->ioptions();
  const MutableCFOptions  mutable_cf = *cfd_->GetLatestMutableCFOptions();

  // 1. Dispatch (input SST 전송 + compaction + output SST 수신)
  CompactionResult              result;
  std::vector<std::vector<char>> out_bufs;

  Status s = dispatcher_->Dispatch(
      compaction_, cfd_, iopts, mutable_cf,
      snapshots, ewc_snapshot, preserve_seqnum,
      &result, &out_bufs);
  if (!s.ok()) return s;

  // spec 재구성 (install 에 필요)
  CompactionSpec spec;
  spec.output_level = compaction_->output_level();
  for (size_t li = 0; li < compaction_->num_input_levels(); li++) {
    int level = compaction_->level(li);
    for (size_t i = 0; i < compaction_->num_input_files(li); i++) {
      SSTFileSpec f;
      f.file_number = compaction_->input(li, i)->fd.GetNumber();
      f.level       = level;
      spec.inputs.push_back(f);
    }
  }

  std::vector<uint64_t> fnums;
  for (const auto& f : spec.inputs) fnums.push_back(f.file_number);


  // 3. DB 디렉토리에 저장 (MANIFEST 용)
  const std::string& db_path = iopts.cf_paths[0].path;
  s = PersistOutputSSTs(result, out_bufs, db_path);
  if (!s.ok()) { dispatcher_->ReleaseReservation(fnums); return s; }

  // 4. VersionEdit 생성 + install
  VersionEdit edit;
  s = BuildVersionEdit(result, spec, &edit);
  if (!s.ok()) { dispatcher_->ReleaseReservation(fnums); return s; }

  {
    InstrumentedMutexLock l(db_->mutex());
    s = InstallUnderLock(&edit);
  }

  dispatcher_->ReleaseReservation(fnums);
  return s;
}

// ── ValidateOutputsInMemory ───────────────────────────────────────

Status RemoteCompactionJob::ValidateOutputsInMemory(
    const CompactionResult&               result,
    const std::vector<std::vector<char>>& bufs) {

  const ImmutableOptions& iopts = *cfd_->ioptions();

  for (size_t i = 0; i < result.output_files.size(); i++) {
    const auto& of  = result.output_files[i];
    const auto& buf = bufs[i];

    if (buf.size() != of.file_size)
      return Status::Corruption("output SST size mismatch for file " +
                                std::to_string(of.file_number));

    // MemReadableFile → TableReader (경로 없음)
    std::string label = "mem_out_" + std::to_string(of.file_number);
    auto file_reader  = MakeMemFileReader(buf.data(), buf.size(), label);

    TableReaderOptions tro(iopts, nullptr, EnvOptions{},
                            iopts.internal_comparator,
                            false, false, false, of.file_number,
                            nullptr, 0, "", of.file_number);

    std::unique_ptr<TableReader> reader;
    Status s = iopts.table_factory->NewTableReader(
        tro, std::move(file_reader), of.file_size, &reader);
    if (!s.ok())
      return Status::Corruption("output SST open failed (in-memory): " +
                                s.ToString());

    // checksum + key ordering 검증
    ReadOptions ro;
    ro.verify_checksums = true;
    auto* arena = new Arena();
    auto* iter  = reader->NewIterator(ro, nullptr, arena, false,
                                       TableReaderCaller::kCompaction);
    iter->SeekToFirst();

    std::string prev_ikey;
    uint64_t    count = 0;
    for (; iter->Valid(); iter->Next()) {
      Slice ik = iter->key();
      if (!prev_ikey.empty() &&
          iopts.internal_comparator.Compare(ik, Slice(prev_ikey)) <= 0) {
        delete iter; delete arena;
        return Status::Corruption("key ordering violation in output SST " +
                                  std::to_string(of.file_number));
      }
      prev_ikey = ik.ToString();
      count++;
    }
    s = iter->status();
    delete iter;
    delete arena;

    if (!s.ok())
      return Status::Corruption("output SST iter error: " + s.ToString());

    fprintf(stderr, "[primary] validated SST #%llu: %llu entries (in-memory)\n",
            (unsigned long long)of.file_number,
            (unsigned long long)count);
  }
  return Status::OK();
}

// ── PersistOutputSSTs ─────────────────────────────────────────────

Status RemoteCompactionJob::PersistOutputSSTs(
    const CompactionResult&               result,
    const std::vector<std::vector<char>>& bufs,
    const std::string&                    db_path) {

  for (size_t i = 0; i < result.output_files.size(); i++) {
    const auto& of  = result.output_files[i];
    const auto& buf = bufs[i];

    std::string dst = db_path + "/" + std::to_string(of.file_number) + ".sst";
    std::ofstream ofs(dst, std::ios::binary | std::ios::trunc);
    if (!ofs)
      return Status::IOError("PersistOutputSSTs: cannot open " + dst);

    ofs.write(buf.data(), static_cast<std::streamsize>(buf.size()));
    if (!ofs)
      return Status::IOError("PersistOutputSSTs: write failed for " + dst);
  }
  return Status::OK();
}

// ── BuildVersionEdit ──────────────────────────────────────────────

Status RemoteCompactionJob::BuildVersionEdit(
    const CompactionResult& result,
    const CompactionSpec&   spec,
    VersionEdit*            edit_out) {

  edit_out->SetColumnFamily(cfd_->GetID());

  for (const auto& f : spec.inputs)
    edit_out->DeleteFile(f.level, f.file_number);

  for (const auto& of : result.output_files) {
    InternalKey smallest, largest;
    smallest.DecodeFrom(of.smallest_ikey);
    largest.DecodeFrom(of.largest_ikey);

    edit_out->AddFile(
        spec.output_level,
        of.file_number, 0, of.file_size,
        smallest, largest,
        of.smallest_seqno, of.largest_seqno,
        false, Temperature::kUnknown,
        kInvalidBlobFileNumber,
        0, 0, 0,
        "", "",
        kNullUniqueId64x2);
  }
  return Status::OK();
}

// ── InstallUnderLock ──────────────────────────────────────────────

Status RemoteCompactionJob::InstallUnderLock(VersionEdit* edit) {
  Status s = db_->GetVersionSet()->LogAndApply(
      cfd_, *cfd_->GetLatestMutableCFOptions(), edit, db_->mutex());
  if (!s.ok())
    return Status::IOError("LogAndApply failed: " + s.ToString());
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
