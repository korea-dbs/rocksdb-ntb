#pragma once
// ============================================================
//  primary_only/remote_compaction_job.h
//  [PRIMARY (srv4) 전용]
// ============================================================

#include <string>
#include <vector>

#include "../shared/compaction_spec.h"
#include "compaction_dispatcher.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

class Compaction;
class DBImpl;
class ColumnFamilyData;
class VersionEdit;

class RemoteCompactionJob {
 public:
  RemoteCompactionJob(DBImpl*               db,
                      Compaction*           compaction,
                      ColumnFamilyData*     cfd,
                      CompactionDispatcher* dispatcher);

  Status Run(const std::vector<SequenceNumber>& snapshots,
             SequenceNumber                     ewc_snapshot,
             SequenceNumber                     preserve_seqnum);

 private:
  // 수신된 output SST 버퍼를 메모리에서 직접 검증 (경로 없음)
  Status ValidateOutputsInMemory(
      const CompactionResult&              result,
      const std::vector<std::vector<char>>& bufs);

  // 검증된 버퍼를 DB 디렉토리에 저장 (MANIFEST 가 경로 요구)
  Status PersistOutputSSTs(
      const CompactionResult&              result,
      const std::vector<std::vector<char>>& bufs,
      const std::string&                   db_path);

  Status BuildVersionEdit(const CompactionResult& result,
                           const CompactionSpec&   spec,
                           VersionEdit*            edit_out);

  Status InstallUnderLock(VersionEdit* edit);

  DBImpl*               db_;
  Compaction*           compaction_;
  ColumnFamilyData*     cfd_;
  CompactionDispatcher* dispatcher_;
};

}  // namespace ROCKSDB_NAMESPACE
