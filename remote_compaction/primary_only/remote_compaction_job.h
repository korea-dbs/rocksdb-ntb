#pragma once
// ============================================================
// ============================================================

#include <functional>
#include <memory>
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
class FSDirectory;

class RemoteCompactionJob
    : public std::enable_shared_from_this<RemoteCompactionJob> {
public:
  RemoteCompactionJob(DBImpl*               db,
                      Compaction*           compaction,
                      ColumnFamilyData*     cfd,
                      CompactionDispatcher* dispatcher,
                      FSDirectory*          db_dir);

  Status Run(const std::vector<SequenceNumber>& snapshots,
             SequenceNumber                     ewc_snapshot,
             SequenceNumber                     preserve_seqnum);

  Status RunAsync(const std::vector<SequenceNumber>&  snapshots,
                  SequenceNumber                      ewc_snapshot,
                  SequenceNumber                      preserve_seqnum,
                  std::shared_ptr<Compaction>         compaction_holder,
                  std::function<void(Status)>         release_cb);

  Status FinishJob(const CompactionSpec&                 spec,
                   const CompactionResult&               result,
                   const std::vector<std::vector<char>>& out_bufs);

private:
  Status ValidateOutputsInMemory(
      const CompactionResult&               result,
      const std::vector<std::vector<char>>& bufs);

  Status PersistOutputSSTs(
      const CompactionResult&               result,
      const std::vector<std::vector<char>>& bufs,
      const std::string&                    db_path);

  Status BuildVersionEdit(const CompactionResult& result,
                           const CompactionSpec&   spec,
                           VersionEdit*            edit_out);

  Status InstallUnderLock(VersionEdit* edit, uint64_t job_id);

  DBImpl*               db_;
  Compaction*           compaction_;
  ColumnFamilyData*     cfd_;
  CompactionDispatcher* dispatcher_;
  FSDirectory*          db_dir_;
};

}  // namespace ROCKSDB_NAMESPACE
