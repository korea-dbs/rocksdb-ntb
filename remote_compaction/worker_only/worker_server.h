#pragma once
// ============================================================
//  worker_only/worker_server.h
//  [WORKER (srv3) 전용]
// ============================================================

#include <atomic>
#include <string>
#include <vector>

#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "table/internal_iterator.h"   // InternalIterator 정의
#include "table/table_reader.h"        // TableReader 정의

#include "../shared/compaction_spec.h"
#include "../shared/ntb_channel.h"

namespace ROCKSDB_NAMESPACE {

struct WorkerOptions {
  uint32_t primary_node_id = 0;
  Env*     env             = nullptr;
};

class WorkerServer {
 public:
  explicit WorkerServer(const WorkerOptions& opts);
  ~WorkerServer();

  Status Connect();
  void   Run();
  void   Stop();

 private:
  // sst_bufs, out_bufs 포함한 시그니처
  Status HandleJob(const CompactionSpec&                 spec,
                   const std::vector<std::vector<char>>& sst_bufs,
                   CompactionResult*                     result_out,
                   std::vector<std::vector<char>>*       out_bufs);

  Status OpenInputIterators(
      const CompactionSpec&                 spec,
      const Options&                        opts,
      const std::vector<std::vector<char>>& sst_bufs,
      std::vector<InternalIterator*>*       iters_out,
      std::vector<TableReader*>*            readers_out);

  Status WriteOutputSSTs(
      const CompactionSpec&           spec,
      const Options&                  opts,
      InternalIterator*               merged_iter,
      CompactionResult*               result_out,
      std::vector<std::vector<char>>* out_bufs);

  const Comparator* FindComparator(const std::string& name);

  WorkerOptions     opts_;
  NtbChannel        channel_;
  std::atomic<bool> stop_{false};
};

}  // namespace ROCKSDB_NAMESPACE
