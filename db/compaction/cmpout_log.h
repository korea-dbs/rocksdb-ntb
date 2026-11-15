#pragma once
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include "db/column_family.h"
#include "db/compaction/compaction.h"
#include "db/version_edit.h"

namespace ROCKSDB_NAMESPACE {
inline void LogCmpOut(const char* src, const Compaction* c, const VersionEdit* edit) {
  if (c == nullptr || edit == nullptr) return;
  const Comparator* ucmp = c->column_family_data()->user_comparator();
  const int out_level = c->output_level();
  uint64_t rn = 0, rnp1 = 0;
  size_t rn_files = 0, rnp1_files = 0;
  for (size_t li = 0; li < c->num_input_levels(); li++) {
    for (size_t i = 0; i < c->num_input_files(li); i++) {
      const uint64_t sz = c->input(li, i)->fd.GetFileSize();
      if (c->level(li) == c->start_level()) { rn += sz; rn_files++; } else { rnp1 += sz; rnp1_files++; }
    }
  }
  const auto& gps = c->grandparents();
  size_t nf = 0, gp_files = 0, max_gp = 0, st0 = 0, oat0 = 0;
  uint64_t ob = 0, gp_bytes = 0, minb = UINT64_MAX, maxb = 0;
  for (const auto& nfp : edit->GetNewFiles()) {
    if (nfp.first != out_level) continue;
    const FileMetaData& f = nfp.second;
    const uint64_t sz = f.fd.GetFileSize();
    nf++; ob += sz; minb = std::min(minb, sz); maxb = std::max(maxb, sz);
    if (f.num_entries == 0) st0++;
    if (f.oldest_ancester_time == 0) oat0++;
    const Slice lo = f.smallest.user_key(), hi = f.largest.user_key();
    size_t k = 0;
    for (const FileMetaData* g : gps) {
      if (ucmp->Compare(g->largest.user_key(), lo) < 0 || ucmp->Compare(g->smallest.user_key(), hi) > 0) continue;
      k++; gp_bytes += g->fd.GetFileSize();
    }
    gp_files += k; max_gp = std::max(max_gp, k);
  }
  if (nf == 0) minb = 0;
  fprintf(stderr,
          "[cmpout] src=%s start_level=%d out_level=%d rn_files=%zu rn_mb=%.1f rnp1_files=%zu rnp1_mb=%.1f "
          "out_files=%zu out_mb=%.1f min_mb=%.1f max_mb=%.1f gp=%zu gp_ovl_files=%zu max_gp_per_file=%zu "
          "gp_ovl_mb=%.1f stats0=%zu oat0=%zu\n",
          src, c->start_level(), out_level, rn_files, rn / 1048576.0, rnp1_files, rnp1 / 1048576.0,
          nf, ob / 1048576.0, minb / 1048576.0, maxb / 1048576.0, gps.size(), gp_files, max_gp,
          gp_bytes / 1048576.0, st0, oat0);
}
}  // namespace ROCKSDB_NAMESPACE
