//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit_handler.h"
#include <cstdio>  // v-plainmf
#include <cstdlib>  // v-plainmf

#include "util/coding.h"

#include <algorithm>
#include <cinttypes>
#include <vector>
#include <sstream>

#include "db/blob/blob_file_reader.h"
#include "db/blob/blob_source.h"
#include "db/version_edit.h"
#include "logging/logging.h"
#include "monitoring/persistent_stats_history.h"
#include "util/udt_util.h"

namespace ROCKSDB_NAMESPACE {
// v-plainmf: MANIFEST 레코드 형식 선택 (기본 = RocksDB 기본 형식)
static bool RcMfPrefixed() {
  static const bool v = [] {
    const char* e = std::getenv("RC_MF_PREFIXED");
    bool r = e && *e == '1';
    std::fprintf(stderr, "[rc] v-plainmf: MANIFEST record format = %s\n", r ? "mseq-prefixed (old dma3)" : "plain (RocksDB default)");
    return r;
  }();
  return v;
}

// gyu modify: v48 N-way 매니페스트 큐
void VersionEditHandlerBase::Iterate(log::Reader& reader,
                                     Status* log_read_status,
                                     const std::vector<log::Reader*>& extra_readers) {
  Slice record, record2;
  std::string scratch, scratch2;
  assert(log_read_status);
  assert(log_read_status->ok());

  log::Reader* err_reader = &reader;
  [[maybe_unused]] size_t recovered_edits = 0;
  Status s = Initialize();

  // v47: v39의 lookahead(1칸) 스트리밍 merge는 "각 스트림이 이미 seq
  // 오름차순으로 정렬돼 있다"는 전제가 있어야만 정확하다. 그런데 v40/v46이
  // mseq 발급 시점(mu 잡고 메모리에 설치할 때)과 디스크에 실제로 기록되는
  // 시점(mu 풀고 각자 진행)을 분리시키면서, 개별 파일 내부조차 그 전제가
  // 깨질 수 있게 됐다 (레코드가 물리적으로 뒤바뀐 순서로 기록됨). 대신
  // 로컬/리모트 두 스트림을 전부 읽어서 메모리에 모은 뒤, seq 기준으로
  // 정렬하고 나서 그 순서대로 replay한다. 물리적 기록 순서가 어떻든
  // 항상 올바른(진짜 발생) 순서로 재생되며, 이 정렬은 DB Open() 시
  // 딱 한 번만 돈다.
  struct PendingRecord {
    uint64_t seq;
    VersionEdit edit;
    bool from_local;
  };
  std::vector<PendingRecord> pending;

  uint64_t rc_plain_seq = 0;  // v-plainmf
  auto drain = [&](log::Reader* rd, bool is_local) {
    Slice rec;
    std::string sc;
    while (s.ok() && log_read_status->ok()) {
      if (is_local && rd->LastRecordEnd() >= max_manifest_read_size_) {
        break;
      }
      if (!rd->ReadRecord(&rec, &sc)) {
        break;
      }
      err_reader = rd;
      Slice body = rec;
      uint64_t seq = 0;
      if (RcMfPrefixed()) {  // v-plainmf: RC_MF_PREFIXED=1 -> 예전 dma3 형식(8바이트 mseq 접두어)
        if (body.size() >= 8) {
          seq = DecodeFixed64(body.data());
          body.remove_prefix(8);
        }
      } else {
        seq = rc_plain_seq++;  // v-plainmf: RocksDB 기본 형식, 파일 안 순서를 그대로 순번으로
      }
      VersionEdit edit;
      s = edit.DecodeFrom(body);
      if (!s.ok()) {
        break;
      }
      if (is_local) {
        // 기존과 동일한 의미: 로컬 파일에서 성공적으로 디코딩된 가장
        // 먼 지점 (apply 성공 여부와 무관, 원본 코드와 동일한 시점에 갱신).
        last_valid_record_end_ = rd->LastRecordEnd();
      }
      pending.push_back(PendingRecord{seq, std::move(edit), is_local});
    }
  };

  if (s.ok()) {
    drain(&reader, true);
  }
  // gyu modify: v48 여러 리모트 스트림을 전부 drain
  for (log::Reader* er : extra_readers) {
    if (!s.ok() || !log_read_status->ok()) {
      break;
    }
    if (er != nullptr) {
      drain(er, false);
    }
  }

  if (s.ok() && log_read_status->ok()) {
    std::stable_sort(pending.begin(), pending.end(),
                     [](const PendingRecord& a, const PendingRecord& b) {
                       return a.seq < b.seq;
                     });

    // gyu modify: v49 - recovery gap 감지. pending은 이미 seq 오름차순
    // 정렬돼있고, 정상이라면(크래시 없었다면) seq가 완전히 연속(dense)
    // 해야 한다 - 모든 edit이 global_manifest_seq_에서 1개씩만 발급받기
    // 때문이다. 중간에 구멍이 있다는 건 그 seq를 배정받은 큐가 크래시로
    // 디스크에 못 쓰고 유실됐다는 뜻이다. 이후 edit들이 그 유실된 edit에
    // 인과적으로 의존할 수 있으므로(예: 없는 파일을 삭제하려는 edit),
    // 구멍이 발견된 지점부터는 아예 replay하지 않고 마지막 gap-free
    // 상태에서 멈춘다 (조용한 롤백).
    bool seq_gap_detected = false;
    uint64_t expected_next_seq = 0;
    bool expected_seq_initialized = false;

    for (auto& pr : pending) {
      if (!s.ok()) {
        break;
      }
      if (seq_gap_detected) {
        break;
      }
      if (!expected_seq_initialized) {
        expected_next_seq = pr.seq;
        expected_seq_initialized = true;
      }
      if (pr.seq != expected_next_seq) {
        fprintf(stderr,
               "[recovery-gap] GAP DETECTED: expected_seq=%llu got_seq=%llu "
               "src=%s -> replay 중단, 마지막 gap-free 상태로 롤백\n",
               (unsigned long long)expected_next_seq,
               (unsigned long long)pr.seq, pr.from_local ? "local" : "remote");
        seq_gap_detected = true;
        break;
      }
      expected_next_seq = pr.seq + 1;

      if (pr.seq < last_seen_manifest_seq_) {
        fprintf(stderr,
               "[manifest_seq] OUT OF ORDER (unexpected after sort)! "
               "seq=%llu last_seen=%llu\n",
               (unsigned long long)pr.seq,
               (unsigned long long)last_seen_manifest_seq_);
      }
      static uint64_t g_seq_record_count = 0;
      g_seq_record_count++;
      fprintf(stderr, "[manifest_seq] record#%llu seq=%llu src=%s (sorted)\n",
             (unsigned long long)g_seq_record_count,
             (unsigned long long)pr.seq, pr.from_local ? "local" : "remote");
      last_seen_manifest_seq_ = pr.seq;

      s = read_buffer_.AddEdit(&pr.edit);
      if (!s.ok()) {
        break;
      }

      ColumnFamilyData* cfd = nullptr;
      if (pr.edit.IsInAtomicGroup()) {
        if (read_buffer_.IsFull()) {
          s = OnAtomicGroupReplayBegin();
          for (size_t i = 0; s.ok() && i < read_buffer_.replay_buffer().size();
               i++) {
            auto& e = read_buffer_.replay_buffer()[i];
            s = ApplyVersionEdit(e, &cfd);
            if (s.ok()) {
              recovered_edits++;
            }
          }
          if (s.ok()) {
            read_buffer_.Clear();
            s = OnAtomicGroupReplayEnd();
          }
        }
      } else {
        s = ApplyVersionEdit(pr.edit, &cfd);
        if (s.ok()) {
          recovered_edits++;
        }
      }
    }
  }
  if (s.ok() && !log_read_status->ok()) {
    s = *log_read_status;
  }

  CheckIterationResult(reader, &s);

  if (!s.ok()) {
    if (s.IsCorruption()) {
      // when we find a Corruption error, something is
      // wrong with the underlying file. in this case we
      // want to report the filename, so in here we append
      // the filename to the Corruption message
      assert(err_reader->file());

      // build a new error message
      std::stringstream message;
      // append previous dynamic state message
      const char* state = s.getState();
      if (state != nullptr) {
        message << state;
        message << ' ';
      }
      // append the filename to the corruption message
      message << " The file " << err_reader->file()->file_name()
              << " may be corrupted.";
      // overwrite the status with the extended status
      s = Status(s.code(), s.subcode(), s.severity(), message.str());
    }
    status_ = s;
  }
  TEST_SYNC_POINT_CALLBACK("VersionEditHandlerBase::Iterate:Finish",
                           &recovered_edits);
}

Status ListColumnFamiliesHandler::ApplyVersionEdit(
    VersionEdit& edit, ColumnFamilyData** /*unused*/) {
  Status s;
  uint32_t cf_id = edit.GetColumnFamily();
  if (edit.IsColumnFamilyAdd()) {
    if (column_family_names_.find(cf_id) != column_family_names_.end()) {
      s = Status::Corruption("Manifest adding the same column family twice");
    } else {
      column_family_names_.insert({cf_id, edit.GetColumnFamilyName()});
    }
  } else if (edit.IsColumnFamilyDrop()) {
    if (column_family_names_.find(cf_id) == column_family_names_.end()) {
      s = Status::Corruption("Manifest - dropping non-existing column family");
    } else {
      column_family_names_.erase(cf_id);
    }
  }
  return s;
}

Status FileChecksumRetriever::FetchFileChecksumList(
    FileChecksumList& file_checksum_list) {
  Status s = Status::OK();
  for (const auto& [cf, file_checksums] : cf_file_checksums_) {
    [[maybe_unused]] const auto& _ = cf;
    for (const auto& [file_number, info] : file_checksums) {
      if (!(s = file_checksum_list.InsertOneFileChecksum(
                file_number, info.first, info.second))
               .ok()) {
        break;
      }
    }
  }
  return s;
}

Status FileChecksumRetriever::ApplyVersionEdit(VersionEdit& edit,
                                               ColumnFamilyData** /*unused*/) {
  uint32_t column_family_id = edit.GetColumnFamily();
  if (edit.IsColumnFamilyDrop()) {
    cf_file_checksums_.erase(column_family_id);
  }
  for (const auto& deleted_file : edit.GetDeletedFiles()) {
    if (cf_file_checksums_.find(column_family_id) == cf_file_checksums_.end()) {
      return Status::NotFound();
    }
    if (cf_file_checksums_[column_family_id].find(deleted_file.second) ==
        cf_file_checksums_[column_family_id].end()) {
      return Status::NotFound();
    }
    cf_file_checksums_[column_family_id].erase(deleted_file.second);
  }
  for (const auto& new_file : edit.GetNewFiles()) {
    cf_file_checksums_[column_family_id].emplace(
        new_file.second.fd.GetNumber(),
        std::make_pair(new_file.second.file_checksum,
                       new_file.second.file_checksum_func_name));
  }
  for (const auto& new_blob_file : edit.GetBlobFileAdditions()) {
    std::string checksum_value = new_blob_file.GetChecksumValue();
    std::string checksum_method = new_blob_file.GetChecksumMethod();
    assert(checksum_value.empty() == checksum_method.empty());
    if (checksum_method.empty()) {
      checksum_value = kUnknownFileChecksum;
      checksum_method = kUnknownFileChecksumFuncName;
    }
    cf_file_checksums_[column_family_id].emplace(
        new_blob_file.GetBlobFileNumber(),
        std::make_pair(checksum_value, checksum_method));
  }
  return Status::OK();
}

VersionEditHandler::VersionEditHandler(
    bool read_only, std::vector<ColumnFamilyDescriptor> column_families,
    VersionSet* version_set, bool track_found_and_missing_files,
    bool no_error_if_files_missing, const std::shared_ptr<IOTracer>& io_tracer,
    const ReadOptions& read_options, bool skip_load_table_files,
    bool allow_incomplete_valid_version,
    EpochNumberRequirement epoch_number_requirement)
    : VersionEditHandlerBase(read_options),
      read_only_(read_only),
      column_families_(std::move(column_families)),
      version_set_(version_set),
      track_found_and_missing_files_(track_found_and_missing_files),
      no_error_if_files_missing_(no_error_if_files_missing),
      io_tracer_(io_tracer),
      skip_load_table_files_(skip_load_table_files),
      initialized_(false),
      allow_incomplete_valid_version_(allow_incomplete_valid_version),
      epoch_number_requirement_(epoch_number_requirement) {
  assert(version_set_ != nullptr);
}

Status VersionEditHandler::Initialize() {
  Status s;
  if (!initialized_) {
    for (const auto& cf_desc : column_families_) {
      name_to_options_.emplace(cf_desc.name, cf_desc.options);
    }
    auto default_cf_iter = name_to_options_.find(kDefaultColumnFamilyName);
    if (default_cf_iter == name_to_options_.end()) {
      s = Status::InvalidArgument("Default column family not specified");
    }
    if (s.ok()) {
      VersionEdit default_cf_edit;
      default_cf_edit.AddColumnFamily(kDefaultColumnFamilyName);
      default_cf_edit.SetColumnFamily(0);
      ColumnFamilyData* cfd =
          CreateCfAndInit(default_cf_iter->second, default_cf_edit);
      assert(cfd != nullptr);
#ifdef NDEBUG
      (void)cfd;
#endif
      initialized_ = true;
    }
  }
  return s;
}

Status VersionEditHandler::ApplyVersionEdit(VersionEdit& edit,
                                            ColumnFamilyData** cfd) {
  Status s;
  if (edit.IsColumnFamilyAdd()) {
    s = OnColumnFamilyAdd(edit, cfd);
  } else if (edit.IsColumnFamilyDrop()) {
    s = OnColumnFamilyDrop(edit, cfd);
  } else if (edit.IsWalAddition()) {
    s = OnWalAddition(edit);
  } else if (edit.IsWalDeletion()) {
    s = OnWalDeletion(edit);
  } else {
    s = OnNonCfOperation(edit, cfd);
  }
  if (s.ok()) {
    assert(cfd != nullptr);
    s = ExtractInfoFromVersionEdit(*cfd, edit);
  }
  return s;
}

Status VersionEditHandler::OnColumnFamilyAdd(VersionEdit& edit,
                                             ColumnFamilyData** cfd) {
  bool do_not_open_cf = false;
  bool cf_in_builders = false;
  CheckColumnFamilyId(edit, &do_not_open_cf, &cf_in_builders);

  assert(cfd != nullptr);
  *cfd = nullptr;
  const std::string& cf_name = edit.GetColumnFamilyName();
  Status s;
  if (cf_in_builders || do_not_open_cf) {
    s = Status::Corruption("MANIFEST adding the same column family twice: " +
                           cf_name);
  }
  if (s.ok()) {
    auto cf_options = name_to_options_.find(cf_name);
    // implicitly add persistent_stats column family without requiring user
    // to specify
    ColumnFamilyData* tmp_cfd = nullptr;
    bool is_persistent_stats_column_family =
        cf_name.compare(kPersistentStatsColumnFamilyName) == 0;
    if (cf_options == name_to_options_.end() &&
        !is_persistent_stats_column_family) {
      do_not_open_column_families_.emplace(edit.GetColumnFamily(), cf_name);
    } else {
      if (is_persistent_stats_column_family) {
        ColumnFamilyOptions cfo;
        OptimizeForPersistentStats(&cfo);
        tmp_cfd = CreateCfAndInit(cfo, edit);
      } else {
        tmp_cfd = CreateCfAndInit(cf_options->second, edit);
      }
      *cfd = tmp_cfd;
    }
  }
  return s;
}

Status VersionEditHandler::OnColumnFamilyDrop(VersionEdit& edit,
                                              ColumnFamilyData** cfd) {
  bool do_not_open_cf = false;
  bool cf_in_builders = false;
  CheckColumnFamilyId(edit, &do_not_open_cf, &cf_in_builders);

  assert(cfd != nullptr);
  *cfd = nullptr;
  ColumnFamilyData* tmp_cfd = nullptr;
  Status s;
  if (cf_in_builders) {
    tmp_cfd = DestroyCfAndCleanup(edit);
  } else if (do_not_open_cf) {
    do_not_open_column_families_.erase(edit.GetColumnFamily());
  } else {
    s = Status::Corruption("MANIFEST - dropping non-existing column family");
  }
  *cfd = tmp_cfd;
  return s;
}

Status VersionEditHandler::OnWalAddition(VersionEdit& edit) {
  assert(edit.IsWalAddition());
  return version_set_->wals_.AddWals(edit.GetWalAdditions());
}

Status VersionEditHandler::OnWalDeletion(VersionEdit& edit) {
  assert(edit.IsWalDeletion());
  return version_set_->wals_.DeleteWalsBefore(
      edit.GetWalDeletion().GetLogNumber());
}

Status VersionEditHandler::OnNonCfOperation(VersionEdit& edit,
                                            ColumnFamilyData** cfd) {
  bool do_not_open_cf = false;
  bool cf_in_builders = false;
  CheckColumnFamilyId(edit, &do_not_open_cf, &cf_in_builders);

  assert(cfd != nullptr);
  *cfd = nullptr;
  Status s;
  if (!do_not_open_cf) {
    if (!cf_in_builders) {
      s = Status::Corruption(
          "MANIFEST record referencing unknown column family");
    }
    ColumnFamilyData* tmp_cfd = nullptr;
    if (s.ok()) {
      tmp_cfd = version_set_->GetColumnFamilySet()->GetColumnFamily(
          edit.GetColumnFamily());
      assert(tmp_cfd != nullptr);
      // It's important to handle file boundaries before `MaybeCreateVersion`
      // because `VersionEditHandlerPointInTime::MaybeCreateVersion` does
      // `FileMetaData` verification that involves the file boundaries.
      // All `VersionEditHandlerBase` subclasses that need to deal with
      // `FileMetaData` for new files are also subclasses of
      // `VersionEditHandler`, so it's sufficient to do the file boundaries
      // handling in this method.
      s = MaybeHandleFileBoundariesForNewFiles(edit, tmp_cfd);
      if (!s.ok()) {
        return s;
      }
      s = MaybeCreateVersionBeforeApplyEdit(edit, tmp_cfd,
                                            /*force_create_version=*/false);
    }
    *cfd = tmp_cfd;
  }
  return s;
}

void VersionEditHandler::CheckColumnFamilyId(const VersionEdit& edit,
                                             bool* do_not_open_cf,
                                             bool* cf_in_builders) const {
  assert(do_not_open_cf != nullptr);
  assert(cf_in_builders != nullptr);
  // Not found means that user didn't supply that column
  // family option AND we encountered column family add
  // record. Once we encounter column family drop record,
  // we will delete the column family from
  // do_not_open_column_families_.
  uint32_t cf_id = edit.GetColumnFamily();
  bool in_do_not_open = do_not_open_column_families_.find(cf_id) !=
                        do_not_open_column_families_.end();
  // in builders means that user supplied that column family
  // option AND that we encountered column family add record
  bool in_builders = builders_.find(cf_id) != builders_.end();
  // They cannot both be true
  assert(!(in_do_not_open && in_builders));
  *do_not_open_cf = in_do_not_open;
  *cf_in_builders = in_builders;
}

void VersionEditHandler::CheckIterationResult(const log::Reader& reader,
                                              Status* s) {
  assert(s != nullptr);
  if (!s->ok()) {
    // Do nothing here.
  } else if (!version_edit_params_.HasLogNumber() ||
             !version_edit_params_.HasNextFile() ||
             !version_edit_params_.HasLastSequence()) {
    std::string msg("no ");
    if (!version_edit_params_.HasLogNumber()) {
      msg.append("log_file_number, ");
    }
    if (!version_edit_params_.HasNextFile()) {
      msg.append("next_file_number, ");
    }
    if (!version_edit_params_.HasLastSequence()) {
      msg.append("last_sequence, ");
    }
    msg = msg.substr(0, msg.size() - 2);
    msg.append(" entry in MANIFEST");
    *s = Status::Corruption(msg);
  }
  // There were some column families in the MANIFEST that weren't specified
  // in the argument. This is OK in read_only mode
  if (s->ok() && MustOpenAllColumnFamilies() &&
      !do_not_open_column_families_.empty()) {
    std::string msg;
    for (const auto& cf : do_not_open_column_families_) {
      msg.append(", ");
      msg.append(cf.second);
    }
    msg = msg.substr(2);
    *s = Status::InvalidArgument("Column families not opened: " + msg);
  }
  if (s->ok()) {
    version_set_->GetColumnFamilySet()->UpdateMaxColumnFamily(
        version_edit_params_.GetMaxColumnFamily());
    version_set_->MarkMinLogNumberToKeep(
        version_edit_params_.GetMinLogNumberToKeep());
    version_set_->MarkFileNumberUsed(version_edit_params_.GetPrevLogNumber());
    version_set_->MarkFileNumberUsed(version_edit_params_.GetLogNumber());
    for (auto* cfd : *(version_set_->GetColumnFamilySet())) {
      if (cfd->IsDropped()) {
        continue;
      }
      auto builder_iter = builders_.find(cfd->GetID());
      assert(builder_iter != builders_.end());
      auto* builder = builder_iter->second->version_builder();
      if (!builder->CheckConsistencyForNumLevels()) {
        *s = Status::InvalidArgument(
            "db has more levels than options.num_levels");
        break;
      }
    }
  }
  if (s->ok()) {
    for (auto* cfd : *(version_set_->GetColumnFamilySet())) {
      if (cfd->IsDropped()) {
        continue;
      }
      if (version_set_->unchanging()) {
        cfd->table_cache()->SetTablesAreImmortal();
      }
      *s = LoadTables(cfd, /*prefetch_index_and_filter_in_cache=*/false,
                      /*is_initial_load=*/true);
      if (!s->ok()) {
        // If s is IOError::PathNotFound, then we mark the db as corrupted.
        if (s->IsPathNotFound()) {
          *s = Status::Corruption("Corruption: " + s->ToString());
        }
        break;
      }
    }
  }

  if (s->ok()) {
    for (auto* cfd : *(version_set_->column_family_set_)) {
      if (cfd->IsDropped()) {
        continue;
      }
      assert(cfd->initialized());
      VersionEdit edit;
      *s = MaybeCreateVersionBeforeApplyEdit(edit, cfd,
                                             /*force_create_version=*/true);
      if (!s->ok()) {
        break;
      }
    }
  }
  if (s->ok()) {
    version_set_->manifest_file_size_ = reader.GetReadOffset();
    assert(version_set_->manifest_file_size_ > 0);
    version_set_->manifest_last_valid_record_end_ = last_valid_record_end_;
    version_set_->next_file_number_.store(version_edit_params_.GetNextFile() +
                                          1);
    SequenceNumber last_seq = version_edit_params_.GetLastSequence();
    assert(last_seq != kMaxSequenceNumber);
    if (last_seq != kMaxSequenceNumber &&
        last_seq > version_set_->last_allocated_sequence_.load()) {
      version_set_->last_allocated_sequence_.store(last_seq);
    }
    if (last_seq != kMaxSequenceNumber &&
        last_seq > version_set_->last_published_sequence_.load()) {
      version_set_->last_published_sequence_.store(last_seq);
    }
    if (last_seq != kMaxSequenceNumber &&
        last_seq > version_set_->last_sequence_.load()) {
      version_set_->last_sequence_.store(last_seq);
    }
    if (last_seq != kMaxSequenceNumber &&
        last_seq > version_set_->descriptor_last_sequence_) {
      // This is the maximum last sequence of all `VersionEdit`s iterated. It
      // may be greater than the maximum `largest_seqno` of all files in case
      // the newest data referred to by the MANIFEST has been dropped or had its
      // sequence number zeroed through compaction.
      version_set_->descriptor_last_sequence_ = last_seq;
    }
    version_set_->prev_log_number_ = version_edit_params_.GetPrevLogNumber();
  }
}

ColumnFamilyData* VersionEditHandler::CreateCfAndInit(
    const ColumnFamilyOptions& cf_options, const VersionEdit& edit) {
  uint32_t cf_id = edit.GetColumnFamily();
  ColumnFamilyData* cfd = version_set_->CreateColumnFamily(
      cf_options, read_options_, &edit, read_only_);
  assert(cfd != nullptr);
  cfd->set_initialized();
  assert(builders_.find(cf_id) == builders_.end());
  builders_.emplace(cf_id, VersionBuilderUPtr(new BaseReferencedVersionBuilder(
                               cfd, this, track_found_and_missing_files_,
                               allow_incomplete_valid_version_)));
  return cfd;
}

ColumnFamilyData* VersionEditHandler::DestroyCfAndCleanup(
    const VersionEdit& edit) {
  uint32_t cf_id = edit.GetColumnFamily();
  auto builder_iter = builders_.find(cf_id);
  assert(builder_iter != builders_.end());
  builders_.erase(builder_iter);
  ColumnFamilyData* ret =
      version_set_->GetColumnFamilySet()->GetColumnFamily(cf_id);
  assert(ret != nullptr);
  ret->SetDropped();
  ret->UnrefAndTryDelete();
  ret = nullptr;
  return ret;
}

Status VersionEditHandler::MaybeCreateVersionBeforeApplyEdit(
    const VersionEdit& edit, ColumnFamilyData* cfd, bool force_create_version) {
  assert(cfd->initialized());
  Status s;
  auto builder_iter = builders_.find(cfd->GetID());
  assert(builder_iter != builders_.end());
  auto* builder = builder_iter->second->version_builder();
  if (force_create_version) {
    auto* v = new Version(cfd, version_set_, version_set_->file_options_,
                          cfd->GetLatestMutableCFOptions(), io_tracer_,
                          version_set_->current_version_number_++,
                          epoch_number_requirement_);
    s = builder->SaveTo(v->storage_info());
    if (s.ok()) {
      // Install new version
      v->PrepareAppend(
          read_options_,
          !(version_set_->db_options_->skip_stats_update_on_db_open));
      version_set_->AppendVersion(cfd, v);
    } else {
      delete v;
    }
  }
  s = builder->Apply(&edit);
  return s;
}

Status VersionEditHandler::LoadTables(ColumnFamilyData* cfd,
                                      bool prefetch_index_and_filter_in_cache,
                                      bool is_initial_load) {
  bool skip_load_table_files = skip_load_table_files_;
  TEST_SYNC_POINT_CALLBACK(
      "VersionEditHandler::LoadTables:skip_load_table_files",
      &skip_load_table_files);
  if (skip_load_table_files) {
    return Status::OK();
  }
  assert(cfd != nullptr);
  assert(!cfd->IsDropped());
  auto builder_iter = builders_.find(cfd->GetID());
  assert(builder_iter != builders_.end());
  assert(builder_iter->second != nullptr);
  VersionBuilder* builder = builder_iter->second->version_builder();
  assert(builder);
  const auto& moptions = cfd->GetLatestMutableCFOptions();
  Status s = builder->LoadTableHandlers(
      cfd->internal_stats(),
      version_set_->db_options_->max_file_opening_threads,
      prefetch_index_and_filter_in_cache, is_initial_load, moptions,
      MaxFileSizeForL0MetaPin(moptions), read_options_);
  if ((s.IsPathNotFound() || s.IsCorruption()) && no_error_if_files_missing_) {
    s = Status::OK();
  }
  if (!s.ok() && !version_set_->db_options_->paranoid_checks) {
    s = Status::OK();
  }
  return s;
}

Status VersionEditHandler::ExtractInfoFromVersionEdit(ColumnFamilyData* cfd,
                                                      const VersionEdit& edit) {
  Status s;
  if (edit.HasDbId()) {
    version_set_->db_id_ = edit.GetDbId();
    version_edit_params_.SetDBId(edit.GetDbId());
  }
  if (cfd != nullptr) {
    if (edit.HasLogNumber()) {
      if (cfd->GetLogNumber() > edit.GetLogNumber()) {
        ROCKS_LOG_WARN(
            version_set_->db_options()->info_log,
            "MANIFEST corruption detected, but ignored - Log numbers in "
            "records NOT monotonically increasing");
      } else {
        cfd->SetLogNumber(edit.GetLogNumber());
        version_edit_params_.SetLogNumber(edit.GetLogNumber());
      }
    }
    if (edit.HasComparatorName()) {
      bool mark_sst_files_has_no_udt = false;
      // If `persist_user_defined_timestamps` flag is recorded in manifest, it
      // is guaranteed to be in the same VersionEdit as comparator. Otherwise,
      // it's not recorded and it should have default value true.
      s = ValidateUserDefinedTimestampsOptions(
          cfd->user_comparator(), edit.GetComparatorName(),
          cfd->ioptions().persist_user_defined_timestamps,
          edit.GetPersistUserDefinedTimestamps(), &mark_sst_files_has_no_udt);
      if (!s.ok() && cf_to_cmp_names_) {
        cf_to_cmp_names_->emplace(cfd->GetID(), edit.GetComparatorName());
      }
      if (mark_sst_files_has_no_udt) {
        cfds_to_mark_no_udt_.insert(cfd->GetID());
      }
    }
    if (edit.HasFullHistoryTsLow()) {
      const std::string& new_ts = edit.GetFullHistoryTsLow();
      cfd->SetFullHistoryTsLow(new_ts);
    }
  }

  if (s.ok()) {
    if (edit.HasPrevLogNumber()) {
      version_edit_params_.SetPrevLogNumber(edit.GetPrevLogNumber());
    }
    if (edit.HasNextFile()) {
      // gyu modify: v49 - N-way 큐에서는 여러 큐가 동시에 각자 다른
      // 시점의 next_file_number_ 스냅샷을 자기 배치 마지막 edit에
      // 남기므로, mseq 순서로 replay해도 NextFile 값이 시간순으로
      // 단조증가한다는 보장이 없다. 그냥 덮어쓰면 나중에 처리된 값이
      // 더 작을 때 next_file_number_가 실제보다 작게 복원돼서, 이미
      // 쓰인 파일 번호를 재사용하는 corruption으로 이어진다. 절대
      // 줄어들지 않도록 max로 갱신한다.
      version_edit_params_.SetNextFile(
          std::max(version_edit_params_.GetNextFile(), edit.GetNextFile()));
    }
    if (edit.HasMaxColumnFamily()) {
      version_edit_params_.SetMaxColumnFamily(edit.GetMaxColumnFamily());
    }
    if (edit.HasMinLogNumberToKeep()) {
      version_edit_params_.SetMinLogNumberToKeep(
          std::max(version_edit_params_.GetMinLogNumberToKeep(),
                   edit.GetMinLogNumberToKeep()));
    }
    if (edit.HasLastSequence()) {
      // `VersionEdit::last_sequence_`s are assumed to be non-decreasing. This
      // is legacy behavior that cannot change without breaking downgrade
      // compatibility.
      assert(!version_edit_params_.HasLastSequence() ||
             version_edit_params_.GetLastSequence() <= edit.GetLastSequence());
      version_edit_params_.SetLastSequence(edit.GetLastSequence());
    }
    if (edit.HasLastCompactedManifestFileSize()) {
      version_set_->last_compacted_manifest_file_size_ =
          edit.GetLastCompactedManifestFileSize();
      version_set_->TuneMaxManifestFileSize();
    }
    if (!version_edit_params_.HasPrevLogNumber()) {
      version_edit_params_.SetPrevLogNumber(0);
    }
  }
  return s;
}

Status VersionEditHandler::MaybeHandleFileBoundariesForNewFiles(
    VersionEdit& edit, const ColumnFamilyData* cfd) {
  if (edit.GetNewFiles().empty()) {
    return Status::OK();
  }
  auto ucmp = cfd->user_comparator();
  assert(ucmp);
  size_t ts_sz = ucmp->timestamp_size();
  if (ts_sz == 0) {
    return Status::OK();
  }

  VersionEdit::NewFiles& new_files = edit.GetMutableNewFiles();
  assert(!new_files.empty());
  // If true, enabling user-defined timestamp is detected for this column
  // family. All its existing SST files need to have the file boundaries handled
  // and their `persist_user_defined_timestamps` flag set to false regardless of
  // its existing value.
  bool mark_existing_ssts_with_no_udt =
      cfds_to_mark_no_udt_.find(cfd->GetID()) != cfds_to_mark_no_udt_.end();
  bool file_boundaries_need_handling = false;
  for (auto& new_file : new_files) {
    FileMetaData& meta = new_file.second;
    if (meta.user_defined_timestamps_persisted &&
        !mark_existing_ssts_with_no_udt) {
      // `FileMetaData.user_defined_timestamps_persisted` field is the value of
      // the flag `AdvancedColumnFamilyOptions.persist_user_defined_timestamps`
      // at the time when the SST file was created. As a result, all added SST
      // files in one `VersionEdit` should have the same value for it.
      if (file_boundaries_need_handling) {
        return Status::Corruption(
            "New files in one VersionEdit has different "
            "user_defined_timestamps_persisted value.");
      }
      break;
    }
    file_boundaries_need_handling = true;
    assert(!meta.user_defined_timestamps_persisted ||
           mark_existing_ssts_with_no_udt);
    if (mark_existing_ssts_with_no_udt) {
      meta.user_defined_timestamps_persisted = false;
    }
    std::string smallest_buf;
    std::string largest_buf;
    Slice largest_slice = meta.largest.Encode();
    PadInternalKeyWithMinTimestamp(&smallest_buf, meta.smallest.Encode(),
                                   ts_sz);
    auto largest_footer = ExtractInternalKeyFooter(largest_slice);
    if (largest_footer == kRangeTombstoneSentinel) {
      PadInternalKeyWithMaxTimestamp(&largest_buf, largest_slice, ts_sz);
    } else {
      PadInternalKeyWithMinTimestamp(&largest_buf, largest_slice, ts_sz);
    }
    meta.smallest.DecodeFrom(smallest_buf);
    meta.largest.DecodeFrom(largest_buf);
  }
  return Status::OK();
}

VersionEditHandlerPointInTime::VersionEditHandlerPointInTime(
    bool read_only, std::vector<ColumnFamilyDescriptor> column_families,
    VersionSet* version_set, const std::shared_ptr<IOTracer>& io_tracer,
    const ReadOptions& read_options, bool allow_incomplete_valid_version,
    EpochNumberRequirement epoch_number_requirement)
    : VersionEditHandler(read_only, column_families, version_set,
                         /*track_found_and_missing_files=*/true,
                         /*no_error_if_files_missing=*/true, io_tracer,
                         read_options, allow_incomplete_valid_version,
                         epoch_number_requirement) {}

VersionEditHandlerPointInTime::~VersionEditHandlerPointInTime() {
  for (const auto& cfid_and_version : atomic_update_versions_) {
    delete cfid_and_version.second;
  }
  for (const auto& elem : versions_) {
    delete elem.second;
  }
  versions_.clear();
}

Status VersionEditHandlerPointInTime::OnAtomicGroupReplayBegin() {
  if (in_atomic_group_) {
    return Status::Corruption("unexpected AtomicGroup start");
  }

  // The AtomicGroup that is about to begin may block column families in a valid
  // state from saving any more updates. So we should save any valid states
  // before proceeding.
  for (const auto& cfid_and_builder : builders_) {
    ColumnFamilyData* cfd = version_set_->GetColumnFamilySet()->GetColumnFamily(
        cfid_and_builder.first);
    assert(!cfd->IsDropped());
    assert(cfd->initialized());
    VersionEdit edit;
    Status s = MaybeCreateVersionBeforeApplyEdit(
        edit, cfd, true /* force_create_version */);
    if (!s.ok()) {
      return s;
    }
  }

  // An old AtomicGroup is incomplete. Throw away the versions that failed to
  // complete it. They must not be used for completing the upcoming
  // AtomicGroup since they are too old.
  for (auto& cfid_and_version : atomic_update_versions_) {
    delete cfid_and_version.second;
  }

  in_atomic_group_ = true;
  // We lazily assume the column families that exist at this point are all
  // involved in the AtomicGroup. Overestimating the scope of the AtomicGroup
  // will sometimes cause less data to be recovered, which is fine for
  // best-effort recovery.
  atomic_update_versions_.clear();
  for (const auto& cfid_and_builder : builders_) {
    atomic_update_versions_[cfid_and_builder.first] = nullptr;
  }
  atomic_update_versions_missing_ = atomic_update_versions_.size();
  return Status::OK();
}

Status VersionEditHandlerPointInTime::OnAtomicGroupReplayEnd() {
  if (!in_atomic_group_) {
    return Status::Corruption("unexpected AtomicGroup end");
  }
  in_atomic_group_ = false;

  // The AtomicGroup must not have changed the column families. We don't support
  // CF adds or drops in an AtomicGroup.
  if (builders_.size() != atomic_update_versions_.size()) {
    return Status::Corruption("unexpected CF change in AtomicGroup");
  }
  for (const auto& cfid_and_builder : builders_) {
    if (atomic_update_versions_.find(cfid_and_builder.first) ==
        atomic_update_versions_.end()) {
      return Status::Corruption("unexpected CF add in AtomicGroup");
    }
  }
  for (const auto& cfid_and_version : atomic_update_versions_) {
    if (builders_.find(cfid_and_version.first) == builders_.end()) {
      return Status::Corruption("unexpected CF drop in AtomicGroup");
    }
  }
  return Status::OK();
}

void VersionEditHandlerPointInTime::CheckIterationResult(
    const log::Reader& reader, Status* s) {
  VersionEditHandler::CheckIterationResult(reader, s);
  assert(s != nullptr);
  if (s->ok()) {
    for (auto* cfd : *(version_set_->column_family_set_)) {
      if (cfd->IsDropped()) {
        continue;
      }
      assert(cfd->initialized());
      auto v_iter = versions_.find(cfd->GetID());
      auto builder_iter = builders_.find(cfd->GetID());
      if (v_iter != versions_.end()) {
        assert(v_iter->second != nullptr);
        assert(builder_iter != builders_.end());

        version_set_->AppendVersion(cfd, v_iter->second);
        versions_.erase(v_iter);
        // Let's clear found_files, since any files in that are part of the
        // installed Version. Any files that got obsoleted would have already
        // been moved to intermediate_files_
        builder_iter->second->version_builder()->ClearFoundFiles();
      }
    }
  } else {
    for (const auto& elem : versions_) {
      delete elem.second;
    }
    versions_.clear();
  }
}

ColumnFamilyData* VersionEditHandlerPointInTime::DestroyCfAndCleanup(
    const VersionEdit& edit) {
  ColumnFamilyData* cfd = VersionEditHandler::DestroyCfAndCleanup(edit);
  uint32_t cfid = edit.GetColumnFamily();
  if (AtomicUpdateVersionsContains(cfid)) {
    AtomicUpdateVersionsDropCf(cfid);
    if (AtomicUpdateVersionsCompleted()) {
      AtomicUpdateVersionsApply();
    }
  }
  auto v_iter = versions_.find(cfid);
  if (v_iter != versions_.end()) {
    delete v_iter->second;
    versions_.erase(v_iter);
  }
  return cfd;
}

Status VersionEditHandlerPointInTime::MaybeCreateVersionBeforeApplyEdit(
    const VersionEdit& edit, ColumnFamilyData* cfd, bool force_create_version) {
  TEST_SYNC_POINT(
      "VersionEditHandlerPointInTime::MaybeCreateVersionBeforeApplyEdit:"
      "Begin1");
  TEST_SYNC_POINT(
      "VersionEditHandlerPointInTime::MaybeCreateVersionBeforeApplyEdit:"
      "Begin2");
  assert(cfd != nullptr);
  if (!force_create_version) {
    assert(edit.GetColumnFamily() == cfd->GetID());
  }

  bool missing_info = !version_edit_params_.HasLogNumber() ||
                      !version_edit_params_.HasNextFile() ||
                      !version_edit_params_.HasLastSequence();

  Status s;
  auto builder_iter = builders_.find(cfd->GetID());
  assert(builder_iter != builders_.end());
  VersionBuilder* builder = builder_iter->second->version_builder();
  const bool valid_pit_before_edit = builder->ValidVersionAvailable();
  builder->CreateOrReplaceSavePoint();
  s = builder->Apply(&edit);
  const bool valid_pit_after_edit = builder->ValidVersionAvailable();

  // A new version will be created if:
  // 1) no error has occurred so far, and
  // 2) log_number_, next_file_number_ and last_sequence_ are known, and
  // 3) not in an AtomicGroup
  // 4) any of the following:
  //   a) a valid Version is available before applying the edit
  //      and a valid Version is not available after the edit.
  //   b) a valid Version is available after the edit and the
  //      caller explicitly request that a new version be created.
  if (s.ok() && !missing_info && !in_atomic_group_ &&
      ((!valid_pit_after_edit && valid_pit_before_edit) ||
       (valid_pit_after_edit && force_create_version))) {
    const auto& mopts = cfd->GetLatestMutableCFOptions();
    auto* version = new Version(
        cfd, version_set_, version_set_->file_options_, mopts, io_tracer_,
        version_set_->current_version_number_++, epoch_number_requirement_);
    s = builder->LoadSavePointTableHandlers(
        cfd->internal_stats(),
        version_set_->db_options_->max_file_opening_threads, false, true, mopts,
        MaxFileSizeForL0MetaPin(mopts), read_options_);
    if (!s.ok()) {
      delete version;
      if (s.IsCorruption()) {
        s = Status::OK();
      }
      return s;
    }
    s = builder->SaveSavePointTo(version->storage_info());
    if (s.ok()) {
      if (AtomicUpdateVersionsContains(cfd->GetID())) {
        AtomicUpdateVersionsPut(version);
        if (AtomicUpdateVersionsCompleted()) {
          AtomicUpdateVersionsApply();
        }
      } else {
        version->PrepareAppend(
            read_options_,
            !version_set_->db_options_->skip_stats_update_on_db_open);
        auto v_iter = versions_.find(cfd->GetID());
        if (v_iter != versions_.end()) {
          delete v_iter->second;
          v_iter->second = version;
        } else {
          versions_.emplace(cfd->GetID(), version);
        }
      }
    } else {
      delete version;
    }
  }

  builder->ClearSavePoint();
  return s;
}

Status VersionEditHandlerPointInTime::VerifyFile(ColumnFamilyData* cfd,
                                                 const std::string& fpath,
                                                 int level,
                                                 const FileMetaData& fmeta) {
  return version_set_->VerifyFileMetadata(read_options_, cfd, fpath, level,
                                          fmeta);
}

Status VersionEditHandlerPointInTime::VerifyBlobFile(
    ColumnFamilyData* cfd, uint64_t blob_file_num,
    const BlobFileAddition& blob_addition) {
  BlobSource* blob_source = cfd->blob_source();
  assert(blob_source);
  CacheHandleGuard<BlobFileReader> blob_file_reader;

  Status s = blob_source->GetBlobFileReader(read_options_, blob_file_num,
                                            &blob_file_reader);
  if (!s.ok()) {
    return s;
  }
  // TODO: verify checksum
  (void)blob_addition;
  return s;
}

Status VersionEditHandlerPointInTime::LoadTables(
    ColumnFamilyData* /*cfd*/, bool /*prefetch_index_and_filter_in_cache*/,
    bool /*is_initial_load*/) {
  return Status::OK();
}

bool VersionEditHandlerPointInTime::HasMissingFiles() const {
  for (const auto& builder : builders_) {
    if (builder.second->version_builder()->HasMissingFiles()) {
      return true;
    }
  }
  return false;
}

bool VersionEditHandlerPointInTime::AtomicUpdateVersionsCompleted() {
  return atomic_update_versions_missing_ == 0;
}

bool VersionEditHandlerPointInTime::AtomicUpdateVersionsContains(
    uint32_t cfid) {
  return atomic_update_versions_.find(cfid) != atomic_update_versions_.end();
}

void VersionEditHandlerPointInTime::AtomicUpdateVersionsDropCf(uint32_t cfid) {
  assert(!AtomicUpdateVersionsCompleted());
  auto atomic_update_versions_iter = atomic_update_versions_.find(cfid);
  assert(atomic_update_versions_iter != atomic_update_versions_.end());
  if (atomic_update_versions_iter->second == nullptr) {
    atomic_update_versions_missing_--;
  } else {
    delete atomic_update_versions_iter->second;
  }
  atomic_update_versions_.erase(atomic_update_versions_iter);
}

void VersionEditHandlerPointInTime::AtomicUpdateVersionsPut(Version* version) {
  assert(!AtomicUpdateVersionsCompleted());
  auto atomic_update_versions_iter =
      atomic_update_versions_.find(version->cfd()->GetID());
  assert(atomic_update_versions_iter != atomic_update_versions_.end());
  if (atomic_update_versions_iter->second == nullptr) {
    atomic_update_versions_missing_--;
  } else {
    delete atomic_update_versions_iter->second;
  }
  atomic_update_versions_iter->second = version;
}

void VersionEditHandlerPointInTime::AtomicUpdateVersionsApply() {
  assert(AtomicUpdateVersionsCompleted());
  for (const auto& cfid_and_version : atomic_update_versions_) {
    uint32_t cfid = cfid_and_version.first;
    Version* version = cfid_and_version.second;
    assert(version != nullptr);
    version->PrepareAppend(
        read_options_,
        !version_set_->db_options_->skip_stats_update_on_db_open);
    auto versions_iter = versions_.find(cfid);
    if (versions_iter != versions_.end()) {
      delete versions_iter->second;
      versions_iter->second = version;
    } else {
      versions_.emplace(cfid, version);
    }
  }
  atomic_update_versions_.clear();
}

Status ManifestTailer::Initialize() {
  if (Mode::kRecovery == mode_) {
    return VersionEditHandler::Initialize();
  }
  assert(Mode::kCatchUp == mode_);
  Status s;
  if (!initialized_) {
    ColumnFamilySet* cfd_set = version_set_->GetColumnFamilySet();
    assert(cfd_set);
    ColumnFamilyData* default_cfd = cfd_set->GetDefault();
    assert(default_cfd);
    auto builder_iter = builders_.find(default_cfd->GetID());
    assert(builder_iter != builders_.end());

    Version* dummy_version = default_cfd->dummy_versions();
    assert(dummy_version);
    Version* base_version = dummy_version->Next();
    assert(base_version);
    base_version->Ref();
    VersionBuilderUPtr new_builder(new BaseReferencedVersionBuilder(
        default_cfd, base_version, this, track_found_and_missing_files_,
        allow_incomplete_valid_version_));
    builder_iter->second = std::move(new_builder);

    initialized_ = true;
  }
  return s;
}

Status ManifestTailer::ApplyVersionEdit(VersionEdit& edit,
                                        ColumnFamilyData** cfd) {
  Status s = VersionEditHandler::ApplyVersionEdit(edit, cfd);
  if (s.ok()) {
    assert(cfd);
    if (*cfd) {
      cfds_changed_.insert(*cfd);
    }
  }
  return s;
}

Status ManifestTailer::OnColumnFamilyAdd(VersionEdit& edit,
                                         ColumnFamilyData** cfd) {
  if (Mode::kRecovery == mode_) {
    return VersionEditHandler::OnColumnFamilyAdd(edit, cfd);
  }
  assert(Mode::kCatchUp == mode_);
  ColumnFamilySet* cfd_set = version_set_->GetColumnFamilySet();
  assert(cfd_set);
  ColumnFamilyData* tmp_cfd = cfd_set->GetColumnFamily(edit.GetColumnFamily());
  assert(cfd);
  *cfd = tmp_cfd;
  if (!tmp_cfd) {
    // For now, ignore new column families created after Recover() succeeds.
    return Status::OK();
  }
  auto builder_iter = builders_.find(edit.GetColumnFamily());
  assert(builder_iter != builders_.end());

  Version* dummy_version = tmp_cfd->dummy_versions();
  assert(dummy_version);
  Version* base_version = dummy_version->Next();
  assert(base_version);
  base_version->Ref();
  VersionBuilderUPtr new_builder(new BaseReferencedVersionBuilder(
      tmp_cfd, base_version, this, track_found_and_missing_files_));
  builder_iter->second = std::move(new_builder);

#ifndef NDEBUG
  auto version_iter = versions_.find(edit.GetColumnFamily());
  assert(version_iter == versions_.end());
#endif  // !NDEBUG
  return Status::OK();
}

void ManifestTailer::CheckIterationResult(const log::Reader& reader,
                                          Status* s) {
  VersionEditHandlerPointInTime::CheckIterationResult(reader, s);
  assert(s);
  if (s->ok()) {
    if (Mode::kRecovery == mode_) {
      mode_ = Mode::kCatchUp;
    } else {
      assert(Mode::kCatchUp == mode_);
    }
  }
}

std::vector<std::string> ManifestTailer::GetAndClearIntermediateFiles() {
  std::vector<std::string> res;
  for (const auto& builder : builders_) {
    auto files =
        builder.second->version_builder()->GetAndClearIntermediateFiles();
    res.insert(res.end(), std::make_move_iterator(files.begin()),
               std::make_move_iterator(files.end()));
    files.erase(files.begin(), files.end());
  }
  return res;
}

Status ManifestTailer::VerifyFile(ColumnFamilyData* cfd,
                                  const std::string& fpath, int level,
                                  const FileMetaData& fmeta) {
  Status s =
      VersionEditHandlerPointInTime::VerifyFile(cfd, fpath, level, fmeta);
  // TODO: Open file or create hard link to prevent the file from being
  // deleted.
  return s;
}

void DumpManifestHandler::CheckIterationResult(const log::Reader& reader,
                                               Status* s) {
  VersionEditHandler::CheckIterationResult(reader, s);
  if (!s->ok()) {
    fprintf(stdout, "%s\n", s->ToString().c_str());
    return;
  }
  assert(cf_to_cmp_names_);
  for (auto* cfd : *(version_set_->column_family_set_)) {
    fprintf(stdout,
            "--------------- Column family \"%s\"  (ID %" PRIu32
            ") --------------\n",
            cfd->GetName().c_str(), cfd->GetID());
    fprintf(stdout, "log number: %" PRIu64 "\n", cfd->GetLogNumber());
    auto it = cf_to_cmp_names_->find(cfd->GetID());
    if (it != cf_to_cmp_names_->end()) {
      fprintf(stdout,
              "comparator: <%s>, but the comparator object is not available.\n",
              it->second.c_str());
    } else {
      fprintf(stdout, "comparator: %s\n", cfd->user_comparator()->Name());
    }
    assert(cfd->current());

    // Print out DebugStrings. Can include non-terminating null characters.
    fwrite(cfd->current()->DebugString(hex_).data(), sizeof(char),
           cfd->current()->DebugString(hex_).size(), stdout);

    fprintf(stdout,
            "By default, manifest file dump prints LSM trees as if %d levels "
            "were configured, "
            "which is not necessarily true for the column family (CF) this "
            "manifest is associated with. "
            "Please consult other DB files, such as the OPTIONS file, to "
            "confirm.\n",
            cfd->ioptions().num_levels);
  }
  fprintf(stdout,
          "next_file_number %" PRIu64 " last_sequence %" PRIu64
          "  prev_log_number %" PRIu64 " max_column_family %" PRIu32
          " min_log_number_to_keep %" PRIu64 "\n",
          version_set_->current_next_file_number(),
          version_set_->LastSequence(), version_set_->prev_log_number(),
          version_set_->column_family_set_->GetMaxColumnFamily(),
          version_set_->min_log_number_to_keep());
}

}  // namespace ROCKSDB_NAMESPACE
