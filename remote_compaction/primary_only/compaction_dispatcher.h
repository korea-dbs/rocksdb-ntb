#pragma once
// ============================================================
//  primary_only/compaction_dispatcher.h  (v30 – CtrlRecvLoop에 job 레벨
//                          워치독 추가: 워커가 input 수신 후 응답 없이
//                          죽거나 멈추면, in_flight_에 영원히 남아있던
//                          job을 강제로 타임아웃 실패 처리)
//
//  v22 배경: 같은 job의 입력 파일이 여러 개일 때(예: job=1,
//  inputs=[file11, file9]), 두 파일이 같은 세그먼트를 순차 공유하는데
//  raw consumed 플래그 폴링만으로는 worker가 이 세그먼트를 감시하기
//  시작하기도 전에 primary가 다음 파일까지 덮어써버리는 레이스가
//  실측으로 확인됨(FILE_MISMATCH: expect=11 hdr=9, worker 쪽
//  [POLL_RECV] spin=0인데 이미 두 번째 파일 데이터였음 — worker가
//  이 job을 처음 들여다본 시점에 이미 두 파일 다 전송 완료 상태였다는
//  뜻). primary 쪽에서 같은 consumed 플래그를 33만~39만 번 스핀해도
//  결국 실제 ack 없이 "1"을 봐버리는 것도 확인 — 즉 이 raw 포인터
//  폴링은 이 NTB 링크에서 신뢰 가능한 동기화 수단이 아님.
//
//  → 이미 안정적으로 검증된 ctrl 채널(SCIMemCpy+SCICheckSequence+
//    SCIStoreBarrier 기반, v20-fix4)을 이용해 "worker가 방금 파일을
//    완전히 수신+ack했다"는 chunk-ack을 worker→primary로 명시적으로
//    보내고, primary는 같은 job의 다음 입력 파일을 보내기 전에 이
//    chunk-ack을 기다리도록 변경. 이를 위해 세그먼트별 대기용
//    mutex/condition_variable/플래그를 추가.
//
//  v29 수정 (2026-07-25, job=128/file=695 케이스 분석 결과):
//    - db_bench 완료 후 펜딩 컴팩션이 몰리는 구간(queue_depth 급증)
//      에서, worker는 실제로는 살아서 나중에 파일을 정상 수신하는데도
//      SendLoop의 chunk_ack_cv_ 30초 대기가 먼저 타임아웃나서 job을
//      포기하고 세그먼트를 재사용해버림 → 뒤늦게 도착한 진짜 ack은
//      아무도 안 기다리고 있어 허사가 되고, 그 사이 세그먼트를 차지한
//      엉뚱한 다음 job의 파일을 worker가 만나 FILE_MISMATCH로 이어짐.
//      (worker_server.cc v10에서 [FILE_MISMATCH]로 이미 이 상황 자체는
//      안전하게 감지/거부하도록 만들어 놨지만, 그 감지가 primary에
//      "즉시" 전달되지 않고 여전히 30초 타임아웃에 의존하고 있었음.)
//    - 근본 원인은 "worker가 파일 수신에 실패했을 때 그 사실을 primary
//      에 즉시 알릴 방법이 없었다"는 것. worker_server.cc v11에서
//      StreamRecvSST가 실패하면(주로 FILE_MISMATCH) 그 즉시 "실패"
//      chunk-ack을 보내도록 바꿨고, 여기서는 그 실패 플래그를 받아서
//      SendLoop가 30초를 다 기다리지 않고 그 자리에서 바로 반응하게
//      한다. 이번 라운드에서는 30초 타임아웃 값 자체는 건드리지
//      않음 — 여전히 "진짜로 응답이 없는" 상황(worker 프로세스 다운
//      등)에 대한 최후의 안전장치로 그대로 둔다.
//    → chunk-ack 페이로드에 1바이트 success 플래그 추가
//      (tag(1)+success(1)+job_id(8)+file_number(8) = 18바이트,
//      worker_server.cc v11의 MakeChunkAckPayload와 짝을 맞춤).
//      CtrlRecvLoop가 이 success 바이트를 파싱해서 세그먼트별
//      chunk_ack_failed_ 플래그에 반영하고, SendLoop의 wait_for가
//      깨어난 뒤 이 플래그를 확인해서 실패면 즉시 그 job을 실패
//      처리(send_ok=false)하고 다음 job으로 넘어간다.
//
//  v30 수정: v29까지도 여전히 못 잡는 구멍이 하나 남아있었다 --
//    worker가 모든 input 파일을 정상 수신(chunk-ack success까지 다
//    보냄)한 뒤, 실제 컴팩션 도중이나 CompactionResult 전송 직전에
//    죽거나 멈추면, 그 job은 chunk-ack 관련 타임아웃(30초)을 전부
//    통과한 뒤라 SendLoop 쪽에선 이미 "성공적으로 다 보냈다"고 보고
//    in_flight_로 넘어간 상태다. 이 상태에서 worker가 최종
//    CompactionResult를 영영 안 보내면, CtrlRecvLoop의 RecvResultMsg
//    타임아웃(10초)은 "이 순간 아무 메시지도 안 왔다"만 알려줄 뿐 --
//    어느 job이 문제인지는 알려줄 수 없다(채널 하나를 모든 job이
//    공유하므로). 그래서 이 job은 in_flight_에 무기한 남아있었고,
//    reservation도 영원히 안 풀리고, 매니페스트 edit도 영원히 안
//    써졌다.
//    → ReapStaleInFlightJobs()를 추가: CtrlRecvLoop 루프에서 주기적으로
//      (kWatchdogScanIntervalMs마다) in_flight_를 스캔해서, t_send_done
//      이후 kJobResultTimeoutMs를 넘긴 job을 찾아 강제로 실패 처리한다
//      -- 기존 !result.success 경로와 완전히 동일한 정리 절차
//      (reservation 해제, job_seg_map_ 정리, finish_queue_에 실패로
//      push)를 재사용한다.
// ============================================================

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <deque>  // v-finooo
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "../shared/compaction_spec.h"
#include "../shared/ntb_channel.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

class Compaction;
class ColumnFamilyData;
struct ImmutableOptions;
struct MutableCFOptions;

// ── SSTReservationTable ───────────────────────────────────────────
class SSTReservationTable {
 public:
  bool TryReserve(const std::vector<uint64_t>& fnums);
  void Release(const std::vector<uint64_t>& fnums);
  bool IsReserved(uint64_t fnum) const;

 private:
  mutable std::mutex mu_;
  std::set<uint64_t> reserved_;
};

// ── DispatchOptions ───────────────────────────────────────────────
struct DispatchOptions {
  uint32_t    worker_node_id   = 8;
  std::string db_path;
  uint64_t    target_file_size = 64ULL << 20;
};

// ── AsyncJob ──────────────────────────────────────────────────────
struct AsyncJob {
  CompactionSpec                 spec;
  std::vector<uint64_t>          fnums;
  CompactionResult               result;
  std::vector<std::vector<char>> out_bufs;
  std::shared_ptr<Compaction>    compaction_holder;
  std::function<void(Status,
                     CompactionSpec&&,
                     CompactionResult&&,
                     std::vector<std::vector<char>>&&)> on_complete;

  // 타이밍
  std::chrono::steady_clock::time_point t_total_start;
  std::chrono::steady_clock::time_point t_send_done;
  std::chrono::steady_clock::time_point enqueue_time;  // v10: 큐 push 시각
};

// ── v-multiseg: job의 입력파일을 여러 세그먼트에 나눠 보내기 위한 공유
// 상태. send_queues_에는 이제 AsyncJob 전체가 아니라 SendTask(이 세그먼트가
// 보낼 파일 인덱스만)만 들어가고, 실제 job 데이터는 pending_sends_에
// job_id로 공유돼 있다가 모든 세그먼트가 자기 몫을 다 처리하면
// (remaining_files==0, 성공/실패 공통) in_flight_ 또는 on_complete로 넘어간다.
struct SendTask {
  uint64_t            job_id = 0;
  std::vector<size_t> file_indices;
};

struct PendingSend {
  AsyncJob             job;
  std::atomic<int>     remaining_files{0};
  std::atomic<bool>    aborted{false};
  int                  responsible_seg = -1;  // ctrl spec 메시지를 보낼 담당 세그먼트
  // v-nowait (one-state): 입력을 다 보내기 전에 결과가 오면 여기 보관한다.
  // pending_send_mu_ 로 보호.
  bool                 result_ready    = false;
  CompactionResult     early_result;
  int                  early_fixed_seg = 0;
};

// ── FinishJob ─────────────────────────────────────────────────────
struct FinishJob {
  Status   status;
  AsyncJob job;
  std::chrono::steady_clock::time_point enqueue_time = std::chrono::steady_clock::now();
};

// ── CompactionDispatcher ──────────────────────────────────────────
class CompactionDispatcher {
 public:
  explicit CompactionDispatcher(const DispatchOptions& opts);
  ~CompactionDispatcher();

  Status Connect();
  void   Disconnect();

  Status Dispatch(Compaction*                         c,
                  ColumnFamilyData*                   cfd,
                  const ImmutableOptions&             iopts,
                  const MutableCFOptions&             mutable_cf,
                  const std::vector<SequenceNumber>&  snapshots,
                  SequenceNumber                      ewc_snapshot,
                  SequenceNumber                      preserve_seqnum,
                  CompactionResult*                   result_out,
                  std::vector<std::vector<char>>*     out_sst_bufs);

  Status DispatchAsync(
      CompactionSpec                      spec,
      std::shared_ptr<Compaction>         compaction_holder,
      std::function<void(Status,
                         CompactionSpec&&,
                         CompactionResult&&,
                         std::vector<std::vector<char>>&&)> on_complete);

  Status BuildSpec(Compaction*                         c,
                   ColumnFamilyData*                   cfd,
                   const ImmutableOptions&             iopts,
                   const MutableCFOptions&             mutable_cf,
                   const std::vector<SequenceNumber>&  snapshots,
                   SequenceNumber                      ewc_snapshot,
                   SequenceNumber                      preserve_seqnum,
                   CompactionSpec*                     spec_out);

  void ReleaseReservation(const std::vector<uint64_t>& fnums);
  SSTReservationTable* reservation_table() { return &reservation_; }

  bool IsAsyncBusy() const {
    for (int i = 0; i < kNumSegments; i++) {
      std::lock_guard<std::mutex> lk(send_mu_[i]);
      if (!send_queues_[i].empty()) return true;
    }
    {  // v-gsendq
      std::lock_guard<std::mutex> lk(gsend_mu_);
      if (!gsend_queue_.empty()) return true;
    }
    return false;
  }

 private:
  struct PendingOutput {
    AsyncJob job;
    std::chrono::steady_clock::time_point enqueue_time = std::chrono::steady_clock::now();
    bool discard = false;  // v-nowait: 포기된 잡의 출력 -- 받아서 버림(세그먼트 비우기)
  };

  void SendLoop(int seg_idx);
  void CtrlRecvLoop();
  void OutputRecvLoop(int seg_idx);
  void DeliverResult(AsyncJob&& job, CompactionResult&& result, int fixed_seg);  // v-nowait
  void FinishWorkerLoop(int idx);
  Status ExecuteJobSync(AsyncJob& job);
  int  PickLeastBusySegment();
  void ReapStaleInFlightJobs();  // v30: CtrlRecvLoop watchdog
  // v-outq: RC_OUTPUT_ANYSEG(기본 켜짐) -- 출력도 빈 세그먼트 아무 데나로 받는다.
  // 잡별 수신 상태. out_jobs_mu_로 보호 (busy>0이면 어떤 OutputRecvLoop가 복사 중).
  struct OutJob {
    AsyncJob                    job;
    std::map<uint64_t, size_t>  idx_by_fnum;
    std::vector<bool>           done;
    std::vector<std::chrono::steady_clock::time_point> t_file;
    size_t files_left = 0;
    int    busy       = 0;
    bool   failed     = false;
    bool   discard    = false;
    bool   seen       = false;
    std::chrono::steady_clock::time_point t_reg, t_first, t_last;
    bool provisional = false;                  // v-stream: 결과 전에 생긴 임시 잡 항목
    std::vector<uint32_t> crc_actual, crc_expect;  // v-stream (도착 순서 인덱스)
    std::vector<size_t>   order;                  // v-stream: 결과 i번 -> 도착 인덱스
  };
  void RegisterOutJob(AsyncJob&& job, bool discard);
  void OutputRecvLoopAny(int seg_idx);
  void CompleteOutJob(uint64_t job_id, std::shared_ptr<OutJob> oj);
  void ReapStaleOutJobs();
  void RetireOutJobLocked(uint64_t job_id);  // out_jobs_mu_ 잡은 상태에서 호출

  static constexpr int kFinishThreads = 54;
  static constexpr int kFinishQueueSoftLimit = kFinishThreads * 2;

  // v30: CtrlRecvLoop watchdog. A single RecvResultMsg timeout only
  // means "nothing arrived in this 10s window" -- it says nothing about
  // WHICH job (if any) is stuck, since every job's result shares this
  // one ctrl channel. kJobResultTimeoutMs bounds how long a job may sit
  // in in_flight_ (measured from t_send_done, i.e. once SendLoop
  // finished pushing all its inputs) before ReapStaleInFlightJobs()
  // force-fails it. Keep this in rough sync with ntb_channel.h's
  // kOutputResultTimeoutMs -- both are trying to bound the same
  // underlying thing (worker compaction time) from two different
  // vantage points, and both are placeholders that need tuning against
  // this deployment's real max compaction duration.
  static constexpr int kJobResultTimeoutMs     = 300000;  // 5 min (was 10 min)
  static constexpr int kWatchdogScanIntervalMs = 5000;    // scan cadence

  DispatchOptions       opts_;
  NtbChannel            channel_;
  SSTReservationTable   reservation_;
  std::atomic<uint64_t> job_id_{1};
  std::atomic<bool>     channel_open_{false};

  mutable std::mutex           send_mu_[kNumSegments];
  std::condition_variable      send_cv_[kNumSegments];
  std::queue<SendTask>         send_queues_[kNumSegments];
  // v-gsendq: 전역 송신 큐 (RC_GLOBAL_SENDQ=0 이면 미사용)
  mutable std::mutex           gsend_mu_;
  std::condition_variable      gsend_cv_;
  std::deque<SendTask>         gsend_queue_;
  std::atomic<bool>            stop_{false};
  std::mutex                   ctrl_send_mu_[kNumSegments];

  std::mutex                   inflight_mu_;
  std::map<uint64_t, AsyncJob> in_flight_;
  std::mutex job_seg_mu_;
  std::map<uint64_t, int> job_seg_map_;  // job_id -> responsible_seg (출력 ack 라우팅용)

  // v-reg: 워커가 pending_recv_ 등록을 끝낸 job_id (SendLoop가 실제
  // 데이터 전송 전에 이걸 확인/대기한다). std::map<uint64_t,bool>로
  // 써서 <set> 추가 include 없이 기존 <map>만 재사용.
  std::mutex job_registered_mu_;
  std::condition_variable job_registered_cv_;
  std::map<uint64_t, bool> job_registered_;

  std::mutex pending_send_mu_;
  std::map<uint64_t, std::shared_ptr<PendingSend>> pending_sends_;  // v-multiseg
  std::mutex seg_pick_mu_;  // PickLeastBusySegment + 큐 push를 원자적으로 묶는 락
  std::atomic<uint64_t> next_seg_{0};  // PickLeastBusySegment 스캔 시작점 로테이션

  mutable std::mutex           output_mu_[kNumSegments];
  std::condition_variable      output_cv_[kNumSegments];
  std::queue<PendingOutput>    output_queues_[kNumSegments];
  // v-outq
  std::mutex                                   out_jobs_mu_;
  std::map<uint64_t, std::shared_ptr<OutJob>>  out_jobs_;
  std::map<uint64_t, bool>                     out_retired_;    // 끝난 잡 (남은 청크는 버림)
  std::queue<uint64_t>                         out_retired_q_;  // out_retired_ 크기 제한용

  // v22: 같은 job의 파일간 전송 순서 보장을 위한 chunk-ack 대기용.
  // worker가 파일 하나를 완전히 수신+ack하면 ctrl 채널로 chunk-ack을
  // 보내고, CtrlRecvLoop가 이를 파싱해서 job_id % kNumSegments로 계산한
  // 세그먼트의 아래 변수들을 세팅+notify한다. SendLoop[seg_idx]는 같은
  // job의 다음 입력 파일을 보내기 전에 이걸 기다린다.
  std::mutex               chunk_ack_mu_[kNumSegments];
  std::condition_variable  chunk_ack_cv_[kNumSegments];
  bool                     chunk_ack_ready_[kNumSegments] = {false};
  uint64_t                 chunk_ack_file_[kNumSegments]  = {0};
  // v29: worker가 그 파일 수신에 실패했을 때(주로 FILE_MISMATCH) 즉시
  // 세팅되는 플래그. chunk_ack_ready_와 함께 세팅되므로 SendLoop의
  // wait_for는 30초를 다 기다리지 않고 바로 깨어나며, 깨어난 뒤 이
  // 플래그를 확인해서 실패면 그 자리에서 job을 포기한다.
  bool                     chunk_ack_failed_[kNumSegments] = {false};

  std::thread send_threads_[kNumSegments];
  std::thread ctrl_recv_thread_;
  std::thread output_recv_threads_[kNumSegments];

  std::vector<std::thread>     finish_pool_;
  mutable std::mutex           finish_mu_;
  std::condition_variable      finish_cv_;
  std::deque<FinishJob>        finish_queue_;  // v-finooo (was std::queue)
  bool                         stop_finish_{false};
};

}  // namespace ROCKSDB_NAMESPACE
