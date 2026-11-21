#include <atomic>  // v-ringlf
#include <thread>  // v-ringlf
#include <fcntl.h>  // v-ring
#include <unistd.h>  // v-ring
#include <linux/falloc.h>  // v-ring
#include <deque>  // v-ring
#include <cerrno>  // v-ring
#include <atomic>  // v-finooo
// ============================================================
// primary_only/compaction_dispatcher.cc (v32 – CtrlRecvLoop에 job 레벨
// 워치독 추가: 워커가 input 수신 후 응답 없이 죽거나 멈추면,
// in_flight_에 영원히 남아있던 job을 강제로 타임아웃 실패 처리)
// [PRIMARY (srv3) 전용]
//
// v32 수정: v31까지도 여전히 못 잡는 구멍이 하나 남아있었다 -- worker가
// 모든 input 파일을 정상 수신(chunk-ack success까지 다 보냄)한 뒤,
// 실제 컴팩션 도중이나 CompactionResult 전송 직전에 죽거나 멈추면,
// 그 job은 chunk-ack 관련 타임아웃(30초)을 전부 통과한 뒤라 SendLoop
// 쪽에선 이미 "성공적으로 다 보냈다"고 보고 in_flight_로 넘어간
// 상태다. 이 상태에서 worker가 최종 CompactionResult를 영영 안
// 보내면, CtrlRecvLoop의 RecvResultMsg 타임아웃(10초)은 "이 순간
// 아무 메시지도 안 왔다"만 알려줄 뿐 -- 어느 job이 문제인지는 알려줄
// 수 없다(채널 하나를 모든 job이 공유하므로). 그래서 이 job은
// in_flight_에 무기한 남아있었고, reservation도 영원히 안 풀리고,
// 매니페스트 edit도 영원히 안 써졌다.
// → ReapStaleInFlightJobs()를 추가: CtrlRecvLoop 루프에서 주기적으로
//   (kWatchdogScanIntervalMs마다) in_flight_를 스캔해서, t_send_done
//   이후 kJobResultTimeoutMs(5분 -- ntb_channel.h의
//   kOutputResultTimeoutMs와 같이 튜닝해야 함)를 넘긴 job을 찾아
//   강제로 실패 처리한다 -- 기존 !result.success 경로와 완전히
//   동일한 정리 절차(reservation 해제, job_seg_map_ 정리,
//   finish_queue_에 실패로 push)를 재사용한다. CtrlRecvLoop의 태그
//   파싱/chunk-ack/CompactionResult 처리 로직, OutputRecvLoop 등
//   나머지는 전혀 건드리지 않았다.
//
// v31 수정 (2026-07-25, job=6(output 86개)에서 L1 파일 #380/#404가
// 키 범위 겹침으로 force_consistency_checks에 걸린 사고 원인 규명):
// - input 쪽은 v22에서 "같은 job의 파일 사이엔 반드시 worker의
//   chunk-ack을 받은 뒤에만 다음 파일을 보낸다"는 순서 보장이
//   있었는데, output 쪽(worker→primary)엔 이 보장이 전혀 없었음 —
//   OutputRecvLoop가 그냥 파일들을 순차로 StreamRecvOutputSST만
//   호출했음. output 파일이 많은 job(이 사고는 86개)일수록 이
//   레이스에 걸릴 확률이 높아짐.
// → OutputRecvLoop가 output 파일 하나를 완전히 수신할 때마다(성공
//   이든 실패든) 즉시 ctrl 채널(SendCtrlMsg, primary→worker 방향
//   — job spec을 보내는 것과 같은 물리 채널)로 output-ack을 보내서
//   worker에게 "이 파일 다 받았다/실패했다"를 알린다. worker의
//   CompactionLoop는 이 ack을 받은 뒤에야 같은 job의 다음 output
//   파일을 보낸다(worker_server.cc v12 참고). 페이로드 포맷은
//   chunk-ack과 동일하게 tag(1)+success(1)+job_id(8)+file_number(8)
//   = 18바이트, 새 태그 kMsgTypeOutputAck=0x04 사용.
// - ntb_channel.cc의 OutputRecvChunk에 file_number 검증도 별도로
//   추가함(v31, ntb_channel.h/cc 참고) — 이건 "겹침이 실제로 생겼을
//   때 즉시 잡아내는" 안전장치이고, 이번 chunk-ack 순서 보장은
//   "애초에 레이스가 안 생기게 막는" 근본 수정. 두 개를 같이 넣음.
//
// v29 이력: chunk-ack 실패 플래그 처리, 30초 타임아웃 없이 즉시 반응
// (아래 원래 v29 헤더 주석 참고).
//
// v10/v11/v12 이력은 이전 버전 헤더 주석 참고 (큐 대기시간 로그,
// DispatchAsync 예약 누락 수정, 소멸자 스레드 join 누락 수정).
//
// v22 수정 (핵심):
// - 문제: job=1처럼 입력 파일이 2개 이상인 job에서, 같은 세그먼트를
//   두 파일이 순차로 공유하는데 raw consumed 플래그 폴링만으로는
//   "worker가 첫 번째 파일을 진짜로 다 받았는지"를 신뢰할 수 없음이
//   실측으로 확인됨:
//   * worker 쪽 InputRecvChunk의 ready 폴링이 spin=0(즉시 통과)
//     으로 file11을 기다리는데 이미 file9 데이터가 있었음 — worker
//     가 이 세그먼트를 감시하기 시작한 시점엔 이미 두 파일 다
//     전송이 끝나있었다는 뜻.
//   * primary 쪽에서도 file9 전송 전 consumed 폴링이 33~39만 번
//     스핀 후 "1"을 봤지만, 그 시점까지 worker의 ack 코드는 전혀
//     실행된 적이 없었음(로그상 최초 ack가 미스매치 처리 중에야
//     나옴) — 즉 그 "1"은 실제 ack가 아니라 신뢰할 수 없는 값.
// → raw 포인터 폴링(consumed/ready)은 데이터 채널 내 개별 청크
//   전송 자체(같은 파일 안에서의 흐름제어)에는 계속 쓰지만,
//   "같은 job의 서로 다른 파일 사이" 경계에서는 이미 안정성이
//   검증된 ctrl 채널(SendResultMsg/RecvResultMsg, v20-fix4로
//   SCIMemCpy+SCICheckSequence+SCIStoreBarrier 기반) 을 통해
//   worker가 명시적으로 보내는 chunk-ack을 받은 뒤에만 다음
//   파일을 전송하도록 변경.
// - wire 프로토콜: 기존에는 SendCtrlMsg/RecvCtrlMsg 페이로드가 바로
//   CompactionSpec/CompactionResult 직렬화 바이트였음. v22부터는
//   맨 앞에 1바이트 태그(kMsgTypeSpec/kMsgTypeResult/kMsgTypeChunkAck)
//   를 붙여서 CtrlRecvLoop(과 worker의 DispatcherLoop)가 메시지
//   종류를 구분할 수 있게 함. chunk-ack 페이로드는
//   tag(1) + job_id(8) + file_number(8) = 17바이트 고정 포맷.
//   worker_server.cc도 동일한 태그 규칙으로 같이 수정해야 함
//   (worker_server.cc v6 참고).
//
// v29 수정 (2026-07-25, job=128/file=695 케이스 분석 결과 — 상세
// 배경은 compaction_dispatcher.h의 v29 주석 참고):
// - chunk-ack 페이로드가 18바이트로 변경됨:
//   tag(1) + success(1) + job_id(8) + file_number(8).
//   (worker_server.cc v11의 MakeChunkAckPayload와 반드시 짝을
//   맞춰야 함 — 순서/바이트 수가 다르면 CtrlRecvLoop이 깨진
//   job_id/file_number를 읽게 됨.)
// - CtrlRecvLoop: success 바이트를 파싱해서 chunk_ack_failed_[seg]에
//   반영. chunk_ack_ready_는 성공/실패 관계없이 항상 true로 세팅해서
//   SendLoop가 즉시 깨어나게 한다.
// - SendLoop: wait_for가 깨어난 뒤(타임아웃이 아니라 ack으로 깨어난
//   경우) chunk_ack_failed_[seg_idx]를 확인. true면 30초를 채우지
//   않고 그 자리에서 곧바로 그 job을 실패 처리(send_ok=false)하고
//   다음 job으로 넘어간다. 30초 wait_for 자체의 타임아웃 시간은
//   이번 라운드에서 변경하지 않음 — worker가 정말로 응답이 없는
//   극단적 상황(프로세스 다운 등)에 대한 최후의 안전장치로 유지.
//
// perf 클린업 (2026-07-28):
// - 성능 평가를 위해 hot-path(정상 성공 경로에서 파일/청크/job마다
//   매번 찍히던) fprintf를 모두 제거함: BuildSpec의 input/output-range
//   로그, Dispatch(동기, 미사용 경로)의 SEND_CALL 로그, DispatchAsync의
//   queue_depth 로그, SendLoop/CtrlRecvLoop/OutputRecvLoop의
//   started/exiting 로그, SEND_POP, queue_wait, SEND_CALL(타임스탬프
//   버전), chunk-ack ok(성공), timing(step1/step3), job submitted,
//   job→OutputRecvLoop 라우팅, primary BEFORE/AFTER recv, output-ack
//   성공 로그, OutputRecvLoop job done 로그.
// - 실패/에러 경로 로그는 그대로 유지: SendCtrlMsg/StreamSendSST 실패,
//   chunk-ack TIMEOUT/FAILED, malformed/unknown ctrl tag, deserialize
//   실패, unknown job_id(드문 케이스), worker failed, StreamRecvOutputSST
//   실패, output-ack FAILED 전송 로그. CtrlRecvLoop의 chunk-ack 로그는
//   success 여부와 무관하게 매번 찍던 것을 실패(success=false)일 때만
//   찍도록 좁혔다(타임스탬프는 성능 평가엔 불필요해 제거).
// ============================================================
#include "compaction_dispatcher.h"
#include <cstdlib>    // v-spinwait
namespace {
inline unsigned RcIdleYield() { static const unsigned v = [] { const char* e = std::getenv("RC_IDLE_YIELD"); return e ? (unsigned)std::strtoul(e, nullptr, 10) : 16u; }(); return v; }
inline unsigned RcIdleSleepUs() { static const unsigned v = [] { const char* e = std::getenv("RC_IDLE_SLEEP_US"); return e ? (unsigned)std::strtoul(e, nullptr, 10) : 200u; }(); return v; }
}  // namespace
#include <pthread.h>  // v-thname
#include <cstdio>

#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <sched.h>

#include <chrono>
#include <cstdio>
#include <cstring>

#include "db/compaction/compaction.h"
#include "db/column_family.h"
#include "rocksdb/comparator.h"
#include "rocksdb/options.h"

#include <cerrno>  // v-stream
#include <condition_variable>
#include <cstring>
#include <map>
#include <queue>
#include <thread>
int RcStreamWriteFileExt(const std::string& dst, const char* data, size_t size);  // v-stream (remote_compaction_job.cc)

namespace ROCKSDB_NAMESPACE {

namespace {

// v22: ctrl 메시지 종류 태그. worker_server.cc와 반드시 동일해야 함.
constexpr char kMsgTypeSpec = 0x01;
constexpr char kMsgTypeResult = 0x02;
constexpr char kMsgTypeChunkAck = 0x03;
constexpr char kMsgTypeJobRegistered = 0x04;  // v-reg
// v31: output 파일 하나 수신 완료/실패를 worker에 알리는 태그
// (primary->worker, SendCtrlMsg/RecvSpecMsg 채널 재사용).
constexpr char kMsgTypeOutputAck = 0x04;

std::string TagMessage(char tag, const std::string& payload) {
  std::string out;
  out.reserve(payload.size() + 1);
  out.push_back(tag);
  out.append(payload);
  return out;
}

// v31: chunk-ack(v29)과 동일한 포맷 — tag(1)+success(1)+job_id(8)+
// file_number(8) = 18바이트. worker_server.cc의 파싱과 순서가 반드시
// 일치해야 함.
std::string MakeOutputAckPayload(uint64_t job_id, uint64_t file_number,
                                  bool success) {
  std::string out;
  out.reserve(1 + 1 + sizeof(job_id) + sizeof(file_number));
  out.push_back(kMsgTypeOutputAck);
  uint8_t ok = success ? 1 : 0;
  out.push_back(static_cast<char>(ok));
  out.append(reinterpret_cast<const char*>(&job_id), sizeof(job_id));
  out.append(reinterpret_cast<const char*>(&file_number),
             sizeof(file_number));
  return out;
}

// [CPU affinity] 원격 컴팩션 스레드(SendLoop/CtrlRecvLoop/OutputRecvLoop/
// FinishWorkerLoop)를 taskset -c 0-7 범위 밖 코어에 고정해 로컬 컴팩션
// 스레드풀과 CPU를 다투지 않게 한다. 정확성과 무관한 순수 스케줄링 최적화.
void PinCurrentThreadToCore(int core_id) {
  // [ablation] CPU affinity pinning disabled for perf comparison.
  (void)core_id;
}
}  // namespace

// ── SSTReservationTable ───────────────────────────────────────────
bool SSTReservationTable::TryReserve(const std::vector<uint64_t>& fnums) {
  std::lock_guard<std::mutex> lk(mu_);
  for (uint64_t fn : fnums)
    if (reserved_.count(fn)) return false;
  for (uint64_t fn : fnums) reserved_.insert(fn);
  return true;
}

void SSTReservationTable::Release(const std::vector<uint64_t>& fnums) {
  std::lock_guard<std::mutex> lk(mu_);
  for (uint64_t fn : fnums) reserved_.erase(fn);
}

bool SSTReservationTable::IsReserved(uint64_t fn) const {
  std::lock_guard<std::mutex> lk(mu_);
  return reserved_.count(fn) > 0;
}

// ── CompactionDispatcher ──────────────────────────────────────────
CompactionDispatcher::CompactionDispatcher(const DispatchOptions& opts)
    : opts_(opts) {}

CompactionDispatcher::~CompactionDispatcher() {
  // v12: send_threads_/ctrl_recv_thread_/output_recv_threads_까지
  // 정지+join 처리하는 Disconnect()를 반드시 먼저 호출해야 함.
  // (이걸 빼먹어서 joinable std::thread가 그대로 소멸 → std::terminate())
  Disconnect();
  {
    std::lock_guard<std::mutex> lk(finish_mu_);
    stop_finish_ = true;
  }
  finish_cv_.notify_all();
  for (auto& t : finish_pool_)
    if (t.joinable()) t.join();
}

Status CompactionDispatcher::Connect() {
  Status s = channel_.OpenAsPrimary(opts_.worker_node_id);
  if (!s.ok()) return s;

  stop_.store(false, std::memory_order_release);
  channel_open_.store(false, std::memory_order_release);
  {
    std::lock_guard<std::mutex> lk(finish_mu_);
    stop_finish_ = false;
  }

  for (int i = 0; i < kNumSegments; i++) {
    int seg = i;
    send_threads_[i] = std::thread([this, seg]() { SendLoop(seg); });
  }
  ctrl_recv_thread_ = std::thread([this]() { CtrlRecvLoop(); });
  for (int i = 0; i < kNumSegments; i++) {
    int seg = i;
    output_recv_threads_[i] =
        std::thread([this, seg]() { OutputRecvLoop(seg); });
  }
  // v-finthr: RC_FINISH_THREADS로 install 스레드 수 지정 (1..kFinishThreads, 기본 kFinishThreads)
  const int rc_fin_n = [] { const char* e = std::getenv("RC_FINISH_THREADS"); int n = e ? std::atoi(e) : kFinishThreads; if (n < 1) n = 1; if (n > kFinishThreads) n = kFinishThreads;
    fprintf(stderr, "[rc] finish threads=%d (RC_FINISH_THREADS, max %d) (v-finthr)\n", n, (int)kFinishThreads); return n; }();
  for (int i = 0; i < rc_fin_n; i++)
    finish_pool_.emplace_back([this, i]() { FinishWorkerLoop(i); });

  channel_open_.store(true, std::memory_order_release);
  for (int i = 0; i < kNumSegments; i++) send_cv_[i].notify_all();
  return s;
}

void CompactionDispatcher::Disconnect() {
  stop_.store(true, std::memory_order_release);
  {  // v-gsendq: 전역 큐에서 대기 중인 SendLoop도 깨운다
    std::lock_guard<std::mutex> lk(gsend_mu_);
  }
  gsend_cv_.notify_all();
  for (int i = 0; i < kNumSegments; i++) send_cv_[i].notify_all();
  for (int i = 0; i < kNumSegments; i++) output_cv_[i].notify_all();
  // v22: chunk-ack을 기다리는 중일 수도 있는 SendLoop를 깨워서
  // 종료시 데드락 없이 빠져나가게 함.
  for (int i = 0; i < kNumSegments; i++) chunk_ack_cv_[i].notify_all();

  for (int i = 0; i < kNumSegments; i++)
    if (send_threads_[i].joinable()) send_threads_[i].join();
  if (ctrl_recv_thread_.joinable()) ctrl_recv_thread_.join();
  for (int i = 0; i < kNumSegments; i++)
    if (output_recv_threads_[i].joinable()) output_recv_threads_[i].join();

  // v37: finish_pool_(kFinishThreads=54) 정지+join+clear. 이게 없으면
  // Connect()/Disconnect()가 재연결로 여러 번 불릴 때마다 54개씩
  // 스레드가 영구 누적된다 (이전엔 destructor에만 있어서 프로세스
  // 종료 시 1회만 정리됐음).
  {
    std::lock_guard<std::mutex> lk(finish_mu_);
    stop_finish_ = true;
  }
  finish_cv_.notify_all();
  for (auto& t : finish_pool_)
    if (t.joinable()) t.join();
  finish_pool_.clear();

  channel_.Close();
}

// ── BuildSpec ────────────────────────────────────────────────────
Status CompactionDispatcher::BuildSpec(
    Compaction* c, ColumnFamilyData* cfd, const ImmutableOptions& iopts,
    const MutableCFOptions& mutable_cf,
    const std::vector<SequenceNumber>& snapshots,
    SequenceNumber ewc_snapshot, SequenceNumber preserve_seqnum,
    CompactionSpec* spec_out) {
  CompactionSpec& s = *spec_out;
  s.job_id = job_id_.fetch_add(1, std::memory_order_relaxed);
  s.output_level = c->output_level();
  s.bottommost = c->bottommost_level();
  s.comparator_name = iopts.user_comparator->Name();
  if (iopts.compaction_filter)
    s.compaction_filter_name = iopts.compaction_filter->Name();

  {
    Slice smallest = c->GetSmallestUserKey();
    if (!smallest.empty()) {
      s.has_start = true;
      s.start_user_key = smallest.ToString();
    }
  }
  {
    Slice largest = c->GetLargestUserKey();
    if (!largest.empty()) {
      s.has_end = true;
      s.end_user_key = largest.ToString();
    }
  }

  for (size_t li = 0; li < c->num_input_levels(); li++) {
    int level = c->level(li);
    for (size_t i = 0; i < c->num_input_files(li); i++) {
      const FileMetaData* fmd = c->input(li, i);
      SSTFileSpec f;
      f.file_number = fmd->fd.GetNumber();
      f.file_size = fmd->fd.GetFileSize();
      f.smallest_seqno = fmd->fd.smallest_seqno;
      f.largest_seqno = fmd->fd.largest_seqno;
      f.smallest_ikey = fmd->smallest.Encode().ToString();
      f.largest_ikey = fmd->largest.Encode().ToString();
      f.level = level;
      s.inputs.push_back(f);
    }
  }

  s.snapshots.list = snapshots;
  s.snapshots.earliest_write_conflict_snapshot = ewc_snapshot;
  s.snapshots.preserve_deletes_seqnum = preserve_seqnum;

  s.output.target_file_size =
      std::min(mutable_cf.target_file_size_base, kChunkDataSize);
  s.output.compression = mutable_cf.compression;
  s.output.compression_opts = mutable_cf.compression_opts;

  // v-gpcut: 워커가 RocksDB와 같은 기준으로 출력 파일을 자를 수 있게 grandparent 경계와 크기 기준을 보낸다.
  // spec은 64KiB 메일박스로 가므로 grandparent 정보가 kGpBudget을 넘으면 보내지 않는다(그 잡만 기존 크기 기준).
  {
    static constexpr size_t kGpBudget = kCtrlPayloadMax - (32 << 10);  // v-ctrlbig: 나머지 spec용 32KiB 여유
    static std::atomic<uint64_t> gp_sent{0}, gp_skipped{0};
    s.output.gp_target_file_size =
        std::min<uint64_t>(c->target_output_file_size(), kChunkDataSize);
    s.output.gp_max_file_size =
        std::min<uint64_t>(c->max_output_file_size(), kChunkDataSize);
    s.output.gp_max_compaction_bytes = c->max_compaction_bytes();
    s.output.grandparents.clear();
    size_t gp_bytes = 0;
    for (const FileMetaData* g : c->grandparents()) {
      GPFileSpec gf;
      gf.smallest_ikey = g->smallest.Encode().ToString();
      gf.largest_ikey  = g->largest.Encode().ToString();
      gf.file_size     = g->fd.GetFileSize();
      gp_bytes += gf.smallest_ikey.size() + gf.largest_ikey.size() + 16;
      s.output.grandparents.push_back(std::move(gf));
    }
    if (gp_bytes > kGpBudget) {
      fprintf(stderr, "[rc] gpcut: job=%llu grandparents=%zu (%zu bytes) > budget -> size-only cut (skipped=%llu)\n",
              (unsigned long long)s.job_id, s.output.grandparents.size(), gp_bytes,
              (unsigned long long)(gp_skipped.fetch_add(1) + 1));
      s.output.grandparents.clear();
    } else if (gp_sent.fetch_add(1) == 0) {
      fprintf(stderr, "[rc] gpcut: sending grandparent boundaries to worker (v-gpcut) target=%llu max=%llu max_compaction_bytes=%llu\n",
              (unsigned long long)s.output.gp_target_file_size, (unsigned long long)s.output.gp_max_file_size,
              (unsigned long long)s.output.gp_max_compaction_bytes);
    }
  }

  // output range 계산: 모든 input files의 key range 합집합
  // → Compaction 객체에 등록해서 RangeOverlapWithCompaction이 체크할 수 있게
  {
    const Comparator* ucmp = iopts.user_comparator;
    std::string out_smallest, out_largest;
    for (size_t li = 0; li < c->num_input_levels(); li++) {
      for (size_t i = 0; i < c->num_input_files(li); i++) {
        const FileMetaData* fmd = c->input(li, i);
        Slice fsmall = fmd->smallest.user_key();
        Slice flarge = fmd->largest.user_key();
        if (out_smallest.empty() ||
            ucmp->CompareWithoutTimestamp(fsmall, out_smallest) < 0)
          out_smallest = fsmall.ToString();
        if (out_largest.empty() ||
            ucmp->CompareWithoutTimestamp(flarge, out_largest) > 0)
          out_largest = flarge.ToString();
      }
    }
    if (!out_smallest.empty()) {
      c->SetOutputRange(out_smallest, out_largest);
    }
  }

  return Status::OK();
}

// ── SST 파일 경로 생성 ────────────────────────────────────────────
static std::string MakeSSTPath(const std::string& db_path,
                                uint64_t file_number) {
  char fname[32];
  snprintf(fname, sizeof(fname), "%06llu.sst",
           (unsigned long long)file_number);
  return db_path + "/" + fname;
}

// ── Dispatch (동기, 거의 미사용 경로 — CompactFilesImpl 전용) ─────
// v22: wire 포맷(태그 바이트)만 SendLoop/CtrlRecvLoop와 맞춤. 이 경로는
// 실제 db_bench 자동 백그라운드 컴팩션에서는 호출되지 않는 것으로
// 확인된 경로라, 파일간 chunk-ack 대기는 추가하지 않았음 — 혹시 이
// 경로가 멀티 인풋 job으로 실사용되면 SendLoop와 동일한 레이스가 남아
// 있을 수 있음.
Status CompactionDispatcher::Dispatch(
    Compaction* c, ColumnFamilyData* cfd, const ImmutableOptions& iopts,
    const MutableCFOptions& mutable_cf,
    const std::vector<SequenceNumber>& snapshots,
    SequenceNumber ewc_snapshot, SequenceNumber preserve_seqnum,
    CompactionResult* result_out,
    std::vector<std::vector<char>>* out_sst_bufs) {
  int fixed_seg = 0;
  CompactionSpec spec;
  Status s = BuildSpec(c, cfd, iopts, mutable_cf, snapshots, ewc_snapshot,
                        preserve_seqnum, &spec);
  if (!s.ok()) return s;

  std::vector<uint64_t> fnums;
  for (const auto& f : spec.inputs) fnums.push_back(f.file_number);
  if (!reservation_.TryReserve(fnums))
    return Status::Busy("input SSTs already reserved");

  s = channel_.SendCtrlMsg(TagMessage(kMsgTypeSpec, spec.Serialize()));
  if (!s.ok()) {
    reservation_.Release(fnums);
    return s;
  }

  for (const auto& f : spec.inputs) {
    std::string path = MakeSSTPath(opts_.db_path, f.file_number);
    s = channel_.StreamSendSST(path, f.file_number, f.file_size, fixed_seg);
    if (!s.ok()) {
      reservation_.Release(fnums);
      return Status::IOError("StreamSendSST failed: " + s.ToString());
    }
  }

  std::string raw;
  s = channel_.RecvCtrlMsg(&raw);
  if (!s.ok()) {
    reservation_.Release(fnums);
    return s;
  }
  if (raw.empty() || raw[0] != kMsgTypeResult) {
    reservation_.Release(fnums);
    return Status::Corruption("Dispatch: unexpected ctrl message tag");
  }
  Slice result_payload(raw.data() + 1, raw.size() - 1);
  if (!CompactionResult::Deserialize(result_payload, result_out)) {
    reservation_.Release(fnums);
    return Status::Corruption("CompactionResult deserialize failed");
  }
  if (!result_out->success) {
    reservation_.Release(fnums);
    return Status::IOError("worker compaction failed: " +
                            result_out->error_msg);
  }

  out_sst_bufs->resize(result_out->output_files.size());
  for (size_t i = 0; i < result_out->output_files.size(); i++) {
    const auto& of = result_out->output_files[i];
    s = channel_.StreamRecvOutputSST(of.file_number, of.file_size,
                                      &(*out_sst_bufs)[i], fixed_seg);
    if (!s.ok()) {
      reservation_.Release(fnums);
      return Status::IOError("StreamRecvOutputSST failed: " + s.ToString());
    }
  }

  return Status::OK();
}

void CompactionDispatcher::ReleaseReservation(
    const std::vector<uint64_t>& fnums) {
  reservation_.Release(fnums);
}

// ── DispatchAsync ─────────────────────────────────────────────────
int CompactionDispatcher::PickLeastBusySegment() { int start = static_cast<int>(next_seg_.fetch_add(1, std::memory_order_relaxed) % kNumSegments); int best = start; size_t best_sz = 0; bool first = true; for (int k = 0; k < kNumSegments; k++) { int i = (start + k) % kNumSegments; std::lock_guard<std::mutex> lk(send_mu_[i]); size_t sz = send_queues_[i].size(); if (first || sz < best_sz) { best_sz = sz; best = i; first = false; } if (sz == 0) break; } return best; }
// v-gsendq (2026-11-01): 기존엔 DispatchAsync가 잡을 받는 순간 세그먼트를
// 정해 그 세그먼트 큐에 넣었다. 한 SendLoop가 등록 ack(5s)/chunk-ack(30s)
// 타임아웃에 걸리면 그 큐에 이미 배정된 잡들이 다른 세그먼트가 놀아도
// 옮겨가지 못해 queue_wait가 평균 19s까지 쌓였다 (ntb.log, IO 바운드).
// 전역 큐 하나에 넣고, 비어 있는 SendLoop가 꺼내면서 자기 세그먼트에 묶는다.
static bool RcGlobalSendQ() {
  static const bool on = [] {
    const char* e = getenv("RC_GLOBAL_SENDQ");
    bool v = !(e != nullptr && atoi(e) == 0) && getenv("RC_SINGLE_SEG") == nullptr;
    fprintf(stderr, "[rc] send queue mode: %s\n",
            v ? "global (v-gsendq)" : "per-segment");
    return v;
  }();
  return on;
}

// v-filesplit (2026-11-01): 전역 큐 모드에서 잡을 입력 파일 단위 태스크로
// 쪼개 넣는다. 각 파일은 그 파일을 꺼낸 SendLoop의 세그먼트로 간다.
// 작업 명세는 그 잡의 파일을 처음 꺼낸 SendLoop가 한 번만 보내고, 나머지는
// 기존 v-reg(등록 ack) 대기 후 자기 파일을 보낸다. 워커는 파일 번호로
// 받는 쪽을 찾으므로(v-byfile + v-filesplit 워커 패치) 세그먼트와 무관하다.
// v-noack (2026-11-01): 파일마다 제어 채널 chunk-ack를 30초까지 기다리던
// v22 대기를 기본으로 끈다. v22는 "다음 파일이 아직 안 읽힌 세그먼트를
// 덮어쓰는" 레이스 때문에 넣었지만, 지금은 InputSendChunk가 청크마다
// 워커의 consumed(=memcpy 끝난 뒤 InputAckChunk)를 기다리므로 같은
// 보장이 채널 레벨에 이미 있다. 제어 ack 하나가 유실되면 30초 TIMEOUT ->
// 잡 포기 -> 워커 output-ack 30초 TIMEOUT 으로 번지던 연쇄를 없앤다.
// v-nowait (2026-11-01): 정상 경로의 고정 대기를 기본으로 끈다.
//   RC_REG_WAIT=1   : 예전처럼 워커의 등록 ack(최대 5s)를 기다린 뒤 입력 전송
//   RC_OUTPUT_ACK=1 : 예전처럼 출력 파일마다 output-ack 전송(워커는 최대 30s 대기)
// 워커도 같은 환경변수로 맞춰야 한다.
static bool RcEnvOn(const char* name) {
  const char* e = getenv(name);
  return e != nullptr && atoi(e) != 0;
}
static bool RcRegWait() {
  static const bool on = [] {
    bool v = RcEnvOn("RC_REG_WAIT");
    fprintf(stderr, "[rc] reg-ack wait: %s\n", v ? "on (v-reg)" : "off (v-nowait)");
    return v;
  }();
  return on;
}
static bool RcOutputAck() {
  static const bool on = [] {
    bool v = RcEnvOn("RC_OUTPUT_ACK");
    fprintf(stderr, "[rc] output-ack: %s\n", v ? "on (v31)" : "off (v-nowait)");
    return v;
  }();
  return on;
}

static bool RcChunkAckWait() {
  static const bool on = [] {
    const char* e = getenv("RC_CHUNK_ACK_WAIT");
    bool v = (e != nullptr && atoi(e) != 0);
    fprintf(stderr, "[rc] chunk-ack wait: %s\n", v ? "on (v22)" : "off (v-noack)");
    return v;
  }();
  return on;
}

static bool RcFileSplit() {
  static const bool on = [] {
    const char* e = getenv("RC_FILE_SPLIT");
    bool v = !(e != nullptr && atoi(e) == 0);
    fprintf(stderr, "[rc] send unit: %s\n", v ? "file (v-filesplit)" : "job");
    return v;
  }();
  return on;
}

// v-outq (2026-11-03): 출력도 입력처럼 "비어 있는 세그먼트 아무 데나".
//   RC_OUTPUT_ANYSEG=0 이면 예전처럼 잡 세그먼트 고정. 워커도 같은 값이어야 한다.
static bool RcOutAnySeg() {
  static const bool on = [] {
    const char* e = getenv("RC_OUTPUT_ANYSEG");
    bool v = !(e != nullptr && atoi(e) == 0);
    fprintf(stderr, "[rc] output segment: %s\n", v ? "any free (v-outq)" : "job-fixed");
    return v;
  }();
  return on;
}

Status CompactionDispatcher::DispatchAsync(
    CompactionSpec spec, std::shared_ptr<Compaction> compaction_holder,
    std::function<void(Status, CompactionSpec&&, CompactionResult&&,
                        std::vector<std::vector<char>>&&)>
        on_complete) {
  uint64_t job_id = spec.job_id;

  // v33 admission cap 제거됨 (2026-10-28) -- 더 이상 pending+in_flight가
  // kNumSegments 넘는다고 Busy 반환하지 않음. 큐는 무제한으로 쌓이고
  // SendLoop의 chunk-ack blocking wait가 자연스러운 백프레셔 역할을 함.
  // v38 (2026-10-28): 관측 전용 로그 -- cap은 안 걸지만, 지금 몇 개의
  // job이 pending+in_flight 상태로 떠있는지(= 그만큼의 L2/L3 범위가
  // being_compacted로 잠겨있을 가능성)만 매 dispatch마다 찍는다.
  {
    size_t cur_pending, cur_inflight;
    {
      std::lock_guard<std::mutex> lk(pending_send_mu_);
      cur_pending = pending_sends_.size();
    }
    {
      std::lock_guard<std::mutex> lk(inflight_mu_);
      cur_inflight = in_flight_.size();
    }
    fprintf(stderr,
            "[rc] DispatchAsync: backlog pending=%zu in_flight=%zu "
            "total=%zu job=%llu\n",
            cur_pending, cur_inflight, cur_pending + cur_inflight,
            (unsigned long long)job_id);
  }

  // ---- v-afb: 설치 큐 길이 기반 적응형 폴백 ----
  // 설치 큐(finish_queue_)가 RC_FB_QHI 이상이면 폴백을 켜고 RC_FB_QLO 이하면 끈다.
  // 켜져 있는 동안 출력 레벨 >= RC_FB_MIN_LEVEL 인 job은 예약 전에 Busy를 돌려준다.
  // 호출자(RunAsync 실패 경로, v-pout/v-fallback-own)가 같은 compaction을 로컬로 실행한다.
  {
    static const long kFbHi = [] {
      const char* e = getenv("RC_FB_QHI"); long v = (e && *e) ? atol(e) : 0;
      if (v < 0) v = 0; fprintf(stderr, "[rc] v-afb: RC_FB_QHI=%ld (0=off)\n", v); return v; }();
    static const long kFbLo = [] {
      const char* e = getenv("RC_FB_QLO"); long v = (e && *e) ? atol(e) : kFbHi / 2;
      if (v < 0) v = 0; if (v > kFbHi) v = kFbHi;
      fprintf(stderr, "[rc] v-afb: RC_FB_QLO=%ld\n", v); return v; }();
    static const int kFbMinLv = [] {
      const char* e = getenv("RC_FB_MIN_LEVEL"); int v = (e && *e) ? atoi(e) : 2;
      fprintf(stderr, "[rc] v-afb: RC_FB_MIN_LEVEL=%d\n", v); return v; }();
    static std::atomic<bool> fb_on{false};
    if (kFbHi > 0) {
      static const int kFbSrc = [] {
        const char* e = getenv("RC_FB_SRC");
        int v = (e && std::string(e) == "inflight") ? 1 : 0;
        fprintf(stderr, "[rc] v-afb: RC_FB_SRC=%s\n", v ? "inflight" : "finish"); return v; }();
      size_t q;
      if (kFbSrc == 1) { std::lock_guard<std::mutex> lk(inflight_mu_); q = in_flight_.size(); }
      else { std::lock_guard<std::mutex> lk(finish_mu_); q = finish_queue_.size(); }
      if (!fb_on.load() && static_cast<long>(q) >= kFbHi) {
        fb_on.store(true);
        fprintf(stderr, "[rc] v-afb: ON qlen=%zu\n", q);
      } else if (fb_on.load() && static_cast<long>(q) <= kFbLo) {
        fb_on.store(false);
        fprintf(stderr, "[rc] v-afb: OFF qlen=%zu\n", q);
      }
      const int out_lv = compaction_holder ? compaction_holder->output_level() : -1;
      if (fb_on.load() && out_lv >= kFbMinLv) {
        fprintf(stderr, "[rc] adaptive-fallback job=%llu qlen=%zu level=%d\n",
                (unsigned long long)job_id, q, out_lv);
        return Status::Busy("v-afb: install queue long -> local");
      }
    }
  }
  // ---- end v-afb ----

  std::vector<uint64_t> fnums;
  for (const auto& f : spec.inputs) fnums.push_back(f.file_number);

  // v11: 동기 Dispatch()와 동일하게 입력 SST 예약을 걸어야 함.
  // 이게 없으면 같은 SST 파일이 서로 다른 두 job(다른 세그먼트)에
  // 동시에 배정될 수 있고, worker 쪽에서 두 전송이 겹치면서
  // FILE_MISMATCH로 이어지는 게 실제로 관측됨.
  // 예약 실패 시 아직 아무것도 큐에 넣지 않았으므로(on_complete도
  // 호출된 적 없음) 그냥 Busy를 반환 — 호출자(RunAsync)가 이 경우
  // pending-outputs capture 등을 직접 해제해줘야 함.
  if (!reservation_.TryReserve(fnums)) {
    fprintf(stderr,
            "[rc] DispatchAsync: input SSTs already reserved, job=%llu\n",
            (unsigned long long)job_id);
    return Status::Busy("input SSTs already reserved");
  }

  AsyncJob job;
  job.spec = std::move(spec);
  job.fnums = std::move(fnums);
  job.compaction_holder = std::move(compaction_holder);
  job.on_complete = std::move(on_complete);
  job.t_total_start = std::chrono::steady_clock::now();
  job.enqueue_time = std::chrono::steady_clock::now();

  // v-multiseg: 파일마다 세그먼트를 따로 배정 (예전엔 job 전체가 세그먼트
  // 하나에 고정).
  std::map<int, std::vector<size_t>> by_seg;
  // v-oneseg (2026-10-31): v-multiseg(파일마다 세그먼트 따로 배정) 폐기.
  // job 하나 = 세그먼트 하나. 이 job의 입력 파일은 전부 같은 세그먼트로
  // 가고, SendLoop[job_seg]가 파일 단위로 순차 전송(파일마다 chunk-ack 대기).
  // RC_SINGLE_SEG=1 이면 모든 job을 세그먼트 0으로 고정(완전 직렬, 진단용).
  // v-gsendq: 전역 큐 모드면 세그먼트는 SendLoop가 꺼낼 때 정한다 (-1 = 미정).
  const int job_seg = RcGlobalSendQ()
                          ? -1
                          : (getenv("RC_SINGLE_SEG") ? 0 : PickLeastBusySegment());
  for (size_t i = 0; i < job.spec.inputs.size(); i++) {
    job.spec.inputs[i].assigned_seg = job_seg;
    by_seg[job_seg].push_back(i);
  }
  if (by_seg.empty()) {
    // 입력 파일이 0개인 예외 케이스에도 ctrl spec 전송 + 마무리가 한 번은
    // 돌아야 하므로 빈 태스크를 세그먼트 하나에 넣어준다.
    by_seg[job_seg];
  }
  int responsible_seg = by_seg.begin()->first;
  job.spec.seg = responsible_seg;  // job_seg_map_과 동일한 값 유지(출력 ack 라우팅용)

  auto ps = std::make_shared<PendingSend>();
  ps->remaining_files.store(static_cast<int>(job.spec.inputs.size()),
                             std::memory_order_relaxed);
  ps->responsible_seg = responsible_seg;
  ps->job = std::move(job);

  if (responsible_seg >= 0) {  // v-gsendq: 미정(-1)이면 SendLoop가 등록
    std::lock_guard<std::mutex> lk(job_seg_mu_);
    job_seg_map_[job_id] = responsible_seg;
  }
  {
    std::lock_guard<std::mutex> lk(pending_send_mu_);
    pending_sends_[job_id] = ps;
  }

  for (auto& kv : by_seg) {
    int seg = kv.first;
    SendTask task;
    task.job_id = job_id;
    task.file_indices = std::move(kv.second);
    if (seg < 0) {  // v-gsendq
      if (RcFileSplit() && task.file_indices.size() > 1) {  // v-filesplit
        {
          std::lock_guard<std::mutex> lk(gsend_mu_);
          for (size_t fi : task.file_indices) {
            SendTask one;
            one.job_id = job_id;
            one.file_indices.push_back(fi);
            gsend_queue_.push_back(std::move(one));
          }
        }
        gsend_cv_.notify_all();
        continue;
      }
      {
        std::lock_guard<std::mutex> lk(gsend_mu_);
        gsend_queue_.push_back(std::move(task));
      }
      gsend_cv_.notify_one();
      continue;
    }
    {
      std::lock_guard<std::mutex> lk(send_mu_[seg]);
      send_queues_[seg].push(std::move(task));
    }
    send_cv_[seg].notify_one();
  }
  return Status::OK();
}

// ── SendLoop : per-segment 송신 ────────────────────────────────────
void CompactionDispatcher::SendLoop(int seg_idx) {
  { char rc_tn[16]; snprintf(rc_tn, sizeof(rc_tn), "rc_send%d", seg_idx); pthread_setname_np(pthread_self(), rc_tn); }  // v-thname
  PinCurrentThreadToCore(8 + seg_idx);
  while (!channel_open_.load(std::memory_order_acquire)) {
    if (stop_.load(std::memory_order_acquire)) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  while (true) {
    SendTask task;
    if (RcGlobalSendQ()) {  // v-gsendq
      std::unique_lock<std::mutex> lk(gsend_mu_);
      gsend_cv_.wait(lk, [this] {
        return stop_.load(std::memory_order_acquire) || !gsend_queue_.empty();
      });
      if (stop_.load(std::memory_order_acquire) && gsend_queue_.empty())
        break;
      task = std::move(gsend_queue_.front());
      gsend_queue_.pop_front();
    } else {
      std::unique_lock<std::mutex> lk(send_mu_[seg_idx]);
      send_cv_[seg_idx].wait(lk, [this, seg_idx] {
        return stop_.load(std::memory_order_acquire) ||
               !send_queues_[seg_idx].empty();
      });
      if (stop_.load(std::memory_order_acquire) &&
          send_queues_[seg_idx].empty())
        break;
      task = std::move(send_queues_[seg_idx].front());
      send_queues_[seg_idx].pop();
    }

    uint64_t job_id = task.job_id;
    std::shared_ptr<PendingSend> ps;
    {
      std::lock_guard<std::mutex> lk(pending_send_mu_);
      auto it = pending_sends_.find(job_id);
      if (it == pending_sends_.end()) {
        fprintf(stderr, "[rc] SendLoop[%d]: unknown job_id=%llu (이미 정리됨?)\n",
                seg_idx, (unsigned long long)job_id);
        continue;
      }
      ps = it->second;
    }

    // v-gsendq: 전역 큐에서 꺼낸 잡은 여기서 이 SendLoop의 세그먼트에 묶는다.
    // spec(seg, 입력별 assigned_seg)은 아래에서 직렬화되어 워커로 가므로
    // 그 전에 채워야 하고, job_seg_map_은 Result/출력 ack 라우팅에 쓰인다.
    // v-filesplit: 한 잡의 파일들을 여러 SendLoop가 동시에 꺼낼 수 있으므로
    // 묶기는 락 안에서 한 번만 하고, 묶은 SendLoop만 작업 명세를 보낸다.
    // (예전 조건 seg_idx == responsible_seg 는 같은 SendLoop가 같은 잡의
    // 파일을 두 번 꺼내면 명세를 두 번 보내므로 쓰지 않는다.)
    bool send_spec = false;
    {
      std::lock_guard<std::mutex> blk(pending_send_mu_);
      if (ps->responsible_seg < 0) {
        ps->responsible_seg = seg_idx;
        ps->job.spec.seg = seg_idx;
        for (auto& in : ps->job.spec.inputs) in.assigned_seg = seg_idx;
        send_spec = true;
        std::lock_guard<std::mutex> lk(job_seg_mu_);
        job_seg_map_[job_id] = seg_idx;
      } else if (!RcGlobalSendQ()) {
        send_spec = (seg_idx == ps->responsible_seg);  // 세그먼트별 큐 모드: 기존 동작
      }
    }

    // v10: 큐 대기 시간 측정 (job 전체 기준)
    auto t1_start = std::chrono::steady_clock::now();
    auto queue_wait_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1_start - ps->job.enqueue_time).count();

    Status s;
    bool seg_failed = false;
    if (send_spec) {  // v-filesplit
      std::lock_guard<std::mutex> ctrl_lk(ctrl_send_mu_[seg_idx]);
      s = channel_.SendCtrlMsg(TagMessage(kMsgTypeSpec, ps->job.spec.Serialize()));
      if (!s.ok()) {
        fprintf(stderr, "[rc] SendLoop[%d]: SendCtrlMsg failed job=%llu: %s\n",
                seg_idx, (unsigned long long)job_id, s.ToString().c_str());
        seg_failed = true;
        ps->aborted.store(true, std::memory_order_relaxed);
      }
    }

    // v22: chunk-ack 대기 상태를 이 job 시작 시 초기화 (혹시 이전
    // job에서 남은 stale ready 플래그가 있으면 안 되므로 자기
    // 세그먼트 것만 리셋).
    // v29: chunk_ack_failed_도 같이 초기화.
    {
      std::lock_guard<std::mutex> ack_lk(chunk_ack_mu_[seg_idx]);
      chunk_ack_ready_[seg_idx] = false;
      chunk_ack_failed_[seg_idx] = false;
    }

    bool send_ok = !seg_failed;

    // v-reg: 워커가 이 job을 pending_recv_에 다 등록했다는 ack을 받을
    // 때까지 기다린다 (multi-seg job이면 이 job의 파일을 보내는 모든
    // 세그먼트의 SendLoop가 각자 이 공유 상태를 기다림).
    if (RcRegWait() && send_ok &&
        !ps->aborted.load(std::memory_order_relaxed)) {  // v-nowait
      std::unique_lock<std::mutex> reg_lk(job_registered_mu_);
      bool reg_ok = job_registered_cv_.wait_for(
          reg_lk, std::chrono::seconds(5),
          [this, job_id] {
            return job_registered_.count(job_id) > 0 ||
                   stop_.load(std::memory_order_acquire);
          });
      if (stop_.load(std::memory_order_acquire)) {
        send_ok = false;
      } else if (!reg_ok) {
        fprintf(stderr,
                "[rc] SendLoop[%d]: job=%llu 등록 ack TIMEOUT -- 포기\n",
                seg_idx, (unsigned long long)job_id);
        send_ok = false;
        ps->aborted.store(true, std::memory_order_relaxed);
      }
    }

    for (size_t ti = 0; send_ok && !ps->aborted.load(std::memory_order_relaxed) &&
                        ti < task.file_indices.size(); ti++) {
      size_t fi = task.file_indices[ti];
      const auto& f = ps->job.spec.inputs[fi];
      auto t_file0 = std::chrono::steady_clock::now();
      s = channel_.StreamSendSST(MakeSSTPath(opts_.db_path, f.file_number),
                                  f.file_number, f.file_size, seg_idx,
                                  job_id);  // v-nowait
      auto t_file1 = std::chrono::steady_clock::now();
      fprintf(stderr,
          "[timing] rc job=%llu seg=%d file=%llu size=%llu idx=%zu step_file_send=%lldms ok=%d\n",
          (unsigned long long)job_id, seg_idx,
          (unsigned long long)f.file_number, (unsigned long long)f.file_size, fi,
          (long long)std::chrono::duration_cast<std::chrono::milliseconds>(t_file1 - t_file0).count(),
          s.ok() ? 1 : 0);
      if (!s.ok()) {
        fprintf(stderr, "[rc] SendLoop[%d]: StreamSendSST failed: %s\n",
                seg_idx, s.ToString().c_str());
        send_ok = false;
        ps->aborted.store(true, std::memory_order_relaxed);
        break;
      }

      // v22: 같은 job의 다음 입력 파일을 보내기 전에, worker가 방금
      // 보낸 파일을 완전히 수신+ack했다는 chunk-ack을 ctrl 채널로
      // 받을 때까지 기다린다. raw consumed 플래그 폴링만 믿으면
      // worker가 이 세그먼트를 감시하기 전에 다음 파일로 덮어써버리는
      // 레이스가 실제로 재현됨(job=1, file11→file9 뒤바뀜).
      // v23b: job 경계에서도 같은 레이스가 재현됨(job의 마지막 입력
      // 다음 job으로 넘어갈 때 이전 job 마지막 파일을 worker가 아직
      // 못 받았는데 덮어써버림) — "마지막 입력이면 대기 생략" 예외를
      // 없애고, 몇 번째 입력이든 예외 없이 항상 chunk-ack을 기다린다.
      // v29: worker가 이 파일 수신에 실패하면(주로 FILE_MISMATCH)
      // chunk_ack_ready_와 함께 chunk_ack_failed_도 true로 세팅되어
      // 들어옴 — 그러면 30초를 채우지 않고 이 자리에서 바로 job을
      // 포기한다(기존엔 이 경우 아무 ack도 안 와서 30초 타임아웃까지
      // 그대로 기다렸고, 그 사이 세그먼트가 재사용되며 뒤이은
      // FILE_MISMATCH 연쇄로 이어졌음 — job=128/file=695 케이스).
      if (RcChunkAckWait()) {  // v-noack: 기본은 대기하지 않음
        std::unique_lock<std::mutex> ack_lk(chunk_ack_mu_[seg_idx]);
        uint64_t expect_file = f.file_number;
        // v24: 타임아웃으로 포기된 이전 파일의 ack이 뒤늦게 도착했을 때
        // 그걸 지금 기다리는(새) 파일의 ack으로 오인하는 버그가 있었음
        // (kNumSegments=1로 격리해도 FILE_MISMATCH 271건 재현 -> 세그먼트
        // 동시성 문제가 아니라 이 stale-ack 오인 버그였음). file_number가
        // 일치할 때만 통과시킨다.
        bool got = chunk_ack_cv_[seg_idx].wait_for(
            ack_lk, std::chrono::seconds(30),
            [this, seg_idx, expect_file] {
              return (chunk_ack_ready_[seg_idx] &&
                      chunk_ack_file_[seg_idx] == expect_file) ||
                     stop_.load(std::memory_order_acquire);
            });
        if (stop_.load(std::memory_order_acquire)) {
          send_ok = false;
          break;
        }
        if (!got) {
          fprintf(stderr,
                  "[rc] SendLoop[%d]: chunk-ack TIMEOUT job=%llu file=%llu\n",
                  seg_idx, (unsigned long long)job_id,
                  (unsigned long long)f.file_number);
          send_ok = false;
          ps->aborted.store(true, std::memory_order_relaxed);
          break;
        }
        // v29: 정상적으로 깨어났어도(타임아웃 아님) 실패 플래그가 서 있으면
        // 즉시 이 job을 포기한다 — worker가 이 파일을 못 받았다는 뜻이므로
        // 다음 파일을 계속 보내봤자 의미가 없고, 남은 30초를 더 기다릴
        // 이유도 없다.
        bool failed = chunk_ack_failed_[seg_idx];
        chunk_ack_ready_[seg_idx] = false;
        chunk_ack_failed_[seg_idx] = false;
        if (failed) {
          fprintf(stderr,
                  "[rc] SendLoop[%d]: chunk-ack FAILED (fast) job=%llu "
                  "file=%llu -> abandon job\n",
                  seg_idx, (unsigned long long)job_id,
                  (unsigned long long)f.file_number);
          send_ok = false;
          ps->aborted.store(true, std::memory_order_relaxed);
          break;
        }
      }
    }

    int remaining = ps->remaining_files.fetch_sub(
                        static_cast<int>(task.file_indices.size()),
                        std::memory_order_acq_rel) -
                    static_cast<int>(task.file_indices.size());
    if (remaining > 0) {
      // 다른 세그먼트가 아직 이 job의 나머지 파일을 보내는 중 —
      // 마무리는 마지막으로 끝내는 세그먼트가 담당.
      continue;
    }

    // v-nowait (one-state): pending 삭제와 in_flight_ 등록을 같은 락 안에서
    // 한 번에 한다(락 순서 pending_send_mu_ -> inflight_mu_, CtrlRecvLoop와
    // 같음). 그래서 결과가 언제 오든 둘 중 한 곳에서 반드시 찾는다. 결과가
    // 이미 보관돼 있으면 in_flight_를 거치지 않고 바로 넘긴다.
    auto t1_end = std::chrono::steady_clock::now();
    bool aborted_now = false;
    bool have_early  = false;
    CompactionResult early;
    int early_seg = 0;
    {
      std::lock_guard<std::mutex> lk(pending_send_mu_);
      pending_sends_.erase(job_id);
      aborted_now = ps->aborted.load(std::memory_order_relaxed);
      if (ps->result_ready) {
        have_early = true;
        early      = std::move(ps->early_result);
        early_seg  = ps->early_fixed_seg;
        ps->result_ready = false;
      }
      if (!aborted_now && !have_early) {
        ps->job.t_send_done = t1_end;
        std::lock_guard<std::mutex> ilk(inflight_mu_);
        in_flight_[job_id] = std::move(ps->job);
      }
    }

    if (aborted_now) {
      reservation_.Release(ps->job.fnums);
      if (ps->job.on_complete)
        ps->job.on_complete(Status::IOError("multi-seg StreamSendSST failed"),
                             std::move(ps->job.spec), std::move(ps->job.result),
                             std::move(ps->job.out_bufs));
      if (have_early && early.success && !early.output_files.empty()) {
        // 워커가 이미 보내고 있는 출력은 받아서 버린다(세그먼트 막힘 방지).
        PendingOutput po;
        po.job.result = std::move(early);
        po.job.out_bufs.resize(po.job.result.output_files.size());
        po.discard = true;
        if (RcOutAnySeg()) {  // v-outq: 세그먼트와 무관하게 받아서 버린다
          RegisterOutJob(std::move(po.job), /*discard=*/true);
        } else {
          {
            std::lock_guard<std::mutex> olk(output_mu_[early_seg]);
            output_queues_[early_seg].push(std::move(po));
          }
          output_cv_[early_seg].notify_one();
        }
      }
      continue;
    }

    fprintf(stderr, "[timing] rc job=%llu step1_send_sst=%lldms queue_wait=%lldms\n", (unsigned long long)job_id, (long long)std::chrono::duration_cast<std::chrono::milliseconds>(t1_end - t1_start).count(), (long long)queue_wait_ms);
    if (have_early) {
      ps->job.t_send_done = t1_end;
      fprintf(stderr, "[rc] SendLoop[%d]: job=%llu result arrived before send finished -- delivered now (v-nowait)\n",
              seg_idx, (unsigned long long)job_id);
      DeliverResult(std::move(ps->job), std::move(early), early_seg);
    } else {
      std::lock_guard<std::mutex> lk(inflight_mu_);
      fprintf(stderr, "[timing] rc job=%llu in_flight_size=%zu\n", (unsigned long long)job_id, in_flight_.size());
    }
  }
}

// ── CtrlRecvLoop ────────────────────────────────────────────────
void CompactionDispatcher::CtrlRecvLoop() {
  pthread_setname_np(pthread_self(), "rc_ctrl");  // v-thname
  PinCurrentThreadToCore(16);
  while (!channel_open_.load(std::memory_order_acquire)) {
    if (stop_.load(std::memory_order_acquire)) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  // v32: watchdog scan cadence tracker -- checked every loop iteration
  // (cheap clock compare), actual scan only every kWatchdogScanIntervalMs.
  // Deliberately placed at the top of the loop, not inside the !s.ok()
  // branch below, so it still runs even while real messages are flowing
  // constantly -- a busy system is exactly when a worker crash is most
  // likely, so the watchdog can't afford to only run while idle.
  auto last_watchdog_scan = std::chrono::steady_clock::now();
  int idle_spins = 0;  // v-nowait

  while (true) {
    {
      auto now = std::chrono::steady_clock::now();
      if (now - last_watchdog_scan >=
          std::chrono::milliseconds(kWatchdogScanIntervalMs)) {
        last_watchdog_scan = now;
        ReapStaleInFlightJobs();
        if (RcOutAnySeg()) ReapStaleOutJobs();  // v-outq
      }
    }

    std::string raw;
    bool got_one = false;
    for (int poll_seg = 0; poll_seg < kNumSegments; poll_seg++) {
      Status s = channel_.TryRecvResultMsg(poll_seg, &raw);
      if (s.ok()) { got_one = true; break; }
    }
    if (!got_one) {
      if (stop_.load(std::memory_order_acquire)) break;
      // v-nowait: 1ms sleep이면 메시지마다 최대 ~1ms 지연. 잠깐 yield로 돌고,
      // 그래도 없으면 50us만 잔다.
      if (++idle_spins < RcIdleYield()) std::this_thread::yield();  // v-spinwait
      else std::this_thread::sleep_for(std::chrono::microseconds(RcIdleSleepUs()));
      continue;
    }
    idle_spins = 0;
    if (raw.empty()) {
      continue;
    }

    char tag = raw[0];

    // v22: chunk-ack — worker가 job 내 파일 하나를 다 받았다는 신호.
    // v-reg: 워커의 "job 등록 완료" ack. tag(1) + job_id(8) = 9바이트.
    if (tag == kMsgTypeJobRegistered) {
      if (raw.size() < 1 + sizeof(uint64_t)) {
        fprintf(stderr,
                "[rc] CtrlRecvLoop: malformed job-registered ack (size=%zu)\n",
                raw.size());
        continue;
      }
      uint64_t reg_job_id = 0;
      memcpy(&reg_job_id, raw.data() + 1, sizeof(reg_job_id));
      {
        std::lock_guard<std::mutex> lk(job_registered_mu_);
        job_registered_[reg_job_id] = true;
      }
      job_registered_cv_.notify_all();
      continue;
    }

    // job_id(8)+file_number(8)를 파싱해서 해당 세그먼트의 SendLoop를
    // 깨운다. CompactionResult로 취급하지 않고 여기서 바로 소비한다.
    // v29: 페이로드에 success(1바이트)가 tag 바로 다음에 추가됨
    // (worker_server.cc v11의 MakeChunkAckPayload와 순서 일치):
    // tag(1) + success(1) + job_id(8) + file_number(8) = 18바이트.
    if (tag == kMsgTypeChunkAck) {
      constexpr size_t kChunkAckMinSize =
          1 /*tag*/ + 1 /*success*/ + sizeof(uint64_t) * 2 + sizeof(uint32_t);
      if (raw.size() < kChunkAckMinSize) {
        fprintf(stderr,
                "[rc] CtrlRecvLoop: malformed chunk-ack (size=%zu)\n",
                raw.size());
        continue;
      }
      uint8_t success_byte = static_cast<uint8_t>(raw[1]);
      uint64_t ack_job_id = 0, ack_file_number = 0;
      memcpy(&ack_job_id, raw.data() + 2, sizeof(ack_job_id));
      memcpy(&ack_file_number, raw.data() + 2 + sizeof(ack_job_id),
             sizeof(ack_file_number));
      uint32_t ack_seg_u32 = 0;
      memcpy(&ack_seg_u32,
             raw.data() + 2 + sizeof(ack_job_id) + sizeof(ack_file_number),
             sizeof(ack_seg_u32));
      bool success = (success_byte != 0);
      // v-ackseg: job_seg_map_(=responsible_seg, 출력 ack 라우팅 전용)으로
      // 조회하면 이 잡의 다른 세그먼트로 보낸 파일의 ack가 엉뚱한
      // 세그먼트로 라우팅된다 (multi-seg 분배 후 버그). worker가 직접
      // 실어보낸 seg를 그대로 신뢰한다.
      int seg = static_cast<int>(ack_seg_u32);
      if (seg < 0 || seg >= kNumSegments) {
        seg = static_cast<int>(ack_job_id % static_cast<uint64_t>(kNumSegments));
      }
      {
        std::lock_guard<std::mutex> lk(chunk_ack_mu_[seg]);
        chunk_ack_ready_[seg] = true;
        chunk_ack_file_[seg] = ack_file_number;
        chunk_ack_failed_[seg] = !success;
      }
      chunk_ack_cv_[seg].notify_one();
      // perf 클린업: 정상(success) 경로는 매 파일마다 찍히는 hot-path라
      // 제거. 실패만 남겨서 진단 가치는 유지.
      if (!success) {
        fprintf(stderr,
                "[rc] CtrlRecvLoop: chunk-ack FAILED job=%llu file=%llu "
                "seg=%d\n",
                (unsigned long long)ack_job_id,
                (unsigned long long)ack_file_number, seg);
        if (!RcChunkAckWait()) {  // v-noack: 기다리는 SendLoop가 없으므로 여기서 처리
          {
            std::lock_guard<std::mutex> plk(pending_send_mu_);
            auto pit = pending_sends_.find(ack_job_id);
            if (pit != pending_sends_.end())
              pit->second->aborted.store(true, std::memory_order_relaxed);
          }
          bool found_inflight = false;
          AsyncJob failed_job;
          {
            std::lock_guard<std::mutex> ilk(inflight_mu_);
            auto iit = in_flight_.find(ack_job_id);
            if (iit != in_flight_.end()) {
              failed_job = std::move(iit->second);
              in_flight_.erase(iit);
              found_inflight = true;
            }
          }
          if (found_inflight) {
            {
              std::lock_guard<std::mutex> jlk(job_seg_mu_);
              job_seg_map_.erase(ack_job_id);
            }
            reservation_.Release(failed_job.fnums);
            {
              std::lock_guard<std::mutex> flk(finish_mu_);
              finish_queue_.push_back(
                  {Status::IOError("worker reported input file receive failure"),
                   std::move(failed_job)});
            }
            finish_cv_.notify_one();
            fprintf(stderr, "[rc] CtrlRecvLoop: job=%llu failed by chunk-ack FAILED (v-noack)\n",
                    (unsigned long long)ack_job_id);
          }
        }
      }
      continue;
    }

    if (tag != kMsgTypeResult) {
      fprintf(stderr,
              "[rc] CtrlRecvLoop: unknown ctrl tag=0x%02x (size=%zu), skip\n",
              (unsigned char)tag, raw.size());
      continue;
    }

    Slice result_payload(raw.data() + 1, raw.size() - 1);
    CompactionResult result;
    if (!CompactionResult::Deserialize(result_payload, &result)) {
      fprintf(stderr, "[rc] CtrlRecvLoop: deserialize failed\n");
      continue;
    }

    uint64_t job_id = result.job_id;
    int fixed_seg = 0; { std::lock_guard<std::mutex> lk(job_seg_mu_); auto sit = job_seg_map_.find(job_id); if (sit != job_seg_map_.end()) { fixed_seg = sit->second; job_seg_map_.erase(sit); } else { fixed_seg = static_cast<int>(job_id % static_cast<uint64_t>(kNumSegments)); } }
    { std::lock_guard<std::mutex> lk(job_registered_mu_); job_registered_.erase(job_id); }  // v-reg cleanup

    // v-nowait (one-state): 잡은 pending_sends_(보내는 중) -> in_flight_(다 보냄)
    // 한 줄기로만 움직이고, 둘 다 pending_send_mu_ -> inflight_mu_ 순서로 본다.
    // 아직 보내는 중이면 결과를 그 잡에 보관 -> 마지막 SendLoop가 넘긴다.
    AsyncJob job;
    bool have_job = false;
    bool parked   = false;
    {
      std::lock_guard<std::mutex> plk(pending_send_mu_);
      auto pit = pending_sends_.find(job_id);
      if (pit != pending_sends_.end() &&
          !pit->second->aborted.load(std::memory_order_relaxed)) {
        pit->second->early_result    = std::move(result);
        pit->second->early_fixed_seg = fixed_seg;
        pit->second->result_ready    = true;
        parked = true;
      } else {
        std::lock_guard<std::mutex> ilk(inflight_mu_);
        auto it = in_flight_.find(job_id);
        if (it != in_flight_.end()) {
          job = std::move(it->second);
          in_flight_.erase(it);
          have_job = true;
        }
      }
    }
    if (parked) {
      fprintf(stderr, "[rc] CtrlRecvLoop: job=%llu result parked (inputs still sending)\n",
              (unsigned long long)job_id);
      continue;
    }
    if (!have_job) {
      // 전송 실패로 포기했거나 워치독이 걷어간 잡. 설치는 안 하지만 워커가
      // 이미 보내는 출력은 받아서 버려야 그 세그먼트가 막히지 않는다.
      fprintf(stderr, "[rc] CtrlRecvLoop: unknown job_id=%llu (outputs=%zu, drain)\n",
              (unsigned long long)job_id, result.output_files.size());
      if (result.success && !result.output_files.empty()) {
        PendingOutput po;
        po.job.result = std::move(result);
        po.job.out_bufs.resize(po.job.result.output_files.size());
        po.discard = true;
        if (RcOutAnySeg()) {  // v-outq: 세그먼트와 무관하게 받아서 버린다
          RegisterOutJob(std::move(po.job), /*discard=*/true);
        } else {
          {
            std::lock_guard<std::mutex> olk(output_mu_[fixed_seg]);
            output_queues_[fixed_seg].push(std::move(po));
          }
          output_cv_[fixed_seg].notify_one();
        }
      }
      continue;
    }

    DeliverResult(std::move(job), std::move(result), fixed_seg);
  }

  for (int i = 0; i < kNumSegments; i++) output_cv_[i].notify_all();
}

// ── DeliverResult (v-nowait) ──────────────────────────────────────
// CtrlRecvLoop와, 결과가 먼저 와서 보관된 경우의 마지막 SendLoop가 같이
// 쓰는 결과 처리. 내용은 예전 CtrlRecvLoop 안의 코드와 같다.
void CompactionDispatcher::DeliverResult(AsyncJob&& job_in,
                                         CompactionResult&& result,
                                         int fixed_seg) {
  AsyncJob job = std::move(job_in);
  uint64_t job_id = result.job_id;
  if (!result.success) {
    fprintf(stderr, "[rc] CtrlRecvLoop: worker failed job=%llu: %s\n",
            (unsigned long long)job_id, result.error_msg.c_str());
    reservation_.Release(job.fnums);
    {
      std::lock_guard<std::mutex> lk(finish_mu_);
      finish_queue_.push_back(
          {Status::IOError("worker failed: " + result.error_msg),
           std::move(job)});
    }
    finish_cv_.notify_one();
    return;
  }

  fprintf(stderr, "[timing] rc job=%llu step2_worker_roundtrip=%lldms\n", (unsigned long long)job_id, (long long)std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - job.t_send_done).count());
  job.result = std::move(result);
  job.out_bufs.resize(job.result.output_files.size());
  if (RcOutAnySeg()) {  // v-outq: 세그먼트 큐 대신 잡별 수신 표에 등록
    RegisterOutJob(std::move(job), /*discard=*/false);
    return;
  }
  {
    std::lock_guard<std::mutex> lk(output_mu_[fixed_seg]);
    output_queues_[fixed_seg].push({std::move(job)});
  }
  output_cv_[fixed_seg].notify_one();
}

// ── ReapStaleInFlightJobs (new, v32) ────────────────────────────────
// Scans in_flight_ for jobs that have been waiting past
// kJobResultTimeoutMs since t_send_done (i.e. since SendLoop finished
// pushing all of that job's input files) for a CompactionResult that
// never arrived -- worker crashed, hung, or the NTB link died after the
// inputs went out. Force-fails each one exactly the way the normal
// !result.success path in CtrlRecvLoop already does: release the
// reservation, drop the job_seg_map_ entry, push a TimedOut FinishJob
// for FinishWorkerLoop to pick up.
//
// Without this, such a job sat in in_flight_ forever: its input files'
// reservation never released, its manifest edit never written.
void CompactionDispatcher::ReapStaleInFlightJobs() {
  auto now = std::chrono::steady_clock::now();
  std::vector<std::pair<uint64_t, AsyncJob>> stale;

  {
    std::lock_guard<std::mutex> lk(inflight_mu_);
    for (auto it = in_flight_.begin(); it != in_flight_.end(); ) {
      auto age_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
          now - it->second.t_send_done).count();
      if (age_ms > kJobResultTimeoutMs) {
        fprintf(stderr,
               "[rc] CtrlRecvLoop watchdog: job=%llu waited %lldms for a "
               "CompactionResult that never arrived (limit=%dms) -- "
               "force-failing it\n",
               (unsigned long long)it->first, (long long)age_ms,
               kJobResultTimeoutMs);
        stale.emplace_back(it->first, std::move(it->second));
        it = in_flight_.erase(it);
      } else {
        ++it;
      }
    }
  }
  if (stale.empty()) return;

  for (auto& kv : stale) {
    uint64_t job_id = kv.first;
    AsyncJob& job    = kv.second;

    {
      std::lock_guard<std::mutex> lk(job_seg_mu_);
      job_seg_map_.erase(job_id);
    }
    reservation_.Release(job.fnums);

    std::lock_guard<std::mutex> lk(finish_mu_);
    finish_queue_.push_back(
        {Status::TimedOut("CtrlRecvLoop watchdog: no CompactionResult "
                          "within timeout, worker presumed dead/stuck"),
         std::move(job)});
  }
  finish_cv_.notify_all();
}

// ── OutputRecvLoop ─────────────────────────────────────────────────
void CompactionDispatcher::OutputRecvLoop(int seg_idx) {
  { char rc_tn[16]; snprintf(rc_tn, sizeof(rc_tn), "rc_out%d", seg_idx); pthread_setname_np(pthread_self(), rc_tn); }  // v-thname
  PinCurrentThreadToCore(17 + seg_idx);
  while (!channel_open_.load(std::memory_order_acquire)) {
    if (stop_.load(std::memory_order_acquire)) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  if (RcOutAnySeg()) { OutputRecvLoopAny(seg_idx); return; }  // v-outq

  while (true) {
    PendingOutput pending;
    {
      std::unique_lock<std::mutex> lk(output_mu_[seg_idx]);
      output_cv_[seg_idx].wait(lk, [this, seg_idx] {
        return stop_.load(std::memory_order_acquire) ||
               !output_queues_[seg_idx].empty();
      });
      if (stop_.load(std::memory_order_acquire) &&
          output_queues_[seg_idx].empty())
        break;
      pending = std::move(output_queues_[seg_idx].front());
      output_queues_[seg_idx].pop();
    }

    AsyncJob& job = pending.job;
    uint64_t job_id = job.result.job_id;
    fprintf(stderr, "[timing] rc job=%llu output_queue_wait=%lldms\n", (unsigned long long)job_id, (long long)std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - pending.enqueue_time).count());
    bool recv_ok = true;
    auto t3_start = std::chrono::steady_clock::now();

    for (size_t i = 0; i < job.result.output_files.size(); i++) {
      const auto& of = job.result.output_files[i];
      auto t_of0 = std::chrono::steady_clock::now();
      Status s = channel_.StreamRecvOutputSST(of.file_number, of.file_size,
                                               &job.out_bufs[i], seg_idx,
                                               job_id);  // v-stale
      auto t_of1 = std::chrono::steady_clock::now();
      fprintf(stderr,
          "[timing] rc job=%llu seg=%d merge_file=%llu size=%llu idx=%zu step_file_recv=%lldms ok=%d\n",
          (unsigned long long)job_id, seg_idx,
          (unsigned long long)of.file_number, (unsigned long long)of.file_size, i,
          (long long)std::chrono::duration_cast<std::chrono::milliseconds>(t_of1 - t_of0).count(),
          s.ok() ? 1 : 0);
      if (!s.ok()) {
        fprintf(stderr,
                "[rc] OutputRecvLoop[%d]: StreamRecvOutputSST failed "
                "job=%llu: %s\n",
                seg_idx, (unsigned long long)job_id, s.ToString().c_str());
        // v31: 실패도 즉시 output-ack(실패)으로 worker에 알린다. 이걸
        // 안 하면 worker의 CompactionLoop가 다음 output 파일을 보내기
        // 전에 (input 쪽 chunk-ack과 대칭으로) 최대 30초를 기다리게
        // 되는데, 실패를 즉시 알려서 그 자리에서 바로 포기하게 한다.
        if (RcOutputAck()) {  // v-nowait
          std::lock_guard<std::mutex> ctrl_lk(ctrl_send_mu_[seg_idx]);
          Status as = channel_.SendSpecMsg(MakeOutputAckPayload(
              job_id, of.file_number, /*success=*/false));
          fprintf(stderr,
                  "[rc] OutputRecvLoop[%d]: sent FAILED output-ack "
                  "job=%llu file=%llu status=%s\n",
                  seg_idx, (unsigned long long)job_id,
                  (unsigned long long)of.file_number, as.ToString().c_str());
        }
        recv_ok = false;
        break;
      }

      // v-crcdiag: worker가 보낸 content_crc32와 실제로 받은
      // job.out_bufs[i]의 crc32를 비교한다. 불일치하면 persist/install을
      // 절대 하지 않고 실패로 처리 -- NTB 전송 경로(또는 그 이후 메모리
      // 조작) 문제라는 뜻이고, 일치하는데도 나중에 sst_dump에서 깨지면
      // PersistOutputSSTs 이후 단계가 범인이라는 뜻이다.
      {
        uint32_t actual_crc =
            NtbDiagCrc32(job.out_bufs[i].data(), job.out_bufs[i].size());
        if (actual_crc != of.content_crc32) {
          fprintf(stderr,
                  "[CRC_MISMATCH] job=%llu file=%llu seg=%d "
                  "expected_crc=%u actual_crc=%u size=%zu\n",
                  (unsigned long long)job_id,
                  (unsigned long long)of.file_number, seg_idx,
                  of.content_crc32, actual_crc, job.out_bufs[i].size());
          if (RcOutputAck()) {  // v-nowait
            std::lock_guard<std::mutex> ctrl_lk(ctrl_send_mu_[seg_idx]);
            Status as = channel_.SendSpecMsg(MakeOutputAckPayload(
                job_id, of.file_number, /*success=*/false));
            fprintf(stderr,
                    "[rc] OutputRecvLoop[%d]: sent FAILED(CRC) output-ack "
                    "job=%llu file=%llu status=%s\n",
                    seg_idx, (unsigned long long)job_id,
                    (unsigned long long)of.file_number, as.ToString().c_str());
          }
          recv_ok = false;
          break;
        }
      }

      // v31: 이 output 파일을 완전히 받았으니 worker에게 즉시 ack —
      // worker는 이걸 받은 뒤에야 같은 job의 다음 output 파일을 보낸다
      // (input 쪽 v22 chunk-ack과 대칭). 이게 없으면 output 파일이
      // 많은 job(86개까지 실측)에서 세그먼트를 여러 파일이 순차 공유
      // 하다 겹치는 레이스가 생겨 L1 파일 키 범위 겹침 부패로 이어짐.
      if (RcOutputAck()) {  // v-nowait
        std::lock_guard<std::mutex> ctrl_lk(ctrl_send_mu_[seg_idx]);
        channel_.SendSpecMsg(MakeOutputAckPayload(job_id, of.file_number,
                                                   /*success=*/true));
      }
    }

    fprintf(stderr, "[timing] rc job=%llu step3_recv_sst=%lldms worker_compact=%lldms\n", (unsigned long long)job_id, (long long)std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t3_start).count(), (long long)(job.result.elapsed_us / 1000));
    if (pending.discard) {  // v-nowait: 포기된 잡의 출력 -- 받기만 하고 버림
      fprintf(stderr, "[rc] OutputRecvLoop[%d]: drained outputs of abandoned job=%llu ok=%d\n",
              seg_idx, (unsigned long long)job_id, recv_ok ? 1 : 0);
      continue;
    }
    Status finish_status =
        recv_ok ? Status::OK()
                : Status::IOError("StreamRecvOutputSST failed");
    if (!recv_ok) reservation_.Release(job.fnums);

    {
      std::lock_guard<std::mutex> lk(finish_mu_);
      finish_queue_.push_back({finish_status, std::move(pending.job)});
      fprintf(stderr, "[timing] rc job=%llu finish_queue_depth=%zu\n", (unsigned long long)job_id, finish_queue_.size());
    }
    finish_cv_.notify_one();
  }
}

// ── v-outq: 출력도 "비어 있는 세그먼트 아무 데나" ─────────────────────
// 워커가 출력 파일을 잡 세그먼트에 고정하지 않고 빈 세그먼트로 보내므로, 프라이머리는
// 세그먼트마다 도착한 청크 헤더의 (job_id, file_number)로 주인을 찾는다 (워커가 입력을
// 받는 v-byfile과 같은 구조). 결과가 등록되기 전에 온 청크는 소비하지 않고 두었다가
// 등록되면 받는다 -- 워커는 결과를 출력보다 먼저, 성공할 때까지 보낸다.

// ── v-stream: 출력 SST를 도착하는 대로 임시 이름(rcs_<job>_<worker번호>.tmp)으로 persist ──
// FinishJob(RcStreamFinalize)에서 최종 번호로 rename. 실패/포기한 잡은 RcStreamAbort가 임시 파일 삭제.
constexpr size_t kRcStreamMaxFiles = 4096;
struct RcStreamJob {
  std::mutex mu;
  std::condition_variable cv;
  int pending = 0;
  bool failed = false;
  std::map<uint64_t, std::string> tmp;  // worker 파일 번호 -> 임시 경로
};
struct RcStreamTask {
  std::shared_ptr<RcStreamJob> sj;
  uint64_t job_id = 0, wfnum = 0;
  std::string path;
  const char* data = nullptr;
  size_t size = 0;
};
static std::mutex g_rs_mu;
static std::map<uint64_t, std::shared_ptr<RcStreamJob>> g_rs_jobs;
static std::mutex& g_rs_qmu = *new std::mutex;  // v-stream-exitfix: leaked, no dtor at exit()
static std::condition_variable& g_rs_qcv = *new std::condition_variable;  // v-stream-exitfix
static std::queue<RcStreamTask>& g_rs_q = *new std::queue<RcStreamTask>;  // v-stream-exitfix
static std::once_flag g_rs_once;

int RcStreamMode() {
  static const int v = [] {
    const char* e = getenv("RC_STREAM_PERSIST");
    int x = e ? atoi(e) : 0;
    if (x < 0 || x > 2) x = 0;
    fprintf(stderr, "[rc] v-stream persist mode: %d (0=off 1=on-arrival 2=before-result)\n", x);
    return x;
  }();
  return v;
}
static int RcStreamThreads() {
  static const int v = [] {
    const char* e = getenv("RC_STREAM_PERSIST_THREADS");
    int x = e ? atoi(e) : 4;
    if (x < 1) x = 1;
    if (x > 64) x = 64;
    fprintf(stderr, "[rc] v-stream persist threads: %d\n", x);
    return x;
  }();
  return v;
}
static std::shared_ptr<RcStreamJob> RcStreamGet(uint64_t job_id, bool create) {
  std::lock_guard<std::mutex> lk(g_rs_mu);
  auto it = g_rs_jobs.find(job_id);
  if (it != g_rs_jobs.end()) return it->second;
  if (!create) return nullptr;
  auto sj = std::make_shared<RcStreamJob>();
  g_rs_jobs[job_id] = sj;
  return sj;
}
// v-finooo: 설치 큐에서 '스트림 쓰기가 끝난 job'을 먼저 꺼내기 위한 상태
static int RcFinOoo() {
  static const int v = [] {
    const char* e = getenv("RC_FIN_OOO");
    int x = (e && *e) ? atoi(e) : 0;
    fprintf(stderr, "[rc] v-finooo: RC_FIN_OOO=%d (0=FIFO 1=ready-first)\n", x);
    return x;
  }();
  return v;
}
static std::atomic<std::mutex*> g_fin_mu_ptr{nullptr};               // v-finooo: CompactionDispatcher::finish_mu_
static std::atomic<std::condition_variable*> g_fin_cv_ptr{nullptr};  // v-finooo: CompactionDispatcher::finish_cv_
static bool RcStreamDone(uint64_t job_id) {  // v-finooo: 이 job의 스트림 파일이 모두 써졌나 (스트림 항목 없으면 true)
  auto sj = RcStreamGet(job_id, false);
  if (!sj) return true;
  std::lock_guard<std::mutex> lk(sj->mu);
  return sj->pending == 0;
}
static void RcFinKick() {  // v-finooo: 쓰기 스레드가 job의 마지막 파일을 끝내면 설치 스레드를 깨움
  std::mutex* m = g_fin_mu_ptr.load();
  std::condition_variable* cv = g_fin_cv_ptr.load();
  if (!m || !cv) return;
  { std::lock_guard<std::mutex> lk(*m); }  // 설치 스레드의 검사~대기 사이에 끼지 않게 (lost wakeup 방지)
  cv->notify_all();
}
static void RcStreamStart() {
  std::call_once(g_rs_once, [] {
    for (int i = 0; i < RcStreamThreads(); i++) {
      std::thread([i] {
        char tn[16]; snprintf(tn, sizeof(tn), "rc_sp%d", i); pthread_setname_np(pthread_self(), tn);
        while (true) {
          RcStreamTask t;
          {
            std::unique_lock<std::mutex> lk(g_rs_qmu);
            g_rs_qcv.wait(lk, [] { return !g_rs_q.empty(); });
            t = std::move(g_rs_q.front());
            g_rs_q.pop();
          }
          auto t0 = std::chrono::steady_clock::now();
          int err = ::RcStreamWriteFileExt(t.path, t.data, t.size);
          if (err != 0) ::remove(t.path.c_str());
          fprintf(stderr, "[timing] rc stream_persist job=%llu wfile=%llu bytes=%zu ms=%lld err=%d\n",
                  (unsigned long long)t.job_id, (unsigned long long)t.wfnum, t.size,
                  (long long)std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - t0).count(), err);
          bool rs_zero = false;  // v-finooo
          {
            std::lock_guard<std::mutex> lk(t.sj->mu);
            if (err != 0) t.sj->failed = true;
            t.sj->pending--;
            rs_zero = (t.sj->pending == 0);  // v-finooo
          }
          t.sj->cv.notify_all();
          if (rs_zero && RcFinOoo()) RcFinKick();  // v-finooo
        }
      }).detach();
    }
  });
}
static void RcStreamSubmit(uint64_t job_id, uint64_t wfnum, const std::string& db_path,
                           const char* data, size_t size) {
  RcStreamStart();
  auto sj = RcStreamGet(job_id, true);
  char nm[80];
  snprintf(nm, sizeof(nm), "/rcs_%llu_%llu.tmp", (unsigned long long)job_id, (unsigned long long)wfnum);
  RcStreamTask t;
  t.sj = sj; t.job_id = job_id; t.wfnum = wfnum; t.path = db_path + nm; t.data = data; t.size = size;
  {
    std::lock_guard<std::mutex> lk(sj->mu);
    sj->pending++;
    sj->tmp[wfnum] = t.path;
  }
  {
    std::lock_guard<std::mutex> lk(g_rs_qmu);
    g_rs_q.push(std::move(t));
  }
  g_rs_qcv.notify_one();
}
// ===== v-ring: 수신 스레드(세그먼트)별 링 버퍼 + 짝 쓰기 스레드 =====
//   RC_STREAM_RING=1 : 한 청크로 오는 출력 파일을 링 칸(4 KiB 정렬)에 memcpy①+ack, 짝 쓰기 스레드가 그 칸에서 바로 pwrite
//   RC_RING_MB=1024  : 링 하나 크기(MiB)
//   v-ringlf: 링 줄/칸 계산을 잠금 없는 SPSC로 (생산자=수신 i, 소비자=쓰기 i). 잠금은 쓰기 스레드가 잠들 때만
struct RcRingSlot { char* p = nullptr; size_t len = 0, waste = 0; };
struct RcRingEnt { RcStreamTask t; RcRingSlot s; bool skip = false; };
constexpr size_t kRcRingQ = 16384;  // v-ringlf: 줄 칸 수 (2의 거듭제곱)
struct RcRing {
  std::once_flag once;
  char* base = nullptr;
  size_t cap = 0;
  size_t head = 0;                    // 수신 스레드만 씀
  std::atomic<size_t> used{0};        // 수신 +, 쓰기 -
  RcRingEnt* q = nullptr;             // 길이 kRcRingQ
  std::atomic<uint64_t> qtail{0};     // 넣은 수 (수신만 씀)
  std::atomic<uint64_t> qhead{0};     // 꺼낸 수 (쓰기만 씀)
  std::atomic<bool> sleeping{false};  // 쓰기 스레드가 잠들려는 중
  std::mutex wmu;                     // 잠들기/깨우기에만 사용
  std::condition_variable wcv;
};
static RcRing* const g_ring = new RcRing[64];  // leaked on purpose (detached threads)
static int RcRingOn() {
  static const int v = [] {
    const char* e = getenv("RC_STREAM_RING");
    int x = (e && *e) ? atoi(e) : 0;
    fprintf(stderr, "[rc] v-ring: RC_STREAM_RING=%d (1=per-receive-thread ring + paired writer)\n", x);
    return x;
  }();
  return v;
}
static size_t RcRingBytes() {
  static const size_t v = [] {
    const char* e = getenv("RC_RING_MB");
    long x = (e && *e) ? atol(e) : 1024;
    if (x < 128) x = 128;
    if (x > 16384) x = 16384;
    fprintf(stderr, "[rc] v-ring: RC_RING_MB=%ld per receive thread\n", x);
    return static_cast<size_t>(x) << 20;
  }();
  return v;
}
static int RcRingWriteFile(const std::string& dst, const char* p, size_t size) {
  const int flags = O_WRONLY | O_CREAT | O_TRUNC;
  int fd = ::open(dst.c_str(), flags | O_DIRECT, 0644);
  const bool direct = fd >= 0;
  if (fd < 0) fd = ::open(dst.c_str(), flags, 0644);
  if (fd < 0) return errno ? errno : EIO;
  if (size > 0) (void)::fallocate(fd, FALLOC_FL_KEEP_SIZE, 0, (off_t)size);
  const size_t wn = direct ? (size + 4095) / 4096 * 4096 : size;
  int e = 0;
  size_t done = 0;
  while (done < wn) {
    ssize_t r = ::pwrite(fd, p + done, wn - done, (off_t)done);
    if (r < 0) { if (errno == EINTR) continue; e = errno; break; }
    if (r == 0) { e = EIO; break; }
    done += (size_t)r;
  }
  if (e == 0 && direct && wn != size && ::ftruncate(fd, (off_t)size) != 0) e = errno;
  if (e == 0 && ::fdatasync(fd) != 0) e = errno;
  if (::close(fd) != 0 && e == 0) e = errno;
  return e;
}
static void RcRingWriterLoop(int i) {
  char tn[16]; snprintf(tn, sizeof(tn), "rc_rw%d", i); pthread_setname_np(pthread_self(), tn);
  RcRing& r = g_ring[i];
  uint64_t h = 0;
  while (true) {
    if (r.qtail.load(std::memory_order_seq_cst) == h) {  // 줄이 비었음 -> 잠들기 (깨우기 신호 놓치지 않게 두 번 확인)
      r.sleeping.store(true, std::memory_order_seq_cst);
      if (r.qtail.load(std::memory_order_seq_cst) == h) {
        std::unique_lock<std::mutex> lk(r.wmu);
        r.wcv.wait_for(lk, std::chrono::milliseconds(1),
                       [&] { return r.qtail.load(std::memory_order_acquire) != h; });
      }
      r.sleeping.store(false, std::memory_order_seq_cst);
      continue;
    }
    RcRingEnt en = std::move(r.q[h & (kRcRingQ - 1)]);
    h++;
    r.qhead.store(h, std::memory_order_release);  // 줄 칸은 바로 비움 (데이터 칸은 쓰기 후 반납)
    if (!en.skip) {
      RcStreamTask& t = en.t;
      auto t0 = std::chrono::steady_clock::now();
      int err = RcRingWriteFile(t.path, t.data, t.size);
      if (err != 0) ::remove(t.path.c_str());
      fprintf(stderr, "[timing] rc stream_persist job=%llu wfile=%llu bytes=%zu ms=%lld err=%d ring=%d\n",
              (unsigned long long)t.job_id, (unsigned long long)t.wfnum, t.size,
              (long long)std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::steady_clock::now() - t0).count(), err, i);
      bool rs_zero = false;
      {
        std::lock_guard<std::mutex> lk(t.sj->mu);
        if (err != 0) t.sj->failed = true;
        t.sj->pending--;
        rs_zero = (t.sj->pending == 0);
      }
      t.sj->cv.notify_all();
      if (rs_zero && RcFinOoo()) RcFinKick();
    }
    r.used.fetch_sub(en.s.len + en.s.waste, std::memory_order_release);  // 데이터 칸 반납
  }
}
static RcRingSlot RcRingAcquire(int i, size_t n) {
  RcRingSlot s;
  if (i < 0 || i >= 64 || n == 0) return s;
  RcRing& r = g_ring[i];
  std::call_once(r.once, [&r, i] {
    void* b = nullptr;
    if (posix_memalign(&b, 4096, RcRingBytes()) == 0) { r.base = static_cast<char*>(b); r.cap = RcRingBytes(); }
    else fprintf(stderr, "[rc] v-ring: ring %d alloc failed -> old path\n", i);
    r.q = new RcRingEnt[kRcRingQ];
    fprintf(stderr, "[rc] v-ringlf: ring %d lock-free SPSC (queue %zu)\n", i, kRcRingQ);
    std::thread(RcRingWriterLoop, i).detach();
  });
  const size_t need = (n + 4095) / 4096 * 4096;
  if (!r.base || need > r.cap) return s;
  auto t0 = std::chrono::steady_clock::now();
  size_t off = 0, waste = 0, used = 0;
  while (true) {  // 칸이 날 때까지 (지금까지 측정으론 기다린 적 없음)
    used = r.used.load(std::memory_order_acquire);
    if (used == 0) r.head = 0;
    if (r.head + need <= r.cap) { off = r.head; waste = 0; }
    else { off = 0; waste = r.cap - r.head; }
    if (used + waste + need <= r.cap) break;
    std::this_thread::sleep_for(std::chrono::microseconds(50));
  }
  r.used.fetch_add(waste + need, std::memory_order_relaxed);
  r.head = off + need;
  if (r.head == r.cap) r.head = 0;
  s.p = r.base + off; s.len = need; s.waste = waste;
  fprintf(stderr, "[timing] rc ring_acquire seg=%d ring_wait_ms=%lld ring_used_mb=%zu\n", i,
          (long long)std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::steady_clock::now() - t0).count(), (used + waste + need) >> 20);
  return s;
}
static void RcRingSubmit(int i, const RcRingSlot& sl, uint64_t job_id, uint64_t wfnum,
                         const std::string& db_path, size_t size, bool ok) {
  RcRingEnt en;
  en.s = sl;
  en.skip = !ok;
  if (ok) {
    auto sj = RcStreamGet(job_id, true);
    char nm[80];
    snprintf(nm, sizeof(nm), "/rcs_%llu_%llu.tmp", (unsigned long long)job_id, (unsigned long long)wfnum);
    en.t.sj = sj; en.t.job_id = job_id; en.t.wfnum = wfnum; en.t.path = db_path + nm;
    en.t.data = sl.p; en.t.size = size;
    std::lock_guard<std::mutex> lk(sj->mu);
    sj->pending++;
    sj->tmp[wfnum] = en.t.path;
  }
  RcRing& r = g_ring[i];
  const uint64_t t = r.qtail.load(std::memory_order_relaxed);
  while (t - r.qhead.load(std::memory_order_acquire) >= kRcRingQ)  // 줄이 꽉 참 (사실상 없음)
    std::this_thread::sleep_for(std::chrono::microseconds(50));
  r.q[t & (kRcRingQ - 1)] = std::move(en);
  r.qtail.store(t + 1, std::memory_order_seq_cst);
  if (r.sleeping.load(std::memory_order_seq_cst)) {  // 쓰기 스레드가 잠들려 할 때만 깨움
    std::lock_guard<std::mutex> lk(r.wmu);
    r.wcv.notify_one();
  }
}
// ===== end v-ring =====
static void RcStreamWait(uint64_t job_id) {
  auto sj = RcStreamGet(job_id, false);
  if (!sj) return;
  std::unique_lock<std::mutex> lk(sj->mu);
  sj->cv.wait(lk, [&] { return sj->pending == 0; });
}
static void RcStreamAbort(uint64_t job_id) {  // 끝까지 기다린 뒤 남은 임시 파일 삭제 + 항목 제거
  auto sj = RcStreamGet(job_id, false);
  if (!sj) return;
  RcStreamWait(job_id);
  {
    std::lock_guard<std::mutex> lk(sj->mu);
    for (auto& kv : sj->tmp) ::remove(kv.second.c_str());
    sj->tmp.clear();
  }
  std::lock_guard<std::mutex> lk(g_rs_mu);
  g_rs_jobs.erase(job_id);
}
Status RcStreamFinalize(uint64_t job_id, const CompactionResult& wres,
                        const CompactionResult& fixed, const std::string& db_path) {
  auto t0 = std::chrono::steady_clock::now();
  const size_t n = fixed.output_files.size();
  auto sj = RcStreamGet(job_id, false);
  if (!sj) return n == 0 ? Status::OK() : Status::Corruption("v-stream: no streamed outputs for job");
  RcStreamWait(job_id);
  Status s;
  std::vector<std::string> renamed;
  {
    std::lock_guard<std::mutex> lk(sj->mu);
    if (sj->failed) s = Status::IOError("v-stream: streamed persist failed");
    for (size_t i = 0; i < n && s.ok(); i++) {
      if (i >= wres.output_files.size()) { s = Status::Corruption("v-stream: result size mismatch"); break; }
      auto it = sj->tmp.find(wres.output_files[i].file_number);
      if (it == sj->tmp.end()) { s = Status::Corruption("v-stream: missing streamed file"); break; }
      char fname[32];
      snprintf(fname, sizeof(fname), "%06llu.sst", (unsigned long long)fixed.output_files[i].file_number);
      const std::string dst = db_path + "/" + fname;
      if (::rename(it->second.c_str(), dst.c_str()) != 0) {
        s = Status::IOError("v-stream: rename failed " + it->second + " -> " + dst + ": " + strerror(errno));
        break;
      }
      renamed.push_back(dst);
      sj->tmp.erase(it);
    }
  }
  if (!s.ok()) for (const auto& p : renamed) ::remove(p.c_str());
  RcStreamAbort(job_id);
  fprintf(stderr, "[timing] rc job=%llu stream_finalize=%lldms files=%zu ok=%d\n",
          (unsigned long long)job_id,
          (long long)std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::steady_clock::now() - t0).count(), n, s.ok() ? 1 : 0);
  return s;
}

void CompactionDispatcher::RetireOutJobLocked(uint64_t job_id) {
  out_retired_[job_id] = true;
  out_retired_q_.push(job_id);
  while (out_retired_q_.size() > 4096) {
    out_retired_.erase(out_retired_q_.front());
    out_retired_q_.pop();
  }
}

void CompactionDispatcher::RegisterOutJob(AsyncJob&& job, bool discard) {

  if (RcStreamMode() == 2) {  // v-stream: 결과보다 먼저 받은 출력(임시 잡 항목)이 있으면 합친다
    const uint64_t jid = job.result.job_id;
    std::shared_ptr<OutJob> fin;
    bool merged = false;
    {
      std::lock_guard<std::mutex> lk(out_jobs_mu_);
      auto it = out_jobs_.find(jid);
      if (it != out_jobs_.end() && it->second->provisional) {
        auto oj = it->second;
        merged = true;
        auto saved = std::move(oj->job.out_bufs);  // 원소 주소는 그대로 (다른 스레드가 복사 중일 수 있음)
        oj->job = std::move(job);
        oj->job.out_bufs = std::move(saved);
        oj->provisional = false;
        oj->discard = discard;
        const auto& ofs = oj->job.result.output_files;
        const size_t n = ofs.size();
        oj->order.assign(n, 0);
        oj->crc_expect.resize(oj->job.out_bufs.size(), 0);
        size_t done_cnt = 0;
        for (size_t i = 0; i < n; i++) {
          auto f = oj->idx_by_fnum.find(ofs[i].file_number);
          size_t a;
          if (f == oj->idx_by_fnum.end()) {
            if (oj->job.out_bufs.size() >= kRcStreamMaxFiles) { oj->failed = true; continue; }
            a = oj->job.out_bufs.size();
            oj->job.out_bufs.emplace_back();
            oj->done.push_back(false);
            oj->t_file.push_back(oj->t_reg);
            oj->crc_actual.push_back(0);
            oj->crc_expect.push_back(0);
            oj->idx_by_fnum[ofs[i].file_number] = a;
          } else {
            a = f->second;
          }
          oj->order[i] = a;
          oj->crc_expect[a] = ofs[i].content_crc32;
          if (oj->done[a]) {
            done_cnt++;
            if (oj->crc_actual[a] != ofs[i].content_crc32) {
              oj->failed = true;
              fprintf(stderr, "[CRC_MISMATCH] job=%llu file=%llu expected_crc=%u actual_crc=%u (v-stream early)\n",
                      (unsigned long long)jid, (unsigned long long)ofs[i].file_number,
                      ofs[i].content_crc32, oj->crc_actual[a]);
            }
          }
        }
        if (oj->idx_by_fnum.size() != n) {
          oj->failed = true;
          fprintf(stderr, "[rc] v-stream: job=%llu got %zu files but result lists %zu -- fail\n",
                  (unsigned long long)jid, oj->idx_by_fnum.size(), n);
        }
        oj->files_left = n - done_cnt;
        if (oj->files_left == 0 && oj->busy == 0) {
          out_jobs_.erase(it);
          RetireOutJobLocked(jid);
          fin = oj;
        }
      }
    }
    if (merged) {
      if (fin) CompleteOutJob(jid, fin);
      return;
    }
  }
  auto oj = std::make_shared<OutJob>();
  oj->job     = std::move(job);
  oj->discard = discard;
  oj->t_reg   = oj->t_last = std::chrono::steady_clock::now();
  const size_t n = oj->job.result.output_files.size();
  oj->job.out_bufs.resize(n);
  oj->done.assign(n, false);
  oj->t_file.assign(n, oj->t_reg);
  for (size_t i = 0; i < n; i++) {
    const auto& of = oj->job.result.output_files[i];
    oj->idx_by_fnum[of.file_number] = i;
    if (!discard && !RcRingOn()) oj->job.out_bufs[i].resize(of.file_size);  // v-ring: 링이면 미리 할당 안 함
  }
  oj->files_left = n;
  const uint64_t job_id = oj->job.result.job_id;
  if (n == 0) {
    CompleteOutJob(job_id, oj);
    return;
  }
  {
    std::unique_lock<std::mutex> lk(out_jobs_mu_);
    // v-stream-racefix: 위 합치기 검사와 여기 사이에 출력 파일이 먼저 도착해 임시 잡 항목이 생겼으면
    // 덮어쓰지 않는다 (덮어쓰면 그 파일을 잃고 files_left=1로 워치독까지 멈춤). 합치기 경로로 다시 등록.
    if (RcStreamMode() == 2) {
      auto pit = out_jobs_.find(job_id);
      if (pit != out_jobs_.end() && pit->second->provisional) {
        lk.unlock();
        fprintf(stderr, "[rc] v-stream-racefix: job=%llu provisional appeared during register -> merge\n",
                (unsigned long long)job_id);
        RegisterOutJob(std::move(oj->job), discard);
        return;
      }
    }
    out_jobs_[job_id] = oj;
  }
}

// out_jobs_에서 빠진(또는 등록된 적 없는) 잡을 설치 큐로 넘긴다. 락 밖에서 호출.
void CompactionDispatcher::CompleteOutJob(uint64_t job_id, std::shared_ptr<OutJob> oj) {
  auto now = std::chrono::steady_clock::now();
  auto ms = [](std::chrono::steady_clock::duration d) {
    return (long long)std::chrono::duration_cast<std::chrono::milliseconds>(d).count();
  };
  fprintf(stderr, "[timing] rc job=%llu output_queue_wait=%lldms\n",
          (unsigned long long)job_id, oj->seen ? ms(oj->t_first - oj->t_reg) : 0LL);
  fprintf(stderr, "[timing] rc job=%llu step3_recv_sst=%lldms worker_compact=%lldms\n",
          (unsigned long long)job_id, ms(now - (oj->seen ? oj->t_first : oj->t_reg)),
          (long long)(oj->job.result.elapsed_us / 1000));
  if (oj->discard) {
    fprintf(stderr, "[rc] OutputRecvLoopAny: drained outputs of abandoned job=%llu failed=%d\n",
            (unsigned long long)job_id, oj->failed ? 1 : 0);
    if (RcStreamMode()) RcStreamAbort(job_id);  // v-stream
    return;
  }
  if (!oj->order.empty()) {  // v-stream: 도착 순서 -> 결과 순서
    std::vector<std::vector<char>> r(oj->order.size());
    for (size_t i = 0; i < oj->order.size(); i++) r[i] = std::move(oj->job.out_bufs[oj->order[i]]);
    oj->job.out_bufs = std::move(r);
  }
  Status st = oj->failed ? Status::IOError("output receive failed (v-outq)") : Status::OK();
  if (oj->failed) reservation_.Release(oj->job.fnums);
  {
    std::lock_guard<std::mutex> lk(finish_mu_);
    finish_queue_.push_back({st, std::move(oj->job)});
    fprintf(stderr, "[timing] rc job=%llu finish_queue_depth=%zu\n",
            (unsigned long long)job_id, finish_queue_.size());
  }
  finish_cv_.notify_one();
}

void CompactionDispatcher::OutputRecvLoopAny(int seg_idx) {
  { char rc_tn[16]; snprintf(rc_tn, sizeof(rc_tn), "rc_outA%d", seg_idx); pthread_setname_np(pthread_self(), rc_tn); }  // v-thname
  int idle = 0;
  uint64_t wait_job = 0;
  std::chrono::steady_clock::time_point wait_since;
  auto backoff = [&idle]() {
    if (++idle < RcIdleYield()) std::this_thread::yield();  // v-spinwait
    else std::this_thread::sleep_for(std::chrono::microseconds(RcIdleSleepUs()));
  };

  while (!stop_.load(std::memory_order_acquire)) {
    uint64_t hf = 0, hoff = 0, hbytes = 0, hjob = 0;
    bool hlast = false;
    if (!channel_.OutputTryPeekChunk(seg_idx, &hf, &hoff, &hbytes, &hlast, &hjob)) {
      backoff();
      continue;
    }

    std::shared_ptr<OutJob> oj;
    size_t idx = 0;
    bool retired = false, bad_file = false;
    {
      std::lock_guard<std::mutex> lk(out_jobs_mu_);
      auto it = out_jobs_.find(hjob);
      if (it == out_jobs_.end()) {
        retired = out_retired_.count(hjob) > 0;
        if (!retired && hoff == 0 && RcStreamMode() == 2) {  // v-stream: 결과 전에 온 출력 -> 임시 잡 항목
          auto p = std::make_shared<OutJob>();
          p->provisional = true;
          p->job.spec.job_id = hjob;
          p->job.result.job_id = hjob;
          p->job.out_bufs.reserve(kRcStreamMaxFiles);  // 재할당 금지 (다른 스레드가 원소를 참조)
          p->t_reg = p->t_last = std::chrono::steady_clock::now();
          p->files_left = static_cast<size_t>(1) << 40;
          out_jobs_[hjob] = p;
          it = out_jobs_.find(hjob);
        }
      }
      if (it != out_jobs_.end()) {
        oj = it->second;
        auto fit = oj->idx_by_fnum.find(hf);
        if (fit == oj->idx_by_fnum.end() && oj->provisional && hoff == 0 &&
            oj->job.out_bufs.size() < kRcStreamMaxFiles) {  // v-stream: 임시 잡의 새 출력 파일
          const size_t a = oj->job.out_bufs.size();
          oj->job.out_bufs.emplace_back();
          oj->done.push_back(false);
          oj->t_file.push_back(std::chrono::steady_clock::now());
          oj->crc_actual.push_back(0);
          fit = oj->idx_by_fnum.emplace(hf, a).first;
        }
        if (fit == oj->idx_by_fnum.end() || oj->done[fit->second]) {
          bad_file = true;      // 살아 있는 잡인데 모르는/이미 받은 파일 -> 잡 실패
          oj->failed = true;
        } else {
          idx = fit->second;
          oj->busy++;
          auto now = std::chrono::steady_clock::now();
          if (!oj->seen) { oj->seen = true; oj->t_first = now; }
          if (hoff == 0) oj->t_file[idx] = now;
        }
      }
    }

    if (!oj && !retired) {
      // 결과가 아직 등록 전: 소비하지 않고 둔다 (등록되면 다음 확인에서 받는다).
      // 워치독과 같은 한도 동안 등록이 안 되면 고아 청크로 보고 비운다 (세그먼트 막힘 방지).
      auto now = std::chrono::steady_clock::now();
      if (wait_job != hjob) {
        wait_job = hjob;
        wait_since = now;
      } else if (now - wait_since > std::chrono::milliseconds(kJobResultTimeoutMs)) {
        fprintf(stderr,
                "[OUTPUT_ORPHAN] seg=%d hdr_job=%llu hdr_file=%llu -- no result "
                "registered within %dms, drained\n",
                seg_idx, (unsigned long long)hjob, (unsigned long long)hf,
                kJobResultTimeoutMs);
        channel_.OutputConsumeChunk(seg_idx, nullptr, 0);
        wait_job = 0;
        continue;
      }
      backoff();
      continue;
    }
    idle = 0;
    wait_job = 0;

    if (!oj || bad_file) {
      fprintf(stderr,
              "[OUTPUT_STALE] seg=%d hdr_job=%llu hdr_file=%llu off=%llu bytes=%llu "
              "(%s) -- drained\n",
              seg_idx, (unsigned long long)hjob, (unsigned long long)hf,
              (unsigned long long)hoff, (unsigned long long)hbytes,
              bad_file ? "unknown file of live job" : "finished job");
      channel_.OutputConsumeChunk(seg_idx, nullptr, 0);
      continue;
    }

    bool crc_ok = true;
    uint32_t s_crc = 0;
    bool s_crc_pending = false;  // v-stream
    RcRingSlot rs_slot;  // v-ring
    if (oj->discard) {
      channel_.OutputConsumeChunk(seg_idx, nullptr, 0);
    } else if (RcRingOn() && RcStreamMode() && hoff == 0 && hlast &&
               (rs_slot = RcRingAcquire(seg_idx, static_cast<size_t>(hbytes))).p != nullptr) {  // v-ring
      channel_.OutputConsumeChunk(seg_idx, rs_slot.p, hbytes);
      const size_t rs_n = static_cast<size_t>(hbytes);
      uint32_t actual_crc = NtbDiagCrc32(rs_slot.p, rs_n);
      if (RcStreamMode() == 2) { s_crc = actual_crc; s_crc_pending = true; }
      else if (actual_crc != oj->job.result.output_files[idx].content_crc32) {
        crc_ok = false;
        fprintf(stderr, "[CRC_MISMATCH] job=%llu file=%llu seg=%d (ring) actual_crc=%u size=%zu\n",
                (unsigned long long)hjob, (unsigned long long)hf, seg_idx, actual_crc, rs_n);
      }
      fprintf(stderr,
              "[timing] rc job=%llu seg=%d merge_file=%llu size=%llu idx=%zu "
              "step_file_recv=%lldms ok=%d\n",
              (unsigned long long)hjob, seg_idx, (unsigned long long)hf,
              (unsigned long long)rs_n, idx,
              (long long)std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::steady_clock::now() - oj->t_file[idx]).count(),
              crc_ok ? 1 : 0);
      RcRingSubmit(seg_idx, rs_slot, hjob, hf, opts_.db_path, rs_n, crc_ok);
    } else {
      std::vector<char>& buf = oj->job.out_bufs[idx];  // 이 파일은 이 세그먼트 하나로만 온다
      if (hoff + hbytes > buf.size()) buf.resize(hoff + hbytes);
      channel_.OutputConsumeChunk(seg_idx, buf.data() + hoff, hbytes);
      if (hlast) {
        buf.resize(hoff + hbytes);
        uint32_t actual_crc = NtbDiagCrc32(buf.data(), buf.size());
        if (RcStreamMode() == 2) { s_crc = actual_crc; s_crc_pending = true; }  // v-stream: 대조는 잠금 안에서
        else if (actual_crc != oj->job.result.output_files[idx].content_crc32) {
          crc_ok = false;
          fprintf(stderr,
                  "[CRC_MISMATCH] job=%llu file=%llu seg=%d expected_crc=%u "
                  "actual_crc=%u size=%zu\n",
                  (unsigned long long)hjob, (unsigned long long)hf, seg_idx,
                  oj->job.result.output_files[idx].content_crc32, actual_crc, buf.size());
        }
        fprintf(stderr,
                "[timing] rc job=%llu seg=%d merge_file=%llu size=%llu idx=%zu "
                "step_file_recv=%lldms ok=%d\n",
                (unsigned long long)hjob, seg_idx, (unsigned long long)hf,
                (unsigned long long)buf.size(), idx,
                (long long)std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - oj->t_file[idx]).count(),
                crc_ok ? 1 : 0);
        if (RcStreamMode() && crc_ok) RcStreamSubmit(hjob, hf, opts_.db_path, buf.data(), buf.size());  // v-stream: 받자마자 persist
      }
    }

    std::shared_ptr<OutJob> fin;
    {
      std::lock_guard<std::mutex> lk(out_jobs_mu_);
      oj->busy--;
      oj->t_last = std::chrono::steady_clock::now();
      if (hlast) {
        oj->done[idx] = true;
        if (oj->files_left > 0) oj->files_left--;  // v-stream: 음수 방지
        if (!crc_ok) oj->failed = true;
        if (s_crc_pending) {  // v-stream
          if (oj->provisional) {
            oj->crc_actual[idx] = s_crc;
          } else {
            const uint32_t e = oj->crc_expect.empty() ? oj->job.result.output_files[idx].content_crc32 : oj->crc_expect[idx];
            if (s_crc != e) {
              oj->failed = true;
              fprintf(stderr, "[CRC_MISMATCH] job=%llu file=%llu seg=%d expected_crc=%u actual_crc=%u (v-stream)\n",
                      (unsigned long long)hjob, (unsigned long long)hf, seg_idx, e, s_crc);
            }
          }
        }
      }
      if (oj->files_left == 0 && oj->busy == 0) {
        auto it = out_jobs_.find(hjob);
        if (it != out_jobs_.end() && it->second == oj) {
          out_jobs_.erase(it);
          RetireOutJobLocked(hjob);
          fin = oj;
        }
      }
    }
    if (fin) CompleteOutJob(hjob, fin);
  }
}

// 워커가 죽거나 출력 일부를 못 보낸 잡: 진행이 kJobResultTimeoutMs 동안 없으면 실패 처리.
// (정상 경로의 대기가 아니라 ReapStaleInFlightJobs와 같은 안전장치)
void CompactionDispatcher::ReapStaleOutJobs() {
  auto now = std::chrono::steady_clock::now();
  std::vector<std::pair<uint64_t, std::shared_ptr<OutJob>>> stale;
  {
    std::lock_guard<std::mutex> lk(out_jobs_mu_);
    for (auto it = out_jobs_.begin(); it != out_jobs_.end();) {
      auto& oj = it->second;
      if (oj->busy == 0 &&
          now - oj->t_last > std::chrono::milliseconds(kJobResultTimeoutMs)) {
        fprintf(stderr,
                "[rc] output watchdog: job=%llu no output progress for %dms "
                "(files_left=%zu) -- force-failing (v-outq)\n",
                (unsigned long long)it->first, kJobResultTimeoutMs, oj->files_left);
        oj->failed = true;
        RetireOutJobLocked(it->first);
        stale.emplace_back(it->first, oj);
        it = out_jobs_.erase(it);
      } else {
        ++it;
      }
    }
  }
  for (auto& kv : stale) CompleteOutJob(kv.first, kv.second);
}

// ── FinishWorkerLoop ───────────────────────────────────────────────
void CompactionDispatcher::FinishWorkerLoop(int idx) {
  { char rc_tn[16]; snprintf(rc_tn, sizeof(rc_tn), "rc_fin%d", idx); pthread_setname_np(pthread_self(), rc_tn); }  // v-thname
  int core = 25 + idx; if (core >= 52) core += 25; PinCurrentThreadToCore(core);
  if (RcFinOoo()) { g_fin_mu_ptr.store(&finish_mu_); g_fin_cv_ptr.store(&finish_cv_); }  // v-finooo
  while (true) {
    FinishJob fjob;
    size_t fin_skip = 0;  // v-finooo: 앞에서 건너뛴 job 수
    long long rc_idle_ms = 0; size_t rc_qlen = 0;  // v-finidle
    {
      std::unique_lock<std::mutex> lk(finish_mu_);
      if (RcFinOoo()) {  // v-finooo: 앞에서부터 보고 스트림 쓰기가 끝난 첫 job을 꺼냄. 없으면 기다림
        size_t pick = SIZE_MAX;
        auto rc_w0 = std::chrono::steady_clock::now();  // v-finidle
        finish_cv_.wait(lk, [&] {
          pick = SIZE_MAX;
          for (size_t i = 0; i < finish_queue_.size(); i++) {
            const FinishJob& cj = finish_queue_[i];
            if (!cj.status.ok() || RcStreamDone(cj.job.spec.job_id)) { pick = i; break; }
          }
          return pick != SIZE_MAX || stop_finish_;
        });
        rc_idle_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - rc_w0).count();  // v-finidle
        if (finish_queue_.empty()) break;      // stop_finish_ && 빈 큐
        if (pick == SIZE_MAX) pick = 0;        // 종료 중: 순서대로 비움
        fin_skip = pick;
        fjob = std::move(finish_queue_[pick]);
        finish_queue_.erase(finish_queue_.begin() + pick);
        rc_qlen = finish_queue_.size();  // v-finidle
      } else {
        auto rc_w0 = std::chrono::steady_clock::now();  // v-finidle
        finish_cv_.wait(
            lk, [this] { return stop_finish_ || !finish_queue_.empty(); });
        rc_idle_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - rc_w0).count();  // v-finidle
        if (stop_finish_ && finish_queue_.empty()) break;
        fjob = std::move(finish_queue_.front());
        finish_queue_.pop_front();
        rc_qlen = finish_queue_.size();  // v-finidle
      }
    }

    AsyncJob& job = fjob.job;
    const uint64_t rs_jid = job.spec.job_id;  // v-stream
    fprintf(stderr, "[timing] rc job=%llu fin_idle_ms=%lld fin_qlen=%zu\n", (unsigned long long)rs_jid, rc_idle_ms, rc_qlen);  // v-finidle
    const auto rs_t_deq = std::chrono::steady_clock::now();  // v-fsplit
    if (RcStreamMode()) RcStreamWait(rs_jid);  // v-stream: persist가 끝나야 버퍼를 넘길 수 있음
    fprintf(stderr, "[timing] rc job=%llu finish_deq_ms=%lld stream_block_ms=%lld\n",  // v-fsplit: 큐 대기 / 스트림 쓰기 대기 분리
            (unsigned long long)rs_jid,
            (long long)std::chrono::duration_cast<std::chrono::milliseconds>(rs_t_deq - fjob.enqueue_time).count(),
            (long long)std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - rs_t_deq).count());
    fprintf(stderr, "[timing] rc job=%llu finish_queue_wait=%lldms\n", (unsigned long long)job.spec.job_id, (long long)std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - fjob.enqueue_time).count());
    if (RcFinOoo()) fprintf(stderr, "[timing] rc job=%llu fin_skip=%zu\n", (unsigned long long)job.spec.job_id, fin_skip);  // v-finooo
    if (!fjob.status.ok()) {
      if (job.on_complete)
        job.on_complete(fjob.status, std::move(job.spec),
                         std::move(job.result), std::move(job.out_bufs));
      if (RcStreamMode()) RcStreamAbort(rs_jid);  // v-stream: 실패 잡의 임시 파일 삭제
      continue;
    }

    if (job.on_complete)
      job.on_complete(Status::OK(), std::move(job.spec),
                       std::move(job.result), std::move(job.out_bufs));
    {  // v-finidle: job 메모리(out_bufs) 해제 시간
      auto rc_d0 = std::chrono::steady_clock::now();
      { std::vector<std::vector<char>> rc_tmp; rc_tmp.swap(job.out_bufs); }
      fprintf(stderr, "[timing] rc job=%llu fin_free_ms=%lld\n", (unsigned long long)rs_jid,
              (long long)std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - rc_d0).count());
    }
  }
}

// ── ExecuteJobSync (미사용, 호환용) ─────────────────────────────────
Status CompactionDispatcher::ExecuteJobSync(AsyncJob& job) {
  (void)job;
  return Status::NotSupported("ExecuteJobSync not implemented");
}

}  // namespace ROCKSDB_NAMESPACE
