#pragma once
// ============================================================
//  worker_only/worker_server.h  (v6 – graceful-shutdown 타임아웃 추가)
//
//  스레드 구조:
//    DispatcherLoop (1개):
//      RecvSpecMsg → fixed_seg = job_id % kNumSegments
//      → spec_queues_[fixed_seg] 에 push
//
//    RecvLoop[kNumSegments] (2개):
//      seg 전담. spec_queues_[seg] pop → StreamRecvSST(fixed_seg=seg)
//      → compact_queue_ 에 push
//
//    CompactionLoop[kNumCompactionThreads] (10개):
//      compact_queue_ pop → HandleJob → SendResultMsg
//      → send_output_mu_[job.fixed_seg] lock
//      → StreamSendOutputSST(fixed_seg=job.fixed_seg)
//
//  v6 수정 (2026-09-16, Ctrl+C/SIGTERM으로 종료 시도할 때 채널이
//  하드웨어적으로 물려있으면 프로세스가 아무 메시지도 없이 무한정
//  멈춰버리는 문제 수정):
//    - 문제: RecvLoop/CompactionLoop 스레드가 channel_.StreamRecvSST /
//      StreamSendOutputSST(= SubmitDma, 물리 DMA 제출) 안에 들어가
//      있는 도중엔 stop_ 플래그가 바뀌어도 그 블로킹 콜이 리턴하기
//      전엔 절대 빠져나오지 못함. 그 상태에서 채널이 (지금까지 봐온
//      channel-2 하드웨어 행처럼) 커널 안에서 영원히 안 리턴되면,
//      Run()의 `t.join()`이 그 스레드를 영원히 기다리게 되고 main()이
//      절대 리턴을 못 해 — Ctrl+C를 눌러도 아무 반응도, 아무 로그도
//      없이 그냥 멈춘 것처럼 보임(실제로 관찰된 증상).
//    - 커널 안에 진짜로 갇힌 스레드 자체를 코드로 풀어주는 방법은
//      없음(리부팅 전엔 불가) — 대신 (a) 그런 스레드가 있으면 즉시
//      명확한 로그를 남기고 (b) 그 스레드는 join() 대신 detach()해서
//      기다리지 않고, 나머지 정상 스레드들의 종료는 계속 진행되게
//      만듦. recv_thread_done_[]/compact_thread_done_[]가 각 스레드
//      함수가 "실제로 리턴했는지"를 나타내는 플래그이고,
//      WaitThreadDone()이 이걸 kShutdownJoinTimeoutSec만큼만 기다림.
//    - 별도로: 시그널 핸들러(worker_main.cc의 OnSignal)가 기존엔
//      Stop()을 직접 호출했는데, Stop()이 내부적으로
//      condition_variable::notify_all()을 호출함 — 이건
//      async-signal-safe하지 않은 호출이라 시그널 핸들러 안에서 부르면
//      안 됨(POSIX 시그널 안전성 규칙 위반, 드물게 데드락/미정의
//      동작으로 이어질 수 있음). 새로 추가한 RequestStop()은 lock-free
//      atomic store 하나만 하므로 시그널 핸들러 안에서 호출해도 안전함.
//      대신 각 대기 루프(spec_cv_/compact_cv_)가 notify를 기다리지
//      않고도 stop_을 주기적으로(200ms) 스스로 재확인하도록
//      RecvLoop/CompactionLoop 쪽(worker_server.cc)도 같이 바꿨음 —
//      그래서 시그널 경로가 notify를 안 해줘도 정상 동작함.
// ============================================================

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <map>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "../shared/compaction_spec.h"
#include "../shared/ntb_channel.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "table/internal_iterator.h"
#include "table/table_reader.h"

namespace ROCKSDB_NAMESPACE {

struct WorkerOptions {
  uint32_t primary_node_id = 8;
  Env*     env             = nullptr;
};

class WorkerServer {
 public:
  explicit WorkerServer(const WorkerOptions& opts);
  ~WorkerServer();

  Status Connect();

  // 일반 스레드 컨텍스트(예: 소멸자)에서 부르는 정지 요청. atomic
  // 플래그를 세팅하고 대기 중인 CV들을 notify까지 해서 좀 더 빠르게
  // 깨움. *시그널 핸들러 안에서는 절대 호출하면 안 됨* — notify_all()이
  // async-signal-safe하지 않음.
  void Stop();

  // 시그널 핸들러 전용. atomic bool store 하나만 함(lock-free이므로
  // signal-safe) — 락도, CV도, I/O도, 메모리 할당도 없음. 각 대기
  // 루프는 notify 없이도 200ms마다 stop_을 스스로 재확인하므로 이거
  // 하나만으로 정지가 시작됨.
  void RequestStop() noexcept;

  void Run();

 private:
  struct WorkerJob {
    CompactionSpec                 spec;
    int                            fixed_seg = 0;
    std::vector<std::vector<char>> sst_bufs;
  };

  // v-multiseg: primary가 한 job의 입력파일들을 여러 세그먼트로 나눠
  // 보내므로, worker도 세그먼트별로 받은 조각을 공유 조립 공간에
  // 모았다가 전부 도착했을 때만 compact_queue_로 넘긴다.
  struct RecvTask {
    uint64_t            job_id = 0;
    std::vector<size_t> file_indices;
  };

  struct JobAssembly {
    CompactionSpec                 spec;
    std::vector<std::vector<char>> sst_bufs;
    std::atomic<int>               remaining_files{0};
    std::atomic<bool>              failed{false};
  };

  // v-byfile: 세그먼트별로 "이 file_number가 오면 이 job의 몇 번째
  // 파일인지, JobAssembly는 어디인지"를 들고 있는 항목. RecvLoop가
  // 헤더로 읽은 실제 file_number로 이걸 찾아 매칭한다(큐 순서 가정 없음).
  struct PendingRecv {
    uint64_t                     job_id     = 0;
    size_t                       file_index = 0;
    std::shared_ptr<JobAssembly> ja;
  };

  void DispatcherLoop();
  void RecvLoop(int seg_idx);
  void CompactionLoop(int thread_id);
  // v-byfile: RecvLoop의 "이 job 다 받았음" 처리와, 입력파일이 0개인
  // job을 DispatcherLoop에서 즉시 끝내는 처리가 공유하는 마무리 로직.
  void FinalizeJobAssembly(uint64_t job_id, std::shared_ptr<JobAssembly> ja);

  const Comparator* FindComparator(const std::string& name);

  Status OpenInputIterators(
      const CompactionSpec&                 spec,
      const Options&                        opts,
      const ReadOptions&                    ro,
      const ImmutableOptions&               iopts,
      const std::vector<std::vector<char>>& sst_bufs,
      std::vector<InternalIterator*>*       iters_out,
      std::vector<TableReader*>*            readers_out);

  Status WriteOutputSSTs(
      const CompactionSpec&           spec,
      const Options&                  opts,
      const ReadOptions&              ro,
      const ImmutableOptions&         iopts,
      InternalIterator*               merged_iter,
      CompactionResult*               result_out,
      std::vector<std::vector<char>>* out_bufs);

  Status HandleJob(
      const CompactionSpec&                 spec,
      const std::vector<std::vector<char>>& sst_bufs,
      CompactionResult*                     result_out,
      std::vector<std::vector<char>>*       out_bufs);

  static constexpr int kNumCompactionThreads = 16;  // v-thr16

  // v6: 종료(shutdown) 시 스레드 하나가 하드웨어에 갇혀서 안 돌아와도
  // 이 시간(초)만 기다리고 넘어감. 실제로 하드웨어에 갇힌 스레드는
  // 이 시간을 늘려도 절대 안 끝나므로(타임아웃 자체가 없는 블로킹
  // 콜이라서), 이 값은 "진짜로 아직 바쁜 스레드"를 잘못 끊어버리지
  // 않을 정도로만 넉넉하게 잡으면 됨.
  static constexpr int kShutdownJoinTimeoutSec = 20;

  // v6: 각 스레드 함수가 실제로 리턴했는지 여부. 스레드 람다가
  // RecvLoop/CompactionLoop 호출이 끝난 직후에 세팅함 — join() 대신
  // 이걸로 먼저 "끝났는지"를 시간제한 두고 확인한 뒤에만 실제 join()을
  // 부른다(끝난 스레드를 join()하는 건 즉시 리턴하므로 안전).
  static bool WaitThreadDone(std::atomic<bool>& done_flag, int timeout_sec);

  WorkerOptions     opts_;
  NtbChannel        channel_;
  std::atomic<bool> stop_{false};

  std::queue<RecvTask>     spec_queues_[kNumSegments];  // v-byfile: 이제 안 씀
  std::mutex               spec_mu_[kNumSegments];       // v-byfile: 이제 안 씀
  std::condition_variable  spec_cv_[kNumSegments];        // v-byfile: 이제 안 씀
  // v-byfile: 세그먼트별 "file_number -> 받을 사람" 매칭 테이블.
  // DispatcherLoop가 ctrl Spec 처리 즉시 등록하고, RecvLoop[seg]가
  // 헤더로 읽은 실제 file_number로 조회한다.
  std::map<uint64_t, PendingRecv> pending_recv_[kNumSegments];
  std::mutex                      pending_mu_[kNumSegments];
  std::mutex               job_seg_mu_;
  std::map<uint64_t, int>  job_seg_map_;  // job_id -> primary가 정한 seg(responsible_seg)

  std::mutex                                       job_assembly_mu_;
  std::map<uint64_t, std::shared_ptr<JobAssembly>>  job_assembly_;  // v-multiseg

  // output 전송 직렬화: 같은 seg를 쓰는 CompactionLoop들이 동시에
  // StreamSendOutputSST를 호출하면 NTB channel이 섞이므로 per-seg lock
  std::mutex               send_output_mu_[kNumSegments];
  // v9: chunk-ack 전송 전용 락. send_output_mu_[seg]는 컴팩션
  // 결과+출력 SST 스트리밍 동안 오래 잡혀있을 수 있는데(큰
  // 파일이면 수십 초), chunk-ack이 같은 락을 기다리다 30초
  // 타임아웃을 넘겨버려서 primary가 job을 포기하고 다음 파일을
  // 조기에 보내는 레이스가 생겼음(kNumSegments=1로 격리해도
  // FILE_MISMATCH 271건 재현되어 확인됨). ack은 이 전용 락으로
  // 분리해서 출력 전송에 절대 안 막히게 한다.
  std::mutex               ack_send_mu_[kNumSegments];

  std::queue<WorkerJob>    compact_queue_;
  std::mutex               compact_mu_;
  std::condition_variable  compact_cv_;

  std::thread              recv_threads_[kNumSegments];
  std::vector<std::thread> compact_pool_;

  // v6: 위 kShutdownJoinTimeoutSec/WaitThreadDone과 짝을 이루는 완료
  // 플래그 배열.
  std::atomic<bool>        recv_thread_done_[kNumSegments]{};
  std::atomic<bool>        compact_thread_done_[kNumCompactionThreads]{};
};

}  // namespace ROCKSDB_NAMESPACE
