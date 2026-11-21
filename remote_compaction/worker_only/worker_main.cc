// ============================================================
//  worker_only/worker_main.cc
//  [WORKER (srv3) 전용]
//
//  사용법:
//    ./worker_main --primary-node <NTB_NODE_ID>
//
//  v2 수정 (2026-09-16, worker_server.h/.cc v6/v13과 짝을 이룸):
//    - OnSignal이 기존엔 g_server->Stop()을 직접 불렀는데, Stop()은
//      내부적으로 std::condition_variable::notify_all()을 호출함 —
//      이건 async-signal-safe하지 않은 호출이라 시그널 핸들러 안에서
//      부르면 안 됨(POSIX 시그널 안전성 규칙 위반). 이제
//      g_server->RequestStop()을 부르는데, 이건 lock-free atomic
//      store 하나만 해서 시그널 핸들러 안에서 호출해도 안전함.
//      (worker_server.h/.cc 쪽에서 각 대기 루프가 notify 없이도
//      200ms마다 stop_을 스스로 재확인하도록 같이 바꿔놨음.)
// ============================================================
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "worker_server.h"

using namespace ROCKSDB_NAMESPACE;

static WorkerServer* g_server = nullptr;

// v2: 시그널 핸들러 안에서는 async-signal-safe한 것만 호출해야 함.
// RequestStop()은 atomic bool store 하나뿐이라 안전함 — 여기서
// Stop()(내부에 notify_all() 있음)을 부르면 안 됨.
static void OnSignal(int) {
  if (g_server) g_server->RequestStop();
}

int main(int argc, char** argv) {
  WorkerOptions opts;
  opts.primary_node_id = 0;

  for (int i = 1; i < argc - 1; i++) {
    if (strcmp(argv[i], "--primary-node") == 0)
      opts.primary_node_id = static_cast<uint32_t>(atoi(argv[++i]));
  }

  fprintf(stderr, "[worker] primary node id = %u\n", opts.primary_node_id);

  WorkerServer server(opts);
  g_server = &server;
  std::signal(SIGINT,  OnSignal);
  std::signal(SIGTERM, OnSignal);

  Status s = server.Connect();
  if (!s.ok()) {
    fprintf(stderr, "[worker] connect failed: %s\n", s.ToString().c_str());
    return 1;
  }

  server.Run();
  return 0;
}
