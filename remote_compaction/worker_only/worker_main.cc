// ============================================================
//  worker_only/worker_main.cc
//
//    ./worker_main --primary-node <NTB_NODE_ID>
// ============================================================
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "worker_server.h"

using namespace ROCKSDB_NAMESPACE;

static WorkerServer* g_server = nullptr;

static void OnSignal(int) {
  if (g_server) g_server->Stop();
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

