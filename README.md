# PCIe-Fabric-Native Remote Compaction for RocksDB

This repository contains the prototype used in our paper (under review).

A primary RocksDB instance offloads compaction jobs to a worker host over a
PCIe non-transparent bridge (NTB). Input SST data and a job-local compaction
specification move between hosts through fabric memory by DMA. The worker
merges the inputs without opening the database (no MANIFEST replay and no
shared file system), and the primary installs the results.

## Branches

| Branch   | Host    | Contents |
|----------|---------|----------|
| `main`   | primary | RocksDB with the remote compaction dispatcher and install path (`db_bench`) |
| `worker` | worker  | Compaction worker (`worker_main`) |

Both branches start from an unmodified snapshot of RocksDB 11.3.0
(facebook/rocksdb@3daabe2db). Compare a branch with its first commit to see
our changes. The original RocksDB README is kept as `README.rocksdb.md`.

## Layout

```
remote_compaction/
  shared/         compaction spec encoding, NTB channel (SISCI), in-memory SST files
  primary_only/   dispatcher and remote compaction job (send, wait, persist, install)
  worker_only/    worker server and entry point
```

Changes to RocksDB itself are mainly under `db/compaction`, `db/db_impl`,
and `db/version_*`.

## Requirements

- Two x86-64 Linux hosts connected through Dolphin PXH830 NTB adapters
  (we used an MXS824 PCIe switch), Ubuntu 24.04.
- Dolphin eXpressWare with the SISCI API, installed under `/opt/DIS` by
  default (override with `-DSISCI_ROOT=...`). It is not included here.
- Enough prefetchable BAR space and preallocated NTB memory for the transfer
  segments; see `remote_compaction/shared/ntb_channel.h`.
- RocksDB build dependencies: gflags, zstd, snappy, liburing.

## Build

```
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DPORTABLE=0 -DFAIL_ON_WARNINGS=OFF \
  -DWITH_GFLAGS=ON -DWITH_ZSTD=ON -DWITH_SNAPPY=ON -DWITH_LIBURING=ON \
  -DWITH_TOOLS=ON -DWITH_BENCHMARK_TOOLS=ON
make -j"$(nproc)" db_bench      # main branch, on the primary
make -j"$(nproc)" worker_main   # worker branch, on the worker
```

## Run

1. On the primary, start `db_bench` with remote compaction enabled
   (see the switches below).
2. On the worker, start the worker after `db_bench` is running:

```
RC_WSUB=8 ./worker_main --primary-node <NTB node id of the primary>
```

## Install-path switches (primary, environment variables)

| Variable | Effect | Default |
|----------|--------|---------|
| `RC_L0_REMOTE_PCT`   | Percentage of L0->L1 compactions offloaded | |
| `RC_FINISH_THREADS`  | Number of install threads (1-54) | 54 |
| `RC_PERSIST_SERIAL`  | `1`: write a job's output SSTs one by one | parallel |
| `RC_SERIAL_MANIFEST` | `1`: single MANIFEST write queue | parallel queues |
| `RC_PERSIST_DIRECT`  | Write output SSTs with `O_DIRECT` | on |
| `RC_PERSIST_DEFER`   | `1`: defer output `fdatasync` to a background thread (measurement only, not crash-safe) | off |

## License

This code is derived from RocksDB and is distributed under the same terms:
GPLv2 (`COPYING`) or Apache 2.0 (`LICENSE.Apache`), at your option.
