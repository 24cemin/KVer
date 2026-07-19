# KVer — Distributed, Fault-Tolerant Key-Value Engine

> A strongly consistent distributed key-value store built from scratch in Go, powered by the Raft consensus algorithm.

**Bachelor's Thesis** · Istanbul Arel University · Computer Engineering · June 2026  
**Author:** M. Emin Sucuoğlu · **Advisor:** Assoc. Prof. Pınar Karadayı Ataş

---

## Overview

KVer is an event-driven, in-memory key-value store that implements the Raft consensus algorithm natively in Go — without relying on any external consensus library. The system is designed as a CP (Consistent + Partition-tolerant) distributed data store: it halts write operations during quorum loss rather than serving potentially inconsistent data.

The project demonstrates that standard hardware and Go's native concurrency primitives are sufficient to build a performant, linearizable distributed storage engine.

### Key Performance Results (from thesis benchmarks)

| Metric | Value |
|---|---|
| Single-node throughput (fsync on) | 4,553 req/s |
| 3-node cluster throughput | ~1,450 req/s |
| Leader crash RTO (median) | 189 ms (±55 ms) |
| Follower crash RTO | 0 ms (zero downtime) |
| Linearizability | ✅ Verified via [Porcupine](https://github.com/anishathalye/porcupine) |
| Protobuf vs JSON serialization | 10× faster, 6× less memory |

---

## Architecture

```
Client (HTTP REST / kvctl CLI)
        │
        ▼
┌─────────────────┐
│   API Gateway   │  HTTP/1.1 → gRPC translation
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   KV Handler    │  gRPC server — leader check, forwarding, ReadIndex
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────────┐
│              Raft Consensus Engine           │
│  ┌──────────┐  ┌──────────┐  ┌───────────┐ │
│  │ Election │  │ Replication│  │ Snapshot │ │
│  │ (PreVote)│  │ (batchLoop)│  │ (async)  │ │
│  └──────────┘  └──────────┘  └───────────┘ │
└────────┬────────────────────────────────────┘
         │
         ▼
┌──────────────────────┐     ┌──────────────────┐
│  KV State Machine    │     │  Binary WAL       │
│  (in-memory)         │     │  (CRC32-validated)│
│  String / Hash /     │     │  + Snapshot       │
│  List / Sorted Set   │     │  (Protobuf)       │
└──────────────────────┘     └──────────────────┘
```

### Raft Extensions Implemented

| Extension | Purpose |
|---|---|
| **Pre-Vote Protocol** | Prevents isolated nodes from term-bombing a stable cluster |
| **ReadIndex Protocol** | Linearizable reads from RAM without writing to the log |
| **Fast Backup Optimization** | Skips linear probing during log conflict resolution |
| **Async Snapshotting** | Isolates disk I/O from the heartbeat loop to prevent false elections |
| **Single-Server Membership** | Safe dynamic cluster scaling one node at a time |

---

## Data Types

All data types support **TTL** (Time-To-Live) expiration, computed once at the leader and distributed to all followers as an absolute Unix timestamp to ensure consistency.

| Type | Operations | Complexity |
|---|---|---|
| **String** | `GET`, `SET`, `DEL`, `INCR`, `DECR` | O(1) |
| **Hash** | `HSET`, `HGET`, `HDEL`, `HGETALL`, `HEXISTS` | O(1) |
| **List** | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN` | O(1) push/pop, O(n) range |
| **Sorted Set** | `ZADD`, `ZREM`, `ZSCORE`, `ZRANK`, `ZRANGE`, `ZREVRANGE` | O(log n) via Skip List |

---

## Quick Start

### Prerequisites

- Go 1.24+
- Docker & Docker Compose
- `protoc` (only needed to regenerate proto files)

### 1. Run a 3-Node Cluster (Docker)

```bash
# Start the cluster
docker compose up -d

# Verify leader election
docker compose logs -f 2>&1 | grep "became leader"
```

### 2. Build the CLI

```bash
go build -o kvctl ./cmd/kvctl/
alias kv='./kvctl --nodes localhost:7001,localhost:7002,localhost:7003'
```

### 3. Basic Operations

```bash
# String
kv set hello world
kv get hello           # → world

# With TTL (seconds)
kv set session token123 --ttl 60

# Counter
kv set hits 0
kv incr hits           # → 1

# Hash
kv hset user:1 name alice
kv hget user:1 name    # → alice

# List
kv rpush queue task1 task2 task3
kv lrange queue 0 -1   # → task1, task2, task3
kv lpop queue          # → task1

# Sorted Set
kv zadd leaderboard 100 alice
kv zadd leaderboard 85 bob
kv zrange leaderboard 0 -1   # → bob, alice (ascending)
kv zrevrange leaderboard 0 -1 # → alice, bob (descending)
```

See [`test_commands.md`](test_commands.md) for a complete end-to-end manual test guide covering fault tolerance, leader failover, quorum loss, persistence, and network partition scenarios.

---

## REST API

Each node exposes an HTTP/1.1 REST gateway (default port `8001`):

```bash
# String
curl -X POST localhost:8001/api/v1/string/set \
  -d '{"key":"foo","value":"bar","ttl_ms":5000}'

curl "localhost:8001/api/v1/string/get?key=foo"

# Hash
curl -X POST localhost:8001/api/v1/hash/set \
  -d '{"key":"user:1","field":"name","value":"alice"}'

# List
curl -X POST localhost:8001/api/v1/list/lpush \
  -d '{"key":"queue","values":["task1","task2"]}'

# Cluster
curl -X POST localhost:8001/api/v1/cluster/add-node \
  -d '{"node_id":"node4","address":"10.0.0.4:7004"}'
```

---

## CLI Reference

```
kvctl --nodes <addr1,addr2,...> <command>

String:     set, get, del, incr, decr
Hash:       hset, hget, hdel, hgetall, hexists
List:       lpush, rpush, lpop, rpop, lrange, llen
Sorted Set: zadd, zrem, zscore, zrank, zrange, zrevrange
Cluster:    cluster status, cluster add-node, cluster remove-node
```

---

## Development

```bash
# Run all unit tests
go test ./...

# Run with race detector
go test -race ./...

# Benchmarks
go test -bench=. -benchtime=30s ./tests/benchmark/...

# Regenerate protobuf files
make proto
# or: bash scripts/gen_proto.sh

# Fault injection scripts
bash scripts/simulate_partition.sh   # Network partition simulation
bash scripts/rto_test.sh             # Recovery Time Objective measurement
bash scripts/chaos.sh                # Chaos testing
```

### Project Structure

```
cmd/
  server/       — Raft node entrypoint (flags: --node-id, --addr, --peers, --data-dir)
  kvctl/        — CLI client (Cobra)
internal/
  raft/         — Raft consensus engine (election, replication, snapshot, WAL, membership)
  kvstore/      — In-memory KV state machine (string, hash, list, sortedset, TTL)
  server/       — gRPC handlers, HTTP gateway, leader forwarding
pkg/
  sdk/          — Go client SDK (leader discovery, retries)
proto/
  raft/         — Raft RPC proto definitions
  kv/           — KV service proto definitions
tests/
  integration/  — Multi-node correctness + linearizability tests
  e2e/          — End-to-end cluster tests
  benchmark/    — Throughput and serialization benchmarks
  load/         — Load test driver
scripts/        — Cluster management and chaos scripts
```

---

## Fault Tolerance Guarantees

| Scenario | Behavior |
|---|---|
| Follower crash | Zero downtime, cluster continues serving requests |
| Leader crash | New leader elected in 150–300 ms; writes resume |
| Network partition (minority) | Isolated minority cannot form quorum; majority continues |
| Network partition (majority loss) | Writes halted to prevent split-brain; reads may fail |
| Full cluster restart | State restored from snapshot + WAL replay in ~50 ms |
| WAL corruption | CRC32 checksum mismatch detected; entry rejected |

---

## Design Decisions

| Decision | Rationale |
|---|---|
| **In-memory state machine** | Sub-millisecond reads from RAM; durability delegated to WAL |
| **Binary WAL over JSON** | 10× faster serialization, 6× lower memory per entry |
| **Skip List for Sorted Sets** | O(log n) without global tree rebalancing; shorter lock durations |
| **gRPC for transport** | Connection pooling, backoff, and protobuf encoding built-in |
| **Go channels for consensus** | Event-driven backpressure prevents goroutine leaks under load |
| **ReadIndex over log reads** | Linearizable reads without write amplification |

---

## Known Limitations

- **No TLS or authentication** — designed for isolated VPCs/trusted networks.
- **No MVCC** — snapshots block writes briefly during serialization (< 5ms for 100K keys).
- **Single-server membership changes only** — Joint Consensus is listed as future work.

---

## Tech Stack

| Component | Technology |
|---|---|
| Language | Go 1.24 |
| Consensus | Raft (custom implementation) |
| Transport | gRPC + Protocol Buffers |
| CLI | Cobra |
| Persistence | Binary WAL + Protobuf Snapshots |
| Containerization | Docker + Docker Compose |
| Testing | Go test, Porcupine (linearizability) |

---

## License

This project was developed as a Bachelor's thesis at Istanbul Arel University. Academic use and reference are welcome.
