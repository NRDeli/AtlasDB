# AtlasDB — Fault‑Tolerant Distributed Key‑Value Store

AtlasDB is a fault‑tolerant distributed key‑value datastore implemented in Java.  
It demonstrates core distributed systems concepts including leader–follower replication, write‑ahead logging (WAL) durability, log‑based state machine replication, crash recovery, and HTTP‑based inter‑node communication.

---

# ⚙️ Features

- Leader–Follower replication model
- Write‑Ahead Log (WAL) durability
- Crash recovery via WAL replay
- Log‑based replication protocol
- HTTP node server with REST endpoints
- Follower write forwarding to leader
- Replication consistency checks
- Integration + unit test suite
- Real multi‑node cluster simulation
- Metrics endpoint for observability

---

# 🏯 Architecture Overview

AtlasDB follows a replicated state machine architecture.

Client → Leader → WAL append → Replication → Followers → Apply to KV Store

Each node consists of:

- AtlasDBEngine (state machine)
- KVStore (in‑memory database)
- WriteAheadLog (durability layer)
- ReplicationManager (log replication)
- NodeServer (HTTP networking)

---

# 🧱 Components

## AtlasDBEngine
Core state machine coordinating storage, WAL, and replication.

Responsibilities:
- Apply PUT/DELETE operations
- Append operations to WAL
- Replicate operations if leader
- Receive replication if follower
- Recover state on startup

---

## KVStore
In‑memory key‑value database.

- HashMap‑based storage
- O(1) reads/writes
- State machine backend

---

## WriteAheadLog (WAL)
Durability layer.

- Append‑only log file
- Stores serialized operations
- Replay on crash recovery
- Ensures write persistence

---

## ReplicationManager
Leader–Follower replication logic.

Leader:
- Tracks log index
- Sends replication packets
- Maintains follower list

Follower:
- Validates log order
- Applies operations
- Rejects out‑of‑sync packets

Consistency model: **Leader‑based linearizable writes**

---

## NodeServer
HTTP server exposing node APIs.

Endpoints:

GET /health  
Returns node role

GET /kv/<key>  
Read key

PUT /kv/<key>  
Write key (leader only)

DELETE /kv/<key>  
Delete key (leader only)

POST /replicate  
Follower replication endpoint

GET /metrics  
Replication state metrics

Followers forward writes to leader automatically.

---

# 🔁 Replication Protocol

Leader‑driven log replication.

Steps:

1. Leader receives write
2. Append to WAL
3. Apply locally
4. Send replication packet
5. Followers validate index
6. Followers append to WAL
7. Followers apply to KV store

Replication packet format:

fromIndex
OPERATION_LINE
OPERATION_LINE
...

This mirrors Raft‑style log shipping (simplified).

---

# 💾 Persistence Model

AtlasDB uses WAL‑based durability.

Guarantees:

- Writes survive crashes
- State reconstructed on restart
- Deterministic replay
- Log‑ordered consistency

Recovery process:

1. Read WAL file
2. Parse operations
3. Apply sequentially
4. Restore KV state

---

# 🦾 Consistency Model

AtlasDB provides:

- Strong consistency for writes
- Leader‑linearizable operations
- Ordered replication
- Single‑leader authority

Followers never accept direct writes.

---

# 📊 Metrics

Endpoint: /metrics

Returns:

role=<leader|follower>
lastApplied=<index>
commitIndex=<index>

Useful for debugging and observability.

---

# 🧪 Testing

## Unit Tests

KVStoreTest
- PUT/GET
- DELETE
- Overwrite
- Missing keys

## Integration Tests

ReplicationIntegrationTest
- Leader→Follower replication
- WAL crash recovery
- Out‑of‑sync rejection

Run tests:

mvn clean test

---

# ▶️ Running a Cluster

## Build

mvn clean package

---

## Start Leader

java -jar target/replicated-datastore-1.0-SNAPSHOT.jar leader 8080 leader.wal http://localhost:8081 http://localhost:8082

---

## Start Followers

java -jar target/replicated-datastore-1.0-SNAPSHOT.jar follower 8081 f1.wal

java -jar target/replicated-datastore-1.0-SNAPSHOT.jar follower 8082 f2.wal

---

# 🔬 Manual Testing

Write to leader:

curl -X PUT localhost:8080/kv/x -d 100

Read from follower:

curl localhost:8081/kv/x

Check metrics:

curl localhost:8081/metrics

---

# 📚 Distributed Systems Concepts Demonstrated

- Replicated state machines
- Leader‑based replication
- Write‑ahead logging
- Crash recovery
- Log consistency checks
- Client request forwarding
- HTTP inter‑node RPC
- Cluster simulation testing

---

# 🪛 Future Improvements

- Real leader election (Raft/ZooKeeper)
- Heartbeats + failure detection
- Snapshotting + log compaction
- Dynamic membership
- Async replication
- Persistent storage engine
- gRPC transport
- Kubernetes deployment

---

# 🎯 Resume Value

AtlasDB demonstrates production‑grade distributed systems skills:

- Distributed architecture design
- Fault‑tolerant replication
- Persistence + recovery
- Networking + protocols
- Concurrency + state machines
- Testing distributed behavior

---

# 📄 License

Educational project for distributed systems learning and portfolio demonstration.
