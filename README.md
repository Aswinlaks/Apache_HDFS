# Distributed File System (HDFS-like) — Go + gRPC + RAFT

A simplified Distributed File System inspired by Apache HDFS, built for a university Distributed Systems case study.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                        CLIENT (Laptop 4)                     │
│                  upload / download / list                     │
└──────────────────────┬───────────────────────────────────────┘
                       │ gRPC (MetadataService)
                       ▼
┌──────────────────────────────────────────────────────────────┐
│              LEADER / METADATA SERVER (Laptop 1)             │
│                                                              │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────────┐ │
│  │  Metadata    │  │  File        │  │  RAFT Consensus     │ │
│  │  Map         │  │  Chunking    │  │  Module             │ │
│  │  file→chunks │  │  1MB chunks  │  │  Leader Election    │ │
│  │              │  │  repl. = 2   │  │  Log Replication    │ │
│  └─────────────┘  └──────────────┘  └─────────────────────┘ │
└──────────┬───────────────────────────────────┬───────────────┘
           │ gRPC (DataNodeService)            │
     ┌─────┴──────┐                  ┌─────────┴────┐
     ▼            ▼                  ▼              ▼
┌──────────┐  ┌──────────┐    ┌──────────┐  ┌──────────┐
│ DataNode1│  │ DataNode2│    │ DataNode1│  │ DataNode2│
│ Laptop 2 │  │ Laptop 3 │    │ (replica)│  │ (replica)│
│ :50052   │  │ :50053   │    │          │  │          │
└──────────┘  └──────────┘    └──────────┘  └──────────┘
```

**Key Features:**
- **Chunk Replication (factor=2):** Each chunk is stored on 2 DataNodes
- **Fault Tolerance:** If a DataNode fails during download, the client retries on the replica
- **RAFT Consensus:** Simplified leader election + heartbeat + log replication
- **gRPC Communication:** All inter-node communication uses gRPC over TCP

## Project Structure

```
DS_casestudy/
├── proto/
│   └── dfs.proto           # gRPC service & message definitions
│       → generates proto/dfs/dfs.pb.go & dfs_grpc.pb.go
├── raft/
│   └── raft.go             # Simplified RAFT consensus module
├── leader/
│   └── leader.go           # Leader / Metadata server
├── datanode/
│   └── datanode.go         # DataNode server
├── client/
│   └── client.go           # CLI client
├── go.mod
└── README.md
```

## Prerequisites

- **Go 1.21+** — [https://go.dev/dl/](https://go.dev/dl/)
- **protoc** (Protocol Buffers compiler) — [https://github.com/protocolbuffers/protobuf/releases](https://github.com/protocolbuffers/protobuf/releases)
- **protoc Go plugins:**
  ```bash
  go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
  ```

## Setup Instructions

### 1. Initialize and download dependencies
```bash
go mod tidy
```

### 2. Generate gRPC code from proto file
```bash
protoc --go_out=. --go-grpc_out=. proto/dfs.proto
```

### 3. Build the project
```bash
go build ./...
```

## Running Locally (Single Machine)

Open **4 terminals:**

**Terminal 1 — DataNode 1:**
```bash
go run datanode/datanode.go --port=50052
```

**Terminal 2 — DataNode 2:**
```bash
go run datanode/datanode.go --port=50053
```

**Terminal 3 — Leader:**
```bash
go run leader/leader.go --port=50051 --datanodes=localhost:50052,localhost:50053
```

**Terminal 4 — Client:**
```bash
# Upload a file
go run client/client.go --leader=localhost:50051 upload sample.txt

# List files
go run client/client.go --leader=localhost:50051 list

# Download a file
go run client/client.go --leader=localhost:50051 download sample.txt
```

## Running Across Multiple Laptops

| Laptop | Role       | Command |
|--------|------------|---------|
| 1      | Leader     | `go run leader/leader.go --port=50051 --datanodes=192.168.1.11:50052,192.168.1.12:50053` |
| 2      | DataNode 1 | `go run datanode/datanode.go --port=50052` |
| 3      | DataNode 2 | `go run datanode/datanode.go --port=50053` |
| 4      | Client     | `go run client/client.go --leader=192.168.1.10:50051 upload sample.txt` |

> Replace `192.168.1.x` with actual IP addresses. Ensure ports are open in Windows Firewall.

## RAFT Multi-Leader Setup (Optional)

To demonstrate RAFT with 3 leader instances:

```bash
# Leader 1
go run leader/leader.go --port=50051 --id=leader1 --peers=localhost:50061,localhost:50071 --datanodes=localhost:50052,localhost:50053

# Leader 2
go run leader/leader.go --port=50061 --id=leader2 --peers=localhost:50051,localhost:50071 --datanodes=localhost:50052,localhost:50053

# Leader 3
go run leader/leader.go --port=50071 --id=leader3 --peers=localhost:50051,localhost:50061 --datanodes=localhost:50052,localhost:50053
```

## Demonstrating Fault Tolerance

1. Upload a file with both DataNodes running
2. Stop DataNode 1 (Ctrl+C)
3. Download the file — the client will show:
   ```
   Chunk retrieval failed from localhost:50052
   Trying replica localhost:50053...
   Successfully retrieved from replica!
   ```

## How It Works

### Upload Flow
1. Client sends file content to Leader
2. Leader splits file into 1 MB chunks
3. Each chunk is sent to **2 DataNodes** (replication factor = 2)
4. Leader stores metadata: `filename → [{chunkId, [node1, node2]}]`
5. Metadata operation is replicated via RAFT log

### Download Flow
1. Client requests file from Leader
2. Leader returns chunk locations (which DataNodes have each chunk)
3. Client fetches each chunk directly from DataNodes
4. If a DataNode is down, client **retries on replica** automatically
5. Client reassembles chunks into original file

### RAFT Consensus
1. On startup, nodes begin as **Followers**
2. If no heartbeat received, a Follower becomes a **Candidate** and starts election
3. Candidate requests votes; **majority wins**
4. Leader sends periodic **heartbeats** to prevent new elections
5. Metadata operations are replicated as **log entries** to all peers
