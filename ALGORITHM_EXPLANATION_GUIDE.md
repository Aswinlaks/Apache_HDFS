# DISTRIBUTED SYSTEMS CASE STUDY: CODE EXPLANATION GUIDE

When your evaluator asks to see the code for the distributed algorithms, you need to open specific files, point to specific functions, and explain the core concept behind what the code is doing.

Here is your cheat sheet for explaining the **3 core algorithms**.

---

## 🟢 ALGORITHM 1: RAFT Leader Election & Fault Detection
**File to open:** `raft/raft.go`
 
### 1. Show: `resetElectionTimer()` (Around line 105)
**What to highlight:**
Look at this code: `timeout := time.Duration(3000+rand.Intn(3000)) * time.Millisecond`

**How to explain:**
> "To prevent multiple nodes from starting an election at the exact same time (a split vote), we implemented **randomized election timeouts** between 3 to 6 seconds. If a Follower doesn't receive a heartbeat from the Leader within this random window, its timer fires and it calls `startElection()`."

### 2. Show: `startElection()` (Around line 115)
**What to highlight:** 
Point to: `rn.state = Candidate` and `rn.currentTerm++`. Also point to the parallel vote gathering loop.

**How to explain:**
> "In this function, the node changes its state to **Candidate** and increments its **Term** (logical clock). It then sends `RequestVote` RPCs to all other nodes concurrently using Goroutines. We calculate the `majority` (total/2 + 1). If the candidate receives votes from a majority of nodes, it successfully becomes the new **Leader**."

---

## 🟢 ALGORITHM 2: Consensus via RAFT Log Replication
**File to open:** `raft/raft.go` and `leader/leader.go`

### 1. Show: `UploadFile()` in `leader/leader.go` (Around line 125)
**What to highlight:**
Point to: `logEntry := fmt.Sprintf("UPLOAD:%s:chunks=%d", filename, len(chunks))` followed by `s.raftNode.AppendLogEntry(logEntry)`

**How to explain:**
> "When a client uploads a file, the Leader updates its metadata map. But to ensure **Distributed Consensus**, the Leader cannot just keep this in its own memory. It creates a log entry detailing the upload and passes it to the RAFT module to be replicated."

### 2. Show: `AppendLogEntry()` in `raft/raft.go` (Around line 210)
**What to highlight:**
Point to the loop where it calls `rn.sendAppendEntries()` and the check `if successes >= majority`.

**How to explain:**
> "This is where Consensus happens. The Leader sends the new log entry to all Follower nodes using the `AppendEntries` RPC. The Leader waits. **Only when a majority (quorum) of nodes acknowledge receiving the log**, the Leader considers the entry 'committed'. This guarantees Strong Consistency—all active metadata servers agree on the file's state."

---

## 🟢 ALGORITHM 3: Chunk Replication & Fault-Tolerant Retrieval
**File to open:** `leader/leader.go` and `client/client.go`

### 1. Show: `UploadFile()` in `leader/leader.go` (Around line 85)
**What to highlight:**
Point to the chunk slicing logic, and specifically the replication loop:
`node1 := s.datanodes[chunkIndex%numNodes]`
`node2 := s.datanodes[(chunkIndex+1)%numNodes]`
`for _, nodeAddr := range []string{node1, node2} { ... sendChunkToDataNode ... }`

**How to explain:**
> "To handle large files, we partition the data into 1MB chunks. For **Fault Tolerance**, we implemented a **Replication Factor of 2**. For every chunk, we use modular arithmetic (Round-Robin) to select two different DataNodes, and we send the chunk to both of them."

### 2. Show: `fetchChunkWithRetry()` in `client/client.go` (Around line 195)
**What to highlight:**
Point to the `for i, addr := range addresses` loop and how `err` is handled if a DataNode is down.

**How to explain:**
> "This is our **Fault Recovery mechanism**. During a download, the Client is given a list of replica addresses for a chunk. It loops through them. It tries the primary DataNode first. If that node has crashed (connection error), the loop continues, the client prints a warning, and it **automatically attempts to fetch from the replica DataNode**. This ensures the system remains highly available even if storage nodes fail."

---

### 💡 General Tips for Answering Follow-up Questions from the Evaluator:

* **If asked "Why didn't you use ZooKeeper like real HDFS?":**
  "We wanted to implement the consensus algorithm entirely from scratch to deeply understand how it works. We chose RAFT over Paxos because RAFT is explicitly designed to be more understandable while providing the same guarantees."
* **If asked "How does your system handle concurrent uploads of the same file?":**
  "In `leader.go`, we use `sync.RWMutex` (Read-Write Locks) around our metadata map. This ensures **Mutual Exclusion**—if two threads try to modify the file records at the exact same millisecond, they are safely queued, preventing data corruption."
* **If asked to PROVE it works (The Demo):**
  Always go back to the terminal. "I can demonstrate this right now by killing DataNode 1 while the download is running, and you'll see the client automatically switch to the replica." (This is the most powerful thing you can do).
