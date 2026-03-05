// =============================================================================
// raft.go — Simplified RAFT Consensus Module
//
// Implements a minimal RAFT protocol for educational purposes:
//   - Leader election via RequestVote RPC
//   - Heartbeat via AppendEntries RPC
//   - Log replication for metadata consistency
//   - Three states: Follower, Candidate, Leader
// =============================================================================

package raft

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	pb "dfs-project/proto/dfs"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ======================== RAFT State Definitions ========================

// NodeState represents the current role of a RAFT node
type NodeState int

const (
	Follower  NodeState = iota // Default state — waits for leader heartbeats
	Candidate                  // Requesting votes to become leader
	Leader                     // Actively sending heartbeats and replicating logs
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "FOLLOWER"
	case Candidate:
		return "CANDIDATE"
	case Leader:
		return "LEADER"
	default:
		return "UNKNOWN"
	}
}

// ======================== RAFT Node ========================

// RaftNode implements a simplified RAFT consensus node.
// It embeds the gRPC RaftServiceServer so the leader's gRPC server
// can register it alongside the MetadataService.
type RaftNode struct {
	pb.UnimplementedRaftServiceServer

	mu sync.Mutex

	// --- Node identity ---
	id    string   // Unique node ID (e.g., "leader1")
	peers []string // Addresses of other RAFT nodes

	// --- Persistent state (simplified — stored in memory) ---
	currentTerm int32          // Latest term this node has seen
	votedFor    string         // CandidateId that received vote in current term
	log         []*pb.LogEntry // Replicated log entries

	// --- Volatile state ---
	commitIndex int32     // Index of highest log entry known to be committed
	state       NodeState // Current role: Follower, Candidate, or Leader

	// --- Election timer ---
	electionTimer *time.Timer
}

// NewRaftNode creates a new RAFT node with the given ID and peer addresses
func NewRaftNode(id string, peers []string) *RaftNode {
	return &RaftNode{
		id:          id,
		peers:       peers,
		currentTerm: 0,
		votedFor:    "",
		log:         make([]*pb.LogEntry, 0),
		commitIndex: 0,
		state:       Follower,
	}
}

// Start begins the RAFT protocol.
// If no peers are configured, the node auto-elects itself (single-node cluster).
func (rn *RaftNode) Start() {
	if len(rn.peers) == 0 {
		// Single-node mode: become leader immediately
		rn.mu.Lock()
		rn.state = Leader
		rn.currentTerm = 1
		rn.mu.Unlock()
		log.Printf("[RAFT %s] No peers configured — auto-elected as LEADER (term 1)", rn.id)
		return
	}

	// Multi-node mode: start election timer and wait
	rn.resetElectionTimer()
	log.Printf("[RAFT %s] Started as FOLLOWER (term %d), waiting for leader...", rn.id, rn.currentTerm)
}

// ======================== Election Timer ========================

// resetElectionTimer resets the election timeout with random jitter.
// If no heartbeat is received before timeout, the node starts an election.
func (rn *RaftNode) resetElectionTimer() {
	if rn.electionTimer != nil {
		rn.electionTimer.Stop()
	}
	// Random timeout between 3-6 seconds (longer than typical RAFT for demo visibility)
	timeout := time.Duration(3000+rand.Intn(3000)) * time.Millisecond
	rn.electionTimer = time.AfterFunc(timeout, func() {
		rn.startElection()
	})
}

// ======================== Leader Election ========================

// startElection transitions to Candidate state and requests votes from all peers.
// If a majority of votes is received, the node becomes the Leader.
func (rn *RaftNode) startElection() {
	rn.mu.Lock()
	rn.state = Candidate
	rn.currentTerm++
	rn.votedFor = rn.id
	term := rn.currentTerm
	lastLogIndex := int32(len(rn.log))
	var lastLogTerm int32
	if len(rn.log) > 0 {
		lastLogTerm = rn.log[len(rn.log)-1].Term
	}
	rn.mu.Unlock()

	log.Printf("[RAFT %s] Starting election for term %d", rn.id, term)

	// Count votes (we vote for ourselves)
	votes := 1
	total := len(rn.peers) + 1
	majority := total/2 + 1

	var votesMu sync.Mutex
	var wg sync.WaitGroup

	// Send RequestVote RPC to every peer in parallel
	for _, peer := range rn.peers {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			granted := rn.sendRequestVote(addr, term, lastLogIndex, lastLogTerm)
			if granted {
				votesMu.Lock()
				votes++
				votesMu.Unlock()
			}
		}(peer)
	}

	// Wait for all responses (with overall timeout)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
	}

	// Evaluate election result
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.state != Candidate || rn.currentTerm != term {
		return // State changed during election
	}

	votesMu.Lock()
	totalVotes := votes
	votesMu.Unlock()

	if totalVotes >= majority {
		rn.state = Leader
		log.Printf("[RAFT %s] Won election for term %d (%d/%d votes)", rn.id, term, totalVotes, total)
		go rn.sendHeartbeats()
	} else {
		log.Printf("[RAFT %s] Lost election for term %d (%d/%d votes)", rn.id, term, totalVotes, total)
		rn.state = Follower
		rn.resetElectionTimer()
	}
}

// sendRequestVote sends a RequestVote RPC to a single peer and returns whether the vote was granted
func (rn *RaftNode) sendRequestVote(addr string, term, lastLogIndex, lastLogTerm int32) bool {
	conn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Printf("[RAFT %s] Cannot reach peer %s for vote: %v", rn.id, addr, err)
		return false
	}
	defer conn.Close()

	client := pb.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	reply, err := client.RequestVote(ctx, &pb.VoteRequest{
		Term:         term,
		CandidateId:  rn.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	})
	if err != nil {
		log.Printf("[RAFT %s] RequestVote RPC to %s failed: %v", rn.id, addr, err)
		return false
	}

	// If the peer has a higher term, step down
	rn.mu.Lock()
	if reply.Term > rn.currentTerm {
		rn.currentTerm = reply.Term
		rn.state = Follower
		rn.votedFor = ""
	}
	rn.mu.Unlock()

	return reply.VoteGranted
}

// ======================== Heartbeat (Leader) ========================

// sendHeartbeats periodically sends AppendEntries RPCs to all peers.
// This serves as a heartbeat to prevent followers from starting elections.
func (rn *RaftNode) sendHeartbeats() {
	ticker := time.NewTicker(1 * time.Second) // 1s interval for demo visibility
	defer ticker.Stop()

	for range ticker.C {
		rn.mu.Lock()
		if rn.state != Leader {
			rn.mu.Unlock()
			return
		}
		term := rn.currentTerm
		commit := rn.commitIndex
		rn.mu.Unlock()

		// Send heartbeat to every peer
		for _, peer := range rn.peers {
			go func(addr string) {
				rn.sendAppendEntries(addr, term, nil, commit)
			}(peer)
		}

		log.Printf("[RAFT %s] Heartbeat sent (term %d)", rn.id, term)
	}
}

// ======================== Log Replication ========================

// AppendLogEntry adds a new command to the leader's log and replicates it.
// Returns true if the entry was committed (acknowledged by a majority).
func (rn *RaftNode) AppendLogEntry(command string) bool {
	rn.mu.Lock()
	if rn.state != Leader {
		rn.mu.Unlock()
		log.Printf("[RAFT %s] Cannot append log: not the leader", rn.id)
		return false
	}

	entry := &pb.LogEntry{Term: rn.currentTerm, Command: command}
	rn.log = append(rn.log, entry)
	term := rn.currentTerm
	rn.mu.Unlock()

	log.Printf("[RAFT %s] Replicating log entry: %s", rn.id, command)

	// Single-node: commit immediately
	if len(rn.peers) == 0 {
		rn.mu.Lock()
		rn.commitIndex++
		rn.mu.Unlock()
		return true
	}

	// Multi-node: replicate to peers and wait for majority
	successes := 1 // Count self
	total := len(rn.peers) + 1
	majority := total/2 + 1

	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, peer := range rn.peers {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			ok := rn.sendAppendEntries(addr, term, []*pb.LogEntry{entry}, rn.commitIndex)
			if ok {
				mu.Lock()
				successes++
				mu.Unlock()
			}
		}(peer)
	}

	wg.Wait()

	if successes >= majority {
		rn.mu.Lock()
		rn.commitIndex++
		rn.mu.Unlock()
		log.Printf("[RAFT %s] Log committed (%d/%d acks)", rn.id, successes, total)
		return true
	}

	log.Printf("[RAFT %s] Log NOT committed (%d/%d acks)", rn.id, successes, total)
	return false
}

// sendAppendEntries sends an AppendEntries RPC to a single peer.
// Returns true if the peer accepted the entries.
func (rn *RaftNode) sendAppendEntries(addr string, term int32, entries []*pb.LogEntry, commit int32) bool {
	conn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return false
	}
	defer conn.Close()

	client := pb.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	reply, err := client.AppendEntries(ctx, &pb.AppendRequest{
		Term:         term,
		LeaderId:     rn.id,
		Entries:      entries,
		LeaderCommit: commit,
	})
	if err != nil {
		return false
	}

	// If peer has higher term, step down
	rn.mu.Lock()
	if reply.Term > rn.currentTerm {
		rn.currentTerm = reply.Term
		rn.state = Follower
		rn.votedFor = ""
		rn.resetElectionTimer()
	}
	rn.mu.Unlock()

	return reply.Success
}

// ======================== gRPC Service Handlers ========================

// RequestVote handles incoming vote requests from candidates
func (rn *RaftNode) RequestVote(ctx context.Context, req *pb.VoteRequest) (*pb.VoteReply, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	log.Printf("[RAFT %s] Received RequestVote from %s (term %d)", rn.id, req.CandidateId, req.Term)

	reply := &pb.VoteReply{Term: rn.currentTerm, VoteGranted: false}

	// Reject if candidate's term is older
	if req.Term < rn.currentTerm {
		return reply, nil
	}

	// If candidate has a newer term, update our term and become follower
	if req.Term > rn.currentTerm {
		rn.currentTerm = req.Term
		rn.state = Follower
		rn.votedFor = ""
	}

	// Grant vote if we haven't voted yet (or already voted for this candidate)
	if rn.votedFor == "" || rn.votedFor == req.CandidateId {
		rn.votedFor = req.CandidateId
		reply.VoteGranted = true
		reply.Term = rn.currentTerm
		rn.resetElectionTimer()
		log.Printf("[RAFT %s] Voted for %s in term %d", rn.id, req.CandidateId, req.Term)
	}

	return reply, nil
}

// AppendEntries handles heartbeats and log replication from the leader
func (rn *RaftNode) AppendEntries(ctx context.Context, req *pb.AppendRequest) (*pb.AppendReply, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply := &pb.AppendReply{Term: rn.currentTerm, Success: false}

	// Reject if leader's term is older
	if req.Term < rn.currentTerm {
		return reply, nil
	}

	// Valid heartbeat received — reset election timer
	rn.resetElectionTimer()

	if req.Term > rn.currentTerm {
		rn.currentTerm = req.Term
		rn.votedFor = ""
	}
	rn.state = Follower

	// Append any new log entries
	if len(req.Entries) > 0 {
		rn.log = append(rn.log, req.Entries...)
		log.Printf("[RAFT %s] Replicated %d entries from leader %s", rn.id, len(req.Entries), req.LeaderId)
	}

	// Update commit index
	if req.LeaderCommit > rn.commitIndex {
		rn.commitIndex = req.LeaderCommit
	}

	reply.Success = true
	reply.Term = rn.currentTerm
	return reply, nil
}

// ======================== State Accessors ========================

// IsLeader returns true if this node is currently the RAFT leader
func (rn *RaftNode) IsLeader() bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.state == Leader
}

// GetState returns the current RAFT state
func (rn *RaftNode) GetState() NodeState {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.state
}

// GetTerm returns the current RAFT term
func (rn *RaftNode) GetTerm() int32 {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.currentTerm
}

// GetLogLength returns the number of entries in the RAFT log
func (rn *RaftNode) GetLogLength() int {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return len(rn.log)
}

// Status returns a human-readable status string
func (rn *RaftNode) Status() string {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return fmt.Sprintf("state=%s term=%d log=%d committed=%d",
		rn.state, rn.currentTerm, len(rn.log), rn.commitIndex)
}
