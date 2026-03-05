// =============================================================================
// leader.go — Leader / Metadata Server
//
// Responsibilities:
//   - Receives file upload requests from clients
//   - Splits files into 1MB chunks
//   - Replicates each chunk to 2 DataNodes (fault tolerance)
//   - Maintains metadata: filename → [{chunkId, [datanode1, datanode2]}]
//   - Uses RAFT module for metadata log replication
//   - Returns chunk locations for downloads
// =============================================================================

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	pb "dfs-project/proto/dfs"
	"dfs-project/raft"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// chunkSize defines the maximum size of each file chunk (1 MB)
const chunkSize = 1024 * 1024

// ChunkMeta stores where a chunk is replicated
type ChunkMeta struct {
	ChunkID   string   // Unique identifier for this chunk
	Locations []string // DataNode addresses holding replicas of this chunk
}

// ======================== Metadata Server ========================

// MetadataServer implements the MetadataService gRPC interface.
// It is the central coordinator of the distributed file system.
type MetadataServer struct {
	pb.UnimplementedMetadataServiceServer

	mu        sync.RWMutex
	metadata  map[string][]ChunkMeta // filename → list of chunk metadata
	datanodes []string               // list of available DataNode addresses
	raftNode  *raft.RaftNode         // RAFT consensus module
}

// NewMetadataServer creates a new MetadataServer
func NewMetadataServer(datanodes []string, raftNode *raft.RaftNode) *MetadataServer {
	return &MetadataServer{
		metadata:  make(map[string][]ChunkMeta),
		datanodes: datanodes,
		raftNode:  raftNode,
	}
}

// ======================== UploadFile RPC ========================

// UploadFile receives a file from the client, splits it into chunks,
// and stores each chunk on 2 DataNodes for fault tolerance.
func (s *MetadataServer) UploadFile(ctx context.Context, req *pb.FileData) (*pb.UploadResponse, error) {
	filename := req.Filename
	data := req.Content

	log.Printf("========================================")
	log.Printf("UPLOAD REQUEST: %s (%d bytes)", filename, len(data))
	log.Printf("========================================")

	if len(s.datanodes) < 2 {
		return &pb.UploadResponse{
			Success: false,
			Message: "Need at least 2 DataNodes for replication",
		}, nil
	}

	// Split file into chunks and distribute to DataNodes
	var chunks []ChunkMeta
	chunkIndex := 0
	numNodes := len(s.datanodes)

	for offset := 0; offset < len(data); offset += chunkSize {
		end := offset + chunkSize
		if end > len(data) {
			end = len(data)
		}

		chunkData := data[offset:end]
		chunkID := fmt.Sprintf("%s_chunk_%d", filename, chunkIndex)

		// Select 2 DataNodes using round-robin for replication
		node1 := s.datanodes[chunkIndex%numNodes]
		node2 := s.datanodes[(chunkIndex+1)%numNodes]

		log.Printf("  Chunk %s (%d bytes) -> [%s, %s]", chunkID, len(chunkData), node1, node2)

		// Send chunk to both DataNodes (replication factor = 2)
		for _, nodeAddr := range []string{node1, node2} {
			err := sendChunkToDataNode(nodeAddr, chunkID, chunkData)
			if err != nil {
				log.Printf("  FAILED to store chunk on %s: %v", nodeAddr, err)
				return &pb.UploadResponse{
					Success: false,
					Message: fmt.Sprintf("Failed to store chunk %s on %s: %v", chunkID, nodeAddr, err),
				}, nil
			}
			log.Printf("  Stored on %s", nodeAddr)
		}

		chunks = append(chunks, ChunkMeta{
			ChunkID:   chunkID,
			Locations: []string{node1, node2},
		})

		chunkIndex++
	}

	// Save metadata in memory
	s.mu.Lock()
	s.metadata[filename] = chunks
	s.mu.Unlock()

	// Replicate metadata operation via RAFT consensus
	logEntry := fmt.Sprintf("UPLOAD:%s:chunks=%d", filename, len(chunks))
	s.raftNode.AppendLogEntry(logEntry)

	log.Printf("File '%s' uploaded successfully: %d chunks, replication factor 2", filename, len(chunks))

	return &pb.UploadResponse{
		Success: true,
		Message: fmt.Sprintf("File '%s' uploaded (%d chunks, replicated to 2 nodes each)", filename, len(chunks)),
	}, nil
}

// ======================== DownloadFile RPC ========================

// DownloadFile returns the chunk locations for a file.
// The client uses these locations to fetch chunks directly from DataNodes.
func (s *MetadataServer) DownloadFile(ctx context.Context, req *pb.FileRequest) (*pb.FileChunks, error) {
	filename := req.Filename

	s.mu.RLock()
	chunks, exists := s.metadata[filename]
	s.mu.RUnlock()

	if !exists {
		log.Printf("DOWNLOAD REQUEST: %s — FILE NOT FOUND", filename)
		return nil, fmt.Errorf("file '%s' not found", filename)
	}

	log.Printf("DOWNLOAD REQUEST: %s (%d chunks)", filename, len(chunks))

	// Build chunk location list for the client
	var locations []*pb.ChunkLocation
	for _, cm := range chunks {
		locations = append(locations, &pb.ChunkLocation{
			ChunkId:           cm.ChunkID,
			DatanodeAddresses: cm.Locations,
		})
	}

	return &pb.FileChunks{Chunks: locations}, nil
}

// ======================== ListFiles RPC ========================

// ListFiles returns all filenames stored in the system
func (s *MetadataServer) ListFiles(ctx context.Context, req *pb.Empty) (*pb.FileList, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var filenames []string
	for name, chunks := range s.metadata {
		filenames = append(filenames, fmt.Sprintf("%s (%d chunks)", name, len(chunks)))
	}

	log.Printf("LIST REQUEST: %d files", len(filenames))
	return &pb.FileList{Filenames: filenames}, nil
}

// ======================== DataNode Communication ========================

// sendChunkToDataNode sends a chunk to a specific DataNode via gRPC
func sendChunkToDataNode(addr, chunkID string, data []byte) error {
	conn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("cannot connect to DataNode %s: %v", addr, err)
	}
	defer conn.Close()

	client := pb.NewDataNodeServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ack, err := client.StoreChunk(ctx, &pb.Chunk{
		ChunkId: chunkID,
		Data:    data,
	})
	if err != nil {
		return err
	}
	if !ack.Success {
		return fmt.Errorf("DataNode rejected chunk: %s", ack.Message)
	}

	return nil
}

// ======================== Main Entry Point ========================

func main() {
	// Command-line flags for configuration
	port := flag.Int("port", 50051, "Leader server port")
	datanodeList := flag.String("datanodes", "10.12.69.102:50052,10.12.68.157:50053",
		"Comma-separated DataNode addresses")
	nodeID := flag.String("id", "leader1", "Unique node ID for RAFT")
	peerList := flag.String("peers", "",
		"Comma-separated RAFT peer addresses (other leader instances)")
	flag.Parse()

	// Parse DataNode addresses
	datanodes := strings.Split(*datanodeList, ",")

	// Parse RAFT peer addresses
	var peers []string
	if *peerList != "" {
		peers = strings.Split(*peerList, ",")
	}

	// Initialize RAFT consensus module
	raftNode := raft.NewRaftNode(*nodeID, peers)

	// Create metadata server
	server := NewMetadataServer(datanodes, raftNode)

	// Start gRPC server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Failed to listen on port %d: %v", *port, err)
	}

	grpcServer := grpc.NewServer()

	// Register both MetadataService and RaftService on the same server
	pb.RegisterMetadataServiceServer(grpcServer, server)
	pb.RegisterRaftServiceServer(grpcServer, raftNode)

	// Start RAFT protocol (election timer / auto-elect)
	raftNode.Start()

	log.Printf("========================================")
	log.Printf("LEADER SERVER started on port %d", *port)
	log.Printf("DataNodes: %v", datanodes)
	log.Printf("RAFT peers: %v", peers)
	log.Printf("RAFT status: %s", raftNode.Status())
	log.Printf("========================================")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
