// =============================================================================
// datanode.go — DataNode Server
//
// Responsibilities:
//   - Stores file chunks on local disk (in storage/ directory)
//   - Receives chunks from the Leader during uploads
//   - Returns chunks to the Client during downloads
//   - Each DataNode runs independently on its own port
// =============================================================================

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"

	pb "dfs-project/proto/dfs"

	"google.golang.org/grpc"
)

// ======================== DataNode Server ========================

// DataNodeServer implements the DataNodeService gRPC interface.
// It stores and retrieves file chunks from a local directory.
type DataNodeServer struct {
	pb.UnimplementedDataNodeServiceServer
	storageDir string // Local directory for storing chunks
	port       int    // Port this DataNode listens on
}

// NewDataNodeServer creates a new DataNodeServer with the given storage directory
func NewDataNodeServer(storageDir string, port int) *DataNodeServer {
	// Create storage directory if it doesn't exist
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		log.Fatalf("Failed to create storage directory '%s': %v", storageDir, err)
	}

	return &DataNodeServer{
		storageDir: storageDir,
		port:       port,
	}
}

// ======================== StoreChunk RPC ========================

// StoreChunk saves a chunk of data to the local storage directory.
// The chunk is stored as a file named by its ChunkID.
func (s *DataNodeServer) StoreChunk(ctx context.Context, chunk *pb.Chunk) (*pb.Ack, error) {
	// Build file path: storage/<chunkId>
	chunkPath := filepath.Join(s.storageDir, chunk.ChunkId)

	// Write chunk data to disk
	err := os.WriteFile(chunkPath, chunk.Data, 0644)
	if err != nil {
		log.Printf("FAILED to store chunk %s: %v", chunk.ChunkId, err)
		return &pb.Ack{
			Success: false,
			Message: fmt.Sprintf("Failed to write chunk: %v", err),
		}, nil
	}

	log.Printf("STORED chunk: %s (%d bytes)", chunk.ChunkId, len(chunk.Data))

	return &pb.Ack{
		Success: true,
		Message: fmt.Sprintf("Chunk %s stored successfully", chunk.ChunkId),
	}, nil
}

// ======================== GetChunk RPC ========================

// GetChunk reads a chunk from the local storage directory and returns it.
// This is called by the Client during file downloads.
func (s *DataNodeServer) GetChunk(ctx context.Context, req *pb.ChunkRequest) (*pb.Chunk, error) {
	// Build file path: storage/<chunkId>
	chunkPath := filepath.Join(s.storageDir, req.ChunkId)

	// Read chunk data from disk
	data, err := os.ReadFile(chunkPath)
	if err != nil {
		log.Printf("FAILED to read chunk %s: %v", req.ChunkId, err)
		return nil, fmt.Errorf("chunk '%s' not found: %v", req.ChunkId, err)
	}

	log.Printf("SERVED chunk: %s (%d bytes)", req.ChunkId, len(data))

	return &pb.Chunk{
		ChunkId: req.ChunkId,
		Data:    data,
	}, nil
}

// ======================== Main Entry Point ========================

func main() {
	// Command-line flags
	port := flag.Int("port", 50052, "DataNode server port")
	storageDir := flag.String("storage", "", "Storage directory for chunks (default: storage_<port>)")
	flag.Parse()

	// Default storage directory includes port to allow multiple DataNodes on same machine
	dir := *storageDir
	if dir == "" {
		dir = fmt.Sprintf("storage_%d", *port)
	}

	// Create DataNode server
	server := NewDataNodeServer(dir, *port)

	// Start gRPC server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Failed to listen on port %d: %v", *port, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterDataNodeServiceServer(grpcServer, server)

	log.Printf("========================================")
	log.Printf("DATANODE SERVER started on port %d", *port)
	log.Printf("Storage directory: %s", dir)
	log.Printf("========================================")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
