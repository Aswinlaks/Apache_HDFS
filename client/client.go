// =============================================================================
// client.go — CLI Client for the Distributed File System
//
// Commands:
//   upload <file>    — Upload a file to the DFS
//   download <file>  — Download a file from the DFS
//   list             — List all stored files
//
// Features:
//   - Connects to the Leader for metadata operations
//   - Fetches chunks directly from DataNodes during download
//   - Automatic retry on replica if a DataNode fails (fault tolerance)
// =============================================================================

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	pb "dfs-project/proto/dfs"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Command-line flags
	leaderAddr := flag.String("leader", "172.17.192.1:50051", "Leader server address")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		fmt.Println("========================================")
		fmt.Println("  Distributed File System — Client")
		fmt.Println("========================================")
		fmt.Println()
		fmt.Println("Usage:")
		fmt.Println("  go run client/client.go [--leader=host:port] <command> [args]")
		fmt.Println()
		fmt.Println("Commands:")
		fmt.Println("  upload <filename>    Upload a file to DFS")
		fmt.Println("  download <filename>  Download a file from DFS")
		fmt.Println("  list                 List all stored files")
		fmt.Println()
		fmt.Println("Examples:")
		fmt.Println("  go run client/client.go upload sample.txt")
		fmt.Println("  go run client/client.go download sample.txt")
		fmt.Println("  go run client/client.go --leader=192.168.1.10:50051 list")
		os.Exit(1)
	}

	command := args[0]

	switch command {
	case "upload":
		if len(args) < 2 {
			fmt.Println("Usage: upload <filename>")
			os.Exit(1)
		}
		uploadFile(*leaderAddr, args[1])

	case "download":
		if len(args) < 2 {
			fmt.Println("Usage: download <filename>")
			os.Exit(1)
		}
		downloadFile(*leaderAddr, args[1])

	case "list":
		listFiles(*leaderAddr)

	default:
		fmt.Printf("Unknown command: %s\n", command)
		fmt.Println("Available commands: upload, download, list")
		os.Exit(1)
	}
}

// ======================== Upload ========================

// uploadFile reads a local file and sends it to the Leader for storage
func uploadFile(leaderAddr, filename string) {
	fmt.Printf("========================================\n")
	fmt.Printf("UPLOADING: %s -> Leader (%s)\n", filename, leaderAddr)
	fmt.Printf("========================================\n")

	// Read the file from disk
	data, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("Failed to read file '%s': %v", filename, err)
	}
	fmt.Printf("File size: %d bytes\n", len(data))

	// Connect to Leader
	conn, err := grpc.Dial(leaderAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Failed to connect to Leader at %s: %v", leaderAddr, err)
	}
	defer conn.Close()

	client := pb.NewMetadataServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Extract just the filename (not the full path) for storage
	baseName := filename
	if idx := strings.LastIndexAny(filename, "/\\"); idx >= 0 {
		baseName = filename[idx+1:]
	}

	// Send upload request
	resp, err := client.UploadFile(ctx, &pb.FileData{
		Filename: baseName,
		Content:  data,
	})
	if err != nil {
		log.Fatalf("Upload RPC failed: %v", err)
	}

	if resp.Success {
		fmt.Printf("\nSUCCESS: %s\n", resp.Message)
	} else {
		fmt.Printf("\nFAILED: %s\n", resp.Message)
	}
}

// ======================== Download ========================

// downloadFile fetches chunk locations from the Leader, then downloads
// each chunk directly from DataNodes with automatic retry on replicas.
func downloadFile(leaderAddr, filename string) {
	fmt.Printf("========================================\n")
	fmt.Printf("DOWNLOADING: %s from Leader (%s)\n", filename, leaderAddr)
	fmt.Printf("========================================\n")

	// Connect to Leader to get chunk locations
	conn, err := grpc.Dial(leaderAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Failed to connect to Leader at %s: %v", leaderAddr, err)
	}
	defer conn.Close()

	client := pb.NewMetadataServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Get chunk locations from Leader
	fileChunks, err := client.DownloadFile(ctx, &pb.FileRequest{Filename: filename})
	if err != nil {
		log.Fatalf("Download RPC failed: %v", err)
	}

	fmt.Printf("File has %d chunks\n\n", len(fileChunks.Chunks))

	// Sort chunks by chunk ID to ensure correct order
	chunks := fileChunks.Chunks
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].ChunkId < chunks[j].ChunkId
	})

	// Fetch each chunk from DataNodes with retry on replica
	var fileData []byte
	for i, loc := range chunks {
		fmt.Printf("Chunk %d/%d: %s\n", i+1, len(chunks), loc.ChunkId)
		fmt.Printf("  Available on: %v\n", loc.DatanodeAddresses)

		chunkData, err := fetchChunkWithRetry(loc.ChunkId, loc.DatanodeAddresses)
		if err != nil {
			log.Fatalf("Failed to download chunk %s: %v", loc.ChunkId, err)
		}

		fileData = append(fileData, chunkData...)
		fmt.Printf("  Retrieved %d bytes\n", len(chunkData))
	}

	// Write reassembled file to disk
	outputName := "downloaded_" + filename
	err = os.WriteFile(outputName, fileData, 0644)
	if err != nil {
		log.Fatalf("Failed to write output file: %v", err)
	}

	fmt.Printf("\nSUCCESS: File saved as '%s' (%d bytes)\n", outputName, len(fileData))
}

// fetchChunkWithRetry tries to fetch a chunk from the list of DataNode addresses.
// If the primary node fails, it automatically retries on replica nodes.
// This demonstrates fault tolerance in the distributed system.
func fetchChunkWithRetry(chunkID string, addresses []string) ([]byte, error) {
	var lastErr error

	for i, addr := range addresses {
		if i > 0 {
			// This is a retry — show the failure handling message
			fmt.Printf("  Chunk retrieval failed from %s\n", addresses[i-1])
			fmt.Printf("  Trying replica %s...\n", addr)
		}

		data, err := fetchChunkFromDataNode(addr, chunkID)
		if err == nil {
			if i > 0 {
				fmt.Printf("  Successfully retrieved from replica!\n")
			}
			return data, nil
		}

		lastErr = err
		fmt.Printf("  WARNING: DataNode %s unavailable: %v\n", addr, err)
	}

	return nil, fmt.Errorf("all replicas failed for chunk %s: %v", chunkID, lastErr)
}

// fetchChunkFromDataNode connects to a single DataNode and retrieves a chunk
func fetchChunkFromDataNode(addr, chunkID string) ([]byte, error) {
	conn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("connection failed: %v", err)
	}
	defer conn.Close()

	client := pb.NewDataNodeServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	chunk, err := client.GetChunk(ctx, &pb.ChunkRequest{ChunkId: chunkID})
	if err != nil {
		return nil, err
	}

	return chunk.Data, nil
}

// ======================== List Files ========================

// listFiles queries the Leader for all stored files
func listFiles(leaderAddr string) {
	fmt.Printf("========================================\n")
	fmt.Printf("FILE LIST from Leader (%s)\n", leaderAddr)
	fmt.Printf("========================================\n")

	conn, err := grpc.Dial(leaderAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Failed to connect to Leader at %s: %v", leaderAddr, err)
	}
	defer conn.Close()

	client := pb.NewMetadataServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.ListFiles(ctx, &pb.Empty{})
	if err != nil {
		log.Fatalf("ListFiles RPC failed: %v", err)
	}

	if len(resp.Filenames) == 0 {
		fmt.Println("\nNo files stored yet.")
	} else {
		fmt.Printf("\nStored files (%d):\n", len(resp.Filenames))
		for i, name := range resp.Filenames {
			fmt.Printf("  %d. %s\n", i+1, name)
		}
	}
}
