package surfstore

import (
	context "context"
	"fmt"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	// keep retrying the net.Dial() until it is set up
	var conn *grpc.ClientConn
	var err error
	for {
		conn, err = grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the server
	// keep retrying the net.Dial() until it is set up
	var conn *grpc.ClientConn
	var err error
	for {
		conn, err = grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err = c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}
	*succ = true
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// connect to the server
	// keep retrying the net.Dial() until it is set up
	var conn *grpc.ClientConn
	var err error
	for {
		conn, err = grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	theBlockHashesIn := BlockHashes{
		Hashes: blockHashesIn,
	}
	theBlockHashesOut, err := c.HasBlocks(ctx, &theBlockHashesIn)
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = theBlockHashesOut.GetHashes()
	// close the connection
	return conn.Close()
}

// Returns a list containing all block hashes on this block server
func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	// connect to the server
	// keep retrying the net.Dial() until it is set up
	var conn *grpc.ClientConn
	var err error
	for {
		conn, err = grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	theBlockHashes, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashes = theBlockHashes.GetHashes()
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	// connect to the server
	// keep retrying the net.Dial() until it is set up
	var conn *grpc.ClientConn
	var err error
	for {
		conn, err = grpc.Dial(surfClient.MetaStoreAddrs[0], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	c := NewRaftSurfstoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	fileInfoMap, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
	fmt.Println("err: ", err)
	if err != nil {
		conn.Close()
		return err
	}
	*serverFileInfoMap = fileInfoMap.GetFileInfoMap()
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	// connect to the server
	// keep retrying the net.Dial() until it is set up
	var conn *grpc.ClientConn
	var err error
	for {
		conn, err = grpc.Dial(surfClient.MetaStoreAddrs[0], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	c := NewRaftSurfstoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	version, err := c.UpdateFile(ctx, fileMetaData)
	if err != nil {
		conn.Close()
		return err
	}
	*latestVersion = version.GetVersion()
	// close the connection
	return conn.Close()
}

// func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
// 	// connect to the server
// 	// keep retrying the net.Dial() until it is set up
// 	var conn *grpc.ClientConn
// 	var err error
// 	for {
// 		conn, err = grpc.Dial(surfClient.MetaStoreAddrs[0], grpc.WithTransportCredentials(insecure.NewCredentials()))
// 		if err == nil {
// 			break
// 		}
// 		time.Sleep(300 * time.Millisecond)
// 	}
// 	c := NewRaftSurfstoreClient(conn)

// 	// perform the call
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
// 	defer cancel()
// 	bsAddr, err := c.GetBlockStoreAddr(ctx, &emptypb.Empty{})
// 	if err != nil {
// 		conn.Close()
// 		return err
// 	}
// 	*blockStoreAddr = bsAddr.Addr
// 	// close the connection
// 	return conn.Close()
// }

// Given a list of block hashes, find out which block server they
// belong to. Returns a mapping from block server address to block hashes.
func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	// connect to the server
	// keep retrying the net.Dial() until it is set up
	var conn *grpc.ClientConn
	var err error
	for {
		conn, err = grpc.Dial(surfClient.MetaStoreAddrs[0], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	c := NewRaftSurfstoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	blockHashes := BlockHashes{
		Hashes: blockHashesIn,
	}

	theBlockStoreMap, err := c.GetBlockStoreMap(ctx, &blockHashes)
	if err != nil {
		conn.Close()
		return err
	}
	origianlBlockStoreMap := theBlockStoreMap.GetBlockStoreMap()

	// convert BlockStoreMap map[string]*BlockHashes to blockStoreMap *map[string][]string.
	for serverName, blockHashes := range origianlBlockStoreMap {
		hashes := blockHashes.GetHashes()
		(*blockStoreMap)[serverName] = hashes
	}
	// *blockStoreMap = theBlockStoreMap.GetBlockStoreMap()
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	// connect to the server
	// keep retrying the net.Dial() until it is set up
	var conn *grpc.ClientConn
	var err error
	for {
		conn, err = grpc.Dial(surfClient.MetaStoreAddrs[0], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	c := NewRaftSurfstoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	theBlockStoreAddrs, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}

	*blockStoreAddrs = theBlockStoreAddrs.GetBlockStoreAddrs()
	// close the connection
	return conn.Close()
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
