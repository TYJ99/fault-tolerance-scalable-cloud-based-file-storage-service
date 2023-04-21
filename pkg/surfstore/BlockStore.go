package surfstore

import (
	context "context"
	"fmt"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

var blockLock = sync.RWMutex{}

type BlockStore struct {
	BlockMap map[string]*Block // blockHashValue : Block
	UnimplementedBlockStoreServer
}

// b = GetBlock(h): Retrieves a block indexed by hash value h
func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	blockLock.RLock()
	defer blockLock.RUnlock()

	hashVal := blockHash.GetHash()
	block, ok := bs.BlockMap[hashVal]
	if !ok {
		return nil, fmt.Errorf("Block not found")
	}
	return block, nil
}

// PutBlock(b): Stores block b in the key-value store, indexed by hash value h
func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	blockLock.Lock()
	defer blockLock.Unlock()

	blockData := block.GetBlockData()
	insertedBlockHashVal := GetBlockHashString(blockData)
	bs.BlockMap[insertedBlockHashVal] = block
	return &Success{
		Flag: true,
	}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	blockHashesOut := &BlockHashes{
		Hashes: make([]string, 0),
	}

	// var blockHashesOut *BlockHashes

	blockLock.RLock()
	defer blockLock.RUnlock()

	for _, hashVal := range blockHashesIn.GetHashes() {
		_, ok := bs.BlockMap[hashVal]
		if ok {
			blockHashesOut.Hashes = append(blockHashesOut.Hashes, hashVal)
		}
	}
	return blockHashesOut, nil
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	blockHashes := BlockHashes{
		Hashes: make([]string, 0),
	}

	for blockHash, _ := range bs.BlockMap {
		blockHashes.Hashes = append(blockHashes.Hashes, blockHash)
	}

	return &blockHashes, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
