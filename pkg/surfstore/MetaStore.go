package surfstore

import (
	context "context"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

var metaLock = sync.RWMutex{}

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

// Returns a mapping of the files stored in the SurfStore cloud service,
// including the version, filename, and hashlist.
func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	metaLock.RLock()
	defer metaLock.RUnlock()

	return &FileInfoMap{
		FileInfoMap: m.FileMetaMap,
	}, nil
}

// Updates the FileInfo values associated with a file stored in the cloud.
// This method replaces the hash list for the file with the provided hash
// list only if the new version number is exactly one greater than the
// current version number. Otherwise, you can send version=-1 to the
// client telling them that the version they are trying to store is not right
// (likely too old).
func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	defer metaLock.Unlock()
	metaLock.Lock()
	var version Version
	_, ok := m.FileMetaMap[fileMetaData.GetFilename()]
	// 1. The file is NOT in the FileMetaMap: add it to the FileMetaMap and set version to fileMetaData.GetVersion()
	// 2. The file is in the FileMetaMap and the new version number is exactly one greater than the
	// 	  current version number: update it and set version to fileMetaData.GetVersion()
	// 3. The file is in the FileMetaMap but the version is WRONG, set version to -1
	if !ok {
		m.FileMetaMap[fileMetaData.GetFilename()] = fileMetaData
		version = Version{Version: fileMetaData.GetVersion()}

	} else {
		if fileMetaData.GetVersion()-m.FileMetaMap[fileMetaData.GetFilename()].GetVersion() == 1 {
			m.FileMetaMap[fileMetaData.GetFilename()] = fileMetaData
			version = Version{Version: fileMetaData.GetVersion()}

		} else {
			version = Version{Version: -1}

		}

	}
	return &version, nil
}

// // Returns the BlockStore address.
// func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
// 	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
// }

// Given a list of block hashes, find out which block server they belong to.
// Returns a mapping from block server address to block hashes.
func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	metaLock.RLock()
	defer metaLock.RUnlock()
	blockStoreMap := BlockStoreMap{
		BlockStoreMap: make(map[string]*BlockHashes),
	}
	// initialize each BlockHashes in blockStoreMap
	cHRServerMap := m.ConsistentHashRing.ServerMap
	for _, serverName := range cHRServerMap {
		blockStoreMap.BlockStoreMap[serverName] = &BlockHashes{
			Hashes: []string{},
		}
	}

	blockHashesInLen := len(blockHashesIn.GetHashes())
	for i := 0; i < blockHashesInLen; i++ {
		responsibleServer := m.ConsistentHashRing.GetResponsibleServer(blockHashesIn.GetHashes()[i])
		blockStoreMap.BlockStoreMap[responsibleServer].Hashes = append(blockStoreMap.BlockStoreMap[responsibleServer].Hashes, blockHashesIn.GetHashes()[i])
	}

	return &blockStoreMap, nil
}

// Returns all the BlockStore addresses.
func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	metaLock.RLock()
	defer metaLock.RUnlock()

	return &BlockStoreAddrs{
		BlockStoreAddrs: m.BlockStoreAddrs,
	}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
