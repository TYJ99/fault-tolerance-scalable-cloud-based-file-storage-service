package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	// serverHash : serverName
	ServerMap map[string]string
}

// Given a blockHash, this func returns which server it belongs to.
// blockID: block hash value
func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	// 1. Since map in golang is unordered, we have to sort the serverHashs
	//    which are the keys in the ConsistentHashRing first.
	serverHashes := []string{}
	for hash, _ := range c.ServerMap {
		serverHashes = append(serverHashes, hash)
	}
	sort.Strings(serverHashes)

	// 2. Find the FIRST server with larger hash value than the block
	responsibleServer := ""
	serverHashesLen := len(serverHashes)
	// blockHash := c.Hash(blockId)
	for i := 0; i < serverHashesLen; i++ {
		if serverHashes[i] > blockId {
			responsibleServer = c.ServerMap[serverHashes[i]]
			break
		}
	}
	// If you cannot find the corresponding block server after traversing
	// all the block servers on the consistentHashRing,
	// the responsibleServer is the first block server on the consistentHashRing.
	if responsibleServer == "" {
		responsibleServer = c.ServerMap[serverHashes[0]]
	}
	return responsibleServer
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	// serverMap := make(map[string]string)
	consistentHashRing := ConsistentHashRing{
		ServerMap: make(map[string]string),
	}
	for _, serverName := range serverAddrs {
		newServerName := "blockstore" + serverName
		serverHash := consistentHashRing.Hash(newServerName)
		consistentHashRing.ServerMap[serverHash] = serverName
	}
	return &consistentHashRing
}
