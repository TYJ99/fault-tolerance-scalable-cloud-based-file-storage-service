package surfstore

import (
	context "context"
	"fmt"
	"math"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation

	metaStore *MetaStore

	nextIndex  []int64
	rpcClients []RaftSurfstoreClient

	// Server Info
	serverID int64
	serverIP string
	peers    []string // server addr list

	pendingCommits []*chan bool
	commitIndex    int64
	lastApplied    int64

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

// 1. If the node is the leader, and if a majority of the nodes are working, should return the correct answer.
// 2. If a majority of the nodes are crashed, should block until a majority recover.
// 3. If not the leader, should indicate an error back to the client
// Returns metadata from the filesystem
func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	// // check if the server is crashed
	// s.isCrashedMutex.RLock()
	// if s.isCrashed {
	// 	s.isCrashedMutex.RUnlock()
	// 	return nil, ERR_SERVER_CRASHED
	// }
	// s.isCrashedMutex.RUnlock()

	// check if the server is the leader
	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()

	for {
		majorityCount := 1
		for peerID, peerAddr := range s.peers {
			if int64(peerID) == s.serverID {
				continue
			}

			conn, err := grpc.Dial(peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return nil, err
			}
			peer := NewRaftSurfstoreClient(conn)

			prevLogTerm := int64(-1)
			prevLogIndex := int64(-1)
			if len(s.log) != 0 {
				prevLogTerm = s.log[len(s.log)-1].Term
				prevLogIndex = int64(len(s.log) - 1)
			}
			// if s.nextIndex[s.serverID] > 0 {
			// 	prevLogIndex = int64(s.nextIndex[s.serverID] - 1)
			// 	prevLogTerm = s.log[prevLogIndex].Term
			// }
			appendEntryInput := &AppendEntryInput{
				Term:         s.term,
				PrevLogTerm:  prevLogTerm,
				PrevLogIndex: prevLogIndex,
				Entries:      make([]*UpdateOperation, 0),
				LeaderCommit: s.commitIndex,
			}

			appendEntryOutput, _ := peer.AppendEntries(ctx, appendEntryInput)

			if appendEntryOutput != nil {
				majorityCount++
				if appendEntryOutput.Term > s.term {
					// There is a peer whose term is greater than the current server,
					// which means that peer might be the real leader.
					// Thus, we should call GetFileInfoMap based on that peer to get fileInfoMap.
					return peer.GetFileInfoMap(ctx, empty)
				}
			}
		}

		if majorityCount > len(s.peers)/2 {
			break
		}
	}
	return s.metaStore.GetFileInfoMap(ctx, empty)
}

// 1. If the node is the leader, and if a majority of the nodes are working, should return the correct answer.
// 2. If a majority of the nodes are crashed, should block until a majority recover.
// 3. If not the leader, should indicate an error back to the client
// Returns the block store map
func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	// // check if the server is crashed
	// s.isCrashedMutex.RLock()
	// if s.isCrashed {
	// 	s.isCrashedMutex.RUnlock()
	// 	return nil, ERR_SERVER_CRASHED
	// }
	// s.isCrashedMutex.RUnlock()

	// check if the server is the leader
	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()

	for {
		majorityCount := 1
		for peerID, peerAddr := range s.peers {
			if int64(peerID) == s.serverID {
				continue
			}

			conn, err := grpc.Dial(peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return nil, err
			}
			peer := NewRaftSurfstoreClient(conn)
			// // perform the call
			// ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			// defer cancel()

			prevLogTerm := int64(-1)
			prevLogIndex := int64(-1)
			if len(s.log) != 0 {
				prevLogTerm = s.log[len(s.log)-1].Term
				prevLogIndex = int64(len(s.log) - 1)
			}
			// if s.nextIndex[s.serverID] > 0 {
			// 	prevLogIndex = int64(s.nextIndex[s.serverID] - 1)
			// 	prevLogTerm = s.log[prevLogIndex].Term
			// }

			appendEntryInput := &AppendEntryInput{
				Term:         s.term,
				PrevLogTerm:  prevLogTerm,
				PrevLogIndex: prevLogIndex,
				Entries:      make([]*UpdateOperation, 0),
				LeaderCommit: s.commitIndex,
			}

			appendEntryOutput, _ := peer.AppendEntries(ctx, appendEntryInput)

			if appendEntryOutput != nil {
				majorityCount++
				if appendEntryOutput.Term > s.term {
					// There is a peer whose term is greater than the current server,
					// which means that peer might be the real leader.
					// Thus, we should call GetBlockStoreMap based on that peer to get blockStoreMap.
					return peer.GetBlockStoreMap(ctx, hashes)
				}
			}
		}

		if majorityCount > len(s.peers)/2 {
			break
		}
	}
	return s.metaStore.GetBlockStoreMap(ctx, hashes)
}

// 1. If the node is the leader, and if a majority of the nodes are working, should return the correct answer.
// 2. If a majority of the nodes are crashed, should block until a majority recover.
// 3. If not the leader, should indicate an error back to the client
// Returns the block store addresses
func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	// // check if the server is crashed
	// s.isCrashedMutex.RLock()
	// if s.isCrashed {
	// 	s.isCrashedMutex.RUnlock()
	// 	return nil, ERR_SERVER_CRASHED
	// }
	// s.isCrashedMutex.RUnlock()

	// check if the server is the leader
	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()

	for {
		majorityCount := 1
		for peerID, peerAddr := range s.peers {
			if int64(peerID) == s.serverID {
				continue
			}

			conn, err := grpc.Dial(peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return nil, err
			}
			peer := NewRaftSurfstoreClient(conn)
			// // perform the call
			// ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			// defer cancel()

			prevLogTerm := int64(-1)
			prevLogIndex := int64(-1)
			if len(s.log) != 0 {
				prevLogTerm = s.log[len(s.log)-1].Term
				prevLogIndex = int64(len(s.log) - 1)
			}
			// if s.nextIndex[s.serverID] > 0 {
			// 	prevLogIndex = int64(s.nextIndex[s.serverID] - 1)
			// 	prevLogTerm = s.log[prevLogIndex].Term
			// }

			appendEntryInput := &AppendEntryInput{
				Term:         s.term,
				PrevLogTerm:  prevLogTerm,
				PrevLogIndex: prevLogIndex,
				Entries:      make([]*UpdateOperation, 0),
				LeaderCommit: s.commitIndex,
			}

			appendEntryOutput, _ := peer.AppendEntries(ctx, appendEntryInput)

			if appendEntryOutput != nil {
				majorityCount++
				if appendEntryOutput.Term > s.term {
					// There is a peer whose term is greater than the current server,
					// which means that peer might be the real leader.
					// Thus, we should call GetBlockStoreAddrs based on that peer to get blockStoreAddrs.
					return peer.GetBlockStoreAddrs(ctx, empty)
				}
			}
		}

		if majorityCount > len(s.peers)/2 {
			break
		}
	}
	return s.metaStore.GetBlockStoreAddrs(ctx, empty)
}

// Updates a file’s metadata
// 1. If the node is the leader, and if a majority of the nodes are working, should return the correct answer.
// 2. If a majority of the nodes are crashed, should block until a majority recover.
// 3. If not the leader, should indicate an error back to the client
func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	// check if the server is the leader
	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()

	// I don't check if the server is crashed here since it might not be the real leader.
	// If the server is not the real leader, we can handle the real leader later.
	// Otherwise, return ERR_SERVER_CRASHED later.

	// // check if the server is crashed
	// s.isCrashedMutex.RLock()
	// if s.isCrashed {
	// 	s.isCrashedMutex.RUnlock()
	// 	return nil, ERR_SERVER_CRASHED
	// }
	// s.isCrashedMutex.RUnlock()

	updateOp := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}

	// append entry to our log
	s.log = append(s.log, &updateOp)
	commitChan := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, &commitChan)

	// send entry to all followers in parallel
	go s.sendToAllFollowersInParallel(ctx)

	// keep trying indefinitely (even after responding) ** rely on sendheartbeat

	// commit the entry once majority of followers have it in their log
	isSuccess := <-commitChan

	// once committed, apply to the state machine
	if isSuccess {
		return s.metaStore.UpdateFile(ctx, filemeta)
	} else {
		// the current server is NOT the real leader, update file with the real leader.
		if len(s.rpcClients) != 0 {
			realLeader := s.rpcClients[0]
			s.rpcClients = make([]RaftSurfstoreClient, 0)
			return realLeader.UpdateFile(ctx, filemeta)
		}
		// the current server is the real leader, return ERR_SERVER_CRASHED.
		s.isCrashedMutex.RLock()
		if s.isCrashed {
			s.isCrashedMutex.RUnlock()
			return nil, ERR_SERVER_CRASHED
		}
		s.isCrashedMutex.RUnlock()
	}

	return nil, nil
}

func (s *RaftSurfstore) sendToAllFollowersInParallel(ctx context.Context) {
	// send entry to all my followers and count the replies
	targetIdx := s.commitIndex + 1
	responses := make(chan *AppendEntryOutput, len(s.peers))
	// contact all the follower, send some AppendEntries call
	for peerID, addr := range s.peers {
		if int64(peerID) == s.serverID {
			continue
		}

		go s.sendToFollower(ctx, int64(peerID), addr, responses)
	}

	// totalResponses := 1
	totalAppends := 1

	// wait in loop for responses
	for {
		result := <-responses
		if result.Term > s.term {
			// s *RaftSurfstore is NOT the leader.
			// We should handle the operation with real leader.
			// The real leader is stored in s.rpcClients.
			*s.pendingCommits[targetIdx] <- false
			return
		}
		// We confirm that (s *RaftSurfstore) is the leader compared to the result.
		s.isCrashedMutex.RLock()
		if s.isCrashed {
			s.isCrashedMutex.RUnlock()
			*s.pendingCommits[targetIdx] <- false
			return
		}
		s.isCrashedMutex.RUnlock()

		// totalResponses++
		if result.Success {
			totalAppends++
		} else {
			// handle followers crashed or have other errors
			followerAddr := s.peers[result.ServerId]
			go s.sendToFollower(ctx, result.ServerId, followerAddr, responses)
		}

		if totalAppends > len(s.peers)/2 {
			// TODO put on correct channel
			*s.pendingCommits[targetIdx] <- true
			// TODO update commit Index correctly
			s.commitIndex = targetIdx
			break
		}

		// if totalResponses == len(s.peers) {
		// 	break
		// }
	}

}

func (s *RaftSurfstore) sendToFollower(ctx context.Context, serverID int64, addr string, responses chan *AppendEntryOutput) {
	for {

		// I don't check if the server is crashed here since it might not be the real leader.
		// If the server is not the real leader, we can find the real leader later and continue with the real leader
		// instead of returning ERR_SERVER_CRASHED immediately.

		// s.isCrashedMutex.RLock()
		// if s.isCrashed {
		// 	responses <- false
		// 	s.isCrashedMutex.RUnlock()
		// 	return
		// }
		// s.isCrashedMutex.RUnlock()

		prevLogTerm := int64(-1)
		prevLogIndex := int64(-1)
		peerNextIndex := s.nextIndex[serverID]
		if peerNextIndex > 0 {
			prevLogIndex = int64(peerNextIndex - 1)
			prevLogTerm = s.log[prevLogIndex].Term
		}
		// peerNextIndex := s.nextIndex[serverID]
		// prevLogIndex := peerNextIndex - 1
		// prevLogTerm := int64(-1)
		// if prevLogIndex >= 0 {
		// 	prevLogTerm = s.log[prevLogIndex].Term
		// }
		entries := make([]*UpdateOperation, 0)
		if peerNextIndex >= 0 {
			entries = s.log[peerNextIndex:]
		}

		appendEntryInput := &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  prevLogTerm,
			PrevLogIndex: prevLogIndex,
			Entries:      entries,
			LeaderCommit: s.commitIndex,
		}

		// TODO check all errors
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return
		}
		peer := NewRaftSurfstoreClient(conn)
		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		appendEntryOutput, err := peer.AppendEntries(ctx, appendEntryInput)
		if err != nil {
			output := &AppendEntryOutput{
				ServerId:     serverID,
				Term:         s.term,
				Success:      false,
				MatchedIndex: -1,
			}
			responses <- output
			return
		} else if appendEntryOutput != nil && appendEntryOutput.Success {
			s.nextIndex[serverID] += int64(len(entries))
			responses <- appendEntryOutput
			return
		} else if appendEntryOutput != nil && appendEntryOutput.Term > s.term {
			// There is a peer whose term is greater than the current server,
			// which means that peer might be the real leader.
			// Thus, we should updateFile based on that peer(real leader).
			s.rpcClients = append(s.rpcClients, peer)
			responses <- appendEntryOutput
			return
		} else {
			//  If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
			if s.nextIndex[serverID] > 0 {
				s.nextIndex[serverID]--
			}
		}

		// We confirm that (s *RaftSurfstore) is the leader compared to the peer whose id is serverID.
		s.isCrashedMutex.RLock()
		if s.isCrashed {
			output := &AppendEntryOutput{
				ServerId:     serverID,
				Term:         s.term,
				Success:      false,
				MatchedIndex: -1,
			}
			responses <- output
			s.isCrashedMutex.RUnlock()
			return
		}
		s.isCrashedMutex.RUnlock()
	}

	// TODO check output
	// responses <- true

}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
// input is sent from the leader
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {

	// check if the server is crashed
	s.isCrashedMutex.RLock()
	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if input.Term > s.term {
		s.isLeaderMutex.Lock()
		s.isLeader = false
		s.isLeaderMutex.Unlock()
		s.term = input.Term
	}

	appendEntryOutput := &AppendEntryOutput{
		ServerId:     s.serverID,
		Term:         s.term,
		Success:      false,
		MatchedIndex: -1,
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if input.Term < s.term {
		fmt.Println("rule1")
		return appendEntryOutput, nil
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
	// matches prevLogTerm (§5.3)
	if int64(len(s.log)) <= input.PrevLogIndex {
		fmt.Println("len(s.log) = ", len(s.log))
		fmt.Println("input.PrevLogIndex = ", input.PrevLogIndex)
		fmt.Println("rule2.1")
		return appendEntryOutput, nil
	}
	if input.PrevLogIndex >= 0 && s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
		fmt.Println("rule2.2")
		return appendEntryOutput, nil
	}

	// 3. If an existing entry conflicts with a new one (same index but different
	// terms), delete the existing entry and all that follow it (§5.3)
	s.log = s.log[:input.PrevLogIndex+1]

	// 4. Append any new entries not already in the log
	s.log = append(s.log, input.Entries...)

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
	// of last new entry)
	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))
	}

	// If commitIndex > lastApplied: increment lastApplied, apply
	// log[lastApplied] to state machine (§5.3)
	for s.lastApplied < s.commitIndex {
		s.lastApplied++
		entry := s.log[s.lastApplied]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
	}

	appendEntryOutput.Success = true
	appendEntryOutput.Term = s.term
	return appendEntryOutput, nil
}

// Emulates elections, sets the node to be the leader
// 1. Should return ERR_SERVER_CRASHED error.
// 2. Procedure has no effect if server is crashed
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	defer time.Sleep(1300 * time.Millisecond)
	// Procedure has no effect if server is crashed
	// and return ERR_SERVER_CRASHED error
	s.isCrashedMutex.RLock()
	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()

	s.isLeaderMutex.Lock()
	defer s.isLeaderMutex.Unlock()
	s.isLeader = true

	s.term++
	// TODO update state
	// 1. update pendingCommits
	s.pendingCommits = make([]*chan bool, s.commitIndex+1)

	// 2. update nextIndex
	for peerID := range s.peers {
		s.nextIndex[peerID] = int64(len(s.log))
	}
	return &Success{Flag: true}, nil
}

// Sends a round of AppendEntries to all other nodes.
// The leader will attempt to replicate logs to
// all other nodes when this is called. It can be called even
// when there are no entries to replicate.
// If a node is not in the leader state, it should do nothing.
// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// check if the server is crashed
	s.isCrashedMutex.RLock()
	if s.isCrashed {
		s.isCrashedMutex.RUnlock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()

	// check if the server is the leader
	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()

	for peerID, peerAddr := range s.peers {
		if int64(peerID) == s.serverID {
			continue
		}

		conn, err := grpc.Dial(peerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		peer := NewRaftSurfstoreClient(conn)
		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		prevLogTerm := int64(-1)
		prevLogIndex := int64(-1)
		peerNextIndex := s.nextIndex[peerID]
		if peerNextIndex > 0 {
			prevLogIndex = int64(peerNextIndex - 1)
			prevLogTerm = s.log[prevLogIndex].Term
		}
		// peerNextIndex := s.nextIndex[peerID]
		// prevLogIndex := peerNextIndex - 1
		// prevLogTerm := int64(-1)
		// if prevLogIndex >= 0 {
		// 	prevLogTerm = s.log[prevLogIndex].Term
		// }
		entries := make([]*UpdateOperation, 0)
		if peerNextIndex >= 0 {
			entries = s.log[peerNextIndex:]
		}

		appendEntryInput := &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  prevLogTerm,
			PrevLogIndex: prevLogIndex,
			Entries:      entries,
			LeaderCommit: s.commitIndex,
		}

		appendEntryOutput, err := peer.AppendEntries(ctx, appendEntryInput)
		if err != nil {
			continue
		}
		if appendEntryOutput != nil {
			if appendEntryOutput.Success {
				s.nextIndex[peerID] += int64(len(entries))
			}
		}
	}

	return &Success{Flag: true}, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
