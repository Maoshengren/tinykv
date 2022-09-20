// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	// machine id
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	logger                log.Logger
	randomElectionTimeOut int

	// number of follower vote
	voteNum int
	// reject number
	rejectNum int
	// an estimate of the size of the uncommitted tail of the Raft log. Used to
	// prevent unbounded log growth. Only maintained by the leader. Reset on
	// term changes.
	uncommittedSize uint64
	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via pendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	pendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hs, _, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	raft := &Raft{
		id:                    c.ID,
		votes:                 make(map[uint64]bool),
		heartbeatTimeout:      c.HeartbeatTick,
		electionTimeout:       c.ElectionTick,
		randomElectionTimeOut: c.ElectionTick + rand.Intn(c.ElectionTick),
		Prs:                   make(map[uint64]*Progress),
		RaftLog:               newLog(c.Storage),
	}
	for _, v := range c.peers {
		//if v != c.ID {
		raft.Prs[v] = makeNewProgress()
		//}
	}
	if !IsEmptyHardState(hs) {
		raft.loadState(hs)
	}
	raft.becomeFollower(hs.Term, hs.Vote)
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return r.maybeSend(to, true)
}

func (r *Raft) maybeSend(to uint64, sendIfEmpty bool) bool {
	pr := r.Prs[to]
	m := pb.Message{}
	m.From = r.id
	m.To = to
	m.Term = r.Term

	term, errt := r.RaftLog.Term(pr.Next - 1)
	ents, erre := r.RaftLog.getEntries(pr.Next)
	if len(ents) == 0 && !sendIfEmpty {
		return false
	}

	if errt != nil || erre != nil {
		r.logger.Panicf("`r.RaftLog.Term` errt: %v, erre: %v", errt, erre)
	}
	m.MsgType = pb.MessageType_MsgAppend
	m.Index = pr.Next - 1
	m.LogTerm = term
	for i := range ents {
		m.Entries = append(m.Entries, &ents[i])
	}
	m.Commit = r.RaftLog.committed
	r.msgs = append(r.msgs, m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat() {
	// Your Code Here (2A).
	for to := range r.Prs {
		if r.id != to {
			msg := pb.Message{
				MsgType: pb.MessageType_MsgHeartbeat,
				To:      to,
				From:    r.id,
				Term:    r.Term,
				Entries: nil,
			}
			r.msgs = append(r.msgs, msg)
		}
	}
}

func (r *Raft) handleHeartBeatResponse(m pb.Message) {
	if m.Index < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
}

// sendRequestVote sends a request vote to peers
func (r *Raft) sendRequestVote(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		handleFollowerTick(r)
	case StateCandidate:
		handleFollowerTick(r)
	case StateLeader:
		handleLeaderTick(r)
	}
}

func handleFollowerTick(r *Raft) {
	r.electionElapsed++
	if r.electionElapsed >= r.randomElectionTimeOut {
		r.electionElapsed = 0
		r.voteNum = 0
		r.randomElectionTimeOut = r.electionTimeout + rand.Intn(r.electionTimeout)
		err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		if err != nil {
			log.Errorf("r `Step` error: %v", err)
		}
	}
}

func handleLeaderTick(r *Raft) {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
		if err != nil {
			log.Errorf("r `Step` error: %v", err)
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Term = term
	r.Lead = lead
	r.Vote = lead
	r.State = StateFollower
	r.logger.Infof("%x became follower at term %d", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	r.Lead = None
	r.Vote = r.id
	r.votes[r.id] = true
	r.voteNum++
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id

	emptyEnt := pb.Entry{Data: nil}
	if !r.appendEntry(&emptyEnt) {
		// This won't happen because we just called reset() above.
		r.logger.Panic("empty entry was dropped")
	}
	r.logger.Infof("%x became leader at term %d", r.id, r.Term)
	r.bcastAppend()
	//for to := range r.Prs {
	//	r.sendAppend(to)
	//}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.handleFollowerMsg(m)
	case StateCandidate:
		r.handleCandidateMsg(m)
	case StateLeader:
		r.handleLeaderMsg(m)
	}
	return nil
}

func (r *Raft) handleFollowerMsg(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	}
}

func (r *Raft) handleCandidateMsg(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	}
}

func (r *Raft) handleLeaderMsg(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgBeat:
		r.sendHeartbeat()
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartBeatResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	case pb.MessageType_MsgAppend:
		if m.Term > r.Term {
			r.State = StateFollower
			r.Term = m.Term
		}
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	}
}

func (r *Raft) startElection() {
	if r.State != StateLeader {
		r.becomeCandidate()
	} else {
		r.State = StateCandidate
	}
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	for to := range r.Prs {
		li := r.RaftLog.LastIndex()
		term, err := r.RaftLog.Term(li)
		if err != nil {
			r.logger.Panicf("`Term` error: %v", err)
		}
		if r.id != to {
			msg := pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				From:    r.id,
				To:      to,
				Term:    r.Term,
				Index:   li,
				LogTerm: term,
			}
			r.msgs = append(r.msgs, msg)
		}
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	if len(m.Entries) == 0 {
		r.logger.Panicf("%x stepped empty MsgProp", r.id)
	}
	if r.leadTransferee != None {
		r.logger.Debugf("%x [term %d] transfer leadership to %x is in progress; dropping proposal", r.id, r.Term, r.leadTransferee)
	}
	if !r.appendEntry(m.Entries...) {
		r.logger.Panicf("ErrProposalDropped")
	}
	r.bcastAppend()
}

func (r *Raft) appendEntry(ents ...*pb.Entry) (accepted bool) {
	li := r.RaftLog.LastIndex()
	for i := range ents {
		ents[i].Term = r.Term
		ents[i].Index = li + uint64(i) + 1
	}
	li = r.RaftLog.append(ents...)
	// 更新自己的状态值
	if r.Prs[r.id].Match < li {
		r.Prs[r.id].Match = li
		r.Prs[r.id].Next = max(r.Prs[r.id].Next, li+1)
	}
	r.maybeCommit()
	return true
}

func (r *Raft) bcastAppend() {
	for to := range r.Prs {
		if to != r.id {
			r.sendAppend(to)
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	msg := pb.Message{
		From:    r.id,
		To:      m.From,
		MsgType: pb.MessageType_MsgAppendResponse,
	}
	if m.Term < r.Term {
		msg.Reject = true
	} else {
		r.Lead = m.From
		r.State = StateFollower
	}
	r.Term = m.Term
	msg.Term = m.Term
	//if m.Index < r.RaftLog.committed {
	//	msg.Index = r.RaftLog.committed
	//	r.msgs = append(r.msgs, msg)
	//	return
	//}
	if mlastIndex, ok := r.RaftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		msg.Index = mlastIndex
	} else {
		r.logger.Debugf("%x [logterm: %d, index: %d] rejected MsgApp [logterm: %d, index: %d] from %x",
			r.id, r.RaftLog.zeroTermOnErrCompacted(r.RaftLog.Term(m.Index)), m.Index, m.LogTerm, m.Index, m.From)
		msg.Reject = true
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Reject {
		r.logger.Debugf("%x received MsgAppResp(rejected, hint: (term %d)) from %x for index %d",
			r.id, m.LogTerm, m.From, m.Index)
		r.logger.Debugf("Prs Next: %v, Match: %v", r.Prs[m.From].Next, r.Prs[m.From].Match)
		if m.Index <= r.Prs[m.From].Match {
			return
		}
		// TODO Next该怎么计算
		r.Prs[m.From].Next = r.Prs[m.From].Match + 1
		r.sendAppend(m.From)
	} else {
		if r.Prs[m.From].Match < m.Index {
			r.Prs[m.From].Match = m.Index
			r.Prs[m.From].Next = max(r.Prs[m.From].Next, m.Index+1)
		}
		if r.maybeCommit() {
			r.bcastAppend()
		}
	}
}

func (r *Raft) maybeCommit() bool {
	// 计算最大committed值
	n := len(r.Prs)
	var srt []uint64
	for i := range r.Prs {
		srt = append(srt, r.Prs[i].Match)
	}
	insertionSort(srt)
	pos := n - (n/2 + 1)
	if term, _ := r.RaftLog.Term(srt[pos]); term < r.Term {
		return false
	}
	return r.RaftLog.commitTo(srt[pos])
}

func insertionSort(sl []uint64) {
	a, b := 0, len(sl)
	for i := a + 1; i < b; i++ {
		for j := i; j > a && sl[j] < sl[j-1]; j-- {
			sl[j], sl[j-1] = sl[j-1], sl[j]
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if r.Term > m.Term && r.RaftLog.LastIndex() > m.Index {
		r.sendHeartBeatResponse(m, true)
	}
	r.State = StateFollower
	r.Term = m.Term
	r.electionElapsed = 0
	r.sendHeartBeatResponse(m, false)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) handleRequestVote(m pb.Message) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	}
	switch r.State {
	case StateFollower, StateLeader:
		if m.Term >= r.Term {
			li := r.RaftLog.LastIndex()
			term, _ := r.RaftLog.Term(li)
			msg.Index = li
			msg.LogTerm = term
			if m.Term > r.Term {
				r.State = StateFollower
				r.Vote = None
				r.Lead = None
			} else if m.Term == r.Term && r.Vote != None && r.Vote != m.From {
				msg.Reject = true
			}
			r.Term = m.Term
			// TODO
			//fmt.Printf("term: %v, li: %v, m.LogTerm: %v, m.Index: %v\n", term, li, m.LogTerm, m.Index)
			if term > m.LogTerm || (term == m.LogTerm && li > m.Index) {
				msg.Reject = true
			}
			if r.Vote != None && r.Vote != m.From {
				msg.Reject = true
			} else {
				r.Vote = m.From
			}
			//if len(r.Prs) <= 3 {
			//	r.Lead = m.From
			//}
		} else {
			msg.Reject = true
		}
	case StateCandidate:
		if m.From == r.id {
			for k := range r.Prs {
				r.sendRequestVote(k)
			}
		} else {
			if m.Term > r.Term {
				r.State = StateFollower
				r.Term = m.Term
				r.Vote = m.From
			}
			if m.Term <= r.Term && r.Vote != m.From {
				msg.Reject = true
			}
		}
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.To == r.id {
		// 任期号小或等，并且被拒绝
		if m.Reject == true {
			r.rejectNum++
			//li := r.RaftLog.LastIndex()
			//term, _ := r.RaftLog.Term(li)
			if r.Term < m.Term || r.rejectNum >= len(r.Prs)>>1+1 {
				//r.Term = m.Term
				r.State = StateFollower
			}
		}
		if m.Reject != true {
			if r.Term >= m.Term {
				r.voteNum++
				if r.voteNum >= (len(r.Prs)>>1)+1 {
					r.becomeLeader()
				}
			}
		}
	}
}

func (r *Raft) sendHeartBeatResponse(m pb.Message, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

func makeNewProgress() *Progress {
	p := &Progress{
		Match: 0,
		Next:  1,
	}
	return p
}

func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) loadState(state pb.HardState) {
	if state.Commit < r.RaftLog.committed || state.Commit > r.RaftLog.LastIndex() {
		r.logger.Panicf("%x state.commit %d is out of range [%d, %d]", r.id, state.Commit, r.RaftLog.committed, r.RaftLog.LastIndex())
	}
	r.RaftLog.committed = state.Commit
	r.Term = state.Term
	r.Vote = state.Vote
}

func (r *Raft) advance(rd Ready) {
	r.reduceUncommittedSize(rd.CommittedEntries)

	// If entries were applied (or a snapshot), update our cursor for
	// the next Ready. Note that if the current HardState contains a
	// new Commit index, this does not mean that we're also applying
	// all of the new entries due to commit pagination by size.
	if newApplied := rd.appliedCursor(); newApplied > 0 {
		//oldApplied := r.RaftLog.applied
		r.RaftLog.appliedTo(newApplied)

		//if oldApplied <= r.pendingConfIndex && newApplied >= r.pendingConfIndex && r.State == StateLeader {
		//	// If the current (and most recent, at least for this leader's term)
		//	// configuration should be auto-left, initiate that now. We use a
		//	// nil Data which unmarshals into an empty ConfChangeV2 and has the
		//	// benefit that appendEntry can never refuse it based on its size
		//	// (which registers as zero).
		//	ent := pb.Entry{
		//		EntryType: pb.EntryType_EntryConfChange,
		//		Data:      nil,
		//	}
		//	// There's no way in which this proposal should be able to be rejected.
		//	if !r.appendEntry(&ent) {
		//		panic("refused un-refusable auto-leaving ConfChangeV2")
		//	}
		//	r.pendingConfIndex = r.RaftLog.LastIndex()
		//}
	}

	if len(rd.Entries) > 0 {
		e := rd.Entries[len(rd.Entries)-1]
		r.RaftLog.stableTo(e.Index, e.Term)
	}
	if !IsEmptySnap(&rd.Snapshot) {
		r.RaftLog.stableSnapTo(rd.Snapshot.Metadata.Index)
	}
}

// reduceUncommittedSize accounts for the newly committed entries by decreasing
// the uncommitted entry size limit.
func (r *Raft) reduceUncommittedSize(ents []pb.Entry) {
	if r.uncommittedSize == 0 {
		// Fast-path for followers, who do not track or enforce the limit.
		return
	}

	var s uint64
	for _, e := range ents {
		s += uint64(len(e.Data))
	}
	if s > r.uncommittedSize {
		// uncommittedSize may underestimate the size of the uncommitted Raft
		// log tail but will never overestimate it. Saturate at 0 instead of
		// allowing overflow.
		r.uncommittedSize = 0
	} else {
		r.uncommittedSize -= s
	}
}
