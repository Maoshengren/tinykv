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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	logger log.Logger
	// next index to receive
	unstabledOffset uint64
	// storage.LastIndex() + 1
	offset uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		log.Panic("storage must not be nil")
	}
	l := &RaftLog{
		storage: storage,
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	l.unstabledOffset = lastIndex + 1
	l.committed = firstIndex - 1
	l.applied = firstIndex - 1
	l.stabled = lastIndex
	l.offset = lastIndex + 1
	ents, err := storage.Entries(firstIndex, l.unstabledOffset)
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	l.entries = append(l.entries, ents...)
	return l
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}
	return l.entries[l.offset-l.firstIndex():]
}

// nextEntries returns all the committed but not applied entries
func (l *RaftLog) nextEntries() (ents []pb.Entry) {
	// Your Code Here (2A).
	off := l.applied + 1
	if l.committed+1 > off {
		ents, err := l.slice(off, l.committed+1)
		if err != nil {
			return nil
		}
		return ents
	}
	return nil
}

func (l *RaftLog) getEntries(i uint64) ([]pb.Entry, error) {
	if i > l.LastIndex() {
		return nil, nil
	}
	return l.slice(i, l.LastIndex()+1)
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if ln := len(l.entries); ln != 0 {
		return l.unstabledOffset - 1
	}
	//if l.pendingSnapshot != nil {
	//	return l.pendingSnapshot.Metadata.Index
	//}
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return i
}

func (l *RaftLog) firstIndex() uint64 {
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index + 1
	}
	index, err := l.storage.FirstIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	return index
}

func (l *RaftLog) commitTo(tocommit uint64) bool {
	if l.committed < tocommit {
		if l.LastIndex() < tocommit {
			l.logger.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, l.LastIndex())
		}
		l.committed = tocommit
		l.offset = tocommit + 1
		return true
	}
	return false
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	dummyIndex := l.firstIndex() - 1
	if i < dummyIndex || i > l.LastIndex() {
		// TODO: return an error instead?
		return 0, nil
	}

	if t, ok := l.maybeTerm(i); ok {
		return t, nil
	}

	t, err := l.storage.Term(i)
	if err == nil {
		return t, nil
	}
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	panic(err)
}

func (l *RaftLog) maybeTerm(i uint64) (uint64, bool) {
	fi, _ := l.storage.FirstIndex()
	if i <= fi-1 {
		if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index == i {
			return l.pendingSnapshot.Metadata.Term, true
		}
		return 0, false
	}

	last, ok := l.maybeLastIndex()
	if !ok {
		return 0, false
	}
	if i > last {
		return 0, false
	}

	return l.entries[i-fi].Term, true
}

// maybeLastIndex returns the last index if it has at least one
// unstable entry or snapshot.
func (l *RaftLog) maybeLastIndex() (uint64, bool) {
	if ln := len(l.entries); ln != 0 {
		return uint64(ln) + 1, true
	}
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index, true
	}
	return 0, false
}

// maybeAppend returns (0, false) if the entries cannot be appended. Otherwise,
// it returns (last index of new entries, true).
func (l *RaftLog) maybeAppend(index, logTerm, committed uint64, ents ...*pb.Entry) (lastnewi uint64, ok bool) {
	if l.matchTerm(index, logTerm) {
		lastnewi = index + uint64(len(ents))
		ci := l.findConflict(ents)
		switch {
		case ci == 0:
		case ci <= l.committed:
			l.logger.Panicf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed)
		default:
			offset := index + 1
			if ci-offset > uint64(len(ents)) {
				l.logger.Panicf("index, %d, is out of range [%d]", ci-offset, len(ents))
			}
			l.append(ents[ci-offset:]...)
		}
		l.commitTo(min(committed, lastnewi))
		return lastnewi, true
	}
	return 0, false
}

func (l *RaftLog) append(ents ...*pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.LastIndex()
	}
	if after := ents[0].Index - 1; after < l.committed {
		l.logger.Panicf("after(%d) is out of range [committed(%d)]", after, l.committed)
	}
	l.truncateAndAppend(ents)
	return l.LastIndex()
}

//{EntryType:EntryNormal Term:1 Index:1 Data:[] XXX_NoUnkeyedLiteral:{} XXX_unrecognized:[] XXX_sizecache:0}
//{EntryType:EntryNormal Term:1 Index:2 Data:[115 111 109 101 32 100 97 116 97] XXX_NoUnkeyedLiteral:{} XXX_unrecognized:[] XXX_sizecache:0}]
//
//{EntryType:EntryNormal Term:1 Index:2 Data:[115 111 109 101 32 100 97 116 97] XXX_NoUnkeyedLiteral:{} XXX_unrecognized:[] XXX_sizecache:0}
func (l *RaftLog) truncateAndAppend(ents []*pb.Entry) {
	after := ents[0].Index
	switch {
	case after == l.unstabledOffset:
		// after is the next index in the u.entries
		// directly append
		for i := range ents {
			l.entries = append(l.entries, *ents[i])
			l.unstabledOffset++
		}
	case after <= l.offset:
		l.logger.Infof("replace the unstable entries from index %d", after)
		// The log is being truncated to before our current unstabledOffset
		// portion, so set the unstabledOffset and replace the entries
		l.unstabledOffset = after
		l.offset = after
		l.entries = l.entries[:after-l.firstIndex()]
		for i := range ents {
			l.entries = append(l.entries, *ents[i])
			l.unstabledOffset++
		}
	default:
		// TODO
		// truncate to after and copy to u.entries
		// then append
		l.logger.Infof("truncate the unstable entries before index %d", after)
		l.entries = append([]pb.Entry{}, l.unstableSlice(l.unstabledOffset, after)...)
		for i := range ents {
			l.entries = append(l.entries, *ents[i])
		}
	}
}

// TODO
func (l *RaftLog) slice(lo, hi uint64) ([]pb.Entry, error) {
	err := l.mustCheckOutOfBounds(lo, hi)
	if err != nil {
		return nil, err
	}
	if lo == hi {
		return nil, nil
	}
	var ents []pb.Entry
	offset, _ := l.storage.LastIndex()
	if lo <= offset {
		storedEnts, err := l.storage.Entries(lo, min(hi, offset+1))
		if err == ErrCompacted {
			return nil, err
		} else if err == ErrUnavailable {
			l.logger.Panicf("entries[%d:%d) is unavailable from storage", lo, min(hi, l.offset+1))
		} else if err != nil {
			panic(err) // TODO(bdarnell)
		}
		if uint64(len(storedEnts)) < min(hi, offset+1)-lo {
			return storedEnts, nil
		}
		ents = storedEnts
	}
	if hi >= offset+1 {
		unstable := l.unstableSlice(max(lo, offset+1), hi)
		if len(ents) > 0 {
			// TODO 为什么不用append
			combined := make([]pb.Entry, len(ents)+len(unstable))
			n := copy(combined, ents)
			copy(combined[n:], unstable)
			ents = combined
		} else {
			ents = unstable
		}
	}
	return ents, nil
}

func (l *RaftLog) unstableSlice(lo uint64, hi uint64) []pb.Entry {
	// u.offset <= lo <= hi <= u.unstabledOffset
	if lo > hi {
		l.logger.Panicf("invalid unstable.unstableSlice %d > %d", lo, hi)
	}
	//if lo < l.stabled || hi > l.unstabledOffset {
	//	l.logger.Panicf("unstable.unstableSlice[%d,%d) out of bound [%d,%d]", lo, hi, l.offset, l.unstabledOffset)
	//}
	//li, _ := l.storage.LastIndex()
	return l.entries[lo-l.firstIndex() : hi-l.firstIndex()]
}

func (l *RaftLog) findConflict(ents []*pb.Entry) uint64 {
	for _, ne := range ents {
		if !l.matchTerm(ne.Index, ne.Term) {
			if ne.Index <= l.LastIndex() {
				l.logger.Infof("found conflict at index %d [existing term: %d, conflicting term: %d]",
					ne.Index, l.zeroTermOnErrCompacted(l.Term(ne.Index)), ne.Term)
			}
			return ne.Index
		}
	}
	return 0
}

// l.firstIndex <= lo <= hi <= l.firstIndex + len(l.entries)
func (l *RaftLog) mustCheckOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		l.logger.Panicf("invalid slice %d > %d", lo, hi)
	}
	fi := l.firstIndex()
	if lo < fi {
		return ErrCompacted
	}

	if hi > l.LastIndex()+1 {
		l.logger.Panicf("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, l.LastIndex())
	}
	return nil
}

func (l *RaftLog) matchTerm(i, term uint64) bool {
	t, err := l.Term(i)
	if err != nil {
		return false
	}
	return t == term
}

func (l *RaftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if err == ErrCompacted {
		return 0
	}
	l.logger.Panicf("unexpected error (%v)", err)
	return 0
}

func (l *RaftLog) stableTo(i, t uint64) {
	gt, ok := l.maybeTerm(i)
	if !ok {
		return
	}
	// if i < offset, term is matched with the snapshot
	// only update the unstable entries if term is matched with
	// an unstable entry.
	if gt == t && i >= l.offset {
		l.entries = l.entries[i+1-l.offset:]
		l.offset = i + 1
		l.shrinkEntriesArray()
	}
}

func (l *RaftLog) stableSnapTo(i uint64) {
	if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index == i {
		l.pendingSnapshot = nil
	}
}

func (l *RaftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if l.committed < i || i < l.applied {
		l.logger.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	}
	l.applied = i
}

// shrinkEntriesArray discards the underlying array used by the entries slice
// if most of it isn't being used. This avoids holding references to a bunch of
// potentially large entries that aren't needed anymore. Simply clearing the
// entries wouldn't be safe because clients might still be using them.
func (l *RaftLog) shrinkEntriesArray() {
	// We replace the array if we're using less than half of the space in
	// it. This number is fairly arbitrary, chosen as an attempt to balance
	// memory usage vs number of allocations. It could probably be improved
	// with some focused tuning.
	const lenMultiple = 2
	if len(l.entries) == 0 {
		l.entries = nil
	} else if len(l.entries)*lenMultiple < cap(l.entries) {
		newEntries := make([]pb.Entry, len(l.entries))
		copy(newEntries, l.entries)
		l.entries = newEntries
	}
}

// TODO
func (l *RaftLog) getUEnts() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}
	li, _ := l.storage.LastIndex()
	return l.entries[li:]
}
