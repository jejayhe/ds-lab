package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"github.com/sasha-s/go-deadlock"
	"math/rand"
	//"sync"

	//"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	IsLeader     bool   // only leader needs to communicate to rpc handler
	Snapshot     []byte // used to send snapshot to state machine
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu deadlock.Mutex // Lock to protect shared access to this peer's state
	//mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// 2A
	state           StateType //
	leaderId        int
	electionTimeout time.Time
	voteCount       int //
	voteMap         map[int]bool
	leaderChan      chan int
	//followerLastResp []int64 // record follower last show up timestamp, this information is for leader

	currentTerm int      // init to 0
	votedFor    int      // -1 to represent nil
	log         []*Entry // first index is 1
	//logIndex int // init to 0

	commitIndex int // init to 0
	lastApplied int // init to 0

	nextIndex  []int // init to last log index +1
	matchIndex []int // initialize to 0

	// 2B
	applyCh chan ApplyMsg
	//lastLogIndex int
	logSyncedMap map[int]bool // indicate a follower's log is synced or not. for leader use

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// lab3
	//ClientSequence map[string]int64 // client unique identifier: sequence number. need to persist

	logOffset int
}

type StateType int

const (
	State_Follower StateType = iota
	State_Candidate
	State_Leader
)

var stateNameMap = map[StateType]string{
	State_Follower:  "follower",
	State_Candidate: "candidate",
	State_Leader:    "leader",
}

func (rf *Raft) UpdateLastApplied(cmdIndex int) {
	rf.mu.Lock()
	if cmdIndex > rf.lastApplied {
		rf.lastApplied = cmdIndex
	}
	rf.mu.Unlock()
}

////if leaderId is valid, return leaderId, nil
////else return -1, error
// if rf is the leader, return 0, ok
// if rf is not the leader, return a server to ask for
func (rf *Raft) IsLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.leaderId == rf.me {
		return true
	}
	return false
	//DPrintf("[GetLeader] rf.leaderId=%d rf.me=%d", rf.leaderId, rf.me)
	//if rf.leaderId >= 0 && rf.leaderId < len(rf.peers) {
	//	if rf.leaderId == rf.me {
	//		return 0, true
	//	} else {
	//		return rf.leaderId, false
	//	}
	//} else {
	//	return rand.Intn(1e4) % len(rf.peers), false
	//}
}

func (rf *Raft) lenLog() int {
	return len(rf.log) + rf.logOffset
}
func (rf *Raft) logAt(idx int) *Entry {
	if idx-rf.logOffset < 0 || idx-rf.logOffset >= len(rf.log) {
		DPrintf("[FATAL ERROR] server %d raft.logAt idx=%d logOffset=%d lenLog= %d", rf.me, idx, rf.logOffset, rf.lenLog())
		return nil
	} else {
		return rf.log[idx-rf.logOffset]
	}
}

func (rf *Raft) logFromTo(from, to int) []*Entry {
	if from == -1 && to == -1 {
		return rf.log
	} else if from == -1 {
		if to-rf.logOffset > len(rf.log) || to-rf.logOffset <= 0 {
			DPrintf("[FATAL ERROR] raft.logFromTo 1")
		}
		return rf.log[:to-rf.logOffset]
	} else if to == -1 {
		if from-rf.logOffset >= len(rf.log) || from-rf.logOffset < 0 {
			DPrintf("[FATAL ERROR] raft.logFromTo 2")
		}
		return rf.log[from-rf.logOffset:]
	} else {
		if to-rf.logOffset > len(rf.log) || to-rf.logOffset <= 0 ||
			from-rf.logOffset >= len(rf.log) || from-rf.logOffset < 0 {
			DPrintf("[FATAL ERROR] raft.logFromTo 3")
		}
		return rf.log[from-rf.logOffset : to-rf.logOffset]
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == State_Leader
	// if leader's follower haven't shown up recently then it's not legit leader
	//if isleader {
	//	acc := 1
	//	nowUnixNano := time.Now().UnixNano()
	//	var heartbeatTimeout int64 = 1e9
	//	for i, val := range rf.followerLastResp {
	//		if i != rf.me {
	//			if nowUnixNano-val < heartbeatTimeout {
	//				acc++
	//			}
	//		}
	//	}
	//	if acc*2 <= len(rf.peers) {
	//		isleader = false
	//	}
	//}

	return term, isleader
}
func (rf *Raft) TimeForSnapshot(threshold int) bool {
	if threshold <= 0 {
		return false
	}
	if rf.persister.RaftStateSize() > threshold*90/100 {
		return true
	}
	return false
}

func (rf *Raft) TakeSnapshot(snapshot []byte, lastIncludedIndex, lastIncludedTerm int) {
	w1 := new(bytes.Buffer)
	e1 := labgob.NewEncoder(w1)

	// todo lock/unlock ??
	rf.mu.Lock()
	DPrintf("[SNAPSHOT] raft server %d take snapshot ...", rf.me)
	e1.Encode(rf.currentTerm)
	e1.Encode(rf.votedFor)
	oldlen := len(rf.log)
	rf.log = rf.logFromTo(rf.lastApplied, -1)
	DPrintf("[DEBUG] server %d rf.lastApplied = %d log trimmed from %d to %d rf.commitIndex = %d", rf.me, rf.lastApplied, oldlen, len(rf.log), rf.commitIndex)
	e1.Encode(rf.log)
	rf.logOffset = rf.lastApplied
	e1.Encode(rf.logOffset)
	rf.mu.Unlock()

	data1 := w1.Bytes()
	// todo trim log
	//
	rf.persister.SaveStateAndSnapshot(data1, snapshot)
}

func (rf *Raft) RestoreSnapshot() []byte {
	data := rf.persister.ReadSnapshot()
	return data
	//r := bytes.NewBuffer(data)
	//d := labgob.NewDecoder(r)
	//var currentTerm int
	//var votedFor int
	//var log []*Entry
	//if d.Decode(&currentTerm) != nil ||
	//	d.Decode(&votedFor) != nil ||
	//	d.Decode(&log) != nil {
	//	DPrintf("[ERROR] readPersist")
	//} else {
	//	rf.mu.Lock()
	//	rf.currentTerm = currentTerm
	//	rf.votedFor = votedFor
	//	rf.log = log
	//	rf.mu.Unlock()
	//}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	//rf.mu.Lock()
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.logOffset)
	//rf.mu.Unlock()

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []*Entry
	var logOffset int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&logOffset) != nil {
		DPrintf("[ERROR] readPersist")
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.logOffset = logOffset
		rf.commitIndex = logOffset
		rf.mu.Unlock()
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	ServerId    int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.ServerId = rf.me
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		// todo
		rf.updateTerm(args.Term)
	}
	// todo valid request should result in timeout clearance
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		//2B compare log
		myLastLogIndex := rf.lenLog() - 1
		myLastLogTerm := rf.logAt(myLastLogIndex).Term
		if args.LastLogTerm > myLastLogTerm || (args.LastLogTerm == myLastLogTerm && args.LastLogIndex >= myLastLogIndex) {
			rf.resetElectionTimeout_Enclosed()
			// todo: need to check log
			reply.Term = args.Term
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.persist()
		}
	}
	return
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*Entry
	LeaderCommit int
	HeartBeat    bool // if it is heartbeat then true
	Synced       bool // if already synced, then follower can commit.

	EntryStartIndex int // indicate start index of entries sent
}

type Entry struct {
	//Index int
	Command interface{}
	Term    int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	Xterm  int // term of conflicting entry
	Xindex int // index of first entry of Xterm
	Xlen   int // length of log
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if rf.state == State_Candidate {
		rf.state = State_Follower
	}
	if (rf.state == State_Leader && args.Term > rf.currentTerm) || (rf.state == State_Candidate && args.Term >= rf.currentTerm) {
		//DPrintf("server %d term %d update to term %d state update from %s to %s \n", rf.me,
		//	rf.currentTerm, args.Term, stateNameMap[rf.state], stateNameMap[State_Follower])
		rf.updateTerm(args.Term)
		rf.leaderId = args.LeaderId
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	// lab3
	if rf.state == State_Follower {
		//if args.LeaderId==-1{
		//	DPrintf("[ERROR] AE rpc handler leaderId is -1")
		//}
		rf.leaderId = args.LeaderId
	}

	// 2B
	if !args.HeartBeat {

		// local log too few, refuse, need a smaller prevLogIndex
		if args.PrevLogIndex >= rf.lenLog() {
			DPrintf("[WARNING] too few local log [prevLogIndex:%d] [rf.lenLog():%d]", args.PrevLogIndex, rf.lenLog())
			reply.Xterm = -1
			reply.Xlen = rf.lenLog()

		} else if args.PrevLogIndex-rf.logOffset < 0 {
			DPrintf("[FATAL] Raft.AppendEntries error 1")
			reply.Xterm = rf.log[0].Term
			reply.Xindex = rf.logOffset
		} else if rf.logAt(args.PrevLogIndex).Term != args.PrevLogTerm {
			DPrintf("[WARNING] PrevLogTerm doesn't match")
			// delete existing entry and all that follow it
			reply.Xterm = rf.logAt(args.PrevLogIndex).Term
			i := args.PrevLogIndex
			for i >= rf.logOffset && rf.logAt(i).Term == reply.Xterm {
				i--
			}
			reply.Xindex = i + 1
		} else {
			// normal op, delete existing entry and all that follow it
			if args.Entries != nil {
				// append entries not already in the log
				/*
					entryStartIndex.... actualNextIndex... end
					entries
					log len 11, last index 10,
					if entryStartIndex = 5
					if entryStartIndex = 11
					entries = [1,2]
					startEntry = 8
				*/
				logs := make([]interface{}, 0)
				// fixing bug I don't quite understand
				if rf.lenLog()-args.EntryStartIndex > len(args.Entries)-1 {
					goto Label
				}
				realEntries := args.Entries[rf.lenLog()-args.EntryStartIndex:]
				for _, e := range args.Entries[rf.lenLog()-args.EntryStartIndex:] {
					logs = append(logs, e.Command)
				}
				DPrintf("[RECEIVE LOG] receive [%d %d] logs on server %d : %+v", rf.lenLog(), rf.lenLog()+len(logs)-1, rf.me, logs)
				rf.log = append(rf.log, realEntries...)
				rf.persist()
			} else {
				DPrintf("[SYNCING] cut log to index %d", args.PrevLogIndex)
				rf.log = rf.logFromTo(-1, args.PrevLogIndex+1)
				rf.persist()
			}
		Label:
			reply.Term = rf.currentTerm
			reply.Success = true
		}
	}
	// leaderCommit
	if args.Synced && args.LeaderCommit > rf.commitIndex {
		var newCommitIndex int
		if args.LeaderCommit > rf.lenLog()-1 {
			newCommitIndex = rf.lenLog() - 1
		} else {
			newCommitIndex = args.LeaderCommit
		}
		for j := rf.commitIndex + 1; j <= newCommitIndex; j++ {
			DPrintf("[FOLLOWER COMMIT] server %d args.LeaderCommit = %d rf.commitIndex = %d lenLog = %d", rf.me, args.LeaderCommit,
				rf.commitIndex, rf.lenLog())
			if j < rf.logOffset {
				break
			}
			DPrintf("[FOLLOWER COMMIT] server %d commit log %d command %v", rf.me, j, rf.logAt(j).Command)
			if j > rf.commitIndex {
				//DPrintf("[FOLLOWER COMMIT] server %d update rf.commitIndex from %d to %d", rf.commitIndex, newCommitIndex)
				rf.commitIndex = j
			}
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logAt(j).Command,
				CommandIndex: j,
			}
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		}

	}

	rf.resetElectionTimeout_Enclosed()
	rf.mu.Unlock()
	// receiver resets election timeout
	//rf.resetElectionTimeout()

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	// if snapshot is old. discard
	if args.LastIncludedIndex < rf.logOffset {
		rf.mu.Unlock()
		return
	}
	DPrintf("[INSTALL SNAPSHOT] reset state machine using snapshot contents")
	rf.applyCh <- ApplyMsg{Snapshot: args.Data}
	rf.log = make([]*Entry, 0)
	rf.log = append(rf.log, &Entry{Term: args.LastIncludedTerm})
	rf.logOffset = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex

	w1 := new(bytes.Buffer)
	e1 := labgob.NewEncoder(w1)
	e1.Encode(rf.currentTerm)
	e1.Encode(rf.votedFor)
	//oldlen := len(rf.log)
	//rf.log = rf.logFromTo(rf.lastApplied, -1)
	//DPrintf("[DEBUG] server %d rf.lastApplied = %d log trimmed from %d to %d rf.commitIndex = %d", rf.me, rf.lastApplied, oldlen, len(rf.log), rf.commitIndex)
	e1.Encode(rf.log)
	//rf.logOffset = rf.lastApplied
	e1.Encode(rf.logOffset)
	//rf.mu.Unlock()

	data1 := w1.Bytes()

	rf.persister.SaveStateAndSnapshot(data1, args.Data)

	rf.mu.Unlock()
}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

const electionDelay = 250
const electionRandRange = 200

func (rf *Raft) resetElectionTimeout() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.resetElectionTimeout_Enclosed()
	return
}

func (rf *Raft) checkElectionTimeout() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state == State_Leader {
			rf.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
			continue
		}
		// not time out yet
		if time.Now().Before(rf.electionTimeout) {
			rf.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
			continue
		}
		rf.mu.Unlock()
		rf.startCampaignForSelf()
	}
}

// if I am leader, send heartbeat to others
func (rf *Raft) monitorLeaderStatus() {
	for !rf.killed() {
		select {
		case <-rf.leaderChan:
			// todo init nextIndex, matchIndex
			rf.mu.Lock()
			n := len(rf.peers)
			lastLogIndex := rf.lenLog() - 1
			for i := 0; i < n; i++ {
				rf.nextIndex[i] = lastLogIndex + 1
				rf.matchIndex[i] = 0
				rf.logSyncedMap[i] = false
			}
			//rf.followerLastResp = make([]int64, len(rf.peers))
			rf.mu.Unlock()
			// if become leader, will send out heartbeat
			rf.leaderHeartbeatJob()
		}
	}
}
func (rf *Raft) sendSingleHeartBeat(me int, idx int, oldTerm int) {
	rf.mu.Lock()
	args := &AppendEntriesArgs{
		Term:         oldTerm,
		LeaderId:     me,
		LeaderCommit: rf.commitIndex,
		HeartBeat:    true,
		Synced:       rf.logSyncedMap[idx],
	}
	rf.mu.Unlock()
	reply := &AppendEntriesReply{}

	rf.sendAppendEntries(idx, args, reply)
	//DPrintf("after send heartbeat from server %d to server %d oldTerm %d result %v", me, idx, oldTerm, ok)
	rf.mu.Lock()
	// update followerLastResp
	//if ok && requestTime > rf.followerLastResp[idx] {
	//	rf.followerLastResp[idx] = requestTime
	//}

	if rf.currentTerm < reply.Term {
		//DPrintf("server %d to %d heartbeat exiting method 1... \n", rf.me, idx)
		rf.updateTerm(reply.Term)
		rf.mu.Unlock()
		return
	}
	// if no longer the leader or term change, exit.
	if rf.state != State_Leader || rf.leaderId != rf.me || rf.currentTerm != oldTerm {
		//DPrintf("server %d to %d heartbeat exiting method 2... \n", rf.me, idx)
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
}
func (rf *Raft) sendHeartBeatTo(me int, idx int, oldTerm int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != State_Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		go rf.sendSingleHeartBeat(me, idx, oldTerm)
		time.Sleep(100 * time.Millisecond)
	}
}
func (rf *Raft) leaderHeartbeatJob() {
	rf.mu.Lock()
	n := len(rf.peers)
	me := rf.me
	oldTerm := rf.currentTerm
	DPrintf("server %d term %d sending heartbeat \n", rf.me, rf.currentTerm)
	rf.mu.Unlock()
	for i := 0; i < n; i++ {
		if i != me {
			go rf.sendHeartBeatTo(me, i, oldTerm)
			go rf.replicateLogTo(i)
		}
	}
}

func (rf *Raft) replicateLogTo(idx int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != State_Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		rf.syncLog(idx)
		time.Sleep(50 * time.Millisecond)
	}
}
func (rf *Raft) syncLog(idx int) {
	rf.mu.Lock()
	/*
		at first,
	*/

	// no new log
	if rf.lenLog() <= rf.nextIndex[idx] {
		rf.mu.Unlock()
		return
	}

	// if not already synced
	/*
		if PrevLogIndex == 0, reply.success, then synced.
	*/
	if !rf.logSyncedMap[idx] {
		// todo debug
		if rf.nextIndex[idx] < rf.logOffset+1 {
			rf.nextIndex[idx] = rf.lenLog()
		}
		nextIndex := rf.nextIndex[idx]
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[idx] - 1,
			PrevLogTerm:  rf.logAt(nextIndex - 1).Term,
			Entries:      nil,
			//Entries:      []*Entry{rf.log[rf.nextIndex[idx]]},
			LeaderCommit: rf.commitIndex,
			HeartBeat:    false,
			Synced:       false,
		}
		//DPrintf("[SYNCING] send %d logs from server %d to server %d", len(entries), rf.me, idx)
		rf.mu.Unlock()
		reply := &AppendEntriesReply{}

		rf.sendAppendEntries(idx, args, reply)

		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		if !reply.Success {
			// if conflicting, xterm!=-1
			// 		if leader doesn't have xterm, go to follower's first entry of term
			// 		else , go to leader's first entry of term.
			// else, go to xlen
			if reply.Xterm != -1 {
				// check if leader has xterm
				leaderHasXterm := false
				i := nextIndex
				// bug that I don't understand
				if i >= rf.lenLog() {
					rf.mu.Unlock()
					goto EXIT1
				}
				for i >= rf.logOffset && rf.logAt(i).Term >= reply.Xterm {
					if rf.logAt(i).Term == reply.Xterm {
						leaderHasXterm = true
						break
					}
					i--
				}
				if !leaderHasXterm {
					DPrintf("[CONFLICT] leader doesn't have xterm, go to reply.xindex %d", reply.Xindex)
					rf.nextIndex[idx] = reply.Xindex
				} else {
					leaderTerm := rf.logAt(nextIndex).Term
					for rf.logAt(i).Term == leaderTerm {
						i--
					}
					DPrintf("[CONFLICT] leader have xterm, go to leader's first entry %d", i+1)
					rf.nextIndex[idx] = i + 1
				}

				rf.mu.Unlock()
			} else {
				if reply.Xlen <= rf.logOffset {
					DPrintf("[CONFLICT] no log, follower %d needs snapshot at %d", idx, rf.logOffset)
					isArgs := &InstallSnapshotArgs{
						Term:              rf.currentTerm,
						LeaderId:          rf.leaderId,
						LastIncludedIndex: rf.logOffset,
						LastIncludedTerm:  rf.log[0].Term,
						Data:              rf.persister.ReadSnapshot(),
					}
					isReply := &InstallSnapshotReply{}
					rf.mu.Unlock()
					rf.sendInstallSnapshot(idx, isArgs, isReply)
				} else {
					DPrintf("[CONFLICT] no log, follower %d backoff to %d", idx, reply.Xlen)
					rf.nextIndex[idx] = reply.Xlen
					rf.mu.Unlock()
				}
			}
		EXIT1:
			return
		} else {
			DPrintf("[SYNCING] syncing is completed from leader %d to follower %d", rf.me, idx)
			rf.logSyncedMap[idx] = true
		}
		rf.mu.Unlock()
		return
	}
	/*
		init state, first log
		nextIndex = 1
		matchIndex = 0
		term = 1
		lastLogIndex = 1
		prevLogIndex = 0
		prevLogTerm = 0
		Entries = first entry
		commitIndex = 0

	*/
	// todo debug
	if rf.nextIndex[idx] <= rf.logOffset {
		rf.logSyncedMap[idx] = false
		rf.mu.Unlock()
		return
	}
	lastLogIndex := rf.lenLog() - 1
	entries := rf.logFromTo(rf.nextIndex[idx], -1)
	nextIndex := rf.nextIndex[idx]
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[idx] - 1,
		PrevLogTerm:  rf.logAt(rf.nextIndex[idx] - 1).Term,
		Entries:      entries,
		//Entries:      []*Entry{rf.log[rf.nextIndex[idx]]},
		LeaderCommit:    rf.commitIndex,
		Synced:          true,
		EntryStartIndex: rf.nextIndex[idx],
	}
	DPrintf("[REPLICATE] send %d logs from server %d to server %d", len(entries), rf.me, idx)
	rf.mu.Unlock()
	reply := &AppendEntriesReply{}

	rf.sendAppendEntries(idx, args, reply)
	// if fail, decrement nextIndex, and retry

	// assume logs are synced
	if reply.Success {
		rf.mu.Lock()
		DPrintf("[RPC REPLY] leader update nextIndex of %d to %d", idx, lastLogIndex+1)
		rf.nextIndex[idx] = lastLogIndex + 1
		rf.matchIndex[idx] = lastLogIndex
		rf.mu.Unlock()
		// increment commitIndex
		// find top (n-1)/2, which is the commit index.
		rf.mu.Lock()
		// enumerate N from nextIndex to lastLogIndex
		N := nextIndex
		// todo
		if N > rf.commitIndex {
			N = rf.commitIndex + 1
		}
		for N <= lastLogIndex {
			// count how many matchIndex larger than N
			acc := 0
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					if rf.matchIndex[i] >= N {
						acc++
					}
				}
			}
			if acc+1 > len(rf.peers)/2 {
				if rf.commitIndex < N {
					rf.commitIndex = N
					//DPrintf("[DEBUG] update commit index to %d", N)
					DPrintf("[LEADER COMMIT] server %d commit log %d command %v", rf.me, N, rf.logAt(N).Command)
					applyMsg := ApplyMsg{
						Command:      rf.logAt(N).Command,
						CommandValid: true,
						CommandIndex: N,
						IsLeader:     true,
					}
					rf.mu.Unlock()
					rf.applyCh <- applyMsg
					rf.mu.Lock()
				}

			} else {
				break
			}
			N++
		}
		rf.mu.Unlock()
	}

}

// enclosed in mutex
func (rf *Raft) updateTerm(newterm int) {
	DPrintf("[UPDATE TERM]server %d term %d update to term %d state update from %s to %s \n", rf.me,
		rf.currentTerm, newterm, stateNameMap[rf.state], stateNameMap[State_Follower])
	rf.currentTerm = newterm
	rf.votedFor = -1
	rf.persist()
	rf.state = State_Follower
	rf.resetElectionTimeout_Enclosed()
}

func (rf *Raft) resetElectionTimeout_Enclosed() {
	electionTimeout := (rand.Intn(electionRandRange) + electionDelay) * 1e6
	rf.electionTimeout = time.Now().Add(time.Duration(electionTimeout))
}

func (rf *Raft) startCampaignForSelf() {
	//for {
	rf.mu.Lock()
	rf.state = State_Candidate
	rf.resetElectionTimeout_Enclosed()
	rf.currentTerm++
	DPrintf("[CAMPAIGN] server %d at term %d start campaign\n", rf.me, rf.currentTerm)
	rf.votedFor = rf.me
	rf.persist()
	rf.voteCount = 1
	rf.voteMap = make(map[int]bool)
	me := rf.me
	rf.voteMap[me] = true

	tmpTerm := rf.currentTerm
	tmpCandidateId := rf.me
	lenLog := rf.lenLog()
	tmpLastLogIndex := lenLog - 1
	tmpLastLogTerm := rf.logAt(lenLog - 1).Term

	n := len(rf.peers)
	rf.mu.Unlock()
	//winChan := make(chan int)
	for i := 0; i < n; i++ {
		if i != me {
			go func(idx int, tmpTerm int, tmpCandidateId int, tmpLastLogIndex int, tmpLastLogTerm int) {
				arg := &RequestVoteArgs{
					Term:         tmpTerm,
					CandidateId:  tmpCandidateId,
					LastLogIndex: tmpLastLogIndex,
					LastLogTerm:  tmpLastLogTerm,
				}
				reply := &RequestVoteReply{}
				rf.sendRequestVote(idx, arg, reply)
				rf.mu.Lock()
				if rf.currentTerm != tmpTerm {
					rf.mu.Unlock() // bug
					return
				}
				if reply.Term > rf.currentTerm {
					rf.updateTerm(reply.Term)
					rf.mu.Unlock()
					return
				}
				defer rf.mu.Unlock()
				if rf.state != State_Candidate {
					return
				}
				_, ok := rf.voteMap[reply.ServerId]
				if reply.Term == tmpTerm && reply.VoteGranted && !ok {
					DPrintf("server %d term %d get a vote from server %d\n", rf.me, rf.currentTerm, reply.ServerId)
					rf.voteMap[reply.ServerId] = true
					rf.voteCount++
				}
				if 2*rf.voteCount > n {
					DPrintf("[NEW LEADER]server %d term %d get enough vote\n", rf.me, tmpTerm)
					rf.voteCount = 0
					// win the election
					rf.leaderId = rf.me
					rf.state = State_Leader
					rf.leaderChan <- 1
					return
				}
			}(i, tmpTerm, tmpCandidateId, tmpLastLogIndex, tmpLastLogTerm)
		}
	}
	//rf.resetElectionTimeout()
	//time.Sleep(2000 * time.Millisecond)
	//select {
	//case <-winChan:
	//	rf.mu.Lock()
	//	DPrintf("node %d become leader\n", rf.me)
	//	rf.mu.Unlock()
	//case <-rf.leaderChan:
	//
	//case <-time.After(100 * time.Millisecond):
	//	continue

	//}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	rf.mu.Lock()
	// Your code here (2B).
	if rf.state != State_Leader {
		rf.mu.Unlock()
		isLeader = false
	} else {
		//commitIndex:=rf.commitIndex
		index = rf.lenLog()
		term = rf.currentTerm
		// todo go send AE rpc to others.
		//rf.logIndex++
		DPrintf("[START CMD] %+v", command)
		rf.log = append(rf.log, &Entry{
			//Index: rf.logIndex,
			Command: command,
			Term:    rf.currentTerm,
		})
		rf.persist()

		rf.mu.Unlock()

	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	DPrintf("[KILL] kill server")
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// 2A
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.leaderId = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = State_Follower
	rf.leaderChan = make(chan int)
	go rf.resetElectionTimeout()
	go rf.checkElectionTimeout()
	go rf.monitorLeaderStatus()

	// 2B
	rf.applyCh = applyCh
	n := len(peers)
	rf.nextIndex = make([]int, n)
	rf.matchIndex = make([]int, n)
	rf.logSyncedMap = make(map[int]bool)
	for i := 0; i < n; i++ {
		// todo for testing purpose
		// assume log are synced
		rf.logSyncedMap[i] = false
		//rf.nextIndex[i] = 1
		//rf.matchIndex[i] = 0
	}
	rf.log = make([]*Entry, 0)
	rf.log = append(rf.log, &Entry{
		Command: nil,
		Term:    0,
	})

	rf.mu.Unlock()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
