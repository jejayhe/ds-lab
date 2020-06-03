package shardmaster

import (
	"../raft"
	"log"
	"sync/atomic"
)
import "../labrpc"
import "sync"
import "../labgob"

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs           []Config // indexed by config num
	returnChanMap     map[int]chan ReturnChanData
	clientLogindexMap map[int64]int // record client max log index

	gidShardMap map[int][]int
	groupNum    int

	dead int32 // set by Kill()
}

type Optype int8

const (
	Optype_join Optype = iota
	Optype_leave
	Optype_move
	Optype_query
)

type Op struct {
	// Your data here.
	Args     interface{}
	Opcode   Optype
	ClientId int64
}

type ReturnChanData struct {
	Ok     bool
	Config *Config
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	ok := sm.rf.IsLeader()
	if !ok {
		reply.WrongLeader = true
		return
	}
	op := Op{
		Args:     args,
		Opcode:   Optype_join,
		ClientId: args.ClientId,
	}
	DPrintf("[ShardMaster %d] Join [args=%+v] sent", sm.me, args)
	sm.mu.Lock()
	logindex, _, _ := sm.rf.Start(op)
	if _, ok := sm.returnChanMap[logindex]; ok {
		DPrintf("[ShardMaster %d FATAL ERROR] Join returnChanMap already full", sm.me)
		sm.mu.Unlock()
		return
	}
	returnChan := make(chan ReturnChanData)
	sm.returnChanMap[logindex] = returnChan
	sm.mu.Unlock()
	<-returnChan
	DPrintf("[ShardMaster] Join get resp from ReturnChan")
	return
}

/*
	oldmap: map[gid] shards
	when add group, unallocated == 0
	when remove group, unallocated != 0
*/
func balance(oldmap map[int][]int, unallocated []int) {
	// compute current num and expected num
	cmap := make(map[int]int)
	emap := make(map[int]int)
	groupCount := 0
	for gid, shards := range oldmap {
		cmap[gid] = len(shards)
		groupCount++
	}
	left := NShards
	for gid, c := range cmap {
		expected := left / groupCount
		modifier := 0
		if left%groupCount != 0 {
			modifier = 1
		}
		if c < expected {
			emap[gid] = expected
			left -= expected
		} else if c > expected+modifier {
			emap[gid] = expected + modifier
			left -= expected + modifier
		} else {
			emap[gid] = c
			left -= c
		}
		groupCount--
	}
	// collect shards from oldmap
	for gid, shards := range oldmap {
		if cmap[gid] > emap[gid] {
			gap := cmap[gid] - emap[gid]
			unallocated = append(unallocated, shards[:gap]...)
			oldmap[gid] = shards[gap:]
		}
	}
	// give shards to oldmap
	for gid, _ := range oldmap {
		if cmap[gid] < emap[gid] {
			gap := emap[gid] - cmap[gid]
			oldmap[gid] = append(oldmap[gid], unallocated[:gap]...)
			unallocated = unallocated[gap:]
		}
	}
}

func (sm *ShardMaster) join_exec(inputArgs interface{}) {
	lastConfig := sm.configs[len(sm.configs)-1]
	newGroup := make(map[int][]string)
	for gid, serverNames := range lastConfig.Groups {
		newGroup[gid] = serverNames
	}
	args := inputArgs.(*JoinArgs)
	extraGroupNum := 0
	for gid, serverNames := range args.Servers {
		extraGroupNum++
		newGroup[gid] = serverNames
		sm.gidShardMap[gid] = make([]int, 0)
	}
	totalGroupNum := sm.groupNum + extraGroupNum
	balance(sm.gidShardMap, make([]int, 0))
	sm.groupNum = totalGroupNum
	var newShards [NShards]int
	for gid, shards := range sm.gidShardMap {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newConfig := Config{
		Num:    sm.groupNum,
		Shards: newShards,
		Groups: newGroup,
	}
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	ok := sm.rf.IsLeader()
	if !ok {
		reply.WrongLeader = true
		return
	}
	op := Op{
		Args:     args,
		Opcode:   Optype_leave,
		ClientId: args.ClientId,
	}
	DPrintf("[ShardMaster %d] Leave [args=%+v] sent", sm.me, args)
	sm.mu.Lock()
	logindex, _, _ := sm.rf.Start(op)
	if _, ok := sm.returnChanMap[logindex]; ok {
		DPrintf("[ShardMaster %d FATAL ERROR] Leave returnChanMap already full", sm.me)
		sm.mu.Unlock()
		return
	}
	returnChan := make(chan ReturnChanData)
	sm.returnChanMap[logindex] = returnChan
	sm.mu.Unlock()
	<-returnChan
	DPrintf("[ShardMaster] Leave get resp from ReturnChan")
	return
}

func (sm *ShardMaster) leave_exec(inputArgs interface{}) {
	lastConfig := sm.configs[len(sm.configs)-1]
	newGroup := make(map[int][]string)
	for gid, serverNames := range lastConfig.Groups {
		newGroup[gid] = serverNames
	}
	args := inputArgs.(*LeaveArgs)
	extraGroupNum := 0
	unallocated := make([]int, 0)
	for _, gid := range args.GIDs {
		extraGroupNum++
		delete(newGroup, gid)
		unallocated = append(unallocated, sm.gidShardMap[gid]...)
		delete(sm.gidShardMap, gid)
	}
	sm.groupNum -= extraGroupNum
	balance(sm.gidShardMap, unallocated)
	var newShards [NShards]int
	for gid, shards := range sm.gidShardMap {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newConfig := Config{
		Num:    sm.groupNum,
		Shards: newShards,
		Groups: newGroup,
	}
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	ok := sm.rf.IsLeader()
	if !ok {
		reply.WrongLeader = true
		return
	}
	op := Op{
		Args:     args,
		Opcode:   Optype_move,
		ClientId: args.ClientId,
	}
	DPrintf("[ShardMaster %d] Move [args=%+v] sent", sm.me, args)
	sm.mu.Lock()
	logindex, _, _ := sm.rf.Start(op)
	if _, ok := sm.returnChanMap[logindex]; ok {
		DPrintf("[ShardMaster %d FATAL ERROR] Move returnChanMap already full", sm.me)
		sm.mu.Unlock()
		return
	}
	returnChan := make(chan ReturnChanData)
	sm.returnChanMap[logindex] = returnChan
	sm.mu.Unlock()
	<-returnChan
	DPrintf("[ShardMaster] Move get resp from ReturnChan")
	return
}

func (sm *ShardMaster) move_exec(inputArgs interface{}) {
	lastConfig := sm.configs[len(sm.configs)-1]
	args := inputArgs.(*MoveArgs)
	needMove := false
	for gid, shards := range sm.gidShardMap {
		for i, shard := range shards {
			if shard == args.Shard && gid != args.GID {
				needMove = true
				sm.gidShardMap[gid] = append(shards[:i], shards[i+1:]...)
			}
		}
	}
	if needMove {
		sm.gidShardMap[args.GID] = append(sm.gidShardMap[args.GID], args.Shard)
	}
	var newShards [NShards]int
	for gid, shards := range sm.gidShardMap {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newConfig := Config{
		Num:    sm.groupNum,
		Shards: newShards,
		Groups: lastConfig.Groups,
	}
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	ok := sm.rf.IsLeader()
	if !ok {
		reply.WrongLeader = true
		return
	}
	op := Op{
		Args:     args,
		Opcode:   Optype_query,
		ClientId: args.ClientId,
	}
	DPrintf("[ShardMaster %d] Query [args=%+v] sent", sm.me, args)
	sm.mu.Lock()
	logindex, _, _ := sm.rf.Start(op)
	if _, ok := sm.returnChanMap[logindex]; ok {
		DPrintf("[ShardMaster %d FATAL ERROR] Query returnChanMap already full", sm.me)
		sm.mu.Unlock()
		return
	}
	returnChan := make(chan ReturnChanData)
	sm.returnChanMap[logindex] = returnChan
	sm.mu.Unlock()
	data := <-returnChan
	reply.Config = *data.Config
	DPrintf("[ShardMaster] Query get resp from ReturnChan")
	return
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sm.dead, 1)
}

func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) apply() {
	for m := range sm.applyCh {
		op := m.Command.(Op)
		clientId := op.ClientId
		sm.mu.Lock()
		switch op.Opcode {
		case Optype_join:
			oldLogIndex := 0
			oldLogIndex, _ = sm.clientLogindexMap[clientId]
			if m.CommandIndex > oldLogIndex {
				// todo do
				sm.join_exec(op.Args)
				sm.clientLogindexMap[clientId] = m.CommandIndex
			}
			if m.IsLeader {
				ch, ok := sm.returnChanMap[m.CommandIndex]
				if ok {
					select {
					case ch <- ReturnChanData{Ok: true}:
					default:
					}
				}
				delete(sm.returnChanMap, m.CommandIndex)
			}
		case Optype_leave:
			oldLogIndex := 0
			oldLogIndex, _ = sm.clientLogindexMap[clientId]
			if m.CommandIndex > oldLogIndex {
				// todo do
				sm.leave_exec(op.Args)
				sm.clientLogindexMap[clientId] = m.CommandIndex
			}
			if m.IsLeader {
				ch, ok := sm.returnChanMap[m.CommandIndex]
				if ok {
					select {
					case ch <- ReturnChanData{Ok: true}:
					default:
					}
				}
				delete(sm.returnChanMap, m.CommandIndex)
			}
		case Optype_move:
			oldLogIndex := 0
			oldLogIndex, _ = sm.clientLogindexMap[clientId]
			if m.CommandIndex > oldLogIndex {
				// todo do
				sm.move_exec(op.Args)
				sm.clientLogindexMap[clientId] = m.CommandIndex
			}
			if m.IsLeader {
				ch, ok := sm.returnChanMap[m.CommandIndex]
				if ok {
					select {
					case ch <- ReturnChanData{Ok: true}:
					default:
					}
				}
				delete(sm.returnChanMap, m.CommandIndex)
			}
		case Optype_query:
			//oldLogIndex := 0
			//oldLogIndex, _ = sm.clientLogindexMap[clientId]
			//if m.CommandIndex > oldLogIndex {
			//	// todo do
			//	sm.join_exec(op.Args)
			//	sm.clientLogindexMap[clientId] = m.CommandIndex
			//}
			args := op.Args.(*QueryArgs)
			var config *Config
			if args.Num < 0 || args.Num >= len(sm.configs) {
				config = &(sm.configs[len(sm.configs)-1])
			} else {
				config = &(sm.configs[args.Num])
			}
			if m.IsLeader {
				ch, ok := sm.returnChanMap[m.CommandIndex]
				if ok {
					select {
					case ch <- ReturnChanData{Ok: true, Config: config}:
					default:
					}
				}
				delete(sm.returnChanMap, m.CommandIndex)
			}
		}

		sm.mu.Unlock()
		if sm.killed() {
			break
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.returnChanMap = make(map[int]chan ReturnChanData)
	sm.clientLogindexMap = make(map[int64]int)

	go sm.apply()
	return sm
}
