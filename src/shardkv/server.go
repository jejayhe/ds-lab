package shardkv

import (
	"../shardmaster"
	"log"
	"sync/atomic"
	"time"
)
import "../labrpc"
import "../raft"
import "sync"
import "../labgob"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	K      string
	V      string
	Opcode Optype

	ClientId  int64
	ClientSeq int

	SmConfig            *shardmaster.Config
	MigrateDict         map[string]string
	MigrateClientSeqMap map[int64]int
	MigrateShards       []int
}
type Optype int8

const (
	Optype_Get Optype = iota
	Optype_Put
	Optype_Append
	Optype_NewconfigStart
	Optype_NewconfigMigrate
)

type ReturnChanData struct {
	Ok bool
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck          *shardmaster.Clerk
	dead         int32 // set by Kill()
	smConfig     *shardmaster.Config
	lastSmConfig *shardmaster.Config
	acceptShards map[int]bool
	taskShards   []int
	dict         map[string]string
	clientSeqMap map[int64]int
	skvs         map[int][]*labrpc.ClientEnd

	returnChanMap        map[int]chan ReturnChanData
	reconfigureInProcess bool
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
func (kv *ShardKV) MigrateShards(args MigrateShardsArgs, reply MigrateShardsReply) {
	kv.mu.Lock()
	if args.ConfigNum > kv.smConfig.Num {
		DPrintf("[ShardKV] MigrateShards not ready on gid %", kv.gid)
		kv.mu.Unlock()
		reply.Err = "[ShardKV] MigrateShards not ready"
		return
	}
	//for _, ms:=range args.MigrateShards{
	//	if kv.lastSmConfig.Shards[ms]!=kv.gid{
	//		DPrintf("[ShardKV] MigrateShards not match on gid %d", kv.gid)
	//		reply.Err = "[ShardKV] MigrateShards not match on server %d"
	//		kv.mu.Unlock()
	//		return
	//	}
	//}

	// fetch kv
	askedShardMap := make(map[int]bool)
	for _, shard := range args.MigrateShards {
		askedShardMap[shard] = true
	}
	migrateDict := make(map[string]string)
	for k, v := range kv.dict {
		if _, ok := askedShardMap[key2shard(k)]; ok {
			migrateDict[k] = v
		}
	}

	// fetch clientId seq
	clientSeqMap := make(map[int64]int)
	for k, v := range kv.clientSeqMap {
		clientSeqMap[k] = v
	}
	kv.mu.Unlock()
	reply.MigrateDict = migrateDict
	reply.MigrateClientSeqMap = clientSeqMap
	return
}
func (kv *ShardKV) fetchShards(gid int, shards []int, doneCh chan bool) {
	if gid == 0 {
		doneCh <- true
		return
	}
	kv.mu.Lock()
	args := &MigrateShardsArgs{
		MigrateShards: shards,
		ConfigNum:     kv.smConfig.Num,
	}
	for _, srv := range kv.skvs[gid] {
		kv.mu.Unlock()
		var reply MigrateShardsReply
		ok := srv.Call("ShardKV.MigrateShards", args, &reply)
		kv.mu.Lock()
		if ok && reply.WrongLeader == false && reply.Err == "" {
			op := Op{
				Opcode:              Optype_NewconfigMigrate,
				MigrateDict:         reply.MigrateDict,
				MigrateClientSeqMap: reply.MigrateClientSeqMap,
				MigrateShards:       shards,
			}
			kv.rf.Start(op)
			select {
			case doneCh <- true:
			default:
			}
			return
		}
	}

}
func (kv *ShardKV) reconfigure() {
	// for leader while taskShards is not empty
	for !kv.killed() {
		for kv.rf.IsLeader() {
			kv.mu.Lock()
			if len(kv.taskShards) != 0 {
				// take a shard, find
				taskGroup := make(map[int][]int) // map[gid] shards
				for _, shard := range kv.taskShards {
					gid := kv.smConfig.Shards[shard]
					taskGroup[gid] = append(taskGroup[gid], shard)
				}
				kv.mu.Unlock()
				jobNum := 0
				for _, _ = range taskGroup {
					jobNum++
				}
				doneCh := make(chan bool, jobNum)
				for gid, shards := range taskGroup {
					go kv.fetchShards(gid, shards, doneCh)
				}
				doneAcc := 0
				for {
					select {
					case <-doneCh:
						doneAcc++
						if doneAcc == jobNum {
							break
						}
					case <-time.After(300 * time.Millisecond):
						DPrintf("[ShardKV Migration] reconfigure migration timeout")
						break
					}
				}
			} else {
				kv.mu.Unlock()
			}
			time.Sleep(100 * time.Millisecond)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) reconfigureStart() {
	// accept shard that are in both oldShards and newShards
	acceptShards := make(map[int]bool)
	// taskShards are for fetching shard that are in newShards and not in oldShards
	taskShards := make([]int, 0)

	oldShards := make(map[int]bool)
	for shard, gid := range kv.lastSmConfig.Shards {
		if gid == kv.gid {
			oldShards[shard] = true
		}
	}
	newShards := make(map[int]bool)
	for shard, gid := range kv.smConfig.Shards {
		if gid == kv.gid {
			newShards[shard] = true
			if _, ok := oldShards[shard]; ok {
				acceptShards[shard] = true
			} else {
				taskShards = append(taskShards, shard)
			}
		}
	}
	kv.acceptShards = acceptShards
	kv.taskShards = taskShards
}
func (kv *ShardKV) migrationApplyDuplicatePositive(shards []int) bool {
	// test if shards are in taskShards
	taskShardMap := make(map[int]bool)
	for _, shard := range kv.taskShards {
		taskShardMap[shard] = true
	}
	for _, shard := range shards {
		if _, ok := taskShardMap[shard]; !ok {
			DPrintf("[ShardKV] migrationApplyDuplicateDetection detect DUPLICATE on gid %d", kv.gid)
			return true
		}
	}
	return false
}

func (kv *ShardKV) apply() {
	for m := range kv.applyCh {
		op := m.Command.(Op)
		clientId := op.ClientId
		clientSeq := op.ClientSeq
		kv.mu.Lock()
		switch op.Opcode {
		case Optype_NewconfigStart:
			if kv.smConfig.Num < op.SmConfig.Num {
				kv.smConfig = op.SmConfig
				// refuse some shards, add some shards to tasklist.
				kv.reconfigureStart()
				kv.reconfigureInProcess = true
			}
			//if m.IsLeader {
			//	ch, ok := kv.returnChanMap[m.CommandIndex]
			//	if ok {
			//		select {
			//		case ch <- ReturnChanData{Ok: true}:
			//		default:
			//		}
			//	}
			//	delete(kv.returnChanMap, m.CommandIndex)
			//}
		case Optype_NewconfigMigrate:
			if !kv.migrationApplyDuplicatePositive(op.MigrateShards) {
				for k, v := range op.MigrateDict {
					kv.dict[k] = v
				}
				for k, v := range op.MigrateClientSeqMap {
					oldv, _ := kv.clientSeqMap[k]
					if v > oldv {
						kv.clientSeqMap[k] = v
					}
				}
				for _, shard := range op.MigrateShards {
					kv.acceptShards[shard] = true
				}
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) pollShardmasterConfig() {
	for !kv.killed() {
		for !kv.killed() && kv.rf.IsLeader() && !kv.reconfigureInProcess {
			newconfig := kv.mck.Query(-1)
			//changed := false
			//for i := 0; i < len(oldconfig.Shards); i++ {
			//	if oldconfig.Shards[i] != newconfig.Shards[i] {
			//		changed = true
			//		break
			//	}
			//}
			kv.mu.Lock()
			oldConfigNum := kv.smConfig.Num
			kv.mu.Unlock()
			if newconfig.Num != oldConfigNum {
				// todo do something
				op := Op{
					Opcode:   Optype_NewconfigStart,
					SmConfig: &newconfig,
				}
				kv.rf.Start(op)
				//oldconfig = newconfig
			}
			time.Sleep(100 * time.Millisecond)
		}
		time.Sleep(100 * time.Millisecond)
	}

}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	var shards [shardmaster.NShards]int
	kv.smConfig = &shardmaster.Config{
		Num:    0,
		Shards: shards,
		Groups: make(map[int][]string),
	}
	kv.lastSmConfig = kv.smConfig
	go kv.apply()
	return kv
}
