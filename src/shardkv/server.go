package shardkv

import (
	"../shardmaster"
	"bytes"
	"github.com/sasha-s/go-deadlock"
	"log"
	"sync/atomic"
	"time"
)
import "../labrpc"
import "../raft"
import "../labgob"

const Debug = 1

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

var OptypeDict = map[string]Optype{
	"Get":    Optype_Get,
	"Put":    Optype_Put,
	"Append": Optype_Append,
}

type ReturnChanData struct {
	Ok bool
	V  string
}

type ShardKV struct {
	//mu           sync.Mutex
	mu           deadlock.Mutex
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
	taskShards   map[int]bool
	dict         map[string]string
	clientSeqMap map[int64]int
	skvs         map[int][]*labrpc.ClientEnd

	returnChanMap map[int]chan ReturnChanData
	//reconfigureInProcess bool
}

/*
	if not leader

	if shard is in taskShards, return "ErrDataNotReady"
	if shard is in acceptShards, continue return ok / "ErrNoKey"
	else return "ErrWrongGroup"
*/
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if !kv.rf.IsLeader() {
		reply.Err = ErrWrongLeader
		return
	}
	shard := key2shard(args.Key)
	kv.mu.Lock()
	if _, ok := kv.acceptShards[shard]; ok {
		goto ShardKV_Get_Continue
	} else if _, ok := kv.taskShards[shard]; ok {
		reply.Err = ErrDataNotReady
		kv.mu.Unlock()
		return
	} else {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
ShardKV_Get_Continue:
	op := Op{
		K:         args.Key,
		Opcode:    Optype_Get,
		ClientId:  args.ClientId,
		ClientSeq: args.ClientSeq,
	}
	DPrintf("[ShardKV %d] Get [K=%s] sent", kv.gid, args.Key)
	logindex, _, _ := kv.rf.Start(op)
	//kv.mu.Lock()
	if _, ok := kv.returnChanMap[logindex]; ok {
		DPrintf("[ShardKV %d FATAL ERROR] Get returnChanMap already full", kv.gid)
		kv.mu.Unlock()
		reply.Err = "[ShardKV %d FATAL ERROR] Get returnChanMap already full"
		return
	}
	returnChan := make(chan ReturnChanData)
	kv.returnChanMap[logindex] = returnChan
	kv.mu.Unlock()
	select {
	case data := <-returnChan:
		if data.Ok {
			reply.Err = OK
			reply.Value = data.V
			DPrintf("[ShardKV] Get get resp from ReturnChan")
		} else {
			reply.Err = ErrWrongGroup
			DPrintf("[ShardKV] Get get resp from ReturnChan ErrWrongGroup")
		}
	case <-time.After(300 * time.Millisecond):
		reply.Err = "[ShardKV] Get get resp timeout"
		DPrintf("[ShardKV] Get get resp timeout")
		kv.mu.Lock()
		delete(kv.returnChanMap, logindex)
		kv.mu.Unlock()
		return
	}
	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if !kv.rf.IsLeader() {
		reply.Err = ErrWrongLeader
		return
	}
	shard := key2shard(args.Key)
	kv.mu.Lock()
	if _, ok := kv.acceptShards[shard]; ok {
		goto ShardKV_Put_Continue
	} else if _, ok := kv.taskShards[shard]; ok {
		reply.Err = ErrDataNotReady
		kv.mu.Unlock()
		return
	} else {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
ShardKV_Put_Continue:
	op := Op{
		K:         args.Key,
		V:         args.Value,
		Opcode:    OptypeDict[args.Op],
		ClientId:  args.ClientId,
		ClientSeq: args.ClientSeq,
	}
	DPrintf("[ShardKV %d] PutAppend [K=%s] [V=%s] [Op=%s] sent", kv.gid, args.Key, args.Value, args.Op)
	logindex, _, _ := kv.rf.Start(op)
	//kv.mu.Lock()
	if _, ok := kv.returnChanMap[logindex]; ok {
		DPrintf("[ShardKV %d FATAL ERROR] PutAppend returnChanMap already full", kv.gid)
		kv.mu.Unlock()
		reply.Err = "[ShardKV %d FATAL ERROR] PutAppend returnChanMap already full"
		return
	}
	returnChan := make(chan ReturnChanData)
	kv.returnChanMap[logindex] = returnChan
	kv.mu.Unlock()
	select {
	case ret := <-returnChan:
		if ret.Ok {
			DPrintf("[ShardKV] PutAppend get resp from ReturnChan")
			reply.Err = OK
		} else {
			DPrintf("[ShardKV] PutAppend get resp from ReturnChan")
			reply.Err = ErrWrongGroup
		}

	case <-time.After(300 * time.Millisecond):
		reply.Err = "[ShardKV] PutAppend get resp timeout"
		DPrintf("[ShardKV] PutAppend get resp timeout")
		kv.mu.Lock()
		delete(kv.returnChanMap, logindex)
		kv.mu.Unlock()
		return
	}
	return
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
func (kv *ShardKV) MigrateShards(args *MigrateShardsArgs, reply *MigrateShardsReply) {
	kv.mu.Lock()
	if args.ConfigNum > kv.smConfig.Num {
		DPrintf("[ShardKV] MigrateShards not ready on gid %d", kv.gid)
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
	DPrintf("[DEBUG] [migrateDict:%+v] [kv.dict:%+v]", migrateDict, kv.dict)
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
		DPrintf("[FETCH SHARDS] gid=0 so returns")
		op := Op{
			Opcode:              Optype_NewconfigMigrate,
			MigrateDict:         make(map[string]string),
			MigrateClientSeqMap: make(map[int64]int),
			MigrateShards:       shards,
		}
		kv.rf.Start(op)
		select {
		case doneCh <- true:
		default:
		}
		return
	}
	kv.mu.Lock()
	args := &MigrateShardsArgs{
		MigrateShards: shards,
		ConfigNum:     kv.smConfig.Num,
	}
	groups := kv.lastSmConfig.Groups
	kv.mu.Unlock()
	if servers, ok := groups[gid]; ok {
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			DPrintf("[FETCH SHARDS] request sent with [args:%+v]", args)
			var reply MigrateShardsReply
			ok := srv.Call("ShardKV.MigrateShards", args, &reply)
			DPrintf("[FETCH SHARDS] resp [reply:%+v]", reply)
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
}
func (kv *ShardKV) reconfigure() {
	// for leader while taskShards is not empty
	for !kv.killed() {
		for kv.rf.IsLeader() {
			//DPrintf("[CHECK...gid:%d maybe locked]", kv.gid)
			kv.mu.Lock()
			//DPrintf("[CHECK...gid:%d taskshards len]", kv.gid)
			if len(kv.taskShards) != 0 {
				//DPrintf("[CHECK...gid:%d taskshards !=0]", kv.gid)
				// take a shard, find
				taskGroup := make(map[int][]int) // map[gid] shards
				for shard, _ := range kv.taskShards {
					gid := kv.lastSmConfig.Shards[shard]
					taskGroup[gid] = append(taskGroup[gid], shard)
				}
				kv.mu.Unlock()
				jobNum := 0
				for _, _ = range taskGroup {
					jobNum++
				}
				doneCh := make(chan bool, jobNum)
				for gid, shards := range taskGroup {
					DPrintf("[RECONFIGURE] [gid:%d]starting kv.fetchShards [gid:%d] [shards:%v]", kv.gid, gid, shards)
					go kv.fetchShards(gid, shards, doneCh)
				}
				doneAcc := 0
				timeout := time.After(300 * time.Millisecond)
			RECONFIG_FOR:
				for {
					select {
					case <-doneCh:
						doneAcc++
						if doneAcc >= jobNum {
							//kv.mu.Lock()
							//kv.reconfigureInProcess = false
							//kv.mu.Unlock()
							break RECONFIG_FOR
						}
					case <-timeout:
						DPrintf("[ShardKV Migration] reconfigure migration timeout on gid:%d", kv.gid)
						break RECONFIG_FOR
					}
				}
			} else {
				//kv.reconfigureInProcess = false
				kv.mu.Unlock()
			}
			//DPrintf("[DEBUG] code reached here for gid:%d", kv.gid)
			time.Sleep(100 * time.Millisecond)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) reconfigureStart() {
	// accept shard that are in both oldShards and newShards
	acceptShards := make(map[int]bool)
	// taskShards are for fetching shard that are in newShards and not in oldShards
	taskShards := make(map[int]bool)

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
				taskShards[shard] = true
			}
		}
	}
	kv.acceptShards = acceptShards
	kv.taskShards = taskShards
	DPrintf("[reconfigure start] gid:%d acceptShards:%+v", kv.gid, kv.acceptShards)
}
func (kv *ShardKV) migrationApplyDuplicatePositive(shards []int) bool {
	// test if shards are in taskShards
	//taskShardMap := make(map[int]bool)
	//for _, shard := range kv.taskShards {
	//	taskShardMap[shard] = true
	//}
	for _, shard := range shards {
		if _, ok := kv.taskShards[shard]; !ok {
			DPrintf("[ShardKV] migrationApplyDuplicateDetection detect DUPLICATE on gid %d", kv.gid)
			return true
		}
	}
	return false
}

/*
	if oldClientSeq<seq, renew seq and return true
	else return false
*/
func CheckClientSeq(seqMap map[int64]int, clientSeq int, clientId int64) bool {
	oldSeq, _ := seqMap[clientId]
	if clientSeq > oldSeq {
		seqMap[clientId] = clientSeq
		return true
	} else {
		return false
	}
}
func (kv *ShardKV) apply() {
	for m := range kv.applyCh {
		if m.Snapshot != nil {
			r := bytes.NewBuffer(m.Snapshot)
			d := labgob.NewDecoder(r)
			dict1 := make(map[string]string)
			dict2 := make(map[int64]int)
			var config1 shardmaster.Config
			var config2 shardmaster.Config
			acceptShards := make(map[int]bool)
			taskShards := make(map[int]bool)
			if d.Decode(&dict1) != nil ||
				d.Decode(&dict2) != nil ||
				d.Decode(&config1) != nil ||
				d.Decode(&config2) != nil ||
				d.Decode(&acceptShards) != nil ||
				d.Decode(&taskShards) != nil {
				DPrintf("[ERROR] ShardKV.apply m.Snapshot")
			} else {
				kv.mu.Lock()
				kv.dict = dict1
				kv.clientSeqMap = dict2
				kv.lastSmConfig = &config1
				kv.smConfig = &config2
				kv.acceptShards = acceptShards
				kv.taskShards = taskShards
				DPrintf("[SNAPSHOT] ShardKV.apply server %d successfully apply", kv.me)
				DPrintf("[ShardKV %d] dict:[%v] seqMap:[%v]config1:[%+v], config2:[%+v]", kv.me, kv.dict, kv.clientSeqMap, kv.lastSmConfig, kv.smConfig)
				kv.mu.Unlock()
			}
			continue
		}
		op := m.Command.(Op)
		clientId := op.ClientId
		clientSeq := op.ClientSeq
		kv.mu.Lock()
		switch op.Opcode {
		case Optype_NewconfigStart:
			if kv.smConfig.Num < op.SmConfig.Num {
				kv.lastSmConfig = kv.smConfig

				kv.smConfig = op.SmConfig
				// refuse some shards, add some shards to tasklist.

				kv.reconfigureStart()
				//kv.lastSmConfig = oldconfig
				//if kv.gid == 101 {
				//	DPrintf("[SHARDKV APPLY gid %d server %d confignum %d] applying new config !!! [taskShards:%v].......................", kv.gid, kv.me, kv.smConfig.Num, kv.taskShards)
				//
				//}
				//DPrintf("[SHARDKV APPLY gid %d server %d confignum %d] applying new config !!! [taskShards:%v].......................", kv.gid, kv.me, kv.smConfig.Num, kv.taskShards)
				if m.IsLeader {
					DPrintf("[SHARDKV APPLY gid %d server %d confignum %d] applying new config !!! [taskShards:%v]", kv.gid, kv.me, kv.smConfig.Num, kv.taskShards)
				}
				//kv.reconfigureInProcess = true
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
					delete(kv.taskShards, shard)
				}
				if m.IsLeader {
					DPrintf("[SHARDKV APPLY MIGRATION gid %d confignum %d] applying migration for [shards:%v] taskshards now:%v", kv.gid, kv.smConfig.Num, op.MigrateShards, kv.taskShards)
				}
			}
		case Optype_Get:
			shard := key2shard(op.K)
			if _, ok := kv.acceptShards[shard]; !ok {
				ch, ok := kv.returnChanMap[m.CommandIndex]
				if ok {
					select {
					case ch <- ReturnChanData{Ok: false, V: ""}:
					default:
					}
				}
				delete(kv.returnChanMap, m.CommandIndex)
			} else {
				v, _ := kv.dict[op.K]

				if m.IsLeader {
					//DPrintf("[DEBUG] gid:%d kv.dict:%+v", kv.gid, kv.dict)
					ch, ok := kv.returnChanMap[m.CommandIndex]
					if ok {
						select {
						case ch <- ReturnChanData{Ok: true, V: v}:
						default:
						}
					}
					delete(kv.returnChanMap, m.CommandIndex)
				}
			}

		case Optype_Put:
			shard := key2shard(op.K)
			if _, ok := kv.acceptShards[shard]; !ok {
				ch, ok := kv.returnChanMap[m.CommandIndex]
				if ok {
					select {
					case ch <- ReturnChanData{Ok: false}:
					default:
					}
				}
				delete(kv.returnChanMap, m.CommandIndex)
			} else {
				if CheckClientSeq(kv.clientSeqMap, clientSeq, clientId) {
					// todo do
					kv.dict[op.K] = op.V
				}
				if m.IsLeader {
					ch, ok := kv.returnChanMap[m.CommandIndex]
					if ok {
						select {
						case ch <- ReturnChanData{Ok: true}:
						default:
						}
					}
					delete(kv.returnChanMap, m.CommandIndex)
				}
			}

		case Optype_Append:
			shard := key2shard(op.K)
			if _, ok := kv.acceptShards[shard]; !ok {
				ch, ok := kv.returnChanMap[m.CommandIndex]
				if ok {
					select {
					case ch <- ReturnChanData{Ok: false}:
					default:
					}
				}
				delete(kv.returnChanMap, m.CommandIndex)
			} else {
				if CheckClientSeq(kv.clientSeqMap, clientSeq, clientId) {
					// todo do
					oldv, ok := kv.dict[op.K]
					if ok {
						kv.dict[op.K] = oldv + op.V
					} else {
						kv.dict[op.K] = op.V
					}
				}
				if m.IsLeader {
					ch, ok := kv.returnChanMap[m.CommandIndex]
					if ok {
						select {
						case ch <- ReturnChanData{Ok: true}:
						default:
						}
					}
					delete(kv.returnChanMap, m.CommandIndex)
				}
			}
		}
		kv.mu.Unlock()
		kv.rf.UpdateLastApplied(m.CommandIndex)
		if kv.killed() {
			break
		}
		if kv.rf.TimeForSnapshot(kv.maxraftstate) {
			DPrintf("[SNAPSHOT] shardkv take snapshot ...")
			// take snapshot
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)

			kv.mu.Lock()
			e.Encode(kv.dict)
			e.Encode(kv.clientSeqMap)
			e.Encode(kv.lastSmConfig)
			e.Encode(kv.smConfig)
			e.Encode(kv.acceptShards)
			e.Encode(kv.taskShards)
			kv.mu.Unlock()
			data := w.Bytes()
			kv.rf.TakeSnapshot(data, -1, -1)
		}
	}
}

func (kv *ShardKV) taskShardsIsEmpty() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return len(kv.taskShards) == 0
}

func (kv *ShardKV) pollShardmasterConfig() {
	for !kv.killed() {
		for !kv.killed() && kv.rf.IsLeader() && kv.taskShardsIsEmpty() {
			DPrintf("[SHARDKV POLL] checking config...")
			kv.mu.Lock()
			oldNum := kv.smConfig.Num
			kv.mu.Unlock()
			newconfig := kv.mck.Query(oldNum + 1)
			//changed := false
			//for i := 0; i < len(oldconfig.Shards); i++ {
			//	if oldconfig.Shards[i] != newconfig.Shards[i] {
			//		changed = true
			//		break
			//	}
			//}
			//kv.mu.Lock()
			//oldConfigNum := kv.smConfig.Num
			//kv.mu.Unlock()
			if newconfig.Num > oldNum {
				DPrintf("[SHARDKV POLL CHANGE gid:%d] found new config %d!!! oldNum:%d", kv.gid, newconfig.Num, oldNum)
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
	kv.returnChanMap = make(map[int]chan ReturnChanData)
	kv.clientSeqMap = make(map[int64]int)
	kv.dict = make(map[string]string)
	kv.restoreSnapShot()
	go kv.apply()
	go kv.pollShardmasterConfig()
	go kv.reconfigure()
	return kv
}

func (kv *ShardKV) restoreSnapShot() {
	DPrintf("[DEBUG] restoring from snapshot-----------------------")
	//kv.testRestoreSnapShot()
	data := kv.rf.RestoreSnapshot()
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	dict1 := make(map[string]string)
	dict2 := make(map[int64]int)
	var config1 shardmaster.Config
	var config2 shardmaster.Config
	acceptShards := make(map[int]bool)
	taskShards := make(map[int]bool)
	if d.Decode(&dict1) != nil ||
		d.Decode(&dict2) != nil ||
		d.Decode(&config1) != nil ||
		d.Decode(&config2) != nil ||
		d.Decode(&acceptShards) != nil ||
		d.Decode(&taskShards) != nil {
		DPrintf("[ERROR] ShardKV.restoreSnapShot")
	} else {
		kv.mu.Lock()
		kv.dict = dict1
		kv.clientSeqMap = dict2
		kv.lastSmConfig = &config1
		kv.smConfig = &config2
		kv.acceptShards = acceptShards
		kv.taskShards = taskShards
		//DPrintf("[DEBUG] restoring from snapshot------------------SUCCESS-----[kv.dict:%+v] [seqMap:%+v] [lastSmConfig:%+v] [smConfig:%+v]",
		//	kv.dict, kv.clientSeqMap, kv.lastSmConfig, kv.smConfig)
		DPrintf("[DEBUG] restoring from snapshot------------------SUCCESS-----[gid:%d] [lastSmConfig:%+v] [smConfig:%+v]",
			kv.gid, kv.lastSmConfig, kv.smConfig)
		kv.mu.Unlock()
	}
}
