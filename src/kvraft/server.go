package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"sync"

	//"github.com/sasha-s/go-deadlock"
	"log"
	"strconv"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Cmd struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	//Key string
	//Value string
	//Operation OpType
	K          string
	V          string
	Op         OpType
	ReturnChan chan ReturnVal // lab3 communicate with rpc handler
	ClientName string
	Sequence   int64
}

//func (op *Op) Get() {
//
//}

type KVServer struct {
	mu sync.Mutex
	//mu      deadlock.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dict map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	ok := kv.rf.IsLeader()
	if !ok {
		reply.IsLeader = false
		return
	}
	reply.IsLeader = true

	returnChan := make(chan ReturnVal)
	cmd := Cmd{
		K:          args.Key,
		Op:         OpType_Get,
		ReturnChan: returnChan,
		ClientName: args.ClientName,
		Sequence:   args.Sequence,
	}
	DPrintf("[KVSERVER %d] Get [K=%s] sent", kv.me, args.Key)
	kv.rf.Start(cmd)
	select {
	case rv := <-cmd.ReturnChan:
		if rv.Ok {
			reply.Value = rv.V
			return
		} else {
			reply.Err = "KVServer.Get rv error"
			return
		}
	case <-time.After(300 * time.Millisecond):
		reply.Err = "KVServer.Get timeout"
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("[KVSERVER %d] PutAppend [K=%s] [V=%s] [Op=%s] start serving", kv.me, args.Key, args.Value, args.Op)
	// Your code here.
	ok := kv.rf.IsLeader()
	if !ok {
		reply.IsLeader = false
		return
	}
	reply.IsLeader = true
	returnChan := make(chan ReturnVal)
	cmd := Cmd{
		K:          args.Key,
		V:          args.Value,
		Op:         OpTypeDict[args.Op],
		ReturnChan: returnChan,
		ClientName: args.ClientName,
		Sequence:   args.Sequence,
	}
	DPrintf("[KVSERVER %d] PutAppend [K=%s] [V=%s] [Op=%s] sent", kv.me, args.Key, args.Value, args.Op)
	kv.rf.Start(cmd)
	DPrintf("[KVSERVER %d] PutAppend waiting...", kv.me)
	select {
	case rv := <-cmd.ReturnChan:
		if rv.Ok {
			return
		} else {
			reply.Err = "KVServer.PutAppend rc error"
			DPrintf("[KVSERVER] PutAppend get resp from ReturnChan")
			return
		}
	case <-time.After(300 * time.Millisecond):
		reply.Err = "KVServer.PutAppend timeout"
		DPrintf("[KVSERVER] PutAppend get resp timeout")
		return
	}
}

/*
	if rf is the leader, return 0, ok
	if rf is not the leader, return a server to ask for
*/
//func (kv *KVServer) isLeader() (int, bool) {
//	leaderId, err := kv.rf.GetLeader()
//	if err == nil {
//		if leaderId == kv.me {
//			return 0, true
//		} else {
//			return leaderId, true
//		}
//	} else {
//		return rand.Intn(1e4) %
//	}
//}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) apply() {
	for m := range kv.applyCh {
		if m.Snapshot != nil {
			r := bytes.NewBuffer(m.Snapshot)
			d := labgob.NewDecoder(r)
			dict := make(map[string]string)
			if d.Decode(&dict) != nil {
				DPrintf("[ERROR] KVServer.apply m.Snapshot")
			} else {
				kv.mu.Lock()
				kv.dict = dict
				DPrintf("[SNAPSHOT] KVServer.apply server %d successfully apply", kv.me)
				DPrintf("[KVSERVER %d] data:[%v]", kv.me, kv.dict)
				kv.mu.Unlock()
			}
			continue
		}
		cmd := (m.Command).(Cmd)
		clientName := cmd.ClientName
		sequence := cmd.Sequence
		kv.mu.Lock()
		switch cmd.Op {
		case OpType_Get:
			v, _ := kv.dict[cmd.K]
			if m.IsLeader {
				select {
				case cmd.ReturnChan <- ReturnVal{V: v, Ok: true}:
				default:
				}
			}

		case OpType_Put:
			/*
				if seq already exists, return ok, do no op.
				else update seq.
			*/
			seqstr, ok := kv.dict[clientName]
			var seq int64 = 0
			if ok {
				seq, _ = strconv.ParseInt(seqstr, 10, 64)
			}
			if sequence <= seq {
				if m.IsLeader {
					select {
					case cmd.ReturnChan <- ReturnVal{V: "", Ok: true}:
					default:
					}
				}
			} else {
				kv.dict[clientName] = strconv.FormatInt(sequence, 10)
				kv.dict[cmd.K] = cmd.V
				if m.IsLeader {
					select {
					case cmd.ReturnChan <- ReturnVal{V: "", Ok: true}:
					default:
					}
				}
			}

		case OpType_Append:
			/*
				if seq already exists, return ok, do no op.
				else update seq.
			*/
			seqstr, ok := kv.dict[clientName]
			var seq int64 = 0
			if ok {
				seq, _ = strconv.ParseInt(seqstr, 10, 64)
			}
			if sequence <= seq {
				if m.IsLeader {
					select {
					case cmd.ReturnChan <- ReturnVal{V: "", Ok: true}:
					default:
					}
				}
			} else {
				kv.dict[clientName] = strconv.FormatInt(sequence, 10)

				oldv, ok := kv.dict[cmd.K]
				if ok {
					kv.dict[cmd.K] = oldv + cmd.V
				} else {
					kv.dict[cmd.K] = cmd.V
				}
				if m.IsLeader {
					select {
					case cmd.ReturnChan <- ReturnVal{V: "", Ok: true}:
					default:
					}
				}
			}
		}
		DPrintf("[KVSERVER %d] data:[%v]", kv.me, kv.dict)
		kv.mu.Unlock()
		kv.rf.UpdateLastApplied(m.CommandIndex)
		if kv.killed() {
			break
		}
		//kv.mu.Lock()
		//maxraftstate:= kv.maxraftstate
		//kv.mu.Unlock()
		// check log too large??
		if kv.rf.TimeForSnapshot(kv.maxraftstate) {
			DPrintf("[SNAPSHOT] kvserver take snapshot ...")
			// take snapshot
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)

			kv.mu.Lock()
			e.Encode(kv.dict)
			kv.mu.Unlock()
			data := w.Bytes()
			kv.rf.TakeSnapshot(data, -1, -1)
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	// todo uncomment
	labgob.Register(Cmd{})

	kv := new(KVServer)
	kv.mu.Lock()
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.dict = make(map[string]string)
	kv.mu.Unlock()

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.restoreSnapShot()
	// You may need initialization code here.
	go kv.apply()
	return kv
}

func (kv *KVServer) restoreSnapShot() {
	//kv.testRestoreSnapShot()
	data := kv.rf.RestoreSnapshot()
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	dict := make(map[string]string)
	if d.Decode(&dict) != nil {
		DPrintf("[ERROR] KVServer.restoreSnapShot")
	} else {
		kv.mu.Lock()
		kv.dict = dict
		kv.mu.Unlock()
	}
}

type Item struct {
	K string
	V string
}

//func (kv *KVServer) testRestoreSnapShot() {
//	DPrintf("[TEST] KVServer.testRestoreSnapShot")
//	w := new(bytes.Buffer)
//	e := labgob.NewEncoder(w)
//
//	oldDict := []*Item{{K: "hello", V: "1"}, {K: "world", V: "2"}}
//	e.Encode(oldDict)
//	data := w.Bytes()
//	r := bytes.NewBuffer(data)
//	d := labgob.NewDecoder(r)
//	dict := make([]*Item, 0)
//	if d.Decode(&dict) != nil {
//		DPrintf("[ERROR] KVServer.testRestoreSnapShot")
//	} else {
//		DPrintf("[ERROR] success! dict = %v", dict)
//	}
//}

//func (kv *KVServer) testRestoreSnapShot() {
//	DPrintf("[TEST] KVServer.testRestoreSnapShot")
//	w := new(bytes.Buffer)
//	e := labgob.NewEncoder(w)
//
//	oldDict := map[string]string{"hello": "world"}
//	e.Encode(oldDict)
//	data := w.Bytes()
//	r := bytes.NewBuffer(data)
//	d := labgob.NewDecoder(r)
//	dict := make(map[string]string)
//	if d.Decode(&dict) != nil {
//		DPrintf("[ERROR] KVServer.testRestoreSnapShot")
//	} else {
//		DPrintf("[ERROR] success! dict = %v", dict)
//	}
//}
