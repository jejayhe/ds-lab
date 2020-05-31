package kvraft

import (
	"../labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	i        int // server chosen
	mu       sync.Mutex
	name     string // client name
	sequence int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	//ck.mu.Lock()
	ck.i = int(nrand()) % len(servers)
	ck.name = "____CLIENT____" + randstring(20)
	//name := ck.name
	ck.sequence = 0
	//ck.mu.Unlock()

	//val := ck.Get(name)
	//
	////ck.mu.Lock()
	//if val == "" {
	//	ck.sequence = 0
	//} else {
	//	ck.sequence, _ = strconv.ParseInt(val, 10, 64)
	//}
	//ck.mu.Unlock()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	//ck.mu.Lock()
	//if ck.sequence == -1{
	//
	//}

	ck.mu.Lock()
	// todo
	ck.sequence++
	sequence := ck.sequence
	clientName := ck.name
	serveri := ck.i
	ck.mu.Unlock()
	for {
		args := &GetArgs{
			Key:        key,
			ClientName: clientName,
			Sequence:   sequence,
		}
		reply := &GetReply{}
		ok := ck.servers[serveri].Call("KVServer.Get", args, reply)
		if !reply.IsLeader || !ok || reply.Err != "" {
			if reply.Err != "" {
				DPrintf("[DEBUG] Clerk.Get err:[%s]", reply.Err)
			}
			// try another server
			ck.mu.Lock()
			ck.i = int(nrand()) % len(ck.servers)
			serveri = ck.i
			ck.mu.Unlock()
			time.Sleep(30 * time.Millisecond)
			continue
		} else {
			DPrintf("[CLERK] Get [K:%s] returns [%s]", key, reply.Value)
			return reply.Value
			//break
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	serveri := ck.i
	ck.sequence++
	sequence := ck.sequence
	clientName := ck.name
	ck.mu.Unlock()
	for {
		args := &PutAppendArgs{
			Key:        key,
			Value:      value,
			Op:         op,
			ClientName: clientName,
			Sequence:   sequence,
		}
		reply := &PutAppendReply{}
		DPrintf("[CLERK] calling KVServer %d.PutAppend [K=%s] [V=%s] [Op=%s] ", serveri, key, value, op)
		ok := ck.servers[serveri].Call("KVServer.PutAppend", args, reply)
		if !reply.IsLeader || !ok || reply.Err != "" {
			if reply.Err != "" {
				DPrintf("[DEBUG] Clerk.PutAppend err:[%s]", reply.Err)
			}
			DPrintf("[CLERK]  KVServer.PutAppend sent to nonleader")
			// try another server
			ck.mu.Lock()
			ck.i = int(nrand()) % len(ck.servers)
			serveri = ck.i
			ck.mu.Unlock()
			time.Sleep(30 * time.Millisecond)
			continue
		} else {
			DPrintf("[CLERK]  KVServer.PutAppend return")
			break
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
