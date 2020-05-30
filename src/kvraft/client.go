package kvraft

import (
	"../labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	i  int // server chosen
	mu sync.Mutex
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
	ck.mu.Lock()
	ck.i = int(nrand()) % len(servers)

	ck.mu.Unlock()
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
	ck.mu.Lock()
	serveri := ck.i
	ck.mu.Unlock()
	for {
		args := &GetArgs{
			Key: key,
		}
		reply := &GetReply{}
		ck.servers[serveri].Call("KVServer.Get", args, reply)
		if reply.NextServerId != -1 {
			// try another server
			ck.mu.Lock()
			ck.i = reply.NextServerId
			ck.mu.Unlock()
			continue
		} else {
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
	ck.mu.Unlock()
	for {
		args := &PutAppendArgs{
			Key:   key,
			Value: value,
			Op:    op,
		}
		reply := &PutAppendReply{}
		ck.servers[serveri].Call("KVServer.PutAppend", args, reply)
		if reply.NextServerId != -1 {
			// try another server
			ck.mu.Lock()
			ck.i = reply.NextServerId
			ck.mu.Unlock()
			continue
		} else {

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
