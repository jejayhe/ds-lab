package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongGroup   = "ErrWrongGroup"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrDataNotReady = "ErrDataNotReady"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	ClientSeq int
}

type PutAppendReply struct {
	Err         Err
	WrongLeader bool
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	ClientSeq int
}

type GetReply struct {
	Err         Err
	Value       string
	WrongLeader bool
}

type MigrateShardsArgs struct {
	MigrateShards []int
	ConfigNum     int
}
type MigrateShardsReply struct {
	MigrateDict         map[string]string
	MigrateClientSeqMap map[int64]int
	Err                 Err
	WrongLeader         bool
}
