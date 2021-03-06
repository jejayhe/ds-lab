package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientName string
	Sequence   int64
}

type PutAppendReply struct {
	Err      Err
	IsLeader bool // if false means need to query another server
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientName string
	Sequence   int64
}

type GetReply struct {
	Err      Err
	Value    string
	IsLeader bool
}

type OpType int

const (
	OpType_Get OpType = iota
	OpType_Put
	OpType_Append
)

var OpTypeDict = map[string]OpType{
	"Get":    OpType_Get,
	"Put":    OpType_Put,
	"Append": OpType_Append,
}

//type Cmd struct {
//	K  string
//	V  string
//	Op OpType
//}
type ReturnVal struct {
	V  string
	Ok bool
}
