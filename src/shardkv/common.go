package shardkv

import "shardmaster"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrNotReady   = "ErrNotReady"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ID    int64
	Prev  int64
}

type PutAppendReply struct {
	Err Err
	Done int64
}

type GetArgs struct {
	Key    string
	// You'll have to add definitions here.
	ID     int64
	Prev   int64
}

type GetReply struct {
	Err   Err
	Value string
	Done  int64
}

type GetShardArgs struct {
     	ShardNum  int
	ConfigNum int
	OldConfig shardmaster.Config
}

type GetShardReply struct {
        KeyValue  map[string]string
	Err       Err
	Records	  map[int64]*Record
}

type PerformOpReply struct {
        Value     string
	KeyValue  map[string]string
	Err	  Err
}
