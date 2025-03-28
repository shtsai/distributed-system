package kvpaxos

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
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
	ID    int64
	Prev  int64   // ID of the last reply received
}

type PutAppendReply struct {
	Err   Err
	ID    int64
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ID     int64
	Prev   int64
}

type GetReply struct {
	Err   Err
	Value string
	ID    int64
}
