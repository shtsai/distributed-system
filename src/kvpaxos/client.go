package kvpaxos

import "net/rpc"
import "crypto/rand"
import "math/big"
import "time"

import "fmt"

type Clerk struct {
	servers []string
	// You will have to modify this struct.
	prev    int64   //seq of the last reply ID received
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.prev = 0	

	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	
	args := &GetArgs{key, nrand(), ck.prev}
	var reply GetReply
	
	serverNum := int(nrand()) % len(ck.servers)
	
//	fmt.Printf("client sends Get() to server %d \n", serverNum)		

	ok := call(ck.servers[serverNum], "KVPaxos.Get", args, &reply)

	for !ok || reply.Err != OK {
//	    fmt.Printf("client fails to send Get to server %d \n", serverNum)	    

	    time.Sleep(10 * time.Millisecond)
	    serverNum = (serverNum + 1) % len(ck.servers)

//	    fmt.Printf("client retry send Get to server %d \n", serverNum)

	    ok = call(ck.servers[serverNum], "KVPaxos.Get", args, &reply)
	}

	// update prev
	ck.prev = reply.ID   
	
	return reply.Value
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{key, value, op, nrand(), ck.prev}
	var reply PutAppendReply

	serverNum := int(nrand()) % len(ck.servers)

//	fmt.Printf("client send %s %s on key %s to server $d \n",op,value,key,serverNum)	

	ok := call(ck.servers[serverNum], "KVPaxos.PutAppend", args, &reply)
	for !ok || reply.Err != OK {
//	    fmt.Printf("client fails to send %s to server %d \n", op,serverNum)	

	    time.Sleep(10 * time.Millisecond)
	    serverNum = (serverNum + 1) % len(ck.servers)

//	    fmt.Printf("client retry send %s to server %d \n ",op, serverNum)
	    ok = call(ck.servers[serverNum], "KVPaxos.PutAppend", args, &reply)
	     
	}

	ck.prev = reply.ID
	
	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
