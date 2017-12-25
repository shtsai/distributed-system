package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

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
	Key	   string
	Value	   string
	Type	   string  // operation type "Put", "Append", or "Get"
	ID	   int64	
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	seq	   int  // sequence number used so far
	keyValue   map[string]string
	records    map[int64]*Record
}

type Record struct {
	Result	   string
	Err	   Err
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.

	kv.mu.Lock()	

	delete(kv.records, args.Prev)

	if record, ok := kv.records[args.ID]; ok {
	   // we have a record associate with this ID
	   reply.Err = record.Err
	   reply.Value = record.Result
	   
	   kv.px.Done(kv.seq)

	   kv.mu.Unlock()
	   DPrintf("server %d found an ID we have seen before, Get %s",kv.me,args.Key)
	   return nil
	}

	v := Op{args.Key, "", "Get", args.ID}
	var result string

	seqOK := false

	for !seqOK {
	    kv.seq++
	    DPrintf("this is current seq: %d\n", kv.seq)
	    
  	    status, value := kv.px.Status(kv.seq)

	    if status == paxos.Decided {
	       value = value.(Op)

	       if _, ok := kv.records[value.(Op).ID]; ok {
	       	  DPrintf("server %d found an ID we seen before,%s %s",kv.me,value.(Op).Type,value.(Op).Value)
	       	  if value.(Op).ID == v.ID {
		     seqOK = true
		     kv.px.Done(kv.seq)
		  }
	       	  continue
	       } 

	    } else {
	       kv.px.Start(kv.seq, v)
	       value = kv.WaitDecided(kv.seq)
	       DPrintf("paxos have decided on %s, Key=%s,Value=%s,seq=%d\n",value.(Op).Type,value.(Op).Key,value.(Op).Value,kv.seq)
	    }

	    if value.(Op).ID == v.ID {
	       seqOK = true
	    }

	    _, result = kv.PerformOp(value.(Op))
	    	    
	    DPrintf("server %d performed %s operation on key %s, result is %s,seq=%d\n",kv.me, value.(Op).Type,value.(Op).Key,result,kv.seq)
//	    fmt.Printf("This is server %d current key value store:\n",kv.me)
//	    fmt.Println(kv.keyValue)	    

	    // add this record
	    if _, ok:= kv.records[value.(Op).ID]; !ok {
	       DPrintf("server %d is adding Record with ID %d", kv.me,value.(Op).ID)
	       newRecord := Record{result,OK}
	       kv.records[value.(Op).ID] = &newRecord
	    }

	    kv.px.Done(kv.seq)
	}

	DPrintf("This is the value return to Get:%s\n",result)
	reply.Value = result
	reply.Err = OK	
	reply.ID = args.ID

	kv.mu.Unlock()
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	
	kv.mu.Lock()

	delete(kv.records, args.Prev)

	if record, ok := kv.records[args.ID]; ok {
	   // we have a record associate with this ID
	   reply.Err = record.Err
	   kv.px.Done(kv.seq)	   

	   kv.mu.Unlock()
	   DPrintf("server %d found an ID we have seen before, Op=%s key=%s value=%s\n",kv.me,args.Op,args.Key,args.Value)
	   return nil
	}

	v := Op{args.Key, args.Value, args.Op, args.ID}
	var result string

	seqOK := false   
	
	for !seqOK {
	    kv.seq++
	    DPrintf("this is current seq: %d\n", kv.seq)

	    status, value := kv.px.Status(kv.seq)

	    if status == paxos.Decided {
	       value = value.(Op)
	       
	       if _,ok := kv.records[value.(Op).ID]; ok {

	          DPrintf("server %d found an ID we have seen before, %s %s",kv.me,value.(Op).Type,value.(Op).Value)
	       	  if value.(Op).ID == v.ID {
		     seqOK = true
		     kv.px.Done(kv.seq)
		  }
		  continue
	       }

	    } else {
	       kv.px.Start(kv.seq, v)	
	       value = kv.WaitDecided(kv.seq)
	    }

	    if value.(Op).ID == v.ID {
	       seqOK = true
	    }
	    _, result = kv.PerformOp(value.(Op))
	    
	    DPrintf("server %d performed %s operation on key %s, result is %s, seq=%d\n",kv.me,value.(Op).Type,value.(Op).Key,result, kv.seq)
//            fmt.Printf("This is server %d current key value store:\n",kv.me)
//            fmt.Println(kv.keyValue)   

	    // add this record
	    if _, ok :=kv.records[value.(Op).ID]; !ok {
	       DPrintf("server %d is adding Record with ID %d",kv.me, value.(Op).ID)
	       newRecord := Record{result, OK}
	       kv.records[value.(Op).ID] = &newRecord
	    }
	    
            kv.px.Done(kv.seq)
	}

	reply.Err = OK
	reply.ID = args.ID

	kv.mu.Unlock()
	return nil
}

func (kv *KVPaxos) WaitDecided(seq int) Op {
     	to := 10 * time.Millisecond
	for {
	    status, op:= kv.px.Status(seq)
	    if status == paxos.Decided{
	       DPrintf("paxos have decided on %s %s\n",op.(Op).Type, op.(Op).Value)
	       return op.(Op)
	    }
	    time.Sleep(to)
	    if to < 10 * time.Second {
	       to *= 2
	    }
	}
}

func (kv *KVPaxos) PerformOp(op Op) (bool, string) {
        
	switch op.Type {
	    case "Get":
	    	 value, ok := kv.keyValue[op.Key]
		 if ok {
		    return true, value
		 } 
	    case "Put":
	    	 kv.keyValue[op.Key] = op.Value
		 return true, op.Value
            case "Append":
	    	 value, ok := kv.keyValue[op.Key]
		 if ok {
		    value += op.Value
		    kv.keyValue[op.Key] = value
		    return true, value
		 } else {
		    kv.keyValue[op.Key] = op.Value
		    return true, op.Value
		 }
	}
	return false, ""
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.seq = 0
	kv.keyValue = make(map[string]string)
	kv.records = make(map[int64]*Record)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l


	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
