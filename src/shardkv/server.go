package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format, a...)
		fmt.Println()
	}
	return
}


type Op struct {
	// Your definitions here.
	ID 	   int64
	Type	   string
	Key	   string
	Value	   string
	Config	   shardmaster.Config	
	ShardNum   int  // for getShard
	KV	   map[string]string // for reconfig
	Records	   map[int64]*Record // for reconfig
}


type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	config	   shardmaster.Config
	keyValue   map[string]string 
	seq	   int
	name       string
	records	   map[int64]*Record
}

type Record struct {
     	Result 	   string
	Err	   Err
}

func (kv *ShardKV) Log(op Op) int {

     seq := kv.seq
     
     for {

	 DPrintf("Current sequence = %d", seq)

	 status, value := kv.px.Status(seq)
	 DPrintf("seq=%d, Op=%v, status=%v",seq,value,status)

	 if status == paxos.Decided {
	    value = value.(Op)
	 } else {
	    kv.px.Start(seq, op)
	    value = kv.WaitDecided(seq)
	 }

	 if value.(Op).ID == op.ID {
	    break
	 }

	 seq++
     }

     return seq
}

func (kv *ShardKV) ReadLog(end int) PerformOpReply {

        DPrintf("%s reading log", kv.name)

        var result PerformOpReply

	for i := kv.seq; i <= end; i++ {
	    status, value := kv.px.Status(i)

	    DPrintf("seq=%d, Op=%v", i,value)	    

	    if status == paxos.Decided {
	       result = kv.PerformOp(value.(Op))
	       
//	       if result.Err == OK {
	       	  newRecord := Record{result.Value, result.Err}
	      	  kv.records[value.(Op).ID] = &newRecord
//	       }

	       DPrintf("Perform operation:%v, result=%v",value.(Op),result)
	       DPrintf("")
	    }
	}

	return result
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.

        kv.mu.Lock()
        defer kv.mu.Unlock()

	delete(kv.records, args.Prev)

        DPrintf("%s Received Get, Key=%s",kv.name,args.Key)
	DPrintf("ID=%v",args.ID)

	if record, ok := kv.records[args.ID]; ok && record.Err == OK {
	   // we have a record associate with this ID
	   reply.Err = record.Err
	   reply.Value = record.Result

	   DPrintf("%s found an ID we have seen before, Get %s",kv.name,args.Key)
	   DPrintf("result=%v, Err=%v", reply.Value,reply.Err)
	   return nil
	}

//	delete(kv.records, args.Prev)

        var v = Op{}
	v.ID = args.ID
        v.Type = "Get"
        v.Key = args.Key

	seq := kv.Log(v)
	result := kv.ReadLog(seq)
       
	DPrintf("inside get, result=%s", result)
	reply.Err = result.Err
	reply.Value = result.Value
	reply.Done = args.ID
	
	kv.px.Done(seq)
	kv.seq = seq + 1

	newRecord := Record{result.Value, result.Err}
	kv.records[args.ID] = &newRecord

        return nil
}


// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	
	kv.mu.Lock()
	defer kv.mu.Unlock()

	delete(kv.records, args.Prev)

	DPrintf("%s Received %s, Key=%s,Value=%s",kv.name,args.Op,args.Key,args.Value)
	DPrintf("ID=%v",args.ID)
	
	if record, ok := kv.records[args.ID]; ok && record.Err==OK {
	   // we have a record associate with this ID
	   reply.Err = record.Err

	   DPrintf("%s found an ID we have seen before,%s key=%s value=%s",kv.name,args.Op,args.Key,args.Value)
	   DPrintf("Err=%v", reply.Err)
	   return nil
	}

//	delete(kv.records, args.Prev)

	var v = Op{}
	v.ID = args.ID
	v.Type = args.Op
	v.Key = args.Key
	v.Value = args.Value
	
	seq := kv.Log(v)
	result := kv.ReadLog(seq)

	reply.Err = result.Err
	reply.Done = args.ID

	kv.px.Done(seq)
	kv.seq = seq + 1

	newRecord := Record{result.Value,result.Err}
	kv.records[args.ID] = &newRecord

	return nil
}


func (kv *ShardKV) Reconfig(newConfig shardmaster.Config, keyValue map[string]string, records map[int64]*Record) {

        DPrintf("%s Received Reconfiguration, newConfig=%v",kv.name,newConfig)

        var v = Op{}
	v.ID = nrand()
        v.Type = "Reconfig"
	v.Config = newConfig
	v.KV = make(map[string]string)
	v.Records = make(map[int64]*Record)

	for key, value := range keyValue {
	    v.KV[key] = value
	}

	for ID, record := range records {
	    v.Records[ID] = record
	}

	seq := kv.Log(v)
        kv.ReadLog(seq)

	kv.config = newConfig
	kv.px.Done(seq)
	kv.seq = seq + 1

}

func (kv *ShardKV) StartReconfig(newConfig shardmaster.Config) {

     currentConfig := kv.config
     
     DPrintf("Inside startReconfig, I'm %s",kv.name)
     DPrintf("new config=%v",newConfig)
     DPrintf("current config=%v",currentConfig)

     keyValue := make(map[string]string)
     records := make(map[int64]*Record)

     for shardNum, newGID := range newConfig.Shards {
     
	 currentGID := currentConfig.Shards[shardNum]
     	 if newGID == kv.gid && currentGID != kv.gid {
	    // this server becomes the new owner of this shard
	    // get shard from old servers
	    servers, ok := currentConfig.Groups[currentGID] 

	    if ok {
	       success := false
	       var reply GetShardReply

	       for !success || reply.Err != OK {
	       	   for _, server := range servers {

	       	       DPrintf("%s get shard from server %v", kv.name,server)

	       	       args := &GetShardArgs{}
		       args.ShardNum = shardNum
		       args.OldConfig = currentConfig

		       // NEED TO THINK MORE CAREFULLy
		       args.ConfigNum = newConfig.Num

		       success = call(server, "ShardKV.GetShard", args,&reply)

		       if success && reply.Err == OK {
		       	  // successfully get shard
			  DPrintf("successfully get shard")

		      	  for key, value := range reply.KeyValue {
			     keyValue[key] = value
		      	  }

			  for ID, record := range reply.Records {
			     records[ID] = record
			  }

		      	  break
		       }
	       	   }
		   
		   if success && reply.Err == OK {
		      break
		   }

		   DPrintf("get shard fail, %v",reply.Err)
	       	   time.Sleep(5 * time.Millisecond)
	       }
	    }   
	 }
     }

     kv.Reconfig(newConfig, keyValue, records)
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) error {
     kv.mu.Lock()
     defer kv.mu.Unlock()
     
     DPrintf("%s Received GetShard, shardNum=%d",kv.name, args.ShardNum)
     DPrintf("args.ConfigNum =%d, kv.config.Num=%d",args.ConfigNum,kv.config.Num)
     if args.ConfigNum > kv.config.Num {
     	// this server is not ready yet
	DPrintf("%s is not ready, Error", kv.name)
	reply.Err = ErrNotReady
	return nil
     }

     prevConfig := kv.sm.Query(args.ConfigNum-1)
//     if prevConfig.Shards[args.ShardNum]!= kv.gid ||
//args.OldConfig.Shards[args.ShardNum] != kv.gid {
     if prevConfig.Shards[args.ShardNum] != kv.gid {
     	DPrintf("%s does not own the shard", kv.name)
	reply.Err = ErrWrongGroup
	return nil
     }

     DPrintf("%s is ready!", kv.name)     

     var v = Op{}
     v.ID = nrand()
     v.Type = "GetShard"
     
     seq := kv.Log(v)
     kv.ReadLog(seq)

     kv.px.Done(seq)
     kv.seq = seq + 1

     DPrintf("%s sending shard %d",kv.name,args.ShardNum)

     reply.KeyValue = make(map[string]string)
     for key, value := range kv.keyValue {
         if key2shard(key) == args.ShardNum {
	    reply.KeyValue[key] = value
	 }
     }

     reply.Records = make(map[int64]*Record)
     for ID, record := range kv.records {
     	 reply.Records[ID] = record
     }

     reply.Err = OK

     DPrintf("Perform getShard, result=%v",reply.KeyValue)

     return nil
}


//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
     kv.mu.Lock()
     defer kv.mu.Unlock()

//     DPrintf("I am %s, here are my key values: %v", kv.name,kv.keyValue)

       DPrintf("current config:%v",kv.config)
       
       newConfig := kv.sm.Query(kv.config.Num + 1)
       
       if newConfig.Num == kv.config.Num + 1 {
	DPrintf("In Tick, %s config change to %v", kv.name,newConfig)

	if newConfig.Num == 1 {
	   kv.config = newConfig
	   return
	}

	kv.StartReconfig(newConfig)
     }

     return
}

func (kv *ShardKV) PerformOp(op Op) PerformOpReply {

     var result PerformOpReply
     shardNum := key2shard(op.Key)

     switch op.Type {
     	    case "Get":
	    	 if kv.config.Shards[shardNum]==kv.gid {
	    	    value, ok := kv.keyValue[op.Key]
		    if !ok {
		       result.Err = ErrNoKey
		    } else {
		       result.Value = value
		       result.Err = OK
		    }
		 } else {
		    DPrintf("get a key that doesn't belong to me, shardNum=%v",shardNum)
		    result.Err = ErrWrongGroup
		 }
		 
		 return result

	    case "Put":
	    	 if kv.config.Shards[shardNum]==kv.gid {
	    	    kv.keyValue[op.Key] = op.Value
		    result.Value = kv.keyValue[op.Key]
		    result.Err = OK
		 } else {
		    DPrintf("put a key that doesn't belong to me,shardNum=%v",shardNum)
		    result.Err = ErrWrongGroup
		 }

		 return result
		 
	    case "Append":
	    	 if kv.config.Shards[shardNum]==kv.gid {
	    	    result.Value = kv.keyValue[op.Key]
		    result.Value += op.Value
		    kv.keyValue[op.Key] = result.Value
		    result.Err = OK
		 } else {
		    DPrintf("append a key that doesn't belong to me,shardNum=%v",shardNum)
		    result.Err = ErrWrongGroup
		 }

		 return result

	    case "Reconfig":
	    	 for key, value := range op.KV {
		     kv.keyValue[key] = value
		 }

		 for ID, record := range op.Records {
		     kv.records[ID] = record
		 }

		 result.Err = OK
	    
            case "GetShard":
	    
     }
     
     return result
}



func (kv *ShardKV) WaitDecided(seq int) Op {
     to := 10 * time.Millisecond
     for {
     	 status, op := kv.px.Status(seq)
	 if status == paxos.Decided{
	    DPrintf("Paxos have decided on %v", op.(Op))
	    return op.(Op)
	 }
	 time.Sleep(to)
	 if to < 10 * time.Second {
	    to *= 2
	 }
     }
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	kv.config = kv.sm.Query(-1)
	kv.keyValue = make(map[string]string)
	kv.seq = 0
	kv.name = servers[me]
	kv.records = make(map[int64]*Record)

	DPrintf("%s come up, config=%v",kv.name,kv.config)

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
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
