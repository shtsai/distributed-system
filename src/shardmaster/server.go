package shardmaster

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

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs    []Config // indexed by config num
	seq	   int      // sequence number used so far
}


type Op struct {
	// Your data here.
	Type 	   string
	GID	   int64
	Servers	   []string //Join
	Shard	   int      //Move
	Num	   int	    //Query	
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	
	sm.mu.Lock()
	defer sm.mu.Unlock()

	DPrintf("Received Join, GID=%d",args.GID)

	var v = Op{}
	v.Type = "Join"
	v.GID = args.GID
	v.Servers = args.Servers

	seqOK := false

	for !seqOK {
	    sm.seq++
	    DPrintf("Current sequence = %d",sm.seq)
	    
	    status, value := sm.px.Status(sm.seq)
	    DPrintf("seq = %d, Op=%v",sm.seq,value)

	    if status == paxos.Decided {
	       value = value.(Op)
	    } else {
	       sm.px.Start(sm.seq, v)
	       value = sm.WaitDecided(sm.seq)
	    }
	    
	    if value.(Op).Type == v.Type && value.(Op).GID == v.GID {
	       seqOK = true
	    }

	    result := sm.PerformOp(value.(Op))
	    
	    DPrintf("Performed %s operation, new config = %v", value.(Op).Type,result)
	    DPrintf("")	    

            sm.px.Done(sm.seq)
	}

	return nil
}


func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.

	sm.mu.Lock()
	defer sm.mu.Unlock()

	DPrintf("Received Leave, GID=%d",args.GID)

	var v = Op{}
	v.Type = "Leave"
	v.GID = args.GID

	seqOK := false

	for !seqOK {
	    sm.seq++
	    DPrintf("Current sequence = %d", sm.seq)
	    
	    status, value := sm.px.Status(sm.seq)
	    DPrintf("seq = %d, op=%v",sm.seq,value)

	    if status == paxos.Decided {
	       value = value.(Op)
	    } else {
	       sm.px.Start(sm.seq, v)
	       value = sm.WaitDecided(sm.seq)
	    }

	    if value.(Op).Type == v.Type && value.(Op).GID == v.GID {
	       seqOK = true
	    }

	    result := sm.PerformOp(value.(Op))
	    
	    DPrintf("Performed %s operation, new config = %v",value.(Op).Type, result)
	    DPrintf("")

	    sm.px.Done(sm.seq)
	}
  
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.

	sm.mu.Lock()
	defer sm.mu.Unlock()

	DPrintf("Received Move, GID =%d, shard =%d", args.GID,args.Shard)
	
	var v = Op{}
	v.Type = "Move"
	v.GID = args.GID
	v.Shard = args.Shard

	seqOK := false

        for !seqOK {
            sm.seq++
            DPrintf("Current sequence = %d", sm.seq)

            status, value := sm.px.Status(sm.seq)
            if status == paxos.Decided {
               value = value.(Op)
            } else {
               sm.px.Start(sm.seq, v)
               value = sm.WaitDecided(sm.seq)
            }

            if value.(Op).Type == v.Type && value.(Op).GID == v.GID {
               seqOK = true
            }

            result := sm.PerformOp(value.(Op))

            DPrintf("Performed %s operation, new config =%v",value.(Op).Type, result)
            DPrintf("")

            sm.px.Done(sm.seq)
        }

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.

	sm.mu.Lock()
	defer sm.mu.Unlock()

	DPrintf("Received Query, config Num = %d", args.Num)

	var v = Op{}
        v.Type = "Query"
	v.Num = args.Num

        seqOK := false

        for !seqOK {
            sm.seq++
            DPrintf("Current sequence = %d", sm.seq)

            status, value := sm.px.Status(sm.seq)
            if status == paxos.Decided {
               value = value.(Op)
            } else {
               sm.px.Start(sm.seq, v)
               value = sm.WaitDecided(sm.seq)
            }

            if value.(Op).Type == v.Type && value.(Op).Num == v.Num  {
               seqOK = true
            }

            result := sm.PerformOp(value.(Op))

            DPrintf("Performed %s operation, new config =%v",value.(Op).Type, result)
            DPrintf("")

            sm.px.Done(sm.seq)
        }

	configNum := args.Num
	
	if (configNum >= len(sm.configs) || configNum < 0) {
	   reply.Config = sm.configs[len(sm.configs)-1]
	} else {
	   reply.Config = sm.configs[configNum]
	}

	DPrintf("Query num: %d", configNum)
	DPrintf("result =%v", reply.Config)
	DPrintf("")

	return nil
}

func (sm *ShardMaster) PerformOp(op Op) Config {
        var config Config
        config.Num = len(sm.configs)
        config.Groups = make(map[int64][]string)
        prevConfig := sm.configs[config.Num-1]

        for index, content := range prevConfig.Groups {
            config.Groups[index] = content
        }
        for index, content := range prevConfig.Shards {
            config.Shards[index] = content
        }

	switch op.Type {
	       case "Join":
			config.Groups[op.GID] = op.Servers

		        DPrintf("config.Group =%v", config.Groups)

    	                shardSize := NShards / len(config.Groups)
			if NShards < len(config.Groups) {
			   shardSize = 1
			}

		        assignmentCount := make(map[int64]int)

		        for _, GID := range prevConfig.Shards {
		            if GID != 0 {
		               assignmentCount[GID]++
            		    }
        		}

		        DPrintf("assignmentCount=%v", assignmentCount)
		        DPrintf("shardSize=%d",shardSize)

		        for shardNum, GID := range config.Shards {
		            if assignmentCount[op.GID] == shardSize {
	                       break
            		    }
		            if assignmentCount[GID] > shardSize || GID == 0 {
        	               config.Shards[shardNum] = op.GID
                     	       assignmentCount[GID]--
               		       assignmentCount[op.GID]++
            		    }
        		}

		        DPrintf("config.Shards=%v",config.Shards)

	       case "Leave":
    	       	    delete(config.Groups, op.GID)

        	    DPrintf("config.Group =%v", config.Groups)

        	    shardSize := NShards / len(config.Groups)
		    if NShards < len(config.Groups) {
		       shardSize = 1
		    }

        	    assignmentCount := make(map[int64]int)

                    for shardNum, GID := range config.Shards {
            	    	if GID == op.GID {
               		   config.Shards[shardNum] = 0
            		} else {
               		   assignmentCount[GID]++
            		}
        	    }

		    // added GID with 0 shard
		    for GID := range config.Groups {
		    	_, ok := assignmentCount[GID]
			if !ok {
			   assignmentCount[GID] = 0
			}
		    }		    

        	    DPrintf("assignmentCount=%v", assignmentCount)
        	    DPrintf("shardSize=%d",shardSize)

        	    for aGID, count := range assignmentCount {
            	    	if count == shardSize {
               		   continue
			}

            		for shardNum, GID := range config.Shards {
                	    if GID == 0  {
                   	       config.Shards[shardNum] = aGID
                   	       count++
                	    }

                	    if count == shardSize {
                   	       break
			    }
            		}
        	    }
        	    DPrintf("config.Shards=%v",config.Shards)

	       case "Move":
	       	    config.Shards[op.Shard] = op.GID
		   
	       case "Query":
	       	    return prevConfig

	}	

	sm.configs = append(sm.configs, config)

//        DPrintf("After %s, latest config=%v", op.Type, sm.configs[config.Num])

	return config
}

func (sm *ShardMaster) WaitDecided(seq int) Op {
        to := 10 * time.Millisecond
	for {
	    status, op := sm.px.Status(seq)
	    if status == paxos.Decided{
	       DPrintf("Paxos have decided on %v %v", op.(Op).Type,op.(Op).GID)
	       return op.(Op)
	    }
	    time.Sleep(to)
	    if to < 10 * time.Second {
	       to *= 2
	    }
	}
}


// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}
	sm.seq = 0

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
