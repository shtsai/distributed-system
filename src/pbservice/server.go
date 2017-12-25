package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"



type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	view       viewservice.View
	Keyvalue   map[string]string
	IDSeen    map[int64]*PutAppendArgs    // store operation ID that has seen
	TickFail   bool
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()	
	
	if pb.TickFail == true {
	   reply.Err = ErrWrongServer
	   pb.mu.Unlock()
	   return nil
	}

	if value, ok := pb.Keyvalue[args.Key]; ok {
	   reply.Value = value
	   reply.Err = OK

	} else {
	   reply.Value = ""
	   reply.Err = ErrNoKey
	}
	
	pb.mu.Unlock()
	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()

	if pb.TickFail == true {
	   reply.Err = ErrWrongServer
	   pb.mu.Unlock()
	   return nil
	}

	if pb.me == pb.view.Primary {
	if _, ok := pb.IDSeen[args.ID]; ok {
	       reply.Err = OK
	       pb.mu.Unlock()
	       return nil
	}
	}

	if pb.view.Backup != "" && pb.view.Primary == pb.me{
	   // I'm primary and forward operation the backup
	   args.Pvalue = pb.Keyvalue[args.Key]
	   var freply PutAppendReply
	   ok := call(pb.view.Backup, "PBServer.PutAppend", args, &freply)
	   if !ok || freply.Err !=OK {
	        reply.Err = ErrWrongServer
		pb.mu.Unlock()
		return nil
           }
        }

	if pb.view.Backup == pb.me{
	   // I'm backup and I receive a forward message from the primary
	   if args.Pvalue != pb.Keyvalue[args.Key] {
	      // found inconsistency
	      pb.Keyvalue[args.Key] = args.Pvalue
	   }
	}

        switch args.Op {
               case "Put":
                    pb.Keyvalue[args.Key] = args.Value
                    reply.Err = OK

               case "Append":
                    if value,ok := pb.Keyvalue[args.Key]; ok {
                       // key exists
                       value += args.Value
                       pb.Keyvalue[args.Key] = value
                       reply.Err = OK

                    } else {
                       // key doesnt exist
                       pb.Keyvalue[args.Key] = args.Value
                       reply.Err = OK

                    }
        }

	// add operation ID to the map
        pb.IDSeen[args.ID] = args

	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) Transfer(args *TransferArgs, reply *PutAppendReply) error {
     	// transfer key value store from primary to backup
     	pb.mu.Lock()
	
	if pb.me != pb.view.Backup {
	   reply.Err = ErrWrongServer
	   pb.mu.Unlock()
	   return nil
	}

	pb.Keyvalue = args.Keyvalue
	reply.Err = OK

	pb.mu.Unlock()
	return nil
}



//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.

	pb.mu.Lock()

	// send Ping to viewServer
	v, err := pb.vs.Ping(pb.view.Viewnum)
	if err != nil {
	   // handle partition
	   pb.TickFail = true
	   pb.mu.Unlock()
	   return
	}


	if pb.view.Backup != v.Backup && v.Backup != "" && v.Primary ==
	   pb.me && pb.TickFail == false{
	   // a backup just come up, transfer keyvalue store to backup
	   args := TransferArgs{pb.Keyvalue}
	   var reply TransferReply
	   ok := call(v.Backup, "PBServer.Transfer", args, &reply)
	   for ok == false || reply.Err != OK {
	       time.Sleep(viewservice.PingInterval)
	       v, err = pb.vs.Ping(pb.view.Viewnum)
	       if err != nil {
	       	  fmt.Println("ping error:", pb.view)
	       }
	       ok = call(v.Backup, "PBServer.Transfer", args, &reply)
	   }
	}

        pb.view = v
	pb.TickFail = false
	
	pb.mu.Unlock()
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.view = viewservice.View{0,"",""}
	pb.Keyvalue = make(map[string]string)
	pb.IDSeen = make(map[int64]*PutAppendArgs)
	pb.TickFail = false

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
