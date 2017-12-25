package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// Your declarations here.
	CurrentView  View
	TimeLog      map[string]time.Time
	idle         string
	PrimaryAcked bool
	PrimaryDead  bool
	BackupDead   bool
	

}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.

	vs.mu.Lock()
	
	// Ping(0)
	if args.Viewnum == 0 && vs.PrimaryAcked {   
	   
	   if vs.CurrentView.Viewnum == 0 {
	        // initial state
	      	vs.CurrentView.Primary = args.Me
	      	vs.CurrentView.Viewnum++
		vs.PrimaryAcked = false

//	   } else if vs.CurrentView.Backup == "" {
//	   	// don't have a backup
//		vs.CurrentView.Backup = args.Me
//		vs.CurrentView.Viewnum++
//		vs.PrimaryAcked = false

	   } else if vs.CurrentView.Primary == args.Me {
	        // primary has failed and send Ping(0)
		// Backup takes over
		vs.CurrentView.Primary = vs.CurrentView.Backup
		if vs.idle != "" { 
		   vs.CurrentView.Backup = vs.idle
		   vs.idle = ""
		} else {
		   vs.CurrentView.Backup = args.Me
		}
		vs.CurrentView.Viewnum++
		vs.PrimaryAcked = false

//	   } else if args.Me != vs.CurrentView.Primary && args.Me != vs.CurrentView.Backup {
//	   	// have a primary and backup, this is an idle server
//		vs.idle = args.Me
	   }

	} 
	 if args.Me == vs.CurrentView.Primary && args.Viewnum == vs.CurrentView.Viewnum {
	   // primary acked with current view number
	   vs.PrimaryAcked = true
	   }

	   if args.Me != vs.CurrentView.Primary && args.Me !=
	   vs.CurrentView.Backup {
	   	vs.idle = args.Me
	   }
	
	   if vs.CurrentView.Backup == "" && vs.idle != "" {
	        vs.CurrentView.Backup = vs.idle
		vs.idle = ""
		vs.CurrentView.Viewnum++
		vs.PrimaryAcked = false
	   }

	   if vs.PrimaryDead && vs.PrimaryAcked  {
	        vs.CurrentView.Primary = vs.CurrentView.Backup
		vs.CurrentView.Backup = vs.idle
		vs.idle = ""
		vs.CurrentView.Viewnum++
		vs.PrimaryAcked = false
		vs.PrimaryDead = false
	   }

	   if vs.BackupDead && vs.PrimaryAcked {
	        vs.CurrentView.Backup = vs.idle
		vs.idle = ""
		vs.CurrentView.Viewnum++
		vs.PrimaryAcked = false
		vs.BackupDead = false
	   }

	

	reply.View = vs.CurrentView
	vs.TimeLog[args.Me] = time.Now()

	vs.mu.Unlock()

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	reply.View = vs.CurrentView	
	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	
	vs.mu.Lock()	

	for k, v := range vs.TimeLog {
	    if time.Now().Sub(v) >= DeadPings * PingInterval && vs.PrimaryAcked {
	       switch k {	       
	       case vs.CurrentView.Primary:
	       // Primary fails
		    vs.PrimaryDead = true
		    

	       case vs.CurrentView.Backup:
	       // Backup fails
		    vs.BackupDead = true

	       }
	    }
	}

	vs.mu.Unlock()
		
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.CurrentView = View{0, "", ""}
	vs.TimeLog = make(map[string]time.Time)
	vs.idle = ""
	vs.PrimaryAcked = true
	vs.PrimaryDead = false
	vs.BackupDead = false

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
