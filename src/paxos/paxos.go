package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"
import "time"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type ProposalId struct {
     	Proposal   int
     	Who 	   int
	Sequence   int
}

type Instance struct {
     	prepareID  ProposalId   // highest prepare seen
	acceptID   ProposalId   // highest accept seen
        value      interface{}
	status	   Fate
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	instances  map[int]*Instance
	majorityNum int
	dones      []int
}

type PrepareArgs struct {
        ID  	   ProposalId
}

type PrepareReply struct {
     	OK         bool
	AcceptID   ProposalId
	Value      interface{}
	Status	   Fate
}

type AcceptArgs struct {
     	ID 	   ProposalId
	Value      interface{}
}

type AcceptReply struct {
        ID         ProposalId
        OK         bool
}

type DecideArgs struct {
     	ID	   ProposalId
        Value      interface{}
	Done       int
}

type DecideReply struct {
        OK         bool
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
//			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

//	fmt.Println(err)
	return false
}


// These are functions for comparing proposal IDs
func (pi ProposalId) Greater(other ProposalId) bool {
     return pi.Proposal > other.Proposal ||
     	    (pi.Proposal == other.Proposal && pi.Who > other.Who)
}

func (pi ProposalId) Equal(other ProposalId) bool {
     return pi.Proposal == other.Proposal && pi.Who == other.Who
}

func (pi ProposalId) Geq(other ProposalId) bool {
     return pi.Greater(other) || pi.Equal(other)
}

func NullProposal(seq int) ProposalId {
     return ProposalId { -1, -1, seq}
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {

     px.mu.Lock()
     // if instance doesn't exist, initialize a new instance
     if _, ok := px.instances[args.ID.Sequence]; !ok {
     	  px.instances[args.ID.Sequence] = &Instance{NullProposal(args.ID.Sequence),NullProposal(args.ID.Sequence),nil,2}
     }

     if args.ID.Greater(px.instances[args.ID.Sequence].prepareID) {
     	  px.instances[args.ID.Sequence].prepareID = args.ID
	  reply.OK = true
     } else {
       	  reply.OK = false
     }
     
     reply.AcceptID = px.instances[args.ID.Sequence].acceptID
     reply.Value = px.instances[args.ID.Sequence].value
     reply.Status = px.instances[args.ID.Sequence].status


     px.mu.Unlock()
     return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
     
     px.mu.Lock()

     // if instance doesn't exist, initialize a new instance
     if _, ok := px.instances[args.ID.Sequence]; !ok {
          px.instances[args.ID.Sequence] = &Instance{NullProposal(args.ID.Sequence),NullProposal(args.ID.Sequence),nil,2}
     }

     if args.ID.Geq(px.instances[args.ID.Sequence].prepareID) {
     	  px.instances[args.ID.Sequence].prepareID = args.ID
	  px.instances[args.ID.Sequence].acceptID = args.ID
	  px.instances[args.ID.Sequence].value = args.Value
	  reply.OK = true
     } else {
       	  reply.OK = false
     }
     
     px.mu.Unlock()
     return nil
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error{

     px.mu.Lock()
     
     if _, ok := px.instances[args.ID.Sequence]; !ok {
     	 px.instances[args.ID.Sequence] = &Instance{NullProposal(args.ID.Sequence),NullProposal(args.ID.Sequence),nil,2}
     }

     px.instances[args.ID.Sequence].status = Decided
     px.instances[args.ID.Sequence].value = args.Value

     // not sure if need to change IDs
     px.instances[args.ID.Sequence].prepareID = args.ID
     px.instances[args.ID.Sequence].acceptID = args.ID
     reply.OK = true
     px.dones[args.ID.Who] = args.Done
     
     px.mu.Unlock()
     return nil
}

func (px *Paxos) sendPrepare(id ProposalId, v interface{}) (int, int, int,interface{}) {

     me := px.me
     count := 0
     highestID := NullProposal(id.Sequence)

     args := PrepareArgs{id}

     for index, server := range px.peers {
            var reply PrepareReply
            ok := false
            if index != me {
                   ok = call(server, "Paxos.Prepare", args, &reply)
            } else {
                   px.Prepare(&args, &reply)
                   ok = true
            }

            if ok && reply.OK {
                   if reply.AcceptID.Greater(highestID) {
		          highestID = reply.AcceptID
                          v = reply.Value
                   }
                   count++
            }
     }
//     fmt.Println("Here is the prepare votes", count,"/", px.majorityNum)
     return count, id.Proposal, id.Who, v
}

func (px *Paxos) sendAccept(proposalnum int, who int, seq int, v interface{}) (int,int, int ,interface{}) {

     id := ProposalId{proposalnum, who, seq}
     me := px.me
     args := AcceptArgs{id, v}
     count := 0
     for index, server := range px.peers {
          var reply AcceptReply
          ok := false
          if index != me {
                ok = call(server, "Paxos.Accept", args, &reply)
          } else {
                px.Accept(&args, &reply)
                ok = true
          }

          if ok && reply.OK {
                count++
          }
     }
//     fmt.Println("Here is the votes for accept", count, "/", px.majorityNum)
     
     return count, id.Proposal, id.Who, v
}

func (px *Paxos) sendDecide (proposalnum int, who int,seq int, v interface{}) {

     id := ProposalId{proposalnum, who, seq}
     me := px.me
     args := DecideArgs{id,v, px.dones[px.me]}
     for index, server := range px.peers {
          var reply DecideReply
          ok := false
          if index != me {
                 ok = call(server, "Paxos.Decide", args,&reply)
          } else {
                 px.Decide(&args, &reply)
                 ok = true
          }

          if !ok || !reply.OK {
//                 fmt.Println("decide failed")
          }
     }
} 


//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.

	go func () {

	   if seq < px.Min() {
	        return
	   }
	   
	   status,_ := px.Status(seq)

	   id := ProposalId{0, px.me, seq}
	   for status != Decided {
	       		       
	        // prepare
		count, proposalnum, who, value := px.sendPrepare(id, v)

		if count < px.majorityNum {
		    id.Proposal += 1		    
		    status,_ = px.Status(seq)		    
		    continue

		// accept
		}  else {
		    count, proposalnum, who, value := px.sendAccept(proposalnum, who, seq, value)

		    // decide
		    if count >= px.majorityNum {
		        px.sendDecide(proposalnum,who,seq, value)
		    }
		}
		status,_ = px.Status(seq)

		time.Sleep(5 * time.Millisecond)
	    }
	}()

}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	
	px.mu.Lock()

	if seq > px.dones[px.me] {
	   px.dones[px.me] = seq
	}

	px.mu.Unlock()
	
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	
	max := -1

	px.mu.Lock()

	for index,_ := range px.instances {
	    if index > max {
	       	max = index
	    }
	} 
	px.mu.Unlock()

	return max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	
	px.mu.Lock()

	done := px.dones[px.me]

	for i := range px.dones {
	    // find min among dones
	    if done > px.dones[i] {
	       	 done = px.dones[i]
	    }
	}

	for id, instance := range px.instances {
	    if id > done {
	       continue
	    } else if instance.status != Decided {
	       continue
	    } else {
	    delete(px.instances, id)
	    }
	}

	px.mu.Unlock()
	return done + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.

//	fmt.Println("I'm",px.me,"here are my instances", px.instances)
	
	if seq < px.Min() {
	    return Forgotten, nil
	}

	px.mu.Lock()
	
	if instance, ok := px.instances[seq]; ok {	 
	    px.mu.Unlock()
	    return instance.status, instance.value
	} 

	px.mu.Unlock()
	return Pending, nil
}



//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
//	px.instances = make(map[ProposalId]*Instance)
	px.instances = make(map[int]*Instance)
	px.majorityNum = int(len(px.peers)/2 + 1)
	px.dones = make([]int, len(px.peers))
	for i:= range px.peers {
	        px.dones[i] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}


	return px
}
