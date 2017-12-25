package mapreduce

import "container/list"
import "fmt"
import "strconv"
//import "time"

type WorkerInfo struct {
	address string
	// You can add definitions here.
	
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	


	go mr.getWorker()
	go mr.getError()

	// start map operations
	go mr.waitAllDone(mr.nMap)
	for i := 0; i < mr.nMap; i++ {	

	    worker := mr.findIdleWorker()
	    go mr.callDoJob("Map", i, mr.nReduce, worker)

	}	
	// wait until all maps are done
	ok := <- mr.waitChannel
	if ok {
	   fmt.Println("Finished all map operation")
	}
	
	
	// start reduce operations
	go mr.waitAllDone(mr.nReduce)

	for i := 0; i < mr.nReduce; i++ {
	    worker := mr.findIdleWorker()
	    go mr.callDoJob("Reduce", i, mr.nMap, worker)

	}
	// wait until all reduce are done
	ok = <- mr.waitChannel
	if ok == true {
	   fmt.Println("Finished all reduce operation")
	}

	
	return mr.KillWorkers()
}

func (mr *MapReduce) callDoJob (operation JobType,jobNumber int,numOtherPhase int, worker string) {
	
	args := &DoJobArgs{}
	args.File = mr.file
	args.Operation = operation
	args.JobNumber = jobNumber
	args.NumOtherPhase = numOtherPhase
	var reply DoJobReply

	ok := call(worker, "Worker.DoJob", args, &reply)
	if ok == false {
	   fmt.Printf("DoJob: RPC %s DoJob error, job %d\n", worker, jobNumber)
	   mr.errorChannel <- args
	} else {
	   mr.countChannel <- true
	   mr.workerChannel <- worker	
	}

}

func (mr *MapReduce) getWorker () {
     	 // this function get workers from registerChannel
	 // and add them to mr.Workers
	 i := 0

     	 for {
	     fmt.Println("getting a new worker")
	     workerAddr := <- mr.registerChannel
	     worker := &WorkerInfo{workerAddr}
	     workername := "worker" + strconv.Itoa(i)
	     mr.Workers[workername] = worker
	     mr.workerChannel <- workerAddr
	     i++
	     }
}


func (mr *MapReduce) findIdleWorker () string {
     	 // this function finds idle worker from workerChannel

	 worker := <- mr.workerChannel
	 return worker	 
	 

}	 

func (mr *MapReduce) getError () {
     	 // this function get DoJobArgs from a error
	 // pass the job to another worker
	 for {

	     args := <- mr.errorChannel
	     worker := mr.findIdleWorker()
	     go mr.callDoJob(args.Operation, args.JobNumber, args.NumOtherPhase, worker)
	     
	 }
}

func (mr * MapReduce) waitAllDone (jobNumber int) {
     	 for jobNumber != 0 {
	     <- mr.countChannel
	     jobNumber-- 
	 }
	 mr.waitChannel <- true
}
