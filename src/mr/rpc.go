package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskType int //lets worker and coordinator agree on what kind of task the worker should process

const (
	MapTask    TaskType = iota // Run mapf on this input file and write intermediate files
	ReduceTask                 // Read intermediate files and run reducef
	WaitTask                   // The coordiantor currently has nothing for the worker, worker should not exit, just wait
	ExitTask                   // All done worker can quit now
)

type RequestTaskArgs struct { //worker doesn't need to send anything that's why the struct is empty; it just says "give me the work"
	// worker -> coordinator
	WorkerID int //ID of the worker requesting the task, in a distributed setup , the coordinator must know which worker is asking for a task so it can: record that worker as the owner of the map task and therefor later tell reducers "map task was done by worker X"
}

type RequestTaskReply struct { // this struct contains what the coordinator sends back to the worker,
	// coordinator -> worker
	TaskType TaskType //says what kind of task this is (map, reduce, wait)
	File     string   //Name of input file for the map task
	NReduce  int      //How many reduce tasks that exists in total, needed by map tasks to know how many intermediate files to create
	TaskID   int      //Map task index (X in mr-X-Y), a unquie number for this task, used for naming intermediate or output files
	NMap     int      // How many map tasks exsist in total, needed by reduce task to know how many files to read (from all mappers)
	Owners   []int    //for reduce tasks; owner worker IDs of each map. Reducers must know which worker holds each intermediate MAP file: mr-mapID-reduceID, because the workers no longer share a filesystem

}

type ReportTaskArgs struct { // this struct is what the worker sends back to the coordinator after finishing its work
	//worker -> coordinator
	TaskType TaskType //is it a map or a reduce task?
	TaskID   int      // which tasknumber is finished
	// report message usually says: I finished task #T of type X
}

type ReportTaskReply struct{} // this is the coordinators reply to the worker reporting completion
//coordinator -> worker
//the struct is empty because the worker doesn't need any information back and the coordinator just need to acknowledge that the task was recorded as done

type RegistersArgs struct {
	WorkerAdress string //This defines the arguments for an RPC call, this is what the worker send to to the coordinator when it says "Hi i'm a worker, please register me"
} // This is the TCO address where the worker's own RPC server is listening for ex: ":8001" or "10.0.0.7:9000"

type RegisterReply struct {
	WorkerID int //This defines what the coordinator sends back to the worker after registration, WorkerID is the unique integer ID the coordinator assigns to this worker
	//the reason we need this is becuse when a worker calls RequestTask it sends: RequestTaskArgs{WorkerID: workerID}
	//the coordinator then stores for each map task which worker ran it: mapOwner[mapTaskID] = workerID
	//Then for reducers, the coordinator can say: Map task 3 was done by worker 2
	// worker ID is the key that connects: mapOwner[i] (which map’s owner) AND workers[workerID] = "ip:port" (where that worker lives)
}

type GetFileArgs struct {
	File string // This struct is arguments to a method like: WorkerRPC.GetFile. One worker (a reducer) calls another worker (a mapper) and says: Please send me the contents of this intermediate file
}

type GetFileReply struct {
	Data []byte //This is what the map worker sends back to the reducer in response to GetFileArg.
	// Data []byte= raw file contents as bytes
}

type WorkerAddressArgs struct {
	WorkerID int //this is the arguments for an RPC Coordinator.GetWorkerAddress. WorkerAddressArgs/Reply exist so other workers (reducers) can ask the coordinator: what is the address of worker X?
}

type WorkerAddressReply struct {
	WorkerAddress string // this is the reply for Coordinator.GetWorkerAddress.
	//WorkerAddress string is the actual address like: "8001" or "10.0.0.7:9000"
	//Flow: reducer calls: getWorkerAddress(ownerID) :uses WorkerAddressArgs/Reply
	//That returns workeradress as string and then reducer does: callWorkerRPC("WorkerRPC.GetFile", WorkerAddress, &GetFileArgs{File: ...}, &fileReply)
	// so this is the bridge from WorkerID(logical identifier) -> address (network location)
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
