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

type RequestTaskArgs struct{} //worker doesn't need to send anything that's why the struct is empty; it just says "give me the work"
//worker -> coordinator

type RequestTaskReply struct { // this struct contains what the coordinator sends back to the worker,
	// coordinator -> worker
	TaskType TaskType //says what kind of task this is (map, reduce, wait)
	File     string   //Name of input file for the map task
	NReduce  int      //How many reduce tasks that exists in total, needed by map tasks to know how many intermediate files to create
	TaskID   int      //Map task index (X in mr-X-Y), a uniquie number for this task, used for naming intermediate or output files
	NMap     int      // How many map tasks exsist in total, needed by reduce task to know how many files to read (from all mappers)

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
