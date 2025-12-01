package mr

import (
	"os"
	"strconv"
)

//
// RPC definitions.

// tells worker what kind of task it has been assigned
// what action to take
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

// worker tells coordinator "im worker, give me a task"
// empty request, because coordinotor already know what tasks exits
// does not need any extra info from worker
type RequestTaskArgs struct{}

// coordinator's reply to worker
// the action's details
// all the info worker needs to perform the task
type RequestTaskReply struct {
	TaskType TaskType //type of task assigned
	File     string   //which file to read
	TaskID   int      //number of the task assigned
	NMap     int      //number of map tasks
	NReduce  int      //number of reduce tasks
}

// worker tells coordinator "im done with this task"
type ReportTaskArgs struct {
	TaskType TaskType //type of task being reported
	TaskID   int      //number of the task being reported
}

// coordinator's reply to worker
type ReportTaskReply struct{}

//
// remember to capitalize all names.
//
//
// example to show how to declare the arguments
// and reply for an RPC.
//

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
