package mr

import (
	"os"
	"strconv"
)

//
// RPC definitions.

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

type RequestTaskArgs struct{}

type RequestTaskReply struct {
	TaskType TaskType
	File     string
	TaskID   int
	NMap     int
	NReduce  int
}

type ReportTaskArgs struct {
	TaskType TaskType
	TaskID   int
}

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
