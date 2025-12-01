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

type TaskType int
type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

type Task struct {
	TaskId     int
	Filename   string
	TaskStatus TaskStatus
}
type GetTaskArgs struct{}

type GetTaskReply struct {
	TaskType TaskType
	Filename string
	TaskId   int
	NReduce  int
	NMap     int
}

type ReportTaskDoneArgs struct {
	TaskType TaskType
	TaskId   int
}
type ReportTaskDoneReply struct{}

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
