package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	DonePhase
)

type Coordinator struct {
	// Your definitions here.
	mu              sync.Mutex
	file            []string
	nReduce         int
	mapTasks        []TaskStatus
	reduceTasks     []TaskStatus
	phase           Phase
	nMap            int
	mapStartTime    []time.Time
	reduceStartTime []time.Time
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase == MapPhase {
		for i := range c.mapStartTime {
			if c.mapTasks[i] == InProgress {
				if time.Since(c.mapStartTime[i]) > 10*time.Second {
					fmt.Println("Reassigning map task", i, "due to timeout")
					c.mapTasks[i] = Idle
				}
			}
		}
		for i, status := range c.mapTasks {
			if status == Idle {
				c.mapTasks[i] = InProgress
				c.mapStartTime[i] = time.Now()

				reply.TaskType = MapTask
				reply.File = c.file[i]
				reply.TaskID = i
				reply.NReduce = c.nReduce
				reply.NMap = len(c.file)

				return nil
			}
		}
		reply.TaskType = WaitTask
		return nil
	}
	if c.phase == ReducePhase {
		for i := range c.reduceStartTime {
			if c.reduceTasks[i] == InProgress {
				if time.Since(c.reduceStartTime[i]) > 10*time.Second {
					fmt.Println("Reassigning reduce task", i, "due to timeout")
					c.reduceTasks[i] = Idle
				}
			}
		}
		for i, status := range c.reduceTasks {
			if status == Idle {
				c.reduceTasks[i] = InProgress
				c.reduceStartTime[i] = time.Now()

				reply.TaskType = ReduceTask
				reply.TaskID = i
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap

				return nil
			}
		}
		reply.TaskType = WaitTask
		return nil
	}
	if c.phase == DonePhase {
		reply.TaskType = ExitTask
		return nil
	}
	reply.TaskType = WaitTask
	return nil
}

// RPC handler in the coordinator for worker to report a completed task
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == MapTask {
		c.mapTasks[args.TaskID] = Completed

		allDone := true
		for _, status := range c.mapTasks {
			if status != Completed {
				allDone = false
				break
			}
		}
		if allDone && c.phase == MapPhase {
			c.phase = ReducePhase
			fmt.Println("All map tasks completed. Moving to Reduce Phase.")
		}
	}
	if args.TaskType == ReduceTask {
		c.reduceTasks[args.TaskID] = Completed

		allDone := true
		for _, status := range c.reduceTasks {
			if status != Completed {
				allDone = false
				break
			}
		}
		if allDone && c.phase == ReducePhase {
			c.phase = DonePhase
			fmt.Println("All reduce tasks completed. Job Done.")
		}
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase == DonePhase {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.file = files
	c.nReduce = nReduce
	c.nMap = len(files)
	c.mapTasks = make([]TaskStatus, len(files))
	c.reduceTasks = make([]TaskStatus, nReduce)
	c.mapStartTime = make([]time.Time, len(files))
	c.reduceStartTime = make([]time.Time, nReduce)
	c.phase = MapPhase

	c.server()
	return &c
}
