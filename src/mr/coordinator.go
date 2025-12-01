package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Stage int

const (
	MapStage Stage = iota
	ReduceStage
	DoneStage
)

type Coordinator struct {
	mu          sync.Mutex
	Stage       Stage
	MapTasks    []Task
	ReduceTasks []Task
	NReduce     int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	// If there is an unassigned map task, give the filename to the worker
	c.mu.Lock()
	defer c.mu.Unlock() // Unlock when the function returns

	switch c.Stage {

	case MapStage:

		for i := range c.MapTasks {
			t := &c.MapTasks[i]
			if t.TaskStatus == Idle {
				t.TaskStatus = InProgress
				reply.TaskType = MapTask
				reply.Filename = t.Filename
				reply.TaskId = t.TaskId
				reply.NReduce = c.NReduce
				reply.NMap = len(c.MapTasks)
				return nil
			}
		}

		if c.mapDone() {
			c.Stage = ReduceStage
		}
		reply.TaskType = WaitTask
		return nil

	case ReduceStage:
		for i := range c.ReduceTasks {
			t := &c.ReduceTasks[i]
			if t.TaskStatus == Idle {
				t.TaskStatus = InProgress
				reply.TaskType = ReduceTask
				reply.TaskId = t.TaskId
				reply.NReduce = c.NReduce
				reply.NMap = len(c.MapTasks)
				return nil
			}
		}
		if c.reduceDone() {
			c.Stage = DoneStage
		}
		reply.TaskType = WaitTask
		return nil
	case DoneStage:
		reply.TaskType = ExitTask
		return nil
	}
	reply.TaskType = WaitTask
	return nil
}

func (c *Coordinator) ReportTaskDone(args *ReportTaskDoneArgs, reply *ReportTaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == MapTask {
		c.MapTasks[args.TaskId].TaskStatus = Completed
	} else if args.TaskType == ReduceTask {
		c.ReduceTasks[args.TaskId].TaskStatus = Completed
	}
	return nil
}

func (c *Coordinator) mapDone() bool {
	for _, t := range c.MapTasks {
		if t.TaskStatus != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) reduceDone() bool {
	for _, t := range c.ReduceTasks {
		if t.TaskStatus != Completed {
			return false
		}
	}
	return true
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
	log.Printf("Listening on %s", sockname)
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Stage == DoneStage
}

// Your code here -- RPC handlers for the worker to call.

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.NReduce = nReduce

	for i, file := range files {
		c.MapTasks = append(c.MapTasks, Task{
			TaskId:     i,
			Filename:   file,
			TaskStatus: Idle,
		})
	}

	c.ReduceTasks = make([]Task, nReduce)
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = Task{
			TaskId:     i,
			TaskStatus: Idle,
		}
	}
	c.Stage = MapStage
	c.server()
	return &c
}
