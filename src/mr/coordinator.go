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

// a label to tell the coordinator the status of a task
type TaskStatus int

const (
	Idle       TaskStatus = iota //no one has started on this task
	InProgress                   //task is currently being worked on
	Completed                    //task has been finished
)

// phase of the entire job
type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	DonePhase
)

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex //prevents workers grabbing the same task,
	// tasks getting overwritten
	//state changes getting messed up
	file            []string     //list of input files for map tasks
	nReduce         int          //how many reduce tasks exist
	mapTasks        []TaskStatus //status of each map task
	reduceTasks     []TaskStatus //status of each reduce task
	phase           Phase        //current phase of the whole job
	nMap            int          //how many map tasks exist
	mapStartTime    []time.Time
	reduceStartTime []time.Time

	workers      map[int]string //map of worker IDs to their addresses
	nextWorkerID int            //counter to assign unique IDs to workers
	mapOwner     []int          //which worker is assigned to which map task
}

// Your code here -- RPC handlers for the worker to call.

// RPC handler in the coordinator for worker to request a task
// worker: "coordinator, do you have a task for me?"
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	//only one worker can be assigned a task at a time
	//without this lock, multiple workers could get the same task
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
		//look through map tasks
		for i, status := range c.mapTasks {
			//if there is any task that hasn't been given to any worker
			if status == Idle {
				//a worker is now doing this task (updating coordinator memory)
				c.mapTasks[i] = InProgress
				c.mapOwner[i] = args.WorkerID
				c.mapStartTime[i] = time.Now()
				fmt.Printf("Assigning map task %d to worker %d\n", i, args.WorkerID)

				//fill in reply to worker with task details
				reply.TaskType = MapTask  //you have a map task
				reply.File = c.file[i]    //the file you should read
				reply.TaskID = i          //you are doing map task i
				reply.NReduce = c.nReduce //you need to split your map output into this many reduce tasks
				reply.NMap = len(c.file)  //total number of map tasks

				return nil //done assigning task
			}
		}
		reply.TaskType = WaitTask //no task for you right now, wait
		return nil
	}
	//reduce phase
	if c.phase == ReducePhase {
		for i := range c.reduceStartTime {
			if c.reduceTasks[i] == InProgress {
				if time.Since(c.reduceStartTime[i]) > 10*time.Second {
					fmt.Println("Reassigning reduce task", i, "due to timeout")
					c.reduceTasks[i] = Idle
				}
			}
		}
		//look through reduce tasks
		for i, status := range c.reduceTasks {
			//if there is any task that hasn't been given to any worker
			if status == Idle {
				//a worker is now doing this task (updating coordinator memory)
				c.reduceTasks[i] = InProgress
				c.reduceStartTime[i] = time.Now()
				fmt.Printf("Assigning reduce task %d to worker %d\n", i, args.WorkerID)

				//fill in reply to worker with task details
				reply.TaskType = ReduceTask //you have a reduce task
				reply.TaskID = i            //you are doing reduce task i
				reply.NReduce = c.nReduce   //total number of reduce tasks
				reply.NMap = c.nMap         //total number of map tasks
				reply.Owners = append([]int{}, c.mapOwner...)

				return nil //done assigning task
			}
		}
		reply.TaskType = WaitTask //no task for you right now, wait
		return nil
	}
	if c.phase == DonePhase {
		reply.TaskType = ExitTask //job done, worker can exit
		return nil
	}
	//if no tasks are idle, but some are in progress
	reply.TaskType = WaitTask //no task for you right now, wait
	return nil
}

// RPC handler in the coordinator for worker to report a completed task
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	//check if the reported task is a map task
	if args.TaskType == MapTask {
		//mark the reported map task as completed
		c.mapTasks[args.TaskID] = Completed

		//check if all map tasks are completed
		allDone := true
		for _, status := range c.mapTasks {
			if status != Completed {
				allDone = false
				break
			}
		}
		if allDone && c.phase == MapPhase {
			c.phase = ReducePhase //move to reduce phase
			fmt.Println("All map tasks completed. Moving to Reduce Phase.")
		}
	}
	//check if the reported task is a reduce task
	if args.TaskType == ReduceTask {
		//mark the reported reduce task as completed
		c.reduceTasks[args.TaskID] = Completed

		//check if all reduce tasks are completed
		allDone := true
		for _, status := range c.reduceTasks {
			if status != Completed {
				allDone = false
				break
			}
		}
		if allDone && c.phase == ReducePhase {
			c.phase = DonePhase //job is done
			fmt.Println("All reduce tasks completed. Job Done.")
		}
	}
	return nil
}

func (c *Coordinator) GetWorkerAddress(args *WorkerAddressArgs, reply *WorkerAddressReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.WorkerAddress = c.workers[args.WorkerID]
	if reply.WorkerAddress == "" {
		return fmt.Errorf("worker ID %d not found", args.WorkerID)
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
	l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
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

// RPC handler in the coordinator for worker registration
func (c *Coordinator) RegisterWorker(args *RegisterArgs, reply *RegisterReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	workerID := c.nextWorkerID              //assign unique ID to worker
	c.workers[workerID] = args.WorkerAdress //store worker address
	c.nextWorkerID++                        //increment for next worker
	reply.WorkerID = workerID

	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.workers = make(map[int]string)
	c.nextWorkerID = 0

	c.file = files
	c.nReduce = nReduce
	c.nMap = len(files)
	c.mapTasks = make([]TaskStatus, len(files)) //status of each map task in a list
	c.reduceTasks = make([]TaskStatus, nReduce) //status of each reduce task in a list
	c.mapStartTime = make([]time.Time, len(files))
	c.reduceStartTime = make([]time.Time, nReduce)
	c.phase = MapPhase

	c.mapOwner = make([]int, len(files))
	for i := range c.mapOwner {
		c.mapOwner[i] = -1 //no worker assigned yet
	}

	c.server()
	return &c
}
