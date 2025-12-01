package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct { //shared state object that coordinates who gets which file
	mu sync.Mutex //protects shared state (multiple workers can call at the same time), mutex stored in mu

	files         []string //List of input files (one per task)
	nextFileIndex int      //Coordinator remembers where it is in the list
	nReduce       int      //Something that has with reduce tasks to do (DO LATER)

}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c) //register coordinator for rpc
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname) //a communication way for the coordinator and worker to communicate with each other (via unix domain sockets)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil) // Serve RPC requests over the socket
}

// A worker calls this over RPC to ask: "Do you have a file for me to process?". The coordinator answers by giving the next available file or saying "no more tasks".
func (c *Coordinator) AssignTask(req *TaskRequest, reply *TaskReply) error { //This function is a method of the Coordinator type, when called via RPC ,Go creates a Coordinator object and call this function on it. This is called by the workers
	c.mu.Lock()         //This locks the coordinator mutex so that only one goroutine (one RPC handler) can change shared data at a time. Only one worker at a time is allowed to access or modify: c.nextFileIndex and c.files. Prevents two workers from getting the same file.
	defer c.mu.Unlock() //when the function finishes run this

	if c.nextFileIndex < len(c.files) { //len(c.files)= how many input files exist, c.nextFileIndex= how many files have already been assigned. Condition that checks "do i still have an unassigned file?"
		reply.FileName = c.files[c.nextFileIndex] //Give the worker the filename, assign the task
		reply.NReduce = c.nReduce                 // tell the worker how many reduce buckets exsists, the worker needs nreduce to know how many intermediate files to create
		reply.TaskID = c.nextFileIndex            // tell the worker which map task number it's working on, this is required so that the worker can correctly name files like mr-0-1

		c.nextFileIndex++ // move to next file for future workers
	} else {
		reply.FileName = "" //if no files are left this tells the worker "I have no more map tasks to give you", alltså no tasks left

	}
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator { //here we pass all the filenames
	c := Coordinator{} //creates an empty coordinator struct

	//initialize fields
	c.files = files     // stores the list of input filenames
	c.nextFileIndex = 0 //This gives files[0] first, sets c.nextFileIndex= 0 (start at the first file)
	c.nReduce = nReduce //save number of reduceTasks for later

	//Starting the RPC server, calls c.server() to start the RPC server
	c.server() //Registers the coordinator with net/rpc, so it creates a unix socket path using CoordinatorSock, it starts a HTTP/RPC server listening on that socket
	return &c
}
