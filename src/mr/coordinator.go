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

//creates a new type called TaskStatus. Its used to decribe task state

const (
	//Three possible values for a task's status
	Idle       TaskStatus = iota // The task has not yet started, no worker is working on it
	InProgress                   // a worker is currently working on this task
	Completed                    // a worker finished this task succesfully

)

type Phase int

//creates another integer like type called Phase that's used to track which stage the entire MapReduce job is in

const (
	//three stages of a mapreduce job
	MapPhase    Phase = iota //The coordinator is still running map tasks, reduce tasks must wait until maps are done
	ReducePhase              //All map tasks are completed, now workers can reduce tasks
	DonePhase                // All map- and reduce tasks are finished. Workers should be told to quit
)

type Coordinator struct { //shared state object that coordinates who gets which file
	mu              sync.Mutex   //protects shared state (multiple workers can call at the same time), mutex stored in mu(= a lock)
	files           []string     //List of input files (one per task)
	nReduce         int          //number of reduce tasks that's provided by the test harness
	mapTasks        []TaskStatus // A list that stores the status of every task, ex of three map tasks: [Idle, InProgess, Completed]
	reduceTasks     []TaskStatus // A list that stores the status of every reduce task.
	phase           Phase        //Stores which stage the job is in: MapPhase, ReducePhase or DonePhase. The coordinator reads this value to decide what kind of work to give workers
	nMap            int          //Number of map tasks, usually equal to len(file)
	mapStartTime    []time.Time  // A list that stores when each map task was assigned, that's used to detect worker timeout
	reduceStartTime []time.Time  // A list that stores when each reduce task was assigned, used to reassign reduce task if a worker crashes during reduce

}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error { // a method named RequestTask on Coordinator. It matches the RPC signature expected by Go: Args -> input from worker and reply-> what we send back to the worker
	c.mu.Lock()         //lock the mutex, only one goroutine (worker request) can modify coordinator state at a time
	defer c.mu.Unlock() // guarantees that the lock is releasted at the end of the function, even if we return early

	//Handle MAP phase
	if c.phase == MapPhase { //check if it is currently in map phase, if yes:
		//1. Reclaim any map tasks that have timed out
		for i := range c.mapStartTime { //start a loop over all map tasks, i is the index of the map task
			if c.mapTasks[i] == InProgress { // for each map task i, check if its status is currently running (InProgess)
				if time.Since(c.mapStartTime[i]) > 10*time.Second { // Check how long this task i has been running, time.Since(c.mapStartTime[i]) = how much time has passed since we gave it to a worker, if it's been more than 10 seconds --> worker crashed/too slow
					log.Printf("Reassigning map task %d due to timeout\n", i)
					c.mapTasks[i] = Idle //mark that map task back to Idle, means also that some other worker can get it now
				}

			}

		}
		//2. Find an idle map task to give to the worker
		for i, status := range c.mapTasks { //loop through all map tasks again, i=index, status= current TaskStatus
			if status == Idle { //if this map task is not started yet
				c.mapTasks[i] = InProgress     //Mark it as inprogress; about to give it to a worker
				c.mapStartTime[i] = time.Now() //store the current time as the start time for this task (used later for the 10 second timeout check)

				reply.TaskType = MapTask  //Tells the worker this task is a map task
				reply.File = c.files[i]   //give the worker the filename to process in this map task
				reply.TaskID = i          //set the ID of this map task, used for naming intermediate files (e.g. mr-<TaskID>-<reduceID>)
				reply.NReduce = c.nReduce //Tell the worker how many reduce tasks exist in total, so the worker knows how many intermediate files to create
				reply.NMap = len(c.files) // Tell the worker how many map tasks exist in total. Mostly used during reduce to know how many mr-X-Y files to read

				//done preparing the reply
				return nil //exit the function, send this reply back to the worker via RPC

			}
		}

		//No map tasks ready; ask the worker to wait
		reply.TaskType = WaitTask // no idle map tasks were found, some may still be InProgess but not done, so we tell the worker waitTask
		return nil
	}

	// Handle reduce phase
	if c.phase == ReducePhase { // now we handle the case when all map tasks are done and we're in the reduce phase
		//1.Reclaim any reduce tasks that have timed out
		for i := range c.reduceStartTime { //loop over all reduce tasks by index i
			if c.reduceTasks[i] == InProgress { //check if reduce task i is currently running
				if time.Since(c.reduceStartTime[i]) > 10*time.Second { //if this reduce task have been running more than 10 seconds
					fmt.Println("Reassigning reduce task", i, "due to timeout")
					c.reduceTasks[i] = Idle //mark it back to Idle so another worker can take it
				}

			}

		}

		//2. Find an idle reduce task to give to the worker
		for i, status := range c.reduceTasks { //loop over all reduce tasks
			if status == Idle { //if we find one that is still idle
				c.reduceTasks[i] = InProgress     // Mark it as in progress
				c.reduceStartTime[i] = time.Now() // Record current time as its start time
				reply.TaskType = ReduceTask       // Tell the worker that this is a reduce task
				reply.TaskID = i                  // set the reduce task ID
				reply.NReduce = c.nReduce         // tell he worker how many reduce tasks total (NReduce)
				reply.NMap = c.nMap               //How many map tasks total (NMap), needed so the worker can read mr-<mapID>-<reduceID> from all map tasks

				return nil // reply to the worker and exit the function
			}
		}

		//No reduce tasks ready; ask the worker to wait
		reply.TaskType = WaitTask //If no reduce tasks were found, tell the worker to wait (waitTask)
		return nil
	}

	//Handle Done phase
	if c.phase == DonePhase { // this means that all map and reduce tasks are complete
		reply.TaskType = ExitTask // we tell the worker ExitTask, (the job is finished)
		return nil
	}

	//Fallback
	reply.TaskType = WaitTask //This is a safety fallback ig somehow none of the above if's are triggered, default behaviour; tell the worker to wait
	return nil                //then exit
}

// ReportTask; state machine brain of the coordinator; checks whether all tasks of a type is now done
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error { // this defines a method called ReportTask on the Coordinator type, it matches the RPC handler (args-> what the worker sends (which task, type) and reply -> what coordinator sends back (empty here))
	c.mu.Lock()         // locks the coordinator's mutex, this prevents two worker from updating task status at the same time; protects mapTasks, reduceTasks, phase etc
	defer c.mu.Unlock() // guarantee we unlock at the end of the function, even if we return early

	if args.TaskType == MapTask { // Check: was the completed task a map task? args.TaskType was sent by the worker (either MapTask or ReduceTask), MapPhase -> ReducePhase
		c.mapTasks[args.TaskID] = Completed // mark this map task as Completed in c.mapTasks, args.TaskID is the task index, before this it was InProgess

		//Check if all map tasks are done
		allDone := true                     // we want to know:("have all map tasks finished now?"). start ny assuming allDone = true, if we find any map task that is NOT completed, we'll flip this to false
		for _, status := range c.mapTasks { //loop over all map task statuses, status is each item from c.mapTasks
			if status != Completed { // if we found any status that is not completed

				allDone = false //set allDone to false
				break           //stop looping early, no need to check further
			}
		}

		if allDone && c.phase == MapPhase { // we check two things: if allDone; if all map tasks are complete and c.phase == MapPhase (if we are currently in map phase). Only if both are true we switch to reduce phase
			c.phase = ReducePhase // move the overall job to the reducePhase this means that from now on, when workers ask for tasks, the coordinator will assign reduce tasks not map tasks
			fmt.Println("All map tasks completed. Moving to Reduce Phase.")
		}

	}

	//Handles when workers report reduce tasks:
	if args.TaskType == ReduceTask { // enter if the completed task is a reduce task
		c.reduceTasks[args.TaskID] = Completed //mark this reduce task as completed in c.reduceTasks

		//Check if all reduce tasks are done
		allDone := true                        // we want to know are all reduce tasks done now?. Start assuming allDone = true
		for _, status := range c.reduceTasks { // Loop over all reduce task statuses
			if status != Completed { // if any reduce task is not completed
				allDone = false // set allDone to false
				break
			}

		}

		if allDone && c.phase == ReducePhase { // if all reduce tasks are done and we are currently in reducephase --> then we can say that the whole job is done
			c.phase = DonePhase //Set the coordinator's phase to done phase. This means: All map tasks: Done, no more work exists and Done() should return True, future RequestTask calls will return ExitTask to workers
			fmt.Println("All reduce tasks completed. Job Done.")
		}
	}
	return nil //return nil error -> RPC succeded, workers "Im done" report has been succesfully processed

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
func (c *Coordinator) server() { //defines a method called server that eblongs to the coordinator type 
	rpc.Register(c) //register coordinator for rpc
	rpc.HandleHTTP() //connects RPC system with the HTTP server, it sets up internal HTTP handlers so that incoming HTTP requests to special paths (like /debug/rpc, /rpc) can be treated as RPC calls
	l, e := net.Listen("tcp", ":1234") // Listen on TCP so workers on other machines can reach this 
	//If your machine’s IP is 172.20.10.5, this means it listens on 172.20.10.5:1234.
	// l is a listener object you can pass to http.Serve, e = error 
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil) // start serving HTTP requests that arrive on listener l, rpc.HandleHTTP() registered the RPC endpoints
	// this connects socket -> http layer -> rpc handler 
	// run this http.Serve in a new gproutine (bakground thread), after this line the coordinator is listening for RPC calls on TCP port 1234

}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator { //here we pass all the filenames
	c := Coordinator{} //creates an empty coordinator struct, c is the coordinator variable

	//initialize worker registry
	c.workers = make(map[int]string) // creates an empty map from int to string. WorkerID -> "ip:port" 
	c.nextWorkerID = 0 // start the worker ID counter at 0


	//initialize fields
	c.files = files     // stores the list of input filenames into the coordinator
	c.nReduce = nReduce //save number of reduceTasks for later
	c.nMap = len(files) //Count how many map tasks we have, we assume 1 file = 1 map task, so just take the length of the files list

	//initialize task status slices (arrays) to track the status of each task
	c.mapTasks = make([]TaskStatus, len(files)) // create a slice of TaskStatus with one entry per map task
	c.reduceTasks = make([]TaskStatus, nReduce)

	//initialize start time slices, these track when each task was given to a worker
	c.mapStartTime = make([]time.Time, len(files)) //One timestamp per map task; asks has this map task been running for more than 10 seconds?
	c.reduceStartTime = make([]time.Time, nReduce) // One timestamo per reduce task

	//inital phase is MapPhase
	c.phase = MapPhase // The job always start in the map phase, when workers ask for tasks, the coordinator will asign only map tasks at first

	//Starting the RPC server for the coordinator, calls c.server() to start the RPC server
	c.server() //Registers the coordinator with net/rpc, so it creates a unix socket path using CoordinatorSock, it starts a HTTP/RPC server listening on that socket
	return &c  //return a pointer to the coordinator that's been set up, the caller (mrcoordinator.go) will use this coordinator to run the job
}
func (c *Coordinator) RegisterWorker(args *RegisterArgs, reply)
func (c *Coordinator) Done() bool { // c is pointer to a coordinator instance, this is called by mrcoordinator.go to check if the entire MapReduce Job is finished

	c.mu.Lock()         // Exclusive access to the coordinator's data. No other goroutine should change while its inside done
	defer c.mu.Unlock() // run c.mu.Unlock() at the end of this function

	if c.phase == DonePhase { //check whether the coordinator's phase is DonePhase
		return true //if yes return true
	}
	return false // if not return false
}
