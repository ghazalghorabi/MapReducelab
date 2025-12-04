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
	workers         map[int]string //workers is a map (dictionary) from workerID(int) -> "ip:port" (string address); we need this because when a worker starts it tells the coordinator ("I am listening on address :8001"). Map is how the coordinator remembers where each worker lives (network wise)
	nextWorkerID    int // integer counter, it starts a 0 and each time a new worker registers: we give it nextWorkerID as its ID then increment nextWorkerID++
	mapOwner        []int //mapOwner is a slice, mapOwner[i] tells you which worker ran map task i. Which means: map task i wrote its intermediate files (mr-i-0, mr-i-1..) on that worker's disk. 
	//mapOwner example: mapOwner = []int{2,0,2}, which means: map task 0 -> worker 2, map task 1-> worker 0, map task 2-> worker 0
	//in the advanced version there's no shared filesystem therefor reducers must fetch mr-i-reduceID from the worker created it 

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
					c.mapOwner[i] = -1// reset the owner to -1 meaning: "no worker currently owns this map task"; we do this because the worker that had it might be dead; we no longer trust that it holds valid intermediate files 
				}

			}

		}
		//2. Find an idle map task to give to the worker
		for i, status := range c.mapTasks { //loop through all map tasks again, i=index, status= current TaskStatus
			if status == Idle { //if this map task is not started yet
				c.mapTasks[i] = InProgress     //Mark it as inprogress; about to give it to a worker
				c.mapOwner[i] = args.WorkerID // Record which worker is now responsible for map task i, args.WorkerID is the ID of the worker that called this RPC, so c.mapOwner[i] now means: "this map's intermediate files are stored on this worker's machine"
				c.mapStartTime[i] = time.Now() //store the current time as the start time for this task (used later for the 10 second timeout check)

				reply.TaskType = MapTask  //Tells the worker this task is a map task
				reply.File = c.file[i]   //give the worker the filename to process in this map task
				reply.TaskID = i          //set the ID of this map task, used for naming intermediate files (e.g. mr-<TaskID>-<reduceID>)
				reply.NReduce = c.nReduce //Tell the worker how many reduce tasks exist in total, so the worker knows how many intermediate files to create
				reply.NMap = len(c.file) // Tell the worker how many map tasks exist in total. Mostly used during reduce to know how many mr-X-Y files to read

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
				reply.NMap = c.nMap               //How many map tasks total (NMap), needed so the worker can read mr-<mapID>-<reduceID> from all map 
				reply.Owners = append([]int{}, c.mapOwner...)// // c.mapOwner is a slice where mapOwner[mapTaskID]= workerID; this tells us which worker owns each map's intermediate files 
				//append([]int{}, c.mapOwner...) makes a copy of the slice so the worker gets its own copy, coordinators internal slice wont accidentley be changed by the worker 
				// with reply.Owners, the reduce worker can later do: for each map task m: ownerID := Owners[m], ask coordinator for that worker's address and call WorkerRPC.GetFile on that worker to download mr-m-reduceID

				return nil // done building the reply this reduce task/send it back to the worker and exit the function
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
// summary of RegisterWorker: 
//1.Lock coordinator state
//2. give the worker a new ID (workerID)
//3. save the mapping workerID -> address
//4. increse nextWorkerID for the next worker
//5. tell the worker its WorkerID
//6. unlock and return 
func (c *Coordinator) RegisterWorker(args *RegisterArgs, reply *RegisterReply) error { // this defines a method on the coordinator type
	//RegisterWorker: This is the RPC name workers will call: "Coordinator.RegisterWorker", args *RegisterArgs: the worker sends in a RegisterArgs object (with its address)
	// reply *RegisterReply: The coordinator fills this in to send something back (the workerID)
	c.mu.Lock() // we lock the mutex my of the coordinator, this means only one goroutine can change coordinator state at a time
	defer c.mu.Unlock() // defer means run c.mu when the function ends no matter where it returns

	workerID := c.nextWorkerID // create a local variable WorkerID. set it to c.nextWorkerID, which is the next free ID
	c.workers[workerID] = args.WorkerAdress // c.workers is a map[int]string: workerID -> address. Store the workers adress like:8001 under that worker id. args.WorkerAdress comes from the worker and it tells that its listening on a TCP address
	c.nextWorkerID ++ // increase nextWorkerID by one. So next time another worker calls RegisterWorker gets a new ID
	reply.WorkerID = workerID // fill the reply sent back to the worker. Set the workerID field of the reply. for ex. this means the worker knows ("i am worker number 0")
	return nil //return nil to mean: no error, the rpc completes succesfully 
}	


//summary of GetWorkerAddress
//1. Lock coordinator state 
//2. Look up the address of the given WorkerID in c.workers
//3. Put thatt address into reply.WorkerAddress
func (c *Coordinator) GetWorkerAddress(args *WorkerAddressArgs, reply *WorkerAddressReply) error {
	// metod on coordinator, getWorkerAddress- reducers call: "Coordinator.GetWorkerAddress". Contain the worker ID and reply *workerAddressReply: coordinator will fill with the address string 
	c.mu.Lock() // lock the coordinator mutex, we'll read from c.workers map and maybe write nothing, but lock still is needed to be safe, it prevents races while other RPCs might be changing workers
	defer c.mu.Unlock() // make we unlock when exiting the function 
	reply.WorkerAddress = c.workers[args.WorkerID] // args.WorkerID tells us which worker the caller is interested in. 
	// we look into the map: c.workers
	// we fetch the address: c.workers[workerID]
	// store it in reply.WorkerAddress so the caller (the reduce worker) can read it 
	// ex: args.WorkerID = 2, c.workers[2] = "10.0.0.7:8001", after this line, reply.WorkerAddress = "10.0.0.7:8001"

	if replyWorkerAddress == "" {
		return fmt.Errorf("worker ID %d not found", args.WorkerID) // something is wrong: i dont know the worker ID 
	}
	return nil // If the address wasn't empty, we return nil eg. success. The caller now reads reply.Workeraddress and uses it to connect 

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
	rpc.Register(c) //register coordinator for rpc, expose all methods on c (coordinator) as RPC methods. So methods like: requestTask, reportTask and registerWoker become callabale as "Coordinator.RequestTask" from the workers
	rpc.HandleHTTP() //connects RPC system with the HTTP server, it sets up internal HTTP handlers so that incoming HTTP requests to special paths (like /debug/rpc, /rpc) can be treated as RPC calls
	l, e := net.Listen("tcp", ":1234") // Listen on TCP so workers on other machines can reach this 
	//If your machine’s IP is 172.20.10.5, this means it listens on 172.20.10.5:1234.
	// l is a listener object you can pass to http.Serve, e = error 
	// net.Listen("tcp", ":1234" means: open a TCP port on this machine, port 1234, accept connections from other machines over the network). So now
	//the coordinator is reachable at: "<coordinator-ip>:1234"
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil) // start serving HTTP requests that arrive on listener l, rpc.HandleHTTP() registered the RPC endpoints
	// this connects socket -> http layer -> rpc handler 
	// run this http.Serve in a new gproutine (bakground thread), after this line the coordinator is listening for RPC calls on TCP port 1234
	//basically start an HTTP server that accepts RPC calls over TCP port 1234 and run it in a go routine so the coordinator is free to do other things 
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
	c.file = files     // stores the list of input filenames into the coordinator
	c.nReduce = nReduce //save number of reduceTasks for later
	c.nMap = len(files) //Count how many map tasks we have, we assume 1 file = 1 map task, so just take the length of the files list

	//initialize task status slices (arrays) to track the status of each task
	c.mapTasks = make([]TaskStatus, len(files)) // create a slice of TaskStatus with one entry per map task. The coordinator uses this to know: which tasks are idle (ready to assign), which tasks are InProgress (some worker is doing them) and which are completed.
	c.reduceTasks = make([]TaskStatus, nReduce)// crete a slice of TaskStatus with one entry per reduce task. Reason its needed is for tracking the status of each reduce task.

	//initialize start time slices, these track when each task was given to a worker
	c.mapStartTime = make([]time.Time, len(files)) //One timestamp per map task; asks has this map task been running for more than 10 seconds?
	c.reduceStartTime = make([]time.Time, nReduce) // One timestamo per reduce task

	//inital phase is MapPhase
	c.phase = MapPhase // The job always start in the map phase, when workers ask for tasks, the coordinator will asign only map tasks at first. So sets the current phase of the job to MapPhase. Only map tasks will be given out at the start and no reduce tasks will be started before all maps are done. 

	//initialize MapOwner 
	c.MapOwner = make([]int, len(files)) //creates a slice of int with one entry per map task. mapOwner[i] will store which workerID ran map task i. For ex: mapOwner[0]=2; map task 0 was done by worker 2 
	for i := range c.mapOwner{ //loops over all indices 
		c.mapOwner[i] = -1 //sets the value at each index to -1, the reason for being -1 is because 0 is a valid WorkerID (first worker) and -1 means that no worker has been assigned this map task ywt 
	}


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
