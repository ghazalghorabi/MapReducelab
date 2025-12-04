package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type WorkerRPC struct{} // new type called WorkerRPC; we need this so the RPC system works with methods on a type. This is the object that exposes RPC functions for this worker. We want to define RPC functions that other workers can call on this worker

// when another worker asks GetFile("mr-X-Y") this method read that file from local disk and sends back its bytes in reply.Data
func (w *WorkerRPC) GetFile(args *GetFileArgs, reply *GetFileReply) error { // w*WorkerRPC this makes it a method on the WorkerRPC type, w is the reciever, GetfFile--> name of the RPC method and args *GetFileArgs is the imput from the caller (another wprker)
	//reply *GetFileReply: the output we send back, GetFileReply has a field Data[] byte (the file contents)
	//this method will be called remotely as: "WorkerRPC.GetFile" by another worker
	data, err := os.ReadFile(args.File) // os.ReadFile: reads the entire file from disk into memory, args.Fule is the filename the caller requested: (eg: mr-0-1), data will be a []byte containing the file's contents. This line basically reads the requested file from the local disk
	if err != nil {                     //check if something went wrong when reading the file
		return err //return error to the caller
	}
	reply.Data = data // if no error happend, we reach this line and store the file's bytes(data) into the reply struct. reply.Data is what the caller will recieve
	return nil        // nil meaning no error, rpc completes succesfully
}

// this function starts a small RPC server on the worker so that other workers can call WorkerRPC.GetFile to download intermediate files
func workerServer(adress string) { //function takes one parameter named adress, this is the TCP address to listen on, Ex: ':8001', "localhost: 9000"
	rpc.Register(new(WorkerRPC)) //new(WorkerRPC) creates a pointer to a WorkerRPC struct, rpc.Register(...) tells Go's RPC system: expose all exported methods of this object as RPC methods. We previosuly defined: func (w *WorkerRPC) GetFile(...),
	// Because of rpc.Register(new(WorkerRPC)), other processes can now call: "WorkerRPC.GetFile" on this worker via RPC: This line hooks up GetFile method on the RPC system
	rpc.HandleHTTP()                  // sets up RPC to be served over HTTP, it configures default HTTP server so that RPC requests are handled under the hood. So combining this with network listening ==> HTTP + RPC over TCP
	l, e := net.Listen("tcp", adress) // Opens a TCP listening socket on the given address. Address might be: "8801". which means listen on port "8801" on all local interfaces.
	//l= listener object, e= any error that occured
	if e != nil { //check if there was an error in net.Listen
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil) // starts an HTTP server that: accepts connections on the listener: l, routes requests to handlers (including the RPC handler set up by rpc.HandleHTTP())
	// go runs in a seperate go routine so the server runs in the background, the worker can keep doing other things (like requesting tasks and running map/reduce code)
	//menas basically in the background, handle incoming HTTP+RPC connections on this address

}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
// the worker reapeatedly:
/*
1. Asks for a task
2. does the work based on task type
3. reports completion (for map or reduce)
4. waits if there's nothing to do
5. exits when told
*/
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) { // this defines the worker function, it takes two arguments
	// mapf--> a function the worker can call for map tasks
	//reducef --> a function the worker can call for reduce tasks
	//these functions are loaded from the.so plug
	workerID := -1          // create a local variable workerID, set it to -1 to indicate that you dont have an ID yet, after we register with the coordinator, this will be set to 0,1,2 etc
	workerAdress := ":8001" // this is the tcp address where this worker will listen for RPC calls, ":8001" means listen on port 8001 on this machine

	//start worker RPC server
	go workerServer(workerAdress) //Starts an RPC server on this worker listening on ":8001", it also exposes methods like WorkerRPC.GetFile, go runs it in a goroutine so the server runs in the background and the worker can continue doing its normal work

	//register with coordinator
	args := RegisterArgs{WorkerAdress: workerAdress} // we create a RegisterArgs struct, WorkerAddress: workerAdress means: tell the coordinator i am listening at ":8001"
	reply := RegisterReply{}                         // Create an empty RegisterReply struct, the coordinator will fill this with the assigned worker ID

	call("Coordinator.RegisterWorker", &args, &reply) // make an RPC call to coordinator.RegisterWorker, send the address in args and the coordinator saves this address ij its workers map and assigns the worker new worker ID.
	workerID = reply.WorkerID                         // write that workerID into reply.WorkerID, store the returned WorkerID in the workerID variable
	//reason we use tgis: later when we require a task, we send this workerID and the coordinator uses workerID to update mapOwner[taskID]. That's how the system know which worker created which intermediate files

	for { //Keep trying to get work from the coordinator
		arg := RequestTaskArgs{WorkerID: workerID} //arg -> the arguments we send to the coordinator when asking for a task; type RequestTaskArgs, tell coordinator which workerID so the coordinator knows whcih worker is asking for work so it can set mapOwner[mapTaskID] = workerID when it gives a map task
		reply := RequestTaskReply{}                // Will hold the coordinators answer; type RequestTaskReply; it will contain things like: taskType, File, TaskID etc.

		ok := call("Coordinator.RequestTask", &arg, &reply) //The RPC call, it tries to call the function Coordinator.RequestTask method on the coordinator object, &arg--> send a pointer to our request arguemnts, &reply --> the coordinator writes its response into this struct
		//ok is a boolean: true if the RPC call succeeded and false if it failed (eg. coordinator exited/crashed)

		if !ok { //Could not reach coordinator: assume that  it's job is done or coordinator is crashed. IF RPC fails
			return
		}

		fmt.Println("worker received TaskType: ", reply.TaskType)
		switch reply.TaskType { // now we look at reply.TaskType to see what kind of task the coordinator gave. Tasktype can be one of: MapTask, ReduceTask, WaitTask and ExitTask
		case MapTask:
			fmt.Println("worker doing map task", reply.TaskID, "on file", reply.File)
			if err := doMapTask(mapf, reply.File, reply.TaskID, reply.NReduce); err != nil { // call doMapTask to actually do the work, mapf --> the user's map function, reply.File --> which file to read, reply.TaskID --> map task ID (used for naming files), reply.NReduce--> tital number of reduce tasks (for bucket count)
				fmt.Printf("doMapTask failed: %v\n", err)
			}

			doneArgs := ReportTaskArgs{ // create a ReportTaskArgs struct called doneArgs, this will be sent back to the coordinator to say "i finished this task"
				TaskType: MapTask,      // this was a mapTask
				TaskID:   reply.TaskID, //the ID of the map task thats just processed.
			}

			doneReply := ReportTaskReply{}                        // empty struct because the coordinator does not need to send anything back when a worker reports task completion
			call("Coordinator.ReportTask", &doneArgs, &doneReply) //make an RPC call to coordinator.reporttask; which includes the arguments: &doneArgs --> I completed map Task X and &doneReply: place to recevive any response;
		//after this, the mapTask is done and the worker goes back to the top of the for loop to ask for more work

		case ReduceTask:
			fmt.Println("worker doing reduce task")
			if err := runReduceTask(reducef, reply.TaskID, reply.NMap, reply.Owners); err != nil { // call doReduceTask with reducef (the users reduce function), reply.TaskID (which reduce task ID to run), reply.NMap (total number of map tasks)
				fmt.Printf("runReduceTask failed: %v\n", err) // if doReuceTask returns an error, inform user
			}

			doneArgs := ReportTaskArgs{
				TaskType: ReduceTask,
				TaskID:   reply.TaskID,
			}
			doneReply := ReportTaskReply{}
			call("Coordinator.ReportTask", &doneArgs, &doneReply) // call coordinator.ReportTask to notify the coordinatior whoch reduce task number is finished

		case WaitTask: //if TaskType is WaitTask, the coordinator is saying that there is no work for the worker and that it sholuld come back later
			time.Sleep(300 * time.Millisecond) // the worker sleeps for 0.3 seconds, this prevents hammering the coordinator with constant requests
			continue                           //jumps back to the top of the for loop, so the worker will call RequestTask again after the sleep

		case ExitTask: //coordinator says job is done; exit. If the TaskType is ExitTask, the entire MapReduce job is finished
			return // exits the worker function and the wroker process will exit shortly after

		}

	}

}

func doMapTask(mapf func(string, string) []KeyValue, filename string, taskID int, nReduce int) error { //It takes two arguments mapf and filename (the name of the input file to process): Use mapf on this filename
	content, err := os.ReadFile(filename) // this read the entire filename into memory, content is a []byte (a slice of bytes), err is nil if the read was succesful, otherwise it describes what went wrong
	if err != nil {                       // if reading the file failes (for ex. the file doesnt exist or permission denied), return an error to the caller
		return err //this stops the function early, we can't map over a file we can't read
	}

	//apply the map function
	kva := mapf(filename, string(content)) //kva: KeyValue array, its type is []KeyValue
	//string(content), convert the file bytes into a Go string(text), mapf(filename, string(content)); call the user-defined map function for ex: wc.Map; mapf lloks at the text and produces key/value pairs
	// for wc, each word becomes a KeyValue{Key: "word", Value: "1"}

	//create intermediateFiles for each reduce task
	intermediateFiles := make([]*os.File, nReduce) // create a slice (array) of length nReduce, each element will eventuelly be a *os.File pointe, so intermediateFiles[0] is the file for reduce task 0
	encoders := make([]*json.Encoder, nReduce)     // create a slice of *json.Encoder with the length nReduce, this is used to encode key/value pairs as JSON into each file

	for i := 0; i < nReduce; i++ { //loop variable i goes from 0 up to nReduce-1, each i represents one reduce task number
		intermediateFileName := fmt.Sprintf("mr-%d-%d", taskID, i) // so we can build a string like: "mr-0-0", we store it in intermediateFileName
		file, err := os.Create(intermediateFileName)               // create a file with that name; file-> a handle we can write to
		if err != nil {
			return err
		}
		intermediateFiles[i] = file         // store the file handle in the slice so we can close it later
		encoders[i] = json.NewEncoder(file) // create a JSON encoder that writes directly to that file, when calling encoders[i].Encode(&kv), it writes JSON into that file
	}

	//partition key/value pairs into reduce buckets
	for _, kv := range kva { // loop through all key/value pairs produced by mapf. kv is a keyValue with kv.Key and kv.Value
		reduceTaskNum := ihash(kv.Key) % nReduce // ihash(kv.Key)= apply a hash function to the key, same key --> always same hash
		// %nReduce; take that hash and do modulo nReduce; that gives a number in [0, nReduce-1]: we store this in reduceTaskNum
		err := encoders[reduceTaskNum].Encode(&kv) //encoders[reduceTaskNum] selects the JSON encoders for that bucket's file, .Encode(&kv) writes the KeyValue as one JSON object into the file
		if err != nil {                            //if writing fails return the error and stop
			return err
		}

	}

	//close all intermediate files
	for _, file := range intermediateFiles { // Loop over all file handles stored in intermediateFiles
		file.Close() // call file.Close() on each one
		//reason to do it is to flush all buffered data to disk, free OS resources and to make sure that everything is properly written before reducer reads these files
	}

	return nil // no errors happend, thus map task completed successfully from the worker's perspective
}

func runReduceTask(reducef func(string, []string) string, reduceID int, nMap int) error { // runs one reduce task, reducef- the user's reduce function (eg. wc.Reduce), nmap = how many map tasks existed (to know how many mr-i-reduceID files to read)
	intermediate := []KeyValue{} //create an empty slice to store all key/value pairs that belongs to this reduce task. We’ll fill it by reading mr-<mapID>-<taskID> from every map

	//read intermediate files from all map tasks
	for i := 0; i < nMap; i++ { //loop i from 0 to nMap-1, each i is a map task ID
		intermediateFileName := fmt.Sprintf("mr-%d-%d", i, reduceID) // build the filename that this reduce task must read from map i. Example: mr-0-2, mr-1-2, mr-2-2 for taskID = 2
		file, err := os.Open(intermediateFileName)                   // open that intermediate file for reading
		if err != nil {                                              // if it fails (file missing, permissions, etc.), return the error immediately
			return err
		}

		decoder := json.NewDecoder(file) //create a JSON decoder that will read KeyValue structs line by line from this file
		for {                            //infinite loop to read all JSON objects in the file
			var kv KeyValue                             //allocate a KeyValue to fill
			if err := decoder.Decode(&kv); err != nil { //try to decode the next JSON value into kv, if it returns an error (usually EOF) break out of the loop
				break
			}
			intermediate = append(intermediate, kv) //if decoding worked, append kv to intermediate
		} //after this inner loop, ve have read all key/value pairs generated by map task i for this reducer and added them to intermediate
		file.Close() // close the file when done reading
	} // intermediate contains all KeyValues from all map tasks that belong to this reducer (taskID

	//sort intermediate key-value pairs by key
	sort.Slice(intermediate, func(i, j int) bool { //sort.Slice sorts the intermediate slice in-place
		return intermediate[i].Key < intermediate[j].Key //the comparison function says: element i should come before j if intermediate[i].Key is lexicographically < intermediate[j].Key
	}) //after this all enteries with the same key are next to each other in the slice whoich makes it easy to group by key

	// create output file mr-out-taskID
	outputFileName := fmt.Sprintf("mr-out-%d", reduceID) // build the output filename for this reduce task, Example: mr-out-0, mr-out-1, etc
	outputFile, err := os.Create(outputFileName)         //create the output file
	if err != nil {                                      //if it fails return the error
		return err
	}
	defer outputFile.Close() // schedule outputFile.Close() to run when the function returns, this makes sure the file is closed even if we exit early due to error later

	//apply reduce function to each key and write to output file
	i := 0                      //start index at the beginning of the intermediate slice
	for i < len(intermediate) { // loop until we've processed all elements
		j := i + 1 // j starts at the index after i
		// Move j forward as long as:
		// 1) j is still inside the slice, and 2) the key at j is the same as the key at i
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++ // move j to the right
		}

		// Now all entries from index i up to (j-1) have the SAME key
		// We'll collect all their values into a slice

		values := []string{}     // start with an empty list of values
		for k := i; k < j; k++ { //go through all indices from i up to j-1
			values = append(values, intermediate[k].Value) // collect the value
		}

		// Now 'values' contains all the values for key 'intermediate[i].Key'.
		// Call the user-defined reduce function with this key and all its values
		reducedValue := reducef(intermediate[i].Key, values)
		fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, reducedValue) // write "key reducedValue" to the final output file
		// Move i to j to skip over this whole group of equal keys
		// Next loop will process the next distinct key (if any)
		i = j

	}
	return nil
}

// TODO:
func doReduceTask(reducef func(string, []string) string, taskID int, nMap int, owners []int) error {
	intermediate := []KeyValue{}

	// read intermediate files from all map tasks
	for i := 0; i < nMap; i++ {
		ownerID := owners[i]
		if ownerID == -1 {
			continue // map task not completed yet (shouldn’t normally happen if coordinator transitions correctly)
		}

		args := GetFileArgs{
			File: fmt.Sprintf("mr-%d-%d", i, taskID),
		}
		fileReply := GetFileReply{}

		workerAddress := getWorkerAddress(ownerID)
		ok := callWorkerRPC("WorkerRPC.GetFile", workerAddress, &args, &fileReply)
		if !ok {
			// failed to reach owning worker; skip or maybe retry
			continue
		}

		dec := json.NewDecoder(bytes.NewReader(fileReply.Data))
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	// sort by key
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	// create output file mr-out-taskID
	outputFileName := fmt.Sprintf("mr-out-%d", taskID)
	outputFile, err := os.Create(outputFileName)
	if err != nil {
		return err
	}
	defer outputFile.Close()

	// run reducef on each key group and write output
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		reducedValue := reducef(intermediate[i].Key, values)
		fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, reducedValue)

		i = j
	}
	return nil
}

//TODO

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool { //worker connects to the socket and sends RPC calls
	coordinatorAddress := "172.20.10.5:1234"          //network address of the coordinator, connect to IP 172.20.10.5 (the coordinator machine) on port 1234 (where the server() is listening)
	c, err := rpc.DialHTTP("tcp", coordinatorAddress) //this tries to open a connection to the coordinators RPC server, we use TCP as the protocol and coordinatorAddress → host:port to connect to, so it basivally opens a TCP connection to the coordinatorAddress. It expects an HTTP + RPC server on the other side (which Coordinator.server() started). c is the RPC client connection
	if err != nil {                                   // on success c is an RPC client object
		fmt.Println("Coordinator unreachable, assuming failure.")
		return false //return false from call
	}
	defer c.Close() // closes the network connection

	err = c.Call(rpcname, args, reply) // worker sends a rpc request, rpcname is something like: "Coordinator.RequestTask", args = request struct pointer and reply is a pointer to where the coordinator will write the response
	if err == nil {                    // if there was no errors return true; The caller (worker code) knows: “RPC call worked, reply is filled
		return true
	}

	fmt.Println(err) // something went wrong while calling the RPC
	return false
}

func callWorkerRPC(rpcname string, workerAddress string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", workerAddress)
	if err != nil {
		return false
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply)
	return err == nil
}

func getWorkerAddress(workerID int) string {
	args := WorkerAddressArgs{WorkerID: workerID}
	reply := WorkerAddressReply{}
	call("Coordinator.GetWorkerAddress", &args, &reply)
	if reply.WorkerAddress == "" {
		fmt.Printf("Worker ID %d address not found\n", workerID)
	}
	return reply.WorkerAddress
}
