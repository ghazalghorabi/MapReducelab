package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
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
	//these functions are loaded from the.so plug in

	for { //Keep trying to get work from the coordinator
		arg := RequestTaskArgs{}    //arg -> the arguments we send to the coordinator when asking for a task; type RequestTaskArgs
		reply := RequestTaskReply{} // Will hold the coordinators answer; type RequestTaskReply; it will contain things like: taskType, File, TaskID etc.

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
			if err := runReduceTask(reducef, reply.TaskID, reply.NMap); err != nil { // call doReduceTask with reducef (the users reduce function), reply.TaskID (which reduce task ID to run), reply.NMap (total number of map tasks)
				fmt.Printf("doReduceTask failed: %v\n", err) // if doReuceTask returns an error, inform user
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
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()            //worker learns the socket path, this returns something like: /var/tmp/5840-mr-501
	c, err := rpc.DialHTTP("unix", sockname) //worker dials the coordinator, this is the exact moment where the worker connects to the UNIX domain socket, establishes an RPC channel and is able to communicate with the coordinator
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply) // worker sends a rpc request
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
