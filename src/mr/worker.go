package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for { //Keep trying to get work from the coordinator
		req := TaskRequest{} //creates an empty RPC request
		reply := TaskReply{} // Will hold the coordinators answer

		createTask := call("Coordinator.AssignTask", &req, &reply) //The RPC call, it tries to call the function Coordinator.AssignTask in the coordinator file

		if !createTask { //Could not reach coordinator: assume that  it's job is done or coordinator is crashed. IF RPC fails
			return
		}

		if reply.FileName == "" { // if there's no file assigned, the worker stops
			return
		}

		runMapTask(mapf, reply.FileName, reply.NReduce, reply.TaskID) // We got a file to process:run the map function on it
		//1. Opens the file, 2. Reads its content, 3.Calls mapf to generate key/value pairs
		// pass NReduce and TaskID to the map task
		return
	}

}

func runMapTask(mapf func(string, string) []KeyValue, filename string, nReduce int, taskID int) { //It takes two arguments mapf and filename (the name of the input file to process): Use mapf on this filename
	file, err := os.Open(filename) // tries to open the file for reading, if it works file is a handle we can use to read from, if it fails err wil be non-nil
	if err != nil {
		log.Fatalf("cannot open %v: %v", filename, err) // if there is an error, this prints an error and crashes the. program
	}
	fileContent, err := io.ReadAll(file) //read the file's content, io.ReadAll(file) reads the entire file into memory as a big []byte
	if err != nil {                      // if reading fails log an error and crash
		log.Fatalf("cannot read %v: %v", filename, err)
	}
	file.Close() // close the file handler

	keyValuePairs := mapf(filename, string(fileContent)) //call the mapfunction and keyValuePairs is []keyValue, a slice of key/value pairs

	log.Printf("worker: map on %v produced %d key/value pairs \n", filename, len(keyValuePairs)) // print how much data the map step produced for this file

	// Splits up the map output into nReduce seperate groups , so that each reduce task gets only the key/value pairs meant for it.

	buckets := make([][]KeyValue, nReduce)   //buckets; slice of slices, this creates nReduce empty boxes and each box will hold the KeyValue pairs for one reduce task
	for _, keyValue := range keyValuePairs { // loop through all key/value pair that the map function produced
		reduce_bucket_index := ihash(keyValue.Key) % nReduce                          // compute which reduce bucket should handle this key
		buckets[reduce_bucket_index] = append(buckets[reduce_bucket_index], keyValue) //take the key/value pair and drop it into the correct bucket
	}

	for reduce_bucket_index := 0; reduce_bucket_index < nReduce; reduce_bucket_index++ { // reduce_bucket_index goes from 0 to nReduce-1, for each reduce_bucket_index we will write one file that contains all keyValues for that reduce task
		tmpFile, err := os.CreateTemp("", "mr-temp-*") //to avoid someone reading half-written files if the worker crashes mid-write
		if err != nil {
			log.Fatalf("Cannot create temp file: %v", err)
		}

		enc := json.NewEncoder(tmpFile)                         // Write key/value pairs as JSON lines into the temp file
		for _, keyValue := range buckets[reduce_bucket_index] { //Write all key/value pairs of this bucket
			if err := enc.Encode(&keyValue); err != nil {
				log.Fatalf("cannot encode kv, %v", err)
			}
		}
		tmpFile.Close() // close the temp file; we finish writing

		finalName := fmt.Sprintf("mr-%d-%d", taskID, reduce_bucket_index) //rename the temp file to finalName
		if err := os.Rename(tmpFile.Name(), finalName); err != nil {
			log.Fatalf("Cannot rename temp file, %v", err)
		}
	}

	log.Printf("worker: map on %v produced %d key/value pairs into %d buckets\n", filename, len(keyValuePairs), nReduce) // after finishing all buckets print this log message
}

func runReduceTask(reducef func(string, []string) string, reduceID int, nMap int) { //nmap = how many map tasks existed (to know how many mr-i-reduceID files to read)

	var allKeyValues []KeyValue //read all intermediate files for this reduceID, we'll gather all key/value pairs into one big slice

	for mapTaskID := 0; mapTaskID < nMap; mapTaskID++ {
		filename := fmt.Sprintf("mr-%d-%d", mapTaskID, reduceID)
	}

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
