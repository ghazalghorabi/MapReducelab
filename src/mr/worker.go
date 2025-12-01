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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		arg := RequestTaskArgs{}    //worker: im ready give a task
		reply := RequestTaskReply{} //coordinator: here is your task with details

		//worker calls request task method in coordinator
		//worker: here are my args, fill in reply with task details
		ok := call("Coordinator.RequestTask", &arg, &reply)
		if !ok {
			return //coordinator died -> worker exits
		}
		//debugging info: shows that coordinator told worker what to do
		fmt.Println("worker received TaskType: ", reply.TaskType)
		switch reply.TaskType {
		case MapTask:
			//do map task
			fmt.Println("worker doing map task", reply.TaskID, "on file", reply.File)
			if err := doMapTask(mapf, reply.File, reply.TaskID, reply.NReduce); err != nil {
				fmt.Printf("doMapTask failed: %v\n", err)
			}
			//after finishing the task, report to coordinator
			doneArgs := ReportTaskArgs{
				TaskType: MapTask,
				TaskID:   reply.TaskID,
			}
			doneReply := ReportTaskReply{}
			//worker tells coordinator: im done with this task
			call("Coordinator.ReportTask", &doneArgs, &doneReply)

		case ReduceTask:
			//do reduce task
			fmt.Println("worker doing reduce task")
			if err := doReduceTask(reducef, reply.TaskID, reply.NMap); err != nil {
				fmt.Printf("doReduceTask failed: %v\n", err)
			}
			//after finishing the task, report to coordinator
			doneArgs := ReportTaskArgs{
				TaskType: ReduceTask,
				TaskID:   reply.TaskID,
			}
			doneReply := ReportTaskReply{}
			//worker tells coordinator: im done with this task
			call("Coordinator.ReportTask", &doneArgs, &doneReply)

		case WaitTask:
			//dont have a task, wait and ask again
			time.Sleep(300 * time.Millisecond) //wait a bit before asking again
			continue
		case ExitTask:
			return //job done, worker exits
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

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
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "172.20.10.5:1234")
	//sockname := coordinatorSock()
	//c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// running a map task
func doMapTask(mapf func(string, string) []KeyValue, filename string, taskID int, nReduce int) error {
	// read file contents
	content, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	// apply map function
	kva := mapf(filename, string(content))

	// create intermediate files for each reduce task
	intermediateFiles := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		intermediateFileName := fmt.Sprintf("mr-%d-%d", taskID, i)
		file, err := os.Create(intermediateFileName)
		if err != nil {
			return err
		}
		intermediateFiles[i] = file
		encoders[i] = json.NewEncoder(file)
	}

	// hash each key to a reduce bucket and write as JSON
	for _, kv := range kva {
		reduceTaskNum := ihash(kv.Key) % nReduce
		err := encoders[reduceTaskNum].Encode(&kv)
		if err != nil {
			return err
		}
	}

	// close all intermediate files
	for _, file := range intermediateFiles {
		file.Close()
	}

	return nil
}

// running a reduce task
func doReduceTask(reducef func(string, []string) string, taskID int, nMap int) error {
	intermediate := []KeyValue{}

	// read intermediate files from all map tasks
	for i := 0; i < nMap; i++ {
		intermediateFileName := fmt.Sprintf("mr-%d-%d", i, taskID)
		file, err := os.Open(intermediateFileName)
		if err != nil {
			continue
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// sort intermediate key-value pairs by key
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

	// apply reduce function to each key and write to output file
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
