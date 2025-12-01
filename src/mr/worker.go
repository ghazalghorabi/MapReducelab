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

var myID int

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type WorkerRPC struct{}

// RPC: allows coordinator to get file contents from worker
func (w *WorkerRPC) GetFile(args *GetFileArgs, reply *GetFileReply) error {
	data, err := os.ReadFile(args.File)
	if err != nil {
		return err
	}
	reply.Data = data
	return nil
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func workerServer(adress string) {
	rpc.Register(new(WorkerRPC))
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", adress)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerID := -1
	workerAdress := ":8001"
	go workerServer(workerAdress)

	args := RegisterArgs{
		WorkerAdress: workerAdress,
	}
	reply := RegisterReply{}
	call("Coordinator.RegisterWorker", &args, &reply)
	workerID = reply.WorkerID

	// Your worker implementation here.
	for {
		arg := RequestTaskArgs{WorkerID: workerID}
		reply := RequestTaskReply{}

		ok := call("Coordinator.RequestTask", &arg, &reply)
		if !ok {
			return
		}

		fmt.Println("worker received TaskType: ", reply.TaskType)
		switch reply.TaskType {
		case MapTask:
			fmt.Println("worker doing map task", reply.TaskID, "on file", reply.File)
			if err := doMapTask(mapf, reply.File, reply.TaskID, reply.NReduce); err != nil {
				fmt.Printf("doMapTask failed: %v\n", err)
			}
			doneArgs := ReportTaskArgs{
				TaskType: MapTask,
				TaskID:   reply.TaskID,
			}
			doneReply := ReportTaskReply{}
			call("Coordinator.ReportTask", &doneArgs, &doneReply)

		case ReduceTask:
			fmt.Println("worker doing reduce task")
			if err := doReduceTask(reducef, reply.TaskID, reply.NMap, reply.Owners); err != nil {
				fmt.Printf("doReduceTask failed: %v\n", err)
			}
			doneArgs := ReportTaskArgs{
				TaskType: ReduceTask,
				TaskID:   reply.TaskID,
			}
			doneReply := ReportTaskReply{}
			call("Coordinator.ReportTask", &doneArgs, &doneReply)

		case WaitTask:
			time.Sleep(300 * time.Millisecond)
			continue
		case ExitTask:
			return
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

func call(rpcname string, args interface{}, reply interface{}) bool {
	coordinatorAddress := "172.20.10.5:1234"
	c, err := rpc.DialHTTP("tcp", coordinatorAddress)
	//sockname := coordinatorSock()
	//c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		fmt.Println("Coordinator unreachable, assuming failure.")
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
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

func doReduceTask(reducef func(string, []string) string, taskID int, nMap int, owners []int) error {
	intermediate := []KeyValue{}

	// read intermediate files from all map tasks
	for i := 0; i < nMap; i++ {
		ownerID := owners[i]
		if ownerID == -1 {
			continue //map task not completed yet
		}
		args := GetFileArgs{
			File: fmt.Sprintf("mr-%d-%d", i, taskID),
		}
		fileReply := GetFileReply{}

		workerAddress := getWorkerAddress(ownerID)
		ok := callWorkerRPC("WorkerRPC.GetFile", workerAddress, &args, &fileReply)
		if !ok {
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

func getWorkerAddress(workerID int) string {
	args := WorkerAddressArgs{
		WorkerID: workerID,
	}
	reply := WorkerAddressReply{}
	call("Coordinator.GetWorkerAddress", &args, &reply)
	if reply.WorkerAddress == "" {
		fmt.Printf("Worker ID %d address not found\n", workerID)
	}
	return reply.WorkerAddress
}
