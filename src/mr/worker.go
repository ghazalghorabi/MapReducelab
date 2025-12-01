package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
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

	for {
		reply := CallGetTask()

		switch reply.TaskType {
		case MapTask:
			log.Printf("Worker starting map task %d on file %s", reply.TaskId, reply.Filename)
			DoMapTask(reply, mapf)
			ReportDone(MapTask, reply.TaskId)
			log.Printf("Worker done with task %d", reply.TaskId)
		case ReduceTask:
			log.Printf("Worker starting reduce task %d", reply.TaskId)
			DoReduceTask(reply, reducef)
			ReportDone(ReduceTask, reply.TaskId)
			log.Printf("Worker done with reduce task %d", reply.TaskId)
		case WaitTask:
			time.Sleep(time.Millisecond * 500)
		case ExitTask:
			return
		default:
			log.Fatalf("Unknown task type: %v", reply.TaskType)

		}
	}

	// Your worker implementation here.

}

func DoMapTask(r *GetTaskReply, mapf func(string, string) []KeyValue) {
	filename := r.Filename

	// Read file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)

	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// Run mapf
	kva := mapf(filename, string(content))

	// Create buckets
	nReduce := r.NReduce
	files := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)

	for i := 0; i < nReduce; i++ {
		fname := fmt.Sprintf("mr-%v-%v", r.TaskId, i)
		f, _ := os.Create(fname)
		files[i] = f
		encoders[i] = json.NewEncoder(f)
	}
	// Put key/value pairs in buckets using hash
	for _, kv := range kva {
		bucket := ihash(kv.Key) % nReduce
		err := encoders[bucket].Encode(&kv)
		if err != nil {
			log.Fatalf("error encoding kv: %v", err)
		}
	}

	for _, f := range files {
		f.Close()
	}
}

func DoReduceTask(r *GetTaskReply, reducef func(string, []string) string) {
	taskId := r.TaskId
	nMap := r.NMap

	var kva []KeyValue

	for i := 0; i < nMap; i++ {
		filename := fmt.Sprintf("mr-%v-%v", i, taskId)
		file, err := os.Open(filename)

		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	oname := fmt.Sprintf("mr-out-%d", taskId)
	outfile, _ := os.Create(oname)

	i := 0

	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		output := reducef(kva[i].Key, values)

		fmt.Fprintf(outfile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	outfile.Close()

}

func ReportDone(taskType TaskType, taskId int) {
	args := ReportTaskDoneArgs{
		TaskType: taskType,
		TaskId:   taskId,
	}
	reply := ReportTaskDoneReply{}

	ok := call("Coordinator.ReportTaskDone", &args, &reply)
	if !ok {
		fmt.Println("ReportTaskDone call failed")
	}

}
func CallGetTask() *GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		fmt.Println("GetWork call failed")
	}
	return &reply
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//func CallExample() {

// declare an argument structure.
//	args := ExampleArgs{}

// fill in the argument(s).
//	args.X = 99

// declare a reply structure.
//	reply := ExampleReply{}

// send the RPC request, wait for the reply.
// the "Coordinator.Example" tells the
// receiving server that we'd like to call
// the Example() method of struct Coordinator.
//	ok := call("Coordinator.Example", &args, &reply)
//	if ok {
//		// reply.Y should be 100.
//		fmt.Printf("reply.Y %v\n", reply.Y)
//	} else {
//		fmt.Printf("call failed!\n")
//	}
//}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
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
