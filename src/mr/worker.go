package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	task := Task{}

	ok := call("Coordinator.RequestTask", nil, &task)
	if !ok {
		log.Fatal("Master cannot be reached")
	}

	switch task.Type {
	case MapTask:
		doMap(&task, mapf)
	case ReduceTask:
		doReduce(&task, reducef)
	}

}

// Read input file, apply map function, and partition results
// into nReduce intermediate files.
func doMap(task *Task, mapf func(string, string) []KeyValue) {
	// Read input file
	content, err := ioutil.ReadFile(task.Input)
	if err != nil {
		log.Fatalf("cannot read %v", task.Input)
	}

	// Apply map function
	kva := mapf(task.Input, string(content))

	// Partition results into r intermediate files, in
	// the form of mr-task-i; i in range(0, r)
	files := make(map[string]*os.File)

	for _, kv := range kva {
		partition := ihash(kv.Key) % task.NumComplementTask
		output := fmt.Sprintf("mr-%d-%d", task.NumTask, partition)

		// Create file if it does not exist
		if _, ok := files[output]; !ok {
			file, err := os.Create(output)
			if err != nil {
				log.Fatalf("cannot create intermediate file %v", output)
			}
			defer file.Close()
			files[output] = file
		}

		// Append to file
		enc := json.NewEncoder(files[output])
		err := enc.Encode(&kv)

		if err != nil {
			log.Fatalf("unable to write to intermediate file %v", output)
		}
	}

	// Deferred calls run here
}

func doReduce(task *Task, reducef func(string, []string) string) {

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
