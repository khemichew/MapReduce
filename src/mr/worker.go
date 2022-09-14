package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
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

	// For an assigned task, partition results into r intermediate files,
	// in the form of mr-task-i; i in range(0, r)
	files := make(map[string]*os.File)

	for _, kv := range kva {
		partition := ihash(kv.Key) % task.NumComplementTask
		filename := fmt.Sprintf("mr-%d-%d", task.NumTask, partition)

		// Create file if it does not exist
		if _, ok := files[filename]; !ok {
			file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
			if err != nil {
				log.Fatalf("cannot create %v", filename)
			}
			defer file.Close()
			files[filename] = file
		}

		// Append to file
		enc := json.NewEncoder(files[filename])
		err := enc.Encode(&kv)

		if err != nil {
			log.Fatalf("cannot write to %v", filename)
		}
	}

	// Deferred calls run here
}

// Read intermediate files, apply reduce function, and store
// results in a single output file.
// This effectively converts the data from row-major to column-major.
func doReduce(task *Task, reducef func(string, []string) string) {
	kva := make(map[string][]string)

	// Process each intermediate file mr-i-task; i in range(0, m),
	// where m is the number of mapped files
	for i := 0; i < task.NumComplementTask; i++ {
		inputFilename := fmt.Sprintf("mr-i-%v", task.NumTask)
		input, err := os.Open(inputFilename)
		if err != nil {
			log.Fatalf("cannot open %v", inputFilename)
		}

		dec := json.NewDecoder(input)

		// Partition data by key
		for dec.More() {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				log.Fatalf("corrupted data in %v", inputFilename)
			}
			kva[kv.Key] = append(kva[kv.Key], kv.Value)
		}
	}

	// Sort key-value pair in map
	keys := make([]string, len(kva))
	for k := range kva {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Create output file
	outputFilename := fmt.Sprintf("mr-out-%v", task.NumTask)
	output, err := os.OpenFile(outputFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer output.Close()
	if err != nil {
		log.Fatalf("cannot create %v", outputFilename)
	}

	// Apply reduce function
	for _, k := range keys {
		_, err := fmt.Fprintf(output, "%v %v\n", k, reducef(k, kva[k]))
		if err != nil {
			log.Fatalf("cannot write to %v", outputFilename)
		}
	}
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
