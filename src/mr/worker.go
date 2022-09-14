package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
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
// Worker requests a task from coordinator via RPC call,
// executes the allocated task, stores result in (intermediate)
// output file(s), reports completion to the coordinator, and
// repeats the cycle until no tasks are assigned by the coordinator.
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		// Request a task
		task, ok := requestTask()
		if !ok {
			break
		}

		// Execute task and store results, then report completion
		status, ok := &ReportTaskReply{Terminate: false}, true
		switch task.Type {
		case MapTask:
			doMap(task, mapf)
			status, ok = reportTaskCompletion(task)
		case ReduceTask:
			doReduce(task, reducef)
			status, ok = reportTaskCompletion(task)
		case VoidTask: // do nothing
		case ExitTask:
			break
		}

		if status.Terminate || !ok {
			break
		}

		// Wait for a second before requesting again
		time.Sleep(time.Second)
	}
}

// WorkerId generated only unique in a single machine
func requestTask() (*Task, bool) {
	args := RequestTaskArgs{WorkerId: os.Getpid()}
	reply := Task{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	return &reply, ok
}

func reportTaskCompletion(task *Task) (*ReportTaskReply, bool) {
	reply := ReportTaskReply{}
	ok := call("Coordinator.ReportTask", task, reply)
	return &reply, ok
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
		partition := ihash(kv.Key) % task.NumReduceTask
		filename := fmt.Sprintf("mr-%d-%d", task.Id, partition)

		// Create temporary files so user cannot observe partially
		// written file in the presence of a crash
		if _, ok := files[filename]; !ok {
			temp, err := os.CreateTemp("", filename)
			if err != nil {
				log.Fatalf("cannot create %v", filename)
			}
			files[filename] = temp
		}

		// Append to file
		enc := json.NewEncoder(files[filename])
		err := enc.Encode(&kv)

		if err != nil {
			log.Fatalf("cannot write to %v", filename)
		}
	}

	// Atomically rename files
	for filename, temp := range files {
		err := os.Rename(temp.Name(), filename)
		if err != nil {
			log.Fatalf("cannot rename intermediate file %v", filename)
		}
		err = temp.Close()
		if err != nil {
			log.Fatalf("cannot close file %v", filename)
		}
	}
}

// Read intermediate files, apply reduce function, and store
// results in a single output file.
// This effectively converts the data from row-major to column-major.
func doReduce(task *Task, reducef func(string, []string) string) {
	kva := make(map[string][]string)

	// Process each intermediate file mr-i-task; i in range(0, m),
	// where m is the number of mapped files
	for i := 0; i < task.NumMapTask; i++ {
		inputFilename := fmt.Sprintf("mr-i-%v", task.Id)
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

	// Create temporary output file so user cannot observe partially written
	// files in the presence of a crash
	outputFilename := fmt.Sprintf("mr-out-%v", task.Id)
	output, err := os.CreateTemp("", outputFilename)
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

	// Atomically rename file
	err = os.Rename(output.Name(), outputFilename)
	if err != nil {
		log.Fatalf("cannot rename intermediate file %v", outputFilename)
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
