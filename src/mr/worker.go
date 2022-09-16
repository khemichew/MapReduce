package mr

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
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

type WorkerTask struct {
	Phase           Phase
	TaskId          int
	InputFilepath   string
	TotalMapTask    int
	TotalReduceTask int
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
		if !ok { // Master unresponsive
			log.Fatalf("Lost connection to master server. Worker %v exiting...", os.Getpid())
		}

		// Execute task and store results, then report completion
		terminate, ok := false, true
		switch task.Phase {
		case MapTask:
			doMap(task, mapf)
			terminate, ok = reportTaskCompletion(task)
		case ReduceTask:
			doReduce(task, reducef)
			terminate, ok = reportTaskCompletion(task)
		case VoidTask: // do nothing
		case ExitTask:
			log.Fatalf("No more tasks. Worker %v exiting...", os.Getpid())
		}

		if terminate {
			log.Fatalf("No more tasks. Worker %v exiting...", os.Getpid())
		}

		if !ok {
			log.Fatalf("Connection issues occured. Worker %v exiting...", os.Getpid())
		}

		// Wait for a second before requesting again
		time.Sleep(time.Second)
	}
}

// WorkerId generated only unique in a single machine
func requestTask() (*WorkerTask, bool) {
	args := RequestTaskArgs{WorkerId: os.Getpid()}
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)

	// Create worker task
	task := WorkerTask{
		Phase:           reply.Phase,
		TaskId:          reply.TaskId,
		InputFilepath:   reply.InputFilepath,
		TotalMapTask:    reply.TotalMapTask,
		TotalReduceTask: reply.TotalReduceTask,
	}

	return &task, ok
}

func reportTaskCompletion(task *WorkerTask) (bool, bool) {
	args := ReportTaskArgs{WorkerId: os.Getpid(), Phase: task.Phase, TaskId: task.TaskId}
	reply := ReportTaskReply{}
	ok := call("Coordinator.ReportTaskCompletion", &args, &reply)
	return reply.Terminate, ok
}

// Read input file, apply map function, and partition results
// into nReduce intermediate files.
func doMap(task *WorkerTask, mapf func(string, string) []KeyValue) {
	// Read input file
	content, err := os.ReadFile(task.InputFilepath)
	if err != nil {
		log.Fatalf("cannot read %v", task.InputFilepath)
	}

	// Apply map function
	kva := mapf(task.InputFilepath, string(content))

	// For an assigned task, partition results into r intermediate files,
	// in the form of mr-task-i; i in range(0, r)
	files := make(map[string]*os.File)

	for _, kv := range kva {
		partition := ihash(kv.Key) % task.TotalReduceTask
		filename := fmt.Sprintf("mr-%d-%d", task.TaskId, partition)

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
		err = temp.Close()
		if err != nil {
			log.Fatalf("cannot close file %v", filename)
		}

		err := os.Rename(temp.Name(), filename)
		if err != nil {
			log.Fatalf("cannot rename intermediate file %v", filename)
		}
	}
}

// Read intermediate files, apply reduce function, and store
// results in a single output file.
// This effectively converts the data from row-major to column-major.
func doReduce(task *WorkerTask, reducef func(string, []string) string) {
	kva := make(map[string][]string)

	// Process each intermediate file mr-i-task; i in range(0, m),
	// *if* file exists, where m is the number of mapped files

	// Note: not all mr-i-task files may exist
	// the partition number produced by hashing might not cover the
	// entire range(0, m)
	pattern := fmt.Sprintf("mr-*-%v", task.TaskId)
	matchingFiles, _ := filepath.Glob(pattern)
	var intermediate []string
	// Filter irrelevant inputs
	for _, file := range matchingFiles {
		pattern := fmt.Sprintf("mr-([0-9]+)-%v", task.TaskId)
		if match, _ := regexp.MatchString(pattern, file); match {
			intermediate = append(intermediate, file)
		}
	}

	for _, inputFilename := range intermediate {
		input, err := os.Open(inputFilename)
		if err != nil {
			log.Fatalf("cannot open %v", inputFilename)
		}

		var kv KeyValue
		dec := json.NewDecoder(input)

		// Partition data by key
		for dec.More() {
			err = dec.Decode(&kv)
			if err != nil {
				log.Fatalf("corrupted data in %v", inputFilename)
			}
			kva[kv.Key] = append(kva[kv.Key], kv.Value)
		}

		input.Close()
	}

	// Sort key-value pair in map
	var keys []string
	for k := range kva {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Create temporary output file so user cannot observe partially written
	// files in the presence of a crash
	outputFilename := fmt.Sprintf("mr-out-%v", task.TaskId)
	output, err := os.CreateTemp("", outputFilename)
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
	output.Close()
	err = os.Rename(output.Name(), outputFilename)
	if err != nil {
		log.Fatalf("cannot rename intermediate file %v", outputFilename)
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
