package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	NoTask
	ExitTask
)

type RequestTaskArgs struct {
	WorkerId int
}

// Task is uniquely identified by Id.
// There are three types of tasks: MapTask, ReduceTask, and in
// the case of NoTask and ExitTask, no fields will be populated.
type Task struct {
	Type          TaskType
	Input         string // only applicable for map tasks
	Id            int
	NumMapTask    int
	NumReduceTask int
}

type ReportTaskReply struct {
	Terminate bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
