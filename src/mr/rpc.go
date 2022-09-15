package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

const (
	MapTask int = iota
	ReduceTask
	VoidTask
	ExitTask
)

type RequestTaskArgs struct {
	WorkerId int
}

type RequestTaskReply struct {
	TaskType        int
	TaskId          int
	InputFilepath   string // Only applicable for map tasks
	TotalMapTask    int
	TotalReduceTask int
}

type ReportTaskArgs struct {
	WorkerId int
	TaskType int
	TaskId   int
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
