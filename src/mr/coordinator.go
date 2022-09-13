package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskState[T comparable] struct {
	Idle       []T
	InProgress []T
	Completed  []T
	//TaskToWorker map[T]Worker
}

type Coordinator struct {
	// Your definitions here.
	MapTasks        []MapTask
	MapTaskState    TaskState[MapTask]
	ReduceTasks     []ReduceTask
	ReduceTaskState TaskState[ReduceTask]
}

// Your code here -- RPC handlers for the worker to call.
//func (c *Coordinator) RegisterWorker()

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Create map tasks.
	c.MapTasks = files
	c.MapTaskState.Idle = make([]MapTask, len(c.MapTasks))
	copy(c.MapTaskState.Idle, c.MapTasks)

	// TODO: store number of reduce tasks

	// Spin up a server.
	c.server()
	return &c
}
