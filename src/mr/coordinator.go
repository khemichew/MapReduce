package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskState struct {
	Idle       []string
	InProgress []string
	Completed  []string
}

type Coordinator struct {
	sync.Mutex
	// ------- CRITICAL SECTION -------
	state TaskState
	// --------------------------------

	// ------ Server ------
	listener net.Listener
	// --------------------

	// ------ Channels ------
	done chan bool
	// ----------------------

	// Constant
	nReduce int
	files   []string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *interface{}, reply *Task) error {

	return nil
}

func (c *Coordinator) ShutdownListener(_, _ *interface{}) error {
	c.listener.Close()
	return nil
}

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
func (c *Coordinator) startRPCServer() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	c.listener = l
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// TODO: Improve shutdown cleanup process
func (c *Coordinator) shutdownRPCServer() {
	ok := call("Coordinator.ShutdownListener", nil, nil)
	if !ok {
		log.Fatalf("error occured when shutting down server")
	}
}

func (c *Coordinator) schedule(taskType TaskType) {
	//var nTask, nComplement int
	//switch taskType {
	//case MapTask:
	//	nTask, nComplement = len(c.files), c.nReduce
	//case ReduceTask:
	//	nTask, nComplement = c.nReduce, len(c.files)
	//}
	//
	//// TODO: Start accepting task requests
	//switch taskType {
	//case MapTask:
	//case ReduceTask:
	//}

}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := initialiseCoordinator(files, nReduce)
	c.startRPCServer()

	// Run in a separate goroutine so main thread can track progress
	go func() {
		// Sequential: map task must be completed before reduce task can run
		c.schedule(MapTask)
		c.schedule(ReduceTask)
		// TODO: give out pseudo-exit tasks
		c.shutdownRPCServer()
		c.done <- true
	}()

	return c
}

func initialiseCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files: files, nReduce: nReduce}
	c.done = make(chan bool)

	return &c
}
