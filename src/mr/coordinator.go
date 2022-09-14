package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	sync.Mutex
	// ------- CRITICAL SECTION -------
	mapTasks    Tasks
	reduceTasks Tasks
	// --------------------------------

	// ------ Server ------
	listener net.Listener
	// --------------------
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *interface{}, reply *Task) error {
	return nil
}

func (c *Coordinator) ReportTaskCompletion(args *Task, reply *ReportTaskReply) error {
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.Lock()
	defer c.Unlock()
	return c.mapTasks.done() && c.reduceTasks.done()
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := initialiseCoordinator(files, nReduce)
	c.startRPCServer()
	return c
}

func initialiseCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	return &c
}
