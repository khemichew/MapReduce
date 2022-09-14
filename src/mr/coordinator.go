package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskState[T comparable] struct {
	Idle       []T
	InProgress []T
	Completed  []T
}

type Coordinator struct {
	sync.Mutex
	registerChannel chan string

	nReduce int
	files   []string
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

func initialiseCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files: files, nReduce: nReduce}
	c.registerChannel = make(chan string)
	return &c
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

func (c *Coordinator) run() {
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
	c := initialiseCoordinator(files, nReduce)
	c.server()
	c.run()
	return c
}
