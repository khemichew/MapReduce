package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const Timeout = 10

type Coordinator struct {
	sync.Mutex
	// ------- CRITICAL SECTION -------
	mapTasks    *Tasks
	reduceTasks *Tasks
	// --------------------------------

	//// ------ Server ------
	//listener net.Listener
	//// --------------------
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.Lock()

	// Get task allocation
	// Sequential: process all map tasks before processing reduce tasks
	var task *Task
	if !c.mapTasks.Done() {
		task = c.mapTasks.GetIdleTask()
	} else if !c.reduceTasks.Done() {
		task = c.reduceTasks.GetIdleTask()
	} else {
		task = &Task{Type: VoidTask}
	}

	// Assign task to worker
	task.WorkerId = args.WorkerId

	// Copy values
	reply.TaskId = task.TaskId
	reply.TaskType = task.Type
	reply.TotalMapTask = c.mapTasks.Capacity
	reply.TotalReduceTask = c.reduceTasks.Capacity
	reply.InputFilepath = task.InputFilepath

	// Check for task completion
	c.Unlock()
	go c.waitTask(task.Type, task.TaskId)
	return nil
}

func (c *Coordinator) ReportTaskCompletion(args *ReportTaskArgs, reply *ReportTaskReply) error {
	if !(args.TaskType == MapTask || args.TaskType == ReduceTask) {
		return nil
	}

	c.Lock()
	defer c.Unlock()

	tasks := c.getTasks(args.TaskType)

	// A task is considered complete if task is not rescheduled
	recordedWorkerId := tasks.GetWorker(args.TaskId)
	if recordedWorkerId == args.WorkerId {
		tasks.UpdateTask(args.TaskId, Completed)
	}

	// Allow worker to terminate without having to invoke another RPC
	reply.Terminate = c.mapTasks.Done() && c.reduceTasks.Done()
	return nil
}

//func (c *Coordinator) ShutdownListener(_, _ *interface{}) error {
//	c.listener.Close()
//	return nil
//}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.Lock()
	defer c.Unlock()
	return c.mapTasks.Done() && c.reduceTasks.Done()
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
	//c.listener = l
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//// TODO: Improve shutdown cleanup process
//func (c *Coordinator) shutdownRPCServer() {
//	ok := call("Coordinator.ShutdownListener", nil, nil)
//	if !ok {
//		log.Fatalf("error occured when shutting down server")
//	}
//}

// Countdown until time expires and check task status. If task is incomplete, the
// coordinator reschedules the task.
func (c *Coordinator) waitTask(taskType, taskId int) {
	if !(taskType == MapTask || taskType == ReduceTask) {
		return
	}

	// Wait for timeout seconds
	<-time.After(time.Second * Timeout)

	c.Lock()
	defer c.Unlock()

	// Timeout: reset task to idle state
	tasks := c.getTasks(taskType)
	if tasks.State[taskId] == InProgress {
		tasks.UpdateTask(taskId, Idle)
	}
}

func (c *Coordinator) getTasks(taskType int) *Tasks {
	switch taskType {
	case MapTask:
		return c.mapTasks
	case ReduceTask:
		return c.reduceTasks
	default:
		return nil
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks, reduceTasks := GenerateTasks(files, nReduce)
	c := Coordinator{mapTasks: mapTasks, reduceTasks: reduceTasks}
	c.startRPCServer()
	return &c
}
