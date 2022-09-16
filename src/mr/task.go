package mr

import "container/list"

// Task is uniquely identified by TaskId.
// There are three types of tasks: MapTask, ReduceTask, and in
// the case of VoidTask and ExitTask, no other fields will be populated.
type Task struct {
	// One of MapTask, ReduceTask, VoidTask, ExitTask.
	Phase Phase
	// Input filepath. Only applicable for map tasks.
	InputFilepath string
	// Unique identifier for each task.
	TaskId int
	// Identifies Worker executing the task.
	WorkerId int
}

// Tasks is a data structure designed to poll idle tasks efficiently,
// doubling as a tracker of tasks assigned to workers.
// Note: the data structure is *not* thread-safe. Use mutexes accordingly.
type Tasks struct {
	// [IdleQueue, InProgressQueue, CompletedQueue]
	Queue map[Progression]*list.List
	// maps Task.TaskId to individual node containing Task.
	Node map[int]*list.Element
	// maps Task.TaskId to one of Idle, InProgress, or Completed.
	State map[int]Progression
	// Number of total tasks. Constant once initialised.
	Capacity int
}

// Helper function to allocate memory for Tasks.
func allocateTasks(capacity int) *Tasks {
	tasks := &Tasks{
		Queue:    make(map[Progression]*list.List, TotalState),
		Node:     make(map[int]*list.Element),
		State:    make(map[int]Progression),
		Capacity: capacity,
	}

	tasks.Queue[Idle] = list.New()
	tasks.Queue[InProgress] = list.New()
	tasks.Queue[Completed] = list.New()

	return tasks
}

// Create a list of map and reduce tasks, ready to be allocated by the coordinator.
func GenerateTasks(files []string, nReduce int) (mapTasks, reduceTasks *Tasks) {
	nMap := len(files)
	mapTasks, reduceTasks = allocateTasks(nMap), allocateTasks(nReduce)

	// Create map tasks
	for i, file := range files {
		task := &Task{Phase: MapTask, InputFilepath: file, TaskId: i}
		mapTasks.Node[task.TaskId] = mapTasks.Queue[Idle].PushBack(task)
		mapTasks.State[task.TaskId] = Idle
	}

	// Create reduce tasks
	for i := 0; i < nReduce; i++ {
		task := &Task{Phase: ReduceTask, TaskId: i}
		reduceTasks.Node[task.TaskId] = reduceTasks.Queue[Idle].PushBack(task)
		reduceTasks.State[task.TaskId] = Idle
	}

	return
}

// Return Task given Task.TaskId. Function signature obeys "comma, ok" idiom.
func (tasks *Tasks) findTask(taskId int) (*Task, bool) {
	if val, ok := tasks.Node[taskId]; ok {
		return val.Value.(*Task), ok
	}
	return nil, false
}

// GetIdleTask assigns an Idle task if available and updates it to InProgress;
// otherwise it returns a void task
func (tasks *Tasks) GetIdleTask() *Task {
	idleTasks := tasks.Queue[Idle]
	if idleTasks.Len() > 0 {
		taskId := idleTasks.Front().Value.(*Task).TaskId
		tasks.UpdateTaskState(taskId, InProgress)
		task, _ := tasks.findTask(taskId)
		return task
	}
	return &Task{Phase: VoidTask}
}

// UpdateTaskState updates the state of the task to Idle, InProgress, or Completed.
func (tasks *Tasks) UpdateTaskState(taskId int, newState Progression) {
	if _, ok := tasks.findTask(taskId); !ok {
		return
	}

	// Remove task
	state := tasks.State[taskId]
	task := tasks.Queue[state].Remove(tasks.Node[taskId]).(*Task)

	// Insert task
	node := tasks.Queue[newState].PushBack(task)
	tasks.Node[taskId] = node

	// Update state
	tasks.State[taskId] = newState
}

// Returns Task.WorkerId if task exists. Function signature obeys "comma, ok" idiom.
func (tasks *Tasks) GetWorker(taskId int) (int, bool) {
	if task, ok := tasks.findTask(taskId); ok {
		return task.WorkerId, ok
	}
	return 0, false
}

// Assign task to new worker if task exists, otherwise do nothing.
func (tasks *Tasks) SetWorker(taskId int, workerId int) {
	if task, ok := tasks.findTask(taskId); ok {
		task.WorkerId = workerId
	}
}

// Return status of phase completion.
func (tasks *Tasks) Done() bool {
	return tasks.Queue[Completed].Len() == tasks.Capacity
}
